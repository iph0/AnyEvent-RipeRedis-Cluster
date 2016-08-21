#!/usr/bin/env perl

use strict;
use warnings;

use AnyEvent::RipeRedis::Cluster;
use Data::Dumper;

my $redis = AnyEvent::RipeRedis::Cluster->new(
  startup_nodes => [
    { host => 'localhost', port => 7000 },
    { host => 'localhost', port => 7001 },
  ],
  refresh_interval => 5,

  on_error => sub {
    my $err = shift;
    print Dumper($err);
  },

  on_node_connect => sub {
    print "Connected\n";
    print Dumper( \@_ );
  },

  on_node_disconnect => sub {
    print "Disconnected\n";
    print Dumper( \@_ );
  },

  on_node_error => sub {
    print Dumper( \@_ );
  },
);

my $cv = AE::cv;
my $timer;

$redis->get( '__last__',
  sub {
    my $last = shift || 0;
    my $err  = shift;

    if ( defined $err ) {
      print Dumper($err);
      $cv->send;

      return;
    }

    $timer = AE::timer( 0, 0.1,
      sub {
        $redis->set( "foo$last", $last,
          sub {
            my $reply = shift;
            my $err   = shift;

            if ( defined $err ) {
              print Dumper($err);
              return;
            }

            $redis->get( "foo$last",
              sub {
                my $reply = shift;

                if ( defined $err ) {
                  print Dumper($err);
                  return;
                }

                print "$reply\n";
              }
            );

            $last++;
          }
        );

        $redis->set( '__last__', $last,
          sub {
            my $reply = shift;
            my $err   = shift;

            if ( defined $err ) {
              print Dumper($err);
              return;
            }
          }
        );
      }
    );
  }
);

my $on_signal = sub {
  print "Stopped\n";
  $cv->send;
};

my $int_w  = AE::signal( INT  => $on_signal );
my $term_w = AE::signal( TERM => $on_signal );

$cv->recv;

$redis->disconnect;
