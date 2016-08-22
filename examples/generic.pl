#!/usr/bin/env perl

use strict;
use warnings;

use AnyEvent::RipeRedis::Cluster;

my $REDIS = AnyEvent::RipeRedis::Cluster->new(
  startup_nodes => [
    { host => 'localhost', port => 7000 },
    { host => 'localhost', port => 7001 },
  ],
  refresh_interval => 5,

  on_node_connect => sub {
    my $host = shift;
    my $port = shift;

    print "Connected to $host:$port\n";
  },

  on_node_disconnect => sub {
    my $host = shift;
    my $port = shift;

    print "Disconnected from $host:$port\n";
  },

  on_node_error => sub {
    my $err  = shift;
    my $host = shift;
    my $port = shift;

    print "$host:$port: " . $err->message . "\n";
  },

  on_error => sub {
    my $err = shift;
    print $err->message . "\n";
  },
);

my $cv = AE::cv;
my $timer;

$REDIS->get( '__last__',
  sub {
    my $num = shift || 0;
    my $err = shift;

    if ( defined $err ) {
      print $err->message . "\n";
      return;
    }

    $timer = AE::timer( 0, 0.1,
      sub {
        set_get( $num++ );
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

$REDIS->disconnect;


sub set_get {
  my $num = shift;

  $REDIS->set( "foo$num", $num,
    sub {
      my $err = $_[1];

      if ( defined $err ) {
        print $err->message . "\n";
        return;
      }

      $REDIS->get( "foo$num",
        sub {
          my $reply = shift;

          if ( defined $err ) {
            print $err->message . "\n";
            return;
          }

          print "$reply\n";
        }
      );
    }
  );

  $REDIS->set( '__last__', $num,
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        print $err->message . "\n";
        return;
      }
    }
  );

  return;
}
