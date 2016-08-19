#!/usr/bin/env perl

use strict;
use warnings;

use AnyEvent::RipeRedis::Cluster;
use Data::Dumper;
use Test::LeakTrace;

printf ( "%x\n", AnyEvent::RipeRedis::Cluster::crc16( '123456789' ) );

#no_leaks_ok {
  my $redis = AnyEvent::RipeRedis::Cluster->new(
    startup_nodes => [
      { host => 'localhost', port => 7000 },
      { host => 'localhost', port => 7001 },
      { host => 'localhost', port => 7002 },
    ],
  default_slot     => 1,
  allow_slaves     => 1,
  refresh_interval => 5,
  lazy             => 1,

    on_error => sub {
      my $err = shift;
      print Dumper( $err );
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

  my $cv = AE::cv();

  $cv->begin;

  $redis->set( 'foo', 'bar',
    sub {
      print Dumper( \@_ );
      $cv->end;
    },
  );

  $cv->begin;

  $redis->get( 'foo',
    sub {
      print Dumper( \@_ );
      $cv->end;
    }
  );

  $cv->begin;

  $redis->get( 'foo',
    sub {
      print Dumper( \@_ );
      $cv->end;
    }
  );

  $cv->begin;

  $redis->get( 'foo',
    sub {
      print Dumper( \@_ );
      $cv->end;
    }
  );

#  $cv->begin;
#
#  $redis->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
#      2, '{key}1', '{key}2', 'first', 'second',
#    sub {
#      print Dumper( \@_ );
#      $cv->end;
#    }
#  );

  $cv->begin;

  $redis->info(
    sub {
      print $_[0]->{redis_version} . "\n";
      $cv->end;
    }
  );

  $cv->begin;

  $redis->watch( 'foo831',
    sub {
      print Dumper( \@_ );
    },
  );
  $redis->multi(
    sub {
      print Dumper( \@_ );
    }
  );
  $redis->get( 'foo',
    sub {
      print Dumper( \@_ );
    }
  );
  #$redis->multi(
  #  sub {
  #    print Dumper( \@_ );
  #  }
  #);
  $redis->get( 'foo',
    sub {
      print Dumper( \@_ );
    }
  );
  $redis->exec(
    { on_reply => sub {
        print Dumper( \@_ );
        $cv->end;
      },
      on_node_error => sub {
        print Dumper( \@_ );
      },
    }
  );

  $cv->begin;

  $redis->get( 'foo',
    { on_reply => sub {
        print Dumper( \@_ );
        $cv->end;
      },
      on_node_error => sub {
        print Dumper( \@_ );
      },
    }
  );

  $cv->recv();

  undef $redis;
#} 'leaks';
