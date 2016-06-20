#!/usr/bin/env perl

use strict;
use warnings;

use AnyEvent::RipeRedis::Cluster;
use Data::Dumper;

printf ( "%x\n", AnyEvent::RipeRedis::Cluster::crc16( '123456789' ) );

my $redis = AnyEvent::RipeRedis::Cluster->new(
  startup_nodes => [
    { host => 'localhost', port => 7000 },
    { host => 'localhost', port => 7001 },
    { host => 'localhost', port => 7002 },
  ],

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

$redis->get( 'foo', sub {} );
$redis->set( 'foo', 'bar', sub {} );

$cv->recv();
