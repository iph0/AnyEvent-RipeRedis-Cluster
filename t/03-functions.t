use 5.008000;
use strict;
use warnings;

use Test::More tests => 1;
use AnyEvent::RipeRedis::Cluster;

my $crc16 = AnyEvent::RipeRedis::Cluster::crc16('123456789');
is( $crc16, 0x31c3, 'crc16' );
