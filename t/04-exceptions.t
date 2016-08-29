use 5.008000;
use strict;
use warnings;

use Test::More tests => 6;
use Test::Fatal;
use AnyEvent::RipeRedis::Cluster;
require 't/test_helper.pl';

t_startup_nodes();
t_refresh_interval();


sub t_startup_nodes {
  like(
    exception {
      my $cluster = AnyEvent::RipeRedis::Cluster->new();
    },
    qr/Startup nodes not specified/,
    'Startup nodes not specified'
  );

  like(
    exception {
      my $cluster = AnyEvent::RipeRedis::Cluster->new(
        startup_nodes => {},
      );
    },
    qr/Startup nodes must be specified as array reference/,
    'Startup nodes in invalid format (hash reference)'
  );
}

sub t_refresh_interval {
  like(
    exception {
      my $cluster = new_cluster(
        refresh_interval => 'invalid',
      );
    },
    qr/"refresh_interval" must be a positive number/,
    'invalid refresh interval (character string; constructor)'
  );

  like(
    exception {
      my $cluster = new_cluster(
        refresh_interval => -5,
      );
    },
    qr/"refresh_interval" must be a positive number/,
    'invalid refresh interval (negative number; constructor)'
  );

  my $cluster = new_cluster();

  like(
    exception {
      $cluster->refresh_interval('invalid');
    },
    qr/"refresh_interval" must be a positive number/,
    'invalid refresh interval (character string; accessor)'
  );

  like(
    exception {
      $cluster->refresh_interval(-5);
    },
    qr/"refresh_interval" must be a positive number/,
    'invalid refresh interval (negative number; accessor)'
  );

  return;
}
