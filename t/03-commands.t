use 5.008000;
use strict;
use warnings;

use Test::More tests => 9;
require 't/test_helper.pl';

my @NODES_CONNECTED;
my @NODES_DISCONNECTED;

my $cluster;

ev_loop(
  sub {
    my $cv = shift;

    $cluster = new_cluster(
      on_node_connect => sub {
        my $host = shift;
        my $port = shift;

        push( @NODES_CONNECTED, [ $host, $port ] );

        return if @NODES_CONNECTED < 7;

        $cv->send;
      },

      on_node_disconnect => sub {
        my $host = shift;
        my $port = shift;

        push( @NODES_DISCONNECTED, [ $host, $port ] );
      },
    );
  }
);

@NODES_CONNECTED = sort {
  $a->[0] cmp $b->[0] || $a->[1] <=> $b->[1]
} @NODES_CONNECTED;

is_deeply( \@NODES_CONNECTED,
  [ [ '127.0.0.1', 7000 ],
    [ '127.0.0.1', 7001 ],
    [ '127.0.0.1', 7002 ],
    [ '127.0.0.1', 7003 ],
    [ '127.0.0.1', 7004 ],
    [ '127.0.0.1', 7005 ],
    [ '127.0.0.1', 7006 ],
    [ 'localhost', 7001 ],
  ],
  'on_node_connect'
);

t_set($cluster);
t_get($cluster);
t_transaction($cluster);
t_multiword_command($cluster);
t_execute($cluster);
t_disconnect($cluster);


sub t_set {
  my $cluster = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->set( 'foo', "some\r\nstring",
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'write; SET' );

  return;
}

sub t_get {
  my $cluster = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->get( 'foo',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, "some\r\nstring", 'reading; GET' );

  return;
}

sub t_transaction {
  my $cluster = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->multi;
      $cluster->set('foo');
      $cluster->set('bar');
      $cluster->exec(
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is_deeply( $t_reply, [ 'OK', 'OK' ], 'transaction; MULTI/EXEC' );

  return;
}

sub t_multiword_command {
  my $cluster = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->client_getname(
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is_deeply( $t_reply, 'test', 'multiword command; CLIENT GETNAME' );

  return;
}

sub t_execute {
  my $cluster = shift;

  can_ok( $cluster, 'execute' );

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->execute( 'set', 'foo', "some\r\nstring",
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'execute' );

  return;
}

sub t_disconnect {
  my $cluster = shift;

  can_ok( $cluster, 'disconnect' );

  $cluster->disconnect;

  @NODES_DISCONNECTED = sort {
    $a->[0] cmp $b->[0] || $a->[1] <=> $b->[1]
  } @NODES_DISCONNECTED;

  is_deeply( \@NODES_DISCONNECTED,
    [ [ '127.0.0.1', 7000 ],
      [ '127.0.0.1', 7001 ],
      [ '127.0.0.1', 7002 ],
      [ '127.0.0.1', 7003 ],
      [ '127.0.0.1', 7004 ],
      [ '127.0.0.1', 7005 ],
      [ '127.0.0.1', 7006 ],
    ],
    'on_node_disconnect'
  );

  return;
}
