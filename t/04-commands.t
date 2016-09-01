use 5.008000;
use strict;
use warnings;

use Test::More tests => 22;
BEGIN {
  require 't/test_helper.pl';
}

my @NODES_CONNECTED;
my @NODES_DISCONNECTED;

my $cluster;

ev_loop(
  sub {
    my $cv = shift;

    $cluster = new_cluster(
      refresh_interval   => 5,
      connection_timeout => 5,
      read_timeout       => 5,
      handle_params      => {
        autocork => 1,
      },

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
t_error_reply($cluster);
t_default_on_error($cluster);
t_global_on_node_error();
t_on_node_error_for_command($cluster);
t_transaction($cluster);
t_multiword_command($cluster);
t_movable_keys($cluster);
t_execute_method($cluster);
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

sub t_error_reply {
  my $cluster = shift;

  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->hget( 'foo', 'test',
        sub {
          my $reply = shift;
          $t_err    = shift;

          $cv->send;
        }
      );
    }
  );

  my $t_npref = 'error reply';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  ok( defined $t_err->message, "$t_npref; error message" );
  is( $t_err->code, E_LOADING_DATASET, "$t_npref; error code" );

  return;
}

sub t_default_on_error {
  my $cluster = shift;

  my $cv;
  my $t_err_msg;

  local $SIG{__WARN__} = sub {
    $t_err_msg = shift;

    chomp( $t_err_msg );

    $cv->send;
  };

  ev_loop(
    sub {
      $cv = shift;

      $cluster->hget( 'foo', 'test' );
    }
  );

  ok( defined $t_err_msg, q{Default "on_error" callback} );

  return;
}

sub t_global_on_node_error {
  my $t_err;
  my @t_node_errors;

  my $cluster = new_cluster(
    on_node_error => sub {
      my $err  = shift;
      my $host = shift;
      my $port = shift;

      push( @t_node_errors, [ $err, $host, $port ] );
    }
  );

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->hget( 'foo', 'test',
        sub {
          my $reply = shift;
          $t_err    = shift;

          $cv->send;
        }
      );
    }
  );

  my $t_npref = 'global "on_node_error"';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  ok( defined $t_err->message, "$t_npref; error message" );
  is( $t_err->code, E_LOADING_DATASET, "$t_npref; error code" );

  my $err = AnyEvent::RipeRedis::Error->new(
      q{LOADING Redis is loading the dataset in memory}, E_LOADING_DATASET );

  @t_node_errors = sort {
    $a->[1] cmp $b->[1] || $a->[2] <=> $b->[2]
  } @t_node_errors;

  is_deeply( \@t_node_errors,
    [ [ $err, '127.0.0.1', 7002 ],
      [ $err, '127.0.0.1', 7006 ],
    ],
    "$t_npref; node errors"
  );

  return;
}

sub t_on_node_error_for_command {
  my $cluster = shift;

  my $t_err;
  my @t_node_errors;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->hget( 'foo', 'test',
        { on_reply => sub {
            my $reply = shift;
            $t_err    = shift;

            $cv->send;
          },

          on_node_error => sub {
            my $err  = shift;
            my $host = shift;
            my $port = shift;

            push( @t_node_errors, [ $err, $host, $port ] );
          },
        }
      );
    }
  );

  my $t_npref = '"on_node_error" for command';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  ok( defined $t_err->message, "$t_npref; error message" );
  is( $t_err->code, E_LOADING_DATASET, "$t_npref; error code" );

  my $err = AnyEvent::RipeRedis::Error->new(
      q{LOADING Redis is loading the dataset in memory}, E_LOADING_DATASET );

  @t_node_errors = sort {
    $a->[1] cmp $b->[1] || $a->[2] <=> $b->[2]
  } @t_node_errors;

  is_deeply( \@t_node_errors,
    [ [ $err, '127.0.0.1', 7002 ],
      [ $err, '127.0.0.1', 7006 ],
    ],
    "$t_npref; node errors"
  );

  return;
}

sub t_transaction {
  my $cluster = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->multi( 'foo' );
      $cluster->set( '{foo}1', "some\r\nstring" );
      $cluster->set( '{foo}2', 42 );
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

sub t_movable_keys {
  my $cluster = shift;

  my $t_reply = shift;

  ev_loop(
    sub {
      my $cv = shift;

      $cluster->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
          2, '{key}1', '{key}2', 'first', 'second',
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

  is_deeply( $t_reply, [qw( {key}1 {key}2 first second )], 'movable keys' );

  return;
}

sub t_execute_method {
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

  is( $t_reply, 'OK', 'execute method' );

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
