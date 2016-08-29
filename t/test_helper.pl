use 5.008000;
use strict;
use warnings;

use AnyEvent;
use Test::MockObject;

BEGIN {
  my %predefined_replies = (
    cluster_slots => [
      [ '0',
        '5961',
        [ '127.0.0.1', '7000', '4b2fa9c315cddbc1c1c729b60ade711fe141d61b' ],
        [ '127.0.0.1', '7003', '14550b7425c44231090719acd5a5d42ad5424b4f' ],
        [ '127.0.0.1', '7005', '050ece77147551db844467770883cf5cd2c8bc2b' ]
      ],
      [ '5962',
        '10922',
        [ '127.0.0.1', '7001', 'f7fc4a7c3f340ea44dc8f92a6fda041dc640de90' ],
        [ '127.0.0.1', '7004', 'a859a49ad96f8312f91fc8c6b402484eda913c83' ]
      ],
      [ '10923',
        '11421',
        [ '127.0.0.1', '7000', '4b2fa9c315cddbc1c1c729b60ade711fe141d61b' ],
        [ '127.0.0.1', '7003', '14550b7425c44231090719acd5a5d42ad5424b4f' ],
        [ '127.0.0.1', '7005', '050ece77147551db844467770883cf5cd2c8bc2b' ]
      ],
      [ '11422',
        '16383',
        [ '127.0.0.1', '7002', '001dadcde7704079c3c6ea679323215c0930af57' ],
        [ '127.0.0.1', '7006', '08c40bd2b18d9c2a20e6d8a27b8da283566a665d' ]
      ],
    ],

    command => [
      [ 'set', -3, [ qw( write denyoom ) ], 1, 1, 1 ],
      [ 'get', '2', [ qw( readonly fast ) ], 1, 1, 1 ],
      [ 'client', -2, [ qw( admin noscript ) ], 0, 0, 0 ],
    ],

    readonly       => 'OK',
    set            => 'OK',
    get            => "some\r\nstring",
    client_getname => 'test',
    multi          => 'OK',
    exec           => [ qw( OK OK ) ],
  );

  Test::MockObject->fake_module( 'AnyEvent::RipeRedis',
    import => sub {
      no strict qw( refs );

      my %error_codes = (
        E_CONN_CLOSED_BY_CLIENT => 5,
        E_ASK                   => 25,
        E_MOVED                 => 26,
      );

      my $caller = caller;
      while ( my ( $name, $err_code ) = each %error_codes ) {
        *{"${caller}::$name"} = sub {
          return $err_code;
        };
      }
    },

    new => sub {
      my $class  = shift;
      my %params = @_;

      my $mock = Test::MockObject->new({});

      $mock->{on_connect}    = $params{on_connect};
      $mock->{on_disconnect} = $params{on_disconnect};

      $mock->{_connected}  = 0;
      $mock->{_multi_mode} = 0;

      $mock->mock( 'execute',
        sub {
          my $self     = shift;
          my $cmd_name = shift;
          my $cbs      = pop;

          unless ( $self->{_connected} ) {
            $self->_connect;
          }

          if ( $cmd_name eq 'multi' ) {
            $self->{_multi_mode} = 1;
          }
          elsif ( $cmd_name eq 'exec'
            || $cmd_name eq 'discard' )
          {
            $self->{_multi_mode} = 0;
          }

          AE::postpone {
            my $reply
                = $self->{_multi_mode}
                    && $cmd_name ne 'exec' && $cmd_name ne 'discard'
                ? 'QUEUED'
                : $predefined_replies{$cmd_name};

            $cbs->{on_reply}->( $predefined_replies{$cmd_name} )
          };
        }
      );

      $mock->mock( 'disconnect',
        sub {
          my $self = shift;
          $self->{on_disconnect}->();
        }
      );

      $mock->mock( '_connect',
        sub {
          my $self = shift;

          $self->{_connected} = 1;
          $self->{on_connect}->();
        }
      );

      unless ( $params{lazy} ) {
        $mock->_connect;
      }

      return $mock;
    },
  );

  *CORE::GLOBAL::rand = sub { return 1 };
}

use AnyEvent::RipeRedis::Cluster;

sub new_cluster {
  my %params = @_;

  return AnyEvent::RipeRedis::Cluster->new(
    startup_nodes => [
      { host => 'localhost', port => 7000 },
      { host => 'localhost', port => 7001 },
    ],
    %params,
  );
}

sub ev_loop {
  my $sub = shift;

  my $cv = AE::cv;

  $sub->($cv);

  my $timer = AE::timer( 10, 0,
    sub {
      diag( 'Emergency exit from event loop.' );
      $cv->send;
    }
  );

  $cv->recv;

  return;
}

1;
