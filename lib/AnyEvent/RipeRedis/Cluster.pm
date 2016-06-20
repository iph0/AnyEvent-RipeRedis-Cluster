package AnyEvent::RipeRedis::Cluster;

use 5.008000;
use strict;
use warnings;

our $VERSION = '0.01';

use AnyEvent::Redis::RipeRedis;

use Scalar::Util qw( weaken );
use List::Util qw( shuffle );

use constant {
 MAX_SLOTS     => 16384,
 MAX_REDIRECTS => 5,
};

my @CONNECTOR_PARAM_NAMES = qw(
  password
  database
  encoding
  connection_timeout
  read_timeout
  lazy
  reconnect
  handle_params
);

my @CRC16_TAB = (
  0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
  0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
  0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
  0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
  0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
  0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
  0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
  0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
  0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
  0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
  0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
  0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
  0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
  0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
  0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
  0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
  0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
  0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
  0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
  0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
  0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
  0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
  0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
  0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
  0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
  0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
  0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
  0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
  0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
  0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
  0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
  0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
);


sub new {
  my $class  = shift;
  my %params = @_;

  my $self = bless {}, $class;

  $self->{startup_nodes} = $params{startup_nodes};
  $self->{scale_reads}
      = exists $params{scale_reads} ? $params{scale_reads} : 1;
  $self->{default_node} = $params{default_node};

  $self->{on_node_connect}       = $params{on_node_connect};
  $self->{on_node_disconnect}    = $params{on_node_disconnect};
  $self->{on_node_connect_error} = $params{on_node_connect_error};
  $self->{on_node_error}         = $params{on_node_error};

  my %connector_params;
  foreach my $name ( @CONNECTOR_PARAM_NAMES ) {
    next unless defined $params{$name};
    $connector_params{$name} = $params{$name};
  }
  $self->{_connector_params} = \%connector_params;

  $self->{_nodes}       = {};
  $self->{_slots}       = [];
  $self->{_commands}    = {};
  $self->{_ready}       = 0;
  $self->{_input_queue} = [];
  $self->{_tmp_queue}   = [];

  # TODO check lazy
  my @shuffled_nodes = shuffle( @{ $self->{startup_nodes} } );
  $self->_discover_cluster( \@shuffled_nodes );

  return $self;
}

sub crc16 {
    my $str = shift;

    unless ( utf8::downgrade( $str, 1 ) ) {
      utf8::encode( $str );
    }

    my $crc = 0;

    foreach my $char ( split //, $str ) {
      $crc = ( $crc << 8 & 0xffff )
          ^ $CRC16_TAB[ ( ( $crc >> 8 ) ^ ord($char) ) & 0xff ];
    }

    return $crc;
}

sub _discover_cluster {
  my $self          = shift;
  my $startup_nodes = shift;
  my $node_num      = shift || 0;

  my $node_params = $startup_nodes->[$node_num];

  my $redis = $self->_prepare_connector( $node_params->{host},
      $node_params->{port} );

  $redis->cluster_slots(
    sub {
      my $slot_ranges = shift;

      $redis->disconnect();
      undef $redis;

      if (@_) {
         unless ( ++$node_num < $self->{startup_nodes} ) {
          # TODO handle error
          # Can't discover cluster through nodes: ...

          return;
        }

        $self->_discover_cluster( $startup_nodes, $node_num );

        return;
      }

      my $nodes = $self->{_nodes};
      my $slots = $self->{_slots};

      foreach my $slot_range ( @{$slot_ranges} ) {
        my @connectors;
        my $length = scalar @{$slot_range};

        foreach my $node ( @{$slot_range}[ 2 .. $length - 1 ] ) {
          my $connector;

          unless ( defined $nodes->{"$node->[0]:$node->[1]"} ) {
            $connector = $self->_prepare_connector( $node->[0], $node->[1] );
            $nodes->{"$node->[0]:$node->[1]"} = $connector;
          }

          push( @connectors, $nodes->{"$node->[0]:$node->[1]"} );
        }

        for ( $slot_range->[0] .. $slot_range->[1] ) {
          $slots->[$_] = \@connectors;
        }
      }

      my @shuffled_nodes = shuffle( values %{$nodes} );
      $self->_discover_commands( \@shuffled_nodes );
    }
  );

  return;
}

sub _prepare_connector {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  return AnyEvent::Redis::RipeRedis->new(
    %{ $self->{_connector_params} },
    host             => $host,
    port             => $port,
    on_connect       => $self->_get_on_node_connect( $host, $port ),
    on_disconnect    => $self->_get_on_node_disconnect( $host, $port ),
    on_connect_error => $self->_get_on_node_connect_error( $host, $port ),
    on_error         => $self->_get_on_node_error( $host, $port ),
  );
}

sub _get_on_node_connect {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken( $self );

  return sub {
    if ( defined $self->{on_node_connect} ) {
      $self->{on_node_connect}->( $host, $port );
    }
  };
}

sub _get_on_node_disconnect {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken( $self );

  return sub {
    if ( defined $self->{on_node_disconnect} ) {
      $self->{on_node_disconnect}->( $host, $port );
    }
  };
}

sub _get_on_node_connect_error {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken( $self );

  return sub {
    my $err_msg = shift;

    if ( defined $self->{on_node_connect_error} ) {
      $self->{on_node_connect_error}->( $host, $port, $err_msg );
    }
  };
}

sub _get_on_node_error {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken( $self );

  return sub {
    my $err_msg = shift;
    my $err_code = shift;

    if ( defined $self->{on_node_error} ) {
      $self->{on_node_error}->( $host, $port, $err_msg, $err_code );
    }
  };
}

sub _discover_commands {
  my $self     = shift;
  my $nodes    = shift;
  my $node_num = shift || 0;

  my $redis = $nodes->[$node_num];

  $redis->command(
    sub {
      my $commands_info = shift;

      if (@_) {
         unless ( ++$node_num < $self->{_nodes} ) {
          # TODO handle error
          # Can't discover commands through nodes: ...

          return;
        }

        $self->_discover_commands( $nodes, $node_num );

        return;
      }

      foreach my $cmd_info ( @{$commands_info} ) {
        my $keyword = lc( $cmd_info->[0] );

        my $write_flag       = 0;
        my $movablekeys_flag = 0;

        foreach my $flag ( @{ $cmd_info->[2] } ) {
          if ( $flag eq 'write' ) {
            $write_flag = 1;
          }
          elsif ( $flag eq 'movablekeys' ) {
            $movablekeys_flag = 1;
          }
        }

        $self->{_commands}{$keyword} = {
          args_num      => $cmd_info->[1],
          write         => $write_flag,
          movablekeys   => $movablekeys_flag,
          first_key_pos => $cmd_info->[3],
          last_key_pos  => $cmd_info->[4],
          keys_step     => $cmd_info->[5],
        };
      }

      $self->{_ready} = 1;
      $self->_flush_input_queue();
    }
  );

  return;
}

sub _execute_cmd {
  my $self = shift;
  my $cmd  = shift;

  unless ( $self->{_ready} ) {
    push( @{ $self->{_input_queue} }, $cmd );

    return;
  }

  my $cmd_info = $self->{_commands}{ $cmd->{kwd} };

  if ( $cmd_info->{movablekeys} ) {
    # TODO COMMAND GETKEYS from any node

    return;
  }



  my $first_key_pos = $self->{_commands}{ $cmd->{kwd} }{first_key_pos};


  if ( $first_key_pos != 0 ) {
    print "$cmd->{args}[ $first_key_pos - 1 ] !!!!\n";
  }

  return;
}

sub _route_cmd {
  my $self      = shift;
  my $first_key = shift;
  my $cmd       = shift;

  my $cmd_info = $self->{_commands}{ $cmd->{kwd} };

  return;
}

sub _flush_input_queue {
  my $self = shift;

  $self->{_tmp_queue}   = $self->{_input_queue};
  $self->{_input_queue} = [];

  while ( my $cmd = shift @{ $self->{_tmp_queue} } ) {
    $self->_execute_cmd($cmd);
  }

  return;
}

sub _get_shuffled_nodes_by_key {
  my $self = shift;
  my $key  = shift;

  my $slot = $self->_get_slot_by_key($key);

  return shuffle @{ $self->{_slots}[$slot] };
}

sub _get_master_node_by_key {
  my $self = shift;
  my $key  = shift;

  my $slot = $self->_get_slot_by_key($key);

  return $self->{_slots}[$slot][0];
}

sub _get_slot_by_key {
  my $self = shift;
  my $key  = shift;

  my $tag = $key;

  if ( $key =~ m/\{([^}]*?)\}/ ) {
    if ( length $1 > 0 ) {
      $tag = $1;
    }
  }

  return crc16($tag) % MAX_SLOTS;
}

sub AUTOLOAD {
  our $AUTOLOAD;
  my $method = $AUTOLOAD;
  $method =~ s/^.+:://;
  my ( $kwd, @args ) = split( m/_/, lc($method) );

  my $sub = sub {
    my $self = shift;
    my $cb   = pop;

    my $cmd = {
      kwd  => $kwd,
      args => [ @args, @_ ],
      cb   => $cb,
    };

    $self->_execute_cmd($cmd);

    return;
  };

  do {
    no strict 'refs';
    *{$method} = $sub;
  };

  goto &{$sub};
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

AnyEvent::RipeRedis::Cluster - Perl extension for blah blah blah

=head1 SYNOPSIS

  use AnyEvent::RipeRedis::Cluster;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for AnyEvent::RipeRedis::Cluster, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.


=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Eugene Ponizovsky, E<lt>iph@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Eugene Ponizovsky

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.20.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
