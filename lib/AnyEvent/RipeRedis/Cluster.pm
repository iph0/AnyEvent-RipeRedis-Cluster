package AnyEvent::RipeRedis::Cluster;

use 5.008000;
use strict;
use warnings;

our $VERSION = '0.01_01';

use AnyEvent::RipeRedis;

use AnyEvent::Socket;
use Scalar::Util qw( looks_like_number weaken );
use List::MoreUtils qw( bsearch );
use Carp qw( croak );

our %ERROR_CODES;

BEGIN {
  our %ERROR_CODES = %AnyEvent::RipeRedis::ERROR_CODES;
  our @EXPORT_OK   = keys %ERROR_CODES;
  our %EXPORT_TAGS = ( err_codes => \@EXPORT_OK, );
}

use constant {
  MAX_SLOTS => 16384,

  %ERROR_CODES,

  # Operation status
  S_NEED_DO      => 1,
  S_IN_PROGRESS  => 2,
  S_DONE         => 3,
};

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

my %SUB_COMMANDS = (
  subscribe  => 1,
  psubscribe => 1,
);


sub new {
  my $class  = shift;
  my %params = @_;

  my $self = bless {}, $class;

  $self->{startup_nodes} = delete $params{startup_nodes};
  $self->{allow_slaves}
      = exists $params{allow_slaves} ? delete $params{allow_slaves} : 1;
  $self->{lazy} = delete $params{lazy};

  $self->refresh_interval( delete $params{refresh_interval} );
  $self->on_error( delete $params{on_error} );

  $self->{on_node_connect}       = delete $params{on_node_connect};
  $self->{on_node_disconnect}    = delete $params{on_node_disconnect};
  $self->{on_node_connect_error} = delete $params{on_node_connect_error};
  $self->{on_node_error}         = delete $params{on_node_error};

  $self->{_node_params}   = \%params;
  $self->{_nodes_pool}    = {};
  $self->{_masters}       = undef;
  $self->{_slots}         = undef;
  $self->{_commands}      = undef;
  $self->{_init_state}    = S_NEED_DO;
  $self->{_refresh_timer} = undef;
  $self->{_ready}         = 0;
  $self->{_input_queue}   = [];
  $self->{_temp_queue}    = [];
  $self->{_need_multi}    = 0;
  $self->{_forced_slot}   = undef;

  unless ( $self->{lazy} ) {
    $self->_init;
  }

  return $self;
}

sub disconnect {
  my $self = shift;

  # TODO
}

sub refresh_interval {
  my $self = shift;

  if (@_) {
    my $seconds = shift;

    if ( defined $seconds
      && ( !looks_like_number($seconds) || $seconds < 0 ) )
    {
      croak qq{"refresh_interval" must be a positive number};
    }
    $self->{refresh_interval} = $seconds;
  }

  return $self->{refresh_interval};
}

sub on_error {
  my $self = shift;

  if ( @_ ) {
    my $on_error = shift;

    if ( defined $on_error ) {
      $self->{on_error} = $on_error;
    }
    else {
      $self->{on_error} = sub {
        my $err = shift;
        warn $err->message . "\n";
      };
    }
  }

  return $self->{on_error};
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

sub _init {
  my $self  = shift;

  $self->{_init_state} = S_IN_PROGRESS;
  undef $self->{_refresh_timer};

  weaken($self);

  $self->_discover_slots(
    sub {
      my $err = $_[1];

      if ( defined $err ) {
        $self->{_init_state} = S_NEED_DO;
        $self->_abort($err);

        return;
      }

      $self->{_init_state} = S_DONE;

      $self->{_ready} = 1;
      $self->_process_input_queue;

      if ( defined $self->{refresh_interval}
        && $self->{refresh_interval} > 0 )
      {
        $self->{_refresh_timer} = AE::timer(
          $self->{refresh_interval}, 0,
          sub {
            $self->{_init_state} = S_NEED_DO;
            $self->{_ready}      = 0;
          }
        );
      }
    }
  );

  return;
}

sub _discover_slots {
  my $self = shift;
  my $cb   = shift;

  my $nodes_pool = $self->{_nodes_pool};

  my @nodes;
  if ( %{$nodes_pool} ) {
    @nodes = keys %{$nodes_pool};
  }
  else {
    foreach my $node_params ( @{ $self->{startup_nodes} } ) {
      my $hostport = "$node_params->{host}:$node_params->{port}";

      unless ( defined $nodes_pool->{$hostport} ) {
        $nodes_pool->{$hostport} = $self->_new_node(
            $node_params->{host}, $node_params->{port}, 1 );
      }

      push( @nodes, $hostport );
    }
  }

  weaken($self);

  $self->_execute(
    { method => 'cluster_slots',
      args   => [],

      on_reply => sub {
        my $slots = shift;
        my $err   = shift;

        if ( defined $err ) {
          $cb->( undef, $err );
          return;
        }

        $self->_process_slots( $slots,
          sub {
            unless ( defined $self->{_commands} ) {
              $self->_discover_commands($cb);
              return;
            }

            $cb->();
          }
        );
      }
    },
    @nodes,
  );

  return;
}

sub _process_slots {
  my $self      = shift;
  my $slots_raw = shift;
  my $cb        = shift;

  my %nodes_pool;
  my @slots;
  my @masters;
  my @slaves;

  my $old_nodes_pool = $self->{_nodes_pool};

  foreach my $range ( @{$slots_raw} ) {
    my $range_start = shift @{$range};
    my $range_end   = shift @{$range};

    my @nodes;
    my $is_master = 1;

    foreach my $node_info ( @{$range} ) {
      my $hostport = "$node_info->[0]:$node_info->[1]";

      unless ( defined $nodes_pool{$hostport} ) {
        if ( defined $old_nodes_pool->{$hostport} ) {
          $nodes_pool{$hostport} = $old_nodes_pool->{$hostport};
        }
        else {
          $nodes_pool{$hostport} = $self->_new_node( @{$node_info} );

          unless ($is_master) {
            push( @slaves, $hostport );
          }
        }

        if ($is_master) {
          push( @masters, $hostport );
          $is_master = 0;
        }
      }

      push( @nodes, $hostport );
    }

    push( @slots, [ $range_start, $range_end, \@nodes ] );
  }

  @slots = sort { $a->[0] <=> $b->[0] } @slots;

  $self->{_nodes_pool} = \%nodes_pool;
  $self->{_masters}    = \@masters;
  $self->{_slots}      = \@slots;

  if ( $self->{allow_slaves} && @slaves ) {
    $self->_prepare_slaves( \@slaves, $cb );
    return;
  }

  $cb->();

  return;
}

sub _prepare_slaves {
  my $self   = shift;
  my $slaves = shift;
  my $cb     = shift;

  my $reply_cnt = scalar @{$slaves};

  my $cmd = {
    method => 'readonly',
    args   => [],

    on_reply => sub {
      return unless --$reply_cnt == 0;
      $cb->();
    }
  };

  foreach my $hostport ( @{$slaves} ) {
    $self->_execute( $cmd, $hostport );
  }

  return;
}

sub _discover_commands {
  my $self = shift;
  my $cb   = shift;

  weaken($self);
  my @nodes = keys %{ $self->{_nodes_pool} };

  $self->_execute(
    { method => 'command',
      args   => [],

      on_reply => sub {
        my $commands_raw = shift;
        my $err          = shift;

        if ( defined $err ) {
          $cb->( undef, $err);
          return;
        }

        my %commands;

        foreach my $cmd_raw ( @{$commands_raw} ) {
          my $kwd     = lc( $cmd_raw->[0] );
          my $flags   = $cmd_raw->[2];
          my $key_pos = $cmd_raw->[3];

          my $cmd_info = {
            readonly    => 0,
            movablekeys => 0,
            key         => $key_pos,
          };

          foreach my $flag ( @{$flags} ) {
            if ( $flag eq 'readonly' ) {
              $cmd_info->{readonly} = 1;
            }
            if ( $flag eq 'movablekeys' ) {
              $cmd_info->{movablekeys} = 1;
            }
          }

          $commands{$kwd} = $cmd_info;
        }

        $commands{watch}{forcing_slot} = 1;
        foreach my $kwd ( qw( exec discard unwatch ) ) {
          $commands{$kwd}{unforcing_slot} = 1;
        }

        $self->{_commands} = \%commands;

        $cb->();
      }
    },
    @nodes,
  );

  return;
}

sub _new_node {
  my $self = shift;
  my $host = shift;
  my $port = shift;
  my $lazy = shift;

  return AnyEvent::RipeRedis->new(
    %{ $self->{_node_params} },
    host             => $host,
    port             => $port,
    lazy             => $lazy,
    on_connect       => $self->_create_on_node_connect( $host, $port ),
    on_disconnect    => $self->_create_on_node_disconnect( $host, $port ),
    on_connect_error => $self->_create_on_node_connect_error( $host, $port ),
    on_error         => $self->_create_on_node_error( $host, $port ),
  );
}

sub _create_on_node_connect {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken($self);

  return sub {
    if ( defined $self->{on_node_connect} ) {
      $self->{on_node_connect}->( $host, $port );
    }
  };
}

sub _create_on_node_disconnect {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken($self);

  return sub {
    if ( defined $self->{on_node_disconnect} ) {
      $self->{on_node_disconnect}->( $host, $port );
    }
  };
}

sub _create_on_node_connect_error {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken($self);

  return sub {
    my $err = shift;

    if ( defined $self->{on_node_connect_error} ) {
      $self->{on_node_connect_error}->( $err, $host, $port );
    }
  };
}

sub _create_on_node_error {
  my $self = shift;
  my $host = shift;
  my $port = shift;

  weaken($self);

  return sub {
    my $err = shift;

    if ( defined $self->{on_node_error} ) {
      $self->{on_node_error}->( $err, $host, $port );
    }
  };
}

sub _prepare {
  my $self   = shift;
  my $method = shift;
  my $args   = shift;

  weaken($self);

  my $cbs;
  if ( ref( $args->[-1] ) eq 'HASH' ) {
    $cbs = pop @{$args};
  }
  else {
    $cbs = {};
    if ( ref( $args->[-1] ) eq 'CODE' ) {
      if ( exists $SUB_COMMANDS{$method} ) {
        $cbs->{on_message} = pop @{$args};
      }
      else {
        $cbs->{on_reply} = pop @{$args};
      }
    }
  }

  my ( $kwd, @sub_kwds ) = split( m/_/, lc($method) );
  if ( $method eq 'eval_cached' ) {
    undef @sub_kwds;
  }

  my $cmd = {
    method   => $method,
    kwd      => $kwd,
    sub_kwds => \@sub_kwds,
    args     => $args,
    %{$cbs},
  };

  unless ( defined $cmd->{on_reply} ) {
    $cmd->{on_reply} = sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        $self->{on_error}->( $err, $reply );
        return;
      }
    }
  }

  return $cmd;
}

sub _route {
  my $self = shift;
  my $cmd  = shift;

  unless ( $self->{_ready} ) {
    if ( $self->{_init_state} == S_NEED_DO ) {
      $self->_init;
    }

    push( @{ $self->{_input_queue} }, $cmd );

    return;
  }

  if ( $cmd->{kwd} eq 'multi' && !defined $self->{_forced_slot} ) {
    AE::postpone { $cmd->{on_reply}->('OK') };
    $self->{_need_multi} = 1;

    return;
  }

  weaken($self);

  $self->_get_route( $cmd,
    sub {
      my $slot         = shift;
      my $allow_slaves = shift;

      my @nodes;

      if ( defined $slot ) {
        if ( $self->{_need_multi} ) {
          $self->{_need_multi} = 0;

          $self->_route(
            { method   => 'multi',
              kwd      => 'multi',
              sub_kwds => [],
              args     => [],
            }
          );

          $self->_route($cmd);

          return;
        }

        my ($range) = bsearch {
          $slot > $_->[1] ? -1 : $slot < $_->[0] ? 1 : 0;
        }
        @{ $self->{_slots} };

        @nodes
            = $allow_slaves
            ? @{ $range->[2] }
            : $range->[2][0];
      }
      else {
        @nodes
            = $allow_slaves
            ? keys %{ $self->{_nodes_pool} }
            : @{ $self->{_masters} };
      }

      $self->_execute( $cmd, @nodes );
    }
  );

  return;
}

sub _execute {
  my $self  = shift;
  my $cmd   = shift;
  my @nodes = @_;

  my $hostport;
  if ( scalar @nodes > 1 ) {
    my $rand_index = int( rand( scalar @nodes ) );
    $hostport = splice( @nodes, $rand_index, 1 );
  }
  else {
    $hostport = shift @nodes;
  }
  my $node        = $self->{_nodes_pool}{$hostport};
  my $forced_slot = $self->{_forced_slot};

  weaken($self);

  my $on_reply = sub {
    my $reply = shift;
    my $err   = shift;

    if ( defined $err ) {
      my $err_code   = $err->code;
      my $nodes_pool = $self->{_nodes_pool};

      if ( ( $err_code == E_MOVED || $err_code == E_ASK )
        && !defined $forced_slot )
      {
        if ( $err_code == E_MOVED ) {
          $self->{_init_state} = S_NEED_DO;
          $self->{_ready}      = 0;
        }

        my ( $slot, $fwd_hostport )
            = ( split( m/\s+/, $err->message ) )[ 1, 2 ];

        unless ( defined $nodes_pool->{$fwd_hostport} ) {
          my ( $host, $port ) = parse_hostport($fwd_hostport);
          $nodes_pool->{$fwd_hostport} = $self->_new_node( $host, $port );
        }

        $self->_execute( $cmd, $fwd_hostport );

        return;
      }

      my $on_node_error = $cmd->{on_node_error} || $self->{on_node_error};

      if ( defined $on_node_error ) {
        my $node = $nodes_pool->{$hostport};
        $on_node_error->( $err, $node->host, $node->port );
      }

      if ( !defined $forced_slot && @nodes ) {
        $self->_execute( $cmd, @nodes );
        return;
      }

      $cmd->{on_reply}->( $reply, $err );

      return;
    }

    $cmd->{on_reply}->($reply);

    return;
  };

  my $method = $cmd->{method};

  $node->$method( @{ $cmd->{args} },
    { on_reply => $on_reply,

      defined $cmd->{on_message}
      ? ( on_message => $cmd->{on_message} )
      : (),
    }
  );

  return;
}

sub _get_route {
  my $self = shift;
  my $cmd  = shift;
  my $cb   = shift;

  my $cmd_info = $self->{_commands}{ $cmd->{kwd} };

  if ( defined $self->{_forced_slot} ) {
    my $slot = $self->{_forced_slot};

    if ( $cmd_info->{unforcing_slot} ) {
      undef $self->{_forced_slot};
    }

    $cb->( $slot, undef );

    return;
  }

  $self->_get_key( $cmd,
    sub {
      my $key = shift;

      unless ( defined $key ) {
        $cb->( undef, $self->{allow_slaves} );
        return;
      }

      my $slot = _get_slot($key);

      if ( $cmd_info->{forcing_slot} || $self->{_need_multi} ) {
        $self->{_forced_slot} = $slot;
      }

      $cb->( $slot, $self->{allow_slaves} && $cmd_info->{readonly} );
    }
  );

  return;
}

sub _get_key {
  my $self = shift;
  my $cmd  = shift;
  my $cb   = shift;

  my $kwd      = $cmd->{kwd};
  my @args     = ( @{ $cmd->{sub_kwds} }, @{ $cmd->{args} } );
  my $cmd_info = $self->{_commands}{$kwd};

  my $key;

  if ( $cmd_info->{movablekeys} ) {
    my @nodes
        = $self->{allow_slaves}
        ? keys %{ $self->{_nodes_pool} }
        : @{ $self->{_masters} };

    $self->_execute(
      { method        => 'command_getkeys',
        args          => [ $kwd, @args ],
        on_node_error => $cmd->{on_node_error},

        on_reply => sub {
          my $keys = shift;
          my $err  = shift;

          if ( defined $err ) {
            $cmd->{on_reply}->( undef, $err );
            return;
          }

          if ( @{$keys} ) {
            $key = $keys->[0];
          }

          $cb->($key);
        }
      },
      @nodes,
    );

    return;
  }
  elsif ( $cmd_info->{key} > 0 ) {
    $key = $args[ $cmd_info->{key} - 1 ];
  }

  $cb->($key);

  return;
}

sub _process_input_queue {
  my $self = shift;

  $self->{_temp_queue}  = $self->{_input_queue};
  $self->{_input_queue} = [];

  while ( my $cmd = shift @{ $self->{_temp_queue} } ) {
    $self->_route($cmd);
  }

  return;
}

sub _abort {
  my $self = shift;
  my $err  = shift;

  my @queued_commands = $self->_queued_commands;

  $self->{_input_queue} = [];
  $self->{_temp_queue}  = [];

  if ( defined $err ) {
    my $err_msg  = $err->message;
    my $err_code = $err->code;

    $self->{on_error}->($err);

    foreach my $cmd (@queued_commands) {
      my $err = AnyEvent::RipeRedis::Error->new(
        qq{Operation "$cmd->{kwd}" aborted: $err_msg}, $err_code );

      $cmd->{on_reply}->( undef, $err );
    }
  }

  return;
}

sub _queued_commands {
  my $self = shift;

  return (
    @{ $self->{_temp_queue} },
    @{ $self->{_input_queue} },
  );
}

sub _get_slot {
  my $key = shift;

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

  my $sub = sub {
    my $self = shift;
    my $cmd  = $self->_prepare( $method, [@_] );

    $self->_route($cmd);

    return;
  };

  do {
    no strict 'refs';
    *{$method} = $sub;
  };

  goto &{$sub};
}

sub DESTROY {
  my $self = shift;

  if ( defined $self->{_input_queue} ) {
    my @queued_commands = $self->_queued_commands;

    foreach my $cmd (@queued_commands) {
      warn "Operation \"$cmd->{kwd}\" aborted:"
          . " Client object destroyed prematurely.\n";
    }
  }

  return;
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
