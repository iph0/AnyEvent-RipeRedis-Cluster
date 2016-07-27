package AnyEvent::RipeRedis::Cluster::Node;

use 5.008000;
use strict;
use warnings;

use AnyEvent::RipeRedis;

use Scalar::Util qw( weaken );
use Carp qw( croak );

use constant {
  S_IN_PROGRESS => 1,
  S_DONE        => 2,
};


sub new {
  my $class  = shift;
  my %params = @_;

  my $self = bless {}, $class;

  $self->{_connector}      = AnyEvent::RipeRedis->new(%params);
  $self->{_processing_cnt} = 0;
  $self->{_finalize_state} = undef;
  $self->{_on_finalize}    = undef;

  return $self;
}

sub host {
  my $self = shift;
  return $self->{connector}->host;
}

sub port {
  my $self = shift;
  return $self->{connector}->port;
}

sub execute {
  my $self = shift;
  my $cmd  = shift;

  $self->_begin;

  weaken($self);

  my %cbs = (
    on_reply => sub {
      $self->_end;
      $cmd->{on_reply}->(@_);
    },
  );
  if ( defined $cmd->{on_message} ) {
    $cbs{on_message} = $cmd->{on_message};
  }

  my $kwd = $cmd->{kwd};
  $self->{_connector}->$kwd( @{ $cmd->{args} }, \%cbs );

  return;
}

sub finalize {
  my $self        = shift;
  my $on_finalize = shift;

  $self->{_finalize_state} = S_IN_PROGRESS;
  $self->{_on_finalize}    = $on_finalize;

  if ( $self->{_processing_cnt} == 0 ) {
    $self->_finalize;
  }

  return;
}

sub _begin {
  my $self = shift;

  if ( defined $self->{_finalize_state} ) {
    my $reason
        = $self->{_finalize_state} == S_IN_PROGRESS
        ? 'is finalizing'
        : 'was finalized';

    croak qq{Redis node $reason and can't handle the command};
  }

  $self->{_processing_cnt}++;

  return;
}

sub _end {
  my $self = shift;

  $self->{_processing_cnt}--;

  if ( $self->{_finalize_state} == S_IN_PROGRESS
    && $self->{_processing_cnt} == 0 )
  {
    $self->_finalize;
  }

  return;
}

sub _finalize {
  my $self = shift;

  undef $self->{_connector};
  $self->{_finalize_state} = S_DONE;

  if ( $self->{_on_finalize} ) {
    $self->{_on_finalize}->();
  }

  return;
}

1;
