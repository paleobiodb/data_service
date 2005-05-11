package Anchor;

sub new
{
  my $class = shift;
  my $self = {};
  bless $self, $class;
  
  return $self;
}

sub setName
{
  my ($self, $name) = @_;
  $self->{_name} = $name;
}

sub setHref
{
  my $self = shift;
  $self->{_href} = shift;
}

sub toHTML
{
  my $self = shift;
  
  $self->{htmlString} = qq|<a href="$self->{_href}"|;
  $self->{htmlString} .= qq| name="$self->{_name}"| if $self->{_name};
  $self->{htmlString} .= '>';
  
  return $self->{htmlString};
}

1;
