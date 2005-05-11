package Checkbox;

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

sub setValue
{
  my ($self, $val) = @_;
  $self->{_value} = $val;
}

sub setChecked
{
  my ($self, $checked) = @_;
  $self->{_checked} = $checked;
}

sub toHTML
{
  my $self = shift;
  
  $self->{htmlString} = qq|<input type="checkbox" name="$self->{_name}"|;
  $self->{htmlString} .= qq| value="$self->{_value}"| if $self->{_value};
  $self->{htmlString} .= ' checked' if $self->{_checked};
  $self->{htmlString} .= '>';
  
  return $self->{htmlString};
}

1;
