package Hidden;
require Exporter;
@ISA = qw(Exporter);

@EXPORT = qw(setName setValue toHTML);
@EXPORT_OK = qw($test_val);

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
  my $self = shift;
  $self->{_value} = shift;
}

sub toHTML
{
  my $self = shift;
  
  $self->{htmlString} = qq|<input type="hidden"|;
  $self->{htmlString} .= qq| name="$self->{_name}"|;
  $self->{htmlString} .= qq| value="$self->{_value}"| if $self->{_value};
  $self->{htmlString} .= '>';
  
  return $self->{htmlString};
}

1;
