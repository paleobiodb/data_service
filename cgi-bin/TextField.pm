package TextField;
require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(setName setText setAllowNulls toHTML);
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

sub setText
{
  my $self = shift;
  $self->{_text} = shift;
}

sub setSize
{
  my $self = shift;
  $self->{_size} = shift;
}

sub setMaxLength
{
  my ($self, $maxLength) = @_;
  $self->{_maxLength} = $maxLength;
}

sub setAllowNulls
{
  my $self = shift;
  $self->{_allowNulls} = shift;
}

sub setDisabled
{
  my $self = shift;
  $self->{_disabled} = 1;
}

sub toHTML
{
  my $self = shift;
  
  $self->{htmlString} = qq|<input name="$self->{_name}"|;
  $self->{htmlString} .= qq| value="$self->{_text}"| if defined $self->{_text};
  $self->{htmlString} .= qq| size="$self->{_size}"| if defined $self->{_size};
  $self->{htmlString} .= qq| maxlength="$self->{_maxLength}"| if defined $self->{_maxLength};
  $self->{htmlString} .= qq| disabled| if defined $self->{_disabled};
  $self->{htmlString} .= '>';
  
  return $self->{htmlString};
}

1;
