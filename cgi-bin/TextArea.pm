package TextArea;
require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(setName setText setRows setCols setWrapVirtual toHTML);
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

sub setRows
{
  my $self = shift;
  $self->{_rows} = shift;
}

sub setCols
{
  my $self = shift;
  $self->{_cols} = shift;
}

sub setWrapVirtual
{
  my $self = shift;
  $self->{_wrap_virtual} = shift;
}

sub toHTML
{
  my $self = shift;
  
  $self->{htmlString} = qq|<textarea name="$self->{_name}"|;
  $self->{htmlString} .= qq| cols="$self->{_cols}"| if $self->{_cols};
  $self->{htmlString} .= qq| rows="$self->{_rows}"| if $self->{_rows};
  $self->{htmlString} .= qq| wrap="virtual"| if $self->{_wrap_virtual};
  $self->{htmlString} .= '>';
  $self->{htmlString} .= qq|$self->{_text}|;
  
  return $self->{htmlString};
}

1;
