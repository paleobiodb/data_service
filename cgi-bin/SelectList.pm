package SelectList;

sub new
{
	my $class = shift;
  my $self = {};
  bless $self, $class;
	
	$self->{_valuesByLabel} = {};
	
  return $self;
}

sub setName
{
  my ($self, $name) = @_;
  $self->{_name} = $name;
}

sub setList
{
  my $self = shift;
  $self->{_itemList} = \@_;
}

sub addItem
{
  my $self = shift;
	my ($label, $val) = @_;
  push(@{$self->{_itemList}}, $label);
	${$self->{_valuesByName}}{$label} = $val;
}

sub setSelected
{
  my $self = shift;
  $self->{_selectedItem} = shift;
}

sub getSelected
{
  my $self = shift;
	return $self->{_selectedItem};
}

sub setSize
{
  my $self = shift;
  $self->{_size} = shift;
}

sub setAllowNulls
{
  my $self = shift;
  $self->{_allowNulls} = shift;
}

sub setMainTagStuff
{
	my $self = shift;
	$self->{_maintagstuff} = shift;
}

sub toHTML
{
  my $self = shift;
	
	my @items = @{$self->{_itemList}};
	my $retVal = $self->{htmlString};
	
	$retVal = qq|<select name="$self->{_name}"|;
	$retVal .= qq| size="$self->{_size}"| if $self->{_size};
	# if the select has both "id" and "name", we'll end up with repeated "name"
	# attrs if we don't do this:
	if($self->{_maintagstuff} && $self->{_maintagstuff} !~ /^\s*name="$self->{_name}"\s*$/){
		$retVal .= qq| $self->{_maintagstuff} |;
	}
	$retVal .= '>';
	
	$retVal .= "<option>" if $self->{_allowNulls} && $items[0] ne '';
	
	my $selectedItem = $self->getSelected();
	foreach my $item (@items)
	{
		my $tagString = '<option';
	# fixes "limestone" bug (values with quotes otherwise are not
	#  recognized as matching) 7.3.02 JA
		$item =~ s/"/&quot;/g;
	 	$tagString = '<option selected' if $item eq $selectedItem && $selectedItem ne '';
		my $val = ${$self->{_valuesByName}}{$item};
		$tagString .= qq| value="$val"| if defined $val;
		$tagString .= '>';
		
		$retVal .= $tagString . $item;
	}
	
  return $retVal;
}

1;
