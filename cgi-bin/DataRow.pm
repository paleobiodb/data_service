# note : rjp : 3/2004 : it looks like this class is used to represent a single
# data row returned from the database.  Not exactly sure why we have this.

package DataRow;

use DBI;
use MetadataModel;


# note : rjp : 3/2004 : apparently, we're supposed to pass two arguments
# to this method.  Looks like the first one is an array reference,
# and the second one is a MetadataModel object.
sub new
{
  my $class = shift();
  my $self = {};
	
  bless $self, $class;
	
  my ($row, $md) = @_;
  
  $self->setMetadataModel($md);
  
  $self->setRow($row);
	
	return $self;
}

sub setRow
{
  my ($self, $row) = @_;
  
  if(UNIVERSAL::isa($row, "ARRAY"))
  {
    $self->{_rowArray} = $row;
  }
  elsif(UNIVERSAL::isa($row, "HASH"))
  {
    my %ROW = %{$row};
    my @rowArray;
    my $md = $self->getMetadataModel();
    my @fieldNames = $md->getFieldNames();
    foreach my $fieldName (@fieldNames)
    {
      push(@rowArray, $ROW{$fieldName});
    }
    $self->{_rowArray} = \@rowArray;
  }
}

sub getRowArray
{
	my $self = shift;

	return @{$self->{_rowArray}};
}

sub getRowHash
{
  my $self = shift;
  
  my $md = $self->getMetadataModel();
  
  my @fieldNames = $md->getFieldNames();
  
  my %RETVAL;
  
  foreach my $fieldName (@fieldNames)
  {
    $RETVAL{$fieldName} = $self->getValue($fieldName);
  }
  
  return %RETVAL;
}

sub getValue
{
	my $self = shift;
	my $fieldName = shift;
	#print "DataRow::getValue: Getting value for $fieldName<br>";
	my $md = $self->getMetadataModel();
	my $fieldNum = $md->getFieldNum($fieldName);
	
	return undef unless defined $fieldNum;
	
	return $self->getValueByIndex($fieldNum);
}

sub setValue
{
	my $self = shift;
	my ($fieldName, $val) = @_;
	
	my $md = $self->getMetadataModel();
	my $fieldNum = $md->getFieldNum($fieldName);
	
	unless(defined($fieldNum))
	{
		print "$fieldName is undefined<br>";
		return;
	}
  
	my @rowArray = @{$self->{_rowArray}};
  
	$rowArray[$fieldNum] = $val;
  
	@{$self->{_rowArray}} = @rowArray;
}

sub getValueByIndex
{
	my $self = shift;
	my $fieldNum = shift;
	my @rowArray = $self->getRowArray();
	
	return $rowArray[$fieldNum];
}

# A convenience method
sub addColumn
{
  my $self = shift;
  
  my $md = $self->getMetadataModel();
  
  $md->addColumn(@_);
}

sub setMetadataModel
{
  my ($self, $md) = @_;
  
  $self->{_metadataModel} = $md;
}

sub getMetadataModel
{
	my $self = shift;
	
	return $self->{_metadataModel};
}

sub toString
{
	my $self = shift;
	
	return join(', ', $self->getRowArray());
}

sub toHTMLTableRow
{
	my $self = shift;
	
	return '<tr><td>' . join('</td><td>', $self->getRowArray()) . '</td></tr>';
}
1;
