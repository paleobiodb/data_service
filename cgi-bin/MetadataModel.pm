package MetadataModel;

sub new
{
	my ($class, $sth) = @_;
	my $self = {};
	
	$self->{_sth} = $sth;
	
	bless $self, $class;
	
	$self->loadMetadata() if $sth;
	
	return $self;
}

sub loadMetadata
{
	my $self = shift;
	my $sth = $self->{_sth};
	
  #print "MetadataModel::loadMetadata: " . join(', ', @{$sth->{NAME}});
	$self->setFieldNames(@{$sth->{NAME}});
	$self->{_numFields} = $sth->{NUM_OF_FIELDS};
	$self->{_numericFieldFlags} = $sth->{mysql_is_num};
	$self->{_fieldTypeNames} = $sth->{mysql_type_name};
	$self->{_nullableFlags} = $sth->{NULLABLE};
	$self->{_priKeyFlags} = $sth->{mysql_is_pri_key};
	$self->{_fieldTypeCodes} = $sth->{mysql_type};
	$self->{_precisions} = $sth->{PRECISION};
	# Get the table name (I'm starting to think that DBI kinda sucks!)
	my $selectString = $sth->{Statement};
	my ($tableName) = ($selectString =~ /SELECT.+?FROM\s+(\w+)/ims);
	$self->{_tableName} = $tableName;
	
	# Needs work: This is an evil hack.  Normalize the database or get better metadata
	my %SET_FIELDS_BY_TABLE_NAME = (refs=>[project_name=>1],
																	collections=>{lithadj=>1,pres_mode=>1,assembl_comps=>1,collection_coverage=>1,coll_meth=>1,lithadj2=>,feed_pred_traces=>1},
																	#occurrences=>[],
																	#authorities=>[],
																	#opinions=>[],
																	#reidentifications=>[]
																	);
	$self->{_setFields} = \%SET_FIELDS_BY_TABLE_NAME;
}

sub isOfTypeSet
{
	my $self = shift;
	my $fieldName = shift;
	
	# If this field is of type 'set', get the correct value
	my %table_hash = %{$self->{_setFields}{$self->getTableName()}};

	if($table_hash{$fieldName})
	{
		return 1;
	}

	return 0;
}

sub isNullable
{
	my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @nullableFlags = @{$self->{_nullableFlags}};
	my $isNullable = $nullableFlags[$fieldNum];
	
	return $isNullable;
}

sub isPrimaryKey
{
	my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @priKeyFlags = @{$self->{_priKeyFlags}};
	my $isPriKey = $priKeyFlags[$fieldNum];
	
	return $isPriKey;
}

sub isNumeric
{
	my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @numericFlags = @{$self->{_numericFieldFlags}};
	my $isNumeric = $numericFlags[$fieldNum];
	
	return $isNumeric;
}

sub getFieldTypeName
{
	my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @fieldTypeNames = @{$self->{_fieldTypeNames}};
	
	return $fieldTypeNames[$fieldNum];
}

sub getFieldTypeCode
{
	my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @fieldTypeCodes = @{$self->{_fieldTypeCodes}};
	
	return $fieldTypeCodes[$fieldNum];
}

sub getPrecision
{
  my $self = shift;
	my $fieldName = shift;
	
	my $fieldNum = $self->getFieldNum($fieldName);
	my @fieldPrecisions = @{$self->{_precisions}};
	
	return $fieldPrecisions[$fieldNum];
}

sub getNumFields
{
	my $self = shift;
	
  my @fieldNames = $self->getFieldNames();
  my $numFields = @fieldNames;
  
	return $numFields;#$self->{_numFields};
}

sub setFieldNames
{
  my ($self, @fieldNames) = @_;
  
	my $fieldCount = 0;
	foreach my $fieldName (@fieldNames)
	{
		$self->{_fieldNumbersByFieldName}{$fieldName} = $fieldCount;
		$fieldCount++;
	}
  $self->{_fieldNames} = \@fieldNames;
}

sub getFieldNames
{
	my $self = shift;
	
	return @{$self->{_fieldNames}};
}

sub getFieldNum
{
	my $self = shift;
	my $fieldName = shift;
	
	return $self->{_fieldNumbersByFieldName}{$fieldName};
}

sub getFieldName
{
	my $self = shift;
	my $fieldNum = shift;
	
	my @fieldNames = $self->getFieldNames();
	
	return $fieldNames[$fieldNum];
}

sub getTableName
{
	my $self = shift;

	return $self->{_tableName};
}

sub addColumn
{
  my ($self, $colName, $numeric, $nullable) = @_;
  
  return if defined $self->getFieldNum($colName);
  
  $self->{_fieldNumbersByFieldName}{$colName} = $self->getNumFields();
  push(@{$self->{_fieldNames}}, $colName);
  push(@{$self->{_numericFieldFlags}}, $numeric);
  push(@{$self->{_nullableFlags}}, $nullable);
}

sub toString
{
  my $self = shift;
  
  return $self->getTableName() . ':' . join(', ', $self->getFieldNames());
}

1;
