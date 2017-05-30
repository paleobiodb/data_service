# 
# The Paleobiology Database
# 
#   TimescaleEdit.pm
# 

package TimescaleEdit;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use CommonEdit;
use TableDefs qw($TIMESCALE_DATA $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS $TIMESCALE_PERMS
	         $INTERVAL_DATA $INTERVAL_MAP $SCALE_MAP $MACROSTRAT_SCALES $MACROSTRAT_INTERVALS
	         $MACROSTRAT_SCALES_INTS);
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use TimescaleTables qw(%TIMESCALE_ATTRS %TIMESCALE_BOUND_ATTRS %TIMESCALE_REFDEF);

use base 'EditTransaction';



sub add_timescale {
    
    
    
    
}


sub update_timescale {



}


sub delete_timescale {



}


# Add a new boundary according to the specified attributes.

sub add_boundary {
    
    my ($edt, $attrs, $conditions) = @_;
    
    croak "add_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
    croak "add_boundary: must not have a value for bound_id\n" if $attrs->{bound_id};
    
    my $dbh = $edt->dbh;
    
    # Make sure that we know what timescale to create the boundary in, and
    # that a bound type was specified.
    
    unless ( $attrs->{timescale_id} )
    {
        $edt->add_condition("E_BOUND_TIMESCALE: you must specify a value for 'timescale_id'");
    }
    
    unless ( $attrs->{bound_type} )
    {
	$edt->add_condition("E_BOUND_TYPE: you must specify a value for 'bound_type'");
    }
    
    # Then check for missing or redundant attributes. These will vary by bound type.
    
    my $timescale_id = $attrs->{timescale_id};
    my $bound_type = $attrs->{bound_type};
    
    if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
    {
	$edt->add_condition("E_AGE_MISSING: you must specify a value for 'age' with this bound type")
	    unless $attrs->{age};

	$edt->add_condition("W_BASE_IGNORED: the value of 'base_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'same' )
    {
	$edt->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id};
	
	$edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};

	$edt->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
    {
	$edt->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id};
	
	$edt->add_condition("E_OFFSET_MISSING: you must specify a value for 'offset' with this bound type")
	    unless $attrs->{offset};
	
	$edt->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};
	
	if ( $bound_type eq 'percent' )
	{
	    $edt->add_condition("E_RANGE_MISSING: you must specify a value for 'range_id' with this bound type")
		unless $attrs->{range_id};
	}
	
	else
	{
	    $edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
		if $attrs->{range_id};
	}
    }
    
    # Now check that all of the specified attributes are of the correct type and in the correct
    # value range, and that all references to other records match up to existing records.
    
    my ($fields, $values) = $edt->check_timescale_attrs('add', 'bound', $attrs, $conditions);
    
    # Then make sure that the necessary attributes have the proper values.
    
    $edt->check_bound_values($attrs);
    
    # If any errors occurred, or if $check_only was specified, we stop here.
    
    return $edt if $conditions->{CHECK_ONLY} || $edt->status eq 'ERROR';
    
    # Otherwise, insert the new record.
    
    my $sql = "INSERT INTO $TIMESCALE_BOUNDS ($fields) VALUES ($values)";
    my ($insert_result, $insert_id);
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    try {
	$insert_result = $dbh->do($sql);
	$insert_id = $dbh->last_insert_id;
	
	if ( $insert_result )
	{
	    $edt->status('OK');
	    $edt->record_keys($insert_id, $timescale_id);
	}
	
	else
	{
	    $edt->add_condition("E_INTERNAL: record could not be created");
	}
    }
	
    catch {
	print STDERR "ERROR: $_\n";
	$edt->add_condition("E_INTERNAL: an error occurred while inserting this record");
    }
    
    return $edt;
}


sub update_boundary {

    my ($edt, $attrs, $conditions) = @_;
    
    croak "update_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
    croak "update_boundary: must have a value for bound_id\n" unless $attrs->{bound_id};
    
    my $dbh = $edt->dbh;
    
    my $bound_id = $attrs->{bound_id};
    
    # We first need to make sure that the record to be updated actually exists, and fetch its
    # current attributes.
    
    unless ( $bound_id =~ /^\d+$/ && $bound_id > 0 )
    {
	return $edt->add_condition("E_BOUND_ID: bad value '$bound_id' for 'bound_id'");
    }
    
    my ($current) = $dbh->selectrow_hashref("
		SELECT * FROM $TIMESCALE_BOUNDS WHERE bound_no = $bound_id");
    
    unless ( $current )
    {
	return $edt->add_condition("E_NOT_FOUND: boundary '$bound_id' is not in the database");
    }
    
    # If a timescale_id was specified, it must match the current one otherwise an error will be
    # thrown. It is not permitted to move a boundary to a different timescale.
    
    if ( defined $attrs->{timescale_id} && $attrs->{timescale_id} ne '' )
    {
	if ( $current->{timescale_no} && $current->{timescale_no} ne $attrs->{timescale_id} )
	{
	    $edt->add_condition("E_BOUND_TIMESCALE: you cannot change the timescale associated with a bound");
	}
    }
    
    # Check for missing or redundant attributes. These will vary by bound type.
    
    my $bound_type = $attrs->{bound_type} || $current->{bound_type};
    my $timescale_id = $current->{timescale_no};
    
    if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
    {
	$edt->add_condition("E_AGE_MISSING: you must specify a value for 'age' with this bound type")
	    unless defined $attrs->{age} || defined $current->{age};
	
	$edt->add_condition("W_BASE_IGNORED: the value of 'base_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'same' )
    {
	$edt->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id} || $current->{base_no};
	
	$edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$edt->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if defined $attrs->{age};

	$edt->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
    {
	$edt->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id} || $current->{base_no};
	
	$edt->add_condition("E_OFFSET_MISSING: you must specify a value for 'offset' with this bound type")
	    unless $attrs->{offset};
	
	$edt->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};
	
	if ( $bound_type eq 'percent' )
	{
	    $edt->add_condition("E_RANGE_MISSING: you must specify a value for 'range_id' with this bound type")
		unless $attrs->{range_id} || $current->{range_no};
	}
	
	else
	{
	    $edt->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
		if $attrs->{range_id};
	}
    }
    
    # Now check that all of the specified attributes are of the correct type and in the correct
    # value range, and that all references to other records match up to existing records.
    
    my ($set_list) = $edt->check_timescale_attrs('update', 'bound', $attrs, $conditions);
    
    # Then make sure that the necessary attributes have the proper values.
    
    $edt->check_bound_values($attrs, $current);
    
    # If any errors occurred, or if $check_only was specified, we stop here.
    
    return $edt if $conditions->{CHECK_ONLY} || $edt->status eq 'ERROR';
    
    # Otherwise, update the record.
    
    my $sql = "	UPDATE $TIMESCALE_BOUNDS SET $set_list, is_updated = 1, modified = now()
		WHERE bound_no = $bound_id";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    my $update_result;
    
    try {
	$update_result = $dbh->do($sql);
	
	if ( $update_result )
	{
	    $edt->status('OK');
	    $edt->record_keys($bound_id, $timescale_id);
	}
	
	else
	{
	    $edt->add_condition("E_INTERNAL: record was not updated");
	}
    }
    
    catch {
	print STDERR "ERROR: $_\n";
	$edt->add_condition("E_INTERNAL: an error occurred while updating this record");
    };
    
    return $edt;
}


sub update_boundary_prepare {
    
    my ($edt) = @_;
    
    my $dbh = $edt->dbh;
    
    my $sql = " UPDATE $TIMESCALE_BOUNDS SET is_updated = 0";
    
    $dbh->do($sql);
}


sub delete_boundary {

    my ($edt, $attrs, $conditions) = @_;
    
    croak "update_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
    croak "update_boundary: must have a value for bound_id\n" unless $attrs->{bound_id};
    
    my $dbh = $edt->dbh;
    
    my $bound_id = $attrs->{bound_id} ? "$attrs->{bound_id}" : "";
    my $timescale_id = $attrs->{timescale_id} ? "$attrs->{timescale_id}" : "";
    my $select_which = $bound_id ne '' ? 'bound' : 'timescale';
    
    # If we are deleting a single boundary, we need to make sure the record actually exists and
    # fetch its current attributes.
    
    if ( $bound_id ne '' )
    {
	unless ( $bound_id =~ /^\d+$/ && $bound_id > 0 )
	{
	    return $edt->add_condition("E_BOUND_ID: bad value '$bound_id' for 'bound_id'");
	}
	
	my ($current) = $dbh->selectrow_hashref("
		SELECT * FROM $TIMESCALE_BOUNDS WHERE bound_no = $bound_id");
	
	unless ( $current )
	{
	    $edt->status('OK');
	    $edt->record_keys(undef, $timescale_id) if $timescale_id;
	    return $edt->add_condition("W_NOT_FOUND: boundary '$bound_id' is not in the database");
	}
	
	# If we get here, then there is a record in the database that we can delete. If a
	# timescale id was specified, it had better match the one in the record.
	
	if ( defined $attrs->{timescale_id} && $attrs->{timescale_id} ne '' )
	{
	    if ( $current->{timescale_no} && $current->{timescale_no} ne $attrs->{timescale_id} )
	    {
		$edt->add_condition("E_BOUND_TIMESCALE: the specified bound is not associated with the specified timescale");
	    }
	}
	
	# Keep track of what timescale this bound is in, if we didn't know it originally.
	
	$timescale_id = $current->{timescale_id};
    }
    
    # If we are given a timescale_id but not a bound_id, check to make sure that timescale
    # actually exists.
    
    elsif ( $timescale_id ne '' )
    {
	unless ( $timescale_id =~ /^\d+$/ && $timescale_id > 0 )
	{
	    return $edt->add_condition("E_TIMESCALE_ID: bad value '$timescale_id' for 'timescale_id'");
	}
	
	my ($ts) = $dbh->selectrow_hashref("
		SELECT * FROM $TIMESCALE_DATA WHERE timescale_no = $timescale_id");
	
	unless ( $ts )
	{
	    return $edt->add_condition("E_NOT_FOUND: timescale '$timescale_id' is not in the database");
	}
    }
    
    # Otherwise, we weren't given anything to work with.
    
    else
    {
	return $edt->add_condition("E_PARAM: you must specify either 'bound_id' or 'timescale_id'");
    }
    
    # Now check to see if there are any other boundaries that depend on this one. If so, then we
    # need to deal with them. If the condition RELATED_RECORDS is allowed, then cut each of these
    # records loose. Otherwise, return a caution.
    
    my $sql;
    
    if ( $bound_id ne '' )
    {
	$sql = "SELECT count(*) FROM $TIMESCALE_BOUNDS
		WHERE base_no = $bound_id or range_no = $bound_id or
			color_no = $bound_id or refsource_no = $bound_id";
    }
    
    else
    {
	$sql = "SELECT count(*) FROM $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as source on tsb.base_no = source.bound_no
		or tsb.range_no = source.bound_no or tsb.color_no = source.bound_no
		or tsb.refsource_no = source.bound_no
		WHERE source.timescale_no = $timescale_id";
    }
    
    my ($dependent_count) = $dbh->selectrow_hashref($sql);
    
    if ( $dependent_count )
    {
	print STDERR "$sql\n\n" if $edt->debug;
	
	unless ( $conditions->{RELATED_RECORDS} )
	{
	    return $edt->add_condition("C_RELATED_RECORDS: there are $dependent_count other bounds that depend on this one");
	}
	
	$edt->detach_related_bounds('bound', $bound_id) || return;
    }
    
    # If we get here, then we can delete. We return an OK as long as no exception is caught, on
    # the assumption that the delete statement is so simple that the only way it could go wrong is
    # if the record is somehow already gone.
    
    $sql = "	DELETE FROM $TIMESCALE_BOUNDS WHERE bound_no = $bound_id";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    my $delete_result;
    
    try {
	$delete_result = $dbh->do($sql);
	
	$edt->add_condition("W_INTERNAL: problem deleting record") unless $delete_result;
	$edt->status('OK');
	$edt->record_keys(undef, $timescale_id);
    }
    
    catch {
	print STDERR "ERROR: $_\n";
	$edt->add_condition("E_INTERNAL: an error occurred while deleting this record");
    }
    
    return $edt;
}


# detach_related_bounds ( select_which, id, options )
# 
# Look for bounds which have the specified bound as a source (or all bounds from the specified
# timescale). Convert these so that this relationship is broken. If $select_which is 'timescale',
# then we are preparing to delete all the bounds in the specified timescale.

sub detach_related_bounds {

    my ($edt, $select_which, $id) = @_;
    
    my $dbh = $edt->dbh;
    
    # Construct the proper filter expression.
    
    my $filter;
    my @sql;
    
    if ( $select_which eq 'bound' )
    {
	$filter = "bound_no in ($id)";
    }
    
    elsif ( $select_which eq 'timescale' )
    {
	$filter = "timescale_no in ($id)";
    }
    
    else
    {
	croak "bad value for 'select_which'\n";
    }
    
    # Detach all derived-color relationships.
    
    $sql[0] = "	UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as source on tsb.color_no = source.bound_no
		SET tsb.color_no = 0
		WHERE $filter";
    
    # Then detach all reference relationships.
    
    $sql[1] = "	UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as source on tsb.refsource_no = source.bound_no
		SET tsb.refsource_no = 0
		WHERE $filter";
    
    # Then detach all range relationships.
    
    $sql[2] = "	UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as source on tsb.range_no = source.bound_no
		SET tsb.bound_type = if(tsb.bound_type = 'percent', 'absolute', tsb.bound_type),
		    tsb.range_no = 0,
		    tsb.base_no = if(tsb.bound_type = 'percent', 0, tsb.base_no)
		WHERE $filter";
    
    # Then detach all base relationships.
    
    $sql[3] = " UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as source on tsb.base_no = source.bound_no
		SET tsb.bound_type = if(tsb.bound_type not in ('absolute','spike'), 'absolute', tsb.bound_type),
		    tsb.base_no = 0
		WHERE $filter";
    
    # Now execute all of these.
    
    try {

	foreach my $i ( 0..3 )
	{
	    print STDERR "$sql[$i]\n\n" if $edt->debug;
	    
	    $dbh->do($sql[$i]);
	}
    }
    
    catch {
	print STDERR "ERROR: $_\n";
	$edt->add_condition("E_INTERNAL: an error occurred while deleting this record");
    }
    
    return $edt;
}



my %IGNORE_ATTRS = ( bound_id => 1, record_id => 1 );

sub check_timescale_attrs {
    
    my ($edt, $op, $record_type, $attrs, $conditions) = @_;
    
    my @sql_list;
    my @field_list;
    
    my $dbh = $edt->dbh;
    
    # Check the attributes one by one, according to their specified types.
    
    my $specification = $record_type eq 'bound' ? \%TIMESCALE_BOUND_ATTRS : 
	\%TIMESCALE_ATTRS;
    
    foreach my $k ( keys %$attrs )
    {
	my $value = $attrs->{$k};
	my $type = $specification->{$k};
	my $quoted;
	
	# First make sure the field name and value are okay.
	
	if ( $IGNORE_ATTRS{$k} )
	{
	    next;
	}
	
	elsif ( ! defined $type )
	{
	    $edt->add_condition("W_BAD_FIELD: $k: unknown attribute");
	    next;
	}
	
	elsif ( ! defined $value )
	{
	    # no checking need be done if the value is undefined
	    
	    $quoted = 'NULL';
	}
	
	# Special case the interval names
	
	elsif ( $k eq 'interval_name' || $k eq 'lower_name' )
	{
	    my $quoted_name = $dbh->quote($value);
	    
	    my ($id) = $dbh->selectrow_array("
		SELECT interval_no FROM $TIMESCALE_INTS WHERE interval_name like $quoted_name");
	    
	    if ( $id )
	    {
		$k =~ s/name/no/;
		$quoted = $id;
	    }
	    
	    elsif ( $conditions->{CREATE_INTERVALS} )
	    {
		$dbh->do("INSERT INTO $TIMESCALE_INTS (interval_name) VALUES ($quoted_name)");
		$quoted = $dbh->last_insert_id();
		
		unless ( $quoted )
		{
		    croak "could not create interval $quoted_name\n";
		}
	    }
	    
	    else
	    {
		$edt->add_condition("C_CREATE_INTERVALS: $k: not found");
	    }
	}
	
	# Otherwise, check other types
	
	elsif ( $type eq 'varchar80' )
	{
	    if ( length($value) > 80 )
	    {
		$edt->add_condition("E_TOO_LONG: $k: must be 80 characters or less");
		next;
	    }
	    
	    $quoted = $dbh->quote($value);
	}
	
	elsif ( $type eq 'colorhex' )
	{
	    unless ( $value =~ qr{ ^ \# [0-9a-z]{6} $ }xsi )
	    {
		$edt->add_condition("E_BAD_COLOR: $k: must be a valid color in hexadecimal notation");
		next;
	    }
	    
	    $quoted = $dbh->quote(uc $value);
	}
	
	elsif ( $type eq 'pos_decimal' )
	{
	    unless ( $value =~ qr{ ^ (?: \d* [.] \d+ | \d+ [.] \d* | \d+ ) $ }xsi )
	    {
		$edt->add_condition("E_BAD_NUMBER: $k: must be a positive decimal number");
		next;
	    }
	    
	    $quoted = $value
	}
	
	elsif ( $type =~ /_no$/ )
	{
	    my ($idtype, $table, $label) = @{$TIMESCALE_REFDEF{$type}};
	    
	    if ( $value =~ $ExternalIdent::IDRE{$idtype} && $2 > 0 )
	    {
		$quoted = $2;
	    }
	    
	    else
	    {
		$edt->add_condition("E_BAD_KEY: $k: must be a valid $label identifier");
		next;
	    }
	    
	    my $check_value;
	    
	    eval {
		($check_value) = $dbh->selectrow_array("SELECT $type FROM $table WHERE $type = $quoted");
	    };
	    
	    unless ( $check_value )
	    {
		$edt->add_condition("E_KEY_NOT_FOUND: $k: the identifier $quoted was not found in the database");
		next;
	    }
	}
	
	elsif ( ref $type eq 'HASH' )
	{
	    if ( $type->{lc $value} )
	    {
		$quoted = $dbh->quote(lc $value);
	    }
	    
	    else
	    {
		$edt->add_condition("E_BAD_VALUE: $k: value not acceptable");
		next;
	    }
	}
	
	else
	{
	    croak "check_attrs: bad data type for '$k'\n";
	}
	
	# Then create the proper SQL expressions for it.
	
	if ( $op eq 'update' )
	{
	    push @sql_list, "$k = $quoted";
	}
	
	else
	{
	    push @field_list, $k;
	    push @sql_list, $quoted;
	}
    }
    
    if ( $op eq 'update' )
    {
	return join(', ', @sql_list);
    }
    
    else
    {
	return join(',', @field_list), join(',', @sql_list);
    }
}


sub check_bound_values {

    my ($edt, $new, $current) = @_;
    
    my $new_age = $new->{age};
    my $new_offset = $new->{offset};
    my $new_bound_type = $new->{bound_type};
    
    # If we are specifying any of 'bound_type', 'age', 'offset', then make sure 'age'
    # and 'offset' have the proper range for the bound type.
    
    if ( (defined $new->{age} && $new->{age} ne '') ||
	 (defined $new->{offset} && $new->{offset} ne '') ||
	 $new->{bound_type} )
    {
	if ( $current )
	{
	    unless ( defined $new_age and $new_age ne '')
	    {
		$new_age = $current->{age};
	    }
	    
	    unless ( defined $new_offset and $new_offset ne '')
	    {
		$new_offset = $current->{offset};
	    }
	    
	    $new_bound_type ||= $current->{bound_type};
	}
	
	if ( $new_bound_type eq 'percent' )
	{
	    unless ( defined $new_offset && $new_offset ne '' && $new_offset >= 0.0 && $new_offset <= 100.0 )
	    {
		$edt->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a percentage between 0 and 100.0 for this bound type");
	    }
	}
	
	elsif ( $new_bound_type eq 'offset' )
	{
	    unless ( defined $new_offset && $new_offset ne '' && $new_offset >= 0.0 && $new_offset <= 1000.0 )
	    {
		$edt->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a value between 0 and 1000.0 Ma for this bound type");
	    }
	}
	
	elsif ( $new_bound_type eq 'absolute' || $new_bound_type eq 'spike' )
	{
	    unless ( defined $new_age && $new_age ne '' && $new_age >= 0.0 && $new_age <= 4600.0 )
	    {
		$edt->add_condition("E_AGE_RANGE: the value of 'age' must be a value between 0 and 4600.0 Ma for this bound type");
	    }
	}
    }
    
    # If we are setting 'base_id' or 'range_id', make sure that both of these are from other
    # timescales.
    
    my @check_bounds;
    
    push @check_bounds, $new->{base_id} if $new->{base_id};
    push @check_bounds, $new->{range_id} if $new->{range_id};
    
    my $timescale_id = $new->{timescale_id} || $current->{timescale_id};
    
    if ( @check_bounds && $timescale_id )
    {
	my $list = join(',', @check_bounds);
	
	my ($check) = $dbh->selectrow_array("
		SELECT timescale_no FROM $TIMESCALE_BOUNDS
		WHERE bound_no in ($list) and timescale_no = $timescale_id");
	
	if ( $check )
	{
	    $edt->add_condition("E_INVALID_KEY: the base and range bounds must be from a different timescale");
	}
    }
    
    return $edt;
}


# propagate_boundary_changes ( dbh, source_bounds )
# 
# Propagate any changes to interval boundaries and timescales to the boundaries that refer to
# them. The parameter $source_bounds may be set to indicate which bounds have changed. If so, then
# the first iteration of the propagation loop will check only bounds which directly reference
# these. This will improve the efficiency of making small changes.

sub propagate_boundary_changes {

    my ($edt, $select_which, $source_nos) = @_;
    
    $options ||= { };
    
    my $update_count;
    my $source_selector = '> 0';
    my $sql;
    
    # First create an SQL filter based on $source_nos.
    
    my $selector = $select_which eq 'bounds' ? 'bound_no' : 'timescale_no';
    
    if ( ref $source_bounds eq 'ARRAY' )
    {
	$source_selector = "$selector in (" . join(',', @$source_nos) . ')';
    }
    
    elsif ( $source_bounds )
    {
	$source_selector = "$selector in ($source_nos)";
    }
    
    # Start by marking the updated records.
    
    $sql = "	UPDATE $TIMESCALE_BOUNDS as tsb SET is_updated = 1
		WHERE $source_selector";
    
    # Then execute a loop, propagating updated information one step at a time. Each newly updated
    # record gets the update flag set as well.
    
    $update_count = 1;
    
    while ( $update_count )
    {
	# First update bounds
	
	$sql = "    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.base_no
		left join $TIMESCALE_BOUNDS as top on top.bound_no = tsb.range_no
	    SET tsb.is_updated = 1, 
		tsb.derived_age = case tsb.bound_type
			when 'same' then base.age
			when 'offset' then base.age + tsb.offset
			when 'percent' then base.age + tsb.offset * ( base.age - top.age )
			end,
		tsb.derived_age_error = case tsb.bound_type
			when 'same' then base.age_error
			when 'offset' then base.age_error + tsb.offset_error
			when 'percent' then base.age_error + tsb.offset_error * ( base.age - top.age )
			end
	    WHERE base.is_updated or top.is_updated";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" if $options->{debug} && $update_count && $update_count > 0;
    }
    
    # Now do the same for colors
    
    $update_count = 1;
    
    while ( $update_count )
    {
	$sql = "
	    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.color_no
	    SET tsb.is_updated = 1, tsb.derived_color = base.color
	    WHERE base.is_updated";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" if $options->{debug} && $update_count && $update_count > 0;
    }

    # And then for reference_nos
    
    $update_count = 1;
    
    while ( $update_count )
    {
	$sql = "
	    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.refsource_no
	    SET tsb.is_updated = 1, tsb.derived_reference_no = base.reference_no
	    WHERE base.is_updated";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" 
	    if $options->{debug} && $update_count && $update_count > 0;
    }
    
    # Now, in all updated records, set or clear the 'is_different' bit depending upon
    # whether the derived attributes are different from the main ones.
    
    $sql = "
	UPDATE $TIMESCALE_BOUNDS as tsb
	SET tsb.is_different = tsb.age <> tsb.derived_age or tsb.age_error <> tsb.derived_age_error
		or tsb.color <> tsb.derived_color or tsb.reference_no <> tsb.derived_reference_no
	WHERE tsb.is_updated and tsb.is_locked";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    print STDERR "updated is_different on $update_count locked rows\n\n" 
	if $options->{debug} && $update_count && $update_count > 0;
    
    # In all unlocked rows, set the main attributes from the derived ones. We clear the
    # is_different bit just in case a newly unlocked record was previously different.
    
    $sql = "
	UPDATE $TIMESCALE_BOUNDS as tsb
	SET tsb.is_different = 0,
	    tsb.age = tsb.derived_age,
	    tsb.age_error = tsb.derived_age_error,
	    tsb.color = tsb.derived_color,
	    tsb.reference_no = tsb.derived_reference_no
	WHERE tsb.is_updated and not tsb.is_locked";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    print STDERR "updated is_different on $update_count unlocked rows\n\n" 
	if $options->{debug} && $update_count && $update_count > 0;
    
    # Now we need to check (and possibly update) all timescales in which updated boundaries are
    # found.
    
    my ($timescale_list) = $dbh->do("
	SELECT group_concat(distinct timescale_no) FROM $TIMESCALE_BOUNDS
	WHERE is_updated");
    
    unless ( $timescale_list )
    {
	print STDERR "PROBLEM: no timescales found\n\n" if $options->{debug};
	$edt->add_condition("W_UPDATE_BOUNDS: problem checking for boundary errors");
	$timescale_list = '0';	# so we won't throw any exceptions below
    }
    
    # Update min and max ages for these timescales.
    
    $sql = "  UPDATE $TIMESCALE_DATA as ts join 
		(SELECT timescale_no, max(age) as max_age, min(age) as min_age
		 FROM $TIMESCALE_BOUNDS as tsb
		 WHERE timescale_no in ($timescale_list) GROUP BY timescale_no) as n using (timescale_no)
	    SET ts.max_age = n.max_age, ts.late_age = n.min_age";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    # Now set the is_error flags for every boundary in these timescales.
    
    $sql = "SET \@r1=0, \@r2=0";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $dbh->do($sql);
    
    # We will be able to substantially simplify the following expression once we are able to
    # upgrade to MariaDB 10.2, which introduces common table expressions. For now:
    # 
    # We select all of the bounds in the updated timescales twice, as b1 and b2. In each
    # selection, we order them by timescale and age and number the rows using @r1 and @r2. We then
    # join each row in b1 to the previous row from the same timescale in b2, so that we can check
    # that the upper interval for each boundary matches the lower interval from the previous
    # boundary, and that the age of each boundary is greater than the age of the previous
    # boundary. This check is done in the second and third lines. Initial boundaries don't have a
    # matching row in b2, so the lower_no and age will be null. The expressions 'bound_ok' and 
    # 'age_ok' are then used to set the error flag for each boundary.
    
    $sql = "  UPDATE $TIMESCALE_BOUNDS as tsb join
		(SELECT b1.bound_no, (b1.age > b2.age or b2.age is null) as age_ok,
		    (b1.interval_no = b2.lower_no or (b1.interval_no = 0 and b2.lower_no is null)) as bound_ok
		 FROM
		  (select (@r1 := @r1 + 1) as row, bound_no, timescale_no, age, interval_no from $TIMESCALE_BOUNDS 
		   WHERE timescale_no in ($timescale_list) ORDER BY timescale_no, age) as b1 LEFT JOIN
		  (select (@r2 := @r2 + 1) as row, timescale_no, age, lower_no FROM $TIMESCALE_BOUNDS
		   WHERE timescale_no in ($timescale_list) ORDER BY timescale_no, age) as b2 on
			b1.row = b2.row + 1 and b1.timescale_no = b2.timescale_no) as bound_check using (bound_no)
	      SET is_error = not(bound_ok and age_ok)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    # Finally, we need to update the error flag for each of the updated timescales.
    
    $sql = "  UPDATE $TIMESCALE_DATA as ts join 
		(SELECT timescale_no, max(is_error) as is_error
		 FROM $TIMESCALE_BOUNDS WHERE timescale_no in ($timescale_list)
		 GROUP BY timescale_no) as any_tsb
	      SET ts.is_error = any_tsb.is_error";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
}



1;
