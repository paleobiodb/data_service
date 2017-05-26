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

use base 'Exporter';

our(@EXPORT_OK) = qw(add_timescale update_timescale delete_timescale clear_timescale
		     add_boundary update_boundary delete_boundary);


sub add_timescale {
    
    
    
    
}


sub update_timescale {



}


sub delete_timescale {



}


sub clear_timescale {



}


# Add a new boundary according to the specified attributes.

sub add_boundary {
    
    my ($dbh, $attrs, $conditions, $options) = @_;
    
    croak "add_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
    croak "add_boundary: must not have a value for bound_id\n" if $attrs->{bound_id};
    
    $options ||= { };
    
    my $result = EditResult->new();
    
    # Make sure that we know what timescale to create the boundary in, and
    # that a bound type was specified.
    
    unless ( $attrs->{timescale_id} )
    {
        $result->add_condition("E_BOUND_TIMESCALE: you must specify a value for 'timescale_id'");
    }
    
    unless ( $attrs->{bound_type} )
    {
	$result->add_condition("E_BOUND_TYPE: you must specify a value for 'bound_type'");
    }
    
    # Then check for missing or redundant attributes. These will vary by bound type.
    
    my $timescale_id = $attrs->{timescale_id};
    my $bound_type = $attrs->{bound_type};
    
    if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
    {
	$result->add_condition("E_AGE_MISSING: you must specify a value for 'age' with this bound type")
	    unless $attrs->{age};

	$result->add_condition("W_BASE_IGNORED: the value of 'base_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'same' )
    {
	$result->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id};
	
	$result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};

	$result->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
    {
	$result->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id};
	
	$result->add_condition("E_OFFSET_MISSING: you must specify a value for 'offset' with this bound type")
	    unless $attrs->{offset};
	
	$result->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};
	
	if ( $bound_type eq 'percent' )
	{
	    $result->add_condition("E_RANGE_MISSING: you must specify a value for 'range_id' with this bound type")
		unless $attrs->{range_id};
	}
	
	else
	{
	    $result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
		if $attrs->{range_id};
	}
    }
    
    # Now check that all of the specified attributes are of the correct type and in the correct
    # value range, and that all references to other records match up to existing records.
    
    my ($fields, $values) = check_timescale_attrs($dbh, $result, 'add', 'bound', $attrs, $conditions, $options);
    
    # Then make sure that the necessary attributes have the proper values.
    
    check_bound_values($dbh, $result, $attrs);
    
    # If any errors occurred, or if $check_only was specified, we stop here.
    
    return $result if $options->{check_only} || $result->status eq 'ERROR';
    
    # Otherwise, insert the new record.
    
    my $sql = "INSERT INTO $TIMESCALE_BOUNDS ($fields) VALUES ($values)";
    my ($insert_result, $insert_id);
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    try {
	$insert_result = $dbh->do($sql);
	$insert_id = $dbh->last_insert_id;
	
	if ( $insert_result )
	{
	    $result->status('OK');
	    $result->record_keys($insert_id, $timescale_id);
	}
	
	else
	{
	    $result->add_condition("E_INTERNAL: record could not be created");
	}
    }
	
    catch {
	print STDERR "ERROR: $_\n";
	$result->add_condition("E_INTERNAL: an error occurred while inserting this record");
    }
    
    return $result;
}


sub update_boundary {

    my ($dbh, $attrs, $conditions, $options) = @_;
    
    croak "update_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
    croak "update_boundary: must have a value for bound_id\n" unless $attrs->{bound_id};
    
    $options ||= { };
    
    my $result = EditResult->new();
    
    my $bound_id = $attrs->{bound_id};
    
    # We first need to make sure that the record to be updated actually exists, and fetch its
    # current attributes.
    
    unless ( $bound_id =~ /^\d+$/ && $bound_id > 0 )
    {
	return $result->add_condition("E_BOUND_ID: bad value '$bound_id' for 'bound_id'");
    }
    
    my ($current) = $dbh->selectrow_hashref("
		SELECT * FROM $TIMESCALE_BOUNDS WHERE bound_no = $bound_id");
    
    unless ( $current )
    {
	return $result->add_condition("E_NOT_FOUND: boundary '$bound_id' is not in the database");
    }
    
    # If a timescale_id was specified, it must match the current one otherwise an error will be
    # thrown. It is not permitted to move a boundary to a different timescale.
    
    if ( defined $attrs->{timescale_id} && $attrs->{timescale_id} ne '' )
    {
	if ( $current->{timescale_no} && $current->{timescale_no} ne $attrs->{timescale_id} )
	{
	    $result->add_condition("E_BOUND_TIMESCALE: you cannot change the timescale associated with a bound");
	}
    }
    
    # Check for missing or redundant attributes. These will vary by bound type.
    
    my $bound_type = $attrs->{bound_type} || $current->{bound_type};
    my $timescale_id = $current->{timescale_no};
    
    if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
    {
	$result->add_condition("E_AGE_MISSING: you must specify a value for 'age' with this bound type")
	    unless defined $attrs->{age} || defined $current->{age};
	
	$result->add_condition("W_BASE_IGNORED: the value of 'base_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'same' )
    {
	$result->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id} || $current->{base_no};
	
	$result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
	    if $attrs->{range_id};
	
	$result->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if defined $attrs->{age};

	$result->add_condition("W_OFFSET_IGNORED: the value of 'offset' will be ignored for this bound type")
	    if $attrs->{offset};
    }
    
    elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
    {
	$result->add_condition("E_BASE_MISSING: you must specify a value for 'base_id' with this bound type")
	    unless $attrs->{base_id} || $current->{base_no};
	
	$result->add_condition("E_OFFSET_MISSING: you must specify a value for 'offset' with this bound type")
	    unless $attrs->{offset};
	
	$result->add_condition("W_AGE_IGNORED: the value of 'age' will be ignored for this bound type")
	    if $attrs->{age};
	
	if ( $bound_type eq 'percent' )
	{
	    $result->add_condition("E_RANGE_MISSING: you must specify a value for 'range_id' with this bound type")
		unless $attrs->{range_id} || $current->{range_no};
	}
	
	else
	{
	    $result->add_condition("W_RANGE_IGNORED: the value of 'range_id' will be ignored for this bound type")
		if $attrs->{range_id};
	}
    }
    
    # Now check that all of the specified attributes are of the correct type and in the correct
    # value range, and that all references to other records match up to existing records.
    
    my ($set_list) = check_timescale_attrs($dbh, $result, 'update', 'bound', $attrs, $conditions, $options);
    
    # Then make sure that the necessary attributes have the proper values.
    
    check_bound_values($dbh, $result, $attrs, $current);
    
    # If any errors occurred, or if $check_only was specified, we stop here.
    
    return $result if $options->{check_only} || $result->status eq 'ERROR';
    
    # Otherwise, update the record.
    
    my $sql = "UPDATE $TIMESCALE_BOUNDS SET $set_list WHERE bound_no = $bound_id";
    my $update_result;
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    try {
	$update_result = $dbh->do($sql);
	
	if ( $update_result )
	{
	    $result->status('OK');
	    $result->record_keys($bound_id, $timescale_id);
	}
	
	else
	{
	    $result->add_condition("E_INTERNAL: record was not updated");
	}
    }
    
    catch {
	print STDERR "ERROR: $_\n";
	$result->add_condition("E_INTERNAL: an error occurred while updating this record");
    };
    
    return $result;
}


sub delete_boundary {


}




my %IGNORE_ATTRS = ( bound_id => 1, record_id => 1 );

sub check_timescale_attrs {
    
    my ($dbh, $result, $op, $record_type, $attrs, $conditions, $options) = @_;
    
    my @sql_list;
    my @field_list;
    
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
	    $result->add_condition("W_BAD_FIELD: $k: unknown attribute");
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
		$result->add_condition("C_CREATE_INTERVALS: $k: not found");
	    }
	}
	
	# Otherwise, check other types
	
	elsif ( $type eq 'varchar80' )
	{
	    if ( length($value) > 80 )
	    {
		$result->add_condition("E_TOO_LONG: $k: must be 80 characters or less");
		next;
	    }
	    
	    $quoted = $dbh->quote($value);
	}
	
	elsif ( $type eq 'colorhex' )
	{
	    unless ( $value =~ qr{ ^ \# [0-9a-z]{6} $ }xsi )
	    {
		$result->add_condition("E_BAD_COLOR: $k: must be a valid color in hexadecimal notation");
		next;
	    }
	    
	    $quoted = $dbh->quote(uc $value);
	}
	
	elsif ( $type eq 'pos_decimal' )
	{
	    unless ( $value =~ qr{ ^ (?: \d* [.] \d+ | \d+ [.] \d* | \d+ ) $ }xsi )
	    {
		$result->add_condition("E_BAD_NUMBER: $k: must be a positive decimal number");
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
		$result->add_condition("E_BAD_KEY: $k: must be a valid $label identifier");
		next;
	    }
	    
	    my $check_value;
	    
	    eval {
		($check_value) = $dbh->selectrow_array("SELECT $type FROM $table WHERE $type = $quoted");
	    };
	    
	    unless ( $check_value )
	    {
		$result->add_condition("E_KEY_NOT_FOUND: $k: the identifier $quoted was not found in the database");
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
		$result->add_condition("E_BAD_VALUE: $k: value not acceptable");
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
	return join(', ', @sql_list, "modified = now()");
    }
    
    else
    {
	return join(',', @field_list), join(',', @sql_list);
    }
}


sub check_bound_values {

    my ($dbh, $result, $new, $current) = @_;
    
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
		$result->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a percentage between 0 and 100.0 for this bound type");
	    }
	}
	
	elsif ( $new_bound_type eq 'offset' )
	{
	    unless ( defined $new_offset && $new_offset ne '' && $new_offset >= 0.0 && $new_offset <= 1000.0 )
	    {
		$result->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a value between 0 and 1000.0 Ma for this bound type");
	    }
	}
	
	elsif ( $new_bound_type eq 'absolute' || $new_bound_type eq 'spike' )
	{
	    unless ( defined $new_age && $new_age ne '' && $new_age >= 0.0 && $new_age <= 4600.0 )
	    {
		$result->add_condition("E_AGE_RANGE: the value of 'age' must be a value between 0 and 4600.0 Ma for this bound type");
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
	    $result->add_condition("E_INVALID_KEY: the base and range bounds must be from a different timescale");
	}
    }
}


# propagate_boundary_changes ( dbh, source_bounds )
# 
# Propagate any changes to interval boundaries and timescales to the boundaries that refer to
# them. The parameter $source_bounds may be set to indicate which bounds have changed. If so, then
# the first iteration of the propagation loop will check only bounds which directly reference
# these. This will improve the efficiency of making small changes.

sub propagate_boundary_changes {

    my ($dbh, $source_bounds, $options) = @_;
    
    $options ||= { };
    
    my $update_count;
    my $source_limit = '> 0';
    my $sql;
    
    # First create an SQL filter based on $source_bounds.
    
    if ( ref $source_bounds eq 'ARRAY' )
    {
	$source_limit = 'in (' . join(',', @$source_bounds) . ')';
    }
    
    elsif ( $source_bounds )
    {
	$source_limit = "in ($source_bounds)";
    }
    
    my $color_limit = $source_limit;
    my $refno_limit = $source_limit;
    
    # Then execute a loop, propagating updated information one step at a time.
    
    $update_count = 1;
    
    while ( $update_count )
    {
	# First update bounds
	
	$sql = "
	    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.base_no
		left join $TIMESCALE_BOUNDS as top on top.bound_no = tsb.range_no
	    SET tsb.derived_age = case tsb.bound_type
			when 'same' then base.age
			when 'offset' then base.age + tsb.offset
			when 'percent' then base.age + tsb.offset * ( base.age - top.age )
			end,
		tsb.derived_age_error = case tsb.bound_type
			when 'same' then base.age_error
			when 'offset' then base.age_error + tsb.offset_error
			when 'percent' then base.age_error + tsb.offset_error * ( base.age - top.age )
			end
	    WHERE tsb.base_no $source_limit and tsb.bound_type not in ('absolute', 'spike')
		and (tsb.bound_type <> 'percent' or tsb.range_no $source_limit)";	
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" if $options->{debug} && $update_count && $update_count > 0;
	
	# Then disdable the source limit, so that any changes will propagate to the rest of the bounds.
	
	$source_limit = '> 0';
    }
    
    # Now do the same for colors
    
    $update_count = 1;
    
    while ( $update_count )
    {
	$sql = "
	    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.color_no
	    SET tsb.derived_color = base.color
	    WHERE tsb.base_no $color_limit";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" if $options->{debug} && $update_count && $update_count > 0;
	
	# Then disable the source limit.
	
	$color_limit = '> 0';
    }

    # And then for reference_nos
    
    $update_count = 1;
    
    while ( $update_count )
    {
	$sql = "
	    UPDATE $TIMESCALE_BOUNDS as tsb
		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.refsource_no
	    SET tsb.derived_reference_no = base.reference_no
	    WHERE tsb.base_no $refno_limit";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$update_count = $dbh->do($sql);
	
	print STDERR "updated $update_count rows\n\n" 
	    if $options->{debug} && $update_count && $update_count > 0;
	
	# Then disable the source limit.
	
	$refno_limit = '> 0';
    }
}


sub update_boundary_attrs {

    my ($dbh, $update_all, $options) = @_;
    
    $options ||= { };
    
    my $sql;
    my $update_count;
    
    my $timescale_selector = "1=1";
    
    unless ( $update_all )
    {
	$sql = "SELECT distinct timescale_no FROM $TIMESCALE_BOUNDS WHERE updated > now() - interval 1 hour";
	my $timescales = $dbh->selectcol_arrayref($sql);
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	unless ( ref $timescales eq 'ARRAY' && @$timescales )
	{
	    logMessage(2, "No timescales were recently updated\n");
	    return;
	}
	
	my $timescale_list = join(',', @$timescales);
	
	logMessage(2, "timescales recently updated: $timescale_list");
	
	$options->{selector} = $timescale_selector = "tsb.timescale_no in ($timescale_list)";
    }
    
    # In all locked rows, set or clear the 'is_different' bit depending upon whether the derived
    # attributes are different rom the main ones.
    
    $sql = "
	UPDATE $TIMESCALE_BOUNDS as tsb
	SET tsb.is_different = tsb.age <> tsb.derived_age or tsb.age_error <> tsb.derived_age_error
		or tsb.color <> tsb.derived_color or tsb.reference_no <> tsb.derived_reference_no
	WHERE tsb.is_locked and $timescale_selector";
    
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
	WHERE not tsb.is_locked and $timescale_selector";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    print STDERR "updated is_different on $update_count unlocked rows\n\n" 
	if $options->{debug} && $update_count && $update_count > 0;
}


sub update_boundary_errors {
    
    my ($dbh, $update_all, $options) = @_;
    
    $options ||= { };
    
    my $sql;
    my $update_count;
    
    # If $update_all is not true, then just update timescales that contain boundaries that were
    # updated recently.
    
    my $timescale_selector = "1=1";
    
    if ( $options->{selector} )
    {
	$timescale_selector = $options->{selector};
    }
    
    elsif ( ! $update_all )
    {
	$sql = "SELECT distinct timescale_no FROM $TIMESCALE_BOUNDS WHERE updated > now() - interval 1 hour";
	my $timescales = $dbh->selectcol_arrayref($sql);
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	unless ( ref $timescales eq 'ARRAY' && @$timescales )
	{
	    logMessage(2, "No timescales were recently updated\n");
	    return;
	}
	
	my $timescale_list = join(',', @$timescales);
	
	logMessage(2, "timescales recently updated: $timescale_list");
	
	$timescale_selector = "tsb.timescale_no in ($timescale_list)";
    }
    
    $sql = "UPDATE $TIMESCALE_DATA as ts join 
		(SELECT timescale_no, max(age) as max_age, min(age) as min_age
		 FROM $TIMESCALE_BOUNDS as tsb
		 WHERE $timescale_selector GROUP BY timescale_no) as n using (timescale_no)
	    SET ts.max_age = n.max_age, ts.late_age = n.min_age";    
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $update_count = $dbh->do($sql);
    
    $sql = "SELECT tsb.bound_no, tsb.age, count(distinct tsbu.bound_no) as count_up, 
		count(distinct tsbl.bound_no) as count_down,
		tsbu.age as age_up, tsbl.age as age_down
	    FROM timescale_bounds as tsb 
		left join timescale_bounds as tsbu on tsbu.lower_no = tsb.interval_no and
			tsbu.timescale_no = tsb.timescale_no and tsb.interval_no <> 0
		left join timescale_bounds as tsbl on tsbl.interval_no = tsb.lower_no and 
			tsbl.timescale_no = tsb.timescale_no and tsb.lower_no <> 0
	    WHERE $timescale_selector GROUP BY tsb.bound_no ORDER by age";
    
}



1;
