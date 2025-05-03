# 
# The Paleobiology Database
# 
#   CollectionEdit.pm - a class for handling operations on collection records.
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting collection records.
#   
#   To use it, first call $edt = CollectionEdit->new with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record, etc.


use strict;

package CollectionEdit;

use parent 'EditTransaction';

use Carp qw(carp croak);

use ExternalIdent qw(generate_identifier);
use TableDefs qw(%TABLE set_table_name set_table_group set_table_property set_column_property);
use CoreTableDefs;
use IntervalBase qw(int_defined int_bounds);

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;


our (@CARP_NOT) = qw(EditTransaction);

{
    CollectionEdit->register_conditions(
       C_DUPLICATE => "Possible duplicate of: &1",
       E_BAD_NAME => "Field '&1' must contain at least one letter",
       E_CANNOT_DELETE => "Deletion of collections has not been implemented");
    
    CollectionEdit->register_allowances('DUPLICATE');
    
    set_table_property('COLLECTION_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('COLLECTION_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('COLLECTION_DATA', CAN_DELETE => 'OWNER');
    set_table_property('COLLECTION_DATA', REQUIRED_COLS =>
		       ['access_level', 'release_date', 'collection_name', 'collection_type',
			'reference_no', 'country', 'latdeg', 'lngdeg', 'latlng_basis',
			'lithology1', 'environment', 'assembl_comps']);
    set_column_property('COLLECTION_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('COLLECTION_DATA', 'reference_no', EXTID_TYPE => 'REF');
    # set_column_property('COLLECTION_DATA', 'max_interval_no', EXTID_TYPE => 'INT');
    # set_column_property('COLLECTION_DATA', 'min_interval_no', EXTID_TYPE => 'INT');
    
    set_column_property('OCCURRENCE_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('OCCURRENCE_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_column_property('REID_DATA', 'reid_no', EXTID_TYPE => 'REI');
    set_column_property('REID_DATA', 'reference_no', EXTID_TYPE => 'REF');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action, keyexpr )
# 
# This method is called from EditTransaction.pm to validate each action. We override it to do
# additional checks.

my ($DIGITS_RE) = qr/^\d+$/;
my ($NUMBER_RE) = qr/^\d+[.]\d*$|^[.]\d+$|^\d+$/;


sub validate_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    # Validation for delete operations is entirely different from validation for
    # insert, replace, or update operations.
    
    if ( $operation eq 'delete' )
    {
	# If this is the type locality for any taxon, clear that. Delete any
	# occurrences, reidentifications, and specimens associated with this
	# collection.
	
	# Until we can set all of this up, just return 'cannot delete'.
	
	$edt->add_condition('E_CANNOT_DELETE');
	return;
    }
    
    my $dbh = $edt->dbh;
    my $keyexpr = $action->keyexpr;
    
    # my $old_record;
    
    # if ( $operation eq 'update' )
    # {
    # 	$old_record = $edt->fetch_old_record();
    # }
    
    # Check the collection name
    # -------------------------
    
    my $coll_name = $action->record_value('collection_name');
    
    if ( defined $coll_name && $coll_name ne '' )
    {
	$edt->add_condition('E_FORMAT', 'collection_name', "must contain at least one letter")
	    unless $coll_name =~ /[a-zA-Z]/;
    }
    
    elsif ( $operation eq 'insert' || $operation eq 'replace' ||
	    $action->field_specified('collection_name') )
    {
	$edt->add_condition($action, 'E_REQUIRED', 'collection_name');
    }
    
    # Check the primary and secondary refs
    # ------------------------------------
    
    # Check the access level and release date
    # ---------------------------------------
    
    my $access_level = $action->record_value('access_level');
    my $release_date = $action->record_value('release_date');
    
    if ( $operation eq 'update' && $action->field_specified('release_date') &&
	 ! $action->field_specified('access_level') )
    {
	($access_level) =
	    $dbh->selectrow_array("
		SELECT access_level FROM $TABLE{COLLECTION_DATA}
		WHERE $keyexpr");
    }
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('access_level') )
    {
	if ( defined $access_level && $access_level eq 'the public' )
	{
	    if ( defined $release_date && $release_date ne 'immediate' )
	    {
		$edt->add_condition('E_BAD_VALUE', 'release_date',
			"the release date must be 'immediate' if the access level is 'the public'");
	    }

	    else
	    {
		$action->set_record_value('release_date', 'created');
		$action->handle_column('release_date', 'unquoted');
	    }
	}
	
	elsif ( !defined $access_level || $access_level eq '' )
	{
	    $edt->add_condition('E_REQUIRED', 'access_level');
	}
    }
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('release_date') )
    {
	if ( defined $release_date && $release_date eq 'immediate' )
	{
	    $action->set_record_value('release_date', 'created');
	    $action->handle_column('release_date', 'unquoted');
	}
	
	elsif ( defined $release_date && $release_date =~ /^(\d)\s*(month|year)s?$/ )
	{
	    $action->set_record_value('release_date', "created + INTERVAL $1 $2");
	    $action->handle_column('release_date', 'unquoted');
	    
	    $edt->add_condition('E_BAD_VALUE', 'release_date', "a maximum of 5 years is allowed")
		if $2 eq 'year' && $1 > 5;
	}
	
	elsif ( defined $release_date && $release_date ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'release_date', 
				"value must be of the form 'n months' or 'n years' where <n> is a digit");
	}
	
	elsif ( $operation eq 'insert' || $operation eq 'replace' )
	{
	    $edt->add_condition('E_REQUIRED', 'release_date');
	}
    }
    
    # Check the collection type
    # -------------------------
    
    my $collection_type = $action->record_value('collection_type');
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('collection_type') )
    {
	unless ( defined $collection_type && $collection_type ne '' )
	{
	    $edt->add_condition('E_REQUIRED', 'collection_type');
	}
    }
    
    # Check the max and min intervals
    # -------------------------------
    
    my $max_interval = $action->record_value('max_interval');
    my $min_interval = $action->record_value('min_interval');
    
    my ($max_interval_no, $min_interval_no);
	
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('max_interval') )
    {
	$action->ignore_field('max_interval');
	
	# If the value of 'max_interval' corresponds to a known interval, set the
	# field 'max_interval_no' to the corresponding interval_no value.
	
	if ( defined $max_interval && int_defined($max_interval) )
	{
	    $max_interval_no = int_defined($max_interval);
	    $action->set_record_value('max_interval_no', $max_interval_no);
	}
	
	# If the value is otherwise non-empty, add a 'bad value' condition.
	
	elsif ( defined $max_interval && $max_interval ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'max_interval',
				"unknown interval '$max_interval'");
	}
	
	# If it is empty, add a 'required' condition.
	
	else
	{
	    $edt->add_condition($action, 'E_REQUIRED', 'max_interval');
	}
    }
    
    if ( $action->field_specified('min_interval') )
    {
	$action->ignore_field('min_interval');
	
	# If the value of 'min_interval' corresponds to a known interval, set the
	# field 'min_interval_no' to the corresponding interval_no value.
	
	if ( defined $min_interval && int_defined($min_interval) )
	{
	    $min_interval_no = int_defined($min_interval);
	    $action->set_record_value('min_interval_no', $min_interval_no);
	    
	    # If 'max_interval' is also valid, check to make sure they were
	    # specified in the proper order. If not, switch them. The interval
	    # specified by 'max_interval_no' must be older than the one specified by
	    # 'min_interval_no'. If the bounds of both intervals are the same, set
	    # 'min_interval_no' to null.
	    
	    if ( defined $max_interval && int_defined($max_interval) )
	    {
		my ($b_max, $t_max) = int_bounds($max_interval);
		my ($b_min, $t_min) = int_bounds($min_interval);
		
		if ( $b_max < $b_min || ($b_max == $b_min && $t_max < $t_min ) )
		{
		    $action->set_record_value('max_interval_no', $min_interval_no);
		    $action->set_record_value('min_interval_no', $max_interval_no);
		}
		
		elsif ( $b_max == $b_min && $t_max == $t_min )
		{
		    $action->set_record_value('min_interval_no', undef);
		}
	    }
	}
	
	# If the value is otherwise non-empty, add a 'bad value' condition.
	
	elsif ( defined $min_interval && $min_interval ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'min_interval',
				"unknown interval '$min_interval'");
	}
	
	# If the value is the empty string, set it to null.
	
	elsif ( defined $min_interval && $min_interval eq '' )
	{
	    $action->set_record_value('min_interval_no', undef);
	}
    }
    
    # Check the latitude and longitude
    # --------------------------------
    
    my $latdeg = $action->record_value('latdeg');
    my $lngdeg = $action->record_value('lngdeg');
    
    my $latmin = $action->record_value('latmin');
    my $lngmin = $action->record_value('lngmin');
    
    my $latsec = $action->record_value('latsec');
    my $lngsec = $action->record_value('lngsec');
    
    my $latdec = $action->record_value('latdec');
    my $lngdec = $action->record_value('lngdec');
    
    my $latdir = $action->record_value('latdir');
    my $lngdir = $action->record_value('lngdir');
    
    # Check the latitude and longitude degree values. Both are required, and
    # must be non-negative integers within the required bounds.
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('latdeg') )
    {
	if ( defined $latdeg && $latdeg =~ $DIGITS_RE )
	{
	    $edt->add_condition('E_BAD_VALUE', 'latdeg', 'must be in the range 0-89')
		unless $latdeg < 90;
	}
	
	elsif ( defined $latdeg && $latdeg ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'latdeg', 'must be a non-negative integer');
	}
	
	else
	{
	    $edt->add_condition('E_REQUIRED', 'latdeg');
	}
    }
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('lngdeg') )
    {
	if ( defined $lngdeg && $lngdeg =~ $DIGITS_RE )
	{
	    $edt->add_condition('E_BAD_VALUE', 'lngdeg', 'must be in the range 0-180')
		unless $lngdeg <= 180;
	}
	
	elsif ( defined $lngdeg && $lngdeg ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'lngdeg', 'must be a non-negative integer');
	}
	
	else
	{
	    $edt->add_condition('E_REQUIRED', 'lngdeg');
	}
    }
    
    # Check the latitude and longitude minute and second values. If specified,
    # they must be integers between 0 and 59.
    
    if ( defined $latmin && $latmin =~ $DIGITS_RE )
    {
	$edt->add_condition('E_BAD_VALUE', 'latmin', 'must be in the range 0-59')
	    unless $latmin < 60;
    }
    
    elsif ( defined $latmin && $latmin ne '' )
    {
	$edt->add_condition('E_FORMAT', 'latmin', 'must be a non-negative integer');
    }
    
    if ( defined $latsec && $latsec =~ $DIGITS_RE )
    {
	$edt->add_condition('E_BAD_VALUE', 'latsec', 'must be in the range 0-59')
	    unless $latsec < 60;
    }
    
    elsif ( defined $latsec && $latsec ne '' )
    {
	$edt->add_condition('E_FORMAT', 'latsec', 'must be a non-negative integer');
    }
    
    if ( defined $lngmin && $lngmin =~ $DIGITS_RE )
    {
	$edt->add_condition('E_BAD_VALUE', 'lngmin', 'must be in the range 0-59')
	    unless $lngmin < 60;
    }
    
    elsif ( defined $lngmin && $lngmin ne '' )
    {
	$edt->add_condition('E_FORMAT', 'lngmin', 'must be a non-negative integer');
    }
    
    if ( defined $lngsec && $lngsec =~ $DIGITS_RE )
    {
	$edt->add_condition('E_BAD_VALUE', 'lngsec', 'must be in the range 0-59')
	    unless $lngsec < 60;
    }
    
    elsif ( defined $lngsec && $lngsec ne '' )
    {
	$edt->add_condition('E_FORMAT', 'lngsec', 'must be a non-negative integer');
    }
    
    # Check the latidude and longitude decimal values. If specified, they must
    # be strings of decimal digits.
    
    if ( defined $latdec && $latdec ne '' && $latdec !~ $DIGITS_RE )
    {
	$edt->add_condition('E_FORMAT', 'latdec', 'must be a string of decimal digits');
    }
    
    if ( defined $lngdec && $lngdec ne '' && $lngdec !~ $DIGITS_RE )
    {
	$edt->add_condition('E_FORMAT', 'lngdec', 'must be a string of decimal digits');
    }
    
    # Check the latitude and longitude directions. They are required, and must
    # be one of the acceptable values.
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('latdir') )
    {
	if ( defined $latdir && $latdir ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'latdir', "must be either 'north' or 'south'")
		unless $latdir =~ /^north$|^south$/i;
	}
	
	else
	{
	    $edt->add_condition('E_REQUIRED', 'latdir');
	}
    }
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('lngdir') )
    {
	if ( defined $lngdir && $lngdir ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'lngdir', "must be either 'east' or 'west'")
		unless $lngdir =~ /^east$|^west$/i;
	}
	
	else
	{
	    $edt->add_condition('E_REQUIRED', 'lngdir');
	}
    }
    
    # Check for incompatible combinations of fields.
    
    if ( defined $latmin && $latmin ne '' && defined $latdec && $latdec ne '' )
    {
	$edt->add_condition('E_NOT_BOTH', 'latmin', 'latdec');
    }
    
    elsif ( defined $latsec && $latsec ne '' && defined $latdec && $latdec ne '' )
    {
	$edt->add_condition('E_NOT_BOTH', 'latsec', 'latdec');
    }
    
    if ( defined $lngmin && $lngmin ne '' && defined $lngdec && $lngdec ne '' )
    {
	$edt->add_condition('E_NOT_BOTH', 'lngmin', 'lngdec');
    }
    
    elsif ( defined $lngsec && $lngsec ne '' && defined $lngdec && $lngdec ne '' )
    {
	$edt->add_condition('E_NOT_BOTH', 'lngsec', 'lngdec');
    }

    # If we are setting one of the two, clear the other one.

    if ( defined $latmin && $latmin ne '' || defined $latsec && $latsec ne '' )
    {
	$action->set_record_value('latdec', undef);
    }

    if ( defined $latdec && $latdec ne '' )
    {
	$action->set_record_value('latmin', undef);
	$action->set_record_value('latsec', undef);
    }

    if ( defined $lngmin && $lngmin ne '' || defined $lngsec && $lngsec ne '' )
    {
	$action->set_record_value('lngdec', undef);
    }

    if ( defined $lngdec && $lngdec ne '' )
    {
	$action->set_record_value('lngmin', undef);
	$action->set_record_value('lngsec', undef);
    }
    
    # If any of the lat/lng fields were specified, and if this action can
    # proceed, compute 'lat', 'lng', and 'latlng_precision'.
    
    if ( $action->field_specified('latdeg') || $action->field_specified('lngdeg') || 
	 $action->field_specified('latdir') || $action->field_specified('lngdir') ||
	 $action->field_specified('latmin') || $action->field_specified('lngmin') ||
	 $action->field_specified('latsec') || $action->field_specified('lngsec') ||
	 $action->field_specified('latdec') || $action->field_specified('lngdec') )
    {
	if ( $action->can_proceed )
	{
	    if ( $operation eq 'update' )
	    {
		my ($oldlatdeg, $oldlngdeg, $oldlatdir, $oldlngdir, $oldlatmin, $oldlngmin,
		    $oldlatsec, $oldlngsec, $oldlatdec, $oldlngdec) =
			$dbh->selectrow_array("
			SELECT latdeg, lngdeg, latdir, lngdir, latmin, lngmin,
			       latsec, lngsec, latdec, lngdec
			FROM $TABLE{COLLECTION_DATA} WHERE $keyexpr");
	    
		$latdeg = $oldlatdeg unless $action->field_specified('latdeg');
		$lngdeg = $oldlngdeg unless $action->field_specified('lngdeg');
		$latdir = $oldlatdir unless $action->field_specified('latdir');
		$lngdir = $oldlngdir unless $action->field_specified('lngdir');
		$latmin = $oldlatmin unless $action->field_specified('latmin');
		$lngmin = $oldlngmin unless $action->field_specified('lngmin');
		$latsec = $oldlatsec unless $action->field_specified('latsec');
		$lngsec = $oldlngsec unless $action->field_specified('lngsec');
		$latdec = $oldlatdec unless $action->field_specified('latdec');
		$lngdec = $oldlngdec unless $action->field_specified('lngdec');
	    }
	    
	    my $lat = $latdeg + 0;
	
	    if ( defined $latdec && $latdec ne '' )
	    {
		$lat = "$latdeg.$latdec" + 0;
	    }
	
	    else
	    {
		if ( defined $latmin && $latmin ne '' )
		{
		    $lat += ($latmin / 60);
		}
		
		if ( defined $latsec && $latsec ne '' )
		{
		    $lat += ($latsec / 3600);
		}
	    }

	    $lat = -1 * $lat if $latdir =~ /^s/i;
	    
	    $action->set_record_value('lat', $lat);
	    
	    my $lng = $lngdeg + 0;
	    
	    if ( defined $lngdec && $lngdec ne '' )
	    {
		$lng = "$lngdeg.$lngdec" + 0;
	    }

	    else
	    {
		if ( defined $lngmin && $lngmin ne '' )
		{
		    $lng += ($lngmin / 60);
		}
		
		if ( defined $lngsec && $lngsec ne '' )
		{
		    $lng += ($lngsec / 3600);
		}
	    }

	    $lng = -1 * $lng if $lngdir =~ /^w/i;
	    
	    $action->set_record_value('lng', $lng);
	    
	    my $latlng_precision;
	    
	    if ( defined $latsec && $latsec ne '' || defined $lngsec && $lngsec ne '' )
	    {
		$latlng_precision = 'seconds';
	    }
	    
	    elsif ( defined $latmin && $latmin ne '' || defined $lngmin && $lngmin ne '' )
	    {
		$latlng_precision = 'minutes';
	    }
	    
	    elsif ( defined $latdec && $latdec ne '' || defined $lngdec && $lngdec ne '' )
	    {
		my $latprec = defined $latdec && $latdec ne '' ? length($latdec) : 0;
		my $lngprec = defined $lngdec && $lngdec ne '' ? length($lngdec) : 0;
	
		$latlng_precision = $latprec > $lngprec ? $latprec : $lngprec;
	    }
	    
	    else
	    {
		$latlng_precision = 'degrees';
	    }
	    
	    $action->set_record_value('latlng_precision', $latlng_precision);
	}
    }
    
    # Check the direct, max, and min dates
    # ------------------------------------
    
    # We only need to do these checks if at least one of the relevant fields was
    # specified, since none of them are required.
    
    if ( $action->field_specified('direct_ma') || $action->field_specified('direct_ma_error') ||
	 $action->field_specified('direct_ma_unit') || $action->field_specified('direct_ma_method') ||
	 $action->field_specified('max_ma') || $action->field_specified('max_ma_error') ||
	 $action->field_specified('max_ma_unit') || $action->field_specified('max_ma_method') ||
	 $action->field_specified('min_ma') || $action->field_specified('min_ma_error') ||
	 $action->field_specified('min_ma_unit') || $action->field_specified('min_ma_method') )
    {
	my ($old_direct_ma, $old_max_ma, $old_min_ma);
	
	my $direct_ma = $action->record_value('direct_ma');
	my $direct_ma_error = $action->record_value('direct_ma_error');
	my $direct_ma_unit = $action->record_value('direct_ma_unit');
	my $direct_ma_method = $action->record_value('direct_ma_method');
	my $max_ma = $action->record_value('max_ma');
	my $max_ma_error = $action->record_value('max_ma_error');
	my $max_ma_unit = $action->record_value('max_ma_unit');
	my $max_ma_method = $action->record_value('max_ma_method');
	my $min_ma = $action->record_value('min_ma');
	my $min_ma_error = $action->record_value('min_ma_error');
	my $min_ma_unit = $action->record_value('min_ma_unit');
	my $min_ma_method = $action->record_value('min_ma_method');
	
	if ( $operation eq 'update' )
	{
	    ($old_direct_ma, $old_max_ma, $old_min_ma) = 
		$dbh->selectrow_array("
		SELECT direct_ma, max_ma, min_ma
		FROM $TABLE{COLLECTION_DATA} WHERE $keyexpr");
	    
	    unless ( defined $old_direct_ma && $old_direct_ma ne '' ||
		     $direct_ma && $direct_ma ne '' )
	    {
		$action->delete_record_value('direct_ma_error');
		$action->delete_record_value('direct_ma_unit');
		$action->delete_record_value('direct_ma_method');
		$direct_ma_error = undef;
		$direct_ma_unit = undef;
		$direct_ma_method = undef;
	    }
	    
	    unless ( defined $old_max_ma && $old_max_ma ne '' ||
		     $max_ma && $max_ma ne '' )
	    {
		$action->delete_record_value('max_ma_error');
		$action->delete_record_value('max_ma_unit');
		$action->delete_record_value('max_ma_method');
		$max_ma_error = undef;
		$max_ma_unit = undef;
		$max_ma_method = undef;
	    }
	    
	    unless ( defined $old_min_ma && $old_min_ma ne '' ||
		     $min_ma && $min_ma ne '' )
	    {
		$action->delete_record_value('min_ma_error');
		$action->delete_record_value('min_ma_unit');
		$action->delete_record_value('min_ma_method');
		$min_ma_error = undef;
		$min_ma_unit = undef;
		$min_ma_method = undef;
	    }
	    
	    $direct_ma = $old_direct_ma unless $action->field_specified($direct_ma);
	    $max_ma = $old_max_ma unless $action->field_specified($max_ma);
	    $min_ma = $old_min_ma unless $action->field_specified($min_ma);
	}
	
	if ( defined $direct_ma && $direct_ma ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'direct_ma', "must be a decimal number")
		unless $direct_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'direct_ma', "maximum is 8 characters")
		unless length($direct_ma) <= 8;
	}
	
	if ( defined $direct_ma_error && $direct_ma_error ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'direct_ma_error', "must be a decimal number")
		unless $direct_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'direct_ma_error', "maximum is 8 characters")
		unless length($direct_ma_error) <= 8;
	}
	
	if ( defined $max_ma && $max_ma ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'max_ma', "must be a decimal number")
		unless $max_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'max_ma', "maximum is 8 characters")
		unless length($max_ma) <= 8;
	}
	
	if ( defined $max_ma_error && $max_ma_error ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'max_ma_error', "must be a decimal number")
		unless $max_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'max_ma_error', "maximum is 8 characters")
		unless length($max_ma_error) <= 8;
	}
	
	if ( defined $min_ma && $min_ma ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'min_ma', "must be a decimal number")
		unless $min_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'min_ma', "maximum is 8 characters")
		unless length($min_ma) <= 8;
	}
	
	if ( defined $min_ma_error && $min_ma_error ne '' )
	{
	    $edt->add_condition('E_FORMAT', 'min_ma_error', "must be a decimal number")
		unless $min_ma =~ $NUMBER_RE;
	    
	    $edt->add_condition('E_WIDTH', 'min_ma_error', "maximum is 8 characters")
		unless length($min_ma_error) <= 8;
	}
    }
    
    # Check for size classes
    # ----------------------
    
    if ( $operation eq 'insert' || $operation eq 'replace' ||
	 $action->field_specified('assembl_comps') )
    {
	my $assembl_comps = $action->record_value('assembl_comps');
	
	unless ( defined $assembl_comps && $assembl_comps ne '' )
	{
	    $edt->add_condition('E_REQUIRED', 'assembl_comps');
	}
    }
}


# after_action ( action, operation, table, new_keyval )
#
# This method is called after an action successfully completes. When a
# collection is added or updated, this method updates the collection matrix.

sub after_action {

    my ($edt, $action, $operation, $table_specifier, $new_keyval) = @_;
    
    my $dbh = $edt->dbh;
    
    my $keyexpr;

    # For an 'insert' operation, the new key value is provided as an argument to
    # this method.
    
    if ( $table_specifier eq 'COLLECTION_DATA' && $operation eq 'insert' )
    {
	$keyexpr = "collection_no = '$new_keyval'";
    }
    
    # Otherwise, we can get it from the action.
    
    elsif ( $table_specifier eq 'COLLECTION_DATA' &&
	    ( $operation eq 'update' || $operation eq 'replace' || $operation eq 'delete' ) )
    {
	$keyexpr = $action->keyexpr;
    }

    # For a 'delete' operation on the COLLECTION_DATA table, remove the
    # corresponding row from the COLLECTION_MATRIX table.
    
    if ( $table_specifier eq 'COLLECTION_DATA' && $operation eq 'delete' && $keyexpr )
    {
	my $sql = "DELETE FROM $TABLE{COLLECTION_MATRIX} WHERE $keyexpr";

	$edt->debug_line("$sql\n") if $edt->debug_mode;

	$dbh->do($sql);
    }
    
    # For any other operation on the COLLECTION_DATA table, generate a fresh row
    # in the COLLECTION_MATRIX table.
    
    elsif ( $table_specifier eq 'COLLECTION_DATA' && $keyexpr )
    {
	my $sql = "REPLACE INTO $TABLE{COLLECTION_MATRIX}
		       (collection_no, lng, lat, loc, cc,
			protected, early_age, late_age,
			early_int_no, late_int_no, 
			reference_no, access_level)
		SELECT c.collection_no, c.lng, c.lat,
			if(c.lng is null or c.lat is null, point(1000.0, 1000.0), point(c.lng, c.lat)), 
			map.cc, cl.protected,
			if(ei.early_age > li.late_age, ei.early_age, li.late_age),
			if(ei.early_age > li.late_age, li.late_age, ei.early_age),
			c.max_interval_no, if(c.min_interval_no > 0, c.min_interval_no, 
									c.max_interval_no),
			c.reference_no,
			case c.access_level
				when 'database members' then if(c.release_date <= now(), 0, 1)
				when 'research group' then if(c.release_date <= now(), 0, 2)
				when 'authorizer only' then if(c.release_date <= now(), 0, 2)
				else 0
			end
		FROM $TABLE{COLLECTION_DATA} as c
			LEFT JOIN $TABLE{COLLECTION_LOC} as cl using (collection_no)
			LEFT JOIN $TABLE{COUNTRY_MAP} as map on map.name = c.country
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.max_interval_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = 
				if(c.min_interval_no > 0, c.min_interval_no, c.max_interval_no)
		WHERE $keyexpr";
	    
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$dbh->do($sql);
	    
	$sql = "UPDATE $TABLE{COLLECTION_MATRIX} as m JOIN
			(SELECT collection_no, count(*) as n_occs FROM $TABLE{OCCURRENCE_DATA}
			 GROUP BY collection_no) as sum using (collection_no)
		    SET m.n_occs = sum.n_occs WHERE $keyexpr";

	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$dbh->do($sql);
    }
    
}


1;
