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
use List::Util qw(any);

use ExternalIdent qw(generate_identifier);
use TableDefs qw(%TABLE set_table_name set_table_group set_table_property set_column_property);
use CoreTableDefs;
use IntervalBase qw(int_defined int_bounds);
use MatrixBase qw(updateCollectionMatrix updateOccurrenceMatrix
		  deleteFromCollectionMatrix deleteFromOccurrenceMatrix
		  deleteReidsFromOccurrenceMatrix updateOccurrenceCounts);

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

our (@CARP_NOT) = qw(EditTransaction);

my ($DIGITS_RE) = qr/^\d+$/;
my ($NUMBER_RE) = qr/^\d+[.]\d*$|^[.]\d+$|^\d+$/;
my (%OCC_RESO, %OCC_RESO_RE);

{
    CollectionEdit->register_conditions(
       C_DUPLICATE => "Possible duplicate of: &1",
       C_DUPLICATE_OCC => "Occurrence is a duplicate of &1",
       W_DUPLICATE_OCC => "Occurrence was skipped because it is a duplicate of &1",
       E_BAD_NAME => "Field '&1' must contain at least one letter",
       E_MULTI_COLLECTIONS => "this action spans more than one collection",
       E_CANNOT_DELETE => { collection => "Deletion of collections has not been implemented",
			    occurrence => "Cannot delete an occurrence with speciments" },
       E_CANNOT_MOVE => { occurrence => "The value of 'collection_id' must match what is " .
			  "already stored in the record",
			  reid => "The value of 'occurrence_id' must match what is " .
			  "already stored in the record" });
    
    CollectionEdit->register_allowances('DUPLICATE');
    
    CollectionEdit->ignore_field('reference_add');
    CollectionEdit->ignore_field('reference_delete');
    CollectionEdit->ignore_field('max_interval');
    CollectionEdit->ignore_field('min_interval');
    
    set_table_property('COLLECTION_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('COLLECTION_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('COLLECTION_DATA', CAN_DELETE => 'OWNER');
    set_table_property('COLLECTION_DATA', REQUIRED_COLS =>
		       ['access_level', 'release_date', 'collection_name', 'collection_type',
			'reference_no', 'country', 'latdeg', 'lngdeg', 'latlng_basis',
			'lithology1', 'environment', 'assembl_comps']);
    set_column_property('COLLECTION_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('COLLECTION_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_table_property('OCCURRENCE_DATA', REQUIRED_COLS => ['reference_no']);
    
    set_column_property('OCCURRENCE_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('OCCURRENCE_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('OCCURRENCE_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_table_property('REID_DATA', REQUIRED_COLS => ['reference_no']);
    
    set_column_property('REID_DATA', 'reid_no', EXTID_TYPE => 'REI');
    set_column_property('REID_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('REID_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('REID_DATA', 'reference_no', EXTID_TYPE => 'REF');
}


# Methods
# -------

# The following methods override methods from EditTransaction.pm:
#
# initialize_action
# validate_action
# after_action
# finalize_transaction


# initialize_action ( action, operation, table_specifier )
#
# This method is called by the EditTransaction library for each new action. It is only
# needed for occurrence actions.

sub initialize_action {

    my ($edt, $action, $operation, $table_specifier) = @_;

    if ( $table_specifier eq 'OCCURRENCE_DATA' || $table_specifier eq 'REID_DATA' )
    {
	$edt->initialize_occ_action($action, $operation, $table_specifier);
    }
}


# validate_action ( action, operation, table_specifier )
# 
# This method is called from EditTransaction.pm to validate each action. The default
# method does nothing.

sub validate_action {
    
    my ($edt, $action, $operation, $table_specifier) = @_;

    if ( $table_specifier eq 'COLLECTION_DATA' )
    {
	return $edt->validate_coll_action($action, $operation);
    }

    elsif ( $table_specifier eq 'OCCURRENCE_DATA' ||
	    $table_specifier eq 'REID_DATA' )
    {
	return $edt->validate_occ_action($action, $operation, $table_specifier);
    }
    
    else
    {
	$edt->add_condition('E_BAD_TABLE', $table_specifier,
			    'is not an allowed table for this operation');
    }
}


# after_action ( action, operation, table, new_keyval )
#
# This method is called after an action successfully completes. When a
# collection is added or updated, this method updates the collection matrix.

sub after_action {
    
    my ($edt, $action, $operation, $table_specifier, $new_keyval) = @_;

    if ( $table_specifier eq 'COLLECTION_DATA' )
    {
	return after_coll_action(@_);
    }
    
    elsif ( $table_specifier eq 'OCCURRENCE_DATA' ||
	    $table_specifier eq 'REID_DATA' )
    {
	return after_occ_action(@_);
    }
}


# finalize_transaction ( )
#
# This method is called after the last action executes, provided that the transaction
# can proceed. It adjusts the collection matrix and/or occurrence matrix to match any
# changes to the collections, occurrences, and reidentifications tables. It also adjusts
# the occurrences and reidentifications tables to reflect the new 'most recent'
# reidentification when reidentifications are added or deleted.

sub finalize_transaction {
    
    my ($edt) = @_;
    
    my $dbh = $edt->dbh;
    my $debug_out = $edt->debug_mode ? $edt : undef;
    
    my @delete_colls = $edt->get_attr_keys('delete_colls');
    
    if ( @delete_colls )
    {
	deleteFromCollectionMatrix($dbh, \@delete_colls, $debug_out);
    }
    
    my @update_colls = $edt->get_attr_keys('update_colls');
    
    if ( @update_colls )
    {
	updateCollectionMatrix($dbh, \@update_colls, $debug_out);
    }
    
    my @delete_occs = $edt->get_attr_keys('delete_occs');
    
    if ( @delete_occs )
    {
	deleteFromOccurrenceMatrix($dbh, \@delete_occs, $debug_out);
    }
    
    my @delete_reids = $edt->get_attr_keys('delete_reids');
    
    if ( @delete_reids )
    {
	deleteReidsFromOccurrenceMatrix($dbh, \@delete_reids, $debug_out);
    }
    
    my @recompute_occs = $edt->get_attr_keys('recompute_occs');
    
    if ( @recompute_occs )
    {
	$edt->recompute_occs($dbh, \@recompute_occs);
    }
    
    my @update_occs = $edt->get_attr_keys('update_occs');
    
    if ( @update_occs )
    {
	updateOccurrenceMatrix($dbh, \@update_occs, $debug_out);
    }
    
    my @update_occ_counts = $edt->get_attr_keys('update_occ_counts');
    
    if ( @update_occ_counts )
    {
	updateOccurrencecounts($dbh, \@update_occ_counts, $debug_out);
    }
}


# Methods for collections
# -----------------------

# validate_coll_action ( action, operation )
#
# Validate an action (insert, replace, update, delete) on the COLLECTION_DATA
# table.

sub validate_coll_action {

    my ($edt, $action, $operation) = @_;
    
    # Validation for delete operations is entirely different from validation for
    # insert, replace, or update operations.
    
    if ( $operation eq 'delete' )
    {
	# If this is the type locality for any taxon, clear that. Delete any
	# occurrences, reidentifications, and specimens associated with this
	# collection.
	
	# Until we can set all of this up, just return 'cannot delete'.
	
	$edt->add_condition('E_CANNOT_DELETE', 'collection');
	return;
    }
    
    my $dbh = $edt->dbh;
    my $keyexpr = $action->keyexpr;
    my $coll_id = $action->keyval;
    
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
    
    # Check the reference parameters
    # ------------------------------
    
    # If a reference id is specified, it must match an existing reference. This field is
    # required, but does not have to be specified in an update operation.
    
    my $reference_no = $action->record_value('reference_id');
    
    if ( defined $reference_no && $reference_no ne '' )
    {
	my $qref = $dbh->quote($reference_no);
	
	my ($check_re) = $dbh->selectrow_array("
		SELECT created FROM $TABLE{REFERENCE_DATA}
		WHERE reference_no = $qref");
	
	unless ( $check_re )
	{
	    $edt->add_condition('E_BAD_VALUE', 'reference_id',
				"unknown reference $qref");
	}
    }
    
    # The following two fields will be handled by the 'after_action' method.
    
    $action->ignore_field('reference_add');
    $action->ignore_field('reference_delete');
    
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
	    
	    $edt->add_condition('E_BAD_VALUE', 'release_date', "a maximum of 3 years is allowed")
		if $2 eq 'year' && $1 > 3;
	}
	
	elsif ( defined $release_date && $release_date ne '' )
	{
	    $edt->add_condition('E_BAD_VALUE', 'release_date', 
				"value must be of the form 'n months' or 'n years' where <n> is a digit");
	    $action->handle_column('release_date', 'unquoted');
	    $action->set_record_value('release_date', 'created');
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
    
    # Check the Geography fields
    # --------------------------
    
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
    
    # Check the Stratigraphy fields
    # -----------------------------
    
    # Check the max and min intervals.
    
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

    # Check the direct, max, and min dates if specified.
    
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
	
	# For each of 'direct', 'max', and 'min': ignore the 'error', 'unit',
	# and 'method' fields unless the 'ma' field is specified, or unless this
	# is an existing record with a corresponding 'ma' value.
	
	if ( $operation eq 'update' )
	{
	    ($old_direct_ma, $old_max_ma, $old_min_ma) = 
		$dbh->selectrow_array("
		SELECT direct_ma, max_ma, min_ma
		FROM $TABLE{COLLECTION_DATA} WHERE $keyexpr");
	    
	    $direct_ma = $old_direct_ma unless $action->field_specified($direct_ma);
	    $max_ma = $old_max_ma unless $action->field_specified($max_ma);
	    $min_ma = $old_min_ma unless $action->field_specified($min_ma);
	}
	
	unless ( $direct_ma && $direct_ma ne '' )
	{
	    $action->ignore_field('direct_ma_error');
	    $action->ignore_field('direct_ma_unit');
	    $action->ignore_field('direct_ma_method');
	    $direct_ma_error = undef;
	    $direct_ma_unit = undef;
	    $direct_ma_method = undef;
	}
	
	unless ( $max_ma && $max_ma ne '' )
	{
	    $action->ignore_field('max_ma_error');
	    $action->ignore_field('max_ma_unit');
	    $action->ignore_field('max_ma_method');
	    $max_ma_error = undef;
	    $max_ma_unit = undef;
	    $max_ma_method = undef;
	}
	
	unless ( $min_ma && $min_ma ne '' )
	{
	    $action->ignore_field('min_ma_error');
	    $action->ignore_field('min_ma_unit');
	    $action->ignore_field('min_ma_method');
	    $min_ma_error = undef;
	    $min_ma_unit = undef;
	    $min_ma_method = undef;
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
    
    # Check the 'altitude_value' field if specified.
    
    my $altitude_value = $action->record_value('altitude_value');

    if ( defined $altitude_value && $altitude_value ne '' )
    {
	$edt->add_condition('E_FORMAT', 'altitude_value', "must be a non-negative integer")
	    unless $altitude_value =~ $DIGITS_RE;
    }
    
    # Checks on the geology fields
    # ----------------------------

    my $fossilsfrom1 = $action->record_value('fossilsfrom1');
    my $fossilsfrom2 = $action->record_value('fossilsfrom2');

    if ( defined $fossilsfrom1 )
    {
	$action->set_record_value('fossilsfrom1', $fossilsfrom1 ? 'Y' : '');
    }

    if ( defined $fossilsfrom2 )
    {
	$action->set_record_value('fossilsfrom2', $fossilsfrom2 ? 'Y' : '');
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


sub after_coll_action {
    
    my ($edt, $action, $operation, $table_specifier, $new_keyval) = @_;
    
    my $dbh = $edt->dbh;
    
    my ($keyexpr, @keyvals, $sql, $result);
    my $tableinfo = $edt->table_info_ref('COLLECTION_DATA');

    # For an 'insert' operation, the new key value is provided as an argument to
    # this method.
    
    if ( $operation eq 'insert' )
    {
	$keyexpr = "collection_no = '$new_keyval'";
	@keyvals = $new_keyval;
	$edt->set_attr_key('update_colls', $new_keyval, 1);
    }
    
    # Otherwise, we can get it from the action.
    
    elsif ( $operation eq 'delete' )
    {
	$keyexpr = $action->keyexpr;
	@keyvals = $action->keyvals;
	$edt->set_attr_key('delete_colls', $_, 1) foreach @keyvals;

    }
    
    else
    {
	$keyexpr = $action->keyexpr;
	@keyvals = $action->keyvals;
	$edt->set_attr_key('update_colls', $_, 1) foreach @keyvals;
    }

    # Then add and remove any necessary entries to the secondary reference table.
    # Because this is happening after the main operation, we don't generate any
    # error conditions. Instead, we just silently ignore any reference identifier
    # that is invalid to add or remove. If the keyvalue is multiple, iterate over
    # all of the corresponding collection identifiers.
    
    my (@add_refs, @delete_refs);
    
    # If a list of references to add was specified, put them on the add list. If a
    # new primary reference identifier was specified, make sure it is on the list
    # too.
    
    my $new_primary = $action->record_value('reference_id');
    
    if ( my $add_refs = $action->record_value('reference_add') )
    {
	if ( ref $add_refs eq 'ARRAY' )
	{
	    @add_refs = @$add_refs;
	}
	
	else
	{
	    @add_refs = $add_refs;
	}
	
	if ( $new_primary && List::Util::none { $_ eq $new_primary } @add_refs )
	{
	    push @add_refs, $new_primary;
	}
    }
    
    elsif ( $new_primary )
    {
	push @add_refs, $new_primary;
    }
    
    # If a list of references to delete was specified, put them on the delete list.
    # However, don't delete the primary reference.
    
    if ( my $delete_refs = $action->record_value('reference_delete') )
    {
	if ( ref $delete_refs eq 'ARRAY' )
	{
	    @delete_refs = @$delete_refs;
	}
	
	else
	{
	    @delete_refs = $delete_refs;
	}
	
	# If a new primary reference was set, make sure we don't delete it.
	
	if ( $new_primary )
	{
	    @delete_refs = grep { $_ ne $new_primary } @delete_refs;
	}
    }
    
    # Now iterate over all of the collections to update. In almost all cases, there
    # will be only one.
    
    foreach my $coll_id ( @keyvals )
    {
	my $qcoll = $dbh->quote($coll_id);
	
	# For logging purposes, fetch the current list of secondary references.
	
	$result = $dbh->selectall_arrayref("SELECT * FROM $TABLE{COLLECTION_REFS}
			WHERE collection_no = $qcoll", { Slice => { } });

	# For a delete operation, we just delete all of the secondary references.

	if ( $operation eq 'delete' )
	{
	    $sql = "DELETE FROM $TABLE{COLLECTION_REFS} WHERE collection_no = $qcoll";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    $dbh->do($sql);
	    
	    if ( $tableinfo->{LOG_CHANGES} && ref $result eq 'ARRAY' )
	    {
		$edt->log_aux_event('delete', 'COLLECTION_REFS', $sql, 'collection_no', $coll_id,
				    $result);
	    }

	    next;
	}
	
	# If there are any refs to add, add them now. Make sure to add only those
	# identifiers that exist in the REFERENCE_DATA table. We use INSERT IGNORE
	# in case any of these entries already exists in the table.
	
	if ( @add_refs )
	{
	    my $add_list = join(',', map { $dbh->quote($_) } @add_refs);
	    
	    $sql = "INSERT IGNORE INTO $TABLE{COLLECTION_REFS} (collection_no, reference_no)
			SELECT $qcoll as collection_no, reference_no FROM $TABLE{REFERENCE_DATA}
			WHERE reference_no in ($add_list)";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    $dbh->do($sql);
	    
	    if ( $tableinfo->{LOG_CHANGES} && ref $result eq 'ARRAY' )
	    {
		$edt->log_aux_event('insert', 'COLLECTION_REFS', $sql, 'collection_no', $coll_id,
				    $result);
	    }
	}
	
	# If there are any refs to delete, delete them now. If a new primary
	# reference was not set, retrieve the current primary reference for this
	# collection and make sure that we don't delete it.
	
	if ( @delete_refs )
	{
	    my $delete_list;
	    
	    if ( $new_primary )
	    {
		$delete_list = join(',', map { $dbh->quote($_) } @delete_refs);
	    }
	    
	    else
	    {
		$sql = "SELECT reference_no FROM $TABLE{COLLECTION_DATA}
			    WHERE collection_no = $qcoll";
		
		$edt->debug_line("$sql\n") if $edt->debug_mode;
		
		my ($current_primary) = $dbh->selectrow_array($sql);
		
		$current_primary //= '';
		
		$delete_list = join(',', map { $dbh->quote($_) }
				    grep { $_ ne $current_primary } @delete_refs);
	    }
	    
	    if ( $delete_list )
	    {
		$sql = "DELETE sr FROM $TABLE{COLLECTION_REFS} as sr
			    left join $TABLE{OCCURRENCE_DATA} as o1 using (collection_no, reference_no)
			    left join $TABLE{OCCURRENCE_DATA} as o2 using (collection_no)
			    left join $TABLE{SPECIMEN_DATA} as sp on
			        sp.occurrence_no = o2.occurrence_no and sp.reference_no = sr.reference_no
		        WHERE collection_no = $qcoll and sr.reference_no in ($delete_list)
			      and o1.occurrence_no is null and sp.specimen_no is null";
		
		$edt->debug_line("$sql\n") if $edt->debug_mode;
		
		$dbh->do($sql);
		
		if ( $tableinfo->{LOG_CHANGES} && ref $result eq 'ARRAY' && @$result )
		{
		    $edt->log_aux_event('delete', 'COLLECTION_REFS', $sql, 'collection_no', $coll_id,
					$result);
		}
	    }
	}
    }
}


# Methods for occurrences and reidentifications
# ---------------------------------------------

# initialize_occ_action ( action, operation, table_specifier )
#
# Make sure that every action on the 'OCCURRENCE_DATA' and 'REID_DATA' tables has a
# valid 'collection_id' field. This is necessary for authorization, because those tables
# use the 'COLLECTION_DATA' table for authorization. Inserts into the REID_DATA table
# also require a valid 'occurrence_id' field.

sub initialize_occ_action {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # First, fetch any existing key values from the action record.
    
    my $collection_no = $action->record_value('collection_id');
    my $occurrence_no = $action->record_value('occurrence_id');
    my $reid_no = $action->record_value('reid_id');
    my $check;
    
    my $occs = $edt->get_attr('occs');
    my $reids = $edt->get_attr('reids');
    my $sql;
    
    # Select the case that applies to this action.
    
    # A delete operation can have multiple key values, but they must all correspond
    # to the same collection.
    
    if ( $operation eq 'delete' )
    {
	my $dbh = $edt->dbh;
	my $keyexpr = $action->keyexpr;
	
	$sql = "SELECT distinct collection_no FROM $TABLE{$table_specifier}
		WHERE $keyexpr";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	my $result = $dbh->selectcol_arrayref($sql);
	
	if ( $result && @$result > 1 )
	{
	    $edt->add_condition('E_MULTI_COLLECTIONS');
	}
	
	else
	{
	    my $collection_no = $result->[0] || '0';
	    
	    $action->set_linkinfo('collection_no', 'collection_no', 'collection_id',
				  $collection_no, "collection_no = '$collection_no'");
	}
    }
    
    # We are currently not allowing 'replace' operations, until I can work out the logic
    # properly.
    
    elsif ( $operation eq 'replace' )
    {
	$edt->add_condition('E_BAD_OPERATION', 'replace');
    }
    
    # For insertions and updates, there are two cases depending on which table we are
    # working with.
    
    elsif ( $table_specifier eq 'OCCURRENCE_DATA' )
    {
	# If reid_id was specified, the wrong table was used when creating this
	# action.
	
	if ( $action->field_specified('reid_id') )
	{
	    croak("table OCCURRENCE_DATA cannot be specified with 'reid_id'");
	}
	
	# If this is an insert operation on the OCCURRENCE_DATA table, then
	# collection_id is required.
	
	elsif ( $operation eq 'insert' )
	{
	    if ( defined $collection_no && $collection_no ne '' )
	    {
		# We must stringify the collection_no value, because it may be a
		# stringifiable object.
		
		$collection_no = "$collection_no";
		
		# Unless we know this is a valid collection number, check it against the
		# COLLECTION_DATA table.
		
		unless ( $edt->get_attr_key('valid_collection', $collection_no ) )
		{
		    my $dbh = $edt->dbh;
		    my $qref = $dbh->quote($collection_no);
		    
		    $sql = "SELECT collection_no FROM $TABLE{COLLECTION_DATA}
			WHERE collection_no = $qref";
		    
		    $edt->debug_line("$sql\n") if $edt->debug_mode;
		    
		    my ($check) = $dbh->selectrow_array($sql);
		    
		    if ( $check )
		    {
			$action->set_attr('collection_no', $collection_no);
			$edt->set_attr_key('valid_collection', $collection_no, 1);
			$edt->set_attr_key('show_collection', $collection_no, 1);
			$edt->fetch_existing_names($collection_no);
		    }
		    
		    else
		    {
			$edt->add_condition('E_BAD_VALUE', 'collection_id',
					    "unknown collection $qref");
			return;
		    }
		}
	    }
	    
	    else
	    {
		$edt->add_condition('E_REQUIRED', 'collection_no');
	    }
	}
	
	# For any other operation on the OCCURRENCE_DATA table, collection_no is ignored
	# if specified. Use the collection_no value from the existing occurrence record.
	
	else
	{
	    $action->handle_column(collection_no => 'ignore');
	    
	    if ( $occurrence_no = $action->keyval )
	    {
		$collection_no = $occs->{$occurrence_no}{collection_no};
		$action->set_attr('collection_no', $collection_no);
	    }
	    
	    # If more than one key was specified, add an error condition.
	    
	    if ( ref $occurrence_no eq 'ARRAY' )
	    {
		$edt->add_condition('E_MULTI_KEY', $operation);
	    }
	    
	    # If we could not find both an occurrence_no and a collection_no, add an
	    # error condition. This should not occur unless something has gone
	    # seriously wrong.
	    
	    elsif ( $action->can_proceed && ! ($occurrence_no && $collection_no) )
	    {
		$edt->add_condition('E_EXECUTE', "could not locate collection_no");
	    }
	}
    }
    
    # Otherwise, the 'initialize_action' method has already ensured that the table must be
    # REID_DATA.
    
    else
    {
	# If this is an insert operation on the REID_DATA table, then
	# occurrence_id is required.
	
	if ( $operation eq 'insert' )
	{
	    if ( defined $occurrence_no && $occurrence_no ne '' )
	    {
		# We must stringify the occurrence_no value, because it may be a
		# stringifiable object.
		
		$occurrence_no = "$occurrence_no";
		
		# If the stringified value is a decimal number greater than zero, check
		# that it corresponds to an existing occurrence.
		
		if ( $occurrence_no > 0 )
		{
		    # Unless we know that this is a valid occurrence, check it against
		    # the OCCURRENCE_DATA table. Fetch the collection_no as well.
		    
		    unless ( $edt->get_attr_key('valid_occurrence', $occurrence_no ) )
		    {
			my $dbh = $edt->dbh;
			my $qref = $dbh->quote($occurrence_no);
			
			$sql = "SELECT occurrence_no, collection_no FROM $TABLE{OCCURRENCE_DATA}
				WHERE occurrence_no = $qref";
			
			$edt->debug_line("$sql\n") if $edt->debug_mode;
			
			($check, $collection_no) = $dbh->selectrow_array($sql);
			
			if ( $check )
			{
			    $action->set_attr('occurrence_no', $occurrence_no);
			    $action->set_attr('collection_no', $collection_no);
			    $action->set_record_value('collection_id', $collection_no);
			    $edt->set_attr_key('valid_occurrence', $occurrence_no, 1);
			    $edt->set_attr_key('show_collection', $collection_no, 1);
			}
			
			else
			{
			    $edt->add_condition('E_BAD_VALUE', 'occurrence_id',
						"unknown occurrence $qref");
			    return;
			}
		    }
		    
		    else
		    {
			$collection_no = $occs->{$occurrence_no}{collection_no};
			$action->set_attr('occurrence_no', $occurrence_no);
			$action->set_attr('collection_no', $collection_no);
			$action->set_record_value('collection_id', $collection_no);
		    }
		}
		
		# If the value is an action label, look up the collection_no from that
		# action. If we can't find one, add an E_BAD_REFERENCE condition.

		elsif ( $occurrence_no =~ /^&/ )
		{
		    # my $referenced_action = $edt->action_ref($occurrence_no);
		    
		    # if ( $referenced_action )
		    # {
		    # 	$collection_no = $referenced_action->record_value('collection_id') || '';
		    # 	$collection_no = "$collection_no";
		    # }
		    
		    # unless ( $collection_no )
		    # {
			$edt->add_condition('E_BAD_REFERENCE', 'occurrence_id', $occurrence_no);
			return;
		    # }
		}
		
		else
		{
		    $edt->add_condition('E_FORMAT', 'occurrence_id',
					"must be an occurrence id or a record label");
		}
	    }
	    
	    else
	    {
		$edt->add_condition('E_REQUIRED', 'occurrence_id');
	    }
	}

	elsif ( $operation eq 'delete' )
	{
	    my @keyvals = $action->keyvals;
	    my %collections;
	    
	    foreach my $reid_no ( @keyvals )
	    {
		my $collection_no = $occs->{"$occurrence_no"}{collection_no};
		$collections{$collection_no || 'bad'} = 1;
	    }
	    
	    if ( $collections{bad} )
	    {
		$edt->add_condition('E_EXECUTE', "collection_no was not found for delete");
	    }
	    
	    elsif ( %collections > 1 )
	    {
		$edt->add_condition('E_MULTI_COLLECTIONS');
	    }
	}
	
	# For any other operation on the REID_DATA table, both collection_no and
	# occurrence_no are ignored if specified. Use the occurrence_no value from
	# the existing occurrence record.
	
	else
	{
	    $action->handle_column('occurrence_id', 'ignore');
	    $action->handle_column('collection_id', 'ignore');
	    
	    if ( $reid_no = $action->keyval )
	    {
		$collection_no = $reids->{$reid_no}{collection_no};
		$occurrence_no = $reids->{$reid_no}{occurrence_no};

		$action->set_attr('collection_no', $collection_no);
		$action->set_attr('occurrence_no', $occurrence_no);
		$action->set_attr('reid_no', $reid_no);
		
		$action->set_record_value('collection_id', $collection_no);
	    }
	    
	    # If more than one key was specified, add an error condition.

	    if ( ref $reid_no eq 'ARRAY' )
	    {
		$edt->add_condition('E_MULTI_KEY', $operation);
	    }
	    
	    # If we could not find all three ids, add an error condition. This should
	    # not occur unless something has gone seriously wrong.
	    
	    elsif ( $action->can_proceed && ! ($reid_no && $occurrence_no && $collection_no) )
	    {
		$edt->add_condition('E_EXECUTE', "could not locate occurrence_no or collection_no");
	    }
	}
    }
}


# validate_occ_action ( action, operation, table_specifier )
# 
# Validate an action (insert, replace, update, delete) on the OCCURRENCE_DATA
# table or the REID_DATA table.

sub validate_occ_action {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    my ($sql, $result);
    
    my $dbh = $edt->dbh;
    
    my $occs = $edt->get_attr('occs');
    my $reids = $edt->get_attr('reids');
    
    my $occurrence_no = $action->get_attr('occurrence_no');
    my $reid_no = $action->get_attr('reid_no');
    
    # If the operation is 'delete', call the appropriate routine and return. We don't
    # need any of the checks below.

    if ( $operation eq 'delete' )
    {
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->validate_delete_occs($action);
	}

	else
	{
	    $edt->validate_delete_reids($action);
	}
    }
    
    # Validate reference_id
    # ---------------------
    
    # This field is required and must match an existing reference.
    
    my $reference_no = $action->record_value('reference_no') ||
	$action->record_value('reference_id');
    
    if ( defined $reference_no && $reference_no ne '' )
    {
	# Stringify this value, because it may be an object representing an external
	# identifier.
	
	$reference_no = "$reference_no";
	
	my $qref = $dbh->quote($reference_no);
	
	unless ( $edt->get_attr_key('valid_reference', $reference_no ) )
	{	    
	    $sql = "SELECT reference_no FROM $TABLE{REFERENCE_DATA}
		    WHERE reference_no = $qref";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my ($check) = $dbh->selectrow_array($sql);
	    
	    if ( $check )
	    {
		$edt->set_attr_key('valid_reference', $reference_no, 1);
	    }
	    
	    else
	    {
		$edt->add_condition('E_BAD_VALUE', 'reference_id',
				    "unknown reference $qref");
	    }
	}
	
	# If this is an occurrence update or replacement, check if any of the
	# reidentifications already use this reference.
	
	if ( $table_specifier eq 'OCCURRENCE_DATA' && $operation =~ /^rep|^upd/ )
	{
	    my $qocc = $dbh->quote($occurrence_no);

	    $sql = "SELECT reference_no FROM $TABLE{REID_DATA}
		WHERE occurrence_no = $qocc";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;

	    my $result = $dbh->selectcol_arrayref($sql);

	    foreach my $rr ( @$result )
	    {
		if ( $rr eq $reference_no )
		{
		    $edt->add_condition('E_BAD_VALUE', 'reference_no',
					"a reidentification of this occurrence uses this reference");
		}
	    }
	}
	
	# If this is a reidentification action, check if the occurrence or any of the
	# other reidentifications already use this reference.
	
	elsif ( $operation =~ /^rep|^upd/ )
	{
	    my $qocc = $dbh->quote($occurrence_no);
	    
	    $sql = "SELECT reference_no FROM $TABLE{OCCURRENCE_DATA}
		WHERE reference_no = $qref and occurrence_no = $qocc
		UNION SELECT reference_no FROM $TABLE{REID_DATA}
		WHERE reference_no = $qref and occurrence_no = $qocc";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my ($check) = $dbh->selectrow_array($sql);
	    
	    if ( $check )
	    {
		$edt->add_condition('E_BAD_VALUE', 'reference_id',
				    "there is already an identification of this occurrence using this reference");
	    }
	}
    }
    
    # Validate the taxonomic name
    # ---------------------------
    
    # This field will be deconstructed and used to set the fields 'genus_name',
    # 'genus_reso', etc. It must not be specified with an empty value.
    
    my $identified_name = $action->record_value('identified_name');
    my $taxon_no = $action->record_value('taxon_no');
    my $collection_no = $action->get_attr('collection_no');
    
    $action->ignore_field('identified_name');
    
    if ( defined $identified_name && $identified_name ne '' )
    {
	$edt->validate_identified_name($action, $table_specifier, $collection_no, $identified_name);
    }

    elsif ( $operation eq 'insert' || $action->field_specified('identified_name') )
    {
	$edt->add_condition('E_REQUIRED', 'identified_name');
    }
    
    # If the name isn't being updated but the taxon_no is, validate it according to the
    # name components stored in the database. This may also involve updating the type
    # locality.
    
    elsif ( $taxon_no && ($operation eq 'update' || $operation eq 'replace') )
    {
	my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	    $species_name, $species_reso, $subspecies_name, $subspecies_reso);
	
	my $identification_data;
	
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    unless ( $occurrence_no && $occs->{$occurrence_no} )
	    {
		$edt->add_condition('E_EXECUTE',
				    "could not retrieve occurrence data during validation");
		return;
	    }
	    
	    $identification_data = $occs->{$occurrence_no};
	}

	else
	{
	    unless ( $reid_no && $reids->{$reid_no} )
	    {
		$edt->add_condition('E_EXECUTE',
				    "could not retrieve reidentification data during validation");
		return;
	    }
	    
	    $identification_data = $reids->{$reid_no};
	}
	
	$genus_name = $identification_data->{genus_name};
	$genus_reso = $identification_data->{genus_reso};
	$subgenus_name = $identification_data->{subgenus_name};
	$subgenus_reso = $identification_data->{subgenus_reso};
	$species_name = $identification_data->{species_name};
	$species_reso = $identification_data->{species_reso};
	$subspecies_name = $identification_data->{subspecies_name};
	$subspecies_reso = $identification_data->{subspecies_reso};
	
	$edt->validate_taxon_no($action, $genus_name, $genus_reso,
				$subgenus_name, $subgenus_reso,
				$species_name, $species_reso,
				$subspecies_name, $subspecies_reso);
    }
    
    # Validate the plant organ
    # ------------------------
    
    # If more than one is specified, they must be (for now) stored in separate fields.
    # Two is (for now) the limit.
    
    my $plant_organs = $action->record_value('plant_organ');
    
    if ( defined $plant_organs && $plant_organs ne '' )
    {
	my @organs = grep { $_ ne '' } split /\s*,\s*/, $plant_organs;
	
	if ( @organs > 2 )
	{
	    $edt->add_condition('E_BAD_VALUE', 'plant_organ',
				"Only two plant organs may be specified");
	}
	
	elsif ( @organs == 2 )
	{
	    $action->set_record_value('plant_organ', $organs[0]);
	    $action->set_record_value('plant_organ2', $organs[1]);
	}
	
	else # @organs == 1
	{
	    $action->set_record_value('plant_organ', $organs[0]);
	    $action->set_record_value('plant_organ2', undef);
	}
    }

    elsif ( $action->field_specified('plant_organ') )
    {
	$action->set_record_value('plant_organ', undef);
	$action->set_record_value('plant_organ2', undef);
    }
}


sub validate_delete_occs {

    my ($edt, $action) = @_;
    
    # When deleting occurrences, we cannot proceed if there are any specimens
    # corresponding to the occurrence(s).
    
    my $dbh = $edt->dbh;
    my $keyexpr = $action->keyexpr;
    my $sql = "SELECT count(specimen_no) FROM $TABLE{SPECIMEN_DATA} WHERE $keyexpr";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ( $check ) = $dbh->selectrow_array($sql);
    
    if ( $check )
    {
	$edt->add_condition('E_CANNOT_DELETE', 'occurrence');
	return;
    }
    
    # Mark each occurrence for deletion.
    
    foreach my $k ( $action->keyvals )
    {
	$edt->set_attr_key('delete_occs', $k, 1);
    }

    # Fetch all of the reidentifications that are attached to the occurrence(s) and add
    # an action to delete them too.
    
    $sql = "SELECT reid_no FROM $TABLE{REID_DATA} WHERE $keyexpr";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my $result = $dbh->selectcol_arrayref($sql);

    if ( $result && @$result )
    {
	$edt->add_child_action('delete', 'REID_DATA', { reid_id => $result });
    }
}


sub validate_delete_reids {

    my ($edt, $action) = @_;
    
    # When deleting reidentifications, we must mark all of the corresponding occurrences
    # for update.
    
    my $dbh = $edt->dbh;
    my $keyexpr = $action->keyexpr;
    my $sql = "SELECT occurrence_no FROM $TABLE{REID_DATA}
		WHERE $keyexpr";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my $result = $dbh->selectcol_arrayref($sql);
    
    foreach my $occurrence_no ( @$result )
    {
	$edt->set_attr_key('recompute_occs', $occurrence_no, 1);
	$edt->set_attr_key('update_occs', $occurrence_no, 1);
    }
}


sub validate_identified_name {

    my ($edt, $action, $table_specifier, $collection_no, $identified_name) = @_;
    
    # The following variables will be filled in by the name parsing routine below.

    my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso);
    
    # Prepare to parse the identified name by trimming whitespace.
    
    $identified_name =~ s/^\s+//;
    $identified_name =~ s/\s+$//;
    
    my $name = $identified_name;
    
    # Check for n. gen., n. sp., etc.  These have an unambiguous meaning
    # wherever they occur. Removing them may leave extra spaces in the
    # name.

    if ( $name =~ /(.*)n[.]\s*gen[.](.*)/ )
    {
	$genus_reso = "n. gen.";
	$name = "$1 $2";
    }
    
    if ( $name =~ /(.*)n[.]\s*subgen[.](.*)/ )
    {
	$subgenus_reso = "n. subgen.";
	$name = "$1 $2";
    }
    
    if ( $name =~ /(.*)n[.]\s*sp[.](.*)/ )
    {
	$species_reso = "n. sp.";
	$name = "$1 $2";
    }
    
    if ( $name =~ /(.*)n[.]\s*(subsp|ssp)[.](.*)/ )
    {
	$subspecies_reso = "n. ssp.";
	$name = "$1 $3";
    }
    
    # Now deconstruct the name component by component, using the same rules as the
    # Javascript form checker.
    
    # Start with the genus and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{genus} )
    {
	if ( $genus_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting genus modifier on '$identified_name'");
	    return;
	}
	
	$genus_reso = $1;
	$name = $2;
    }
    
    if ( $name =~ /^\s*<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $genus_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting genus modifier on '$identified_name'");
	    return;
	}
	
	$genus_reso = 'informal';
	$genus_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^\s*("?)([A-Za-z]+)("?)\s*(.*)/ )
    {
	if ( $genus_reso && $1 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting genus modifier on '$identified_name'");
	    return;
	}
	
	elsif ( $1 )
	{
	    $genus_reso = $1;
	}
	
	$genus_name = $2;
	$name = $4;
	
	unless ( $1 eq $3 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Mismatched &quot; on '$identified_name'");
	    return;
	}
    }
    
    else
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not resolve genus");
	return;
    }
    
    if ( $genus_name && $genus_reso ne 'informal' && $genus_name !~ /^[A-Z][a-z]+$/ )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': bad capitalization on genus");
	return;
    }
    
    # Continue with a possible subgenus and preceding qualifier.
    
    if ( $name =~ $OCC_RESO_RE{subgenus} )
    {
	if ( $subgenus_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subgenus modifier on '$identified_name'");
	    return;
	}
	
	$subgenus_reso = $1;
	$name = $2;
    }
    
    if ( $name =~ /^[(]<(.*?\S.*?)>[)]\s*(.*)/ )
    {
	if ( $subgenus_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subgenus modifier on '$identified_name'");
	    return;
	}
	
	$subgenus_reso = 'informal';
	$subgenus_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^[(]("?)([A-Za-z]+)("?)[)]\s*(.*)/ )
    {
	if ( $subgenus_reso && $1 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subgenus modifier on '$identified_name'");
	    return;
	}
	
	elsif ( $1 )
	{
	    $subgenus_reso = $1;
	}
	
	$subgenus_name = $2;
	$name = $4;
	
	unless ( $1 eq $3 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Invalid name '$identified_name': mismatched &quot; on subgenus");
	    return;
	}
    }
    
    elsif ( $name =~ /[(]/ )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not resolve subgenus");
	return;
    }
    
    else
    {
	$subgenus_name ||= '';
    }
    
    if ( $subgenus_name && $subgenus_reso ne 'informal' &&
	 $subgenus_name !~ /^[A-Z][a-z]+$/ )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': bad capitalization on subgenus");
    }
    
    # Continue with a species name and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{species} )
    {
	if ( $species_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting species modifier on '$identified_name'");
	    return;
	}
	
	$species_reso = $1;
	$name = $2;
    }
    
    if ( ($species_reso eq 'cf.' || $species_reso eq 'aff.') &&
	 $name =~ /([A-Z])[.]\s*(.*)/ )
    {
	if ( $1 ne substr($genus_name, 0, 1) )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Genus initial after $species_reso did not match genus");
	    return;
	}

	$name = $2;
    }
    
    if ( $name =~ /^<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $species_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting species modifier on '$identified_name'");
	    return;
	}
	
	$species_reso = 'informal';
	$species_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^("?)([A-Za-z]+[.]?)("?)\s*(.*)/ )
    {
	if ( $species_reso && $1 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting species modifier on '$identified_name'");
	    return;
	}
	
	elsif ( $1 )
	{
	    $species_reso = $1;
	}
	
	$species_name = $2;
	$name = $4;
	
	unless ( $1 eq $3 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Invalid name '$identified_name': mismatched &quot; on species");
	}
    }
    
    elsif ( $species_reso && ! $species_name  )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not resolve species");
	return;
    }
    
    else
    {
	$species_name ||= '';
    }
    
    if ( $species_name && $species_reso ne 'informal' )
    {
	if ( $species_name =~ /[.]$/ )
	{
	    if ( $species_name !~ /^(?:sp|spp|indet)[.]$/ )
	    {
		$edt->add_condition('E_FORMAT', 'identified_name',
				    "Invalid name '$identified_name': '$species_name' is not valid");
		return;
	    }
	}
	
	elsif ( $species_name !~ /^[a-z]+$/ )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Invalid name '$identified_name': bad capitalization on species");
	    return;
	}
    }
    
    # Finish with a possible subspecies name and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{subspecies} )
    {
	if ( $subspecies_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subspecies modifier on '$identified_name'");
	    return;
	}
	
	$subspecies_reso = $1;
	$name = $2;
    }
    
    if ( $name =~ /^<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $subspecies_reso )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subspecies modifier on '$identified_name'");
	    return;
	}
	
	$subspecies_reso = 'informal';
	$subspecies_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^("?)([A-Za-z]+[.]?)("?)\s*(.*)/ )
    {
	if ( $subspecies_reso && $1 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Conflicting subspecies modifier on '$identified_name'");
	    return;
	}
	
	elsif ( $1 )
	{
	    $subspecies_reso = $1;
	}
	
	$subspecies_name = $2;
	$name = $4;
	
	unless ( $1 eq $3 )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Invalid name '$identified_name': mismatched &quot; on subspecies");
	}
    }
    
    elsif ( $name && ! $species_name )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not resolve species");
	return;
    }
    
    elsif ( $subspecies_reso )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not resolve subspecies");
	return;
    }
    
    elsif ( $name && $name !~ /^\s+$/ )
    {
	$edt->add_condition('E_FORMAT', 'identified_name',
			    "Invalid name '$identified_name': could not parse '$name'");
	return;
    }
    
    else
    {
	$subspecies_name ||= '';
    }
    
    if ( $subspecies_name && $subspecies_reso ne 'informal' )
    {
	if ( $subspecies_name =~ /[.]$/ )
	{
	    if ( $subspecies_name !~ /^(?:subsp|subspp|indet)[.]$/ )
	    {
		$edt->add_condition('E_FORMAT', 'identified_name',
				    "Invalid name '$identified_name': '$subspecies_name' is not valid");
		return;
	    }
	}
	
	elsif ( $subspecies_name !~ /^[a-z]+$/ )
	{
	    $edt->add_condition('E_FORMAT', 'identified_name',
				"Invalid name '$identified_name': bad capitalization on subspecies");
	    return;
	}
    }
    
    # If we get here, then the name has been correctly parsed.
    
    no warnings 'uninitialized';
    
    my $name_check = join('|', $genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
			  $species_name, $species_reso, $subspecies_name, $subspecies_reso, '#');
    
    if ( $edt->get_attr_2key('occ_names', $collection_no, $name_check) )
    {
	$edt->add_condition('E_BAD_VALUE', 'identified_name',
			    "duplicates another occurrence in this collection");
    }

    else
    {
	$edt->set_attr_2key('occ_names', $collection_no, $name_check, 1);
    }
    
    $action->set_record_value('genus_name', $genus_name);
    $action->set_record_value('genus_reso', $genus_reso);
    $action->set_record_value('subgenus_name', $subgenus_name);
    $action->set_record_value('subgenus_reso', $subgenus_reso);
    $action->set_record_value('species_name', $species_name);
    $action->set_record_value('species_reso', $species_reso);
    $action->set_record_value('subspecies_name', $subspecies_name);
    $action->set_record_value('subspecies_reso', $subspecies_reso);
    
    # Now put the name back together without modifiers, and link it up to its
    # corresponding authority record if the name has already been entered. This may also
    # involve updating the type locality.
    
    $edt->validate_taxon_no($action, $genus_name, $genus_reso,
			    $subgenus_name, $subgenus_reso,
			    $species_name, $species_reso,
			    $subspecies_name, $subspecies_reso);
}


# validate_taxon_no ( genus_name, genus_reso, ... )
#
# Put the taxon name back together from its individual components without modifiers, and
# link it up to its corresponding authority record if the name has already been entered
# into the database.

sub validate_taxon_no {

    my ($edt, $action, $genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) = @_;
    
    if ( $genus_reso eq 'informal' )
    {
	$action->set_record_value('taxon_no', 0);
	return;
    }
    
    my $taxon_name = $genus_name;
    
    $taxon_name .= " ($subgenus_name)"
	if $subgenus_name && $subgenus_reso ne 'informal';
    
    $taxon_name .= " $species_name"
	if $species_name && $species_name !~ /[.?]$/ && $species_reso ne 'informal';
    
    $taxon_name .= " $subspecies_name"
	if $subspecies_name && $subspecies_name !~ /[.?]$/ && $subspecies_reso ne 'informal';
    
    my $dbh = $edt->dbh;
    my $qname = $dbh->quote($taxon_name);
    
    my $sql = "SELECT taxon_no FROM $TABLE{AUTHORITY_DATA} WHERE taxon_name = $qname";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my $result = $dbh->selectcol_arrayref($sql);
    
    if ( my $submitted_no = $action->record_value('taxon_no') )
    {
	unless ( any { $_ eq $submitted_no } @$result )
	{
	    $edt->add_condition('E_BAD_VALUE', 'taxon_no',
				"does not match the identified name");
	}
    }

    else
    {
	my $taxon_no = $result->[0] || 0;

	$action->set_record_value('taxon_no', $taxon_no);
    }
}


sub after_occ_action {
    
    my ($edt, $action, $operation, $table_specifier, $new_keyval) = @_;
    
    my @keyvals;
    
    # For an 'insert' operation, the new key value is provided as an argument to
    # this method.
    
    if ( $operation eq 'insert' )
    {
	@keyvals = $new_keyval;
	
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->set_attr_key('update_occs', $new_keyval, 1);
	}
	
	elsif ( my $occurrence_no = $action->get_attr('occurrence_id') )
	{
	    $edt->set_attr_key('update_occs', $occurrence_no, 1);
	}
	
	# $$$ need to handle type locality here
    }
    
    # Otherwise, we can get it from the action.
    
    elsif ( $operation eq 'delete' )
    {
	@keyvals = $action->keyvals;
	
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->set_attr_key('delete_occs', $_, 1) foreach @keyvals;
	}
	
	else
	{
	    $edt->set_attr_key('delete_reids', $_, 1) foreach @keyvals;
	}
	
	# $$$ need to handle type locality here
    }
    
    else
    {
	@keyvals = $action->keyvals;

	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->set_attr_key('update_occs', $_, 1) foreach @keyvals;
	}
	
	elsif ( my $occurrence_no = $action->get_attr('occurrence_id') )
	{
	    $edt->set_attr_key('update_occs', $occurrence_no, 1);
	}
	
	# $$$ need to handle type locality here
    }
}


sub recompute_occs {

    my ($edt, $dbh, $occs_list) = @_;

    foreach my $occ_id ( @$occs_list )
    {
	my $quoted = $dbh->quote($occ_id);
	my $sql = "UPDATE $TABLE{OCCURRENCE_DATA} as o
			left join $TABLE{REID_DATA} as re using (occurrence_no)
			left join $TABLE{REFERENCE_DATA} as r on r.reference_no = re.reference_no
		SET o.reid_no = if(re.reid_no is null, 0, re.reid_no)
		WHERE occurrence_no = $quoted
		ORDER BY pubyr desc, re.reid_no desc LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$dbh->do($sql);
	
	$sql = "UPDATE $TABLE{OCCURRENCE_DATA} as o
		    join $TABLE{REID_DATA} as re using (occurrence_no)
		SET re.most_recent = if(o.reid_no = re.reid_no, 'YES', 'NO')
		WHERE occurrence_no = $quoted";

	$edt->debug_line("$sql\n") if $edt->debug_mode;

	$dbh->do($sql);
    }
}


# The following methods are unique to this subclass:
# --------------------------------------------------

# initialize_occs ( occurrence_nos, reid_nos )
#
# This method should be called for any EditTransaction involving occurrences. The
# parameter $occurrence_nos must be a hashref whose keys are the existing occurrence
# numbers involved in the transaction. The parameter $reid_nos must be a hashref whose
# keys are the existing reid numbers involved in the transaction.
#
# The purpose of this method is to fetch all of the existing occurrences for the
# collection(s) in which the submitted occurrences are located. This will allow checking
# for duplicates, checking for collection_no changes, etc.

sub initialize_occs {

    my ($edt, $occurrence_nos, $reid_nos) = @_;
    
    my $dbh = $edt->dbh;
    
    # Select all occurrences from the collection(s) corresponding to the submitted
    # occurrence numbers.
    
    my (%occs_by_id, %reids_by_id, %names_by_collection);
    my ($sql, $result);
    
    my $occ_string = join(',', map { $dbh->quote($_) } keys $occurrence_nos->%*);
    
    if ( $occ_string )
    {
	$sql = "SELECT * FROM $TABLE{OCCURRENCE_DATA} WHERE collection_no = any
	       (SELECT collection_no FROM $TABLE{OCCURRENCE_DATA}
		WHERE occurrence_no in ($occ_string))";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	foreach my $occ ( @$result )
	{
	    no warnings 'uninitialized';
	    
	    my $occurrence_no = $occ->{occurrence_no};
	    my $collection_no = $occ->{collection_no};
	    my $check_name = join('|', $occ->{genus_name}, $occ->{genus_reso},
				  $occ->{subgenus_name}, $occ->{subgenus_reso},
				  $occ->{species_name}, $occ->{species_reso},
				  $occ->{subspecies_name}, $occ->{subspecies_reso}, '#');

	    $edt->set_attr_key('occs', $occurrence_no, $occ) if $occurrence_no;
	    $edt->set_attr_2key('occ_names', $collection_no, $check_name, 1) if $collection_no;
	    $edt->set_attr_key('show_collection', $collection_no, 1) if $collection_no;
	}
    }
    
    my $reid_string = join(',', map { $dbh->quote($_) } keys $reid_nos->%*);
    
    if ( $reid_string )
    {
	$sql = "SELECT reid_no, occurrence_no, collection_no, reference_no FROM $TABLE{REID_DATA}
		WHERE reid_no in ($reid_string)";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	foreach my $reid ( @$result )
	{
	    my $reid_no = $reid->{reid_no};
	    $edt->set_attr_key('reids', $reid_no, $reid) if $reid_no;
	    my $collection_no = $reid->{collection_no};
	    $edt->set_attr_key('show_collection', $collection_no, 1) if $collection_no;
	}
    }
}


sub fetch_existing_names {

    my ($edt, $collection_no) = @_;
    
    # Make sure that we have a valid collection_no. We must stringify it, in case it is
    # an object representing an external identifier.
    
    return unless "$collection_no" > 0;
    
    # If we already have the occurrence taxonomic names for the specified collection,
    # return.
    
    return if $edt->get_attr_key('occ_names', "$collection_no" );
    
    # Otherwise, fetch them.
    
    my $dbh = $edt->dbh;
    
    my (%names_by_collection);
    my ($sql, $result);
    
    $sql = "SELECT genus_reso, genus_name, subgenus_reso, subgenus_name,
		       species_reso, species_name, subspecies_reso, subspecies_name
		FROM $TABLE{OCCURRENCE_DATA} WHERE collection_no = '$collection_no'";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    foreach my $occ ( @$result )
    {
	no warnings 'uninitialized';
	
	my $check_name = join('|', $occ->{genus_name}, $occ->{genus_reso},
			      $occ->{subgenus_name}, $occ->{subgenus_reso},
			      $occ->{species_name}, $occ->{species_reso},
			      $occ->{subspecies_name}, $occ->{subspecies_reso}, '#');
	
	$edt->set_attr_2key('occ_names', $collection_no, $check_name, 1);
    }
}


# initialize_occ_resos ( dbh )
#
# This method must be called before any EditTransactions involving occurrences are
# created. It reads the database definition of the fields 'genus_reso', 'subgenus_reso',
# 'species_reso', and 'subspecies_reso', and stores all of the valid modifiers.
# 
# IMPORTANT NOTE: this method assumes that the REID_DATA table accepts the same
# modifiers as the OCCURRENCE_DATA table. If you change one, you must also change the
# other.

sub initialize_occ_resos {

    my ($class, $dbh) = @_;
    
    foreach my $field ( qw(genus subgenus species subspecies) )
    {
	# Fetch the column definition from the database.
	
	my ($name, $definition) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{OCCURRENCE_DATA} like '${field}_reso'");
	
	# Extract the list of allowed modifiers. Skip the empty string, and also '"',
	# and 'informal'.
	
	my @modifier_list = grep { $_ && $_ ne 'informal' && $_ ne '"' } $definition =~ /'(.*?)'/g;
	
	$OCC_RESO{$field}{$_} = 1 foreach @modifier_list;
	
	my $regex_source = join('|', @modifier_list);
	$regex_source =~ s/([.?])/[$1]/g;

	if ( $field eq 'subgenus' )
	{
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s+([(].*)};
	}

	elsif ( $field eq 'subspecies' )
	{
	    $regex_source =~ s/(var|forma|morph|mut)/$1\[.\]?/g;
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s+(.*)};
	}
	
	else
	{
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s+(.*)};
	}
    }
}

1;
