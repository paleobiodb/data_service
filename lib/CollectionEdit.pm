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
use TableDefs qw(%TABLE set_table_property set_column_property);
use CoreTableDefs;
use IntervalBase qw(int_defined int_bounds);
use OccurrenceBase qw(parseIdentifiedName matchExactName matchIdentifiedName findParentTaxon
		      computeTaxonMatch hashIdentification);
use MatrixBase qw(updateCollectionMatrix updateOccurrenceMatrix
		  deleteFromCollectionMatrix deleteFromOccurrenceMatrix
		  deleteReidsFromOccurrenceMatrix deleteCollsFromOccurrenceMatrix
		  updateOccurrenceCounts);

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
       E_AMBIGUOUS => "Field 'identified_name' matches more than one taxon in the database",
       E_CANNOT_DELETE => { collection => "Cannot delete a collection that has associated specimens",
			    occurrence => "Cannot delete an occurrence that has associated speciments" },
       E_CANNOT_MOVE => { occurrence => "The value of 'collection_id' must match what is " .
			  "already stored in the record",
			  reid => "The value of 'occurrence_id' must match what is " .
			  "already stored in the record" });
    
    CollectionEdit->register_allowances('DUPLICATE');
    
    CollectionEdit->ignore_field('COLLECTION_DATA', 'reference_add');
    CollectionEdit->ignore_field('COLLECTION_DATA', 'reference_delete');
    CollectionEdit->ignore_field('COLLECTION_DATA', 'max_interval');
    CollectionEdit->ignore_field('COLLECTION_DATA', 'min_interval');
    CollectionEdit->ignore_field('OCCURRENCE_DATA', 'identified_name');
    CollectionEdit->ignore_field('REID_DATA', 'identified_name');
    CollectionEdit->ignore_field('OCCURRENCE_DATA', 'validation');
    CollectionEdit->ignore_field('REID_DATA', 'validation');
    
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
    set_column_property('OCCURRENCE_DATA', 'taxon_no', EXTID_TYPE => 'TID');
    
    set_table_property('REID_DATA', REQUIRED_COLS => ['reference_no']);
    
    set_column_property('REID_DATA', 'reid_no', EXTID_TYPE => 'REI');
    set_column_property('REID_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('REID_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('REID_DATA', 'reference_no', EXTID_TYPE => 'REF');
    set_column_property('REID_DATA', 'taxon_no', EXTID_TYPE => 'TID');
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
	deleteCollsFromOccurrenceMatrix($dbh, \@delete_colls, $debug_out);
	
	my $collection_list = join(',', map { $dbh->quote($_) } @delete_colls);
	
	my $sql = "UPDATE $TABLE{AUTHORITY_DATA} SET type_locality = null
		WHERE type_locality in ($collection_list)";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	my $result = $dbh->do($sql);
	
	$edt->debug_line("Updated $result authority records.\n") if $edt->debug_mode;
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
    
    my $set_type_locality = $edt->get_attr('set_type_locality');
    my $remove_type_locality = $edt->get_attr('remove_type_locality');

    if ( $set_type_locality || $remove_type_locality )
    {
	$edt->resolveTypeLocality();
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
    
    my $dbh = $edt->dbh;
    my $keyexpr = $action->keyexpr;
    my $coll_id = $action->keyval;
    
    # Validation for delete operations is entirely different from validation for
    # insert, replace, or update operations.
    
    if ( $operation eq 'delete' )
    {
	# If this is the type locality for any taxon, clear that. Delete any
	# occurrences and reidentifications associated with this collection.
	
	my $sql = "SELECT specimen_no FROM $TABLE{SPECIMEN_DATA} as sp
		join $TABLE{OCCURRENCE_DATA} as oc using (occurrence_no)
		WHERE $keyexpr";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	my ($check) = $dbh->selectrow_array($sql);
	
	if ( $check )
	{	
	    $edt->add_condition('E_CANNOT_DELETE', 'collection');
	}
	
	return;
    }
        
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
	
	$sql = "DELETE FROM $TABLE{OCCURRENCE_DATA} WHERE $keyexpr";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$result = $dbh->do($sql);
	
	$edt->debug_line("Deleted $result rows.\n") if $edt->debug_mode;
	
	$sql = "DELETE FROM $TABLE{REID_DATA} WHERE $keyexpr";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$result = $dbh->do($sql);
	
	$edt->debug_line("Deleted $result rows.\n") if $edt->debug_mode;
	
	$sql = "DELETE FROM $TABLE{COLLECTION_REFS} WHERE $keyexpr";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$result = $dbh->do($sql);
	
	$edt->debug_line("Deleted $result rows.\n") if $edt->debug_mode;
	
	return;
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
# valid 'collection_no' field. This is necessary for authorization, because those tables
# use the 'COLLECTION_DATA' table for authorization. Inserts into the REID_DATA table
# also require a valid 'occurrence_id' field.

sub initialize_occ_action {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # First, fetch any existing key values from the action record.
    
    my $collection_no = $action->record_value('collection_id');
    my $occurrence_no = $action->record_value('occurrence_id');
    my $reid_no = $action->record_value('reid_id');
    my ($check, $sql);
    
    my $occs = $edt->get_attr('occs');
    my $reids = $edt->get_attr('reids');
    
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
	
	elsif ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $action->set_attr('occurrence_no', "$occurrence_no");
	    $action->set_attr('collection_no', $occs->{"$occurrence_no"}{collection_no});
	}
	
	else
	{
	    $action->set_attr('reid_no', "$reid_no");
	    $action->set_attr('occurrence_no', $reids->{"$reid_no"}{occurrence_no});
	    $action->set_attr('collection_no', $reids->{"$reid_no"}{collection_no});
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
		$collection_no = $occs->{"$occurrence_no"}{collection_no};
		$action->set_attr('collection_no', $collection_no);
		$action->set_attr('occurrence_no', "$occurrence_no");
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
		$edt->add_condition('E_EXECUTE',
				    "could not locate reid_no, occurrence_no, or collection_no");
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
    
    my $collection_no = $action->get_attr('collection_no');
    my $occurrence_no = $action->get_attr('occurrence_no');
    my $reid_no = $action->get_attr('reid_no');
    
    # If the operation is 'update', 'replace', or 'delete', get a reference to the
    # existing data for this record.
    
    my $is_update = $operation eq 'update' || $operation eq 'replace';
    my $existing_data;
    
    if ( $is_update || $operation eq 'delete' )
    {
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    if ( $occurrence_no && $occs->{$occurrence_no} )
	    {
		$existing_data = $occs->{$occurrence_no};
	    }
	    
	    else
	    {
		$edt->add_condition('E_EXECUTE',
				    "could not retrieve occurrence data for '$occurrence_no' during validation");
		return;
	    }
	}

	else
	{
	    if ( $reid_no && $reids->{$reid_no} )
	    {
		$existing_data = $reids->{$reid_no};
	    }
	    
	    else
	    {
		$edt->add_condition('E_EXECUTE',
				    "could not retrieve reidentification data for '$reid_no' during validation");
		return;
	    }
	}
    }
    
    # If the operation is 'delete', call the appropriate routine and return. We don't
    # need any of the checks below in this case.
    
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
	
	# If the row to be deleted contains an 'n. gen.', 'n. sp.', etc. modifier,
	# then we may need to remove one or more type locality links.
	
	no warnings 'uninitialized';
	
	if ( $existing_data->{genus_reso} eq 'n. gen.' ||
	     $existing_data->{subgenus_reso} eq 'n. subgen.' ||
	     $existing_data->{species_reso} eq 'n. sp.' ||
	     $existing_data->{subspecies_reso} eq 'n. ssp.' )
	{
	    $edt->check_remove_type_locality($action, $existing_data->{taxon_no}, $collection_no,
		      $existing_data->{genus_name}, $existing_data->{genus_reso},
		      $existing_data->{subgenus_name}, $existing_data->{subgenus_reso},
		      $existing_data->{species_name}, $existing_data->{species_reso},
		      $existing_data->{subspecies_name}, $existing_data->{subspecies_reso});
	}
    }
    
    
    # Validate reference_id
    # ---------------------
    
    # This field is required and must match an existing reference.
    
    my $reference_no = $action->record_value('reference_no') ||
	$action->record_value('reference_id');
    
    if ( defined $reference_no && $reference_no ne '' )
    {
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
				    "Unknown reference $qref");
	    }
	}
	
	# If this is an occurrence update or replacement, check if any of the
	# reidentifications already use this reference.
	
	if ( $table_specifier eq 'OCCURRENCE_DATA' && $is_update )
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
					"A reidentification of this occurrence already uses " .
					"this reference");
		}
	    }
	}
	
	# If this is a reidentification action, check if the occurrence or any of the
	# other reidentifications already use this reference.
	
	elsif ( $table_specifier eq 'REID_DATA' )
	{
	    my $qocc = $dbh->quote($occurrence_no);
	    my $qreid = $dbh->quote($reid_no);
	    
	    $sql = "SELECT reference_no FROM $TABLE{OCCURRENCE_DATA}
		WHERE reference_no = $qref and occurrence_no = $qocc
		UNION SELECT reference_no FROM $TABLE{REID_DATA}
		WHERE reference_no = $qref and occurrence_no = $qocc and
			reid_no <> $qreid";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my ($check) = $dbh->selectrow_array($sql);
	    
	    if ( $check )
	    {
		$edt->add_condition('E_BAD_VALUE', 'reference_id',
				    "This occurrence or a reidentification already uses " .
				    "this reference");
	    }
	}
    }
    
    # Validate the taxonomic name and taxon number
    # --------------------------------------------
    
    # The 'identified_name' field will be deconstructed and used to set the fields
    # 'genus_name', 'genus_reso', etc. It must not be specified with an empty value.
    
    my $identified_name = $action->record_value('identified_name');
    my $taxon_no = $action->record_value('taxon_id');
    my $collection_no = $action->get_attr('collection_no');
    
    if ( defined $identified_name && $identified_name ne '' )
    {
	# Validate the specified name and associated taxon number if any. We need the
	# collection_no so we can verify that no other occurrence associated with this
	# collection has the same name.
	
	$edt->validate_identification($action, $table_specifier, $collection_no,
				      $identified_name, $taxon_no);
    }
    
    elsif ( $operation eq 'insert' || $action->field_specified('identified_name') )
    {
	$edt->add_condition('E_REQUIRED', 'identified_name');
    }
    
    # If the name isn't being updated but the taxon_no is, validate it according to
    # the name components stored in the database row.
    
    elsif ( $taxon_no && $is_update )
    {
	my $genus_name = $existing_data->{genus_name};
	my $genus_reso = $existing_data->{genus_reso};
	my $subgenus_name = $existing_data->{subgenus_name};
	my $subgenus_reso = $existing_data->{subgenus_reso};
	my $species_name = $existing_data->{species_name};
	my $species_reso = $existing_data->{species_reso};
	my $subspecies_name = $existing_data->{subspecies_name};
	my $subspecies_reso = $existing_data->{subspecies_reso};
	
	$edt->validate_taxon_no($action, "$taxon_no", $collection_no,
				$genus_name || '', $genus_reso,
				$subgenus_name || '', $subgenus_reso,
				$species_name || '', $species_reso,
				$subspecies_name || '', $subspecies_reso);
    }

    # If neither the name nor the taxon_no is being updated but _set_locality is
    # specified, do that.
    
    elsif ( $action->record_value('_set_locality') )
    {
	my $genus_name = $existing_data->{genus_name};
	my $genus_reso = $existing_data->{genus_reso};
	my $subgenus_name = $existing_data->{subgenus_name};
	my $subgenus_reso = $existing_data->{subgenus_reso};
	my $species_name = $existing_data->{species_name};
	my $species_reso = $existing_data->{species_reso};
	my $subspecies_name = $existing_data->{subspecies_name};
	my $subspecies_reso = $existing_data->{subspecies_reso};
	my $taxon_no = $existing_data->{taxon_no};
	
	$edt->check_type_locality($action, $taxon_no, $collection_no, 1,
				  $genus_name || '', $genus_reso,
				  $subgenus_name || '', $subgenus_reso,
				  $species_name || '', $species_reso,
				  $subspecies_name || '', $subspecies_reso);
    }
    
    # If new name components are being set and there is an existing name which is
    # different, then we need to check whether to unlink the old name's type
    # localit(ies).

    my $new_name = $action->record_value('identified_name');
    my $new_taxon = $action->record_value('taxon_id');
    
    if ( $is_update && $existing_data->{taxon_no} &&
	 ($new_name || $new_taxon && "$new_taxon" ne $existing_data->{taxon_no}) )
    {
	$edt->check_remove_type_locality($action, $existing_data->{taxon_no}, $collection_no,
		      $existing_data->{genus_name}, $existing_data->{genus_reso},
		      $existing_data->{subgenus_name}, $existing_data->{subgenus_reso},
		      $existing_data->{species_name}, $existing_data->{species_reso},
		      $existing_data->{subspecies_name}, $existing_data->{subspecies_reso});
	
	# my $old_gen = $existing_data->{genus_reso};
	# my $old_subgen = $existing_data->{subgenus_reso};
	# my $old_sp = $existing_data->{species_reso};
	# my $old_subsp = $existing_data->{subspecies_reso};
	
	# my $new_gen = $action->record_value('genus_reso');
	# my $new_subgen = $action->record_value('subgenus_reso');
	# my $new_sp = $action->record_value('species_reso');
	# my $new_subsp = $action->record_value('subspecies_reso');
    }
    
    # Validate the abundance
    # ----------------------
    
    # If an abundance value is specified, an abundance unit must also be specified.
    # Otherwise, the abundance unit is ignored.
    
    my $abund_value = $action->record_value('abund_value');
    my $abund_unit = $action->record_value('abund_unit');
    my ($existing_value, $existing_unit);
    
    if ( $operation =~ /^upd|^rep/ )
    {
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $existing_value = $occs->{$occurrence_no}{abund_value};
	    $existing_unit = $occs->{$occurrence_no}{abund_unit};
	}
	
	else
	{
	    $existing_value = $reids->{$occurrence_no}{abund_value};
	    $existing_unit = $reids->{$occurrence_no}{abund_unit};
	}
    }
    
    if ( $abund_value )
    {
	unless ( $abund_unit || $existing_unit )
	{
	    $edt->add_condition('E_REQUIRED', 'abund_unit');
	}
	
	elsif ( !$abund_unit && $action->field_specified('abund_unit') )
	{
	    $edt->add_condition('E_REQUIRED', 'abund_unit');
	}
    }
    
    elsif ( $action->field_specified('abund_value') )
    {
	$action->set_record_value('abund_unit', undef);
    }
    
    elsif ( $abund_unit )
    {
	unless ( $abund_value || $existing_value )
	{
	    $action->ignore_field('abund_unit');
	}
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
	    $action->set_record_value('plant_organ2', undef) if $table_specifier ne 'REID_DATA';
	}
    }

    elsif ( $action->field_specified('plant_organ') )
    {
	$action->set_record_value('plant_organ', undef);
	$action->set_record_value('plant_organ2', undef) if $table_specifier ne 'REID_DATA';
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
    
    # # Mark each occurrence for deletion.
    
    # foreach my $k ( $action->keyvals )
    # {
    # 	$edt->set_attr_key('delete_occs', $k, 1);
    # }

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
	if ( ! $edt->get_attr_key('delete_occs', $occurrence_no) )
	{
	    $edt->set_attr_key('recompute_occs', $occurrence_no, 1);
	    $edt->set_attr_key('update_occs', $occurrence_no, 1);
	}
    }
}


sub validate_identification {

    my ($edt, $action, $table_specifier, $collection_no, $identified_name, $taxon_no) = @_;
    
    # Parse the name and get either the name components or a hashref indicating an error
    # message.
    
    my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) =
	    parseIdentifiedName($identified_name);

    if ( ref $genus_name )
    {
	$edt->add_condition('E_FORMAT', 'identified_name', $genus_name->{error} || 'Invalid name');
	return;
    }
    
    # If we are inserting or updating an occurrence, the identified name must not
    # duplicate the identified name of any other occurrence in this collection.
    # Reidentifications are allowed to be duplicates, however.
    
    if ( $table_specifier eq 'OCCURRENCE_DATA' )
    {
	no warnings 'uninitialized';
	
	my $name_check = join('|', $genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
			      $species_name, $species_reso, $subspecies_name, $subspecies_reso, '#');
	
	if ( $edt->get_attr_2key('occ_names', $collection_no, $name_check) )
	{
	    $edt->add_condition('E_BAD_VALUE', 'identified_name',
				"duplicates another occurrence in this collection");
	    return;
	}
	
	else
	{
	    $edt->set_attr_2key('occ_names', $collection_no, $name_check, 1);
	}
    }
    
    # Store the individual name components in the occurrence or reidentification record.
    
    $action->set_record_value('genus_name', $genus_name);
    $action->set_record_value('genus_reso', $genus_reso);
    $action->set_record_value('subgenus_name', $subgenus_name);
    $action->set_record_value('subgenus_reso', $subgenus_reso);
    $action->set_record_value('species_name', $species_name);
    $action->set_record_value('species_reso', $species_reso);
    $action->set_record_value('subspecies_name', $subspecies_name);
    $action->set_record_value('subspecies_reso', $subspecies_reso);
    
    # Now validate the taxon_no (if one was submitted) against the name components, or
    # else do a database query to figure out what the taxon_no must be.
    
    my $new_taxon_no = $taxon_no ? "$taxon_no" : '';
    
    $edt->validate_taxon_no($action, $new_taxon_no, $collection_no,
			    $genus_name || '', $genus_reso,
			    $subgenus_name || '', $subgenus_reso,
			    $species_name || '', $species_reso,
			    $subspecies_name || '', $subspecies_reso);
}


# validate_taxon_no ( genus_name, genus_reso, ... )
#
# Put the taxon name back together from its individual components without modifiers, and
# link it up to its corresponding authority record if the name has already been entered
# into the database.

sub validate_taxon_no {

    my ($edt, $action, $new_taxon_no, $collection_no,
	$genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) = @_;
    
    # An informal genus name means the new taxon_no must be 0.
    
    if ( $genus_reso eq 'informal' )
    {
	$action->set_record_value('taxon_id', 0);
	return;
    }
    
    # Otherwise, if we were provided with both a taxon_no value and a validation hash
    # value, see if they match up. If so, accept the submitted taxon_no value.

    my $validation = $action->record_value('validation');
    
    if ( defined $new_taxon_no && $new_taxon_no ne '' &&
	 defined $validation && $validation ne '' )
    {
	my $validation_name = join('|', $genus_name, $subgenus_name,
				   $species_name, $subspecies_name);
	
	my $check = hashIdentification($validation_name, $new_taxon_no);

	if ( $check eq $validation )
	{
	    $edt->debug_line("VALIDATION ACCEPTED\n") if $edt->debug_mode;
	    
	    # If the submitted taxon_no value is not 0, check to see if we need to link
	    # up the type locality.
	    
	    if ( $new_taxon_no ne "0" )
	    {
		my $override = $action->record_value("_set_locality");
		
		$edt->check_type_locality($action, $new_taxon_no, $collection_no, $override,
					  $genus_name, $genus_reso,
					  $subgenus_name, $subgenus_reso,
					  $species_name, $species_reso,
					  $subspecies_name, $subspecies_reso);
	    }
	    
	    return;
	}
    }
    
    # Otherwise, check to see which taxa (if any) from the authorities table match the
    # submitted name.

    if ( $subgenus_reso && $subgenus_reso eq 'informal' )
    {
	$subgenus_name = '';
    }
    
    if ( $species_reso && $species_reso eq 'informal' ||
	 $species_name && $species_name =~ /[.?]$/ )
    {
	$species_name = '';
	$subspecies_name = '';
    }
    
    elsif ( $subspecies_reso && $subspecies_reso eq 'informal' ||
	    $subspecies_name && $subspecies_name =~ /[.]$/ )
    {
	$subspecies_name = '';
    }
    
    my $dbh = $edt->dbh;
    my $debug_out = $edt->debug_mode ? $edt : undef;
    my @matches;
    
    # If this is a new name, look for the best possible exact match in the authorities
    # table.
    
    if ( $genus_reso eq 'n. gen.' || $subgenus_reso eq 'n. subgen.' ||
	 $species_reso eq 'n. sp.' || $subspecies_reso eq 'n. ssp.' )
    {
	@matches = matchExactName($dbh, $debug_out, $genus_name, $subgenus_name,
				  $species_name, $subspecies_name);
    }
    
    # Otherwise, do a complex query on the authorities table with somewhat
    # looser criteria. This query will, for example, match the species name
    # in a different subgenus, or with the subgenus as genus, etc.
    
    else
    {	
	@matches = matchIdentifiedName($dbh, $debug_out, $genus_name, $subgenus_name,
				       $species_name, $subspecies_name);
    }
    
    # If the submitted taxon_no value is equal to one of the matches, accept it. Otherwise, add
    # an error condition.
    
    if ( defined $new_taxon_no && $new_taxon_no ne '' )
    {
	unless ( any { $_ eq $new_taxon_no } @matches )
	{
	    $edt->add_condition('E_BAD_VALUE', 'taxon_id', "does not match the identified name");
	    return;
	}
    }
    
    # If no taxon_id was submitted and there is more than one match, add an error
    # condition.
    
    elsif ( @matches > 1 )
    {
	$edt->add_condition('E_AMBIGUOUS', 'identified_name');
	return;
    }
    
    # Otherwise, accept the single match as the taxon_id value.
    
    else
    {
	$new_taxon_no = $matches[0];
	$action->set_record_value('taxon_id', $new_taxon_no);
    }

    # If the new taxon_no value is not 0, check to see if we need to link up the type
    # locality.
    
    if ( defined $new_taxon_no && $new_taxon_no ne "0" )
    {
	my $override = $action->record_value("_set_locality");
		
	$edt->check_type_locality($action, $new_taxon_no, $collection_no, $override,
				  $genus_name, $genus_reso,
				  $subgenus_name, $subgenus_reso,
				  $species_name, $species_reso,
				  $subspecies_name, $subspecies_reso);
    }
}


# check_type_locality ( action, new_taxon_no, collection_no, override, genus_name .... )
#
# If the specified name components include the modifiers 'n. gen.', 'n. sp.', etc.,
# check to see if we are able to link the matching taxonomic names to this collection as
# the type locality.

sub check_type_locality {

    my ($edt, $action, $new_taxon_no, $collection_no, $override,
	$genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) = @_;
    
    # If the species or subspecies is marked as new, check that $new_taxon_no refers to a
    # taxon of equal rank. If so, add it to the 'set_type_locality' hash. 
    
    my $expected_genus = $genus_name;
    $expected_genus .= " ($subgenus_name)" if $subgenus_name && $subgenus_reso ne 'informal';
    
    my $expected_species = $expected_genus;
    $expected_species .= " $species_name" if $species_name;
    $expected_species .= " $subspecies_name" if $subspecies_name;
    
    if ( $subspecies_reso eq 'n. ssp.' )
    {
	if ( $edt->check_taxon_match($new_taxon_no, 'subspecies', $expected_species) )
	{
	    $action->set_attr_key('set_type_locality', $new_taxon_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $new_taxon_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY subsp = $new_taxon_no\n") if $edt->debug_mode;
	}
    }
    
    elsif ( $species_reso eq 'n. sp.' )
    {
	if ( $edt->check_taxon_match($new_taxon_no, 'species', $expected_species) )
	{
	    $action->set_attr_key('set_type_locality', $new_taxon_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $new_taxon_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY sp = $new_taxon_no\n") if $edt->debug_mode;
	}
    }
    
    if ( $subgenus_reso eq 'n. subgen.' )
    {
	my $dbh = $edt->dbh;
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	if ( $edt->check_taxon_match($new_taxon_no, 'subgenus', "$genus_name ($subgenus_name)") )
	{
	    $action->set_attr_key('set_type_locality', $new_taxon_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $new_taxon_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY subgen = $new_taxon_no\n") if $edt->debug_mode;
	}
	
	elsif ( my $parent_no = findParentTaxon($dbh, $debug_out, $new_taxon_no,
						"$genus_name ($subgenus_name)") )
	{
	    $action->set_attr_key('set_type_locality', $parent_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $parent_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY subgen = $parent_no\n") if $edt->debug_mode;
	}
    }
    
    if ( $genus_reso eq 'n. gen.' )
    {
	my $dbh = $edt->dbh;
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	if ( $edt->check_taxon_match($new_taxon_no, 'genus', $genus_name) )
	{
	    $action->set_attr_key('set_type_locality', $new_taxon_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $new_taxon_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY genus = $new_taxon_no\n") if $edt->debug_mode;
	}
	
	elsif ( my $parent_no = findParentTaxon($dbh, $debug_out, $new_taxon_no, $genus_name) )
	{
	    $action->set_attr_key('set_type_locality', $parent_no, $collection_no);
	    $action->set_attr_key('override_type_locality', $parent_no, 1) if $override;
	    $edt->debug_line("SET TYPE LOCALITY genus = $parent_no\n") if $edt->debug_mode;
	}
	
	# else
	# {
	#     my $dbh = $edt->dbh;
	#     my $debug_out = $edt->debug_mode ? $edt : undef;
	    
	#     my (@matches) = matchOrigName($dbh, $debug_out, $genus_name);
	    
	#     if ( @matches == 1 )
	#     {
	# 	$action->set_attr_key('set_type_locality', $matches[0], $collection_no);
	# 	$action->set_attr_key('override_type_locality', $matches[0], 1) if $override;
	# 	$edt->debug_line("SET TYPE LOCALITY gen = $matches[0]\n") if $edt->debug_mode;
	#     }
	# }
    }
}


# check_remove_type_locality ( edt, existing_taxon_no, collection_no, genus_name .... )
#
# If the specified name components include the modifiers 'n. gen.', 'n. sp.', etc.,
# check to see if we are able to unlink the matching taxonomic names from this collection as
# the type locality.

sub check_remove_type_locality {

    my ($edt, $action, $existing_taxon_no, $collection_no,
	$genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) = @_;
    
    # If the species or subspecies is marked as new, check that $existing_taxon_no refers to a
    # taxon of equal rank. If so, add it to the 'remove_type_locality' hash. 
        
    if ( $subspecies_reso eq 'n. ssp.' )
    {
	if ( $edt->check_locality_link($existing_taxon_no, 'subspecies', $collection_no) )
	{
	    $action->set_attr_key('remove_type_locality', $existing_taxon_no, $collection_no);
	    $edt->debug_line("REMOVE TYPE LOCALITY subsp = $existing_taxon_no\n") if $edt->debug_mode;
	}
    }
    
    elsif ( $species_reso eq 'n. sp.' )
    {
	if ( $edt->check_locality_link($existing_taxon_no, 'species', $collection_no) )
	{
	    $action->set_attr_key('remove_type_locality', $existing_taxon_no, $collection_no);
	    $edt->debug_line("REMOVE TYPE LOCALITY sp = $existing_taxon_no\n") if $edt->debug_mode;
	}
    }
    
    if ( $subgenus_reso eq 'n. subgen.' )
    {
	my $dbh = $edt->dbh;
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	if ( $edt->check_locality_link($existing_taxon_no, 'subgenus', $collection_no) )
	{
	    $action->set_attr_key('remove_type_locality', $existing_taxon_no, $collection_no);
	    $edt->debug_line("REMOVE TYPE LOCALITY subgen = $existing_taxon_no\n") if $edt->debug_mode;
	}
	
	elsif ( my $parent_no = findParentTaxon($dbh, $debug_out, $existing_taxon_no,
						"$genus_name ($subgenus_name)") )
	{
	    if ( $edt->check_locality_link($parent_no, 'subgenus', $collection_no) )
	    {
		$action->set_attr_key('remove_type_locality', $parent_no, $collection_no);
		$edt->debug_line("REMOVE TYPE LOCALITY subgen = $parent_no\n")
		    if $edt->debug_mode;
	    }
	}
    }
    
    if ( $genus_reso eq 'n. gen.' )
    {
	my $dbh = $edt->dbh;
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	if ( $edt->check_locality_link($existing_taxon_no, 'genus', $collection_no) )
	{
	    $action->set_attr_key('remove_type_locality', $existing_taxon_no, $collection_no);
	    $edt->debug_line("REMOVE TYPE LOCALITY gen = $existing_taxon_no\n") if $edt->debug_mode;
	}
	
	elsif ( my $parent_no = findParentTaxon($dbh, $debug_out, $existing_taxon_no, $genus_name) )
	{
	    if ( $edt->check_locality_link($parent_no, 'genus', $collection_no) )
	    {
		$action->set_attr_key('remove_type_locality', $parent_no, $collection_no);
		$edt->debug_line("REMOVE TYPE LOCALITY genus = $parent_no\n")
		    if $edt->debug_mode;
	    }
	}
    }
    
    # 	{
    # 	    my $dbh = $edt->dbh;
    # 	    my $debug_out = $edt->debug_mode ? $edt : undef;
	    
    # 	    my (@matches) = matchOrigName($dbh, $debug_out, $genus_name);
	    
    # 	    foreach my $orig_no ( @matches )
    # 	    {
    # 		if ( $edt->check_locality_link($orig_no, 'genus', $collection_no) )
    # 		{
    # 		    $action->set_attr_key('remove_type_locality', $matches[0], $collection_no);
    # 		    $edt->debug_line("REMOVE TYPE LOCALITY gen = $matches[0]\n") if $edt->debug_mode;
    # 		}
    # 	    }
    # 	}
    # }
}


sub check_taxon_match {
    
    my ($edt, $taxon_no, $expected_rank, $expected_name) = @_;
    
    my $dbh = $edt->dbh;
    my $sql = "SELECT taxon_rank, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_no = '$taxon_no'";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ($taxon_rank, $taxon_name) = $dbh->selectrow_array($sql);

    return ($taxon_rank eq $expected_rank && $taxon_name eq $expected_name);
}


sub check_locality_link {

    my ($edt, $taxon_no, $rank, $collection_no) = @_;
    
    my $dbh = $edt->dbh;
    my $sql = "SELECT type_locality, taxon_rank FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_no = '$taxon_no' and type_locality = '$collection_no'";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ($check_locality, $check_rank) = $dbh->selectrow_array($sql);

    if ( $check_rank eq $rank && $check_locality eq $collection_no)
    {
	return 1;
    }

    else
    {
	return '';
    }
}


sub after_occ_action {
    
    my ($edt, $action, $operation, $table_specifier, $new_keyval) = @_;
    
    my @keyvals;
    
    # For an 'insert' operation, the new key value is provided as an argument to
    # this method.
    
    if ( $operation eq 'insert' )
    {
	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->set_attr_key('update_occs', $new_keyval, 1);
	}
	
	elsif ( my $occurrence_no = $action->get_attr('occurrence_no') )
	{
	    $edt->set_attr_key('update_occs', $occurrence_no, 1);
	    $edt->set_attr_key('recompute_occs', $occurrence_no, 1);
	}
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
    }
    
    else
    {
	@keyvals = $action->keyvals;

	if ( $table_specifier eq 'OCCURRENCE_DATA' )
	{
	    $edt->set_attr_key('update_occs', $_, 1) foreach @keyvals;
	}
	
	elsif ( my $occurrence_no = $action->get_attr('occurrence_no') )
	{
	    $edt->set_attr_key('update_occs', $occurrence_no, 1);
	}
    }
    
    # If the action would affect type locality links, add this info as a transaction
    # attribute. We do this here because we know the action went through.
    
    $edt->save_action_attr($action, 'set_type_locality');
    $edt->save_action_attr($action, 'override_type_locality');
    $edt->save_action_attr($action, 'remove_type_locality');
}


sub save_action_attr {

    my ($edt, $action, $attr) = @_;
    
    my $attr_value = $action->get_attr($attr);
    
    if ( $attr_value && ref $attr_value eq 'HASH' )
    {
	foreach my $k ( keys $attr_value->%* )
	{
	    $edt->set_attr_key($attr, $k, $attr_value->{$k});
	}
    }

    else
    {
	$edt->set_attr($attr, $attr_value);
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
		SET o.reid_no = if(re.reid_no is null, 0, re.reid_no), o.modified = o.modified
		WHERE occurrence_no = $quoted
		ORDER BY pubyr desc, re.reid_no desc LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	$dbh->do($sql);
	
	$sql = "UPDATE $TABLE{OCCURRENCE_DATA} as o
		    join $TABLE{REID_DATA} as re using (occurrence_no)
		SET re.most_recent = if(o.reid_no = re.reid_no, 'YES', 'NO'), re.modified = re.modified
		WHERE occurrence_no = $quoted";

	$edt->debug_line("$sql\n") if $edt->debug_mode;

	$dbh->do($sql);
    }
}


# resolveTypeLocality ( )
#
# Add and remove type localities based on the transaction attributes
# 'set_type_locality', 'remove_type_locality', and 'override_type_locality'.

sub resolveTypeLocality {
    
    my ($edt) = @_;

    my $dbh = $edt->dbh;
    my $sql;
    
    # Get these three attribute values. 
    
    my $set_type_locality = $edt->get_attr('set_type_locality') || { };
    my $override_type_locality = $edt->get_attr('override_type_locality') || { };
    my $remove_type_locality = $edt->get_attr('remove_type_locality') || { };
    
    # Remove the type locality from all of the specified taxon_nos, unless it would be
    # set again.
    
    foreach my $taxon_no ( keys $remove_type_locality->%* )
    {
	unless ( $set_type_locality->{$taxon_no} &&
		 $set_type_locality->{$taxon_no} eq $remove_type_locality->{$taxon_no} )
	{
	    my $collection_no = $remove_type_locality->{$taxon_no};
	    
	    $sql = "UPDATE $TABLE{AUTHORITY_DATA} as a
			join $TABLE{AUTHORITY_DATA} as base using (orig_no)
		SET a.type_locality = null, a.modified = a.modified
		WHERE base.taxon_no = '$taxon_no' and a.type_locality = '$collection_no'";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my $result = $dbh->do($sql);
	    
	    $edt->debug_line("UPDATED $result ROW\n") if $result && $edt->debug_mode;
	}
    }
    
    # Add the type locality from all of the specified taxon_nos. Override those which
    # are also keys in the 'override_type_locality' hash.

    foreach my $taxon_no ( keys $set_type_locality->%* )
    {
	my $collection_no = $set_type_locality->{$taxon_no};
	my $override = $override_type_locality->{$taxon_no};

	unless ( $override )
	{
	    $sql = "SELECT a.type_locality FROM $TABLE{AUTHORITY_DATA} as a
			join $TABLE{AUTHORITY_DATA} as base using (orig_no)
		WHERE base.taxon_no = '$taxon_no'";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my ($existing_locality) = $dbh->selectrow_array($sql);
	    
	    if ( $existing_locality && $existing_locality ne $collection_no )
	    {
		next;
	    }
	}
	
	$sql = "UPDATE $TABLE{AUTHORITY_DATA} as a
			join $TABLE{AUTHORITY_DATA} as base using (orig_no)
		SET a.type_locality = '$collection_no', a.modified = a.modified
		WHERE base.taxon_no = '$taxon_no'";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	my $result = $dbh->do($sql);
	
	$edt->debug_line("UPDATED $result ROW\n") if $result && $edt->debug_mode;
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
	$sql = "SELECT * FROM $TABLE{REID_DATA}
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
