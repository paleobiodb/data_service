# 
# The Paleobiology Database
# 
#   SpecimenEdit.pm - a class for handling updates to fossil specimen records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting fossil specimen records.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = SpecimenEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record(), etc.


package SpecimenEdit;

use strict;

use Carp qw(carp croak);

use TableDefs qw(%TABLE get_table_property);
use CoreTableDefs;
use ExternalIdent qw(%IDRE);

use Taxonomy;

use base 'EditTransaction';

use namespace::clean;



{
    SpecimenEdit->register_conditions(
	C_UNKNOWN_TAXON => "The specified taxon name is not in the database. Add 'allow=UNKNOWN_TAXON' to continue.",
	E_MISSING_IDENTIFICATION => "A specimen record requires either 'taxon_name', 'taxon_id' or 'occurrence_id'.",
	E_UNREGISTERED_TAXON => "A specimen record requires either 'taxon_id' or a 'taxon_name' that is known to the database, unless it is tied to an existing collection or occurrence.",
	E_CANNOT_CHANGE => "The field '%1' cannot be modified once created. Delete the specimen and add another.");
    
    SpecimenEdit->register_allowances('UNKNOWN_TAXON');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action )

# This method is called from EditTransaction.pm to validate each insert and update action.
# We override it to do additional checks before calling validate_against_schema.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;

    # call the appropriate routine to validate this action.

    if ( $table eq 'SPECIMEN_DATA' )
    {
	return $edt->validate_specimen($action, $operation, $keyexpr);
    }

    elsif ( $table eq 'MEASUREMENT_DATA' )
    {
	return $edt->validate_measurement($action, $operation, $keyexpr);
    }

    else
    {
	croak "invalid table '$table'";
    }
}


# Validate the specimen record that has been submitted for insertion or update.

sub validate_specimen {
    
    my ($edt, $action, $operation, $keyexpr) = @_;
    
    my $record = $action->record;
    my $dbh = $edt->dbh;
    
    # If we have a taxon name rather than a taxon id, look it up and see if it is already in the database.
    
    if ( $record->{taxon_name} )
    {
	if ( $record->{taxon_id} )
	{
	    $edt->add_condition('E_PARAM', "you may not specify both 'taxon_name' and 'taxon_id' in the same record");
	}
	
	else
	{
	    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
	    
	    my ($result) = $taxonomy->resolve_names($record->{taxon_name}, { fields => 'SEARCH' });
	    
	    if ( $result && $result->{orig_no} )
	    {
		$record->{taxon_id} = $result->{orig_no} + 0;
		$edt->debug_line("taxon_no: $record->{taxon_id}");
	    }
	}
    }
    
    # Now we need to make sure that any inserted record has a taxonomic identification.
    
    if ( $operation eq 'insert' )
    {
	# If we have a taxon_id then we are okay. But not if occurrence_id was also specified.
	
	if ( $record->{taxon_id} )
	{
	    $edt->add_condition($action, 'E_PARAM', "you may not specify both 'taxon_id' and 'occurrence_id' in the same record")
		if $record->{occurrence_id};
	}
	
	# If we have an occurrence_id then we are okay. But not if taxon_name was also specified.
	
	elsif ( $record->{occurrence_id} )
	{
	    $edt->add_condition($action, 'E_PARAM', "you may not specify both 'taxon_name' and 'occurrence_id' in the same record")
		if $record->{taxon_name};
	    
	    # Add another error condition if both 'collection_id' and 'occurrence_id' are specified.
	    
	    $edt->add_condition($action, 'E_PARAM', "you may not specify both 'collection_id' and 'occurrence_id' in the same record")
		if $record->{collection_id};
	}
	
	# If we have a collection_id and a taxon_name, then we are okay as long as 'UNKNOWN_TAXON' is
	# allowed.
	
	elsif ( $record->{collection_id} && $record->{taxon_name} )
	{
	    $edt->add_condition($action, 'C_UNKNOWN_TAXON', 'taxon_name') unless $edt->allows('UNKNOWN_TAXON');
	}
	
	# If we have just a taxon name but no collection_id, then we have an UNREGISTERED_TAXON error.
	
	elsif ( $record->{taxon_name} )
	{
	    $edt->add_condition($action, 'E_UNREGISTERED_TAXON');
	}

	# Otherwise, we don't have any taxonomic identification at all.

	else
	{
	    $edt->add_condition($action, 'E_MISSING_IDENTIFICATION');
	}
    }
    
    # If we are updating a specimen, we cannot change either the collection_id or occurrence_id.
    
    elsif ( $operation eq 'update' )
    {
	if ( exists $record->{occurrence_id} || exists $record->{collection_id} )
	{
	    my ($occurrence_no) = $edt->get_old_values('SPECIMEN_DATA', $keyexpr, 'occurrence_no');
	    
	    print STDERR "UPDATE occ = $occurrence_no / $record->{occurrence_id}\n" if $edt->debug;
	    
	    # If a new occurrence_id is specified and there is an existing one, add an error condition.
	    
	    my $new = ($record->{occurrence_id} + '') || 0;
	    my $old = $occurrence_no || 0;
	    
	    if ( $new ne $old )
	    {
		$edt->add_condition($action, 'E_CANNOT_CHANGE', 'occurence_id');
	    }
	
	    if ( $occurrence_no )
	    {
		my ($collection_no) = $edt->dbh->selectrow_array("
		SELECT collection_no FROM $TABLE{OCCURRENCE_DATA}
		WHERE occurrence_no = $occurrence_no");
		
		print STDERR "UPDATE coll = $collection_no / $record->{collection_id}\n" if $edt->debug;
		
		my $new = ($record->{collection_id} + '') || 0;
		my $old = $collection_no || 0;
		
		if ( $new ne $old )
		{
		    $edt->add_condition($action, 'E_CANNOT_CHANGE', 'collection_id');
		}
	    }
	}
	
	# $$$ need to check old values and make sure no more than one of taxon, occ, coll are
	# specified.
    }
    
    # If we do have a collection_id, check to make sure it exists. Also check to see if that
    # collection already has an occurrence of that taxon. If not, we will need to create one.

    if ( $record->{collection_id} && $operation ne 'delete' )
    {
	$edt->validate_collection($action, $operation, $record);
    }
    
    # $$$ IMPORTANT!!!
    #
    # need to move creation and deletion of occurrences to before_action and after_action do we
    # need to allow modification of taxon? probably yes. This will involve possibly both a
    # deletion and a creation of an occurrence.
}


# If the specimen record is tied to an existing collection, we need to check that it exists and
# whether or not there is already an occurrence of the specified taxon in this collection. If not,
# one must be created.

sub validate_collection {
    
    my ($edt, $action, $operation, $record) = @_;
    
    # First make sure that the specified collection exists.
    
    my $collection_no;
    
    if ( ref $record->{collection_id} eq 'PBDB::ExtIdent' )
    {
	$collection_no = $record->{collection_id} + 0;
    }
    
    elsif ( $record->{collection_id} =~ qr{ ^ \d+ $ }xs )
    {
	$collection_no = $record->{collection_id};
    }
    
    elsif ( $record->{collection_id} =~ $IDRE{COL} )
    {
	$collection_no = $2;
    }
    
    unless ( $collection_no && $collection_no > 0 )
    {
	$edt->add_condition($action, 'E_RANGE', 'collection_id', "value does not specify a valid record");
	return;
    }
    
    unless ( $edt->check_key('COLLECTION_DATA', 'collection_no', $collection_no) )
    {
	$edt->add_condition($action, 'E_KEY_NOT_FOUND', 'collection_id', $collection_no);
	return;
    }
    
    my ($sql, $result, $insert_fields, $insert_values);
    my $dbh = $edt->dbh;

    # Then check for an occurrence of the taxon associated with this specimen in the collection.
    
    if ( my $taxon_no = $record->{taxon_id} + 0 )
    {
	$sql = "SELECT occurrence_no FROM $TABLE{OCCURRENCE_DATA} as oc
			join $TABLE{AUTHORITY_DATA} as a1 using (taxon_no)
			join $TABLE{AUTHORITY_DATA} as au on a1.orig_no = au.orig_no
		WHERE oc.collection_no = $collection_no and au.taxon_no = $taxon_no";

	$insert_fields = 'taxon_no';
	$insert_values = $taxon_no;
    }

    elsif ( my $taxon_name = $record->{taxon_name} )
    {
	my ($genus, $species, $next) = split(/\s+/, $taxon_name);
	my ($subgenus);
	
	unless ( $genus && $genus ne '' )
	{
	    $edt->add_condition($action, 'E_FORMAT', "not a valid taxon name: '$taxon_name'");
	    return;
	}
	
	if ( $species =~ qr{ ^ [(] (\w+) [)] $ }xs )
	{
	    $subgenus = $1;
	    $species = $next;
	}

	if ( $genus && $genus =~ /[^\w]/ )
	{
	    $edt->add_condition($action, 'E_FORMAT', "not a valid genus: '$genus'");
	    return;
	}
	
	if ( $subgenus && $subgenus =~ /[^\w]/ )
	{
	    $edt->add_condition($action, 'E_FORMAT', "not a valid subgenus: '$subgenus'");
	    return;
	}

	if ( $species && $species =~ /[^\w]/ )
	{
	    $edt->add_condition($action, 'E_FORMAT', "not a valid species: '$species'");
	    return;
	}
	
	my $g_expr = "oc.genus_name = " . $dbh->quote($genus);
	my $s_expr = $species ? "oc.species_name = " . $dbh->quote($species) :
	    "(oc.species_name = '' or oc.species_name is null)";
	my $u_expr = $subgenus ? "oc.subgenus_name = " . $dbh->quote($subgenus) :
	    "(oc.subgenus_name = '' or oc.subgenus_name is null)";
	
	$sql = "SELECT occurrence_no FROM $TABLE{OCCURRENCE_DATA} as oc
		WHERE oc.collection_no = $collection_no and $g_expr and $s_expr and $u_expr";

	$insert_fields = 'genus_name,subgenus_name,species_name';
	$insert_values = $dbh->quote($genus) . ',' . $dbh->quote($subgenus || '') . ',' . $dbh->quote($species || '');
    }
    
    $edt->debug_line("$sql\n\n");
    
    my ($occurrence_no) = $dbh->selectrow_array($sql);
    
    # If such an occurrence exists, then we increment the 'abund_value' field. Otherwise, we
    # create a new occurrence. Either way, we must start the database transaction associated with
    # this entire entry operation so that these changes will be part of it.
    
    $edt->start_transaction;
    
    if ( $occurrence_no )
    {
	$record->{occurrence_no} = $occurrence_no;
	
	$sql = "UPDATE $TABLE{OCCURRENCE_DATA} SET abund_value = abund_value + 1
		WHERE occurrence_no = $occurrence_no and abund_unit = 'specimens'
			and abund_value rlike '^[0-9]+\$' and comments rlike '^SPEC_GEN'";
	
	$edt->debug_line($sql);
	
	$result = $dbh->do($sql);
	
	$edt->debug_line("updated 1 record") if $result;
    }
    
    else
    {
	my $authorizer_no = $edt->perms->authorizer_no;
	my $enterer_no = $edt->perms->enterer_no;
	
	unless ( $authorizer_no && $enterer_no )
	{
	    $edt->add_condition($action, 'E_PERM', 'insert');
	}
	
	my $reference_no = ($record->{reference_id} + 0) || '0';

	$sql = "INSERT INTO $TABLE{OCCURRENCE_DATA} (authorizer_no, enterer_no, collection_no, reference_no,
			$insert_fields, abund_unit, abund_value, comments)
		VALUES ($authorizer_no, $enterer_no, $collection_no, $reference_no, $insert_values, 'specimens', 1, 'SPEC_GEN: this record was automatically generated from specimen entry')";
	
	$edt->debug_line($sql);
	
	$result = $dbh->do($sql);
	
	my $new_occurrence_no = $dbh->last_insert_id(undef, undef, undef, undef);
	
	unless ( $result && $new_occurrence_no )
	{
	    $edt->add_condition($action, 'E_EXECUTE', "could not insert the required occurrence record");
	    return;
	}
	
	$record->{occurrence_no} = $new_occurrence_no;

	# If we get here, then we need to add a new record to the occurrence matrix as well.

	$sql = "REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso, 
			species_name, species_reso, plant_organ, plant_organ2,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT o.occurrence_no, 0, true, o.collection_no, o.taxon_no, a.orig_no, 
			o.genus_name, o.genus_reso, o.subgenus_name, o.subgenus_reso,
			o.species_name, o.species_reso, o.plant_organ, o.plant_organ2,
			ei.early_age, li.late_age,
			if(o.reference_no > 0, o.reference_no, c.reference_no),
			o.authorizer_no, o.enterer_no, o.modifier_no, o.created, o.modified
		FROM $TABLE{OCCURRENCE_DATA} as o JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE occurrence_no = $new_occurrence_no";
	
	$edt->debug_line($sql);
	
	$result = $dbh->do($sql);
	
	# If this process fails, produce a warning rather than an error. I'm not sure this is
	# the correct thing to do.
	
	unless ( $result )
	{
	    $edt->add_condition($action, 'W_EXECUTE', "could not update the occurrence matrix");
	}
    }
}


sub validate_measurement {
    
    my ($edt, $action, $operation, $keyexpr) = @_;
    
    my $record = $action->record;
    
    # If no specimen_id was given, fill in the id of the last specimen inserted or updated on this
    # action.
    
    if ( $operation eq 'insert' && ! $record->{specimen_id} )
    {
	my $last_id = $edt->get_attr('last_specimen_key');
	
	if ( $last_id )
	{
	    $record->{specimen_id} = $last_id;
	}
	
	else
	{
	    $edt->add_condition($action, 'E_REQUIRED',
				"a value for 'specimen_id' is required unless this record follows a specimen insertion or update");
	}
    }
    
    # Now set the real_x fields corresponding to any measurement values

    foreach my $f ( qw(average median min max error) )
    {
	my $value = $record->{$f};

	if ( defined $value && $value =~ qr{ ^ \s* ( \d+ | \d+ [.] \d* | \d* [.] \d+ ) }xs )
	{
	    $record->{"real_$f"} = $1;
	}

	else
	{
	    delete $record->{"real_$f"};
	}
    }
}



# before_action ( action, operation, table )
#
# This method is called from EditTransaction.pm before each action. For specimen deletion, we
# check to see if an occurrence record was auto-generated. If so, then we decrement its count and
# delete it if the count reaches zero.

sub before_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    my $dbh = $edt->dbh;
    my ($sql, $count);
    
    if ( $operation eq 'delete' && $table eq 'SPECIMEN_DATA' )
    {
	my $keyexpr = $action->keyexpr;
	
	$sql = "SELECT o.occurrence_no, o.abund_value, o.comments
		FROM $TABLE{OCCURRENCE_DATA} as o JOIN $TABLE{SPECIMEN_DATA} as s using (occurrence_no)
		WHERE s.$keyexpr";

	$edt->debug_line($sql);
	
	my ($occurrence_no, $abund_value, $comments) = $dbh->selectrow_array($sql);
	
	# print STDERR "DELETING: $occurrence_no : $abund_value : $comments\n\n";
	
	if ( $occurrence_no && $comments && $comments =~ /^SPEC_GEN/ )
	{
	    if ( $abund_value && $abund_value > 1 )
	    {
		$sql = "UPDATE $TABLE{OCCURRENCE_DATA} SET abund_value = abund_value - 1
			WHERE occurrence_no = $occurrence_no";
		
		$edt->debug_line($sql);

		# print STDERR "$sql\n\n";
		
		$dbh->do($sql);
	    }
	    
	    else
	    {
		$sql = "DELETE FROM $TABLE{OCCURRENCE_DATA} WHERE occurrence_no = $occurrence_no";
		
		$edt->debug_line($sql);
		
		# print STDERR "$sql\n\n";
		
		$dbh->do($sql);
	    }
	}
    }
}


# after_action ( action, operation, table, keyval )
#
# This method is called from EditTransaction.pm after each action. We override it to add the
# proper record to the specimen matrix for each newly added specimen.

sub after_action {
    
    my ($edt, $action, $operation, $table, $keyval) = @_;

    my $dbh = $edt->dbh;
    my ($sql, $count);
    
    # If we have inserted a record into the SPECIMEN_DATA table, then we need to take some other
    # actions.
    
    if ( $operation eq 'insert' && $table eq 'SPECIMEN_DATA' )
    {
	# Add a record to the specimen matrix, derived from the new record in the specimen table.
	# Ignore any previous record that might have been left there in error.
	
	$sql = "REPLACE INTO $TABLE{SPECIMEN_MATRIX}
		       (specimen_no, occurrence_no, reid_no, latest_ident, taxon_no, orig_no,
			reference_no, authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT s.specimen_no, s.occurrence_no, o.reid_no, ifnull(o.latest_ident, 1), 
		       if(s.taxon_no is not null and s.taxon_no > 0, s.taxon_no, o.taxon_no),
		       if(a.orig_no is not null and a.orig_no > 0, a.orig_no, o.orig_no),
		       s.reference_no, s.authorizer_no, s.enterer_no, s.modifier_no,
		       s.created, s.modified
		FROM $TABLE{SPECIMEN_DATA} as s LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
			LEFT JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.occurrence_no = s.occurrence_no
		WHERE s.specimen_no = $keyval";
	
	$edt->debug_line($sql);
	
	$count = $dbh->do($sql);

	unless ( $count )
	{
	    $edt->add_condition('E_EXECUTE', 'error inserting to the specimen matrix');
	}
	
	# Now record the key value of the newly inserted record so that subsequent measurement
	# insertions will be able to use that.

	$edt->set_attr('last_specimen_key', $keyval);
    }

    # If we have updated a record in the SPECIMEN_DATA table, then we just need to record the key
    # value.

    elsif ( $operation eq 'update' && $table eq 'SPECIMEN_DATA' )
    {
	$edt->set_attr('last_specimen_key', $keyval);
    }
    
    # If we have deleted one or more records in the SPECIMEN_DATA table, then we need to delete the
    # corresponding records in the SPECIMEN_MATRIX and MEASUREMENT_DATA tables.

    elsif ( $operation eq 'delete' && $table eq 'SPECIMEN_DATA' )
    {
	my $keyexpr = $action->keyexpr;

	$sql = "DELETE FROM $TABLE{SPECIMEN_MATRIX} WHERE $keyexpr";
	
	$edt->debug_line($sql);
	
	$count = $dbh->do($sql);
	
	$sql = "DELETE FROM $TABLE{MEASUREMENT_DATA} WHERE $keyexpr";

	$edt->debug_line($sql);

	$count = $dbh->do($sql);
	
	# We should also delete any created occurrence, or decrement the abund_value count for a
	# previously existing occurrence.
    }
    
    # If we are acting on the MEASUREMENT_DATA table, then we need to record the specimenid.
    
    elsif ( $table eq 'MEASUREMENT_DATA' )
    {
	my $specimen_no = $action->record_value('specimen_no');
	$specimen_no = $specimen_no + 0 if ref $specimen_no;

	$edt->set_attr_key('updated_specimen', $specimen_no, 1);
    }
}



1;
