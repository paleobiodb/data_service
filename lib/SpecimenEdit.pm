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
	E_UNREGISTERED_TAXON => "Specimen records cannot be stored without either a 'collection_id' or a taxon that is known to the database");

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

    else
    {
	return $edt->validate_measurement($action, $operation, $keyexpr);
    }
}


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
	    
	    elsif ( ! $edt->allows('UNKNOWN_TAXON') )
	    {
		$edt->add_condition($action, 'C_UNKNOWN_TAXON', 'taxon_name');
	    }
	}
    }
    
    # If we have neither a collection_no nor a taxon_no, then we cannot store this specimen.

    if ( $operation eq 'insert' )
    {
	unless ( $record->{collection_id} || $record->{taxon_id} )
	{
	    $edt->add_condition($action, 'E_UNREGISTERED_TAXON');
	}
    }

    elsif ( $operation eq 'update' && $record->{taxon_name} )
    {
	# This needs to be added $$$
    }
    
    # If we do have a collection_id, check to make sure it exists. Also check to see if that
    # collection already has an occurrence of that taxon. If not, we will need to create one.

    if ( $record->{collection_id} )
    {
	$edt->validate_collection($action, $operation, $record);
    }
}


sub validate_collection {
    
    my ($edt, $action, $operation, $record) = @_;
    
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
    
    $edt->start_transaction;
    
    if ( $occurrence_no )
    {
	$record->{occurrence_no} = $occurrence_no;

	$sql = "UPDATE $TABLE{OCCURRENCE_DATA} SET abund_value = abund_value + 1
		WHERE occurrence_no = $occurrence_no and abund_unit = 'specimens'
			and abund_value rlike '^[0-9]+\$'";
	
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
		VALUES ($authorizer_no, $enterer_no, $collection_no, $reference_no, $insert_values, 'specimens', 1, 'this record was automatically generated from specimen entry')";
	
	$edt->debug_line($sql);
	
	$result = $dbh->do($sql);
	
	my $new_occurrence_no = $dbh->last_insert_id(undef, undef, undef, undef);
	
	unless ( $result && $new_occurrence_no )
	{
	    $edt->add_condition($action, 'E_EXECUTE', "could not insert the required occurrence record");
	    return;
	}

	$record->{occurrence_no} = $new_occurrence_no;
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


# after_action ( action, operation, table, result )
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
	
	$sql = "INSERT INTO $TABLE{SPECIMEN_MATRIX}
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
    
    # If we are acting on the MEASUREMENT_DATA table, then we need to record the specimenid.
    
    elsif ( $table eq 'MEASUREMENT_DATA' )
    {
	my $specimen_no = $action->record_value('specimen_no') || $action->record_value('specimen_id');
	$specimen_no = $specimen_no + 0 if ref $specimen_no;

	$edt->set_attr_key('updated_specimen', $specimen_no, 1);
    }
}



1;
