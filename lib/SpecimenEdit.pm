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

use Taxonomy;

use base 'EditTransaction';

use namespace::clean;



{
    SpecimenEdit->register_conditions(
	C_UNKNOWN_TAXON => "The specified taxon name is not in the database. Add 'allow=UNKNOWN_TAXON' to continue.");
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action )

# This method is called from EditTransaction.pm to validate each insert and update action.
# We override it to do additional checks before calling validate_against_schema.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;
    
    if ( $record->{taxon_name} )
    {
	if ( $record->{taxon_id} )
	{
	    $edt->add_condition('E_PARAM', "you may not specify both 'taxon_name' and 'taxon_id' in the same record");
	}

	else
	{
	    my $dbh = $edt->dbh;
	    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

	    my ($result) = $taxonomy->resolve_names($record->{taxon_name}, { fields => 'SEARCH' });
	    
	    if ( $result && $result->{orig_no} )
	    {
		$record->{taxon_id} = $result->{taxon_no};
	    }
	    
	    else
	    {
		$edt->add_condition('C_UNKNOWN_TAXON', 'taxon_name');
	    }
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
    
    if ( $operation eq 'insert' )
    {
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
    }
}



1;
