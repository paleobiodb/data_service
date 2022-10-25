# 
# The Paleobiology Database
# 
#   ReferenceEdit.pm - a class for handling updates to bibliographic reference records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting bibliographic reference records.
#   
#   To use it, first call $edt = ReferenceEdit->new with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record, etc.


use strict;

package ReferenceEdit;

use parent 'EditTransaction';

use ReferenceMatch;

use Carp qw(carp croak);

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);
use CoreTableDefs;

use Class::Method::Modifiers qw(before around);
use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';

use namespace::clean;


our (@CARP_NOT) = qw(EditTransaction);

{
    ReferenceEdit->register_conditions(
       C_DUPLICATE_REF => "Allow 'DUPLICATE_REF' to add records that are potential duplicates.",
       W_DUPLICATE_REF => "Possible duplicate for reference '&1' in the database");
    
    ReferenceEdit->register_allowances('DUPLICATE_REF');
    
    set_table_name(REF_DATA => 'ref_local');
    set_table_name(REF_ATTRIB => 'ref_attrib');
    set_table_name(REF_PEOPLE => 'ref_people');
    set_table_name(REF_PUBS => 'ref_pubs');
    set_table_name(REF_TYPES => 'ref_types');
    set_table_name(REF_SOURCES => 'ref_sources');
    set_table_name(REF_EXTDATA => 'ref_external');
    set_table_name(REF_TEMPDATA => 'ref_tempdata');
    
    set_table_group('references' => qw(REF_DATA REF_ATTRIB REF_CONTRIB REF_PEOPLE
				       REF_PUBS REF_TYPES REF_SOURCES
				       REF_EXTDATA REF_TEMPDATA));
    
    set_table_property('REF_ENTRIES', CAN_MODIFY => 'authorized');
    set_table_property('REF_ENTRIES', CAN_DELETE => 'admin');
    set_table_property('REF_ENTRIES', REQUIRED_COLS => 'ref_type, pubyr, attribution');
    set_table_property('REF_ENTRIES', SPECIAL_COLS => 
		       'ts_created, ts_modified, authorizer_no, enterer_no, modifier_no');
    
    set_table_property(REF_ATTRIB => CAN_MODIFY => 'authorized');
    set_table_property(REF_ATTRIB => REQUIRED_COLS => 'last, role');
    
    set_table_property(REF_PEOPLE => CAN_MODIFY => 'authorized');
    set_table_property(REF_PEOPLE => REQUIRED_COLS => 'last, role');
    
    set_table_property(REF_PUBS => CAN_MODIFY => 'authorized');
    set_table_property(REF_PUBS => REQUIRED_COLS => 'pub_type, pub_title, pubyr');
    
    set_table_property(REF_ROLES => CAN_MODIFY => 'admin');
    set_table_property(REF_ROLES => AUTH_TABLE => 'REF_DATA');
    set_table_property(REF_ROLES => REQUIRED_COLS => 'role');
    
    set_table_property(REF_TYPES => CAN_MODIFY => 'admin');
    set_table_property(REF_TYPES => AUTH_TABLE => 'REF_DATA');
    set_table_property(REF_TYPES => REQUIRED_COLS => 'type');
    
    set_table_property(REF_SOURCES => CAN_MODIFY => 'admin');
    set_table_property(REF_SOURCES => AUTH_TABLE => 'REF_DATA');
    set_table_property(REF_SOURCES => REQUIRED_COLS => 'name');
    
    set_table_property(REF_EXTERNAL => CAN_MODIFY => 'authorized');
    set_table_property(REF_EXTERNAL => REQUIRED_COLS => 'source');
    
    set_table_property(REF_TEMPDATA => CAN_MODIFY => 'authorized');
    set_table_property(REF_TEMPDATA => REQUIRED_COLS => 'source');    
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action, keyexpr )
# 
# This method is called from EditTransaction.pm to validate each action. We override it to do
# additional checks.

before 'validate_action' => sub {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;

    # If this transaction allows duplicates, we skip the check.

    if ( ! $edt->allows('DUPLICATE_REF') )
    {
	my $threshold = 80;
	
	# For 'insert' and 'replace' operations, we check for the possibility that the submitted
	# record duplicates one that is already in the database. The variable $estimate will hold
	# a rough estimate of the probability that a duplicate has been found, with values in the
	# range 0-100.
	
	my ($duplicate_no, $estimate);
	
	if ( $operation eq 'insert' || $operation eq 'replace' )
	{
	    ($duplicate_no, $estimate) = check_for_duplication($record);
	}
	
	# For 'update' operations, we fetch the current record and apply the updates, then check
	# for the possibility that the update record duplicates a different one that is already in
	# the databse.
	
	elsif ( $operation eq 'update' )
	{
	    my $current = $edt->fetch_old_record($action, $table, $keyexpr);
	    
	    my %check = (%$current, %$record);

	    ($duplicate_no, $estimate) = check_for_duplication(\%check);
	}
	
	# If we have an estimate that passes the threshold, then throw a caution.

	if ( $estimate >= $threshold )
	{
	    $edt->add_condition($action, 'C_DUPLICATE_REF', $duplicate_no);
	    return;
	}
    }
};


# check_for_duplication ( attrs )
#
# Given a hashref of reference attributes in $attrs, check and see if this set of attributes
# matches any records currently in the REFERENCES table. If so, return the reference_no of the
# most likely match, along with an estimated probability of a duplication.

sub check_for_duplication {

    my ($attrs) = @_;

    
}

