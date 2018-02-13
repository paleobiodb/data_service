# 
# The Paleobiology Database
# 
#   TestEdit.pm - a class for use in testing EditTransaction.pm
#   
#   This class is a subclass of EditTransaction, and is used by the unit tests
#   for EditTransaction and its related classes.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back.


package EditTest;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(set_table_property set_column_property $EDT_TEST);
use ResourceTables;

use base 'EditTransaction';

use namespace::clean;

our (%CONDITION_TEMPLATE) = (E_TEST => "TEST ERROR",
			     W_TEST => "TEST WARNING");

# At runtime, set column properties for our test table
# ----------------------------------------------------

{
    set_table_property($EDT_TEST, ALLOW_POST => 'AUTHORIZED');
    set_table_property($EDT_TEST, ALLOW_DELETE => 1);
    set_table_property($EDT_TEST, PRIMARY_KEY => 'test_no');
    set_column_property($EDT_TEST, 'string_req', REQUIRED => 1);
    set_column_property($EDT_TEST, 'signed_req', REQUIRED => 1);

    EditTest->register_allows('TEST_ALLOW');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# get_condition_template ( code, table, selector )
#
# Return the proper template for error and warning conditions defined by this subclass. If no
# matching template can be found, we call the parent method. Other subclasses of EditTransaction
# should do something similar.

sub get_condition_template {
    
    my ($edt, $code, $table, $selector) = @_;
    
    if ( $CONDITION_TEMPLATE{$code} )
    {
	return $CONDITION_TEMPLATE{$code};
    }

    else
    {
	return $edt->SUPER::get_condition_template($code, $table, $selector);
    }
}


sub authorize_action {

    return EditTransaction::authorize_action(@_);
}


sub validate_action {

    return EditTransaction::validate_action(@_);
}


sub initialize_transaction {

}


sub finalize_transaction {

}


sub before_action {

}


sub after_action {

}


# establishTestTables ( class, dbh, options )
# 
# This class method creates database tables necessary to use this class for testing purposes, or
# replaces the existing ones.

sub establish_tables {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= { };
    
    # Create, or re-create, the table 'edt_test'.
    
    $dbh->do("DROP TABLE IF EXISTS $EDT_TEST");
    
    $dbh->do("CREATE TABLE $EDT_TEST (
		test_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		modifier_no int unsigned not null,
		interval_no int unsigned not null,
		string_val varchar(40) not null,
		string_req varchar(40) not null,
		signed_val mediumint not null,
		unsigned_val mediumint unsigned not null,
		signed_req int unsigned not null,
		decimal_val decimal(5,2),
		float_val double,
		boolean_val boolean,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp)");
    
}
