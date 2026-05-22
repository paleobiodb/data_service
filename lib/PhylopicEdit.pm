# 
# The Paleobiology Database
# 
#   PhylopicEdit.pm - a class for handling updates to taxon image choice records.
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for updating
#   phylopic image choices.
#   
#   Each instance of this class is responsible for initiating a database
#   transaction, checking a set of records for insertion, update, or deletion,
#   and either committing or rolling back the transaction depending on the
#   results of the checks. If the object is destroyed, the transaction will be
#   rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = PhylopicEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call PhylopicEdit->replace_record, etc. 


package PhylopicEdit;

use strict;

use TableDefs qw(%TABLE set_table_property set_column_property);

use parent 'EditTransaction';

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

our (@CARP_NOT) = qw(EditTransaction);

{
    PhylopicEdit->ignore_field('PHYLOPIC_CHOICE', 'considered');
    
    set_table_property('PHYLOPIC_CHOICE', CAN_POST => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', CAN_MODIFY => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', CAN_DELETE => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', REQUIRED_COLS => ['image_no']);
    set_column_property('PHYLOPIC_CHOICE', 'image_no', ALTERNATE_NAME => 'selected');
    set_column_property('PHYLOPIC_CHOICE', 'image_no', EXTID_TYPE => 'PHP');
}


# Methods
# -------

# validate_action ( )
#
# This method is called automatically for each action, before it is executed. It is
# called for both kinds of actions we use, 'replace_record' and 'update_considered'.

sub validate_action {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    my $dbh = $edt->dbh;
    
    my $orig_no = $action->record_value('orig_no');
    my $image_no = $action->record_value('selected');
    
    my %good_image;
    
    # Both operations require a good value for 'orig_no'.
    
    my $qorig = $edt->my_validate_identifier('orig_no', $orig_no, 'TXN') || return;
    
    # If a value for 'selected' was given, check that it is a possible choice for
    # the specified taxon.
    
    if ( $image_no )
    {
	my $qimage = $edt->my_validate_identifier('selected', $image_no, 'PHP') || return;
	
	my ($check) = $edt->selectrow_array(<<~END_SQL);
		SELECT image_no FROM $TABLE{PHYLOPIC_NAMES}
		WHERE orig_no = $qorig and image_no = $qimage
		END_SQL
	
	if ( $check )
	{
	    $good_image{$qimage} = 1;
	}
	
	else
	{
	    $edt->add_condition('E_NOT_FOUND', 'custom',
			"Field 'selected': $qimage is not an available choice for taxon $qorig");
	    return;
	}
    }
    
    # If one or more values for 'considered' were given, check that each is a possible
    # choice for the specified taxon. If the value was a string, replace it in the
    # action record with a list of values for use by the action method later.
    
    if ( my $considered = $action->record_value('considered') )
    {
	my @considered;
	
	if ( ref $considered eq 'ARRAY' )
	{
	    @considered = @$considered;
	}
	
	else
	{
	    @considered = grep { $_ } split /\s*,\s*/, $considered;
	    $action->set_record_value('considered', \@considered);
	}
	
	foreach my $c ( @considered )
	{
	    if ( my $qcons = $edt->my_validate_identifier('considered', $c, 'PHP') )
	    {
		next if $good_image{$qcons};
		
		my ($check) = $edt->selectrow_array(<<~END_SQL);
		    SELECT image_no FROM $TABLE{PHYLOPIC_NAMES}
		    WHERE orig_no = $qorig and image_no = $qcons
		    END_SQL

		if ( $check )
		{
		    $good_image{$c} = 1;
		}
		
		else
		{
		    $edt->add_condition('E_NOT_FOUND', 'custom',
			"Field 'considered': $qcons is not an available choice for taxon $qorig");
		}
	    }
	}
    }
}


sub my_validate_identifier {

    my ($edt, $field, $value, $type) = @_;

    # If the value is empty, return false.
    
    return unless $value;
    
    # Otherwise, validate it as an external identifier. If the first result is an
    # arrayref, it represents an error condition.
    
    my ($err, $value) = $edt->validate_extid_value($type, $value, $field);
    
    if ( ref $err )
    {
	$edt->add_condition(@$err);
	return;
    }
    
    # Otherwise, $value is the unpacked value. Quote it and return it.
    
    else
    {
	return $edt->dbh->quote($value);
    }
}


# after_action ( )
#
# This method is called automatically for each action after it is successfully executed.

sub after_action {

    my ($edt, $action, $operation, $table_specifier) = @_;

    # If the table specifier is 'PHYLOPIC_CHOICE', update the TAXON_ATTRS table to match
    # the new phylopic choice. If 'considered' was specified, call op_update_considered
    # as well.
    
    if ( $table_specifier eq 'PHYLOPIC_CHOICE' )
    {
	my $orig_no = $action->record_value('orig_no');
	my $image_no = $action->record_value('selected');
	
	my $dbh = $edt->dbh;
	my $qorig = $dbh->quote($orig_no);
	my $qimage = $dbh->quote($image_no);
	
	$edt->do_stmt(<<~END_SQL);
	    UPDATE $TABLE{TAXON_ATTRS} SET image_no = $qimage
	    WHERE orig_no = $qorig
	    END_SQL
	
	$edt->op_update_considered($action);
    }
}


# op_update_considered ( )
#
# This method is called at the appropriate time to execute this action.

sub op_update_considered {

    my ($edt, $action) = @_;
    
    my $orig_no = $action->record_value('orig_no');
    
    # Update the PHYLOPIC_SEEN table to record all of the considered images.

    my $dbh = $edt->dbh;
    my $qpers = $dbh->quote($edt->permission->enterer_no);
    my $qorig = $dbh->quote($orig_no);
    
    my $considered = $action->record_value('considered');

    foreach my $c ( @$considered )
    {
	my $qcons = $dbh->quote($c);
	
	$edt->do_stmt(<<~END_SQL);
	    REPLACE INTO $TABLE{PHYLOPIC_SEEN} (person_no, orig_no, image_no)
	    VALUES ($qpers, $qorig, $qcons)
	    END_SQL
    }
    
    # If the operation is 'other', set the key value to the orig_no. We have to quote it
    # to stringify it, in case it was originally an external identifier.
    
    if ( $action->operation eq 'other' )
    {
	$action->set_keyval("$orig_no");
    }
}


1;
