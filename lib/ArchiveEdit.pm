# 
# The Paleobiology Database
# 
#   ArchiveEdit.pm
# 

package ArchiveEdit;

use strict;

use Carp qw(carp croak);

use TableDefs qw(%TABLE);

use base 'EditTransaction';

our (@CARP_NOT) = qw(EditTransaction);


{
    ArchiveEdit->register_conditions(
	C_ARCHIVE_HAS_DOI => "Allow 'ARCHIVE_HAS_DOI' in order to alter or delete a record with an assigned DOI",
	E_IMMUTABLE => "An archive that has a DOI assigned is immutable except for the description".);
    
    ArchiveEdit->register_allowances('ARCHIVE_HAS_DOI');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# Update and delete are restricted if the record contains a DOI. Only an administrator can add a
# DOI to a record, or update or delete a record that contains a DOI.

sub validate_action {

    my ($edt, $action, $operation, $table) = @_;
    
    # Get the action record, the current record if any, and action permission.
    
    my $record = $action->record;
    my $current = $edt->fetch_old_record($action);
    my $permission = $action->permission;
    
    # If the operation is 'insert', 'update', or 'replace', only an administrator can change a doi or
    # alter any field except 'description' of a record with a doi. And an administrator can only
    # do this with the allowance ARCHIVE_HAS_DOI.
    
    if ( $operation eq 'insert' || $operation eq 'update' || $operation eq 'replace' )
    {
	# The doi can only be changed by an administrator.
	
	if ( defined $record->{doi} )
	{
	    if ( $current && $current->{doi} && $current->{doi} eq $record->{doi} )
	    {
		# Do nothing if the value is not being changed.
	    }
	    
	    elsif ( $permission ne 'admin' )
	    {
		$edt->add_condition('E_PERM_COL', 'doi');	# A non-administrator gets an error
	    }
	    
	    elsif ( ! $edt->allows('ARCHIVE_HAS_DOI') )
	    {
		$edt->add_condition('C_ARCHIVE_HAS_DOI');	# An administrator gets a caution
	    }
	}
	
	# Any record with a doi is immutable except for the description field, except for
	# administrators. The doi field was already checked above, so need not be checked again. 
	
	if ( $current && $current->{doi} )
	{
	    my $check_admin;
	    
	    foreach my $key ( keys %$record )
	    {
		$check_admin = 1 unless $key =~ /^description$|^doi$|^_/;
	    }
	    
	    $check_admin = 1 if $operation eq 'replace';    # Only an administrator can replace a
                                                            # record with a doi.
	    
	    if ( $check_admin && $permission ne 'admin' )
	    {
		$edt->add_condition('E_IMMUTABLE');		# Non-administrators get an error
	    }
	    
	    elsif ( $check_admin && ! $edt->allows('ARCHIVE_HAS_DOI') )
	    {
		$edt->add_condition('C_ARCHIVE_HAS_DOI');	# Administrators get a caution
	    }
	}
    }
    
    # If the action is 'delete', only an an administrator can delete a record with a doi. And an
    # administrator can do this only with the allowance ARCHIVE_HAS_DOI.
    
    elsif ( $operation eq 'delete' )
    {
	if ( $current && $current->{doi} )
	{
	    if ( $permission ne 'admin' )
	    {
		$edt->add_condition('E_IMMUTABLE');	# Non-administrators get an error.
	    }

	    elsif ( ! $edt->allows('ARCHIVE_HAS_DOI') )
	    {
		$edt->add_condition('C_ARCHIVE_HAS_DOI');
	    }
	}
    }
}


1;
