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

use ReferenceMatch qw(parse_authorname);

use Carp qw(carp croak);

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);
use CoreTableDefs;

use Class::Method::Modifiers qw(before around);
use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;


our (@CARP_NOT) = qw(EditTransaction);

{
    ReferenceEdit->ignore_field('REF_DATA', 'authors');
    ReferenceEdit->ignore_field('REF_DATA', 'editors');
    
    ReferenceEdit->register_conditions(
       C_DUPLICATE => "Allow 'DUPLICATE' to add records that are potential duplicates.",
       W_DUPLICATE => "Possible duplicate for reference '&1' in the database",
       E_AUTHOR_EMPTY => "Author &1 has an empty value",
       E_LASTNAME_WIDTH => "Author lastname &1 exceeds column width of 80",
       E_FIRSTNAME_WIDTH => "Author firstname &1 exceeds column width of 80");
    
    ReferenceEdit->register_allowances('DUPLICATE');
    
    set_table_name(REF_DATA => 'refs');
    set_table_name(REF_AUTHORS => 'ref_authors');
    set_table_name(REF_SOURCES => 'ref_sources');
    set_table_name(REF_EXTDATA => 'ref_external');
    
    set_table_group('references' => qw(REF_DATA REF_AUTHORS REF_SOURCES REF_EXTDATA));
    
    set_table_property('REF_DATA', CAN_MODIFY => 'authorized');
    set_table_property('REF_DATA', CAN_DELETE => 'admin');
    set_table_property('REF_DATA', REQUIRED_COLS => 'publication_type, pubyr, authors');
    set_table_property('REF_DATA', SPECIAL_COLS => 
		       'ts_created, ts_modified, authorizer_no, enterer_no, modifier_no');
    set_column_property('REF_DATA', 'publication_type', REQUIRED => 1);
    set_column_property('REF_DATA', 'pubyr', REQUIRED => 1);
    
    set_table_property(REF_EXTERNAL => CAN_MODIFY => 'authorized');
    
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
    
    my ($edt, $action, $operation, $table) = @_;
    
    my $record = $action->record;

    # If this transaction allows duplicates, we skip the check.

    if ( ! $edt->allows('DUPLICATE') )
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
	
	# # For 'update' operations, we fetch the current record and apply the updates, then check
	# # for the possibility that the update record duplicates a different one that is already in
	# # the databse.
	
	# elsif ( $operation eq 'update' )
	# {
	#     my $current = $edt->fetch_old_record($action, $table, $keyexpr);
	    
	#     my %check = (%$current, %$record);

	#     ($duplicate_no, $estimate) = check_for_duplication(\%check);
	# }
	
	# If we have an estimate that passes the threshold, then throw a caution.

	if ( $estimate >= $threshold )
	{
	    $edt->add_condition($action, 'C_DUPLICATE', $duplicate_no);
	    return;
	}
    }
    
    # Now handle the author and editor lists.
    
    # $action->handle_column("FIELD:authors", 'ignore');
    # $action->handle_column("FIELD:editors", 'ignore');
    
    my $author_list = $record->{authors};
    my (@authorname, @firstname, @lastname);
    
    if ( ref $author_list eq 'ARRAY' )
    {
	@authorname = $author_list->@*;
    }
    
    elsif ( ref $author_list )
    {
	croak "The value of 'authors' must be a listref or a scalar";
    }
    
    else
    {
	@authorname = split /;\s+/, $author_list;
    }
    
    $record->{author1last} = '';
    $record->{author1init} = '';
    $record->{author2last} = '';
    $record->{author2init} = '';
    $record->{otherauthors} = '';
    my @otherauthors;
    
    foreach my $i ( 0..$#authorname )
    {
	unless ( $authorname[$i] =~ / \pL /xs )
	{
	    $edt->add_condition('E_AUTHOR_EMPTY', $i+1);
	    next;
	}
	
	my ($lastname, $firstname, $affiliation, $orcid) = parse_authorname($authorname[$i]);
	
	$firstname[$i] = $firstname;
	$lastname[$i] = $lastname;
	
	my $suffix;
	
	my $initial = substr($firstname, 0, 1) . '.'; # $$$ allow for multiple initials
	
	if ( $i == 0 )
	{
	    $record->{author1last} = $lastname;
	    $record->{author1init} = $initial;
	}
	
	elsif ( $i == 1 )
	{
	    $record->{author2last} = $lastname;
	    $record->{author2init} = $initial;
	}
	
	else
	{
	    push @otherauthors, "$initial $lastname";
	}
	
	if ( $authorname[$i] =~ / (.*) (,\s*jr.\s*|,\s*iii\s*) (.*) /xsi )
	{
	    $authorname[$i] = "$1$3";
	    $suffix = $2;
	}
	
	if ( $authorname[$i] =~ / (.*?) , \s* (.*) /xs )
	{
	    $lastname[$i] = $1;
	    $lastname[$i] .= $suffix if $suffix;
	    $firstname[$i] = $2;
	}
	
	elsif ( $authorname[$i] =~ / (.*) \s (.*) /xs )
	{
	    $firstname[$i] = $1;
	    $lastname[$i] = $2;
	    $lastname[$i] .= $suffix if $suffix;
	}
	
	if ( @otherauthors )
	{
	    $record->{otherauthors} = join(', ', @otherauthors);
	}
    }
    
    $record->{firstname} = \@firstname;
    $record->{lastname} = \@lastname;
    $record->{n_authors} = scalar(@authorname);
    
    $action->handle_column("FIELD:firstname", 'ignore');
    $action->handle_column("FIELD:lastname", 'ignore');
    $action->handle_column("FIELD:n_authors", 'ignore');
    
    # Handle the page number(s).
    
    if ( $record->{pages} =~ /(.*)\s*-\s*(.*)/ )
    {
	$record->{firstpage} = $1;
	$record->{lastpage} = $2;
    }
    
    else
    {
	$record->{firstpage} = $record->{pages};
    }
    
    $action->handle_column("FIELD:pages", 'ignore');
};


sub after_action {
    
    my ($edt, $action, $operation, $table_specifier) = @_;
    
    my $keyval = $action->keyval;
    my $dbh = $edt->dbh;
    my $qkeyval = $dbh->quote($keyval);
    
    
    if ( $operation eq 'update' || $operation eq 'replace' )
    {
	
    }
    
}




# check_for_duplication ( attrs )
#
# Given a hashref of reference attributes in $attrs, check and see if this set of attributes
# matches any records currently in the REFERENCES table. If so, return the reference_no of the
# most likely match, along with an estimated probability of a duplication.

sub check_for_duplication {

    my ($attrs) = @_;

    
}

1;
