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

use ReferenceMatch qw(ref_similarity parse_authorname);

use Carp qw(carp croak);

use ExternalIdent qw(generate_identifier);
use TableDefs qw(%TABLE set_table_name set_table_group set_table_property set_column_property);
use CoreTableDefs;

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;


our (@CARP_NOT) = qw(EditTransaction);

{
    ReferenceEdit->ignore_field('REFERENCE_DATA', 'authors');
    ReferenceEdit->ignore_field('REFERENCE_DATA', 'pages');
    
    ReferenceEdit->register_conditions(
       C_DUPLICATE => "Possible duplicate of: &1",
       C_CAPITAL => ["&1 &2 has bad capitalization", "Field '&1': bad capitalization"],
       E_NAME_EMPTY => "&1 &2 must have a last name containing at least one letter",
       E_NAME_WIDTH => "&1 &2 exceeds column width of 100",
       E_CANNOT_DELETE => "This reference is used by other records in the database");
    
    ReferenceEdit->register_allowances('DUPLICATE', 'CAPITAL');
    
    set_table_name(REFERENCE_DATA => 'refs');
    set_table_name(REFERENCE_AUTHORS => 'ref_authors');
    set_table_name(REFERENCE_SOURCES => 'ref_sources');
    set_table_name(REFERENCE_EXTDATA => 'ref_external');
    
    set_table_group('references' => qw(REFERENCE_DATA REFERENCE_AUTHORS 
				       REFERENCE_SOURCES REFERENCE_EXTDATA));
    
    set_table_property('REFERENCE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_DELETE => 'ADMIN');
    set_table_property('REFERENCE_DATA', REQUIRED_COLS => 'reftitle, publication_type, pubyr');
    set_table_property('REFERENCE_DATA', SPECIAL_COLS => 
		       'ts_created, ts_modified, authorizer_no, enterer_no, modifier_no');
    
    set_table_property(REFERENCE_EXTDATA => CAN_POST => 'AUTHORIZED');
    set_table_property(REFERENCE_EXTDATA => CAN_MODIFY => 'AUTHORIZED');
    set_table_property(REFERENCE_TEMPDATA => CAN_MODIFY => 'AUTHORIZED');
    set_table_property(REFERENCE_TEMPDATA => REQUIRED_COLS => 'source');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action, keyexpr )
# 
# This method is called from EditTransaction.pm to validate each action. We override it to do
# additional checks.

sub validate_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    # If the operation is 'delete', we check to see if any other records use
    # this reference. If so, it cannot be deleted. If there is no bar to
    # deletion, then no other checks need to be done.
    
    if ( $operation eq 'delete' )
    {
	unless ( $edt->able_to_delete($action) )
	{
	    $edt->add_condition('E_CANNOT_DELETE');
	}
	
	return;
    }
    
    # Otherwise, do some checks on the incoming data.
    
    my $record = $action->record;
    my $keyexpr = $action->keyexpr;
    
    if ( $action->keymult )
    {
	$edt->add_condition('E_MULTI_KEY', $operation);
	return;
    }
    
    # If the operation is 'update' and the record does not contain
    # a 'publication_type' field, fetch it from the existing record.
    
    my $pubtype = $record->{publication_type};
    
    if ( exists $record->{publication_type} && ! $record->{publication_type} )
    {
	# my $dbh = $edt->dbh;
	# my $sql = "SELECT publication_type FROM refs WHERE $keyexpr";
	
	# $edt->debug_line("$sql\n") if $edt->debug_mode;
	
	# ($pubtype) = $dbh->selectrow_array($sql);
	
	$edt->add_condition('E_REQUIRED', 'publication_type');
    }
    
    if ( $operation !~ /^update/ && ! $record->{publication_type} )
    {
	$edt->add_condition('E_REQUIRED', 'publication_type');
    }
    
    # Check capitalizatiion of the reference title and publication title.
    
    unless ( $edt->allows('CAPITAL') )
    {
	if ( $record->{reftitle} && $record->{reftitle} !~ / ^ \p{Lu} .* \p{Ll} /xs )
	{
	    $edt->add_condition('C_CAPITAL', 'reftitle');
	}
	
	if ( $record->{pubtitle} && $record->{pubtitle} !~ / ^ \p{Lu} .* \p{Ll} /xs )
	{
	    $edt->add_condition('C_CAPITAL', 'pubtitle');
	}
    }
    
    # Handle the author names, if any.
    
    if ( exists $record->{authors} )
    {
	my $author_list = $record->{authors};
	my $bad_format;
	
	my (@authorname, @authorfirst, @authorlast);
	
	if ( ref $author_list eq 'ARRAY' )
	{
	    @authorname = $author_list->@*;
	}
	
	elsif ( ref $author_list )
	{
	    $edt->add_condition('E_FORMAT', 'authors', 'must be a string or an array of strings');
	    $bad_format = 1;
	}
	
	elsif ( $author_list )
	{
	    @authorname = split /;\s+/, $author_list;
	}
	
	$record->{author1last} = '';
	$record->{author1init} = '';
	$record->{author2last} = '';
	$record->{author2init} = '';
	$record->{otherauthors} = '';
	my @otherauthors;
	
	# Process the author names one at a time.
	
	foreach my $i ( 0..$#authorname )
	{
	    # Check that each name is within the column width limit.
	    
	    if ( length($authorname[$i]) >= 100 )
	    {
		$edt->add_condition('E_NAME_WIDTH', 'Author', $i+1);
		next;
	    }
	    
	    # If a name includes an asterisk '*', treat it as a last name
	    # only. Remove the asterisk, of course.
	    
	    my ($authorfirst, $authorlast);
	    
	    if ( $authorname[$i] =~ /[*]/ )
	    {
		$authorfirst = '';
		$authorlast = $authorname[$i];
		$authorlast =~ s/\s*[*]\s*//g;
	    }
	    
	    # Otherwise, parse the name into first and last.
	    
	    else
	    {
		($authorlast, $authorfirst) = parse_authorname($authorname[$i]);
	    }
	    
	    $authorfirst[$i] = $authorfirst;
	    $authorlast[$i] = $authorlast;
	    
	    # Make sure that each author has at least two letters in the last name,
	    # and that the name is properly capitalized unless CAPITAL is allowed.
	    
	    if ( $authorlast[$i] !~ / \pL .* \pL /xs )
	    {
		$edt->add_condition('E_NAME_EMPTY', 'Author', $i+1);
		next;
	    }
	    
	    elsif ( ! $edt->allows('CAPITAL') )
	    {
		if ( $authorlast !~ / ^ \p{Lu} .* \p{Ll} /xs )
		{
		    $edt->add_condition('C_CAPITAL', 'Author', $i+1);
		}
		
		if ( defined $authorfirst && $authorfirst ne '' && 
		     $authorfirst !~ / ^ \p{Lu} /xs )
		{
		    $edt->add_condition('C_CAPITAL', 'Author', $i+1);
		}
	    }
	    
	    my $suffix;
	    
	    my $initials = '';
	    
	    # If a first name is given, generate initials.
	    
	    if ( $authorfirst )
	    {
		foreach my $word ( split(/\s+/, $authorfirst ) )
		{
		    if ( $word =~ /(\p{L})/ )
		    {
			$initials .= "$1. ";
		    }
		}
		
		$initials =~ s/ $//;
	    }
	    
	    if ( $i == 0 )
	    {
		$record->{author1last} = $authorlast;
		$record->{author1init} = $initials;
	    }
	    
	    elsif ( $i == 1 )
	    {
		$record->{author2last} = $authorlast;
		$record->{author2init} = $initials;
	    }
	    
	    else
	    {
		push @otherauthors, ($initials ? "$initials $authorlast" : $authorlast);
	    }
	
	    # if ( $authorname[$i] =~ / (.*) (,\s*jr.\s*|,\s*iii\s*) (.*) /xsi )
	    # {
	    #     $authorname[$i] = "$1$3";
	    #     $suffix = $2;
	    # }
	    
	    # if ( $authorname[$i] =~ / (.*?) , \s* (.*) /xs )
	    # {
	    #     $authorlast[$i] = $1;
	    #     $authorlast[$i] .= $suffix if $suffix;
	    #     $authorfirst[$i] = $2;
	    # }
	    
	    # elsif ( $authorname[$i] =~ / (.*) \s (.*) /xs )
	    # {
	    #     $authorfirst[$i] = $1;
	    #     $authorlast[$i] = $2;
	    #     $authorlast[$i] .= $suffix if $suffix;
	    # }
	}
	
	if ( @otherauthors )
	{
	    $record->{otherauthors} = join(', ', @otherauthors);
	}
	
	$record->{authorfirst} = \@authorfirst;
	$record->{authorlast} = \@authorlast;
	$record->{n_authors} = scalar(@authorname);
	
	# At least one author name is required.
	
	unless ( @authorlast && $authorlast[0] )
	{
	    $edt->add_condition('E_REQUIRED', 'authors') unless $bad_format;
	}
	
	$action->ignore_field('authorfirst');
	$action->ignore_field('authorlast');
	$action->ignore_field('n_authors');
    }
    
    elsif ( $operation !~ /^update/ )
    {
	$edt->add_condition('E_REQUIRED', 'authors');
    }
    
    # Handle the editor names, if any.
    
    my $editor_list = $record->{editors};
    my (@editorname, @editorfirst, @editorlast);
    
    if ( ref $editor_list eq 'ARRAY' )
    {
	@editorname = $editor_list->@*;
    }
    
    elsif ( ref $editor_list )
    {
	croak "The value of 'editors' must be a listref or a scalar";
    }
    
    else
    {
	@editorname = split /;\s+/, $editor_list;
    }
    
    my @ref_editors;
    
    foreach my $i ( 0..$#editorname )
    {
	# Check that each name is within the column width limit.
	
	if ( length($editorname[$i]) >= 100 )
	{
	    $edt->add_condition('E_NAME_WIDTH', 'Editor', $i+1);
	    next;
	}
	
	# Parse each name into first and last.
	
	my ($editorlast, $editorfirst) = parse_authorname($editorname[$i]);
	
	$editorfirst[$i] = $editorfirst;
	$editorlast[$i] = $editorlast;
	
	# Make sure that each editor has at least two letters in the last name,
	# and that the name is properly capitalized unless CAPITAL is allowed.
	
	if ( $editorlast[$i] !~ / \pL .* \pL /xs )
	{
	    $edt->add_condition('E_NAME_EMPTY', 'Editor', $i+1);
	    next;
	}
	
	elsif ( ! $edt->allows('CAPITAL') )
	{
	    if ( $editorlast !~ / ^ \p{Lu} .* \p{Ll} /xs )
	    {
		$edt->add_condition('C_CAPITAL', 'Editor', $i+1);
	    }
	    
	    if ( defined $editorfirst && $editorfirst ne '' && 
		 $editorfirst !~ / ^ \p{Lu} .* \p{Ll} /xs )
	    {
		$edt->add_condition('C_CAPITAL', 'Editor', $i+1);
	    }
	}
	
	my $suffix;
	
	my $initials = '';
	
	# If a first name is given, generate initials.
	
	if ( $editorfirst )
	{
	    foreach my $word ( split(/\s+/, $editorfirst ) )
	    {
		if ( $word =~ /(\p{L})/ )
		{
		    $initials .= "$1. ";
		}
	    }
	    
	    $initials =~ s/ $//;
	}
	
	push @ref_editors, ($initials ? "$initials $editorlast" : $editorlast);
    }
    
    $record->{editors} = join(', ', @ref_editors) if $editor_list;
    # $record->{editorfirst} = \@editorfirst;
    # $record->{editorlast} = \@editorlast;
    # $record->{n_editors} = scalar(@editorname);
    
    # $action->handle_column("FIELD:editorfirst", 'ignore');
    # $action->handle_column("FIELD:editorlast", 'ignore');
    # $action->handle_column("FIELD:n_editors", 'ignore');
    
    # $action->ignore_field('editorfirst');
    # $action->ignore_field('editorlast');
    # $action->ignore_field('n_editors');
    
    # Handle the page number(s).
    
    if ( $record->{pages} =~ /(.*)\s*-\s*(.*)/ )
    {
	$record->{firstpage} = $1;
	$record->{lastpage} = $2;
    }
    
    elsif ( $record->{pages} )
    {
	$record->{firstpage} = $record->{pages};
	$record->{lastpage} = undef;
    }
    
    elsif ( exists $record->{pages} )
    {
	$record->{firstpage} = undef;
	$record->{lastpage} = undef;
    }
    
    # Check that the other fields are string valued, and change empty string
    # values to null.
    
    foreach my $field ( qw(publication_type reftitle pubtitle pubvol pubno language
			   doi publisher pubcity isbn project_name comments) )
    {
	if ( ref $record->{$field} )
	{
	    $edt->add_condition('E_FORMAT', $field, "must be a string value");
	}
	
	elsif ( exists $record->{$field} && $record->{$field} eq '' )
	{
	    $record->{$field} = undef;
	}
    }
    
    # If the operation is 'insert' and this transaction does not allow
    # duplicates, check to see if the new record duplicates an existing one. If
    # so, throw a caution.
    
    if ( $operation eq 'insert' && $action->can_proceed && ! $edt->allows('DUPLICATE') )
    {
	my $test = { %$record };
	
	my (@duplicate_nos) = $edt->my_duplicate_check($test);
	
	if ( @duplicate_nos )
	{
	    my $dup_string = join(',', map { generate_identifier('REF', $_) } @duplicate_nos);
	    
	    $edt->add_condition($action, 'C_DUPLICATE', $dup_string);
	    return;
	}
    }
    
};


sub after_action {
    
    my ($edt, $action, $operation, $table_specifier) = @_;
    
    my $keyval = $action->keyval;
    my $record = $action->record;
    my $dbh = $edt->dbh;
    my $qkeyval = $dbh->quote($keyval);
    my $result;
    
    if ( $operation eq 'replace' || $operation eq 'delete' ||
	 ($operation eq 'update' && $record->{n_authors}) )
    {
	my $sql = "DELETE FROM $TABLE{REFERENCE_AUTHORS} WHERE reference_no = $qkeyval";
	
	$edt->debug_line("$sql\n\n");
	
	$result = $dbh->do($sql);
    }
    
    if ( $operation ne 'delete' )
    {
	my @author_records;
	
	foreach my $i ( 0 .. $record->{n_authors} - 1 )
	{
	    my $place = $i + 1;
	    my $qfirst = $dbh->quote($record->{authorfirst}[$i]);
	    my $qlast = $dbh->quote($record->{authorlast}[$i]);
	    
	    push @author_records, "($qkeyval,$place,$qfirst,$qlast)";
	}
	
	# foreach my $i ( 0 .. $record->{n_editors} - 1 )
	# {
	# 	my $place = $i + 1;
	# 	my $qfirst = $dbh->quote($record->{editorfirst}[$i]);
	# 	my $qlast = $dbh->quote($record->{editorlast}[$i]);
	
	# 	push @author_records, "($qkeyval,'editor',$place,$qfirst,$qlast)";
	# }
	
	if ( @author_records )
	{
	    my $authorstring = join(',', @author_records);
	    
	    my $sql = "INSERT INTO $TABLE{REFERENCE_AUTHORS}
		    (reference_no, place, firstname, lastname)
		    VALUES $authorstring";
	    
	    $edt->debug_line("$sql\n\n");
	    
	    $result = $dbh->do($sql);
	}
    }
}


# able_to_delete ( )
# 
# Return true if the specified record(s) is not referenced by any record in any of
# the core database tables, false otherwise.

sub able_to_delete {
    
    my ($edt, $action) = @_;
    
    my $dbh = $edt->dbh;
    
    my @quoted = map { $dbh->quote($_) } $action->keyvalues;
    
    my $idstring = join(',', @quoted);
    
    return () if $edt->my_reference_check($dbh, $TABLE{COLLECTION_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{OCCURRENCE_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{SPECIMEN_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{AUTHORITY_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{OPINION_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{INTERVAL_DATA}, $idstring);
    return () if $edt->my_reference_check($dbh, $TABLE{SCALE_DATA}, $idstring);
    
    return 1;
}


sub my_reference_check {
    
    my ($edt, $dbh, $table_name, $idstring) = @_;
    
    my $sql = "SELECT count(*) FROM $table_name WHERE reference_no in ($idstring)";
    
    $edt->debug_line($sql) if $edt->debug_mode;
    
    my ($count) = $dbh->selectrow_array($sql);
    
    return !$count;
}
    

# my_duplicate_check ( attrs )
#
# Given a hashref of reference attributes in $attrs, check and see if this set of attributes
# matches any records currently in the REFERENCES table. If so, return the reference_no of the
# most likely match, along with an estimated probability of a duplication.

sub my_duplicate_check {

    my ($edt, $attrs) = @_;
    
    my $dbh = $edt->dbh;
    
    my @matches;
    
    # Start by matching against the reftitle and pubtitle fields with a pubyr
    # filter.  If a DOI is provided, use that for the initial search as well.
    # But if the search returns no results, try it again without the DOI.
    
    @matches = $edt->my_reftitle_query($attrs, { doi => 1 });
    
    if ( !@matches && $attrs->{doi} )
    {
	@matches = $edt->my_reftitle_query($attrs, { });
    }
    
    # If we can't find any matches at all, try again with just the DOI and see
    # if we get any hits.
    
    if ( !@matches && $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	
	my $sql = "SELECT * FROM refs WHERE doi=$quoted";
	
	@matches = $edt->my_sql_query($dbh, $sql);
    }
    
    # If we haven't found any matches at this point, we can stop.
    
    return @matches;
}


sub my_reftitle_query {
    
    my ($edt, $attrs, $options) = @_;
    
    my $dbh = $edt->dbh;
    
    my ($fulltext, $having);
    
    my $ref_title = $attrs->{reftitle};
    my $pub_title = $attrs->{pubtitle};
    
    if ( $ref_title && $pub_title )
    {
	my $refquoted = $dbh->quote($ref_title);
	my $pubquoted = $dbh->quote($pub_title);
	
	$fulltext = "match(r.reftitle) against($refquoted) as score1,
		  match(r.pubtitle) against ($pubquoted) as score2";
	$having = "score1 > 5 and score2 > 5";
    }
    
    elsif ( $ref_title )
    {
	my $quoted = $dbh->quote($ref_title);
	
	$fulltext = "match(r.reftitle) against($quoted) as score";
	$having = "score > 5";
    }
    
    elsif ( $pub_title )
    {
	my $quoted = $dbh->quote($pub_title);
	
	$fulltext = "match(r.pubtitle) against($quoted) as score";
	$having = "score > 0";
    }
    
    my $pubyr_quoted = $dbh->quote($attrs->{pubyr});
    
    my $sql = "SELECT r.*, $fulltext
		FROM $TABLE{REFERENCE_DATA} as r
		WHERE r.pubyr = $pubyr_quoted
		HAVING $having";
    
    # my $sql = "SELECT r.*, $fulltext
    # 		FROM $TABLE{REFERENCE_DATA} as r
    # 		     join $TABLE{REFERENCE_SEARCH} as rs using (reference_no)
    # 		WHERE r.pubyr = $pubyr_quoted
    # 		HAVING $having";
    
    my @raw = $edt->my_sql_query($dbh, $sql);
    
    my @matches;
    
    foreach my $r ( @raw )
    {
	my $similarity = ref_similarity($r, $attrs, { });
	
	# if ( $similarity->{sum_s} > 500 )
	# {
	#     return $r->{reference_no};
	# }
	
	if ( $similarity->{sum_s} > 400 & $similarity->{sum_c} < 200 )
	{
	    push @matches, $r->{reference_no};
	}
    }
    
    return @matches;
}


sub my_sql_query {
    
    my ($edt, $dbh, $sql) = @_;
    
    $edt->debug_line($sql) if $edt->debug_mode;
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result eq 'ARRAY' )
    {
	return @$result;
    }
    
    else
    {
	return ();
    }
}



1;
