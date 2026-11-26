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
use TableDefs qw(%TABLE);
use CoreTableDefs;

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;


# The tables written by this module, including those written through MatrixBase and by
# the data service operations that use it. The pe2 tests switch every one of them to the
# test database with PBDBTester::select_module_tables, so a table written by new code
# must be added here. [Added by Claude under the guidance of Michael McClennen]

our (@MODULE_WRITES) = qw(REFERENCE_DATA REFERENCE_AUTHORS REFERENCE_EDITORS);


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
    
    ReferenceEdit->register_allowances('DUPLICATE', 'IGNORE-DUPLICATE', 'CAPITAL');
    
    # [Changed by Claude under the guidance of Michael McClennen: the table definitions that
    # were here have been moved to CoreTableDefs.pm.]
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action, keyexpr )
# 
# This method is called from EditTransaction.pm to validate each action. We override it to do
# additional checks.

sub validate_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    # Ignore operations on tables other than REFERENCE_DATA.
    
    return unless $table eq 'REFERENCE_DATA';
    
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
	# [Changed by Claude under the guidance of Michael McClennen: this commented-out query
	# uses the REFERENCE_DATA table instead of the table name 'refs'.]
	
	# my $dbh = $edt->dbh;
	# my $sql = "SELECT publication_type FROM $TABLE{REFERENCE_DATA} WHERE $keyexpr";
	
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
	
	# [Changed by Claude under the guidance of Michael McClennen: each string in a
	# list is split on semicolons, as a single string is. The data service always
	# passes a list, so a string of names separated by semicolons was taken as a
	# single name.]
	
	if ( ref $author_list eq 'ARRAY' )
	{
	    @authorname = map { ref $_ ? $_ : split /\s*;\s*/ } $author_list->@*;
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
    
    # [Changed by Claude under the guidance of Michael McClennen: an 'editors' value that is
    # neither a string nor a list of strings is reported as E_FORMAT, the same as for
    # 'authors'. It threw an exception before.]
    
    if ( ref $record->{editors} && ref $record->{editors} ne 'ARRAY' )
    {
	$edt->add_condition('E_FORMAT', 'editors', 'must be a string or an array of strings');
    }
    
    elsif ( exists $record->{editors} )
    {
	my $editor_list = $record->{editors};
	my (@editorname, @editorfirst, @editorlast);
	
	# [Changed by Claude under the guidance of Michael McClennen: each string in a
	# list is split on semicolons, as for the authors.]
	
	if ( ref $editor_list eq 'ARRAY' )
	{
	    @editorname = map { ref $_ ? $_ : split /\s*;\s*/ } $editor_list->@*;
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
	    
	    # If a name includes an asterisk '*', treat it as a last name
	    # only. Remove the asterisk, of course.
	    
	    my ($editorfirst, $editorlast);
	    
	    if ( $editorname[$i] =~ /[*]/ )
	    {
		$editorfirst = '';
		$editorlast = $editorname[$i];
		$editorlast =~ s/\s*[*]\s*//g;
	    }
	    
	    # Otherwise, parse the name into first and last.
	    
	    else
	    {
		($editorlast, $editorfirst) = parse_authorname($editorname[$i]);
	    }
	    
	    $editorfirst[$i] = $editorfirst;
	    $editorlast[$i] = $editorlast;
	    
	    # Make sure that each editor has at least two letters in the last name,
	    # and that the name is properly capitalized unless CAPITAL is allowed.
	    
	    if ( $editorlast !~ / \pL .* \pL /xs )
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
	
	$record->{editors} = join(', ', @ref_editors);
	
	$record->{editorfirst} = \@editorfirst;
	$record->{editorlast} = \@editorlast;
	$record->{n_editors} = scalar(@editorname);
	
	$action->ignore_field('editorfirst');
	$action->ignore_field('editorlast');
	$action->ignore_field('n_editors');
    }
    
    # Handle the page number(s).
    
    # [Changed by Claude under the guidance of Michael McClennen: spaces around the dash, and
    # at either end, are not kept in the page numbers.]
    
    if ( $record->{pages} =~ /^\s*(.*?)\s*-\s*(.*?)\s*$/ )
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
    # duplicates, check to see if the new record duplicates an existing one.
    
    if ( $operation eq 'insert' && $action->can_proceed && ! $edt->allows('DUPLICATE') )
    {
	my $test = { %$record };
	
	my (@duplicate_nos) = $edt->my_duplicate_check($test);
	
	if ( @duplicate_nos )
	{
	    # If this transaction allows ignoring duplicates, then abort this action and
	    # set the action result to the duplicate list.
	    
	    if ( $edt->allows('IGNORE-DUPLICATE') )
	    {
		$action->set_result(\@duplicate_nos);
		$edt->abort_action();
	    }
	    
	    # Otherwise, add a C_DUPLICATE caution to the transaction. The caution
	    # message will contain external identifiers, because it is intended to be
	    # sent back to the client whose request initiated this transaction.
	    
	    else
	    {
		my $dup_string = join(',', map { generate_identifier('REF', $_) } @duplicate_nos);
		$edt->add_condition($action, 'C_DUPLICATE', $dup_string);
	    }
	    
	    return;
	}
    }
    
};


sub after_action {
    
    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # Ignore actions on tables other than REFERENCE_DATA.
    
    return unless $table_specifier eq 'REFERENCE_DATA';
    
    my $keyval = $action->keyval;
    my $keyexpr = $action->keyexpr;
    my $record = $action->record;
    my $dbh = $edt->dbh;
    my $qkeyval = $dbh->quote($keyval);
    my $tableinfo = $edt->table_info_ref('REFERENCE_DATA');
    my $result;
    
    if ( $operation eq 'replace' || $operation eq 'delete' ||
	 ($operation eq 'update' && exists $record->{n_authors}) )
    {
	# [Changed by Claude under the guidance of Michael McClennen: the rows to be deleted are
	# read before deleting them, so that the deletion can be logged with a statement that
	# would put them back. They were read afterward, so they were never found.]
	
	my $old_rows;
	
	if ( $tableinfo->{LOG_CHANGES} )
	{
	    $old_rows = $dbh->selectall_arrayref("SELECT * FROM $TABLE{REFERENCE_AUTHORS}
			WHERE $keyexpr", { Slice => { } });
	}
	
	my $sql = "DELETE FROM $TABLE{REFERENCE_AUTHORS} WHERE $keyexpr";
	
	$edt->debug_line("$sql\n\n");
	
	$result = $dbh->do($sql);
	
	if ( ref $old_rows eq 'ARRAY' && @$old_rows )
	{
	    $edt->log_aux_event('delete', 'REFERENCE_AUTHORS', $sql,
				"reference_no = '$keyval'", $old_rows);
	}
    }
    
    if ( $operation eq 'replace' || $operation eq 'delete' ||
	 ($operation eq 'update' && exists $record->{n_editors}) )
    {
	# [Changed by Claude under the guidance of Michael McClennen: the rows to be deleted are
	# read before deleting them, so that the deletion can be logged with a statement that
	# would put them back. They were read afterward, so they were never found.]
	
	my $old_rows;
	
	if ( $tableinfo->{LOG_CHANGES} )
	{
	    $old_rows = $dbh->selectall_arrayref("SELECT * FROM $TABLE{REFERENCE_EDITORS}
			WHERE $keyexpr", { Slice => { } });
	}
	
	my $sql = "DELETE FROM $TABLE{REFERENCE_EDITORS} WHERE $keyexpr";
	
	$edt->debug_line("$sql\n\n");
	
	$result = $dbh->do($sql);
	
	if ( ref $old_rows eq 'ARRAY' && @$old_rows )
	{
	    $edt->log_aux_event('delete', 'REFERENCE_EDITORS', $sql,
				"reference_no = '$keyval'", $old_rows);
	}
    }
    
    if ( $operation ne 'delete' )
    {
	my (@author_records, @editor_records);
	
	if ( $record->{n_authors} )
	{
	    foreach my $i ( 0 .. $record->{n_authors} - 1 )
	    {
		my $place = $i + 1;
		my $qfirst = $dbh->quote($record->{authorfirst}[$i]);
		my $qlast = $dbh->quote($record->{authorlast}[$i]);
		
		push @author_records, "($qkeyval,$place,$qfirst,$qlast)";
	    }
	    
	    my $authorstring = join(',', @author_records);
	    
	    my $sql = "INSERT INTO $TABLE{REFERENCE_AUTHORS}
		    (reference_no, place, firstname, lastname)
		    VALUES $authorstring";
	    
	    $edt->debug_line("$sql\n\n");
	    
	    $result = $dbh->do($sql);
	    
	    if ( $tableinfo->{LOG_CHANGES} )
	    {
		$edt->log_aux_event('insert', 'REFERENCE_AUTHORS', $sql,
				    "reference_no = '$keyval'");
	    }
	}
	
	if ( $record->{n_editors} )
	{
	    foreach my $i ( 0 .. $record->{n_editors} - 1 )
	    {
		my $place = $i + 1;
		my $qfirst = $dbh->quote($record->{editorfirst}[$i]);
		my $qlast = $dbh->quote($record->{editorlast}[$i]);
		
		push @editor_records, "($qkeyval,$place,$qfirst,$qlast)";
	    }
	    
	    my $editorstring = join(',', @editor_records);
	    
	    my $sql = "INSERT INTO $TABLE{REFERENCE_EDITORS}
		    (reference_no, place, firstname, lastname)
		    VALUES $editorstring";
	    
	    $edt->debug_line("$sql\n\n");
	    
	    $result = $dbh->do($sql);
	    
	    if ( $tableinfo->{LOG_CHANGES} )
	    {
		$edt->log_aux_event('insert', 'REFERENCE_EDITORS', $sql,
				    "reference_no = '$keyval'");
	    }
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
    
    return () if $edt->ref_has_dependency($dbh, $TABLE{COLLECTION_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{COLLECTION_REFS}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{OCCURRENCE_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{SPECIMEN_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{AUTHORITY_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{OPINION_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{INTERVAL_DATA}, $idstring);
    return () if $edt->ref_has_dependency($dbh, $TABLE{SCALE_DATA}, $idstring);
    
    return 1;
}


sub ref_has_dependency {
    
    my ($edt, $dbh, $table_name, $idstring) = @_;
    
    my $sql = "SELECT 1 FROM $table_name WHERE reference_no in ($idstring) limit 1";
    
    $edt->debug_line($sql) if $edt->debug_mode;
    
    my ($found) = $dbh->selectrow_array($sql);
    
    return $found;
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
    # filter.
    # 
    # [Changed by Claude under the guidance of Michael McClennen: the query was done a
    # second time if a DOI was given and nothing matched. But my_reftitle_query does not use
    # the DOI, so the second query was the same as the first.]
    
    @matches = $edt->my_reftitle_query($attrs, { });
    
    # If we can't find any matches at all, try again with just the DOI and see
    # if we get any hits.
    
    if ( !@matches && $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	
	# [Changed by Claude under the guidance of Michael McClennen: this used the table name
	# 'refs' instead of the REFERENCE_DUPLICATE_SEARCH table.]
	
	my $sql = "SELECT reference_no FROM $TABLE{REFERENCE_DUPLICATE_SEARCH} WHERE doi=$quoted";
	
	@matches = grep { $_ } map { $_->{reference_no} } $edt->my_sql_query($dbh, $sql);
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
    
    # [Added by Claude under the guidance of Michael McClennen: if neither title was given,
    # there is nothing to match. The query below would be malformed.]
    
    else
    {
	return;
    }
    
    my $pubyr_quoted = $dbh->quote($attrs->{pubyr});
    
    # [Changed by Claude under the guidance of Michael McClennen: the search uses
    # REFERENCE_DUPLICATE_SEARCH, which is the same table as REFERENCE_DATA except when
    # testing. See CoreTableDefs.pm.]
    
    my $sql = "SELECT r.*, $fulltext
		FROM $TABLE{REFERENCE_DUPLICATE_SEARCH} as r
		WHERE r.pubyr = $pubyr_quoted
		HAVING $having";
    
    # [Changed by Claude under the guidance of Michael McClennen: this commented-out query
    # searches REFERENCE_DUPLICATE_SEARCH, like the one above.]
    
    # my $sql = "SELECT r.*, $fulltext
    # 		FROM $TABLE{REFERENCE_DUPLICATE_SEARCH} as r
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
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
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
