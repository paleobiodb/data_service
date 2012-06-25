#
# TaxonQuery
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataQuery.
# 
# Author: Michael McClennen

package TaxonQuery;

use strict;
use base 'DataQuery';



# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass to set any parameters it recognizes.
    
    $self->DataQuery::setParameters($params);
    
    # The 'parent' parameter restricts the output to children of the specified
    # taxon.  This allows us to query a subset of the taxa tree.  This can be
    # applied to both multi-taxon and single-taxon queries; in the latter
    # case, no result will be returned if the requested taxon is not a descendant
    # of the 'parent' taxon.
    
    if ( defined $params->{parent} && ($params->{parent} + 0) > 0 )
    {
	$self->{parent_taxon} = $params->{parent} + 0;
    }
    
    # The 'type' parameter selects the category of taxa to return: 'valid' is
    # the default, and returns only taxa that are valid and are not junior
    # synonyms of other taxa.  'synonyms' is a little more broad,
    # returning all valid taxa even if they are junior synonyms.  'invalid'
    # returns only invalid taxa, while 'all' returns all taxa regardless of
    # their validity.
    # 
    # This parameter has no effect for single-taxon queries.
    
    if ( defined $params->{type} )	# type=synonyms is the default
    {
	$self->{taxon_type} = 'invalid' if lc $params->{type} eq 'invalid';
	$self->{taxon_type} = 'valid' if lc $params->{type} eq 'valid';
	$self->{taxon_type} = 'all' if lc $params->{type} eq 'all';
	$self->{taxon_type} = 'synonyms' if lc $params->{type} eq 'synonyms';
	$self->{taxon_type} = 'synonyms' unless $self->{taxon_type};
    }
    
    # If 'exact' is specified, then information is returned about the exact taxon
    # specified.  Otherwise (default) information is returned about the senior
    # synonym or correct spelling of the specified taxon if such exist.
    
    if ( defined $params->{exact} && $params->{exact} ne '' )
    {
	$self->{fetch_exact} = 1;
    }
    
    # If 'rank' is specified, then information is returned only about taxa of
    # the given rank.  Multiple ranks can be specified, separated by commas.
    
    if ( defined $params->{rank} && $params->{rank} ne '' )
    {
	$self->{rank} = $params->{rank};
    }
    
    # If 'prefix' is specified, then information is returned only about taxa
    # whose name starts with the given prefix.
    
    if ( defined $params->{prefix} && $params->{prefix} ne '' )
    {
	$self->{prefix_match} = $params->{prefix};
    }
    
    # If 'match' is specified, then information is returned only about taxa
    # whose name matches the given value.  The value will be matched against
    # the database with a wildcard added at the beginning and end.
    
    if ( defined $params->{match} && $params->{match} ne '' )
    {
	$self->{name_match} = $params->{match};
	delete $self->{prefix_match};
    }
    
    # If 'rewrite' is specified, then rewrite the classification of genus and
    # species taxa to match their names.  This is required in order to match
    # the output of the existing PaleoDB website.
    
    if ( defined $params->{rewrite} && $params->{rewrite} )
    {
	$self->{rewrite_classification} = 1;
    }
    
    # If 'showref' is specified, then include publication references for each
    # taxon. 
    
    if ( defined $params->{showref} )
    {
	$self->{include_ref} = 1;
    }
    
    # If 'showcode' is specified, then include nomenclatural code info for
    # each taxon.
    
    if ( defined $params->{showcode} )
    {
	$self->{include_nc} = 1;
    }
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database for basic info about all taxa satisfying the conditions
# previously specified (i.e. by a call to setParameters).
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@taxon_filter, @extra_tables, @extra_fields);
    my ($taxon_limit) = "";
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL string.
    
    if ( defined $self->{limit} && $self->{limit} > 0 )
    {
	$taxon_limit = "LIMIT " . $self->{limit};
    }
    
    # If we need to show the reference for each taxon, include the necessary
    # fields. 
    
    if ( defined $self->{include_ref} )
    {
	push @extra_fields, "r.author1init r_a1i", "r.author1last r_a1l",
	    "r.author2init r_a2i", "r.author2last r_a2l", "r.otherauthors r_otherauthors",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    # If we need to show the nomenclatural code under which each taxon falls,
    # include the necessary fields.  We also need to get the tree-range for
    # the kingdom-level taxa Metazoa, Plantae and Metaphyta.
    
    if ( defined $self->{include_nc} )
    {
	push @extra_fields, "t.lft";
	$self->getCodeRanges();
    }
    
    # If a restriction has been entered on the category of taxa to be
    # returned, add the appropriate filter.
    
    if ( defined $self->{taxon_type} )
    {
	if ( $self->{taxon_type} eq 'valid' ) {
	    push @taxon_filter, "o.status = 'belongs to'";
	    push @taxon_filter, "t.taxon_no = t.synonym_no";
	}
	elsif ( $self->{taxon_type} eq 'synonyms' ) {
	    push @taxon_filter, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
	    push @taxon_filter, "t.taxon_no = t.spelling_no";
	}
	elsif ( $self->{taxon_type} eq 'invalid' ) {
	    push @taxon_filter, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
	}
    }
    
    # If the result set is to be restricted to the descendants of a specified
    # taxon, add the appropriate filter.
    
    if ( defined $self->{parent_taxon} and $self->{parent_taxon} > 0 )
    {
	my $orig = $self->{parent_taxon} + 0;
	my $parent = $dbh->selectrow_array("SELECT synonym_no FROM taxa_tree_cache WHERE taxon_no = $orig");
	push @extra_tables, "JOIN taxa_list_cache l ON l.child_no = taxon_no";
	push @taxon_filter, "l.parent_no = " . ($parent + 0);
    }
    
    # If the result set is to be restricted by name matching, add the
    # appropriate filters.
    
    if ( defined $self->{name_match} )
    {
	my $match_filter = 'a.taxon_name LIKE ' . $dbh->quote('%' . $self->{name_match} . '%');
	push @taxon_filter, $match_filter;
    }
    
    elsif ( defined $self->{prefix_match} )
    {
	my $match_filter = 'a.taxon_name LIKE ' . $dbh->quote($self->{prefix_match} . '%');
	push @taxon_filter, $match_filter;
    }
    
    # If the result set is to be restricted to taxa of a given rank or ranks,
    # add the appropriate filters.
    
    if ( defined $self->{rank} )
    {
	if ( $self->{rank} !~ /,/ )
	{
	    push @taxon_filter, 'a.taxon_rank = ' . $dbh->quote($self->{rank});
	}
	
	else
	{
	    my @ranks = split /,/, $self->{rank};
	    my $rank_filter = 'a.taxon_rank IN (';
	    my $separator = '';
	    foreach my $r (@ranks)
	    {
		$rank_filter .= $separator . $dbh->quote($r);
		$separator = ',';
	    }
	    push @taxon_filter, ($rank_filter . ')');
	}
    }
    
    # Now construct the filter expression and extra_tables expression
    
    my $taxon_filter = join(' AND ', @taxon_filter);
    $taxon_filter = "WHERE $taxon_filter" if $taxon_filter ne '';
    
    my $extra_tables = join('', @extra_tables);
    
    # and the extra_fields expression
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Counting the total number of rows is too expensive for any category of
    # taxa except 'all'.
    
    $self->{total_count} = $dbh->selectrow_array("SELECT count(a.taxon_no) FROM authorities a")
	if defined $self->{taxon_type} && $self->{taxon_type} eq 'all';
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $sql = 
	"SELECT a.taxon_no, a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status, o.spelling_reason,
		pa.taxon_no as parent_no, pa.taxon_name as parent_name, t.spelling_no, t.synonym_no, t.orig_no,
		xa.taxon_no as accepted_no, xa.taxon_name as accepted_name,
	        IF (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
		    a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) pubyr,
		IF (a.ref_is_authority = 'YES', r.author1last, a.author1last) author1,
		IF (a.ref_is_authority = 'YES', r.author2last, a.author2last) author2,
		IF (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) otherauthors
		$extra_fields
         FROM authorities a JOIN taxa_tree_cache t USING (taxon_no) $extra_tables
		LEFT JOIN refs r USING (reference_no)
		LEFT JOIN opinions o USING (opinion_no)
		LEFT JOIN authorities xa ON (xa.taxon_no = CASE
		    WHEN status <> 'belongs to' AND status <> 'invalid subgroup of' THEN o.parent_spelling_no
		    WHEN t.taxon_no <> t.synonym_no THEN t.synonym_no END)
		LEFT JOIN (taxa_tree_cache pt JOIN authorities pa ON (pa.taxon_no = pt.synonym_no))
		    ON (pt.taxon_no = if(o.status = 'belongs to', o.parent_spelling_no, null))
	 $taxon_filter ORDER BY t.lft ASC $taxon_limit";
    
    $self->{main_sth} = $dbh->prepare($sql);
    $self->{main_sth}->execute();
    
    # Indicate that output should be streamed rather than assembled and
    # returned immediately.  This will avoid a huge burden on the server to
    # marshall a single result string from a potentially large set of data.
    
    $self->{streamOutput} = 1;
    $self->{processMethod} = 'processRow';
    
    return 1;
}


# fetchSingle ( taxon_requested )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class DataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchSingle {

    my ($self, $taxon_requested) = @_;
    my ($sql, @extra_fields);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Unless we were directed to use the exactly specified taxon, first get
    # the senior synonym.  Because the synonym_no field is not always correct,
    # we may need to iterate this query.  In order to save time, we do three
    # iterations in one query.
    
    unless ( $self->{fetch_exact} )
    {
	my $sql =
	    "SELECT t3.taxon_no, t3.synonym_no
	     FROM taxa_tree_cache t1 JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
				     JOIN taxa_tree_cache t3 ON t3.taxon_no = t2.synonym_no
	     WHERE t1.taxon_no = ?";

	my $prev; my $count = 0;
	
	do {
	    ($prev, $taxon_requested) = $dbh->selectrow_array($sql, undef, $taxon_requested + 0);
	} while ( $prev != $taxon_requested and $count++ < 3 );
    }
    
    # If we need to show the reference for each taxon, include the necessary
    # fields. 
    
    if ( defined $self->{include_ref} )
    {
	push @extra_fields, "r.author1init r_a1i", "r.author1last r_a1l",
	    "r.author2init r_a2i", "r.author2last r_a2l", "r.otherauthors r_otherauthors",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    # If we need to show the nomenclatural code under which the taxon falls,
    # include the necessary fields.  We also need to get the tree-range for
    # Metazoa and Plantae.
    
    if ( defined $self->{include_nc} )
    {
	push @extra_fields, "t.lft";
	$self->getCodeRanges();
    }
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Next, fetch basic info about the taxon.
    
    $sql = 
	"SELECT a.taxon_no, a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status, o.spelling_reason,
		pa.taxon_no as parent_no, pa.taxon_name as parent_name, t.spelling_no, t.synonym_no, t.orig_no,
		xa.taxon_no as accepted_no, xa.taxon_name as accepted_name,
	        IF (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
		    a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) pubyr,
		IF (a.ref_is_authority = 'YES', r.author1last, a.author1last) author1,
		IF (a.ref_is_authority = 'YES', r.author2last, a.author2last) author2,
		IF (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) otherauthors
		$extra_fields
         FROM authorities a JOIN taxa_tree_cache t USING (taxon_no)
	       	LEFT JOIN refs r USING (reference_no)
		LEFT JOIN opinions o USING (opinion_no)
		LEFT JOIN authorities xa ON (xa.taxon_no = CASE
		    WHEN status <> 'belongs to' AND status <> 'invalid subgroup of' THEN o.parent_spelling_no
		    WHEN t.taxon_no <> t.synonym_no THEN t.synonym_no END)
		LEFT JOIN (taxa_tree_cache pt JOIN authorities pa ON (pa.taxon_no = pt.synonym_no))
		    ON (pt.taxon_no = if(o.status = 'belongs to', o.parent_spelling_no, null))
         WHERE a.taxon_no = ?";
    
    my $sth = $dbh->prepare($sql);
    $sth->execute($taxon_requested);
    
    my ($main_row) = $sth->fetchrow_hashref();
    
    $self->processRow($main_row);
    $self->{main_row} = $main_row;
    
    # Make sure we know the senior synonym of this taxon (this is relevant
    # only if the 'fetch_exact' flag is true) so that the parent list will
    # turn out properly.
    
    my ($senior_synonym) = $main_row->{synonym_no};
    
    # Now fetch all parent info
    
    $sql = 
       "SELECT a.taxon_no, a.taxon_name, a.taxon_rank, a.common_name, a.extant, 
		pt.synonym_no as parent_no, t.orig_no,
		IF (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
		    a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) pubyr,
		IF (a.ref_is_authority = 'YES', r.author1last, a.author1last) author1,
		IF (a.ref_is_authority = 'YES', r.author2last, a.author2last) author2,
		IF (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) otherauthors
	FROM taxa_list_cache l JOIN taxa_tree_cache t ON (t.taxon_no = l.parent_no)
		JOIN authorities a ON a.taxon_no = t.taxon_no
		LEFT JOIN refs r USING (reference_no)
		LEFT JOIN opinions o USING (opinion_no)
		LEFT JOIN taxa_tree_cache pt ON (pt.taxon_no = o.parent_spelling_no)
	WHERE l.child_no = ? AND l.parent_no <> l.child_no ORDER BY t.lft ASC";

    $sth = $dbh->prepare($sql);
    $sth->execute($senior_synonym);
    
    my $parent_list = $sth->fetchall_arrayref({});
    
    # Run through the parent list and note when we reach the last
    # kingdom-level taxon.  Any entries before that point are dropped 
    # [note: TaxonInfo.pm, line 1316]
    
    my $last_kingdom = 0;
    
    for (my $i = 0; $i < scalar(@$parent_list); $i++)
    {
	$last_kingdom = $i if $parent_list->[$i]{taxon_rank} eq 'kingdom';
    }
    
    splice(@$parent_list, 0, $last_kingdom) if $last_kingdom > 0;
    
    $self->{parents} = $parent_list;
    
    # If this is a species or genus, we may need to rewrite the parent list
    # according to the name given for the main taxon.
    
    if ( $self->{rewrite_classification} )
    {
	my ($genus, $subgenus, $species, $subspecies) = 
	    interpretSpeciesName($main_row->{taxon_name});
	
	# Create a new parent list, and copy all taxa of family or higher rank
	# to it.  Taxa of genus and lower rank are used to populate the
	# taxon_no_by_rank table.
	
	$self->{parents} = [];
	my ($taxon_row_by_rank) = {};
	
	foreach my $row (@$parent_list)
	{
	    unless ( $row->{taxon_rank} =~ /species|genus/ )
	    {
		push @{$self->{parents}}, $row;
	    }
	    
	    else
	    {
		$taxon_row_by_rank->{$row->{taxon_rank}} = $row;
	    }
	}
	
	# Now add the genus, but with the genus name extracted from the current
	# taxon's name.  If one wasn't found, use the current taxon.
	
	if ( defined $taxon_row_by_rank->{'genus'} )
	{
	    my $genus_row = $taxon_row_by_rank->{'genus'};
	    $genus_row->{taxon_name} = $genus;
	    push @{$self->{parents}}, $genus_row;
	}
	
	else
	{
	    push @{$self->{parents}}, $main_row;
	}
	
	# Then, add the subgenus, species and subspecies only if they occur in
	# the current taxon's name.
	
	if ( defined $subgenus and $subgenus ne '' )
	{
	    my $subgenus_row = ( defined $taxon_row_by_rank->{'subgenus'} ?
				 $taxon_row_by_rank->{'subgenus'} :
				 { taxon_rank => 'subgenus' } );
	    $subgenus_row->{taxon_name} = "$genus ($subgenus)";
	    push @{$self->{parents}}, $subgenus_row;
	}
	
	if ( defined $species and $species ne '' )
	{
	    my $species_row = ( defined $taxon_row_by_rank->{'species'} ?
				$taxon_row_by_rank->{'species'} :
				{ taxon_rank => 'species' } );
	    $species_row->{taxon_name} = "$genus $species";
	    if ( defined $subgenus and $subgenus ne '' ) {
		$species_row->{taxon_name} = "$genus ($subgenus) $species";
	    }
	    push @{$self->{parents}}, $species_row;
	}
	
	if ( defined $subspecies and $subspecies ne '' )
	{
	    my $subspecies_row = ( defined $taxon_row_by_rank->{'subspecies'} ?
				   $taxon_row_by_rank->{'subspecies'} :
				   { taxon_rank => 'subspecies' } );
	    $subspecies_row->{taxon_name} = $main_row->{taxon_name};
	    push @{$self->{parents}}, $subspecies_row;
	}
    }
    
    # Take off the last entry on the parent list if it is a duplicate of the
    # main row, unless we have been told not to.
    
    unless ( $self->{dont_trim_parent_list} or scalar(@{$self->{parents}}) == 0 )
    {
	pop @{$self->{parents}} if $self->{parents}[-1]{taxon_rank} eq $self->{main_row}{taxon_rank};
    }
    
    # Return success
    
    return 1;
}


# getCodeRanges ( )
# 
# Fetch the ranges necessary to determine which nomenclatural code (i.e. ICZN,
# ICN) applies to any given taxon.  This is only done if that information is
# asked for.

my @codes = ('Metazoa', 'Animalia', 'Plantae', 'Biliphyta', 'Metaphytae',
	     'Fungi', 'Cyanobacteria');

my $codes = { Metazoa => { code => 'ICZN'}, 
	      Animalia => { code => 'ICZN'},
	      Plantae => { code => 'ICN'}, 
	      Biliphyta => { code => 'ICN'},
	      Metaphytae => { code => 'ICN'},
	      Fungi => { code => 'ICN'},
	      Cyanobacteria => { code => 'ICN' } };

sub getCodeRanges {

    my ($self) = @_;
    my ($dbh) = $self->{dbh};
    
    $self->{code_ranges} = $codes;
    $self->{code_list} = \@codes;
    
    my $code_name_list = "'" . join("','", @codes) . "'";
    
    my $code_range_query = $dbh->prepare("
	SELECT taxon_name, lft, rgt
	FROM taxa_tree_cache join authorities using (taxon_no)
	WHERE taxon_name in ($code_name_list)");
    
    $code_range_query->execute();
    
    while ( my($taxon, $lft, $rgt) = $code_range_query->fetchrow_array() )
    {
	$codes->{$taxon}{lft} = $lft;
	$codes->{$taxon}{rgt} = $rgt;
    }
}


# processRow ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub processRow {
    
    my ($self, $row) = @_;
    
    # Interpret the status info based on the code stored in the database.  The
    # code as stored in the database encompasses both taxonomic and
    # nomenclatural status info, which needs to be separated out.  In
    # addition, we need to know whether to report an "acceptedUsage" taxon
    # (i.e. senior synonym or proper spelling).
    
    my ($taxonomic, $report_accepted, $nomenclatural) = interpretStatusCode($row->{status});
    
    # Override the status code if the synonym_no is different from the
    # taxon_no.  This is necessary because sometimes the opinion record that
    # was used to build this part of the hierarchy indicates a 'belongs to'
    # relationship (which normally indicates a valid taxon) but the
    # taxa_tree_cache record indicates a different synonym number.  In this
    # case, the taxon is in fact no valid but is a junior synonym or
    # misspelling.  If spelling_no and synonym_no are equal, it's a
    # misspelling.  Otherwise, it's a junior synonym.
    
    if ( $taxonomic eq 'valid' && $row->{synonym_no} ne $row->{taxon_no} )
    {
	if ( $row->{spelling_no} eq $row->{synonym_no} )
	{
	    $taxonomic = 'invalid' unless $row->{spelling_reason} eq 'recombination';
	    $nomenclatural = $row->{spelling_reason};
	}
	else
	{
	    $taxonomic = 'synonym';
	}
    }
    
    # If no status was found, assume 'valid'.  We need to do this because of
    # the many entries in taxa_tree_cache that don't reference an opinion.
    
    $taxonomic = 'valid' unless $taxonomic;
    
    # Put the two status strings into the row record.
    
    $row->{taxonomic} = $taxonomic;
    $row->{nomenclatural} = $nomenclatural;
    
    # Determine the nomenclatural code that has jurisidiction, if that was
    # requested. 
    
    if ( defined $row->{lft} )
    {
	$self->determineNomenclaturalCode($row);
    }
    
    # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->addReference($row);
    }
}


# add_reference ( )
# 
# Process the fields that define a publication reference

sub addReference {

    my ($self, $row) = @_;
    
    # First format the author string.  This includes stripping extra periods
    # from initials and dealing with "et al" where it occurs.
    
    my $a1i = $row->{r_a1i} || '';
    my $a1l = $row->{r_a1l} || '';
    
    $a1i =~ s/\.//g;
    $a1i =~ s/([A-Za-z])/$1./g;
    
    my $auth1 = $a1i;
    $auth1 .= ' ' if $a1i ne '' && $a1l ne '';
    $auth1 .= $a1l;
    
    my $a2i = $row->{r_a2i} || '';
    my $a2l = $row->{r_a2l} || '';
    
    $a2i =~ s/\.//g;
    $a2i =~ s/([A-Za-z])/$1./g;
    
    my $auth2 = $a2i;
    $auth2 .= ' ' if $a2i ne '' && $a2l ne '';
    $auth2 .= $a2l;
    
    my $auth3 = $row->{r_otherauthors} || '';
    
    $auth3 =~ s/\.//g;
    $auth3 =~ s/\b(\w)\b/$1./g;
    
    # Then construct the author string
    
    my $authorstring = $auth1;
    
    if ( $auth2 =~ /et al/ )
    {
	$authorstring .= " $auth2";
    }
    elsif ( $auth2 ne '' && $auth3 ne '' )
    {
	$authorstring .= ", $auth2";
	if ( $auth3 =~ /et al/ )
	{
	    $authorstring .= " $auth3";
	}
	else
	{
	    $authorstring .= ", and $auth3";
	}
    }
    elsif ( $auth2 )
    {
	$authorstring .= " and $auth2";
    }
    
    # Now start building the reference with authorstring, publication year,
    # reference title and publication title
    
    my $longref = $authorstring;
    
    if ( $authorstring ne '' )
    {
	$longref .= '.' unless $authorstring =~ /\.$/;
	$longref .= ' ';
    }
    
    my $pubyr = $row->{r_pubyr} || '';
    
    if ( $pubyr ne '' )
    {
	$longref .= "$pubyr. ";
    }
    
    my $reftitle = $row->{r_reftitle} || '';
    
    if ( $reftitle ne '' )
    {
	$longref .= $reftitle;
	$longref .= '.' unless $reftitle =~ /\.$/;
	$longref .= ' ';
    }
    
    my $pubtitle = $row->{r_pubtitle} || '';
    my $editors = $row->{r_editors} || '';
    
    if ( $pubtitle ne '' )
    {
	# Later, ((i)) will be translated to <i>, etc.
	
	my $pubstring = "((i))$pubtitle((/i))";
	
	if ( $editors =~ /,| and / )
	{
	    $pubstring = " In $editors (eds.), $pubstring";
	}
	elsif ( $editors )
	{
	    $pubstring = " In $editors (ed.), $pubstring";
	}
	
	$longref .= $pubstring . " ";
    }
    
    # Now add volume and page number information if available
    
    my $pubvol = $row->{r_pubvol} || '';
    my $pubno = $row->{r_pubno} || '';
    
    if ( $pubvol ne '' || $pubno ne '' )
    {
	# Later, ((b)) will be translated to <b>, etc.
	
	$longref .= '((b))';
	$longref .= $pubvol if $pubvol ne '';
	$longref .= "($pubno)" if $pubno ne '';
	$longref .= '((/b))';
    }
    
    my $fp = $row->{r_fp} || '';
    my $lp = $row->{r_lp} || '';
    
    if ( ($pubvol ne '' || $pubno ne '') && ($fp ne '' || $lp ne '') )
    {
	$longref .= ':';
	$longref .= $fp if $fp ne '';
	$longref .= '-' if $fp ne '' && $lp ne '';
	$longref .= $lp if $lp ne '';
    }
    
    $row->{pubref} = $longref if $longref ne '';
    
    if ( $longref ne '' )
    {
	$row->{pubref} = $longref;
    }
}


# determineNomenclaturalCode ( row )
# 
# Determine which nomenclatural code the given row's taxon falls under

sub determineNomenclaturalCode {
    
    my ($self, $row) = @_;

    my ($lft) = $row->{lft} || return;
    
    # Anything with a rank of 'unranked clade' falls under PhyloCode.
    
    if ( defined $row->{taxon_rank} && $row->{taxon_rank} eq 'unranked clade' )
    {
	$row->{nom_code} = 'PhyloCode';
	return;
    }
    
    # For all other taxa, we go through the list of known ranges in
    # taxa_tree_cache and use the appropriate code.
    
    foreach my $taxon (@{$self->{code_list}})
    {
	my $range = $self->{code_ranges}{$taxon};
	
	if ( $lft >= $range->{lft} && $lft <= $range->{rgt} )
	{
	    $row->{nom_code} = $range->{code};
	    last;
	}
    }
    
    # If this taxon does not fall within any of the ranges, we leave the
    # nom_code field empty.
}


# generateRecord ( row, is_first_record )
# 
# Return a string representing one row of the result, in the selected output
# format.  The parameter $is_first_record indicates whether this is the first
# record, which is significant for JSON output (it controls whether or not to
# output an initial comma, in that case).

sub generateRecord {

    my ($self, $row, $is_first_record) = @_;
    
    # If the content type is XML, then we need to check whether the result
    # includes a list of parents.  If so, because of the inflexibility of XML
    # and Darwin Core, we cannot output a hierarchical list.  The best we can
    # do is to output all of the parent records first, before the main record
    # (see http://eol.org/api/docs/hierarchy_entries).
    
    if ( $self->{content_type} eq 'xml' )
    {
	my $output = '';
	
	# If there are any parents, output them first with the "short record"
	# flag to indicate that many of the fields should be elided.  These
	# are not the primary object of the query, so less information needs
	# to be provided about them.
	
	if ( defined $self->{parents} && ref $self->{parents} eq 'ARRAY' )
	{
	    foreach my $parent_row ( @{$self->{parents}} )
	    {
		$output .= $self->emitTaxonXML($parent_row, 1);
	    }
	}
    
	# Now, we output the main record.
	
	$output .= $self->emitTaxonXML($row, 0);
	
	return $output;
    }
    
    # Otherwise, it must be JSON.  In that case, we need to insert a comma if
    # this is not the first record.  The subroutine emitTaxonJSON() will also
    # output the parent records, if there are any, as a sub-array.
    
    my $insert = ($is_first_record ? '' : ',');
    
    return $insert . $self->emitTaxonJSON($row, $self->{parents});
}


# emitTaxonXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

my %fixbracket = ( '((' => '<', '))' => '>' );

sub emitTaxonXML {
    
    no warnings;
    
    my ($self, $row, $short_record) = @_;
    my $output = '';
    my @remarks = ();
    
    $output .= '  <dwc:Taxon>' . "\n";
    $output .= '    <dwc:taxonID>' . $row->{taxon_no} . '</dwc:taxonID>' . "\n";
    $output .= '    <dwc:taxonRank>' . $row->{taxon_rank} . '</dwc:taxonRank>' . "\n";
    
    # Taxon names shouldn't contain any invalid characters, but just in case...
    
    $output .= '    <dwc:scientificName>' . DataQuery::xml_clean($row->{taxon_name}) . 
	'</dwc:scientificName>' . "\n";
    
    # species have extra fields to indicate which genus, etc. they belong to
    
    if ( $row->{taxon_rank} =~ /species/ && !$short_record ) {
	my ($genus, $subgenus, $species, $subspecies) = interpretSpeciesName($row->{taxon_name});
	$output .= '    <dwc:genus>' . $genus . '</dwc:genus>' . "\n" if defined $genus;
	$output .= '    <dwc:subgenus>' . "$genus ($subgenus)" . '</dwc:subgenus>' . "\n" if defined $subgenus;
	$output .= '    <dwc:specificEpithet>' . $species . '</dwc:specificEpithet>' . "\n" if defined $species;
	$output .= '    <dwc:infraSpecificEpithet>' . $subspecies . '</dwc:infraSpecificEpithet>' if defined $subspecies;
    }
    
    if ( defined $row->{parent_no} && $row->{parent_no} > 0 ) {
	$output .= '    <dwc:parentNameUsageID>' . $row->{parent_no} . '</dwc:parentNameUsageID>' . "\n";
    }
    
    if ( defined $row->{parent_name} && $self->{show_parent_names} ) {
    	$output .= '    <dwc:parentNameUsage>' . DataQuery::xml_clean($row->{parent_name}) . 
	    '</dwc:parentNameUsage>' . "\n";
    }
    
    if ( defined $row->{author1} && $row->{author1} ne '' ) {
	my $authorship = formatAuthorName($row->{author1}, $row->{author2}, $row->{otherauthors},
					  $row->{pubyr});
	$authorship = "($authorship)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && $row->{orig_no} != $row->{taxon_no};
	$authorship = DataQuery::xml_clean($authorship);
	$output .= '    <dwc:scientificNameAuthorship>' . $authorship . '</dwc:scientificNameAuthorship>' . "\n";
    }
    
    if ( defined $row->{taxonomic} ) {
	$output .= '    <dwc:taxonomicStatus>' . $row->{taxonomic} . '</dwc:taxonomicStatus>' . "\n";
    }
    
    if ( defined $row->{nomenclatural} ) {
	$output .= '    <dwc:nomenclaturalStatus>' . $row->{nomenclatural} . '</dwc:nomenclaturalStatus>' . "\n";
    }
    
    if ( defined $row->{nom_code} ) {
	$output .= '    <dwc:nomenclaturalCode>' . $row->{nom_code} . '</dwc:nomenclaturalCode>' . "\n";
    }
    
    if ( defined $row->{accepted_no} ) {
	$output .= '    <dwc:acceptedNameUsageID>' . $row->{accepted_no} . '</dwc:acceptedNameUsageID>' . "\n";
    }
    
    if ( defined $row->{accepted_name} ) {
	# taxon names shouldn't contain any wide characters, but just in case...
	$output .= '    <dwc:acceptedNameUsage>' . DataQuery::xml_clean($row->{accepted_name}) . 
	    '</dwc:acceptedNameUsage>' . "\n";
    }
    
    if ( defined $row->{pubref} ) {
	my $pubref = DataQuery::xml_clean($row->{pubref});
	# We now need to translate, i.e. ((b)) to <b>.  This is done after
	# xml_clean, because otherwise <b> would be turned into &lt;b&gt;
	if ( $pubref =~ /\(\(/ )
	{
	    #$row->{pubref} =~ s/(\(\(|\)\))/$fixbracket{$1}/eg;
	    #actually, we're just going to take them out for now
	    $pubref =~ s/\(\(\/?\w*\)\)//g;
	}
	$output .= '    <dwc:namePublishedIn>' . $pubref . '</dwc:namePublishedIn>' . "\n";
    }
    
    if ( defined $row->{common_name} && $row->{common_name} ne '' ) {
	$output .= '    <dwc:vernacularName xml:lang="en">' . 
	    DataQuery::xml_clean($row->{common_name}) . '</dwc:vernacularName>' . "\n";
    }
    
    if ( defined $row->{extant} and $row->{extant} =~ /(yes|no)/i ) {
	push @remarks, "extant: $1";
    }
    
    if ( @remarks ) {
	my $remarks = join('; ', @remarks);
	$output .= '    <dwc:taxonRemarks>' . $remarks . '</dwc:taxonRemarks>' . "\n";
    }
    
    $output .= '  </dwc:Taxon>' . "\n";
}


# emitTaxonJSON ( row, parents, short_record )
# 
# Return a string representing the given taxon record (row) in JSON format.
# If 'parents' is specified, it should be an array of hashes each representing
# a parent taxon record.  If 'short_record' is true, then some fields will be
# suppressed.

sub emitTaxonJSON {
    
    no warnings;
    
    my ($self, $row, $parents, $short_record) = @_;
    
    my $output = '';
    
    $output .= '{"taxonID":"' . $row->{taxon_no} . '"'; 
    $output .= ',"taxonRank":"' . $row->{taxon_rank} . '"';
    $output .= ',"scientificName":"' . DataQuery::json_clean($row->{taxon_name}) . '"';
    
    if ( defined $row->{author1} && $row->{author1} ne '' ) {
	my $authorship = formatAuthorName($row->{author1}, $row->{author2}, $row->{otherauthors},
					  $row->{pubyr});
	$authorship = "($authorship)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && $row->{orig_no} != $row->{taxon_no};
	$output .= ',"scientificNameAuthorship":"' . DataQuery::json_clean($authorship) . '"';
    }
    
    if ( defined $row->{parent_no} && $row->{parent_no} > 0 ) {
	$output .= ',"parentNameUsageID":"' . $row->{parent_no} . '"';
    }
    
    if ( defined $row->{parent_name} && $self->{show_parent_names} ) {
	$output .= ',"parentNameUsage":"' . DataQuery::json_clean($row->{parent_name}) . '"';
    }
    
    if ( defined $row->{taxonomic} ) {
	$output .= ',"taxonomicStatus":"' . $row->{taxonomic} . '"';
    }
    
    if ( defined $row->{nomenclatural} ) {
	$output .= ',"nomenclaturalStatus":"' . $row->{nomenclatural} . '"';
    }
    
    if ( defined $row->{nom_code} ) {
	$output .= ',"nomenclaturalCode":"' . $row->{nom_code} . '"';
    }
    
    if ( defined $row->{accepted_no} ) {
	$output .= ',"acceptedNameUsageID":"' . $row->{accepted_no} . '"';
    }
    
    if ( defined $row->{accepted_name} ) {
	$output .= ',"acceptedNameUsage":"' . DataQuery::json_clean($row->{accepted_name}) . '"';
    }
    
    if ( defined $row->{pubref} )
    {
	$output .= ',"namePublishedIn":"' . DataQuery::json_clean($row->{pubref}) . '"';
    }
    
    if ( defined $row->{common_name} ne '' && $row->{common_name} ne '' ) {
	$output .= ',"vernacularName":"' . DataQuery::json_clean($row->{common_name}) . '"';
    }
    
    if ( defined $row->{extant} and $row->{extant} =~ /(yes|no)/i ) {
	$output .= ',"extant":"' . $1 . '"';
    }
    
    if ( defined $parents && ref $parents eq 'ARRAY' )
    {
	my $is_first_parent = 1;
	$output .= ',"ancestors":[';
	
	foreach my $parent_row ( @{$self->{parents}} )
	{
	    $output .= ( $is_first_parent ? "\n" : "\n," );
	    $output .= $self->emitTaxonJSON($parent_row);
	    $is_first_parent = 0;
	}
	
	$output .= ']';
    }
    
    $output .= "}";
    return $output;
}


# formatAuthorName ( author1last, author2last, otherauthors, pubyr, orig_no )
# 
# Format the given info into a single string that can be output as the
# attribution of a taxon.

sub formatAuthorName {
    
    no warnings;
    
    my ($author1last, $author2last, $otherauthors, $pubyr) = @_;
    
    my $a1 = defined $author1last ? $author1last : '';
    my $a2 = defined $author2last ? $author2last : '';
    
    $a1 =~ s/( Jr)|( III)|( II)//;
    $a1 =~ s/\.$//;
    $a1 =~ s/,$//;
    $a1 =~ s/\s*$//;
    $a2 =~ s/( Jr)|( III)|( II)//;
    $a2 =~ s/\.$//;
    $a2 =~ s/,$//;
    $a2 =~ s/\s*$//;
    
    my $shortRef = $a1;
    
    if ( $otherauthors ne '' ) {
        $shortRef .= " et al.";
    } elsif ( $a2 ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if ( $a2 !~ /^et al/i ) {
            $shortRef .= " and $a2";
        } else {
            $shortRef .= " et al.";
        }
    }
    if ($pubyr) {
	$shortRef .= " " . $pubyr;
    }
    
    return $shortRef;
}


# interpretSpeciesName ( taxon_name )
# 
# Separate the given name into genus, subgenus, species and subspecies.

sub interpretSpeciesName {

    my ($taxon_name) = @_;
    my @components = split(/\s+/, $taxon_name);
    
    my ($genus, $subgenus, $species, $subspecies);
    
    # If the first character is a space, the first component will be blank;
    # ignore it.
    
    shift @components if @components && $components[0] eq '';
    
    # If there's nothing left, we were given bad input-- return nothing.
    
    return unless @components;
    
    # The first component is always the genus.
    
    $genus = shift @components;
    
    # If the next component starts with '(', it is a subgenus.
    
    if ( @components && $components[0] =~ /^\((.*)\)$/ )
    {
	$subgenus = $1;
	shift @components;
    }
    
    # The next component must be the species
    
    $species = shift @components if @components;
    
    # The last component, if there is one, must be the subspecies.  Strip
    # parentheses if there are any.
    
    $subspecies = shift @components if @components;
    
    if ( defined $subspecies && $subspecies =~ /^\((.*)\)$/ ) {
	$subspecies = $1;
    }
    
    return ($genus, $subgenus, $species, $subspecies);
}


# The following hashes map the status codes stored in the opinions table of
# PaleoDB into taxonomic and nomenclatural status codes in compliance with
# Darwin Core.  The third one, %REPORT_ACCEPTED_TAXON, indicates which status
# codes should trigger the "acceptedUsage" and "acceptedUsageID" fields in the
# output.

our (%TAXONOMIC_STATUS) = (
	'belongs to' => 'valid',
	'subjective synonym of' => 'heterotypic synonym',
	'objective synonym of' => 'homotypic synonym',
	'invalid subgroup of' => 'invalid',
	'misspelling of' => 'invalid',
	'replaced by' => 'invalid',
	'nomen dubium' => 'invalid',
	'nomen nudum' => 'invalid',
	'nomen oblitum' => 'invalid',
	'nomen vanum' => 'invalid',
);


our (%NOMENCLATURAL_STATUS) = (
	'invalid subgroup of' => 'invalid subgroup',
	'misspelling of' => 'misspelling',
	'replaced by' => 'replaced by',
	'nomen dubium' => 'nomen dubium',
	'nomen nudum' => 'nomen nudum',
	'nomen oblitum' => 'nomen oblitum',
	'nomen vanum' => 'nomen vanum',
);


our (%REPORT_ACCEPTED_TAXON) = (
	'subjective synonym of' => 1,
	'objective synonym of' => 1,
	'misspelling of' => 1,
	'replaced by' => 1,
);


# interpretStatusCode ( pbdb_status )
# 
# Use the hashes given above to interpret a status code from the opinions
# table of PaleoDB.  Returns: taxonomic status, whether we should report an
# "acceptedUsage" taxon, and the nomenclatural status.

sub interpretStatusCode {

    my ($pbdb_status) = @_;
    
    # If the status is empty, return nothing.
    
    unless ( defined $pbdb_status and $pbdb_status ne '' )
    {
	return '', '', '';
    }
    
    # Otherwise, interpret the status code according to the mappings specified
    # above.
    
    return $TAXONOMIC_STATUS{$pbdb_status}, $REPORT_ACCEPTED_TAXON{$pbdb_status}, 
	$NOMENCLATURAL_STATUS{$pbdb_status};
}


1;
