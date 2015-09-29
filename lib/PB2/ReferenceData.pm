# 
# ReferenceData
# 
# A class that returns information from the PaleoDB database about
# bibliographic references.
# 
# Author: Michael McClennen

use strict;

package PB2::ReferenceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($REF_SUMMARY);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier %IDP);

our (@REQUIRES_ROLE) = qw(PB2::CommonData);

use Moo::Role;


# initialize ( )
# 
# This routine is called by the data service to initialize this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Start by defining an output map.
    
    $ds->define_output_map('1.2:refs:output_map' =>
	{ value => 'counts', maps_to => '1.2:refs:counts' },
	    "Include the number of taxa, opinions, occurrences, and collections",
	    "derived from this reference that have been entered into the database.",
	{ value => 'formatted' },
	    "If this option is specified, show the formatted reference instead of",
	    "the individual fields.",
	{ value => 'both' },
	    "If this option is specified, show both the formatted reference and",
	    "the individual fields",
	{ value => 'comments' },
	    "Include any additional comments associated with this reference",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
	{ value => 'crmod', maps_to => '1.2:common:crmod' },
	    "Include the creation and modification times for this record");
    
    # Output sets:
    
    $ds->define_set('1.2:refs:reftype' =>
	{ value => 'auth (A)' },
	    "This reference gives the authority for a taxonomic name",
	{ value => 'class (C)' },
	    "This reference is the source for a classification opinion",
	{ value => 'unclass (U)' },
	    "This reference is the source for an opinion that is not selected for",
	    "classification because of its date of publication and/or basis",
	{ value => 'occ (O)' },
	    "This reference is the source for a fossil occurrence",
	{ value => 'prim (P)' },
	    "This reference is indicated to be the primary source for a fossil collection",
	{ value => 'sec (S)' },
	    "This reference is an additional source for a fossil collection",
	{ value => 'ref (R)' },
	    "This reference has an unknown or unspecified role in the database");
    
    # Then some output blocks:
    
    # One block for the reference routes themselves.
    
    $ds->define_block( '1.2:refs:basic' =>
      { select => ['r.reference_no', 'r.comments as r_comments',
		   'r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'formatted', from => '*', code => \&format_reference },
      { set => 'ref_type', from => '*', code => \&set_reference_type, if_vocab => 'pbdb' },
      { output => 'reference_no', com_name => 'oid' }, 
	  "Numeric identifier for this document reference in the database",
      { output => 'record_type', com_name => 'typ', value => $IDP{REF} },
	  "The type of this object: C<$IDP{REF}> for a document reference.",
      { output => 'ref_type', com_name => 'rtp' },
	  "The role(s) played by this reference in the database.  This field will only appear",
	  "in the result of queries for occurrence, collection, or taxonomic references.",
	  "Values can include one or more of the following, as a comma-separated list:", 
	  $ds->document_set('1.2:refs:reftype'),
	{ output => 'n_reftaxa', name => 'n_taxa', com_name => 'ntx', if_block => 'counts' },
	    "The number of taxa associated with this reference",
	{ output => 'n_refclass', name => 'n_class', com_name => 'ncl', if_block => 'counts' },
	    "The number of classification opinions associated with this reference",
	{ output => 'n_refunclass', name => 'n_unclass', com_name => 'nuc', if_block => 'counts' },
	    "The number of opinions not selected for classification associated with this reference",
	{ output => 'n_refoccs', name => 'n_occs', com_name => 'noc', if_block => 'counts' },
	    "The number of occurrences associated with this reference",
	{ output => 'n_refcolls', name => 'n_colls', com_name => 'nco', if_block => 'counts' },
	    "The number of collections for which this is the primary reference",
      { output => 'formatted', com_name => 'ref', if_block => 'formatted,both' },
	  "Formatted reference",
      { output => 'r_ai1', com_name => 'ai1', pbdb_name => 'author1init', not_block => 'formatted' },
	  "First initial of the first author",
      { output => 'r_al1', com_name => 'al1', pbdb_name => 'author1last', not_block => 'formatted' },
	  "Last name of the second author",
      { output => 'r_ai2', com_name => 'ai2', pbdb_name => 'author2init', not_block => 'formatted' },
	  "First initial of the second author",
      { output => 'r_al2', com_name => 'al2', pbdb_name => 'author2last', not_block => 'formatted' },
	  "Last name of the second author",
      { output => 'r_oa', com_name => 'oau', pbdb_name => 'otherauthors', not_block => 'formatted' },
	  "The names of the remaining authors",
      { output => 'r_pubyr', com_name => 'pby', pbdb_name => 'pubyr', not_block => 'formatted', data_type => 'str' },
	  "The year in which the document was published",
      { output => 'r_reftitle', com_name => 'tit', pbdb_name => 'reftitle', not_block => 'formatted' },
	  "The title of the document",
      { output => 'r_pubtitle', com_name => 'pbt', pbdb_name => 'pubtitle', not_block => 'formatted' },
	  "The title of the publication in which the document appears",
      { output => 'r_editors', com_name => 'eds', pbdb_name => 'editors', not_block => 'formatted' },
	  "Names of the editors, if any",
      { output => 'r_pubvol', com_name => 'vol', pbdb_name => 'pubvol', not_block => 'formatted', data_type => 'str' },
	  "The volume number, if any",
      { output => 'r_pubno', com_name => 'num', pbdb_name => 'pubno', not_block => 'formatted', data_type => 'str' },
	  "The series number within the volume, if any",
      { output => 'r_fp', com_name => 'pgf', pbdb_name => 'firstpage', not_block => 'formatted', data_type => 'str' },
	  "First page number",
      { output => 'r_lp', com_name => 'pgl', pbdb_name => 'lastpage', not_block => 'formatted', data_type => 'str' },
	  "Last page number",
      { output => 'r_pubtype', com_name => 'pty', pbdb_name => 'publication_type', not_block => 'formatted' },
	  "Publication type",
      { output => 'r_language', com_name => 'lan', pbdb_name => 'language', not_block => 'formatted' },
	  "Language",
      { output => 'r_doi', com_name => 'doi', pbdb_name => 'doi' },
	  "The DOI for this document, if known",
      { output => 'r_comments', com_name => 'rem', pbdb_name => 'comments', if_block => 'comments' },
	  "Additional comments about this reference, if any",
      { set => '*', code => \&process_ref_com, if_vocab => 'com' });
    
    $ds->define_block('1.2:refs:counts' =>
	{ select => ['rs.n_taxa', 'rs.n_class', 'rs.n_opinions', 'rs.n_occs', 'rs.n_colls'], 
	  tables => 'rs' },
	{ set => '*', code => \&adjust_ref_counts });
    
    # Then blocks for other classes to use when including one or more
    # references into other output.
    
    $ds->define_block('1.2:refs:primary' =>
      { select => ['r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'ref_text', from => '*', code => \&format_reference },
      { output => 'ref_text', pbdb_name => 'primary_reference', dwc_name => 'associatedReferences', 
	com_name => 'ref' },
	  "The primary reference associated with this record (as formatted text)");

    $ds->define_block('1.2:refs:all' =>
      { select => ['r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'ref_list', append => 1, from => '*', code => \&format_reference },
      { set => 'ref_list', append => 1, from => 'sec_refs', code => \&format_reference },
      { output => 'ref_list', pbdb_name => 'all_references', dwc_name => 'associatedReferences', 
	com_name => 'ref', text_join => '|||' },
	  "All references associated with this record (as formatted text)");
    
    # Then rulesets.
    
    $ds->define_set('1.2:refs:order' =>
	{ value => 'author' },
	    "Results are ordered alphabetically by the name of the primary author (last, first)",
	{ value => 'author.asc', undocumented => 1 },
	{ value => 'author.desc', undocumented => 1 },
	{ value => 'pubyr' },
	    "Results are ordered by the year of publication",
	{ value => 'pubyr.asc', undocumented => 1 },
	{ value => 'pubyr.desc', undocumented => 1 },
	{ value => 'pubtitle' },
	    "Results are ordered alphabetically by the title of the publication",
	{ value => 'pubtitle.asc', undocumented => 1 },
	{ value => 'pubtitle.desc', undocumented => 1 },
	{ value => 'created' },
	    "Results are ordered by the date the record was created, most recent first",
	    "unless you add C<.asc>.",
	{ value => 'created.asc', undocumented => 1 },
	{ value => 'created.desc', undocumented => 1 },
	{ value => 'modified' },
	    "Results are ordered by the date the record was last modified",
	    "most recent first unless you add C<.asc>",
	{ value => 'modified.asc', undocumented => 1 },
	{ value => 'modified.desc', undocumented => 1 },
	{ value => 'rank' },
	    "Results are ordered by the number of associated records, highest first unless you add C<.asc>.",
	    "This is only useful when querying for references associated with occurrences, taxa, etc.",
	{ value => 'rank.asc', undocumented => 1 },
	{ value => 'rank.desc', undocumented => 1 });
    
    $ds->define_ruleset('1.2:refs:display' =>
	{ optional => 'show', valid => $ds->valid_set('1.2:refs:output_map'), list => ',' },
	    "Indicates additional information to be shown along",
	    "with the basic record.  The value should be a comma-separated list containing",
	    "one or more of the following values:",
	    $ds->document_set('1.2:refs:output_map'),
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.2:refs:specifier' => 
	{ param => 'id', valid => VALID_IDENTIFIER('REF'), alias => 'ref_id' },
	    "A unique number identifying the reference to be selected");
    
    $ds->define_ruleset('1.2:refs:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all bibliographic references known to the database.",
	{ param => 'id', valid => VALID_IDENTIFIER('REF'), alias => 'ref_id', list => ',' },
	    "A list of one or more reference identifiers, separated by commas.  You can",
	    "use this parameter to get information about a specific list of references,",
	    "or to filter a known list against other criteria.");
    
    $ds->define_ruleset('1.2:refs:filter' =>
    	{ param => 'ref_author', valid => MATCH_VALUE('.*\w.*') },
    	    "Select only references for which any of the authors matches the specified name",
    	{ param => 'ref_primary', valid => MATCH_VALUE('.*\w.*') },
    	    "Select only references for which the primary author matches the specified name",
	{ param => 'ref_published_after', valid => POS_VALUE, alias => 'pubyr_before' },
	    "Selects only references published during or after the indicated year.",
	{ param => 'ref_published_before', valid => POS_VALUE, alias => 'pubyr_after' },
	    "Selects only references published during or before the indicated year.",
	{ param => 'ref_published', valid => ANY_VALUE, alias => 'pubyr' },
	    "Selects only references published during the indicated year or range of years.",
    	# { param => 'year', valid => MATCH_VALUE('(?:(?:\d{4})\s*(?:-\s*\d{4})?|\d{4}\s*)'),
    	#   error => "the value of {param} must be a range of years, with either bound optional ('2010-' is okay); found {value}" },
    	#     "Select only references published in the specified year",
	{ param => 'ref_title', valid => ANY_VALUE },
	    "Select only references whose title matches the specified value.  You can",
	    "use C<%> and C<_> as wildcards.",
    	{ param => 'pub_title', valid => ANY_VALUE },
    	    "Select only references that involve the specified publication.  You can",
	    "use C<%> and C<_> as wildcards.");
    
    $ds->define_ruleset('1.2:refs:single' => 
    	{ require => '1.2:refs:specifier' },
    	{ allow => '1.2:refs:display' },
    	{ allow => '1.2:special_params' },
    	"^You can also use any of the L<special parameters|node:special>");
    
    $ds->define_ruleset('1.2:refs:list' =>
	"You must include at least one of the following parameters:",
    	{ allow => '1.2:refs:selector' },
	{ allow => '1.2:refs:filter' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	{ require_any => ['1.2:refs:selector', '1.2:refs:filter'] },
	">>You can also specify any of the following parameters:",
    	{ allow => '1.2:refs:display' },
	{ optional => 'order', valid => '1.2:refs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:refs:order'),
	    ">If no order is specified, the results are sorted alphabetically according to",
	    "the name of the primary author, unless C<all_records> is specified in which",
	    "case they are returned by default in the order they occur in the database.",
    	{ allow => '1.2:special_params' },
    	"^You can also use any of the L<special parameters|node:special>",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author, unless C<all_records> is specified in which case",
	"the records are returned in the order in which they occur in the database.");
}



# get ( )
# 
# Return information about a single reference.

sub get {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( cd => 'r' );
    
    my $fields = $request->select_string;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM refs as r
        WHERE r.reference_no = $id
	GROUP BY r.reference_no";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
}


# list ( )
# 
# Return information about one or more references.

sub list {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.

    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $all_records = $request->clean_param('all_records');
    
    my @filters = $request->generate_ref_filters();
    push @filters, $request->generate_common_filters( { refs => 'r', bare => 'r' } );
    push @filters, '1=1' if $all_records;
    
    my $filter_string = join(' and ', @filters);
    
    # Select the order in which the results should be returned.  If none was
    # specified, sort by the name of the primary author first and the
    # publication year second.
    
    my $order = $request->generate_order_clause();
    
    unless ( $order )
    {
	$order = $all_records ? 'NULL' : 'r.author1last, r.author1init';
    }
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( cd => 'r' );
    
    my $fields = $request->select_string;
    my $tables = $request->tables_hash;
    
    my $join_list = $request->generate_join_list($tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM refs as r
		$join_list
        WHERE $filter_string
	GROUP BY r.reference_no
	ORDER BY $order
	$limit";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    return 1;
}


# generate_ref_filters ( )
# 
# Generate the necessary filter clauses to reflect the query parameters.

sub generate_ref_filters {
    
    my ($request, $tables_hash) = @_;
    
    my $dbh = $request->get_connection;
    my @filters;
    
    if ( my @ids = $request->safe_param_list('id') )
    {
	my $id_string = join(',', @ids);
	push @filters, "r.reference_no in ($id_string)";
    }
    
    if ( my $year = $request->clean_param('ref_published') )
    {
	if ( $year =~ /(\d+)\s*-/ )
	{
	    push @filters, "r.pubyr >= $1";
	}
	
	if ( $year =~ /-\s*(\d+)/ )
	{
	    push @filters, "r.pubyr <= $1";
	}
	
	if ( $year =~ /^(\d+)$/ )
	{
	    push @filters, "r.pubyr = $1";
	}
    }
    
    if ( my $authorname = $request->clean_param('ref_author') )
    {
	push @filters, $request->generate_auth_filter($authorname, 'author');
    }
    
    if ( my $authorname = $request->clean_param('ref_primary') )
    {
	push @filters, $request->generate_auth_filter($authorname, 'primary');
    }
    
    if ( my $reftitle = $request->clean_param('ref_title') )
    {
	my $quoted = $dbh->quote($reftitle);
	
	push @filters, "r.reftitle like $quoted";
    }
    
    if ( my $pubtitle = $request->clean_param('pub_title') )
    {
	my $quoted = $dbh->quote($pubtitle);
	
	push @filters, "r.pubtitle like $quoted";
    }
    
    return @filters;
}


sub generate_auth_filter {

    my ($request, $authorname, $selector) = @_;
    
    my ($firstname, $lastname, $initpat, $lastpat, $fullpat);
    my @authfilters;
    my $dbh = $request->get_connection;
    
    if ( $authorname =~ /(.*)[.] +(.*)/ )
    {
	$firstname = $1;
	$lastname = $2;
    }
    
    else
    {
	$lastname = $authorname;
    }
    
    $lastname =~ s/%/[^,]*/g;
    $lastname =~ s/_/[^,]/g;
    
    $lastpat = "^$lastname(,|\$)";
    
    if ( $firstname )
    {
	$initpat = "^$firstname";
	$initpat =~ s/\./[.]/g;
	    
	$fullpat = "$firstname\[.][^,]* $lastname(,|\$)";
    }
    
    else
    {
	$fullpat = "(^| )$lastname(,|\$)";
    }
    
    my $initquote = $dbh->quote($initpat) if $initpat;
    my $lastquote = $dbh->quote($lastpat);
    my $fullquote = $dbh->quote($fullpat);
    
    if ( $initpat )
    {
	push @authfilters, "r.author1init rlike $initquote and r.author1last rlike $lastquote";
	push @authfilters, "r.author2init rlike $initquote and r.author2last rlike $lastquote" unless $selector eq 'primary';
	push @authfilters, "r.otherauthors rlike $fullquote" unless $selector eq 'primary';
    }
    
    else
    {
	push @authfilters, "r.author1last rlike $lastquote";
	push @authfilters, "r.author2last rlike $lastquote" unless $selector eq 'primary';
	push @authfilters, "r.otherauthors rlike $fullquote" unless $selector eq 'primary';
    }
    
    return '(' . join(' or ', @authfilters) . ')';
}


# generate_order_clause ( options )
# 
# Return the order clause for the list of references, or the empty string if
# none was selected.  If the option 'rank_table' is true, then allow ordering
# by the 'reference_rank' field in that table.  Otherwise, this option if specified results
# in a dummy ordering by reference_no.

sub generate_order_clause {
    
    my ($request, $options) = @_;
    
    $options ||= {};
    
    my $order = $request->clean_param('order');
    my @terms = ref $order eq 'ARRAY' ? @$order : $order;
    my @exprs;
    
    # Now generate the corresponding expression for each term.
    
    foreach my $term ( @terms )
    {
	my $dir = '';
	next unless $term;
	
	if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	{
	    $term = $1;
	    $dir = $2;
	}
	
	if ( $term eq 'author' )
	{
	    push @exprs, "r.author1last $dir, r.author1init $dir";
	}
	
	elsif ( $term eq 'pubyr' )
	{
	    push @exprs, "r.pubyr $dir";
	}
	
	elsif ( $term eq 'pubtitle' )
	{
	    push @exprs, "r.pubtitle $dir",
	}
	
	elsif ( $term eq 'rank' && $options->{rank_table} )
	{
	    $dir ||= 'desc';
	    push @exprs, "$options->{rank_table}.reference_rank $dir";
	}
	
	elsif ( $term eq 'rank' )
	{
	    $dir ||= 'desc';
	    push @exprs, "r.reference_no $dir";
	}
	
	elsif ( $term eq 'created' )
	{
	    $dir ||= 'desc';
	    push @exprs, "r.reference_no $dir";
	}
	
	elsif ( $term eq 'modified' )
	{
	    $dir ||= 'desc';
	    push @exprs, "r.modified $dir";
	}
	
	else
	{
	    die "400 bad value for parameter 'order': must be one of 'year', 'author', 'id', rank' with optional suffix '.asc' or '.desc' (was '$term')\n";
	}
    }
    
    return join(', ', @exprs);
}


# generate_join_list ( )
# 
# Return any extra joins that need to be joined to the query.

sub generate_join_list {

    my ($request, $tables_hash) = @_;
    
    return "	JOIN $REF_SUMMARY as rs on rs.reference_no = r.reference_no\n"
	if $tables_hash->{rs};
    
    return '';
}


# format_reference ( )
# 
# Generate a reference string for the given record.  This relies on the
# fields "r_al1", "r_ai1", "r_al2", "r_ai2", "r_ao", "r_pubyr", "r_reftitle",
# "r_pubtitle", "r_pubvol", "r_pubno".
# 

sub format_reference {

    my ($request, $row) = @_;
    
    my $markup = $request->clean_param('markrefs');
    
    # First format the author string.  This includes stripping extra periods
    # from initials and dealing with "et al" where it occurs.
    
    my $ai1 = $row->{r_ai1} || '';
    my $al1 = $row->{r_al1} || '';
    
    $ai1 =~ s/\.//g;
    $ai1 =~ s/([A-Za-z])/$1./g;
    
    my $auth1 = $ai1;
    $auth1 .= ' ' if $ai1 ne '' && $al1 ne '';
    $auth1 .= $al1;
    
    my $ai2 = $row->{r_ai2} || '';
    my $al2 = $row->{r_al2} || '';
    
    $ai2 =~ s/\.//g;
    $ai2 =~ s/([A-Za-z])/$1./g;
    
    my $auth2 = $ai2;
    $auth2 .= ' ' if $ai2 ne '' && $al2 ne '';
    $auth2 .= $al2;
    
    my $auth3 = $row->{r_ao} || '';
    
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
	my $pubstring = $markup ? "<i>$pubtitle</i>" : $pubtitle;
	
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
	$longref .= '<b>' if $markup;
	$longref .= $pubvol if $pubvol ne '';
	$longref .= "($pubno)" if $pubno ne '';
	$longref .= '</b>' if $markup;
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
    
    return $longref if $longref ne '';
    return;
}


# set_reference_type ( )
# 
# Set the ref_type field for a reference record.  This is based on fields
# such as 'is_auth', etc.

sub set_reference_type {
    
    my ($request, $record) = @_;
    
    my $ref_type = $record->{ref_type} || '';
    my @types;
    
    if ( $ref_type =~ qr{A} )
    {
	push @types, 'auth' || $record->{n_reftaxa};
    }
    
    if ( $ref_type =~ qr{C} || $record->{n_refclass} )
    {
	push @types, 'class';
    }
    
    if ( $ref_type =~ qr{U} || $record->{n_refunclass} )
    {
	push @types, 'unsel';
    }
    
    if ( $ref_type =~ qr{O} || $record->{n_refoccs} )
    {
	push @types, 'occ';
    }
    
    if ( $ref_type =~ qr{P} || $record->{n_refcolls} )
    {
	push @types, 'prim';
    }
    
    elsif ( $ref_type =~ qr{S} && $ref_type !~ qr{O} )
    {
	push @types, 'sec';
    }
    
    # if ( defined $record->{n_refcolls} && defined $record->{n_refprim} && 
    # 	 $record->{n_refcolls} > $record->{n_refprim} )
    # {
    # 	push @types, 'sec';
    # }
    
    push @types, 'ref' unless @types;
    
    return join(',', @types);
}


sub process_ref_com {
    
    my ($request, $record) = @_;
    
    $record->{reference_no} = generate_identifier('REF', $record->{reference_no}) if defined $record->{reference_no};
    # "$IDP{REF}:$record->{reference_no}" if defined $record->{reference_no};
}


sub adjust_ref_counts {

    my ($request, $record) = @_;
    
    return unless defined $record->{n_class};
    
    if ( defined $record->{n_opinions} && ! defined $record->{n_unclass} )
    {
	$record->{n_unclass} = $record->{n_opinions} - $record->{n_class};
    }
    
    elsif ( defined $record->{n_unclass} && ! defined $record->{n_opinions} )
    {
	$record->{n_opinions} = $record->{n_class} + $record->{n_unclass};
    }
}

1;
