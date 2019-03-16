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
use PB2::CommonData qw(generateAttribution);

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
	    "Report the number of taxonomic names, opinions, occurrences, specimens, and collections",
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
	    "This reference gives the authority for at least one taxonomic name",
	{ value => 'var (V)' },
	    "This reference is the source for at least one taxonomic name variant",
	    "that is not currently accepted.",
	{ value => 'class (C)' },
	    "This reference is the source for at least one classification opinion",
	{ value => 'unclass (U)' },
	    "This reference is the source for a least one opinion that is not selected for",
	    "classification because of its date of publication and/or basis",
	{ value => 'occ (O)' },
	    "This reference is the source for at least one fossil occurrence",
	{ value => 'spec (S)' },
	    "This reference is the source for at least one fossil specimen",
	{ value => 'prim (P)' },
	    "This reference is indicated to be the primary source for at least one fossil collection",
	{ value => 'ref (R)' },
	    "This reference has an unknown or unspecified role in the database");
    
    # Then some output blocks:
    
    # One block for the reference routes themselves.
    
    $ds->define_block( '1.2:refs:basic' =>
      { select => ['r.reference_no', 'r.comments as r_comments',
		   'r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle',
		   'r.publisher as r_publisher', 'r.pubcity as r_pubcity',
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'formatted', from => '*', code => \&format_reference },
      { set => 'ref_type', from => '*', code => \&set_reference_type, if_vocab => 'pbdb' },
      { output => 'reference_no', com_name => 'oid' }, 
	  "Numeric identifier for this document reference in the database",
      { output => 'record_type', com_name => 'typ', value => $IDP{REF}, not_block => 'extids' },
	  "The type of this object: C<$IDP{REF}> for a document reference.",
      { output => 'ref_type', com_name => 'rtp' },
	  "The role(s) played by this reference in the database.  This field will only appear",
	  "in the result of queries for occurrence, collection, or taxonomic references.",
	  "Values can include one or more of the following, as a comma-separated list:", 
	  $ds->document_set('1.2:refs:reftype'),
      { output => 'n_reftaxa', pbdb_name => 'n_taxa', com_name => 'ntx', if_block => 'counts' },
	  "The number of distinct taxa associated with this reference",
      { output => 'n_refauth', pbdb_name => 'n_auth', com_name => 'nau', if_block => 'counts' },
	  "The number of taxa for which this reference gives the authority for the current name variant",
      { output => 'n_refvar', pbdb_name => 'n_var', com_name => 'nva', if_block => 'counts' },
	  "The number of non-current name variants for which this reference gives the authority",
      { output => 'n_refclass', pbdb_name => 'n_class', com_name => 'ncl', if_block => 'counts' },
	  "The number of classification opinions entered from this reference",
      { output => 'n_refunclass', pbdb_name => 'n_unclass', com_name => 'nuc', if_block => 'counts' },
	  "The number of opinions not selected for classification entered from this reference",
      { output => 'n_refoccs', pbdb_name => 'n_occs', com_name => 'noc', if_block => 'counts' },
	  "The number of occurrences entered from this reference",
      { output => 'n_refspecs', pbdb_name => 'n_specs', com_name => 'nsp', if_block => 'counts' },
	  "The number of specimens entered from this reference",
      { output => 'n_refcolls', pbdb_name => 'n_colls', com_name => 'nco', if_block => 'counts' },
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
      { output => 'r_publisher', com_name => 'pbl', pbdb_name => 'publisher', not_block => 'formatted' },
	  "Name of the publisher, if this data has been entered",
      { output => 'r_pubcity', com_name => 'pbc', pbdb_name => 'pubcity', not_block => 'formatted' },
	  "City of publication, if this data has been entered",
      { output => 'r_pubvol', com_name => 'vol', pbdb_name => 'pubvol', not_block => 'formatted', data_type => 'str' },
	  "The volume number, if any",
      { output => 'r_pubno', com_name => 'vno', pbdb_name => 'pubno', not_block => 'formatted', data_type => 'str' },
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
      { set => '*', code => \&process_ref_ids });
    
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
		   'r.publisher as r_publisher', 'r.pubcity as r_pubcity',
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
		   'r.publisher as r_publisher', 'r.pubcity as r_pubcity',
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'ref_list', append => 1, from => '*', code => \&format_reference },
      { set => 'ref_list', append => 1, from => 'sec_refs', code => \&format_reference },
      { output => 'ref_list', pbdb_name => 'all_references', dwc_name => 'associatedReferences', 
	com_name => 'ref', text_join => '|||' },
	  "All references associated with this record (as formatted text)");
    
    $ds->define_block('1.2:refs:attr' =>
        { select => ['r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
	  	     'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr'],
          tables => ['r'] },
        { set => 'ref_author', from => '*', code => \&format_authors },
	{ set => 'ref_pubyr', from => 'r_pubyr' });
    
    # Then rulesets.
    
    $ds->define_set('1.2:refs:order' =>
	{ value => 'author' },
	    "Results are ordered alphabetically by the name of the primary and authors (last, first)",
	{ value => 'author.asc', undocumented => 1 },
	{ value => 'author.desc', undocumented => 1 },
	{ value => 'pubyr' },
	    "Results are ordered by the year of publication",
	{ value => 'pubyr.asc', undocumented => 1 },
	{ value => 'pubyr.desc', undocumented => 1 },
	{ value => 'reftitle' },
	    "Results are ordered alphabetically by the title of the publication",
	{ value => 'reftitle.asc', undocumented => 1 },
	{ value => 'reftitle.desc', undocumented => 1 },
	{ value => 'pubtitle' },
	    "Results are ordered alphabetically by the title of the publication",
	{ value => 'pubtitle.asc', undocumented => 1 },
	{ value => 'pubtitle.desc', undocumented => 1 },
	{ value => 'pubtype' },
	    "Results are ordered according to the publication type",
	{ value => 'pubtype.asc', undocumented => 1 },
	{ value => 'pubtype.desc', undocumented => 1 },
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
    
    $ds->define_set('1.2:refs:pubtype' =>
	{ value => 'unpublished' },
	{ value => 'journal article' },
	{ value => 'serial monograph' },
	{ value => 'book' },
	{ value => 'book chapter' },
	{ value => 'book/book chapter' },
	{ value => 'abstract' },
	{ value => 'guidebook' },
	{ value => 'compendium' },
	{ value => 'news article' },
	{ value => 'Ph.D. thesis' },
	{ value => 'M.S. thesis' });
    
    $ds->define_ruleset('1.2:refs:display' =>
	{ optional => 'show', valid => '1.2:refs:output_map', list => ',' },
	    "Indicates additional information to be shown along",
	    "with the basic record.  The value should be a comma-separated list containing",
	    "one or more of the following values:");
    
    $ds->define_ruleset('1.2:refs:order' =>
	{ optional => 'order', valid => '1.2:refs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:refs:order'),
	    ">If no order is specified, the results are sorted alphabetically according to",
	    "the name of the primary and secondary authors, unless B<C<all_records>> is specified in which",
	    "case they are returned by default in the order they occur in the database.");
    
    $ds->define_ruleset('1.2:refs:specifier' => 
	{ param => 'ref_id', valid => VALID_IDENTIFIER('REF'), alias => 'id' },
	    "The identifier of the reference to be returned");
    
    $ds->define_ruleset('1.2:refs:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all bibliographic references known to the database.",
	{ param => 'ref_id', valid => VALID_IDENTIFIER('REF'), alias => 'id', list => ',', bad_value => '_' },
	    "A list of one or more reference identifiers, separated by commas.  You can",
	    "use this parameter to get information about a specific list of references,",
	    "or to filter a known list against other criteria.");
    
    my $no_auth = "invalid author name {value}, must contain at least one letter";
    my $no_letter = "the value of {param} must contain at least one letter (was {value})";
    
    $ds->define_ruleset('1.2:refs:filter' =>
    	{ param => 'ref_author', valid => MATCH_VALUE('.*\p{L}.*'), list => ',', 
	  bad_value => '_', errmsg => $no_auth },
    	    "Select only references for which any of the authors matches the specified",
	    "name or names.",
	    "You can specify names in any of the following patterns:",
	    "=over","=item Smith","=item J. Smith", "=item Smith and Jones", "=back",
	    "The last form selects only references where both names are listed as authors, in any order.",
	    "You can use C<%> and C<_> as wildcards, but each name must contain at least one letter.",
	    "You can include more than one name, separated by commas.",
    	{ param => 'ref_primary', valid => MATCH_VALUE('.*\p{L}.*'), list => ',',
	  bad_value => '_', errmsg => $no_auth },
    	    "Select only references for which the primary author matches the specified name",
	    "or names (see C<ref_author> above).  If you give a name like 'Smith and Jones', then references",
	    "are selected only if Smith is the primary author and Jones is also an author.",
	{ param => 'ref_pubyr', valid => \&valid_pubyr, alias => 'pubyr' },
	    "Selects only references published during the indicated year or range of years.",
	    "The parameter value must match one of the following patterns:",
	    "=over","=item 2000","=item 1990-2000", "=item 1990-", "=item -2000", "=back",
	{ param => 'ref_title', valid => MATCH_VALUE('.*\p{L}.*'), errmsg => $no_letter },
	    "Select only references whose title matches the specified word or words.  You can",
	    "use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
    	{ param => 'pub_title', valid => MATCH_VALUE('.*\p{L}.*'), errmsg => $no_letter },
	    "Select only references from publications whose title matches the specified",
	    "word or words.  You can use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
	{ param => 'pub_type', valid => '1.2:refs:pubtype', list => ',', bad_value => 'NOTHING' },
	    "Select only references of the indicated type or types.  You can specify",
	    "one or more from the following list, separated by commas:",
	{ param => 'ref_doi', valid => ANY_VALUE, list => ',' },
	    "Select only records entered from references with any of the specified DOIs.",
	    "You may specify one or more, separated by commas.");
    
    $ds->define_ruleset('1.2:refs:aux_selector' =>
	{ param => 'ref_id', valid =>  VALID_IDENTIFIER('REF'), list => ',',
	  bad_value => '_' },
	    "Select only records entered from one of a specified list of references,",
	    "indicated by reference identifier.  You can enter more than one identifier,",
	    "as a comma-separated list.",
    	{ param => 'ref_author', valid => MATCH_VALUE('.*\p{L}.*'), list => ',', 
	  bad_value => '_', errmsg => $no_auth },
    	    "Select only records entered from references for which any of the authors",
	    "matches the specified name or names.",
	    "You can specify names in any of the following patterns:",
	    "=over","=item Smith","=item J. Smith", "=item J. Smith and S. Jones", "=back",
	    "The last form selects only references where both names are listed as authors, in any order.",
	    "You can use C<%> and C<_> as wildcards, but each name must contain at least one letter.",
	    "You can include multiple names or name pairs, separated by commas.  In that",
	    "case, records matching any of them will be selected.",
    	{ param => 'ref_primary', valid => MATCH_VALUE('.*\p{L}.*'), list => ',', 
	  bad_value => '_', errmsg => $no_auth },
     	    "Select only records entered from references for which the primary author matches the specified name",
	    "or names (see B<C<ref_author>> above).  If you give a name like C<Smith and Jones>, then references",
	    "are selected only if Smith is the primary author and Jones is also an author.",
	    "You can include multiple names or name pairs, separated by commas.  In that case,",
	    "records matching any of them will be selected.",
	{ optional => 'ref_pubyr', valid => \&valid_pubyr, alias => 'pubyr' },
	    "Selects only records entered from references published during the indicated year or range of years.",
	    "The parameter value must match one of the following patterns:",
	    "=over","=item 2000","=item 1990-2000", "=item 1990-", "=item -2000", "=back",
	{ param => 'ref_title', valid => MATCH_VALUE('.*\p{L}.*'), errmsg => $no_letter },
	    "Select only records entered from references whose title matches the specified word or words.  You can",
	    "use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
    	{ param => 'pub_title', valid => MATCH_VALUE('.*\p{L}.*'), errmsg => $no_letter },
	    "Select only records entered from references in publications whose title matches the specified",
	    "word or words.  You can use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
	{ param => 'pub_type', valid => '1.2:refs:pubtype', list => ',', bad_value => 'NOTHING' },
	    "Select only references of the indicated type or types.  You can specify",
	    "one or more from the following list, separated by commas:", 
	{ param => 'ref_doi', valid => ANY_VALUE, list => ',' },
	    "Select only records entered from references with any of the specified DOIs.",
	    "You may specify one or more, separated by commas.");
    
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
    
    my $id = $request->clean_param('ref_id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    $request->strict_check;
    $request->extid_check;
    
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
    push @filters, $request->generate_refno_filter('r');
    push @filters, $request->generate_common_filters( { refs => 'r', bare => 'r' } );
    push @filters, '1=1' if $all_records;
    
    my $filter_string = join(' and ', @filters);
    
    # Check for strictness and external identifiers
    
    $request->strict_check;
    $request->extid_check;
    
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


# auto_complete_ref ( name, limit )
# 
# This operation provides for auto-completion of bibliographic references, designed to be called
# by the combined auto-complete operation. It will only trigger if the specified name ends in a
# four-digit year.

sub auto_complete_ref {
    
    my ($request, $name, $limit, $options) = @_;
    
    # Do nothing unless the name ends in a four-digit year.
    
    return unless $name =~ qr{ ^ (.*) \s+ (\d\d\d\d) $ }xsi;
    
    my $author_string = $1;
    my $pubyear = $2;
    
    my $dbh = $request->get_connection();
    
    $limit ||= 10;
    $options ||= { };
    my @filters;
    
    # Add a filter to select collections that match the specified name. Special-case it so that a
    # ^ will leave off the initial % wildcard.
    
    $author_string =~ s/\s+$//;
    my $quoted_year = $dbh->quote($pubyear);
    
    push @filters, $request->generate_auth_filter($author_string, 'primary');
    push @filters, "r.pubyr = $quoted_year";
    
    my $use_extids = $request->has_block('extids');
    
    # Construct the query.
    
    my $filter_string = join(' and ', @filters);
    
    my $sql = "
	SELECT r.reference_no, r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2,
		   r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, 
		   r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, 
		   r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, 
		   r.firstpage as r_fp, r.lastpage as r_lp, r.publication_type as r_pubtype 
	FROM refs as r
	WHERE $filter_string
	ORDER BY author1last, author1init, author2last, author2init LIMIT $limit";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result_list eq 'ARRAY' )
    {
	foreach my $r ( @$result_list )
	{
	    $r->{record_id} = $use_extids ? generate_identifier('REF', $r->{reference_no}) :
		$r->{reference_no};
	    
	    $r->{name} = $request->format_reference($r);
	}
	
	return @$result_list;
    }
    
    return;
}


# generate_ref_filters ( )
# 
# Generate the necessary filter clauses to reflect the query parameters.

sub generate_ref_filters {
    
    my ($request, $tables_hash) = @_;
    
    my $dbh = $request->get_connection;
    my @filters;
    
    if ( my $year = $request->clean_param('ref_pubyr') )
    {
	if ( $year =~ qr{ ^ (?: max_ | \s* - \s* ) (\d\d\d\d) $ }xs )
	{
	    push @filters, "r.pubyr <= $1";
	}
	
	elsif ( $year =~ qr{ ^ min_ (\d\d\d\d) $ }xs )
	{
	    push @filters, "r.pubyr >= $1";
	}
	
	elsif ( $year =~ qr{ ^ (\d\d\d\d) \s* - \s* (\d\d\d\d)? $ }xs )
	{
	    if ( defined $2 && $2 ne '' ) {
		push @filters, "r.pubyr between $1 and $2";
	    } else {
		push @filters, "r.pubyr >= $1";
	    }
	}
	
	elsif ( $year =~ qr{ ^ \d+ $ }xs )
	{
	    push @filters, "r.pubyr = $year";
	}
	
	else
	{
	    die "400 invalid value '$year' for parameter 'ref_pubyr'\n";
	}
	
	$tables_hash->{r} = 1;
    }
    
    if ( my $authorname = $request->clean_param('ref_author') )
    {
	push @filters, $request->generate_auth_filter($authorname, 'author');
	$tables_hash->{r} = 1;
    }
    
    if ( my $authorname = $request->clean_param('ref_primary') )
    {
	push @filters, $request->generate_auth_filter($authorname, 'primary');
	$tables_hash->{r} = 1;
    }
    
    if ( my $reftitle = $request->clean_param('ref_title') )
    {
	my $op = 'rlike';
	
	if ( $reftitle =~  qr{ ^ [!] \s* (.*) }x )
	{
	    $reftitle = $1;
	    $op = 'not rlike';
	}
	
	$reftitle =~ s/%/.*/g;
	$reftitle =~ s/_/./g;
	$reftitle =~ s/\s+/\\s+/g;
	$reftitle =~ s/\(/\\(/g;
	$reftitle =~ s/\)/\\)/g;
	
	my $quoted = $dbh->quote("^$reftitle");
	
	push @filters, "coalesce(r.reftitle, r.pubtitle) $op $quoted";
	$tables_hash->{r} = 1;
    }
    
    if ( my $pubtitle = $request->clean_param('pub_title') )
    {
	my $op = 'rlike';
	
	if ( $pubtitle =~ qr{ ^ [!] \s* (.*) }x )
	{
	    $pubtitle = $1;
	    $op = 'not rlike';
	}
	
	$pubtitle =~ s/%/.*/g;
	$pubtitle =~ s/_/.*/g;
	$pubtitle =~ s/\s+/\\s+/g;
	$pubtitle =~ s/\(/\\(/g;
	$pubtitle =~ s/\)/\\)/g;
	
	my $quoted = $dbh->quote("^$pubtitle");
	
	push @filters, "r.pubtitle $op $quoted";
	$tables_hash->{r} = 1;
    }
    
    if ( my @doi_list = $request->clean_param_list('ref_doi') )
    {
	my @fixed_list;
	my @like_list;
	my @filter_list;
	
	if ( @doi_list == 1 && $doi_list[0] eq '!' )
	{
	    push @filters, "(r.doi is null or r.doi = '')";
	}
	
	else
	{
	    foreach my $d ( @doi_list )
	    {
		if ( $d =~ /[_%]/ ) {
		    push @like_list, $d;
		} elsif ( defined $d && $d ne '' ) {
		    push @fixed_list, $d;
		}
	    }
	    
	    foreach my $d ( @like_list )
	    {
		my $quoted = $dbh->quote($d);
		push @filter_list, "r.doi like $quoted"; 
	    }
	    
	    if ( @fixed_list )
	    {
		my $quoted = join( ',', map { $dbh->quote($_) } @fixed_list );
		push @filter_list, "r.doi in ($quoted)";
	    }
	    
	    push @filter_list, "r.doi = 'NOTHING'" unless @filter_list;
	    push @filters, @filter_list;
	}
    }
    
    if ( my @type_list = $request->clean_param_list('pub_type') )
    {
	my $op = 'in';
	
	if ( @type_list == 1 && $type_list[0] eq '!' )
	{
	    push @filters, "(r.publication_type is null or r.publication_type = '')";
	}
	
	else
	{
	    my $quoted = join( ',', map { $dbh->quote($_) } @type_list );
	    push @filters, "r.publication_type $op ($quoted)";
	}
    }
    
    return @filters;
}


sub generate_refno_filter {
    
    my ($request, $mt) = @_;
    
    # If the parameter 'ref_id' was specified, return the appropriate filter.
    # If the value '_' is found, that means that all specified values were
    # invalid.  In that case, return a filter that will select no records.
    
    my @refno_list = $request->clean_param_list('ref_id');
    
    if ( @refno_list )
    {
	my $refno_str = join(',', @refno_list);
	$refno_str = '-1' if $refno_str eq '' or $refno_str eq '_';
	
	return "$mt.reference_no in ($refno_str)";
    }
    
    return;	# otherwise, return no filter at all
}


# generate_auth_filter ( name, selector )
# 
# Generate a filter for the specified author name(s).  The selector must be
# either 'author' or 'primary'; in the latter case, only the first author will
# be matched.

sub generate_auth_filter {

    my ($request, $authorname, $selector) = @_;
    
    my @authfilters;
    my $dbh = $request->get_connection;
    
    my @authnames = ref $authorname eq 'ARRAY' ? @$authorname : $authorname;
    
    # First check for the "bad value".  If this is found, then one of the
    # given values were valid.  In this case, we return a filter which will
    # select nothing.
    
    return "r.reference_no in (-1)" if @authnames == 0 || $authnames[0] eq '_';
    
    # If the author list starts with '!', we need to generate an exclusion filter.  But difficult
    # to implement because we would need to add "or <field> is null" to many of the filters.
    
    # my $op = ' or ';
    
    # if ( $authnames[0] && $authnames[0] =~ qr{ ^ [!] \s* (.*) }x )
    # {
    # 	$authnames[0] = $1;
    # 	$op = ' and ';
    # }
    
    # Then go through each name one by one.
    
    foreach my $name (@authnames)
    {   
	if ( $name =~ qr{ ^ (\w.*?) \s+ and (?: \s+ (.*) | $ ) }xsi )
	{
	    push @authfilters, $request->generate_two_auth_filter($dbh, $1, $2, $selector);
	}
	
	elsif ( $name =~ qr{ ^ (\w.*?) \s+ et \s+ al[.]? \s* $ }xsi )
	{
	    push @authfilters, $request->generate_one_auth_filter($dbh, $1, 'primary');
	}
	
	else
	{
	    push @authfilters, $request->generate_one_auth_filter($dbh, $name, $selector);
	}
    }
    
    # If we don't have any filters at this point, it is because none of the
    # names were validly formed.  So add a filter that will select nothing.
    
    push @authfilters, "r.reference_no in (-1)" unless @authfilters;
    
    return '(' . join(' or ', @authfilters) . ')';
}


# generate_one_auth_filter ( name, selector )
# 
# Generate a filter for a single author name.  If the selector is 'primary',
# then it must match the author1 fields.  If it is 'secondary' then it must
# match the author2 or otherauthor fields.  Otherwise, it may match any of the
# author fields.

sub generate_one_auth_filter {
    
    my ($request, $dbh, $name, $selector) = @_;
    
    # First check to make sure that the name contains at least one letter.
    
    unless ( $name =~ qr{ \p{L} }xs )
    {
	$request->add_warning("invalid author name '$name', must contain at least one letter");
	return;
    }
    
    my ($firstname, $lastname, $initpat, $lastpat, $fullpat, @authfilters);
	
    if ( $name =~ /(.*)[.] +(.*)/ )
    {
	$firstname = $1;
	$lastname = $2;
    }
    
    else
    {
	$lastname = $name;
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
	push @authfilters, "r.author1init rlike $initquote and r.author1last rlike $lastquote" unless $selector eq 'secondary';
	push @authfilters, "r.author2init rlike $initquote and r.author2last rlike $lastquote" unless $selector eq 'primary';
	push @authfilters, "r.otherauthors rlike $fullquote" unless $selector eq 'primary';
    }
    
    else
    {
	push @authfilters, "r.author1last rlike $lastquote" unless $selector eq 'secondary';
	push @authfilters, "r.author2last rlike $lastquote" unless $selector eq 'primary';
	push @authfilters, "r.otherauthors rlike $fullquote" unless $selector eq 'primary';
    }
    
    return @authfilters;
}


# generate_two_auth_filter ( name, selector )
# 
# Generate a filter for a name such as "Smith and Jones".  If the selector is
# 'primary', then the first author name must match the first of the two names.
# An error is thrown if more than two names are given.

sub generate_two_auth_filter {
    
    my ($request, $dbh, $name1, $name2, $selector) = @_;
    
    my (@filters1, @filters2);
    
    # First check to make sure that there isn't more than one 'and' in the
    # name.  We used a non-greedy match to find the first one above, so we
    # only need to check the second name.
    
    if ( $name2 =~ qr{ \s+ and (?: \s+ | $ ) }xsi )
    {
	$request->add_warning("invalid author selector '$name1 and $name2', only one 'and' is permitted");
	return;
    }
    
    elsif ( $name1 !~ qr{ \p{L} }xs || $name2 !~ qr{ \p{L} }xs )
    {
	$request->add_warning("invalid author selector '$name1 and $name2', each name must contain at least one letter");
	return;
    }
    
    # Now construct the appropiate expression.  If the selector is 'primary',
    # then the first name must be the primary author.
    
    if ( $selector eq 'primary' )
    {
	@filters1 = $request->generate_one_auth_filter($dbh, $name1, 'primary');
	@filters2 = $request->generate_one_auth_filter($dbh, $name2, 'secondary');
    }
    
    else
    {
	@filters1 = $request->generate_one_auth_filter($dbh, $name1, 'author');
	@filters2 = $request->generate_one_auth_filter($dbh, $name2, 'author');
    }
    
    my ($expr1, $expr2);
    
    if ( @filters1 == 1 )
    {
	$expr1 = $filters1[0];
    }
    
    else
    {
	$expr1 = '(' . join(' or ', @filters1) . ')';
    }
    
    if ( @filters2 == 1 )
    {
	$expr2 = $filters2[0];
    }
    
    else
    {
	$expr2 = '(' . join(' or ', @filters2) . ')';
    }
    
    return "($expr1 and $expr2)";
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
	    push @exprs, "r.author1last $dir, r.author1init $dir, ifnull(r.author2last, '') $dir, ifnull(r.author2last, '') $dir";
	}
	
	elsif ( $term eq 'pubyr' )
	{
	    push @exprs, "r.pubyr $dir";
	}
	
	elsif ( $term eq 'reftitle' )
	{
	    push @exprs, "r.reftitle $dir",
	}
	
	elsif ( $term eq 'pubtitle' )
	{
	    push @exprs, "r.pubtitle $dir",
	}
	
	elsif ($term eq 'pubtype' )
	{
	    push @exprs, "r.publication_type $dir",
	}
	
	elsif ( $term eq 'language' )
	{
	    push @exprs, "r.language $dir",
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
	    push @exprs, "r.created $dir";
	}
	
	elsif ( $term eq 'modified' )
	{
	    $dir ||= 'desc';
	    push @exprs, "r.modified $dir";
	}
	
	else
	{
	    die "400 bad value for parameter 'order': must be a valid order expression with optional suffix '.asc' or '.desc' (was '$term')\n";
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
# fields "r_al1", "r_ai1", "r_al2", "r_ai2", "r_oa", "r_pubyr", "r_reftitle",
# "r_pubtitle", "r_pubvol", "r_pubno".
# 

sub format_reference {

    my ($request, $row) = @_;
    
    my $markup = $request->clean_param('markrefs');
    
    # First format the author string.  This includes stripping extra periods
    # from initials and dealing with "et al" where it occurs.
    
    my $ai1 = $row->{r_ai1} || '';
    my $al1 = $row->{r_al1} || '';
    
    #$ai1 =~ s/\.//g;
    #$ai1 =~ s/([A-Za-z])/$1./g;
    
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
    
    my $auth3 = $row->{r_oa} || '';
    
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
    
    my $publisher = $row->{r_publisher};
    my $pubcity = $row->{r_pubcity};

    if ( $publisher )
    {
	$longref =~ s/\s+$//;
	$longref .= ". ";
	$longref .= "$pubcity: " if $pubcity;
	$longref .= $publisher . ". ";
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


# format_authors ( )
# 
# Generate an attribution string for the primary reference associated with
# the given record.  This relies on the fields "r_al1", "r_al2", "r_oa".  This
# string does not include the publication year.

sub format_authors {

    my ($request, $record) = @_;
    
    my $auth1 = $record->{r_al1} || '';
    my $auth2 = $record->{r_al2} || '';
    my $auth3 = $record->{r_oa} || '';
    
    $auth1 =~ s/( Jr)|( III)|( II)//;
    $auth1 =~ s/\.$//;
    $auth1 =~ s/,$//;
    $auth2 =~ s/( Jr)|( III)|( II)//;
    $auth2 =~ s/\.$//;
    $auth2 =~ s/,$//;
    
    my $attr_string = $auth1;
    
    if ( $auth3 ne '' or $auth2 =~ /et al/ )
    {
	$attr_string .= " et al.";
    }
    elsif ( $auth2 ne '' )
    {
	$attr_string .= " and $auth2";
    }
    
    return $attr_string;
}


# set_reference_type ( )
# 
# Set the ref_type field for a reference record.  This is based on fields
# such as 'is_auth', etc.

sub set_reference_type {
    
    my ($request, $record) = @_;
    
    my $ref_type = $record->{ref_type} || '';
    my @types;
    
    if ( $ref_type =~ qr{A} || $record->{n_refauth} )
    {
	push @types, 'auth';
    }
    
    if ( $ref_type =~ qr{V} || $record->{n_refvar} )
    {
	push @types, 'var';
    }
    
    if ( $ref_type =~ qr{C} || $record->{n_refclass} )
    {
	push @types, 'class';
    }
    
    if ( $ref_type =~ qr{U} || $record->{n_refunclass} )
    {
	push @types, 'unclass';
    }
    
    if ( $ref_type =~ qr{O} || $record->{n_refoccs} )
    {
	push @types, 'occ';
    }
    
    if ( $ref_type =~ qr{X} )
    {
	push @types, 'suppressed';
    }
    
    if ( $ref_type =~ qr{S} || $record->{n_refspecs} )
    {
	push @types, 'spec';
    }
    
    if ( $ref_type =~ qr{P} || $record->{n_refcolls} )
    {
	push @types, 'coll';
    }
    
    # if ( defined $record->{n_refcolls} && defined $record->{n_refprim} && 
    # 	 $record->{n_refcolls} > $record->{n_refprim} )
    # {
    # 	push @types, 'sec';
    # }
    
    push @types, 'ref' unless @types;
    
    return join(',', @types);
}


sub process_ref_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
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


# valid_pubyr ( )
# 
# Validate the value of the 'ref_pubyr' parameter.  If it is valid, return a
# cleaned value.  Otherwise, return an error record with an appropriate
# message.
# 
# Accepted values are: a 4-digit year, a range of 4-digit years, or a 4-digit
# year prefixed by 'max_' or 'min_'.

sub valid_pubyr {
    
    my ($value, $context) = @_;
    
    if ( $value =~ qr{ ^ (?: max_ | min_ | \s* - \s* )? \d\d\d\d $ }xs )
    {
	return;
    }
    
    elsif ( $value =~ qr{ ^ (\d\d\d\d) \s* - \s* (\d\d\d\d)? $ }xs )
    {
	my $top = $2 // '';
	return "$1 - $top";
    }
    
    else
    {
	return { errmsg => "the value of {param} must be a 4-digit year or a range of years (was {value})" };
    }
}

1;
