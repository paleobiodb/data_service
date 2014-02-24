# 
# OccurrenceData
# 
# A class that returns information from the PaleoDB database about a single
# occurence or a category of occurrences.  It is a subclass of 'CollectionData'.
# 
# Author: Michael McClennen

package OccurrenceData;

use strict;

use parent 'CollectionData';

use Web::DataService qw(:validators);

use CommonData;
use OccurrenceTables qw($OCC_MATRIX);
use CollectionTables qw($COLL_MATRIX $COLL_BINS @BIN_LEVEL $COUNTRY_MAP);
use CollectionData;
use IntervalTables qw($INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP);
use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_RANK %RANK_STRING);
use Taxonomy;


our (@REQUIRES_CLASS) = qw(CommonData ReferenceData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($self, $ds) = @_;
    
    # We start by defining an output map for this class.
    
    $ds->define_output_map('1.1:occs:basic_map' =>
	{ value => 'basic', maps_to => '1.1:occs:basic', fixed => 1 },
	{ value => 'coords', maps_to => '1.1:occs:geo' },
	     "The latitude and longitude of this occurrence",
        { value => 'attr', maps_to => '1.1:colls:attr' },
	    "The attribution of the occurrence: the author name(s) from",
	    "the primary reference, and the year of publication.  If no reference",
	    "is recorded for this occurrence, the reference for its collection is used.",
	{ value => 'ident', maps_to => '1.1:occs:ident' },
	    "The actual taxonomic name by which this occurrence was identified",
	{ value => 'phylo', maps_to => '1.1:occs:phylo' },
	    "Additional information about the taxonomic classification of the occurence",
        { value => 'loc', maps_to => '1.1:colls:loc' },
	    "Additional information about the geographic locality of the occurrence",
	{ value => 'prot', maps_to => '1.1:colls:prot' },
	    "Indicate whether the containing collection is on protected land",
        { value => 'time', maps_to => '1.1:colls:time' },
	    "Additional information about the temporal locality of the occurrence",
	{ value => 'strat', maps_to => '1.1:colls:strat' },
	    "Basic information about the stratigraphic context of the occurrence.",
	{ value => 'stratext', maps_to => '1.1:colls:stratext' },
	    "Detailed information about the stratigraphic context of the occurrence.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.1:colls:lith' },
	    "Basic information about the lithological context of the occurrence.",
	{ value => 'lithext', maps_to => '1.1:colls:lithext' },
	    "Detailed information about the lithological context of the occurrence.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'geo', maps_to => '1.1:colls:geo' },
	    "Information about the geological context of the occurrence",
        { value => 'rem', maps_to => '1.1:colls:rem' },
	    "Any additional remarks that were entered about the containing collection.",
        { value => 'ref', maps_to => '1.1:refs:primary' },
	    "The primary reference for the occurrence, as formatted text.",
	    "If no reference is recorded for this occurrence, the primary reference for its",
	    "collection is returned.",
	{ value => 'ent', maps_to => '1.1:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.1:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.1:common:crmod' },
	    "The C<created> and C<modified> timestamps for the occurrence record");
    
    # Then define those blocks which are not already defined in
    # CollectionData.pm 
    
    $ds->define_block('1.1:occs:basic' =>
	{ select => ['o.occurrence_no', 'o.collection_no',
		     'ts.spelling_no as taxon_no', 'ts.name as taxon_name', 'ts.rank as taxon_rank', 
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'o.early_age', 'o.late_age', 'o.reference_no'],
	  tables => ['t', 'ts', 'ei', 'li'] },
	{ output => 'occurrence_no', dwc_name => 'occurrenceID', com_name => 'oid' },
	    "A positive integer that uniquely identifies the occurrence",
	{ output => 'record_type', value => 'occurrence', com_name => 'typ', com_value => 'occ', 
	  dwc_value => 'Occurrence',  },
	    "The type of this object: 'occ' for an occurrence",
	{ output => 'collection_no', com_name => 'cid', dwc_name => 'CollectionId' },
	    "The identifier of the collection with which this occurrence is associated.",
	{ output => 'taxon_name', com_name => 'tna', dwc_name => 'associatedTaxa' },
	    "The taxonomic name by which this occurrence is identified, with synonymy taken",
	    "into account.  This may be a synonym or alternate spelling of the name by which",
	    "it was actually identified.  To get the actual identification, use the C<show>",
	    "parameter to select the C<ident> block.  This field may be empty if the",
	    "identified taxonomic name was never entered into the taxonomic hierarchy",
	    "stored in this database.  If so, then we have no",
	    "information about the taxonomic classification of this occurrence.",
	{ set => 'taxon_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The taxonomic rank of this name",
	{ output => 'taxon_no', com_name => 'tid' },
	    "The unique identifier of this name.",
	{ set => '*', code => \&CollectionData::fixTimeOutput },
	{ output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	    "The specific geologic time range associated with this collection (not necessarily a",
	    "standard interval), or the interval that begins the range if C<late_interval> is also given",
	{ output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	    "The interval that ends the specific geologic time range associated with this collection,",
	    "if different from the value of C<early_interval>",
	{ output => 'early_age', com_name => 'eag' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	{ set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.1:occs:ident' =>
	{ select => ['oc.genus_name', 'oc.genus_reso', 'oc.species_name', 'oc.species_reso'],
	  tables => 'oc' },
	{ output => 'genus_name', com_name => 'idt' },
	    "The taxonomic name (less species) by which this occurrence was identified.",
	    "This is often a genus, but may be a higher taxon.",
	{ output => 'genus_reso', com_name => 'rst' },
	    "The resolution of this taxonomic name, i.e. C<sensu lato> or C<aff.>",
	{ output => 'species_name', com_name => 'ids' },
	    "The species name (if any) by which this occurrence was identified",
	{ output => 'species_reso', com_name => 'rss' },
	    "The resolution of the species name, i.e. C<sensu lato> or C<n. sp.>");
    
    $ds->define_block('1.1:occs:phylo' =>
	{ select => ['ph.family', 'ph.family_no', 'ph.order', 'ph.order_no',
		     'ph.class', 'ph.class_no', 'ph.phylum', 'ph.phylum_no'],
	  tables => ['ph', 't'] },
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this occurrence is classified",
	{ output => 'family_no', com_name => 'fmn' },
	    "The identifier of the family in which this occurrence is classified",
	{ output => 'order', com_name => 'odn' },
	    "The name of the order in which this occurrence is classified",
	{ output => 'order_no', com_name => 'odl' },
	    "The identifier of the order in which this occurrence is classified",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this occurrence is classified",
	{ output => 'class_no', com_name => 'cln' },
	    "The identifier of the class in which this occurrence is classified",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this occurrence is classified",
	{ output => 'phylum_no', com_name => 'phn' },
	    "The identifier of the phylum in which this occurrence is classified");
    
    $ds->define_block('1.1:occs:geo' =>
	{ select => ['c.lat', 'c.lng'], tables => 'c' },
        { output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng' },
	    "The longitude at which the occurrence was found (in degrees)",
        { output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat' },
	    "The latitude at which the occurrence was found (in degrees)");
    
    # Then define parameter rulesets to validate the parameters passed to the
    # operations implemented by this class.
    
    $ds->define_set('1.1:occs:order' =>
	{ value => 'earlyage' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'earlyage.asc', undoc => 1 },
	{ value => 'earlyage.desc', undoc => 1 },
	{ value => 'lateage' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'lateage.asc', undoc => 1 },
	{ value => 'lateage.desc', undoc => 1 },
	{ value => 'agespread' },
	    "Results are ordered based on the difference between the early and late age bounds, starting",
	    "with occurrences with the smallest spread (most precise temporal resolution) unless you add C<.desc>",
	{ value => 'agespread.asc', undoc => 1 },
	{ value => 'agespread.desc', undoc => 1 },
	{ value => 'taxon' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will follow immediately",
	    "after parents.",
	{ value => 'taxon.asc', undoc => 1 },
	{ value => 'taxon.desc', undoc => 1 },
	{ value => 'reso' },
	    "Results are ordered according to the taxonomic rank to which they are resolved.  Unless",
	    "you add C<.desc>, this would start with subspecies, species, genus, ...",
	{ value => 'reso.asc', undoc => 1 },
	{ value => 'reso.desc', undoc => 1 },
	{ value => 'formation' },
	    "Results are ordered by the geological formation in which they were found, sorted alphabetically.",
	{ value => 'formation.asc', undoc => 1 },
	{ value => 'formation.desc', undoc => 1 },
	{ value => 'geogroup' },
	    "Results are ordered by the geological group in which they were found, sorted alphabetically.",
	{ value => 'geogroup.asc', undoc => 1 },
	{ value => 'geogroup.desc', undoc => 1 },
	{ value => 'member' },
	    "Results are ordered by the geological member in which they were found, sorted alphabetically.",
	{ value => 'member.asc', undoc => 1 },
	{ value => 'member.desc', undoc => 1 },
	{ value => 'created' },
	    "Results are ordered by the date the record was created, most recent first",
	    "unless you add C<.asc>.",
	{ value => 'created.asc', undoc => 1 },
	{ value => 'created.desc', undoc => 1 },
	{ value => 'modified' },
	    "Results are ordered by the date the record was last modified",
	    "most recent first unless you add C<.asc>",
	{ value => 'modified.asc', undoc => 1 },
	{ value => 'modified.desc', undoc => 1 });
    
    $ds->define_ruleset('1.1:occs:specifier' =>
	{ param => 'id', valid => POS_VALUE, alias => 'occ_id' },
	    "The identifier of the occurrence you wish to retrieve (REQUIRED)");
    
    $ds->define_ruleset('1.1:occs:selector' =>
	{ param => 'id', valid => POS_VALUE, list => ',', alias => 'occ_id' },
	    "A comma-separated list of occurrence identifiers.",
	{ param => 'coll_id', valid => POS_VALUE, list => ',' },
	    "A comma-separated list of collection identifiers.  All occurrences associated with",
	    "the specified collections are returned, provided they satisfy the other parameters",
	    "given with this request");
    
    $ds->define_ruleset('1.1:occs:display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved occurrence.  Some of the additional fields describe the collection",
	"of which it is a member.",
	{ param => 'show', list => q{,},
	  valid => $ds->valid_set('1.1:occs:basic_map') },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:occs:basic_map'),
	{ optional => 'order', valid => $ds->valid_set('1.1:occs:order'), split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.1:occs:order'),
	    "If no order is specified, results are sorted by occurrence identifier.",
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.1:occs:single' => 
	"The following required parameter selects a record to retrieve:",
    	{ require => '1.1:occs:specifier', 
	  error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" },
    	{ allow => '1.1:occs:display' },
    	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
    
    $ds->define_ruleset('1.1:occs:list' => 
	">You can use the following parameters if you wish to retrieve information about",
	"a known list of occurrences or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.1:occs:selector' },
        ">The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	{ allow => '1.1:main_selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:occs:selector', '1.1:main_selector',
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	"You can also specify any of the following parameters:",
	{ allow => '1.1:occs:display' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
    
    $ds->define_ruleset('1.1:occs:refs' =>
	">You can use the following parameters if you wish to retrieve the references associated",
	"with a known list of occurrences or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.1:occs:selector' },
        ">The following parameters can be used to retrieve the references associated with occurrences",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	{ allow => '1.1:main_selector' },
	{ require_any => ['1.1:occs:selector', '1.1:main_selector'] },
	"You can also specify any of the following parameters:",
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ allow => '1.1:refs:filter' },
	{ allow => '1.1:refs:display' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id} || '';
    
    die "Bad identifier '$id'" unless defined $self->{params}{id} and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', $self->select_list({ mt => 'o', bt => 'oc' }));
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $self->tables_hash);
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE o.occurrence_no = $id and c.access_level = 0
	GROUP BY o.occurrence_no";
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
        
    return 1;
}


# list ( )
# 
# Query the database for basic info about all occurrences satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    my $tables = $self->tables_hash;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = CollectionData::generateMainFilters($self, 'list', 'c', $tables);
    push @filters, OccurrenceData::generateOccFilters($self, $tables);
    push @filters, CommonData::generate_crmod_filters($self, 'oc', $tables);
    push @filters, CommonData::generate_ent_filters($self, 'oc', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    $self->add_table('oc');
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'o', bt => 'oc' });
    
    $self->adjustCoordinates(\$fields);
    
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{ts} ? 'ts' : 't';
    
    my $order_clause = $self->generate_order_clause($tables, { at => 'c', bt => 'cc', tt => $tt }) || 'o.occurrence_no';
    
    # Determine if any extra tables need to be joined in.
    
    my $join_list = $self->generateJoinList('c', $self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# refs ( )
# 
# Query the database for the references associated with occurrences satisfying
# the conditions specified by the parameters.

sub refs {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = {};
    
    my @filters = CollectionData::generateMainFilters($self, 'list', 'c', $inner_tables);
    push @filters, $self->generateOccFilters($inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the references.
    
    my @ref_filters = ReferenceData::generate_filters($self, $self->tables_hash);
    push @ref_filters, CommonData::generate_crmod_filters($self, 'r');
    push @ref_filters, CommonData::generate_ent_filters($self, 'r');
    push @ref_filters, "1=1" unless @ref_filters;
    
    my $ref_filter_string = join(' and ', @ref_filters);
    
    # Figure out the order in which we should return the references.  If none
    # is selected by the options, sort by rank descending.
    
    my $order = $self->ReferenceData::generate_order_clause({ rank_table => 's' }) ||
	"r.author1last, r.author1init";
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'r', bt => 'r' });
    
    $self->adjustCoordinates(\$fields);
    
    my $inner_join_list = $self->generateJoinList('c', $inner_tables);
    my $outer_join_list = $self->ReferenceData::generate_join_list($self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields, s.reference_rank FROM refs as r, 1 as is_occ JOIN
	   (SELECT o.reference_no, count(*) as reference_rank
	    FROM $OCC_MATRIX as o STRAJOIN $COLL_MATRIX as c using (collection_no)
		$inner_join_list
            WHERE $filter_string
	    GROUP BY o.reference_no) as s using (reference_no)
	$outer_join_list
	WHERE $ref_filter_string
	ORDER BY $order
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# generateOccFilters ( tables_ref )
# 
# Generate a list of filter clauses that will be used to compute the
# appropriate result set.  This routine handles only parameters that are specific
# to occurrences.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.

sub generateOccFilters {

    my ($self, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my @filters;
    
    # Check for parameter 'id'
    
    if ( ref $self->{params}{id} eq 'ARRAY' and
	 @{$self->{params}{id}} )
    {
	my $id_list = join(',', @{$self->{params}{id}});
	push @filters, "o.occurrence_no in ($id_list)";
    }
    
    elsif ( $self->{params}{id} )
    {
	push @filters, "o.occurrence_no = $self->{params}{id}";
    }
    
    # Check for parameter 'coll_id'
    
    if ( ref $self->{params}{coll_id} eq 'ARRAY' and
	 @{$self->{params}{coll_id}} )
    {
	my $id_list = join(',', @{$self->{params}{coll_id}});
	push @filters, "o.collection_no in ($id_list)";
    }
    
    elsif ( $self->{params}{coll_id} )
    {
	push @filters, "o.collection_no = $self->{params}{coll_id}";
    }
    
    # Check for parameters 'person_no', 'person_name'
    
    if ( my $person_id = $self->clean_param('person_id') )
    {
	my $person_string = ref $person_id eq 'ARRAY' ? join(q{,}, @$person_id)
			  : defined $person_id	      ? $person_id
			  :				-1;
	
	push @filters, "(oc.authorizer_no in ($person_string) or oc.enterer_no in ($person_string))";
	$tables_ref->{oc} = 1;
    }
    
    return @filters;
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($self, $mt, $tables, $summary_join_field) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN collections as cc on c.collection_no = cc.collection_no\n"
	if $tables->{cc};
    $join_list .= "JOIN occurrences as oc on o.occurrence_no = oc.occurrence_no\n"
	if $tables->{oc};
    $join_list .= "JOIN authorities as a on a.taxon_no = o.taxon_no\n";
    
    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = o.orig_no\n"
	if $tables->{t};
    $join_list .= "LEFT JOIN taxon_trees as ts on ts.orig_no = t.synonym_no\n"
	if $tables->{ts};
    $join_list .= "LEFT JOIN taxon_ints as ph on ph.ints_no = t.ints_no\n"
	if $tables->{ph};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = o.reference_no\n" 
	if $tables->{r};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = c.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = c.enterer_no\n"
	if $tables->{ppe};
    $join_list .= "LEFT JOIN person as ppm on ppm.person_no = c.modifier_no\n"
	if $tables->{ppm};
    $join_list .= "LEFT JOIN $INTERVAL_MAP as im on im.early_age = $mt.early_age and im.late_age = $mt.late_age and scale_no = 1\n"
	if $tables->{im};
    
    $join_list .= "LEFT JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no\n"
	if $tables->{ei};
    $join_list .= "LEFT JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no\n"
	if $tables->{li};
    $join_list .= "LEFT JOIN $COUNTRY_MAP as ccmap on ccmap.cc = c.cc"
	if $tables->{ccmap};
    
    return $join_list;
}


1;
