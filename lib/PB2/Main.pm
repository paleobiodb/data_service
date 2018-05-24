# 
# Paleobiology Data Service version 1.2
# 
# This file defines version 1.2 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;
use feature 'unicode_strings';

package PBData;

use PB2::CommonData;
use PB2::ConfigData;
use PB2::IntervalData;
use PB2::TaxonData;
use PB2::CollectionData;
use PB2::OccurrenceData;
use PB2::SpecimenData;
use PB2::DiversityData;
use PB2::ReferenceData;
use PB2::PersonData;
use PB2::CombinedData;

{
    # We start by defining a data service instance for version 1.2
    
    our ($ds2) = Web::DataService->new(
	{ name => '1.2',
	  title => 'PBDB Data Service',
	  version => 'v2',
	  features => 'standard',
	  special_params => 'standard,count=rowcount',
	  path_prefix => 'data1.2/',
	  ruleset_prefix => '1.2:',
	  doc_template_dir => 'doc/1.2',
	  doc_compile_dir => 'doc/ttc-1.2' });
    
    
    # We then define the vocabularies that will be used to label the data
    # fields returned by this service.
    
    $ds2->define_vocab(
        { name => 'null', disabled => 1 },	 
	{ name => 'pbdb', title => 'PaleobioDB field names',
	  use_field_names => 1 },
	    "The PBDB vocabulary is derived from the underlying field names and values in the database,",
	    "augmented by a few new fields. For the most part any response that uses this vocabulary",
	    "will be directly comparable to downloads from the PBDB Classic interface.",
	    "This vocabulary is the default for L<text format|node:formats/text> responses.",
	{ name => 'com', title => 'Compact field names' },
	    "The Compact vocabulary is a set of 3-character field names designed to minimize",
	    "the size of the response message.  This is the default for L<JSON format|node:formats/json>",
	    "responses. Some of the field values are similarly abbreviated, while others are conveyed",
	    "in their entirety. For details, see the documentation for the individual response fields.",
	{ name => 'dwc', title => 'Darwin Core', disabled => 1 },
	    "The Darwin Core vocabulary follows the L<Darwin Core standard|http://www.tdwg.org/standards/450/>",
	    "set by the L<TDWG|http://www.tdwg.org/>.  This includes both the field names and field values.",
	    "Because the Darwin Core standard is XML-based, it is very strict.  Many",
	    "but not all of the fields can be expressed in this vocabulary; those that",
	    "cannot are unavoidably left out of the response.");
    
    
    # Then the formats in which data can be returned.
    
    $ds2->define_format(
	{ name => 'json', content_type => 'application/json',
	  doc_node => 'formats/json', title => 'JSON',
	  default_vocab => 'com' },
	    "The JSON format is intended primarily to support client applications,",
	    "including the PBDB Navigator.  Response fields are named using compact",
	    "3-character field names.",
	{ name => 'xml', disabled => 1, content_type => 'text/xml', title => 'XML',
	  doc_node => 'formats/xml', disposition => 'attachment',
	  default_vocab => 'dwc' },
	    "The XML format is intended primarily to support data interchange with",
	    "other databases, using the Darwin Core element set.",
	{ name => 'txt', content_type => 'text/plain',
	  doc_node => 'formats/text', title => 'Comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'csv', content_type => 'text/csv',
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'tsv', content_type => 'text/tab-separated-values', 
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Tab-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	# { name => 'html', content_type => 'text/html', doc_node => 'formats/html', title => 'HTML',
	#   module => 'Template', disabled => 1 },
	#     "The HTML format returns formatted web pages describing the selected",
	#     "object or objects from the database.",
	{ name => 'ris', content_type => 'application/x-research-info-systems',
	  doc_node => 'formats/ris', title => 'RIS', disposition => 'attachment',
	  encode_as_text => 1, default_vocab => '', module => 'RISFormat'},
	    "The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	    "common format for bibliographic references.",
	{ name => 'png', content_type => 'image/png', module => '',
	  default_vocab => '', doc_node => 'formats/png', title => 'PNG' },
	    "The PNG suffix is used with a few URL paths to fetch images stored",
	    "in the database.");
    
    
    # Then define the URL paths that this subservice will accept.  We start with
    # the root of the hierarchy, which sets defaults for all the rest of the nodes.
    
    $ds2->define_node({ path => '/', 
			public_access => 1,
			doc_default_op_template => 'operation.tt',
			allow_format => 'json,csv,tsv,txt',
			allow_vocab => 'pbdb,com',
			default_save_filename => 'pbdb_data',
			title => 'Documentation' });
    
    # If a default_limit value was defined in the configuration file, get that
    # now so that we can use it to derive limits for certain nodes.
    
    my $base_limit = $ds2->node_attr('/', 'default_limit');
    my $taxa_limit = $base_limit ? $base_limit * 5 : undef;
    $taxa_limit = 20000 if defined $taxa_limit && $taxa_limit < 20000;
    my $ref_limit = $base_limit ? $base_limit * 5 : undef;
    $ref_limit = 10000 if defined $ref_limit && $ref_limit < 10000;
    
    # Configuration. This path is used by clients who need to configure themselves
    # based on parameters supplied by the data service.
    
    $ds2->define_node({ path => 'config',
			place => 10,
			title => 'Client configuration',
			usage => [ "config.json?show=all",
				   "config.txt?show=clusters" ],
			role => 'PB2::ConfigData',
			method => 'get',
			optional_output => '1.2:config:get_map' },
	"This operation provides information about the structure, encoding and organization",
	"of the information in the database. It is designed to enable the easy",
	"configuration of client applications.");
    
    # Combined data.
    
    $ds2->define_node({ path => 'combined',
			place => 11,
			title => 'Combined data',
			role => 'PB2::CombinedData' },
	"The operations in this group provide access to multiple types of data records,",
	"including auto-completion for client applications.");
    
    $ds2->define_node({ path => 'combined/auto',
			place => 1,
			title => 'General auto-completion',
			method => 'auto_complete',
			output => '1.2:combined:auto',
			optional_output => '1.2:combined:auto_optional'},
	"Return a list of names matching any string of characters. This operation is intended",
	"to be used for auto-completion in client applications. The desired record types",
	"can be specified using the B<C<type>> parameter, and the number of records to be returned",
	"using B<C<limit>>.");
    
    # Occurrences.  These paths are used to fetch information about fossil
    # occurrences known to the database.
    
    $ds2->define_node({ path => 'occs',
			place => 1,
			title => 'Fossil occurrences',
			role => 'PB2::OccurrenceData',
			allow_format => '+xml' },
	"A fossil occurence represents the occurrence of a particular organism at a particular",
	"location in time and space. Each occurrence is a member of a single fossil collection,",
	"and has a taxonomic identification which may be more or less specific.  The fossil occurrence",
	"records are the core data concept around which this database is built.");
    
    $ds2->define_node({ path => 'occs/single',
			place => 1,
			usage => [ "/occs/single.json?id=1001&show=loc", 
				   "/occs/single.txt?id=1001&show=loc,crmod" ],
			method => 'get_occ',
			before_operation_hook => 'prune_field_list',
			output => '1.2:occs:basic',
			optional_output => '1.2:occs:basic_map',
			title => 'Single fossil occurrence'},
	"This operation returns information about a single occurrence, selected by its identifier.",
	"Depending upon which output blocks you select, the response will contain some",
	"fields describing the occurrence and some describing the collection to which it belongs.");
    
    $ds2->define_node({ path => 'occs/list',
			place => 2,
			usage => [ "/occs/list.txt?base_name=Cetacea&interval=Miocene&show=loc,class",
				   "/occs/list.json?base_name=Cetacea&interval=Miocene&show=loc,class" ],
			method => 'list_occs',
			before_operation_hook => 'prune_field_list',
			output => '1.2:occs:basic',
			optional_output => '1.2:occs:basic_map',
			title => 'Lists of fossil occurrences' },
	"This operation returns information about multiple occurrences, selected according to the parameters you provide.",
	"You can select occurrences by taxonomy, geography, age, environment, and many other criteria.",
	"If you select the C<csv> or C<tsv> output format, the output you get will be very similar to the Classic",
	"occurrence download.");
    
    $ds2->define_node({ path => 'occs/geosum',
			place => 2,
			usage => [ "occs/geosum.json?base_name=cetacea&level=2",
				   "config.json?show=clusters" ],
			role => 'PB2::CollectionData',
			method => 'summary',
			output => '1.2:colls:summary',
			optional_output => '1.2:colls:summary_map',
			title => 'Geographic summary of fossil occurrences' },
	"This operation summarizes the selected set of occurrences by mapping them onto geographic clusters.",
	"Its purpose is to provide for the generation of maps displaying the geographic distribution of",
	"fossil occurrences.  You can specify any of the parameters that are available for the",
	"L<occs/list|node:occs/list> operation described above.  Multiple levels of geographic resolution",
	"are available.");
    
    $ds2->define_node({ path => 'occs/diversity',
			place => 5,
			usage => [ "/occs/diversity.txt?base_name=Dinosauria^Aves&continent=NOA&count=genera" ],
			method => 'diversity',
			output => '1.2:occs:diversity',
			default_limit => 1000,
			summary => '1.2:occs:diversity:summary',
			title => 'Fossil diversity over time (full computation)' },
	"This operation returns a tabulation of fossil diversity over time, based on a selected set of occurrences.",
	"You can select the set of occurrences to be analyzed using any of the parameters that are",
	"valid for the L<occs/list|node:occs/list> operation.  This operation can take up a lot of server time,",
	"so if you just want to display a quick overview plot please use the L<occs/quickdiv|node:occs/quickdiv>",
	"operation.");
    
    $ds2->extended_doc({ path => 'occs/diversity' },
	"It is very important to note that the diversity statistics returned by this",
	"operation reflect only I<the fossil occurrences recorded in this database>, and",
	"not the entire fossil record.  Note also that any occurrences that",
	"are insufficiently resolved either temporally or taxonomically are ignored.",
	"If you wish to apply a different procedure for determining how to count such",
	"occurrences, we suggest that you use the L<occs/list|node:occs/list> operation",
	"instead and apply your procedure directly to the returned list of occurrences.",
	">The field names returned by this operation are derived from the following source:",
	    "M. Foote. Origination and Extinction Components of Taxonomic Diversity: General Problems.",
	    "I<Paleobiology>, Vol. 26(4). 2000.",
	    "pp. 74-102. L<http://www.jstor.org/stable/1571654>.");
    
    $ds2->define_node({ path => 'occs/quickdiv',
			place => 6,
			usage => [ "/occs/quickdiv.txt?base_name=Dinosauria^Aves&continent=NOA&count=genera" ],
			method => 'quickdiv',
			output => '1.2:occs:quickdiv',
			default_limit => 1000,
			title => 'Fossil diversity over time (quick computation)' },
	"This operation returns a tabulation of fossil diversity over time, similar to that",
	"provided by L<occs/diversity|node:occs/diversity>.  It returns results much more quickly,",
	"but returns",
	"only the basic counts of distinct taxa appearing in each time interval.",
	"This operation is intended for quick overview plots; if you want to do",
	"detailed diversity analyses, we suggest using the L<occs/diversity|node:occs/diversity>",
	"operation instead, or downloading a L<list of occurrences|node:occs/list> and",
	"performing your own procedure to tabulate the diversity of taxa over time.");
    
    $ds2->define_node({ path => 'occs/checkdiv',
    			place => 6,
    			usage => [ "/occs/checkdiv.txt?base_name=Dinosauria^Aves&continent=NOA&count=genera&list=Santonian" ],
    			method => 'diversity',
    			arg => 'check',
    			output => '1.2:occs:checkdiv',
    			title => 'Fossil diversity over time (diagnostic)' },
    	"This operation provides a means of checking the taxa counted by the",
    	"L<occs/diversity|node:occs/diversity> operation. You can pass the same",
    	"parameters to this operation as you pass to the latter, but add either the B<C<diag>>",
	"or the B<C<list>> parameter. The former will show you how the relevant occurrences are",
	"being interpreted, while the latter will show you which taxonomic names were counted.");
    
    $ds2->define_node({ path => 'occs/taxa',
			place => 3,
			usage => [ "/occs/taxa.txt?base_name=Cetacea&interval=Miocene&show=attr",
				   "/occs/taxa.txt?strat=green river fm&rank=genus-order&show=attr" ],
			method => 'list_occs_taxa',
			output => '1.2:taxa:basic',
			optional_output => '1.2:occs:taxa_opt',
			summary => '1.2:occs:taxa_summary',
			default_limit => $taxa_limit,
			title => 'Taxonomy of fossil occurrences', },
	"This operation returns the taxonomic hierarchy of a selected set of fossil occurrences.",
	"You can select the set of occurrences to be analyzed using any of the parameters that are",
	"valid for the L<occs/list|node:occs/list> operation.  You can make requests using both",
	"both operations with identical parameters, which will give you both a list of",
	"occurrences and a summary tabulation by taxon.  If you include the block F<subcounts>,",
	"then each taxon record will include a count of the number of species, genera, etc.",
	"from the selected set of occurrences that are contained within that taxon.");
    
    $ds2->extended_doc({ path => 'occs/taxa' },
	"The result of this operation reports every taxon appearing in the selected set of occurrences,",
	"in hierarchical order.  It includes the number of occurrences specifically identified to each",
	"listed taxon, along with the total number of occurrences of the taxon including all subtaxa.",
	"It also includes the number of species, genera, families and orders within each taxon that",
	"appear within the selected set of occurrences.  The parent taxon identifier is also reported",
	"for each taxon, so that you are able to organize the result records into their proper hierarchy.");
    
    $ds2->define_node({ path => 'occs/prevalence',
			place => 8,
			usage => [ "/occs/prevalence.json?continent=noa&interval=mesozoic&limit=10" ],
			title => 'Most prevalent taxa',
			method => 'prevalence',
			output => '1.2:occs:prevalence',
			default_limit => 20},
	"This operation returns a list of the most prevalent taxa (according to number of occurrences)",
	"from among the selected set of fossil occurrences.  These taxa will be phyla and/or classes,",
	"depending upon the size of the list and the requested number of entries.",
	"Major taxa that are roughly at the level of classes may be included even if they are not",
	"not formally ranked at that level.",
	"Unlike most of the operations of this data service, the parameter C<limit> is",
	"significant in determining the elements of the result.  A",
	"larger limit will tend to show classes instead of phyla.");
    
    $ds2->define_node({ path => 'occs/strata',
			place => 9,
			usage => [ "/occs/strata.json?base_name=Cetacea&interval=Miocene&textresult" ],
			method => 'list_occs_strata',
			output => '1.2:strata:occs',
			optional_output => '1.2:strata:basic_map',
			title => 'Stratigraphy of fossil occurrences' },
	"This operation returns information about the geological strata in which fossil occurrences",
	"were found.  You can pass identical filtering parameters to L<occs/list|node:occs/list> and",
	"L<occs/strata|node:occs/strata> which will give you both a list of occurrences and a summary",
	"by stratum.");
    
    $ds2->define_node({ path => 'occs/refs',
			place => 10,
			usage => [ "/occs/refs.ris?base_name=Cetacea&interval=Miocene&textresult" ],
			title => 'Bibliographic references for fossil occurrences',
			method => 'list_occs_associated',
			arg => 'refs',
			allow_format => '+ris,-xml',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation returns information about the bibliographic references associated with fossil occurrences.",
	"You can pass identical filtering parameters to L<occs/list|node:occs/list> and to L<occs/refs|node:occs/refs>,",
	"which will give you both a list of occurrences and a list of the associated references.");
    
    $ds2->define_node({ path => 'occs/byref',
			place => 10,
			usage => [ "occs/byref.txt?base_name=Cetacea&interval=Miocene&textresult" ],
			title => "Occurrences grouped by bibliographic reference",
			method => 'list_occs',
			arg => 'byref',
			output => '1.2:occs:basic',
			optional_output => '1.2:occs:basic_map' },
	"This operation returns information about multiple occurrences, selected with respect to some combination of",
	"the attributes of the occurrences and the attributes of the bibliographic reference(s) from which",
	"they were entered.  You can use this operation in",
	"conjunction with L<occs/refs|node:occs/refs> to show, for each selected reference, all of the occurrences",
	"entered from it, or all which meet certain criteria.");
   
    $ds2->define_node({ path => 'occs/taxabyref',
			place => 11,
			usage => [ "/occs/taxabyref.txt?base_name=Cetacea&interval=Miocene&textresult" ],
			title => 'Taxa associated with fossil occurrences grouped by bibliographic reference',
			method => 'list_occs_associated',
			arg => 'taxa',
			output => '1.2:taxa:reftaxa',
			optional_output => '1.2:taxa:mult_output_map' },
	"This operation returns information about taxonomic names associated with fossil occurrences,",
	"grouped according to the bibliographic",
	"reference in which they are mentioned.  You can use this operation in conjunction with",
	"L<node:occs/refs> to show, for each reference, all of the taxa entered from it that",
	"are associated with at least one occurrence from the selected set.");
    
    $ds2->define_node({ path => 'occs/opinions',
			place => 11,
			usage => [ "/occs/opinions.txt?base_name=Cetacea&interval=Miocene&textresult" ],
			title => 'Opinions for fossil occurrences',
			method => 'list_occs_associated',
			arg => 'opinions',
			default_limit => $ref_limit,
			output => '1.2:opinions:basic',
			optional_output => '1.2:opinions:output_map' },
	"This operation returns information about taxonomic opinions associated with fossil occurrences.",
	"You can use this to retrieve just the opinions relevant to any selected set of occurrences.");
    
    # Collections.  These paths are used to fetch information about fossil
    # collections known to the database.
    
    $ds2->define_node({ path => 'colls',
			place => 1,
			title => 'Fossil collections',
			role => 'PB2::CollectionData',
			use_cache => '1.2:colls' },
	"A fossil collection is somewhat loosely defined as a set of fossil occurrences that are",
	"co-located geographically and temporally. Each collection has a geographic location,",
	"stratigraphic context, and age estimate.");
    
    $ds2->define_node({ path => 'colls/single',
			place => 1,
			title => 'Single fossil collection',
			usage => [ "colls/single.json?id=50068&show=loc,stratext" ],
			before_operation_hook => 'prune_field_list',
			method => 'get_coll',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"This operation returns information about a single collection, selected by its identifier.");
    
    $ds2->define_node({ path => 'colls/list',
			place => 2,
			title => 'Lists of fossil collections',
			usage => [ "colls/list.txt?base_name=Cetacea&interval=Miocene&show=ref,loc,stratext" ],
			before_operation_hook => 'prune_field_list',
			method => 'list_colls',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"This operation returns information about multiple collections, selected according to the parameters you provide.",
	"You can select collections by taxonomy, geography, age, environment, and many other criteria.",
	"If you select the C<csv> or C<tsv> output format, the output you get will be very similar to the Classic",
	"collection download.");
    
    $ds2->define_node({ path => 'colls/summary',
			place => 3,
			title => 'Geographic summary of fossil collections',
			usage => [ "colls/summary.json?lngmin=0.0&lngmax=15.0&latmin=0.0&latmax=15.0&level=2",
				   "config.json?show=clusters" ],
			method => 'summary',
			output => '1.2:colls:summary',
			optional_output => '1.2:colls:summary_map' },
	"This operation is essentially the same as L<occs/geosum|node:occs/geosum>.  It summarizes the selected set",
	"of collections by mapping them onto geographic clusters.",
	"Its purpose is to provide for the generation of maps displaying the geographic distribution of",
	"fossil collections.  You can specify any of the parameters that are available for the",
	"L<occs/list|node:occs/list> operation described above.  Multiple levels of geographic resolution",
	"are available.");
    
    $ds2->define_node({ path => 'colls/refs',
			place => 4,
			title => 'Bibliographic references for fossil collections',
			usage => [ "colls/refs.ris?base_name=Cetacea&interval=Miocene&show=comments&textresult" ],
			method => 'refs',
			allow_format => '+ris',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation returns information about the bibliographic references associated with fossil",
	"collections.  You can pass identical filtering parameters to L<colls/byref|node:colls/byref>",
	"and L<colls/refs|node:colls/refs>, which will give you both a list of collections and a list",
	"of the associated references.  However, the operation L<occs/refs|node:occs/refs> is",
	"much more flexible.  It allows you to retrieve taxonomy and specimen references as well as",
	"collection and occurrence references, and can report the number of taxa, occurrences, specimens,",
	"etc. entered from each record.  If you are looking for any of this information, you should",
	"use that operation instead.");
    
    $ds2->define_node({ path => 'colls/byref',
			place => 4,
			title => 'Collections grouped by bibliographic reference',
			usage => [ "colls/refs.ris?base_name=Cetacea&interval=Miocene&show=comments&textresult" ],
			method => 'list_colls',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"This operation returns information about multiple collections, selected with respect to some combination of",
	"the attributes of the collections and the attributes of the bibliographic reference(s) from which",
	"they were entered.  You can use this operation in",
	"conjunction with L<colls/refs|node:colls/refs> to show, for each selected reference, all of the collections",
	"entered from it, or all which meet certain criteria.");
    
    # Strata.  These paths are used to fetch information about geological strata
    # known to the database.
    
    $ds2->define_node({ path => 'strata',
			place => 4,
			title => 'Geological strata',
			role => 'PB2::CollectionData' },
	"Most of the fossil collections in the database are categorized by the formation",
	"from which each was collected, and many by group and member.");
    
    $ds2->define_node({ path => 'strata/list',
			place => 1,
			title => 'Lists of geological strata',
			usage => [ "strata/list.txt?lngmin=0&lngmax=15&latmin=0&latmax=15&rank=formation" ],
			method => 'list_coll_strata',
			output => '1.2:strata:basic',
			optional_output => '1.2:strata:basic_map' },
	"This operation returns information about geological strata selected by name, rank,",
	"and/or geographic location.");
    
    $ds2->define_node({ path => 'strata/auto',
			place => 2,
			title => 'Auto-completion for geological strata',
			usage => [ "strata/auto.json?name=aba&limit=10" ],
			method => 'strata_auto',
			default_limit => 10,
			output => '1.2:strata:auto' },
	"This operation returns a list of geological strata from the database that match the given",
	"prefix or partial name.  This can be used to implement auto-completion for strata names,",
	"and can be limited by geographic location if desired.");
    
    # Specimens and measurements.  These operations are used to fetch
    # information about specimens and associated measurements.
    
    $ds2->define_node({ path => 'specs',
			place => 1,
			title => 'Specimens and measurements',
			role => 'PB2::SpecimenData' },
	"Many of the fossil occurrences in the database are based on specimens that can",
	"be examined and measured.  There are also specimens entered into the database",
	"for which no information was available as to the location and context in which",
	"they were found.");
    
    $ds2->define_node({ path => 'specs/single',
			place => 1,
			title => 'Single specimen',
			usage => [ "specs/single.json?id=1027&show=class,ecospace" ],
			method => 'get_specimen',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"This operation returns information about a single fossil specimen, identified either",
	"by name or by identifier.");
    
    $ds2->define_node({ path => 'specs/list',
			place => 2,
			title => 'Lists of specimens',
			usage => [ "specs/list.txt?base_name=stegosauria" ],
			method => 'list_specimens',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"This operation returns information about multiple specimens, selected according to the parameters you provide.",
	"Depending upon which output blocks you select, the response will contain some",
	"fields describing the specimens and some describing the occurrences and collections (if any)",
	"with which they are associated.");
    
    $ds2->define_node({ path => 'specs/refs',
			place => 3,
			title => 'Bibiographic references for specimens',
			method => 'list_specimens_associated',
			arg => 'refs',
			allow_format => '+ris,-xml',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation returns information about the bibliographic references associated with fossil specimens.",
	"You can pass identical filtering parameters to L<specs/byref|node:specs/byref> and to L<specs/refs|node:specs/refs>,",
	"which will give you both a list of occurrences and a list of the associated references.");
    
    $ds2->define_node({ path => 'specs/byref',
			place => 4,
			title => 'Specimens grouped by bibliographic reference',
			method => 'list_specimens',
			arg => 'byref',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"This operation returns information about multiple specimens, selected with respect to some combination of",
	"the attributes of the occurrences and the attributes of the bibliographic reference(s) from which",
	"they were entered.  You can use this operation in",
	"conjunction with L<specs/refs|node:specs/refs> to show, for each selected reference, all of the specimens",
	"entered from it, or all which meet certain criteria.");
    
    $ds2->define_node({ path => 'specs/measurements',
			place => 5,
			title => 'Measurements of specimens',
			method => 'list_measurements',
			output => '1.2:measure:basic',
		        optional_output => '1.2:measure:output_map' },
	"This operation returns information about the measurements associated with selected",
	"specimens.");
    
    $ds2->define_node({ path => 'specs/elements',
			place => 6,
			title => 'Specimen elements',
			method => 'list_elements',
			output => '1.2:specs:element',
			optional_output => '1.2:specs:element_map' },
	"This operation returns lists of elements that can be used to describe specimens,",
	"for example 'bone', 'tooth', 'valve'.");
    
    # Taxa.  These paths are used to fetch information about biological taxa known
    # to the database.
    
    my $show = $ds2->special_param('show');
    
    $ds2->define_node({ path => 'taxa',
			place => 2,
			title => 'Taxonomic names',
			role => 'PB2::TaxonData',
			output => '1.2:taxa:basic' },
	"The taxonomic names stored in the database are arranged hierarchically.",
	"Our tree of life is quite complete down to the class level, and reasonably complete",
	"down to the suborder level. Below that, coverage varies. Many parts of the tree have",
	"been completely entered, while others are sparser.");
    
    $ds2->define_node({ path => 'taxa/single',
			place => 1,
			title => 'Single taxon',
			usage => [ "taxa/single.json?id=txn:69296&show=attr",
				   "taxa/single.txt?name=Dascillidae" ],
			method => 'get_taxon',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			optional_output => '1.2:taxa:single_output_map' },
	"This operation returns information about a single taxonomic name, specified either",
	"by name or by identifier.",
	">>Follow this link for more information on ",
	"L<the use of taxonomic names in this data service|node:general/taxon_names>.");
    
    $ds2->define_node({ path => 'taxa/list',
			place => 2,
			title => 'Lists of taxa',
			usage => [ "taxa/list.txt?id=69296&rel=all_children&show=ref",
				   "taxa/list.json?name=Dascillidae&rel=all_parents" ],
			method => 'list_taxa',
			default_limit => $taxa_limit,
			allow_format => '+xml',
			allow_vocab => '+dwc',
			optional_output => '1.2:taxa:mult_output_map' },
	"This operation returns information about multiple taxonomic names, selected according to",
	"the criteria you specify.  This operation could be used to query for all of the children",
	"or parents of a given taxon, among other operations.");
    
    $ds2->define_node({ path => 'taxa/refs',
			place => 7,
			title => 'Bibliographic references for taxa',
			usage => [ "taxa/refs.ris?base_name=Felidae&textresult" ],
			method => 'list_associated',
			arg => 'refs',
			default_limit => $ref_limit,
			allow_format => '+ris',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation returns information about the bibliographic references associated with taxonomic names.",
	"You can pass identical filtering parameters to L<node:taxa/list> and to L<node:taxa/refs>,",
	"which will give you both a list of taxonomic names and a list of the associated references.");
    
    $ds2->define_node({ path => 'taxa/byref',
			place => 8,
			title => 'Taxa grouped by bibliographic reference',
			usage => [ "taxa/byref.txt?base_name=Felidae" ],
			method => 'list_associated',
			arg => 'taxa',
			output => '1.2:taxa:reftaxa',
			optional_output => '1.2:taxa:mult_output_map' },
	"This operation returns information about taxonomic names, grouped according to the bibliographic",
	"reference in which they are mentioned.  This is a companion operation to L<taxa/refs|node:taxa/refs>,",
	"and you can use the two together to retrieve a list of references and a list of taxa grouped by",
	"reference. You can then match the two lists using the L<reference_no/rid|#reference_no> field.",
	"For this reason, this operation takes all of the parameters that L<taxa/refs|node:taxa/refs> does.", 
	">>You can also use this operation simply to list the taxa mentioned in a given reference or a",
	"set of references selected by reference identifier, author, year of publication, etc.");
    
    $ds2->extended_doc({ path => 'taxa/byref' },
	"In database terminology, this operation is essentially a B<join> between the references table",
	"and the taxonomic name table. It basically selects a set of (taxonomic name, reference) tuples,",
	"orders them by reference identifier and secondarily according to the taxonomic hierarchy, and then",
	"returns a set of annotated",
	"taxon records. A given taxon may appear more than once, if it is mentioned in more than one",
	"reference. Each record contains the field L<reference_no|#reference_no> to indicate the relevant reference,",
	"and the field L<ref_type|#ref_type> to indicate the relationship(s) between the taxon and this",
	"particular reference.");
    
    $ds2->define_node({ path => 'taxa/opinions',
			place => 3,
			title => 'Opinions about taxa',
			usage => [ "taxa/opinions.json?base_name=Felidae" ],
			method => 'list_associated',
			default_limit => $ref_limit,
			arg => 'opinions',
			output => '1.2:opinions:basic',
			optional_output => '1.2:opinions:output_map' },
	"This operation returns information about the taxonomic opinions used to build the taxonomic",
	"hierarchy.  From all of the opinions entered into the database about a particular",
	"taxon, the most recent opinion that is stated with the most evidence is used to classify",
	"that taxon.  The others are considered to be superseded and are ignored.");
    
    $ds2->list_node({ list => 'taxa',
		      path => 'occs/taxa',
		      place => 4 });
    
    $ds2->define_node({ path => 'taxa/auto',
			place => 10,
			method => 'auto',
			title => 'Auto-completion for taxonomic names',
			usage => [ "taxa/auto.json?name=h. sap&limit=10",
				   "taxa/auto.json?name=cani&limit=10" ],
			allow_format => 'json',
			default_limit => 10,
			output => '1.2:taxa:auto' },
	"This operation returns a list of names matching the given prefix or partial name.",
	"You can use it for auto-completion of taxonomic names in a client application.");
    
    $ds2->define_node({ path => 'taxa/thumb',
			place => 11,
			title => 'Thumbnail images of lifeforms',
			usage => [ 'taxa/thumb.png?id=910',
				   'html:<img src="/data1.2/taxa/thumb.png?id=910">',
				   'taxa/thumb.json?id=910' ],
			method => 'get_image',
			arg => 'thumb',
			allow_format => '+png',
			output => '1.2:taxa:imagedata' },
	"This operation returns an image to represent the specified taxon, or else",
	"information about the image.  If the suffix is C<.png>, then the image content",
	"data is returned.  Otherwise, a descriptive record is returned in the specified format.",
	">These 64x64 thumbnail images are sourced from L<http://phylopic.org/>.",
	"If multiple images are available for a particular taxon, one has been arbitrarily selected.",
	"You can obtain image identifiers by including C<$show=img> with any taxonomic",
	"name query.");
    
    $ds2->define_node({ path => 'taxa/icon',
			place => 11,
			title => 'Icon images of lifeforms',
			usage => [ 'taxa/icon.png?id=910', 
				   'html:<img src="/data1.2/taxa/icon.png?id=910">',
				   'taxa/icon.json?id=910' ],
			method => 'get_image',
			arg => 'icon',
			allow_format => '+png',
			output => '1.2:taxa:imagedata' },
	"This operation returns an image to represent the specified taxon, or else",
	"information about the image.  If the suffix is C<.png>, then the image content",
	"data is returned.  Otherwise, a descriptive record is returned in the specified format.",
	">These 32x32 icon (blue silhouette) images are sourced from L<http://phylopic.org/>.",
	"If multiple images are available for a particular taxon, one has been arbitrarily selected.",
	"You can obtain image identifiers by including C<$show=img> with any taxonomic",
	"name query.");
    
    $ds2->define_node({ path => 'taxa/list_images',
			title => 'List the available images of lifeforms',
			output => '1.2:taxa:imagedata',
			method => 'list_images' });
    
    # Opinions
    
    $ds2->define_node({ path => 'opinions',
			place => 2,
			title => 'Taxonomic opinions',
			role => 'PB2::TaxonData',
			output => '1.2:opinions:basic' },
	"The taxonomic hierarchy in our database is computed algorithmically based on a",
	"constantly growing set of taxonomic opinions.  These opinions are ranked",
	"by publication year and basis, yielding a 'consensus taxonomy' based on",
	"the latest research.");
    
    $ds2->define_node({ path => 'opinions/single',
			place => 1,
			title => 'Single opinion',
			usage => [ "opinions/single.json?id=1000&show=entname" ],
			method => 'get_opinion',
			optional_output => '1.2:opinions:output_map' },
	"This operation returns information about a single taxonomic opinion selected",
	"by identifier.");
    
    $ds2->define_node({ path => 'opinions/list',
			place => 2,
			title =>'Lists of opinions',
			usage => [ "opinions/list.json?created_since=7d",
				   "opinions/list.json?author=Osborn" ],
			method => 'list_opinions',
			default_limit => $ref_limit,
			optional_output => '1.2:opinions:output_map' },
	"This operation returns information about multiple taxonomic opinions, selected according to",
	"criteria other than taxon name.  This operation could be used to query for all of the opinions",
	"attributed to a particular author, or to show all of the recently entered opinions.");
    
    $ds2->list_node({ path => 'taxa/opinions',
		      place => 3,
		      list => 'other',
		      title => 'Opinions about taxa',
		      usage => [ "taxa/opinions.json?base_name=Felidae" ] },
	"This operation returns information about taxonomic opinions, selected by taxon name.");
    
    # Time scales and intervals.  These paths are used to fetch information about
    # geological time scales and time intervals known to the database.

    $ds2->define_node({ path => 'intervals',
			place => 3,
			role => 'PB2::IntervalData',
			output => '1.2:intervals:basic',
			default_limit => undef,
			title => 'Geological time intervals and time scales' },
	"The database lists almost every geologic time interval in current use, including the",
	"standard set established by the L<International Commission on Stratigraphy|http://www.stratigraphy.org/>",
	"(L<2013-01|http://www.stratigraphy.org/ICSchart/ChronostratChart2013-01.jpg>).");
    
    $ds2->define_node({ path => 'intervals/single',
			place => 1,
			title => 'Single geological time interval',
			usage => "intervals/single.json?id=16",
			method => 'get' },
	"This operation returns information about a single interval, selected by identifier.");
    
    $ds2->define_node({ path => 'intervals/list',
			place => 2,
			title => 'Lists of geological time intervals',
			usage => "intervals/list.txt?scale=1",
			method => 'list' },
	"This operation returns information about multiple intervals, selected according to",
	"the parameters you provide.");
    
    $ds2->list_node({ path => 'scales/single',
		      place => 3,
		      list => 'intervals' });
    
    $ds2->list_node({ path => 'scales/list',
		      place => 4,
		      list => 'intervals' });
    
    $ds2->define_node({ path => 'scales',
			role => 'PB2::IntervalData',
			output => '1.2:scales:basic',
			default_limit => undef,
			title => 'Geological time scales' });
    
    $ds2->define_node({ path => 'scales/single',
			place => 1,
			title => 'Single geological time scale',
			usage => "scales/single.json?id=1",
			method => 'list_scales' },
	"This operation returns information about a single time scale, selected by identifier.");
    
    $ds2->define_node({ path => 'scales/list',
			place => 2,
			title => 'Lists of geological time scales',
			usage => "scales/list.json",
			method => 'list_scales' },
	"This operation returns information about multiple time scales.  To get a list of all of the available",
	"scales, use this path with no parameters.");


    # People.  These paths are used to fetch the names of database contributors.

    $ds2->define_node({ path => 'people',
			place => 0,
			title => 'Database contributors',
			role => 'PB2::PersonData',
			default_limit => undef,
			output => '1.2:people:basic' });
    
    $ds2->define_node({ path => 'people/single', 
			method => 'get' });
    
    $ds2->define_node({ path => 'people/list',
			method => 'list' });
    
    $ds2->define_node({ path => 'people/auto',
			method => 'people_auto',
			default_limit => 10,
			usage => "people/auto?name=smi" },
	"This operation is used for auto-completion of database contributor names.",
	"It returns a list of people whose last name begins with the specified string.",
	"The default limit is 10, unless overridden.");

    
    # Bibliographic References

    $ds2->define_node({ path => 'refs',
			place => 5,
			title => 'Bibliographic references',
			role => 'PB2::ReferenceData',
			allow_format => '+ris',
			default_limit => $ref_limit,
			output => '1.2:refs:basic',
		        optional_output => '1.2:refs:output_map' },
	"Each fossil occurrence, collection, specimen, taxonomic name, and opinion in the database is",
	"associated with one or more bibliographic references, identifying the source from",
	"which this information was entered.");
    
    $ds2->define_node({ path => 'refs/single',
			place => 1,
			title => 'Single bibliographic reference',
			usage => "refs/single.json?id=6930&show=both",
			method => 'get' },
	"This operation returns information about a single bibliographic reference,",
	"selected by its identifier");
    
    $ds2->define_node({ path => 'refs/list',
			place => 2,
			title => 'Lists of bibliographic references',
			usage => "refs/list.txt?ref_author=Sepkoski",
			method => 'list' },
	"This operation returns information about lists of bibliographic references,",
	"selected according to the parameters you provide");
    
    $ds2->list_node({ path => 'occs/refs',
		      list => 'refs',
		      place => 3,
		      title => 'References for fossil occurrences',
		      usage => "occs/refs.ris?base_name=Cetacea&interval=Eocene&textresult" },
	"This operation returns information about the references from which the",
	"selected occurrence data were entered.");
    
    $ds2->list_node({ path => 'specs/refs',
		      list => 'refs',
		      place => 3,
		      title => 'References for fossil specimens',
		      usage => "specs/refs.ris?base_name=Cetacea&interval=Eocene&textresult" },
	"This operation returns information about the references from which the",
	"selected occurrence data were entered.");
    
    $ds2->list_node({ path => 'colls/refs',
		      list => 'refs',
		      place => 4,
		      title => 'References for fossil collections' },
	"This operation returns information about the references from which",
	"the selected collections were entered.");
    
    $ds2->list_node({ path => 'taxa/refs',
		      list => 'refs',
		      place => 5,
		      title => 'References for taxonomic names' },
	"This operation returns information about the references from which",
	"the selected taxonomic names were entered.");
    
    # The following paths are used for miscellaneous documentation
    
    $ds2->define_node({ path => 'general',
			title => 'General documentation',
			doc_default_template => 'default.tt' },
	"This page lists general documentation about how to use the data service.");
    
    $ds2->define_node({ path => 'general/identifiers',
			title => 'Record identifiers and record numbers',
			place => 1 },
	"Records retrieved from the data service can be identified either by using the",
	"numeric identifiers from the underlying database records (i.e. 'occurrence_no'),",
	"or using an extended identifier syntax.");
    
    $ds2->define_node({ path => 'general/taxon_names',
			title => 'Specifying taxonomic names',
		        place => 1 },
	"The data service accepts taxonomic names using several different parameters,",
	"and there are modifiers that you can add in order to precisely specify",
	"which taxa you are interested in.");
    
    $ds2->define_node({ path => 'general/ecotaph',
			title => 'Ecological and taphonomic vocabulary',
			place => 1 },
	"The ecology of organisms and the taphonomy of their fossil remains are described",
	"by several different data fields with an associated vocabulary.");
    
    $ds2->define_node({ path => 'general/datetime',
		      title => 'Specifying dates and times',
		      place => 2 },
	"You can retrieve records based on when they were modified and/or created.");
    
    $ds2->define_node({ path => 'general/references',
			title => 'Bibliographic references',
			place => 3 },
	"Each piece of data entered into the database is linked to the bibliographic reference from which it was entered.");
    
    $ds2->define_node({ path => 'general/basis_precision',
			title => 'Basis and precision of coordinates',
			place => 4 },
	"The basis and precision of geographic locations is specified by a set of code values.");
    
    $ds2->define_node({ path => 'special',
			ruleset => '1.2:special_params',
			title => 'Special parameters' },
	"There are a number of special parameters which you can use with almost any data service",
	"operation. These constrain or alter the response in various ways.");
    
    $ds2->define_node({ path => 'formats',
			title => 'Output formats and Vocabularies' },
	"You can get the results of query operations in a variety of formats, and with the",
	"field names expressed in any of the available vocabularies.");
    
    $ds2->list_node({ path => 'formats',
		      list => 'general',
		      place => 5,
		      title => 'Formats and Vocabularies' });
    
    $ds2->list_node({ path => 'special',
		      list => 'general',
		      place => 6,
		      title => 'Special Parameters' });
    
    $ds2->define_node({ path => 'formats/json',
			title => 'JSON format' });
    
    $ds2->define_node({ path => 'formats/xml',
			title => 'XML format' });
    
    $ds2->define_node({ path => 'formats/text',
			title => 'Text formats' });
    
    $ds2->define_node({ path => 'formats/ris',
			title => "RIS format" });
    
    $ds2->define_node({ path => 'formats/png',
			title => 'PNG format' });
    
    
    # And finally, stylesheets and such
    
    $ds2->define_node({ path => 'css',
			file_dir => 'css' });
    
    $ds2->define_node({ path => 'images',
			file_dir => 'images' });
};

1;
