# 
# Paleobiology Data Service version 1.1
# 
# This file defines version 1.1 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


package PBData;

use PB1::CommonData;
use PB1::ConfigData;
use PB1::IntervalData;
use PB1::TaxonData;
use PB1::CollectionData;
use PB1::OccurrenceData;
use PB1::ReferenceData;
use PB1::PersonData;

{
    # We start by defining a data service instance for version 1.1
    
    our ($ds1) = Web::DataService->new(
	{ name => '1.1',
	  title => 'PBDB Data Service',
	  version => '6',
	  features => 'standard',
	  special_params => 'standard, linebreak=linebreak, datainfo=showsource, count=count/rowcount',
	  path_prefix => 'data1.1/',
	  ruleset_prefix => '1.1:',
	  doc_template_dir => 'doc/1.1' });
    
    
    # We then define the vocabularies that will be used to label the data
    # fields returned by this service.
    
    $ds1->define_vocab(
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
    
    $ds1->define_format(
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
    
    $ds1->define_node({ path => '/', 
			public_access => 1,
			doc_default_op_template => 'operation.tt',
			allow_format => 'json,csv,tsv,txt',
			allow_vocab => 'pbdb,com',
			default_save_filename => 'pbdb_data',
			default_limit => 500,
			title => 'Documentation' });
    
    # Configuration. This path is used by clients who need to configure themselves
    # based on parameters supplied by the data service.
    
    $ds1->define_node({ path => 'config',
			place => 10,
			title => 'Client configuration',
			usage => [ "config.json?show=all",
				   "config.txt?show=clusters" ],
			role => 'PB1::ConfigData',
			method => 'get',
			optional_output => '1.1:config:get_map' },
	"This class provides information about the structure, encoding and organization",
	"of the information in the database. It is designed to enable the easy",
	"configuration of client applications.");

    # Occurrences.  These paths are used to fetch information about fossil
    # occurrences known to the database.
    
    $ds1->define_node({ path => 'occs',
			place => 1,
			title => 'Fossil occurrences',
			role => 'PB1::OccurrenceData',
			allow_format => '+xml',
			default_save_filename => 'pbdb_occs' },
	"A fossil occurence represents the occurrence of a particular organism at a particular",
	"location in time and space. Each occurrence is a member of a single fossil collection,",
	"and has a taxonomic identification which may be more or less specific.");
    
    $ds1->define_node({ path => 'occs/single',
			place => 1,
			usage => [ "/occs/single.json?id=1001&show=loc", 
				   "/occs/single.txt?id=1001&show=loc,crmod" ],
			method => 'get',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			optional_output => '1.1:occs:basic_map',
			title => 'Single fossil occurrence'},
	"This path returns information about a single occurrence, selected by its identifier.",
	"Depending upon which output blocks you select, the response will contain some",
	"fields describing the occurrence and some describing the collection to which it belongs.");
    
    $ds1->define_node({ path => 'occs/list',
			place => 2,
			usage => [ "/occs/list.txt?base_name=Cetacea&interval=Miocene&show=loc,time" ],
			method => 'list',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			optional_output => '1.1:occs:basic_map',
			title => 'Lists of fossil occurrences' },
	"This path returns information about multiple occurrences, selected according to the parameters you provide.",
	"Depending upon which output blocks you select, the response will contain some",
	"fields describing the occurrences and some describing the collections to which they belong.");
    
    $ds1->define_node({ path => 'occs/refs',
			place => 3,
			usage => [ "/occs/refs.ris?base_name=Cetacea&interval=Miocene&textresult" ],
			method => 'refs',
			allow_format => '+ris,-xml',
			output => '1.1:refs:basic',
			optional_output => '1.1:refs:output_map',
			default_save_filename => 'pbdb_refs',
			title => 'Bibliographic references for fossil occurrences' },
	"This path returns information about the bibliographic references associated with fossil occurrences.",
	"You can pass identical filtering parameters to L<occs/list|node:occs/list> and to L<occs/refs|node:occs/refs>,",
	"which will give you both a list of occurrences and a list of the associated references.");
    
    $ds1->define_node({ path => 'occs/taxa',
			place => 4,
			usage => [ "/occs/taxa.txt?base_name=Cetacea&interval=Miocene" ],
			method => 'taxa',
			output => '1.1:taxa:basic',
			optional_output => '1.1:taxa:output_map',
			default_save_filename => 'pbdb_taxa',
			title => 'Taxa from fossil occurrences' },
	"This path returns information about the taxonomic names associated with fossil occurrences.",
	"You can pass identical filtering parameters to L<occs/list|node:occs/list> and to L<occs/taxa|node:occs/taxa>,",
	"which will give you both a list of occurrences and a list of the associated taxa.");
    
    # Collections.  These paths are used to fetch information about fossil
    # collections known to the database.
    
    $ds1->define_node({ path => 'colls',
			place => 1,
			title => 'Fossil collections',
			role => 'PB1::CollectionData',
			default_save_filename => 'pbdb_colls' },
	"A fossil collection is somewhat loosely defined as a set of fossil occurrences that are",
	"co-located geographically and temporally. Each collection has a geographic location,",
	"stratigraphic context, and age estimate.");
    
    $ds1->define_node({ path => 'colls/single',
			place => 1,
			title => 'Single fossil collection',
			usage => [ "colls/single.json?id=50068&show=loc,time" ],
			post_configure_hook => 'prune_field_list',
			method => 'get',
			output => '1.1:colls:basic',
			optional_output => '1.1:colls:basic_map' },
	"This path returns information about a single collection, selected by its identifier.");
    
    $ds1->define_node({ path => 'colls/list',
			place => 2,
			title => 'Lists of fossil collections',
			usage => [ "colls/list.txt?base_name=Cetacea&interval=Miocene&show=ref,loc,time" ],
			post_configure_hook => 'prune_field_list',
			method => 'list',
			output => '1.1:colls:basic',
			optional_output => '1.1:colls:basic_map' },
	"This path returns information about multiple collections, selected according to the",
	"parameters you provide.");
    
    $ds1->define_node({ path => 'colls/summary',
			place => 3,
			title => 'Geographic clusters of fossil collections',
			usage => [ "colls/summary.json?lngmin=0.0&lngmax=15.0&latmin=0.0&latmax=15.0&level=2",
				   "config.json?show=clusters" ],
			method => 'summary',
			output => '1.1:colls:summary',
			optional_output => '1.1:colls:summary_map' },
	"This path returns information about geographic clusters of collections.  These clusters",
	"have been computed in order to facilitate the generation of maps at low resolutions.",
	"You can make a L<config|node:config> request in order to get a list of the available summary levels.");
    
    $ds1->define_node({ path => 'colls/refs',
			place => 4,
			title => 'Bibliographic references for fossil collections',
			usage => [ "colls/refs.ris?base_name=Cetacea&interval=Miocene&show=comments&textresult" ],
			method => 'refs',
			allow_format => '+ris',
			output => '1.1:refs:basic',
			default_save_filename => 'pbdb_refs',
			optional_output => '1.1:refs:output_map' },
	"This path returns information about the bibliographic references associated with fossil",
	"collections.  You can pass identical filtering parameters to L<colls/list|node:colls/list>",
	"and L<colls/refs|node:colls/refs>, which will give you both a list of occurrences and a list",
	"of the associated references.");
    
    # Strata.  These paths are used to fetch information about geological strata
    # known to the database.
    
    $ds1->define_node({ path => 'strata',
			place => 4,
			title => 'Geological strata',
			role => 'PB1::CollectionData',
		        default_save_filename => 'pbdb_strata' },
	"Every fossil collection in the database is categorized by the formation",
	"from which it was collected, and many by group and member.");
    
    $ds1->define_node({ path => 'strata/list',
			place => 1,
			title => 'Lists of geological strata',
			usage => [ "strata/list.txt?lngmin=0&lngmax=15&latmin=0&latmax=15&rank=formation" ],
			method => 'strata',
			output => '1.1:colls:strata' },
	"This path returns information about geological strata selected by name, rank,",
	"and/or geographic location.");
    
    $ds1->define_node({ path => 'strata/auto',
			place => 2,
			title => 'Auto-completion for geological strata',
			usage => [ "strata/auto.json?name=aba&limit=10" ],
			method => 'strata',
			arg => 'auto',
			output => '1.1:colls:strata' },
	"This path returns a list of geological strata from the database that match the given",
	"prefix or partial name.  This can be used to implement auto-completion for strata names,",
	"and can be limited by geographic location if desired.");
    
    # Taxa.  These paths are used to fetch information about biological taxa known
    # to the database.
    
    my $show = $ds1->special_param('show');
    
    $ds1->define_node({ path => 'taxa',
			place => 2,
			title => 'Taxonomic names',
			role => 'PB1::TaxonData',
			output => '1.1:taxa:basic',
		        default_save_filename => 'pbdb_taxa' },
	"The taxonomic names stored in the database are arranged hierarchically.",
	"Our tree of life is quite complete down to the class level, and reasonably complete",
	"down to the suborder level. Below that, coverage varies. Many parts of the tree have",
	"been completely entered, while others are sparser.");
    
    $ds1->define_node({ path => 'taxa/single',
			place => 1,
			title => 'Single taxon',
			usage => [ "taxa/single.json?id=69296&show=attr",
				   "taxa/single.txt?name=Dascillidae" ],
			method => 'get',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			optional_output => '1.1:taxa:output_map' },
	"This path returns information about a single taxonomic name, identified either",
	"by name or by identifier.");
    
    $ds1->define_node({ path => 'taxa/list',
			place => 2,
			title => 'Lists of taxa',
			usage => [ "taxa/list.txt?id=69296&rel=all_children&show=ref",
				   "taxa/list.json?name=Dascillidae&rel=all_parents" ],
			method => 'list',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			optional_output => '1.1:taxa:output_map' },
	"This path returns information about multiple taxonomic names, selected according to",
	"the criteria you specify.  This path could be used to query for all of the children",
	"or parents of a given taxon, among other operations.");
    
    $ds1->define_node({ path => 'taxa/refs',
			place => 3,
			title => 'Bibliographic references for taxa',
			usage => [ "taxa/refs.ris?base_name=Felidae&textresult" ],
			method => 'list',
			arg => 'refs',
			allow_format => '+ris',
			default_save_filename => 'pbdb_refs',
			output => '1.1:refs:basic',
			optional_output => '1.1:refs:output_map' },
	"This path returns information about the bibliographic references associated with taxonomic names.",
	"You can pass identical filtering parameters to L<node:taxa/list> and to L<node:taxa/refs>,",
	"which will give you both a list of taxonomic names and a list of the associated references.");
    
    $ds1->list_node({ list => 'taxa',
		      path => 'occs/taxa',
		      place => 4 });
    
    $ds1->define_node({ path => 'taxa/match',
			method => 'match' });
    
    $ds1->define_node({ path => 'taxa/auto',
			place => 5,
			method => 'auto',
			title => 'Auto-completion for taxonomic names',
			usage => [ "taxa/auto.json?name=h. sap&limit=10",
				   "taxa/auto.json?name=cani&limit=10" ],
			allow_format => 'json',
			output => '1.1:taxa:auto' },
	"This path returns a list of names matching the given prefix or partial name.",
	"You can use it for auto-completion of taxonomic names in a client application.");
    
    $ds1->define_node({ path => 'taxa/thumb',
			place => 7,
			title => 'Thumbnail images of lifeforms',
			usage => [ 'taxa/thumb.png?id=910',
				   'html:<img src="/data1.1/taxa/thumb.png?id=910">' ],
			method => 'get_image',
			arg => 'thumb',
			allow_format => '+png',
			default_save_filename => 'pbdb_image',
			output => '1.1:taxa:imagedata' },
	"This path returns an image to represent the specified taxon.",
	"These 64x64 thumbnail images are sourced from L<http://phylopic.org/>.",
	"You can get the image identifiers by including C<$show=img> with any taxonomic",
	"name query.");
    
    $ds1->define_node({ path => 'taxa/icon',
			place => 7,
			title => 'Icon images of lifeforms',
			usage => [ 'taxa/icon.png?id=910', 
				   'html:<img src="/data1.1/taxa/icon.png?id=910">' ],
			method => 'get_image',
			arg => 'icon',
			allow_format => '+png',
			default_save_filename => 'pbdb_image',
			output => '1.1:taxa:imagedata' },
	"This path returns an image to represent the specified taxon.",
	"These 32x32 icon (blue silhouette) images are sourced from L<http://phylopic.org/>.",
	"You can get the image identifiers by including C<$show=img> with any taxonomic",
	"name query.");
    
    $ds1->define_node({ path => 'taxa/list_images',
			title => 'List the available images of lifeforms',
			output => '1.1:taxa:imagedata',
			method => 'list_images' });
    
    
# Time scales and intervals.  These paths are used to fetch information about
# geological time scales and time intervals known to the database.

    $ds1->define_node({ path => 'intervals',
			place => 3,
			role => 'PB1::IntervalData',
			output => '1.1:intervals:basic',
			default_save_filename => 'pbdb_intervals',
			title => 'Geological time intervals and time scales' },
	"The database lists almost every geologic time interval in current use, including the",
	"standard set established by the L<International Commission on Stratigraphy|http://www.stratigraphy.org/>",
	"(L<v2013-1|http://www.stratigraphy.org/ICSchart/ChronostratChart2013-01.jpg>).");
    
    $ds1->define_node({ path => 'intervals/single',
			place => 1,
			title => 'Single geological time interval',
			usage => "intervals/single.json?id=16",
			method => 'get' },
	"This path returns information about a single interval, selected by identifier.");
    
    $ds1->define_node({ path => 'intervals/list',
			place => 2,
			title => 'Lists of geological time intervals',
			usage => "intervals/list.txt?scale=1",
			method => 'list' },
	"This path returns information about multiple intervals, selected according to",
	"the parameters you provide.");
    
    $ds1->list_node({ path => 'scales/single',
		      place => 3,
		      list => 'intervals' });
    
    $ds1->list_node({ path => 'scales/list',
		      place => 4,
		      list => 'intervals' });
    
    $ds1->define_node({ path => 'scales',
			role => 'PB1::IntervalData',
			output => '1.1:scales:basic',
			default_save_filename => 'pbdb_scales',
			title => 'Geological time scales' });
    
    $ds1->define_node({ path => 'scales/single',
			place => 1,
			title => 'Single geological time scale',
			usage => "scales/single.json?id=1",
			method => 'list_scales' },
	"This path information about a single time scale, selected by identifier.");
    
    $ds1->define_node({ path => 'scales/list',
			place => 2,
			title => 'Lists of geological time scales',
			usage => "scales/list.json",
			method => 'list_scales' },
	"This path returns information about multiple time scales.  To get a list of all of the available",
	"scales, use this path with no parameters.");


    # People.  These paths are used to fetch the names of database contributors.

    $ds1->define_node({ path => 'people',
			place => 0,
			title => 'Database contributors',
			role => 'PB1::PersonData',
			output => '1.1:people:basic' });
    
    $ds1->define_node({ path => 'people/single', 
			method => 'get' });
    
    $ds1->define_node({ path => 'people/list',
			method => 'list' });

    
    # Bibliographic References

    $ds1->define_node({ path => 'refs',
			place => 5,
			title => 'Bibliographic references',
			role => 'PB1::ReferenceData',
			allow_format => '+ris',
			default_save_filename => 'pbdb_refs',
			output => '1.1:refs:basic',
		        optional_output => '1.1:refs:output_map' },
	"Each fossil occurrence, fossil collection and taxonomic name in the database is",
	"associated with one or more bibliographic references, identifying the source from",
	"which this information was entered.");
    
    $ds1->define_node({ path => 'refs/single',
			place => 1,
			title => 'Single bibliographic reference',
			usage => "refs/single.json?id=6930&show=both",
			method => 'get' },
	"This path returns information about a single bibliographic reference,",
	"selected by its identifier");
    
    $ds1->define_node({ path => 'refs/list',
			place => 2,
			title => 'Lists of bibliographic references',
			usage => "refs/list.txt?author=Sepkoski",
			method => 'list' },
	"This path returns information about lists of bibliographic references,",
	"selected according to the parameters you provide");
    
    $ds1->list_node({ path => 'occs/refs',
		      list => 'refs',
		      place => 3,
		      title => 'References for fossil occurrences',
		      usage => "occs/refs.ris?base_name=Cetacea&interval=Eocene&textresult" },
	"This path returns information about the references from which the",
	"selected occurrence data were entered.");
    
    $ds1->list_node({ path => 'colls/refs',
		      list => 'refs',
		      place => 4,
		      title => 'References for fossil collections' },
	"This path returns information about the references from which",
	"the selected collections were entered.");
    
    $ds1->list_node({ path => 'taxa/refs',
		      list => 'refs',
		      place => 5,
		      title => 'References for taxonomic names' },
	"This path returns information about the references from which",
	"the selected taxonomic names were entered.");
    
    # The following paths are used for miscellaneous documentation
    
    $ds1->define_node({ path => 'special',
			ruleset => '1.1:special_params',
			title => 'Special parameters' });
    
    $ds1->define_node({ path => 'datetime',
			title => 'Selecting records by date and time' });
    
    $ds1->define_node({ path => 'formats',
			title => 'Formats and Vocabularies' });
    
    $ds1->define_node({ path => 'formats/json',
			title => 'JSON format' });
    
    $ds1->define_node({ path => 'formats/xml',
			title => 'XML format' });
    
    $ds1->define_node({ path => 'formats/text',
			title => 'Text formats' });
    
    $ds1->define_node({ path => 'formats/ris',
			title => "RIS format" });
    
    $ds1->define_node({ path => 'formats/png',
			title => 'PNG format' });
    
    
    # And finally, stylesheets and such
    
    $ds1->define_node({ path => 'css',
			file_dir => 'css' });
    
    $ds1->define_node({ path => 'images',
			file_dir => 'images' });
};

1;
