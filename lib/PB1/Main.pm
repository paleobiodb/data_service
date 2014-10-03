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
	  version => '5',
	  features => 'standard',
	  special_params => 'standard, linebreak=linebreak, datainfo=showsource',
	  path_prefix => 'data1.1/',
	  ruleset_prefix => '1.1:',
	  doc_template_dir => 'doc1' },
	    "This is the current stable version of the data service.  The interface is guaranteed",
	    "not to change, except possibly for extremely important bug fixes.  In such a case,",
	    "every effort would be made not to change anything that would break any existing applications.");
    
    
    # We then define the vocabularies that will be used to label the data
    # fields returned by this service.
    
    $ds1->define_vocab(
	{ name => 'pbdb', title => 'PaleobioDB field names',
	  use_field_names => 1 },
	    "The PBDB vocabulary is derived from the underlying field names and values in the database,",
	    "augmented by a few new fields. For the most part any response that uses this vocabulary",
	    "will be directly comparable to downloads from the PBDB Classic interface.",
	    "This vocabulary is the default for L<text format|/data1.1/formats/text> responses.",
	{ name => 'com', title => 'Compact field names' },
	    "The Compact vocabulary is a set of 3-character field names designed to minimize",
	    "the size of the response message.  This is the default for L<JSON format|/data1.1/formats/json>",
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
	  doc_node => 'formats/text', title => 'comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'csv', content_type => 'text/csv',
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'tsv', content_type => 'text/tab-separated-values', 
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'tab-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'ris', content_type => 'application/x-research-info-systems',
	  doc_path => 'formats/ris', title => 'RIS', disposition => 'attachment',
	  module => 'RISFormat'},
	    "The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	    "common format for bibliographic references.",
	{ name => 'png', content_type => 'image/png', class => '',
	  doc_path => 'formats/png', title => 'PNG' },
	    "The PNG suffix is used with a few URL paths to fetch images stored",
	    "in the database.");
    
    
    # Then define the URL paths that this subservice will accept.  We start with
    # the root of the hierarchy, which sets defaults for all the rest of the nodes.
    
    $ds1->define_node({ path => '/', 
			public_access => 1,
			allow_format => 'json,csv,tsv,txt',
			allow_vocab => 'pbdb,com',
			title => 'Documentation' });
    
    # Configuration. This path is used by clients who need to configure themselves
    # based on parameters supplied by the data service.
    
    $ds1->define_node({ path => 'config',
			role => 'PB1::ConfigData',
			method => 'get',
			optional_output => '1.1:config:get_map',
			title => 'Client configuration' });

    # Occurrences.  These paths are used to fetch information about fossil
    # occurrences known to the database.
    
    $ds1->define_node({ path => 'occs',
			role => 'PB1::OccurrenceData',
			allow_format => '+xml',
			title => 'Fossil occurrences' });
    
    $ds1->define_node({ path => 'occs/single',
			method => 'get',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			optional_output => '1.1:occs:basic_map',
			title => 'Single fossil occurrence'});
    
    $ds1->define_node({ path => 'occs/list',
			method => 'list',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			optional_output => '1.1:occs:basic_map',
			title => 'Lists of fossil occurrences' });
    
    $ds1->define_node({ path => 'occs/refs',
			method => 'refs',
			allow_format => '+ris,-xml',
			output => '1.1:refs:basic',
			optional_output => '1.1:refs:output_map',
			title => 'Bibliographic references for fossil occurrences' });
    
    $ds1->define_node({ path => 'occs/taxa',
			method => 'taxa',
			output => '1.1:taxa:basic',
			optional_output => '1.1:taxa:output_map',
			title => 'Taxa from fossil occurrences' });
    
    # Collections.  These paths are used to fetch information about fossil
    # collections known to the database.
    
    $ds1->define_node({ path => 'colls',
			role => 'PB1::CollectionData',
			use_cache => '1.1:colls',
			title => 'Fossil collections' });
    
    $ds1->define_node({ path => 'colls/single',
			method => 'get',
			post_configure_hook => 'prune_field_list',
			output => '1.1:colls:basic',
			optional_output => '1.1:colls:basic_map',
			title => 'Single fossil collection' });
    
    $ds1->define_node({ path => 'colls/list',
			method => 'list',
			post_configure_hook => 'prune_field_list',
			output => '1.1:colls:basic',
			optional_output => '1.1:colls:basic_map',
			title => 'Lists of fossil collections' });
    
    $ds1->define_node({ path => 'colls/summary',
			method => 'summary',
			output => '1.1:colls:summary',
			optional_output => '1.1:colls:summary_map',
			title => 'Geographic clusters of fossil collections' });
    
    $ds1->define_node({ path => 'colls/refs',
			method => 'refs',
			allow_format => '+ris',
			output => '1.1:refs:basic',
			optional_output => '1.1:refs:output_map',
			title => 'Bibliographic references for fossil collections' });
    
    # Strata.  These paths are used to fetch information abot geological strata
    # known to the database.
    
    $ds1->define_node({ path => 'strata',
			role => 'PB1::CollectionData',
			title => 'Geological strata' });
    
    $ds1->define_node({ path => 'strata/list',
			method => 'strata',
			output => '1.1:colls:strata' });
    
    $ds1->define_node({ path => 'strata/auto',
			method => 'strata',
			arg => 'auto',
			output => '1.1:colls:strata',
			title => 'Auto-completion for geological strata' });
    
    # Taxa.  These paths are used to fetch information about biological taxa known
    # to the database.
    
    $ds1->define_node({ path => 'taxa',
			role => 'PB1::TaxonData',
			output => '1.1:taxa:basic',
			title => 'Taxonomic names' });
    
    $ds1->define_node({ path => 'taxa/single',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			method => 'get',
			optional_output => '1.1:taxa:output_map',
			title => 'Single taxon' });
    
    $ds1->define_node({ path => 'taxa/list',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			method => 'list',
			optional_output => '1.1:taxa:output_map',
			title => 'Lists of taxa' });
    
    $ds1->define_node({ path => 'taxa/refs',
			output => '1.1:refs:basic',
			optional_output => '1.1:refs:output_map',
			method => 'list',
			arg => 'refs',
			title => 'Bibliographic references for taxa' });
    
    $ds1->define_node({ path => 'taxa/match',
			method => 'match' });
    
    $ds1->define_node({ path => 'taxa/auto',
			method => 'auto', 
			allow_format => 'json',
			output => '1.1:taxa:auto',
			title => 'Auto-completion for taxonomic names' });
    
    $ds1->define_node({ path => 'taxa/thumb',
			allow_format => '+png',
			output => '1.1:taxa:imagedata',
			method => 'get_image',
			arg => 'thumb',
			title => 'Thumbnail images of lifeforms' });
    
    $ds1->define_node({ path => 'taxa/icon',
			allow_format => '+png',
			output => '1.1:taxa:imagedata',
			method => 'get_image',
			arg => 'icon',
			title => 'Icon images of lifeforms' });
    
    $ds1->define_node({ path => 'taxa/list_images',
			output => '1.1:taxa:imagedata',
			method => 'list_images',
			title => 'List the available images of lifeforms' });
    
    
# Time scales and intervals.  These paths are used to fetch information about
# geological time scales and time intervals known to the database.

    $ds1->define_node({ path => 'intervals',
			role => 'PB1::IntervalData',
			output => '1.1:intervals:basic',
			title => 'Geological Time scales and time intervals' });
    
    $ds1->define_node({ path => 'intervals/single',
			method => 'get',
			title => 'Single geological time interval' });
    
    $ds1->define_node({ path => 'intervals/list',
			method => 'list',
			title => 'Lists of geological time intervals' });
    
    $ds1->define_node({ path => 'scales',
			role => 'PB1::IntervalData',
			output => '1.1:scales:basic',
			title => 'Geological time scales' });
    
    $ds1->define_node({ path => 'scales/single',
			method => 'list_scales',
			title => 'Single geological time scale' });
    
    $ds1->define_node({ path => 'scales/list',
			method => 'list_scales',
			title => 'List of available geological time scales' });


    # People.  These paths are used to fetch the names of database contributors.

    $ds1->define_node({ path => 'people',
			role => 'PB1::PersonData',
			output => '1.1:people:basic',
			title => 'Database contributors' });
    
    $ds1->define_node({ path => 'people/single', 
			method => 'get' });
    
    $ds1->define_node({ path => 'people/list',
			method => 'list' });

    
    # Bibliographic References

    $ds1->define_node({ path => 'refs',
			role => 'PB1::ReferenceData',
			allow_format => '+ris',
			output => '1.1:refs:output_map',
			title => 'Bibliographic references' });
    
    $ds1->define_node({ path => 'refs/single',
			method => 'get',
			title => 'Single bibliographic reference' });
    
    $ds1->define_node({ path => 'refs/list',
			method => 'list',
			title => 'Lists of bibliographic references' });
    
    
    # The following paths are used for miscellaneous documentation
    
    $ds1->define_node({ path => 'common',
			ruleset => '1.1:common_params',
			title => 'common parameters' });
    
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
			file_dir => 'public/css' });

};

1;
