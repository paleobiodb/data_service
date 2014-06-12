# 
# Paleobiology Data Service version 1.1
# 
# This file defines version 1.1 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


package Data_1_1;

use Data_1_1::CommonData;
use Data_1_1::ConfigData;
use Data_1_1::IntervalData;
use Data_1_1::TaxonData;
use Data_1_1::CollectionData;
use Data_1_1::OccurrenceData;
use Data_1_1::ReferenceData;
use Data_1_1::PersonData;


# setup ( ds )
# 
# This routine is called from the main program, in order to set up version 1.1
# of the data service.  The main service object is provided as a parameter,
# and we instantiate a sub-service object here.

sub setup {

    my ($ds) = @_;
    
    my $ds1 = $ds->define_subservice(
	{ name => 'data1.1',
	  label => 'version 1.1',
	  version => 'b4',
	  path_prefix => 'data1.1',
	  ruleset_prefix => '1.1:',
	  doc_dir => 'doc/1.1',
	  package => 'Data_1_1' },
	    "This is the current stable version of the data service.  The interface is guaranteed",
            "not to change, except possibly for extremely important bug fixes.  In such a case,",
	    "every effort would be made not to change anything that would break any existing applications.");
    
    # We then define the vocabularies that will be used to label the data
    # fields returned by this service.
    
    $ds1->define_vocab(
	{ name => 'pbdb', title => 'PaleobioDB field names',
	  use_field_names => 1 },
	    "The original Paleobiology Database field names, augmented by some",
	    "additional new fields.  This vocabulary is the",
	    "default for text format responses (.tsv, .csv, .txt).",
	{ name => 'com', title => 'Compact field names' },
	    "3-character abbreviated field names.",
	    "This is the default for JSON responses.",
	{ name => 'dwc', title => 'Darwin Core', disabled => 1 },
	    "Darwin Core element names.  This is the default for XML responses.",
	    "Note that many fields are not represented in this vocabulary,",
	    "because of limitations in the Darwin Core element set.");
    
    
    # Then the formats in which data can be returned.
    
    $ds1->define_format(
	{ name => 'json', content_type => 'application/json',
	  doc_path => '1.1/formats/json', title => 'JSON',
	  default_vocab => 'com' },
	    "The JSON format is intended primarily to support client applications,",
	    "including the PBDB Navigator.  Response fields are named using compact",
	    "3-character field names.",
	{ name => 'xml', disabled => 1, content_type => 'text/xml', title => 'XML',
	  doc_path => '1.1/xml',
	  default_vocab => 'dwc' },
	    "The XML format is intended primarily to support data interchange with",
	    "other databases, using the Darwin Core element set.",
	{ name => 'txt', content_type => 'text/plain',
	  doc_path => '1.1/formats/text', title => 'comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'csv', content_type => 'text/csv',
	  disposition => 'attachment',
	  doc_path => '1.1/formats/text', title => 'comma-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'tsv', content_type => 'text/tab-separated-values', 
	  disposition => 'attachment',
	  doc_path => '1.1/formats/text', title => 'tab-separated text',
	  default_vocab => 'pbdb' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'ris', content_type => 'application/x-research-info-systems',
	  doc_path => '1.1/formats/ris', title => 'RIS', disposition => 'attachment',
	  class => 'RISFormat'},
	    "The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	    "common format for bibliographic references.",
	{ name => 'png', content_type => 'image/png', class => '',
	  doc_path => '1.1/formats/png', title => 'PNG' },
	    "The PNG suffix is used with a few URL paths to fetch images stored",
	    "in the database.");


    # Define caches (i.e. cache namespaces) that will be used to satisfy queries
    # whenever possible.
    
    $ds1->define_cache(
	{ name => '1.1:colls', check_entry => \&CollectionData::cache_still_good });


    # Then define the URL paths that this subservice will accept.  We start with
    # the root of the hierarchy, which sets defaults for all the rest of the paths.
    
    $ds1->define_path({ path => '/', 
			public_access => 1,
			output_param => 'show',
			output_label => 'basic',
			vocab_param => 'vocab',
			limit_param => 'limit',
			count_param => 'count',
			uses_dbh => 1,
			default_limit => 500,
			allow_format => 'json,csv,tsv,txt',
			allow_vocab => 'pbdb,com',
			doc_layout => '1.1/doc_new.tt',
			doc_title => 'Documentation' });
    
    # Configuration. This path is used by clients who need to configure themselves
    # based on parameters supplied by the data service.
    
    $ds1->define_path({ path => 'config',
			class => 'Data_1_1::ConfigData',
			method => 'get',
			output_opt => '1.1:config:get_map',
			doc_title => 'Client configuration' });

    # Occurrences.  These paths are used to fetch information about fossil
    # occurrences known to the database.
    
    $ds1->define_path({ path => 'occs',
			class => 'Data_1_1::OccurrenceData',
			allow_format => '+xml',
			doc_title => 'Fossil occurrences' });
    
    $ds1->define_path({ path => 'occs/single',
			method => 'get',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			output_opt => '1.1:occs:basic_map',
			doc_title => 'Single fossil occurrence'});
    
    $ds1->define_path({ path => 'occs/list',
			method => 'list',
			post_configure_hook => 'prune_field_list',
			output => '1.1:occs:basic',
			output_opt => '1.1:occs:basic_map',
			doc_title => 'Lists of fossil occurrences' });
    
    $ds1->define_path({ path => 'occs/refs',
			method => 'refs',
			allow_format => '+ris,-xml',
			output => '1.1:refs:basic',
			output_opt => '1.1:refs:output_map',
			doc_title => 'Bibliographic references for fossil occurrences' });
    
    $ds1->define_path({ path => 'occs/taxa',
			method => 'taxa',
			output => '1.1:taxa:basic',
			output_opt => '1.1:taxa:output_map',
			doc_title => 'Taxa from fossil occurrences' });
    
    # Collections.  These paths are used to fetch information about fossil
    # collections known to the database.
    
    $ds1->define_path({ path => 'colls',
			class => 'Data_1_1::CollectionData',
			use_cache => '1.1:colls',
			doc_title => 'Fossil collections' });
    
    $ds1->define_path({ path => 'colls/single',
			method => 'get',
			post_configure_hook => 'prune_field_list',
			output => '1.1:colls:basic_map',
			doc_title => 'Single fossil collection' });
    
    $ds1->define_path({ path => 'colls/list',
			method => 'list',
			post_configure_hook => 'prune_field_list',
			output => '1.1:colls:basic_map',
			doc_title => 'Lists of fossil collections' });
    
    $ds1->define_path({ path => 'colls/summary',
			method => 'summary',
			output => '1.1:colls:summary',
			output_opt => '1.1:colls:summary_map',
			doc_title => 'Geographic clusters of fossil collections' });
    
    $ds1->define_path({ path => 'colls/refs',
			method => 'refs',
			allow_format => '+ris',
			output => '1.1:refs:output_map',
			doc_title => 'Bibliographic references for fossil collections' });
    
    # Strata.  These paths are used to fetch information abot geological strata
    # known to the database.
    
    $ds1->define_path({ path => 'strata',
			class => 'Data_1_1::CollectionData',
			doc_title => 'Geological strata' });
    
    $ds1->define_path({ path => 'strata/list',
			method => 'strata',
			output => '1.1:colls:strata' });
    
    $ds1->define_path({ path => 'strata/auto',
			method => 'strata',
			arg => 'auto',
			output => '1.1:colls:strata',
			doc_title => 'Auto-completion for geological strata' });
    
    # Taxa.  These paths are used to fetch information about biological taxa known
    # to the database.
    
    $ds1->define_path({ path => 'taxa',
			class => 'Data_1_1::TaxonData',
			output => '1.1:taxa:basic',
			output_opt => '1.1:taxa:output_map',
			doc_title => 'Taxonomic names' });
    
    $ds1->define_path({ path => 'taxa/single',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			method => 'get',
			doc_title => 'Single taxon' });
    
    $ds1->define_path({ path => 'taxa/list',
			allow_format => '+xml',
			allow_vocab => '+dwc',
			method => 'list',
			doc_title => 'Lists of taxa' });
    
    $ds1->define_path({ path => 'taxa/refs',
			output => '1.1:refs:basic',
			output_opt => '1.1:refs:output_map',
			method => 'list',
			arg => 'refs',
			doc_title => 'Bibliographic references for taxa' });
    
    $ds1->define_path({ path => 'taxa/match',
			method => 'match' });
    
    $ds1->define_path({ path => 'taxa/auto',
			method => 'auto', 
			allow_format => 'json',
			output => '1.1:taxa:auto_map',
			doc_title => 'Auto-completion for taxonomic names' });
    
    $ds1->define_path({ path => 'taxa/thumb',
			allow_format => '+png',
			output => '1.1:taxa:imagedata',
			method => 'get_image',
			arg => 'thumb',
			doc_title => 'Thumbnail images of lifeforms' });
    
    $ds1->define_path({ path => 'taxa/icon',
			allow_format => '+png',
			output => '1.1:taxa:imagedata',
			method => 'get_image',
			arg => 'icon',
			doc_title => 'Icon images of lifeforms' });
    
    $ds1->define_path({ path => 'taxa/list_images',
			output => '1.1:taxa:imagedata',
			method => 'list_images',
			doc_title => 'List the available images of lifeforms' });
    
    
# Time scales and intervals.  These paths are used to fetch information about
# geological time scales and time intervals known to the database.

    $ds1->define_path({ path => 'intervals',
			class => 'Data_1_1::IntervalData',
			output => '1.1:intervals:basic',
			doc_title => 'Geological Time scales and time intervals' });
    
    $ds1->define_path({ path => 'intervals/single',
			method => 'get',
			doc_title => 'Single geological time interval' });
    
    $ds1->define_path({ path => 'intervals/list',
			method => 'list',
			doc_title => 'Lists of geological time intervals' });
    
    $ds1->define_path({ path => 'scales',
			class => 'Data_1_1::IntervalData',
			output => '1.1:scales:basic',
			doc_title => 'Geological time scales' });
    
    $ds1->define_path({ path => 'scales/single',
			method => 'list_scales',
			doc_title => 'Single geological time scale' });
    
    $ds1->define_path({ path => 'scales/list',
			method => 'list_scales',
			doc_title => 'List of available geological time scales' });


    # People.  These paths are used to fetch the names of database contributors.

    $ds1->define_path({ path => 'people',
			class => 'Data_1_1::PersonData',
			output => '1.1:people:basic',
			doc_title => 'Database contributors' });
    
    $ds1->define_path({ path => 'people/single', 
			method => 'get' });
    
    $ds1->define_path({ path => 'people/list',
			method => 'list' });

    
    # Bibliographic References

    $ds1->define_path({ path => 'refs',
			class => 'Data_1_1::ReferenceData',
			allow_format => '+ris',
			output => '1.1:refs:output_map',
			doc_title => 'Bibliographic references' });
    
    $ds1->define_path({ path => 'refs/single',
			method => 'get',
			doc_title => 'Single bibliographic reference' });
    
    $ds1->define_path({ path => 'refs/list',
			method => 'list',
			doc_title => 'Lists of bibliographic references' });
    
    
    # The following paths are used for miscellaneous documentation
    
    $ds1->define_path({ path => 'common',
			ruleset => '1.1:common_params',
			doc_title => 'common parameters' });
    
    $ds1->define_path({ path => 'datetime',
			doc_title => 'Selecting records by date and time' });
    
    $ds1->define_path({ path => 'formats',
			doc_title => 'Formats and Vocabularies' });
    
    $ds1->define_path({ path => 'formats/json',
			doc_title => 'JSON format' });
    
    $ds1->define_path({ path => 'formats/xml',
			doc_title => 'XML format' });
    
    $ds1->define_path({ path => 'formats/text',
			doc_title => 'Text formats' });
    
    $ds1->define_path({ path => 'formats/ris',
			doc_title => "RIS format" });
    
    $ds1->define_path({ path => 'formats/png',
			doc_title => 'PNG format' });
    
    
    # And finally, stylesheets and such
    
    $ds1->define_path({ path => 'css',
			file_dir => 'public/css',
			send_files => 1 });

};

1;
