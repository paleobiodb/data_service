# 
# Paleobiology Data Service version 1.9
# 
# This file defines version 1.0 of the Paleobiology Data Service, which is
# just a stub.  This version is obsolete, but we want to be able to respond
# to root documentation queries at least.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


package PBData;

{
    # We start by defining a data service instance for version 1.0
    
    our ($ds0) = Web::DataService->new(
	{ name => '1.0',
	  title => 'PBDB Data Service',
	  version => '1',
	  features => 'standard',
	  special_params => 'standard',
	  path_prefix => 'data1.0/',
	  doc_template_dir => 'doc/1.0' });
    
    
    # We define the basic formats, just in case somebody asks for JSON.
    
    $ds0->define_format(
	{ name => 'json', content_type => 'application/json',
	  doc_node => 'formats/json', title => 'JSON' },
	    "The JSON format is intended primarily to support client applications,",
	    "including the PBDB Navigator.  Response fields are named using compact",
	    "3-character field names.",
	{ name => 'txt', content_type => 'text/plain',
	  doc_node => 'formats/text', title => 'Comma-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'csv', content_type => 'text/csv',
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Comma-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.",
	{ name => 'tsv', content_type => 'text/tab-separated-values', 
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Tab-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.  The field names are",
	    "taken from the PBDB Classic interface, for compatibility with existing",
	    "tools and analytical procedures.");
    
    
    # Then define the URL paths that this subservice will accept.  We start
    # with the root of the hierarchy, which sets defaults for all the rest of
    # the nodes.  In fact, this is the only documentation node we need, since
    # the service is non-functional.
    
    $ds0->define_node({ path => '/', 
			public_access => 1,
			# error_template => '',
			allow_format => 'json,csv,tsv,txt',
			title => 'Documentation' });
    
    # The following paths are used for miscellaneous documentation
    
    $ds0->define_node({ path => 'special',
			ruleset => '1.1:special_params',
			title => 'Special parameters' });
    
    $ds0->define_node({ path => 'datetime',
			title => 'Selecting records by date and time' });
    
    $ds0->define_node({ path => 'formats',
			title => 'Formats and Vocabularies' });
    
    $ds0->define_node({ path => 'formats/json',
			title => 'JSON format' });
    
    $ds0->define_node({ path => 'formats/text',
			title => 'Text formats' });
        
    # And finally, stylesheets and such
    
    $ds0->define_node({ path => 'css',
			file_dir => 'css' });
    
    $ds0->define_node({ path => 'images',
			file_dir => 'images' });
};

1;
