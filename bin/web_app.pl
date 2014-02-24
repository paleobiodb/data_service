#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

use Dancer;

use Template;
use Try::Tiny;
use Scalar::Util qw(blessed);

use Web::DataService qw( :validators );

use CommonData;
use ConfigData;
use IntervalData;
use TaxonData;
use CollectionData;
use CollectionSummary;
use OccurrenceData;
use ReferenceData;
use PersonData;


# Start by instantiating the data service.
# ========================================

# Many of the configuration parameters are set by entries in config.yml.

my $ds = Web::DataService->new({ name => 'Paleobiodb Data',
				 path_prefix => '/data' });

# If we were called from the command line with 'GET' as the first argument,
# then assume that we have been called for debugging purposes.

if ( defined $ARGV[0] and $ARGV[0] eq 'GET' )
{
    set apphandler => 'Debug';
    set logger => 'console';
    set show_errors => 0;
    
    $ds->{DEBUG} = 1;
    $ds->{ONE_REQUEST} = 1;
}

# Then call methods from the DataService class to
# define the shape and behavior of the data service.
# ==================================================

# We start by defining the vocabularies that are available for describing the
# data returned by this service.

$ds->define_vocab(
    { name => 'pbdb', title => 'PaleoDB field names',
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
        "because of limitations of the Darwin Core element set.");


# Then we define the formats in which data can be returned.

$ds->define_format(
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


# Define caches (CHI namespaces) that will be used to satisfy queries whenever possible.

$ds->define_cache(
    { name => '1.1:colls', check_entry => \&CollectionData::cache_still_good });


# Then define the URL paths that our data service accepts.  We start with the
# root of the hierarchy, which is a protocol version number.  The following
# calls define version 1.1 of this data servive.

$ds->define_path({ path => '1.1', 
		   version => '1.1',
		   public_access => 1,
		   output_param => 'show',
		   vocab_param => 'vocab',
		   limit_param => 'limit',
		   count_param => 'count',
		   uses_dbh => 1,
		   default_limit => 500,
		   allow_format => 'json,csv,tsv,txt',
		   allow_vocab => 'pbdb,com',
		   doc_title => 'Documentation' });

# Configuration. This path is used by clients who need to configure themselves
# based on parameters supplied by the data service.

$ds->define_path({ path => '1.1/config',
		   class => 'ConfigData',
		   method => 'get',
		   output => '1.1:config:get_map',
		   doc_title => 'Client configuration' });

# Occurrences.  These paths are used to fetch information about fossil
# occurrences known to the database.

$ds->define_path({ path => '1.1/occs',
		   class => 'OccurrenceData',
		   allow_format => '+xml',
		   doc_title => 'Fossil occurrences' });

$ds->define_path({ path => '1.1/occs/single',
		   method => 'get',
		   output => '1.1:occs:basic_map' });

$ds->define_path({ path => '1.1/occs/list',
		   method => 'list',
		   output => '1.1:occs:basic_map' });

$ds->define_path({ path => '1.1/occs/refs',
		   method => 'refs',
		   allow_format => '+ris,-xml',
	           output => '1.1:refs:output_map' });

# Collections.  These paths are used to fetch information about fossil
# collections known to the database.

$ds->define_path({ path => '1.1/colls',
		   class => 'CollectionData',
		   use_cache => '1.1:colls',
		   doc_title => 'Fossil collections' });

$ds->define_path({ path => '1.1/colls/single',
		   method => 'get',
		   output => '1.1:colls:basic_map'});
		 
$ds->define_path({ path => '1.1/colls/list',
		   method => 'list',
		   output => '1.1:colls:basic_map'});

$ds->define_path({ path => '1.1/colls/summary',
		   class => 'CollectionSummary',
		   method => 'summary',
		   output => '1.1:colls:summary_map'});

$ds->define_path({ path => '1.1/colls/refs',
		   method => 'refs',
		   allow_format => '+ris',
	           output => '1.1:refs:output_map' });

# Strata.  These paths are used to fetch information abot geological strata
# known to the database.

$ds->define_path({ path => '1.1/strata',
		   class => 'CollectionData',
		   doc_title => 'Geological strata' });

$ds->define_path({ path => '1.1/strata/list',
		   method => 'strata',
		   output => '1.1:colls:strata' });

$ds->define_path({ path => '1.1/strata/auto',
		   method => 'strata',
		   arg => 'auto',
		   output => '1.1:colls:strata' });

# Taxa.  These paths are used to fetch information about biological taxa known
# to the database.

$ds->define_path({ path => '1.1/taxa',
		   class => 'TaxonData',
		   output => '1.1:taxa:output_map' });

$ds->define_path({ path => '1.1/taxa/single',
		   allow_format => '+xml',
		   allow_vocab => '+dwc',
		   method => 'get' });

$ds->define_path({ path => '1.1/taxa/list',
		   allow_format => '+xml',
		   allow_vocab => '+dwc',
		   method => 'list' });

$ds->define_path({ path => '1.1/taxa/refs',
		   output => '1.1:refs:output_map',
		   method => 'list_refs' });

$ds->define_path({ path => '1.1/taxa/auto',
		   method => 'auto', 
		   allow_format => 'json',
		   output => '1.1:taxa:auto_map' });

$ds->define_path({ path => '1.1/taxa/thumb',
		   allow_format => '+png',
		   output => '1.1:taxa:imagedata',
		   method => 'get_image',
		   arg => 'thumb' });

$ds->define_path({ path => '1.1/taxa/icon',
		   allow_format => '+png',
		   output => '1.1:taxa:imagedata',
		   method => 'get_image',
		   arg => 'icon' });

$ds->define_path({ path => '1.1/taxa/list_images',
		   output => '1.1:taxa:imagedata',
		   method => 'list_images' });


# Time scales and intervals.  These paths are used to fetch information about
# geological time scales and time intervals known to the database.

$ds->define_path({ path => '1.1/intervals',
		   class => 'IntervalData',
		   output => '1.1:intervals:basic',
		   doc_title => 'Geological Time Scales and Time Intervals' });

$ds->define_path({ path => '1.1/intervals/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/intervals/list',
		   method => 'list' });

$ds->define_path({ path => '1.1/scales',
		   class => 'IntervalData',
		   output => '1.1:scales:basic',
		   doc_title => 'Geological Time Scales' });

$ds->define_path({ path => '1.1/scales/single',
		   method => 'list_scales' });

$ds->define_path({ path => '1.1/scales/list',
		   method => 'list_scales' });



# People.  These paths are used to fetch the names of database contributors.

$ds->define_path({ path => '1.1/people',
		   class => 'PersonData',
		   output => '1.1:people:basic',
		   doc_title => 'Database contributors' });

$ds->define_path({ path => '1.1/people/single', 
		   method => 'get' });

$ds->define_path({ path => '1.1/people/list',
		   method => 'list' });

# References

$ds->define_path({ path => '1.1/refs',
 		   class => 'ReferenceData',
		   allow_format => '+ris',
 		   output => '1.1:refs:output_map'});

$ds->define_path({ path => '1.1/refs/single',
 		   method => 'get' });

$ds->define_path({ path => '1.1/refs/list',
		   method => 'list' });

# The following paths are used only for documentation

$ds->define_path({ path => '1.1/common',
		   ruleset => '1.1:common_params',
		   doc_title => 'common parameters' });

$ds->define_path({ path => '1.1/datetime',
		   doc_title => 'Selecting records by date and time' });

$ds->define_path({ path => '1.1/formats',
		   doc_title => 'Formats and Vocabularies' });

$ds->define_path({ path => '1.1/formats/json',
		   doc_title => 'JSON format' });

$ds->define_path({ path => '1.1/formats/xml',
		   doc_title => 'XML format' });

$ds->define_path({ path => '1.1/formats/text',
		   doc_title => 'Text formats' });

$ds->define_path({ path => '1.1/formats/ris',
		   doc_title => "RIS format" });

$ds->define_path({ path => '1.1/formats/png',
		   doc_title => 'PNG format' });


# Now we configure a set of Dancer routes to serve
# the data, documentation, stylesheets, etc.
# ================================================

my ($PREFIX) = $ds->get_path_prefix;

# Any URL starting with /data/css indicates a stylesheet

get qr{ ^ $PREFIX [\d.]* /css/(.*) }xs => sub {
    
    $DB::single = 1;
    my ($filename) = splat;
    send_file("css/$filename");
};


# Any other URL starting with /data/... or just /data should display the list of
# available versions.

get qr{ ^ $PREFIX ( / | $ ) }xs => sub {
    
    my ($path) = splat;
    
    $DB::single = 1;
    my $format = 'pod' if $path =~ /\.pod$/;
    
    return $ds->document_path("/version_list.tt", $format);
};


# Any URL starting with /data<version> and ending in either .html, .pod, or no
# suffix at all is interpreted as a request for documentation.  If the given
# path does not correspond to any known documentation, we provide a page
# explaining what went wrong and providing the proper URLs.

get qr{ ^ $PREFIX ( \d+ \. \d+ / (?: [^/.]* / )* )
	          (?: ( index | \w+_doc ) \. ( html | pod ) )? $ }xs => sub {
	    
    my ($path, $last, $suffix) = splat;
    
    $DB::single = 1;
    $path .= $last unless !defined $last || $last eq 'index';
    $path =~ s{/$}{};
    $path =~ s{_doc}{};
    
    return $ds->document_path($path, $suffix);
};


get qr{ ^ $PREFIX ( \d+ \. \d+ (?: / [^/.]+ )* $ ) }xs => sub {
    
    my ($path) = splat;
    
    $DB::single = 1;
    
    return $ds->document_path($path, 'html');
};


# Any path that ends in a suffix other than .html or .pod is a request for an
# operation.

get qr{ ^ $PREFIX ( \d+ \. \d+ / (?: [^/.]* / )* \w+ ) \. (\w+) }xs => sub {
    
    my ($path, $suffix) = splat;
    
    $DB::single = 1;
    
    # If the path ends in a number, replace it by 'single' and add the parameter
    # as 'id'.
    
    if ( $path =~ qr{ (\d+) $ }xs )
    {
	params->{id} = $1;
	$path =~ s{\d+$}{single};
    }
    
    # Execute the path if allowed, otherwise indicate a bad request.
    
    if ( $ds->can_execute_path($path) )
    {
	return $ds->execute_path($path, $suffix);
    }
    
    else
    {
	$ds->error_result("", "html", "404 The resource you requested was not found.");
    }
};


# Any other URL is an error.

get qr{(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    $ds->error_result("", "html", "404 The resource you requested was not found.");
};


dance;


