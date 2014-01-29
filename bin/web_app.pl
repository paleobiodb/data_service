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
      doc_path => '1.1/json', title => 'JSON',
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
      doc_path => '1.1/text', title => 'tab-separated text',
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'tsv', content_type => 'text/tab-separated-values', 
      doc_path => '1.1/text', title => 'tab-separated text',
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'csv', content_type => 'text/csv',
      doc_path => '1.1/text', title => 'comma-separated text',
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'ris', disabled => 1, content_type => 'application/x-research-info-systems',
      doc_path => '1.1/ris', title => 'RIS',
      module => 'RIS.pm'},
	"The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	"common format for bibliographic references.");

# Next, we define the parameters we will accept, and the acceptable values
# for each of them.

$ds->initialize_class('CommonData');

$ds->define_ruleset('1.1:refs_display' =>
    "The following parameter indicates which information should be returned about each resulting collection:",
    [param => 'show', ENUM_VALUE('formatted','comments','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<refbasic>.",
    [ignore => 'level']);

$ds->define_ruleset('1.1:taxon_specifier' => 
    [param => 'name', \&TaxonData::validNameSpec, { alias => 'taxon_name' }],
    "Return information about the most fundamental taxonomic name matching this string.",
    "The C<%> character may be used as a wildcard.",
    [param => 'id', POS_VALUE, { alias => 'taxon_id' }],
    "Return information about the taxonomic name corresponding to this identifier.",
    [at_most_one => 'name', 'id'],
    ">You may not specify both C<name> and C<id> in the same query.",
    [optional => 'rank', \&TaxonData::validRankSpec],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact'),
      { default => 'current' } ]);

$ds->define_ruleset('1.1:taxon_selector' =>
    "The following parameters are used to indicate a base taxon or taxa:",
    [param => 'name', \&TaxonData::validNameSpec, { alias => 'taxon_name', list => "," }],
    "Select the most fundamental taxon corresponding to the specified name(s).",
    "To specify more than one, separate them by commas.",
    "The C<%> character may be used as a wildcard.",
    [param => 'id', POS_VALUE, { list => ',', alias => 'base_id' }],
    "Selects the taxa corresponding to the specified identifier(s).",
    "You may specify more than one, separated by commas.",
    ">The following parameters indicate which related taxonomic names to return:",
    [param => 'rel', ENUM_VALUE('self', 'synonyms', 'children', 'all_children', 
				'parents', 'all_parents', 'common_ancestor', 'all_taxa'),
      { default => 'self' } ],
    "Accepted values include:", "=over 4",
    "=item self", "Return information about the base taxon or taxa themselves.  This is the default.",
    "=item synonyms", "Return information about all synonyms of the base taxon or taxa.",
    "=item children", "Return information about the immediate children of the base taxon or taxa.",
    "=item all_children", "Return information about all taxa contained within the base taxon or taxa.",
    "=item parents", "Return information about the immediate parent of each base taxon.",
    "=item all_parents", "Return information about all taxa which contain any of the base taxa.",
    "=item common_ancestor", "Return information about the common ancestor of all of the base taxa.",
    "=item all_taxa", "Return information about all taxa in the database.",
    "You need not specify either C<name> or C<id> in this case.",
    "Use with caution, because the maximum data set returned may be as much as 80 MB.",
    [param => 'status', ENUM_VALUE('valid', 'senior', 'invalid', 'all'),
      { default => 'valid' } ],
    "Return only names that have the specified status.  Accepted values include:", "=over 4",
    "=item valid", "Return only valid names.  This is the default.",
    "=item senior", "Return only valid names that are not junior synonyms",
    "=item invalid", "Return only invalid names (e.g. nomen dubia).",
    "=item all", "Return all names.",
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact', 'all'),
      { default => 'current' } ]);

$ds->define_ruleset('1.1:taxon_filter' => 
    "The following parameters further filter the list of return values:",
    [optional => 'rank', \&TaxonData::validRankSpec],
    "Return only taxonomic names at the specified rank (e.g. 'genus').",
    [optional => 'extant', BOOLEAN_VALUE],
    "Return only extant or non-extant taxa.  Accepted values include C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
    [optional => 'depth', POS_VALUE]);

$ds->define_ruleset('1.1:taxon_display' => 
    "The following parameter indicates which information should be returned about each resulting name:",
    [optional => 'show', ENUM_VALUE('ref','attr','app','applong',
				    'appfirst','size','nav'),
	{ list => ','}],
    "This parameter specifies what fields should be returned.  For the full list of fields,",
    "See the L<RESPONSE|#RESPONSE> section.  Its value should be a comma-separated list",
    "of section names, and defaults to C<basic>.",
    [optional => 'exact', FLAG_VALUE]);

$ds->define_ruleset('1.1/taxa/single' => 
    [require => '1.1:taxon_specifier',
	{ error => "you must specify either 'name' or 'id'" }],
    [allow => '1.1:taxon_display'], 
    [allow => '1.1:common_params'],
    "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

$ds->define_ruleset('1.1/taxa/list' => 
    [require => '1.1:taxon_selector',
	{ error => "you must specify one of 'name', 'id', 'status', 'base_name', 'base_id', 'leaf_name', 'leaf_id'" }],
    [allow => '1.1:taxon_filter'],
    [allow => '1.1:taxon_display'], 
    [allow => '1.1:common_params'],
    "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

$ds->define_ruleset('1.1/taxa/auto' =>
    [param => 'name', ANY_VALUE],
    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
    [allow => '1.1:common_params'],
    "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

$ds->define_ruleset('1.1/taxa/thumb' =>
    [content_type => 'ct', 'png=image/png', { key => 'output_format' }],
    [ignore => 'splat'],
    [param => 'id', POS_VALUE]);

$ds->define_ruleset('1.1/taxa/icon' =>
    [content_type => 'ct', 'png=image/png', { key => 'output_format' }],
    [ignore => 'splat'],
    [param => 'id', POS_VALUE]);

$ds->define_ruleset('1.1:interval_selector' => 
    [param => 'scale_id', POS_VALUE, ENUM_VALUE('all'), 
	{ list => ',', alias => 'scale',
	  error => "the value of {param} should be a list of positive integers or 'all'" }],
    "Return intervals from the specified time scale(s) should be returned.",
    "The value of this parameter should be a list of positive integers or 'all'",
    [param => 'min_ma', DECI_VALUE(0)],
    [param => 'max_ma', DECI_VALUE(0)],
    [param => 'order', ENUM_VALUE('older', 'younger'), { default => 'younger' }],
    "Return the intervals in order starting as specified.  Possible values include ",
    "'older', 'younger'.  Defaults to 'younger'.");

$ds->define_ruleset('1.1:interval_specifier' =>
    [param => 'id', POS_VALUE],
    "Returns the interval corresponding to the specified identifier");

$ds->define_ruleset('1.1/intervals/list' => 
    [allow => '1.1:interval_selector'],
    [allow => '1.1:common_params']);

$ds->define_ruleset('1.1/intervals/single' => 
    [allow => '1.1:interval_specifier'],
    [allow => '1.1:common_params']);

$ds->define_ruleset('1.1:person_selector' => 
    [param => 'name', ANY_VALUE]);

$ds->define_ruleset('1.1:person_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'person_id' }]);

$ds->define_ruleset('1.1/people/single' => 
    [allow => '1.1:person_specifier'],
    [allow => '1.1:common_params']);

$ds->define_ruleset('1.1/people/list' => 
    [require => '1.1:person_selector'],
    [allow => '1.1:common_params']);

$ds->define_ruleset('1.1:refs_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'ref_id' }]);

$ds->define_ruleset('1.1/refs/single' => 
    [require => '1.1:refs_specifier'],
    [allow => '1.1:common_params']);

#$ds->define_ruleset('1.1/refs/toprank' => 
#    [require => '1.1:main_selector'],
#    [allow => '1.1:common_params']);



$ds->initialize_class('CommonData');

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
		   allow_vocab => 'pbdb,com' });

# Configuration. This path is used by clients who need to configure themselves
# based on parameters supplied by the data service.

$ds->define_path({ path => '1.1/config',
		   class => 'ConfigData',
		   method => 'get',
		   output_map => '1.1:config:get_map',
		   doc_title => 'Client configuration' });

# Intervals.  These paths are used to fetch information about geological time
# intervals known to the database.

$ds->define_path({ path => '1.1/intervals',
		   class => 'IntervalData',
		   doc_output => 'basic,ref' });

$ds->define_path({ path => '1.1/intervals/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/intervals/list',
		   method => 'list' });

# Taxa.  These paths are used to fetch information about biological taxa known
# to the database.

$ds->define_path({ path => '1.1/taxa',
		   class => 'TaxonData',
		   #allow_format => '+xml',
		   allow_vocab => '+dwc',
		   doc_output => 'basic,ref,attr,size,app,nav' });

$ds->define_path({ path => '1.1/taxa/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/taxa/list',
		   method => 'list' });

$ds->define_path({ path => '1.1/taxa/auto',
		   method => 'auto', 
		   allow_format => 'json',
		   base_output => 'auto',
		   doc_output => 'auto' });

$ds->define_path({ path => '1.1/taxa/thumb',
		   allow_format => 'json',
		   allow_vocab => 'com',
		   method => 'getThumb' });

$ds->define_path({ path => '1.1/taxa/icon',
		   allow_format => 'json',
		   allow_vocab => 'com',
		   method => 'getIcon' });

# Collections.  These paths are used to fetch information about fossil
# collections known to the database.

$ds->define_path({ path => '1.1/colls',
		   class => 'CollectionData',
		   allow_format => '+xml',
		   doc_title => 'Fossil collections' });

$ds->define_path({ path => '1.1/colls/single',
		   method => 'get',
		   output_map => '1.1:colls:basic_map'});
		 
$ds->define_path({ path => '1.1/colls/list',
		   method => 'list',
		   output_map => '1.1:colls:basic_map'});

$ds->define_path({ path => '1.1/colls/summary',
		   class => 'CollectionSummary',
		   method => 'summary',
		   output_map => '1.1:colls:summary_map'});

$ds->define_path({ path => '1.1/colls/refs',
		   method => 'refs',
		   allow_format => '+ris,-xml',
		   #also_initialize => 'ReferenceData',
		   output_map => '1.1:refs:map',
		   doc_output => 'refbase,formatted,comments' });

# Occurrences.  These paths are used to fetch information about fossil
# occurrences known to the database.

$ds->define_path({ path => '1.1/occs',
		   class => 'OccurrenceData',
		   #allow_format => '+xml',
		   base_output => 'basic' });

$ds->define_path({ path => '1.1/occs/single',
		   method => 'get',
		   doc_output => 'basic,coll,ref,geo,loc,time,ent,crmod' });

$ds->define_path({ path => '1.1/occs/list',
		   method => 'list',
		   doc_output => 'basic,coll,ref,geo,loc,time,ent,crmod' });

# People

$ds->define_path({ path => '1.1/people',
		   class => 'PersonData',
		   uses_dbh => 1 });

$ds->define_path({ path => '1.1/people/single', 
		   method => 'get' });

$ds->define_path({ path => '1.1/people/list',
		   method => 'list' });

# References

# $ds->define_path({ path => '1.1/refs',
# 		   class => 'ReferenceData',
# 		   allow_format => '+ris',
# 		   uses_dbh => 1 });

# $ds->define_path({ path => '1.1/refs/single',
# 		   method => 'get' });

# $ds->define_path({ path => '1.1/refs/list',
# 		   method => 'list' });

# The following paths are used only for documentation

$ds->define_path({ path => '1.1/common',
		   ruleset => '1.1:common_params',
		   doc_title => 'common parameters' });

$ds->define_path({ path => '1.1/json',
		   doc_title => 'JSON format' });

$ds->define_path({ path => '1.1/xml',
		   doc_title => 'XML format' });

$ds->define_path({ path => '1.1/text',
		   doc_title => 'Text formats' });


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


