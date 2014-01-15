#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

package PBDB_Data;

use Dancer;

use Template;
use Try::Tiny;
use Scalar::Util qw(blessed);

use Web::DataService qw( :validators );

use ConfigData;
use IntervalData;
use TaxonData;
use CollectionData;
use OccurrenceData;
use PersonData;


# Start by instantiating the data service.
# ========================================

# Many of the configuration parameters are set by entries in config.yml.

my $ds = DataService->new({ path_prefix => '/data' });

# If we were called from the command line with 'GET' as the first argument,
# then assume that we have been called for debugging purposes.

if ( defined $ARGV[0] and $ARGV[0] eq 'GET' )
{
    set apphandler => 'Debug';
    set logger => 'console';
    set show_errors => 0;
    
    $ds->{DEBUG} = 1;
}


# Then call methods from the DataService class to
# define the shape and behavior of the data service.
# ==================================================

# We start by defining the vocabularies that are available for describing the
# data returned by this service.

$ds->define_vocab(
    { name => 'pbdb',
      use_field_names => 1 },
        "The Paleobiology Database field names.  This vocabulary is the",
	"default for text format responses (tsv, csv, txt).",
    { name => 'com' },
        "3-character abbreviated (\"compact\") field names.",
        "This is the default for JSON responses.",
    { name => 'dwc' },
        "Darwin Core element names.  This is the default for XML responses.",
        "Note that many fields are not represented in this vocabulary,",
        "because of limitations of the Darwin Core element set.");


# Then we define the formats in which data can be returned.

$ds->define_format(
    { name => 'json', content_type => 'application/json',
      default_vocab => 'com' },
	"The JSON format is intended primarily to support client applications,",
	"including the PBDB Navigator.  Response fields are named using compact",
	"3-character field names.",
    { name => 'xml', content_type => 'text/xml',
      default_vocab => 'dwc' },
	"The XML format is intended primarily to support data interchange with",
	"other databases, using the Darwin Core element set.",
    { name => 'txt', content_type => 'text/plain', 
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'tsv', content_type => 'text/tab-separated-values',
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'csv', content_type => 'text/csv',
      default_vocab => 'pbdb' },
        "The text formats (txt, tsv, csv) are intended primarily for researchers",
	"downloading data from the database.  These downloads can easily be",
	"loaded into spreadsheets or other analysis tools.  The field names are",
	"taken from the PBDB Classic interface, for compatibility with existing",
	"tools and analytical procedures.",
    { name => 'ris', content_type => 'application/x-research-info-systems' },
	"The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	"common format for bibliographic references.");

# Next, we define the parameters we will accept, and the acceptable values
# for each of them.

$ds->define_ruleset('1.1:common_params' => 
       "The following parameter is used with most requests:",
    { param => 'show', valid => ANY_VALUE },
       "Return extra result fields in addition to the basic fields.  The value should be a comma-separated",
       "list of values corresponding to the sections listed in the response documentation for the URL path",
       "that you are using.  If you include, e.g. 'app', then all of the fields whose section is C<app>",
       "will be included in the result set.",
       "You can use this parameter to tailor the result to your particular needs.",
       "If you do not include it then you will usually get back only the fields",
       "labelled C<basic>.  For more information, see the documentation pages",
       "for the individual URL paths.",
    "!!The following parameters can be used with all requests:",
    { optional => 'limit', valid => [POS_ZERO_VALUE, ENUM_VALUE('all')], 
      error => "acceptable values for 'limit' are a positive integer, 0, or 'all'",
	default => 500 },
       "Limits the number of records returned.  The value may be a positive integer, zero, or C<all>.  Defaults to 500.",
    { optional => 'offset', valid => POS_ZERO_VALUE },
       "Returned records start at this offset in the result set.  The value may be a positive integer or zero.",
    { optional => 'count', valid => FLAG_VALUE },
       "If specified, then the response includes the number of records found and the number returned.",
       "For more information about how this information is encoded, see the documentation pages",
       "for the various response formats.",
    { optional => 'vocab', valid => $ds->vocab_rule },
       "Selects the vocabulary used to name the fields in the response.  You only need to use this if",
       "you want to override the default vocabulary for your selected format.",
       "Possible values include:", $ds->vocab_doc,
    "!!The following parameters are only relevant to the text formats (csv, tsv, txt):",
    { optional => 'no_header', valid => FLAG_VALUE },
       "If specified, then the header line (which gives the field names) is omitted.",
    { optional => 'linebreak', valid => ENUM_VALUE('cr','crlf'), default => 'crlf' },
       "Specifies the linebreak character sequence.",
       "The value may be either 'cr' or 'crlf', and defaults to the latter.",
    { ignore => 'splat' });

$ds->define_ruleset('1.1:main_selector' =>
    "The following parameters can be used to specify which records to return.  Except as specified below, you can use these in combination:",
    [param => 'clust_id', POS_VALUE, { list => ',' }],
    "Return only records associated with the specified geographic clusters.",
    [param => 'taxon_name', \&TaxonData::validNameSpec],
    "Return only records associated with the specified taxonomic name(s).  You may specify multiple names, separated by commas.",
    [param => 'taxon_id', POS_VALUE, { list => ','}],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier.",
    "You may specify multiple identifiers, separated by commas.",
    [param => 'taxon_actual', FLAG_VALUE],
    "Return only records that were actually identified with the specified taxonomic name, not those which match due to synonymy",
    "or other correspondences between taxa",
    [param => 'base_name', \&TaxonData::validNameSpec, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), or I<any of their children>.",
    "You may specify multiple names, separated by commas.",
    [param => 'base_id', POS_VALUE, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier, or I<any of their children>.",
    "You may specify multiple identifiers, separated by commas.",
    "Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
    [at_most_one => 'taxon_name', 'taxon_id', 'base_name', 'base_id'],
    [param => 'exclude_id', POS_VALUE, { list => ','}],
    "Exclude any records whose associated taxonomic name is a child of the given name or names, specified by numeric identifier.",
    [param => 'person_id', POS_VALUE, { list => ','}],
    "Return only records whose entry was authorized by the given person or people, specified by numeric identifier.",
    [param => 'lngmin', DECI_VALUE],
    "",
    [param => 'lngmax', DECI_VALUE],
    "",
    [param => 'latmin', DECI_VALUE],
    "",
    [param => 'latmax', DECI_VALUE],
    "Return only records whose geographic location falls within the given bounding box.",
    "The longitude boundaries will be normalized to fall between -180 and 180, and will generate",
    "two adjacent bounding boxes if the range crosses the antimeridian",
    "Note that if you specify one of these parameters then you must specify all four of them.",
    [together => 'lngmin', 'lngmax', 'latmin', 'latmax',
	{ error => "you must specify all of 'lngmin', 'lngmax', 'latmin', 'latmax' if you specify any of them" }],
    [param => 'loc', ANY_VALUE],		# This should be a geometry in WKT format
    "Return only records whose geographic location falls within the specified geometry, specified in WKT format.",
    [param => 'continent', ANY_VALUE],
    "Return only records whose geographic location falls within the specified continents.  The list of accepted",
    "continents can be retrieved via L</data1.1/config>.",
    [param => 'min_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at least this old, specified in Ma.",
    [param => 'max_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at most this old, specified in Ma.",
    [param => 'interval_id', POS_VALUE],
    "Return only records whose temporal locality falls within the given geologic time interval, specified by numeric identifier.",
    [param => 'interval', ANY_VALUE],
    "Return only records whose temporal locality falls within the named geologic time interval.",
    [at_most_one => 'interval_id', 'interval', 'min_ma'],
    [at_most_one => 'interval_id', 'interval', 'max_ma'],
    [optional => 'timerule', ENUM_VALUE('contain','overlap','buffer')],
    "Resolve temporal locality according to the specified rule:", "=over 4",
    "=item contain", "Return only collections whose temporal locality is strictly contained in the specified time range.",
    "=item overlap", "Return only collections whose temporal locality overlaps the specified time range.",
    "=item buffer", "Return only collections whose temporal locality overlaps the specified range and is contained",
    "within the specified time range plus a buffer on either side.  If an interval from one of the timescales known to the database is",
    "given, then the default buffer will be the intervals immediately preceding and following at the same level.",
    "Otherwise, the buffer will default to 10 million years on either side.  This can be overridden using the parameters",
    "C<earlybuffer> and C<latebuffer>.  This is the default value for this option.",
    [optional => 'earlybuffer', POS_VALUE],
    "Override the default buffer period for the beginning of the time range when resolving temporal locality.",
    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).",
    [optional => 'latebuffer', POS_VALUE],
    "Override the default buffer period for the end of the time range when resolving temporal locality.",
    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).");

$ds->define_ruleset('1.1:coll_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'coll_id' }],
    "The identifier of the collection you wish to retrieve");

$ds->define_ruleset('1.1:coll_selector' =>
    "You can use the following parameter if you wish to retrieve information about",
    "a known list of collections, or to filter a known list against other criteria such as location or time.",
    "Only the records which match the other parameters that you specify will be returned.",
    [param => 'id', INT_VALUE, { list => ',', alias => 'coll_id' }],
    "A comma-separated list of collection identifiers.");

$ds->define_ruleset('1.1:coll_display' =>
    "The following parameter indicates which information should be returned about each resulting collection:",
    [param => 'show', ENUM_VALUE('bin','attr','ref','ent','loc','time','taxa','rem','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<basic>.",
    [ignore => 'level']);

$ds->define_ruleset('1.1/colls/single' => 
    [require => '1.1:coll_specifier', { error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:coll_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1/colls/list' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:coll_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1:refs_display' =>
    "The following parameter indicates which information should be returned about each resulting collection:",
    [param => 'show', ENUM_VALUE('formatted','comments','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<refbasic>.",
    [ignore => 'level']);

$ds->define_ruleset('1.1/colls/refs' =>
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:refs_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1:summary_display' => 
    [param => 'level', POS_VALUE, { default => 1 }],
    [param => 'show', ENUM_VALUE('ext','time'), { list => ',' }]);

$ds->define_ruleset('1.1/colls/summary' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:summary_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1:toprank_selector' =>
    [param => 'show', ENUM_VALUE('formation', 'ref', 'author'), { list => ',' }]);

$ds->define_ruleset('1.1:colls/toprank' => 
    [require => '1.1:main_selector'],
    [require => '1.1:toprank_selector'],
    [allow => '1.1:common_params']);

$ds->define_ruleset('1.1:occ_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'occ_id' }],
    "The identifier of the occurrence you wish to retrieve");

$ds->define_ruleset('1.1:occ_selector' =>
    [param => 'id', POS_VALUE, { list => ',', alias => 'occ_id' }],
    "Return occurrences identified by the specified identifier(s).  The value of this parameter may be a comma-separated list.",
    [param => 'coll_id', POS_VALUE, { list => ',' }],
    "Return occurences associated with the specified collections.  The value of this parameter may be a single collection",
    "identifier or a comma-separated list.");

$ds->define_ruleset('1.1:occ_display' =>
    "The following parameter indicates which information should be returned about each resulting occurrence:",
    [param => 'show', ENUM_VALUE('attr','ref','ent','geo','loc','coll','time','rem','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<basic>.",
    [ignore => 'level']);

$ds->define_ruleset('1.1/occs/single' =>
    [require => '1.1:occ_specifier', { error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:occ_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1/occs/list' => 
    [require_one => '1.1:occ_selector', '1.1:main_selector'],
    [allow => '1.1:occ_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1:taxon_specifier' => 
    [param => 'name', \&TaxonData::validNameSpec, { alias => 'taxon_name' }],
    "Return information about the most fundamental taxonomic name matching this string.",
    "The C<%> character may be used as a wildcard.",
    [param => 'id', POS_VALUE, { alias => 'taxon_id' }],
    "Return information about the taxonomic name corresponding to this identifier.",
    [at_most_one => 'name', 'id'],
    "!!You may not specify both C<name> and C<identifier> in the same query.",
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
    "!!The following parameters indicate which related taxonomic names to return:",
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
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

$ds->define_ruleset('1.1/taxa/list' => 
    [require => '1.1:taxon_selector',
	{ error => "you must specify one of 'name', 'id', 'status', 'base_name', 'base_id', 'leaf_name', 'leaf_id'" }],
    [allow => '1.1:taxon_filter'],
    [allow => '1.1:taxon_display'], 
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

$ds->define_ruleset('1.1/taxa/auto' =>
    [param => 'name', ANY_VALUE],
    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

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

$ds->define_ruleset('1.1/config' =>
    [param => 'show', ENUM_VALUE('geosum', 'ranks', 'all'), { list => ',', default => 'all' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below, or 'all'.  It defaults to 'all'.", 
    [allow => '1.1:common_params'],
    "!>You can use any of the L<common parameters|/data1.1/common_doc.html> with this request.");

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

$ds->define_ruleset('1.1/refs/toprank' => 
    [require => '1.1:main_selector'],
    [allow => '1.1:common_params']);


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
		   default_limit => 500,
		   allow_format => 'json,csv,tsv,txt',
		   base_output => 'basic' });

# Configuration. This path is used by clients who need to configure themselves
# based on parameters supplied by the data service.

$ds->define_path({ path => '1.1/config',
		   class => 'ConfigData',
		   base_output => undef,
		   method => 'get',
		   uses_dbh => 1,
		   output_doc => 'geosum,ranks'});

# Intervals.  These paths are used to fetch information about geological time
# intervals known to the database.

$ds->define_path({ path => '1.1/intervals',
		   class => 'IntervalData',
		   uses_dbh => 1,
		   output_doc => 'basic,ref' });

$ds->define_path({ path => '1.1/intervals/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/intervals/list',
		   method => 'list' });

# Taxa.  These paths are used to fetch information about biological taxa known
# to the database.

$ds->define_path({ path => '1.1/taxa',
		   class => 'TaxonData',
		   allow_format => '+xml',
		   allow_vocab => '+dwc',
		   uses_dbh => 1,
		   output_doc => 'basic,ref,attr,size,app,nav' });

$ds->define_path({ path => '1.1/taxa/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/taxa/list',
		   method => 'list' });

$ds->define_path({ path => '1.1/taxa/auto',
		   method => 'auto', 
		   allow_format => 'json',
		   base_output => 'auto',
		   output_doc => 'auto' });

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
		   uses_dbh => 1,
		   base_output => 'basic' });

$ds->define_path({ path => '1.1/colls/single',
		   method => 'get',
		   output_doc => 'basic,bin,ref,sref,loc,time,taxa,ent,crmod'});
		 
$ds->define_path({ path => '1.1/colls/list',
		   method => 'list', 
		   output_doc => 'basic,bin,ref,sref,loc,time,taxa,ent,crmod' });

$ds->define_path({ path => '1.1/colls/summary',
		   method => 'summary', 
		   base_output => 'summary',
		   output_doc => 'summary,ext,summary_time' });

$ds->define_path({ path => '1.1/colls/refs',
		   method => 'refs',
		   allow_format => '+ris,-xml',
		   base_output => 'refbase',
		   output_doc => 'refbase,formatted,comments' });

# Occurrences.  These paths are used to fetch information about fossil
# occurrences known to the database.

$ds->define_path({ path => '1.1/occs',
		   class => 'OccurrenceData',
		   allow_format => '+xml',
		   uses_dbh => 1,
		   base_output => 'basic' });

$ds->define_path({ path => '1.1/occs/single',
		   method => 'get',
		   output_doc => 'basic,coll,ref,geo,loc,time,ent,crmod' });

$ds->define_path({ path => '1.1/occs/list',
		   method => 'list',
		   output_doc => 'basic,coll,ref,geo,loc,time,ent,crmod' });

# People

$ds->define_path({ path => '1.1/people',
		   class => 'PersonData',
		   uses_dbh => 1 });

$ds->define_path({ path => '1.1/people/single', 
		   method => 'get' });

$ds->define_path({ path => '1.1/people/list',
		   method => 'list' });

# References

$ds->define_path({ path => '1.1/refs',
		   class => 'ReferenceData',
		   allow_format => '+ris',
		   uses_dbh => 1 });

$ds->define_path({ path => '1.1/refs/single',
		   method => 'get' });

$ds->define_path({ path => '1.1/refs/list',
		   method => 'list' });

# The following paths are used only for documentation

$ds->define_path({ path => '1.1/common',
		   ruleset => '1.1:common_params',
		   doc_title => 'common parameters' });

$ds->define_path({ path => '1.1/json',
		   doc_title => 'JSON format' });

$ds->define_path({ path => '1.1/xml',
		   doc_title => 'XML format' });

$ds->define_path({ path => '1.1/text',
		   doc_title => 'text formats' });


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

get qr{ ^ $PREFIX ( / | / .* )? $ }xs => sub {
    
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
	          ( index | \w+_doc ) \. ( html | pod ) $ }xs => sub {
	    
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
    
    # Abort if this path is not valid for execution
    
    forward unless $ds->can_execute_path($path);
    
    # Execute the specified request
    
    return $ds->execute_path($path, $suffix);
};


# Any other URL is an error.

get qr{(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    $ds->error_result("", "html", "404 The resource you requested was not found.");
};


dance;


