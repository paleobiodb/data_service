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

use HTTP::Validate qw( :validators );
use Template;
use Try::Tiny;
use Scalar::Util qw(blessed);
#use Pod::Simple::HTML;
#use Pod::Simple::Text;

use DataService;

use ConfigData;
use IntervalData;
use TaxonData;
use CollectionData;
use OccurrenceData;
use PersonData;



# If we were called from the command line with 'GET' as the first argument,
# then assume we are running in debug mode.

if ( defined $ARGV[0] and $ARGV[0] eq 'GET' )
{
    set apphandler => 'Debug';
    set logger => 'console';
    
    our($DEBUG) = 1;
}


# Instantiate the request validator.

my $dv = HTTP::Validate->new();

# Instantiate the data service.

my $ds = DataService->new({ validator => $dv,
			    response_selector => 'show',
			    default_limit => 500,
			    stream_threshold => 20480,
			    public_access => 1,
			    needs_dbh => 1 });


# Specify the parameters we will accept, and the acceptable value types for
# each of them.

ruleset $dv '1.1:common_params' => 
    "The following parameter is used with most requests:",
    [param => 'show', ANY_VALUE],
    "Return extra result fields in addition to the basic fields.  The value should be a comma-separated",
    "list of values corresponding to the sections listed in the response documentation for the URL path",
    "that you are using.  If you include, e.g. 'app', then all of the fields whose section is C<app>",
    "will be included in the result set.\n",
    "You can use this parameter to tailor the result to your particular needs.",
    "If you do not include it then you will usually get back only the fields",
    "labelled C<basic>.  For more information, see the documentation pages",
    "for the individual URL paths.", "=back",
    "!!The following parameters can be used with all requests:",
    [content_type => 'ct', 'json', 'xml', 'txt=text/plain', 'tsv=text/tab-separated-values', 'csv', 
    	{ key => 'output_format' }],
    [optional => 'limit', POS_ZERO_VALUE, ENUM_VALUE('all'), 
      { error => "acceptable values for 'limit' are a positive integer, 0, or 'all'",
	default => 500 } ],
    "Limits the number of records returned.  The value may be a positive integer, zero, or C<all>.  Defaults to 500.",
    [optional => 'offset', POS_ZERO_VALUE],
    "Returned records start at this offset in the result set.  The value may be a positive integer or zero.",
    [optional => 'count', FLAG_VALUE],
    "If specified, then the response includes the number of records found and the number returned.",
    "This is ignored for the text formats (csv, tsv, txt) because they provide no way to fit this information in.",
    [optional => 'vocab', ENUM_VALUE('dwc', 'com', 'pbdb')],
    "Selects the vocabulary used to name the fields in the response.  You only need to use this if",
    "you want to override the default vocabulary for your selected format.",
    "Possible values include:", "=over",
    "=item pbdb", "The PBDB classic field names.  This is the default for text format responses (csv, tsv, txt).",
    "=item dwc", "Darwin Core element names.  This is the default for XML responses.",
    "Note that many fields are not represented in this vocabulary, because of limitations of the Darwin Core element set.",
    "=item com", "3-character abbreviated (\"compact\") field names.  This is the default for JSON responses.",
    "!!The following parameters are only relevant to the text formats (csv, tsv, txt):",
    [optional => 'no_header', FLAG_VALUE],
    "If specified, then the header line (which gives the field names) is omitted.",
    [optional => 'linebreak', ENUM_VALUE('cr','crlf'), { default => 'crlf' }],
    "Specifies the linebreak character sequence.  The value may be either 'cr' or 'crlf', and defaults to the latter.",
    [ignore => 'splat'];

ruleset $dv '1.1:main_selector' =>
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
    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).";

ruleset $dv '1.1:coll_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'coll_id' }],
    "The identifier of the collection you wish to retrieve";

ruleset $dv '1.1:coll_selector' =>
    "You can use the following parameter if you wish to retrieve information about",
    "a known list of collections, or to filter a known list against other criteria such as location or time.",
    "Only the records which match the other parameters that you specify will be returned.",
    [param => 'id', INT_VALUE, { list => ',', alias => 'coll_id' }],
    "A comma-separated list of collection identifiers.";

ruleset $dv '1.1:coll_display' =>
    "The following parameter indicates which information should be returned about each resulting collection:",
    [param => 'show', ENUM_VALUE('bin','attr','ref','ent','loc','time','taxa','rem','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<basic>.",
    [ignore => 'level'];

ruleset $dv '1.1/colls/single' => 
    [require => '1.1:coll_specifier', { error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:coll_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset $dv '1.1/colls/list' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:coll_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset $dv '1.1:summary_display' => 
    [param => 'level', POS_VALUE, { default => 1 }],
    [param => 'show', ENUM_VALUE('ext','time'), { list => ',' }];

ruleset $dv '1.1/colls/summary' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:summary_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset $dv '1.1:toprank_selector' =>
    [param => 'show', ENUM_VALUE('formation', 'ref', 'author'), { list => ',' }];

ruleset $dv '1.1:colls/toprank' => 
    [require => '1.1:main_selector'],
    [require => '1.1:toprank_selector'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1:occ_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'occ_id' }],
    "The identifier of the occurrence you wish to retrieve";

ruleset $dv '1.1:occ_selector' =>
    [param => 'id', POS_VALUE, { list => ',', alias => 'occ_id' }],
    "Return occurrences identified by the specified identifier(s).  The value of this parameter may be a comma-separated list.",
    [param => 'coll_id', POS_VALUE, { list => ',' }],
    "Return occurences associated with the specified collections.  The value of this parameter may be a single collection",
    "identifier or a comma-separated list.";

ruleset $dv '1.1:occ_display' =>
    "The following parameter indicates which information should be returned about each resulting occurrence:",
    [param => 'show', ENUM_VALUE('attr','ref','ent','geo','loc','coll','time','rem','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<basic>.",
    [ignore => 'level'];

ruleset $dv '1.1/occs/single' =>
    [require => '1.1:occ_specifier', { error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:occ_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset $dv '1.1/occs/list' => 
    [require_one => '1.1:occ_selector', '1.1:main_selector'],
    [allow => '1.1:occ_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset $dv '1.1:taxon_specifier' => 
    [param => 'name', \&TaxonData::validNameSpec, { alias => 'taxon_name' }],
    "Return information about the most fundamental taxonomic name matching this string.",
    "The C<%> character may be used as a wildcard.",
    [param => 'id', POS_VALUE, { alias => 'taxon_id' }],
    "Return information about the taxonomic name corresponding to this identifier.",
    [at_most_one => 'name', 'id'],
    "!!You may not specify both C<name> and C<identifier> in the same query.",
    [optional => 'rank', \&TaxonData::validRankSpec],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact'),
      { default => 'current' } ];

ruleset $dv '1.1:taxon_selector' =>
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
      { default => 'current' } ];

ruleset $dv '1.1:taxon_filter' => 
    "The following parameters further filter the list of return values:",
    [optional => 'rank', \&TaxonData::validRankSpec],
    "Return only taxonomic names at the specified rank (e.g. 'genus').",
    [optional => 'extant', BOOLEAN_VALUE],
    "Return only extant or non-extant taxa.  Accepted values include C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
    [optional => 'depth', POS_VALUE];

ruleset $dv '1.1:taxon_display' => 
    "The following parameter indicates which information should be returned about each resulting name:",
    [optional => 'show', ENUM_VALUE('ref','attr','app','applong',
				    'appfirst','size','nav'),
	{ list => ','}],
    "This parameter specifies what fields should be returned.  For the full list of fields,",
    "See the L<RESPONSE|#RESPONSE> section.  Its value should be a comma-separated list",
    "of section names, and defaults to C<basic>.",
    [optional => 'exact', FLAG_VALUE];

ruleset $dv '1.1/taxa/single' => 
    [require => '1.1:taxon_specifier',
	{ error => "you must specify either 'name' or 'id'" }],
    [allow => '1.1:taxon_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.", 
    [allow => '1.1:common_params'];

ruleset $dv '1.1/taxa/list' => 
    [require => '1.1:taxon_selector',
	{ error => "you must specify one of 'name', 'id', 'status', 'base_name', 'base_id', 'leaf_name', 'leaf_id'" }],
    [allow => '1.1:taxon_filter'],
    [allow => '1.1:taxon_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.", 
    [allow => '1.1:common_params'];

ruleset $dv '1.1/taxa/auto' =>
    [param => 'name', ANY_VALUE],
    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex",
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.", 
    [allow => '1.1:common_params'];

ruleset $dv '1.1/taxa/thumb' =>
    [content_type => 'ct', 'png=image/png', { key => 'output_format' }],
    [ignore => 'splat'],
    [param => 'id', POS_VALUE];

ruleset $dv '1.1/taxa/icon' =>
    [content_type => 'ct', 'png=image/png', { key => 'output_format' }],
    [ignore => 'splat'],
    [param => 'id', POS_VALUE];

ruleset $dv '1.1:interval_selector' => 
    [param => 'scale_id', POS_VALUE, ENUM_VALUE('all'), 
	{ list => ',', alias => 'scale',
	  error => "the value of {param} should be a list of positive integers or 'all'" }],
    "Return intervals from the specified time scale(s) should be returned.",
    "The value of this parameter should be a list of positive integers or 'all'",
    [param => 'min_ma', DECI_VALUE(0)],
    [param => 'max_ma', DECI_VALUE(0)],
    [param => 'order', ENUM_VALUE('older', 'younger'), { default => 'younger' }],
    "Return the intervals in order starting as specified.  Possible values include 'older', 'younger'.  Defaults to 'younger'.";

ruleset $dv '1.1:interval_specifier' =>
    [param => 'id', POS_VALUE],
    "Returns the interval corresponding to the specified identifier";

ruleset $dv '1.1/intervals/list' => 
    [allow => '1.1:interval_selector'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1/intervals/single' => 
    [allow => '1.1:interval_specifier'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1/config' =>
    "!> You can use any of the L<common parameters|/data1.1/common_doc.html> with this request.", 
    [allow => '1.1:common_params'];

ruleset $dv '1.1:person_selector' => 
    [param => 'name', ANY_VALUE];

ruleset $dv '1.1:person_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'person_id' }];

ruleset $dv '1.1/people/single' => 
    [allow => '1.1:person_specifier'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1/people/list' => 
    [require => '1.1:person_selector'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1:refs_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'ref_id' }];

ruleset $dv '1.1/refs/single' => 
    [require => '1.1:refs_specifier'],
    [allow => '1.1:common_params'];

ruleset $dv '1.1/refs/toprank' => 
    [require => '1.1:main_selector'],
    [allow => '1.1:common_params'];


# Configure the routes
# ====================

define_directory $ds '1.1' => { output => 'basic' };

# Miscellaneous

define_route $ds '1.1/config' => { class => 'ConfigData',
				   op => 'get',
				   docresp => 'basic',
				 };

define_route $ds '1.1/common' => { ruleset => '1.1:common_params',
				   doctitle => 'common parameters' };

define_route $ds '1.1/json' => { doctitle => 'JSON format' };

define_route $ds '1.1/xml' => { doctitle => 'XML format' };

define_route $ds '1.1/text' => { doctitle => 'text formats' };

# Intervals

define_directory $ds '1.1/intervals' => { class => 'IntervalData',
					  docresp => 'basic,ref' };

define_route $ds '1.1/intervals/single' => { op => 'get' };

define_route $ds '1.1/intervals/list' => { op => 'list' };

# Taxa

define_directory $ds '1.1/taxa' => { class => 'TaxonData',
				     docresp => 'basic,ref,attr,size,app,nav' };

define_route $ds '1.1/taxa/single' => { op => 'get' };

define_route $ds '1.1/taxa/list' => { op => 'list' };

define_route $ds '1.1/taxa/auto' => { op => 'auto', 
				      output => 'auto',
				      docresp => 'auto' };

define_route $ds '1.1/taxa/thumb' => { op => 'getThumb' };

define_route $ds '1.1/taxa/icon' => { op => 'getIcon' };

# Collections

define_directory $ds '1.1/colls' => { class => 'CollectionData',
				      output => 'basic' };

define_route $ds '1.1/colls/single' => { op => 'get', 
				         docresp => 'bin,ref,sref,loc,time,taxa,ent,crmod'};

define_route $ds '1.1/colls/list' => { op => 'list', 
				       docresp => 'bin,ref,sref,loc,time,taxa,ent,crmod' };

define_route $ds '1.1/colls/summary' => { op => 'summary', 
					  output => 'summary',
					  docresp => 'summary,ext,summary_time' };

# Occurrences

define_directory $ds '1.1/occs' => { class => 'OccurrenceData',
				     output => 'basic' };

define_route $ds '1.1/occs/single' => { op => 'get',
				        docresp => 'basic,coll,ref,geo,loc,time,ent,crmod' };

define_route $ds '1.1/occs/list' => { op => 'list',
				      docresp => 'basic,coll,ref,geo,loc,time,ent,crmod' };

# People

define_directory $ds '1.1/people' => { class => 'PersonData' };

define_route $ds '1.1/people/single' => { op => 'get' };

define_route $ds '1.1/people/list' => { op => 'list' };

# References

define_directory $ds '1.1/refs' => { class => 'ReferenceData' };

define_route $ds '1.1/refs/single' => { op => 'get' };

define_route $ds '1.1/refs/list' => { op => 'list' };



# Send app pages

get '/testapp/:filename' => sub {
    
    $DB::single = 1;
    my $filename = param "filename";
    return send_file("testapp/$filename", streaming => 1);
};


# Send style sheets

get qr{ ^ /data [\d.]* /css/(.*) }xs => sub {
    
    $DB::single = 1;
    my ($filename) = splat;
    send_file("css/$filename", streaming => 1);
};


# Any path starting with /data/... or just /data should display the list of
# available versions.

get qr{ ^ /data (?: / $ | / .* \. ( html | pod ) )? $ }xs => sub {
    
    my ($path, $suffix) = splat;
    
    $DB::single = 1;
    $ds->send_documentation( "/version_list.tt", { format => $suffix } );
};


# If the given URL asks for documentation, provide that as best we can.  If
# the given path does not correspond to any known documentation, we provide
# a page explaining what went wrong and providing the proper URLs.

# Any path that is not interpreted above might be a request for documentation.

get qr{ ^ /data ( \d+ \. \d+ / (?: [^/.]* / )* )
	  (?: ( index | \w+_doc ) \. ( html | pod ) )? $ }xs => sub {
	    
    my ($path, $last, $suffix) = splat;
    
    $DB::single = 1;
    $path .= $last unless !defined $last || $last eq 'index';
    $path =~ s{/$}{};
    $path =~ s{_doc}{};
    
    $ds->send_documentation( $path, { format => $suffix } );
};


get qr{ ^ /data ( \d+ \. \d+ (?: / [^/.]+ )* $ ) }xs => sub {
    
    my ($path) = splat;
    
    $DB::single = 1;
    
    $ds->send_documentation( $path, { format => 'html' } );
};


# Any path that ends in a suffix is a request for an operation.

get qr{ ^ /data ( \d+ \. \d+ / (?: [^/.]* / )* \w+ ) \. (\w+) }xs => sub {
    
    my ($path, $suffix) = splat;
    
    $DB::single = 1;
    
    # If the path ends in a number, replace it by 'single' and add the parameter
    # as 'id'.
    
    if ( $path =~ qr{ (\d+) $ }xs )
    {
	params->{id} = $1;
	$path =~ s{\d+$}{single};
    }
    
    $ds->execute_operation( $path, { format => $suffix } );
};



# get '/data1.1/config_doc.:ct' => sub {
    
#     sendDocumentation({ path => '1.1/config',
# 			file => '1.1/config_doc.tt',
# 			class => 'ConfigQuery',
# 		        output => ['single'] });
# };

# get '/data1.1/common_doc.:ct' => sub {

#     sendDocumentation({ params => '1.1:common_params', 
# 			file => '1.1/common_doc.tt',
# 			title => 'common parameters' });
# };

# Taxa

# get qr{/data1.1/taxa(?:|/|/index.html?)} => sub {

#     sendDocumentation({ class => 'TaxonQuery',
# 			path => '1.1/taxa/index' });
# };

# get '/data1.1/taxa/single.:ct' => sub {

#     querySingle({ class => 'TaxonQuery',
# 		  path => '1.1/taxa/single',
# 		  op => 'single' });
# };

# get '/data1.1/taxa/single_doc.:ct' => sub {
    
#     sendDocumentation({ class => 'TaxonQuery',
# 			path => '1.1/taxa/single',
# 		        output => ['single', 'nav'] });
# };

# get '/data1.1/taxa/list.:ct' => sub {

#     queryMultiple({ class => 'TaxonQuery',
# 		    path => '1.1/taxa/list',
# 		    op => 'list' });
# };

# get '/data1.1/taxa/all.:ct' => sub {

#     queryMultiple({ class => 'TaxonQuery',
# 		    path => '1.1/taxa/list',
# 		    op => 'list' });
# };

# get '/data1.1/taxa/hierarchy.:ct' => sub {

#     queryMultiple({ class => 'TaxonQuery',
# 		    path => '1.1/taxa/list',
# 		    op => 'hierarchy' });
# };

# get '/data1.1/taxa/:id.:ct' => sub {

#     querySingle({ class => 'TaxonQuery',
# 		  path => '1.1/taxa/single',
# 		  op => 'single' });
# };

# # Collections

# get '/data1.1/colls/single.:ct' => sub {
    
#     querySingle('CollectionQuery', v => '1.1',
# 		validation => '1.1/colls/single',
# 		op => 'single');
# };

# get '/data1.1/colls/list.:ct' => sub {

#     queryMultiple('CollectionQuery', v => '1.1',
# 		  validation => '1.1/colls/list',
# 		  op => 'list');
# };

# get '/data1.1/colls/list_doc.:ct' => sub {

#     sendDocumentation({ class => 'CollectionQuery',
# 			path => '1.1/colls/list',
# 			output => ['single', 'attr', 'ref', 'author', 'bin', 'formation'] });
    
# };

# get '/data1.1/colls/all.:ct' => sub {

#     queryMultiple('CollectionQuery', v => '1.1',
# 		  validation => '1.1/colls/list',
# 		  op => 'list');
# };

# get '/data1.1/colls/toprank.:ct' => sub {

#     queryMultiple('CollectionQuery', v => '1.1',
# 		  validation => '1.1/colls/toprank',
# 		  op => 'toprank');
# };

# get '/data1.1/colls/summary.:ct' => sub {

#     queryMultiple('CollectionQuery', v => '1.1',
# 		  validation => '1.1/colls/summary',
# 		  op => 'summary');
# };

# get '/data1.1/colls/:id.:ct' => sub {
    
#     returnErrorResult({}, "404 Not found") unless params('id') =~ /^[0-9]+$/;
#     querySingle('CollectionQuery', v => '1.1',
# 		validation => '1.1/colls/single',
# 		op => 'single');
# };

# get '/data1.1/intervals/list.:ct' => sub {

#     queryMultiple('IntervalQuery', v => '1.1',
# 		  validation => '1.1/intervals',
# 		  op => 'list');
# };

# get '/data1.1/intervals/hierarchy.:ct' => sub {
    
#     queryMultiple('IntervalQuery', v => '1.1',
# 		  validation => '1.1/intervals',
# 		  op => 'hierarchy');
# };

# get '/data1.1/people/list.:ct' => sub {
    
#     queryMultiple('PersonQuery', v => '1.1',
# 		  validation => '1.1/people/list', op => 'list');
# };

# get '/data1.1/people/single.:ct' => sub {

#     querySingle('PersonQuery', v => '1.1',
# 		  validation => '1.1/people/single', op => 'single');
# };

# get '/data1.1/people/:id.:ct' => sub {
    
#     returnErrorResult({}, "404 Not found") unless params('id') =~ /^[0-9]+$/;
#     querySingle('PersonQuery', v => '1.1',
# 		validation => '1.1/people/single', op => 'single');
# };

# Any other URL beginning with '/data1.1/' is an error.

get qr{(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    $ds->error_result("", "html", "404 The resource you requested was not found.");
};


1;


dance;


