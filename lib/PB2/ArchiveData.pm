#  
# ArchiveData
# 
# A role that returns information from the PaleoDB database about a single
# data archive or a category of data archives.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ArchiveData;

use HTTP::Validate qw(:validators);

use TableDefs;
use ExternalIdent qw(%IDRE VALID_IDENTIFIER generate_identifier);
# use TableData qw(complete_output_block);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData);


sub initialize {

    my ($class, $ds) = @_;
    
    # Value sets
    
    $ds->define_set('1.2:archives:status' =>
	{ value => 'complete' },
	    "The archive has been properly constructed and is ready for use.",
	{ value => 'deleted' },
	    "The archive has been deleted, and is no longer accessible.",
	{ value => 'fail' },
	    "An error occurred during the construction of the archive.",
	{ value => 'loading' },
	    "The archive is in the process of being constructed. This status",
	    "should only exist for a few seconds, and should then automatically",
	    "be replaced by C<complete> or C<fail>.");
    
    $ds->define_set('1.2:archives:enterer' =>
	{ value => 'me' },
	    "Select only records that were entered by the current requestor.",
	{ value => 'auth' },
	    "Select only records that were entered by the current requestor",
	    "or one of their authorized enterers.");
    
    # Optional output
    
    $ds->define_output_map('1.2:archives:output_map' =>
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Output blocks
    
    $ds->define_block('1.2:archives:basic' =>
	{ output => 'archive_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_record},
	{ output => '_label', com_name => 'rlb' },
	    "For all data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'status', com_name => 'sta' },
	    "The status will be one of the following codes:",
	    $ds->document_set('1.2:archives:status'),
	{ output => 'title', com_name => 'tit' },
	    "The title assigned to this data achive by its creator",
	{ output => 'authors', com_name => 'oau' },
	    "The list of author names assigned to this data archive by its creator",
	{ output => 'description', com_name => 'dsc' },
	    "The description text written by the data archive's creator",
	{ output => 'doi', com_name => 'doi' },
	    "The DOI (if any) assigned to this data archive.",
	{ output => 'orcid', com_name => 'orc' },
	    "A list of one or more ORCIDs associated with this archive.",
	    "These should in most cases correspond to the listed authors.",
	{ output => 'uri', com_name => 'uri' },
	    "The URI of the API call that was used to generate this",
	    "data archive.",
	{ output => 'fetched', com_name => 'dft' },
	    "The date and time at which the API call used to generate this",
	    "archive was made.");
    
    # Rulesets
    
    $ds->define_ruleset('1.2:archives:specifier' =>
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => 'id' },
	    "Return the data archive record corresponding to the specified identifier",
	{ param => 'doi', valid => ANY_VALUE },
	    "Return the data archive record corresponding to the specified DOI.",
	{ at_most_one => ['archive_id', 'doi'] });
    
    $ds->define_ruleset('1.2:archives:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all records in the database",
	    "will be returned, subject to any other parameters that are also specified.",
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => 'id', list => ',' },
	    "Return the data archive record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'status', valid => '1.2:archives:status', list => ',' },
	    "Return only data archive records with the specified status or statuses.",
	{ param => 'doi', valid => ANY_VALUE, list => ',' },
	    "Return only data archive records associated with one of the specified DOIs.",
	    "You may specify more than one, as a comma-separated list.",
	{ param => 'enterer', valid => ANY_VALUE, list => ',' },
	    "If this parameter is specified, then only data archives created by the specified",
	    "person are shown. Only archives that have a DOI assigned to them are viewable",
	    "by anyone except their creator and his or her authorizer group, and the",
	    "database administrators. The value of this parameter may be the",
	    "identifier(s) of one or more database contributors, or else either of the following values:",
	{ param => 'search_re', valid => ANY_VALUE },
	    "Return only records for which the specified regular expression matches",
	    "either the title or the description.");
    
    $ds->define_ruleset('1.2:archives:single' =>
	{ require => '1.2:archives:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:output_map' },
	    "Include one or more of the following optional output blocks in the result:",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:archives:list' =>
	{ require => '1.2:archives:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:output_map' },
	    "Include one or more of the following optional output blocks in the result:",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # my $dbh = $ds->get_connection;
    
    # complete_output_block($ds, $dbh, '1.2:archives:basic', 'DATA_ARCHIVES');
}


sub get_archive {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number or DOI.

    my $filter;
    my $session_id = '';
    my $authorizer_no;
    my $show_notfound;
    
    if ( my $id = $request->clean_param('archive_id') )
    {
	die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
	$filter = "dar.archive_no = '$id'";
	
	if ( my $cookie_id = Dancer::cookie('session_id') )
	{
	    $session_id = $dbh->quote($cookie_id);
	    
	    my $sql = "
		SELECT authorizer_no, enterer_no FROM session_data
		WHERE session_id = $session_id";
	    
	    ($authorizer_no) = $dbh->selectrow_array($sql);

	    $show_notfound = 1 if $authorizer_no;
	}
	
	unless ( $authorizer_no )
	{
	    $filter .= " and (dar.doi <> '' or dar.is_public)";
	}
    }
    
    elsif ( my $doi = $request->clean_param('doi') )
    {
	my $quoted = $dbh->quote($doi);
	$filter = "dar.doi = $quoted and dar.doi <> ''";
	$show_notfound = 1;
    }
    
    # print STDERR "session_id = $session_id,  authorizer_no = $authorizer_no\n";
    
    # Delete unnecessary output fields.
    
    $request->delete_output_field('_label');
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( cd => 'dar' );
    
    my $tables = $request->tables_hash;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list;
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT dar.* $extra_fields FROM data_archives as dar
		$join_list
        WHERE $filter";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.

    unless ( $request->{main_record} )
    {
	if ( $show_notfound )
	{
	    die "404 Not found\n";
	}

	else
	{
	    die "403 Permission denied\n";
	}
    }
    
    return 1;
}


sub list_archives {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Generate a list of filter expressions.
    
    my @filters;
    
    if ( my @id_list = $request->safe_param_list('archive_id') )
    {
	my $id_string = join(',', @id_list);

	if ( $id_string && $id_string =~ /^[\d\s,]+$/ )
	{
	    push @filters, "dar.archive_no in ($id_string)";
	}

	else
	{
	    push @filters, "dar.archive_no = 0";
	    $request->add_warning("bad value '$id_string' for 'archive_no'");
	}
    }
    
    if ( my @status_list = $request->safe_param_list('status', 'SELECT_NONE') )
    {
	my $status_string = join("','", @status_list);
	push @filters, "dar.status in ('$status_string')";
    }

    if ( my $search_re = $request->clean_param('search_re') )
    {
	my $quoted = $dbh->quote($search_re);
	push @filters, "(dar.title rlike $quoted or dar.description rlike $quoted)";
    }

    if ( my @doi = $request->clean_param_list('doi') )
    {
	my @list;
	
	foreach my $d ( @doi )
	{
	    push @list, $dbh->quote($d) if $d;
	}

	push @list, "'SELECT_NONE'" unless @list;

	my $doi_string = join(',', @list);

	push @filters, "dar.doi in ($doi_string)";
    }
    
    push @filters, $request->generate_enterer_filters;
    
    # Make sure that we have either one filter expression or 'all_records' was selected.
    
    unless ( @filters || $request->clean_param('all_records') )
    {
	$request->add_warning("you must include the parameter 'all_records' unless you specify another filtering parameter");
	die $request->exception(400, "Bad request");
    }
    
    push @filters, "1=1" unless @filters;
    
    # Delete unnecessary output fields.
    
    $request->delete_output_field('_label');
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'dat', cd => 'dar' );
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list;
    
    my $filter_string = join( q{ and }, @filters );
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc dar.* $extra_fields
	FROM data_archives as dar
		$join_list
        WHERE $filter_string $limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub generate_enterer_filters {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # If an 'enterer' parameter was given, we first have to check if this API call was made by a
    # logged-in user. If not, then only publicly viewable records will be returned. Those are ones
    # which either have a DOI or have the is_public flag set.

    my @filters;
    my ($authorizer_no, $enterer_no);
    
    if ( my $cookie_id = Dancer::cookie('session_id') )
    {
	my $session_id = $dbh->quote($cookie_id);
	
	my $sql = "
		SELECT authorizer_no, enterer_no FROM session_data
		WHERE session_id = $session_id";
	
	($authorizer_no, $enterer_no) = $dbh->selectrow_array($sql);
    }
    
    unless ( $authorizer_no )
    {
	push @filters, "(dar.doi <> '' or dar.is_public)";
    }
    
    # If no 'enterer' parameter was given, we do not filter.
    
    my @enterers = $request->clean_param_list('enterer');
    
    if ( @enterers )
    {
	my (@ef, @eid, @bad);

	foreach my $e ( @enterers )
	{
	    if ( $e eq 'me' )
	    {
		$enterer_no ||= 0;
		push @ef, "dar.enterer_no = '$enterer_no'";
	    }

	    elsif ( $e eq 'auth' )
	    {
		$authorizer_no ||= 0;
		push @ef, "dar.authorizer_no = '$authorizer_no'";
	    }
	    
	    elsif ( $e =~ $IDRE{PRS} )
	    {
		push @eid, $2;
	    }

	    elsif ( $e =~ /^(\d+)$/ )
	    {
		push @eid, $1;
	    }
	    
	    else
	    {
		push @bad, $e;
	    }
	}

	if ( @bad )
	{
	    my $bad_list = join("', '", @bad);
	    $request->add_warning("Field 'enterers': bad values '$bad_list'");
	}

	if ( @eid )
	{
	    my $good_list = join("', '", @eid);
	    push @ef, "dar.enterer_no in ('$good_list')";
	}

	if ( @ef )
	{
	    push @filters, @ef;
	}

	else
	{
	    push @filters, "dar.enterer_no = 'SELECT_NONE'";
	}
    }
    
    return @filters;
}


sub generate_join_list {
    
    my ($request) = @_;
    
    my $tables = $request->tables_hash;
    
    my $join_list = '';
    
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = dar.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = dar.enterer_no\n"
	if $tables->{ppe};
    $join_list .= "LEFT JOIN person as ppm on ppm.person_no = dar.modifier_no\n"
	if $tables->{ppm};
    
    return $join_list;
}


# process_record ( request, record )
# 
# This procedure is called automatically for each record that is expressed via the output block
# '1.2:archives:basic'. It cleans up some of the data fields.

sub process_record {
    
    my ($request, $record) = @_;
    
    # If we have a record label hash, fill in those values.
    
    if ( my $label = $request->{my_record_label}{$record->{archive_no}} )
    {
	$record->{_label} = $label;
    }
    
    # Generate the proper external identifiers.
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{archive_no} = generate_identifier('DAR', $record->{archive_no})
	    if $record->{archive_no};
    }
    
    # Assemble the URI field from uri_path and uri_args
    
    my $uri = $record->{uri_path} || '';
    
    if ( $record->{uri_path} && $record->{uri_args} )
    {
	$uri .= "?$record->{uri_args}";
    }

    $record->{uri} = $uri;
}


1;
