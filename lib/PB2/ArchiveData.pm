#  
# ArchiveData
# 
# A role that returns information from the PaleoDB database about a single
# official research publication or a category of official research publications.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ArchiveData;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
use TableData qw(complete_output_block);

use Carp qw(carp croak);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::Authentication);


sub initialize {

    my ($class, $ds) = @_;
    
    # Optional output
    
    $ds->define_output_map('1.2:archives:optional_output' =>
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the data archive record");
    
    # Output blocks
    
    $ds->define_block('1.2:archives:basic' =>
	{ output => 'archive_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_record},
	{ output => '_label', com_name => 'rlb' },
	    "For all data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'status', com_name => 'sta' },
	    "The status of this archive record.",
	    "In the output of record entry operations, each deleted record",
	    "will have the value C<'deleted'> in this field.",
	{ output => 'permissions', com_name => 'prm' },
	    "This field will be non-empty if the user making this request",
	    "has edit permission on this record.");
    
    # Rulesets
    
    $ds->define_ruleset('1.2:archives:specifier' =>
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => ['id', 'archive_no'] },
	    "Return the data archive record corresponding to the specified identifier",
	{ param => 'doi', valid => ANY_VALUE },
	    "Return the data archive record with the specified DOI, if one exists.");
    
    $ds->define_ruleset('1.2:archives:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all data archive records in the database",
	    "will be returned, subject to any other parameters that are also specified.",
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => ['id', 'archive_no' ], 
	  list => ',' },
	    "Return the data archive record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'public', valid => BOOLEAN_VALUE },
	    "If a true value is specified, return only data archive records marked with the",
	    "is_public attribute. If false, return only data archive records no so marked.",
	{ param => 'enterer', valid => ANY_VALUE, alias => 'entered_by' },
	    "Return only data archive records entered by the specified database member.",
	{ param => 'authorizer', valid => ANY_VALUE, alias => 'authorized_by' },
	    "Return only data archive records authorized by the specified database member.",
	{ param => 'title', valid => ANY_VALUE, list => ',' },
	    "Return only records with the given word(s) or phrase(s) in the title.",
	    "You can specify more than one, separated by commas.",
	{ param => 'author', valid => ANY_VALUE, alias => 'ref_author', list => ',' },
	    "Return only records where any of the specified names appear",
	    "in the authors field.");
    
    $ds->define_ruleset('1.2:archives:single' =>
	{ require => '1.2:archives:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:archives:list' =>
	{ require => '1.2:archives:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:archives:public' =>
	{ require => '1.2:archives:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:optional_output' },
    	{ allow => '1.2:special_params' },
			"^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:archives:retrieve' =>
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => ['id', 'archive_no'] },
	"Return the content of the data archive corresponding to the specified identifier",
	">No other parameters are accepted by this request.");
    
    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:archives:basic', 'ARCHIVES');
}


sub get_archive {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $perms = $request->authenticate;

    my $query_string;
    
    # Make sure we have a valid id number.
    
    if ( my $id = $request->clean_param('archive_id') )
    {
	die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
	$query_string = "archive_no = $id";
    }
    
    elsif ( my $doi = $request->clean_param('doi') )
    {
	$query_string = "doi = " . $dbh->quote($doi);
    }
    
    else
    {
	die "400 Bad request, no identifier given";
    }
    
    # Delete unnecessary output fields.
    
    $request->delete_output_field('_label');
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT arch.* FROM $TABLE{ARCHIVES} as arch
        WHERE $query_string LIMIT 1";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    unless ( $request->{main_record} )
    {
	die $request->exception('404', "Not found");
    }
    
    # Return an error response if we aren't authorized to view the record.

    unless ( $request->{main_record}{is_public} )
    {
	my $archive_no = $request->{main_record}{archive_no};
	my $perms = $request->require_authentication('ARCHIVES');

	my $p = $perms->check_record_permission('ARCHIVES', 'view', "archive_no=$archive_no",
						$request->{main_record});
	
	unless ( $p =~ /view|admin/ )
	{
	    die $request->exception('401', "Permission denied");
	}
    }
    
    return 1;
}


sub retrieve_archive {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('archive_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Get the metadata record for this archive. If there isn't one, return a 404 Not Found error.
    
    my $archive_id = $dbh->quote($id);
    
    my $sql = "
	SELECT arch.* FROM $TABLE{ARCHIVES} as arch
        WHERE archive_no = $archive_id";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my ($record) = $dbh->selectrow_hashref($sql) ||
	die $request->exception('404', "Not found");	
    
    # If this archive is public, then its data can be retrieved. Otherwise, only logged-in users
    # with the proper permissions can retrieve it.
    
    unless ( $record->{is_public} )
    {
	my $perms = $request->require_authentication('ARCHIVES');

	my $p = $perms->check_record_permission('ARCHIVES', 'view', "archive_no=$id", $record);
	
	unless ( $p =~ /view|admin/ )
	{
	    die $request->exception('401', "Permission denied");
	}
    }
    
    # If the actual archive file is missing or not readable, we return a 500 error rather than a
    # 404. The presence of the archive metadata record means that a missing file is an internal
    # error rather than a resource not found.
    
    my $filename;
    my $encoding;
    
    if ( -r "/var/paleomacro/archives/$id.gz" )
    {
	$filename = "/var/paleomacro/archives/$id.gz";
	$encoding = "gzip";
    }
    
    elsif ( -r "/var/paleomacro/archives/$id.bz2" )
    {
	$filename = "/var/paleomacro/archives/$id.bz2";
	$encoding = "application/x-bzip2";
    }
    
    elsif ( -r "/var/paleomacro/archives/$id" )
    {
	$filename = "/var/paleomacro/archives/$id";
	$encoding = undef;
    }
    
    else
    {
	print STDERR "ERROR: missing or unreadable archive file /var/paleomacro/archives/$id.gz|bz2\n";
	die $request->exception('500', "Missing or unreadable archive data file");
    }
    
    # If we get here, then we can send the file. We set the content type and disposition from the
    # URI path. If that is somehow missing, then we return a 500 error.
    
    my $content_type;
    my $suffix;
    
    if ( $record->{uri_path} =~ / [.] (\w+) $ /xs )
    {
	$suffix = $1;
	$content_type = $request->{ds}{format}{$1}{content_type};
    }

    unless ( $content_type )
    {
	print STDERR "ERROR: cannot determine content type for archive file $id: $record->{uri_path}\n";
	die $request->exception('500', "Cannot determine content type for archive data file");
    }
    
    $request->file_result($filename, system_path => 1, 
			  content_type => $content_type,
			  content_encoding => $encoding,
			  content_disposition => "attachment; filename=pbdb_archive_$id.$suffix");
}


sub list_archives {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $perms = $request->authenticate;
    
    # Generate a list of filter expressions.
    
    my (@filter, @auth_filter);
    
    if ( my @id_list = $request->safe_param_list('archive_id') )
    {
	my $id_string = join(',', @id_list);
	push @filter, "arch.archive_no in ($id_string)";
    }
    
    if ( my @titles = $request->clean_param_list('title') )
    {
	my @title_filter;
	
	foreach my $value ( @titles )
	{
	    if ( $value =~ /\w/ )
	    {
		$value =~ s/[.]/.*/g;
		my $quoted = $dbh->quote($value);
		push @title_filter, "arch.title rlike $quoted";
	    }
	}
	
	if ( @title_filter )
	{
	    push @filter, '(' . join(' or ', @title_filter) . ')';
	}
    }
    
    if ( my @authors = $request->clean_param_list('author') )
    {
	my @author_filter;
	
	foreach my $value ( @authors )
	{
	    if ( $value =~ /\w/ )
	    {
		$value =~ s/[.]/.*/g;
		my $quoted = $dbh->quote($value);
		push @author_filter, "arch.authors rlike $quoted";
	    }
	}
	
	if ( @author_filter )
	{
	    push @filter, '(' . join(' or ', @author_filter) . ')';
	}
    }
    
    my $public = $arg eq 'public' || $request->clean_param_boolean('public');
    my $enterer = $request->clean_param('enterer');
    my $authorizer = $request->clean_param('authorizer');
    
    if ( $public || $perms->{enterer_no} eq '0' )
    {
	push @filter, "is_public";
    }
    
    else
    {
	my $enterer_no = $perms->{enterer_no};
	my $authorizer_no = $perms->{authorizer_no};
	
	unless ( $perms->{is_superuser} )
	{
	    push @auth_filter, "(is_public or enterer_no = $enterer_no or authorizer_no = $authorizer_no)";
	}
    }
    
    if ( defined $public && $public eq '0' )
    {
	push @filter, "not is_public";
    }
    
    if ( $enterer eq 'me' )
    {
	my $me = $perms->{enterer_no};
	push @filter, "enterer_no = $me";
    }
    
    elsif ( $enterer eq 'auth' )
    {
	my $authorizer_no = $perms->{authorizer_no};
	push @filter, "authorizer_no = $authorizer_no";
    }
    
    elsif ( $enterer )
    {
	push @filter, "enterer_no = $enterer";
    }

    if ( $authorizer eq 'me' )
    {
	my $me = $perms->{enterer_no};
	push @filter, "(authorizer_no = $me or enterer_no = $me)";
    }
    
    elsif ( $authorizer )
    {
	push @filter, "authorizer_no = $authorizer";
    }
    
    # We require either at least one filter or the 'all_records' parameter.
    
    unless ( @filter || $request->clean_param('all_records') )
    {
	die $request->exception(400, "Bad request");
    }
    
    push @filter, @auth_filter;
    
    push @filter, "1=1" unless @filter;
    
    # Delete unnecessary output fields.
    
    $request->delete_output_field('_label');
    $request->delete_output_field('status');
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list($tables, $active);
    
    my $filter_string = join( ' and ', @filter );
    
    # my $extra_fields = $request->select_string;
    # $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc arch.* FROM $TABLE{ARCHIVES} as arch
        WHERE $filter_string
	ORDER BY arch.archive_no desc $limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# process_record ( request, record )
# 
# This procedure is called automatically for each record that is expressed via the output block
# '1.2:pubs:basic'. It cleans up some of the data fields.

sub process_record {
    
    my ($request, $record) = @_;
    
    # Fill in edit permissions if the user is logged in.
    
    if ( $request->{my_perms} && $request->{my_perms}{enterer_no} )
    {
	$record->{permissions} =
	    $request->{my_perms}->check_record_permission('ARCHIVES',
							  'edit',
							  "archive_no=$record->{archive_no}",
							  $record);
    }
    
    # If we have a record label hash, fill in those values.
    
    if ( $request->{my_record_label} && $record->{archive_no} )
    {
	if ( my $label = $request->{my_record_label}{$record->{archive_no}} )
	{
	    $record->{_label} = $label;
	}
    }
    
    # Generate the proper external identifiers.
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{archive_no} = generate_identifier('DAR', $record->{archive_no})
	    if $record->{archive_no};
    }
}


1;
