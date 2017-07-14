#  
# ResourceData
# 
# A role that returns information from the PaleoDB database about a single
# educational resource or a category of educational resources.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ResourceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($RESOURCE_DATA $RESOURCE_QUEUE);

use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
use PB2::TableData qw(complete_output_block);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData);

our ($RESOURCE_ACTIVE, $RESOURCE_TAGS, $RESOURCE_IDFIELD);


sub initialize {

    my ($class, $ds) = @_;
    
    $ds->define_block('1.2:eduresources:basic' =>
	{ output => 'eduresource_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => 'eduresource_no', code => sub {
		    my ($request, $value) = @_;
		    return $value unless $request->{block_hash}{extids};
		    return generate_identifier('EDR', $value);
		} },
	{ output => 'status', com_name => 'sta' },
	    "If this educational resource record is displayed",
	    "on the appropriate website page, the status is C<B<active>>.",
	    "Otherwise, it will be C<B<pending>>.");
    
    $ds->define_ruleset('1.2:eduresources:specifier' =>
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "Return the educational resource record corresponding to the specified identifier");
    
    $ds->define_ruleset('1.2:eduresources:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all records in the database",
	    "will be returedn, subject to any other parameters that are also specified.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id', list => ',' },
	    "Return the educational resource record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'title', valid => ANY_VALUE },
	    "Return only records with the given word or phrase in the title",
	{ param => 'keyword', valid => ANY_VALUE, list => ',' },
	    "Return only records associated with the given keyword(s). You can",
	    "specify more than one, as a comma-separated list. Only records",
	    "with all of the listed keywords will be returned.");
    
    $ds->define_ruleset('1.2:eduresources:single' =>
	{ require => '1.2:eduresources:specifier' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the active version of the record is",
	    "returned if one exists, and a 'not found' error otherwise. If this parameter",
	    "is not included, or is included with the value C<B<no>>, then the master",
	    "version of the record is returned (if one exists).",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:list' =>
	{ require => '1.2:eduresources:selector' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the active version of the record is",
	    "returned if one exists, and a 'not found' error otherwise. If this parameter",
	    "is not included, or is included with the value C<B<no>>, then the master",
	    "version of the record is returned (if one exists).",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:active' =>
	{ allow => '1.2:eduresources:selector' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $RESOURCE_ACTIVE = $ds->config_value('eduresources_active');
    $RESOURCE_TAGS = $ds->config_value('eduresources_tags');
    $RESOURCE_IDFIELD = $ds->config_value('eduresources_idfield') || 'id';
    
    die "You must provide a configuration value for 'eduresources_active' and 'eduresources_tags'"
	unless $RESOURCE_ACTIVE && $RESOURCE_TAGS;

    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:eduresources:basic', $RESOURCE_QUEUE);
}


sub get_resource {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('eduresource_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    my $active = $request->clean_param('active');
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    my $tables = $request->tables_hash;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    # $$$
    
    # Determine the necessary joins.
    
    my $main_table = $active ? $RESOURCE_DATA : $RESOURCE_QUEUE;
    
    # my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT edr.* FROM $main_table as edr
        WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    # Return an error response if we could retrieve the record but the user is not authorized to
    # access it.  Any specimen not tied to an occurrence record is public by definition.
    
    # die $request->exception(403, "Access denied") 
    # 	unless $request->{main_record}{access_ok};
    
    return 1;
}


sub list_resources {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    my $active; $active = 1 if ($arg && $arg eq 'active') || $request->clean_param('active');
    
    my $main_table = $active ? $RESOURCE_ACTIVE : $RESOURCE_QUEUE;
    my $primary_key = $active ? $RESOURCE_IDFIELD : 'eduresource_no';
    
    # Generate a list of filter expressions.
    
    my @filters;
    
    if ( my @id_list = $request->safe_param_list('eduresource_id') )
    {
	my $id_string = join(',', @id_list);
	push @filters, "edr.$primary_key in ($id_string)";
    }
    
    if ( my $title = $request->clean_param('title') )
    {
	my $quoted = $dbh->quote("%${title}%");
	push @filters, "edr.title like $quoted";
    }
    
    if ( my @keywords = $request->clean_param_list('keywords') )
    {
	# This will need to be implemented later.
	push @filters, "edr.title like 'SELECT_NONE'";
    }
    
    # Make sure that we have either one filter expression or 'all_records' was selected or else
    # that we are listing active resources.
    
    unless ( @filters || $request->clean_param('all_records') || ($arg && $arg eq 'active') )
    {
	die $request->exception(400, "Bad request");
    }
    
    push @filters, "1=1" unless @filters;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    my $tables = $request->tables_hash;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Figure out what information we need to determine access permissions.
    
    # $$$
    
    # Determine the necessary joins.
    
    my $filter_string = join( q{ and }, @filters );
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc edr.* FROM $main_table as edr
        WHERE $filter_string
	GROUP BY edr.$primary_key $limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


1;
