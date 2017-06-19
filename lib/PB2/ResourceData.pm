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
    
    $ds->define_ruleset('1.2:eduresources:single' =>
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "Return the educational resource record corresponding to the specified identifier",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
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
    
    # my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	(SELECT edr.*, 'active' as status FROM $RESOURCE_DATA as edr
        WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no)
	UNION
	(SELECT edr.*, 'pending' as status FROM $RESOURCE_QUEUE as edr
	WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no)
	LIMIT 1";
    
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


1;
