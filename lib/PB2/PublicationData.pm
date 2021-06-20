#  
# PublicationData
# 
# A role that returns information from the PaleoDB database about a single
# official research publication or a category of official research publications.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::PublicationData;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
use TableData qw(complete_output_block);

use Carp qw(carp croak);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::CommonData);


sub initialize {

    my ($class, $ds) = @_;
    
    # Optional output
    
    $ds->define_output_map('1.2:pubs:optional_output' =>
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Output blocks
    
    $ds->define_block('1.2:pubs:basic' =>
	{ output => 'pub_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_record},
	{ output => '_label', com_name => 'rlb' },
	    "For all data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'status', com_name => 'sta' },
	    "In the output of record entry operations, each deleted record",
	    "will have the value C<'deleted'> in this field.");
    
    # Rulesets
    
    $ds->define_ruleset('1.2:pubs:specifier' =>
	{ param => 'pub_id', valid => VALID_IDENTIFIER('PUB'), alias => ['id', 'pub_no'] },
	    "Return the official publication record corresponding to the specified identifier");
    
    $ds->define_ruleset('1.2:pubs:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all official publication records in the database",
	    "will be returned, subject to any other parameters that are also specified.",
	{ param => 'pub_id', valid => VALID_IDENTIFIER('PUB'), alias => 'id', list => ',' },
	    "Return the official publiation record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'title', valid => ANY_VALUE, alias => 'ref_title' },
	    "Return only records with the given word or phrase in the title.",
	    "You can use C<%> and C<_> as wildcards, but you must include at least",
	    "one letter.",
	{ param => 'author', valid => ANY_VALUE, alias => 'ref_author', list => ',' },
	    "Return only records where any of the specified names appear",
	    "in the author field.",
	{ param => 'primary', valid => ANY_VALUE, alias => 'ref_primary', list => ',' },
	    "Return only records where any of the specified names appear in",
	    "the first position in the author field.",
	{ param => 'journal', valid => ANY_VALUE, alias => 'pub_title', list => ',' },
	    "Return only records for which the journal field matches the given",
	    "word or phrase");
    
    $ds->define_ruleset('1.2:pubs:single' =>
	{ require => '1.2:pubs:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:pubs:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:pubs:list' =>
	{ require => '1.2:pubs:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:pubs:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:pubs:basic', 'PUBLICATIONS');
}


sub get_publication {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('pub_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Delete unnecessary output fields.
    
    $request->delete_output_field('_label');
    $request->delete_output_field('status');
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Generate the main query.
    
    my $pub_id = $dbh->quote($id);
    
    $request->{main_sql} = "
	SELECT pub.* FROM $TABLE{PUBLICATIONS} as pub
        WHERE pub_no = $pub_id";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    return 1;
}


sub list_publications {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Generate a list of filter expressions.
    
    my @filters;
    
    if ( my @id_list = $request->safe_param_list('pub_id') )
    {
	my $id_string = join(',', @id_list);
	push @filters, "pub.pub_no in ($id_string)";
    }
    
    if ( my $title = $request->clean_param('title') )
    {
	my $quoted = $dbh->quote($title);
	push @filters, "pub.title like $quoted";
    }
    
    if ( my $journal = $request->clean_param('journal') )
    {
	my $quoted = $dbh->quote($journal);
	push @filters, "pub.journal like $quoted";
    }

    # We require either at least one filter or the 'all_records' parameter.
    
    unless ( @filters || $request->clean_param('all_records') )
    {
	die $request->exception(400, "Bad request");
    }
    
    push @filters, "1=1" unless @filters;
    
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
    
    my $filter_string = join( ' and ', @filters );
    
    # my $extra_fields = $request->select_string;
    # $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc pub.* FROM $TABLE{PUBLICATIONS} as pub
        WHERE $filter_string
	ORDER BY pub.pub_no desc $limit";
    
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
    
    # If we have a record label hash, fill in those values.
    
    if ( $request->{my_record_label} && $record->{pub_no} )
    {
	if ( my $label = $request->{my_record_label}{$record->{pub_no}} )
	{
	    $record->{_label} = $label;
	}
    }
    
    # Generate the proper external identifiers.
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{pub_no} = generate_identifier('PUB', $record->{pub_no})
	    if $record->{pub_no};
    }
}


1;
