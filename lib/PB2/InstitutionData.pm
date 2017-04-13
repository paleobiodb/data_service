# 
# InstitutionData.pm
# 
# A class that returns information from the PaleoDB database about a single
# institution or institutional collection, or a list of them.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::InstitutionData;

use HTTP::Validate qw(:validators);

use TableDefs qw($INSTITUTIONS $INST_COLLS $INST_ALTNAMES $INST_COLL_ALTNAMES);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use Taxonomy;

use Try::Tiny;
use Carp qw(carp croak);

use Moo::Role;



our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ConfigData);

# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $ds->define_output_map('1.2:institutions:basic_map' =>
	{ value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for this record.");
    
    $ds->define_block('1.2:institutions:basic' =>
	{ select => ['institution_no', 'institution_name', 'institution_url', 'institution_lsid',
		     'institution_code', 'last_updated'] },
		     # 'group_concat(distinct instaka.institution_code separator `|`) as other_codes',
		     # 'group_concat(distinct instaka.institution_name separator `|`) as other_names'] },
	{ output => 'institution_no', com_name => 'oid' },
	    "A unique identifier for this institution.",
	{ output => 'institution_code', com_name => 'cod' },
	    "The primary acronym or other code for this institution.",
	{ output => 'institution_name', com_name => 'nam' },
	    "The primary name of the institution.",
	{ output => 'other_codes', com_name => 'cd2' },
	    "Any other codes that are given for this institution are listed here.",
	{ output => 'other_names', com_name => 'aka' },
	    "Any other names that are given for this collection are listed here.",
	{ output => 'institution_url', com_name => 'url' },
	    "The main URL for this institution.",
	{ output => 'institution_lsid', com_name => 'lsi' },
	    "The institution's LSID, if any.",
	{ output => 'last_updated', com_name => 'dpd' },
	    "The date on which this information was last verified or updated.");
    
    $ds->define_output_map('1.2:instcolls:basic_map' => 
	{ value => 'loc', maps_to => '1.2:instcolls:loc' },
	    "The physical location of the institutional collection, as latitude and longitude.",
	{ value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for this record.");
    
    $ds->define_block('1.2:instcolls:basic' =>
	{ select => ['ic.instcoll_no', 'ic.institution_no', 'ic.instcoll_name', 'ic.instcoll_code',
		     'ic.instcoll_url'] },
	#	     'group_concat(distinct icaka.instcoll_code separator `|`) as other_codes',
	#	     'group_concat(distinct icaka.instcoll_name SEPARATOR `|`) as other_names'] },
	{ output => 'instcoll_no', com_name => 'oid' },
	    "A unique identifier for this institutional collection.",
	{ output => 'institution_no', com_name => 'iid' },
	    "The identifier of the primary institution with which this institutional",
	    "collection is associated (if known).",
	{ set => '*', code => \&process_ids },
	{ output => 'instcoll_code', com_name => 'cod' },
	    "An acronym or other code for this institutional collection.",
	{ output => 'instcoll_name', com_name => 'nam' },
	    "The primary name of this institutional collection.",
	{ output => 'other_codes', com_name => 'aco' },
	     "Any other codes that are known for this collection are listed here.",
	{ output => 'other_names', com_name => 'anm' },
	     "Any other names that are known for this collection are listed here.",
	{ output => 'instcoll_url', com_name => 'url' },
	     "The URL for this institutional collection, if known",
	{ output => 'institution_code', com_name => 'ico' },
	    "An acronym or other code for the institution, if any is known",
	{ output => 'institution_name', com_name => 'inm' },
	    "The primary name for the institution, if any is known",
	{ output => 'status', com_name => 'sta' },
	    "The status of this institutional collection. Outputs include:",
	    "=over", "=item active", "=item inactive",
	{ output => 'instcoll_url', com_name => 'url' },
	    "The main URL for this institutional collection.",
	{ output => 'catalog_url', com_name => 'urc' },
	    "The URL for this collection's catalog.",
	{ output => 'last_updated', com_name => 'dpd' },
	    "The date on which this information was last verified or updated.");
    
    # $ds->define_block('1.2:instcolls:inst' =>
    # 	{ select => ['inst.institution_code', 'inst.institution_name',
    # 		     'group_concat(distinct instaka.institution_code separator `|`) as other_codes',
    # 		     'group_concat(distinct instaka.institution_name separator `|`) as other_names'],
    # 	  tables => ['inst', 'instaka'] });
    
    $ds->define_block('1.2:instcolls:loc' =>
	{ select => ['ic.lon', 'ic.lat', 'ic.physical_address', 'ic.physical_city',
		     'ic.physical_state', 'ic.physical_country', 'ic.physical_cc'] },
	{ output => 'lng', com_name => 'lng' },
	    "The longitude at which this collection is located.",
	{ output => 'lat', com_name => 'lat' },
	    "The latitude at which this collection is located.",
	{ output => 'physical_address', com_name => 'pa1' },
	    "The physical address at which this collection is located.",
	{ output => 'physical_city', com_name => 'pa2' },
	{ output => 'physical_state', com_name => 'pa3' },
	{ output => 'physical_country', com_name => 'pa4' },
	{ output => 'physical_cc', com_name => 'pcc' });
    
    $ds->define_ruleset('1.2:instcolls:specifier' =>
	{ param => 'instcoll_id', valid => VALID_IDENTIFIER('ICO'), alias => 'id' },
	    "The identifier of the institutional collection record you wish to retrieve (REQUIRED).",
	    "You may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:institutions:specifier' =>
	{ param => 'institution_id', valid => VALID_IDENTIFIER('IST'), alias => 'id' },
	    "The identifier of the institution record you wish to retrieve (REQUIRED).",
	    "You may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:instcolls:selector' =>
	{ param => 'instcoll_id', valid => VALID_IDENTIFIER('IN2'), alias => ['id', 'institution_id'], list => ',' },
	    "A comma-separated list of institution or institutional collection identifiers.",
	    "Records identified by these identifiers are selected, provided they satisfy any",
	    "other parameters. You may instead use the parameter name B<C<id>>.",
	{ param => 'code', valid => ANY_VALUE, list => ',' },
	    "An acronym associated with an institution, i.e. 'AMNH'. Some acronyms",
	    "may correspond to several different institutions, and all matching records",
	    "will be returned. You can specify more than one, as a comma-separated list.",
	    "You can also use C<%> and C<_> as wildcards.",
	{ allow => '1.2:common:select_updated' });
    
    $ds->define_ruleset('1.2:instcolls:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all institution and institutional collection records entered in the database,",
	    "subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    $ds->define_ruleset('1.2:instcolls:single' =>
	"The following parameter selects a record to retrieve:",
    	{ require => '1.2:instcolls:specifier', 
	  error => "you must specify an institutional collection identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameter to specify any additional information you wish to retrieve:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:instcolls:basic_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:instcolls:list' =>
	"You can use the following parameter if you wish to retrieve the entire set of",
	"institutional collection records entered in this database.  Please use this with care, since the",
	"result set will contain more than 8,000 records.",
    	{ allow => '1.2:instcolls:all_records' },
	">>The following parameters can be used to query for specimens by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:instcolls:selector' },
	">>You may also use the following parameter to specify any additional information you wish to retrieve:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:instcolls:basic_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:institutions:single' =>
	{ require => '1.2:institutions:specifier',
	  error => "you mus tspecify an institution identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameter to specify any additional information you wish to retrieve:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:instcolls:basic_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

}


# get_instcoll ( )
# 
# This data service operation returns information about a single institutional collection or institution,
# specified by identifier.

sub get_instcoll {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('instcoll_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'ic', cd => 'ic' );
    
    my $tables = $request->tables_hash;
    
    my $fields = $request->select_string;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('ic', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $INST_COLLS as ic LEFT JOIN $INSTITUTIONS as inst using (institution_no)
	#	LEFT JOIN $INST_COLL_ALTNAMES as icaka using (instcoll_no)
        WHERE ic.instcoll_no = $id
	GROUP BY ic.instcoll_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    # Return an error response if we could retrieve the record but the user is not authorized to
    # access it.  Any specimen not tied to an occurrence record is public by definition.
    
    die $request->exception(403, "Access denied") 
	unless $request->{main_record}{access_ok} || ! $request->{main_record}{occurrence_no};
    
    return 1;
}


# get_inst ( )
# 
# This data service operation returns information about a single institution, specified by
# identifier.

sub get_institution {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('institution_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'inst', cd => 'inst' );
    
    my $tables = $request->tables_hash;
    
    my $fields = $request->select_string;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('inst', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $INSTITUTIONS as inst
	#	LEFT JOIN $INST_COLL_ALTNAMES as icaka using (instcoll_no)
        WHERE inst.institution_no = $id
	GROUP BY inst.institution_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    # Return an error response if we could retrieve the record but the user is not authorized to
    # access it.  Any specimen not tied to an occurrence record is public by definition.
    
    die $request->exception(403, "Access denied") 
	unless $request->{main_record}{access_ok} || ! $request->{main_record}{occurrence_no};
    
    return 1;
}


# list_inst_colls ( )
# 
# This operation lists institutional collections based on the parameters given in the request.

sub list_instcolls {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'ic', cd => 'ic' );
    
    my @filters = $request->generate_instcoll_filters('list', 'c', $tables);
    push @filters, $request->generate_common_filters( { instcoll => 'ic', inst => 'inst', bare => 'ic' } );
    
    if ( my @ids = $request->clean_param_list('instcoll_id') )
    {
	my (@inst_ids, @coll_ids, @id_filters);
	
	if ( $request->{raw_params} && $request->{raw_params}{institution_id} )
	{
	    @inst_ids = map { $dbh->quote($_) } @ids;
	}
	
	else
	{
	    foreach my $i ( @ids )
	    {
		if ( ref $i && $i->{type} eq $ExternalIdent::IDP{IST} )
		{
		    push @inst_ids, $dbh->quote($i);
		}
		
		else
		{
		    push @coll_ids, $dbh->quote($i);
		}
	    }
	}
	
	if ( @inst_ids )
	{
	    my $id_list = join(',', @inst_ids);
	    push @id_filters, "ic.institution_no in ($id_list)";
	}
	
	if ( @coll_ids )
	{
	    my $id_list = join(',', @coll_ids);
	    push @id_filters, "ic.instcoll_no in ($id_list)";
	}
	
	push @id_filters, "1" unless @id_filters;
	
	push @filters, '(' . join(' or ', @id_filters) . ')';
    }
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
    }
    
    push @filters, "1" unless @filters;
    
    my $filter_string = join(' and ', @filters);
    
    my $fields = $request->select_string;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('ic', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $INST_COLLS as ic LEFT JOIN $INSTITUTIONS as inst using (institution_no)
	#	LEFT JOIN $INST_COLL_ALTNAMES as icaka using (instcoll_no)
        WHERE $filter_string
	GROUP BY ic.instcoll_no
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub list_institutions {
    
    my ($request) = @_;
    
    
}


sub generate_instcoll_filters {

    my ($request) = @_;
    
    my @filters;
    my $dbh = $request->get_connection;
    
    if ( my @raw_codes = $request->clean_param_list('code') )
    {
	my (@codes, @bad, @code_filters);
	
	foreach my $c ( @raw_codes )
	{
	    next unless $c;
	    
	    if ( $c =~ qr{ ^ [a-z]+ $ }xsi )
	    {
		push @codes, $dbh->quote($c);
	    }
	    
	    elsif ( $c =~ qr{ ^ [a-z%_-]+ $ }xsi )
	    {
		my $quoted = $dbh->quote($c);
		push @code_filters, "ic.instcoll_code like $quoted";
		push @code_filters, "inst.institution_code like $quoted";
	    }
	    
	    else
	    {
		push @bad, $dbh->quote($c);
	    }
	}
	
	if ( @codes )
	{
	    my $code_string = join(',', @codes);
	    push @code_filters, "ic.instcoll_code in ($code_string)";
	    push @code_filters, "inst.institution_code in ($code_string)";
	}
	
	if ( @bad )
	{
	    my $bad_string = join(', ', @bad);
	    $request->add_warning("invalid code: $bad_string");
	}
	
	if ( @code_filters )
	{
	    push @filters, "(" . join(' or ', @code_filters) . ")";
	}

	else
	{
	    push @filters, "ic.instcoll_code = 'SELECT_NOTHING'";
	    $request->add_warning("no valid codes were given");
	}
    }
    
    return @filters;
}



sub generate_join_list {
    
    my ($request, $tables_ref) = @_;
    
    my $joins = "";
    
    return $joins;
}


sub process_ids {

    my ($request, $record) = @_;
    
    $record->{institution_no} = '' if defined $record->{institution_no} && $record->{institution_no} eq '0';
    
    return unless $request->{block_hash}{extids};
    
    foreach my $f ( qw(instcoll_no) )
    {
	$record->{$f} = generate_identifier('ICO', $record->{$f}) if defined $record->{$f} && $record->{$f} ne '';
    }
    
    foreach my $f ( qw(institution_no) )
    {
	$record->{$f} = generate_identifier('IST', $record->{$f}) if defined $record->{$f} && $record->{$f} ne '';
    }
}

1;
