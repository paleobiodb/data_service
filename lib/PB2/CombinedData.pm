# 
# CombinedData.pm
# 
# This class implements a combined auto-complete operation for the data service. This is
# designed for use with web applications such as Navigator.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::CombinedData;

use HTTP::Validate qw(:validators);

use PB2::IntervalData;
use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_LITH $COLL_STRATA $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $PVL_MATRIX);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Try::Tiny;
use Carp qw(carp croak);

use Moo::Role;

no warnings 'numeric';


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::IntervalData PB2::CollectionData PB2::TaxonData PB2::PersonData);

our ($MAX_BIN_LEVEL) = 0;
our (%COUNTRY_NAME, %CONTINENT_NAME);
our (%ETVALUE, %EZVALUE);
our (%LITH_VALUE, %LITHTYPE_VALUE);


# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $ds->define_block('1.2:combined:auto' =>
	# common fields
	{ output => 'record_id', com_name => 'oid' },
	    "The identifier (if any) of the database record corresponding to this name.",
	{ output => 'record_type', com_name => 'typ' },
	    "The type of this record: varies by record. Will be one of: C<B<str>>, C<B<txn>>, C<B<prs>>, C<B<int>>.",
	{ output => 'name', com_name => 'nam' },
	    "A name that matches the characters given by the B<C<name>> parameter.",
	# taxa
	{ output => 'taxon_no', com_name => 'vid', dedup => 'record_id' },
	    "For taxonomic names: the identifier of the actual matching name, if this",
	    "is different from the identifier of the corresponding accepted name.",
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "For taxonomic names: this field specifies the taxonomic rank of the actual matching name.",
	{ output => 'difference', com_name => 'tdf' },
	    "For taxonomic names: if the name is either a junior synonym or is invalid for some reason,",
	    "this field gives the reason.  The fields B<C<accepted_no>>",
	    "and B<C<accepted_name>> then specify the name that should be used instead.",
	{ output => 'accepted_name', com_name => 'acn', dedup => 'name' },
	    "For taxonomic names: if the name is not valid, this field gives the",
	    "corresponding accepted name.",
	{ output => 'accepted_rank', com_name => 'acr', dedup => 'taxon_rank' },
	    "For taxonomic names: if the accepted name has a different rank, this",
	    "field gives the rank of that name.",
	{ output => 'higher_taxon', com_name => 'htn' },
	    "For taxonomic names: a higher taxon (class if known or else phylum) in",
	    "which this taxon is contained.",
	# strata
	{ output => 'type', com_name => 'rnk' },
	    "For strata, this field specifies: group, formation, or member.",
	{ output => 'cc_list', com_name => 'cc2' },
	    "For strata: the country or countries in which it lies, as ISO-3166 country codes.",
	# intervals
	{ output => 'abbrev', com_name => 'abr' },
	    "For intervals: the standard abbreviation, if any",
	{ output => 'early_age', pbdb_name => 'max_ma', com_name => 'eag' },
	    "For intervals: the early age boundary in Ma",
	{ output => 'late_age', pbdb_name => 'min_ma', com_name => 'lag' },
	    "For intervals: the late age boundary in Ma",
	# people
	{ output => 'institution', com_name => 'ist' },
	    "For people: the person's listed institution.",
	{ output => 'country', com_name => 'ctr' },
	    "For people: the person's listed country.",
	# count fields
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "For strata: the number of fossil collections associated with this record in the database.",
	{ output => 'n_occs', com_name => 'noc' },
	    "For strata and taxa: the number of fossil occurrences associated with this record in the database.");
    
    $ds->define_set('1.2:combined:auto_types' =>
	{ value => 'int' },
	    "geological time intervals",
	{ value => 'str' },
	    "geological strata",
	{ value => 'prs' },
	    "database contributors",
	{ value => 'txn' },
	    "taxonomic names");
    
    $ds->define_set('1.2:combined:auto_optional' =>
	{ value => 'countries' },
	    "Show country names instead of country codes for strata records.");
    
    $ds->define_ruleset('1.2:combined:auto' =>
	{ param => 'name', valid => ANY_VALUE },
	    "A partial name or prefix.  It must have at least 3 significant characters,",
	    "and for taxa may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ optional => 'type', valid => '1.2:combined:auto_types', list => ',' },
	    "One or more record types from the following list. Matching records",
	    "from each of the specified types will be returned, up to the specified",
	    "limit, in the order specified. To the extent possible, an equal number of records from each type",
	    "will be returned. If this parameter is not specified, all four types",
	    "will be returned in the order listed below.",
	{ optional => 'SPECIAL(show)', valid => '1.2:combined:auto_optional', list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with each record.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
}



sub auto_complete {
    
    my ($request) = @_;
    
    my (@requested_types) = $request->clean_param_list('type') || ('int', 'str', 'prs', 'txn');
    my $name = $request->clean_param('name');
    
    # Unless we have at least three letters, and at least one record type, return an empty result.
    
    unless ( $name =~ qr{ [a-z] .* [a-z] .* [a-z] }xsi && @requested_types )
    {
	return;
    }
    
    # Get the limit, if any. Do other necessary parameter checks.
    
    my $total_limit = $request->clean_param('limit') || 10;
    
    $name =~ s/^\s+//;
    
    $request->strict_check;
    $request->extid_check;
    
    # Now collect up all of the results.
    
    my (@txn_results, @int_results, @str_results, @prs_results);
    my (%type_processed, @found_types);
    
    foreach my $type ( @requested_types )
    {
	next if $type_processed{$type};
	$type_processed{$type} = 1;
	
	if ( $type eq 'int' )
	{
	    @int_results = $request->auto_complete_int($name, $total_limit);
	    push @found_types, 'int' if @int_results;
	}
	
	elsif ( $type eq 'txn' )
	{
	    @txn_results = $request->auto_complete_txn($name, $total_limit);
	    push @found_types, 'txn' if @txn_results;
	}
	
	elsif ( $type eq 'str' )
	{
	    my $options = { countries =>  $request->has_block('countries') };
	    @str_results = $request->auto_complete_str($name, $total_limit, $options);
	    push @found_types, 'str' if @str_results;
	}
	
	elsif ( $type eq 'prs' )
	{
	    @prs_results = $request->auto_complete_prs($name, $total_limit);
	    push @found_types, 'prs' if @prs_results;
	}
    }
    
    return unless @found_types;
    
    my $per_type_limit = int($total_limit / scalar(@found_types));
    my @results;
    
    foreach my $type ( @found_types )
    {
	my $count = 0;
	
	if ( $type eq 'int' )
	{
	    foreach my $r (@int_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
	
	elsif ( $type eq 'txn' )
	{
	    foreach my $r (@txn_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
	
	elsif ( $type eq 'str' )
	{
	    foreach my $r (@str_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
	
	elsif ( $type eq 'prs' )
	{
	    foreach my $r (@prs_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
    }
    
    $request->list_result(\@results);
}


1;
