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


our (@REQUIRES_ROLE) = qw(PB2::IntervalData PB2::CollectionData PB2::TaxonData PB2::PersonData);

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
	{ output => 'record_type', com_name => 'typ' },
	    "The type of this record: varies by record. Will be one of: C<B<str>>, C<B<txn>>, C<B<prs>>, C<B<int>>.",
	{ output => 'record_id', com_name => 'oid' },
	    "The identifier (if any) of the database record corresponding to this name.",
	{ output => 'name', com_name => 'nam' },
	    "A name that matches the characters given by the B<C<name>> parameter.",
	# taxa
	# { set => 'taxon_rank', if_vocab => 'com', lookup => \%TAXON_RANK },
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "For taxonomic names: this field specifies the taxonomic rank.",
	{ output => 'misspelling', com_name => 'msp' },
	    "If this name is marked as a misspelling, then this field will be included with the value '1'",
	{ output => 'status', com_name => 'sta' },
	    "For taxonomic names: this field specifies the taxonomic status.",
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
	{ value => 'txn' },
	    "taxonomic names",
	{ value => 'int' },
	    "geological time intervals",
	{ value => 'str' },
	    "geological strata",
	{ value => 'prs' },
	    "database contributors");
    
    # 	{ output => 'type', com_name => 'rnk' },
    # 	    "The type of stratum: group, formation, or member.",
    # 	{ output => 'cc_list', com_name => 'cc2' },
    # 	    "The country or countries in which this stratum lies, as ISO-3166 country codes.",
    # 	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
    # 	    "The number of fossil collections in the database that are associated with this stratum.",
    # 	    "Note that if your search is limited to a particular geographic area, then",
    # 	    "only collections within the selected area are counted.",
    # 	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
    # 	    "The number of fossil occurrences in the database that are associated with this stratum.",
    # 	    "The above note about geographic area selection also applies.");
    
    # $ds->define_block('1.2:taxa:auto' =>
    # 	{ output => 'taxon_no', dwc_name => 'taxonID', com_name => 'oid' },
    # 	    "A positive integer that uniquely identifies this taxonomic name",
    # 	{ output => 'record_type', com_name => 'typ', com_value => 'txn', dwc_value => 'Taxon', value => $IDP{TXN} },
    # 	    "The type of this object: {value} for a taxonomic name",
    # 	{ set => 'taxon_rank', if_vocab => 'com', lookup => \%TAXON_RANK },
    # 	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
    # 	    "The taxonomic rank of this name",
    # 	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
    # 	    "The scientific name of this taxon",
    # 	{ output => 'misspelling', com_name => 'msp' },
    # 	    "If this name is marked as a misspelling, then this field will be included with the value '1'",
    # 	{ output => 'n_occs', com_name => 'noc' },
    # 	    "The number of occurrences of this taxon in the database");
    
    # $ds->define_block('1.2:people:basic' =>
    # 	{ select => [ qw(p.person_no p.name p.country p.institution
    # 			 p.email p.is_authorizer) ] },
    # 	{ output => 'person_no', com_name => 'oid' },
    # 	    "A positive integer that uniquely identifies this database contributor",
    # 	{ output => 'record_type', com_name => 'typ', com_value => 'prs', value => 'person' },
    # 	    "The type of this object: {value} for a database contributor",
    # 	{ output => 'name', com_name => 'nam' },
    # 	    "The person's name",
    # 	{ output => 'institution', com_name => 'ist' },
    # 	    "The person's institution",
    # 	{ output => 'country', com_name => 'ctr' },
    # 	    "The database contributor's country");
    
    # $ds->define_block('1.2:intervals:basic' =>
    # 	{ select => [ qw(i.interval_no i.interval_name i.abbrev sm.scale_no sm.scale_level
    # 			 sm.parent_no sm.color i.early_age i.late_age i.reference_no) ] },
    # 	{ output => 'interval_no', com_name => 'oid' },
    # 	    "A positive integer that uniquely identifies this interval",
    # 	{ output => 'record_type', com_name => 'typ', value => $IDP{INT} },
    # 	    "The type of this object: C<$IDP{INT}> for an interval",
    # 	{ output => 'scale_no', com_name => 'tsc' },
    # 	    "The time scale in which this interval lies.  An interval may be reported more than",
    # 	    "once, as a member of different time scales",
    # 	{ output => 'scale_level', com_name => 'lvl' },
    # 	    "The level within the time scale to which this interval belongs.  For example,",
    # 	    "the default time scale is organized into the following levels:",
    # 	    "=over", "=item Level 1", "Eons",
    # 		       "=item Level 2", "Eras",
    # 		       "=item Level 3", "Periods",
    # 		       "=item Level 4", "Epochs",
    # 		       "=item Level 5", "Stages",
    # 	    "=back",
    # 	{ output => 'interval_name', com_name => 'nam' },
    # 	    "The name of this interval",
    # 	{ output => 'abbrev', com_name => 'abr' },
    # 	    "The standard abbreviation for the interval name, if any",
    # 	{ output => 'parent_no', com_name => 'pid' },
    # 	    "The identifier of the parent interval",
    # 	{ Output', com_name => 'col' },
    # 	    "The standard color for displaying this interval",
    # 	{ output => 'early_age', pbdb_name => 'max_ma', com_name => 'eag' },
    # 	    "The early age boundary of this interval (in Ma)",
    # 	{ output => 'late_age', pbdb_name => 'min_ma', com_name => 'lag' },
    # 	    "The late age boundary of this interval (in Ma)",
    # 	# { set => 'reference_no', append => 1 },
    # 	{ output => 'reference_no', com_name => 'rid', text_join => ', ', show_as_list => 1 },
    # 	    "The identifier(s) of the references from which this data was entered",
    # 	{ set => '*', code => \&process_int_ids });
    
    $ds->define_ruleset('1.2:combined:auto' =>
	{ param => 'name', valid => ANY_VALUE },
	    "A partial name or prefix.  It must have at least 3 significant characters,",
	    "and for taxa may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ param => 'type', valid => '1.2:combined:auto_types', list => ',' },
	    "One or more record types from the following list. Matching records",
	    "from each of the specified types will be returned, up to the specified",
	    "limit. To the extent possible, an equal number of records from each type",
	    "will be returned.",
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
    
    # $request->strict_check;
    # $request->extid_check;
    
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
	    @str_results = $request->auto_complete_str($name, $total_limit);
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


sub auto_complete_int {
    
    my ($request, $name, $limit) = @_;
    
    return if $name =~ qr{ ^ \s }xsi;
    return if $name =~ qr{ ^ (?: e(a(r(ly?)?)?)? \s* | m(i(d(d(le?)?)?)?)? \s* | l(a(te?)?)? \s* ) $ }xsi;
    
    my $search_name = lc $name;
    
    $search_name =~ s/ ^early\s* | ^middle\s* | ^late\s* //xs;
    
    my $prefix = substr($search_name, 0, 3);
    my $name_len = length($name);
    
    return unless length($prefix) == 3;
    
    my @results;
    
    foreach my $i ( @{$PB2::IntervalData::IPREFIX{$prefix}} )
    {
	if ( lc substr($i->{interval_name}, 0, $name_len) eq $name )
	{
	    push @results, { name => $i->{interval_name}, record_type => 'int',
			     record_id => generate_identifier('int', $i->{interval_no}),
			     early_age => $i->{early_age}, late_age => $i->{late_age} };
	}
    }
    
    return @results;
}


sub auto_complete_txn {
    
    my ($request, $name, $limit) = @_;
    
    return;
}


sub auto_complete_str {
    
    my ($request) = @_;
    
    return;
}


sub auto_complete_prs {
    
    my ($request) = @_;
    
    return;
}


1;
