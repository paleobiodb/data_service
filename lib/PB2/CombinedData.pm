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

use IntervalBase qw(int_record);
use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_LITH $COLL_STRATA $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $PVL_MATRIX);
use ExternalIdent qw(generate_identifier %IDRE VALID_IDENTIFIER);

use Try::Tiny;
use Carp qw(carp croak);

use Moo::Role;

no warnings 'numeric';


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData PB2::IntervalData PB2::CollectionData PB2::TaxonData PB2::PersonData);

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
    
    # The following definitions specify the 'auto' operation. It is used for
    # auto-completion.
    
    $ds->define_block('1.2:combined:auto' =>
	{ output => 'record_id', com_name => 'oid' },
	    "The identifier (if any) of the database record corresponding to this name.",
	{ output => 'record_type', com_name => 'typ' },
	    "The type of this record: varies by record. Will be one of: C<B<txn>>, C<B<str>>,",
	    "C<B<prs>>, C<B<int>>, C<B<col>>, C<B<ref>>.",
	{ output => 'name', com_name => 'nam' },
	    "A name that matches the characters given by the B<C<name>> parameter.",
	# taxa
	{ output => 'taxon_no', com_name => 'vid', dedup => 'record_id' },
	    "For taxonomic names: the identifier of the actual matching name, if this",
	    "is different from the identifier of the corresponding accepted name.",
	{ output => 'taxon_rank', com_name => 'rnk', data_type => 'mix' },
	    "For taxonomic names: this field specifies the taxonomic rank of the actual matching name.",
	{ output => 'difference', com_name => 'tdf' },
	    "For taxonomic names: if the name is either a junior synonym or is invalid for some",
	    "reason, this field gives the reason.  The fields B<C<accepted_no>>",
	    "and B<C<accepted_name>> then specify the name that should be used instead.",
	{ output => 'accepted_name', com_name => 'acn', dedup => 'name' },
	    "For taxonomic names: if the name is not valid, this field gives the",
	    "corresponding accepted name.",
	{ output => 'accepted_rank', com_name => 'acr', dedup => 'taxon_rank', data_type => 'mix' },
	    "For taxonomic names: if the accepted name has a different rank, this",
	    "field gives the rank of that name.",
	{ output => 'higher_taxon', com_name => 'htn' },
	    "For taxonomic names: a higher taxon (class if known or else phylum) in",
	    "which this taxon is contained.",
	# strata and collections
	{ output => 'type', com_name => 'rnk' },
	    "For strata, this field specifies: group, formation, or member.",
	{ output => 'cc_list', com_name => 'cc2' },
	    "For strata and collections: the country or countries in which it lies, as ISO-3166 country codes.",
	# collections
	{ output => 'early_interval', com_name => 'oei' },
	    "For collections, this field specifies the interval from which the collection dates, or",
	    "the early end of the range.",
	{ output => 'late_interval', com_name => 'oli', dedup => 'early_interval' },
	    "For collections, this field (if not empty) specifies the end of the time range from which",
	    "this collection dates.",
	# intervals
	{ output => 'abbrev', com_name => 'abr' },
	    "For intervals: the standard abbreviation, if any",
	{ output => 'early_age', pbdb_name => 'max_ma', com_name => 'eag', data_type => 'dec' },
	    "For intervals: the early age boundary in Ma",
	{ output => 'late_age', pbdb_name => 'min_ma', com_name => 'lag', data_type => 'dec' },
	    "For intervals: the late age boundary in Ma",
	# people
	{ output => 'institution', com_name => 'ist' },
	    "For people: the person's listed institution.",
	{ output => 'country', com_name => 'ctr' },
	    "For people: the person's listed country.",
	# count fields
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "For strata: the number of fossil collections associated with this record in the database.",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "For strata and taxa: the number of fossil occurrences associated with this record in the database.");
    
    $ds->define_set('1.2:combined:auto_types' =>
	{ value => 'int' },
	    "geological time intervals",
	{ value => 'str' },
	    "geological strata",
	{ value => 'prs' },
	    "database contributors",
	{ value => 'txn' },
	    "taxonomic names",
	{ value => 'col' },
	    "fossil collections",
	{ value => 'ref' },
	    "bibliographic references",
	{ value => 'nav' },
	    "select the set of types appropriate for auto-complete in the Navigator web application",
	{ value => 'cls' },
	    "select the set of types appropriate for auto-complete in the Classic web application");
    
    $ds->define_set('1.2:combined:auto_optional' =>
	{ value => 'countries' },
	    "Show country names instead of country codes for strata and collection records.");    
    
    $ds->define_ruleset('1.2:combined:auto' =>
	{ param => 'name', valid => ANY_VALUE },
	    "A partial name or prefix.  It must have at least 3 significant characters,",
	    "and for taxa may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ optional => 'type', valid => '1.2:combined:auto_types', list => ',', bad_value => 'none' },
	    "One or more record types from the following list. Matching records",
	    "from each of the specified types will be returned, up to the specified",
	    "limit, in the order specified. To the extent possible, an equal number of records from each type",
	    "will be returned. If this parameter is not specified, it will default to C<B<int, str, prs, txn>>.",
	{ optional => 'interval', valid => ANY_VALUE },
	    "If the list of types to be returned includes collections, you can provide the name",
	    "or identifier of a geological time interval. In this case, only collections",
	    "occurring within that interval (or overlapping it) will be returned.",
	{ optional => 'SPECIAL(show)', valid => '1.2:combined:auto_optional', list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with each record.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    # The following definitions specify the 'associated' operation. It is used to return
    # records of various kinds associated with the specified reference(s).
    
    $ds->define_set('1.2:combined:associated_types' =>
	{ value => 'txn' },
	    "All taxonomic names (primary reference, not simply mentioned in an opinion)",
	{ value => 'var', undocumented => 1 },
	{ value => 'opn' },
	    "All opinions",
	{ value => 'col' },
	    "All collections (primary or secondary reference)",
	{ value => 'all' },
	    "All of the above.");
    
    $ds->define_set('1.2:combined:associated_optional' =>
	{ value => 'countries' },
	    "Show country names instead of country codes for collection records.");    
    
    $ds->define_ruleset('1.2:combined:associated' =>
	{ param => 'ref_id', valid => VALID_IDENTIFIER('REF'), list => ',',
	  bad_value => '-1' },
	    "You must specify one or more reference identifier, as a comma-separated list.",
	{ param => 'type', valid => '1.2:combined:associated_types', list => ',',
	  bad_value => '_' },
	    "You must specify one or more types of records to return.",
	{ optional => 'SPECIAL(show)', valid => '1.2:combined:auto_optional', list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with each record.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.",
	">If the parameter B<C<order>> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
    $ds->define_block('1.2:combined:associated' =>
	{ output => 'record_id', com_name => 'oid' },
	    "The identifier of a database record associated with the specified",
	    "bibliographic reference(s)",
	{ output => 'record_type', com_name => 'typ' },
	    "The type of this record. Will be one of C<B<txn>>,",
	    "C<B<opn>>, C<B<col>>.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the bibliographic reference with which this record",
	    "is associated",
	{ output => 'ref_type', com_name => 'rtp' },
	    "The role of the bibliographic reference with respect to the record,",
	    "or for an opinion record the role played by the opinion.",
	    "The value will be one of the following:",
	    "A - the reference is the authority for this name;",
	    "C - the opinion has been chosen as a classification opinion;",
	    "U - the opinion is not a classification opinion;",
	    "X - the opinion is suppressed (ignored);",
	    "P - the reference is the primary reference for this collection;",
	    "S - the reference is a secondary reference for this collection",
	# taxa
	{ output => 'taxon_name', com_name => 'nam' },
	    "A taxonomic name for which a bibliographic reference is the primary source",
	{ output => 'taxon_no', com_name => 'tid', dedup => 'record_id' },
	    "The identifier of this taxon, if different from the original name",
	{ output => 'taxon_rank', com_name => 'rnk', data_type => 'mix' },
	    "The taxonomic rank of that name",
	# collections
	{ output => 'collection_name', com_name => 'nam' },
	    "The name of a collection",
	{ output => 'early_interval', com_name => 'oei' },
	    "The interval from which the collection dates, or the early end of the range.",
	{ output => 'late_interval', com_name => 'oli', dedup => 'early_interval' },
	    "The late end of the range, if different from the early interval",
	{ output => 'cc_list', com_name => 'cc2' },
	    "The country or countries in which the collection lies, as ISO-3166 country codes",
	    "or country names",
	# opinions
	{ output => 'child_name', com_name => 'nam' },
	    "A taxonomic name that is the subject of an opinion",
	{ output => 'child_spelling_no', com_name => 'vid' },
	    "The identifier of this name",
	{ output => 'status', com_name => 'sta' },
	    "The taxonomic status of this name, as expressed by this opinion.",
	{ output => 'parent_name', com_name => 'prl' },
	    "The taxonomic name under which the subject is being placed, the parent name.",
	    "Note that the value of this field is the particular variant of the name that was given",
	    "in the opinion, not necessarily the currently accepted variant.",
	{ output => 'parent_spelling_no', com_name => 'par' },
	    "The identifier of the parent name.",
	{ output => 'spelling_reason', com_name => 'spl' },
	    "An indication of why this name was given.",
	# { output => 'opinion_type', com_name => 'otp' },
	#     "The type of opinion represented: B<C> for a",
	#     "classification opinion, B<U> for an opinion which was not selected",
	#     "as a classification opinion, B<X> for an opinion which is suppressed.",
	{ set => 'opinion_type', lookup => \%PB2::TaxonData::pbdb_opinion_code, if_vocab => 'pbdb' },
	{ output => 'author', com_name => 'oat' },
	    "The author of the opinion, if different from the bibliographic reference.",
	{ output => 'pubyr', com_name => 'opy' },
	    "The year in which the opinion was published, if different from the",
	    "bibliographic reference.");
    
}


# auto_complete ( )
# 
# This operation returns a list of records of the specified types whose names start with
# the specified letters. It is used for auto-completion in Navigator, Classic, and various
# apps.

sub auto_complete {
    
    my ($request) = @_;
    
    my @requested_types = $request->clean_param_list('type');
    
    if ( @requested_types == 1 )
    {
	@requested_types = ('int', 'str', 'prs', 'txn', 'col', 'ref') if $requested_types[0] eq 'nav';
	@requested_types = ('int', 'str', 'txn', 'col', 'ref') if $requested_types[0] eq 'cls';
    }
    
    elsif ( @requested_types == 0 )
    {
	@requested_types = ('int', 'str', 'prs', 'txn');
    }
    
    my $name = $request->clean_param('name');
    
    # Unless we have at least three letters, and at least one record type, return an empty result.
    
    unless ( $name =~ qr{ [a-z] .* [a-z] .* [a-z] }xsi && @requested_types )
    {
	return;
    }
    
    # Get the limit, if any. Do other necessary parameter checks.
    
    my $total_limit = $request->clean_param('limit') || 10;
    my $interval = $request->clean_param('interval');
    
    $name =~ s/^\s+//;
    
    my $options = { countries =>  $request->has_block('countries') };
    
    if ( $interval )
    {
	my $interval_record;
	
	if ( $interval =~ $IDRE{INT} )
	{
	    $interval_record = int_record($2);
	}
	
	else
	{
	    $interval_record = int_record($interval);
	}
	
	unless ( ref $interval_record )
	{
	    die $request->exception(400, "Unknown interval '$interval'");
	}
	
	$options->{early_age} = $interval_record->{early_age};
	$options->{late_age} = $interval_record->{late_age};
    }
    
    $request->strict_check;
    $request->extid_check;
    
    # Now collect up all of the results.
    
    my (@txn_results, @int_results, @str_results, @prs_results, @col_results, @ref_results);
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
	    @str_results = $request->auto_complete_str($name, $total_limit, $options);
	    push @found_types, 'str' if @str_results;
	}
	
	elsif ( $type eq 'prs' )
	{
	    @prs_results = $request->auto_complete_prs($name, $total_limit);
	    push @found_types, 'prs' if @prs_results;
	}
	
	elsif ( $type eq 'col' )
	{
	    @col_results = $request->auto_complete_col($name, $total_limit, $options);
	    push @found_types, 'col' if @col_results;
	}
	
	elsif ( $type eq 'ref' )
	{
	    @ref_results = $request->auto_complete_ref($name, $total_limit);
	    push @found_types, 'ref' if @ref_results;
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
	
	elsif ( $type eq 'col' )
	{
	    foreach my $r (@col_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
	
	elsif ( $type eq 'ref' )
	{
	    foreach my $r (@ref_results)
	    {
		push @results, $r;
		last if ++$count >= $per_type_limit;
		last if @results > $total_limit;
	    }
	}
    }
    
    $request->list_result(\@results);
}


# list_associated ( )
# 
# This operation returns a list of records of the specified types which are associated
# with the specified reference(s).

sub list_associated {
    
    my ($request) = @_;
    
    # Check that we have the proper parameter values.
    
    my @requested_types = $request->clean_param_list('type');
    
    unless ( @requested_types )
    {
	@requested_types = ('txn', 'opn', 'col');
    }
    
    elsif ( $requested_types[0] eq 'all' || $requested_types[-1] eq 'all' )
    {
	@requested_types = ('txn', 'opn', 'col');
    }
    
    elsif ( $requested_types[0] eq '_' )
    {
	die $request->exception(400, "You must specify at least one valid record type");
    }
    
    my @reference_nos = $request->clean_param_list('ref_id');
    
    if ( $reference_nos[0] eq '-1' )
    {
	die $request->exception(400, "You must specify at least one valid reference identifier");
    }
    
    # Check for strict and extid.
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the result limit, and then remove the automatic limit check on the overall
    # result.
    
    my $limit = $request->clean_param('limit') || 100;
    
    $limit = '9999' if $limit eq 'all';
    
    $request->result_limit('all');
    
    my $generate_extids = $request->has_block('extids');
    
    # Now collect up the results.
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my $debug_sub = $request->debug ? \&Web::DataService::IRequest::debug_line : undef;
    
    my (@results, %ref_list, %type_processed);
    
    foreach my $type ( @requested_types )
    {
	$type = 'txn' if $type eq 'var';	# 'var' is an alias for 'txn'
	
	next if $type_processed{$type};
	$type_processed{$type} = 1;
	
	if ( $type eq 'txn' )
	{
	    my $options = { reference_no => \@reference_nos, fields => 'AUTH_ASSOC',
			    debug_out => $debug_sub };
	    
	    my %ref_count;
	    
	    foreach my $r ( $taxonomy->list_taxa('all_records', undef, $options) )
	    {
		my $refno = $r->{reference_no};
		
		$r->{record_id} = $r->{orig_no} || $r->{taxon_no};
		$r->{record_type} = $r->{orig_no} && $r->{taxon_no} eq $r->{orig_no} ? 'txn' : 'var';
		$r->{ref_type} = 'A';
		$request->generate_extids($r) if $generate_extids;
		
		push $ref_list{$refno}->@*, $r unless ++$ref_count{$refno} > $limit;
	    }
	}
	
	elsif ( $type eq 'opn' )
	{
	    my $options = { reference_no => \@reference_nos, fields => 'OP_ASSOC',
			    record_type => 'opinions', debug_out => $debug_sub };
	    
	    my %ref_count;
	    
	    foreach my $r ( $taxonomy->list_associated('all_taxa', undef, $options) )
	    {
		my $refno = $r->{reference_no};
		
		$r->{record_id} = $r->{opinion_no};
		$r->{record_type} = 'opn';
		$request->generate_extids($r) if $generate_extids;
		
		push $ref_list{$refno}->@*, $r unless ++$ref_count{$refno} > $limit;
	    }
	}
	
	elsif ( $type eq 'col' )
	{
	    my $cc_field = $request->has_block('countries') ? 'cm.name' : 'c.cc';
	    
	    my $ref_list = join("','", @reference_nos);
	    
	    my $sql = "
	SELECT collection_no as record_id, 'P' as ref_type, cc.collection_name,
		cc.reference_no, $cc_field as cc_list, n_occs, c.early_age, c.late_age,
		ei.interval_name as early_interval, li.interval_name as late_interval
	FROM collections as cc join $COLL_MATRIX as c using (collection_no)
		left join $COUNTRY_MAP as cm using (cc)
		left join $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
		left join $INTERVAL_DATA as li on li.interval_no = c.late_int_no
	WHERE cc.reference_no in ('$ref_list')
	UNION
	SELECT collection_no as record_id, 'S' as ref_type, cc.collection_name,
		sr.reference_no, $cc_field as cc_list, n_occs, c.early_age, c.late_age,
		ei.interval_name as early_interval, li.interval_name as late_interval
	FROM collections as cc join $COLL_MATRIX as c using (collection_no)
		join secondary_refs as sr using (collection_no)
		left join $COUNTRY_MAP as cm using (cc)
		left join $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
		left join $INTERVAL_DATA as li on li.interval_no = c.late_int_no
	WHERE sr.reference_no in ('$ref_list')
	ORDER BY reference_no, record_id";
	    
	    print STDERR "$sql\n\n" if $request->debug;
	    
	    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
	    
	    my %ref_count;
	    
	    if ( ref $result_list eq 'ARRAY' )
	    {
		foreach my $r ( @$result_list )
		{
		    my $refno = $r->{reference_no};
		    
		    $r->{record_type} = 'col';
		    $request->generate_extids($r) if $generate_extids;
		    
		    push $ref_list{$refno}->@*, $r unless ++$ref_count{$refno} > $limit;
		}
	    }
	}
    }
    
    foreach my $sorted ( sort { $a <=> $b } keys %ref_list )
    {
	push @results, $ref_list{$sorted}->@*;
    }
    
    $request->list_result(\@results);
}


# generate_extids ( record )
# 
# Generate external identifiers for every appropriate field in the record.

my %ID_MAP = (col => 'COL', txn => 'TXN', var => 'VAR', opn => 'OPN');

sub generate_extids {
    
    my ($request, $r) = @_;
    
    # Start with the record identifier.
    
    my $id_type = $ID_MAP{$r->{record_type}};
    
    $r->{record_id} = generate_identifier($id_type, $r->{record_id});
    
    # Then the reference identifier.
    
    $r->{reference_no} = generate_identifier('REF', $r->{reference_no});
    
    # Then check other possible fields.
    
    if ( $id_type eq 'OPN' )
    {
	$r->{child_spelling_no} = generate_identifier('VAR', $r->{child_spelling_no});
	$r->{parent_spelling_no} = generate_identifier('VAR', $r->{parent_spelling_no});
    }
}


1;
