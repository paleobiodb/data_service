#!/usr/bin/env perl
#
# strata.pl - manipulate stratigraphic names in the PBDB and Macrostrat
#
# Written by: Michael McClennen
# Created: 2025-10-07

use strict;

use lib 'lib';
use feature qw(say fc);

use CoreFunction qw(loadConfig configData connectDB);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use DBQuery qw(DBHashQuery DBRowQuery DBSingleHashQuery DBTextQuery DBCommand DBInsert CheckMode);

use Text::Levenshtein::Damerau qw(edistance);

use Getopt::Long qw(:config bundling no_auto_abbrev permute);
use YAML;
use Encode qw(encode_utf8);
use Term::ReadLine;


# use List::Util qw(any max min);


# Read the configuration file, and open database connections.

my ($opt_quiet, $opt_verbose,  $opt_force, $opt_debug, $opt_help);
my ($opt_config);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "config|f" => \$opt_config,
	   "force" => \$opt_force,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

loadConfig($opt_config);

my $dbconf = configData('Database');

if ( $ENV{PWD} !~ qr{^/var/paleomacro/pbapi} )
{
    $dbconf->{host} = '127.0.0.1';
}

our ($mstr) = connectDB($opt_config, 'macrostrat');
our ($pbdb) = connectDB($opt_config, 'pbdb');

our ($chunk_size) = 20000;

die "Could not connect to database: $DBI::errstr\n" unless $mstr && $pbdb;

our (%second_words);

our (%is_rock_type) = ( and => 1, argile => 1, ash => 1, ashe => 1,
			bed => 1, band => 1, 'bänderschiefer' => 1, black => 1, breccia => 1,
			calcaire => 1, calcarenite => 1, calcareous => 1, calcareou => 1,
			calcari => 1,
			calciferous => 1, calcilutite => 1, calcirudite => 1, carbonate => 1,
			chalk => 1, chalke => 1, chert => 1, clay => 1, coal => 1, complex => 1,
			conglomerate => 1, conglomeratic => 1,
			coquina => 1, crag => 1, cyclothem => 1,
			diatomite => 1, dolomite => 1, dolostone => 1,
			equivalent => 1, facie => 1, flag => 1, flagstone => 1,
			formtation => 1,
			gravel => 1, greensand => 1, 'grès' => 1, grey => 1, grigi => 1,
			grit => 1, gypsum => 1, hoj => 1, 'høj' => 1, horizon => 1, 
			iron => 1, ironstone => 1, lignite => 1,
			limesetone => 1, limestone => 1, limstone => 1, ls => 1, 'ls.' => 1,
			lutite => 1,
			marble => 1, 'marine sand' => 1, marl => 1, marlstone => 1, marly => 1,
			measure => 1, mudstone => 1,
			oolite => 1, ore => 1,
			pebble => 1, pebbly => 1, phonolite => 1, phosphatic => 1,
			platy => 1, porcelain => 1, pyrite => 1,
			quarry => 1, quartzite => 1, 'q-sand' => 1,
			radiolaridic => 1, rag => 1, red => 1, reef => 1, sand => 1,
			sandstone => 1, ss => 1, 'ss.' => 1, schichten => 1, series => 1, serie => 1,
			sh => 1, ss => 1, shale => 1, shellbed => 1, silt => 1, silty => 1,
			siltsone => 1, siltstone => 1, slate => 1, stage => 1, suite => 1,
			stone => 1, subsuite => 1, suite => 1, svita => 1,
			tuff => 1, tuffaceous => 1, unit => 1, volcanic => 1, waterstones => 1,
			yellow => 1, zone => 1 );

our (%excluded_name) = ( lower => 1, middle => 1, upper => 1, first => 1, second => 1, third => 1,
			 fourth => 1, base => 1, top => 1, 'upper part' => 1, 'lower part' => 1,
			 'middle part' => 1, alpha => 1, beta => 1, informal => 1 );

our (%allowed_suffix) = ( fjord => 1, land => 1, mountain => 1 );

# Check the command arguments, and execute the specified function.

if ( $ARGV[0] eq 'generate' )
{
    shift @ARGV;
    
    if ( $ARGV[0] eq 'concepts' )
    {
	shift @ARGV;
	&GenerateConcepts(@ARGV);
    }
    
    else
    {
	die "Unknown subcommand '$ARGV[0]'\n";
    }
}

elsif ( $ARGV[0] eq 'import' )
{
    shift @ARGV;
    
    if ( $ARGV[0] eq 'macrostrat' || $ARGV[0] eq 'ms' )
    {
	shift @ARGV;
	&ImportMacrostrat()
    }
    
    else
    {
	die "Unknown subcommand '$ARGV[0]'\n";
    }
}

elsif ( $ARGV[0] eq 'check' || $ARGV[0] eq 'update' )
{
    my $cmd = shift @ARGV;
    
    if ( $ARGV[1] eq 'tables' )
    {
	shift @ARGV;
	&UpdateTables($cmd, @ARGV);
    }
    
    else
    {
	die "Unknown subcommand '$ARGV[0]'\n";
    }
}

else
{
    die "Unknown subcommand '$ARGV[0]'\n";
}

exit;



# GenerateConcepts ( )
#
# Extract stratigraphic names and concepts from the COLLECTION_STRATA table. This table
# contains exactly what was typed in by the database contributors, minus
# suffixes like 'Fm.' or 'Gp.'. This subroutine completely rebuilds the
# STRAT_NAMES and STRAT_CONCEPTS tables.

our (%strat_raw, %strat_name, %strat_prelim, %strat_concept, %contained_in, %prelim_to_real);
our (%country_map);

our $raw_colls = 0;
our $raw_occs = 0;
our $parent_matches = 0;

sub GenerateConcepts {

    # Step I: iterate through the rows of the COLLECTION_STRATA table. Create a set of
    # stratigraphic name records in %strat_raw, and record the relationships between
    # them in %contained_in. The SQL query below returns information from
    # COLLECTION_STRATA, and also from COLLECTION_MATRIX and COLLECTION_DATA. All of
    # these tables use collection_no as their primary key, so they have a 1-1
    # correspondence.
    
    say "Reading from table '$TABLE{COLLECTION_STRATA}'...";
    
    my $stratigraphy_data = DBHashQuery($pbdb, "
	SELECT cs.grp, cs.formation, cs.member, c.cc,
		count(*) as n_colls, sum(c.n_occs) as n_occs,
		max(c.early_age) as early_age, min(c.late_age) as late_age,
		min(c.lat) as lat_min, max(c.lat) as lat_max,
		min(c.lng) as lng_min, max(c.lng) as lng_max,
		group_concat(distinct cc.reference_no) as reference_no,
		group_concat(distinct cc.lithology1) as lithology1,
		group_concat(distinct cc.lithology2) as lithology2
	FROM $TABLE{COLLECTION_STRATA} as cs
		join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	GROUP BY grp, formation, member, cc");
    
    foreach my $row ( $stratigraphy_data->@* )
    {
	my ($gp_key, $fm_key, $mbr_key, $fm_good);
	
	# The rows of COLLECTION_STRATA contain columns 'grp', 'formation', and
	# 'member'. Create a stratigraphic name record for each of these values,
	# provided the value contains at least three letters in a row and is not
	# "unnamed". These criteria are checked by the UpdateStratRaw function. If there
	# is an existing stratigraphic name record with the same key, it is expanded to
	# include the information from the current row. The key for each record is made
	# up of the raw stratigraphic name, rank, and the country code in which it
	# occurs. That means we will have separate name records for strata which appear
	# in multiple countries, though as we will see below these may be linked
	# together into a single stratigraphic concept.
    
	if ( $row->{grp} )
	{
	    $gp_key = "$row->{grp}|Gp|$row->{cc}";
	    UpdateStratRaw($gp_key, $row, 'grp');
	    $raw_colls += $row->{n_colls};
	    $raw_occs += $row->{n_occs};
	}
	
	if ( $row->{formation} )
	{
	    $fm_key = "$row->{formation}|Fm|$row->{cc}";
	    $fm_good = UpdateStratRaw($fm_key, $row, 'formation');
	    $raw_colls += $row->{n_colls};
	    $raw_occs += $row->{n_occs};
	}
	
	if ( $row->{member} )
	{
	    $mbr_key = "$row->{member}|Mbr|$row->{cc}";
	    UpdateStratRaw($mbr_key, $row, 'member');
	    $raw_colls += $row->{n_colls};
	    $raw_occs += $row->{n_occs};
	}
	
	# Record the relationships between non-empty stratigraphic names, represented by
	# their raw name keys as described above. If there is an existing relationship with
	# the same keys, it is expanded to include the information from the current row.
	# A member-grp relationship is only recorded if the formation name is empty or
	# doesn't meet the criteria for inclusion.
	
	if ( $row->{grp} && $row->{formation} && $fm_good )
	{
	    UpdateContainedIn($fm_key, $gp_key, $row);
	}
	
	elsif ( $row->{grp} && $row->{member} )
	{
	    UpdateContainedIn($mbr_key, $gp_key, $row);
	}
	
	if ( $row->{formation} && $row->{member} )
	{
	    UpdateContainedIn($mbr_key, $fm_key, $row);
	}
    }
    
    # Also read in the country map, so that we can convert country codes to country
    # names.
    
    %country_map = DBSingleHashQuery($pbdb, "SELECT cc, name FROM $TABLE{COUNTRY_MAP}", "cc");
    
    
    # Step II: Iterate through the raw stratigraphc name records in alphabetical order,
    # and store references to them in the %strat_name hash. Each raw name is decomposed
    # into one or more component stratigraphic names, and each component will generate a
    # key composed of the component name, rank, and country code. This means that the
    # %strat_name hash will have more keys than the %strat_raw hash does and is a
    # many-many mapping. Raw names which generate multiple components are those which
    # contain 'and', 'or', 'x (= y)', etc.
    
    # In the code below, $rkey refers to a raw name key, while $nkey refers to a name
    # component key. The relation stored in %strat_name between name component keys and
    # name records will be the basis for the rows of the STRAT_NAMES table.
    
    # Because the child-parent relationships are extracted from the COLL_STRATA table,
    # and because all components of a multiple-component name have the same parent,
    # child-parent relationships are stored using raw name keys in the %contained_in
    # hash. The converse, however, is not true. In general, the individual components of
    # a multiple-component name will have different children. Because we have no way to
    # differentiate these children, we ignore relationships where the parent (raw)
    # name has multiple components.
    
    say "Decomposing names...";
    
    my (%gp_keys, %fm_keys, %sfm_keys, %mbr_keys);
    my (%first_two, %first_last, %alias_of);
    
    foreach my $rkey ( sort { fc($a) cmp fc($b) } keys %strat_raw )
    {
	my $nr = $strat_raw{$rkey};
	my $raw_name = $nr->{name};
	my $rank = $nr->{rank};
	my @components;
	
	# Start by processing the raw name to remove double quotation marks and question
	# marks surrounded by parentheses.
	
	$raw_name =~ s/"//g;
	
	if ( $raw_name =~ /(.*?)\s*[(][?][)]\s*(.*)/ )
	{
	    if ( $1 ne '' && $2 ne '' )
	    {
		$raw_name = "$1 $2";
	    }
	    
	    else
	    {
		$raw_name = ($1 ne '' ? $1 : $2);
	    }
	}
	
	# A few names have the form "x? y?". Replace these with "x or y", which will be
	# split up below.
	
	elsif ( $raw_name =~ /(.*?)\s*[?]\s*(.*?)\s*[?]$/ )
	{
	    $raw_name = "$1 or $2";
	}
	
	# Other than this special case, process single question marks the same way as
	# those surrounded by parentheses.
	
	elsif ( $raw_name =~ /(.*?)\s*[?]\s*(.*)/ )
	{
	    if ( $1 ne '' && $2 ne '' )
	    {
		$raw_name = "$1 $2";
	    }

	    else
	    {
		$raw_name = ($1 ne '' ? $1 : $2);
	    }
	}
	
	# Remove whitespace on either side of a hyphen.
	
	if ( $raw_name =~ /-/ )
	{
	    $raw_name =~ s/\s*-\s*/-/g;
	}
	
	# Split up names that include '&', 'and/or', 'and', 'or', or commas. The
	# 'and/&' case is the hardest by far, because 'and' can sometimes be
	# part of a stratigraphic name.
	
	if ( $raw_name =~ /(.*?)\s*\/\s*(.*)/ )
	{
	    @components = ($1, $2);
	}
	
	elsif ( $raw_name =~ /(.*?)\s+(or|and\s*\/\s*or)\s+(.*)/ )
	{
	    @components = ($1, $2);
	}
	
	# There are a few special cases which we handle here.
	
	elsif ( $raw_name =~ /^Aloformation Playa del Zorro|^Grès, argiles et lignites/ )
	{
	    @components = $raw_name;
	}
	
	elsif ( $raw_name =~ /,/ )
	{
	    @components = split /\s*,\s*/, $raw_name;
	    
	    if ( $components[-1] =~ /^(?:&|and|et)\s*(.*)/ )
	    {
		$components[-1] = $1;
	    }
	}
	
	# Use the ParseConjunction function to handle "x & y" or "x and y". We handle
	# the first one separately because there are a few names of the form "a b and c
	# & d e and f" where "a b and c" and "d e and f" are both valid name components.
	
	elsif ( $raw_name =~ /(.*?)\s+&\s+(.*)/ )
	{
	    @components = ParseConjunction($1, $2);
	}
	
	elsif ( $raw_name =~ /(.*?)\s+and\s+(.*)/ )
	{
	    @components = ParseConjunction($1, $2);
	}
	
	# Otherwise, treat the name as a single component.
	
	else
	{
	    @components = $raw_name;
	}
	
	# Iterate through the individual components, assuming that each represents a
	# separate stratum name. For each one that appears to be a valid stratigraphic
	# name, add it to the %strat_name hash with the name component key as the key
	# and the raw name record as its value.
	
	while ( @components )
	{
	    my $c = shift @components;
	    my $alias;
	    my $is_subfm;
	    
	    # Separate out any alias that is specified in the component.
	    
	    if ( $c =~ /^(.*?)\s*[(](?:=|also|also called)?\s*(.*?)\s*[)]\s*$/ )
	    {
		$c = $1;
		$alias = $2;
	    }
	    
	    # Remove paired single quotes, but leave unpaired ones. The latter
	    # are part of the name, i.e. the 'Arqov formation in Israel.
	    
	    if ( $c =~ /'.*'(\s|$)/ )
	    {
		$c =~ s/'//g;
	    }
	    
	    # If the component ends in ' fm' or ' fm.' and the rank is 'Mbr', then
	    # it represents a sub-formation contained in a larger formation.
	    
	    if ( $nr->{rank} eq 'Mbr' && $c =~ /(.*)\s+fm[.]?$/i )
	    {
		$c = $1;
		$is_subfm = 1;
		$rank = 'Fm';
	    }
	    
	    # Ignore any name component that doesn't contain at least three
	    # letters in a row, and ignore roman numerals.
	    
	    next unless $c =~ /[[:alpha:]]{3}/;
	    next if $c =~ /^[ivx]+$/i;
	    
	    # Put the component into fold-case so that we can do case-insensitive
	    # comparisons.
	    
	    my $fc = fc $c;
	    
	    # Ignore any of the excluded name components, i.e. "upper" and "lower".
	    
	    next if $excluded_name{$fc};
	    
	    # Increment the component count for the name record.
	    
	    $nr->{n_names}++;
	    
	    # Generate a key for this name component consisting of the stratum name,
	    # rank, and country code. Store a reference to the name record in
	    # %strat_name under this key. As mentioned above, this makes %strat_name a
	    # many-to-many mapping.
	    
	    my $nkey = "$c|$rank|$nr->{cc}";
	    
	    $strat_name{$nkey} ||= [];
	    push $strat_name{$nkey}->@*, $nr;
	    
	    # Also store the name key in the hashes %first_two, and %first_last. The
	    # hash key for the former is the first two letters in fold-case, and the
	    # hash key for the latter is the first and last letters of the first word,
	    # provided it contains at least three letters. This will allow us to
	    # associate names together into concepts even if they have different ranks
	    # and/or are spelled slightly differently.
	    
	    my $ft = substr($fc, 0, 2);
	    
	    $first_two{$ft} ||= [];
	    push $first_two{$ft}->@*, $nkey;
	    
	    my @fl = ($fc =~ /^([[:alpha:]])\S*[[:alpha:]]\S*([[:alpha:]])/);
	    
	    if ( @fl )
	    {
		my $fl = $fl[0] . $fl[1];
		$first_last{$fl} ||= [];
		push $first_last{$fl}->@*, $nkey;
	    }
	    
	    # If an alias was specified, generate a second key and store it in the same
	    # way. Also store the relationship between the two keys in %alias_of. This
	    # will enable us to ensure later that the two names are associated with the
	    # same concept.

	    my $alias_key;
	    
	    if ( $alias && ! $excluded_name{fc $alias} )
	    {
		$alias_key = "$alias|$rank|$nr->{cc}";
		my $afc = fc $alias;
		
		$strat_name{$alias_key} ||= [];
		push $strat_name{$alias_key}->@*, $nr;
		
		# Associate the two keys together using the %alias_of hash. Do this such
		# that the hash key will be processed later than the hash value when the
		# keys are sorted alphabetically. This guarantees that when we look up
		# the hash key we will get a key that corresponds to an already
		# processed name record. Because the two keys have the same rank, they
		# will be sorted as part of the same sub-list.
		
		if ( fc($nkey) lt fc($alias_key) )
		{
		    $alias_of{$alias_key} = $nkey;
		}

		else
		{
		    $alias_of{$nkey} = $alias_key;
		}
		
		# Put the alias into the %first_two and %first_last hashes as well.
		
		my $aft = substr($afc, 0, 2);
		
		$first_two{$aft} ||= [];
		push $first_two{$aft}->@*, $alias_key;
		
		my @afl = ($afc =~ /^([[:alpha:]])\S*[[:alpha:]]\S*([[:alpha:]])/);
		
		if ( @afl )
		{
		    my $afl = $afl[0] . $afl[1];
		    $first_last{$afl} ||= [];
		    push $first_last{$afl}->@*, $nkey;
		}
	    }
	    
	    # Store the name key (and the alias key if there is one) under either
	    # @gp_keys, @fm_keys, @sfm_keys, or @mbr_keys. This allows the names to be
	    # processed in rank order, so that parents are resolved before children.
	    
	    if ( $nr->{rank} eq 'Gp' )
	    {
		$gp_keys{$nkey} = 1;
		$gp_keys{$alias_key} = 1 if $alias_key;
	    }
	    
	    elsif ( $is_subfm )
	    {
		$sfm_keys{$nkey} = 1;
	    }
	    
	    elsif ( $nr->{rank} eq 'Fm' )
	    {
		$fm_keys{$nkey} = 1;
		$fm_keys{$alias_key} = 1 if $alias_key;
	    }
	    
	    else
	    {
		$mbr_keys{$nkey} = 1;
		$mbr_keys{$alias_key} = 1 if $alias_key;
	    }
	}
    }
    
    
    # Step III: Iterate through the %strat_name relation, which is a subset of the cross
    # product between name component keys and stratigraphic name records. Each
    # unconsolidated member of this relation will be assigned to a preliminary name
    # concept record using the 'concept' field of the stratigraphic name record,
    # subdivided by name component key.
    
    # The iteration starts with keys that represent groups, then formations, then
    # sub-formations, then members. This guarantees that parents will be resolved before
    # children, so that the 'same parents' relation is always correct.
        
    say "Assigning preliminary concepts...";
    
    my $progress_rank = '';
    my $preliminary_concept_no = 0;
    
    foreach my $nkey ( (sort { fc($a) cmp fc($b) } keys %gp_keys),
		       (sort { fc($a) cmp fc($b) } keys %fm_keys),
		       (sort { fc($a) cmp fc($b) } keys %sfm_keys),
		       (sort { fc($a) cmp fc($b) } keys %mbr_keys) )
    {
	my ($name, $rank, $cc) = split /\|/, $nkey;
	
	# Note the progress through the ranks.
	
	if ( $rank ne $progress_rank )
	{
	    say "  Processing names of rank '$rank'...";
	    $progress_rank = $rank;
	}
	
	# If there are multiple stratigraphic name records for a given key, start by
	# consolidating them if possible. For example, if we have name records whose raw
	# names are 'A', 'B', and 'A or B', then when we get to name component 'A' we
	# consolidate the information from the 'A or B' name record into the 'A' name
	# record. When we get to name component 'B', we consolidate the same information
	# from the 'A or B' name record into the 'B' name record. In this case, the 'A
	# or B' name record will end up orphaned, with no remaining references to it in
	# the %strat_name hash.
	
	if ( $strat_name{$nkey}->@* > 1 )
	{
	    # Look for a record with only a single associated name component. Keep track
	    # of all the others as well.
	    
	    my ($single, @rest, @separate);
	    
	    foreach my $rn ( $strat_name{$nkey}->@* )
	    {
		if ( $rn->{n_names} == 1 && ! $single )
		{
		    $single = $rn;
		}

		else
		{
		    push @rest, $rn;
		}
	    }
	    
	    # If we couldn't find a record with just one name component, construct a
	    # placeholder for the other record(s) to be consolidated into.
	    
	    unless ( $single )
	    {
		$single = { name => $name, rank => $rank, n_names => 1, cc => $cc,
			    n_colls => 0, n_occs => 0, lithology1 => { }, lithology2 => { },
			    reference_no => { } };
	    }
	    
	    # Consolidate all the other records with the first one, or else keep them
	    # separate if they are not compatible geographically or temporally with the
	    # first one.
	    
	    foreach my $other ( @rest )
	    {
		if ( ContextsAreCompatible($single, $other) )
		{
		    ConsolidateNames($single, $other);
		}

		else
		{
		    push @separate, $other;
		}
	    }
	    
	    $strat_name{$nkey} = [$single, @separate];
	}
	
	# For each key, iterate through the consolidated stratigraphic name records
	# associated with that key. Assign each name record to a stratigraphic concept.
	# In the code below, $nr points to a stratigraphic name record, while $cr points
	# to a stratigraphic concept record.
	
      RECORD:
	foreach my $nr ( $strat_name{$nkey}->@* )
	{
	    # Generate a list of existing concepts that match this name record.
	    
	    my (%matching_concepts);
	    
	    # If this component name key is an alias for another key that has already
	    # been assigned to a concept, then add that concept to the list.
	    
	    if ( my $orig_key = $alias_of{$nkey} )
	    {
		foreach my $orig_rn ( $strat_name{$orig_key}->@* )
		{
		    if ( my $orig_preliminary_no = $orig_rn->{preliminary_no}{$orig_key} )
		    {
			$matching_concepts{$orig_preliminary_no} = 1;
		    }
		}
	    }
	    
	    # Check for other name records that could represent the same name or a
	    # spelling variant, possibly at a different rank.
	    
	    my @candidates;
	    
	    my $ft = fc(substr($name, 0, 2));
	    push @candidates, $first_two{$ft}->@* if $first_two{$ft};
	    
	    my @fl = ($ft =~ /^([[:alpha:]])\S*[[:alpha:]]\S*([[:alpha:]])/);
	    
	    if ( @fl )
	    {
		my $fl = fc($fl[0] . $fl[1]);
		push @candidates, $first_last{$fl}->@* if $first_last{$fl};
	    }
	    
	    foreach my $alt_key ( @candidates )
	    {
		my ($alt_name, $alt_rank) = split /\|/, $alt_key;
		
		# Skip any potential name which is not compatible with the name being
		# checked. Compatibility is indicated by various measures such as having
		# a small edit distance between the names, having the same stratigraphic
		# parents, being geographically and temporally close, etc.

		foreach my $alt_nr ( $strat_name{$alt_key}->@* )
		{
		    if ( NamesAreCompatible($name, $nr, $alt_name, $alt_nr) )
		    {
			if ( my $alt_preliminary_no = $alt_nr->{preliminary_no}{$alt_key} )
			{
			    $matching_concepts{$alt_preliminary_no} = 1;
			}
		    }
		}
	    }
	    
	    # Now, if there is exactly one matching concept, associate the component name
	    # key with this concept.
	    
	    if ( keys %matching_concepts == 1 )
	    {
		my $preliminary_no = (keys %matching_concepts)[0];
		AddToConcept($strat_prelim{$preliminary_no}, $nr, $nkey);
	    }
	    
	    # If there is more than one matching concept, then this name bridges
	    # multiple concepts and we must consolidate them. The one with the lowest
	    # concept number will be selected, and the rest will be consolidated with
	    # it.
	    
	    elsif ( keys %matching_concepts > 1 )
	    {
		my ($first, @rest) = sort { $a <=> $b } keys %matching_concepts;
		
		foreach my $preliminary_no ( @rest )
		{
		    $strat_prelim{$preliminary_no}{consolidated_with} = $first;
		}
		
		AddToConcept($strat_prelim{$first}, $nr, $nkey);
	    }
	    
	    # If there aren't any matching concepts, create a new stratigraphic concept
	    # record and associate the component name key with it.
	    
	    else
	    {
		my $preliminary_no = ++$preliminary_concept_no;
		CreateConcept($nr, $nkey, "${preliminary_no}A");
	    }
	}
    }
    
    say "Found $parent_matches parent matches.";
    
    
    # Step IV: generate the set of final stratigraphic concepts from the set of
    # preliminary concepts. This involves consolidating the information from concepts
    # that have been linked together in the previous step, and choosing a name for each
    # concept from among the stratigraphic name components associated with it.
    
    say "Generating final concepts...";
    
    # Iterate through the preliminary concept records, and consolidate the information
    # from concepts that have been linked together in Step III. Generate the relation
    # %prelim_to_consolidated, mapping preliminary numbers to consolidated concept
    # records. The @consolidated_concepts array lists all of the consolidated concept
    # records.
    
    my (%prelim_to_concept, @consolidated_concepts, %concept_by_name);
    
    foreach my $preliminary_no ( keys %strat_prelim )
    {
	my $rc = $strat_prelim{$preliminary_no};
	my $canonical = $rc;
	
	while ( $canonical->{consolidated_with} )
	{
	    $canonical = $strat_prelim{$canonical->{consolidated_with}};
	}
	
	if ( $canonical ne $rc )
	{
	    ConsolidateConcepts($canonical, $rc);
	    $prelim_to_concept{$preliminary_no} = $canonical;
	}
	
	else
	{
	    $prelim_to_concept{$preliminary_no} = $rc;
	    push @consolidated_concepts, $rc;
	}
    }
    
    # Now iterate through the consolidated concept records and choose a name for each
    # one. We do this before generating final concept numbers, because we want those to
    # be assigned to the concepts in alphabetical order. Note that multiple concepts may
    # end up with the same name, because the same name may be used differently in
    # different parts of the world.
    
    foreach my $cr ( @consolidated_concepts )
    {
	$cr->{name} = ChooseConceptName($cr);
	$concept_by_name{$cr->{name}} ||= [ ];
	push $concept_by_name{$cr->{name}}->@*, $cr;
    }
    
    # Finally, iterate through the concept names and assign a final concept number to
    # each consolidated concept record using the %strat_concept hash.
    
    my $real_concept_no = 0;
    
    foreach my $name ( sort { fc($a) cmp fc($b) } keys %concept_by_name )
    {
	foreach my $cr ( $concept_by_name{$name}->@* )
	{
	    $cr->{concept_no} = ++$real_concept_no;
	    $strat_concept{$real_concept_no} = $cr;
	}
    }
    
    # # For each stratigraphic concept record that has not been consolidated with another,
    # # assign it a unique concept number and store it under this number in the
    # # %strat_concept hash. Assign the concept numbers in the same order as the
    # # preliminary concept numbers, and generate %prelim_to_real as a mapping from
    # # preliminary concept numbers to real ones.
    
    # foreach my $preliminary_no ( sort { $a <=> $b } keys %strat_prelim )
    # {
    # 	my $cr = $strat_prelim{$preliminary_no};
	
    # 	unless ( $cr->{consolidated_with} )
    # 	{
    # 	    $cr->{concept_no} = ++$real_concept_no;
    # 	    $strat_concept{$real_concept_no} = $cr;
    # 	    $prelim_to_real{$preliminary_no} = $real_concept_no;
    # 	}
    # }
    
    # # Now go through the preliminary concepts again and actually consolidate the concept
    # # information. Add the records being consolidated to the %prelim_to_real mapping.
    
    
    # Step V: link up the consolidated stratigraphic concepts into hierarchical
    # relationships, using the containment relation that was generated in step I above.
    
    say "Computing concept relationships...";
    
    # Iterate through the final stratigraphic name concepts. For each one, generate all
    # possible relationships based on the list of raw name keys associated with it along
    # with the %contained_in relation on those keys.

    foreach my $cr ( @consolidated_concepts )
    {
	my @child_name_keys = $cr->{rkeys}->@*;
	
	foreach my $child_key ( @child_name_keys )
	{
	    my (undef, $child_rank, $child_cc) = split /[|]/, $child_key;
	    
	    if ( $contained_in{$child_key} )
	    {
		my @parent_keys = keys $contained_in{$child_key}->%*;
		my @canonical_parent_keys;
		
		foreach my $parent_key ( @parent_keys )
		{
		    my $nr = $strat_raw{$parent_key};
		    
		    next unless $nr->{n_names} == 1;

		    if ( $nr->{consolidated_with} )
		    {
			push @canonical_parent_keys, $nr->{consolidated_with}->@*;
		    }

		    else
		    {
			push @canonical_parent_keys, $parent_key;
		    }
		}
		
		foreach my $parent_key ( @canonical_parent_keys )
		{
		    my (undef, $parent_rank, $parent_cc) = split /[|]/, $parent_key;
		    my ($preliminary_no, @rest) = values $strat_raw{$parent_key}{preliminary_no}->%*;
		    
		    while ( @rest && $rest[0] eq $preliminary_no )
		    {
			shift @rest;
		    }
		    
		    if ( @rest )
		    {
			say "Ambiguous concept for '$parent_key'";
		    }
		    
		    unless ( $preliminary_no )
		    {
			say "No preliminary_no found for '$parent_key'";
			next;
		    }
		    
		    my $parent_no = $prelim_to_concept{$preliminary_no}{concept_no};
		    my @reference_nos = keys $contained_in{$child_key}{$parent_key}->%*;
		    
		    if ( $child_cc ne $parent_cc )
		    {
			say "Bad cc match for '$child_key' and '$parent_key'";
			next;
		    }

		    unless ( $parent_no )
		    {
			say "No parent_no found for '$parent_key'";
			next;
		    }
		    
		    next if $parent_no == $cr->{concept_no};
		    
		    my $relationship_key = "$child_rank|$child_cc|$parent_rank|$parent_no";
		    
		    $cr->{parent_concept}{$relationship_key} = 1;
		    $cr->{parent_refs}{$relationship_key}{$_} = 1 foreach @reference_nos;
		}
	    }
	}
    }
    
    
    # Step VI: (re)generate the STRAT_NAMES, STRAT_CONCEPTS, and STRAT_OPINIONS tables.
    
    # First, empty these tables so we can refill them from our computed data.
    
    $DB::single = 1;
    
    say "Emptying tables: '$TABLE{STRAT_NAMES}', '$TABLE{STRATN_REFS}', '$TABLE{STRAT_CONCEPTS}', " .
	"'$TABLE{STRAT_OPINIONS}', '$TABLE{STRATO_REFS}'...";
    
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_NAMES}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_NREFS}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_CONCEPTS}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_OPINIONS}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_OREFS}");
    
    # Then go through the list of concepts, and add one row to the STRAT_CONCEPTS table
    # per concept. For efficiency, these rows are added in blocks of roughly $chunk_size
    # characters instead of one at a time. The last call to InsertConcepts adds any
    # remaining rows after the last block.
    
    say "Generating the STRAT_CONCEPTS ($TABLE{STRAT_CONCEPTS}) table...";
    
    my $concept_values = '';
    my $new_concepts = 0;
    my $concept_colls = 0;
    my $concept_occs = 0;
    
    foreach my $key ( sort { $a <=> $b } keys %strat_concept )
    {
	my $cr = $strat_concept{$key};
	
	$new_concepts++;
	
	$concept_colls += $cr->{n_colls};
	$concept_occs += $cr->{n_occs};
	
	my $qstratc = $pbdb->quote($cr->{concept_no});
	my $qname = $pbdb->quote(ChooseConceptName($cr));
	my $qcc = $pbdb->quote(join ',', keys $cr->{cc}->%*);
	my $qcou = $pbdb->quote('');
	my $qlith1 = $pbdb->quote(join ',', keys $cr->{lithology1}->%*);
	my $qlith2 = $pbdb->quote(join ',', keys $cr->{lithology2}->%*);
	my $qncolls = $pbdb->quote($cr->{n_colls} || 0);
	my $qnoccs = $pbdb->quote($cr->{n_occs} || 0);
	my $qearly = $pbdb->quote($cr->{early_age} || '');
	my $qlate = $pbdb->quote($cr->{late_age} || '');
	my $qlatmin = $pbdb->quote($cr->{lat_min});
	my $qlatmax = $pbdb->quote($cr->{lat_max});
	my $qlngmin = $pbdb->quote($cr->{lng_min});
	my $qlngmax = $pbdb->quote($cr->{lng_max});
	
	$concept_values .= ', ' if $concept_values;
	$concept_values .= "($qstratc, $qname, $qcc, $qcou, $qlith1, $qlith2, $qncolls, $qnoccs, " .
	    "$qearly, $qlate, $qlatmin, $qlatmax, $qlngmin, $qlngmax)";

	if ( length($concept_values) > $chunk_size )
	{
	    InsertConcepts($pbdb, $concept_values);
	    $concept_values = '';
	}
    }
    
    InsertConcepts($pbdb, $concept_values) if $concept_values;
    
    # Then go through the list of names, and add one row to the STRAT_NAMES table for
    # each name. As before, these rows are added in large blocks. The association
    # between each name and its set of reference_no values is stored in the STRATN_REFS
    # table.
    
    say "Generating the STRAT_NAMES ($TABLE{STRAT_NAMES}) and " .
	"STRATN_REFS ($TABLE{STRAT_NREFS}) tables...";
    
    my $name_values = '';
    my $name_ref_values = '';
    my $new_names = 0;
    my $name_colls = 0;
    my $name_occs = 0;
    my $missing_concept = 0;
    
    foreach my $key ( sort keys %strat_name )
    {
	my ($name, $rank, $cc) = split /\|/, $key;
	
	foreach my $nr ( $strat_name{$key}->@* )
	{
	    my $concept_no = $prelim_to_concept{$nr->{preliminary_no}{$key}}{concept_no};
	    my $name_no = ++$new_names;
	    
	    $name_colls += $nr->{n_colls};
	    $name_occs += $nr->{n_occs};
	    
	    unless ( $concept_no )
	    {
		$missing_concept++;
		next;
	    }
	    
	    my $qstratn = $pbdb->quote($name_no);
	    my $qstratc = $pbdb->quote($concept_no);
	    my $qname = $pbdb->quote($name);
	    my $qrank = $pbdb->quote($rank);
	    my $qcc = $pbdb->quote($nr->{cc});
	    my $qlith1 = $pbdb->quote(join ',', keys $nr->{lithology1}->%*);
	    my $qlith2 = $pbdb->quote(join ',', keys $nr->{lithology2}->%*);
	    my $qcou = $pbdb->quote($nr->{country} || '');
	    my $qncolls = $pbdb->quote($nr->{n_colls} || 0);
	    my $qnoccs = $pbdb->quote($nr->{n_occs} || 0);
	    my $qearly = $pbdb->quote($nr->{early_age} || '');
	    my $qlate = $pbdb->quote($nr->{late_age} || '');
	    my $qlatmin = $pbdb->quote($nr->{lat_min});
	    my $qlatmax = $pbdb->quote($nr->{lat_max});
	    my $qlngmin = $pbdb->quote($nr->{lng_min});
	    my $qlngmax = $pbdb->quote($nr->{lng_max});
	    
	    $name_values .= ', ' if $name_values;
	    $name_values .= "($qstratn, $qstratc, $qname, $qrank, $qcc, $qcou, $qlith1, $qlith2, " .
		"$qncolls, $qnoccs, $qearly, $qlate, $qlatmin, $qlatmax, $qlngmin, $qlngmax)";
	    
	    if ( length($name_values) > $chunk_size )
	    {
		InsertNames($pbdb, $name_values);
		$name_values = '';
	    }

	    foreach my $reference_no ( sort { $a <=> $b } keys $nr->{reference_no}->%* )
	    {
		my $qrefno = $pbdb->quote($reference_no);
		$name_ref_values .= ', ' if $name_ref_values;
		$name_ref_values .= "($qstratn, $qrefno)";
	    }
	    
	    if ( length($name_ref_values) > $chunk_size )
	    {
		InsertNameRefs($pbdb, $name_ref_values);
		$name_ref_values = '';
	    }
	}
    }
    
    InsertNames($pbdb, $name_values) if $name_values;
    InsertNameRefs($pbdb, $name_ref_values) if $name_ref_values;

    # Now run through the concepts again and add an opinion for every child-parent
    # relationship.
    
    say "Generating the STRAT_OPINIONS ($TABLE{STRAT_OPINIONS}) and " .
	"STRAT_OREFS ($TABLE{STRAT_OREFS}) tables...";
    
    my $opinion_values = '';
    my $opinion_ref_values = '';
    my $new_opinions = 0;
    
    foreach my $child_no ( sort { $a <=> $b } keys %strat_concept )
    {
	my $cr = $strat_concept{$child_no};
	
	foreach my $relationship ( keys $cr->{parent_concept}->%* )
	{
	    my ($child_rank, $cc, $parent_rank, $parent_no) = split /[|]/, $relationship;
	    
	    my $qopno = $pbdb->quote(++$new_opinions);
	    my $qchildno = $pbdb->quote($child_no);
	    my $qchildrank = $pbdb->quote($child_rank);
	    my $qcc = $pbdb->quote($cc);
	    my $qparno = $pbdb->quote($parent_no);
	    my $qparrank = $pbdb->quote($parent_rank);
	    
	    $opinion_values .= ', ' if $opinion_values;
	    $opinion_values .= "($qopno, $qchildno, $qchildrank, $qcc, 'belongs to', " .
		"$qparno, $qparrank)";
	    
	    foreach my $reference_no ( sort { $a <=> $b } keys $cr->{parent_refs}{$relationship}->%* )
	    {
		my $qrefno = $pbdb->quote($reference_no);
		$opinion_ref_values .= ', ' if $opinion_ref_values;
		$opinion_ref_values .= "($qopno, $qrefno)";
	    }
	}
	
	if ( length($opinion_values) > $chunk_size )
	{
	    InsertOpinions($pbdb, $opinion_values);
	    $opinion_values = '';
	}
	
	if ( length($opinion_ref_values) > $chunk_size )
	{
	    InsertOpinionRefs($pbdb, $opinion_ref_values);
	    $opinion_ref_values = '';
	}
    }
    
    InsertOpinions($pbdb, $opinion_values) if $opinion_values;
    InsertOpinionRefs($pbdb, $opinion_ref_values) if $opinion_ref_values;
    
    # foreach my $key ( sort keys %strat_name )
    # {
    # 	my ($name, $rank, $cc) = split /\|/, $key;
	
    # 	foreach my $rn ( $strat_name{$key}->@* )
    # 	{
    # 	    next unless $rn->{parent_rn};
	    
    # 	    foreach my $parent_rkey ( keys $rn->{parent_rn}{$key}->%* )
    # 	    {
    # 		my $qopno = $pbdb->quote(++$new_opinions);
    # 		my $qchildno = $pbdb->quote($rn->{name_no}{$key});
    # 		my $qchildrank = $pbdb->quote($rank);
		
    # 		my $parent_rn = $strat_raw{$parent_rkey};
		
    # 		my ($parent_key) = keys $parent_rn->{concept}->%*;
		
    # 		unless ( $parent_key && $parent_rn->{name_no}{$parent_key} )
    # 		{
    # 		    # say encode_utf8("No name key for parent $parent_rkey");
    # 		    next;
    # 		}
		
    # 		my ($parent_name, $parent_rank, $parent_cc) = split /\|/, $parent_key;
    # 		my $qparno = $pbdb->quote($parent_rn->{name_no}{$parent_key});
    # 		my $qparrank = $pbdb->quote($parent_rank);
    # 		my $qrefno = $pbdb->quote(0);
		
    # 		$opinion_values .= ', ' if $opinion_values;
    # 		$opinion_values .= "($qopno, $qchildno, $qchildrank, 'belongs to', " .
    # 		    "$qparno, $qparrank, $qrefno)";
    # 	    }
	    
    # 	    if ( length($opinion_values) > 50000 )
    # 	    {
    # 		InsertOpinions($pbdb, $opinion_values);
    # 		$opinion_values = '';
    # 	    }
    # 	}
    # }
    
    # InsertOpinions($pbdb, $opinion_values);
        
    say "WARNING: $missing_concept names had no concept" if $missing_concept > 0;
    say "Created $new_names names";
    say "Created $new_concepts concepts";
    say "Created $new_opinions concept relationships";
    
    # my $words_fh;
    
    # if ( open($words_fh, '>', 'second_words.txt') )
    # {
    # 	say $words_fh encode_utf8($_) foreach sort keys %second_words;
    # }
    
    # else
    # {
    # 	say "Could not open second_words.txt: $!";
    # }
}


# UpdateStratRaw ( key, source, field )
#
# Create a new record in %strat_raw with the given key, or expand an existing record if
# there is already one with that key, using information from the source record. The
# $field parameter specifies which field from the source record contains the raw
# stratigraphic name. Only create records for names which contain at least three
# letters in a row, are not Roman numerals, and are not 'unnamed'.
#
# Return true if a record was created or expanded, false otherwise.

sub UpdateStratRaw {

    my ($key, $source, $field) = @_;

    my ($record);
    
    if ( $record = $strat_raw{$key} )
    {
	# The 'early_age' and 'late_age' fields are the max and min respectively of the
	# aggregate values.
	
	$record->{early_age} = $source->{early_age}
	    if ! defined $record->{early_age} || $source->{early_age} > $record->{early_age};
	$record->{late_age} = $source->{late_age}
	    if ! defined $record->{late_age} || $source->{late_age} < $record->{late_age};
	
	# Same for the lat/lng fields.
	
	$record->{lat_min} = $source->{lat_min}
	    if ! defined $record->{lat_min} || $source->{lat_min} < $record->{lat_min};
	$record->{lat_max} = $source->{lat_max}
	    if ! defined $record->{lat_max} || $source->{lat_max} > $record->{lat_max};
	$record->{lng_min} = $source->{lng_min}
	    if ! defined $record->{lng_min} || $source->{lng_min} < $record->{lng_min};
	$record->{lng_max} = $source->{lng_max}
	    if ! defined $record->{lng_max} || $source->{lng_max} > $record->{lng_max};

	# The 'n_colls' and 'n_occs' fields are the sum of the aggregate values.
	
	$record->{n_colls} += $source->{n_colls};
	$record->{n_occs} += $source->{n_occs};
    }
    
    else
    {
	my $name = $source->{$field};
	
	return '' unless $name =~ /[[:alpha:]]{3}/;
	return '' if $name =~ /^[ivx]+$|^unnamed$/i;
	
	my $rank = $field eq 'grp'       ? 'Gp'
	         : $field eq 'formation' ? 'Fm'
		 :                         'Mbr';
	
	$record = $strat_raw{$key} =
		{ name => $source->{$field}, rank => $rank, cc => $source->{cc}, rkey => $key,
		  early_age => $source->{early_age}, late_age => $source->{late_age},
		  lat_min => $source->{lat_min}, lat_max => $source->{lat_max},
		  lng_min => $source->{lng_min}, lng_max => $source->{lng_max},
		  n_colls => $source->{n_colls}, n_occs => $source->{n_occs},
		  reference_no => { }, lithology1 => { }, lithology2 => { } };
    }
    
    # The 'reference_no', 'lithology1', and 'lithology2' fields are the union of the
    # respective aggregate values, expressed as a hash.
    
    $record->{reference_no}{$_} = 1 foreach grep { $_ } split /,/, $source->{reference_no};
    $record->{lithology1}{$_} = 1 foreach grep { $_ && $_ ne 'not reported' }
	split /,/, $source->{lithology1};
    $record->{lithology2}{$_} = 1 foreach grep { $_ } split /,/, $source->{lithology2};
    
    return 1;
}


# UpdateContainedIn ( contained_key, container_key, source )
#
# Store one element of the 'contained in' relation, between the stratigraphic name
# record associated with $contained_key and the stratigraphic name record associated
# with $container_key. If such an element already exists, expand it using information from
# the source record. Currently, the only information we store about each element of this
# relation is a set of reference_no values, representing bibliographic references where
# this relationship appears.
# 
# This relation is stored in two ways: in the %contained_in hash, and under the
# 'parent_name' key in the contained stratigraphic name record.

sub UpdateContainedIn {

    my ($contained_key, $container_key, $source) = @_;
    
    my $record = ( $contained_in{$contained_key}{$container_key} ||= { } );
    
    if ( $source->{reference_no} )
    {
	$record->{$_} = 1 foreach grep { $_ } split /,/, $source->{reference_no};
    }
    
    # if ( $strat_raw{$contained_key} )
    # {
    # 	$strat_raw{$contained_key}{parent_name}{$container_key} = 1;
    # }
}


# ConsolidateNames ( remaining, other )
#
# Extend the name record referred to by $remaining using the information from the name
# record referred to by $other. The latter will be abandoned, unless it is the sole
# record associated with some other name component.

sub ConsolidateNames {

    my ($remaining, $other) = @_;
    
    $remaining->{lat_min} = $other->{lat_min}
	if ! defined $remaining->{lat_min} || $other->{lat_min} < $remaining->{lat_min};
    $remaining->{lat_max} = $other->{lat_max}
	if ! defined $remaining->{lat_max} || $other->{lat_max} > $remaining->{lat_max};
    $remaining->{lng_min} = $other->{lng_min}
	if ! defined $remaining->{lng_min} || $other->{lng_min} < $remaining->{lng_min};
    $remaining->{lng_max} = $other->{lng_max}
	if ! defined $remaining->{lng_max} || $other->{lng_max} > $remaining->{lng_max};

    $remaining->{early_age} = $other->{early_age}
	if ! defined $remaining->{early_age} || $other->{early_age} > $remaining->{early_age};
    $remaining->{late_age} = $other->{late_age}
	if ! defined $remaining->{late_age} || $other->{late_age} < $remaining->{late_age};
    
    $remaining->{n_colls} += $other->{n_colls};
    $remaining->{n_occs} += $other->{n_occs};

    # if ( $other->{parent_name} )
    # {
    # 	$remaining->{parent_name}{$_} = 1 foreach keys $other->{parent_name}->%*;
    # }

    if ( $other->{reference_no} )
    {
	$remaining->{reference_no}{$_} = 1 foreach keys $other->{reference_no}->%*;
    }
    
    if ( $other->{lithology1} )
    {
	$remaining->{lithology1}{$_} = 1 foreach keys $other->{lithology1}->%*;
    }
    
    if ( $other->{lithology2} )
    {
	$remaining->{lithology2}{$_} = 1 foreach keys $other->{lithology2}->%*;
    }
    
    $other->{consolidated_with} ||= [ ];
    push $other->{consolidated_with}->@*, $remaining->{rkey};
}


# ParseConjunction ( a, b )
# 
# Parse the two parts of a conjunction, i.e. "A B and X". These could be two
# separate stratigraphic names, or alternatively two names in the form "X Silts and
# Shales".

sub ParseConjunction {

    my ($a, $b) = @_;
    
    if ( $a =~ /^Walpen\b|^Wall\b|^Daleje\b/ )
    {
	return ("$a and $b");
    }
    
    if ( IsRockType($b) )
    {
	return ("$a and $b");
    }
    
    if ( $b =~ /(.*?)\s+suites?$/i )
    {
	$b = $1;
	substr($b, 0, 1) = uc substr($b, 0, 1);
	
	return ("$a Suite", "$b Suite");
    }
    
    if ( $b =~ /(.*?)\s+horizons?$/i )
    {
	$b = $1;
	substr($b, 0, 1) = uc substr($b, 0, 1);
	
	return ("$a Horizon", "$b Horizon");
    }
    
    if ( $b =~ /(.*?) & (.*)/ )
    {
	return ("$a $1", $2);
    }
    
    return ($a, $b);
}


# NamesAreCompatible ( name, key, name_context, alt_name, alt_key, alt_name_context )
#
# Return true if name and alt_name are likely to be variants of each other.

sub NamesAreCompatible {

    my ($name, $nr, $alt_name, $alt_nr) = @_;

    # Convert both names to fold-case, to make this comparison case-insensitive.

    $name = fc $name;
    $alt_name = fc $alt_name;
    
    # If the two names are the same, return true.

    if ( $name eq $alt_name )
    {
	return 1;
    }
    
    my $similarities = 0;
    
    my ($same_prefix, $same_parents, $same_rank,
	$ages_overlap, $ages_identical, $locations_close, $locations_overlap);
    
    # If the age ranges overlap, the two names are potentially compatible.
    
    if ( $nr->{early_age} >= $alt_nr->{late_age} &&
	 $nr->{late_age} <= $alt_nr->{early_age} )
    {
	$ages_overlap = 1;
	$similarities++;
    }
    
    # If the age ranges are exactly the same or very close, the two names are even more
    # likely to be compatible.
    
    if ( $nr->{early_age} == $alt_nr->{early_age} &&
	 abs($nr->{late_age} - $alt_nr->{late_age}) < 5 ||
	 $nr->{late_age} == $alt_nr->{late_age} &&
	 abs($nr->{early_age} - $alt_nr->{early_age}) < 5 )
    {
	$ages_identical = 1;
	$similarities++;
    }
    
    # If the geographic ranges overlap or are very close to each other, the two names are
    # potentially compatible.

    if ( $nr->{cc} eq $alt_nr->{cc} )
    {
	$locations_close = 1;
	$similarities++;
    }
    
    elsif ( $nr->{lat_min} <= $alt_nr->{lat_max} + 5 &&
	    $nr->{lat_max} >= $alt_nr->{lat_min} - 5 &&
	    $nr->{lng_min} <= $alt_nr->{lng_max} + 5 &&
	    $nr->{lng_max} >= $alt_nr->{lng_min} - 5 )
    {
	$locations_close = 1;
	$similarities++;
    }
    
    # If the geographic ranges are very close to each other, the two names are even more
    # likely to be compatible.
    
    if ( $nr->{lat_min} <= $alt_nr->{lat_max} + 1 &&
	 $nr->{lat_max} >= $alt_nr->{lat_min} - 1 &&
	 $nr->{lng_min} <= $alt_nr->{lng_max} + 1 &&
	 $nr->{lng_max} >= $alt_nr->{lng_min} - 1 )
    {
	$locations_overlap = 1;
	$similarities++;
    }

    # At this point, reject the match unless we have at least one geographic or
    # temporal similarity.
    
    return '' unless $similarities > 0;
    
    # If the two names have the same rank, they are potentially compatible.
    
    if ( $nr->{rank} eq $alt_nr->{rank} )
    {
	$same_rank = 1;
	$similarities++;
    }
    
    # If the two names have identical stratigraphic parents, they are potentially
    # compatible.
    
    # if ( $nr->{parent_name} && $alt_nr->{parent_name} )
    # {
    # 	my (@parents, @alt_parents, $rkey);
	
    # 	foreach $rkey ( keys $nr->{parent_name}->%* )
    # 	{
    # 	    my $anr = $strat_raw{$rkey};
	    
    # 	    if ( $anr->{consolidated_with} )
    # 	    {
    # 		push @parents, $anr->{consolidated_with}->@*;
    # 	    }
	    
    # 	    else
    # 	    {
    # 		push @parents, $rkey;
    # 	    }
    # 	}
	
    # 	foreach $rkey ( keys $alt_nr->{parent_name}->%* )
    # 	{
    # 	    my $bnr = $strat_raw{$rkey};

    # 	    if ( $bnr->{consolidated_with} )
    # 	    {
    # 		push @alt_parents, $bnr->{consolidated_with}->@*;
    # 	    }
	    
    # 	    else
    # 	    {
    # 		push @alt_parents, $rkey;
    # 	    }
    # 	}

    if ( $contained_in{$nr->{rkey}} && $contained_in{$alt_nr->{rkey}} )
    {
	my (@parents, @alt_parents, $rkey);
	
	foreach $rkey ( keys $contained_in{$nr->{rkey}}->%* )
	{
	    my $anr = $strat_raw{$rkey};
	    
	    if ( $anr->{consolidated_with} )
	    {
		push @parents, $anr->{consolidated_with}->@*;
	    }
	    
	    else
	    {
		push @parents, $rkey;
	    }
	}
	
	foreach $rkey ( keys $contained_in{$nr->{rkey}}->%* )
	{
	    my $bnr = $strat_raw{$rkey};

	    if ( $bnr->{consolidated_with} )
	    {
		push @alt_parents, $bnr->{consolidated_with}->@*;
	    }
	    
	    else
	    {
		push @alt_parents, $rkey;
	    }
	}
	
      KEY:
	foreach my $a ( @parents )
	{
	    foreach my $b ( @parents )
	    {
		if ( $a eq $b )
		{
		    $same_parents = 1;
		    $parent_matches++;
		    last KEY;
		}
	    }
	}
    }
    
    # At this point, reject the match unless we have at least two geographic,
    # stratigraphic, or temporal similarities.

    return '' unless $similarities > 1;
    
    # If the names differ in a sequence that includes numbers (including roman numerals)
    # possibly followed by letters, they are not compatible. That means they are
    # individually lettered or numbered members.
    
    my @sequence = $name =~ /(\d+[[:alpha:]]*|\b[ivx]+\b)/g;
    my @alt_sequence = $alt_name =~ /(\d+[[:alpha:]]*|\b[ivx]+\b)/g;
    
    if ( @sequence != @alt_sequence )
    {
	return '';
    }

    elsif ( @sequence )
    {
	foreach my $i ( 0..$#sequence )
	{
	    if ( $sequence[$i] ne $alt_sequence[$i] )
	    {
		return '';
	    }
	}
    }
    
    # If the two names start with the same three letters, they are potentially compatible.
    
    if ( substr($name, 0, 3) eq substr($alt_name, 0, 3) )
    {
	$same_prefix = 1;
	$similarities++;
    }
    
    # Otherwise, if the initial words have an edit distance of 2 or less, the names are
    # potentially compatible.
    
    else
    {
	my ($first) = $name =~ /(\S+)/;
	my ($alt_first) = $alt_name =~ /(\S+)/;
	
	if ( edistance($first, $alt_first, 2) >= 0 )
	{
	    $same_prefix = 1;
	    $similarities++;
	}
    }
    
    # If the two names are of the form "X" and "X Y" where Y is a rock type, then the
    # two names are compatible. If the two names are of the form "X Y" and "X Z" where Y
    # and Z are rock types, then the two names are compatible. But this test is only
    # done if the two names have either identical parents or else overlapping ages.

    if ( $same_prefix && ($same_parents || $ages_overlap) )
    {
	my @smaller = $name =~ /(\S+)/g;
	my @larger = $alt_name =~ /(\S+)/g;
	my @prefix;
	
	if ( @smaller > @larger )
	{
	    my @temp = @smaller;
	    @smaller = @larger;
	    @larger = @temp;
	}
	
	while ( $smaller[0] eq $larger[0] || edistance($smaller[0], $larger[0], 2) >= 0 )
	{
	    push @prefix, shift @smaller;
	    shift @larger;
	    last unless @smaller;
	}
	
	if ( @smaller == 0 && $same_parents && $ages_overlap )
	{
	    return 1;
	}
	
	elsif ( @smaller == 0 && @larger && @larger == grep { IsRockType($_) } @larger )
	{
	    return 1;
	}
	
	elsif ( @smaller == 0 && @larger == 1 && $allowed_suffix{$larger[0]} )
	{
	    return 1;
	}
	
	elsif ( @smaller && @larger && $same_rank &&
		(@smaller == grep { IsRockType($_) } @smaller) &&
		(@larger == grep { IsRockType($_) } @larger) )
	{
	    return 1;
	}
    }
    
    # If the names have a Levenshtein-Damerau edit distance of 1 or 2, then the two
    # names are compatible. But only if they either have the same parents or else their
    # ages and locations both overlap.
    
    my $ldd = edistance($name, $alt_name, 2);
    
    if ( $ldd >= 0 && $ldd <= 2 &&
	 ($same_parents || $ages_identical ||
	  ($ages_overlap && $locations_overlap) ||
	  ($ages_overlap && $locations_close && $same_rank)) )
    {
	return 1;
    }
    
    # If none of the criteria are satisfied, return false.
    
    return '';
}


sub IsRockType {

    my ($string) = @_;
    
    $string =~ s/s$// if length($string) > 3;
    $string = fc $string;
    
    my @words = ($string =~ /(\S+)/g);
    my @rock_type_words = grep { $is_rock_type{$_} } @words;
    
    if ( @words && @words == @rock_type_words )
    {
	return 1;
    }

    else
    {
	return '';
    }
}


sub CreateConcept {

    my ($nr, $key, $preliminary_no) = @_;
    
    my ($strat_name) = split /\|/, $key;
    
    my $cr = { name => $strat_name, strat_name => { $strat_name => 1 },
	       rkeys => [ $nr->{rkey} ],
	       preliminary_no => $preliminary_no,
	       lat_min => $nr->{lat_min}, lat_max => $nr->{lat_max},
	       lng_min => $nr->{lng_min}, lng_max => $nr->{lng_max},
	       early_age => $nr->{early_age}, late_age => $nr->{late_age},
	       n_colls => $nr->{n_colls}, n_occs => $nr->{n_occs}, cc => { },
	       reference_no => { }, lithology1 => { }, lithology2 => { } };
    
    $cr->{cc}{$nr->{cc}} = 1 if $nr->{cc};
    $cr->{reference_no}{$_} = 1 foreach keys $nr->{reference_no}->%*;
    $cr->{lithology1}{$_} = 1 foreach keys $nr->{lithology1}->%*;
    $cr->{lithology2}{$_} = 1 foreach keys $nr->{lithology2}->%*;
    
    $nr->{preliminary_no}{$key} = $preliminary_no;
    
    $strat_prelim{$preliminary_no} = $cr;
    
    return $cr;
}


sub ConceptIsCompatible {

    my ($cr, $nr) = @_;
    
    unless ( $cr->{cc}{$nr->{cc}} )
    {
	if ( $nr->{lat_min} > $cr->{lat_max} + 10 ) { return undef; }
	if ( $nr->{lat_max} < $cr->{lat_min} - 10 ) { return undef; }
	if ( $nr->{lng_min} > $cr->{lng_max} + 10 ) { return undef; }
	if ( $nr->{lng_max} < $cr->{lng_min} - 10 ) { return undef; }
    }

    else
    {
	if ( $nr->{early_age} <= $cr->{late_age} - 5 ) { return undef; }
	if ( $nr->{late_age} >= $cr->{early_age} + 5 ) { return undef; }
    }
    
    return 1;
}


sub AddToConcept {

    my ($cr, $nr, $key) = @_;
    
    if ( $nr->{preliminary_no}{$key} )
    {
	say "Overwrote concept for key '$key'";
	$DB::single = 1;
    }
    
    $nr->{preliminary_no}{$key} = $cr->{preliminary_no};
    
    my ($strat_name) = split /[|]/, $key;
    
    $cr->{strat_name}{$strat_name} = 1;
    push $cr->{rkeys}->@*, $nr->{rkey};
    
    $cr->{lat_min} = $nr->{lat_min} if $nr->{lat_min} < $cr->{lat_min};
    $cr->{lat_max} = $nr->{lat_max} if $nr->{lat_max} > $cr->{lat_max};
    $cr->{lng_min} = $nr->{lng_min} if $nr->{lng_min} < $cr->{lng_min};
    $cr->{lng_max} = $nr->{lng_max} if $nr->{lng_max} > $cr->{lng_max};

    $cr->{early_age} = $nr->{early_age} if $nr->{early_age} > $cr->{early_age};
    $cr->{late_age} = $nr->{late_age} if $nr->{late_age} < $cr->{late_age};
    
    $cr->{n_colls} += $nr->{n_colls};
    $cr->{n_occs} += $nr->{n_occs};
    
    $cr->{cc}{$nr->{cc}} = 1 if $nr->{cc};
    $cr->{reference_no}{$_} = 1 foreach keys $nr->{reference_no}->%*;
    $cr->{lithology1}{$_} = 1 foreach keys $nr->{lithology1}->%*;
    $cr->{lithology2}{$_} = 1 foreach keys $nr->{lithology2}->%*;
}


sub ConsolidateConcepts {

    my ($remaining, $other) = @_;

    push $remaining->{rkeys}->@*, $other->{rkeys}->@*;
    $remaining->{strat_name}{$_} = 1 foreach keys $other->{strat_name}->%*;
    
    $remaining->{lat_min} = $other->{lat_min} if $other->{lat_min} < $remaining->{lat_min};
    $remaining->{lat_max} = $other->{lat_max} if $other->{lat_max} > $remaining->{lat_max};
    $remaining->{lng_min} = $other->{lng_min} if $other->{lng_min} < $remaining->{lng_min};
    $remaining->{lng_max} = $other->{lng_max} if $other->{lng_max} > $remaining->{lng_max};
    
    $remaining->{early_age} = $other->{early_age} if $other->{early_age} > $remaining->{early_age};
    $remaining->{late_age} = $other->{late_age} if $other->{late_age} < $remaining->{late_age};
    
    $remaining->{n_colls} += $other->{n_colls};
    $remaining->{n_occs} += $other->{n_occs};

    $remaining->{cc}{$_} = 1 foreach keys $other->{cc}->%*;
    $remaining->{reference_no}{$_} = 1 foreach keys $other->{reference_no}->%*;
    $remaining->{lithology1}{$_} = 1 foreach keys $other->{lithology1}->%*;
    $remaining->{lithology2}{$_} = 1 foreach keys $other->{lithology2}->%*;
}


sub ContextsAreCompatible {

    my ($context1, $context2) = @_;
    
    if ( ! defined $context1->{early_age} ) { return 1; }
    
    if ( $context2->{lat_min} > $context1->{lat_max} + 10 ) { return undef; }
    if ( $context2->{lat_max} < $context1->{lat_min} - 10 ) { return undef; }
    if ( $context2->{lng_min} > $context1->{lng_max} + 10 ) { return undef; }
    if ( $context2->{lng_max} < $context1->{lng_min} - 10 ) { return undef; }
    
    if ( $context2->{early_age} < $context1->{late_age} - 5 ) { return undef; }
    if ( $context2->{late_age} > $context1->{early_age} + 5 ) { return undef; }
    
    return 1;
}


sub ChooseConceptName {

    my ($cr) = @_;
    
    my @candidates = keys $cr->{strat_name}->%*;
    
    # Start by looking for names which have a character other than letters, numbers, and
    # punctuation. Since data enterers often leave out accent marks, the names with
    # accent marks are more likely to be correct. Out of all those, pick the first one
    # in alphabetical order.

    if ( my @with_accents = grep { /[!a-zA-Z0-9 .,:-]/ } @candidates )
    {
	return (sort @with_accents)[0];
    }

    # Otherwise, choose the first name in alphabetical order.

    elsif ( @candidates )
    {
	return (sort @candidates)[0];
    }

    else
    {
	return $cr->{name};
    }
}


# ImportMacrostrat ( )
#
# Execute the command 'import macrostrat'.
#
# This command imports stratigraphic names from `macrostrat`.`strat_names` and
# associated tables, and puts the processed names into `pbdb`.`strat_ms_names`.

our (%place);

our (%infer_from_ref) = ( 1 => 'US', 2 => 'CA', 5 => 'NZ', 10 => 'CA', 11 => 'CA', 12 => 'CA',
			  19 => 'US', 21 => 'CA', 22 => 'AU', 29 => 'BR', 31 => 'BR', 32 => 'CL',
			  34 => 'BR', 38 => 'BR', 39 => 'CL', 40 => 'BO', 41 => 'BR',
			  48 => 'NZ', 73 => 'CA', 74 => 'CA', 75 => 'CA', 77 => 'CA', 78 => 'CA',
			  100 => 'MX', 105 => 'US', 109 => 'US', 115 => 'CA', 116 => 'CA',
			  118 => 'CA', 121 => 'CA', 122 => 'CA', 125 => 'CA', 131 => 'CA',
			  134 => 'GL', 135 => 'SJ', 156 => 'MX', 159 => 'GL',
			  163 => 'MY', 164 => 'MY', 165 => 'VN', 166 => 'VN', 167 => 'VN',
			  168 => 'ID', 169 => 'ID', 170 => 'ID', 171 => 'ID', 172 => 'MY',
			  173 => 'TH', 174 => 'LA', 175 => 'ID', 176 => 'ID', 177 => 'ID',
			  178 => 'ID', 188 => 'NZ', 218 => 'SJ', 225 => 'CA' );
			  

sub ImportMacrostrat {

    # Step I: Truncate the STRAT_MS_NAMES table.
    
    say "Truncating tables: STRAT_MS_NAMES (strat_ms_names)";
    
    DBCommand($pbdb, "TRUNCATE `$TABLE{STRAT_MS_NAMES}`");
    
    # Step II: Extract all of the relevant information from Macrostrat.

    # Start with the `places` table.

    my $places = DBHashQuery($mstr, "
	SELECT p.name, p.postal, p.country_abbrev, 
	    round(y(st_pointn(st_exteriorring(envelope(p.geom)), 1)), 2) as lat_min,
	    round(y(st_pointn(st_exteriorring(envelope(p.geom)), 3)), 2) as lat_max,
	    round(x(st_pointn(st_exteriorring(envelope(p.geom)), 1)), 2) as lng_min,
	    round(x(st_pointn(st_exteriorring(envelope(p.geom)), 3)), 2) as lng_max
	FROM places as p");
    
    # Iterate through the rows, correcting problems. Add each row to the %place hash.
    
    foreach my $row ( $places->@* )
    {
	# Generate the correct country code.
	
	$row->{cc} = $row->{country_abbrev} eq 'USA' ? 'US' : $row->{country_abbrev};
	$row->{cc} = 'PW' if $row->{name} eq 'Palau';
	
	# Add the place record under the key 'name'.
	
	$place{$row->{name}} = $row;
	
	# The Australian provinces are added with the prefix 'AU-' to distinguish them
	# from similarly named American states. Everything else is added under the key
	# 'postal'.
	
	if ( $row->{cc} eq 'AU' )
	{
	    $place{"AU-$row->{postal}"} = $row;
	}

	else
	{
	    $place{$row->{postal}} = $row;
	}
    }
    
    # Now fetch the `strat_names` table and associated information.
    
    my $imported_names = 0;
    
    my $strat_names = DBHashQuery($mstr, "
	SELECT sn.id, sn.concept_id, sn.strat_name, sn.rank, sn.places,
		max(iub.age_bottom) as early_unit_age,
	    min(iut.age_top) as late_unit_age,
	    max(icb.age_bottom) as early_concept_age,
	    min(ict.age_top) as late_concept_age,
	    min(round(y(st_pointn(st_exteriorring(envelope(ca.col_area)), 1)), 2)) as lat_min,
	    max(round(y(st_pointn(st_exteriorring(envelope(ca.col_area)), 3)), 2)) as lat_max,
	    min(round(x(st_pointn(st_exteriorring(envelope(ca.col_area)), 1)), 2)) as lng_min,
	    max(round(x(st_pointn(st_exteriorring(envelope(ca.col_area)), 3)), 2)) as lng_max,
	    sn.ref_id
	FROM strat_names as sn
	  left join strat_names_meta as sc using (concept_id)
	  left join unit_strat_names as usn on usn.strat_name_id = sn.id
	  left join units as u on u.id = usn.unit_id
	  left join cols as c on c.id = u.col_id
	  left join col_areas as ca on ca.col_id = u.col_id
	  left join intervals as iub on iub.id = u.FO
	  left join intervals as iut on iut.id = u.LO
	  left join intervals as icb on icb.id = sc.b_int
	  left join intervals as ict on ict.id = sc.t_int
	WHERE (concept_id > 0 or c.status_code = 'active')
	GROUP BY sn.id
	HAVING (early_unit_age is not null or early_concept_age is not null)");
    
    # Iterate through the rows of the result, processing them to generate all of the
    # necessary fields for the STRAT_MS_NAMES table.
    
    my $strat_ms_values = '';
    
    foreach my $row ( @$strat_names )
    {
	# Compute 'early_age' and 'late_age', using the broader of the two age definitions if
	# both are given.
	
	if ( defined $row->{early_unit_age} && defined $row->{early_concept_age} )
	{
	    $row->{early_age} = MaxValue($row->{early_unit_age}, $row->{early_concept_age});
	    $row->{late_age} = MinValue($row->{late_unit_age}, $row->{late_concept_age});
	}
	
	elsif ( defined $row->{early_unit_age} )
	{
	    $row->{early_age} = $row->{early_unit_age};
	    $row->{late_age} = $row->{late_unit_age};
	}
	
	elsif ( defined $row->{early_concept_age} )
	{
	    $row->{early_age} = $row->{early_concept_age};
	    $row->{late_age} = $row->{late_concept_age};
	}
	
	# Determine the country code for this name, and get a list of state/province
	# codes as well.
	
	if ( $row->{places} =~ qr{ ^ [{] (.*) [}] \s* $ }xs )
	{
	    my @codes = split /,/, $1;
	    
	    $row->{cc} = $place{$codes[0]}{cc};
	}

	elsif ( $row->{places} )
	{
	    my @codes = split /,/, $row->{places};
	    
	    $row->{cc} = $place{substr($codes[0],0,2)}{cc};
	}
	
	else
	{
	    $row->{cc} = $infer_from_ref{$row->{ref_id}};
	}
	
	$imported_names++;
	
	my $qstratn = $pbdb->quote($row->{id});
	my $qstratc = $pbdb->quote($row->{concept_id});
	my $qname = $pbdb->quote($row->{strat_name});
	my $qrank = $pbdb->quote($row->{rank});
	my $qcc = $pbdb->quote($row->{cc});
	my $qearly = $pbdb->quote($row->{early_age});
	my $qlate = $pbdb->quote($row->{late_age});
	my $qlatmin = $pbdb->quote($row->{lat_min});
	my $qlatmax = $pbdb->quote($row->{lat_max});
	my $qlngmin = $pbdb->quote($row->{lng_min});
	my $qlngmax = $pbdb->quote($row->{lng_max});
	
	$strat_ms_values .= ', ' if $strat_ms_values;
	$strat_ms_values .= "($qstratn, $qstratc, $qname, $qrank, $qcc, $qearly, $qlate, " .
	    "$qlatmin, $qlatmax, $qlngmin, $qlngmax)";

	if ( length($strat_ms_values) > $chunk_size )
	{
	    InsertMSNames($pbdb, $strat_ms_values);
	    $strat_ms_values = '';
	}
    }
    
    InsertMSNames($pbdb, $strat_ms_values) if $strat_ms_values;
    
    say "Imported $imported_names names.";
}


sub MaxValue {

    my ($a, $b) = @_;

    return ($a > $b ? $a : $b);
}


sub MinValue {

    my ($a, $b) = @_;

    return ($a < $b ? $a : $b);
}


# UpdateTables ( )
# 
# Execute the subcommand 'update tables' or 'check tables'.
# 
# This command creates (or updates) the database tables used by this script. If the
# latter form of the command is used, the necessary statements are printed out but not
# executed.

sub UpdateTables {
    
    my ($cmd) = @_;

    CheckMode(1) if $cmd eq 'check';
    
    my $check = DBTextQuery($pbdb, "SHOW TABLES LIKE 'strat%'");
    my $activity;
    
    unless ( $check =~ /\bstrat_names\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_names` (
	  `stratn_no` int(10) unsigned NOT NULL AUTO_INCREMENT,
	  `stratc_no` int(10) unsigned NOT NULL,
	  `rank` enum('SGp','Gp','SubGp','Fm','Mbr','Bed') NOT NULL,
	  `name` varchar(255) NOT NULL,
	  `cc` varchar(255) NOT NULL DEFAULT '',
	  `country` varchar(255) NOT NULL DEFAULT '',
	  `lithology1` varchar(255) NOT NULL DEFAULT '',
	  `lithology2` varchar(255) NOT NULL DEFAULT '',
	  `n_colls` smallint(6) NOT NULL DEFAULT 0,
	  `n_occs` smallint(6) NOT NULL DEFAULT 0,
	  `early_age` decimal(9,6) DEFAULT NULL,
	  `late_age` decimal(9,6) DEFAULT NULL,
	  `lat_min` decimal(9,6) DEFAULT NULL,
	  `lat_max` decimal(9,6) DEFAULT NULL,
	  `lng_min` decimal(9,6) DEFAULT NULL,
	  `lng_max` decimal(9,6) DEFAULT NULL,
	  PRIMARY KEY (`stratn_no`),
	  KEY `name` (`name`,`rank`),
	  KEY `cc` (`cc`)
	) ENGINE=InnoDB", 1);
    }
    
    unless ( $check =~ /\bstrat_nrefs\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_nrefs` (
	  `stratn_no` int(10) unsigned NOT NULL,
	  `reference_no` int(10) unsigned NOT NULL,
	  PRIMARY KEY (`stratn_no`, `reference_no`),
	  KEY (`reference_no`)
	) ENGINE=InnoDB", 1);
    }
    
    unless ( $check =~ /\bstrat_concepts\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_concepts` (
	  `stratc_no` int(11) unsigned NOT NULL AUTO_INCREMENT,
	  `name` varchar(255) NOT NULL,
	  `cc_list` varchar(255) NOT NULL DEFAULT '',
	  `country_list` varchar(255) NOT NULL DEFAULT '',
	  `lithology1` varchar(255) NOT NULL DEFAULT '',
	  `lithology2` varchar(255) NOT NULL DEFAULT '',
	  `n_colls` smallint(6) NOT NULL DEFAULT 0,
	  `n_occs` smallint(6) NOT NULL DEFAULT 0,
	  `early_age` decimal(9,6) DEFAULT NULL,
	  `late_age` decimal(9,6) DEFAULT NULL,
	  `lat_min` decimal(9,6) DEFAULT NULL,
	  `lat_max` decimal(9,6) DEFAULT NULL,
	  `lng_min` decimal(9,6) DEFAULT NULL,
	  `lng_max` decimal(9,6) DEFAULT NULL,
	  PRIMARY KEY (`stratc_no`),
	  KEY `name` (`name`)
	) ENGINE=InnoDB", 1);
    }
    
    unless ( $check =~ /\bstrat_opinions\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_opinions` (
	  `strato_no` int(11) unsigned NOT NULL AUTO_INCREMENT,
	  `child_no` int(10) unsigned NOT NULL,
	  `child_rank` enum('SGp','Gp','SubGp','Fm','Mbr','Bed') NOT NULL,
	  `cc` varchar(10) NOT NULL,
	  `relationship` enum('belongs to','synonym of') DEFAULT NULL,
	  `parent_no` int(10) unsigned NOT NULL,
	  `parent_rank` enum('SGp','Gp','SubGp','Fm','Mbr','Bed') NOT NULL,
	  PRIMARY KEY (`strato_no`),
	  UNIQUE KEY `child_no` (`child_no`,`child_rank`,`cc`,`parent_rank`,`parent_no`),
	  KEY `parent_no` (`parent_no`,`parent_rank`)
	) ENGINE=InnoDB", 1);
    }
    
    unless ( $check =~ /\bstrat_orefs\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_orefs` (
	  `strato_no` int(10) unsigned NOT NULL,
	  `reference_no` int(10) unsigned NOT NULL,
	  PRIMARY KEY (`strato_no`,`reference_no`),
	  KEY `reference_no` (`reference_no`)
	) ENGINE=InnoDB", 1);
    }
    
    unless ( $check =~ /\bstrat_ms_names\b/ )
    {
	$activity = 1;
	
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `strat_ms_names` (
	  `stratn_id` int(10) unsigned NOT NULL PRIMARY KEY,
	  `stratc_id` int(10) unsigned NOT NULL default '0',
	  `name` varchar(255) NOT NULL,
	  `rank` enum('SGp','Gp','SubGp','Fm','Mbr','Bed') NOT NULL,
	  `cc` varchar(2) NULL,
	  `early_age` decimal(9,6) DEFAULT NULL,
	  `late_age` decimal(9,6) DEFAULT NULL,
	  `lat_min` decimal(9,6) DEFAULT NULL,
	  `lat_max` decimal(9,6) DEFAULT NULL,
	  `lng_min` decimal(9,6) DEFAULT NULL,
	  `lng_max` decimal(9,6) DEFAULT NULL,
	  KEY (`name`, `rank`)
	) ENGINE=InnoDB", 1);
    }

    unless ( $activity )
    {
	say "No updates.";
    }
}


sub InsertNames {

    my ($dbh, $name_values) = @_;

    DBCommand($dbh, "INSERT INTO `$TABLE{STRAT_NAMES}` (stratn_no, stratc_no, name, rank, cc, " .
	      "country, lithology1, lithology2, n_colls, n_occs, early_age, late_age, " .
	      "lat_min, lat_max, lng_min, lng_max) VALUES " .
	      $name_values);
}


sub InsertMSNames {

    my ($dbh, $name_values) = @_;
    
    DBCommand($dbh, "INSERT INTO `$TABLE{STRAT_MS_NAMES}` (stratn_id, stratc_id, name, rank, cc, " .
	      "early_age, late_age, lat_min, lat_max, lng_min, lng_max) VALUES " .
	      $name_values);
}


sub InsertNameRefs {

    my ($dbh, $ref_values) = @_;
    
    DBCommand($dbh, "INSERT INTO `$TABLE{STRAT_NREFS}` (stratn_no, reference_no) VALUES $ref_values");
}


sub InsertConcepts {

    my ($dbh, $concept_values) = @_;

    DBCommand($dbh, "INSERT INTO $TABLE{STRAT_CONCEPTS} (stratc_no, name, cc_list, country_list, " .
	      "lithology1, lithology2, n_colls, n_occs, early_age, late_age, " .
	      "lat_min, lat_max, lng_min, lng_max) VALUES " .
	      $concept_values);
}
	

sub InsertOpinions {

    my ($dbh, $opinion_values) = @_;

    DBCommand($dbh, "INSERT INTO `$TABLE{STRAT_OPINIONS}` (strato_no, child_no, child_rank, cc, " .
	      "relationship, parent_no, parent_rank) VALUES " .
	      $opinion_values);
    
    # DBCommand($dbh, "INSERT INTO $TABLE{STRAT_OPINIONS} (strato_no, child_no, child_rank, " .
    # 	      "relationship, parent_no, parent_rank, reference_no) VALUES " .
    # 	      $opinion_values);
}


sub InsertOpinionRefs {

    my ($dbh, $ref_values) = @_;
    
    DBCommand($dbh, "INSERT INTO `$TABLE{STRAT_OREFS}` (strato_no, reference_no) VALUES $ref_values");
}



