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
use DBQuery qw(DBHashQuery DBRowQuery DBCommand DBInsert);

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

our ($dbh, $EXECUTE_MODE);
our ($STRAT_NAMES) = 'strata_names';
our ($STRAT_DATA) = 'stratum_data';

loadConfig($opt_config);

my $dbconf = configData('Database');

if ( $ENV{PWD} !~ qr{^/var/paleomacro/pbapi} )
{
    $dbconf->{host} = '127.0.0.1';
}

our($mstr) = connectDB($opt_config, 'macrostrat');
our($pbdb) = connectDB($opt_config, 'pbdb');

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

}

else
{
    die "Unknown subcommand '$ARGV[0]'\n";
}

exit;



# GenerateConcepts ( )
#
# Extract stratigraphic names and concepts from the STRAT_RAW table. This table
# contains exactly what was typed in by the database contributors, minus
# suffixes like 'Fm.' or 'Gp.'. This subroutine completely rebuilds the
# STRAT_NAMES and STRAT_CONCEPTS tables.

sub GenerateConcepts {

    # Step I: iterate through the rows of COLLECTION_STRATA. Record the relationships
    # between raw stratigraphic names.
    
    say "Reading from table '$TABLE{COLLECTION_STRATA}'...";
    
    my (%contained_in, %rkey_lookup);
    
    my $hierarchy = DBHashQuery($pbdb, "
	SELECT cs.grp, cs.formation, cs.member, c.cc,
		group_concat(distinct c.reference_no) as reference_nos
	FROM $TABLE{COLLECTION_STRATA} as cs join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
	GROUP BY grp, formation, member, cc");
    
    # Record each Mbr -> Fm, Mbr -> Gp, and Fm -> Gp relationship using "relationship
    # keys", which consist of the raw stratigraphic name plus the rank and country code.
    # These are different from the "name keys" generated below, which use individual
    # name components extracted from the raw stratigraphic names.
    
    foreach my $a ( $hierarchy->@* )
    {
	if ( $a->{grp} && $a->{formation} )
	{
	    my $larger_key = "$a->{grp}|Gp|$a->{cc}";
	    my $smaller_key = "$a->{formation}|Fm|$a->{cc}";
	    
	    $contained_in{$smaller_key}{$larger_key} = 1; # $$$ reference_nos
	}
	
	elsif ( $a->{grp} && $a->{member} )
	{
	    my $larger_key = "$a->{grp}|Gp|$a->{cc}";
	    my $smaller_key = "$a->{member}|Mbr|$a->{cc}";
	    
	    $contained_in{$smaller_key}{$larger_key} = 1;
	}
	
	if ( $a->{formation} && $a->{member} )
	{
	    my $larger_key = "$a->{formation}|Fm|$a->{cc}";
	    my $smaller_key = "$a->{member}|Mbr|$a->{cc}";
	    
	    $contained_in{$smaller_key}{$larger_key} = 1;
	}
    }
    
    say "Reading from table '$TABLE{STRAT_RAW}'...";
    
    my $raw_names = DBHashQuery($pbdb, "SELECT * FROM $TABLE{STRAT_RAW} order by name");
    
    my (%orig_name, %first_two, %first_last, %alias_of);
    
    # Step II: Iterate through the rows of STRAT_RAW. Each row generates a name context
    # record, along with one or more name keys. Situations which generate
    # multiple keys include names which contain 'and', 'or', 'x (= y)', etc.
    
    foreach my $rn ( @$raw_names )
    {
	my $raw_name = $rn->{name};
	my @components;
	
	# Remove quotation marks and question marks.
	
	$raw_name =~ s/"//g;
	
	if ( $raw_name =~ /(.*?)\s*[(][?][)]\s*(.*)/ )
	{
	    if ( $1 && $2 )
	    {
		$raw_name = "$1$2";
	    }

	    elsif ( $1 )
	    {
		$raw_name = $1;
	    }

	    else
	    {
		$raw_name = $2;
	    }
	}

	elsif ( $raw_name =~ /(.*?)\s*[?]\s*(.*?)\s*[?]$/ )
	{
	    $raw_name = "$1 or $2";
	}
	
	elsif ( $raw_name =~ /(.*?)\s*[?]\s*(.*)/ )
	{
	    if ( $1 && $2 )
	    {
		$raw_name = "$1 $2";
	    }

	    elsif ( $1 )
	    {
		$raw_name = $1;
	    }

	    else
	    {
		$raw_name = $2;
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
	
	elsif ( $raw_name =~ /(.*?)\s+(?:and|&)\s+(.*)/ )
	{
	    @components = ParseConjunction($1, $3);
	}
	
	else
	{
	    @components = $raw_name;
	}
	
	# Iterate through the individual components, assuming that each
	# represents a separate stratum name. For each one that appears to be an
	# actual stratigraphic name, add it to the %orig_name hash with the name
	# context record as its value.
	
	while ( @components )
	{
	    my $c = shift @components;
	    my $alias;
	    
	    # Separate out any alias that is specified in the name.
	    
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
	    
	    # # Remove common descriptive words from the name component.
	    
	    # if ( $c =~ /(?:\bunits?\b|\bupper\b|\bmiddle\b|\blower\b|\bbetween\b|\bcontact\b)/i )
	    # {
	    # 	$c =~ s/\s*\bunits?\b\s*//;
	    # 	$c =~ s/\s*\bupper\b\s*//;
	    # 	$c =~ s/\s*\bmiddle\b\s*//;
	    # 	$c =~ s/\s*\blower\b\s*//;
	    # 	if ( $rn->{rank} eq 'Mbr' ) {
	    # 	    $c =~ s/\s*\bbetween\b\s*//;
	    # 	    $c =~ s/\s*\bcontact\b\s*//;
	    # 	}
	    # }
	    
	    # Ignore any name component that doesn't contain at least three
	    # letters in a row.
	    
	    next unless $c =~ /[[:alpha:]]{3}/;

	    # Ignore any of the excluded name components.
	    
	    next if $excluded_name{lc $c};
	    
	    # Increment the component count
	    
	    $rn->{n_names}++;
	    
	    # Generate a "name key" consisting of the stratum name, rank, and country
	    # code. Store this key in %orig_name, with the value being a list of
	    # all the name context records associated with the name.
	    
	    my $key = "$c|$rn->{rank}|$rn->{cc}";
	    
	    $orig_name{$key} ||= [];
	    push $orig_name{$key}->@*, $rn;
	    
	    # Store the name key in the hashes %exact_name, %first_two, and
	    # %first_last. The hash key for the first is the exact name, for the
	    # second is the first two letters, and for the last the first/last
	    # letters. These will allow us to associate names together into
	    # concepts even if they have different ranks and/or are spelled slightly
	    # differently.
	    
	    # $exact_name{$c} ||= [];
	    # push $exact_name{$c}->@*, $key;
	    
	    my $ft = substr($c, 0, 2);
	    
	    $first_two{$ft} ||= [];
	    push $first_two{$ft}->@*, $key;
	    
	    my @fl = ($c =~ /^([[:alpha:]]).*?[[:alpha:]]{2}([[:alpha:]])(?: |$)/);
	    
	    if ( @fl )
	    {
		my $fl = $fl[0] . $fl[1];
		$first_last{$fl} ||= [];
		push $first_last{$fl}->@*, $key;
	    }
	    
	    # If an alias was specified, store it in the same way. Also store it
	    # under %alias_key with the original key as a value. This will
	    # enable us to ensure later that the two names are associated with
	    # the same concept.
	    
	    if ( $alias && ! $excluded_name{$alias} )
	    {
		my $alias_key = "$alias|$rn->{rank}|$rn->{cc}";
		
		$orig_name{$alias_key} ||= [];
		push $orig_name{$alias_key}->@*, $rn;

		if ( $key lt $alias_key )
		{
		    $alias_of{$alias_key} = $key;
		}

		else
		{
		    $alias_of{$key} = $alias_key;
		}
		
		my @afl = ($c =~ /^([[:alpha:]]).*?[[:alpha:]]{2}([[:alpha:]])(?: |$)/);
		
		if ( @afl )
		{
		    my $afl = $afl[0] . $afl[1];
		    $first_last{$afl} ||= [];
		    push $first_last{$afl}->@*, $key;
		}
		
		# $exact_name{$alias} ||= [];
		# push $exact_name{$alias}->@*, $alias_key;
		
		# my $aft = substr($alias, 0, 2);
		
		# $first_two{$aft} ||= [];
		# push $first_two{$aft}->@*, $alias_key;
		# my $afl = substr($alias, 0, 1) . substr($alias, -1, 1);
		# $first_last{$afl} ||= [];
		# push $first_last{$afl}->@*, $alias_key;
	    }
	}

	# Map "relationship keys" to name context records. This mapping will be used in
	# the next step.
	
	$rn->{rkey} = "$rn->{name}|$rn->{rank}|$rn->{cc}";
	$rkey_lookup{$rn->{rkey}} = $rn;
    }

    # # Step IIa: assign hierarchical relationships among names. A parent/child relationship is
    # # only possible if the parent name context is associated with a single name, because
    # # otherwise we don't know which name to associate each child with.
    
    say "Assigning name relationships...";
    
    foreach my $key ( sort keys %orig_name )
    {
	foreach my $rn ( $orig_name{$key}->@* )
	{
	    foreach my $parent_rkey ( keys $contained_in{$rn->{rkey}}->%* )
	    {
		my $parent = $rkey_lookup{$parent_rkey};
		
		if ( $parent->{n_names} == 1 )
		{
		    $rn->{parent_rn}{$key}{$parent_rkey} = 1;
		}
	    }
	}
    }
    
    # Step III: Iterate through the name keys, assigning each one to a concept.
    
    say "Assigning concepts...";
    
    my (%name_concept, %preliminary_concept, $progress_letter);
    my $preliminary_concept_no = 0;
    
    foreach my $key ( sort keys %orig_name )
    {
	my ($name, $rank, $cc) = split /\|/, $key;
	
	my $first_letter = uc substr($name, 0, 1);
	
	if ( $first_letter gt $progress_letter && $first_letter =~ /[A-Z]/ )
	{
	    $progress_letter = $first_letter;
	    print STDERR "$first_letter..";
	}
	
	# If there are multiple name context records for a given key, start by
	# consolidating them if possible.
	
	if ( $orig_name{$key}->@* > 1 )
	{
	    # Look for a name context record with only a single associated name. Keep
	    # track of all the others as well.
	    
	    my ($single, @rest, @separate);
	    
	    foreach my $rn ( $orig_name{$key}->@* )
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

	    unless ( $single )
	    {
		$single = { name => $name, rank => $rank, n_names => 1,
			    cc => $cc, country => $orig_name{$key}[0]{country} };
	    }
	    
	    foreach my $other ( @rest )
	    {
		if ( ContextsAreCompatible($single, $other) )
		{
		    AddToContext($single, $other, $key);
		}

		else
		{
		    push @separate, $other;
		}
	    }
	    
	    $orig_name{$key} = [$single, @separate];
	    # say "Consolidated names for " . encode_utf8($key);
	}
	
	# For each key, iterate through the consolidated name context records
	# associated with that key. In the code below, $rn points to a *name*
	# context record, while $rc points to a *name concept* context record.
	
      RECORD:
	foreach my $rn ( $orig_name{$key}->@* )
	{
	    my ($rc, @matching_concepts);
	    
	    # If this key is an alias for another key that has already been assigned to
	    # a concept, then assign all associated name contexts to that same concept.
	    
	    if ( my $orig_key = $alias_of{$key} )
	    {
		foreach my $orig_rn ( $orig_name{$orig_key}->@* )
		{
		    if ( my $orig_concept = $orig_rn->{context}{$orig_key} )
		    {
			push @matching_concepts, $orig_concept;
			last;
		    }
		}
	    }
	    
	    # Check for other records with the same name and (possibly) a different
	    # rank, plus any names that could be spelling variants.
	    
	    my @candidates;
	    
	    my $ft = substr($name, 0, 2);
	    push @candidates, $first_two{$ft}->@* if $first_two{$ft};
	    
	    my @fl = ($name =~ /^([[:alpha:]]).*?[[:alpha:]]{2}([[:alpha:]])(?: |$)/);
	    
	    if ( @fl )
	    {
		my $fl = $fl[0] . $fl[1];
		push @candidates, $first_last{$fl}->@* if $first_last{$fl};
	    }
	    
	    foreach my $alt_key ( @candidates )
	    {
		my ($alt_name, $alt_rank) = split /\|/, $alt_key;
		
		my $second_word;
		
		# Skip any potential name which is not compatible with the name being
		# checked. Compatibility is indicated by various measures such as having
		# a small edit distance, having a rock type added to one name but not
		# the other, etc.
		
		my $match_level = NamesAreCompatible($name, $key, $rn,
						     $alt_name, $alt_key, $orig_name{$alt_key}[0]);
		
		unless ( $match_level )
		{
		    $second_words{$second_word} = 1 if $second_word;
		    next;
		}
		
		# If we get here, we have a key which is potentially a variant
		# spelling of the name we are checking. So check all name
		# concept records associated with this key and see if any are
		# compatible with the name context record we are checking.
		
		foreach $rc ( $name_concept{$alt_key}->@* )
		{
		    if ( ConceptIsCompatible($rc, $rn, $match_level) )
		    {
			unless ( grep { $_ eq $rc } @matching_concepts )
			{
			    push @matching_concepts, $rc;
			}
		    }
		}
	    }
	    
	    # If there is exactly one matching concept, add the name to it.
	    
	    if ( @matching_concepts == 1 )
	    {
		AddToConcept($matching_concepts[0], $rn, $key);
		$name_concept{$key} ||= [];
		unless ( grep { $_ eq $matching_concepts[0] } $name_concept{$key}->@* )
		{
		    push $name_concept{$key}->@*, $matching_concepts[0];
		}
	    }
	    
	    # If there is more than one, then this name bridges multiple
	    # concepts and we must consolidate them. The one with the lowest
	    # concept number will be selected, and the rest will be consolidated
	    # with it.
	    
	    elsif ( @matching_concepts > 1 )
	    {
		my ($first, @rest) =
		    sort { $a->{preliminary_concept_no} <=> $b->{preliminary_concept_no} }
		    @matching_concepts;
		
		foreach my $rc ( @rest )
		{
		    if ( $rc ne $first )
		    {
			$rc->{consolidated_with} = $first->{preliminary_concept_no};
		    }
		}

		AddToConcept($first, $rn, $key);
		$name_concept{$key} ||= [];
		unless ( grep { $_ eq $rc } $name_concept{$key}->@* )
		{
		    push $name_concept{$key}->@*, $rc;
		}
	    }
	    
	    # If there aren't any, we must create a new stratigraphic name concept.
	    
	    else
	    {
		$rc = CreateConcept($rn, $key, ++$preliminary_concept_no);
		
		$name_concept{$key} ||= [];
		push $name_concept{$key}->@*, $rc;
		
		$preliminary_concept{"${preliminary_concept_no}A"} = $rc;
	    }
	}
    }
    
    # Step IV: consolidate the preliminary stratigraphic concepts into real ones.
    
    say "Consolidating concepts...";
    
    my $real_concept_no = 0;
    my (%real_concept, %prelim_to_real);
    
    foreach my $preliminary_concept_no ( sort { $a <=> $b } keys %preliminary_concept )
    {
	my $rc = $preliminary_concept{$preliminary_concept_no};
	
	unless ( $rc->{consolidated_with} )
	{
	    $rc->{concept_no} = ++$real_concept_no;
	    $real_concept{$real_concept_no} = $rc;
	    $prelim_to_real{$preliminary_concept_no} = $real_concept_no;
	}
    }

    foreach my $preliminary_concept_no ( sort keys %preliminary_concept )
    {
	my $rc = $preliminary_concept{$preliminary_concept_no};
	my $canonical = $rc;
	
	while ( $canonical->{consolidated_with} )
	{
	    $canonical = $preliminary_concept{$canonical->{consolidated_with}};
	}

	if ( $canonical ne $rc )
	{
	    ConsolidateConcepts($canonical, $rc);
	    $prelim_to_real{$preliminary_concept_no} = $canonical->{concept_no};
	}
    }
    
    # Step V: link up stratigraphic concepts into hierarchical relationships, using the
    # "relationship keys" and the name context relationships that were stored in the
    # %contained_in relation in Step I above.
    
    say "Computing concept relationships...";
    
    foreach my $key ( sort keys %orig_name )
    {
	foreach my $rn ( $orig_name{$key}->@* )
	{
	    my %parent_concept;
	    
	    if ( $rn->{parent_rn} && $rn->{parent_rn}{$key} )
	    {
		my $child_rc = $rn->{concept}{$key};
		
		while ( $child_rc->{consolidated_with} )
		{
		    $child_rc = $preliminary_concept{$child_rc->{consolidated_with}};
		}
		
		foreach my $rkey ( keys $rn->{parent_rn}{$key}->%* )
		{
		    my $parent_rkey = $rkey;
		    my $parent_rn = $rkey_lookup{$parent_rkey};
		    
		    while ( $parent_rn->{consolidated_with} )
		    {
			$parent_rkey = $parent_rn->{consolidated_with};
			$parent_rn = $rkey_lookup{$parent_rkey};
		    }
		    
		    if ( $parent_rn->{concept} && $parent_rn->{concept}->%* )
		    {
			if ( values $parent_rn->{concept}->%* > 1 )
			{
			    say encode_utf8("Concept ambiguity for $parent_rkey");
			    next;
			}
			
			my ($parent_rc) = values $parent_rn->{concept}->%*;
			
			unless ( $parent_rc )
			{
			    say encode_utf8("No concept for $parent_rkey");
			    next;
			}
			
			while ( $parent_rc->{consolidated_with} )
			{
			    $parent_rc = $preliminary_concept{$parent_rc->{consolidated_with}};
			}
			
			$child_rc->{parent_rc}{"$rn->{rank}|$rn->{cc}|$parent_rkey"} =
			    $parent_rc->{concept_no};
		    }
		}
	    }
	}
    }
    
    # Step VI: regenerate the STRAT_NAMES, STRAT_CONCEPTS, and STRAT_OPINIONS tables.
    
    # First, empty these tables so we can refill them from our computed data.
    
    $DB::single = 1;
    
    say "Emptying tables: '$TABLE{STRAT_NAMES}', '$TABLE{STRAT_CONCEPTS}', " .
	"'$TABLE{STRAT_OPINIONS}', strat2_opinions...";
    
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_NAMES}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_CONCEPTS}");
    DBCommand($pbdb, "TRUNCATE $TABLE{STRAT_OPINIONS}");
    DBCommand($pbdb, "TRUNCATE strat2_opinions");
    
    # Then go through the list of concepts, and add one row to the STRAT_CONCEPTS table
    # per concept.
    
    say "Generating the STRAT_CONCEPTS ($TABLE{STRAT_CONCEPTS}) table...";
    
    my $concept_values = '';
    my $new_concepts = 0;
    
    my @concept_list = sort { $a <=> $b } keys %real_concept;
    
    foreach my $key ( @concept_list )
    {
	my $rc = $real_concept{$key};
	
	my $qstratc = $pbdb->quote($rc->{concept_no});
	my $qname = $pbdb->quote(ChooseConceptName($rc));
	my $qcc = $pbdb->quote(join ',', keys $rc->{cc}->%*);
	my $qncolls = $pbdb->quote($rc->{n_colls} || 0);
	my $qnoccs = $pbdb->quote($rc->{n_occs} || 0);
	my $qearly = $pbdb->quote($rc->{early_age} || '');
	my $qlate = $pbdb->quote($rc->{late_age} || '');
	my $qlatmin = $pbdb->quote($rc->{lat_min});
	my $qlatmax = $pbdb->quote($rc->{lat_max});
	my $qlngmin = $pbdb->quote($rc->{lng_min});
	my $qlngmax = $pbdb->quote($rc->{lng_max});
	
	$concept_values .= ', ' if $concept_values;
	$concept_values .= "($qstratc, $qname, $qcc, '', $qncolls, $qnoccs, $qearly, $qlate, " .
	    "$qlatmin, $qlatmax, $qlngmin, $qlngmax)";
	
	$new_concepts++;
	
	if ( length($concept_values) > 50000 )
	{
	    InsertConcepts($pbdb, $concept_values);
	    $concept_values = '';
	}
    }
    
    InsertConcepts($pbdb, $concept_values) if $concept_values;
    
    # Then go through the list of names, and add one row to the STRAT_NAMES table for
    # each name.
    
    say "Generating the STRAT_NAMES ($TABLE{STRAT_NAMES}) table...";
    
    my $name_values = '';
    my $new_names = 0;
    my $missing_names = 0;
    
    foreach my $key ( sort keys %orig_name )
    {
	my ($name, $rank, $cc) = split /\|/, $key;
	
	foreach my $rn ( $orig_name{$key}->@* )
	{
	    unless ( $rn->{concept}{$key}{preliminary_concept_no} )
	    {
		$missing_names++;
		next;
	    }
	    
	    my $concept_no = $prelim_to_real{$rn->{concept}{$key}{preliminary_concept_no}};
	    my $name_no = ++$new_names;
	    
	    $rn->{name_no}{$key} = $name_no;
	    
	    my $qname = $pbdb->quote($name);
	    my $qrank = $pbdb->quote($rank);
	    my $qstratn = $pbdb->quote($name_no);
	    my $qstratc = $pbdb->quote($concept_no);
	    my $qcc = $pbdb->quote($rn->{cc});
	    my $qcou = $pbdb->quote($rn->{country});
	    my $qncolls = $pbdb->quote($rn->{n_colls} || 0);
	    my $qnoccs = $pbdb->quote($rn->{n_occs} || 0);
	    my $qearly = $pbdb->quote($rn->{early_age} || '');
	    my $qlate = $pbdb->quote($rn->{late_age} || '');
	    
	    $name_values .= ', ' if $name_values;
	    $name_values .= "($qstratn, $qstratc, $qname, $qrank, $qcc, $qcou, $qncolls, $qnoccs, " .
		"$qearly, $qlate, $rn->{lat_min}, $rn->{lat_max}, $rn->{lng_min}, $rn->{lng_max})";
	    
	    if ( length($name_values) > 50000 )
	    {
		InsertNames($pbdb, $name_values);
		$name_values = '';
	    }
	}
    }
    
    InsertNames($pbdb, $name_values) if $name_values;
    
    # Then go through the names again, and add an opinion for each child-parent
    # relationship.
    
    say "Generating the STRAT_OPINIONS ($TABLE{STRAT_OPINIONS}) table...";
    
    my $opinion_values = '';
    my $new_opinions = 0;

    foreach my $key ( sort keys %orig_name )
    {
	my ($name, $rank, $cc) = split /\|/, $key;
	
	foreach my $rn ( $orig_name{$key}->@* )
	{
	    next unless $rn->{parent_rn};
	    
	    foreach my $parent_rkey ( keys $rn->{parent_rn}{$key}->%* )
	    {
		my $qopno = $pbdb->quote(++$new_opinions);
		my $qchildno = $pbdb->quote($rn->{name_no}{$key});
		my $qchildrank = $pbdb->quote($rank);
		
		my $parent_rn = $rkey_lookup{$parent_rkey};
		
		my ($parent_key) = keys $parent_rn->{concept}->%*;
		
		unless ( $parent_key && $parent_rn->{name_no}{$parent_key} )
		{
		    # say encode_utf8("No name key for parent $parent_rkey");
		    next;
		}
		
		my ($parent_name, $parent_rank, $parent_cc) = split /\|/, $parent_key;
		my $qparno = $pbdb->quote($parent_rn->{name_no}{$parent_key});
		my $qparrank = $pbdb->quote($parent_rank);
		my $qrefno = $pbdb->quote(0);
		
		$opinion_values .= ', ' if $opinion_values;
		$opinion_values .= "($qopno, $qchildno, $qchildrank, 'belongs to', " .
		    "$qparno, $qparrank, $qrefno)";
	    }
	    
	    if ( length($opinion_values) > 50000 )
	    {
		InsertOpinions($pbdb, $opinion_values);
		$opinion_values = '';
	    }
	}
    }
    
    InsertOpinions($pbdb, $opinion_values);
    
    # Finally, go through the concepts and add an opinion for each child-parent
    # relationship.
    
    say "Generating the strat2_opinions table...";
    
    my $opinion2_values = '';
    my $new_opinions2 = 0;
    my $bad_ccs = 0;

    @concept_list = sort { $a <=> $b } @concept_list;
    
    foreach my $child_no ( @concept_list )
    {
	my $child_rc = $real_concept{$child_no};

	foreach my $key ( keys $child_rc->{parent_rc}->%* )
	{
	    my ($child_rank, $child_cc, $unused, $parent_rank, $parent_cc) = split /[|]/, $key;
	    
	    if ( $child_cc ne $parent_cc )
	    {
		$bad_ccs++;
	    }
	    
	    # my $parent_rank = $child_rc->{parent_rc}{$child_rank}{$parent_no};
	    
	    my $qopno = $pbdb->quote(++$new_opinions2);
	    my $qchildno = $pbdb->quote($child_no);
	    my $qchildrank = $pbdb->quote($child_rank);
	    my $qchildcc = $pbdb->quote($child_cc);
	    my $qparno = $pbdb->quote($child_rc->{parent_rc}{$key});
	    my $qparrank = $pbdb->quote($parent_rank);
	    my $qrefno = $pbdb->quote(0);
	    
	    $opinion2_values .= ', ' if $opinion2_values;
	    $opinion2_values .= "($qopno, $qchildno, $qchildrank, $qchildcc, 'belongs to', " .
		"$qparno, $qparrank, $qrefno)";
	}
	
	if ( length($opinion2_values) > 50000 )
	{
	    InsertOpinions2($pbdb, $opinion2_values);
	    $opinion2_values = '';
	}
    }
    
    InsertOpinions2($pbdb, $opinion2_values);
    
    say "Missing: $missing_names" if $missing_names > 0;
    say "Bad ccs: $bad_ccs" if $bad_ccs > 0;
    say "Created $new_names names";
    say "Created $new_concepts concepts";
    say "Created $new_opinions name opinions";
    say "Created $new_opinions2 concept opinions";
    
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

    my ($name, $key, $rn, $alt_name, $alt_key, $alt_rn) = @_;

    # Convert both names to fold-case, to make this comparison case-insensitive.

    $name = fc $name;
    $alt_name = fc $alt_name;
    
    # If the two names are the same, return true.

    if ( $name eq $alt_name )
    {
	return 1;
    }
    
    my ($smaller, $larger, $same_prefix, $same_parents, $same_rank,
	$ages_overlap, $ages_identical, $ages_close, $locations_close, $locations_overlap);
    
    # If the age ranges overlap, the two names are potentially compatible.
    
    if ( $rn->{early_age} >= $alt_rn->{late_age} &&
	 $rn->{late_age} <= $alt_rn->{early_age} )
    {
	$ages_overlap = 1;
    }
    
    # If the age ranges are exactly the same or very close, the two names are even more
    # likely to be compatible.
    
    if ( $rn->{early_age} == $alt_rn->{early_age} &&
	 abs($rn->{late_age} - $alt_rn->{late_age}) < 5 ||
	 $rn->{late_age} == $alt_rn->{late_age} &&
	 abs($rn->{early_age} - $alt_rn->{early_age}) < 5 )
    {
	$ages_identical = 1;
    }
    
    # If the geographic ranges overlap or are very close to each other, the two names are
    # potentially compatible.

    if ( $rn->{cc} eq $alt_rn->{cc} )
    {
	$locations_close = 1;
    }
    
    elsif ( $rn->{lat_min} <= $alt_rn->{lat_max} + 5 &&
	    $rn->{lat_max} >= $alt_rn->{lat_min} - 5 &&
	    $rn->{lng_min} <= $alt_rn->{lng_max} + 5 &&
	    $rn->{lng_max} >= $alt_rn->{lng_min} - 5 )
    {
	$locations_close = 1;
    }

    if ( $rn->{lat_min} <= $alt_rn->{lat_max} + 1 &&
	 $rn->{lat_max} >= $alt_rn->{lat_min} - 1 &&
	 $rn->{lng_min} <= $alt_rn->{lng_max} + 1 &&
	 $rn->{lng_max} >= $alt_rn->{lng_min} - 1 )
    {
	$locations_overlap = 1;
    }
    
    # If the two names start with the same three letters, they are potentially compatible.
    
    if ( substr($name, 0, 3) eq substr($alt_name, 0, 3) )
    {
	$same_prefix = 1;
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
	}
    }
    
    # If the two names have identical parents, they are potentially compatible.
    
    if ( $rn->{parent_rn} && $rn->{parent_rn}{$key} &&
	 $alt_rn->{parent_rn} && $alt_rn->{parent_rn}{$alt_key} )
    {
      KEY:
	foreach my $a ( keys $rn->{parent_rn}{$key}->%* )
	{
	    foreach my $b ( keys $alt_rn->{parent_rn}{$alt_key}->%* )
	    {
		if ( $a eq $b )
		{
		    $same_parents = 1;
		    last KEY;
		}
	    }
	}
    }
    
    # If they have the same rank, they are potentially compatible.
    
    if ( $rn->{rank} eq $alt_rn->{rank} )
    {
	$same_rank = 1;
    }
    
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
	
	# if ( length($name) < length($alt_name) )
	# {
	#     $smaller = $name;
	#     $larger = $alt_name;
	# }
	
	# else
	# {
	#     $smaller = $alt_name;
	#     $larger = $name;
	# }
	
	# if ( edistance(substr($larger, 0, length($smaller) + 1), "$smaller ", 2) >= 0 &&
	#      substr($larger, length($smaller), 2) =~ / / && ! IsRockType($smaller) )
	# {
	#     my $suffix = substr($larger, length($smaller) + 1);
	#     $suffix =~ s/^\s+//;
	    
	#     if ( IsRockType($suffix) )
	#     {
	# 	return 1;
	#     }
	    
	#     # elsif ( $suffix =~ /[[:alpha:]]{2}/ )
	#     # {
	#     # 	$$second_word_ref = lc "$suffix ($smaller)";
	#     # }
	# }
	
	# else
	# {
	#     say "Candidate pair: $name | $alt_name";
	# }
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
    
	# if ( $ldd == 1 )
	# {
	#     return 1;
	# }
	
	# my @words = $name =~ /([[:alpha:]]+)/g;
	# my @alt_words = $alt_name =~ /([[:alpha:]]+)/g;
	
	# my @not_words = grep { $_ !~ /[[:alpha:]]{3}/ } @words;
	# my @alt_not_words = grep { $_ !~ /[[:alpha:]]{3}/ } @alt_words;
	
	# while ( @not_words && lc $not_words[0] eq lc $alt_not_words[0] )
	# {
	#     shift @not_words;
	#     shift @alt_not_words;
	# }
	
	# if ( @not_words == 0 && @alt_not_words == 0 )
	# {
	#     return 1;
	# }
	
	# else
	# {
	#     say encode_utf8("Edistance pair: $name | $alt_name");
	# }
    
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

    my ($rn, $key, $preliminary_concept_no) = @_;
    
    my ($strat_name) = split /\|/, $key;
    
    my $rc = { name => $strat_name, strat_name => { $strat_name => 1 },
	       preliminary_concept_no => "${preliminary_concept_no}A",
	       lat_min => $rn->{lat_min}, lat_max => $rn->{lat_max},
	       lng_min => $rn->{lng_min}, lng_max => $rn->{lng_max},
	       early_age => $rn->{early_age}, late_age => $rn->{late_age},
	       n_colls => $rn->{n_colls}, n_occs => $rn->{n_occs} };
    
    $rc->{cc}{$rn->{cc}} = 1 if $rn->{cc};
    $rc->{country}{$rn->{country}} = 1 if $rn->{country};
    
    $rn->{concept}{$key} = $rc;
    
    return $rc;
}


sub ConceptIsCompatible {

    my ($concept, $name, $match_level) = @_;
    
    unless ( $concept->{cc}{$name->{cc}} )
    {
	if ( $name->{lat_min} > $concept->{lat_max} + 10 ) { return undef; }
	if ( $name->{lat_max} < $concept->{lat_min} - 10 ) { return undef; }
	if ( $name->{lng_min} > $concept->{lng_max} + 10 ) { return undef; }
	if ( $name->{lng_max} < $concept->{lng_min} - 10 ) { return undef; }
    }

    if ( $match_level && $match_level == 2 )
    {
	if ( $name->{early_age} <= $concept->{late_age} ) { return undef; }
	if ( $name->{late_age} >= $concept->{early_age} ) { return undef; }
    }

    else
    {
	if ( $name->{early_age} < $concept->{late_age} - 10 ) { return undef; }
	if ( $name->{late_age} > $concept->{early_age} + 10 ) { return undef; }
    }
    
    return 1;
}


sub AddToConcept {

    my ($concept, $name, $key) = @_;
    
    if ( $name->{concept}{$key} )
    {
	say "Overwrote concept for key '$key'";
	$DB::single = 1;
    }
    
    $name->{concept}{$key} = $concept;
    
    my ($strat_name) = split /[|]/, $key;
    
    $concept->{strat_name}{$strat_name} = 1;
    
    $concept->{cc}{$name->{cc}} = 1 if $name->{cc};
    $concept->{country}{$name->{country}} = 1 if $name->{country};

    $concept->{lat_min} = $name->{lat_min} if $name->{lat_min} < $concept->{lat_min};
    $concept->{lat_max} = $name->{lat_max} if $name->{lat_max} > $concept->{lat_max};
    $concept->{lng_min} = $name->{lng_min} if $name->{lng_min} < $concept->{lng_min};
    $concept->{lng_max} = $name->{lng_max} if $name->{lng_max} > $concept->{lng_max};

    $concept->{early_age} = $name->{early_age} if $name->{early_age} > $concept->{early_age};
    $concept->{late_age} = $name->{late_age} if $name->{late_age} < $concept->{late_age};

    $concept->{n_colls} += $name->{n_colls};
    $concept->{n_occs} += $name->{n_occs};
}


sub ConsolidateConcepts {

    my ($concept, $alt) = @_;
    
    $concept->{alt}{$alt->{concept_no}} = 1;
    
    $concept->{cc}{$_} = 1 foreach keys $alt->{cc}->%*;
    $concept->{country}{$_} = 1 foreach keys $alt->{country}->%*;
    
    $concept->{lat_min} = $alt->{lat_min} if $alt->{lat_min} < $concept->{lat_min};
    $concept->{lat_max} = $alt->{lat_max} if $alt->{lat_max} > $concept->{lat_max};
    $concept->{lng_min} = $alt->{lng_min} if $alt->{lng_min} < $concept->{lng_min};
    $concept->{lng_max} = $alt->{lng_max} if $alt->{lng_max} > $concept->{lng_max};

    $concept->{early_age} = $alt->{early_age} if $alt->{early_age} > $concept->{early_age};
    $concept->{late_age} = $alt->{late_age} if $alt->{late_age} < $concept->{late_age};

    $concept->{n_colls} += $alt->{n_colls};
    $concept->{n_occs} += $alt->{n_occs};
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


sub AddToContext {

    my ($context1, $context2, $key) = @_;
    
    $context1->{lat_min} = $context2->{lat_min}
	if ! defined $context1->{lat_min} || $context2->{lat_min} < $context1->{lat_min};
    $context1->{lat_max} = $context2->{lat_max}
	if ! defined $context1->{lat_max} || $context2->{lat_max} > $context1->{lat_max};
    $context1->{lng_min} = $context2->{lng_min}
	if ! defined $context1->{lng_min} || $context2->{lng_min} < $context1->{lng_min};
    $context1->{lng_max} = $context2->{lng_max}
	if ! defined $context1->{lng_max} || $context2->{lng_max} > $context1->{lng_max};

    $context1->{early_age} = $context2->{early_age}
	if ! defined $context1->{early_age} || $context2->{early_age} > $context1->{early_age};
    $context1->{late_age} = $context2->{late_age}
	if ! defined $context1->{late_age} || $context2->{late_age} < $context1->{late_age};
    
    $context1->{n_colls} += $context2->{n_colls};
    $context1->{n_occs} += $context2->{n_occs};

    if ( $context2->{parent_rn} && $context2->{parent_rn}{$key} )
    {
	foreach my $a ( keys $context2->{parent_rn}{$key}->%* )
	{
	    $context1->{parent_rn}{$key}{$a} = $context2->{parent_rn}{$key}{$a};
	}
    }
    
    $context2->{consolidated_with} = $context1->{rkey};
}


sub ChooseConceptName {

    my ($context) = @_;
    
    my @candidates = keys $context->{strat_name}->%*;
    
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
	return $context->{name};
    }
}


sub InsertNames {

    my ($dbh, $name_values) = @_;

    DBCommand($dbh, "INSERT INTO $TABLE{STRAT_NAMES} (stratn_no, stratc_no, name, rank, cc, country, " .
	      "n_colls, n_occs, early_age, late_age, lat_min, lat_max, lng_min, lng_max) VALUES " .
	      $name_values);
}


sub InsertConcepts {

    my ($dbh, $concept_values) = @_;

    DBCommand($dbh, "INSERT INTO $TABLE{STRAT_CONCEPTS} (stratc_no, name, cc_list, country_list, " .
	     "n_colls, n_occs, early_age, late_age, lat_min, lat_max, lng_min, lng_max) VALUES " .
	      $concept_values);
}
	

sub InsertOpinions {

    my ($dbh, $opinion_values) = @_;

    DBCommand($dbh, "INSERT INTO $TABLE{STRAT_OPINIONS} (strato_no, child_no, child_rank, " .
	      "relationship, parent_no, parent_rank, reference_no) VALUES " .
	      $opinion_values);
}


sub InsertOpinions2 {

    my ($dbh, $opinion_values) = @_;

    DBCommand($dbh, "INSERT IGNORE INTO strat2_opinions (strato_no, child_no, child_rank, child_cc, " .
	      "relationship, parent_no, parent_rank, reference_no) VALUES " .
	      $opinion_values);
}


