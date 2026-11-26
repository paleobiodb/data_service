#!/usr/bin/env perl
#
# update_records.pl
# 
# Update various problems with PBDB records.
#
# Written by: Michael McClennen
# Created: 2026-08-06
# Derived from: update_unicode.pl

use strict;

use lib 'lib';
use feature 'say';

use CoreFunction qw(loadConfig configData connectDB);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use DBQuery qw(DBHashQuery DBRowQuery DBCommand DBInsert);
use MatrixBase qw(updateOccurrenceMatrix updateOccurrenceMatrixReids
		  deleteFromOccurrenceMatrix deleteReidsFromOccurrenceMatrix
		  updateOccurrenceCounts);
use OccurrenceBase qw(defaultSpeciesName);

use Getopt::Long qw(:config bundling no_auto_abbrev permute);
use Encode qw(encode_utf8);
use Term::ReadLine;

use PBDBEdit;


# Read the configuration file, and open a database connection.

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

# Adjust settings depending upon whether we are running inside the 'pbapi' container or
# on the outside.
#
# [Changed by Claude under the guidance of Michael McClennen: this is decided by the file
# /.dockerenv, which Docker creates in every container. It was decided by $ENV{PWD}, which
# is set only by a shell, so the test failed when the script was run by 'docker exec'
# or 'pbdb exec'.]

if ( -e '/.dockerenv' )
{
    PBDBEdit->log_filename('./datalogs/datalog-DATE.sql');
}

else
{
    $dbconf->{host} = '127.0.0.1';
    PBDBEdit->log_filename('/var/paleobiodb/datalogs/datalog-DATE.sql');
}

our($pbdb) = connectDB($opt_config, 'pbdb');

die "Could not connect to database: $DBI::errstr\n" unless $pbdb;


# Determine which update to perform based on @ARGV.

# [Changed by Claude under the guidance of Michael McClennen: added 'empty-species',
# 'unnamed-occs', and 'orphan-reids'.]

our (%update_routine) = ( limestome => \&UpdateLimestome,
			  'eodp-taxa' => \&UpdateEODPTaxa,
			  'empty-species' => \&UpdateEmptySpecies,
			  'unnamed-occs' => \&DeleteUnnamedOccs,
			  'orphan-reids' => \&DeleteOrphanReids );

our (@do_update);

foreach my $update_name ( @ARGV )
{
    if ( $update_routine{$update_name} )
    {
	push @do_update, $update_name;
    }
    
    else
    {
	say "Unknown update '$update_name'";
    }
}

unless ( @do_update )
{
    say "Nothing to update.";
}


# Authenticate to the database.

our ($authorizer_no) = $ENV{AUTHORIZER_NO};

unless ( $authorizer_no )
{
    print "What authorizer number shall be used for these updates? ";
    $authorizer_no = <STDIN>;
    $authorizer_no += 0;
}

unless ( $authorizer_no > 0 )
{
    die "Invalid authorizer_no '$authorizer_no'\n";
}

our ($session_id) = DBRowQuery($pbdb, "SELECT session_id FROM $TABLE{SESSION_DATA}
			WHERE authorizer_no = '$authorizer_no' ORDER BY record_date desc LIMIT 1");

unless ( $session_id )
{
    die "You must log in to the PBDB first.\n";
}

our $perms = Permissions->new($pbdb, $session_id) ||
    die "Could not establish permissions: $!\n";
    

# Make the selected updates, and exit.

&ExecuteUpdates();

exit;


sub ExecuteUpdates {

    foreach my $name ( @do_update )
    {
	&{$update_routine{$name}}();
    }
}


# UpdateLimestome ( )
#
# A few collection records list the formation 'Foraker Limestome' with an m instead of
# an n. This is an easily fixable mistake.

sub UpdateLimestome {

    say "\nUpdating collection records with formation 'Foraker Limesto>m<e'...";

    my $edt = PBDBEdit->new($pbdb, { permission => $perms, 
				     table => 'COLLECTION_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my $update_count = 0;
    
    my $rows = DBHashQuery($pbdb, <<~END_SQL);
	SELECT collection_no, formation FROM $TABLE{COLLECTION_DATA}
	WHERE formation = 'Foraker Limestome'
	END_SQL
    
    foreach my $row ( @$rows )
    {
	$row->{formation} = 'Foraker Limestone';
	$edt->update_record($row);
	$update_count++;
    }
    
    say "Generated $update_count update records.";
        
    $DB::single = 1;
    
    FinishUpdate($edt);
}


# UpdateEODPTaxa ( )
#
# This routine fixes the following problem: When the eODP dataset was originally
# compiled, most of the species had not yet been entered. So the taxon_no values mostly
# linked the occurrences to be uploaded to their genera, which had been entered. By the
# time the upload was actually done, Andrew McCoy had entered most or all of the
# species. But I (Michael McClennen) didn't know that, so I uploaded the original
# taxon_no values. This routine finds all occurrences whose identified name doesn't
# match the linked taxonomic name, and fixes them.

sub UpdateEODPTaxa {

    say "\nUpdating occurrence records (mostly from the eODP upload) with bad taxon links...\n";
    
    # Create an EditTransaction to do the updates.
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms,
				     table => 'OCCURRENCE_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my $eodp_count = 0;
    my $other_count = 0;
    
    # First do the simple case: fetch occurrences with no subgenus, no subspecies.
    
    my $rows1 = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.species_name, a.taxon_name as current_name,
	  na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name = concat_ws(' ', oc.genus_name, oc.species_name)
	WHERE not(subgenus_name <> '') and not(subspecies_name <> '') and
	  a.taxon_name not like '%(%' and a.taxon_name <> concat_ws(' ', oc.genus_name, oc.species_name)
	END_SQL
    
    # Then fetch occurrences with a subspecies.
    
    my $rows2 = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.species_name, oc.subspecies_name,
	  a.taxon_name as current_name, na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name =
		concat_ws(' ', oc.genus_name, oc.species_name, oc.subspecies_name)
	WHERE not(subgenus_name <> '') and subspecies_name <> '' and
	  a.taxon_name <> concat_ws(' ', oc.genus_name, oc.species_name, oc.subspecies_name)
	END_SQL
    
    # Then fetch occurrences with a subgenus.
    
    my $rows3 = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.subgenus_name, oc.species_name,
	  a.taxon_name as current_name, na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name = concat_ws(' ', oc.genus_name,
		concat('(', oc.subgenus_name, ')'), oc.species_name)
	WHERE subgenus_name <> '' and not(subspecies_name <> '') and
	  a.taxon_name <> concat_ws(' ', oc.genus_name,
		concat('(', oc.subgenus_name, ')'), oc.species_name)
	END_SQL
    
    # Then fetch occurrences with a subgenus and a subspecies.
    
    my $rows4 = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.subgenus_name, oc.species_name,
	  oc.subspecies_name, a.taxon_name as current_name, na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name = concat_ws(' ', oc.genus_name,
		concat('(', oc.subgenus_name, ')'), oc.species_name, oc.subspecies_name)
	WHERE subgenus_name <> '' and subspecies_name <> '' and
	  a.taxon_name <> concat_ws(' ', oc.genus_name, concat('(', oc.subgenus_name, ')'),
		oc.species_name, oc.subspecies_name)
	END_SQL
    
    # Do the updates.
    
    my @occurrence_nos;
    
    foreach my $row ( @$rows1, @$rows2, @$rows3, @$rows4 )
    {
	my $update = { occurrence_no => $row->{occurrence_no},
		       taxon_no => $row->{taxon_no} };
	
	$edt->update_record('OCCURRENCE_DATA', $update);
	
	push @occurrence_nos, $row->{occurrence_no};
	
	if ( $row->{upload} eq 'eODP' )
	{
	    $eodp_count++;
	}
	
	else
	{
	    $other_count++;
	}
    }
    
    say "    Generated $eodp_count eODP update records.";
    say "    Generated $other_count other update records.";
    
    $DB::single = 1;
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix...";
	
	updateOccurrenceMatrix($pbdb, \@occurrence_nos);
	
	say "Done.";
    }
    
    # Now do a second pass and fix occurrences with a subspecies where the species was
    # entered but not the subspecies, where the occurrence links to the genus.
    
    my @occurrence_nos2;
    my $eodp_count2 = 0;
    my $other_count2 = 0;
    
    $edt = PBDBEdit->new($pbdb, { permission => $perms,
				  table => 'OCCURRENCE_DATA',
				  allows => ['FIXUP_MODE'] } );
    
    # First, occurrences without a subgenus.
    
    my $rows2a = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.species_name, oc.subspecies_name,
	  a.taxon_name as current_name, na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name =
		concat_ws(' ', oc.genus_name, oc.species_name)
	WHERE not(subgenus_name <> '') and subspecies_name <> '' and
	  a.taxon_name <> concat_ws(' ', oc.genus_name, oc.species_name, oc.subspecies_name)
	END_SQL
    
    # Then fetch occurrences with a subgenus.
    
    my $rows4a = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.upload, oc.genus_name, oc.subgenus_name, oc.species_name,
	  oc.subspecies_name, a.taxon_name as current_name, na.taxon_no, na.taxon_name
	FROM occurrences as oc
	  join authorities as a using (taxon_no)
	  join authorities as na on na.taxon_name = concat_ws(' ', oc.genus_name,
		concat('(', oc.subgenus_name, ')'), oc.species_name)
	WHERE subgenus_name <> '' and subspecies_name <> '' and a.taxon_no <> na.taxon_no and
	  a.taxon_name <> concat_ws(' ', oc.genus_name, concat('(', oc.subgenus_name, ')'),
		oc.species_name, oc.subspecies_name)
	END_SQL
    
    foreach my $row ( @$rows2a, @$rows4a )
    {
	my $update = { occurrence_no => $row->{occurrence_no},
		       taxon_no => $row->{taxon_no} };
	
	$edt->update_record('OCCURRENCE_DATA', $update);
	
	push @occurrence_nos2, $row->{occurrence_no};
	
	if ( $row->{upload} eq 'eODP' )
	{
	    $eodp_count2++;
	}
	
	else
	{
	    $other_count2++;
	}
    }
    
    say "    Generated $eodp_count2 eODP update records.";
    say "    Generated $other_count2 other update records.";
    
    $DB::single = 1;
    
    # [Changed by Claude under the guidance of Michael McClennen: the matrix is updated
    # for the occurrences of this pass, @occurrence_nos2. It was given @occurrence_nos,
    # those of the first pass.]
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix...";
	
	updateOccurrenceMatrix($pbdb, \@occurrence_nos2);
	
	say "Done.";
    }
    
}

# UpdateEmptySpecies ( )
#
# [Added by Claude under the guidance of Michael McClennen.] Some occurrences and
# reidentifications were entered with an empty species name. A name without a species is
# now stored with the species name 'sp.' if it is linked to a genus or subgenus, or to no
# taxon, and 'indet.' if it is linked to a taxon above the genus level. This routine gives
# the existing records the species name they would have been given. Records whose genus
# is informal, or which have no genus name at all, are left alone; the latter are listed.
# A record whose subspecies name is 'indet' and which has no species name gets the species
# name 'indet.' and an empty subspecies name.

sub UpdateEmptySpecies {

    say "\nUpdating occurrence and reidentification records with an empty species name...\n";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms,
				     table => 'OCCURRENCE_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my (@occurrence_nos, @reid_nos, %count);
    
    foreach my $table ( 'OCCURRENCE_DATA', 'REID_DATA' )
    {
	my $key = $table eq 'OCCURRENCE_DATA' ? 'occurrence_no' : 'reid_no';
	
	my $rows = DBHashQuery($pbdb, <<~END_SQL);
	    SELECT $key, genus_name, genus_reso, subspecies_name, taxon_no
	    FROM $TABLE{$table}
	    WHERE (species_name = '' or species_name is null) and
	      coalesce(genus_reso, '') <> 'informal'
	    ORDER BY $key
	    END_SQL
	
	foreach my $row ( @$rows )
	{
	    my $record_no = $row->{$key};
	    
	    # A record with no genus name has no name at all, so it is left alone.
	    
	    unless ( defined $row->{genus_name} && $row->{genus_name} ne '' )
	    {
		say "    Skipped $key $record_no, which has no genus name.";
		$count{$table}{skipped}++;
		next;
	    }
	    
	    my $update = { $key => $record_no };
	    
	    # A subspecies name of 'indet' was put in the wrong field. Any other subspecies
	    # name without a species is left for someone to look at.
	    
	    if ( defined $row->{subspecies_name} && $row->{subspecies_name} ne '' )
	    {
		if ( $row->{subspecies_name} =~ /^indet[.]?$/ )
		{
		    $update->{species_name} = 'indet.';
		    $update->{subspecies_name} = '';
		}
		
		else
		{
		    say "    Skipped $key $record_no, which has the subspecies name " .
			"'$row->{subspecies_name}' and no species name.";
		    $count{$table}{skipped}++;
		    next;
		}
	    }
	    
	    else
	    {
		$update->{species_name} = defaultSpeciesName($pbdb, $row->{taxon_no});
	    }
	    
	    $edt->update_record($table, $update);
	    
	    $count{$table}{$update->{species_name}}++;
	    
	    if ( $table eq 'OCCURRENCE_DATA' )
	    {
		push @occurrence_nos, $record_no;
	    }
	    
	    else
	    {
		push @reid_nos, $record_no;
	    }
	}
	
	my $label = $table eq 'OCCURRENCE_DATA' ? 'occurrence' : 'reidentification';
	
	say sprintf("    Generated %d $label update records with 'sp.', %d with 'indet.'.",
		    $count{$table}{'sp.'} || 0, $count{$table}{'indet.'} || 0);
	say sprintf("    Skipped %d ${label}s.", $count{$table}{skipped})
	    if $count{$table}{skipped};
    }
    
    $DB::single = 1;
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix...";
	
	updateOccurrenceMatrix($pbdb, \@occurrence_nos) if @occurrence_nos;
	updateOccurrenceMatrixReids($pbdb, \@reid_nos) if @reid_nos;
	
	say "Done.";
    }
}


# DeleteUnnamedOccs ( )
#
# [Added by Claude under the guidance of Michael McClennen.] A few old occurrences have
# nothing in any of the name fields and are not linked to any taxon, so they hold no
# identification at all. Some of them are not even in a collection. This routine deletes
# them, except for any that have reidentifications or specimens, which are listed instead.

sub DeleteUnnamedOccs {

    say "\nDeleting occurrence records with nothing in any name field...\n";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms,
				     table => 'OCCURRENCE_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my $rows = DBHashQuery($pbdb, <<~END_SQL);
	SELECT oc.occurrence_no, oc.collection_no, oc.reference_no,
	  exists (SELECT 1 FROM $TABLE{REID_DATA} as re
		  WHERE re.occurrence_no = oc.occurrence_no) as has_reid,
	  exists (SELECT 1 FROM $TABLE{SPECIMEN_DATA} as sp
		  WHERE sp.occurrence_no = oc.occurrence_no) as has_spec
	FROM $TABLE{OCCURRENCE_DATA} as oc
	WHERE coalesce(oc.genus_name, '') = '' and coalesce(oc.subgenus_name, '') = '' and
	  coalesce(oc.species_name, '') = '' and coalesce(oc.subspecies_name, '') = '' and
	  coalesce(oc.taxon_no, 0) = 0
	ORDER BY oc.occurrence_no
	END_SQL
    
    my (@occurrence_nos, %collection_nos, $skip_count);
    
    foreach my $row ( @$rows )
    {
	my $occurrence_no = $row->{occurrence_no};
	
	if ( $row->{has_reid} || $row->{has_spec} )
	{
	    say "    Skipped occurrence_no $occurrence_no, which has " .
		($row->{has_reid} ? "reidentifications" : "specimens") . ".";
	    $skip_count++;
	    next;
	}
	
	say "    Deleting occurrence_no $occurrence_no (collection_no $row->{collection_no}, " .
	    "reference_no $row->{reference_no})" if $opt_verbose;
	
	$edt->delete_record('OCCURRENCE_DATA', $occurrence_no);
	
	push @occurrence_nos, $occurrence_no;
	$collection_nos{$row->{collection_no}} = 1 if $row->{collection_no};
    }
    
    say "    Generated " . scalar(@occurrence_nos) . " delete records.";
    say "    Skipped $skip_count occurrences." if $skip_count;
    
    $DB::single = 1;
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix and occurrence counts...";
	
	deleteFromOccurrenceMatrix($pbdb, \@occurrence_nos) if @occurrence_nos;
	updateOccurrenceCounts($pbdb, [ sort { $a <=> $b } keys %collection_nos ])
	    if %collection_nos;
	
	say "Done.";
    }
}


# DeleteOrphanReids ( )
#
# [Added by Claude under the guidance of Michael McClennen.] A few reidentifications
# belong to occurrences that were deleted without them. They cannot be shown with any
# occurrence, but the occurrence matrix is built from them, so it has rows for occurrences
# that do not exist. This routine deletes them and their rows in the occurrence matrix.

sub DeleteOrphanReids {

    say "\nDeleting reidentification records whose occurrence does not exist...\n";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms,
				     table => 'REID_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my $rows = DBHashQuery($pbdb, <<~END_SQL);
	SELECT re.reid_no, re.occurrence_no, re.collection_no
	FROM $TABLE{REID_DATA} as re left join $TABLE{OCCURRENCE_DATA} as oc
	  using (occurrence_no)
	WHERE oc.occurrence_no is null
	ORDER BY re.reid_no
	END_SQL
    
    my @reid_nos;
    
    foreach my $row ( @$rows )
    {
	say "    Deleting reid_no $row->{reid_no} (occurrence_no $row->{occurrence_no}, " .
	    "collection_no $row->{collection_no})" if $opt_verbose;
	
	$edt->delete_record('REID_DATA', $row->{reid_no});
	
	push @reid_nos, $row->{reid_no};
    }
    
    say "    Generated " . scalar(@reid_nos) . " delete records.";
    
    $DB::single = 1;
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix...";
	
	deleteReidsFromOccurrenceMatrix($pbdb, \@reid_nos) if @reid_nos;
	
	say "Done.";
    }
}



sub FinishUpdate {

    my ($edt) = @_;
    
    my $result;
    
    if ( !$edt->actions )
    {
	say "\nNothing to update.";
	$result = '';
    }
    
    elsif ( $edt->commit )
    {
	say "\nUpdate succeeded.";
	$result = 1;
    }
    
    else
    {
	say "\nUpdate failed!";
	$result = '';
    }
    
    my @warnings = $edt->nonfatals;
    my @errors = $edt->fatals;
    
    if ( @warnings )
    {
	say $_ foreach ("Warnings:", @warnings);
    }
    
    if ( @errors )
    {
	say $_ foreach ("Errors:", @errors);
    }
    
    return $result;
}

