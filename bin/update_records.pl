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
use MatrixBase qw(updateOccurrenceMatrix);

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

if ( $ENV{PWD} =~ qr{^/var/paleomacro/pbapi} )
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

our (%update_routine) = ( limestome => \&UpdateLimestome,
			  'eodp-taxa' => \&UpdateEODPTaxa );

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
    
    if ( FinishUpdate($edt) )
    {
	say "Updating occurrence matrix...";
	
	updateOccurrenceMatrix($pbdb, \@occurrence_nos);
	
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

