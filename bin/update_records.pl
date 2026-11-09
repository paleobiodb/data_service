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

our (%update_routine) = (limestome => \&UpdateLimestome);
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


sub FinishUpdate {

    my ($edt) = @_;

    if ( !$edt->actions )
    {
	say "Nothing to update.";
    }
    
    elsif ( $edt->commit )
    {
	say "Update succeeded.";
    }
    
    else
    {
	say "Update failed!";
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
}

