#!/usr/bin/env perl
#
# update_unicode.pl
#
# Change unicode escapes (i.e. &#352;) to unicode characters in all PBDB fields
# that might have them.
#
# Written by: Michael McClennen
# Created: 2025-10-07

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

if ( $ENV{PWD} !~ qr{^/var/paleomacro/pbapi} )
{
    $dbconf->{host} = '127.0.0.1';
}

our($pbdb) = connectDB($opt_config, 'pbdb');

die "Could not connect to database: $DBI::errstr\n" unless $pbdb;


# Determine which tables to update based on @ARGV.

our (%core_table) = (collections => 1, colls => 1,
		     occurrences => 1, occs => 1,
		     reidentifications => 1, reids => 1,
		     specimens => 1, specs => 1,
		     authorities => 1, auths => 1,
		     opinions => 1, ops => 1,
		     refs => 1,
		     all => 1);

our (%update_table);

foreach my $table_name ( @ARGV )
{
    if ( $core_table{$table_name} )
    {
	$update_table{$table_name} = 1;
    }

    else
    {
	say "Unknown table '$table_name'";
    }
}

unless ( %update_table )
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
    

# Ensure that our updates are logged to the proper datalog file.

PBDBEdit->log_filename('./datalogs/datalog-DATE.sql');


# Update the selected tables, and exit.

&UpdateTables();

exit;


sub UpdateTables {

    if ( $update_table{collections} || $update_table{colls} || $update_table{all} )
    {
	UpdateCollections();
    }
    
    if ( $update_table{occurrences} || $update_table{occs} || $update_table{all} )
    {
	UpdateOccurrences();
    }
    
    if ( $update_table{reidentifications} || $update_table{reids} || $update_table{all} )
    {
	UpdateReidentifications();
    }
    
    if ( $update_table{specimens} || $update_table{specs} || $update_table{all} )
    {
	say "Update is not yet implemented for 'specimens'";
    }
    
    if ( $update_table{authorities} || $update_table{auths} || $update_table{all} )
    {
	say "Update is not yet implemented for 'authorities'";
    }
    
    if ( $update_table{opinions} || $update_table{ops} || $update_table{all} )
    {
	say "Update is not yet implemented for 'opinions'";
    }
    
    if ( $update_table{refs} || $update_table{all} )
    {
	UpdateRefs();
    }
}


sub UpdateCollections {

    say "\nUpdating escaped unicode characters in 'pbdb.collections'...";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms, 
				     table => 'COLLECTION_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my %update_record;
    
    my $update_count = 0;
    
    my @convert_fields = qw(collection_name collection_aka state county geogcomments
			    localsection localbed regionalsection regionalbed stratcomments
			    lithdescript geology_comments preservation_comments
			    component_comments collection_comments taxonomy_comments);
    
    foreach my $field ( @convert_fields )
    {
	my $rows = DBHashQuery($pbdb, "SELECT collection_no, $field FROM $TABLE{COLLECTION_DATA}
			WHERE $field like '%&#%'");
	
	$update_count += UpdateRecords(\%update_record, $rows, 'collection_no', $field);
    }
        
    my $record_count = scalar(%update_record);
    
    say "Generated $record_count update records for $update_count updates.";
    
    foreach my $key_no ( sort { $a <=> $b } keys %update_record )
    {
	$edt->update_record($update_record{$key_no});
    }
    
    $DB::single = 1;
    
    FinishUpdate($edt);
}


sub UpdateOccurrences {

    say "\nUpdating escaped unicode characters in 'pbdb.occurrences'...";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms, 
				     table => 'OCCURRENCE_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my %update_record;
    
    my $update_count = 0;
    
    my @convert_fields = qw(comments);
    
    foreach my $field ( @convert_fields )
    {
	my $rows = DBHashQuery($pbdb, "SELECT occurrence_no, $field FROM $TABLE{OCCURRENCE_DATA}
			WHERE $field like '%&#%'");
	
	$update_count += UpdateRecords(\%update_record, $rows, 'occurrence_no', $field);
    }
        
    my $record_count = scalar(%update_record);
    
    say "Generated $record_count update records for $update_count updates.";
    
    foreach my $key_no ( sort { $a <=> $b } keys %update_record )
    {
	$edt->update_record($update_record{$key_no});
    }
    
    $DB::single = 1;
    
    FinishUpdate($edt);
}


sub UpdateReidentifications {

    say "\nUpdating escaped unicode characters in 'pbdb.reidentifications'...";
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms, 
				     table => 'REID_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    my %update_record;
    
    my $update_count = 0;
    
    my @convert_fields = qw(comments);
    
    foreach my $field ( @convert_fields )
    {
	my $rows = DBHashQuery($pbdb, "SELECT reid_no, $field FROM $TABLE{REID_DATA}
			WHERE $field like '%&#%'");
	
	$update_count += UpdateRecords(\%update_record, $rows, 'reid_no', $field);
    }
        
    my $record_count = scalar(%update_record);
    
    say "Generated $record_count update records for $update_count updates.";
    
    foreach my $key_no ( sort { $a <=> $b } keys %update_record )
    {
	$edt->update_record($update_record{$key_no});
    }
    
    $DB::single = 1;
    
    FinishUpdate($edt);
}


sub UpdateRefs {

    say "\nUpdating escaped unicode characters in 'pbdb.refs'...";
    
    my $edt = ReferenceEdit->new($pbdb, { permission => $perms, 
					  table => 'REFERENCE_DATA',
					  allows => ['FIXUP_MODE'] } );
    
    my %update_record;
    
    my $update_count = 0;
    
    my @convert_fields = qw(author1init author1last author2init author2last otherauthors
			    reftitle pubtitle editors publisher comments);
    
    foreach my $field ( @convert_fields )
    {
	my $rows = DBHashQuery($pbdb, "SELECT reference_no, $field FROM $TABLE{REFERENCE_DATA}
			WHERE $field like '%&#%'");
	
	$update_count += UpdateRecords(\%update_record, $rows, 'reference_no', $field);
    }
        
    my $record_count = scalar(%update_record);
    
    say "Generated $record_count update records for $update_count updates.";
    
    foreach my $key_no ( sort { $a <=> $b } keys %update_record )
    {
	$edt->update_record($update_record{$key_no});
    }
    
    $DB::single = 1;
    
    FinishUpdate($edt);
}


sub UpdateRecords {

    my ($update_hash, $rows, $key_field, $update_field) = @_;
    
    my $count = 0;
    
    foreach my $r ( @$rows )
    {
	if ( $r->{$update_field} =~ /&#/ )
	{
	    $update_hash->{$r->{$key_field}} ||= { $key_field => $r->{$key_field} };
	    $r->{$update_field} =~ s/&\#(\d+);*/chr($1)/eg;
	    $r->{$update_field} =~ s/&\#[xX](\w+)(?:;+|$)/chr(hex($1))/eg;
	    $update_hash->{$r->{$key_field}}{$update_field} = $r->{$update_field};
	    say encode_utf8("Record # $r->{$key_field} / $update_field : '$r->{$update_field}'");
	    $count++;
	}
    }
    
    return $count;
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

