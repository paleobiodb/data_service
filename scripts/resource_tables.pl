#!/usr/bin/env perl
# 
# timescale_tables.pl
# 
# Establish or reload the timescale tables.

use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;
use Try::Tiny;
use JSON;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use FileData qw(decode_input_lines);
use Permissions;
use ResourceDefs;
use ResourceTables;
use ResourceEdit;
use TableDefs qw(init_table_names %TABLE);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_update, $opt_addupdate, $opt_delete, $opt_replace_tables);
my ($opt_session_id, $opt_actual, $opt_debug, $opt_man, $opt_help, $opt_verbose);

GetOptions("replace-tables" => \$opt_replace_tables,
	   "update" => \$opt_update,
	   "addupdate" => \$opt_addupdate,
	   "delete=s" => \$opt_delete,
	   "session=s" => \$opt_session_id,
	   "actual" => \$opt_actual,
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(2) unless $opt_update || $opt_addupdate || $opt_delete || $opt_replace_tables;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# If we get here, then we have stuff to do. So get a database handle.

my $dbh = connectDB("config.yml");

ResourceEdit->configure(configData);


my $message = $opt_actual ? 'ACTUAL' : 'TEST';

initMessages(2, $message);
logTimestamp();


my $options = { };
$options->{debug} = 1 if $opt_debug;

# Unless we were directed to use the real tables, use the test ones.

unless ( $opt_actual )
{
    init_table_names(configData, 1);
    ResourceDefs->enable_test_mode('eduresources', $opt_debug);
}


# If we were given an option that involves database modification, then authenticate and generate
# an EditTransaction object (subclass ResourceEdit).


if ( $opt_update || $opt_addupdate || $opt_delete )
{
    my $count = 0;
    $count++ if $opt_update;
    $count++ if $opt_addupdate;
    $count++ if $opt_delete;
    
    die "Cannot do more than one of --update, --addupdate, --delete.\n" if $count > 1;
    
    my $perms = Permissions->new($dbh, $opt_session_id, $TABLE{RESOURCE_QUEUE}, $options);
    my $allows = { };
    
    $allows->{CREATE} = 1 if $opt_addupdate;
    
    my $edt = ResourceEdit->new($dbh, $perms, $TABLE{RESOURCE_QUEUE}, $allows);
    
    # Unless we were given a list of identifiers for deletion, read records from whatever files are
    # specified on the command line.
    
    my $records;
    
    unless ( $opt_delete && $opt_delete ne '1' )
    {
	$records = decode_input_lines('-');
    }
    
    # Now process these records.
    
    if ( $opt_update || $opt_addupdate )
    {
	foreach my $r (@$records)
	{
	    $edt->insert_update_record($TABLE{RESOURCE_QUEUE}, $r);
	}
    }
    
    elsif ( $opt_delete && $opt_delete ne '1' )
    {
	my @ids = split(/\s*,\s*/, $opt_delete);
	
	foreach my $id (@ids)
	{
	    $edt->delete_record($id) if $id;
	}
    }
    
    else
    {
	foreach my $r (@$records)
	{
	    $edt->delete_record($r);
	}
    }
    
    # Now execute the database transaction.
    
    $edt->execute;
    
    # If any errors or warnings were generated, report them now:
    
    foreach my $e ($edt->errors)
    {
	logMessage(1, $e);
    }
    
    foreach my $w ($edt->warnings)
    {
	logMessage(1, $w);
    }

    return;
}


if ( $opt_replace_tables )
{
    my $result = ResourceTables->establish_tables($dbh, { debug => $opt_debug });
    
    if ( $result )
    {
	logMessage("New tables were installed.");
    }
}

1;


__END__

=head1 NAME

resource_tables.pl - perform database operations on the resource tables for The Paleobiology Database

=head1 SYNOPSIS

  timescale_tables [options] [database_name]

  Options:
    
    --help              Display a brief help message
    
    --man               Display the full documentation
    
    --debug             Produce debugging output
    
    --addupdate         whatever
    
    --update            whatever
    
    --delete            whatever
    
    --session=<id>      authenticate using the specified login session identifier
    
=head1 OPTIONS

To be written later...

=cut


    
