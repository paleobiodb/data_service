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

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage
		  logTimestamp);
use SpecimenTables qw(establish_spec_element_tables load_spec_element_tables);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_help, $opt_man, $opt_verbose, $opt_debug);
my ($opt_init_tables, $opt_load_data);

my $options = { };

GetOptions("init-tables" => \$opt_init_tables,
	   "load-data" => \$opt_load_data,
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(2) unless $opt_init_tables || $opt_load_data;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# If we get here, then we have stuff to do. So get a database handle.

my $cmd_line_db_name = shift;
my $dbh = connectDB("config.yml", $cmd_line_db_name);

initMessages(2, 'Specimen tables');
logTimestamp();

# Then process the options and execute the necessary functions.

$options->{verbose} = $opt_verbose ? 3 : 2;
$options->{debug} = 1 if $opt_debug;

# First check for the init-tables option.

if ( $opt_init_tables )
{
    establish_spec_element_tables($dbh, $options);
}

# If the "load-data" option is given, then read data lines from standard
# input. We expect CSV format, with the first line giving the field names.

if ( $opt_load_data )
{
    # open(my $input, "<&1") || die "error opening stdin: $!";
    
    load_spec_element_tables($dbh, \*STDIN, $options);
    exit;
}

1;


__END__

=head1 NAME

timescale_tables.pl - initialize and/or reset the new timescale tables for The Paleobiology Database

=head1 SYNOPSIS

  timescale_tables [options] [database_name]

  Options:
    
    --help              Display a brief help message
    
    --man               Display the full documentation
    
    --debug             Produce debugging output
    
    --init-tables       Create or re-create the necessary database tables.
                        The tables will be empty after this is done.
    
    --load-data         Read specimen element data from standard input, and
			replace the contents of the tables with the data read.
			It should be in CSV, with the first line giving field
			names.
    
=head1 OPTIONS

To be written later...

=cut


    
