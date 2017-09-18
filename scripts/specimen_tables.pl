#!/usr/bin/env perl
# 
# specimen_tables.pl
# 
# Establish or reload the specimen tables.

use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage
		  logTimestamp);
use SpecimenTables qw(init_specelt_tables load_specelt_tables build_specelt_map);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_init_tables, $opt_load_data, $opt_map_elts, $opt_debug,
    $opt_help, $opt_man, $opt_verbose);

GetOptions("init-elts" => \$opt_init_tables,
	   "load-elts" => \$opt_load_data,
	   "map-elts" => \$opt_map_elts,
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(2) unless $opt_init_tables || $opt_load_data || $opt_map_elts;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# If we get here, then we have stuff to do. So get a database handle.

my $cmd_line_db_name = shift;
my $dbh = connectDB("config.yml", $cmd_line_db_name);

initMessages(2, 'Specimen tables');
logTimestamp();

# Then process the options and execute the necessary functions.

my $options = { };

$options->{verbose} = $opt_verbose ? 3 : 2;
$options->{debug} = 1 if $opt_debug;

if ( $opt_init_tables )
{
    init_specelt_tables($dbh, $options);
}

if ( $opt_load_data )
{
    load_specelt_tables($dbh, '-', $options);
}

if ( $opt_map_elts )
{
    build_specelt_map($dbh, 'taxon_trees', $options);
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

    --init-elts         Create or re-create the necessary database tables.
                        The tables will be empty after this is done.
    
    --load-elts         Read specimen element data from standard input and load it
			into the new tables.

    --map-elts		Build the specimen element map table using the current taxonomy.


=head1 OPTIONS

To be written later...

=cut


    
