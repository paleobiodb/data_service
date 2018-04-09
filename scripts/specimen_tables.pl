#!/usr/bin/env perl
# 
<<<<<<< HEAD
# specimen_tables.pl
# 
# Establish or reload the specimen tables.
=======
# timescale_tables.pl
# 
# Establish or reload the timescale tables.
>>>>>>> editing

use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;
<<<<<<< HEAD
=======
use Try::Tiny;
>>>>>>> editing

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage
		  logTimestamp);
<<<<<<< HEAD
use SpecimenTables qw(init_specelt_tables load_specelt_tables build_specelt_map);
=======
use SpecimenTables qw(establish_spec_element_tables establish_extra_specimen_tables
		      load_spec_element_tables);
>>>>>>> editing


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

<<<<<<< HEAD
my ($opt_init_tables, $opt_load_data, $opt_map_elts, $opt_debug,
    $opt_help, $opt_man, $opt_verbose);

GetOptions("init-elts" => \$opt_init_tables,
	   "load-elts" => \$opt_load_data,
	   "map-elts" => \$opt_map_elts,
=======
my ($opt_help, $opt_man, $opt_verbose, $opt_debug);
my ($opt_elt_tables, $opt_spec_tables, $opt_elt_data, $opt_place_data);

my $options = { };

GetOptions("init-elt-tables" => \$opt_elt_tables,
	   "init-spec-tables" => \$opt_spec_tables,
	   "load-elt-data" => \$opt_elt_data,
	   "load-place-data=s" => \$opt_place_data,
>>>>>>> editing
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
<<<<<<< HEAD
pod2usage(2) unless $opt_init_tables || $opt_load_data || $opt_map_elts;
=======
pod2usage(2) unless $opt_elt_tables || $opt_spec_tables || $opt_elt_data || $opt_place_data;
>>>>>>> editing
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# If we get here, then we have stuff to do. So get a database handle.

my $cmd_line_db_name = shift;
my $dbh = connectDB("config.yml", $cmd_line_db_name);

initMessages(2, 'Specimen tables');
logTimestamp();

# Then process the options and execute the necessary functions.

<<<<<<< HEAD
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
=======
$options->{verbose} = $opt_verbose ? 3 : 2;
$options->{debug} = 1 if $opt_debug;

# First check for the init-tables option.

if ( $opt_elt_tables )
{
    establish_spec_element_tables($dbh, $options);
}

if ( $opt_spec_tables )
{
    establish_extra_specimen_tables($dbh, $options);
}

# If the "load-data" option is given, then read data lines from standard
# input. We expect CSV format, with the first line giving the field names.

if ( $opt_elt_data )
{
    load_spec_element_tables($dbh, \*STDIN, $options);
    exit;
}

if ( $opt_place_data )
{
    my $filename = shift;
    load_place_data_table($dbh, $opt_place_data, $options);
>>>>>>> editing
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
<<<<<<< HEAD

    --init-elts         Create or re-create the necessary database tables.
                        The tables will be empty after this is done.
    
    --load-elts         Read specimen element data from standard input and load it
			into the new tables.

    --map-elts		Build the specimen element map table using the current taxonomy.


=======
    
    --debug             Produce debugging output
    
    --init-elt-tables   Create or re-create the database tables for specimen elements.
                        The tables will be empty after this is done.
    
    --load-elt-data     Read specimen element data from standard input, and
			replace the contents of the tables with the data read.
			It should be in CSV, with the first line giving field
			names.
    
    --init-spec-tables  Create or re-create the database tables for the expanded specimen
			system. The tables and/or columns added will be empty after this is done.
    
    --load-place-data [filename]

			Load WOF data into the 'wof_places' table.
    
>>>>>>> editing
=head1 OPTIONS

To be written later...

=cut


    
