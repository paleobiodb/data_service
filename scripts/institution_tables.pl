#!/usr/bin/env perl
# 
# timescale_tables.pl
# 
# Establish or reload the timescale tables.

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
use InstitutionTables qw(init_institution_tables);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_init_tables, $opt_help, $opt_man, $opt_verbose);
my $options = { };

GetOptions("init-tables" => \$opt_init_tables,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(2) unless $opt_init_tables;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# If we get here, then we have stuff to do. So get a database handle.

my $cmd_line_db_name = shift;
my $dbh = connectDB("config.yml", $cmd_line_db_name);

initMessages(2, 'Timescale tables');
logTimestamp();

# Then process the options and execute the necessary functions.

$options->{verbose} = $opt_verbose ? 3 : 2;

if ( $opt_init_tables )
{
    init_institution_tables($dbh, $options);
}

1;


__END__

=head1 NAME

institution_tables.pl - initialize and/or reset the new timescale tables for The Paleobiology Database

=head1 SYNOPSIS

  institution_tables.pl [options] [database_name]

  Options:
    
    --help              Display a brief help message
    
    --man               Display the full documentation

    --init-tables       Create or re-create the necessary database tables.
                        The tables will be empty after this is done.
    
    --verbose | -v	Display extra diagnostic messages

=head1 OPTIONS

To be written later...

=cut


    
