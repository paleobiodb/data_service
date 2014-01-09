#!/opt/local/bin/perl

# Load data from an ESRI shapefile into the PaleobioDB.


use lib '../lib', 'lib';
use Getopt::Std;
use Geo::ShapeFile;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage);
use CollectionTables qw(startProtLandInsert finishProtLandInsert insertProtLandRecord);
use OccurrenceTables qw(buildOccurrenceTables);


# First parse options, and look for the name of the file to load as the
# command-line argument.

my %options;

getopts('d:f', \%options);

my ($command) = shift;

unless ( $command eq 'load' or $command eq 'add' )
{
    print <<USAGE;
Usage:
land_protection.pl <options> load <cc> <filename>
                             add <cc> <filename>
Options:
-d <database_name>
USAGE

    exit;
}

my ($cc, $filename) = @ARGV;

die "You must specify a country code, '$cc' is not valid" unless $cc =~ qr{ ^ [A-Z][A-Z] $ }x;
die "Could not open file $filename: $!" unless -r $filename || -r "$filename.shp";


# Initialize the output-message subsystem

initMessages(2, 'Load protected land data');


# Get a database handle.  If no database name was specified, the one named in
# config.yml will be used.

my $dbh = connectDB("config.yml", $options->{d});

my ($sql, $result);

# Verify the database that we are loading into.

if ( $dbh->{Name} =~ /database=([^;]+)/ )
{
    logMessage(1, "Using database: $1");
}
else
{
    logMessage(1, "Using connect string: $dbh->{Name}");
}


# Initialize the load.

startProtLandInsert($dbh);


# Then create a Geo::ShapeFile object with which to read in the data.

my $shapefile = new Geo::ShapeFile $filename
    or die "Could not open $filename: $!";


# Iterate through all of the shapes, entering each one in turn into the
# database. 

foreach my $i (1..$shapefile->shapes())
{
    logMessage(2, "    $i records...") if $i % 10000 == 0;
    
    my $shape = $shapefile->get_shp_record($i);
    my $db = $shapefile->get_dbf_record($i);
    
    my $category = 'GEN';
    
    if ( $cc eq 'US' )
    {
	$category = $db->{AGBUR} eq 'NPS' ? 'NPS'
					  : 'FED';
    }
    
    else
    {
	warn "unrecognized country code '$cc'\n";
    }
    
    # Now go through each point and add it to the specification for this
    # polygon.
    
    my $wkt = 'POLYGON(';
    
    foreach my $j (1..$shape->num_parts)
    {
	my @points = $shape->get_part($j);
	
	unless ( $points[0]{X} eq $points[-1]{X} and $points[0]{Y} eq $points[-1]{Y} )
	{
	    push @points, $points[0];
	}
	
	$wkt .= $j > 1 ? ',(' : '(';
	$wkt .= join(',', map { $_->{X} . ' ' . $_->{Y} } @points);
	$wkt .= ')';
    }
    
    $wkt .= ')';
    
    insertProtLandRecord($dbh, $cc, $category, $wkt);
    
    my $a = 1;	# we can stop here when debugging.
}


finishProtLandInsert($dbh, [$cc]);
