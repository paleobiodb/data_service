# 
# DBHandle.pm
# 
# Basic database connection routines.


package DBHandle;

use strict;

use DBI;
use YAML qw(LoadFile);
use Term::ReadKey;
use Carp qw(carp croak);


# connect ( db_name )
# 
# Create a new database handle, reading the connection attributes from
# config.yml.  If $db_name is given, it overrides the database name from the
# configuration file.

sub connect {

    my ($class, $db_name) = @_;
    
    # Parse the configuration file.  It might be in 'config.yml' or
    # '../config.yml'. 
    
    my $CONFIG = LoadFile('config.yml');
    
    unless ( $CONFIG )
    {
	$CONFIG = LoadFile('../config.yml');
    }
    
    carp "Could not read config.yml: $!" unless $CONFIG;
    
    # Extract the relevant configuration parameters.  Ask for a password unless
    # one was specified in the configuration file.
    
    my $DB_DRIVER = $CONFIG->{plugins}{Database}{driver};
    my $DB_NAME = $db_name || $CONFIG->{plugins}{Database}{database};
    my $DB_HOST = $CONFIG->{plugins}{Database}{host};
    my $DB_PORT = $CONFIG->{plugins}{Database}{port};
    my $DB_USER = $CONFIG->{plugins}{Database}{username};
    my $DB_PASSWD = $CONFIG->{plugins}{Database}{password};
    my $DBI_PARAMS = $CONFIG->{plugins}{Database}{dbi_params};
    
    croak "You must specify the database driver as 'driver' in config.yml" unless $DB_DRIVER;
    croak "You must specify the database name as 'database' in config.yml or on the command line" unless $DB_NAME;
    croak "You must specify the database host as 'host' in config.yml" unless $DB_HOST;
    
    unless ( $DB_PASSWD )
    {
	ReadMode('noecho');
	print "Password: ";
	$DB_PASSWD = <STDIN>;
	chomp $DB_PASSWD;
	ReadMode('restore');
    }
    
    # Connect to the database.
    
    my $dsn = "DBI:$DB_DRIVER:database=$DB_NAME";
    
    if ( $DB_HOST )
    {
	$dsn .= ";host=$DB_HOST";
    }
    
    if ( $DB_PORT )
    {
	$dsn .= ";port=$DB_PORT";
    }
    
    $dsn .= ";mysql_client_found_rows=0";
    
    my $dbh = DBI->connect($dsn, $DB_USER, $DB_PASSWD, $DBI_PARAMS);
    
    croak "Could not connect to database: $DBI::errstr" unless $dbh;
    
    return $dbh;
}


1;
