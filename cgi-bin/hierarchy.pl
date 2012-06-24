#!/opt/local/bin/perl

# 

use lib qw(.);
use strict;	

# CPAN modules
use CGI qw(escapeHTML);
use URI::Escape;
use CGI::Carp qw(fatalsToBrowser);
use DBI;

# PBDB modules
use DBConnection;
use DataQuery;
use TreeQuery;

# Autoloaded libs
use Constants qw($READ_URL $WRITE_URL $HOST_URL $HTML_DIR $DATA_DIR $IS_FOSSIL_RECORD $TAXA_TREE_CACHE $DB $PAGE_TOP $PAGE_BOTTOM $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO);

#*************************************
# some global variables 
#*************************************
# 
# Some of these variable names are used throughout the code
# $q		: The CGI object - used for getting parameters from HTML forms.
# $s		: The session object - used for keeping track of users, see Session.pm
# $hbo		: HTMLBuilder object, used for populating HTML templates with data. 
# $dbt		: DBTransactionManager object, used for querying the database.
# $dbt->dbh	: Connection to the database, see DBConnection.pm
#

# Create the CGI object, so we can get at the parameters.
my $q = new CGI;

# Make a database connection handle
my $dbh = DBConnection::connect();

# Output the proper header
print $q->header('application/json');

# Create a query and execute it.  Assume that the content type will be JSON.

my (%params) = $q->Vars;
$params{ct} = 'json';

my $query = TreeQuery->new($dbh);

$query->setParameters(\%params);

if ( $query->fetchMultiple() )
{
    print $query->generateCompoundResult();
}

else
{
    print $query->reportError();
}

