#!/opt/local/bin/perl

# This script stores "original combination" information in taxa_tree_cache.
# 
# Author: Michael McClennen

use lib qw(../cgi-bin/);
#use lib qw(../pbdata/lib);
use strict;	



# CPAN modules
#use CGI qw(escapeHTML);
#use URI::Escape;
#use Text::CSV_XS;
#use CGI::Carp qw(fatalsToBrowser);
use Class::Date qw(date localdate gmdate now);
use POSIX qw(ceil floor);
use DBI;

# PBDB modules
use DBConnection;
use DBTransactionManager;
#use Session;

use TaxonInfo;
#use PBDataQuery;
#use PBTaxonQuery;


# First get a database connection using the pbdb legacy code, then grab the
# actual dbh.

my $dbt = new DBTransactionManager();
my $dbh = $dbt->dbh;

# Now go through each record of taxa_tree_cache.  For each record, we get the
# taxon_no, call "getOriginalCombination", and store the result in
# orig_combo_no. 

#my ($total_count) = $dbh->selectrow_array("SELECT count(*) from taxa_tree_cache WHERE orig_combo_no = 0");

my $select_stmt = $dbh->prepare("SELECT taxon_no FROM taxa_tree_cache WHERE orig_no = 0");
$select_stmt->execute();

my $insert_stmt = $dbh->prepare("UPDATE taxa_tree_cache SET orig_no = ? WHERE taxon_no = ?");
my $count = 0;
my $length = 0;

#select STDOUT; $| = 1;

while ( my ($taxon_no) = $select_stmt->fetchrow_array )
{
    my $orig_no = TaxonInfo::getOriginalCombination($dbt, $taxon_no);
    
    $insert_stmt->execute($orig_no, $taxon_no);
    
    $count++;
#    if ( $count % 100 == 0 )
#    {
#	my $pct = int(($count / $total_count) * 100) . '%';
#	print("\b" x $length) if $length > 0;
#	print $pct;
#	$length = length($pct);
#    }
}

print "Finished updating orig_no: $count entries updated.\n";
