#!/opt/local/bin/perl

# This script stores "original combination" information in taxa_tree_cache.
# 
# Author: Michael McClennen

use lib qw(../cgi-bin);
use strict;	

use Class::Date qw(date localdate gmdate now);
use POSIX qw(ceil floor);
use DBI;

# PBDB modules
use DBConnection;
use DBTransactionManager;

use TaxonInfo;


# First get a database connection using the pbdb legacy code, then grab the
# actual dbh.

my $dbt = new DBTransactionManager();
my $dbh = $dbt->dbh;

# If the authorities table doesn't have an 'orig_no' field, add it.

my ($table_name, $table_definition);

eval { ($table_name, $table_definition) = 
	   $dbh->selectrow_array("SHOW CREATE TABLE authorities"); 
   };

unless ( $table_definition =~ /`orig_no` int/ )
{
    $dbh->do("ALTER TABLE authorities
	      ADD COLUMN orig_no INT UNSIGNED NOT NULL AFTER taxon_no");
    
    $dbh->do("ALTER TABLE authorities
	      ADD KEY (orig_no)");
}

# Now go through the authorities table.  For each record, we get the taxon_no,
# call "getOriginalCombination", and store the result in orig_no.

my $select_stmt = $dbh->prepare("SELECT taxon_no FROM authorities WHERE orig_no = 0");
$select_stmt->execute();

my $insert_stmt = $dbh->prepare("UPDATE authorities SET orig_no = ? WHERE taxon_no = ?");
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
