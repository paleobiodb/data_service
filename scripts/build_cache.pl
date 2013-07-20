#!/opt/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Taxonomy;
use Getopt::Std;

my $dbh = DBConnection::connect();

my $t = Taxonomy->new($dbh, 'taxon_trees');

my %options;

getopts('tT:mbvk', \%options);
#getopts('abcdefghikxmMyt', \%options);

ensureOrig($dbh);
populateOrig($dbh);

TaxonTrees::initMessages(2);

TaxonTrees::computeCollectionTables($dbh) if $options{b};
TaxonTrees::computeOccurrenceMatrix($dbh) if $options{m};
TaxonTrees::computeCollectionCounts($dbh) if $options{v};
TaxonTrees::buildTables($dbh, 'taxon_trees', { msg_level => 2 }, $options{T}) 
    if $options{t} or $options{T};

print "done rebuilding caches\n";



# ensureOrig ( dbh )
# 
# Unless the authorities table has an 'orig_no' field, create one.

sub ensureOrig {
    
    my ($dbh) = @_;
    
    # Check the table definition, and return if it already has 'orig_no'.
    
    my ($table_name, $table_definition) = $dbh->selectrow_array("SHOW CREATE TABLE authorities"); 
    
    return if $table_definition =~ /`orig_no` int/;
    
    print STDERR "Creating 'orig_no' field...\n";
    
    # Create the 'orig_no' field.
    
    $dbh->do("ALTER TABLE authorities
	      ADD COLUMN orig_no INT UNSIGNED NOT NULL AFTER taxon_no");
    
    return;
}


# createOrig ( dbh )
# 
# If there are any entries where 'orig_no' is not set, fill them in.

sub populateOrig {

    my ($dbh) = @_;
    
    # Check to see if we have any unset orig_no entries, and return if we do
    # not.
    
    my ($count) = $dbh->selectrow_array("
	SELECT count(*) from authorities
	WHERE orig_no = 0");
    
    return unless $count > 0;
    
    # Populate all unset orig_no entries.  This algorithm is taken from
    # TaxonInfo::getOriginalCombination() in the old code.
    
    print STDERR "Populating 'orig_no' field...\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.child_spelling_no
	SET a.orig_no = o.child_no WHERE a.orig_no = 0");
    
    print STDERR "   child_spelling_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.child_no
	SET a.orig_no = o.child_no WHERE a.orig_no = 0");
    
    print STDERR "   child_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.parent_spelling_no
	SET a.orig_no = o.parent_no WHERE a.orig_no = 0");
        
    print STDERR "   parent_spelling_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.parent_no
	SET a.orig_no = o.parent_no WHERE a.orig_no = 0");
    
    print STDERR "   parent_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a
	SET a.orig_no = a.taxon_no WHERE a.orig_no = 0");
    
    print STDERR "   self: $count\n";
    
    # Index the field, unless there is already an index.
    
    my ($table_name, $table_definition) = $dbh->selectrow_array("SHOW CREATE TABLE authorities"); 
    
    return if $table_definition =~ /KEY `orig_no`/;
    
    $dbh->do("ALTER TABLE authorities
              ADD KEY (orig_no)");
    
    print STDERR "  done.\n";
}
