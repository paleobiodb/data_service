#!/opt/local/bin/perl

use lib '../lib', 'lib';
use Getopt::Std;

use DBHandle;
use TaxonTrees;
use Taxonomy;

my %options;

# First parse option switches

getopts('tT:mbckuivry', \%options);

# If we were given an argument, then use that as the database name
# overrriding what was in the configuration file.

my $cmd_line_db_name = shift;

# Get a database handle and a taxonomy object.

my $dbh = DBHandle->connect($cmd_line_db_name);

my $t = Taxonomy->new($dbh, 'taxon_trees');

# If we are debugging, stop here.

$DB::single = 1;

# Now make sure that the 'orig_no' field is set for each entry in the
# authorities table.

ensureOrig($dbh);
populateOrig($dbh);

# Initialize the output-message subsystem

TaxonTrees::initMessages(2);

# Call the routines that build the various caches, depending upon the options
# that were specified.

TaxonTrees::loadIntervalTables($dbh, 1) if $options{i};
TaxonTrees::computeIntervalMap($dbh) if $options{u};
TaxonTrees::computeCollectionTables($dbh, $options{k}, $CONFIG->{bins}) if $options{c};
TaxonTrees::computeOccurrenceTables($dbh) if $options{m};
TaxonTrees::computeCollectionCounts($dbh) if $options{v};
TaxonTrees::createRankMap($dbh) if $options{r};
TaxonTrees::buildTables($dbh, 'taxon_trees', { msg_level => 2 }, $options{T}) 
    if $options{t} or $options{T};
TaxonTrees::computeTaxaCacheTables($dbh, 'taxon_trees') if $options{y};

print "done rebuilding tables\n";

# Done!

exit;



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
