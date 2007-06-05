# made from a copy of fixAuthRefnos 6.4.04
# populates the taxon_no field in the occurrences table

use lib "../cgi-bin";
use Class::Date qw(date localdate gmdate now);
use DBI;
use DBTransactionManager;
use Session;
use Class::Date qw(date localdate gmdate now);
use DBConnection;

# Flags and constants
my $DEBUG = 0;                  # The debug level of the calling program
my $sql;                                # Any SQL string

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

$|=1;

# get a list of species appearing only once and go through it
$sql = "SELECT taxon_name,taxon_no FROM authorities WHERE taxon_rank='species' GROUP BY taxon_name HAVING count(*)=1";
@authrefs = @{$dbt->getData($sql)};
for $ar (@authrefs)	{
	($genus,$species) = split / /,$ar->{taxon_name};

	$sql = "SELECT count(*) AS c FROM occurrences WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND (species_reso NOT LIKE '%informal%' OR species_reso IS NULL) AND genus_name='$genus' AND species_name='$species'";
	$count = $count + ${$dbt->getData($sql)}[0]->{c};

	$no = $ar->{taxon_no};
	$sql = "UPDATE occurrences SET modified=modified, taxon_no=$no WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND (species_reso NOT LIKE '%informal%' OR species_reso IS NULL) AND genus_name='$genus' AND species_name='$species'";
    print "$sql\n";
    if ($doUpdate) {
	    $dbt->getData($sql);
    }
}
print "$count species occurrences\n";

for $ar (@authrefs)	{
	($genus,$species) = split / /,$ar->{taxon_name};

	$sql = "SELECT count(*) AS c FROM reidentifications WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND (species_reso NOT LIKE '%informal%' OR species_reso IS NULL) AND genus_name='$genus' AND species_name='$species'";
	$rcount = $rcount + ${$dbt->getData($sql)}[0]->{c};

	$no = $ar->{taxon_no};
	$sql = "UPDATE reidentifications SET modified=modified, taxon_no=$no WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND (species_reso NOT LIKE '%informal%' OR species_reso IS NULL) AND genus_name='$genus' AND species_name='$species'";
    print "$sql\n";
    if ($doUpdate) {
	    $dbt->getData($sql);
    }
}
print "$rcount species reIDs\n";

$count = 0;
$rcount = 0;
# get a list of higher taxa appearing only once and go through it
$sql = "SELECT taxon_name,taxon_no FROM authorities WHERE taxon_rank not like '%species%' GROUP BY taxon_name HAVING count(*)=1";
@authrefs = @{$dbt->getData($sql)};
for $ar (@authrefs)	{
	$genus = $ar->{taxon_name};

	$sql = "SELECT count(*) AS c FROM occurrences WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND genus_name='$genus'";
	$count = $count + ${$dbt->getData($sql)}[0]->{c};

	$no = $ar->{taxon_no};
	$sql = "UPDATE occurrences SET modified=modified, taxon_no=$no WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND genus_name='$genus'";
    print "$sql\n";
    if ($doUpdate) {
	    $dbt->getData($sql);
    }
}
print "$count higher taxon occurrences\n";

for $ar (@authrefs)	{
	$genus = $ar->{taxon_name};

	$sql = "SELECT count(*) AS c FROM reidentifications WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND genus_name='$genus'";
	$rcount = $rcount + ${$dbt->getData($sql)}[0]->{c};

	$no = $ar->{taxon_no};
	$sql = "UPDATE reidentifications SET modified=modified, taxon_no=$no WHERE taxon_no<1 AND (genus_reso NOT LIKE '%informal%' OR genus_reso IS NULL) AND genus_name='$genus'";
    print "$sql\n";
    if ($doUpdate) {
	    $dbt->getData($sql);
    }
}
print "$rcount higher taxon reIDs\n";

