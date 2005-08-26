#!/usr/bin/perl

use lib '/Volumes/pbdb_RAID/httpdocs/cgi-bin';
use DBI;
use DBTransactionManager;
use DBConnection;
use Data::Dumper;
use Classification;
use TaxonInfo;
use Reference;
use Time::HiRes qw(gettimeofday);


# the password is stored in a file.  This path will work on both the linux box
# and the XServe since we have a symbolic link set up.
my $user = 'gbifwriter';
my $passwd = 'PBL1nk26B1f';
my $dsn = "DBI:mysql:database=gbif;host=localhost";
my $dbh_gbif = DBI->connect($dsn, $user, $passwd);

my $dbh_pbdb = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh_pbdb);

my $DEBUG = 0;
if ($ARGV[0] eq '--debug') {
    $DEBUG = 1;
}

# All the potential fields that we're going to provide to GBIF
@fields = qw(DateLastModified InstitutionCode CollectionCode CatalogNumber ScientificName BasisOfRecord Kingdom Phylum Class Order Family Genus Species Subspecies ScientificNameAuthor ContinentOcean Country StateProvince County Locality Longitude Latitude MaximumElevation MinimumElevation IndividualCount Notes);

# Populate country --> continent look up table
my %continentLUT = ();
my $regions_file = '/Volumes/pbdb_RAID/httpdocs/cgi-bin/data/PBDB.regions';
if ( ! open REGIONS,"<$regions_file" ) {
    print STDERR "Couldn't open $regions_file: $!";
}
while (<REGIONS>)   {
    chomp;
    my ($continent,$country_list) = split /:/, $_, 2;
    @countries = split /\t/,$country_list;
    for $country (@countries)   {
        $continentLUT{$country} = $continent;
    }
}

$sql = "DROP TABLE IF EXISTS gbif_new";
$dbh_gbif->do($sql);

$sql = "SHOW CREATE TABLE gbif";
$sth = $dbh_gbif->prepare($sql);
$result = $sth->execute();
if (!$result) {
    print $sth->errstr;
}
$row = $sth->fetchrow_arrayref();
$definition = $row->[1];
$definition =~ s/CREATE TABLE `gbif`/CREATE TABLE `gbif_new`/;
print $definition if ($DEBUG);
$sth = $dbh_gbif->prepare($definition);
$result = $sth->execute();
if (!$result) {
    print $sth->errstr;
}

if ($DEBUG) {
    $do_limit = " limit 100";
} else {
    $do_limit ="";
}

my ($th0, $th1);
$th0 = gettimeofday;

# Now fetch all the occurrence recods from the database
$sql = "(SELECT c.altitude_value,c.altitude_unit,c.collection_no,c.museum,DATE_FORMAT(o.modified,'%Y-%m-%d %H:%i:%s') occ_modified,o.occurrence_no, o.genus_reso,o.genus_name,o.species_reso,o.species_name,o.subgenus_reso,o.subgenus_name, c.country, c.state, c.county, c.collection_name, c.latdeg, c.latmin, c.latsec, c.latdir, c.latdec, c.lngdeg, c.lngmin, c.lngsec, c.lngdir, c.lngdec, o.comments, o.taxon_no,o.abund_value,o.abund_unit FROM collections c, occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE o.collection_no=c.collection_no AND re.reid_no IS NULL AND o.genus_name not like 'ERROR' AND (c.access_level LIKE 'the public' OR release_date < NOW()) $do_limit)".
" UNION ".
"(SELECT c.altitude_value,c.altitude_unit,c.collection_no,c.museum,DATE_FORMAT(re.modified,'%Y-%m-%d %H:%i:%s') occ_modified,o.occurrence_no, re.genus_reso,re.genus_name,re.species_reso,re.species_name,re.subgenus_reso,re.subgenus_name, c.country, c.state, c.county, c.collection_name, c.latdeg, c.latmin, c.latsec, c.latdir, c.latdec, c.lngdeg, c.lngmin, c.lngsec, c.lngdir, c.lngdec, re.comments, re.taxon_no,o.abund_value,o.abund_unit FROM collections c, occurrences o,reidentifications re WHERE o.collection_no=c.collection_no AND o.occurrence_no=re.occurrence_no AND re.most_recent='YES' AND re.genus_name not like 'ERROR' AND (c.access_level LIKE 'the public' OR release_date < NOW()) $do_limit) ORDER BY collection_no,occurrence_no";
print $sql if ($DEBUG);
$sth = $dbh_pbdb->prepare($sql);
$sth->execute();

#$th1 = gettimeofday;
#print ("Query exec time: ".sprintf ("%5.3f",($th1 - $th0))); 
#$th0 = $th1;

my %class_cache = (); #speed this lookup up
while($row = $sth->fetchrow_hashref()) {
    my %gbif_row = ();
    
    $gbif_row{'DateLastModified'} = $row->{'occ_modified'};
    $gbif_row{'InstitutionCode'} = ($row->{'museum'} ne '') ? $row->{'museum'} : "";
    $gbif_row{'CollectionCode'} = 'PBDB collection number '.$row->{'collection_no'};
    $gbif_row{'CatalogNumber'} = $row->{'occurrence_no'};

    $taxon_no = $row->{'taxon_no'};
    $taxon_name = $row->{'genus_name'};
    $taxon_name .= " ".$row->{'subgenus_name'} if ($row->{'subgenus_name'});
    $taxon_name .= " ".$row->{'species_name'};
    $taxon_name .= " ".$row->{'subspecies_name'} if ($row->{'subspecies_name'});
    $gbif_row{'ScientificName'} = $taxon_name;
    $gbif_row{'BasisOfRecord'} = ''; # Should be publication

    if ($taxon_no) {
        if (! exists $class_cache{$taxon_no}) {
            print "Making cache for $taxon_no\n" if ($DEBUG);
            my $class_hash = Classification::get_classification_hash($dbt,'kingdom,phylum,class,order,family',[$taxon_no]);
            my @higher_taxon = split(/,/,$class_hash->{$taxon_no});
            $class_cache{$taxon_no} = \@higher_taxon;
            ($kingdom,$phylum,$class,$order,$family) = @{$class_cache{$taxon_no}}
        } elsif ($DEBUG) {
            print "Hit cache for $taxon_no\n";
        }
        ($kingdom,$phylum,$class,$order,$family) = @{$class_cache{$taxon_no}};

        if ($row->{'species_name'} =~ /indet/) {
            my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$taxon_no);
            if ($taxon->{'taxon_rank'} eq 'kingdom') {
                $kingdom = $taxon->{'taxon_name'};
            } elsif ($taxon->{'taxon_rank'} eq 'phylum') {
                $phylum = $taxon->{'taxon_name'};
            } elsif ($taxon->{'taxon_rank'} eq 'class') {
                $class = $taxon->{'taxon_name'};
            } elsif ($taxon->{'taxon_rank'} eq 'order') {
                $order = $taxon->{'taxon_name'};
            } elsif ($taxon->{'taxon_rank'} eq 'family') {
                $family = $taxon->{'taxon_name'};
            }
        }
    } else {
        ($kingdom,$phylum,$class,$order,$family) = ('','','','','');
    }
    
    $gbif_row{'Kingdom'} = $kingdom;
    $gbif_row{'Phylum'} = $phylum;
    $gbif_row{'Class'} = $class;
    $gbif_row{'Order'} = $order;
    $gbif_row{'Family'} = $family;
    $gbif_row{'Genus'} = $row->{'genus_name'};
    $gbif_row{'Species'} = $row->{'species_name'};
    $gbif_row{'Subspecies'} = $row->{'subspecies_name'};

    $short_ref = "";
    if ($taxon_no) {
        $taxon_row = TaxonInfo::getTaxon($dbt,'taxon_no'=>$taxon_no,'get_reference'=>1); 
        if ($taxon_row) {
            $short_ref = Reference::formatShortRef($taxon_row);
        }
    }
    
    $gbif_row{'ScientificNameAuthor'} = $short_ref;
#    $gbif_row{'IdentifiedBy'}
#    $gbif_row{'YearIdentified'}
#    $gbif_row{'MonthIdentified'}
#    $gbif_row{'DayIdentified'}
#    $gbif_row{'TypeStatus'}
#    $gbif_row{'CollectorNumber'}
#    $gbif_row{'FieldNumber'}
#    $gbif_row{'Collector'}

#    $gbif_row{'MonthCollected'}
#    $gbif_row{'DayCollected'}
#    $gbif_row{'JulianDay'}
#    $gbif_row{'TimeOfDay'}
    $gbif_row{'ContinentOcean'} = $continentLUT{$row->{'country'}};
    $gbif_row{'Country'} = $row->{'country'};
    $gbif_row{'StateProvince'} = $row->{'state'};
    $gbif_row{'County'} = $row->{'county'};
    $gbif_row{'Locality'} = $row->{'collection_name'};

    my ($latitude,$longitude) = ('','');
    if ($row->{'latmin'} =~ /\d+/) {
        if ($row->{'latsec'} =~ /\d+/) {
            $latitude = sprintf("%.4f",$row->{'latdeg'} + $row->{'latmin'}/60 + $row->{'latsec'}/3600);
        } else {
            $latitude = sprintf("%.2f",$row->{'latdeg'} + $row->{'latmin'}/60);
        }
    } elsif ($row->{'latdec'} =~ /\d+/) {
        $latitude = sprintf("%s",$row->{'latdeg'}.".".int($row->{'latdec'}));
    } else {
        $latitude = $row->{'latdeg'};
    }
    if ($latitude > 90) {
        $latitude = 90;
    }
    $latitude *= -1 if ($row->{'latdir'} =~ /South/);

    if ($row->{'lngmin'} =~ /\d+/) {
        if ($row->{'lngsec'} =~ /\d+/) {
            $longitude = sprintf("%.4f",$row->{'lngdeg'} + $row->{'lngmin'}/60 + $row->{'lngsec'}/3600);
        } else {
            $longitude = sprintf("%.2f",$row->{'lngdeg'} + $row->{'lngmin'}/60); 
        }
    } elsif ($row->{'lngdec'} =~ /\d+/) {
        $longitude = sprintf("%s",$row->{'lngdeg'}.".".int($row->{'lngdec'}));
    } else {
        $longitude = $row->{'lngdeg'};
    }
    if ($latitude > 180) {
        $latitude = 180;
    }
    $longitude *= -1 if ($row->{'lngdir'} =~ /West/);


    $gbif_row{'Longitude'} = $longitude;
    $gbif_row{'Latitude'} = $latitude;
#    $gbif_row{'CoordinatePrecision'}
#    $gbif_row{'BoundingBox'}
   
    my $altitude = "";
    if ($row->{'altitude_value'}) {
        if ($row->{'altitude_unit'} =~ /feet/) {
            # Convert feet to meters;
            $altitude = $row->{'altitude_value'} * .3048;
        } else {
            $altitude = $row->{'altitude_value'};
        }
    }
    $gbif_row{'MinimumElevation'} = $altitude;
    $gbif_row{'MaximumElevation'} = $altitude;
#    $gbif_row{'MinimumDepth'}
#    $gbif_row{'MaximumDepth'}
#    $gbif_row{'Sex'}
#    $gbif_row{'PreparationType'}

    $abund_count = "";
    if ($row->{'abund_unit'} =~ /specimens|individuals/ && $row->{'abund_value'} =~ /^\d+$/) {
        $abund_count = $row->{'abund_value'};
    }

    $gbif_row{'IndividualCount'} = $abund_count;
#    $gbif_row{'PreviousCatalogNumber'}
#    $gbif_row{'RelationshipType'}
#    $gbif_row{'RelatedCatalogItem'}
    $gbif_row{'Notes'} = "$row->{comments}";

    if ($DEBUG) {
        for($i=0;$i<scalar(@fields);$i++) {
            printf "%20s: %s\n",$fields[$i],$gbif_row{$fields[$i]};
        }
    }

    my @keys;
    my @values;
    for($i=0;$i< scalar(@fields);$i++) {
        if ($gbif_row{$fields[$i]}) {
            push @keys,'`'.$fields[$i].'`';
            push @values,$dbh_gbif->quote($gbif_row{$fields[$i]});
        } else {
            push @keys,'`'.$fields[$i].'`';
            push @values,"''";
        }
    }

    $sql = "INSERT INTO gbif_new (".join(',',@keys).") VALUES (".join(',',@values).")\n\n";
    $sth_i = $dbh_gbif->prepare($sql);
    $result = $sth_i->execute();
    if (!$result) {
        print $sth->errstr;
    }
    if ($DEBUG) {
        print $sql;
    }
}

#$th1 = gettimeofday;
#print ("Loop exec time: ".sprintf ("%5.3f",($th1 - $th0))); 
#$th0 = $th1;

$sql = "DROP TABLE gbif_old";
$sth = $dbh_gbif->prepare($sql);
$result = $sth->execute();
if (!$result) {
    print $sth->errstr;
}

$sql = "RENAME TABLE gbif TO gbif_old, gbif_new TO gbif";
$sth = $dbh_gbif->prepare($sql);
$result = $sth->execute();
if (!$result) {
    print $sth->errstr;
}

#$th1 = gettimeofday;
#print ("Finish exec time: ".sprintf ("%5.3f",($th1 - $th0))); 
#$th0 = $th1;



