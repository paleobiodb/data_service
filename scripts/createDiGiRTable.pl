#!/usr/bin/perl

use lib '/Volumes/pbdb_RAID/httpdocs/cgi-bin';
use strict;

use DBI;
use DBTransactionManager;
use DBConnection;
use Data::Dumper;
use Classification;
use TaxonInfo;
use Reference;


# the password is stored in a file.  This path will work on both the linux box
# and the XServe since we have a symbolic link set up.
my $user = 'gbifwriter';
my $passwd = 'PBL1nk26B1f';
my $dsn = "DBI:mysql:database=gbif;host=localhost";
my $dbh_gbif = DBI->connect($dsn, $user, $passwd);
my $dbt_gbif = DBTransactionManager->new($dbh_gbif);

my $dbh_pbdb = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh_pbdb);

my $DEBUG = 0;
if ($ARGV[0] eq '--debug') {
    $DEBUG = 1;
}


# Don't log thse inserts - this is a session variable
# gbifwriter must have SUPER privilege
my $return = $dbh_gbif->do("SET SQL_LOG_BIN=0");
if (!$return) {
    print "WARNING: could not turn off binary logging";
}

# All the potential fields that we're going to provide to GBIF
my @fields = qw(DateLastModified InstitutionCode CollectionCode CatalogNumber ScientificName BasisOfRecord Kingdom Phylum Class Order Family Genus Species Subspecies ScientificNameAuthor ContinentOcean Country StateProvince County Locality Longitude Latitude MaximumElevation MinimumElevation IndividualCount Notes);

# Populate country --> continent look up table
my %continentLUT = ();
my $regions_file = '/Volumes/pbdb_RAID/httpdocs/cgi-bin/data/PBDB.regions';
if ( ! open REGIONS,"<$regions_file" ) {
    print STDERR "Couldn't open $regions_file: $!";
}
while (<REGIONS>)   {
    chomp;
    my ($continent,$country_list) = split /:/, $_, 2;
    my @countries = split /\t/,$country_list;
    foreach my $country (@countries)   {
        $continentLUT{$country} = $continent;
    }
}

my $do_limit = "";
if ($DEBUG) {
    $do_limit = " limit 100";
} 

my %in_gbif;
my %in_pbdb;

my $sql = "SELECT CatalogNumber FROM gbif";
my $sth = $dbh_gbif->prepare($sql);
$sth->execute();
while(my $row = $sth->fetchrow_arrayref()) {
    $in_gbif{$row->[0]} = 1;
}
$sth->finish();

# Now fetch all the occurrence recods from the database
$sql = "(SELECT c.altitude_value,c.altitude_unit,c.collection_no,c.museum,DATE_FORMAT(o.modified,'%Y-%m-%d %H:%i:%s') occ_modified,o.occurrence_no, o.genus_reso,o.genus_name,o.species_reso,o.species_name,o.subgenus_reso,o.subgenus_name, c.country, c.state, c.county, c.collection_name, c.latdeg, c.latmin, c.latsec, c.latdir, c.latdec, c.lngdeg, c.lngmin, c.lngsec, c.lngdir, c.lngdec, o.comments, o.taxon_no,o.abund_value,o.abund_unit FROM collections c, occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE o.collection_no=c.collection_no AND re.reid_no IS NULL AND o.genus_name not like 'ERROR' AND (c.access_level LIKE 'the public' OR release_date < NOW()) $do_limit)".
" UNION ".
"(SELECT c.altitude_value,c.altitude_unit,c.collection_no,c.museum,DATE_FORMAT(re.modified,'%Y-%m-%d %H:%i:%s') occ_modified,o.occurrence_no, re.genus_reso,re.genus_name,re.species_reso,re.species_name,re.subgenus_reso,re.subgenus_name, c.country, c.state, c.county, c.collection_name, c.latdeg, c.latmin, c.latsec, c.latdir, c.latdec, c.lngdeg, c.lngmin, c.lngsec, c.lngdir, c.lngdec, re.comments, re.taxon_no,o.abund_value,o.abund_unit FROM collections c, occurrences o,reidentifications re WHERE o.collection_no=c.collection_no AND o.occurrence_no=re.occurrence_no AND re.most_recent='YES' AND re.genus_name not like 'ERROR' AND (c.access_level LIKE 'the public' OR release_date < NOW()) $do_limit) ORDER BY collection_no,occurrence_no";
print $sql if ($DEBUG);
$sth = $dbh_pbdb->prepare($sql);
$sth->execute();

my %class_cache = (); #speed this lookup up
my %ref_lookup = ();
while(my $row = $sth->fetchrow_hashref()) {
    $in_pbdb{$row->{'occurrence_no'}} = 1;
    my %gbif_row = ();
    
    $gbif_row{'DateLastModified'} = $row->{'occ_modified'};
#    $gbif_row{'InstitutionCode'} = ($row->{'museum'} ne '') ? $row->{'museum'} : "";
    $gbif_row{'InstitutionCode'} = "PBDB";
    $gbif_row{'CollectionCode'} = $row->{'collection_no'};
    $gbif_row{'CatalogNumber'} = $row->{'occurrence_no'};

    my $taxon_no = $row->{'taxon_no'};
    my $taxon_name = $row->{'genus_name'};
    $taxon_name .= " (".$row->{'subgenus_name'}.")" if ($row->{'subgenus_name'});
    $taxon_name .= " ".$row->{'species_name'};
    $gbif_row{'ScientificName'} = $taxon_name;
    $gbif_row{'BasisOfRecord'} = 'fossil'; # Should be publication

    my %lookup_by_rank = ();
    if ($taxon_no) {
        unless (exists $class_cache{$taxon_no}) {
            my $hash = TaxaCache::getParents($dbt,[$taxon_no],'array_full');
            my $ss = TaxaCache::getSeniorSynonym($dbt,$taxon_no);
            $class_cache{$taxon_no} = [$ss,@{$hash->{$taxon_no}}];
        }
        foreach my $p (@{$class_cache{$taxon_no}}) {
            if (ref $p eq 'HASH') {
                $lookup_by_rank{$p->{'taxon_rank'}} = $p;
            }
        }
    } 
    $gbif_row{'Kingdom'} = $lookup_by_rank{'kingdom'}{'taxon_name'} || undef;
    $gbif_row{'Phylum'} = $lookup_by_rank{'phylum'}{'taxon_name'} || undef;
    $gbif_row{'Class'} = $lookup_by_rank{'class'}{'taxon_name'} || undef;
    $gbif_row{'Order'} = $lookup_by_rank{'order'}{'taxon_name'} || undef;
    $gbif_row{'Family'} = $lookup_by_rank{'family'}{'taxon_name'} || undef;
    my $genus = $lookup_by_rank{'genus'}{'taxon_name'} || $row->{'genus_name'};
    my $species = $lookup_by_rank{'species'}{'taxon_name'};
    my ($g,$sg,$sp) = Taxon::splitTaxon($species);
    if ($sp) {
        $species = $sp;
    } else {
        $species = $row->{'species_name'};
    }
    $gbif_row{'Genus'} = $genus; 
    $gbif_row{'Species'} = $species; 
    $gbif_row{'Subspecies'} = '';

    my $short_ref = "";
    if (exists $ref_lookup{$taxon_no}) {
        $short_ref = $ref_lookup{$taxon_no};
    } else {
        my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no},['*']);
        if ($taxon) {
            if ($taxon->{'taxon_rank'} =~ /species/ || $species =~ /^indet\.|^sp\.|^indet$|^sp$/) {
                my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
                my $is_recomb = ($orig_no == $taxon->{'taxon_no'}) ? 0 : 1;
                $short_ref = Reference::formatShortRef($taxon,'is_recombination'=>$is_recomb); 
            }
        }
        $ref_lookup{$taxon_no} = $short_ref;
    }
    
    $gbif_row{'ScientificNameAuthor'} = $short_ref || undef;
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
    $gbif_row{'ContinentOcean'} = $continentLUT{$row->{'country'}} || undef;
    $gbif_row{'Country'} = $row->{'country'} || undef;
    $gbif_row{'StateProvince'} = $row->{'state'} || undef;
    $gbif_row{'County'} = $row->{'county'} || undef;
    $gbif_row{'Locality'} = $row->{'collection_name'} || undef;

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
   
    my $altitude;
    if ($row->{'altitude_value'}) {
        if ($row->{'altitude_unit'} =~ /feet/) {
            # Convert feet to meters;
            $altitude = $row->{'altitude_value'} * .3048;
        } else {
            $altitude = $row->{'altitude_value'};
        }
    }
    $altitude = undef unless $altitude =~ /\d/;
    $gbif_row{'MinimumElevation'} = $altitude;
    $gbif_row{'MaximumElevation'} = $altitude;
#    $gbif_row{'MinimumDepth'}
#    $gbif_row{'MaximumDepth'}
#    $gbif_row{'Sex'}
#    $gbif_row{'PreparationType'}

    my $abund_count;
    if ($row->{'abund_unit'} =~ /specimens|individuals/ && $row->{'abund_value'} =~ /^\d+$/) {
        $abund_count = $row->{'abund_value'};
    }

    $gbif_row{'IndividualCount'} = $abund_count;
#    $gbif_row{'PreviousCatalogNumber'}
#    $gbif_row{'RelationshipType'}
#    $gbif_row{'RelatedCatalogItem'}
    $gbif_row{'Notes'} = "$row->{comments}";

    if ($DEBUG) {
        for(my $i=0;$i<scalar(@fields);$i++) {
            printf "%20s: %s\n",$fields[$i],$gbif_row{$fields[$i]};
        }
    }

    if ($in_gbif{$row->{'occurrence_no'}}) {
        updateRecord($row->{'occurrence_no'},\%gbif_row);
    } else {
        insertRecord(\%gbif_row);
    }
}

foreach my $occurrence_no (keys %in_gbif) {
    if ($occurrence_no) {
        unless ($in_pbdb{$occurrence_no}) {
            my $sql = "DELETE FROM gbif WHERE CatalogNumber=".$occurrence_no;
            my $sth_i = $dbh_gbif->prepare($sql);
            my $result = $sth_i->execute();
            if (!$result) {
                print $sth->errstr;
            }
            if ($DEBUG) {
                print $sql,"\n";
            }
        }
    }
}

sub updateRecord {
    my $occurrence_no = $_[0];
    my %gbif_row = %{$_[1]};
    my @keys;
    my @values;
    my @updates;
    for(my $i=0;$i< scalar(@fields);$i++) {
        push @updates, '`'.$fields[$i].'`'."=".$dbh_gbif->quote($gbif_row{$fields[$i]});
    }

    my $sql = "UPDATE gbif SET ".join(",",@updates)." WHERE CatalogNumber=$occurrence_no";
    my $sth_i = $dbh_gbif->prepare($sql);
    my $result = $sth_i->execute();
    if (!$result) {
        print $sth->errstr;
    }
    if ($DEBUG) {
        print $sql,"\n";
    }
}
sub insertRecord {
    my %gbif_row = %{$_[0]};
    my @keys;
    my @values;
    for(my $i=0;$i< scalar(@fields);$i++) {
        push @keys,'`'.$fields[$i].'`';
        push @values,$dbh_gbif->quote($gbif_row{$fields[$i]});
    }

    my $sql = "INSERT INTO gbif (".join(',',@keys).") VALUES (".join(',',@values).")\n\n";
    my $sth_i = $dbh_gbif->prepare($sql);
    my $result = $sth_i->execute();
    if (!$result) {
        print $sth->errstr;
    }
    if ($DEBUG) {
        print $sql,"\n";
    }
}

