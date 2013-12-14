#
# ConfigQuery
# 
# A class that returns information from the PaleoDB database about the
# parameters necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package ConfigData;

use strict;
use base 'DataService::Base';

use CollectionTables qw($CONTINENT_DATA $COLL_BINS);
use TaxonDefs qw(%TAXON_RANK %RANK_STRING);

use Carp qw(carp croak);


our (%SELECT, %TABLES, %PROC, %OUTPUT, %GROUP);

$GROUP{all} = ['geosum', 'ranks'];

$OUTPUT{geosum} = 
   [
    { rec => 'bin_level', com => 'lvl',
      doc => "Cluster level, starting at 1" },
    { rec => 'degrees', com => 'deg', 
      doc => "The size of each cluster in degrees.  Each level of clustering is aligned so that 0 lat and 0 lng fall on cluster boundaries, and the cluster size must evenly divide 180." },
    { rec => 'count', com => 'cnt',
      doc => "The number of summary clusters at this level" },
    { rec => 'max_colls', com => 'mco',
      doc => "The maximum nmber of collections in any cluster at this level (can be used for scaling cluster indicators)" },
    { rec => 'max_occs', com => 'moc',
      doc => "The maximum number of occurrences in any cluster at this level (can be used for scaling cluster indicators)" },
   ];

$OUTPUT{ranks} = 
   [
    { rec => 'rank', com => 'rnk', doc => 'Taxonomic rank' },
    { rec => 'code', com => 'cod', doc => 'Numeric code used for this rank in compact (i.e. JSON) responses' },
   ];


# Variables to store the configuration information.

our ($BINS, $RANKS, $CONTINENTS);

# configure ( )
# 
# This routine is called by the DataService module, and is passed the
# configuration data as a hash ref.

sub configure {
    
    my ($self, $dbh, $config) = @_;
    
    # Get the list of geographical cluster data from the $COLL_BINS table.
    
    my $sql = "
	SELECT b.bin_level, count(*) as count, max(n_colls) as max_colls, max(n_occs) as max_occs, 
		(SELECT 360.0/n_colls FROM $COLL_BINS as x
		 WHERE bin_level = b.bin_level and interval_no = 999999) as degrees
	FROM $COLL_BINS as b where interval_no = 0 GROUP BY bin_level";
    
    $BINS = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    # Get the list of taxonomic ranks from the module TaxonDefs.pm.
    
    $RANKS = [];
    
    foreach my $r ($TAXON_RANK{min}..$TAXON_RANK{max})
    {
	next unless exists $RANK_STRING{$r};
	push @$RANKS, { code => $r, rank => $RANK_STRING{$r} };
    }
    
    # Get the list of continents from the database.
    
    $CONTINENTS = $dbh->selectall_arrayref("
	SELECT continent as code, name FROM $CONTINENT_DATA", { Slice => {} });
}


# get ( )
# 
# Return configuration information.

sub get {

    my ($self) = @_;
    
    $self->{main_result} = [];
    
    push @{$self->{main_result}}, @$BINS if $self->{show}{geosum};
    push @{$self->{main_result}}, @$RANKS if $self->{show}{ranks};
    
    return 1;
}


1;
