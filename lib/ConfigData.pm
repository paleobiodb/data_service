#
# ConfigQuery
# 
# A class that returns information from the PaleoDB database about the
# parameters necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package ConfigData;

use strict;
use base 'Web::DataService::Request';

use CollectionTables qw($CONTINENT_DATA $COLL_BINS);
use TaxonDefs qw(%TAXON_RANK %RANK_STRING);

use Carp qw(carp croak);


# Variables to store the configuration information.

our ($BINS, $RANKS, $CONTINENTS);


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds, $config, $dbh) = @_;
    
    # We start by defining the output blocks which will be used for this class.
    
    $ds->define_block('1.1:config:geosum' =>
	{ output => 'bin_level', com_name => 'lvl' },
	    "Cluster level, starting at 1",
	{ output => 'degrees', com_name => 'deg' },
	    "The size of each cluster in degrees.  Each level of clustering is aligned so that",
	    "0 lat and 0 lng fall on cluster boundaries, and the cluster size must evenly divide 180.",
	{ output => 'count', com_name => 'cnt' },
	    "The number of summary clusters at this level",
	{ output => 'max_colls', com_name => 'mco' },
	    "The maximum nmber of collections in any cluster at this level (can be used for scaling cluster indicators)",
	{ output => 'max_occs', com_name => 'moc' },
	    "The maximum number of occurrences in any cluster at this level (can be used for scaling cluster indicators)");
    
    $ds->define_block('1.1:config:ranks' =>
	{ output => 'rank', com_name => 'rnk' },
	    "Taxonomic rank",
	{ output => 'code', com_name => 'cod' },
	    "Numeric code used for this rank in responses using the 'com' vocabulary,",
	    "which is the default for json format");
    
    $ds->define_block('1.1:config:all',
	{ include => '1.1:config:geosum' },
	{ include => '1.1:config:ranks' });
    
    # Then define a value map to allow these blocks to be selected by the client.
    
    $ds->define_valueset('1.1:config:show' =>
	{ value => 'geosum', result => '1.1:config:geosum' },
	    "Return information about the levels of geographic clustering defined in this database.",
	{ value => 'ranks', result => '1.1:config:ranks' },
	    "Return information about the taxonomic ranks defined in this database.",
	{ value => 'all', result => '1.1:config:all' },
	    "Return both of the above sets of information.",
	    "This is generally useful only with C<json> format.");
    
    # Then define a ruleset to interpret the parmeters accepted by operations
    # from this class.
    
    $ds->define_ruleset('1.1/config' =>
	{ param => 'show', valid => $ds->validate_valueset('1.1:config:show'),
	  list => q{,}, default => 'all' },
	    "The value of this parameter should be a comma-separated list of block names drawn",
	    "From following list.  It defaults to C<all>.", 
	    $ds->document_valueset('1.1:config:show'),
	{ allow => '1.1:common_params' },
	    "^You can use any of the L<common parameters|/data1.1/common_doc.html> with this request.");
    
    # Now gather the information that will be reported by this module.  It
    # won't change, so for the sake of efficiency we get it once at startup.
    
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


# Data service operations
# -----------------------

# get ( )
# 
# Return configuration information.

sub get {

    my ($self) = @_;
    
    my $show = $self->output_set;
    
    $self->{main_result} = [];
    
    push @{$self->{main_result}}, @$BINS if $show->{geosum};
    push @{$self->{main_result}}, @$RANKS if $show->{ranks};
    
    if ( my $offset = $self->result_offset(1) )
    {
    	splice(@{$self->{main_result}}, 0, $offset);
    }
    
    return 1;
}


1;
