# CollectionSummary
# 
# A class that returns information from the PaleoDB database about geographic
# summary clusters.
# 
# Author: Michael McClennen

package CollectionSummary;

use strict;

use parent 'CollectionData';

use Web::DataService qw( :validators );
use CollectionTables qw( $COLL_MATRIX $COLL_BINS );



# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds, $config, $dbh) = @_;
    
    # Start by defining an output map for this class.
    
    $ds->define_output_map('1.1:colls:summary_map' =>
        { value => 'basic', maps_to => '1.1:colls:summary', fixed => 1 },
        { value => 'ext', maps_to => '1.1:colls:ext' },
	    "Additional information about the geographic extent of each cluster.",
        { value => 'time', maps_to => '1.1:colls:time' },
	  # This block is defined in our parent class, CollectionData.pm
	    "Additional information about the temporal range of the",
	    "cluster.");
    
    # Then define the output blocks (except those which are defined elsewhere).
    
    $ds->define_block( '1.1:colls:summary' =>
      { select => ['s.bin_id', 's.n_colls', 's.n_occs', 's.lat', 's.lng'] },
      { output => 'bin_id', com_name => 'oid' }, 
	  "A positive integer that identifies the cluster",
      { output => 'bin_id_1', com_name => 'lv1' }, 
	  "A positive integer that identifies the containing level-1 cluster, if any",
      { output => 'bin_id_2', com_name => 'lv2' }, 
	  "A positive integer that identifies the containing level-2 cluster, if any",
      { output => 'bin_id_3', com_name => 'lv3' },
	  "A positive integer that identifies the containing level-3 cluster, if any",
      { output => 'bin_id_4', com_name => 'lv4' },
	  "A positive integer that identifies the containing level-4 cluster, if any",
      { output => 'record_type', com_name => 'typ', value => 'clu' },
	  "The type of this object: 'clu' for a collection cluster",
      { output => 'n_colls', com_name => 'nco' },
	  "The number of collections in cluster",
      { output => 'n_occs', com_name => 'noc' },
	  "The number of occurrences in this cluster",
      { output => 'lng', com_name => 'lng' },
	  "The longitude of the centroid of this cluster",
      { output => 'lat', com_name => 'lat' },
	  "The latitude of the centroid of this cluster");
    
    $ds->define_block( '1.1:colls:ext' =>
      { select => ['s.lng_min', 'lng_max', 's.lat_min', 's.lat_max', 's.std_dev'] },
      { output => 'lng_min', com_name => 'lg1' },
	  "The mimimum longitude for collections in this cluster",
      { output => 'lng_max', com_name => 'lg2' },
	  "The maximum longitude for collections in this cluster",
      { output => 'lat_min', com_name => 'la1' },
	  "The mimimum latitude for collections in this cluster",
      { output => 'lat_max', com_name => 'la2' },
	  "The maximum latitude for collections in this cluster",
      { output => 'std_dev', com_name => 'std' },
	  "The standard deviation of the coordinates in this cluster");
    
    # Finally, define some rulesets for interpreting the parameters passed to
    # methods implemented by this class.
    
    $ds->define_ruleset('1.1:summary_display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved cluster:",
	{ param => 'show', list => q{,},
	  valid => $ds->valid_set('1.1:colls:summary_map') },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each cluster.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:colls:summary_map'),);
    
    $ds->define_ruleset('1.1:colls:summary' => 
	"The following required parameter selects from one of the available clustering levels:",
	{ param => 'level', valid => POS_VALUE, default => 1 },
	    "Return records from the specified cluster level.  You can find out which",
	    "levels are available by means of the L<config|/data1.1/config_doc.html> URL path.",
	">You can use the following parameters to query for summary clusters by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
    	{ allow => '1.1:main_selector' },
	">You can use the following parameter if you wish to retrieve information about",
	"the summary clusters which contain a specified collection or collections.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.1:colls:selector' },
    	{ allow => '1.1:summary_display' },
    	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
}


# summary ( )
# 
# This operation queries for geographic summary clusters matching the
# specified parameters.

sub summary {
    
    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    my $tables = $self->tables_hash;
    
    # Figure out which bin level we are being asked for.  The default is 1.    

    my $bin_level = $self->{params}{level} || 1;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $self->generateMainFilters('summary', 's', $tables);
    push @filters, $self->generateCollFilters($tables);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 's' });
    
    $self->adjustCoordinates(\$fields);
    
    my $summary_joins = '';
    
    $summary_joins .= "JOIN $COLL_MATRIX as c on s.bin_id = c.bin_id_${bin_level}\n"
	if $tables->{c} || $tables->{cc} || $tables->{t} || $tables->{o} || $tables->{oc} || $tables->{tf};
    
    $summary_joins .= "JOIN collections as cc using (collection_no)\n" if $tables->{cc};
    
    $summary_joins .= $self->generateJoinList('s', $tables);
    
    if ( $self->{select_tables}{o} )
    {
	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
	$fields =~ s/s.n_occs/count(distinct o.occurrence_no) as n_occs/;
    }
    
    elsif ( $self->{select_tables}{c} )
    {
	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
	$fields =~ s/s.n_occs/sum(c.n_occs) as n_occs/;
    }
    
    push @filters, $self->{select_tables}{c} ? "c.access_level = 0" : "s.access_level = 0";
    push @filters, "s.bin_level = $bin_level";
    
    my $filter_string = join(' and ', @filters);
    
    $self->{main_sql} = "
		SELECT $calc $fields
		FROM $COLL_BINS as s $summary_joins
		WHERE $filter_string
		GROUP BY s.bin_id
		ORDER BY s.bin_id $limit";
    
    # Then prepare and execute the query..
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # Get the result count, if we were asked to do so.
    
    $self->sql_count_rows;
    
    return 1;
}


1;
