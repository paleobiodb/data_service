#
# TaxonData
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataService::Base.
# 
# Author: Michael McClennen

use strict;

package TaxonData;

use base 'DataService::Base';
use Carp qw(carp croak);

use PBDBData qw(generateReference generateAttribution);
use TaxonDefs qw(%TAXON_TABLE %TAXON_RANK %RANK_STRING);
use Taxonomy;


our (%OUTPUT, %PROC);

$OUTPUT{basic} = 
   [
    { rec => 'taxon_no', dwc => 'taxonID', com => 'oid',
	doc => "A positive integer that uniquely identifies this taxonomic name"},
    { rec => 'orig_no', com => 'gid',
        doc => "A positive integer that uniquely identifies the taxonomic concept"},
    { rec => 'record_type', com => 'typ', com_value => 'txn', dwc_value => 'Taxon', value => 'taxon',
        doc => "The type of this object: {value} for a taxonomic name" },
    { rec => 'rank', dwc => 'taxonRank', com => 'rnk', pbdb_code => \%RANK_STRING,
	doc => "The taxonomic rank of this name" },
    { rec => 'taxon_name', dwc => 'scientificName', com => 'nam',
	doc => "The scientific name of this taxon" },
    { rec => 'common_name', dwc => 'vernacularName', com => 'nm2',
        doc => "The common (vernacular) name of this taxon, if any" },
    { rec => 'attribution', dwc => 'scientificNameAuthorship', com => 'att', show => 'attr',
	doc => "The attribution (author and year) of this taxonomic name" },
    { rec => 'pubyr', dwc => 'namePublishedInYear', com => 'pby', show => 'attr', 
        doc => "The year in which this name was published" },
    { rec => 'status', com => 'sta',
        doc => "The taxonomic status of this name" },
    { rec => 'parent_no', dwc => 'parentNameUsageID', com => 'par', 
	doc => "The identifier of the parent taxonomic concept, if any" },
    { rec => 'synonym_no', dwc => 'acceptedNameUsageID', pbdb => 'senior_no', com => 'snr', dedup => 'orig_no',
        doc => "The identifier of the senior synonym of this taxonomic concept, if any" },
    { rec => 'pubref', com => 'ref', dwc => 'namePublishedIn', show => 'ref', json_list => 1, txt_list => "|||",
	doc => "The reference from which this name was entered into the database (as formatted text)" },
    { rec => 'reference_no', com => 'rid', json_list => 1,
	doc => "A list of identifiers indicating the source document(s) from which this name was entered." },
    { rec => 'is_extant', com => 'ext', dwc => 'isExtant',
        doc => "True if this taxon is extant on earth today, false if not, not present if unrecorded" },
    { rec => 'size', com => 'siz', show => 'size',
        doc => "The total number of taxa in the database that are contained within this taxon, including itself" },
    { rec => 'extant_size', com => 'exs', show => 'size',
        doc => "The total number of extant taxa in the database that are contained within this taxon, including itself" },
    { rec => 'firstapp_ea', com => 'fea', dwc => 'firstAppearanceEarlyAge', show => 'app',
        doc => "The early age bound for the first appearance of this taxon in this database" },
    { rec => 'firstapp_la', com => 'fla', dwc => 'firstAppearanceLateAge', show => 'app', 
        doc => "The late age bound for the first appearance of this taxon in this database" },
    { rec => 'lastapp_ea', com => 'lea', dwc => 'lastAppearanceEarlyAge', show => 'app',
        doc => "The early age bound for the last appearance of this taxon in this database" },
    { rec => 'lastapp_la', com => 'lla', dwc => 'lastAppearanceLateAge', show => 'app', 
        doc => "The late age bound for the last appearance of this taxon in this database" },
   ];

$PROC{attr} = 
   [
    { rec => 'a_al1', set => 'attribution', use_main => 1, code => \&generateAttribution },
    { rec => 'a_pubyr', set => 'pubyr' },
   ];

$PROC{ref} =
   [
    { rec => 'r_al1', set => 'pubref', use_main => 1, code => \&generateReference },
   ];

my $child_rule = [ { rec => 'taxon_no', com => 'oid', dwc => 'taxonID' },
		  { rec => 'orig_no', com => 'gid' },
		  { rec => 'record_type', com => 'typ', com_value => 'txn' },
		  { rec => 'taxon_rank', com => 'rnk', dwc => 'taxonRank' },
		  { rec => 'taxon_name', com => 'nam', dwc => 'scientificName' },
		  { rec => 'synonym_no', com => 'snr', pbdb => 'senior_no', 
			dwc => 'acceptedNameUsageID', dedup => 'orig_no' },
		  { rec => 'size', com => 'siz' },
		  { rec => 'extant_size', com => 'exs' },
		  { rec => 'firstapp_ea', com => 'fea' },
		];

$OUTPUT{nav} =
   [
    { rec => 'parent_name', com => 'prl', dwc => 'parentNameUsage',
        doc => "The name of the parent taxonomic concept, if any" },
    { rec => 'parent_rank', com => 'prr', doc => "The rank of the parent taxonomic concept, if any" },
    { rec => 'parent_txn', com => 'prt', rule => $child_rule },
    { rec => 'kingdom_no', com => 'kgn', doc => "The identifier of the kingdom in which this taxon occurs" },
    { rec => 'kingdom', com => 'kgl', doc => "The name of the kingdom in which this taxon occurs" },
    { rec => 'kingdom_txn', com => 'kgt', rule => $child_rule },
    { rec => 'phylum_no', com => 'phn', doc => "The identifier of the phylum in which this taxon occurs" },
    { rec => 'phylum', com => 'phl', doc => "The name of the phylum in which this taxon occurs" },
    { rec => 'phylum_txn', com => 'pht', rule => $child_rule },
    { rec => 'phylum_count', com => 'phc', doc => "The number of phyla within this taxon" },
    { rec => 'class_no', com => 'cln', doc => "The identifier of the class in which this taxon occurs" },
    { rec => 'class', com => 'cll', doc => "The name of the class in which this taxon occurs" },
    { rec => 'class_txn', com => 'clt', rule => $child_rule },
    { rec => 'class_count', com => 'clc', doc => "The number of classes within this taxon" },
    { rec => 'order_no', com => 'odn', doc => "The identifier of the order in which this taxon occurs" },
    { rec => 'order', com => 'odl', doc => "The name of the order in which this taxon occurs" },
    { rec => 'order_txn', com => 'odt', rule => $child_rule },
    { rec => 'order_count', com => 'odc', doc => "The number of orders within this taxon" },
    { rec => 'family_no', com => 'fmn', doc => "The identifier of the family in which this taxon occurs" },
    { rec => 'family', com => 'fml', doc => "The name of the family in which this taxon occurs" },
    { rec => 'family_txn', com => 'fmt', rule => $child_rule },
    { rec => 'family_count', com => 'fmc', doc => "The number of families within this taxon" },
    { rec => 'genus_count', com => 'gnc', doc => "The number of genera within this taxon" },
    
    { rec => 'children', com => 'chl', use_each => 1,
        doc => "The immediate children of this taxonomic concept, if any",
        rule => $child_rule },
    { rec => 'phylum_list', com => 'phs', use_each => 1,
        doc => "A list of the phyla within this taxonomic concept",
        rule => $child_rule },
    { rec => 'class_list', com => 'cls', use_each => 1,
        doc => "A list of the classes within this taxonomic concept",
        rule => $child_rule },
    { rec => 'order_list', com => 'ods', use_each => 1,
        doc => "A list of the orders within this taxonomic concept",
        rule => $child_rule },
    { rec => 'family_list', com => 'fms', use_each => 1,
        doc => "A list of the families within this taxonomic concept",
        rule => $child_rule },
    { rec => 'genus_list', com => 'gns', use_each => 1,
        doc => "A list of the genera within this taxonomic concept",
        rule => $child_rule },
    { rec => 'subgenus_list', com => 'sgs', use_each => 1,
        doc => "A list of the subgenera within this taxonomic concept",
        rule => $child_rule },
    { rec => 'species_list', com => 'sps', use_each => 1,
        doc => "A list of the species within this taxonomic concept",
        rule => $child_rule },
     { rec => 'subspecies_list', com => 'sss', use_each => 1,
        doc => "A list of the subspecies within this taxonomic concept",
        rule => $child_rule },
  ];

$OUTPUT{img} = 
  [
   { rec => 'image_no', com => 'img', 
     doc => "If this value is non-zero, you can use it to construct image URLs using L</data1.1/taxa/thumb_doc|/data1.1/taxa/thumb> and L</data1.1/taxa/icon_doc|/data1.1/taxa/icon>." },
  ];


# get ( )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class DataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub get {

    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    my $valid = $self->{valid};
    my $taxon_no;
    
    # Then figure out which taxon we are looking for.  If we have a taxon_no,
    # we can use that.
    
    my $not_found_msg = '';
    
    if ( $valid->value('id') )
    {    
	$taxon_no = $valid->value('id');
	$not_found_msg = "Taxon number $taxon_no was not found in the database";
    }
    
    # Otherwise, we must have a taxon name.  So look for that.
    
    elsif ( defined $valid->value('name') )
    {
	$not_found_msg = "Taxon '$valid->value('name')' was not found in the database";
	my $name_select = { order => 'size.desc', spelling => 'exact', return => 'id' };
	
	if ( defined $valid->value('rank') )
	{
	    $name_select->{rank} = $valid->value('rank');
	    $not_found_msg .= " at rank '$self->{base_taxon_rank}'";
	}
	
	($taxon_no) = $taxonomy->getTaxaByName($valid->value('name'), $name_select);
    }
    
    # If we haven't found a record, the result set will be empty.
    
    unless ( defined $taxon_no and $taxon_no > 0 )
    {
	return;
    }
    
    # Now add the fields necessary to show the requested info.
    
    my $options = {};
    
    my @fields;
    
    push @fields, 'ref' if $self->{show}{ref};
    push @fields, 'attr' if $self->{show}{attr};
    push @fields, 'size' if $self->{show}{size};
    push @fields, 'app' if $self->{show}{app};
    push @fields, 'img' if $self->{show}{img};
    
    push @fields, 'link' if $self->{show}{nav};
    push @fields, 'parent' if $self->{show}{nav};
    push @fields, 'phylo' if $self->{show}{nav};
    push @fields, 'counts' if $self->{show}{nav};
    
    $options->{fields} = \@fields;
    
    # If we were asked for the senior synonym, choose it.
    
    my $rel = $valid->value('senior') ? 'senior' : 'self';
    
    # Next, fetch basic info about the taxon.
    
    ($self->{main_record}) = $taxonomy->getRelatedTaxon($rel, $taxon_no, $options);
    
    # If we were asked for 'nav' info, also show the various categories
    # of subtaxa and whether or not each of the parents are extinct.
    
    if ( $self->{show}{nav} )
    {
	my $r = $self->{main_record};
	
	# First get taxon records for all of the relevant supertaxa.
	
	if ( $r->{kingdom_no} )
	{
	    $r->{kingdom_txn} = $taxonomy->getTaxon($r->{kingdom_no}, { fields => ['size'] });
	}
	
	if ( $r->{phylum_no} )
	{
	    $r->{phylum_txn} = $taxonomy->getTaxon($r->{phylum_no}, { fields => ['size'] });
	}
	
	if ( $r->{class_no} )
	{
	    $r->{class_txn} = $taxonomy->getTaxon($r->{class_no}, { fields => ['size'] });
	}
	
	if ( $r->{order_no} )
	{
	    $r->{order_txn} = $taxonomy->getTaxon($r->{order_no}, { fields => ['size'] });
	}
	
	if ( $r->{family_no} )
	{
	    $r->{family_txn} = $taxonomy->getTaxon($r->{family_no}, { fields => ['size'] });
	}
	
	if ( $r->{parent_no} )
	{
	    $r->{parent_txn} = $taxonomy->getTaxon($r->{parent_no}, { fields => ['size'] });
	}
	
	# Then add the various lists of subtaxa.
	
	unless ( $r->{phylum_no} or (defined $r->{rank} && $r->{rank} <= 20) )
	{
	    $r->{phylum_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { limit => 10, order => 'size.desc', rank => 20, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{class_no} or $r->{rank} <= 17 )
	{
	    $r->{class_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { limit => 10, order => 'size.desc', rank => 17, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{order_no} or $r->{rank} <= 13 )
	{
	    my $order = defined $r->{order_count} && $r->{order_count} > 100 ? undef : 'size.desc';
	    $r->{order_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { limit => 10, order => $order, rank => 13, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{family_no} or $r->{rank} <= 9 )
	{
	    my $order = defined $r->{family_count} && $r->{family_count} > 100 ? undef : 'size.desc';
	    $r->{family_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { limit => 10, order => $order, rank => 9, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} > 5 )
	{
	    my $order = defined $r->{genus_count} && $r->{order_count}> 100 ? undef : 'size.desc';
	    $r->{genus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						    { limit => 10, order => $order, rank => 5, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 )
	{
	    $r->{subgenus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 4, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 or $r->{rank} == 4 )
	{
	    $r->{species_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 3, fields => ['size', 'app'] } ) ];
	}
	
	$r->{children} = 
	    [ $taxonomy->getTaxa('children', $taxon_no, { limit => 10, order => 'size.desc', fields => ['size', 'app'] } ) ];
    }
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database for basic info about all taxa satisfying the conditions
# previously specified by a call to setParameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    my $valid = $self->{valid};
    my $taxon_no;
    
    # First, figure out what info we need to provide
    
    my $options = {};
    
    my @fields = ('link');
    
    push @fields, 'ref' if $self->{show}{ref};
    push @fields, 'attr' if $self->{show}{attr};
    push @fields, 'size' if $self->{show}{size};
    push @fields, 'app' if $self->{show}{app};
    
    push @fields, 'link' if $self->{show}{nav};
    push @fields, 'parent' if $self->{show}{nav};
    push @fields, 'phylo' if $self->{show}{nav};
    push @fields, 'counts' if $self->{show}{nav};
    
    $options->{fields} = \@fields;
    
    # Specify the other query options according to the query parameters.
    
    my $limit = $valid->value('limit');
    my $offset = $valid->value('offset');
    
    $options->{limit} = $limit if defined $limit;
    $options->{offset} = $offset if defined $offset;
    
    # If the parameter 'name' was given, then fetch all matching taxa.  Order
    # them in descending order by size.
    
    my @name_matches;
    
    if ( $valid->value('name') )
    {
	my $name = $valid->value('name');
	
	my $options = { %$options, order => 'size.desc' };
	$options->{exact} = 1 if $valid->value('exact');
    	
	@name_matches = $taxonomy->getTaxaByName($name, $options);
	return unless @name_matches;
    }
    
    # If a name was given and the relationship is 'self' (or not specified,
    # being the default) then just return the list of matches.
    
    if ( $valid->value('name') and $valid->value('rel') eq 'self' )
    {
	$self->{main_result} = \@name_matches;
	$self->{result_count} = scalar(@name_matches);
	return 1;
    }
    
    # If a name was given and some other relationship was specified, use the
    # first name found.
    
    elsif ( $valid->value('name') )
    {
	$options->{return} = 'stmt';
	my $id = $name_matches[0]{orig_no};
	my $rel = $valid->value('rel') || 'self';
	
	if ( defined $valid->value('rank') )
	{
	    $options->{rank} = $valid->value('rank');
	}
	
	($self->{main_sth}) = $taxonomy->getTaxa($rel, $id, $options);
	return $self->{main_sth};
    }
    
    # Otherwise, we are listing taxa by relationship.  If we are asked for
    # 'common_ancestor', then we have to process the result further.
    
    elsif ( $valid->value('rel') eq 'common_ancestor' )
    {
	$options->{return} = 'list';
	my $id_list = $valid->value('id');
	
	($self->{main_record}) = $taxonomy->getTaxa('common_ancestor', $id_list, $options);
    }
    
    # Otherwise, we just 
    
    elsif ( $valid->value('id') or $valid->value('rel') eq 'all_taxa' )
    {
	$options->{return} = 'stmt';
	my $id_list = $valid->value('id');
	my $rel = $valid->value('rel') || 'self';
	
	if ( defined $valid->value('rank') )
	{
	    $options->{rank} = $valid->value('rank');
	}
	
	($self->{main_sth}) = $taxonomy->getTaxa($rel, $id_list, $options);
	return $self->{main_sth};
    }
    
    # Otherwise, we have an empty result.
    
    else
    {
	return;
    }
}


# getThumb ( )
# 
# Fetch a thumbnail given a taxon_no value.

sub getThumb {
    
    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $TAXON_IMAGES = $TAXON_TABLE{taxon_trees}{images};
    
    my $orig_no = $self->{params}{id};
    
    my $sql = "SELECT thumb FROM $TAXON_IMAGES
	       WHERE orig_no = $orig_no and priority >= 0
	       ORDER BY priority desc LIMIT 1";
    
    ($self->{main_data}) = $dbh->selectrow_array($sql);
}


# getIcon ( )
# 
# Fetch an icon given a taxon_no value.

sub getIcon {

    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $TAXON_IMAGES = $TAXON_TABLE{taxon_trees}{images};
    
    my $orig_no = $self->{params}{id};
    
    my $sql = "SELECT icon FROM $TAXON_IMAGES
	       WHERE orig_no = $orig_no and priority >= 0
	       ORDER BY priority desc LIMIT 1";
    
    ($self->{main_data}) = $dbh->selectrow_array($sql);
}


# validNameSpec ( name )
# 
# Returns true if the given value is a valid taxonomic name specifier.  We
# allow not only single names, but also lists of names and extra modifiers as
# follows: 
# 
# valid_spec:	name_spec [ , name_spec ... ]
# 
# name_spec:	[ single_name : ] general_name [ < exclude_list > ]
# 
# single_name:	no spaces, but may include wildcards
# 
# general_name: may include up to four components, second component may
#		include parentheses, may include wildcards
# 
# exclude_list:	general_name [ , general_name ]

sub validNameSpec {
    
    my ($value, $context) = @_;
    
    return;	# for now
    
}


sub validRankSpec {
    
    my ($value, $context) = @_;
    
    return;
}


# This routine will be called if necessary in order to properly process the
# results of a query for taxon parents.

sub processResultSet {
    
    my ($self, $rowlist) = @_;
    
    # Run through the parent list and note when we reach the last
    # kingdom-level taxon.  Any entries before that point are dropped 
    # [see TaxonInfo.pm, line 1252 as of 2012-06-24]
    # 
    # If the leaf entry is of rank subgenus or lower, we may need to rewrite the
    # last few entries so that their names properly match the higher level entries.
    # [see TaxonInfo.pm, lines 1232-1271 as of 2012-06-24]
    
    my @new_list;
    my ($genus_name, $subgenus_name, $species_name, $subspecies_name);
    
    for (my $i = 0; $i < scalar(@$rowlist); $i++)
    {
	# Only keep taxa from the last kingdom-level entry on down.
	
    	@new_list = () if $rowlist->[$i]{taxon_rank} eq 'kingdom';
	
	# Skip junior synonyms, we only want a list of 'belongs to' entries.
	
	next unless $rowlist->[$i]{status} eq 'belongs to';
	
	# Note genus, subgenus, species and subspecies names, and rewrite as
	# necessary anything lower than genus in order to match the genus, etc.
	
	my $taxon_name = $rowlist->[$i]{taxon_name};
	my $taxon_rank = $rowlist->[$i]{taxon_rank};
	
	if ( $taxon_rank eq 'genus' )
	{
	    $genus_name = $taxon_name;
	}
	
	elsif ( $taxon_rank eq 'subgenus' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*\((\w+)\)/ )
	    {
		$subgenus_name = "$genus_name ($2)";
		$rowlist->[$i]{taxon_name} = $subgenus_name;
	    }
	}
	
	elsif ( $taxon_rank eq 'species' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*(\(\w+\)\s*)?(\w+)/ )
	    {
		$species_name = $subgenus_name || $genus_name;
		$species_name .= " $3";
		$rowlist->[$i]{taxon_name} = $species_name;
	    }
	}
	
	elsif ( $taxon_rank eq 'subspecies' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*(\(\w+\)\s*)?(\w+)\s+(\w+)/ )
	    {
		$subspecies_name = "$species_name $4";
		$rowlist->[$i]{taxon_name} = $subspecies_name;
	    }
	}
	
	# Now add the (possibly rewritten) entry to the list
	
	push @new_list, $rowlist->[$i];
    }
    
    # Now substitute the processed list for the raw one.
    
    @$rowlist = @new_list;
}


# processRecord ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub oldProcessRecord {
    
    my ($self, $row) = @_;
    
    # The strings stored in the author fields of the database are encoded in
    # utf-8, and need to be decoded (despite the utf-8 configuration flag).
    
    $self->decodeFields($row);
    
    # Interpret the status info based on the code stored in the database.  The
    # code as stored in the database encompasses both taxonomic and
    # nomenclatural status info, which needs to be separated out.  In
    # addition, we need to know whether to report an "acceptedUsage" taxon
    # (i.e. senior synonym or proper spelling).
    
    my ($taxonomic, $report_accepted, $nomenclatural) = interpretStatusCode($row->{status});
    
    # Override the status code if the synonym_no is different from the
    # taxon_no.  This is necessary because sometimes the opinion record that
    # was used to build this part of the hierarchy indicates a 'belongs to'
    # relationship (which normally indicates a valid taxon) but the
    # taxa_tree_cache record indicates a different synonym number.  In this
    # case, the taxon is in fact no valid but is a junior synonym or
    # misspelling.  If spelling_no and synonym_no are equal, it's a
    # misspelling.  Otherwise, it's a junior synonym.
    
    if ( $taxonomic eq 'valid' && $row->{synonym_no} ne $row->{taxon_no} )
    {
	if ( $row->{spelling_no} eq $row->{synonym_no} )
	{
	    $taxonomic = 'invalid' unless $row->{spelling_reason} eq 'recombination';
	    $nomenclatural = $row->{spelling_reason};
	}
	else
	{
	    $taxonomic = 'synonym';
	}
    }
    
    # Put the two status strings into the row record.  If no value exists,
    # leave it blank.
    
    $row->{taxonomic} = $taxonomic || '';
    $row->{nomenclatural} = $nomenclatural || '';
    
    # Determine the nomenclatural code that has jurisidiction, if that was
    # requested.
    
    if ( $self->{show_code} and defined $row->{lft} )
    {
	$self->determineNomenclaturalCode($row);
    }
    
    # Determine the first appearance data, if that was requested.
    
    if ( $self->{show_firstapp} )
    {
	$self->determineFirstAppearance($row);
    }
    
    # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->generateReference($row);
    }
    
    # Create an attribution if that data was incluced in the query
    
    if ( exists $row->{a_pubyr} )
    {
	$self->generateAttribution($row);
    }
}


# getCodeRanges ( )
# 
# Fetch the ranges necessary to determine which nomenclatural code (i.e. ICZN,
# ICN) applies to any given taxon.  This is only done if that information is
# asked for.

sub getCodeRanges {

    my ($self) = @_;
    my ($dbh) = $self->{dbh};
    
my @codes = ('Metazoa', 'Animalia', 'Plantae', 'Biliphyta', 'Metaphytae',
	     'Fungi', 'Cyanobacteria');

my $codes = { Metazoa => { code => 'ICZN'}, 
	      Animalia => { code => 'ICZN'},
	      Plantae => { code => 'ICN'}, 
	      Biliphyta => { code => 'ICN'},
	      Metaphytae => { code => 'ICN'},
	      Fungi => { code => 'ICN'},
	      Cyanobacteria => { code => 'ICN' } };

    $self->{code_ranges} = $codes;
    $self->{code_list} = \@codes;
    
    my $code_name_list = "'" . join("','", @codes) . "'";
    
    my $code_range_query = $dbh->prepare("
	SELECT taxon_name, lft, rgt
	FROM taxa_tree_cache join authorities using (taxon_no)
	WHERE taxon_name in ($code_name_list)");
    
    $code_range_query->execute();
    
    while ( my($taxon, $lft, $rgt) = $code_range_query->fetchrow_array() )
    {
	$codes->{$taxon}{lft} = $lft;
	$codes->{$taxon}{rgt} = $rgt;
    }
}


# determineNomenclaturalCode ( row )
# 
# Determine which nomenclatural code the given row's taxon falls under

sub determineNomenclaturalCode {
    
    my ($self, $row) = @_;

    my ($lft) = $row->{lft} || return;
    
    # Anything with a rank of 'unranked clade' falls under PhyloCode.
    
    if ( defined $row->{taxon_rank} && $row->{taxon_rank} eq 'unranked clade' )
    {
	$row->{nom_code} = 'PhyloCode';
	return;
    }
    
    # For all other taxa, we go through the list of known ranges in
    # taxa_tree_cache and use the appropriate code.
    
    foreach my $taxon (@{$self->{code_list}})
    {
	my $range = $self->{code_ranges}{$taxon};
	
	if ( $lft >= $range->{lft} && $lft <= $range->{rgt} )
	{
	    $row->{nom_code} = $range->{code};
	    last;
	}
    }
    
    # If this taxon does not fall within any of the ranges, we leave the
    # nom_code field empty.
}


# determineFirstAppearance ( row )
# 
# Calculate the first appearance of this taxon.

sub determineFirstAppearance {
    
    my ($self, $row) = @_;
    
    my $dbh = $self->{dbh};
    
    # Generate a parameter hash to pass to calculateFirstAppearance().
    
    my $params = { taxonomic_precision => $self->{firstapp_precision},
		   types_only => $self->{firstapp_types_only},
		   traces => $self->{firstapp_include_traces},
		 };
    
    # Get the results.
    
    my $results = calculateFirstAppearance($dbh, $row->{taxon_no}, $params);
    return unless ref $results eq 'HASH';
    
    # Check for error
    
    if ( $results->{error} )
    {
	$self->{firstapp_error} = "An error occurred while calculating the first apperance";
	return;
    }
    
    # If we got results, copy each field into the row.
    
    foreach my $field ( keys %$results )
    {
	$row->{$field} = $results->{$field};
    }
}


# interpretSpeciesName ( taxon_name )
# 
# Separate the given name into genus, subgenus, species and subspecies.

sub interpretSpeciesName {

    my ($taxon_name) = @_;
    my @components = split(/\s+/, $taxon_name);
    
    my ($genus, $subgenus, $species, $subspecies);
    
    # If the first character is a space, the first component will be blank;
    # ignore it.
    
    shift @components if @components && $components[0] eq '';
    
    # If there's nothing left, we were given bad input-- return nothing.
    
    return unless @components;
    
    # The first component is always the genus.
    
    $genus = shift @components;
    
    # If the next component starts with '(', it is a subgenus.
    
    if ( @components && $components[0] =~ /^\((.*)\)$/ )
    {
	$subgenus = $1;
	shift @components;
    }
    
    # The next component must be the species
    
    $species = shift @components if @components;
    
    # The last component, if there is one, must be the subspecies.  Strip
    # parentheses if there are any.
    
    $subspecies = shift @components if @components;
    
    if ( defined $subspecies && $subspecies =~ /^\((.*)\)$/ ) {
	$subspecies = $1;
    }
    
    return ($genus, $subgenus, $species, $subspecies);
}


# The following hashes map the status codes stored in the opinions table of
# PaleoDB into taxonomic and nomenclatural status codes in compliance with
# Darwin Core.  The third one, %REPORT_ACCEPTED_TAXON, indicates which status
# codes should trigger the "acceptedUsage" and "acceptedUsageID" fields in the
# output.

our (%TAXONOMIC_STATUS) = (
	'belongs to' => 'valid',
	'subjective synonym of' => 'heterotypic synonym',
	'objective synonym of' => 'homotypic synonym',
	'invalid subgroup of' => 'invalid',
	'misspelling of' => 'invalid',
	'replaced by' => 'invalid',
	'nomen dubium' => 'invalid',
	'nomen nudum' => 'invalid',
	'nomen oblitum' => 'invalid',
	'nomen vanum' => 'invalid',
);


our (%NOMENCLATURAL_STATUS) = (
	'invalid subgroup of' => 'invalid subgroup',
	'misspelling of' => 'misspelling',
	'replaced by' => 'replaced by',
	'nomen dubium' => 'nomen dubium',
	'nomen nudum' => 'nomen nudum',
	'nomen oblitum' => 'nomen oblitum',
	'nomen vanum' => 'nomen vanum',
);


our (%REPORT_ACCEPTED_TAXON) = (
	'subjective synonym of' => 1,
	'objective synonym of' => 1,
	'misspelling of' => 1,
	'replaced by' => 1,
);


# interpretStatusCode ( pbdb_status )
# 
# Use the hashes given above to interpret a status code from the opinions
# table of PaleoDB.  Returns: taxonomic status, whether we should report an
# "acceptedUsage" taxon, and the nomenclatural status.

sub interpretStatusCode {

    my ($pbdb_status) = @_;
    
    # If the status is empty, return nothing.
    
    unless ( defined $pbdb_status and $pbdb_status ne '' )
    {
	return '', '', '';
    }
    
    # Otherwise, interpret the status code according to the mappings specified
    # above.
    
    return $TAXONOMIC_STATUS{$pbdb_status}, $REPORT_ACCEPTED_TAXON{$pbdb_status}, 
	$NOMENCLATURAL_STATUS{$pbdb_status};
}


1;
