package Download;

use PBDBUtil;
use Classification;
use TimeLookup;

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;				# The database handle
my $dbt;				# The new and improved database object
my $q;					# Reference to the parameters
my $s;					# Reference to the session data
my $sql;				# Any SQL string
my $rs;					# Generic recordset
$|=1;

# These arrays contain names of possible fields to be checked by a user in the
# download form.  When writing the data out to files, these arrays are compared
# to the query params to determine the file header line and then the data to
# be written out.
my @collectionsFieldNames = qw(authorizer enterer modifier collection_no collection_subset reference_no collection_name collection_aka country state county latdeg latmin latsec latdec latdir lngdeg lngmin lngsec lngdec lngdir latlng_basis altitude_value altitude_unit geogscale geogcomments emlperiod_max emlperiod_min period_max emlepoch_max emlepoch_min epoch_max epoch_min emlintage_max intage_max emlintage_min intage_min emllocage_max locage_max emllocage_min locage_min zone max_interval_no min_interval_no research_group formation geological_group member localsection localbed localorder regionalsection regionalbed regionalorder stratscale stratcomments lithdescript lithadj lithification lithology1 fossilsfrom1 lithology2 fossilsfrom2 environment tectonic_setting pres_mode geology_comments collection_type collection_coverage collection_meth collection_size collection_size_unit museum collection_comments taxonomy_comments created modified release_date access_level lithification2 lithadj2 period_min otherenvironment rock_censused_unit rock_censused spatial_resolution temporal_resolution feed_pred_traces encrustation bioerosion fragmentation sorting dissassoc_minor_elems dissassoc_maj_elems art_whole_bodies disart_assoc_maj_elems seq_strat lagerstatten concentration orientation preservation_quality sieve_size_min sieve_size_max assembl_comps taphonomy_comments);
my @occurrencesFieldNames = qw(authorizer enterer modifier occurrence_no collection_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name abund_value abund_unit reference_no comments created modified plant_organ plant_organ2);
my @reidentificationsFieldNames = qw(authorizer enterer modifier reid_no occurrence_no collection_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name reference_no comments created modified modified_temp plant_organ);
my @paleozoic = qw(cambrian ordovician silurian devonian carboniferous permian);
my @mesoCenozoic = qw(triassic jurassic cretaceous tertiary);

my %ecotaph1 = ();
my %ecotaph2 = ();
my %ecotaph3 = ();

my $csv;
my $OUT_HTTP_DIR = "/paleodb/data";
my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};
my $DATAFILE_DIR = $ENV{DOWNLOAD_DATAFILE_DIR};
my $outFileBaseName;


sub new {
	my $class = shift;
	$dbh = shift;
	$dbt = shift;
	$q = shift;
	$s = shift;
	my $self = {};

	bless $self, $class;
	return $self;
}

# Main handling routine
sub buildDownload {
	my $self = shift;

	print "
<center>
<h2>The Paleobiology Database: Download Results</h2>
</center>
";

	$self->setupOutput ( );
	$self->retellOptions ( );
	if ( $q->param('ecology1') )	{
		$self->getEcology ( );
	}
	$self->doQuery ( );

}

# Tells the user what options they chose
sub retellOptions {
	my $self = shift;

	my $html = "
<center>
<table border='0' width='600'>
<tr>
	<td colspan='2' bgcolor='#E0E0E0'><b><font size='+1'>Download criteria</font></b></td>
</tr>
";

	$html .= $self->retellOptionsRow ( "Research group or project", $q->param("research_group") );
	INTERVAL: {
		if ( $q->param("interval") eq "Phanerozoic" ) { 
			$html .= $self->retellOptionsRow ( "Time interval", "All periods" );
			last;
		}
		if ( $q->param("interval") eq "Interval1" ) {
			$html .= $self->retellOptionsRow ( "Time interval", "All of the Paleozoic" );
			last;
		}
		if ( $q->param("interval") eq "Interval2" ) {
			$html .= $self->retellOptionsRow ( "Time interval", "All of the Meso-Cenozoic" );
			last;
		}
		if ( $q->param("interval") eq "other" ) {
			my @intervals = $q->param("intervals");
			$html .= $self->retellOptionsRow ( "Time interval", join ( ", ", @intervals ) );
			last;
		}
	}
# LEGACY CODE
#	$html .= $self->retellOptionsRow ( "Epoch", $q->param("epoch") );
#	$html .= $self->retellOptionsRow ( "Single age/stage", $q->param("stage") );
	$html .= $self->retellOptionsRow ( "Oldest interval", $q->param("max_interval_name") );
	$html .= $self->retellOptionsRow ( "Youngest interval", $q->param("min_interval_name") );
	$html .= $self->retellOptionsRow ( "Environment", $q->param("environment") );
	$html .= $self->retellOptionsRow ( "Genus name", $q->param("genus_name") );
	$html .= $self->retellOptionsRow ( "Class", $q->param("class") );
	$html .= $self->retellOptionsRow ( "Only your own data?", $q->param("owndata") ) if ( !  $s->guest ( ) );

	# Continents or country
	my @continents = ( );
	# If a country was selected, ignore the continents JA 6.7.02
	if ( $q->param("country") )	{
		$html .= $self->retellOptionsRow ( "Country", $q->param("country") );
	}
	else	{
		if ( $q->param("global") ) 			{ push ( @continents, "Global" ); }
		if ( $q->param("Africa") ) 			{ push ( @continents, "Africa" ); }
		if ( $q->param("Antarctica") ) 		{ push ( @continents, "Antarctica" ); }
		if ( $q->param("Asia") ) 			{ push ( @continents, "Asia" ); }
		if ( $q->param("Australia") ) 		{ push ( @continents, "Australia" ); }
		if ( $q->param("Europe") ) 			{ push ( @continents, "Europe" ); }
		if ( $q->param("North America") ) 	{ push ( @continents, "North America" ); }
		if ( $q->param("South America") ) 	{ push ( @continents, "South America" ); }
		if ( $#continents > -1 ) {
			$html .= $self->retellOptionsRow ( "Continents", join (  ", ", @continents ) );
		}
	}
	
	$html .= $self->retellOptionsRow ( "Lump lists of same county & formation?", $q->param("lumplist") );
	$html .= $self->retellOptionsRow ( "Lump occurrences of same genus of same collection?", $q->param("lumpgenera") );
	$html .= $self->retellOptionsRow ( "Include poor geographical data?", $q->param("strictgeography") );
	$html .= $self->retellOptionsRow ( "Include poor temporal data?", $q->param("strictchronology") );
	$html .= $self->retellOptionsRow ( "Include occurrences that are generically indeterminate?", $q->param("indet") );
	$html .= $self->retellOptionsRow ( "Output data format", $q->param("collections_put") );

	my @occurrenceOutputFields = (	"occurrences_authorizer",
					"occurrences_enterer",
					"occurrences_modifier",
					"occurrences_occurrence_no",
					"occurrences_subgenus_name",
					"occurrences_species_name",
					"occurrences_abund_value",
					"occurrences_abund_unit",
					"occurrences_reference_no",
					"occurrences_comments",
					"occurrences_plant_organ",
					"occurrences_plant_organ2",
					"occurrences_created",
					"occurrences_modified",
					"collections_only" );

	my @reidOutputFields = ("reidentifications_authorizer",
							"reidentifications_enterer",
							"reidentifications_modifier",
							"reidentifications_occurrence_no",
							"reidentifications_subgenus_name",
							"reidentifications_species_name",
							"reidentifications_reference_no",
							"reidentifications_comments",
							"reidentifications_plant_organ",
							"reidentifications_created",
							"reidentifications_modified");

	my @occurrenceOutputResult = ( "occurrences_genus_name" );
	foreach my $field ( @occurrenceOutputFields ) {
		if( $q->param ( $field ) ){ 
			push ( @occurrenceOutputResult, $field ); 
		}
	}
	push(@occurrenceOutputResult,"reidentifications_genus_name" );
	foreach my $field ( @reidOutputFields ) {
		my $temp = $field;
		$temp =~ s/reidentifications/occurrences/;
		if( $q->param ( $temp ) ){ 
			push ( @occurrenceOutputResult, $field ); 
		}
	}

	$html .= $self->retellOptionsRow ( "Occurrence output fields", join ( "<BR>", @occurrenceOutputResult ) );


	my @collectionOutputFields = (	"collections_authorizer", 
					"collections_enterer", 
					"collections_modifier", 
					"collections_collection_no", 
					"collections_collection_subset", 
					"collections_reference_no", 
					"collections_collection_name", 
					"collections_collection_aka", 
					"collections_country", 
					"collections_state", 
					"collections_county", 
					"collections_latdeg", 
					"collections_latmin", 
					"collections_latsec", 
					"collections_latdec", 
					"collections_latdir", 
					"collections_lngdeg", 
					"collections_lngmin", 
					"collections_lngsec", 
					"collections_lngdec", 
					"collections_lngdir", 
					"collections_latlng_basis", 
					"collections_geogscale", 
					"collections_geogcomments", 
					"collections_emlperiod_max", 
					"collections_period_max", 
					"collections_emlperiod_min", 
					"collections_period_min", 
					"collections_emlepoch_max", 
					"collections_epoch_max", 
					"collections_emlepoch_min", 
					"collections_epoch_min", 
					"collections_emlintage_max", 
					"collections_intage_max", 
					"collections_emlintage_min", 
					"collections_intage_min", 
					"collections_emllocage_max", 
					"collections_locage_max", 
					"collections_emllocage_min", 
					"collections_locage_min", 
					"collections_zone", 
					"collections_max_interval_no", 
					"collections_min_interval_no", 
					"collections_formation", 
					"collections_member", 
					"collections_localsection", 
					"collections_localbed", 
					"collections_localorder", 
					"collections_regionalsection", 
					"collections_regionalbed", 
					"collections_regionalorder", 
					"collections_stratscale", 
					"collections_stratcomments", 
					"collections_lithdescript", 
					"collections_lithadj", 
					"collections_lithification", 
					"collections_lithology1", 
					"collections_fossilsfrom1", 
					"collections_lithadj2", 
					"collections_lithification2", 
					"collections_lithology2", 
					"collections_fossilsfrom2", 
					"collections_environment", 
					"collections_tectonic_setting", 
					"collections_preservation", 
					"collections_geology_comments", 
					"collections_collection_type", 
					"collections_collection_coverage", 
					"collections_collection_attributes", 
					"collections_collection_size", 
					"collections_museum", 
					"collections_collection_comments", 
					"collections_taxonomy_comments", 
					"collections_created", 
					"collections_modified" );

	my @collectionOutputResult = ( "collection_no" );	# A freebie
	if ( ! $q->param("collections_only") ) { push ( @collectionOutputResult, "genus_reso", "genus_name" ); }
	foreach my $field ( @collectionOutputFields ) {
		if ( $q->param ( $field ) ) { push ( @collectionOutputResult, $field ); }
	}
	$html .= $self->retellOptionsRow ( "Collection output fields", join ( "<BR>", @collectionOutputResult ) );

	$html .= "
</table>
";

	$html =~ s/_/ /g;
	print $html;
}

# Formats a query parameter (name=value) as a table row in html.
sub retellOptionsRow {
	my $self = shift;
	my $name = shift;
	my $value = shift;

	if ( $value ) {
		return "<tr><td valign='top'>$name</td><td><i>$value</i></td></tr>\n";
	}
}

# Returns a list of field names to print out by comparing the query params
# to the above global params arrays.
sub getOutFields {
	my $self = shift;
	my $tableName = shift;
	my $isReID = shift;
	my @outFields;
	
	if($tableName eq "collections") {
		@fieldNames = @collectionsFieldNames;
	} elsif($tableName eq "occurrences") {
		if($isReID eq 'reidentifications'){
			@fieldNames = @reidentificationsFieldNames;
		}
		else{
			@fieldNames = @occurrencesFieldNames;
		}
	} else {
		$self->dbg("getOutFields(): Unknown table [$tableName]");
	}
	
	my @outFields = ( );
	foreach my $fieldName ( @fieldNames ) {
		# use brackets below because the underscore is a valid
		# character for identifiers (could also have done $var."_".$var
		if ( $q->param("${tableName}_${fieldName}") eq "YES") {
			if($fieldName eq 'subgenus_name') {
				if($isReID eq 'reidentifications'){
					push(@outFields, "reidentifications.subgenus_reso as reid_subgenus_reso");
				}
				else{
					push(@outFields, "$tableName.subgenus_reso");
				}
			}
			elsif($fieldName eq 'species_name') {
				if($isReID eq 'reidentifications'){
					push(@outFields, "reidentifications.species_reso as reid_species_reso");
				}
				else{
					push(@outFields, "$tableName.species_reso");
				}
			}
			if($isReID eq 'reidentifications'){
				push(@outFields, "reidentifications.$fieldName as reid_$fieldName");
			}
			else{
				push(@outFields, "$tableName.$fieldName");
			}
		}
	}
	return @outFields;
}

# Wrapper to getOutFields() for returning all table rows in a single string.
sub getOutFieldsString {
	my $self = shift;
	my $tableName = shift;
	my $isReID = shift;

	my $outFieldsString = join ( ",\n", $self->getOutFields($tableName, $isReID) );
	return $outFieldsString;
}

# LEGACY CODE
# Returns a string representation of the time interval (period)
# portion of the collections where clause.  The return value
# looks like: period='period_name' OR period='period2_name'
sub getTimeIntervalString {
	my $self = shift;
	my $single_interval = $q->param('interval');
	
	# If the user selected 'other', get the list of periods
	if($single_interval eq 'other')
	{
		my @intervals = $q->param('intervals');
		return $self->makeOrString('collections.period_max', @intervals);
	}
	# If the user selected Phanerozoic, do not constrain this field
	# (All but Neoproterozoic).
	elsif($single_interval eq "Phanerozoic")
	{
		return $self->makeOrString('collections.period_max', @paleozoic) . " OR " . $self->makeOrString('collections.period_max', @mesoCenozoic);
	}
	# If the user selected interval1 ("all of the Paleozoic")
	# (Cambrian-Permian)
	elsif($single_interval eq "Interval1")
	{
		return $self->makeOrString('collections.period_max', @paleozoic);
	}
	# If the user selected interval2 ("all of the Meso-Cenozoic")
	# (Triassic-Tertiary)
	elsif($single_interval eq "Interval2")
	{
		return $self->makeOrString('collections.period_max', @mesoCenozoic);
	}
}

# Takes a field name and an array of values, and returns a string that 
# looks like: 
# 	field_name='array_member' 
#	OR fieldname='array2_member'
# 	...
sub makeOrString {
	my $self = shift;
	my $fieldName = shift;
	my @vals = @_;
	foreach my $val (@vals)
	{
		$val = "$fieldName='$val'";
	}
	return " " . join("\n OR ", @vals) . " ";
}

# Returns research group
sub getResGrpString {
	my $self = shift;
	my $result = "";

	my $resgrp = $q->param('research_group');

	if($resgrp && $resgrp =~ /(^ETE$)|(^5%$)|(^1%$)|(^PACED$)|(^PGAP$)/){
		my $resprojstr = PBDBUtil::getResearchProjectRefsStr($dbh,$q);
		if($resprojstr ne ""){
			$result = " collections.reference_no IN (" . $resprojstr . ")";
		}
	}
	elsif($resgrp){
		$result = " FIND_IN_SET( '$resgrp', collections.research_group ) ";
	}

	return $result;
}

# 6.7.02 JA
sub getCountryString {
	my $self = shift;

	my $country = $q->param('country');

	if ( $country ) { return " collections.country='$country' "; }
	return "";
}

sub getIntervalString	{
	my $self = shift;
	my $max = $q->param('max_interval_name');
	my $min = $q->param('min_interval_name');

	if ( $max )	{
		my $collref = TimeLookup::processLookup($dbh, $dbt, '', $max, '', $min);
		my @colls = @{$collref};
		if ( ! @colls )	{
			print "<p><b>WARNING: Can't complete the download because the specified time intervals are unknown</b></p>\n"; exit;
		}
		return " ( collections.collection_no IN ( " . join (',',@colls) . " ) )";
	}

	return "";
}

# LEGACY CODE
sub getEpochString {
	my $self = shift;
	my $epoch = $q->param('epoch');
	if($epoch)	{
		return qq| (collections.epoch_max LIKE "$epoch%" OR collections.epoch_min LIKE "$epoch%") |;
	}
	return "";
}

# LEGACY CODE
# 19.3.02 JA
sub getStageString {
	my $self = shift;
	my $stage = $q->param('stage');
	if($stage)	{
		return qq| (collections.intage_max LIKE "$stage%" OR collections.intage_min LIKE "$stage%"  OR collections.locage_max LIKE "$stage%" OR collections.locage_min LIKE "$stage%") |;
	}
	return "";
}

sub getEnvironmentString{
	my $self = shift;
	my $environment = $q->param('environment');
	if($environment){
		return qq| collections.environment='$environment' |;
	}
	return "";
}

sub getRegionsString {
	my $self = shift;
	my $retVal = "";
	my @bits;
	
	# Get the regions
	if ( ! open ( REGIONS, "$DATAFILE_DIR/PBDB.regions" ) ) {
	print "<font color='red'>Skipping regions.</font> Error message is $!<BR><BR>\n";
	return;
	}

	while (<REGIONS>)
	{
		chomp();
		my ($region, $countries) = split(/:/, $_, 2);
		# Clean up the string (need to escape single quotes) JA 6.7.02
		$countries =~ s/'/\\'/g;
		$REGIONS{$region} = $countries;
		#print "$region: $REGIONS{$region}\n";
	}
	# Add the countries within selected regions
	my @regions = (	"North America", 
					"South America", 
					Europe, 
					Africa,
					Antarctica, 
					Asia, 
					Australia );

	foreach my $region (@regions)
	{
		if($q->param($region) eq 'YES')
		{
			#print "$region = 'YES'\n";
			push(@bits, $self->makeOrString('collections.country', split(/\t/, $REGIONS{$region})));
		}
	}
	$retVal = join('OR', @bits);
	return $retVal;
}

sub getDataForAuthorizer{
	my $self = shift;
	return $q->param('authorizer');
}

sub getOccurrencesWhereClause {
	my $self = shift;
	my $retVal = "";
	
	my $authorizer = $self->getDataForAuthorizer();

	$retVal .= " occurrences.authorizer='$authorizer' " if $authorizer ne "All";

	if($q->param('genus_name') ne ""){
		$retVal .= " AND " if $retVal;
		my $genusNames = $self->getGenusNames($q->param('genus_name'));
		$retVal .= " occurrences.genus_name IN (".$genusNames.")";
	}
	$retVal .= " AND " if $retVal && $q->param('indet') eq 'NO';
	$retVal .= " occurrences.species_name!='indet.' " if $q->param('indet') eq 'NO';
	
	return $retVal;
}

sub getCollectionsWhereClause {
	my $self = shift;
	my $retVal = "";
# LEGACY CODE
#	my $time_interval = $self->getTimeIntervalString();
	
	my $authorizer = $self->getDataForAuthorizer();
	# This is handled by getOccurrencesWhereClause if we're getting occs data.
	if($authorizer ne "All" && $q->param('collections_only') eq 'YES'){
		$retVal .= " collections.authorizer='$authorizer' ";
	}
# LEGACY CODE
#	$retVal .= " AND " if $retVal && $time_interval;
#	$retVal .= "(" . $time_interval . ")" if $time_interval;
	$retVal .= " AND " if $retVal && $self->getResGrpString();
	$retVal .= $self->getResGrpString() if $self->getResGrpString();
	$retVal .= " AND " if $retVal && $self->getCountryString();
	$retVal .= $self->getCountryString() if $self->getCountryString();
	$retVal .= " AND " if $retVal && $self->getIntervalString();
	$retVal .= $self->getIntervalString() if $self->getIntervalString();
# LEGACY CODE
#	$retVal .= " AND " if $retVal && $self->getEpochString();
#	$retVal .= $self->getEpochString() if $self->getEpochString();
#	$retVal .= " AND " if $retVal && $self->getStageString();
#	$retVal .= $self->getStageString() if $self->getStageString();
	$retVal .= " AND " if $retVal && $self->getEnvironmentString();
	$retVal .= $self->getEnvironmentString() if $self->getEnvironmentString();
	
	my $regionsString = $self->getRegionsString();
	if($regionsString ne "")
	{
		if($retVal ne "")
		{
			$retVal .= " AND ";
		}
		$retVal .= "($regionsString)";
	}
	
	return $retVal;
}

sub getNotNullString {
	my $self = shift;
	my (@fieldNames) = @_;
	my $retVal = "";
	
	foreach my $fieldName (@fieldNames) {
		$retVal .= " OR ($fieldName IS NOT NULL AND $fieldName!='') ";
	}
	$retVal =~ s/\A OR//;
	return $retVal;
}

# create a hash table relating taxon names to eco/taphonomic categories
# JA 12.8.03
sub getEcology	{
	my $self = shift;

	my $sql = "SELECT ecotaph.taxon_no,taxon_name," . $q->param('ecology1');
	if ( $q->param('ecology2') )	{
		$sql .= "," . $q->param('ecology2');
	}
	if ( $q->param('ecology3') )	{
		$sql .= "," . $q->param('ecology3');
	}
	$sql .= " FROM ecotaph LEFT JOIN authorities ON ecotaph.taxon_no = authorities.taxon_no";
	my @ecos = @{$dbt->getData($sql)};
for $i (0..5){print "$ecos[$i]->{taxon_name}<br>\n"; }

	for $i (0..$#ecos)	{
		$ecotaph1{$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology1')};
	}
	if ( $q->param('ecology2') )	{
		for $i (0..$#ecos)	{
			$ecotaph2{$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology2')};
		}
	}
	if ( $q->param('ecology3') )	{
		for $i (0..$#ecos)	{
			$ecotaph3{$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology3')};
		}
	}
}

# Assembles and executes the query.  Does a join between the occurrences and
# collections tables, filters the results against the appropriate 
# cgi-bin/data/classdata/ file if necessary, does another select for all fields
# from the refs table for any as-yet-unqueried ref, and writes out the data.
sub doQuery {
	my $self = shift;
	my $p = Permissions->new ( $s );
	my @collectionHeaderCols = ( );
	my @occurrenceHeaderCols = ( );
	my @reidHeaderCols = ( );
	my $outFieldsString = '';
	my %COLLS_DONE;
	my %REFS_DONE;
	my $collections_only = $q->param('collections_only');
	my $distinct_taxa_only = $q->param('distinct_taxa_only');

	# get the names of time intervals
	if ( $q->param('collections_max_interval_no') || $q->param('collections_min_interval_no') ) {
		$sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
		my @intnorefs = @{$dbt->getData($sql)};
		for $intnoref ( @intnorefs )	{
			if ( $intnoref->{eml_interval} )	{
				$max_interval_name{$intnoref->{interval_no}} = $intnoref->{eml_interval} . " " . $intnoref->{interval_name};
			} else	{
				$max_interval_name{$intnoref->{interval_no}} = $intnoref->{interval_name};
			}
		}
	}


	# Getting only collection data:
	if($collections_only eq 'YES'){
		$sql =  "SELECT collections.reference_no, collections.collection_no, ".
				" DATE_FORMAT(collections.release_date, '%Y%m%d') rd_short, ".
				" collections.access_level, ";
	# Getting distinct taxon names from occurrences table (JA 12.9.03)
	} elsif ( $distinct_taxa_only eq "YES" )	{
		$sql = "SELECT DISTINCT occurrences.genus_name ";
	# Getting occurrence and collection data:
	# Create the sql: we're doing a join on occurrences and collections
	# so as to select all the data at once.
	} else{
		$sql =	"SELECT occurrences.reference_no, ".
				"occurrences.genus_reso, occurrences.genus_name, ".
				"occurrences.collection_no, ".
				"reidentifications.reference_no as reid_reference_no, ".
				"reidentifications.genus_reso as reid_genus_reso, ".
				"reidentifications.genus_name as reid_genus_name";
		$outFieldsString = $self->getOutFieldsString('occurrences');
		if ($outFieldsString ne '') { $sql .= ", $outFieldsString" ; }
		$outFieldsString = $self->getOutFieldsString('occurrences','reidentifications');
		if ($outFieldsString ne '') { $sql .= ", $outFieldsString" ; }
	}

	# may be getting this for either of the above cases
	$outFieldsString = '';
	$outFieldsString = $self->getOutFieldsString('collections');
	my $comma = ", ";
	$comma = "" if $outFieldsString eq "";

	# may be getting this for either of the above cases
	my $collectionsWhereClause = $self->getCollectionsWhereClause();

	# complete the collections only query string
	if($collections_only eq 'YES'){
		$sql .= " collections.research_group ".$comma.$outFieldsString.
				" FROM collections ";
		if($collectionsWhereClause ne ""){
			$sql .= " WHERE $collectionsWhereClause ";
		}
		if($q->param('strictgeography') eq 'NO'){
			if($sql =~ /WHERE/){
				$sql .= "\nAND (collections.county IS NOT NULL OR (collections.latdeg IS NOT NULL AND collections.lngdeg IS NOT NULL))\n";
			}
			else{
				$sql .= "\nWHERE (collections.county IS NOT NULL OR (collections.latdeg IS NOT NULL AND collections.lngdeg IS NOT NULL))\n";
			}
		}
# LEGACY CODE
#		if($q->param('strictchronology') eq 'NO'){
#			if($sql =~ /WHERE/){
#				$sql .= "\nAND ((" . $self->getNotNullString(('collections.epoch_max')) . ") OR (" . $self->getNotNullString(('collections.locage_min', 'collections.locage_max', 'collections.intage_max', 'collections.intage_min')) . "))";
#			}
#			else{
#				$sql .= "\nWHERE ((" . $self->getNotNullString(('collections.epoch_max')) . ") OR (" . $self->getNotNullString(('collections.locage_min', 'collections.locage_max', 'collections.intage_max', 'collections.intage_min')) . "))";
#			}
#		}
	# complete the collections/occurrences join query string
	} else{
		$sql .= ", collections.collection_no, " .
				" DATE_FORMAT(collections.release_date, '%Y%m%d') rd_short, ".
				" collections.access_level, ".
				" collections.research_group ".$comma.$outFieldsString.
				" FROM occurrences, collections LEFT JOIN reidentifications ON".
				" occurrences.occurrence_no = reidentifications.occurrence_no ".
				" WHERE collections.collection_no = occurrences.collection_no";

		my $occWhereClause = $self->getOccurrencesWhereClause();
		$sql .= " AND ".  $occWhereClause if $occWhereClause;
		$sql .= " AND $collectionsWhereClause " if $collectionsWhereClause ne "";
		$sql .= "\nAND (collections.county IS NOT NULL OR (collections.latdeg IS NOT NULL AND collections.lngdeg IS NOT NULL))\n" if $q->param('strictgeography') eq 'NO';
# LEGACY CODE
#		$sql .= "\nAND ((" . $self->getNotNullString(('collections.epoch_max')) . ") OR (" . $self->getNotNullString(('collections.locage_min', 'collections.locage_max', 'collections.intage_max', 'collections.intage_min')) . "))" if $q->param('strictchronology') eq 'NO';

	}

	# GROUP BY basically does a 'DISTINCT' on these two columns (for join only)
	if($q->param('lumpgenera') eq 'YES' && $collections_only ne 'YES'){
		$sql .= " GROUP BY occurrences.genus_name, occurrences.collection_no";
	} elsif ( $distinct_taxa_only eq "YES" )	{
		$sql .= " GROUP BY occurrences.genus_name";
	}

	$sql =~ s/\s+/ /g;
	$self->dbg("<b>Occurrences query:</b><br>\n$sql<BR>");

	my $sth = $dbh->prepare($sql); #|| die $self->dbg("Prepare query failed ($!)<br>");
	my $rv = $sth->execute(); # || die $self->dbg("Execute query failed ($!)<br>");
	$self->dbg($sth->rows()." rows returned.<br>");

	# print column names to occurrence output file JA 19.8.01
	my $header = "";
	if($q->param('collections_put') eq 'comma-delimited text'){
		if( ! $q->param('distinct_taxa_only') ){
			$header =  "collection_no";
		} else	{
			$header = "genus_name";
		}
		if( ! $q->param('collections_only') && ! $q->param('distinct_taxa_only') ){
			$header .= ",genus_reso,genus_name,reid_genus_reso,reid_genus_name";
		}
		$sepChar = ',';
	}
	elsif( $q->param('collections_put') eq 'tab-delimited text'){
		if( ! $q->param('distinct_taxa_only') ){
			$header =  "collection_no";
		} else	{
			$header = "genus_name";
		}
		if( ! $q->param('collections_only') && ! $q->param('distinct_taxa_only') ){
			$header .= "\tgenus_reso\tgenus_name\treid_genus_reso\treid_genus_name";
		}
		$sepChar = "\t";
	}

	# Occurrence header
	@occurrenceHeaderCols = $self->getOutFields('occurrences');	# Need this (for later...)
	my $occurrenceCols = join($sepChar, @occurrenceHeaderCols);
	if ( $occurrenceCols ) { $header .= $sepChar.$occurrenceCols; }

	# ReID header
	# Need this (for later...)
	@reidHeaderCols = $self->getOutFields('occurrences','reidentifications');	
	foreach my $col (@reidHeaderCols){
		$col =~ s/.*?as (reid_(.*))$/reidentifications.$2/;
	}
	my $reidCols = join($sepChar, @reidHeaderCols);
	if ( $reidCols ) { $header .= $sepChar.$reidCols; }

	if ( $q->param('ecology1') )	{
		$header .= $sepChar.$q->param('ecology1');
	}
	if ( $q->param('ecology2') )	{
		$header .= $sepChar.$q->param('ecology2');
	}
	if ( $q->param('ecology3') )	{
		$header .= $sepChar.$q->param('ecology3');
	}

	# Collection header
	@collectionHeaderCols = $self->getOutFields('collections');	# Need this (for later...)
	my $collectionCols = join ( $sepChar, @collectionHeaderCols );
	if ( $collectionCols ) { $header .= $sepChar.$collectionCols; }

	print OUTFILE "$header\n";
	$self->dbg ( "Output header: $header" );
	
	# Alroy hack 16.8.01  
	my $classString = $q->param('class');

	# Loop through the result set

	# See if rows okay by permissions module
	my @dataRows = ( );
	my $limit = 1000000;
	my $ofRows = 0;
	$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );

	$sth->finish();
	$self->dbg("Rows that passed Permissions: number of rows $ofRows, length of dataRows array: ".@dataRows."<br>");

	# Run through the result set.
	my $acceptedCount = 0;
	foreach my $row ( @dataRows ){
		# These DON'T come back with a table name prepended.
		my $reference_no = $row->{reference_no};
		my $genus_reso = $row->{genus_reso};
		my $genusName = $row->{genus_name};
		my $reid_genus_reso = $row->{reid_genus_reso};
		my $reid_genus_name = $row->{reid_genus_name};
		my $collection_no = $row->{collection_no};

		#$self->dbg("reference_no: $reference_no<br>genus_reso: $genus_reso<br>genusName: $genusName<br>collection_no: $collection_no<br>");

		# Only print one occurrence per collection if "collections only"
		# was checked; do this by fooling the system into lumping all
		# occurrences in a collection
		my $tempGenus = $genusName;
		if( $q->param('collections_only') ){
			$tempGenus = '';
			if($COLLS_DONE{"$collection_no.$tempGenus"} == 1){
				next;
			}
			# else
			$COLLS_DONE{"$collection_no.$tempGenus"} = 1;
		}
		
		# NOTE:  This would be much more efficient if done in a separate thread.
		if(!exists($REFS_DONE{$reference_no}) && $reference_no){
			my $refsQueryString = "SELECT * FROM refs WHERE reference_no=$reference_no";
			$self->dbg ( "Refs select: $refsQueryString" );
			my $sth2 = $dbh->prepare($refsQueryString);
			$sth2->execute();
			$REFS_DONE{$reference_no} = $self->formatRow($sth2->fetchrow_array());
			$sth2->finish();
		}

		my @coll_row = ();
		my @occs_row = ();
		my @reid_row = ();

		# We don't want to add the collection_no, rd_short, access_level, etc.

		# These next two loops put the values in the correct order since
		# they retrieve the values from the data row in the same order
		# as they appear in the header

		# Loop over each occurrence output column
		foreach my $column ( @occurrenceHeaderCols ){
			$column =~ s/^occurrences\.//;
			push ( @occs_row, $row->{$column} );
		}

		# Loop over each reid output column
		foreach my $column ( @reidHeaderCols ){
			$column =~ s/^reidentifications\./reid_/;
			push ( @reid_row, $row->{$column} );
		}

		# Loop over each collection output column
		foreach my $column ( @collectionHeaderCols ){
			$column =~ s/^collections\.//;
			# translate interval nos into names JA 18.9.03
			if ( $column eq "max_interval_no" )	{
				$row->{$column} = $max_interval_name{$row->{$column}};
			} elsif ( $column eq "min_interval_no" )	{
				$row->{$column} = $min_interval_name{$row->{$column}};
			}
			push ( @coll_row, $row->{$column} );
		}

		# Push the eco/taphonomic data, if any, onto the reid rows
		# WARNING: this only works on genus or higher-order data,
		#  assuming species won't be scored separately
		if ( $q->param('ecology1') )	{
			push @reid_row , $ecotaph1{$genusName};
		}
		if ( $q->param('ecology2') )	{
			push @reid_row , $ecotaph2{$genusName};
		}
		if ( $q->param('ecology3') )	{
			push @reid_row , $ecotaph3{$genusName};
		}

		if( $q->param('collections_only') ){
			$curLine = $self->formatRow(($collection_no, @occs_row, @coll_row));
		} elsif ( $q->param('distinct_taxa_only') )	{
			$curLine = $self->formatRow(($genusName));
		} else{
			$curLine = $self->formatRow(($collection_no, $genus_reso, $genusName, $reid_genus_reso, $reid_genus_name, @occs_row, @reid_row, @coll_row));
		}
		print OUTFILE "$curLine\n";
		$acceptedCount++;
		if(exists($REFS_DONE{$reference_no}) && $REFS_DONE{$reference_no} ne "Y"){
			print REFSFILE "$REFS_DONE{$reference_no}\n";
			$REFS_DONE{$reference_no} = "Y";
			$acceptedRefs++;
		}
	}

	close OUTFILE;
	close REFSFILE;
	
	# Post process, if necessary (family, order, class names)
	my $family_level = $q->param("occurrences_family_name");
	my $order_level = $q->param("occurrences_order_name");
	my $class_level = $q->param("occurrences_class_name");
	my $post_file = "$OUT_FILE_DIR/$occsOutFileName";
	my $levels = "";
	if($family_level eq "YES"){
		$levels = "family";
	}
	if($order_level eq "YES"){
		if($levels ne ""){
			$levels .= ",";
		}
		$levels .= "order";
	}
	if($class_level eq "YES"){
		if($levels ne ""){
			$levels .= ",";
		}
		$levels .= "class";
	}
	if($levels ne ""){
		foc($post_file, $levels);
	}

	# Tell what happened
	if ( ! $acceptedCount ) { $acceptedCount = 0; }
	if ( ! $acceptedRefs ) { $acceptedRefs = 0; }
	my $outputType = "occurrences";
	if ( $q->param("collections_only") )	{
		$outputType = "collections";
	}
	print "
<table border='0' width='600'>
<tr><td bgcolor='#E0E0E0'><b><font size='+1'>Output files</font></b></td></tr>
<tr><td>$acceptedCount $outputType were printed to <a href='$OUT_HTTP_DIR/$occsOutFileName'>$occsOutFileName</a></td></tr>
<tr><td>$acceptedRefs references were printed to <a href='$OUT_HTTP_DIR/$refsOutFileName'>$refsOutFileName</a></td></tr>
</table>
<p align='center'><a href='?action=displayDownloadForm'>Do another download</a>
</p>
</center>
";
	
}

sub setupOutput {
	my $self = shift;
	my $outputType = $q->param('collections_put');
	my $sepChar;
	if($outputType eq 'comma-delimited text')
	{
		$sepChar = ',';
		$outFileExtension = 'csv';
	}
	elsif($outputType eq 'tab-delimited text')
	{
		$sepChar = "\t";
		$outFileExtension = 'tab';
	}
	else
	{
		print "Unknown output type: $outputType\n";
		return;
	}
	
	$csv = Text::CSV_XS->new({
			'quote_char'  => '"',
			'escape_char' => '"',
			'sep_char'    => $sepChar,
			'binary'      => 1
	});
	
	my $authorizer = $s->get("authorizer");
	if ( ! $authorizer ) { $authorizer = "unknown"; }
	$authorizer =~ s/(\s|\.)//g;
	$occsOutFileName = $authorizer . "-occs.$outFileExtension";
	if ( $q->param("collections_only") )	{
		$occsOutFileName = $authorizer . "-cols.$outFileExtension";
	}
	$refsOutFileName = $authorizer . "-refs.$outFileExtension";

	if ( ! open(OUTFILE, ">$OUT_FILE_DIR/$occsOutFileName") ) {
	die ( "Could not open output file: $OUT_FILE_DIR/$occsOutFileName ($!) <BR>\n" );
	}
	if ( ! open(REFSFILE, ">$OUT_FILE_DIR/$refsOutFileName") ) {
	die ( "Could not open output file: $!<BR>\n" );
	}

	chmod 0664, "$OUT_FILE_DIR/$occsOutFileName";
	chmod 0664, "$OUT_FILE_DIR/$refsOutFileName";
}

sub formatRow {
	my $self = shift;

	if ( $csv->combine ( @_ ) ) {
		return $csv->string();
	}
}

sub getGenusNames {
	my $self = shift;
	my $genus_name = (shift || "");

	#my $cslist = PBDBUtil::taxonomic_search($genus_name, $dbt);
	my $cslist = `./recurse $genus_name`;
	return $cslist;
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

sub foc{
	my $filename = shift;
	my $levels = shift;

	open(FILE,$filename) or die "couldn't open $filename ($!)";
	my $headers = <FILE>;
	my @lines = <FILE>;
	close FILE;

	# Parse the headers to find the positions of 'genus_name' and 'genus_reso'
	my @headers = split(',', $headers);
	my $genus_reso_pos = -1;
	my $genus_pos = -1;
	for($i=0; $i<@headers; $i++){
		if($headers[$i] eq "genus_reso"){
			$genus_reso_pos = $i;
		}
		if($headers[$i] eq "genus_name"){
			$genus_pos = $i;
			last;
		}

	}

	# get a list of unique genus names
	my %genera;

	foreach my $item (@lines){
		my @parsed_line = split(',', $item);
		# I'm just using a hash to guarantee uniqueness. 
		# I'll only need the keys...
		next if($parsed_line[$genus_reso_pos] =~ /informal/);
		$parsed_line[$genus_pos] =~ s/\n//;
		$genera{$parsed_line[$genus_pos]} = 1;
	}

	# get the classifications
	my %master_class;
	my @genera = keys %genera;
	%master_class=%{Classification::get_classification_hash($dbt,$levels,\@genera)};
	my $insert_pos = $genus_pos-1;
	if ( $insert_pos < 0 )	{
		$insert_pos = 0;
	}
	if($levels =~ /class/){
		splice(@headers, $insert_pos++, 0, "class_name");
	}
	if($levels =~ /order/){
		splice(@headers, $insert_pos++, 0, "order_name");
	}
	if($levels =~ /family/){
		splice(@headers, $insert_pos++, 0, "family_name");
	}
	my @altered_lines = ();
	foreach my $item (@lines){
		my @parsed_line = split(',', $item);
		my @fkeys = ();
		my @okeys = ();
		my $key = $parsed_line[$genus_pos];
		$key =~ s/\n//;
		my @parents = split ',',$master_class{$key};
		$insert_pos = $genus_pos-1;
		if ( $insert_pos < 0 )	{
			$insert_pos = 0;
		}
		# first insert class
		if ( $levels =~ /class/ )	{
			my $class = $parents[0];
			if($class && $class ne ""){
				splice(@parsed_line, $insert_pos++, 0, $class);
			} else	{
				splice(@parsed_line, $insert_pos++, 0, "");
			}
		}
		# then insert order
		if ( $levels =~ /order/ )	{
			my $order = $parents[1];
			if($order && $order ne ""){
				splice(@parsed_line, $insert_pos++, 0, $order);
			} else	{
				splice(@parsed_line, $insert_pos++, 0, "");
			}
		}
		# then insert family
		if($levels =~ /family/){
			my $family = $parents[2];
			if ( $family && $family ne "" )	{
				splice(@parsed_line, $insert_pos++, 0, $family);
			} else	{
				splice(@parsed_line, $insert_pos++, 0, "");
			}
		}
		my $altered = join(',', @parsed_line);	
		push(@altered_lines, $altered);
	}
	open(FILE,">$filename") or die "couldn't open $filename ($!)";
	$headers = join(",", @headers);
	print FILE $headers;
	foreach my $line (@altered_lines){
		print FILE $line;
	}
	close FILE;
}

1;
