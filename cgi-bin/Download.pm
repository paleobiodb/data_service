package Download;

#use strict;
use PBDBUtil;
use Classification;
use TimeLookup;
use Globals;
use DBTransactionManager;

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
my @collectionsFieldNames = qw(authorizer enterer modifier collection_no collection_subset reference_no collection_name collection_aka country state county latdeg latmin latsec latdec latdir lngdeg lngmin lngsec lngdec lngdir latlng_basis altitude_value altitude_unit geogscale geogcomments zone period_max epoch_max locage_max max_interval_no min_interval_no research_group geological_group formation member localsection localbed localorder regionalsection regionalbed regionalorder stratscale stratcomments lithdescript lithadj lithification lithology1 fossilsfrom1 lithology2 fossilsfrom2 environment tectonic_setting pres_mode geology_comments collection_type collection_coverage collection_meth collection_size collection_size_unit museum collection_comments taxonomy_comments created modified release_date access_level lithification2 lithadj2 otherenvironment rock_censused_unit rock_censused spatial_resolution temporal_resolution feed_pred_traces encrustation bioerosion fragmentation sorting dissassoc_minor_elems dissassoc_maj_elems art_whole_bodies disart_assoc_maj_elems seq_strat lagerstatten concentration orientation preservation_quality sieve_size_min sieve_size_max assembl_comps taphonomy_comments);
my @occurrencesFieldNames = qw(authorizer enterer modifier occurrence_no collection_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name abund_value abund_unit reference_no comments created modified plant_organ plant_organ2);
my @reidentificationsFieldNames = qw(authorizer enterer modifier reid_no occurrence_no collection_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name reference_no comments created modified modified_temp plant_organ);
my @paleozoic = qw(cambrian ordovician silurian devonian carboniferous permian);
my @mesoCenozoic = qw(triassic jurassic cretaceous tertiary);

my %ecotaph = ();

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
	</center>";

	$self->setupOutput ( );
	$self->retellOptions ( );
	if ( $q->param('time_scale') )	{
		$self->getTimeLookup ( );
	}
	if ( $q->param('ecology1') )	{
		$self->getEcology ( );
	}
	$self->doQuery ( );

}


# Prints out the options which the user selected in summary form.
sub retellOptions {
	my $self = shift;

	my $html = "
	<center>
	<table border='0' width='600'>
	<tr>
	<td colspan='2' class='darkList'><b><font size='+1'>Download criteria</font></b></td>
	</tr>";

	# authorizer added 30.6.04 JA (left out by mistake?) 
	$html .= $self->retellOptionsRow ( "Authorizer", $q->param("authorizer") );
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

	# added by rjp on 12/30/2003
	if ($q->param('year')) {
		my $dataCreatedBeforeAfter = $q->param("created_before_after") . " " . $q->param("date") . " " . $q->param("month") . " " . $q->param("year");
		$html .= $self->retellOptionsRow ( "Data records created", $dataCreatedBeforeAfter);
	}
	
	$html .= $self->retellOptionsRow ( "Oldest interval", $q->param("max_interval_name") );
	$html .= $self->retellOptionsRow ( "Youngest interval", $q->param("min_interval_name") );
	$html .= $self->retellOptionsRow ( "Lithologies", $q->param("lithology") );
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

	if ( $q->param("latmin") > -90 && $q->param("latmin") < 90 )	{
		$html .= $self->retellOptionsRow ( "Minimum latitude", $q->param("latmin") . "&deg;" );
	}
	if ( $q->param("latmax") > -90 && $q->param("latmax") < 90 )	{
		$html .= $self->retellOptionsRow ( "Maximum latitude", $q->param("latmax") . "&deg;" );
	}
	if ( $q->param("lngmin") > -180 && $q->param("lngmin") < 180 )	{
		$html .= $self->retellOptionsRow ( "Minimum longitude", $q->param("lngmin") . "&deg;" );
	}
	if ( $q->param("lngmax") > -180 && $q->param("lngmax") < 180 )	{
		$html .= $self->retellOptionsRow ( "Maximum longitude", $q->param("lngmax") . "&deg;" );
	}
	
	$html .= $self->retellOptionsRow ( "Lump lists of same county & formation?", $q->param("lumplist") );
	$html .= $self->retellOptionsRow ( "Lump occurrences of same genus of same collection?", $q->param("lumpgenera") );

	if ( $q->param('small_collection') )	{
		$geogscales = "small collection";
	}
	if ( $q->param('outcrop') )	{
		$geogscales .= ", outcrop";
	}
	if ( $q->param('local_area') )	{
		$geogscales .= ", local area";
	}
	if ( $q->param('basin') )	{
		$geogscales .= ", basin";
	}
	$geogscales =~ s/^,//;

	$html .= $self->retellOptionsRow ( "Geographic scale of collections", $geogscales );

	if ( $q->param('bed') )	{
		$stratscales = "bed";
	}
	if ( $q->param('group_of_beds') )	{
		$stratscales .= ", group of beds";
	}
	if ( $q->param('member') )	{
		$stratscales .= ", member";
	}
	if ( $q->param('formation') )	{
		$stratscales .= ", formation";
	}
	if ( $q->param('group') )	{
		$stratscales .= ", group";
	}
	$stratscales =~ s/^,//;

	$html .= $self->retellOptionsRow ( "Stratigraphic scale of collections", $stratscales );

	$html .= $self->retellOptionsRow ( "Include occurrences that are generically indeterminate?", $q->param("indet") );
	$html .= $self->retellOptionsRow ( "Include occurrences with informal names?", $q->param("informal") );
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
					"collections_period_max", 
					"collections_epoch_max", 
					"collections_locage_max", 
					"collections_max_interval_no", 
					"collections_min_interval_no", 
					"collections_zone", 
					"collections_geological_group", 
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
	# hey, if you want the data binned into time intervals you have to
	#  download the interval names into the occurrences table too
	if ( $q->param('time_scale') )	{
		$q->param('collections_max_interval_no' => "YES");
		$q->param('collections_min_interval_no' => "YES");
	}
	# and hey, if you want counts of some enum field split up within
	#  time interval bins, you have to download that field
	if ( $q->param('binned_field') )	{
		if ( $q->param('binned_field') =~ /plant_organ/ )	{
			$q->param('occurrences_' . $q->param('binned_field') => "YES");
		} else	{
			$q->param('collections_' . $q->param('binned_field') => "YES");
		}
	}
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
		return "<tr><td valign='top'>$name</td><td valign='top'><i>$value</i></td></tr>\n";
	}
}


# Returns a list of field names to print out by comparing the query params
# to the above global params arrays.
sub getOutFields {
	my $self = shift;
	my $tableName = shift;
	my $isReID = shift;
	my @outFields;
	
	my @fieldNames;
	
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

# 29.4.04 JA
sub getLatLongString	{
	my $self = shift;

	my $latmin = $q->param('latmin');
	my $latmax = $q->param('latmax');
	my $lngmin = $q->param('lngmin');
	my $lngmax = $q->param('lngmax');
	my $abslatmin = abs($latmin);
	my $abslatmax = abs($latmax);
	my $abslngmin = abs($lngmin);
	my $abslngmax = abs($lngmax);

	# all the boundaries must be given
	if ( $latmin eq "" || $latmax eq "" || $lngmin eq "" || $lngmax eq "" )	{
		return "";
	}
	# at least one of the boundaries must be non-trivial
	if ( $latmin <= -90 && $latmax >= 90 && $lngmin <= -180 || $lngmax >= 180 )	{
		return "";
	}

	my $latlongclause = " ( ";

	if ( $latmin >= 0 )	{
		$latlongclause .= " ( latdeg>=$abslatmin && latdir='North' ) ";
	} else	{
		$latlongclause .= " ( ( latdeg<$abslatmin && latdir='South' ) OR latdir='North' ) ";
	}
	$latlongclause .= "AND";
	if ( $latmax >= 0 )	{
		$latlongclause .= " ( ( latdeg<$abslatmax && latdir='North' ) OR latdir='South' ) ";
	} else	{
		$latlongclause .= " ( latdeg>=$abslatmax && latdir='South' ) ";
	}
	$latlongclause .= "AND";
	if ( $lngmin >= 0 )	{
		$latlongclause .= " ( lngdeg>=$abslngmin AND lngdir='East' ) ";
	} else	{
		$latlongclause .= " ( ( lngdeg<$abslngmin AND lngdir='West' ) OR lngdir='East' ) ";
	}
	$latlongclause .= "AND";
	if ( $lngmax >= 0 )	{
		$latlongclause .= " ( lngdeg<$abslngmax AND lngdir='East' ) OR lngdir='West' ) ";
	} else	{
		$latlongclause .= " ( lngdeg>=$abslngmax AND lngdir='West' ) ";
	}

	$latlongclause .= " ) ";

	return $latlongclause;
}

sub getIntervalString	{
	my $self = shift;
	my $max = $q->param('max_interval_name');
	my $min = $q->param('min_interval_name');

	# return immediately if the user already selected a full time scale
	#  to bin the data
	if ( $q->param('time_scale') )	{
		return "";
	}

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

# JA 1.7.04
# WARNING: relies on fixed lists of lithologies; if these ever change in
#  the database, this section will need to be modified
sub getLithologyString	{
	my $self = shift;
	my $lithology = $q->param('lithology');
	if ( $lithology eq "siliciclastic only" )	{
		return qq| ( collections.lithology1 IN ('"siliciclastic"','claystone','mudstone','"shale"','siltstone','sandstone','conglomerate') AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ('"siliciclastic"','claystone','mudstone','"shale"','siltstone','sandstone','conglomerate') ) ) |;
	} elsif ( $lithology eq "carbonate only" )	{
		return qq| ( collections.lithology1 IN ('wackestone','packstone','grainstone','"reef rocks"','floatstone','rudstone','bafflestone','bindstone','framestone','"limestone"','dolomite','"carbonate"') AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ('wackestone','packstone','grainstone','"reef rocks"','floatstone','rudstone','bafflestone','bindstone','framestone','"limestone"','dolomite','"carbonate"') ) ) |;
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

sub getGeogscaleString{
	my $self = shift;
	my $geogscales;
	if ( ! $q->param('small_collection') )	{
		$geogscales = "'small collection'";
	}
	if ( ! $q->param('outcrop') )	{
		$geogscales .= ",'outcrop'";
	}
	if ( ! $q->param('local_area') )	{
		$geogscales .= ",'local area'";
	}
	if ( ! $q->param('basin') )	{
		$geogscales .= ",'basin'";
	}
	$geogscales =~ s/^,//;
	if ( $geogscales )	{
		return qq| collections.geogscale NOT IN ($geogscales) |;
	}
}

sub getStratscaleString{
	my $self = shift;
	my $stratscales;
	if ( ! $q->param('bed') )	{
		$stratscales = "'bed'";
	}
	if ( ! $q->param('group_of_beds') )	{
		$stratscales .= ",'group of beds'";
	}
	if ( ! $q->param('member') )	{
		$stratscales .= ",'member'";
	}
	if ( ! $q->param('formation') )	{
		$stratscales .= ",'formation'";
	}
	if ( ! $q->param('group') )	{
		$stratscales .= ",'group'";
	}
	$stratscales =~ s/^,//;
	if ( $stratscales )	{
		return qq| collections.stratscale NOT IN ($stratscales) |;
	}
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

sub getOccurrencesWhereClause {
	my $self = shift;
	
	my $where = DBTransactionManager->new();
	$where->setWhereSeparator("AND");
	
	my $authorizer = $q->param('authorizer');
	
	$where->addWhereItem(" occurrences.authorizer='$authorizer' ") if ($authorizer ne "");
	
	if($q->param('genus_name') ne ""){
		my $genusNames = $self->getGenusNames($q->param('genus_name'));
		$where->addWhereItem(" occurrences.genus_name IN (".$genusNames.")");
	}
	
	$where->addWhereItem(" occurrences.species_name!='indet.' ") if $q->param('indet') eq 'NO';
	$where->addWhereItem(" (occurrences.genus_reso NOT LIKE '%informal%' OR occurrences.genus_reso IS NULL) ") if $q->param('informal') eq 'NO';

	return $where->whereExpr();
}

sub getCollectionsWhereClause {
	my $self = shift;
	
	my $where = DBTransactionManager->new();
	$where->setWhereSeparator("AND");
	
	my $authorizer = $q->param('authorizer');
	# This is handled by getOccurrencesWhereClause if we're getting occs data.
	if($authorizer ne "" && $q->param('collections_only') eq 'YES'){
		$where->addWhereItem(" collections.authorizer='$authorizer' ");
	}
	
	$where->addWhereItem($self->getResGrpString()) if $self->getResGrpString();	
	
	# should we filter the data based on collection creation date?
	# added by rjp on 12/30/2003, some code copied from Curve.pm.
	# (filter it if they enter a year at the minimum.
	if ($q->param('year')) {
		
		my $month = Globals::monthNameToNumber($q->param('month'));
		my $day = $q->param('date');

		# use default values if the user didn't enter any.		
		if (! $q->param('month')) { $month = "01" }
		if (! $q->param('date')) { $day = "01" }
				 
		if ( length $day == 1 )	{
			$day = "0".$q->param('date'); #prepend a zero if only one digit.
		}
		
		# note, this should really be handled by a function?  maybe in bridge.pl.
		my $created_date = $q->param('year').$month.$day."000000";
		# note, the version of mysql on flatpebble needs the 000000 at the end, but the
		# version on the linux box doesn't need it.  weird.						 
	
		my $created_string;
		# if so, did they want to look before or after?
		if ($q->param('created_before_after') eq "before") {
			if ( $q->param('collections_only') eq 'YES' )	{
				$created_string = " collections.created < $created_date ";
			} else	{
				$created_string = " occurrences.created < $created_date ";
			}
		} elsif ($q->param('created_before_after') eq "after") {
			if ( $q->param('collections_only') eq 'YES' )	{
				$created_string = " collections.created > $created_date ";
			} else	{
				$created_string = " occurrences.created > $created_date ";
			}
		}
		
		$where->addWhereItem($created_string);
	}
	
	$where->addWhereItem($self->getCountryString()) if $self->getCountryString();
	$where->addWhereItem($self->getLatLongString()) if $self->getLatLongString();
	$where->addWhereItem($self->getIntervalString()) if $self->getIntervalString();
	$where->addWhereItem($self->getLithologyString()) if $self->getLithologyString();
	$where->addWhereItem($self->getEnvironmentString()) if $self->getEnvironmentString();
	$where->addWhereItem($self->getGeogscaleString()) if $self->getGeogscaleString();
	$where->addWhereItem($self->getStratscaleString()) if $self->getStratscaleString();
		
	my $regionsString = $self->getRegionsString();
	if($regionsString ne "") {
		$where->addWhereItem("($regionsString)");
	}

	return $where->whereExpr();
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

# get a hash table mapping interval numbers into intervals of the selected
#   time scale
# WARNING: the tables are used only to produce the occurrence counts
#  output to the -scale file, not to produce assignments or age estimates
#  for individual collections or occurrences
sub getTimeLookup	{
	my $intervalInScaleRef;
	if ( $q->param('time_scale') eq "PBDB 10 m.y. bins" )	{
		@_ = TimeLookup::processBinLookup($dbh,$dbt);
		%intervallookup = %{$_[0]};
	# hash array is by name
		%upperbinbound = %{$_[1]};
		%lowerbinbound = %{$_[2]};
	} else	{
		$intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, $q->param('time_scale'));
		%intervallookup = %{$intervalInScaleRef};
		@_ = TimeLookup::findBoundaries($dbh,$dbt);
	# first two hash arrays are by number, so used the next two,
	#  which are by name
		%upperbinbound = %{$_[2]};
		%lowerbinbound = %{$_[3]};
	}
}

# create a hash table relating taxon names to eco/taphonomic categories
# JA 12.8.03
# JA 4.4.04: note that keying of ecotaph is here by name and not number
sub getEcology	{
	my $self = shift;

	my $sql = "SELECT ecotaph.taxon_no,taxon_name," . $q->param('ecology1');
	$etfields = 1;
	if ( $q->param('ecology2') )	{
		$sql .= "," . $q->param('ecology2');
		$etfields = 2;
	}
	if ( $q->param('ecology3') )	{
		$sql .= "," . $q->param('ecology3');
		$etfields = 3;
	}
	if ( $q->param('ecology4') )	{
		$sql .= "," . $q->param('ecology4');
		$etfields = 4;
	}
	if ( $q->param('ecology5') )	{
		$sql .= "," . $q->param('ecology5');
		$etfields = 5;
	}
	if ( $q->param('ecology6') )	{
		$sql .= "," . $q->param('ecology6');
		$etfields = 6;
	}
	$sql .= " FROM ecotaph LEFT JOIN authorities ON ecotaph.taxon_no = authorities.taxon_no";
	my @ecos = @{$dbt->getData($sql)};
	my $i;

	for $i (0..$#ecos)	{
		$ecotaph{'1'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology1')};
	}
	if ( $q->param('ecology2') )	{
		for $i (0..$#ecos)	{
			$ecotaph{'2'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology2')};
		}
	}
	if ( $q->param('ecology3') )	{
		for $i (0..$#ecos)	{
			$ecotaph{'3'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology3')};
		}
	}
	if ( $q->param('ecology4') )	{
		for $i (0..$#ecos)	{
			$ecotaph{'4'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology4')};
		}
	}
	if ( $q->param('ecology5') )	{
		for $i (0..$#ecos)	{
			$ecotaph{'5'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology5')};
		}
	}
	if ( $q->param('ecology6') )	{
		for $i (0..$#ecos)	{
			$ecotaph{'6'.$ecos[$i]->{taxon_name}} = $ecos[$i]->{$q->param('ecology6')};
		}
	}
}

# JA 28.2.04
# this is a little confusing; the ancestor ecotaph values are keyed by name,
#  whereas the genus values are now keyed by number - made necessary by fact
#  that occurrences are linked to taxon numbers and not names to avoid problems
#  with homonymy
sub getAncestralEcology	{

	my $etfield = shift;
	my $genus = shift;
	my $ancestor_hash = shift;

	my @parents = split ',',$ancestor_hash;
	for $p ( @parents )	{
		if ( $ecotaph{$etfield.$p} && ! $ecotaph{$etfield.$genus} )	{
			$ecotaph{$etfield.$genus} = $ecotaph{$etfield.$p};
			last;
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
				$min_interval_name{$intnoref->{interval_no}} = $intnoref->{eml_interval} . " " . $intnoref->{interval_name};
			} else	{
				$max_interval_name{$intnoref->{interval_no}} = $intnoref->{interval_name};
				$min_interval_name{$intnoref->{interval_no}} = $intnoref->{interval_name};
			}
		}
	}

	# get the period names for the collections JA 22.2.04
	# based on scale 2 = Harland periods
	if ( $q->param('collections_period_max') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '2');
		%myperiod = %{$intervalInScaleRef};
	}

	# get the epoch names for the collections JA 22.2.04
	# based on scale 4 = Harland epochs
	if ( $q->param('collections_epoch_max') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '4');
		%myepoch = %{$intervalInScaleRef};
	}
	# get the PBDB 10 m.y. bin names for the collections JA 3.3.04
	# WARNING: the locage_max field is just being used as a dummy
	if ( $q->param('collections_locage_max') )	{
		@_ = TimeLookup::processBinLookup($dbh,$dbt);
		%mybin = %{$_[0]};
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
				"occurrences.taxon_no, ".
				"occurrences.collection_no, ".
				"occurrences.abund_value, ".
				"occurrences.abund_unit, ".
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

	#print "collectionsWhereClause = $collectionsWhereClause<BR>";
	
	# complete the collections only query string
	if($collections_only eq 'YES'){
		$sql .= " collections.research_group ".$comma.$outFieldsString.
				" FROM collections ";
		if($collectionsWhereClause ne ""){
			$sql .= " WHERE $collectionsWhereClause ";
		}
		# LEGACY CODE
#		if($q->param('strictgeography') eq 'NO'){
#			if($sql =~ /WHERE/i){
#				$sql .= "\nAND (collections.county IS NOT NULL OR (collections.latdeg IS NOT NULL AND collections.lngdeg IS NOT NULL))\n";
#			}
#			else{
#				$sql .= "\nWHERE (collections.county IS NOT NULL OR (collections.latdeg IS NOT NULL AND collections.lngdeg IS NOT NULL))\n";
#			}
#		}
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

		#print "sql = $sql<BR>";

	
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
	if ( $q->param('ecology4') )	{
		$header .= $sepChar.$q->param('ecology4');
	}
	if ( $q->param('ecology5') )	{
		$header .= $sepChar.$q->param('ecology5');
	}
	if ( $q->param('ecology6') )	{
		$header .= $sepChar.$q->param('ecology6');
	}

	# Collection header
	@collectionHeaderCols = $self->getOutFields('collections');	# Need this (for later...)
	my $collectionCols = join ( $sepChar, @collectionHeaderCols );
	if ( $collectionCols ) { $header .= $sepChar.$collectionCols; }

	# trivial clean up: "period_max" is actually a computed period value,
	#  so call it "period"; likewise "epoch_max"/"epoch"
	$header =~ s/period_max/period/;
	$header =~ s/epoch_max/epoch/;

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

	# knock out collections with paleolatitudes that are too high or
	#  too low JA 30.6.04
	if ( $q->param('paleolatlimit') > 0 )	{
		open COORDS,"<./data/collection.ageplace";
		my %goodlats;
		my @tempDataRows;
		while (<COORDS>)	{
			my @temp = split /\t/,$_;
			# col 0 is the collection number; col 8 is the
			#  paleolatitude
			# we're expecting something like "greater than"
			#  or "less than" as a query parameter
			if ( $q->param('paleolatdirection') =~ /greater/ && abs($temp[8]) >= $q->param('paleolatlimit') )	{
				$goodlats{$temp[0]}++;
			} elsif ( $q->param('paleolatdirection') =~ /less/ && abs($temp[8]) < $q->param('paleolatlimit') )	{
				$goodlats{$temp[0]}++;
			}
		}
		close COORDS;
		foreach my $row ( @dataRows ){
			if ( $goodlats{$row->{collection_no}} )	{
				push @tempDataRows , $row;
			}
		}
		@dataRows = @tempDataRows;
	}

	# run through the result set

	# first do a quick hit to get some by-taxon and by-collection stats
	foreach my $row ( @dataRows ){
		# cumulate number of collections including each genus
		$totaloccs{$row->{genus_name}}++;
		# need these two for ecology lookup below
		$totaloccsbyno{$row->{taxon_no}}++;
		$genusbyno{$row->{taxon_no}} = $row->{genus_name};
		# cumulate number of specimens per collection, and number of
		#  times a genus has abundance counts at all
		if ( ( $row->{abund_unit} eq "specimens" || $row->{abund_unit} eq "individuals" ) && ( $row->{abund_value} > 0 ) )	{
			$nisp{$row->{collection_no}} = $nisp{$row->{collection_no}} + $row->{abund_value};
			$numberofcounts{$row->{genus_name}}++;
		}
	}

	$genusbyno{'0'} = "";

	# get the higher order names associated with each genus name,
	#   then set the ecotaph values by running up the hierarchy
	# JA 28.2.04
	# only do this is ecotaph data were requested
	# WARNING: only the common ranks are retrieved
	# JA 4.4.04: adapted this to use taxon numbers instead of names
	if ( $q->param('ecology1') )	{

		# finally, get the higher order names
		my @genera = keys %totaloccsbyno;
		my $levels = "family,order,class,phylum";
		my %ancestor_hash=%{Classification::get_classification_hash($dbt,$levels,\@genera)};
		for $etfield ( 1..$etfields )	{
			for my $g ( @genera )	{
				&getAncestralEcology($etfield,$g,$ancestor_hash{$g});
			}
		}
	}

	# main pass through the results set
	my $acceptedCount = 0;
	foreach my $row ( @dataRows ){
		# These DON'T come back with a table name prepended.
		my $reference_no = $row->{reference_no};
		my $genus_reso = $row->{genus_reso};
		my $genusName = $row->{genus_name};
		my $genusNo = $row->{taxon_no};
		my $reid_genus_reso = $row->{reid_genus_reso};
		my $reid_genus_name = $row->{reid_genus_name};
		my $collection_no = $row->{collection_no};

		# count up occurrences per time interval bin
		if ( $q->param('time_scale') )	{
			# only use occurrences from collections that map
			#  into exactly one bin
			if ( $intervallookup{$row->{collection_no}} )	{
				$occsbybin{$intervallookup{$row->{collection_no}}}++;
				$occsbybintaxon{$intervallookup{$row->{collection_no}}}{$genusNo}++;
				if ( $occsbybintaxon{$intervallookup{$row->{collection_no}}}{$genusNo} == 1 )	{
					$taxabybin{$intervallookup{$row->{collection_no}}}++;
				}
			}
			# now things get nasty: if a field was selected to
			#  break up into categories, add to the count involving
			#  the appropriate enum value
			# WARNING: algorithm assumes that only enum fields are
			#  ever selected for processing
			if ( $q->param('binned_field') )	{
				# special processing for ecology data
				if ( $q->param('binned_field') eq "ecology" )	{
					$occsbybinandcategory{$intervallookup{$row->{collection_no}}}{$ecotaph{'1'.$genusNo}}++;
					$occsbybincattaxon{$intervallookup{$row->{collection_no}}}{$ecotaph{'1'.$genusNo}.$genusNo}++;
					if ( $occsbybincattaxon{$intervallookup{$row->{collection_no}}}{$ecotaph{'1'.$genusNo}.$genusNo} == 1 )	{
						$taxabybinandcategory{$intervallookup{$row->{collection_no}}}{$ecotaph{'1'.$genusNo}}++;
					}
					$occsbycategory{$ecotaph{'1'.$genusNo}}++;
				} else	{
				# default processing
					$occsbybinandcategory{$intervallookup{$row->{collection_no}}}{$row->{$q->param('binned_field')}}++;
					$occsbybincattaxon{$intervallookup{$row->{collection_no}}}{$row->{$q->param('binned_field')}.$genusNo}++;
					if ( $occsbybincattaxon{$intervallookup{$row->{collection_no}}}{$row->{$q->param('binned_field')}.$genusNo} == 1 )	{
						$taxabybinandcategory{$intervallookup{$row->{collection_no}}}{$row->{$q->param('binned_field')}}++;
					}
					$occsbycategory{$row->{$q->param('binned_field')}}++;
				}
			}
		}

		# compute relative abundance proportion and add to running total
		# WARNING: sum is of logged abundances because geometric means
		#   are desired
		if ( ( $row->{abund_unit} eq "specimens" || $row->{abund_unit} eq "individuals" ) && ( $row->{abund_value} > 0 ) )	{
			$summedproportions{$row->{genus_name}} = $summedproportions{$row->{genus_name}} + log( $row->{abund_value} / $nisp{$row->{collection_no}} );
		}

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
			# translate bogus period or epoch max into legitimate,
			#   looked-up period or epoch names JA 22.2.04
			# WARNING: this won't work at all if the period_max
			#   and/or epoch_max fields are ever removed from
			#   the database
			} elsif ( $column eq "period_max" )	{
				$row->{$column} = $myperiod{$row->{collection_no}};
			} elsif ( $column eq "epoch_max" )	{
				$row->{$column} = $myepoch{$row->{collection_no}};
			# WARNING: similar trick here in which useless legacy
			#  field locage_max is used as a placeholder for the
			#  bin name
			} elsif ( $column eq "locage_max" )	{
				$row->{$column} = $mybin{$row->{collection_no}};
			}

			push ( @coll_row, $row->{$column} );
		}

		# Push the eco/taphonomic data, if any, onto the reid rows
		# WARNING: this only works on genus or higher-order data,
		#  assuming species won't be scored separately
		if ( $q->param('ecology1') )	{
			push @reid_row , $ecotaph{'1'.$genusNo};
		}
		if ( $q->param('ecology2') )	{
			push @reid_row , $ecotaph{'2'.$genusNo};
		}
		if ( $q->param('ecology3') )	{
			push @reid_row , $ecotaph{'3'.$genusNo};
		}
		if ( $q->param('ecology4') )	{
			push @reid_row , $ecotaph{'4'.$genusNo};
		}
		if ( $q->param('ecology5') )	{
			push @reid_row , $ecotaph{'5'.$genusNo};
		}
		if ( $q->param('ecology6') )	{
			push @reid_row , $ecotaph{'6'.$genusNo};
		}

		if( $q->param('collections_only') ){
			$curLine = $self->formatRow(($collection_no, @occs_row, @coll_row));
		} elsif ( $q->param('distinct_taxa_only') )	{
			$curLine = $self->formatRow(($genusName));
		} else{
			$curLine = $self->formatRow(($collection_no, $genus_reso, $genusName, $reid_genus_reso, $reid_genus_name, @occs_row, @reid_row, @coll_row));
		}
		# get rid of carriage returns 24.5.04 JA
		# failing to do this is lethal and I'm not sure why no-one
		#  alerted me to this bug previously
		$curLine =~ s/\n/ /g;
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

	my $outputType = "occurrences";
	if ( $q->param("collections_only") )	{
		$outputType = "collections";
	}

	# print out a list of genera with total number of occurrences and average relative abundance
	if ( $outputType eq "occurrences" )	{
		my @genera = keys %totaloccs;
		@genera = sort @genera;
		open ABUNDFILE,">$OUT_FILE_DIR/$generaOutFileName";
		print ABUNDFILE "genus\tcollections\twith abundances\tgeometric mean abundance\n";
		for $g ( @genera )	{
			print ABUNDFILE "$g\t$totaloccs{$g}\t";
			printf ABUNDFILE "%d\t",$numberofcounts{$g};
			if ( $numberofcounts{$g} > 0 )	{
				printf ABUNDFILE "%.4f\n",exp($summedproportions{$g} / $numberofcounts{$g});
			} else	{
				print ABUNDFILE "NaN\n";
			}
			$acceptedGenera++;
		}
		close ABUNDFILE;
	}

	# print out a list of time intervals with counts of occurrences
	if ( $q->param('time_scale') )	{
		open SCALEFILE,">$OUT_FILE_DIR/$scaleOutFileName";
		my @intervalnames;

		#  list of 10 m.y. bin names
		if ( $q->param('time_scale') =~ /bin/ )	{
			@intervalnames = ("Cenozoic 6", "Cenozoic 5",
			 "Cenozoic 4", "Cenozoic 3", "Cenozoic 2", "Cenozoic 1",
			 "Cretaceous 8", "Cretaceous 7", "Cretaceous 6",
			 "Cretaceous 5", "Cretaceous 4", "Cretaceous 3",
			 "Cretaceous 2", "Cretaceous 1", "Jurassic 6",
			 "Jurassic 5", "Jurassic 4", "Jurassic 3",
			 "Jurassic 2", "Jurassic 1", "Triassic 5",
			 "Triassic 4", "Triassic 3", "Triassic 2",
			 "Triassic 1", "Permian 5", "Permian 4",
			 "Permian 3", "Permian 2", "Permian 1",
			 "Carboniferous 5", "Carboniferous 4",
			 "Carboniferous 3", "Carboniferous 2",
			 "Carboniferous 1", "Devonian 5",
			 "Devonian 4", "Devonian 3", "Devonian 2",
			 "Devonian 1", "Silurian 2", "Silurian 1",
			 "Ordovician 5", "Ordovician 4", "Ordovician 3",
			 "Ordovician 2", "Ordovician 1",
			 "Cambrian 4", "Cambrian 3", "Cambrian 2",
			 "Cambrian 1" );
		} else	{
		# or we need a list of interval names in the order they appear
		#   in the scale, which is stored in the correlations table
			my $sql = "SELECT intervals.eml_interval,intervals.interval_name,intervals.interval_no,correlations.correlation_no FROM intervals,correlations WHERE intervals.interval_no=correlations.interval_no AND correlations.scale_no=" . $q->param('time_scale') . " ORDER BY correlations.correlation_no";
			my @intervalrefs = @{$dbt->getData($sql)};
			for my $ir ( @intervalrefs )	{
				my $iname = $ir->{interval_name};
				if ( $ir->{eml_interval} ne "" )	{
					$iname = $ir->{eml_interval} . " " . $iname;
				}
				push @intervalnames, $iname;
			}
		}

		# need a list of enum values that actually have counts
		# NOTE: we're only using occsbycategory to generate this list,
		#  but it is kind of cute
		@enumvals = keys %occsbycategory;
		@enumvals = sort @enumvals;

		# now print the results
		print SCALEFILE "interval";
		print SCALEFILE "\tlower boundary\tupper boundary\tmidpoint";
		print SCALEFILE "\ttotal occurrences";
		print SCALEFILE "\ttotal genera";
		for my $val ( @enumvals )	{
			if ( $val eq "" )	{
				print SCALEFILE "\tno data occurrences";
				print SCALEFILE "\tno data taxa";
			} else	{
				print SCALEFILE "\t$val occurrences";
				print SCALEFILE "\t$val taxa";
			}
		}
		for my $val ( @enumvals )	{
			if ( $val eq "" )	{
				print SCALEFILE "\tproportion no data occurrences";
				print SCALEFILE "\tproportion no data taxa";
			} else	{
				print SCALEFILE "\tproportion $val occurrences";
				print SCALEFILE "\tproportion $val taxa";
			}
		}
		print SCALEFILE "\n";

		for my $intervalName ( @intervalnames )	{
			$acceptedIntervals++;
			print SCALEFILE "$intervalName";
			printf SCALEFILE "\t%.2f",$lowerbinbound{$intervalName};
			printf SCALEFILE "\t%.2f",$upperbinbound{$intervalName};
			printf SCALEFILE "\t%.2f",($lowerbinbound{$intervalName} + $upperbinbound{$intervalName}) / 2;
			printf SCALEFILE "\t%d",$occsbybin{$intervalName};
			printf SCALEFILE "\t%d",$taxabybin{$intervalName};
			for my $val ( @enumvals )	{
				printf SCALEFILE "\t%d",$occsbybinandcategory{$intervalName}{$val};
				printf SCALEFILE "\t%d",$taxabybinandcategory{$intervalName}{$val};
			}
			for my $val ( @enumvals )	{
				if ( $occsbybinandcategory{$intervalName}{$val} eq "" )	{
					print SCALEFILE "\t0.0000";
					print SCALEFILE "\t0.0000";
				} else	{
					printf SCALEFILE "\t%.4f",$occsbybinandcategory{$intervalName}{$val} / $occsbybin{$intervalName};
					printf SCALEFILE "\t%.4f",$taxabybinandcategory{$intervalName}{$val} / $taxabybin{$intervalName};
				}
			}
			print SCALEFILE "\n";
		}
		close SCALEFILE;
	}

	# Tell what happened
	if ( ! $acceptedCount ) { $acceptedCount = 0; }
	if ( ! $acceptedRefs ) { $acceptedRefs = 0; }
	print "
<table border='0' width='600'>
<tr><td class='darkList'><b><font size='+1'>Output files</font></b></td></tr>
<tr><td>$acceptedCount $outputType were printed to <a href='$OUT_HTTP_DIR/$occsOutFileName'>$occsOutFileName</a></td></tr>\n";
	if ( $outputType eq "occurrences" )	{
		print "
<tr><td>$acceptedGenera genus names were printed to <a href='$OUT_HTTP_DIR/$generaOutFileName'>$generaOutFileName</a></td></tr>\n";
	}
	print "
<tr><td>$acceptedRefs references were printed to <a href='$OUT_HTTP_DIR/$refsOutFileName'>$refsOutFileName</a></td></tr>\n";
	if ( $q->param('time_scale') )	{
		print "
<tr><td>$acceptedIntervals time intervals were printed to <a href=\"$OUT_HTTP_DIR/$scaleOutFileName\">$scaleOutFileName</a></td></tr>\n";
	}
print "</table>
<p align='center'><b><a href='?action=displayDownloadForm'>Do another download</a></b>
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
	$generaOutFileName = $authorizer . "-genera.$outFileExtension";
	if ( $q->param("collections_only") )	{
		$occsOutFileName = $authorizer . "-cols.$outFileExtension";
	}
	$refsOutFileName = $authorizer . "-refs.$outFileExtension";
	if ( $q->param('time_scale') )	{
		$scaleOutFileName = $authorizer . "-scale.$outFileExtension";
	}

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

# JA Paul replaced taxonomic_search call with recurse call because it's faster,
#  but I'm reverting because I'm not maintaining recurse
sub getGenusNames {
	my $self = shift;
	my $genus_name = (shift || "");

	my $cslist = PBDBUtil::taxonomic_search($genus_name, $dbt);
	#my $cslist = `./recurse $genus_name`;
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
	my $i;
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
