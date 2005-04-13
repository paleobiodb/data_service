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
my @errors;           # Possible errors in user input
$|=1;

# These arrays contain names of possible fields to be checked by a user in the
# download form.  When writing the data out to files, these arrays are compared
# to the query params to determine the file header line and then the data to
# be written out. 
my @collectionsFieldNames = qw(authorizer enterer modifier collection_no collection_subset reference_no collection_name collection_aka country state county latdeg latmin latsec latdir latdec lngdeg lngmin lngsec lngdir lngdec latlng_basis paleolatdeg paleolatmin paleolatsec paleolatdir paleolatdec paleolngdeg paleolngmin paleolngsec paleolngdir paleolngdec altitude_value altitude_unit geogscale geogcomments period epoch 10mybin max_interval_no min_interval_no emlperiod_max period_max emlperiod_min period_min emlepoch_max epoch_max emlepoch_min epoch_min emlintage_max intage_max emlintage_min intage_min emllocage_max locage_max emllocage_min locage_min zone research_group geological_group formation member localsection localbed localorder regionalsection regionalbed regionalorder stratscale stratcomments lithdescript lithadj lithification lithology1 fossilsfrom1 lithology2 fossilsfrom2 environment tectonic_setting pres_mode geology_comments collection_type collection_coverage collection_meth collection_size collection_size_unit museum collection_comments taxonomy_comments created modified release_date access_level lithification2 lithadj2 rock_censused_unit rock_censused spatial_resolution temporal_resolution feed_pred_traces encrustation bioerosion fragmentation sorting dissassoc_minor_elems dissassoc_maj_elems art_whole_bodies disart_assoc_maj_elems seq_strat lagerstatten concentration orientation preservation_quality abund_in_sediment sieve_size_min sieve_size_max assembl_comps taphonomy_comments);
my @occurrencesFieldNames = qw(authorizer enterer modifier occurrence_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name abund_value abund_unit reference_no comments created modified plant_organ plant_organ2);
my @reidentificationsFieldNames = qw(authorizer enterer modifier reid_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name reference_no comments created modified modified_temp plant_organ);
my @refsFieldNames = qw(authorizer enterer modifier reference_no author1init author1last author2init author2last otherauthors pubyr reftitle pubtitle pubvol pubno firstpage lastpage created modified publication_type comments project_name project_ref_no);
my @paleozoic = qw(cambrian ordovician silurian devonian carboniferous permian);
my @mesoCenozoic = qw(triassic jurassic cretaceous tertiary);

my %ecotaph = ();

my $csv;
my $OUT_HTTP_DIR = "/paleodb/data";
my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};
my $DATAFILE_DIR = $ENV{DOWNLOAD_DATAFILE_DIR};
my $outFileBaseName;

my $bestbothscale;

sub new {
	my $class = shift;
	$dbh = shift;
	$dbt = shift;
	$q = shift;
	$s = shift;
    $hbo = shift;
	my $self = {'dbh'=>$dbh,'dbt'=>$dbt,'q'=>$q,'s'=>$s,'hbo'=>$hbo};

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
    
	if ( $q->param('compendium_ranges') eq 'NO' )	{
		$self->getCompendiumAgeRanges ( );
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
	$html .= $self->retellOptionsRow ( "Output data type", $q->param("output_data") );
	$html .= $self->retellOptionsRow ( "Output data format", $q->param("output_format") );
	$html .= $self->retellOptionsRow ( "Research group or project", $q->param("research_group") );
    
	# added by rjp on 12/30/2003
	if ($q->param('year')) {
		my $dataCreatedBeforeAfter = $q->param("created_before_after") . " " . $q->param("date") . " " . $q->param("month") . " " . $q->param("year");
		$html .= $self->retellOptionsRow ( "Data records created", $dataCreatedBeforeAfter);
	}
	
	# JA 31.8.04
	if ($q->param('pubyr')) {
		my $dataPublishedBeforeAfter = $q->param("published_before_after") . " " . $q->param("pubyr");
		$html .= $self->retellOptionsRow ( "Data records published", $dataPublishedBeforeAfter);
	}
	if ( $q->param("taxon_name") !~ /[ ,]/ )	{
		$html .= $self->retellOptionsRow ( "Taxon name", $q->param("taxon_name") );
	} else	{
		$html .= $self->retellOptionsRow ( "Taxon names", $q->param("taxon_name") );
	}
	$html .= $self->retellOptionsRow ( "Class", $q->param("class") );

    if ($q->param("max_interval_name")) {
    	$html .= $self->retellOptionsRow ( "Oldest interval", $q->param("max_eml_interval") . " " . $q->param("max_interval_name") );
    }
    if ($q->param("min_interval_name")) { 
	    $html .= $self->retellOptionsRow ( "Youngest interval", $q->param("min_eml_interval") . " " .$q->param("min_interval_name") );
    }

    if ( ! $q->param("lithification_lithified") || ! $q->param("lithification_poorly_lithified") || ! $q->param("lithification_unlithified") || ! $q->param("lithification_unknown")) {
	my $lithifs;
	if ( $q->param("lithification_lithified") )	{
		$lithifs .= ", lithified";
	}
	if ( $q->param("lithification_poorly_lithified") )	{
		$lithifs .= ", poorly lithified";
	}
	if ( $q->param("lithification_unlithified") )	{
		$lithifs .= ", unlithified";
	}
	if ( $q->param("lithification_unknown") )	{
		$lithifs .= ", unknown";
	}
	$lithifs =~ s/^, //;
	$html .= $self->retellOptionsRow ( "Lithification", $lithifs );
    }

    # Lithologies or lithology
    if ( $q->param("lithology1") ) {
        $html .= $self->retellOptionsRow("Lithology: ", $q->param("include_exclude_lithology1") . " " .$q->param("lithology1"));
    } else {
        my $liths;
        if ( ! $q->param("lithology_carbonate") || ! $q->param("lithology_mixed") ||
             ! $q->param("lithology_siliciclastic") || ! $q->param("lithology_unknown") ){
            my $liths;
            if ( $q->param("lithology_carbonate") )	{
                $liths = "carbonate";
            }
            if ( $q->param("lithology_mixed") )	{
                $liths .= ", mixed";
            }
            if ( $q->param("lithology_siliciclastic") )	{
                $liths .= ", siliciclastic";
            }
            if ( $q->param("lithology_unknown") ) {
                $liths .= ", unknown";
            }    
            $liths =~ s/^, //;
            $html .= $self->retellOptionsRow ( "Lithologies", $liths );
        }
    }

    # Environment or environments
    if ( $q->param('environment') ) {
    	$html .= $self->retellOptionsRow ( "Environment", $q->param("include_exclude_environment") . " " .$q->param("environment") );
    } else {
        if (! $q->param("environment_carbonate") || ! $q->param("environment_unknown") ||
            ! $q->param("environment_siliciclastic") || ! $q->param("environment_terrestrial")) {
            my $envs;
            if ( $q->param("environment_carbonate") ) {
                $envs .= "carbonate";
            }
            if ( $q->param("environment_siliciclastic") ) {
                $envs .= ", siliciclastic";
            }
            if ( $q->param("environment_terrestrial") ) {
                $envs .= ", terrestrial";
            }
            if ( $q->param("environment_unknown") ) {
                $envs .= ", unknown";
            }    
            $envs =~ s/^,//;
            $html .= $self->retellOptionsRow( "Environments", $envs);
        }
    }    

    # Collection types
    if (! $q->param('collection_type_archaeological') || ! $q->param('collection_type_biostratigraphic') ||
        ! $q->param('collection_type_paleoecologic') || ! $q->param('collection_type_taphonomic') || 
        ! $q->param('collection_type_taxonomic') || ! $q->param('collection_type_general_faunal/floral') ||
        ! $q->param('collection_type_unknown')) {
        my $colltypes;
        if ($q->param('collection_type_archaeological')) {
            $colltypes .= "archaeological";
        }
        if ($q->param('collection_type_biostratigraphic')) {
            $colltypes .= ", biostratigraphic";
        }
        if ($q->param('collection_type_paleoecologic')) {
            $colltypes .= ", paleoecologic";
        }
        if ($q->param('collection_type_taphonomic')) {
            $colltypes .= ", taphonomic";
        }
        if ($q->param('collection_type_taxonomic')) {
            $colltypes .= ", taxonomic";
        }
        if ($q->param('collection_type_general_faunal/floral')) {
            $colltypes .= ", general faunal/floral";
        }
        if ($q->param('collection_type_unknown')) {
            $colltypes .= ", unknown";
        }
        $colltypes =~ s/^,//;
        $plural = ($colltypes =~ /,/) ? "s":"";
        $html .= $self->retellOptionsRow( "Collections included for reason$plural",$colltypes);
    }
    

	# Continents or country
	my @continents = ( );
	# If a country was selected, ignore the continents JA 6.7.02
	if ( $q->param("country") )	{
		$html .= $self->retellOptionsRow ( "Country", $q->param("include_exclude_country") . " " . $q->param("country") );
	}
	else	{
		if ( $q->param("global") ) 			{ push ( @continents, "global" ); }
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

	if ( $q->param("paleolatmin") > -90 && $q->param("paleolatmin") < 90 )	{
		$html .= $self->retellOptionsRow ( "Minimum paleolatitude", $q->param("paleolatmin") . "&deg;" );
	}
	if ( $q->param("paleolatmax") > -90 && $q->param("paleolatmax") < 90 )	{
		$html .= $self->retellOptionsRow ( "Maximum paleolatitude", $q->param("paleolatmax") . "&deg;" );
	}
	if ( $q->param("paleolngmin") > -180 && $q->param("paleolngmin") < 180 )	{
		$html .= $self->retellOptionsRow ( "Minimum paleolongitude", $q->param("paleolngmin") . "&deg;" );
	}
	if ( $q->param("paleolngmax") > -180 && $q->param("paleolngmax") < 180 )	{
		$html .= $self->retellOptionsRow ( "Maximum paleolongitude", $q->param("paleolngmax") . "&deg;" );
	}
	
	$html .= $self->retellOptionsRow ( "Lump lists of same county & formation?", $q->param("lumplist") );

    if (! $q->param('geogscale_small_collection') || ! $q->param('geogscale_hand_sample') || ! $q->param('geogscale_outcrop') || ! $q->param('geogscale_local_area') ||
        ! $q->param('geogscale_basin') || ! $q->param('geogscale_unknown')) { 
        my $geogscales;
        if ( $q->param('geogscale_hand_sample') )	{
            $geogscales = "hand sample";
        }
        if ( $q->param('geogscale_small_collection') )	{
            $geogscales = "small collection";
        }
        if ( $q->param('geogscale_outcrop') )	{
            $geogscales .= ", outcrop";
        }
        if ( $q->param('geogscale_local_area') )	{
            $geogscales .= ", local area";
        }
        if ( $q->param('geogscale_basin') )	{
            $geogscales .= ", basin";
        }
        if ( $q->param('geogscale_unknown')) {
            $geogscales .= ", unknown";
        }    
        $geogscales =~ s/^,//;

        $html .= $self->retellOptionsRow ( "Geographic scale of collections", $geogscales );
    }

    if (! $q->param('stratscale_bed') || ! $q->param('stratscale_group_of_beds') || ! $q->param('stratscale_member') ||
        ! $q->param('stratscale_formation') || ! $q->param('stratscale_group') || ! $q->param('stratscale_unknown')) {
        my $stratscales;
        if ( $q->param('stratscale_bed') )	{
            $stratscales = "bed";
        }
        if ( $q->param('stratscale_group_of_beds') )	{
            $stratscales .= ", group of beds";
        }
        if ( $q->param('stratscale_member') )	{
            $stratscales .= ", member";
        }
        if ( $q->param('stratscale_formation') )	{
            $stratscales .= ", formation";
        }
        if ( $q->param('stratscale_group') )	{
            $stratscales .= ", group";
        }
        if ( $q->param('stratscale_unknown') ) {
            $stratscales .= ", unknown";
        }
        $stratscales =~ s/^,//;

        $html .= $self->retellOptionsRow ( "Stratigraphic scale of collections", $stratscales );
    }

	$html .= $self->retellOptionsRow ( "Lump by exact geographic coordinate?", $q->param("lump_by_coord") );
	$html .= $self->retellOptionsRow ( "Lump by formation and member?", $q->param("lump_by_mbr") );
	$html .= $self->retellOptionsRow ( "Lump by published reference?", $q->param("lump_by_ref") );
	$html .= $self->retellOptionsRow ( "Lump by time interval?", $q->param("lump_by_interval") );

    if ($q->param('output_data') ne 'collections') {
        $html .= $self->retellOptionsRow ( "Lump occurrences of same genus of same collection?", $q->param("lumpgenera") );
        $html .= $self->retellOptionsRow ( "Replace genus names with subgenus names?", $q->param("split_subgenera") );
        $html .= $self->retellOptionsRow ( "Include occurrences that are generically indeterminate?", $q->param("indet") );
        $html .= $self->retellOptionsRow ( "Include occurrences that are specifically indeterminate?", $q->param("sp") );
        $html .= $self->retellOptionsRow ( "Include occurrences qualified by \"aff.\" or quotes?", $q->param("poor_genus_reso") );
        $html .= $self->retellOptionsRow ( "Include occurrences with informal names?", $q->param("informal") );
        $html .= $self->retellOptionsRow ( "Include occurrences falling outside Compendium age ranges?", $q->param("compendium_ranges") );
        $html .= $self->retellOptionsRow ( "Include occurrences without abundance data?", $q->param("without_abundance") );

        if ($q->param('output_data') eq "occurrences") {
            my $occFields = ();
            push (@occFields, 'class_name') if ($q->param("occurrences_class_name") eq "YES");
            push (@occFields, 'order_name') if ($q->param("occurrences_order_name") eq "YES");
            push (@occFields, 'family_name') if ($q->param("occurrences_family_name") eq "YES");
            push (@occFields, 'genus_reso','genus_name');
            foreach my $field ( @occurrencesFieldNames ) {
                if( $q->param ( "occurrences_".$field ) ){ 
                    push ( @occFields, "occurrences_".$field ); 
                }
            }
            push(@occFields,"reidentifications_genus_name" );
            foreach my $field ( @reidentificationsFieldNames ) {
                if( $q->param ( "occurrences_".$field) ){ 
                    push ( @occFields, "reidentifications_".$field ); 
                }
            }
            $html .= $self->retellOptionsRow ( "Occurrence output fields", join ( "<BR>", @occFields) );
        }
    } 

    if ($q->param('output_data') eq 'genera') {
	    $html .= $self->retellOptionsRow ( "Collection output fields", "genus_name");
    } else {
        my @collFields = ( "collection_no");
        foreach my $field ( @collectionsFieldNames ) {
            if ( $q->param ( 'collections_'.$field ) ) { push ( @collFields, 'collections_'.$field ); }
        }
	    $html .= $self->retellOptionsRow ( "Collection output fields", join ( "<BR>", @collFields) );
    }

	$html .= "\n</table>\n";

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
    my $isSQL = shift;
	
	my @fieldNames;
	
	if($tableName eq "collections") {
        if ($isSQL) {
            # These fieldnames are created virtually, not from the DB
	        @fieldNames = grep {!/^(paleo(lat|lng)(deg|dec|min|sec|dir)|epoch|period|10mybin)$/} @collectionsFieldNames;
        } else {
	        @fieldNames = @collectionsFieldNames;
        }
	} elsif($tableName eq "occurrences") {
        @fieldNames = @occurrencesFieldNames;
	} elsif($tableName eq "reidentifications") {
        @fieldNames = @reidentificationsFieldNames;
    } else {
		$self->dbg("getOutFields(): Unknown table [$tableName]");
	}
	
	my @outFields = ( );
	foreach my $fieldName ( @fieldNames ) {
		# use brackets below because the underscore is a valid char
		if ( $q->param("${tableName}_${fieldName}") eq "YES") {
            # Rename field to avoid conflicts with fieldnames in collections/occ table
			if($tableName eq 'reidentifications'){
                if ($isSQL) {
				    push(@outFields, "$tableName.$fieldName as reid_$fieldName");
                } else {
                    push(@outFields, "$tableName.$fieldName");
                }
			}
            # Rename field to avoid conflicts with fieldnames in collections table
			elsif ($tableName eq 'occurrences') {
                if ($isSQL) {
				    push(@outFields, "$tableName.$fieldName as occ_$fieldName");
                } else {
				    push(@outFields, "$tableName.$fieldName");
                }
			} else { 
                push(@outFields,"$tableName.$fieldName");
            }
		}
	}

    # Need these fields to perform various computations
    # So make sure they included in the SQL query, but not necessarily  included
    # into a CSV header.
    my %fieldExists;
    @fieldExists{@outFields} = ();
    if ($isSQL && $tableName eq "collections") {
        my %impliedFields;
        %impliedFields = (
            'collections_paleolat'=>['paleolat'],
            'collections_paleolng'=>['paleolng'],
            'collections_lat'=>['latdeg', 'latmin', 'latsec', 'latdec', 'latdir'],
            'collections_lng'=>['lngdeg', 'lngmin', 'lngsec', 'lngdec', 'lngdir'],
            'lump_by_coord'=>['latdeg','latmin','latsec','latdec','latdir',
                            'lngdeg','lngmin','lngsec','lngdec','lngdir'],
            'lump_by_interval'=>['max_interval_no','min_interval_no'],
            'lump_by_mbr'=>['formation','member'],
            'lump_by_ref'=>['reference_no']);

        foreach $key (keys %impliedFields) {
            if ($q->param($key) eq "YES") {
                foreach $field (@{$impliedFields{$key}}) {
                    if (!exists $fieldExists{$field}) {
                        push @outFields, "$tableName.$field";
                        $fieldExists{$field} = 1;
                    }    
                }    
            }
        }  
        if (!exists $fieldExists{'reference_no'}) {
            push @outFields, "$tableName.reference_no";
        }
    } elsif ($isSQL && $tableName eq "occurrences") {    
		# subgenus name must be downloaded if subgenera are to be
		#  treated as genera
		# an amusing hack, if I do say so myself JA 18.8.04
		if ( $q->param('split_subgenera') eq 'YES' && !exists $fieldExists{'subgenus_name'} )	{
            push @outFields, "$tableName.subgenus_name as occ_subgenus_name";
		}
        if (!exists $fieldExists{'reference_no'}) {
            push @outFields, "$tableName.reference_no as occ_reference_no";
        }
    }    
	return @outFields;
}

# Wrapper to getOutFields() for returning all table rows in a single string.
sub getOutFieldsString {
	my $self = shift;
	my $tableName = shift;
    my $isSQL = shift;

	my $outFieldsString = join ( ",\n", $self->getOutFields($tableName,$isSQL) );
	return $outFieldsString;
}

# Returns research group
sub getResGrpString {
	my $self = shift;
	my $result = "";

	my $resgrp = $q->param('research_group');

	if($resgrp && $resgrp =~ /(^decapod$)|(^EJECT$)|(^ETE$)|(^5%$)|(^1%$)|(^PACED$)|(^PGAP$)/){
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
    my $country_sql = "";
	my $in_str = "";

    #Country or Countries
    if ($q->param('country')) {
        $country_term = $dbh->quote($q->param('country'));
        if ($q->param('include_exclude_country') eq "exclude") {
            $country_sql = qq| collections.country NOT LIKE $country_term |;
        } else {
            $country_sql = qq| collections.country LIKE $country_term |;
        }
    } else {     
        # Get the regions
        if ( ! open ( REGIONS, "$DATAFILE_DIR/PBDB.regions" ) ) {
            print "<font color='red'>Skipping regions.</font> Error message is $!<BR><BR>\n";
            return;
        }

        while (<REGIONS>)
        {
            chomp();
            my ($region, $countries) = split(/:/, $_, 2);
            $countries =~ s/'/\\'/g;
            $REGIONS{$region} = $countries;
        }
        # Add the countries within selected regions
        my @regions = (	"North America", 
                        "South America", 
                        Europe, 
                        Africa,
                        Antarctica, 
                        Asia, 
                        Australia );

        foreach my $region (@regions) {
            if($q->param($region) eq 'YES') {
                $in_str = $in_str .','. "'".join("','", split(/\t/,$REGIONS{$region}))."'";
            }
            $in_str =~ s/^,//; 
        }
        if ($in_str) {
            $country_sql = qq| collections.country IN ($in_str) |;
        } else {
            $country_sql = "";
        }    
    }
	return $country_sql;
}

# 12/13/2004 PS 
sub getPaleoLatLongString	{
	my $self = shift;
	
    my $coord_sql = "";

	# all the boundaries must be given
	if ( $q->param('paleolatmin') eq "" || $q->param('paleolatmax') eq "" || 
	     $q->param('paleolngmin') eq "" || $q->param('paleolngmax') eq "") {
		return "";
	}
	# at least one of the boundaries must be non-trivial
	if ( $q->param('paleolatmin') <= -90 && $q->param('paleolatmax') >= 90 && 
	     $q->param('paleolngmin') <= -180 && $q->param('paleolngmax') >= 180) {
		return "";
	}

    if ($q->param('paleolatmin') > -90 ) {
        $coord_sql .= " AND paleolat >= ".$dbh->quote($q->param('paleolatmin'));
    }
    if ($q->param('paleolatmax') < 90 ) {
        $coord_sql .= " AND paleolat <= ".$dbh->quote($q->param('paleolatmax'));
    }
    if ($q->param('paleolngmin') > -180 ) {
        $coord_sql .= " AND paleolng >= ".$dbh->quote($q->param('paleolngmin'));
    }
    if ($q->param('paleolngmax') < 180 ) {
        $coord_sql .= " AND paleolng <= ".$dbh->quote($q->param('paleolngmax'));
    }
    $coord_sql =~ s/ AND //;
    return $coord_sql;
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
	if ( $latmin <= -90 && $latmax >= 90 && $lngmin <= -180 && $lngmax >= 180 )	{
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
		$latlongclause .= " ( ( lngdeg<$abslngmax AND lngdir='East' ) OR lngdir='West' ) ";
	} else	{
		$latlongclause .= " ( lngdeg>=$abslngmax AND lngdir='West' ) ";
	}

	$latlongclause .= " ) ";

	return $latlongclause;
}

sub getIntervalString	{
	my $self = shift;
	my $max = ($q->param('max_interval_name') || "");
	my $min = ($q->param('min_interval_name') || "");
    my $eml_max  = ($q->param("max_eml_interval") || "");  
    my $eml_min  = ($q->param("min_eml_interval") || "");  

	my $collref;

	# return immediately if the user already selected a full time scale
	#  to bin the data
	if ( $q->param('time_scale') )	{
		return "";
	}

	if ( $max )	{
		($collref,$bestbothscale) = TimeLookup::processLookup($dbh, $dbt, $eml_max, $max, $eml_min, $min);
		my @colls = @{$collref};
		if ( @colls )	{
		    return " ( collections.collection_no IN ( " . join (',',@colls) . " ) )";
		} else {
            return " ( collections.collection_no IN (0) )";
        }
	}

    

	return "";
}

# Returns a hash reference filled with all interval names
sub getIntervalNames {
    my $self = shift;
    my %interval_names;

	# get the names of time intervals
    my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
    my @intnorefs = @{$dbt->getData($sql)};
    for $intnoref ( @intnorefs )	{
        if ( $intnoref->{eml_interval} )	{
            $interval_names{$intnoref->{interval_no}} = $intnoref->{eml_interval} . " " . $intnoref->{interval_name};
        } else	{
            $interval_names{$intnoref->{interval_no}} = $intnoref->{interval_name};
        }
    }
    return \%interval_names;
}

# Querys the database for a single interval name
# returns the interval_eml and interval_name, if it can
sub getIntervalName {
    my $self = shift;
    my $interval_no = shift;

    my ($interval_name, $interval_eml);

	# get the names of time intervals
    my $sql = "SELECT eml_interval,interval_name FROM intervals";
    $sql .= " WHERE interval_no=$interval_no";
    my @intnorefs = @{$dbt->getData($sql)};
    if (scalar(@intnorefs) > 0) {
        my $href = $intnorefs[0];
        $interval_name = $href->{'interval_name'};
        $interval_eml = $href->{'eml_interval'};
    }
    return ($interval_eml, $interval_name);
}

# JA 11.4.05
sub getLithificationString	{
	my $self = shift;
	my $lithified = $q->param('lithification_lithified');
	my $poorly_lithified = $q->param('lithification_poorly_lithified');
	my $unlithified = $q->param('lithification_unlithified');
	my $unknown = $q->param('lithification_unknown');
	my $lithif_sql = "";
	my $lithvals = "";
        # if all the boxes were unchecked, just return (idiot proofing)
        if ( ! $lithified && ! $poorly_lithified && ! $unlithified && ! $unknown )	{
            return "";
        }
	# all other combinations
	if ( $lithified )	{
		$lithvals = " collections.lithification='lithified' ";
	}
	if ( $poorly_lithified )	{
		$lithvals = " OR collections.lithification='poorly lithified' ";
	}
	if ( $unlithified )	{
		$lithvals .= " OR collections.lithification='unlithified' ";
	}
	if ( $unknown )	{
		$lithvals .= " OR collections.lithification='' OR collections.lithification IS NULL ";
	}
	$lithvals =~ s/^ OR//;
	$lithif_sql = qq| ( $lithvals ) |;

	return $lithif_sql;
}


# JA 1.7.04
# WARNING: relies on fixed lists of lithologies; if these ever change in
#  the database, this section will need to be modified
# major rewrite JA 14.8.04 to handle checkboxes instead of pulldown
sub getLithologyString	{
	my $self = shift;
	my $carbonate = $q->param('lithology_carbonate');
	my $mixed = $q->param('lithology_mixed');
	my $silic = $q->param('lithology_siliciclastic');
    my $unknown = $q->param('lithology_unknown');
    my $lith_sql = "";

    # Lithology or Lithologies
    if ( $q->param('lithology1') ) {
        my $lith_term = $dbh->quote($q->param('lithology1'));
        if ($q->param('include_exclude_lithology1') eq "exclude") {
		    return qq| (collections.lithology1 NOT LIKE $lith_term OR collections.lithology1 IS NULL) AND|.
                   qq| (collections.lithology2 NOT LIKE $lith_term OR collections.lithology2 IS NULL)|;
        } else {
		    return qq| (collections.lithology1 LIKE $lith_term OR collections.lithology2 LIKE $lith_term) |;
        }
    } else {
        # if all the boxes were unchecked, just return (idiot proofing)
        if ( ! $carbonate && ! $mixed && ! $silic && ! $unknown)	{
            return "";
        }
        # only do something if some of the boxes aren't checked
        if  ( ! $carbonate || ! $mixed || ! $silic || ! $unknown)	{

            my $silic_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_siliciclastic'}});
            my $mixed_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_mixed'}});
            my $carbonate_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_carbonate'}});
            
            # the logic is basically different for every combination,
            #  so go through all of them
            # carbonate only
            if  ( $carbonate && ! $mixed && ! $silic )	{
                $lith_sql =  qq| ( collections.lithology1 IN ($carbonate_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($carbonate_str) ) ) |;
            }
            # mixed only
            elsif  ( ! $carbonate && $mixed && ! $silic )	{
                $lith_sql = qq| ( collections.lithology1 IN ($mixed_str) OR collections.lithology2 IN ($mixed_str) OR ( collections.lithology1 IN ($carbonate_str) && collections.lithology2 IN ($silic_str) ) OR ( collections.lithology1 IN ($silic_str) && collections.lithology2 IN ($carbonate_str) ) ) |;
            }
            # siliciclastic only
            elsif  ( ! $carbonate && ! $mixed && $silic )	{
                $lith_sql = qq| ( collections.lithology1 IN ($silic_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($silic_str) ) ) |;
            }
            # carbonate and siliciclastic but NOT mixed
            elsif  ( $carbonate && ! $mixed && $silic )	{
                $lith_sql = qq| ( ( collections.lithology1 IN ($carbonate_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($carbonate_str) ) ) OR ( collections.lithology1 IN ($silic_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($silic_str) ) ) ) |;
            }
            # carbonate and mixed
            elsif  ( $carbonate && $mixed && ! $silic )	{
                $lith_sql = qq| ( ( collections.lithology1 IN ($mixed_str) OR collections.lithology2 IN ($mixed_str) OR ( collections.lithology1 IN ($carbonate_str) && collections.lithology2 IN ($silic_str) ) OR ( collections.lithology1 IN ($silic_str) && collections.lithology2 IN ($carbonate_str) ) ) OR ( collections.lithology1 IN ($carbonate_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($carbonate_str) ) ) ) |;
            }
            # mixed and siliciclastic
            elsif  ( ! $carbonate && $mixed && $silic )	{
                $lith_sql = qq| ( ( collections.lithology1 IN ($mixed_str) OR collections.lithology2 IN ($mixed_str) OR ( collections.lithology1 IN ($carbonate_str) && collections.lithology2 IN ($silic_str) ) OR ( collections.lithology1 IN ($silic_str) && collections.lithology2 IN ($carbonate_str) ) ) OR ( collections.lithology1 IN ($silic_str) AND ( collections.lithology2 IS NULL OR collections.lithology2='' OR collections.lithology2 IN ($silic_str) ) ) ) |;
            }
            
            # lithologies where both fields are null
            if ($unknown) {
                my $unknown_sql = qq|((collections.lithology1 IS NULL OR collections.lithology1='') AND (collections.lithology2 IS NULL OR collections.lithology2=''))|;
                if ($lith_sql) {
                    $lith_sql = "($lith_sql OR $unknown_sql)";
                } else {
                    $lith_sql = "$unknown_sql";
                }    

            }
        }
    }

	return $lith_sql;
}

# A bad thing about this, and the lithology checkboxes: 
# they don't add up right now, there are environment/lithologies that don't go into any
# of the categories, so the 4 checkboxes checked individually don't add up to all four check boxes checked (fetch all).
sub getEnvironmentString{
	my $self = shift;
    my $env_sql = '';

    # Environment or environments
    if ( $q->param('environment') ) {
        # Maybe this is redundant, but for consistency sake
        my $environment;
        if ($q->param('environment') =~ /General/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_general'}});
        } elsif ($q->param('environment') =~ /Terrestrial/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_terrestrial'}});
        } elsif ($q->param('environment') =~ /Siliciclastic/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_siliciclastic'}});
        } elsif ($q->param('environment') =~ /Carbonate/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_carbonate'}});
        } else {
            $environment = $dbh->quote($q->param('environment'));
        }

        if ($q->param('include_exclude_environment') eq "exclude") {
		    return qq| (collections.environment NOT IN ($environment) OR collections.environment IS NULL) |;
        } else {
		    return qq| collections.environment IN ($environment)|;
        }
    } else {
        my $carbonate_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_carbonate'}});
        my $siliciclastic_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_siliciclastic'}});
        my $terrestrial_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_terrestrial'}});
        if (! $q->param("environment_carbonate") || ! $q->param("environment_siliciclastic") || 
            ! $q->param("environment_terrestrial")) {
            if ( $q->param("environment_carbonate") ) {
                $env_sql .= " OR collections.environment IN ($carbonate_str)";
            }
            if ( $q->param("environment_siliciclastic") ) {
                $env_sql .= " OR collections.environment IN ($siliciclastic_str)";
            }
            if ( $q->param("environment_carbonate") && $q->param("environment_siliciclastic") )	{
		$env_sql .= " OR collections.environment IN ('marine indet.')";
            }
            if ( $q->param("environment_terrestrial") ) {
                $env_sql .= " OR collections.environment IN ($terrestrial_str)";
            }
            if ( $q->param("environment_unknown")) {
                $env_sql .= " OR collections.environment = '' OR collections.environment IS NULL"; 
            }
            $env_sql =~ s/^ OR//;
            $env_sql = '('.$env_sql.')';
        }
   }

	return $env_sql;
}

sub getGeogscaleString{
	my $self = shift;
	my $geogscales = "";
    if (! $q->param('geogscale_small_collection') || !$q->param('geogscale_hand_sample') || ! $q->param('geogscale_outcrop') || ! $q->param('geogscale_local_area') ||
        ! $q->param('geogscale_basin') || ! $q->param('geogscale_unknown')) { 
        if ( $q->param('geogscale_hand_sample') )	{
            $geogscales = "'hand sample'";
        }
        if ( $q->param('geogscale_small_collection') )	{
            $geogscales = ",'small collection'";
        }
        if ( $q->param('geogscale_outcrop') )	{
            $geogscales .= ",'outcrop'";
        }
        if ( $q->param('geogscale_local_area') )	{
            $geogscales .= ",'local area'";
        }
        if ( $q->param('geogscale_basin') )	{
            $geogscales .= ",'basin'";
        }
        if ( $q->param('geogscale_unknown') ) {
            $geogscales .= ",''";
        }    
        $geogscales =~ s/^,//;
        if ( $geogscales )	{
            $geogscales = qq| collections.geogscale IN ($geogscales) |;
            if ( $q->param('geogscale_unknown')) {
                $geogscales = " (".$geogscales."OR collections.geogscale IS NULL)";
            }
        }
    }
    return $geogscales;
}

sub getStratscaleString{
	my $self = shift;
	my $stratscales = "";

    if (! $q->param('stratscale_bed') || ! $q->param('stratscale_group_of_beds') || ! $q->param('stratscale_member') ||
        ! $q->param('stratscale_formation') || ! $q->param('stratscale_group') || ! $q->param('stratscale_unknown')) {
        if ( $q->param('stratscale_bed') )	{
            $stratscales = "'bed'";
        }
        if ( $q->param('stratscale_group_of_beds') )	{
            $stratscales .= ",'group of beds'";
        }
        if ( $q->param('stratscale_member') )	{
            $stratscales .= ",'member'";
        }
        if ( $q->param('stratscale_formation') )	{
            $stratscales .= ",'formation'";
        }
        if ( $q->param('stratscale_group') )	{
            $stratscales .= ",'group'";
        }
        if ( $q->param('stratscale_unknown') ) {
            $stratscales .= ",''";
        }
       
        $stratscales =~ s/^,//;
        if ( $stratscales )	{
            $stratscales = qq| collections.stratscale IN ($stratscales) |;
            if ($q->param('stratigraphic_scale_unknown')) {
                $stratscales = " (".$stratscales."OR collections.stratscale IS NULL)";
            }
        }
    }
    return $stratscales;
}

sub getCollectionTypeString{
    my $self = shift;
    my $colltypes = "";
    # Collection types
    if (! $q->param('collection_type_archaeological') || ! $q->param('collection_type_biostratigraphic') ||
        ! $q->param('collection_type_paleoecologic') || ! $q->param('collection_type_taphonomic') ||
        ! $q->param('collection_type_taxonomic') || ! $q->param('collection_type_general_faunal/floral') ||
        ! $q->param('collection_type_unknown')) {
        if ($q->param('collection_type_archaeological')) {
            $colltypes .= ",'archaeological'";
        }
        if ($q->param('collection_type_biostratigraphic')) {
            $colltypes .= ",'biostratigraphic'";
        }
        if ($q->param('collection_type_paleoecologic')) {
            $colltypes .= ",'paleoecologic'";
        }
        if ($q->param('collection_type_taphonomic')) {
            $colltypes .= ",'taphonomic'";
        }
        if ($q->param('collection_type_taxonomic')) {
            $colltypes .= ",'taxonomic'";
        }
        if ($q->param('collection_type_general_faunal/floral')) {
            $colltypes .= ",'general faunal/floral'";
        }
        if ($q->param('collection_type_unknown')) {
            $colltypes .= ",''";
        }
        $colltypes =~ s/^,//;
        if ( $colltypes)	{
            $colltypes = qq| collections.collection_type IN ($colltypes) |;
            if ($q->param('collection_type_unknown')) {
                $colltypes = "(".$colltypes." OR collections.collection_type IS NULL)";
            }
        }
    }
    return $colltypes;
}


sub getOccurrencesWhereClause {
	my $self = shift;
	
	my $where = DBTransactionManager->new();
	$where->setWhereSeparator("AND");

	if ( $q->param('pubyr') > 0 )	{
		if ( $q->param('published_before_after') eq "before" )	{
			$pubyrrelation = "<";
		} else	{
			$pubyrrelation = ">";
		}
		$where->addWhereItem(" pubyr".$pubyrrelation.$q->param('pubyr')." ");
	}
	
	my $authorizer = $q->param('authorizer');
	
	$where->addWhereItem(" occurrences.authorizer='$authorizer' ") if ($authorizer ne "");

    $where->addWhereItem($self->getTaxonString()) if ($q->param('taxon_name') ne "");
	
    $where->addWhereItem(" occurrences.abund_value NOT LIKE \"\" AND occurrences.abund_value IS NOT NULL ") if $q->param("without_abundance") eq 'NO';
	$where->addWhereItem(" occurrences.species_name!='indet.' ") if $q->param('indet') eq 'NO';
	$where->addWhereItem(" occurrences.species_name!='sp.' ") if $q->param('sp') eq 'NO';
	$where->addWhereItem(" (occurrences.genus_reso NOT IN ('aff.','\"') OR occurrences.genus_reso IS NULL) ") if $q->param('poor_genus_reso') eq 'NO';
	$where->addWhereItem(" (occurrences.genus_reso NOT LIKE '%informal%' OR occurrences.genus_reso IS NULL) ") if $q->param('informal') eq 'NO';

	return $where->whereExpr();
}

sub getCollectionsWhereClause {
	my $self = shift;
	
	my $where = DBTransactionManager->new();
	$where->setWhereSeparator("AND");
	
	my $authorizer = $q->param('authorizer');
	# This is handled by getOccurrencesWhereClause if we're getting occs data.
	if($authorizer ne "" && $q->param('output_data') eq 'collections'){
		$where->addWhereItem(" collections.authorizer='$authorizer' ");
	}
	
	$where->addWhereItem($self->getResGrpString()) if $self->getResGrpString();	
	
	# should we filter the data based on collection creation date?
	# added by rjp on 12/30/2003, some code copied from Curve.pm.
	# (filter it if they enter a year at the minimum.
	if ($q->param('year')) {
		
		my $month = $q->param('month');
		my $day = $q->param('date');

		# use default values if the user didn't enter any.		
		if (! $q->param('month')) { $month = "01" }
		if (! $q->param('date')) { $day = "01" }
				 
		if ( length $day == 1 )	{
			$day = "0".$q->param('date'); #prepend a zero if only one digit.
		}
		
		# note, this should really be handled by a function?  maybe in bridge.pl.
		my $created_date = $dbh->quote($q->param('year')."-".$month."-".$day." 00:00:00");
		# note, the version of mysql on flatpebble needs the 000000 at the end, but the
		# version on the linux box doesn't need it.  weird.						 
	
		my $created_string;
		# if so, did they want to look before or after?
		if ($q->param('created_before_after') eq "before") {
			if ( $q->param('output_data') eq 'collections' )	{
				$created_string = " collections.created < $created_date ";
			} else	{
				$created_string = " occurrences.created < $created_date ";
			}
		} elsif ($q->param('created_before_after') eq "after") {
			if ( $q->param('output_data') eq 'collections' )	{
				$created_string = " collections.created > $created_date ";
			} else	{
				$created_string = " occurrences.created > $created_date ";
			}
		}
		
		$where->addWhereItem($created_string);
	}

    $where->addWhereItem($_) for (
        $self->getCountryString(),
        $self->getLatLongString(),
        $self->getPaleoLatLongString(),
        $self->getIntervalString(),
        $self->getLithificationString(),
        $self->getLithologyString(),
        $self->getEnvironmentString(),
        $self->getGeogscaleString(),
        $self->getStratscaleString(),
        $self->getCollectionTypeString());

		
	return $where->whereExpr();
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

sub getCompendiumAgeRanges	{
	my $self = shift;

	open IN,"<./data/compendium.ranges";
	while (<IN>)	{
		s/\n//;
		my ($genus,$bin) = split /\t/,$_;
		$incompendium{$genus.$bin}++;
	}
	close IN;
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
    my $interval_names;


	# get the period names for the collections JA 22.2.04
	# based on scale 2 = Harland periods
	if ( $q->param('collections_period') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '2');
		%myperiod = %{$intervalInScaleRef};
	}

	# get the epoch names for the collections JA 22.2.04
	# based on scale 4 = Harland epochs
	if ( $q->param('collections_epoch') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '4');
		%myepoch = %{$intervalInScaleRef};
	}
	# get the PBDB 10 m.y. bin names for the collections JA 3.3.04
	# WARNING: the locage_max field is just being used as a dummy
	if ( $q->param('collections_10mybin') || $q->param('compendium_ranges') eq 'NO' )	{
		@_ = TimeLookup::processBinLookup($dbh,$dbt);
		%mybin = %{$_[0]};
	}

    # Get hash to map interval_no -> interval_name
    if ($q->param("collections_max_interval_no") eq "YES" || 
        $q->param("collections_min_interval_no") eq "YES") {
        $interval_names = $self->getIntervalNames();
    }

    #
    # Handle generation of the SELECT part of the query
    #      
    my $permissionFields = "collections.authorizer,collections.research_group,collections.access_level, " 
				         . " DATE_FORMAT(collections.release_date, '%Y%m%d') rd_short ";
    my $commonFields = "collections.reference_no, collections.collection_no";
	# Getting only collection data
	if($q->param('output_data') eq 'collections'){
		$sql =  "SELECT $commonFields,$permissionFields";
        my $outFieldsString = $self->getOutFieldsString('collections',TRUE);
        if ($outFieldsString ne '') { $sql .= ",$outFieldsString" ; }
	# Getting distinct taxon names from occurrences table (JA 12.9.03)
    # Use a group by later
	} elsif ( $q->param('output_data') eq "genera" )	{
		$sql = "SELECT occurrences.genus_name as occ_genus_name, $commonFields, $permissionFields ";
	# Getting occurrence and collection data:
	# Create the sql: we're doing a join on occurrences and collections
	# so as to select all the data at once.
	} elsif ($q->param('output_data') eq "occurrences") {
		$sql =	"SELECT $commonFields,$permissionFields,". 
                "occurrences.reference_no as occ_reference_no,".
				"occurrences.genus_reso as occ_genus_reso, occurrences.genus_name as occ_genus_name, ".
				"occurrences.taxon_no as occ_taxon_no, ".
				"occurrences.abund_value as occ_abund_value, ".
				"occurrences.abund_unit as occ_abund_unit, ".
                "occurrences.authorizer as occ_authorizer, ".
				"reidentifications.reference_no as reid_reference_no, ".
				"reidentifications.genus_reso as reid_genus_reso, ".
				"reidentifications.genus_name as reid_genus_name";
		my $outFieldsString = $self->getOutFieldsString('occurrences',TRUE);
		if ($outFieldsString ne '') { $sql .= ", $outFieldsString" ; }
		$outFieldsString = $self->getOutFieldsString('reidentifications',TRUE);
		if ($outFieldsString ne '') { $sql .= ", $outFieldsString" ; }
        $outFieldsString = $self->getOutFieldsString('collections',TRUE);
        if ($outFieldsString ne '') { $sql .= ", $outFieldsString" ; }
    }

    #
    # Handle generation of the FROM, WHERE, parts of the query, and JOIN conditions
    #      
    if ($q->param('output_data') eq 'collections') {
	    $sql .=" FROM collections";
        my $qualifier = 'WHERE';
        if ($q->param('taxon_name') ne '') {
            $sql .= ",occurrences LEFT JOIN reidentifications ON".
                    " occurrences.occurrence_no = reidentifications.occurrence_no ".
                    " WHERE collections.collection_no = occurrences.collection_no";

            my $taxonWhereClause = $self->getTaxonString();
            if ($taxonWhereClause ne '') { 
                $sql .= " AND $taxonWhereClause"; 
            }
            $qualifier = 'AND';        
        }
        my $collectionsWhereClause = $self->getCollectionsWhereClause();
	    if($collectionsWhereClause ne ''){ $sql .= " $qualifier $collectionsWhereClause "; }
    } else { # both genera and occurrences 
	    $sql .=" FROM collections, occurrences";
		if ( $q->param('pubyr') > 0 )	{
			$sql .= " LEFT JOIN refs ON refs.reference_no=occurrences.reference_no";
		}
		$sql .= " LEFT JOIN reidentifications ON".
				" occurrences.occurrence_no = reidentifications.occurrence_no ".
				" WHERE collections.collection_no = occurrences.collection_no";

        my $collectionsWhereClause = $self->getCollectionsWhereClause();
	    if($collectionsWhereClause ne ''){ $sql .= " AND $collectionsWhereClause "; }
		my $occWhereClause = $self->getOccurrencesWhereClause();
	    if($occWhereClause ne ''){ $sql .= " AND $occWhereClause "; }

    }
   
    # Handle GROUP BY
	if ( $q->param('output_data') eq "genera" )	{
        $sql .= " GROUP BY occurrences.genus_name";
    } elsif ($q->param('output_data') eq 'occurrences') {
        if($q->param('lumpgenera') eq 'YES') {
           $sql .= " GROUP BY occurrences.genus_name, occurrences.collection_no";
        } 
    } else { # = collections
        if ($q->param('taxon_name') ne '') {
            $sql .= " GROUP BY collections.collection_no";
        }
    }
	
    #
    # Header Generation
    #
	# print column names to occurrence output file JA 19.8.01
	my $header = "";
    $sepChar = ","  if ($q->param('output_format') eq 'comma-delimited text');
    $sepChar = "\t" if ($q->param('output_format') eq 'tab-delimited text');
    if( $q->param('output_data') eq 'genera'){
        $header = "genus_name";
    } else {
        $header =  "collection_no";
	    if($q->param('output_data') eq 'occurrences') {
            $header .= $sepChar.'class_name' if ($q->param("occurrences_class_name") eq "YES");
            $header .= $sepChar.'order_name' if ($q->param("occurrences_order_name") eq "YES");
            $header .= $sepChar.'family_name' if ($q->param("occurrences_family_name") eq "YES");
            $header .= $sepChar.'genus_reso'.$sepChar.'genus_name'.$sepChar.'reidentifications.genus_reso'.$sepChar.'reidentifications.genus_name';

            # Occurrence header
            @occurrenceHeaderCols = $self->getOutFields('occurrences');	# Need this (for later...)
            my $occurrenceCols = join($sepChar, @occurrenceHeaderCols);
            if ( $occurrenceCols ) { $header .= $sepChar.$occurrenceCols; }

            # ReID header
            # Need this (for later...)
            @reidHeaderCols = $self->getOutFields('reidentifications');	
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
        }

        # Collection header
        @collectionHeaderCols = $self->getOutFields('collections');	# Need this (for later...)
        my $collectionCols = join ( $sepChar, @collectionHeaderCols );
        if ( $collectionCols ) { $header .= $sepChar.$collectionCols; }
    }
	print OUTFILE "$header\n";
	$self->dbg ( "Output header: $header" );
    
	#
	# Loop through the result set
    #
	$sql =~ s/\s+/ /g;
	$self->dbg("<b>Occurrences query:</b><br>\n$sql<BR>");

	my $sth = $dbh->prepare($sql); #|| die $self->dbg("Prepare query failed ($!)<br>");
	my $rv = $sth->execute(); # || die $self->dbg("Execute query failed ($!)<br>");
	$self->dbg($sth->rows()." rows returned.<br>");

	# See if rows okay by permissions module
	my @dataRows = ( );
	my $limit = 1000000;
	my $ofRows = 0;
	$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );

	$sth->finish();
	$self->dbg("Rows that passed Permissions: number of rows $ofRows, length of dataRows array: ".@dataRows."<br>");

	# first do a quick hit to get some by-taxon and by-collection stats
	#  ... and so on
    # also records genera names, useful for later loop
	my %mbrseen;
	my %occseen;
	foreach my $row ( @dataRows ){
		# raise subgenera to genus level JA 18.8.04
		if ( $q->param('split_subgenera') eq 'YES' && $row->{occ_subgenus_name} )	{
			$row->{occ_genus_name} = $row->{occ_subgenus_name};
		}
		my $exclude = 0;
		# get rid of occurrences of genera either (1) not in the
		#  Compendium or (2) falling outside the official Compendium
		#  age range JA 27.8.04
		if ( $q->param('compendium_ranges') eq 'NO' )	{
			if ( ! $incompendium{$row->{occ_genus_name}.$mybin{$row->{collection_no}}} )	{
				$exclude++;
#				print "exc. compendum ".$row->{'collection_no'};
			}
		}
		# lump bed/group of beds scale collections with the exact same
		#  formation/member and geographic coordinate JA 21.8.04
		if ( $exclude == 0 && ( $q->param('lump_by_coord') eq 'YES' || $q->param('lump_by_interval') eq 'YES' || $q->param('lump_by_mbr') eq 'YES' || $q->param('lump_by_ref') eq 'YES' ) )	{

			my $mbrstring;

			if ( $q->param('lump_by_coord') eq 'YES' )	{
				$mbrstring = $row->{'latdeg'}.$row->{'latmin'}.$row->{'latsec'}.$row->{'latdec'}.$row->{'latdir'}.$row->{'lngdeg'}.$row->{'lngmin'}.$row->{'lngsec'}.$row->{'lngdec'}.$row->{'lngdir'};
			}
			if ( $q->param('lump_by_interval') eq 'YES' )	{
				$mbrstring .= $row->{'max_interval_no'}.$row->{'min_interval_no'};
			}
			if ( $q->param('lump_by_mbr') eq 'YES' )	{
				$mbrstring .= $row->{'formation'}.$row->{'member'};
			}
			if ( $q->param('lump_by_ref') eq 'YES' )	{
				$mbrstring .= $row->{'reference_no'};
			}

			if ( $mbrseen{$mbrstring} )	{
				$row->{collection_no} = $mbrseen{$mbrstring};
				if ( $occseen{$row->{collection_no}.$row->{occ_genus_name}} > 0 )	{
					$exclude++;
				}
			} else	{
				$mbrseen{$mbrstring} = $row->{collection_no};
			}
			$occseen{$row->{collection_no}.$row->{occ_genus_name}}++;
		}
		if ( $exclude == 0 )	{
			push @tempDataRows, $row;
		# cumulate number of collections including each genus
			$totaloccs{$row->{occ_genus_name}}++;
		# need these two for ecology lookup below
			$totaloccsbyno{$row->{occ_taxon_no}}++;
			$genusbyno{$row->{occ_taxon_no}} = $row->{occ_genus_name};
		# cumulate number of specimens per collection, and number of
		#  times a genus has abundance counts at all
			if ( ( $row->{occ_abund_unit} eq "specimens" || $row->{occ_abund_unit} eq "individuals" ) && ( $row->{occ_abund_value} > 0 ) )	{
				$nisp{$row->{collection_no}} = $nisp{$row->{collection_no}} + $row->{occ_abund_value};
				$numberofcounts{$row->{occ_genus_name}}++;
			}
		# also make a master list of all collection numbers that are
		#  used, so we can properly get all the primary AND secondary
		#  refs for those collections JA 16.7.04
			$COLLECTIONS_USED{$row->{collection_no}}++;
			$REFS_USED{$row->{'reference_no'}}++;
			#$REFS_USED{$row->{'occ_reference_no'}}++ if ($row->{'occ_reference_no'});
			#$REFS_USED{$row->{'reid_reference_no'}}++ if ($row->{'reid_reference_no'});
		}
	}
	@dataRows = @tempDataRows;
    $self->dbg("primary refs: " . (join(",",keys %REFS_USED))); 

	# now hit the secondary refs table, mark all of those references as
	#  having been used, and print all the refs JA 16.7.04
	my @collnos = keys %COLLECTIONS_USED;
	if ( @collnos )	{
		$secondary_sql = "SELECT reference_no,collection_no FROM secondary_refs WHERE collection_no IN (" . join (',',@collnos) . ")";
        $self->dbg("secondary refs sql: $secondary_sql"); 
		my @refrefs= @{$dbt->getData($secondary_sql)};
		for my $refref (@refrefs)	{
			$REFS_USED{$refref->{reference_no}}++;
		}
		my @refnos = keys %REFS_USED;
        $self->dbg("primary +secondary refs: " . join(",",(keys %REFS_USED))); 

	# print the header
		print REFSFILE join (',',@refsFieldNames), "\n";

	# print the refs
		$ref_sql = "SELECT * FROM refs WHERE reference_no IN (" . join (',',@refnos) . ")";
        $self->dbg("Get ref data sql: $ref_sql"); 
		@refrefs= @{$dbt->getData($ref_sql)};
		for my $refref (@refrefs)	{
			my @refvals = ();
			for my $r (@refsFieldNames)	{
				push @refvals , $refref->{$r};
			}
			printf REFSFILE "%s\n",$self->formatRow(@refvals);
			$acceptedRefs++;
		}
	}
	close REFSFILE;

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


    # This list of genera is needed  both for the abundance file far below
    # and for retrieving the class/order/family hash for the occurences output file
    # right below here
	my @genera = keys %totaloccs;
	my @genera_nos = keys %totaloccsbyno;
	@genera = sort @genera;

	my %master_class;
    if ($q->param("output_data") eq "occurrences") {
        my $levels = "";
        if($q->param("occurrences_class_name") eq "YES"){
            $levels .= ",class";
        }
        if($q->param("occurrences_order_name") eq "YES"){
            $levels .= ",order";
        }
        if($q->param("occurrences_family_name") eq "YES"){
            $levels .= ",family";
        }
        $levels =~ s/^,//;
        if ($levels) {
    	    %master_class=%{Classification::get_classification_hash($dbt,$levels,\@genera_nos)};
        }
    }    

	# main pass through the results set
	my $acceptedCount = 0;
	foreach my $row ( @dataRows ){
		# These DON'T come back with a table name prepended.
		my $reference_no = $row->{reference_no};
		my $genus_reso = $row->{occ_genus_reso};
		my $genusName = $row->{occ_genus_name};
		my $genusNo = $row->{occ_taxon_no};
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
		if ( ( $row->{occ_abund_unit} eq "specimens" || $row->{occ_abund_unit} eq "individuals" ) && ( $row->{occ_abund_value} > 0 ) )	{
			$summedproportions{$row->{occ_genus_name}} = $summedproportions{$row->{occ_genus_name}} + log( $row->{occ_abund_value} / $nisp{$row->{collection_no}} );
		}

		#$self->dbg("reference_no: $reference_no<br>genus_reso: $genus_reso<br>genusName: $genusName<br>collection_no: $collection_no<br>");

		# Only print one occurrence per collection if "collections only"
		# was checked; do this by fooling the system into lumping all
		# occurrences in a collection
		my $tempGenus = $genusName;
		if( $q->param('output_data') eq 'collections'){
			$tempGenus = '';
			if($COLLS_DONE{"$collection_no.$tempGenus"} == 1){
				next;
			}
			# else
			$COLLS_DONE{"$collection_no.$tempGenus"} = 1;
		}
		
		my @coll_row = ();
		my @occs_row = ();
		my @reid_row = ();

        #
        # Set up occurence fields
        #
        if ($q->param("output_data") eq "occurrences") {
            # Put the values in the correct order since by looping through this array
            foreach my $column ( @occurrenceHeaderCols ){
                $column =~ s/^occurrences\./occ_/;
                push ( @occs_row, $row->{$column} );
            }
        }


        #
        # Set up reid fields
        #
        
        if ($q->param("output_data") eq "occurrences") {
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

            # Put the values in the correct order since by looping through this array
            foreach my $column ( @reidHeaderCols ){
                $column =~ s/^reidentifications\./reid_/;
                push ( @reid_row, $row->{$column} );
            }
        }


        #
        # Set up collections fields
        #

        if ($q->param("output_data") ne "genera") {
            # Get coordinates into the correct format (decimal or deg/min/sec/dir), performing
            # conversions as necessary
            if ($q->param('collections_lng') eq "YES") {
                if ($q->param('collections_lng_format') eq 'decimal') {
                    if ($row->{'lngmin'} =~ /\d+/) {
                        $row->{'lngdec'} = sprintf("%.6f",$row->{'lngdeg'} + $row->{'lngmin'}/60 + $row->{'lngsec'}/3600);
                    } else {
                        if ($row->{'lngdec'} =~ /\d+/) {
                            $row->{'lngdec'} = sprintf("%s",$row->{'lngdeg'}.".".int($row->{'lngdec'}));
                        } else {
                            $row->{'lngdec'} = $row->{'lngdeg'};
                        }
                    }    
                    $row->{'lngdec'} *= -1 if ($row->{'lngdir'} eq "West");
                } else {
                    if (!($row->{'lngmin'} =~ /\d+/)) {
                        if ($row->{'lngdec'} =~ /\d+/) {
                            my $min = 60*(".".$row->{'lngdec'});
                            $row->{'lngmin'} = int($min);
                            $row->{'lngsec'} = int(($min-int($min))*60);
                        }    
                    }
                }
            }
            if ($q->param('collections_lat') eq "YES") {
                if ($q->param('collections_lat_format') eq 'decimal') {
                    if ($row->{'latmin'} =~ /\d+/) {
                        $row->{'latdec'} = sprintf("%.6f",$row->{'latdeg'} + $row->{'latmin'}/60 + $row->{'latsec'}/3600);
                    } else {
                        if ($row->{'latdec'} =~ /\d+/) {
                            $row->{'latdec'} = sprintf("%s",$row->{'latdeg'}.".".int($row->{'latdec'}));
                        } else {
                            $row->{'latdec'} = $row->{'latdeg'};
                        }
                    }    
                    $row->{'latdec'} *= -1 if ($row->{'latdir'} eq "South");
                } else {
                    if (!($row->{'latmin'} =~ /\d+/)) {
                        if ($row->{'latdec'} =~ /\d+/) {
                            my $min = 60*(".".$row->{'latdec'});
                            $row->{'latmin'} = int($min);
                            $row->{'latsec'} = int(($min-int($min))*60);
                        }    
                    }
                }
            }
            if ($q->param('collections_paleolat') eq "YES") {
                if ($q->param('collections_paleolat_format') eq 'decimal') {
                    $row->{'paleolatdec'} = $row->{'paleolat'};
                } else {
                    if ($row->{'paleolat'} =~ /\d+/) {
                        $row->{'paleolatdir'}  = ($row->{'paleolat'} >= 0) ? "North" : "South";
                        my $abs_lat = ($row->{'paleolat'} < 0) ? -1*$row->{'paleolat'} : $row->{'paleolat'};
                        my $deg = int($abs_lat);
                        my $min = 60*($abs_lat-$deg);
                        $row->{'paleolatdeg'} = $deg;
                        $row->{'paleolatmin'} = int($min);
                        $row->{'paleolatsec'} = int(($min-int($min))*60);
                    }    
                }
            }
            if ($q->param('collections_paleolng') eq "YES") {
                if ($q->param('collections_paleolng_format') eq 'decimal') {
                    $row->{'paleolngdec'} = $row->{'paleolng'};
                } else {
                    if ($row->{'paleolng'} =~ /\d+/) {
                        $row->{'paleolngdir'}  = ($row->{'paleolng'} >= 0) ? "East" : "West";
                        my $abs_lat = ($row->{'paleolng'} < 0) ? -1*$row->{'paleolng'} : $row->{'paleolng'};
                        my $deg = int($abs_lat);
                        my $min = 60*($abs_lat-$deg);
                        $row->{'paleolngdeg'} = $deg;
                        $row->{'paleolngmin'} = int($min);
                        $row->{'paleolngsec'} = int(($min-int($min))*60);
                    }    
                }
            }
            if ($q->param("collections_max_interval_no") eq "YES") {
			    # translate interval nos into names JA 18.9.03
                $row->{'max_interval_no'} = $interval_names->{$row->{'max_interval_no'}};
            }
            if ($q->param("collections_min_interval_no") eq "YES") {
                $row->{'min_interval_no'} = $interval_names->{$row->{'min_interval_no'}};
            }    
            if ($q->param("collections_period") eq "YES") {
                # translate bogus period or epoch max into legitimate,
                #   looked-up period or epoch names JA 22.2.04
                # WARNING: this won't work at all if the period_max
                #   and/or epoch_max fields are ever removed from
                #   the database
                $row->{'period'} = $myperiod{$row->{'collection_no'}};
            }
            if ($q->param("collections_epoch") eq "YES") {
                $row->{'epoch'} = $myepoch{$row->{'collection_no'}};
            }
            if ($q->param("collections_10mybin") eq "YES") {
                # WARNING: similar trick here in which useless legacy
                #  field locage_max is used as a placeholder for the
                #  bin name
                $row->{'10mybin'} = $mybin{$row->{'collection_no'}};
            }

            # Put the values in the correct order since by looping through this array
            foreach my $column ( @collectionHeaderCols ){
                $column =~ s/collections\.//;
                push ( @coll_row, $row->{$column} );
            }
        }


		if( $q->param('output_data') eq 'collections'){
			$curLine = $self->formatRow(($collection_no, @occs_row, @coll_row));
		} elsif ( $q->param('output_data') eq 'genera')	{
			$curLine = $self->formatRow(($genusName));
		} else{
            my @firstCols = ($collection_no);
            if ($q->param("occurrences_class_name") eq "YES" || 
                $q->param("occurrences_order_name") eq "YES" ||
                $q->param("occurrences_family_name") eq "YES") {
                # -1 at the end is essential so trailing whitespace shows up in the array
                my @parents = split(/,/,$master_class{$row->{'occ_taxon_no'}},-1);
                push @firstCols, @parents;
            }
            push (@firstCols,$genus_reso,$genusName,$reid_genus_reso,$reid_genus_name);
			$curLine = $self->formatRow((@firstCols, @occs_row, @reid_row, @coll_row));
		}
		# get rid of carriage returns 24.5.04 JA
		# failing to do this is lethal and I'm not sure why no-one
		#  alerted me to this bug previously
		$curLine =~ s/\n/ /g;
		print OUTFILE "$curLine\n";
		$acceptedCount++;
	}
	close OUTFILE;

	# print out a list of genera with total number of occurrences and average relative abundance
	if ( $q->param("output_data") ne "collections" )	{
		my @abundline = ();
		open ABUNDFILE,">$OUT_FILE_DIR/$generaOutFileName";
		push @abundline, 'genus','collections','with abundances','geometric mean abundance';
		print ABUNDFILE join($sepChar,@abundline)."\n";
		for $g ( @genera )	{
			@abundline = ();
			push @abundline, $g, $totaloccs{$g}, sprintf("%d",$numberofcounts{$g});
			if ( $numberofcounts{$g} > 0 )	{
				push @abundline, sprintf("%.4f",exp($summedproportions{$g} / $numberofcounts{$g}));
			} else	{
				push @abundline, "NaN";
			}
			print ABUNDFILE join($sepChar,@abundline)."\n";
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
			 "Jurassic 2", "Jurassic 1",
			 "Triassic 4", "Triassic 3", "Triassic 2",
			 "Triassic 1", "Permian 4",
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
		my @scaleline;
		push @scaleline, 'interval','lower boundary','upper boundary','midpoint','total occurrences','total genera';
		for my $val ( @enumvals )	{
			if ( $val eq "" )	{
				push @scaleline, 'no data occurrences', 'no data taxa';
			} else	{
				push @scaleline, "$val occurrences", "$val taxa";
			}
		}
		for my $val ( @enumvals )	{
			if ( $val eq "" )	{
				push @scaleline, 'proportion no data occurrences', 'proportion no data taxa';
			} else	{
				push @scaleline, "proportion $val occurrences", "proportion $val taxa";
			}
		}
		print SCALEFILE join($sepChar,@scaleline)."\n";

		for my $intervalName ( @intervalnames )	{
			$acceptedIntervals++;
			@scaleline = ();
			push @scaleline, $intervalName;
			push @scaleline, sprintf("%.2f",$lowerbinbound{$intervalName});
			push @scaleline, sprintf("%.2f",$upperbinbound{$intervalName});
			push @scaleline, sprintf("%.2f",($lowerbinbound{$intervalName} + $upperbinbound{$intervalName}) / 2);
			push @scaleline, sprintf("%d",$occsbybin{$intervalName});
			push @scaleline, sprintf("%d",$taxabybin{$intervalName});
			for my $val ( @enumvals )	{
				push @scaleline, sprintf("%d",$occsbybinandcategory{$intervalName}{$val});
				push @scaleline, sprintf("%d",$taxabybinandcategory{$intervalName}{$val});
			}
			for my $val ( @enumvals )	{
				if ( $occsbybinandcategory{$intervalName}{$val} eq "" )	{
					push @scaleline, "0.0000","0.0000";
				} else	{
					push @scaleline, sprintf("%.4f",$occsbybinandcategory{$intervalName}{$val} / $occsbybin{$intervalName});
					push @scaleline, sprintf("%.4f",$taxabybinandcategory{$intervalName}{$val} / $taxabybin{$intervalName});
				}
			}
			print SCALEFILE join($sepChar,@scaleline)."\n";
		}
		close SCALEFILE;
	}

	# Tell what happened
	if ( ! $acceptedCount ) { $acceptedCount = 0; }
	if ( ! $acceptedRefs ) { $acceptedRefs = 0; }
    if ( ! $acceptedGenera) { $acceptedGenera = 0; }
	print "
<table border='0' width='600'>
<tr><td class='darkList'><b><font size='+1'>Output files</font></b></td></tr>
<tr><td>$acceptedCount ".$q->param("output_data")." were printed to <a href='$OUT_HTTP_DIR/$occsOutFileName'>$occsOutFileName</a></td></tr>\n";
	if ( $q->param("output_data") ne "collections" )	{
		print "
<tr><td>$acceptedGenera genus names were printed to <a href='$OUT_HTTP_DIR/$generaOutFileName'>$generaOutFileName</a></td></tr>\n";
	}
	print "
<tr><td>$acceptedRefs references were printed to <a href='$OUT_HTTP_DIR/$refsOutFileName'>$refsOutFileName</a></td></tr>\n";
	if ( $q->param('time_scale') )	{
		print "
<tr><td>$acceptedIntervals time intervals were printed to <a href=\"$OUT_HTTP_DIR/$scaleOutFileName\">$scaleOutFileName</a></td></tr>\n";
	}
	if ( $q->param("max_interval_name") && ! $bestbothscale )	{
		print "<tr><td><b>WARNING</b>: the two intervals are not in the same time scale, so intervals in between them could not be determined.</td></tr>\n";
	}
print "</table>
<p align='center'><b><a href='?action=displayDownloadForm'>Do&nbsp;another&nbsp;download</a> -
<a href='?action=displayCurveForm'>Generate&nbsp;diversity&nbsp;curves</a></b>
</p>
</center>
";
	
}

# This functions sets up handles to the output files, and
# also sets certain implied param fields in the CGI object so the SQL
# and CSV get generated correctly
sub setupOutput {
	my $self = shift;
	my $outputType = $q->param('output_format');
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
	if ( $q->param("output_data") eq 'collections')	{
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

    # Setup additional fields that should appear as columns in the CSV file

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
    # this sets a bunch of checkbox values to true if a corresponding checkbox is also true
    # so that the download script may be a bit simpler. 
    # must be called before retellOptions
    # i.e  if intage_max == YES, then set emlintage_max == YES as well 
    # PS 11/22/2004
    $q->param('collections_emlperiod_max' => "YES") if ($q->param('collections_period_max'));
    $q->param('collections_emlperiod_min' => "YES") if ($q->param('collections_period_min'));
    $q->param('collections_emlintage_max' => "YES") if ($q->param('collections_intage_max'));
    $q->param('collections_emlintage_min' => "YES") if ($q->param('collections_intage_min'));
    $q->param('collections_emlepoch_max' => "YES")  if ($q->param('collections_epoch_max'));
    $q->param('collections_emlepoch_min' => "YES")  if ($q->param('collections_epoch_min'));
    $q->param('collections_emllocage_max' => "YES") if ($q->param('collections_locage_max'));
    $q->param('collections_emllocage_min' => "YES") if ($q->param('collections_locage_min'));

    # Get the right lat/lng/paleolat/paleolng fields
    if ($q->param('collections_lat')) {
        if ($q->param('collections_lat_format') eq 'decimal') {
            $q->param('collections_latdec'=>'YES');
        } else {
            $q->param('collections_latdeg'=>'YES');
            $q->param('collections_latmin'=>'YES');
            $q->param('collections_latsec'=>'YES');
            $q->param('collections_latdir'=>'YES');
        }
    }
    if ($q->param('collections_lng')) {
        if ($q->param('collections_lng_format') eq 'decimal') {
            $q->param('collections_lngdec'=>'YES');
        } else {
            $q->param('collections_lngdeg'=>'YES');
            $q->param('collections_lngmin'=>'YES');
            $q->param('collections_lngsec'=>'YES');
            $q->param('collections_lngdir'=>'YES');
        }
    }
    if ($q->param('collections_paleolat')) {
        if ($q->param('collections_paleolat_format') eq 'decimal') {
            $q->param('collections_paleolatdec'=>'YES');
        } else {
            $q->param('collections_paleolatdeg'=>'YES');
            $q->param('collections_paleolatmin'=>'YES');
            $q->param('collections_paleolatsec'=>'YES');
            $q->param('collections_paleolatdir'=>'YES');
        }
    }
    if ($q->param('collections_paleolng')) {
        if ($q->param('collections_paleolng_format') eq 'decimal') {
            $q->param('collections_paleolngdec'=>'YES');
        } else {
            $q->param('collections_paleolngdeg'=>'YES');
            $q->param('collections_paleolngmin'=>'YES');
            $q->param('collections_paleolngsec'=>'YES');
            $q->param('collections_paleolngdir'=>'YES');
        }
    }
    # Get these related fields as well
    if ($q->param('occurrences_subgenus_name')) {
        $q->param('occurrences_subgenus_reso'=>'YES');
        $q->param('reidentifications_subgenus_reso'=>'YES');
    }
    if ($q->param('occurrences_species_name')) {
        $q->param('occurrences_species_reso'=>'YES');
        $q->param('reidentifications_species_reso'=>'YES');
    }
    # There is no separate reidentifications checkboxes on the form
    # So they get included if the corresponding occurences checkbox is set
    foreach $field (@reidentificationsFieldNames) {
        if ($q->param('occurrences_'.$field)) {
            $q->param("reidentifications_$field"=>'YES');
        }
    }

    # Get EML values, check interval names
    if ($q->param('max_interval_name')) {
        if ($q->param('max_interval_name') =~ /[a-zA-Z]/) {
            my ($eml, $name) = TimeLookup::splitInterval($dbt,$q->param('max_interval_name'));
            my $ret = Validation::checkInterval($dbt,$eml,$name);
            if (!$ret) {
                push @errors, "There is no record of ".$q->param('max_interval_name')." in the database";
                $q->param('max_interval_name'=>'');
                $q->param('max_eml_interval'=>'');
            } else {
                $q->param('max_interval_name'=>$name);
                $q->param('max_eml_interval'=>$eml);
            }
        }
    }
    if ($q->param('min_interval_name')) {
        if ($q->param('min_interval_name') =~ /[a-zA-Z]/) {
            my ($eml, $name) = TimeLookup::splitInterval($dbt,$q->param('min_interval_name'));
            my $ret = Validation::checkInterval($dbt,$eml,$name);
            if (! $ret) {
                push @errors, "There is no record of ".$q->param('min_interval_name')." in the database";
                $q->param('min_interval_name'=>'');
                $q->param('min_eml_interval'=>'');
            } else {
                $q->param('min_interval_name'=>$name);
                $q->param('min_eml_interval'=>$eml);
            }
        }
    }

    # Generate warning for taxon with homonyms
    my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));
    foreach my $taxon (@taxa) {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon);
        if (scalar(@taxon_nos)  > 1) {
            push @errors, "The taxon name '$taxon' is ambiguous and belongs to multiple taxonomic hierarchies. Right the download script can't distinguish between these different cases. If this is a problem email <a href='mailto: alroy\@nceas.ucsb.edu'>John Alroy</a>.";
        }
    } 

    # Now if there are any errors, die
    if (@errors) {
        PBDBUtil::printErrors(@errors);
        print main::stdIncludes("std_page_bottom");
        exit;     
    }    
}

sub formatRow {
	my $self = shift;

	if ( $csv->combine ( @_ ) ) {
		return $csv->string();
	}
}

# JA: Paul replaced taxonomic_search call with recurse call because it's faster,
#  but I'm reverting because I'm not maintaining recurse
# renamed from getGenusNames to getTaxonString to reflect changes in how this works PS 01/06/2004
sub getTaxonString {
	my $self = shift;

	my @taxon_nos_unique;
    my $taxon_nos_string;
    my $genus_names_string;

    @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));

    my %taxon_nos_unique = ();
    foreach $taxon (@taxa) {
        @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon);
        $self->dbg("Found ".scalar(@taxon_nos)." taxon_nos for $taxon");
        if (scalar(@taxon_nos) == 0) {
            $genus_names_string .= ", ".$dbh->quote($taxon);
        } elsif (scalar(@taxon_nos) == 1) {
            my @all_taxon_nos = PBDBUtil::taxonomic_search('',$dbt,$taxon_nos[0],'return taxon nos');
            # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
            @taxon_nos_unique{@all_taxon_nos} = ();
        } else { #result > 1
            #do nothing here, quit above
        }
    }
    $taxon_nos_string = join(", ", keys %taxon_nos_unique);
    $genus_names_string =~ s/^,//;    

	my $sql;
    if ($taxon_nos_string) {
        $sql .= " OR occurrences.taxon_no IN (".$taxon_nos_string.") OR reidentifications.taxon_no IN (".$taxon_nos_string.")";
    } 
    if ($genus_names_string) {
        $sql .= " OR occurrences.genus_name IN (".$genus_names_string.") OR reidentifications.genus_name IN (".$genus_names_string.")";
    }
    $sql =~ s/^ OR //g;
    return "(".$sql.")";
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}


1;
