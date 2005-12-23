package Download;

#use strict;
use PBDBUtil;
use Classification;
use TimeLookup;
use Data::Dumper;
use DBTransactionManager;
use TaxaCache;
use CGI::Carp;

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;				# The database handle
my $dbt;				# The new and improved database object
my $q;					# Reference to the parameters
my $s;					# Reference to the session data
my $sql;				# Any SQL string
my $rs;					# Generic recordset
my $hbo;                # HTMLBuilder object
my @errors;           # Possible errors in user input
$|=1;

# These arrays contain names of possible fields to be checked by a user in the
# download form.  When writing the data out to files, these arrays are compared
# to the query params to determine the file header line and then the data to
# be written out. 
my @collectionsFieldNames = qw(authorizer enterer modifier collection_no collection_subset reference_no collection_name collection_aka country state county latdeg latmin latsec latdir latdec lngdeg lngmin lngsec lngdir lngdec latlng_basis paleolatdeg paleolatmin paleolatsec paleolatdir paleolatdec paleolngdeg paleolngmin paleolngsec paleolngdir paleolngdec altitude_value altitude_unit geogscale geogcomments period epoch stage 10mybin max_interval_no min_interval_no ma_max ma_min ma_mid emlperiod_max period_max emlperiod_min period_min emlepoch_max epoch_max emlepoch_min epoch_min emlintage_max intage_max emlintage_min intage_min emllocage_max locage_max emllocage_min locage_min zone research_group geological_group formation member localsection localbed localorder regionalsection regionalbed regionalorder stratscale stratcomments lithdescript lithadj lithification lithology1 fossilsfrom1 lithology2 fossilsfrom2 environment tectonic_setting pres_mode geology_comments collection_type collection_coverage coll_meth collection_size collection_size_unit museum collection_comments taxonomy_comments created modified release_date access_level lithification2 lithadj2 rock_censused_unit rock_censused spatial_resolution temporal_resolution feed_pred_traces encrustation bioerosion fragmentation sorting dissassoc_minor_elems dissassoc_maj_elems art_whole_bodies disart_assoc_maj_elems seq_strat lagerstatten concentration orientation preservation_quality abund_in_sediment sieve_size_min sieve_size_max assembl_comps taphonomy_comments);
my @occurrencesFieldNames = qw(authorizer enterer modifier occurrence_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name taxon_no abund_value abund_unit reference_no comments created modified plant_organ plant_organ2);
my @reidentificationsFieldNames = qw(authorizer enterer modifier reid_no genus_reso genus_name subgenus_reso subgenus_name species_reso species_name taxon_no reference_no comments created modified modified_temp plant_organ);
my @specimenFieldNames = qw(authorizer enterer modifier specimen_no reference_no specimens_measured specimen_id specimen_side specimen_part specimen_coverage measurement_source magnification specimen_count comments created modified);
my @measurementTypes = qw(average min max median error error_unit);
my @measurementFields =  qw(length width height diagonal inflation);
my @plantOrganFieldNames = ('unassigned','leaf','seed/fruit','axis','plant debris','marine palyn','microspore','megaspore','flower','seed repro','non-seed repro','wood','sterile axis','fertile axis','root','cuticle','multi organs');
my @refsFieldNames = qw(authorizer enterer modifier reference_no author1init author1last author2init author2last otherauthors pubyr reftitle pubtitle pubvol pubno firstpage lastpage created modified publication_type comments project_name project_ref_no);
my @paleozoic = qw(cambrian ordovician silurian devonian carboniferous permian);
my @mesoCenozoic = qw(triassic jurassic cretaceous tertiary);
my @ecoFields = (); # Note: generated at runtime in setupOutput

my $csv;
my $OUT_HTTP_DIR = "/paleodb/data";
my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};
my $DATAFILE_DIR = $ENV{DOWNLOAD_DATAFILE_DIR};
my $COAST_DIR = $ENV{MAP_COAST_DIR};
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
	$html .= $self->retellOptionsRow ( "Authorizer", $q->param("authorizer_reversed") );
	$html .= $self->retellOptionsRow ( "Output data type", $q->param("output_data") );
	$html .= $self->retellOptionsRow ( "Output data format", $q->param("output_format") );
    if ($q->param("research_group_restricted_to")) {
	    $html .= $self->retellOptionsRow ( "Research group or project", "restricted to ".$q->param("research_group"));
    } else {
	    $html .= $self->retellOptionsRow ( "Research group or project", "includes ".$q->param("research_group"));
    }
    
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

    my @lithification_group = ('lithified','poorly_lithified','unlithified','unknown');
    $html .= $self->retellOptionsGroup('Lithification','lithification_',\@lithification_group);
   
    # Lithologies or lithology
    if ( $q->param("lithology1") ) {
        $html .= $self->retellOptionsRow("Lithology: ", $q->param("include_exclude_lithology1") . " " .$q->param("lithology1"));
    } else {
        my @lithology_group = ('carbonate','mixed','siliciclastic','unknown');
        $html .= $self->retellOptionsGroup('Lithologies','lithology_',\@lithology_group);
    }

    # Environment or environments
    if ( $q->param('environment') ) {
    	$html .= $self->retellOptionsRow ( "Environment", $q->param("include_exclude_environment") . " " .$q->param("environment") );
    } else {
        my @environment_group = ('carbonate','unknown','siliciclastic','terrestrial');
        $html .= $self->retellOptionsGroup('Environments','environment_',\@environment_group);
    }    

    # Collection types
    my @collection_types_group = ('archaeological','biostratigraphic','paleoecologic','taphonomic','taxonomic','general_faunal/floral','unknown');
    $html .= $self->retellOptionsGroup('Reasons for describing included collections:','collection_type',\@collection_types_group);

	# Continents or country
	my (@continents,@paleocontinents);
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

		if ( $q->param("paleo Australia") ) 	{ push ( @paleocontinents, "Australia" ); }
		if ( $q->param("Avalonia") ) 	{ push ( @paleocontinents, "Avalonia" ); }
		if ( $q->param("Baltoscandia") ) 	{ push ( @paleocontinents, "Baltoscandia" ); }
		if ( $q->param("Kazakhstania") ) 	{ push ( @paleocontinents, "Kazakhstania" ); }
		if ( $q->param("Laurentia") ) 	{ push ( @paleocontinents, "Laurentia" ); }
		if ( $q->param("Mediterranean") ) 	{ push ( @paleocontinents, "Mediterranean" ); }
		if ( $q->param("North China") ) 	{ push ( @paleocontinents, "North China" ); }
		if ( $q->param("Precordillera") ) 	{ push ( @paleocontinents, "Precordillera" ); }
		if ( $q->param("Siberia") ) 	{ push ( @paleocontinents, "Siberia" ); }
		if ( $q->param("paleo South America") ) 	{ push ( @paleocontinents, "South America" ); }
		if ( $q->param("South China") ) 	{ push ( @paleocontinents, "South China" ); }
		if ( $#paleocontinents > -1 ) {
			$html .= $self->retellOptionsRow ( "Paleocontinents", join (  ", ", @paleocontinents ) );
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

    my @geogscale_group = ('small_collection','hand_sample','outcrop','local_area','basin','unknown');
    $html .= $self->retellOptionsGroup( "Geographic scale of collections", 'geogscale_', \@geogscale_group);

    my @stratscale_group = ('bed','group_of_beds','member','formation','group','unknown');
    $html .= $self->retellOptionsGroup("Stratigraphic scale of collections", 'stratscale_',\@stratscale_group);

	$html .= $self->retellOptionsRow ( "Lump by exact geographic coordinate?", $q->param("lump_by_coord") );
	$html .= $self->retellOptionsRow ( "Lump by formation and member?", $q->param("lump_by_mbr") );
	$html .= $self->retellOptionsRow ( "Lump by published reference?", $q->param("lump_by_ref") );
	$html .= $self->retellOptionsRow ( "Lump by time interval?", $q->param("lump_by_interval") );

    if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        $html .= $self->retellOptionsRow ( "Lump occurrences of same genus of same collection?", $q->param("lumpgenera") );
        $html .= $self->retellOptionsRow ( "Replace genus names with subgenus names?", $q->param("split_subgenera") );
        $html .= $self->retellOptionsRow ( "Replace names with senior synonyms?", $q->param("replace_with_ss") );
        $html .= $self->retellOptionsRow ( "Include occurrences that are generically indeterminate?", $q->param("indet") );
        $html .= $self->retellOptionsRow ( "Include occurrences that are specifically indeterminate?", $q->param("sp") );
    	my @genus_reso_types_group = ('aff.','cf.','ex_gr.','sensu lato','?','"');
    	$html .= $self->retellOptionsGroup('Include occurrences qualified by','genus_reso_',\@genus_reso_types_group);
        $html .= $self->retellOptionsRow ( "Include occurrences with informal names?", $q->param("informal") );
        $html .= $self->retellOptionsRow ( "Include occurrences falling outside Compendium age ranges?", $q->param("compendium_ranges") );
        $html .= $self->retellOptionsRow ( "Include occurrences without abundance data?", $q->param("without_abundance") );
        $html .= $self->retellOptionsRow ( "Minimum # of specimens to compute mean abundance", $q->param("min_mean_abundance") ) if ($q->param("min_mean_abundance"));

        my $plantOrganFieldCount = 0;
        foreach my $plantOrganField (@plantOrganFieldNames) {
            if ($q->param("plant_organ_".$plantOrganField)) {
                $plantOrganFieldCount++;
            }
        }
        if ($plantOrganFieldCount != 0 && $plantOrganFieldCount != scalar(@plantOrganFieldNames)) {
            $html .= $self->retellOptionsGroup('Include plant organs','plant_organ_',\@plantOrganFieldNames);
        }

        my @occFields = ();
        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            push (@occFields, 'class_name') if ($q->param("occurrences_class_name") eq "YES");
            push (@occFields, 'order_name') if ($q->param("occurrences_order_name") eq "YES");
            push (@occFields, 'family_name') if ($q->param("occurrences_family_name") eq "YES");
        }
        if ($q->param('output_data') =~ /occurrences|specimens/) {
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
        } elsif ($q->param('output_data') eq 'genera') {
            push @occFields, 'genus_name';
        } elsif ($q->param('output_data') eq 'species') {
            push @occFields, 'genus_name';
            push @occFields, 'subgenus_name' if ($q->param('occurences_subgenus_name'));
            push @occFields, 'species_name';
        }

        if (@occFields) {
            $html .= $self->retellOptionsRow ( "Occurrence output fields", join ( "<BR>", @occFields) );
        }
        
        # Ecology fields
        if (@ecoFields) {
            $html .= $self->retellOptionsRow ( "Ecology output fields", join ( "<BR>", @ecoFields) );
        }
        
    } 

    if ($q->param('output_data') =~ /occurrences|collections|specimens/) {
        # collection table fields
        my @collFields = ( "collection_no");
        foreach my $field ( @collectionsFieldNames ) {
            if ( $q->param ( 'collections_'.$field ) ) { push ( @collFields, 'collections_'.$field ); }
        }
	    $html .= $self->retellOptionsRow ( "Collection output fields", join ( "<BR>", @collFields) );
    }

    # specimen/measurement table fields
    my @specimenFields = ();

    if ($q->param('output_data') eq 'specimens') {
        foreach my $f (@specimenFieldNames) {
            if ($q->param('specimens_'.$f)) {
                push (@specimenFields, 'specimens_'.$f); 
            }
        }
    } else {
        if ($q->param('specimens_specimens_measured')) {
            push @specimenFields,'specimens_specimens_measured';
        }
    } 

    foreach my $t (@measurementTypes) {
        foreach my $f (@measurementFields) {
            if ($q->param('specimens_'.$t) && $q->param('specimens_'.$f)) {
                push (@specimenFields,'specimens_'.$t."_".$f);
            }   
        }
    }
    $html .= $self->retellOptionsRow ( "Specimen output fields", join ( "<BR>", @specimenFields) ) if (@specimenFields);

	$html .= "\n</table>\n";

	$html =~ s/_/ /g;
	print $html;
}


# Formats a bunch of checkboses of a query parameter (name=value) as a table row in html.
sub retellOptionsGroup {
    my ($self,$message,$form_prepend,$group_ref) = @_;
    my $missing = 0;

    foreach my $item (@$group_ref) {
        $missing = 1 if (!$q->param($form_prepend.$item));
    }

    if ($missing) {
        my $options = "";
        foreach my $item (@$group_ref) {
            if ($q->param($form_prepend.$item)) {
                $options .= ", ".$item;
            }
        }
        $options =~ s/_/ /g;
        $options =~ s/^, //;
        return $self->retellOptionsRow($message,$options);
    } else {
        return "";
    }
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
	        @fieldNames = grep {!/^(paleo(lat|lng)(deg|dec|min|sec|dir)|ma_max|ma_min|ma_mid|epoch|stage|period|10mybin)$/} @collectionsFieldNames;
        } else {
	        @fieldNames = @collectionsFieldNames;
        }
	} elsif($tableName eq "occurrences") {
        @fieldNames = @occurrencesFieldNames;
	} elsif($tableName eq "reidentifications") {
        @fieldNames = @reidentificationsFieldNames;
	} elsif($tableName eq "specimens") {
        @fieldNames = @specimenFieldNames;
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
                    if ($fieldName !~ /authorizer|enterer|modifier/) {
				        push(@outFields, "re.$fieldName as reid_$fieldName");
                    }
                } else {
                    push(@outFields, "original.$fieldName");
                }
			}
            # Rename field to avoid conflicts with fieldnames in collections table
			elsif ($tableName eq 'occurrences') {
                if ($isSQL) {
                    if ($fieldName !~ /authorizer|enterer|modifier/) {
				        push(@outFields, "o.$fieldName as occ_$fieldName");
                    }
                } else {
				    push(@outFields, "occurrences.$fieldName");
                }
			} elsif ($tableName eq 'specimens') {
                if ($isSQL) {
                    if ($fieldName !~ /authorizer|enterer|modifier/) {
				        push(@outFields, "s.$fieldName specimens_$fieldName");
                    }
                } else {
				    push(@outFields, "specimens.$fieldName");
                }
			} else { 
                if ($isSQL) {
                    if ($fieldName !~ /authorizer|enterer|modifier/) {
                        push(@outFields,"c.$fieldName");
                    }
                } else {
                    push(@outFields,"collections.$fieldName");
                }
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

        foreach my $key (keys %impliedFields) {
            if ($q->param($key) eq "YES") {
                foreach my $field (@{$impliedFields{$key}}) {
                    if (!exists $fieldExists{$field}) {
                        push @outFields, "c.$field";
                        $fieldExists{$field} = 1;
                    }    
                }    
            }
        }  
        if (!exists $fieldExists{'reference_no'}) {
            push @outFields, "c.reference_no";
        }
    } elsif ($isSQL && $tableName eq "occurrences") {    
		# subgenus name must be downloaded if subgenera are to be
		#  treated as genera
		# an amusing hack, if I do say so myself JA 18.8.04
		if ( $q->param('split_subgenera') eq 'YES' && !exists $fieldExists{'subgenus_name'} )	{
            push @outFields, "o.subgenus_name as occ_subgenus_name";
		}
        if (!exists $fieldExists{'reference_no'}) {
            push @outFields, "o.reference_no as occ_reference_no";
        }
    } elsif ($isSQL && $tableName eq "reidentifications") {    
		if ( $q->param('split_subgenera') eq 'YES' && !exists $fieldExists{'subgenus_name'} )	{
            push @outFields, "re.subgenus_name as reid_subgenus_name";
		}
        if (!exists $fieldExists{'reference_no'}) {
            push @outFields, "re.reference_no as reid_reference_no";
        }
    }    
	return @outFields;
}

# 6.7.02 JA
sub getCountryString {
	my $self = shift;
    my $country_sql = "";
	my $in_str = "";

    #Country or Countries
    if ($q->param('country')) {
        my $country_term = $dbh->quote($q->param('country'));
        if ($q->param('include_exclude_country') eq "exclude") {
            $country_sql = qq| c.country NOT LIKE $country_term |;
        } else {
            $country_sql = qq| c.country LIKE $country_term |;
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
        my @regions = (	'North America', 
                        'South America', 
                        'Europe', 
                        'Africa',
                        'Antarctica', 
                        'Asia', 
                        'Australia');

        foreach my $region (@regions) {
            if($q->param($region) eq 'YES') {
                $in_str = $in_str .','. "'".join("','", split(/\t/,$REGIONS{$region}))."'";
            }
            $in_str =~ s/^,//; 
        }
        if ($in_str) {
            $country_sql = qq| c.country IN ($in_str) |;
        } else {
            $country_sql = "";
        }    
    }
	return $country_sql;
}

# 15.8.05 JA
sub getPlateString	{
	my $self = shift;
	my $plate_sql = "";
	my @plates = ();

	if ( $q->param('paleo Australia') ne "YES" && $q->param('Avalonia') ne "YES" && $q->param('Baltoscandia') ne "YES" && $q->param('Kazakhstania') ne "YES" && $q->param('Laurentia') ne "YES" && $q->param('Mediterranean') ne "YES" && $q->param('North China') ne "YES" && $q->param('Precordillera') ne "YES" && $q->param('Siberia') ne "YES" && $q->param('paleo South America') ne "YES" && $q->param('South China') ne "YES" )	{
		return "";
	}

	if ( $q->param('paleo Australia') eq "YES" && $q->param('Avalonia') eq "YES" && $q->param('Baltoscandia') eq "YES" && $q->param('Kazakhstania') eq "YES" && $q->param('Laurentia') eq "YES" && $q->param('Mediterranean') eq "YES" && $q->param('North China') eq "YES" && $q->param('Precordillera') eq "YES" && $q->param('Siberia') eq "YES" && $q->param('paleo South America') eq "YES" && $q->param('South China') eq "YES" )	{
		return "";
	}

	if ( $q->param('paleo Australia') eq "YES" )	{
		push @plates , (801);
	}
	if ( $q->param('Avalonia') eq "YES" )	{
		push @plates , (315);
	}
	if ( $q->param('Baltoscandia') eq "YES" )	{
		push @plates , (301);
	}
	if ( $q->param('Kazakhstania') eq "YES" )	{
		push @plates , (402);
	}
	if ( $q->param('Laurentia') eq "YES" )	{
		push @plates , (101);
	}
	if ( $q->param('Mediterranean') eq "YES" )	{
		push @plates , (304,305,707,714);
	}
	if ( $q->param('North China') eq "YES" )	{
		push @plates , (604);
	}
	if ( $q->param('Precordillera') eq "YES" )	{
		push @plates , (291);
	}
	if ( $q->param('Siberia') eq "YES" )	{
		push @plates , (401);
	}
	if ( $q->param('paleo South America') eq "YES" )	{
		push @plates , (201);
	}
	if ( $q->param('South China') eq "YES" )	{
		push @plates , (611);
	}

	for my $p ( @plates )	{
		$platein{$p} = "Y";
	}

	if ( ! open ( PLATES, "$COAST_DIR/plateidsv2.lst" ) ) {
		print "<font color='red'>Skipping plates.</font> Error message is $!<BR><BR>\n";
		return;
	}

	$plate_sql = " ( ";
	while (<PLATES>)	{
		s/\n//;
		my ($pllng,$pllat,$plate) = split /,/,$_;
		if ( $platein{$plate} )	{
			if ( $pllng < 0 )	{
				$pllng = abs($pllng);
				$pllngdir = "West";
			} else	{
				$pllngdir = "East";
			}
			if ( $pllat < 0 )	{
				$pllat = abs($pllat);
				$pllatdir = "South";
			} else	{
				$pllatdir = "North";
			}
			$plate_sql .= " OR ( lngdeg=$pllng AND lngdir='$pllngdir' AND latdeg=$pllat AND latdir='$pllatdir' ) ";
		}
	}
	$plate_sql .= " ) ";
	$plate_sql =~ s/^ \(  OR / \( /;

	return $plate_sql;
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
		    return " ( c.collection_no IN ( " . join (',',@colls) . " ) )";
		} else {
            return " ( c.collection_no IN (0) )";
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
	# likewise, if all the boxes are checked do nothing
	elsif ( $lithified && $poorly_lithified && $unlithified && $unknown )	{
            return "";
        }
	# all other combinations
	if ( $lithified )	{
		$lithvals = " c.lithification='lithified' ";
	}
	if ( $poorly_lithified )	{
		$lithvals .= " OR c.lithification='poorly lithified' ";
	}
	if ( $unlithified )	{
		$lithvals .= " OR c.lithification='unlithified' ";
	}
	if ( $unknown )	{
		$lithvals .= " OR c.lithification='' OR c.lithification IS NULL ";
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
		    return qq| (c.lithology1 NOT LIKE $lith_term OR c.lithology1 IS NULL) AND|.
                   qq| (c.lithology2 NOT LIKE $lith_term OR c.lithology2 IS NULL)|;
        } else {
		    return qq| (c.lithology1 LIKE $lith_term OR c.lithology2 LIKE $lith_term) |;
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
                $lith_sql =  qq| ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) |;
            }
            # mixed only
            elsif  ( ! $carbonate && $mixed && ! $silic )	{
                $lith_sql = qq| ( c.lithology1 IN ($mixed_str) OR c.lithology2 IN ($mixed_str) OR ( c.lithology1 IN ($carbonate_str) && c.lithology2 IN ($silic_str) ) OR ( c.lithology1 IN ($silic_str) && c.lithology2 IN ($carbonate_str) ) ) |;
            }
            # siliciclastic only
            elsif  ( ! $carbonate && ! $mixed && $silic )	{
                $lith_sql = qq| ( c.lithology1 IN ($silic_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($silic_str) ) ) |;
            }
            # carbonate and siliciclastic but NOT mixed
            elsif  ( $carbonate && ! $mixed && $silic )	{
                $lith_sql = qq| ( ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) OR ( c.lithology1 IN ($silic_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($silic_str) ) ) ) |;
            }
            # carbonate and mixed
            elsif  ( $carbonate && $mixed && ! $silic )	{
                $lith_sql = qq| ( ( c.lithology1 IN ($mixed_str) OR c.lithology2 IN ($mixed_str) OR ( c.lithology1 IN ($carbonate_str) && c.lithology2 IN ($silic_str) ) OR ( c.lithology1 IN ($silic_str) && c.lithology2 IN ($carbonate_str) ) ) OR ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) ) |;
            }
            # mixed and siliciclastic
            elsif  ( ! $carbonate && $mixed && $silic )	{
                $lith_sql = qq| ( ( c.lithology1 IN ($mixed_str) OR c.lithology2 IN ($mixed_str) OR ( c.lithology1 IN ($carbonate_str) && c.lithology2 IN ($silic_str) ) OR ( c.lithology1 IN ($silic_str) && c.lithology2 IN ($carbonate_str) ) ) OR ( c.lithology1 IN ($silic_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($silic_str) ) ) ) |;
            }
            
            # lithologies where both fields are null
            if ($unknown) {
                my $unknown_sql = qq|((c.lithology1 IS NULL OR c.lithology1='') AND (c.lithology2 IS NULL OR c.lithology2=''))|;
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
		    return qq| (c.environment NOT IN ($environment) OR c.environment IS NULL) |;
        } else {
		    return qq| c.environment IN ($environment)|;
        }
    } else {
        my $carbonate_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_carbonate'}});
        my $siliciclastic_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_siliciclastic'}});
        my $terrestrial_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'environment_terrestrial'}});
        if (! $q->param("environment_carbonate") || ! $q->param("environment_siliciclastic") || 
            ! $q->param("environment_terrestrial")) {
            if ( $q->param("environment_carbonate") ) {
                $env_sql .= " OR c.environment IN ($carbonate_str)";
            }
            if ( $q->param("environment_siliciclastic") ) {
                $env_sql .= " OR c.environment IN ($siliciclastic_str)";
            }
            if ( $q->param("environment_carbonate") && $q->param("environment_siliciclastic") )	{
		$env_sql .= " OR c.environment IN ('marine indet.')";
            }
            if ( $q->param("environment_terrestrial") ) {
                $env_sql .= " OR c.environment IN ($terrestrial_str)";
            }
            if ( $q->param("environment_unknown")) {
                $env_sql .= " OR c.environment = '' OR c.environment IS NULL"; 
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
            $geogscales = qq| c.geogscale IN ($geogscales) |;
            if ( $q->param('geogscale_unknown')) {
                $geogscales = " (".$geogscales."OR c.geogscale IS NULL)";
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
            $stratscales = qq| c.stratscale IN ($stratscales) |;
            if ($q->param('stratscale_unknown')) {
                $stratscales = " (".$stratscales."OR c.stratscale IS NULL)";
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
            $colltypes = qq| c.collection_type IN ($colltypes) |;
            if ($q->param('collection_type_unknown')) {
                $colltypes = "(".$colltypes." OR c.collection_type IS NULL)";
            }
        }
    }
    return $colltypes;
}

sub getGenusResoString{
	my $self = shift;
	my $resos = "";
    if ( ! $q->param('genus_reso_aff.') || ! $q->param('genus_reso_cf.') || ! $q->param('genus_reso_ex_gr.') || ! $q->param('genus_reso_?') || ! $q->param('genus_reso_"') ) { 
        if ( $q->param('genus_reso_aff.') )	{
            $resos = "'aff.'";
        }
        if ( $q->param('genus_reso_cf.') )	{
            $resos = ",'cf.'";
        }
        if ( $q->param('genus_reso_ex_gr.') )	{
            $resos .= ",'ex gr.'";
        }
        if ( $q->param('genus_reso_sensu_lato') )	{
            $resos .= ",'sensu lato'";
        }
        if ( $q->param('genus_reso_?') )	{
            $resos .= ",'?'";
        }
        if ( $q->param('genus_reso_"') )	{
            $resos .= ",'\"'";
        }
        $resos =~ s/^,//;
        if ( $resos )	{
            $resos = qq| o.genus_reso IN ($resos) |;
            $resos = " (" . $resos ."OR o.genus_reso IS NULL OR o.genus_reso='')";
        }
    }
    return $resos;
}


# Returns three where array: The first is applicable to the both the occs
# and reids table, the second is occs only, and the third is reids only
sub getOccurrencesWhereClause {
	my $self = shift;

    my (@all_where,@occ_where,@reid_where);

	if ( $q->param('pubyr') > 0 )	{
		if ( $q->param('published_before_after') eq "before" )	{
			$pubyrrelation = "<";
		} else	{
			$pubyrrelation = ">";
		}
		push @all_where,"r.pubyr".$pubyrrelation.$q->param('pubyr');
	}

    my $plantOrganFieldCount = 0;
    my @includedPlantOrgans = ();
    foreach my $plantOrganField (@plantOrganFieldNames) {
        if ($q->param("plant_organ_".$plantOrganField)) {
            $plantOrganFieldCount++;
            push @includedPlantOrgans, $dbh->quote($plantOrganField);
        }
    }
    if ($plantOrganFieldCount != 0 && $plantOrganFieldCount != scalar(@plantOrganFieldNames)) {
        my $plant_organs = join(",", @includedPlantOrgans);
        push @occ_where, "(o.plant_organ IN ($plant_organs) OR o.plant_organ2 IN ($plant_organs))";
    }  

    
    my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('authorizer_reversed'));
    my $authorizer_no = ${$dbt->getData($sql)}[0]->{'person_no'};

	push @all_where, "o.authorizer_no=".$authorizer_no if ($authorizer_no);

    push @all_where, "o.abund_value NOT LIKE \"\" AND o.abund_value IS NOT NULL" if $q->param("without_abundance") eq 'NO';

	push @occ_where, "o.species_name!='indet.'" if $q->param('indet') eq 'NO';
	push @occ_where, "o.species_name!='sp.'" if $q->param('sp') eq 'NO';
	my $genusResoString = $self->getGenusResoString();
	push @occ_where, $genusResoString if $genusResoString;
	push @occ_where, "(o.genus_reso NOT LIKE '%informal%' OR o.genus_reso IS NULL)" if $q->param('informal') eq 'NO';

	push @reid_where, "re.species_name!='indet.'" if $q->param('indet') eq 'NO';
	push @reid_where, "re.species_name!='sp.'" if $q->param('sp') eq 'NO';
	# this is kind of a hack, I admit it JA 31.7.05
	$genusResoString =~ s/o\.genus_reso/re.genus_reso/g;
	push @reid_where, $genusResoString if $genusResoString;
	push @reid_where, "(re.genus_reso NOT LIKE '%informal%' OR re.genus_reso IS NULL)" if $q->param('informal') eq 'NO';

    return (\@all_where,\@occ_where,\@reid_where);
}

sub getCollectionsWhereClause {
	my $self = shift;
    my @where = ();
	
	# This is handled by getOccurrencesWhereClause if we're getting occs data.
	if($q->param('output_data') eq 'collections'){
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('authorizer_reversed'));
        my $authorizer_no = ${$dbt->getData($sql)}[0]->{'person_no'};
		push @where, "c.authorizer_no=".$authorizer_no if ($authorizer_no);
	}
    my $resGrpRestrictedTo = ($q->param('research_group_restricted_to')) ? '1' : '0';
    my $resGrpString = PBDBUtil::getResearchGroupSQL($dbt,$q->param('research_group'),$resGrpRestrictedTo);
    push @where, $resGrpString if ($resGrpString);
	
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
				$created_string = " c.created < $created_date ";
			} else	{
				$created_string = " o.created < $created_date ";
			}
		} elsif ($q->param('created_before_after') eq "after") {
			if ( $q->param('output_data') eq 'collections' )	{
				$created_string = " c.created > $created_date ";
			} else	{
				$created_string = " o.created > $created_date ";
			}
		}
	
        push @where,$created_string;
	}

    foreach my $whereItem (
        $self->getCountryString(),
        $self->getPlateString(),
        $self->getLatLongString(),
        $self->getPaleoLatLongString(),
        $self->getIntervalString(),
        $self->getLithificationString(),
        $self->getLithologyString(),
        $self->getEnvironmentString(),
        $self->getGeogscaleString(),
        $self->getStratscaleString(),
        $self->getCollectionTypeString()) {
        push @where,$whereItem if ($whereItem);
    }

    return @where;
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
	my $p = Permissions->new ($s,$dbt);
	my @collectionHeaderCols = ( );
	my @occurrenceHeaderCols = ( );
	my @reidHeaderCols = ( );
	my $outFieldsString = '';
	my %COLLS_DONE;
	my %REFS_DONE;
    my $interval_names;


	# get the period names for the collections JA 22.2.04
	# updated to use Gradstein instead of Harland JA 5.12.05
	# based on scale 69 = Gradstein periods
	if ( $q->param('collections_period') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '69');
		%myperiod = %{$intervalInScaleRef};
	}

	# get the epoch names for the collections JA 22.2.04
	# updated to use Gradstein instead of Harland JA 5.12.05
	# based on scale 71 = Gradstein epochs
	if ( $q->param('collections_epoch') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '71');
		%myepoch = %{$intervalInScaleRef};
	}

	# get the stage names for the collections PS 08/19/2005
	# updated to use Gradstein instead of Harland JA 5.12.05
	# based on scale 73 = Gradstein epochs
	if ( $q->param('collections_stage') )	{
		my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, '73');
		%mystage = %{$intervalInScaleRef};
	}

    if ($q->param('collections_ma_max') ||
        $q->param('collections_ma_min') ||
        $q->param('collections_ma_mid')) {
        my @bounds = TimeLookup::findBoundaries($dbh,$dbt);
        %upper_bound = %{$bounds[2]};
        %lower_bound = %{$bounds[3]}; 
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

    my (@fields,@where,@occ_where,@reid_where,@taxon_where,@tables,@from,@groupby);

    @fields = ('c.authorizer_no','c.reference_no','c.collection_no','c.research_group','c.access_level',"DATE_FORMAT(c.release_date, '%Y%m%d') rd_short");
    @tables = ('collections c');
    @where = $self->getCollectionsWhereClause();

    # This confusing block relates to getting specimen measurement data that isn't
    # tied to a specific occurrence/collection - if these fields are set, we have to throw out
    # that data, since we can't know if its valid. anything that filters collections should
    # cause us to throw out this data pretty much, except record created date (hence the grep)
    # this may be a bit incomplete, gl trying to keep this logic up to date PS 07/18/2005
    if (scalar(grep(!/created/,@where)) ||
        $q->param('without_abundance') eq 'NO' ||
        $q->param('pubyr')) {
        $q->param('get_global_specimens'=>0);
        $self->dbg("get_global_specimens is 0");
    } else {
        $q->param('get_global_specimens'=>1);
    }
  
	# Getting only collection data
    if ($q->param('output_data') =~ /specimens|occurrences|collections/) {
        push @fields, $self->getOutFields('collections',TRUE);
        if ($q->param('collections_authorizer') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc1 ON c.authorizer_no=pc1.person_no";
            push @fields, 'pc1.name authorizer';
        }
        if ($q->param('collections_enterer') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc2 ON c.enterer_no=pc2.person_no";
            push @fields, 'pc2.name enterer';
        }
        if ($q->param('collections_modifier') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc3 ON c.modifier_no=pc3.person_no";
            push @fields, 'pc3.name modifier';
        }
    }

    # We'll want to join with the reid ids if we're hitting the occurrences table,
    # or if we're getting collections and filtering using the taxon_no in the occurrences table
    my $join_reids = ($q->param('output_data') =~ /occurrences|specimens|genera|species/ || $q->param('taxon_name')) ? 1 : 0;
    if ($join_reids) {
        push @tables, 'occurrences o';
        unshift @where, 'c.collection_no = o.collection_no';

        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            push @fields, 'o.occurrence_no', 
                       'o.reference_no occ_reference_no', 
                       'o.genus_reso occ_genus_reso', 
                       'o.genus_name occ_genus_name',
                       'o.species_name occ_species_name',
				       'o.taxon_no occ_taxon_no',
				       'o.abund_value occ_abund_value',
				       'o.abund_unit occ_abund_unit',
                       're.reid_no',
				       're.reference_no reid_reference_no',
				       're.genus_reso reid_genus_reso',
				       're.genus_name reid_genus_name',
				       're.species_name reid_species_name',
				       're.taxon_no reid_taxon_no';
            my ($whereref,$occswhereref,$reidswhereref) = $self->getOccurrencesWhereClause();
            push @where, @$whereref;
            push @occ_where, @$occswhereref;
            push @reid_where, @$reidswhereref;
		    push @fields, $self->getOutFields('occurrences',TRUE);
            push @fields, $self->getOutFields('reidentifications',TRUE);
            if ($q->param('occurrences_authorizer') eq 'YES') {
                push @left_joins, "LEFT JOIN person po1 ON o.authorizer_no=po1.person_no";
                push @fields, 'po1.name occ_authorizer';
                push @left_joins, "LEFT JOIN person pre1 ON re.authorizer_no=pre1.person_no";
                push @fields, 'pre1.name reid_authorizer';
            }
            if ($q->param('occurrences_enterer') eq 'YES') {
                push @left_joins, "LEFT JOIN person po2 ON o.enterer_no=po2.person_no";
                push @fields, 'po2.name occ_enterer';
                push @left_joins, "LEFT JOIN person pre2 ON re.enterer_no=pre2.person_no";
                push @fields, 'pre2.name reid_enterer';
            }
            if ($q->param('occurrences_modifier') eq 'YES') {
                push @left_joins, "LEFT JOIN person po3 ON o.modifier_no=po3.person_no";
                push @fields, 'po3.name occ_modifier';
                push @left_joins, "LEFT JOIN person pre3 ON re.modifier_no=pre3.person_no";
                push @fields, 'pre3.name reid_modifier';
            }
        } 

        if ($q->param('taxon_name')) {
            my (@occ_sql,@reid_sql);
            # Don't include @taxon_where in my () above, it needs to stay in scope
            # so it can be used much later in function
            @taxon_where = $self->getTaxonString();
            push @occ_sql, "o.$_" for @taxon_where;
            push @reid_sql, "re.$_" for @taxon_where;
            push @occ_where, "(".join(" OR ",@occ_sql).")";
            push @reid_where, "(".join(" OR ",@reid_sql).")";
        }

        if ( $q->param('pubyr') > 0) {
            push @tables, 'refs r';
            unshift @where, 'r.reference_no=o.reference_no';
        }
    }

    if ($q->param('output_data') =~ /specimens/) {
        push @fields, 's.specimen_no',
                       's.specimens_measured', 
                       's.specimen_part';
        push @tables, 'specimens s';
	    unshift @where,	'o.occurrence_no = s.occurrence_no';

		push @fields, $self->getOutFields('specimens',TRUE);

        if ($q->param('specimens_authorizer') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps1 ON s.authorizer_no=ps1.person_no";
            push @fields, 'ps1.name specimens_authorizer';
        }
        if ($q->param('specimens_enterer') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps2 ON s.enterer_no=ps2.person_no";
            push @fields, 'ps2.name specimens_enterer';
        }
        if ($q->param('specimens_modifier') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps3 ON s.modifier_no=ps3.person_no";
            push @fields, 'ps3.name specimens_modifier';
        }
    } elsif ($q->param('include_specimen_fields')) {
        if ($q->param('output_data') =~ /collections/ && !$join_reids) {
            push @left_joins , "LEFT JOIN occurrences o ON o.collection_no=c.collection_no LEFT JOIN specimens s ON s.occurrence_no = o.occurrence_no";
            push @fields,"(COUNT(DISTINCT s.specimen_no) > 0) specimens_exist";
        } else {
            push @left_joins , "LEFT JOIN specimens s ON s.occurrence_no = o.occurrence_no";
            push @fields,"(COUNT(DISTINCT s.specimen_no) > 0) specimens_exist";
        }
    }
	
    # Handle matching against secondary refs
    if($q->param('research_group') =~ /^(?:decapod|ETE|5%|1%|PACED|PGAP)$/){
        push @left_joins, "LEFT JOIN secondary_refs sr ON sr.collection_no=c.collection_no";
    }

    # Handle GROUP BY
    # This is important: don't grouping by genus_name for the obvious cases, 
    # Do the grouping in PERL, since otherwise we can't get a list of references
    # nor can we filter out old reids and rows that don't pass permissions.
	if ( $q->param('output_data') =~ /genera|species|occurrences/ )	{
        push @groupby, 'o.occurrence_no,re.reid_no';
    } elsif ($q->param('output_data') eq 'collections' && ($q->param('research_group') || $join_reids || $q->param('include_specimen_fields'))) { # = collections
       push @groupby, 'c.collection_no';
    }


    
    # Assemble the final SQL
    if ($join_reids) {
        # Reworked PS  07/15/2005
        # Instead of doing a left join on the reids table, we achieve the close to the same effect
        # with a union of the (collections,occurrences left join reids where reid_no IS NULL) UNION (collections,occurrences,reids).
        # but for the first SQL in the union, we use o.taxon_no, while in the second we use re.taxon_no
        # This has the advantage in that it can use indexes in each case, thus is super fast (rather than taking ~5-8s for a full table scan)
        # Just doing a simple left join does the full table scan because an OR is needed (o.taxon_no IN () OR re.taxon_no IN ())
        # and because you can't use indexes for tables that have been LEFT JOINED as well
        # By hitting the occ/reids tables separately, it also has the advantage in that it excludes occurrences that
        # have been reid'd into something that no longer is the taxon name and thus shouldn't show up.
        #
        # In the future: when we update to mysql 4.1, filter for "most recent" REID in a subquery, rather
        # than after the fact in the "exclude++" section of the code
        @left_joins1 = ('LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no',@left_joins);

        # This term very important.  sql1 deal with occs with NO reid, sql2 deals with only reids
        # This way WHERE terms can focus on only pruning the occurrences table for sql1 and only
        # pruning on the reids table for sql2 PS 07/15/2005
        @where1 = (@where,@occ_where,"re.reid_no IS NULL");
        $sql1 = "SELECT ".join(",",@fields).
               " FROM " .join(",",@tables)." ".join (" ",@left_joins1).
               " WHERE ".join(" AND ",@where1);
        $sql1 .= " GROUP BY ".join(",",@groupby) if (@groupby);

        @where2 = ("re.occurrence_no=o.occurrence_no AND re.most_recent='YES'",@where,@reid_where);
        @tables2 = (@tables,'reidentifications re'); 
        $sql2 = "SELECT ".join(",",@fields).
               " FROM " .join(",",@tables2)." ".join (" ",@left_joins).
               " WHERE ".join(" AND ",@where2);
        $sql2 .= " GROUP BY ".join(",",@groupby) if (@groupby);
        $sql = "($sql1) UNION ($sql2)";

        # This is a tricky part of the code. This will get records for specimens/genera
        # for which there is just a specimen measurement but there is no occurrence.  
        if ($q->param('output_data') =~ /^(?:genera|specimens|species)$/ && 
            $q->param('get_global_specimens') &&
            $q->param('taxon_name') && 
            $q->param('include_specimen_fields') ) {
            my $taxon_nos_clause = "";
            for (@taxon_where) {
                if ($_ =~ /taxon_no/) {
                    $taxon_nos_clause = $_;
                    last;
                }
            }
            if ($taxon_nos_clause) {
                my @specimen_fields = ();
                for (@fields) {
                    if ($_ =~ /^s\.|specimens_enterer|specimens_authorizer|specimens_modifier/) {
                        push @specimen_fields, $_;
                    } elsif ($_ =~ /specimens_exist/) {
                        push @specimen_fields, 1;
                    } elsif ($_ =~ /occ_taxon_no/) {
                        push @specimen_fields, 's.taxon_no occ_taxon_no';
                    } elsif ($_ =~ /occ_genus_name/) {
                        push @specimen_fields, 'substring_index(taxon_name," ",1) occ_genus_name';
                    } elsif ($_ =~ /occ_species_name/) {
                        # If taxon name containts a space, species name is whatever follow that space
                        # If no space, species name is sp. if rank is a genus, indet if its higher order
                        push @specimen_fields, 'IF(taxon_name REGEXP " ",trim(trim(leading substring_index(taxon_name," ",1) from taxon_name)),IF(taxon_rank REGEXP "genus","sp.","indet.")) occ_species_name';
                    } else {
                        push @specimen_fields, 'NULL';
                    }
                }
                my $sql3 = "SELECT ".join(",",@specimen_fields).
                         " FROM specimens s, authorities a ";
                if ($q->param('specimens_authorizer') eq 'YES') {
                    $sql3 .= " LEFT JOIN person p1 ON s.authorizer_no=p1.person_no";
                }
                if ($q->param('specimens_enterer') eq 'YES') {
                    $sql3 .= " LEFT JOIN person p2 ON s.enterer_no=p2.person_no";
                }
                if ($q->param('specimens_modifier') eq 'YES') {
                    $sql3 .= " LEFT JOIN person p3 ON s.modifier_no=p3.person_no";
                }
                $sql3 .= " WHERE s.taxon_no=a.taxon_no AND s.$taxon_nos_clause";
                $sql .= " UNION ($sql3)";

            }
        }
    } else {
        $sql = "SELECT ".join(",",@fields).
               " FROM " .join(",",@tables)." ".join (" ",@left_joins).
               " WHERE ".join(" AND ",@where);
        $sql .= " GROUP BY ".join(",",@groupby) if (@groupby);
    }
	# added this because the occurrences must be ordered by collection no or the CONJUNCT output will split up the collections JA 14.7.05
	if ( $q->param('output_data') =~ /occurrences|specimens/)	{
		$sql .= " ORDER BY collection_no";
	}
	
    #
    # Header Generation
    #
	# print column names to occurrence output file JA 19.8.01
	my @header = ();
    $sepChar = ","  if ($q->param('output_format') eq 'comma-delimited text');
    $sepChar = "\t" if ($q->param('output_format') eq 'tab-delimited text');

    if($q->param('output_data') =~ /occurrences|species|genera|specimens/) {
        push @header,'class_name' if ($q->param("occurrences_class_name") eq "YES");
        push @header,'order_name' if ($q->param("occurrences_order_name") eq "YES");
        push @header,'family_name' if ($q->param("occurrences_family_name") eq "YES");
    }

    if( $q->param('output_data') eq 'genera') {
        push @header, 'genus_name';
        # Ecology row
        push @header,@ecoFields;
    } elsif ($q->param('output_data') eq 'species') {
        push @header, 'genus_name';
        push @header,'subgenus_name' if ($q->param("occurrences_subgenus_name") eq "YES");
        push @header, 'species_name';

        # Ecology row
        push @header,@ecoFields;
    } elsif($q->param('output_data') =~ /occurrences|specimens/) {
        unshift @header, 'collection_no';
        push @header,'genus_reso','genus_name','original.genus_reso','original.genus_name';

        # Occurrence header, need this for later...
        @occurrenceHeaderCols = $self->getOutFields('occurrences');
        push @header,@occurrenceHeaderCols;
            
        # ReID header, need this for later...
        @reidHeaderCols = $self->getOutFields('reidentifications');	
        push @header,@reidHeaderCols;

        # Ecology row
        push @header,@ecoFields;
        
        # Collection header
        @collectionHeaderCols = $self->getOutFields('collections');
        push @header, @collectionHeaderCols;
        
    } else {
        unshift @header, 'collection_no';

        # Collection header
        @collectionHeaderCols = $self->getOutFields('collections');
        push @header, @collectionHeaderCols;
    }

    my @dummy_measurement_row;
    #Measurement header
    if ($q->param('include_specimen_fields')) {
        if ($q->param('output_data') eq 'specimens') {
            foreach my $f (@specimenFieldNames) {
                if ($q->param('specimens_'.$f)) {
                    push @header,'specimens.'.$f;
                    push @dummy_measurement_row,"";
                }
            }
        } else {
            if ($q->param('specimens_specimen_part')) {
                push @header,'specimens.specimen_part';
                push @dummy_measurement_row,"";
            }
            if ($q->param('specimens_specimens_measured')) {
                push @header,'specimens.specimens_measured';
                push @dummy_measurement_row,"";
            }
        } 

        foreach my $t (@measurementTypes) {
            foreach my $f (@measurementFields) {
                if ($q->param('specimens_'.$t) && $q->param('specimens_'.$f)) {
                    push @header, 'specimens.'.$t." ".$f;
                    push @dummy_measurement_row,"";
                }
            }
        }   
    }

    my $headerline = join($sepChar,@header);
	if ( $q->param('output_format') ne "CONJUNCT" )	{
		print OUTFILE "$headerline\n";
	}
	$self->dbg ( "Output header: $headerline" );
    
	#
	# Loop through the result set
    #
	$self->dbg("<b>Occurrences query:</b><br>\n$sql<BR>");

	my $sth = $dbh->prepare($sql); #|| die $self->dbg("Prepare query failed ($!)<br>");

    eval { $sth->execute() };
    if ($sth->errstr) {
        my $cgi = new CGI;
        my $errstr = "SQL error: sql($sql)"
                   . " STH err (".$sth->errstr.")";
        my $getpoststr; 
        my %params = $cgi->Vars; 
        while(my ($k,$v)=each(%params)) { $getpoststr .= "&$k=$v" if ($v ne ''); }
        $getpoststr =~ s/\n//;
        $errstr .= " GET,POST ($getpoststr)";
        croak $errstr;
    } 

	$self->dbg($sth->rows()." rows returned.<br>");

	# See if rows okay by permissions module
	my @dataRows = ( );
	my $limit = 1000000;
	my $ofRows = 0;
	$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );

	$sth->finish();
	$self->dbg("Rows that passed Permissions: number of rows $ofRows, length of dataRows array: ".@dataRows."<br>");


    # ss = senior_synonym
    my %ss_taxon_nos = ();
    my %ss_taxon_names = ();
    if ($q->param("replace_with_ss") eq 'YES' && $q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        my %all_taxa = ();
        foreach my $row (@dataRows) {
            if ($row->{'reid_taxon_no'}) {
                $all_taxa{$row->{'reid_taxon_no'}} = 1; 
            } elsif ($row->{'occ_taxon_no'}) {
                $all_taxa{$row->{'occ_taxon_no'}} = 1;
            }
        }
        my @taxon_nos = keys %all_taxa;

        if (@taxon_nos) {
            # Get senior synonyms for taxon used in this download
            # Note the t.taxon_no != t.synonym_no clause - this is here
            # so that the array only gets filled with replacement names,
            # not cluttered up with taxa who don't have a senior synonym
            my $sql = "SELECT t.taxon_no,t.synonym_no,a.taxon_name ".
                      "FROM taxa_tree_cache t, authorities a ".
                      "WHERE t.synonym_no=a.taxon_no ".
                      "AND t.taxon_no != t.synonym_no ".
                      "AND t.taxon_no IN (".join(",",@taxon_nos).")";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
               $ss_taxon_nos{$row->{'taxon_no'}} = $row->{'synonym_no'};
               # Split it into bits here and store that, optimazation
               my @name_bits = Taxon::splitTaxon($row->{'taxon_name'});
               $ss_taxon_names{$row->{'taxon_no'}} = \@name_bits;
            }
        }
    }

	# next do a quick hit to get some by-taxon and by-collection stats
	#  ... and so on
    # also records genera names, useful for later loop
	my %lumpseen;
	my %occseen;
    my %genusseen;
    my %specimens_occ_list;
	foreach my $row ( @dataRows ){
		my $exclude = 0;
        my $lump = 0;
            
        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            if ($row->{'reid_no'}) {
                foreach my $field (@reidentificationsFieldNames) {
                    $row->{'original_'.$field}=$row->{'occ_'.$field};
                    $row->{'occ_'.$field}=$row->{'reid_'.$field};
                }
            }
            # Replace with senior_synonym_no PS 11/1/2005
            if ( $q->param('replace_with_ss') eq 'YES' ) {
                if ($ss_taxon_nos{$row->{'occ_taxon_no'}}) {
                    my ($genus,$subgenus,$species,$subspecies) = @{$ss_taxon_names{$row->{'occ_taxon_no'}}};
                    #print "$row->{occurrence_no}, SENIOR SYN FOR $row->{occ_genus_name}/$row->{occ_subgenus_name}/$row->{occ_species_name}/$row->{occ_subspecies_name} IS $genus/$subgenus/$species/$subspecies<BR>";

                    $row->{'original_taxon_no'} = $row->{'occ_taxon_no'};
                    $row->{'occ_taxon_no'} = $ss_taxon_nos{$row->{'occ_taxon_no'}};
                    $row->{'original_genus_name'} = $row->{'occ_genus_name'};
                    $row->{'occ_genus_name'} = $genus;
                    $row->{'original_subgenus_name'} = $row->{'occ_subgenus_name'};
                    $row->{'original_species_name'} = $row->{'occ_species_name'};
                    $row->{'original_subspecies_name'} = $row->{'occ_subspecies_name'};
                    if ($species) {
                        $row->{'occ_subgenus_name'} = $subgenus;
                        $row->{'occ_species_name'} = $species;
                        $row->{'occ_subspecies_name'} = $subspecies;
                    }
                }
            }
		    # raise subgenera to genus level JA 18.8.04
		    if ( $q->param('split_subgenera') eq 'YES' && $row->{occ_subgenus_name} )	{
			    $row->{occ_genus_name} = $row->{occ_subgenus_name};
		    }
            # get rid of occurrences of genera either (1) not in the
            #  Compendium or (2) falling outside the official Compendium
            #  age range JA 27.8.04
            if ( $q->param('compendium_ranges') eq 'NO' )	{
                if ( ! $incompendium{$row->{occ_genus_name}.$mybin{$row->{collection_no}}} )	{
                    $exclude++;
    #				print "exc. compendum ".$row->{'collection_no'};
                }
            }

            # Lump by genera or genera/collection here in code, so we can get refs + filter reids
            # correctly above
            if ($exclude == 0 && ($q->param('output_data') eq 'genera' || $q->param('output_data') eq 'species' || $q->param('lumpgenera') eq 'YES')) {
                my $genus_string;
                if ($q->param('lumpgenera') eq 'YES') {
                    $genus_string .= $row->{collection_no};
                }
                if ($q->param('output_data') =~ /species/) {
                    $genus_string .= $row->{'occ_genus_name'}.$row->{'occ_species_name'};
                } else {
                    $genus_string .= $row->{'occ_genus_name'};
                }
                if ($genusseen{$genus_string}) {
                    $lump++;
                } else {
                    $genusseen{$genus_string}++;
                }
            }
        }
        # lump bed/group of beds scale collections with the exact same
        #  formation/member and geographic coordinate JA 21.8.04
        if ( $exclude == 0 && ( $q->param('lump_by_coord') eq 'YES' || $q->param('lump_by_interval') eq 'YES' || $q->param('lump_by_mbr') eq 'YES' || $q->param('lump_by_ref') eq 'YES' ) )	{

            my $lump_string;

            if ( $q->param('lump_by_coord') eq 'YES' )	{
                $lump_string .= $row->{'latdeg'}.$row->{'latmin'}.$row->{'latsec'}.$row->{'latdec'}.$row->{'latdir'}.$row->{'lngdeg'}.$row->{'lngmin'}.$row->{'lngsec'}.$row->{'lngdec'}.$row->{'lngdir'};
            }
            if ( $q->param('lump_by_interval') eq 'YES' )	{
                $lump_string .= $row->{'max_interval_no'}.$row->{'min_interval_no'};
            }
            if ( $q->param('lump_by_mbr') eq 'YES' )	{
                $lump_string .= $row->{'formation'}.$row->{'member'};
            }
            if ( $q->param('lump_by_ref') eq 'YES' )	{
                $lump_string .= $row->{'reference_no'};
            }

            my $genus_string;
            if ($q->param('output_data') =~ /species/) {
                $genus_string = $row->{'occ_genus_name'}.$row->{'occ_species_name'};
            } else {
                $genus_string = $row->{'occ_genus_name'};
            }

            if ( $lumpseen{$lump_string} )	{
                # Change  the collection_no to be the same collection_no as the first
                # collection_no encountered that has the same $lump_string, so that 
                # Curve.pm will lump them together when doing a calculation
                $row->{collection_no} = $lumpseen{$lump_string};
                if ( $occseen{$row->{collection_no}.$genus_string} > 0 )	{
                    $lump++;
                }
            } else	{
                $lumpseen{$lump_string} = $row->{collection_no};
            }
            $occseen{$row->{collection_no}.$row->{occ_genus_name}}++;
        }
        if ( $exclude == 0) {
            if ($q->param('output_data') eq 'genera' || $q->param('output_data') eq 'species') {
                if ($row->{'specimens_exist'}) {
                    if ($q->param('output_data')  eq 'species') {
                        $genus_string = $row->{'occ_genus_name'}." ".$row->{'occ_species_name'};
                    } else {
                        $genus_string = $row->{'occ_genus_name'};
                    }
                    push @{$specimens_occ_list{$genus_string}}, $row->{'occurrence_no'};
                }
            }
            $REFS_USED{$row->{'reference_no'}}++;
            # make a master list of all collection numbers that are
            #  used, so we can properly get all the primary AND secondary
            #  refs for those collections JA 16.7.04
			$COLLECTIONS_USED{$row->{collection_no}}++;
            $REFS_USED{$row->{'occ_reference_no'}}++ if ($row->{'occ_reference_no'});
            $REFS_USED{$row->{'reid_reference_no'}}++ if ($row->{'reid_reference_no'});

        }
		if ( $exclude == 0 && $lump == 0)	{
			push @tempDataRows, $row;

    		# cumulate number of collections including each genus
            if ($q->param('output_data') =~ /species/) {
			    $totaloccs{$row->{occ_genus_name}." ".$row->{occ_species_name}}++;
            } else {
			    $totaloccs{$row->{occ_genus_name}}++;
            }
	    	# need these two for ecology lookup below
			$totaloccsbyno{$row->{occ_taxon_no}}++;
    		# cumulate number of specimens per collection, and number of
	    	#  times a genus has abundance counts at all
			if ( ( $row->{occ_abund_unit} eq "specimens" || $row->{occ_abund_unit} eq "individuals" ) && ( $row->{occ_abund_value} > 0 ) )	{
				$nisp{$row->{collection_no}} = $nisp{$row->{collection_no}} + $row->{occ_abund_value};
			}
		}
	}
	@dataRows = @tempDataRows;
    $self->dbg("primary refs: " . (join(",",keys %REFS_USED))); 

	# now hit the secondary refs table, mark all of those references as
	#  having been used, and print all the refs JA 16.7.04
	my @collnos = keys %COLLECTIONS_USED;
	if ( @collnos )	{
		my $sql = "SELECT reference_no FROM secondary_refs WHERE collection_no IN (" . join (',',@collnos) . ")";
        $self->dbg("secondary refs sql: $sql"); 
		my @results = @{$dbt->getData($sql)};
		for my $row (@results)	{
			$REFS_USED{$row->{reference_no}}++;
		}
	}
    my @refnos = keys %REFS_USED;
    $self->dbg("primary+secondary refs: " . join(",",(@refnos))); 

# print the header
    print REFSFILE join (',',@refsFieldNames), "\n";

# print the refs
    if (@refnos) {
        $refFieldsSQL = join(",",map{"r.".$_} grep{!/^(?:authorizer|enterer|modifier)$/} @refsFieldNames);
        $ref_sql = "SELECT p1.name authorizer, p2.name enterer, p3.name modifier, $refFieldsSQL FROM refs r ".
                   " LEFT JOIN person p1 ON p1.person_no=r.authorizer_no" .
                   " LEFT JOIN person p2 ON p2.person_no=r.enterer_no" .
                   " LEFT JOIN person p3 ON p3.person_no=r.modifier_no" .
                   " WHERE reference_no IN (" . join (', ',@refnos) . ")";
        $self->dbg("Get ref data sql: $ref_sql"); 
        @refrefs= @{$dbt->getData($ref_sql)};
        for my $refref (@refrefs)	{
            my @refvals = ();
            for my $r (@refsFieldNames)	{
                push @refvals , $refref->{$r};
            }
            my $refLine = $self->formatRow(@refvals);
            $refLine =~ s/\r|\n/ /g;
            printf REFSFILE "%s\n",$refLine;
            $acceptedRefs++;
        }
    }
	close REFSFILE;


    # Get a list of parents for classification purposes
	my @genera_nos = keys %totaloccsbyno;

	my %master_class;
    if (@ecoFields || 
        ($q->param("output_data") =~ /occurrences|specimens|genera|species/ && 
        ($q->param("occurrences_class_name") eq "YES" || 
         $q->param("occurrences_order_name") eq "YES" || 
         $q->param("occurrences_family_name") eq "YES"))){
    	%master_class=%{TaxaCache::getParents($dbt,\@genera_nos,'array_full')};
    }

    # Sort by
    if ($q->param('output_data') =~ /^(?:genera|specimens|species)$/) {
        @dataRows = sort { $a->{'occ_genus_name'} cmp $b->{'occ_genus_name'} ||
                           $a->{'occ_species_name'} cmp $b->{'occ_species_name'}} @dataRows;
    }

	# get the higher order names associated with each genus name,
	#   then set the ecotaph values by running up the hierarchy
	# JA 28.2.04: only do this is ecotaph data were requested
	# JA 4.4.04: adapted this to use taxon numbers instead of names
    # PS 08/20/2005 - get for all higher ranks, not just common ones
    my %ecotaph;
	if (@ecoFields) {
	    %ecotaph = %{Ecology::getEcology($dbt,\%master_class,\@ecoFields)};
	}


	# main pass through the results set
	my $acceptedCount = 0;
	foreach my $row ( @dataRows ){
		my $reference_no = $row->{reference_no};
		my $collection_no = $row->{collection_no};

		my $genus_reso = $row->{occ_genus_reso};
		my $genusName = $row->{occ_genus_name};
		my $genusNo = $row->{occ_taxon_no};

		# count up occurrences per time interval bin
		if ( $q->param('time_scale') )	{
            my $interval = $intervallookup{$row->{collection_no}};
			# only use occurrences from collections that map
			#  into exactly one bin
			if ($interval) {
				$occsbybin{$interval}++;
				$occsbybintaxon{$interval}{$genusNo}++;
				if ( $occsbybintaxon{$interval}{$genusNo} == 1 )	{
					$taxabybin{$interval}++;
				}
			}
			# now things get nasty: if a field was selected to
			#  break up into categories, add to the count involving
			#  the appropriate enum value
			# WARNING: algorithm assumes that only enum fields are
			#  ever selected for processing
			if ( $q->param('binned_field') )	{
                my $rowvalue;
				if ( $q->param('binned_field') eq "ecology" )	{
				    # special processing for ecology data
                    $rowvalue= $ecotaph{$genusNo}{$ecoFields[0]};
                } else {
				    # default processing
                    $rowvalue = $row->{$q->param('binned_field')};
                }
                $occsbybinandcategory{$interval}{$rowvalue}++;
                $occsbybincattaxon{$interval}{$rowvalue.$genusNo}++;
                if ( $occsbybincattaxon{$interval}{$rowvalue.$genusNo} == 1 )	{
                    $taxabybinandcategory{$interval}{$rowvalue}++;
                }
                $occsbycategory{$rowvalue}++;
			}
		}

		# compute relative abundance proportion and add to running total
		# WARNING: sum is of logged abundances because geometric means
		#   are desired
		if ( ( $row->{occ_abund_unit} eq "specimens" || $row->{occ_abund_unit} eq "individuals" ) && ( $row->{occ_abund_value} > 0 ) )	{
            if (int($q->param('min_mean_abundance'))) {
                if ($nisp{$row->{collection_no}} >= int($q->param('min_mean_abundance'))) {
		            $numberofcounts{$row->{occ_genus_name}}++;
			        $summedproportions{$row->{occ_genus_name}} = $summedproportions{$row->{occ_genus_name}} + log( $row->{occ_abund_value} / $nisp{$row->{collection_no}} );
                } else {
                    $self->dbg("Skipping collection_no $row->{collection_no}, count $nisp{$row->{collection_no}} is below count ".$q->param('min_mean_abundance'));
                }
            } else {
		        $numberofcounts{$row->{occ_genus_name}}++;
			    $summedproportions{$row->{occ_genus_name}} = $summedproportions{$row->{occ_genus_name}} + log( $row->{occ_abund_value} / $nisp{$row->{collection_no}} );
            }
		}

		#$self->dbg("reference_no: $reference_no<br>genus_reso: $genus_reso<br>genusName: $genusName<br>collection_no: $collection_no<br>");

        # Deprecated - we do a group by in the SQL if necessary- I don't think this code does anything and I don't
        # know when was the last time it did - PS 07/13/2004
		# Only print one occurrence per collection if "collections only"
		# was checked; do this by fooling the system into lumping all
		# occurrences in a collection
		#my $tempGenus = $genusName;
		#if( $q->param('output_data') eq 'collections'){
		#	$tempGenus = '';
		#	if($COLLS_DONE{"$collection_no.$tempGenus"} == 1){
		#		next;
		#	}
			# else
		#	$COLLS_DONE{"$collection_no.$tempGenus"} = 1;
		#}
		
		my @coll_row = ();
		my @occs_row = ();
		my @reid_row = ();
        my @eco_row  = ();

        #
        # Set up occurrence and reid fields
        #
        if ($q->param("output_data") =~ /occurrences|specimens/) {
            # Put the values in the correct order since by looping through this array
            foreach my $column ( @occurrenceHeaderCols ){
                $column =~ s/^occurrences\./occ_/;
                push ( @occs_row, $row->{$column} );
            }

            # Put the values in the correct order since by looping through this array
            foreach my $column ( @reidHeaderCols ){
                $column =~ s/^original\./original_/;
                push ( @reid_row, $row->{$column} );
            }
        }

        if ($q->param("output_data") =~ /occurrences|specimens|genera|species/) {
            # Push the eco/taphonomic data, if any, onto the reid rows
            # WARNING: this only works on genus or higher-order data,
            #  assuming species won't be scored separately
            foreach my $field (@ecoFields) {
                if ($ecotaph{$genusNo}{$field}) {
                    push @eco_row, $ecotaph{$genusNo}{$field};
                } else {
                    push @eco_row, '';
                }
            }
        }


        #
        # Set up collections fields
        #

        if ($q->param("output_data") =~ /collections|specimens|occurrences/) {
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
            if ($q->param("collections_stage") eq "YES") {
                $row->{'stage'} = $mystage{$row->{'collection_no'}};
            }
            if ($q->param("collections_10mybin") eq "YES") {
                # WARNING: similar trick here in which useless legacy
                #  field locage_max is used as a placeholder for the
                #  bin name
                $row->{'10mybin'} = $mybin{$row->{'collection_no'}};
            }
            if ($q->param('collections_ma_max') eq "YES") {
                $row->{'ma_max'} = $lower_bound{$row->{'max_interval_no'}};
            }
            if ($q->param('collections_ma_min') eq "YES") {
                if ($row->{'min_interval_no'}) {
                    $row->{'ma_min'} = $upper_bound{$row->{'min_interval_no'}};
                } else {
                    $row->{'ma_min'} = $upper_bound{$row->{'max_interval_no'}};
                }
            }
            if ($q->param('collections_ma_mid') eq "YES") {
                my ($max,$min);
                $max = $lower_bound{$row->{'max_interval_no'}};
                if ($row->{'min_interval_no'}) {
                    $min = $upper_bound{$row->{'min_interval_no'}};
                } else {
                    $min = $upper_bound{$row->{'max_interval_no'}};
                }
                $row->{'ma_mid'} = ($max+$min)/2;
            }

            # Put the values in the correct order since by looping through this array
            foreach my $column ( @collectionHeaderCols ){
                $column =~ s/collections\.//;
                push ( @coll_row, $row->{$column} );
            }
        }

        my @measurement_rows = ();
        if ($q->param('include_specimen_fields')) {
            my @measurements = ();
            if ($q->param('output_data') eq 'collections' && $row->{specimens_exist}) {
                @measurements = Measurement::getMeasurements($dbt,'collection_no'=>$row->{'collection_no'});
            } elsif ($q->param('output_data') =~ /occurrences/ && $row->{specimens_exist}) {
                @measurements = Measurement::getMeasurements($dbt,'occurrence_no'=>$row->{'occurrence_no'});
            } elsif ($q->param('output_data') eq 'genera') {
                my $genus_string = "$row->{occ_genus_name}";
                if (@{$specimens_occ_list{$genus_string}}) {
                    if ($q->param('get_global_specimens')) {
                        # Note: will run into homonym issues till we figure out how to pass taxon_no
                        @measurements = Measurement::getMeasurements($dbt,'taxon_name'=>$genus_string,'get_global_specimens'=>$q->param('get_global_specimens'));
                    } else {
                        @measurements = Measurement::getMeasurements($dbt,'occurrence_list'=>$specimens_occ_list{$genus_string});
                    }
                }
            } elsif ($q->param('output_data') eq 'species') {
                my $genus_string = "$row->{occ_genus_name} $row->{occ_species_name}";
                if (@{$specimens_occ_list{$genus_string}}) {
                    if ($q->param('get_global_specimens')) {
                        # Note: will run into homonym issues till we figure out how to pass taxon_no
                        @measurements = Measurement::getMeasurements($dbt,'taxon_name'=>$genus_string,'get_global_specimens'=>$q->param('get_global_specimens'));
                    } else {
                        #print "OCC_LIST for $genus_string: ".join(", ",@{$specimens_occ_list{$genus_string}})."<BR>";
                        @measurements = Measurement::getMeasurements($dbt,'occurrence_list'=>$specimens_occ_list{$genus_string});
                    }
                }
            } elsif ($q->param('output_data') eq 'specimens') {
                $sql = "SELECT m.* FROM measurements m WHERE m.specimen_no =".$row->{'specimen_no'};
                @measurements = @{$dbt->getData($sql)};
                #Ugly hack - getMeasurementTable expect specimens joined w/measurements dataset, so add in these
                # needed fields again
                foreach (@measurements) {
                    $_->{specimens_measured} = $row->{specimens_measured};
                    $_->{specimen_no} = $row->{specimen_no};
                    $_->{specimen_part} = $row->{specimen_part};
                }
            } 

            if (@measurements) {
                # NOTE!: getMeasurementTable normally returns a 3-indexed hash with the indexes (specimen_part,measurement_type,value_measured) i.e. $p_table{'leg'}{'average'}{'length'}
                # We have to denormalize on specimen part in PERL.  This is nasty but the alternatives are just as messy (denormalize in SQL, but then abundance calculations will
                # be screwy unless you keep track of UNIQUE occurrence_nos
                $p_table = Measurement::getMeasurementTable(\@measurements);    

                while (my ($part,$m_table)=each %$p_table) {
                    my @measurement_row = ();
                    if ($q->param('output_data') eq 'specimens') {
                        foreach my $f (@specimenFieldNames) {
                            if ($q->param('specimens_'.$f)) {
                                push @measurement_row,$row->{'specimens_'.$f};
                            }
                        }  
                    } else {
                        if ($q->param('specimens_specimen_part')) {
                            push @measurement_row,$part;
                        }
                        if ($q->param('specimens_specimens_measured')) {
                            push @measurement_row,$m_table->{'specimens_measured'};
                        }
                    }

                    foreach my $t (@measurementTypes) {
                        foreach my $f (@measurementFields) {
                            if ($q->param('specimens_'.$f) && $q->param('specimens_'.$t)) {
                                if ($m_table->{$f}{$t}) {
                                    $value = sprintf("%.4f",$m_table->{$f}{$t});
                                    $value =~ s/0+$//;
                                    $value =~ s/\.$//;    
                                    push @measurement_row,$value;
                                } else {
                                    push @measurement_row,'';
                                }
                            }
                        }
                    }

                    push @measurement_rows, \@measurement_row;
                }
            } 
        }

        my @final_row = ();
        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            if ($q->param("occurrences_class_name") eq "YES" || 
                $q->param("occurrences_order_name") eq "YES" ||
                $q->param("occurrences_family_name") eq "YES") {
                my @parents = @{$master_class{$row->{'occ_taxon_no'}}};
                my ($class, $order, $family) = ("","","");
                foreach my $parent (@parents) {
                    if ($parent->{'taxon_rank'} eq 'family') {
                        $family = $parent->{'taxon_name'}; 
                    } elsif ($parent->{'taxon_rank'} eq 'order') {
                        $order = $parent->{'taxon_name'};
                    } elsif ($parent->{'taxon_rank'} eq 'class') {
                        $class = $parent->{'taxon_name'};
                        last;
                    }
                }
                # Get higher order names for indets as well
                if ($row->{'occ_species_name'} =~ /indet/ && $row->{'occ_taxon_no'}) {
                    my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$row->{'occ_taxon_no'});
                    if ($taxon->{'taxon_rank'} eq 'family') {
                        $family = $taxon->{'taxon_name'}; 
                    } elsif ($taxon->{'taxon_rank'} eq 'order') {
                        $order = $taxon->{'taxon_name'};
                    } elsif ($taxon->{'taxon_rank'} eq 'class') {
                        $class = $taxon->{'taxon_name'};
                    }
                }
                
                push @final_row, $class  if ($q->param("occurrences_class_name") eq "YES");
                push @final_row, $order  if ($q->param("occurrences_order_name") eq "YES");
                push @final_row, $family if ($q->param("occurrences_family_name") eq "YES");
            }
        }
		if( $q->param('output_data') eq 'collections'){
            unshift @final_row,$collection_no;
            push @final_row,@coll_row;
		} elsif ( $q->param('output_data') eq 'genera')	{
            push @final_row, ($genusName,@eco_row);
        } elsif ( $q->param('output_data') eq 'species') {
            push @final_row, $genusName;
            push @final_row, $row->{'occ_subgenus_name'} if ($q->param("occurrences_subgenus_name") eq "YES");
            push @final_row, $row->{'occ_species_name'};
            push @final_row, @eco_row;
		} else { # occurrences/specimens
            unshift @final_row, $collection_no;
            push(@final_row,$genus_reso,$genusName,$row->{'original_genus_reso'},$row->{'original_genus_name'},@occs_row,@reid_row,@eco_row,@coll_row);
		}
		if ( $q->param('output_format') eq "CONJUNCT" )	{
			if ( $lastcoll != $collection_no )	{
				if ( $lastcoll )	{
					print OUTFILE ".\n\n";
				}
				if ( $row->{collection_name} )	{
					$row->{collection_name} =~ s/ /_/g;
					printf OUTFILE "%s\n",$row->{collection_name};
				} else	{
					print OUTFILE "Collection_$collection_no\n";
				}

                my @comments = ();
                foreach my $column ( ('collection_no',@collectionHeaderCols) ){
                    my $column_name = $column;
                    $column_name =~ s/collections\.//;
                    if ($row->{$column}) {
                        push @comments,"$column: $row->{$column}"; 
                    }
                }
                if (@comments) {
                    print OUTFILE "[".join("\n",@comments)."]\n";
                }
        
                my $level;
                my $level_value;
                if ($row->{'regionalsection'} && $row->{'regionalbed'} =~ /^[0-9]+(\.[0-9]+)*$/) {
                    $level = $row->{'regionalsection'};
                    $level_value = $row->{'regionalbed'};
                    if ($row->{'regionalorder'} eq 'top to bottom') {
                        $level_value *= -1;
                    }
                }
                if ($row->{'localsection'} && $row->{'localbed'} =~ /^[0-9]+(\.[0-9]+)*$/) {
                    $level = $row->{'localsection'};
                    $level_value = $row->{'localbed'};
                    if ($row->{'localorder'} eq 'top to bottom') {
                        $level_value *= -1;
                    }
                }
                if ($level) {
                    $level =~ s/ /_/g;
                    print OUTFILE "level: _${level}_ $level_value\n";
                }
			}
            
			if ( $row->{occ_genus_reso} && $row->{occ_genus_reso} !~ /informal/ && $row->{occ_genus_reso} !~ /"/ )	{
				printf OUTFILE "%s ",$row->{occ_genus_reso};
			}
			print OUTFILE "$genusName ";
			if ( $row->{occ_species_reso} && $row->{occ_species_reso} !~ /informal/ && $row->{occ_species_reso} !~ /"/ )	{
				printf OUTFILE "%s ",$row->{occ_species_reso};
			}
			if ( ! $row->{occ_species_name} )	{
				print OUTFILE "sp.\n";
			} else	{
				printf OUTFILE "%s\n",$row->{occ_species_name};
			}
			$lastcoll = $collection_no;
		} else	{
            if (@measurement_rows) {
                foreach my $measurement_rowref (@measurement_rows) {
                    $curLine = $self->formatRow(@final_row,@$measurement_rowref);
                    # get rid of carriage returns 24.5.04 JA
                    # failing to do this is lethal and I'm not sure why no-one
                    #  alerted me to this bug previously
                    $curLine =~ s/\r|\n/ /g;
			        print OUTFILE "$curLine\n";
                }
            } else {
                $curLine = $self->formatRow(@final_row,@dummy_measurement_row);
                $curLine =~ s/\r|\n/ /g;
                print OUTFILE "$curLine\n";
            }
		}
		$acceptedCount++;
	}


	if ( $q->param('output_format') eq "CONJUNCT" )	{
		print OUTFILE ".\n\n";
	}
	close OUTFILE;

	# print out a list of genera with total number of occurrences and average relative abundance
	if ( $q->param("output_data") =~ /occurrences|specimens/ )	{
        # This list of genera is needed abundance file far below
        my @genera = sort keys %totaloccs;
		my @abundline = ();
		open ABUNDFILE,">$OUT_FILE_DIR/$generaOutFileName";
		push @abundline, 'genus';
        if ($q->param('output_data') =~ /species/) {
            push @abundline, 'species';
        }
        push @abundline, 'collections','with abundances','geometric mean abundance';
		print ABUNDFILE join($sepChar,@abundline)."\n";
		for $g ( @genera )	{
			@abundline = ();
            if ($q->param('output_data') =~ /species/) {
                my ($genus,$species)=split(/ /,$g);
			    push @abundline, $genus, $species, $totaloccs{$g}, sprintf("%d",$numberofcounts{$g});
            } else {
			    push @abundline, $g, $totaloccs{$g}, sprintf("%d",$numberofcounts{$g});
            }
            
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
            @intervalnames = TimeLookup::getTenMYBins();
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
	if ( $q->param("output_data") =~ /occurrences|specimens/ )	{
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
	elsif($outputType eq 'CONJUNCT')	{
		$outFileExtension = 'conjunct';
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
	if ( ! $authorizer )	{
		if ( $q->param("yourname") )	{
			$authorizer = $q->param("yourname");
		} else	{
			$authorizer = "unknown";
		}
	}
	$authorizer =~ s/(\s|\.|[^A-Za-z0-9])//g;
	$occsOutFileName = $authorizer . "-occs.$outFileExtension";
	$generaOutFileName = $authorizer . "-genera.$outFileExtension";
	if ( $q->param("output_data") eq 'collections')	{
		$occsOutFileName = $authorizer . "-cols.$outFileExtension";
	} elsif ($q->param("output_data") eq 'specimens') {
        $occsOutFileName = $authorizer . "-specimens.".$outFileExtension;
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
    # Need to get the species_name field as well
    if ($q->param('output_data') =~ /species/) {
        $q->param('occurrences_species_name'=>'YES');
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
    # So they get included if the corresponding occurrences checkbox is set
    foreach my $field (@reidentificationsFieldNames) {
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

    # Specimen measurements implied fields
    @fields = $q->param();
    foreach my $f (@fields) {
        if ($f =~ /^specimens_/) {
            $q->param('include_specimen_fields'=>1);
            last;
        }
    }
    if ($q->param('specimens_error')) {
        $q->param('specimens_error_unit'=>'YES');
    }

    # Generate warning for taxon with homonyms
    my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));
    foreach my $taxon (@taxa) {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon);
        if (scalar(@taxon_nos)  > 1) {
            push @errors, "The taxon name '$taxon' is ambiguous and belongs to multiple taxonomic hierarchies. Right the download script can't distinguish between these different cases. If this is a problem email <a href='mailto: alroy\@nceas.ucsb.edu'>John Alroy</a>.";
        }
    } 

    # Generate these fields on the fly
    @ecoFields = ();
    if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        for(1..6) {
            if ($q->param("ecology$_")) {
                push @ecoFields, $q->param("ecology$_");
            }
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

# renamed from getGenusNames to getTaxonString to reflect changes in how this works PS 01/06/2004
sub getTaxonString {
	my $self = shift;

	my @taxon_nos_unique;
    my $taxon_nos_string;
    my $genus_names_string;

    @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));

    my @sql_bits;
    my %taxon_nos_unique = ();
    foreach my $taxon (@taxa) {
        @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon);
        $self->dbg("Found ".scalar(@taxon_nos)." taxon_nos for $taxon");
        if (scalar(@taxon_nos) == 0) {
            push @sql_bits, "genus_name like ".$dbh->quote($taxon);
        } elsif (scalar(@taxon_nos) == 1) {
            my @all_taxon_nos = TaxaCache::getChildren($dbt,$taxon_nos[0]);
            # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
            @taxon_nos_unique{@all_taxon_nos} = ();
        } else { #result > 1
            #do nothing here, quit above
        }
    }
    push @sql_bits, "taxon_no IN (".join(", ",keys(%taxon_nos_unique)).")";
    return @sql_bits;
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}


1;
