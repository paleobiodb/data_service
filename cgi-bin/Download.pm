package Download;

#use strict;
use PBDBUtil;
use Classification;
use TimeLookup;
use Data::Dumper;
use DBTransactionManager;
use TaxaCache;
use CGI::Carp;

use strict;

# Flags and constants
my $DEBUG=0;
$|=1;

# These arrays contain names of possible fields to be checked by a user in the
# download form.  When writing the data out to files, these arrays are compared
# to the query params to determine the file header line and then the data to
# be written out. 
my @collectionsFieldNames = qw(authorizer enterer modifier collection_subset reference_no collection_name collection_aka country state county tectonic_plate_id latdeg latmin latsec latdir latdec lngdeg lngmin lngsec lngdir lngdec latlng_basis paleolatdeg paleolatmin paleolatsec paleolatdir paleolatdec paleolngdeg paleolngmin paleolngsec paleolngdir paleolngdec altitude_value altitude_unit geogscale geogcomments period epoch subepoch stage 10mybin max_interval min_interval ma_max ma_min ma_mid emlperiod_max period_max emlperiod_min period_min emlepoch_max epoch_max emlepoch_min epoch_min emlintage_max intage_max emlintage_min intage_min emllocage_max locage_max emllocage_min locage_min zone research_group geological_group formation member localsection localbed localbedunit localorder regionalsection regionalbed regionalbedunit regionalorder stratscale stratcomments lithdescript lithadj lithification lithology1 fossilsfrom1 lithology2 fossilsfrom2 environment tectonic_setting pres_mode geology_comments collection_type collection_coverage coll_meth collection_size collection_size_unit museum collection_comments taxonomy_comments created modified release_date access_level lithification2 lithadj2 rock_censused_unit rock_censused spatial_resolution temporal_resolution feed_pred_traces encrustation bioerosion fragmentation sorting dissassoc_minor_elems dissassoc_maj_elems art_whole_bodies disart_assoc_maj_elems seq_strat lagerstatten concentration orientation preservation_quality abund_in_sediment sieve_size_min sieve_size_max assembl_comps taphonomy_comments);
my @occFieldNames = qw(authorizer enterer modifier occurrence_no abund_value abund_unit reference_no comments created modified plant_organ plant_organ2);
my @occTaxonFieldNames = qw(genus_reso genus_name subgenus_reso subgenus_name species_reso species_name taxon_no);
my @reidFieldNames = qw(authorizer enterer modifier reid_no reference_no comments created modified modified_temp plant_organ);
my @reidTaxonFieldNames = qw(genus_reso genus_name subgenus_reso subgenus_name species_reso species_name taxon_no);
my @specimenFieldNames = qw(authorizer enterer modifier specimen_no reference_no specimens_measured specimen_id specimen_side specimen_part specimen_coverage measurement_source magnification specimen_count comments created modified);
my @measurementTypes = qw(average min max median error error_unit);
my @measurementFields =  qw(length width height diagonal inflation);
my @plantOrganFieldNames = ('unassigned','leaf','seed/fruit','axis','plant debris','marine palyn','microspore','megaspore','flower','seed repro','non-seed repro','wood','sterile axis','fertile axis','root','cuticle','multi organs');
my @refsFieldNames = qw(authorizer enterer modifier reference_no author1init author1last author2init author2last otherauthors pubyr reftitle pubtitle pubvol pubno firstpage lastpage created modified publication_type comments project_name project_ref_no);
my @paleozoic = qw(cambrian ordovician silurian devonian carboniferous permian);
my @mesoCenozoic = qw(triassic jurassic cretaceous tertiary);
my @ecoFields = (); # Note: generated at runtime in setupQueryFields

my $OUT_HTTP_DIR = "/paleodb/data";
my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};
my $DATAFILE_DIR = $ENV{DOWNLOAD_DATAFILE_DIR};
my $COAST_DIR = $ENV{MAP_COAST_DIR};

my (@form_errors,@form_warnings);

sub new {
    my ($class,$dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;
    my $sepChar = ',';
    
    if($q->param('output_format') =~ /tab/i) {
        $sepChar = "\t";
    } 
    
    my $csv = Text::CSV_XS->new({
        'quote_char'  => '"',
        'escape_char' => '"',
        'sep_char'    => $sepChar,
        'binary'      => 1
    });

    my $name = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
    my $filename = PBDBUtil::getFilename($name);


    my $p = Permissions->new($s,$dbt);

    my $self = {'dbh'=>$dbh,
                'dbt'=>$dbt,
                'q'=>$q,
                'p'=>$p,
                's'=>$s,
                'hbo'=>$hbo, 
                'csv'=>$csv, 
                'setup_query_fields_called'=>0,
                'filename'=>$filename};
    bless $self, $class;
    return $self;
}

# Main handling routine
sub buildDownload {
    my $self = shift;
    my $q = $self->{'q'};

    print '<div align="center"><h2>The Paleobiology Database: Download Results</h2></div>';
    print $self->retellOptions();

    my ($lumpedResults,$allResults) = $self->queryDatabase();

    my ($refsCount,$taxaCount,$scaleCount,$mainCount) = (0,0,0,scalar(@$lumpedResults));
    my ($refsFile,$taxaFile,$scaleFile,$mainFile) = ('','','','');
    
    if ( $q->param('time_scale') ) {
        ($scaleCount,$scaleFile) = $self->printScaleFile($allResults);
    }

    if ( $q->param("output_data") =~ /occurrences|specimens/ ) {
        ($taxaCount,$taxaFile) = $self->printAbundFile($allResults);
    }

    ($refsCount,$refsFile) = $self->printRefsFile($allResults);
  
    if ($q->param('output_format') =~ /tab|csv/) {
        ($mainCount,$mainFile) = $self->printCSV($lumpedResults);
    } else {
        ($mainCount,$mainFile) = $self->printCONJUNCT($lumpedResults);
    }

    if (@form_warnings) {
        print Debug::printWarnings(\@form_warnings);
    } 

    # Tell what happened
    print '<div align="center">';
    print '<table border=0 width=600><tr><td>';
    print '<h3 class="darkList" style="margin-bottom: 0em;">Output files</h3>'; 
    print "$mainCount ".$q->param("output_data")." were printed to <a href=\"$OUT_HTTP_DIR/$mainFile\">$mainFile</a><br>\n";
    if ( $q->param("output_data") =~ /occurrences|specimens/ ) {
        print "$taxaCount taxonomic names were printed to <a href=\"$OUT_HTTP_DIR/$taxaFile\">$taxaFile</a><br>\n";
    }
    print "$refsCount references were printed to <a href=\"$OUT_HTTP_DIR/$refsFile\">$refsFile</a><br>\n";
    if ( $q->param('time_scale') )    {
        print "$scaleCount time intervals were printed to <a href=\"$OUT_HTTP_DIR/$scaleFile\">$scaleFile</a><br>\n";
    }
    print '</table>';
    print '<p align="center" style="white-space: nowrap;"><b><a href="bridge.pl?action=displayDownloadForm">Do another download</a> -';
    print '<a href="bridge.pl?action=displayCurveForm">Generate diversity curves</a></b></p></div>';
}


# Prints out the options which the user selected in summary form.
sub retellOptions {
    my $self = shift;
    my $q = $self->{'q'};
    # Call as needed
    $self->setupQueryFields() if (! $self->{'setup_query_fields_called'});

    my $html = '<div align="center"><table border=0 width=600>';
    $html .= '<tr><td colspan=2 class="darkList"><h3>Download criteria</h3></td></tr>';

    # authorizer added 30.6.04 JA (left out by mistake?) 
    $html .= $self->retellOptionsRow ( "Output data type", $q->param("output_data") );
    $html .= $self->retellOptionsRow ( "Output data format", $q->param("output_format") );
    $html .= $self->retellOptionsRow ( "Authorizer", $q->param("authorizer_reversed") );
    if ($q->param('research_group')) {
        if ($q->param("research_group_restricted_to")) {
            $html .= $self->retellOptionsRow ( "Research group or project", "restricted to ".$q->param("research_group"));
        } else {
            $html .= $self->retellOptionsRow ( "Research group or project", "includes ".$q->param("research_group"));
        }
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
    if ( $q->param("taxon_name") !~ /[ ,]/ )    {
        $html .= $self->retellOptionsRow ( "Taxon name", $q->param("taxon_name") );
    } else    {
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
    if ( $q->param("country") )    {
        $html .= $self->retellOptionsRow ( "Country", $q->param("include_exclude_country") . " " . $q->param("country") );
    }
    else    {
        if ( $q->param("global") )             { push ( @continents, "global" ); }
        if ( $q->param("Africa") )             { push ( @continents, "Africa" ); }
        if ( $q->param("Antarctica") )         { push ( @continents, "Antarctica" ); }
        if ( $q->param("Asia") )             { push ( @continents, "Asia" ); }
        if ( $q->param("Australia") )         { push ( @continents, "Australia" ); }
        if ( $q->param("Europe") )             { push ( @continents, "Europe" ); }
        if ( $q->param("North America") )     { push ( @continents, "North America" ); }
        if ( $q->param("South America") )     { push ( @continents, "South America" ); }
        if ( $#continents > -1 ) {
            $html .= $self->retellOptionsRow ( "Continents", join (  ", ", @continents ) );
        }

        if ( $q->param("paleo Australia") )     { push ( @paleocontinents, "Australia" ); }
        if ( $q->param("Avalonia") )     { push ( @paleocontinents, "Avalonia" ); }
        if ( $q->param("Baltoscandia") )     { push ( @paleocontinents, "Baltoscandia" ); }
        if ( $q->param("Kazakhstania") )     { push ( @paleocontinents, "Kazakhstania" ); }
        if ( $q->param("Laurentia") )     { push ( @paleocontinents, "Laurentia" ); }
        if ( $q->param("Mediterranean") )     { push ( @paleocontinents, "Mediterranean" ); }
        if ( $q->param("North China") )     { push ( @paleocontinents, "North China" ); }
        if ( $q->param("Precordillera") )     { push ( @paleocontinents, "Precordillera" ); }
        if ( $q->param("Siberia") )     { push ( @paleocontinents, "Siberia" ); }
        if ( $q->param("paleo South America") )     { push ( @paleocontinents, "South America" ); }
        if ( $q->param("South China") )     { push ( @paleocontinents, "South China" ); }
        if ( $#paleocontinents > -1 ) {
            $html .= $self->retellOptionsRow ( "Paleocontinents", join (  ", ", @paleocontinents ) );
        }
    }

    # all the boundaries must be given
    my @ranges = ([$q->param('latmin1'),$q->param('latmax1'),-90,90],
               [$q->param('lngmin1'),$q->param('lngmax1'),-180,180],
               [$q->param('latmin2'),$q->param('latmax2'),-90,90],
               [$q->param('lngmin2'),$q->param('lngmax2'),-180,180],
               [$q->param('paleolatmin1'),$q->param('paleolatmax1'),-90,90],
               [$q->param('paleolngmin1'),$q->param('paleolngmax1'),-180,180],
               [$q->param('paleolatmin2'),$q->param('paleolatmax2'),-90,90],
               [$q->param('paleolngmin2'),$q->param('paleolngmax2'),-180,180]);
    my @range_descriptions;

    # Create text descriptions of what the user has entered for the diff. ranges above
    for(my $i=0;$i<@ranges;$i++) {
        my ($min,$max,$lower,$upper) = @{$ranges[$i]};
        my $description = "";

	# user may have confused min and max, so swap them JA 5.7.06
	if ( $min > $max )	{
		my $temp = $min;
		$min = $max;
		$max = $temp;
	}

        # If either the min or max value has been changed from an upper bound
        # (i.e. its been modified by the user) than we want to print a description
        # message
        if (($min > $lower && $min < $upper) ||
            ($max > $lower && $max < $upper)) {

            # Case 1: they've both been changed, print a range
            # Case 2: one has been change, its the minimum value.  max is unchanged
            # Case 3: (else): one has been changed, its the maximum value. min is unchanged
            if ($min > $lower && $max < $upper) {
                $description .= "$min&deg; to $max&deg;";
            } elsif ($min > $lower) {
                $description .= "greater than $min&deg; "
            } else {
                $description .= "less than $max&deg; "
            }
        }

        # Now store this text description, which will be a empty string if the user
        # didn't edit this range, else will be a text string describing the changes
        $range_descriptions[$i]=$description;
    }
  
    # Print out the text generated above
    $html .= $self->retellOptionsRow("Latitudinal range", $range_descriptions[0]); 
    $html .= $self->retellOptionsRow("Longitudinal range", $range_descriptions[1]); 
    $html .= $self->retellOptionsRow("Additional latitudinal range", $range_descriptions[2]); 
    $html .= $self->retellOptionsRow("Additional longitudinal range", $range_descriptions[3]); 
    $html .= $self->retellOptionsRow("Paleolatitudinal range", $range_descriptions[4]); 
    $html .= $self->retellOptionsRow("Paleolongitudinal range", $range_descriptions[5]); 
    $html .= $self->retellOptionsRow("Additional paleolatitudinal range", $range_descriptions[6]); 
    $html .= $self->retellOptionsRow("Additional paleolongitudinal range", $range_descriptions[7]); 
    
    
    $html .= $self->retellOptionsRow ( "Lump lists of same county & formation?", $q->param("lumplist") );

    my @geogscale_group = ('small_collection','hand_sample','outcrop','local_area','basin','unknown');
    $html .= $self->retellOptionsGroup( "Geographic scale of collections", 'geogscale_', \@geogscale_group);

    my @stratscale_group = ('bed','group_of_beds','member','formation','group','unknown');
    $html .= $self->retellOptionsGroup("Stratigraphic scale of collections", 'stratscale_',\@stratscale_group);

    $html .= $self->retellOptionsRow ( "Lump by exact geographic coordinate?", $q->param("lump_by_coord") );
    $html .= $self->retellOptionsRow ( "Lump by formation and member?", $q->param("lump_by_mbr") );
    $html .= $self->retellOptionsRow ( "Lump by published reference?", $q->param("lump_by_ref") );
    $html .= $self->retellOptionsRow ( "Lump by time interval?", $q->param("lump_by_interval") );
    $html .= $self->retellOptionsRow ( "Restrict to collection(s): ",$q->param("collection_no"));
    $html .= $self->retellOptionsRow ( "Exclude collections with subset collections? ",$q->param("exclude_superset"));
    $html .= $self->retellOptionsRow ( "Exclude collections with ".$q->param("occurrence_count_qualifier")." than",$q->param("occurrence_count")." occurrences") if ($q->param("occurrence_count") =~ /^\d+$/);

    if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        $html .= $self->retellOptionsRow ( "Lump occurrences of same genus of same collection?", $q->param("lump_genera") );
        $html .= $self->retellOptionsRow ( "Replace genus names with subgenus names?", $q->param("split_subgenera") );
        $html .= $self->retellOptionsRow ( "Replace names with reidentifications?", $q->param("replace_with_reid") );
        $html .= $self->retellOptionsRow ( "Replace names with senior synonyms?", $q->param("replace_with_ss") );
        $html .= $self->retellOptionsRow ( "Include occurrences that are generically indeterminate?", $q->param('indet') );
        $html .= $self->retellOptionsRow ( "Include occurrences that are specifically indeterminate?", $q->param('sp') );
        my @genus_reso_types_group = ('aff.','cf.','ex_gr.','n. gen.','sensu lato','?','"');
        $html .= $self->retellOptionsGroup('Include occurrences qualified by','genus_reso_',\@genus_reso_types_group);
        $html .= $self->retellOptionsRow ( "Include occurrences with informal names?", $q->param("informal") );
        $html .= $self->retellOptionsRow ( "Include occurrences falling outside Compendium age ranges?", $q->param("compendium_ranges") );
        $html .= $self->retellOptionsRow ( "Include occurrences without abundance data?", $q->param("without_abundance") );
        $html .= $self->retellOptionsRow ( "Exclude classified occurrences?", $q->param("classified") );
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
        if ($q->param('output_data') =~ /occurrences|specimens/) {
            foreach my $field ( @occTaxonFieldNames) {
                if( $q->param ( "occurrences_".$field ) ){ 
                    if ($field =~ /family_name|order_name|class_name/) {
                        push ( @occFields, $field ); 
                    } else {
                        push ( @occFields, "occurrences_".$field ); 
                    }
                }
            }
            foreach my $field ( @reidTaxonFieldNames ) {
                if( $q->param ( "occurrences_".$field) ){ 
                    push ( @occFields, "original_".$field ); 
                }
            }
            foreach my $field ( @occFieldNames ) {
                if( $q->param ( "occurrences_".$field) ){ 
                    push ( @occFields, "occurrences_".$field ); 
                }
            }
            foreach my $field ( @reidFieldNames ) {
                if( $q->param ( "occurrences_".$field) ){ 
                    push ( @occFields, "original_".$field ); 
                }
            }
        } elsif ($q->param('output_data') eq 'genera') {
            push @occFields, 'genus_name';
        } elsif ($q->param('output_data') eq 'species') {
            push @occFields, 'genus_name';
            push @occFields, 'subgenus_name' if ($q->param('occurrences_subgenus_name'));
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

    $html .= "</table></div>";

    $html =~ s/_/ /g;
    return $html;
}


# Formats a bunch of checkboses of a query parameter (name=value) as a table row in html.
sub retellOptionsGroup {
    my ($self,$message,$form_prepend,$group_ref) = @_;
    my $q = $self->{'q'};
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


# 6.7.02 JA
sub getCountryString {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbh = $self->{'dbh'};

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

        my %REGIONS;
        while (<REGIONS>) {
            chomp();
            my ($region, $countries) = split(/:/, $_, 2);
            $countries =~ s/'/\\'/g;
            $REGIONS{$region} = $countries;
        }
        close REGIONS;
        # Add the countries within selected regions
        my @regions = ( 'North America', 
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
sub getPlateString    {
    my $self = shift;
    my $q = $self->{'q'};
    my $plate_sql = "";
    my @plates = ();

    if ( $q->param('paleo Australia') ne "YES" && $q->param('Avalonia') ne "YES" && $q->param('Baltoscandia') ne "YES" && $q->param('Kazakhstania') ne "YES" && $q->param('Laurentia') ne "YES" && $q->param('Mediterranean') ne "YES" && $q->param('North China') ne "YES" && $q->param('Precordillera') ne "YES" && $q->param('Siberia') ne "YES" && $q->param('paleo South America') ne "YES" && $q->param('South China') ne "YES" )    {
        return "";
    }

    if ( $q->param('paleo Australia') eq "YES" && $q->param('Avalonia') eq "YES" && $q->param('Baltoscandia') eq "YES" && $q->param('Kazakhstania') eq "YES" && $q->param('Laurentia') eq "YES" && $q->param('Mediterranean') eq "YES" && $q->param('North China') eq "YES" && $q->param('Precordillera') eq "YES" && $q->param('Siberia') eq "YES" && $q->param('paleo South America') eq "YES" && $q->param('South China') eq "YES" )    {
        return "";
    }

    if ( $q->param('paleo Australia') eq "YES" )    {
        push @plates , (801);
    }
    if ( $q->param('Avalonia') eq "YES" )    {
        push @plates , (315);
    }
    if ( $q->param('Baltoscandia') eq "YES" )    {
        push @plates , (301);
    }
    if ( $q->param('Kazakhstania') eq "YES" )    {
        push @plates , (402);
    }
    if ( $q->param('Laurentia') eq "YES" )    {
        push @plates , (101);
    }
    if ( $q->param('Mediterranean') eq "YES" )    {
        push @plates , (304,305,707,714);
    }
    if ( $q->param('North China') eq "YES" )    {
        push @plates , (604);
    }
    if ( $q->param('Precordillera') eq "YES" )    {
        push @plates , (291);
    }
    if ( $q->param('Siberia') eq "YES" )    {
        push @plates , (401);
    }
    if ( $q->param('paleo South America') eq "YES" )    {
        push @plates , (201);
    }
    if ( $q->param('South China') eq "YES" )    {
        push @plates , (611);
    }

    my %platein;
    foreach my $p ( @plates )    {
        $platein{$p} = "Y";
    }

    if ( ! open ( PLATES, "$COAST_DIR/plateidsv2.lst" ) ) {
        print "<font color='red'>Skipping plates.</font> Error message is $!<BR><BR>\n";
        return;
    }

    $plate_sql = " ( ";
    while (<PLATES>)    {
        s/\n//;
        my ($pllng,$pllat,$plate) = split /,/,$_;
        my ($pllngdir,$pllatdir);
        if ( $platein{$plate} )    {
            if ( $pllng < 0 )    {
                $pllng = abs($pllng);
                $pllngdir = "West";
            } else    {
                $pllngdir = "East";
            }
            if ( $pllat < 0 )    {
                $pllat = abs($pllat);
                $pllatdir = "South";
            } else    {
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
sub getPaleoLatLongString    {
    my $self = shift;
    my $q = $self->{'q'};
    
    my $coord_sql = "";

    # all the boundaries must be given
    foreach my $i (1..2) {
        my $latmin = $q->param('paleolatmin'.$i);
        my $latmax = $q->param('paleolatmax'.$i);
        my $lngmin = $q->param('paleolngmin'.$i);
        my $lngmax = $q->param('paleolngmax'.$i);

	# user may have confused min and max, so swap them JA 5.7.06
	if ( $latmin > $latmax )	{
		my $temp = $latmin;
		$latmin = $latmax;
		$latmax = $temp;
	}
	if ( $lngmin > $lngmax )	{
		my $temp = $lngmin;
		$lngmin = $lngmax;
		$lngmax = $temp;
	}

        # if all are blank, just return (no parameters may have been passed in if this wasn't called form a script
        if ($latmin =~ /^\s*$/ && $latmax =~ /^\s*$/ && $lngmin =~ /^\s*$/ && $lngmax =~ /^\s*$/) {
            next;
        }

        # all the boundaries must be given
        if ( $latmin !~ /^-?\d+$/ || $latmax !~ /^-?\d+$/ || $lngmin !~ /^-?\d+$/ || $lngmax !~ /^-?\d+$/)    {
            push @form_errors,"Paleolatitude and paleolongitude must be positive or negative integer values";
            next;
        }
        # at least one of the boundaries must be non-trivial
        if ( $latmin <= -90 && $latmax >= 90 && $lngmin <= -180 && $lngmax >= 180 )    {
            next;
        }

        my @clauses = ();
        if ($latmin > -90) {
            push @clauses,"paleolat >= $latmin";
        }
        if ($latmax < 90 ) {
            push @clauses, "paleolat <= $latmax";
        }
        if ($lngmin > -180 ) {
            push @clauses, "paleolng >= $lngmin";
        }
        if ($lngmax < 180 ) {
            push @clauses, "paleolng <= $lngmax";
        }
        $coord_sql .= " OR (".join(" AND ",@clauses).")";
    }
    $coord_sql =~ s/^ OR//;
    if ($coord_sql) {
        $coord_sql = '('.$coord_sql.')';
    }
    return $coord_sql;
}

# 29.4.04 JA
sub getLatLongString    {
    my $self = shift;
    my $q = $self->{'q'};

    my $latlongclause = "";
    foreach my $i (1..2) {
        my $latmin = $q->param('latmin'.$i);
        my $latmax = $q->param('latmax'.$i);
        my $lngmin = $q->param('lngmin'.$i);
        my $lngmax = $q->param('lngmax'.$i);

	# user may have confused min and max, so swap them JA 5.7.06
	if ( $latmin > $latmax )	{
		my $temp = $latmin;
		$latmin = $latmax;
		$latmax = $temp;
	}
	if ( $lngmin > $lngmax )	{
		my $temp = $lngmin;
		$lngmin = $lngmax;
		$lngmax = $temp;
	}

        my $abslatmin = abs($latmin);
        my $abslatmax = abs($latmax);
        my $abslngmin = abs($lngmin);
        my $abslngmax = abs($lngmax);

        if ($latmin =~ /^\s*$/ && $latmax =~ /^\s*$/ && $lngmin =~ /^\s*$/ && $lngmax =~ /^\s*$/) {
            next;
        }   

        # all the boundaries must be given
        if ( $latmin !~ /^-?\d+$/ || $latmax !~ /^-?\d+$/ || $lngmin !~ /^-?\d+$/ || $lngmax !~ /^-?\d+$/)    {
            push @form_errors,"Latitude and longitude must be positive or negative integer values";
            next;
        }
        # at least one of the boundaries must be non-trivial
        if ( $latmin <= -90 && $latmax >= 90 && $lngmin <= -180 && $lngmax >= 180 )    {
            next;
        }

        $latlongclause .= " OR (";
        if ( $latmin >= 0 )    {
            $latlongclause .= "latdeg>=$abslatmin AND latdir='North'";
        } else    {
            $latlongclause .= "((latdeg<$abslatmin AND latdir='South') OR latdir='North')";
        }
        $latlongclause .= " AND ";
        if ( $latmax >= 0 )    {
            $latlongclause .= "((latdeg<$abslatmax AND latdir='North') OR latdir='South')";
        } else    {
            $latlongclause .= "latdeg>=$abslatmax AND latdir='South'";
        }
        $latlongclause .= " AND ";
        if ( $lngmin >= 0 )    {
            $latlongclause .= "lngdeg>=$abslngmin AND lngdir='East'";
        } else    {
            $latlongclause .= "((lngdeg<$abslngmin AND lngdir='West') OR lngdir='East')";
        }
        $latlongclause .= " AND ";
        if ( $lngmax >= 0 )    {
            $latlongclause .= "((lngdeg<$abslngmax AND lngdir='East') OR lngdir='West')";
        } else    {
            $latlongclause .= "lngdeg>=$abslngmax AND lngdir='West'";
        }
        $latlongclause .= ")";
    }
    $latlongclause =~ s/^ OR//;
    if ($latlongclause) {
        $latlongclause = '('.$latlongclause.')';
    }


    return $latlongclause;
}

sub getIntervalString    {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbh = $self->{'dbh'};
    my $dbt = $self->{'dbt'};

    my $max = ($q->param('max_interval_name') || "");
    my $min = ($q->param('min_interval_name') || "");
    my $eml_max  = ($q->param("max_eml_interval") || "");  
    my $eml_min  = ($q->param("min_eml_interval") || "");  

    # return immediately if the user already selected a full time scale
    #  to bin the data
    if ( $q->param('time_scale') )    {
        return "";
    }

    if ( $max )    {
        my $use_mid=0;
        if ($q->param("use_midpoints")) {
            $use_mid = 1;
        }
        my ($intervals,$errors,$warnings) = TimeLookup::processLookup($dbh, $dbt, $eml_max, $max, $eml_min, $min,'',$use_mid);
        @form_errors = @$errors;
        @form_warnings = @$warnings;
        my $intervals_sql = join(", ",@$intervals);
        # -1 to prevent crashing on blank string
        return " (c.max_interval_no IN (-1,$intervals_sql) AND c.min_interval_no IN (0,$intervals_sql))"
    }

    

    return "";
}

# JA 11.4.05
sub getLithificationString    {
    my $self = shift;
    my $q = $self->{'q'};

    my $lithified = $q->param('lithification_lithified');
    my $poorly_lithified = $q->param('lithification_poorly_lithified');
    my $unlithified = $q->param('lithification_unlithified');
    my $unknown = $q->param('lithification_unknown');
    my $lithif_sql = "";
    my $lithvals = "";
        # if all the boxes were unchecked, just return (idiot proofing)
        if ( ! $lithified && ! $poorly_lithified && ! $unlithified && ! $unknown )    {
            return "";
        }
    # likewise, if all the boxes are checked do nothing
    elsif ( $lithified && $poorly_lithified && $unlithified && $unknown )    {
            return "";
        }
    # all other combinations
    if ( $lithified )    {
        $lithvals = " c.lithification='lithified' ";
    }
    if ( $poorly_lithified )    {
        $lithvals .= " OR c.lithification='poorly lithified' ";
    }
    if ( $unlithified )    {
        $lithvals .= " OR c.lithification='unlithified' ";
    }
    if ( $unknown )    {
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
sub getLithologyString    {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbh = $self->{'dbh'};

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
        if ( ! $carbonate && ! $mixed && ! $silic && ! $unknown)    {
            return "";
        }
        # only do something if some of the boxes aren't checked
        if  ( ! $carbonate || ! $mixed || ! $silic || ! $unknown)    {

            my $silic_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_siliciclastic'}});
            my $mixed_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_mixed'}});
            my $carbonate_str = join(",", map {"'".$_."'"} @{$self->{'hbo'}{'SELECT_LISTS'}{'lithology_carbonate'}});
            
            # the logic is basically different for every combination,
            #  so go through all of them
            # carbonate only
            if  ( $carbonate && ! $mixed && ! $silic )    {
                $lith_sql =  qq| ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) |;
            }
            # mixed only
            elsif  ( ! $carbonate && $mixed && ! $silic )    {
                $lith_sql = qq| ( c.lithology1 IN ($mixed_str) OR c.lithology2 IN ($mixed_str) OR ( c.lithology1 IN ($carbonate_str) && c.lithology2 IN ($silic_str) ) OR ( c.lithology1 IN ($silic_str) && c.lithology2 IN ($carbonate_str) ) ) |;
            }
            # siliciclastic only
            elsif  ( ! $carbonate && ! $mixed && $silic )    {
                $lith_sql = qq| ( c.lithology1 IN ($silic_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($silic_str) ) ) |;
            }
            # carbonate and siliciclastic but NOT mixed
            elsif  ( $carbonate && ! $mixed && $silic )    {
                $lith_sql = qq| ( ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) OR ( c.lithology1 IN ($silic_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($silic_str) ) ) ) |;
            }
            # carbonate and mixed
            elsif  ( $carbonate && $mixed && ! $silic )    {
                $lith_sql = qq| ( ( c.lithology1 IN ($mixed_str) OR c.lithology2 IN ($mixed_str) OR ( c.lithology1 IN ($carbonate_str) && c.lithology2 IN ($silic_str) ) OR ( c.lithology1 IN ($silic_str) && c.lithology2 IN ($carbonate_str) ) ) OR ( c.lithology1 IN ($carbonate_str) AND ( c.lithology2 IS NULL OR c.lithology2='' OR c.lithology2 IN ($carbonate_str) ) ) ) |;
            }
            # mixed and siliciclastic
            elsif  ( ! $carbonate && $mixed && $silic )    {
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
    my $q = $self->{'q'};
    my $hbo = $self->{'hbo'};
    my $dbh = $self->{'dbh'};

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
            ! $q->param("environment_terrestrial") || ! $q->param("environment_unknown")) {
            if ( $q->param("environment_carbonate") ) {
                $env_sql .= " OR c.environment IN ($carbonate_str)";
            }
            if ( $q->param("environment_siliciclastic") ) {
                $env_sql .= " OR c.environment IN ($siliciclastic_str)";
            }
            if ( $q->param("environment_carbonate") && $q->param("environment_siliciclastic") )    {
                $env_sql .= " OR c.environment IN ('marine indet.')";
            }
            if ( $q->param("environment_terrestrial") ) {
                $env_sql .= " OR c.environment IN ($terrestrial_str)";
            }
            if ( $q->param("environment_unknown")) {
                $env_sql .= " OR c.environment = '' OR c.environment IS NULL"; 
            }
            $env_sql =~ s/^ OR//;
            if ($env_sql) {
                $env_sql = '('.$env_sql.')';
            }
        }
   }

    return $env_sql;
}

sub getGeogscaleString{
    my $self = shift;
    my $q = $self->{'q'};

    my $geogscales = "";
    if (! $q->param('geogscale_small_collection') || !$q->param('geogscale_hand_sample') || ! $q->param('geogscale_outcrop') || ! $q->param('geogscale_local_area') ||
        ! $q->param('geogscale_basin') || ! $q->param('geogscale_unknown')) { 
        if ( $q->param('geogscale_hand_sample') )    {
            $geogscales = "'hand sample'";
        }
        if ( $q->param('geogscale_small_collection') )    {
            $geogscales = ",'small collection'";
        }
        if ( $q->param('geogscale_outcrop') )    {
            $geogscales .= ",'outcrop'";
        }
        if ( $q->param('geogscale_local_area') )    {
            $geogscales .= ",'local area'";
        }
        if ( $q->param('geogscale_basin') )    {
            $geogscales .= ",'basin'";
        }
        if ( $q->param('geogscale_unknown') ) {
            $geogscales .= ",''";
        }    
        $geogscales =~ s/^,//;
        if ( $geogscales )    {
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
    my $q = $self->{'q'};

    my $stratscales = "";

    if (! $q->param('stratscale_bed') || ! $q->param('stratscale_group_of_beds') || ! $q->param('stratscale_member') ||
        ! $q->param('stratscale_formation') || ! $q->param('stratscale_group') || ! $q->param('stratscale_unknown')) {
        if ( $q->param('stratscale_bed') )    {
            $stratscales = "'bed'";
        }
        if ( $q->param('stratscale_group_of_beds') )    {
            $stratscales .= ",'group of beds'";
        }
        if ( $q->param('stratscale_member') )    {
            $stratscales .= ",'member'";
        }
        if ( $q->param('stratscale_formation') )    {
            $stratscales .= ",'formation'";
        }
        if ( $q->param('stratscale_group') )    {
            $stratscales .= ",'group'";
        }
        if ( $q->param('stratscale_unknown') ) {
            $stratscales .= ",''";
        }
       
        $stratscales =~ s/^,//;
        if ( $stratscales )    {
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
    my $q = $self->{'q'};

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
        if ( $colltypes)    {
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
    my $q = $self->{'q'};

    my $resos = "";
    if ( !$q->param('genus_reso_n. gen.') || ! $q->param('genus_reso_aff.') || ! $q->param('genus_reso_cf.') || ! $q->param('genus_reso_ex_gr.') || ! $q->param('genus_reso_?') || ! $q->param('genus_reso_"') ) { 
        if ( $q->param('genus_reso_aff.') )    {
            $resos .= ",'aff.'";
            if ($q->param('informal') eq 'YES') {
                $resos .= ",'informal aff.'";
            }
        }
        if ( $q->param('genus_reso_cf.') )    {
            $resos .= ",'cf.'";
            if ($q->param('informal') eq 'YES') {
                $resos .= ",'informal cf.'";
            }
        }
        if ( $q->param('genus_reso_ex_gr.') )    {
            $resos .= ",'ex gr.'";
        }
        if ( $q->param('genus_reso_sensu_lato') )    {
            $resos .= ",'sensu lato'";
        }
        if ( $q->param('genus_reso_?') )    {
            $resos .= ",'?'";
        }
        if ( $q->param('genus_reso_"') )    {
            $resos .= ",'\"'";
        }
        if ( $q->param('genus_reso_n. gen.') ) {
            $resos .= ",'n. gen.'";
        }
        $resos =~ s/^,//;
        if ( $resos )    {
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
    my $q = $self->{'q'};
    my $dbh = $self->{'dbh'};
    my $dbt = $self->{'dbt'};

    my (@all_where,@occ_where,@reid_where);

    if ( $q->param('pubyr') > 0 )    {
        my $pubyrrelation = ">";
        if ( $q->param('published_before_after') eq "before" )    {
            $pubyrrelation = "<";
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

# I'm not sure this is going to work if reidentifications are not used
    if ($q->param("classified") =~ /unclassified/i) {
        push @occ_where, "o.taxon_no=0";
        push @reid_where, "re.taxon_no=0";
    } elsif ($q->param("classified") =~ /classified/i) {
        push @occ_where, "o.taxon_no != 0";
        push @reid_where, "re.taxon_no != 0";
    }

   
    if ($q->param('authorizer_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('authorizer_reversed')));
        my $authorizer_no = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @all_where, "o.authorizer_no=".$authorizer_no if ($authorizer_no);
    }

    push @all_where, "o.abund_value NOT LIKE \"\" AND o.abund_value IS NOT NULL" if $q->param("without_abundance") eq 'NO';

    push @occ_where, "o.species_name NOT LIKE '%indet.%'" if $q->param('indet') ne 'YES';
    push @occ_where, "o.species_name NOT LIKE '%sp.%'" if $q->param('sp') ne 'YES';
    my $genusResoString = $self->getGenusResoString();
    push @occ_where, $genusResoString if $genusResoString;
    push @occ_where, "(o.genus_reso NOT LIKE '%informal%' OR o.genus_reso IS NULL)" if $q->param('informal') ne 'YES';

# nasty hack: if we're not going to use the reIDs, then don't worry about
#  whether they meet the indet. and sp. tests JA 16.4.06
    #if ( $q->param('replace_with_reid') ne 'NO' )	{
    push @reid_where, "re.species_name NOT LIKE '%indet.%'" if $q->param('indet') ne 'YES';
    push @reid_where, "re.species_name NOT LIKE '%sp.%'" if $q->param('sp') ne 'YES';
    #} else	{
    #    push @reid_where, "o.species_name NOT LIKE '%indet.%'" if $q->param('indet') ne 'YES';
    #    push @reid_where, "o.species_name NOT LIKE '%sp.%'" if $q->param('sp') ne 'YES';
    #}
    # this is kind of a hack, I admit it JA 31.7.05
    $genusResoString =~ s/o\.genus_reso/re.genus_reso/g;
    push @reid_where, $genusResoString if $genusResoString;
    push @reid_where, "(re.genus_reso NOT LIKE '%informal%' OR re.genus_reso IS NULL)" if $q->param('informal') ne 'YES';

    return (\@all_where,\@occ_where,\@reid_where);
}

sub getCollectionsWhereClause {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbt = $self->{'dbt'};
    my $dbh = $self->{'dbh'};

    my @where = ();
    
    # This is handled by getOccurrencesWhereClause if we're getting occs data.
    if($q->param('output_data') eq 'collections' && $q->param('authorizer_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('authorizer_reversed')));
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
                 
        if ( length $day == 1 )    {
            $day = "0".$q->param('date'); #prepend a zero if only one digit.
        }
        
        # note, this should really be handled by a function?  maybe in bridge.pl.
        my $created_date = $dbh->quote($q->param('year')."-".$month."-".$day." 00:00:00");
        # note, the version of mysql on flatpebble needs the 000000 at the end, but the
        # version on the linux box doesn't need it.  weird.                         
    
        my $created_string;
        # if so, did they want to look before or after?
        if ($q->param('created_before_after') eq "before") {
            if ( $q->param('output_data') eq 'collections' )    {
                $created_string = " c.created < $created_date ";
            } else    {
                $created_string = " o.created < $created_date ";
            }
        } elsif ($q->param('created_before_after') eq "after") {
            if ( $q->param('output_data') eq 'collections' )    {
                $created_string = " c.created > $created_date ";
            } else    {
                $created_string = " o.created > $created_date ";
            }
        }
    
        push @where,$created_string;
    }

    if ($q->param("collection_no") =~ /\d/) {
        # Clean it up
        my @collection_nos = split(/[^0-9]/,$q->param('collection_no'));
        @collection_nos = map {int($_)} @collection_nos;
        push @where, "c.collection_no IN (".join(",",@collection_nos).")";
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
        $self->getCollectionTypeString(),
        $self->getSubsetString()) {
        push @where,$whereItem if ($whereItem);
    }

    return @where;
}


sub getSubsetString {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbt = $self->{'dbt'};

    my $str = "";
    # Maybe this isn't super scalable (just gets all collections that have at least one subset) but its
    # fast enough as this is only 260 collections or so right now (PS 2/13/2005);
    # maybe update this to a subselect if mysql ever gets upgraded
    if ($q->param('exclude_superset') =~ /YES/i) {
        my $sql = "select distinct collection_subset from collections where collection_subset is not null";
        my @results = @{$dbt->getData($sql)};
        my $collections = join(", ",map{$_->{'collection_subset'}} @results);
        $str = "c.collection_no NOT IN ($collections)";
    } 
    return $str;
}


# Assembles and executes the query.  Does a join between the occurrences and
# collections tables, filters the results against the appropriate 
# cgi-bin/data/classdata/ file if necessary, does another select for all fields
# from the refs table for any as-yet-unqueried ref, and writes out the data.
sub queryDatabase {
    my $self = shift;
    my $q = $self->{'q'};
    my $p = $self->{'p'}; #Permissions object
    my $dbt = $self->{'dbt'};
    my $dbh = $self->{'dbh'};

    # Call as needed
    $self->setupQueryFields() if (! $self->{'setup_query_fields_called'});


    ###########################################################################
    #  Generate the query
    ###########################################################################

    my (@fields,@where,@occ_where,@reid_where,@taxon_where,@tables,@from,@groupby,@left_joins);

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
        my @collection_columns = $dbt->getTableColumns('collections');
        foreach my $c (@collection_columns) {
            next if ($c =~ /^(lat|lng)(deg|dec|min|sec|dir)$/); # handled below - handle these through special "coords" fields
            if ($q->param("collections_".$c)) {
                push @fields,"c.$c AS `c.$c`";
            }
        }
        if ($q->param('collections_paleocoords') eq 'YES') {
            push @fields,"c.paleolat AS `c.paleolat`";
            push @fields,"c.paleolng AS `c.paleolng`";
        }
        if ($q->param('collections_coords') eq 'YES') {
            push @fields,"c.lngdeg AS `c.lngdeg`";
            push @fields,"c.lngmin AS `c.lngmin`";
            push @fields,"c.lngsec AS `c.lngsec`";
            push @fields,"c.lngdec AS `c.lngdec`";
            push @fields,"c.lngdir AS `c.lngdir`";
            push @fields,"c.latdeg AS `c.latdeg`";
            push @fields,"c.latmin AS `c.latmin`";
            push @fields,"c.latsec AS `c.latsec`";
            push @fields,"c.latdec AS `c.latdec`";
            push @fields,"c.latdir AS `c.latdir`";
        }
        if ($q->param('collections_authorizer') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc1 ON c.authorizer_no=pc1.person_no";
            push @fields, 'pc1.name AS `c.authorizer`';
        }
        if ($q->param('collections_enterer') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc2 ON c.enterer_no=pc2.person_no";
            push @fields, 'pc2.name AS `c.enterer`';
        }
        if ($q->param('collections_modifier') eq 'YES') {
            push @left_joins, "LEFT JOIN person pc3 ON c.modifier_no=pc3.person_no";
            push @fields, 'pc3.name AS `c.modifier`';
        }
    }

    # We'll want to join with the reid ids if we're hitting the occurrences table,
    # or if we're getting collections and filtering using the taxon_no in the occurrences table
    my $join_reids = ($q->param('output_data') =~ /occurrences|specimens|genera|species/ || $q->param('taxon_name')) ? 1 : 0;
    if ($join_reids) {
        push @tables, 'occurrences o';
        unshift @where, 'c.collection_no = o.collection_no';

        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            push @fields, 'o.occurrence_no AS `o.occurrence_no`', 
                       'o.reference_no AS `o.reference_no`', 
                       'o.genus_reso AS `o.genus_reso`', 
                       'o.genus_name AS `o.genus_name`',
                       'o.species_name AS `o.species_name`',
                       'o.taxon_no AS `o.taxon_no`',
                       'o.abund_value AS `o.abund_value`',
                       'o.abund_unit AS `o.abund_unit`';
                       
            if ( $q->param('replace_with_reid') ne 'NO' )   {
                push @fields, 're.reid_no AS `re.reid_no`',
                           're.reference_no AS `re.reference_no`',
                           're.genus_reso AS `re.genus_reso`',
                           're.genus_name AS `re.genus_name`',
                           're.species_name AS `re.species_name`',
                           're.taxon_no AS `re.taxon_no`';
            }
            my ($whereref,$occswhereref,$reidswhereref) = $self->getOccurrencesWhereClause();
            push @where, @$whereref;
            push @occ_where, @$occswhereref;
            push @reid_where, @$reidswhereref;
            my @occurrences_columns = $dbt->getTableColumns('occurrences');
            my @reid_columns = $dbt->getTableColumns('reidentifications');
            foreach my $c (@occurrences_columns) {
                if ($q->param("occurrences_".$c)) {
                    push @fields,"o.$c AS `o.$c`";
                }
            }
            if ( $q->param('replace_with_reid') ne 'NO' )   {
                foreach my $c (@reid_columns) {
                    # Note we use occurrences_ fields
                    if ($q->param("occurrences_".$c)) {
                        push @fields,"re.$c AS `re.$c`";
                    }
                }
            }
            if ($q->param('occurrences_authorizer') eq 'YES') {
                push @left_joins, "LEFT JOIN person po1 ON o.authorizer_no=po1.person_no";
                push @fields, 'po1.name AS `o.authorizer`';
                if ( $q->param('replace_with_reid') ne 'NO' )   {
                    push @left_joins, "LEFT JOIN person pre1 ON re.authorizer_no=pre1.person_no";
                    push @fields, 'pre1.name AS `re.authorizer`';
                }
            }
            if ($q->param('occurrences_enterer') eq 'YES') {
                push @left_joins, "LEFT JOIN person po2 ON o.enterer_no=po2.person_no";
                push @fields, 'po2.name AS `o.enterer`';
                if ( $q->param('replace_with_reid') ne 'NO' )   {
                    push @left_joins, "LEFT JOIN person pre2 ON re.enterer_no=pre2.person_no";
                    push @fields, 'pre2.name AS `re.enterer`';
                }
            }
            if ($q->param('occurrences_modifier') eq 'YES') {
                push @left_joins, "LEFT JOIN person po3 ON o.modifier_no=po3.person_no";
                push @fields, 'po3.name AS `o.modifier`';
                if ( $q->param('replace_with_reid') ne 'NO' )   {
                    push @left_joins, "LEFT JOIN person pre3 ON re.modifier_no=pre3.person_no";
                    push @fields, 'pre3.name AS `re.modifier`';
                }
            }
        } 

        if ($q->param('taxon_name')) {
            my (@occ_sql,@reid_sql);
            # Don't include @taxon_where in my () above, it needs to stay in scope
            # so it can be used much later in function
            @taxon_where = $self->getTaxonString();
            push @occ_sql, "o.$_" for @taxon_where;
            push @reid_sql, "re.$_" for @taxon_where;
            push @reid_sql, "o.$_" for @taxon_where;
            push @occ_where, "(".join(" OR ",@occ_sql).")";
# nasty hack: just ignore the taxon no in list for the reIDs table if the
#  user isn't going to use the reIDs anyway JA 16.4.06
            #if ( $q->param('replace_with_reid') ne 'NO' )	{
            push @reid_where, "(".join(" OR ",@reid_sql).")";
            #}
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
        unshift @where, 'o.occurrence_no = s.occurrence_no';

        my @specimen_columns = $dbt->getTableColumns('specimens');
        foreach my $c (@specimen_columns) {
            if ($q->param("specimens_".$c)) {
                push @fields,"s.$c AS `s.$c`";
            }
        }

        if ($q->param('specimens_authorizer') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps1 ON s.authorizer_no=ps1.person_no";
            push @fields, 'ps1.name AS `s.authorizer`';
        }
        if ($q->param('specimens_enterer') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps2 ON s.enterer_no=ps2.person_no";
            push @fields, 'ps2.name AS `s.enterer`';
        }
        if ($q->param('specimens_modifier') eq 'YES') {
            push @left_joins, "LEFT JOIN person ps3 ON s.modifier_no=ps3.person_no";
            push @fields, 'ps3.name AS `s.modifier`';
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
    if($q->param('research_group') =~ /^(?:divergence|decapod|ETE|5%|1%|PACED|PGAP)$/){
        push @left_joins, "LEFT JOIN secondary_refs sr ON sr.collection_no=c.collection_no";
    }


    # Handle GROUP BY
    # This is important: don't grouping by genus_name for the obvious cases, 
    # Do the grouping in PERL, since otherwise we can't get a list of references
    # nor can we filter out old reids and rows that don't pass permissions.
    if ( $q->param('output_data') =~ /genera|species|occurrences/ )    {
        if ( $q->param('replace_with_reid') ne 'NO' )   {
            push @groupby, 'o.occurrence_no,re.reid_no';
        } else {
            push @groupby, 'o.occurrence_no';
        }
    } elsif ($q->param('output_data') eq 'collections' && ($q->param('research_group') || $join_reids || $q->param('include_specimen_fields'))) { # = collections
       push @groupby, 'c.collection_no';
    }


    
    # Assemble the final SQL
    my $sql;
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
        if ( $q->param('replace_with_reid') ne 'NO' )   {
            my @left_joins1 = ('LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no',@left_joins);

            # This term very important.  sql1 deal with occs with NO reid, sql2 deals with only reids
            # This way WHERE terms can focus on only pruning the occurrences table for sql1 and only
            # pruning on the reids table for sql2 PS 07/15/2005
            my @where1 = (@where,@occ_where,"re.reid_no IS NULL");
            my $sql1 = "SELECT ".join(",",@fields).
                   " FROM " .join(",",@tables)." ".join (" ",@left_joins1).
                   " WHERE ".join(" AND ",@where1);
            $sql1 .= " GROUP BY ".join(",",@groupby) if (@groupby);

            my @where2 = ("re.occurrence_no=o.occurrence_no AND re.most_recent='YES'",@where,@reid_where);
            my @tables2 = (@tables,'reidentifications re'); 
            my $sql2 = "SELECT ".join(",",@fields).
                   " FROM " .join(",",@tables2)." ".join (" ",@left_joins).
                   " WHERE ".join(" AND ",@where2);
            $sql2 .= " GROUP BY ".join(",",@groupby) if (@groupby);
            $sql = "($sql1) UNION ($sql2)";
        } else {
            my @where1 = (@where,@occ_where);
            my $sql1 = "SELECT ".join(",",@fields).
                   " FROM " .join(",",@tables)." ".join (" ",@left_joins).
                   " WHERE ".join(" AND ",@where1);
            $sql1 .= " GROUP BY ".join(",",@groupby) if (@groupby);
            $sql = $sql1;
        }

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
                    if ($_ =~ /^s\.|specimens\.enterer|specimens\.authorizer|specimens\.modifier/) {
                        push @specimen_fields, $_;
                    } elsif ($_ =~ /specimens_exist/) {
                        push @specimen_fields, 1;
                    } elsif ($_ =~ /o\.taxon_no/) {
                        push @specimen_fields, 's.taxon_no AS `o.taxon_no`';
                    } elsif ($_ =~ /o\.genus_name/) {
                        push @specimen_fields, 'substring_index(taxon_name," ",1) AS `o.genus_name`';
                    } elsif ($_ =~ /o\.species_name/) {
                        # If taxon name containts a space, species name is whatever follow that space
                        # If no space, species name is sp. if rank is a genus, indet if its higher order
                        push @specimen_fields, 'IF(taxon_name REGEXP " ",trim(trim(leading substring_index(taxon_name," ",1) from taxon_name)),IF(taxon_rank REGEXP "genus","sp.","indet.")) AS `o.species_name`';
                    } else {
                        push @specimen_fields, 'NULL';
                    }
                }
                my $sql3 = "SELECT ".join(",",@specimen_fields).
                         " FROM specimens s, authorities a ";
                if ($q->param('specimens_authorizer') eq 'YES') {
                    $sql3 .= " LEFT JOIN person ps1 ON s.authorizer_no=ps1.person_no";
                }
                if ($q->param('specimens_enterer') eq 'YES') {
                    $sql3 .= " LEFT JOIN person ps2 ON s.enterer_no=ps2.person_no";
                }
                if ($q->param('specimens_modifier') eq 'YES') {
                    $sql3 .= " LEFT JOIN person ps3 ON s.modifier_no=ps3.person_no";
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
    if ( $q->param('output_data') =~ /occurrences|specimens/)    {
        $sql .= " ORDER BY collection_no";
    }

    $self->dbg("<b>Occurrences query:</b><br>\n$sql<BR>");

    if (@form_errors) {
        print Debug::printErrors(\@form_errors);
        return ([],[]);
    } 

    ###########################################################################
    #  Set up various lookup tables before we loop through results
    ###########################################################################
    # Changed to use prebuild lookup table PS
    my $time_lookup;
    my @time_fields = ();
    if ($q->param("output_data") !~ /genera|species/) {
        push @time_fields, 'period_name' if ($q->param('collections_period'));
        push @time_fields, 'subepoch_name'  if ($q->param('collections_epoch'));
        push @time_fields, 'epoch_name'  if ($q->param('collections_epoch'));
        push @time_fields, 'stage_name'  if ($q->param('collections_stage'));
        if ($q->param('collections_ma_max') ||
            $q->param('collections_ma_min') ||
            $q->param('collections_ma_mid')) {
            push @time_fields, 'lower_boundary','upper_boundary';
        }
        if (($q->param("collections_max_interval") eq "YES" || 
             $q->param("collections_min_interval") eq "YES")) {
            push @time_fields, 'interval_name';
        }
    }

    # get the PBDB 10 m.y. bin names for the collections JA 3.3.04
    if ( ($q->param('collections_10mybin') && $q->param("output_data") !~ /genera|species/) ||
         $q->param('compendium_ranges') eq 'NO' ) {
        push @time_fields, 'ten_my_bin'  if ($q->param('collections_10mybin'));
    }
    if (@time_fields) {
        $time_lookup = TimeLookup::lookupIntervals($dbt,[],\@time_fields);
    }


    # Get a list of acceptable Sepkoski compendium taxa/10 m.y. bin pairs
    my %incompendium = ();
    if ( $q->param('compendium_ranges') eq 'NO' )    {
        if (!open IN,"<./data/compendium.ranges") {
            die "Could not open Sepkoski compendium ranges file<BR>";
        }
        while (<IN>) {
            chomp;
            my ($genus,$bin) = split /\t/,$_;
            $incompendium{$genus.$bin}++;
        }
        close IN;
    }

    # Get the plate ids if those will be downloaded
    my %plate_ids;
    if ($q->param('collections_tectonic_plate_id') eq "YES" && $q->param("output_data") !~ /genera|species/) {
        if ( ! open ( PLATES, "$COAST_DIR/plateidsv2.lst" ) ) {
            print "<font color='red'>Skipping plates.</font> Error message is $!<BR><BR>\n";
        } else {
            <PLATES>;

            while (my $line = <PLATES>) {
                chomp $line;
                my ($lng,$lat,$plate_id) = split /,/,$line;
                $plate_ids{$lng."_".$lat}=$plate_id;
            }
        } 
    }

    ###########################################################################
    #  Loop through the result set and build a flattened version suitable for
    #  printing out to file or normal use
    ###########################################################################

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
    my $limit = 10000000;
#    if ($q->param('limit')) {
#        $limit = $q->param('limit');
#    }
#    if (int($q->param('row_offset'))) {
#        $limit += $q->param('row_offset');
#    }
    my $ofRows = 0;
    $p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );

    $sth->finish();
    $self->dbg("Rows that passed Permissions: number of rows $ofRows, length of dataRows array: ".@dataRows."<br>");

    # ss = senior_synonym
    my %ss_taxon_nos = ();
    my %ss_taxon_names = ();
    if ($q->param("replace_with_ss") ne 'NO' && $q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        my %all_taxa = ();
        foreach my $row (@dataRows) {
            if ($row->{'re.taxon_no'}) {
                $all_taxa{$row->{'re.taxon_no'}} = 1; 
            } elsif ($row->{'o.taxon_no'}) {
                $all_taxa{$row->{'o.taxon_no'}} = 1;
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

    # We have to do this now instead of before in the query cause
    # 4.0 doesn't support subqueries and we can't the count correctly
    # if the user downloads occurrences instead of collections. Do it before
    # we tally refs and other stats below so those don't get screwed up as well
    if ($q->param('occurrence_count') =~ /^\d+$/) {
        my %COLL = ();
        foreach my $row (@dataRows) {
            $COLL{$row->{'collection_no'}}=1;
        }
        my @collnos = keys %COLL;
        if ( @collnos ) {
            my $qualifier = "<";
            if ($q->param('occurrence_count_qualifier') =~ /more/i) {
                $qualifier = ">";
            }
            my $sql = "SELECT c.collection_no FROM collections c LEFT JOIN occurrences o ON c.collection_no=o.collection_no WHERE c.collection_no IN (".join(", ",@collnos).") GROUP BY c.collection_no HAVING COUNT(o.occurrence_no) $qualifier ".$q->param('occurrence_count');
            my @results = @{$dbt->getData($sql)};
            my %exclude = ();
            foreach my $row (@results) {
                $exclude{$row->{'collection_no'}} = 1;
            }
            my @temp = ();
            foreach my $row (@dataRows) {
                if (!$exclude{$row->{'collection_no'}}) {
                    push @temp, $row;
                }
            }
            @dataRows = @temp;
        }
    }

    #use Benchmark qw(:all);
    #my $t0 = new Benchmark;

    # Populate calculated time fields, which are needed below
    if (@time_fields) {
        my %COLL = ();
        foreach my $row ( @dataRows ){
            if ($COLL{$row->{'collection_no'}}) {
                # This is merely a timesaving measure, reuse old collections values if we've already come across this collection
                my $orow = $COLL{$row->{'collection_no'}};
                $row->{'c.10mybin'} = $orow->{'c.10mybin'};
                $row->{'c.stage'}   = $orow->{'c.stage'};
                $row->{'c.epoch'}   = $orow->{'c.epoch'};
                $row->{'c.subepoch'}= $orow->{'c.subepoch'};
                $row->{'c.max_interval'}= $orow->{'c.max_interval'};
                $row->{'c.min_interval'}= $orow->{'c.min_interval'};
                $row->{'c.period'}  = $orow->{'c.period'};
                $row->{'c.ma_max'}  = $orow->{'c.ma_max'};
                $row->{'c.ma_min'}  = $orow->{'c.ma_min'};
                $row->{'c.ma_mid'}  = $orow->{'c.ma_mid'};
                next;
            }
            # Populate the generated time fields;
            my $max_lookup = $time_lookup->{$row->{'c.max_interval_no'}};
            my $min_lookup = ($row->{'c.min_interval_no'}) ? $time_lookup->{$row->{'c.min_interval_no'}} : $max_lookup;
            
            if ($max_lookup->{'ten_my_bin'} && $max_lookup->{'ten_my_bin'} eq $min_lookup->{'ten_my_bin'}) {
                $row->{'c.10mybin'} = $max_lookup->{'ten_my_bin'};
            }
            
            if ($max_lookup->{'period_name'} && $max_lookup->{'period_name'} eq $min_lookup->{'period_name'}) {
                $row->{'c.period'} = $max_lookup->{'period_name'};
            }
            if ($max_lookup->{'epoch_name'} && $max_lookup->{'epoch_name'} eq $min_lookup->{'epoch_name'}) {
                $row->{'c.epoch'} = $max_lookup->{'epoch_name'};
            }
            if ($max_lookup->{'subepoch_name'} && $max_lookup->{'subepoch_name'} eq $min_lookup->{'subepoch_name'}) {
                $row->{'c.subepoch'} = $max_lookup->{'subepoch_name'};
            }
            if ($max_lookup->{'stage_name'} && $max_lookup->{'stage_name'} eq $min_lookup->{'stage_name'}) {
                $row->{'c.stage'} = $max_lookup->{'stage_name'};
            }
            $row->{'c.max_interval'} = $time_lookup->{$row->{'c.max_interval_no'}}{'interval_name'};
            $row->{'c.min_interval'} = $time_lookup->{$row->{'c.min_interval_no'}}{'interval_name'};
            $row->{'c.ma_max'} = $max_lookup->{'lower_boundary'};
            $row->{'c.ma_min'} = $min_lookup->{'upper_boundary'};
            $row->{'c.ma_mid'} = ($max_lookup->{'lower_boundary'} + $min_lookup->{'upper_boundary'})/2;
            $COLL{$row->{'collection_no'}} = $row; 
        }
    }
    #my $t1 = new Benchmark;
    #my $td = timediff($t1,$t0);
    #my $time = 0+$td->[0] + $td->[1];
    #print "TD for populateStuffs: $time\n";

    my %lumpseen;
    my %occseen;
    my %genusseen;
    my %taxon_nos_seen;
    my %occs_by_taxa;
    my @allDataRows = ();
    my @lumpedDataRows = ();
    foreach my $row ( @dataRows ){
        my $exclude = 0;
        my $lump = 0;
            
        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            # swap the reIDs into the occurrences (pretty important!)
            # don't do this unless the user requested it (the default)
            #  JA 16.4.06
            if ($row->{'re.reid_no'}) {
                if ( $q->param('replace_with_reid') ne 'NO' ) {
                    foreach my $field (@reidFieldNames,@reidTaxonFieldNames) {
                        $row->{'or.'.$field}=$row->{'o.'.$field};
                        $row->{'o.'.$field}=$row->{'re.'.$field};
                    }
                }
            }
            # Replace with senior_synonym_no PS 11/1/2005
            # this is ugly because the "original name" is actually a reID
            #  whenever a reID exists
            if ( $q->param('replace_with_ss') ne 'NO' ) {
                if ($ss_taxon_nos{$row->{'o.taxon_no'}}) {
                    my ($genus,$subgenus,$species,$subspecies) = @{$ss_taxon_names{$row->{'o.taxon_no'}}};
                    #print "$row->{occurrence_no}, SENIOR SYN FOR $row->{o.genus_name}/$row->{o.subgenus_name}/$row->{o.species_name}/$row->{o.subspecies_name} IS $genus/$subgenus/$species/$subspecies<BR>";

                    $row->{'or.taxon_no'} = $row->{'o.taxon_no'};
                    $row->{'o.taxon_no'} = $ss_taxon_nos{$row->{'o.taxon_no'}};
                    $row->{'or.genus_name'} = $row->{'o.genus_name'};
                    $row->{'o.genus_name'} = $genus;
                    $row->{'or.subgenus_name'} = $row->{'o.subgenus_name'};
                    $row->{'or.species_name'} = $row->{'o.species_name'};
                    $row->{'or.subspecies_name'} = $row->{'o.subspecies_name'};
                    if ($species) {
                        $row->{'o.subgenus_name'} = $subgenus;
                        $row->{'o.species_name'} = $species;
                        $row->{'o.subspecies_name'} = $subspecies;
                    }
                }
            }
            # raise subgenera to genus level JA 18.8.04
            if ( $q->param('split_subgenera') eq 'YES' && $row->{'o.subgenus_name'} )    {
                $row->{'o.genus_name'} = $row->{'o.subgenus_name'};
            }
            # get rid of occurrences of genera either (1) not in the
            #  Compendium or (2) falling outside the official Compendium
            #  age range JA 27.8.04
            if ( $q->param('compendium_ranges') eq 'NO' )    {
                if ( ! $incompendium{$row->{'o.genus_name'}.$row->{'c.10mybin'}} )    {
                    $exclude++;
                }
            }

            # Lump by genera or genera/collection here in code, so we can get refs + filter reids
            # correctly above
            if ($exclude == 0 && ($q->param('output_data') =~ /genera|species/ || $q->param('lump_genera') eq 'YES')) {
                my $genus_string;
                if ($q->param('lump_genera') eq 'YES') {
                    $genus_string .= $row->{'collection_no'};
                }
              
                if ($q->param('output_data') =~ /species/i) {
                    $genus_string .= $row->{'o.genus_name'}.$row->{'o.species_name'};
                } else {
                    $genus_string .= $row->{'o.genus_name'};
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
        if ( $exclude == 0 && ( $q->param('lump_by_coord') eq 'YES' || $q->param('lump_by_interval') eq 'YES' || $q->param('lump_by_mbr') eq 'YES' || $q->param('lump_by_ref') eq 'YES' ) )    {

            my $lump_string;

            if ( $q->param('lump_by_coord') eq 'YES' )    {
                $lump_string .= $row->{'c.latdeg'}.$row->{'c.latmin'}.$row->{'c.latsec'}.$row->{'c.latdec'}.$row->{'c.latdir'}.$row->{'c.lngdeg'}.$row->{'c.lngmin'}.$row->{'c.lngsec'}.$row->{'c.lngdec'}.$row->{'c.lngdir'};
            }
            if ( $q->param('lump_by_interval') eq 'YES' )    {
                $lump_string .= $row->{'c.max_interval_no'}.$row->{'c.min_interval_no'};
            }
            if ( $q->param('lump_by_mbr') eq 'YES' )    {
                $lump_string .= $row->{'c.formation'}.$row->{'c.member'};
            }
            if ( $q->param('lump_by_ref') eq 'YES' )    {
                $lump_string .= $row->{'c.reference_no'};
            }

            my $genus_string;

            if ($q->param('occurrences_species_name') =~ /yes/i) {
                $genus_string = $row->{'o.genus_name'}.$row->{'o.species_name'};
            } else {
                $genus_string = $row->{'o.genus_name'};
            }

            if ( $lumpseen{$lump_string} )    {
                # Change  the collection_no to be the same collection_no as the first
                # collection_no encountered that has the same $lump_string, so that 
                # Curve.pm will lump them together when doing a calculation
                $row->{'collection_no'} = $lumpseen{$lump_string};
                if ( $occseen{$row->{'collection_no'}.$genus_string} > 0 )    {
                    $lump++;
                }
            } else    {
                $lumpseen{$lump_string} = $row->{'collection_no'};
            }
            $occseen{$row->{'collection_no'}.$genus_string}++;
        }
        if ( $exclude == 0) {
            if ($q->param('output_data') eq 'genera' || $q->param('output_data') eq 'species') {
                if ($row->{'specimens_exist'}) {
                    my $genus_string = $row->{'o.genus_name'};
                    if ($q->param('output_data')  eq 'species') {
                        $genus_string .= " $row->{'o.species_name'}";
                    } 
                    push @{$occs_by_taxa{$genus_string}}, $row->{'o.occurrence_no'};
                }
            }
            push @allDataRows, $row;
        }
        if ( $exclude == 0 && $lump == 0)    {
            $taxon_nos_seen{$row->{'o.taxon_no'}}++;
            push @lumpedDataRows, $row;
        }
    }
    
    @dataRows = @lumpedDataRows;
    my $dataRowsSize = scalar(@dataRows);

    # This is a bit nasty doing it here, but since we do lumping in PERL code, we have to do this after the
    # fact and can't do it after the fact. Will have to amend table structure or radically alter queries
    # so we can do more in SQL to get around this I think PS
    if (int($q->param('row_offset'))) {
        splice(@dataRows,0,int($q->param('row_offset')));
    }
    if (int($q->param('limit'))) {
        splice(@dataRows,int($q->param('limit')));
    }


    # Get a list of parents for classification purposes
    my @taxon_nos = keys %taxon_nos_seen;

    my %master_class;
    if (@ecoFields || 
        ($q->param("output_data") =~ /occurrences|specimens|genera|species/ && 
        ($q->param("occurrences_class_name") eq "YES" || 
         $q->param("occurrences_order_name") eq "YES" || 
         $q->param("occurrences_family_name") eq "YES"))){
        %master_class=%{TaxaCache::getParents($dbt,\@taxon_nos,'array_full')};
    }

    # Sort by
    if ($q->param('output_data') =~ /^(?:genera|specimens|species)$/) {
        @dataRows = sort { $a->{'o.genus_name'} cmp $b->{'o.genus_name'} ||
                           $a->{'o.species_name'} cmp $b->{'o.species_name'}} @dataRows;
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

        if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
            # Setup up family/name/class fields
            if ($q->param("occurrences_class_name") eq "YES" || 
                $q->param("occurrences_order_name") eq "YES" ||
                $q->param("occurrences_family_name") eq "YES") {
                my @parents = @{$master_class{$row->{'o.taxon_no'}}};
                my ($class, $order, $family) = ("","","");
                foreach my $parent (@parents) {
                    if ($parent->{'taxon_rank'} eq 'family') {
                        $row->{'o.family_name'} = $parent->{'taxon_name'};
                    } elsif ($parent->{'taxon_rank'} eq 'order') {
                        $row->{'o.order_name'} = $parent->{'taxon_name'};
                    } elsif ($parent->{'taxon_rank'} eq 'class') {
                        $row->{'o.class_name'} = $parent->{'taxon_name'};
                        last;
                    }
                }
                # Get higher order names for indets as well
                if ($row->{'o.species_name'} =~ /indet/ && $row->{'o.taxon_no'}) {
                    my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'o.taxon_no'}});
                    if ($taxon->{'taxon_rank'} eq 'family') {
                        $row->{'o.family_name'} = $taxon->{'taxon_name'};
                    } elsif ($taxon->{'taxon_rank'} eq 'order') {
                        $row->{'o.order_name'} = $taxon->{'taxon_name'};
                    } elsif ($taxon->{'taxon_rank'} eq 'class') {
                        $row->{'o.class_name'} = $taxon->{'taxon_name'};
                    }
                }
            }

            # Set up the ecology fields
            foreach (@ecoFields) {
                if ($ecotaph{$row->{'o.taxon_no'}}{$_} !~ /^\s*$/) {
                    $row->{$_} = $ecotaph{$row->{'o.taxon_no'}}{$_};
                } else {
                    $row->{$_} = '';
                }
            }
        }

        # Set up collections fields
        if ($q->param("output_data") =~ /collections|specimens|occurrences/) {
            # Get coordinates into the correct format (decimal or deg/min/sec/dir), performing
            # conversions as necessary
            if ($q->param('collections_coords') eq "YES") {
                if ($q->param('collections_coords_format') eq 'DMS') {
                    if (!($row->{'c.lngmin'} =~ /\d+/)) {
                        if ($row->{'c.lngdec'} =~ /\d+/) {
                            my $min = 60*(".".$row->{'c.lngdec'});
                            $row->{'c.lngmin'} = int($min);
                            $row->{'c.lngsec'} = int(($min-int($min))*60);
                        }    
                    }
                    
                    if (!($row->{'c.latmin'} =~ /\d+/)) {
                        if ($row->{'c.latdec'} =~ /\d+/) {
                            my $min = 60*(".".$row->{'c.latdec'});
                            $row->{'c.latmin'} = int($min);
                            $row->{'c.latsec'} = int(($min-int($min))*60);
                        }    
                    }
                } else {
                    if ($row->{'c.latmin'} =~ /\d+/) {
                        $row->{'c.latdec'} = sprintf("%.6f",$row->{'c.latdeg'} + $row->{'c.latmin'}/60 + $row->{'c.latsec'}/3600);
                    } else {
                        if ($row->{'c.latdec'} =~ /\d+/) {
                            $row->{'c.latdec'} = sprintf("%s",$row->{'c.latdeg'}.".".int($row->{'c.latdec'}));
                        } else {
                            $row->{'c.latdec'} = $row->{'c.latdeg'};
                        }
                    }    
                    $row->{'c.latdec'} *= -1 if ($row->{'c.latdir'} eq "South");
                    
                    if ($row->{'c.lngmin'} =~ /\d+/) {
                        $row->{'c.lngdec'} = sprintf("%.6f",$row->{'c.lngdeg'} + $row->{'c.lngmin'}/60 + $row->{'c.lngsec'}/3600);
                    } else {
                        if ($row->{'c.lngdec'} =~ /\d+/) {
                            $row->{'c.lngdec'} = sprintf("%s",$row->{'c.lngdeg'}.".".int($row->{'c.lngdec'}));
                        } else {
                            $row->{'c.lngdec'} = $row->{'c.lngdeg'};
                        }
                    }    
                    $row->{'c.lngdec'} *= -1 if ($row->{'c.lngdir'} eq "West");
                }
            }
            if ($q->param('collections_paleocoords') eq "YES") {
                if ($q->param('collections_paleocoords_format') eq 'DMS') {
                    if ($row->{'c.paleolat'} =~ /\d+/) {
                        $row->{'c.paleolatdir'}  = ($row->{'c.paleolat'} >= 0) ? "North" : "South";
                        my $abs_lat = ($row->{'c.paleolat'} < 0) ? -1*$row->{'c.paleolat'} : $row->{'c.paleolat'};
                        my $deg = int($abs_lat);
                        my $min = 60*($abs_lat-$deg);
                        $row->{'c.paleolatdeg'} = $deg;
                        $row->{'c.paleolatmin'} = int($min);
                        $row->{'c.paleolatsec'} = int(($min-int($min))*60);
                    }    
                    if ($row->{'c.paleolng'} =~ /\d+/) {
                        $row->{'c.paleolngdir'}  = ($row->{'c.paleolng'} >= 0) ? "East" : "West";
                        my $abs_lat = ($row->{'c.paleolng'} < 0) ? -1*$row->{'c.paleolng'} : $row->{'c.paleolng'};
                        my $deg = int($abs_lat);
                        my $min = 60*($abs_lat-$deg);
                        $row->{'c.paleolngdeg'} = $deg;
                        $row->{'c.paleolngmin'} = int($min);
                        $row->{'c.paleolngsec'} = int(($min-int($min))*60);
                    }    
                } else {
                    $row->{'c.paleolatdec'} = $row->{'c.paleolat'};
                    $row->{'c.paleolngdec'} = $row->{'c.paleolng'};
                }
            }
            if ($q->param('collections_tectonic_plate_id') eq 'YES') {
                my $plate_key = "";
                $plate_key .= "-" if ($row->{'c.lngdir'} eq 'West' && $row->{'c.lngdeg'} != 0);
                $plate_key .= $row->{'c.lngdeg'};
                $plate_key .= "_";
                $plate_key .= "-" if ($row->{'c.latdir'} eq 'South' && $row->{'c.latdeg'} != 0);
                $plate_key .= $row->{'c.latdeg'};
                $row->{'c.tectonic_plate_id'} = $plate_ids{$plate_key};
            }
        }

        my @measurement_rows = ();
        if ($q->param('include_specimen_fields')) {
            $row->{'specimen_parts'} = createSpecimenPartsRows($row,$q,$dbt,\%occs_by_taxa);
        }
    }

    return (\@dataRows,\@allDataRows,$dataRowsSize);
}


# This returns an array (reference) of hashrefs, where each row in the array is a set of measurements for a different part
# This is necessary since the measurements need to be denormalized by part (might be multiple different parts i.e. tooth/leg) for
# a single taxa/occurence/collection and its a pain to denormalize in SQL without screwing up various stats and such.
# So denormalize in perl by just storing an array (1 row per part) and printing out and reprinting the same taxa/occurrence/collection data for
# each row in the array.
sub createSpecimenPartsRows {
    my ($row,$q,$dbt,$occs_by_taxa) = @_;
    my @measurements = ();
    if ($q->param('output_data') eq 'collections' && $row->{'specimens_exist'}) {
        @measurements = Measurement::getMeasurements($dbt,'collection_no'=>$row->{'collection_no'});
    } elsif ($q->param('output_data') =~ /occurrences/ && $row->{specimens_exist}) {
        @measurements = Measurement::getMeasurements($dbt,'occurrence_no'=>$row->{'o.occurrence_no'});
    } elsif ($q->param('output_data') eq 'genera') {
        my $genus_string = "$row->{'o.genus_name'}";
        if ($occs_by_taxa->{$genus_string} && @{$occs_by_taxa->{$genus_string}}) {
            if ($q->param('get_global_specimens')) {
                # Note: will run into homonym issues till we figure out how to pass taxon_no(s)
                @measurements = Measurement::getMeasurements($dbt,'taxon_name'=>$genus_string,'get_global_specimens'=>$q->param('get_global_specimens'));
            } else {
                @measurements = Measurement::getMeasurements($dbt,'occurrence_list'=>$occs_by_taxa->{$genus_string});
            }
        }
    } elsif ($q->param('output_data') eq 'species') {
        my $genus_string = "$row->{'o.genus_name'} $row->{'o.species_name'}";
        if ($occs_by_taxa->{$genus_string} && @{$occs_by_taxa->{$genus_string}}) {
            if ($q->param('get_global_specimens')) {
                # Note: will run into homonym issues till we figure out how to pass taxon_no(s)
                @measurements = Measurement::getMeasurements($dbt,'taxon_name'=>$genus_string,'get_global_specimens'=>$q->param('get_global_specimens'));
            } else {
                #print "OCC_LIST for $genus_string: ".join(", ",@{$occs_by_taxa->{$genus_string}})."<BR>";
                @measurements = Measurement::getMeasurements($dbt,'occurrence_list'=>$occs_by_taxa->{$genus_string});
            }
        }
    } elsif ($q->param('output_data') eq 'specimens') {
        my $sql = "SELECT m.* FROM measurements m WHERE m.specimen_no =".$row->{'specimen_no'};
        @measurements = @{$dbt->getData($sql)};
        #Ugly hack - getMeasurementTable expect specimens joined w/measurements dataset, so add in these
        # needed fields again so the function works correctly
        foreach (@measurements) {
            $_->{'specimens_measured'} = $row->{'specimens_measured'};
            $_->{'specimen_no'} = $row->{'specimen_no'};
            $_->{'specimen_part'} = $row->{'specimen_part'};
        }
    } 

    my @parts_rows = ();
    if (@measurements) {
        # NOTE!: getMeasurementTable returns a 3-indexed hash with the indexes (specimen_part,measurement_type,value_measured) i.e. $p_table{'leg'}{'average'}{'length'}
        my $p_table = Measurement::getMeasurementTable(\@measurements);    

        while (my ($part,$m_table)=each %$p_table) {
            my $part_row = {};
            if ($q->param('output_data') eq 'specimens') {
                foreach my $f (@specimenFieldNames) {
                    if ($q->param('specimens_'.$f)) {
                        $part_row->{'s.'.$f} = $row->{'s.'.$f};
                    }
                }
            } else {
                if ($q->param('specimens_specimen_part')) {
                    $part_row->{'s.specimen_part'} = $part;
                }
                if ($q->param('specimens_specimens_measured')) {
                    $part_row->{'s.specimens_measured'} = $m_table->{'specimens_measured'};
                }
            }

            foreach my $t (@measurementTypes) {
                foreach my $f (@measurementFields) {
                    if ($q->param('specimens_'.$f) && $q->param('specimens_'.$t)) {
                        if ($m_table->{$f}{$t}) {
                            my $value = sprintf("%.4f",$m_table->{$f}{$t});
                            $value =~ s/0+$//;
                            $value =~ s/\.$//;    
                            $part_row->{'s.'.$t.'_'.$f}=$value;
                        } else {
                            $part_row->{'s.'.$t.'_'.$f}='';
                        }
                    }
                }
            }

            push @parts_rows,$part_row;
        }
    }
    return \@parts_rows;
}


sub printCSV {
    my $self = shift;
    my $results = shift;
    my $csv = $self->{'csv'};
    my $q = $self->{'q'};

    my $ext = 'csv';
    if ($q->param('output_format') =~ /tab/i) {
        $ext = 'tab';
    } 

    my $mainFile = "";
    my $filename = $self->{'filename'};
    if ( $q->param("output_data") eq 'collections') {
        $mainFile = "$filename-cols.$ext";
    } elsif ($q->param("output_data") eq 'specimens') {
        $mainFile = "$filename-specimens.".$ext;
    } elsif ( $q->param("output_data") eq 'genera') {
        $mainFile = "$filename-genera.$ext";
    } elsif ( $q->param("output_data") eq 'species') {
        $mainFile = "$filename-species.$ext";
    } else {
        $mainFile = "$filename-occs.$ext";
    }

    if (! open(OUTFILE, ">$OUT_FILE_DIR/$mainFile") ) {
        die ( "Could not open output file: $mainFile ($!)<BR>\n" );
    }

    #
    # Header Generation
    #

    # Here is the relative ordering of stuff for each output type:
    # -For collections: collection fields, specimen fields
    # -For occurrences: collection_no, classification fields, occurrence fields, ecology fields, collection fields, aggregate specimen fields
    # -For specimens: collection_no,classification fields, occurrence fields, ecology fields, collection fields, specific specimen fields, aggregate specimen fields
    # -For genera: classification fields, aggregate specimen fields
    # -For species: classification fields, aggregate specimen fields
    # print column names to occurrence output file JA 19.8.01
    my @header = ();

    if ($q->param('output_data') =~ /occurrences|collections|specimens/) {
        push @header,'collection_no';
    }
    if ($q->param('output_data') =~ /occurrences|genera|species|specimens/) {
        push @header, 'o.class_name' if ($q->param("occurrences_class_name") eq 'YES');
        push @header, 'o.order_name' if ($q->param("occurrences_order_name") eq 'YES');
        push @header, 'o.family_name' if ($q->param("occurrences_family_name") eq 'YES');
    }      
    if ($q->param('output_data') =~ /genera/) {
        push @header, 'o.genus_name';
    } elsif ($q->param('output_data') =~ /species/) {
        push @header, 'o.genus_name';
        push @header, 'o.subgenus_name' if ($q->param("occurrences_subgenus_name") eq "YES");
        push @header, 'o.species_name';
    } elsif ($q->param('output_data') =~ /occurrences|specimens/) {
        foreach (@occTaxonFieldNames) {
            if ($q->param("occurrences_".$_) eq 'YES') {
                push(@header, "o.$_") 
            }
        }
        foreach (@reidTaxonFieldNames) {
            if ($q->param("occurrences_".$_) eq 'YES') {
                push @header, "or.$_"
            }
        }
        foreach (@occFieldNames) {
            if ($q->param("occurrences_".$_) eq 'YES') {
                push @header, "o.$_";
            }
        }
        foreach (@reidFieldNames) {
            if ($q->param("occurrences_".$_) eq 'YES') {
                push @header,"or.$_"
            }
        }
    }

    if ($q->param('output_data') =~ /occurrences|specimens|genera|species/) {
        push @header,@ecoFields;
    }
       
    if ($q->param('output_data') =~ /collections|occurrences|specimens/) {
        foreach (@collectionsFieldNames) {
            if ($q->param("collections_paleocoords_format") eq "DMS") {
                next if ($_ =~ /^paleo(?:latdec|lngdec)/);
            } else {
                next if ($_ =~ /^paleo(?:latdeg|latdir|latmin|latsec|lngdeg|lngdir|lngmin|lngsec)/);
            }
            if ($q->param("collections_coords_format") eq "DMS") {
                next if ($_ =~ /^(?:latdec|lngdec)/);
            } else {
                next if ($_ =~ /^(?:latdeg|latdir|latmin|latsec|lngdeg|lngdir|lngmin|lngsec)/);
            }
            if ($q->param("collections_".$_) eq 'YES') {
                push @header, "c.$_";
            }
        }
    }

    my @specimen_header;
    my @dummy_specimen_row;
    #Measurement header
    if ($q->param('include_specimen_fields')) {
        if ($q->param('output_data') eq 'specimens') {
            foreach my $f (@specimenFieldNames) {
                if ($q->param('specimens_'.$f)) {
                    push @specimen_header,'s.'.$f;
                }
            }
        } else {
            if ($q->param('specimens_specimen_part')) {
                push @specimen_header,'s.specimen_part';
            }
            if ($q->param('specimens_specimens_measured')) {
                push @specimen_header,'s.specimens_measured';
            }
        } 

        foreach my $t (@measurementTypes) {
            foreach my $f (@measurementFields) {
                if ($q->param('specimens_'.$t) && $q->param('specimens_'.$f)) {
                    push @specimen_header, 's.'.$t."_".$f;
                }
            }
        }   
        foreach (@specimen_header) {
            push @dummy_specimen_row, '';
        }
    }

    my @printedHeader = (); 
    foreach (@header,@specimen_header) {
        my $v = $_;
        $v =~ s/^o\./occurrences\./;
        $v =~ s/^or\./original\./;
        $v =~ s/^re\./reidentifications\./;
        $v =~ s/^c\./collections\./;
        $v =~ s/^s\./specimens\./;
        #$v =~ s/^e\./ecology\./;
        $v =~ s/^e\.//;
        if ($q->param("output_data") =~ /genera|species/) {
            $v =~ s/^[a-z]+\.//g;
        }
        push @printedHeader,$v;
    }
    my $headerline = $self->formatRow(@printedHeader);
    print OUTFILE $headerline."\n";
    #$self->dbg ( "Output header: $headerline" );
   
    foreach my $row (@$results) {
        my @line = ();

        foreach my $v (@header) {
            if ($row->{$v} =~ /^\s*$/) {
                push @line, '';
            } else {
                push @line, $row->{$v};
            }
        }

        if ($q->param('include_specimen_fields')) {
            my @specimen_parts;
            if ($row->{'specimen_parts'}) {
                @specimen_parts = @{$row->{'specimen_parts'}};
            }
            if (@specimen_parts) {
                foreach my $part (@specimen_parts) {
                    my @specimen_row = ();
                    foreach (@specimen_header) {
                        if ($part->{$_} !~ /^\s*$/) {
                            push @specimen_row, $part->{$_};
                        } else {
                            push @specimen_row, '';
                        }
                    }
                    my $curLine = $self->formatRow(@line,@specimen_row);
                    $curLine =~ s/\r|\n/ /g;
                    print OUTFILE "$curLine\n";
                }
            } else {
                my $curLine = $self->formatRow(@line,@dummy_specimen_row);
                $curLine =~ s/\r|\n/ /g;
                print OUTFILE "$curLine\n";
            }
        } else {
            my $curLine = $self->formatRow(@line);
            $curLine =~ s/\r|\n/ /g;
            print OUTFILE "$curLine\n";
        }
    }

    return (scalar (@$results),$mainFile);
}

# this section is by Schroeter; I'm just doing a cleanup 15.4.06 JA
sub printCONJUNCT {
    my $self = shift;
    my $results = shift;
    my $csv = $self->{'csv'};
    my $q = $self->{'q'};

    my $filename = $self->{'filename'};
    # Conjunct is expecint an "occurrences" type or it might screw up, so keep this short
    my $mainFile = "$filename-occs.conjunct";

    if (! open(OUTFILE, ">$OUT_FILE_DIR/$mainFile") ) {
        die ( "Could not open output file: $mainFile ($!)<BR>\n" );
    } 

    my $lastcoll;
    foreach my $row (@$results) {
        if ($lastcoll != $row->{'collection_no'}) {
            if ( $lastcoll )    {
                print OUTFILE ".\n\n";
            }
            if ( $row->{'c.collection_name'} )    {
                $row->{'c.collection_name'} =~ s/ /_/g;
                printf OUTFILE "%s\n",$row->{'c.collection_name'};
            } else    {
                print OUTFILE "Collection_$row->{'collection_no'}\n";
            }

            my @comments = ("collection_no: $row->{'collection_no'}");

            foreach (@collectionsFieldNames) {
                if ($q->param("collections_".$_) eq 'YES') {
                    if ($row->{'c.'.$_}) {
			# these three will be printed anyway
			if ( $_ !~ /(collection_name)|(localsection)|(localbed)/ )	{
                        	push @comments,"$_: $row->{'c.'.$_}"; 
                    	}
                    }
                }
            } 

            if (@comments) {
                print OUTFILE "[".join("\n",@comments)."]\n";
            }

            my $level;
            my $level_value;
            if ($row->{'c.regionalsection'} && $row->{'c.regionalbed'} =~ /^[0-9]+(\.[0-9]+)*$/) {
                $level = $row->{'c.regionalsection'};
                $level_value = $row->{'c.regionalbed'};
                if ($row->{'c.regionalorder'} eq 'top to bottom') {
                    $level_value *= -1;
                }
            }
            if ($row->{'c.localsection'} && $row->{'c.localbed'} =~ /^[0-9]+(\.[0-9]+)*$/) {
                $level = $row->{'c.localsection'};
                $level_value = $row->{'c.localbed'};
                if ($row->{'c.localorder'} eq 'top to bottom') {
                    $level_value *= -1;
                }
            }
            if ($level) {
                $level =~ s/ /_/g;
                print OUTFILE "level: $level $level_value\n";
            }
        }
        
# informals and new taxa don't work; we're punting on adding quotes
        if ( $row->{'o.genus_reso'} && $row->{'o.genus_reso'} !~ /(informal)|(")|(n\. gen\.)/) {
            printf OUTFILE "%s ",$row->{'o.genus_reso'};
        }
        print OUTFILE "$row->{'o.genus_name'} ";
        if ( $row->{'o.species_reso'} && $row->{'o.species_reso'} !~ /(informal)|(")|(n\. sp\.)/) {
            printf OUTFILE "%s ",$row->{'o.species_reso'};
        }
        if ( ! $row->{'o.species_name'} )    {
            print OUTFILE "sp.\n";
        } else {
            my $species_name = $row->{'o.species_name'};
# if there are spaces in the species name, it's some stupid thing like "sp. A",
#   so put the garbage in brackets JA 16.4.06
            if ( $species_name =~ / / )	{
                $species_name =~ s/ / \[/;
                $species_name .= "]";
            }
            printf OUTFILE "%s\n",$species_name;
        }
        $lastcoll = $row->{'collection_no'};
    }

    print OUTFILE ".\n\n";
    close OUTFILE;

    return (scalar(@$results),$mainFile);
}


sub printRefsFile {
    my $self = shift;
    my $results = shift;
    my $q = $self->{'q'};
    my $dbt = $self->{'dbt'};
    my $csv = $self->{'csv'};

    # Open the file handlea we're going to use or die
    my $filename = $self->{'filename'};
    my $ext = ($q->param('output_format') =~ /tab/) ? "tab" : "csv";
    my $refsFile = "$filename-refs.$ext";
    if (!open(REFSFILE, ">$OUT_FILE_DIR/$refsFile")) {
        die ("Could not open output file: $refsFile($!) <br>");
    } 


    # now hit the secondary refs table, mark all of those references as
    #  having been used, and print all the refs JA 16.7.04
    my %all_refs;
    my %all_colls;
    foreach my $row (@$results) {
        $all_colls{$row->{'collection_no'}}++;
        $all_refs{$row->{'reference_no'}}++;
        if ($row->{'o.reference_no'}) {
            $all_refs{$row->{'o.reference_no'}}++;
        }
        if ($row->{'re.reference_no'}) {
            $all_refs{$row->{'re.reference_no'}}++;
        }
    }
    if (%all_colls) {
        my $sql = "SELECT reference_no FROM secondary_refs WHERE collection_no IN (".join (', ',keys %all_colls).")";
        $self->dbg("secondary refs sql: $sql"); 
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results){
            $all_refs{$row->{'reference_no'}}++;
        }
    }
    delete $all_refs{''};

    # print the header
    print REFSFILE join (',',@refsFieldNames), "\n";

    # print the refs
    my $ref_count = 0;
    if (%all_refs) {
        my $fields = join(",",map{"r.".$_} grep{!/^(?:authorizer|enterer|modifier)$/} @refsFieldNames);
        my $sql = "SELECT p1.name authorizer, p2.name enterer, p3.name modifier, $fields FROM refs r ".
                   " LEFT JOIN person p1 ON p1.person_no=r.authorizer_no" .
                   " LEFT JOIN person p2 ON p2.person_no=r.enterer_no" .
                   " LEFT JOIN person p3 ON p3.person_no=r.modifier_no" .
                   " WHERE reference_no IN (" . join (', ',keys %all_refs) . ")";
        $self->dbg("Get ref data sql: $sql"); 
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            my @refvals = ();
            foreach my $r (@refsFieldNames) {
                if ($row->{$r}) {
                    push @refvals,$row->{$r};
                } else {
                    push @refvals,'';
                }
            }
            my $refLine = $self->formatRow(@refvals);
            $refLine =~ s/\r|\n/ /g;
            printf REFSFILE "%s\n",$refLine;
            $ref_count++;
        }
    }
    close REFSFILE;
    return ($ref_count,$refsFile);
}


sub printAbundFile {
    my $self = shift;
    my $results = shift;
    my $q = $self->{'q'};
    my $csv = $self->{'csv'};

    # Open the file handlea we're going to use or die
    my $filename = $self->{'filename'};
    my $ext = ($q->param('output_format') =~ /tab/) ? "tab" : "csv";
    my $abundFile = "$filename-abund.$ext";
    if (!open(ABUNDFILE, ">$OUT_FILE_DIR/$abundFile")) {
        die ("Could not open output file: $abundFile($!) <br>");
    }
         

    # cumulate number of specimens per collection, 
    my %abundance = ();
    foreach my $row (@$results) {
        if ( $row->{'o.abund_unit'} =~ /^(?:specimens|individuals)$/ && $row->{'o.abund_value'} > 0 ) {
            $abundance{$row->{'collection_no'}} += $row->{'o.abund_value'};
        }
    }

    # compute relative abundance proportion and add to running total
    # WARNING: sum is of logged abundances because geometric means are desired
    my (%occs_by_taxa,%summed_proportions,%number_of_counts);
    my $min_abund = int($q->param('min_mean_abundance'));
    foreach my $row (@$results) {
        my $taxa_key = $row->{'o.genus_name'};
        # cumulate number of collections including each genus
        if ($q->param('occurrences_species_name') =~ /yes/i) {
            $taxa_key .= "|".$row->{'o.species_name'};
        } 
        $occs_by_taxa{$taxa_key}++;
        # need these two for ecology lookup below

        if ( ($row->{'o.abund_unit'} eq "specimens" || $row->{'o.abund_unit'} eq "individuals") && 
             ($row->{'o.abund_value'} =~ /^\d+$/ && $row->{'o.abund_value'} > 0) ) {
            if ($min_abund) {
                if ($abundance{$row->{collection_no}} >= $min_abund) {
                    $number_of_counts{$taxa_key}++;
                    $summed_proportions{$taxa_key} += log($row->{'o.abund_value'} / $abundance{$row->{'collection_no'}});
                } else {
                    $self->dbg("Skipping collection_no $row->{collection_no}, count $abundance{$row->{collection_no}} is below count $min_abund");
                }
            } else {
                $number_of_counts{$taxa_key}++;
                $summed_proportions{$taxa_key} += log($row->{'o.abund_value'} / $abundance{$row->{'collection_no'}});
            }
        }
    }

    # print out a list of genera with total number of occurrences and average relative abundance
    # This list of genera is needed abundance file far below
    my @abundline = ();
    push @abundline, 'genus';
    if ($q->param('occurrences_species_name') =~ /YES/i) {
        push @abundline, 'species';
    }
    push @abundline, 'collections','with abundances','geometric mean abundance';
    print ABUNDFILE $self->formatRow(@abundline)."\n";

    my @taxa = sort keys %occs_by_taxa;
    foreach my $taxon ( @taxa ) {
        @abundline = ();
        if ($q->param('occurrences_species_name') =~ /YES/i) {
            my ($genus,$species)=split(/\|/,$taxon);
            push @abundline, $genus, $species, $occs_by_taxa{$taxon}, sprintf("%d",$number_of_counts{$taxon});
        } else {
            push @abundline, $taxon, $occs_by_taxa{$taxon}, sprintf("%d",$number_of_counts{$taxon});
        }
        
        if ( $number_of_counts{$taxon} > 0 )    {
            push @abundline, sprintf("%.4f",exp($summed_proportions{$taxon} / $number_of_counts{$taxon}));
        } else    {
            push @abundline, "NaN";
        }
        print ABUNDFILE $self->formatRow(@abundline)."\n";
    }
    close ABUNDFILE;
    return (scalar(@taxa),$abundFile);
}


sub printScaleFile {
    my $self = shift;
    my $results = shift;
    my $q = $self->{'q'};
    my $csv = $self->{'csv'};
    my $dbt = $self->{'dbt'};
    my $dbh = $self->{'dbh'};

    return ('','') if (! $q->param('time_scale'));
   
    # Open the file handlea we're going to use or die
    my $filename = $self->{'filename'};
    my $ext = ($q->param('output_format') =~ /tab/) ? "tab" : "csv";
    my $scaleFile = "$filename-scale.$ext";
    if (!open(SCALEFILE, ">$OUT_FILE_DIR/$scaleFile")) {
        die ("Could not open output file: $scaleFile($!) <br>");
    }

    my (%interval_lookup,%upper_bin_bound,%lower_bin_bound);
    if ( $q->param('time_scale') eq "PBDB 10 m.y. bins" ) {
        @_ = TimeLookup::processBinLookup($dbh,$dbt);
        %interval_lookup = %{$_[0]};
        # hash array is by name
        %upper_bin_bound = %{$_[1]};
        %lower_bin_bound = %{$_[2]};
    } else {
        my $intervalInScaleRef = TimeLookup::processScaleLookup($dbh,$dbt, $q->param('time_scale'));
        %interval_lookup = %{$intervalInScaleRef};
        @_ = TimeLookup::findBoundaries($dbh,$dbt);
        # first two hash arrays are by number, so used the next two, which are by name
        %upper_bin_bound = %{$_[2]};
        %lower_bin_bound = %{$_[3]};
    }

    my (%occs_by_bin,%taxa_by_bin);
    my (%occs_by_bin_taxon,%occs_by_category,%occs_by_bin_cat_taxon);
    my (%occs_by_bin_and_category,%taxa_by_bin_and_category);
    foreach my $row (@$results) {
        # count up occurrences per time interval bin
        my $interval = $interval_lookup{$row->{collection_no}};
        my $bin_key = $row->{'o.genus_name'};
        if ($q->param("occurrences_species_name") eq 'YES') {
            $bin_key .="_".$row->{'o.species_name'};
        } 
        # only use occurrences from collections that map into exactly one bin
        if ($interval) {
            $occs_by_bin{$interval}++;
            $occs_by_bin_taxon{$interval}{$bin_key}++;
            if ( $occs_by_bin_taxon{$interval}{$bin_key} == 1 )    {
                $taxa_by_bin{$interval}++;
            }
        }
        # now things get nasty: if a field was selected to
        #  break up into categories, add to the count involving
        #  the appropriate enum value
        # WARNING: algorithm assumes that only enum fields are
        #  ever selected for processing
        if ( $q->param('binned_field') )    {
            my $row_value;
            if ( $q->param('binned_field') eq "ecology" )    {
                # special processing for ecology data
                $row_value= $row->{$q->param('ecology1')}; 
                #$ecotaph{$bin_key}{$ecoFields[0]};
            } else {
                # default processing
                if ($q->param('binned_field') =~ /plant_organ/) {
                    $row_value = $row->{"o.".$q->param('binned_field')};
                } else {
                    $row_value = $row->{"c.".$q->param('binned_field')};
                }
            }
            $occs_by_bin_and_category{$interval}{$row_value}++;
            $occs_by_bin_cat_taxon{$interval}{$row_value.$bin_key}++;
            if ( $occs_by_bin_cat_taxon{$interval}{$row_value.$bin_key} == 1 )    {
                $taxa_by_bin_and_category{$interval}{$row_value}++;
            }
            $occs_by_category{$row_value}++;
        }
    }

    # print out a list of time intervals with counts of occurrences

    my @intervalnames;
    #  list of 10 m.y. bin names
    if ( $q->param('time_scale') =~ /bin/ )    {
        @intervalnames = TimeLookup::getTenMYBins();
    } else {
        @intervalnames = TimeLookup::getScaleOrder($dbt,$q->param('time_scale'));
    }

    # need a list of enum values that actually have counts
    # NOTE: we're only using occs_by_category to generate this list,
    #  but it is kind of cute
    my @enumvals = sort keys %occs_by_category;

    # now print the results
    my @scaleline;
    push @scaleline, 'interval','lower boundary','upper boundary','midpoint','total occurrences';
    if ($q->param("occurrences_species_name") eq 'YES') {
        push @scaleline, 'total species';
    } else {
        push @scaleline, 'total genera';
    }
    foreach my $val ( @enumvals )    {
        if ( $val eq "" )    {
            push @scaleline, 'no data occurrences', 'no data taxa';
        } else    {
            push @scaleline, "$val occurrences", "$val taxa";
        }
    }
    foreach my $val ( @enumvals )    {
        if ( $val eq "" )    {
            push @scaleline, 'proportion no data occurrences', 'proportion no data taxa';
        } else    {
            push @scaleline, "proportion $val occurrences", "proportion $val taxa";
        }
    }
    print SCALEFILE $self->formatRow(@scaleline)."\n";

    foreach my $intervalName ( @intervalnames ) {
        my @scaleline = ();
        push @scaleline, $intervalName;
        push @scaleline, sprintf("%.2f",$lower_bin_bound{$intervalName});
        push @scaleline, sprintf("%.2f",$upper_bin_bound{$intervalName});
        push @scaleline, sprintf("%.2f",($lower_bin_bound{$intervalName} + $upper_bin_bound{$intervalName}) / 2);
        push @scaleline, sprintf("%d",$occs_by_bin{$intervalName});
        push @scaleline, sprintf("%d",$taxa_by_bin{$intervalName});
        foreach my $val ( @enumvals )    {
            push @scaleline, sprintf("%d",$occs_by_bin_and_category{$intervalName}{$val});
            push @scaleline, sprintf("%d",$taxa_by_bin_and_category{$intervalName}{$val});
        }
        foreach my $val ( @enumvals )    {
            if ( $occs_by_bin_and_category{$intervalName}{$val} eq "" )    {
                push @scaleline, "0.0000","0.0000";
            } else    {
                push @scaleline, sprintf("%.4f",$occs_by_bin_and_category{$intervalName}{$val} / $occs_by_bin{$intervalName});
                push @scaleline, sprintf("%.4f",$taxa_by_bin_and_category{$intervalName}{$val} / $taxa_by_bin{$intervalName});
            }
        }
        print SCALEFILE $self->formatRow(@scaleline)."\n";
    }
    close SCALEFILE;
    return (scalar(@intervalnames),$scaleFile);
}


# The purpose of this function is set up various implied fields in the Q object
# so other functions (retellOptions and queryDatabsae) can behave correctly.  Should
# never have to be manually called, it will be called as needed by the various functions
# that use it.
sub setupQueryFields {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbt = $self->{'dbt'};
    $self->{'setup_query_fields_called'} = 1;

    # Setup default parameters
    $q->param('output_format'=>'csv') if ($q->param('output_format') !~ /csv|tab|conjunct/i);
    $q->param('output_data'=>'occurrences') if ($q->param('output_data') !~ /collections|occurrences|specimens|genera|species/);

    if ($q->param('output_format') =~ /conjunct/i) {
        $q->param("output_data"=>"occurrences");
        $q->param("collections_regionalbed"=>"YES");
        $q->param("collections_regionalsection"=>"YES");
        $q->param("collections_regionalbedunit"=>"YES");
        $q->param("collections_localbed"=>"YES");
        $q->param("collections_localsection"=>"YES");
        $q->param("collections_localbedunit"=>"YES");
    }

    # Setup additional fields that should appear as columns in the CSV file

    # Just always download these two fields, since so many other fields are dependent on them 
    $q->param('collections_max_interval_no' => "YES");
    $q->param('collections_min_interval_no' => "YES");

    # and hey, if you want counts of some enum field split up within
    #  time interval bins, you have to download that field
    if ( $q->param('binned_field') )    {
        if ( $q->param('binned_field') =~ /plant_organ/ )    {
            $q->param('occurrences_' . $q->param('binned_field') => "YES");
        } else    {
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

    if ($q->param('collections_tectonic_plate_id')) {
        if (!$q->param("collections_coords")) {
            $q->param("collections_coords"=>"YES");
        }
    }

    if ($q->param('lump_by_coord')) {
        if (!$q->param("collections_coords")) {
            $q->param("collections_coords"=>"YES");
        }
    }
    if ($q->param('lump_by_mbr')) {
        $q->param('collections_formation'=>'YES');
        $q->param('collections_member'=>'YES');
    }
    if ($q->param('lump_by_ref')) {
        $q->param('collections_reference_no'=>'YES');
    }
    if ($q->param('collections_paleocoords')) {
        if ($q->param('collections_paleocoords_format') eq 'DMS') {
            $q->param('collections_paleolngdeg'=>'YES');
            $q->param('collections_paleolngmin'=>'YES');
            $q->param('collections_paleolngsec'=>'YES');
            $q->param('collections_paleolngdir'=>'YES');
            $q->param('collections_paleolatdeg'=>'YES');
            $q->param('collections_paleolatmin'=>'YES');
            $q->param('collections_paleolatsec'=>'YES');
            $q->param('collections_paleolatdir'=>'YES');
        } else { 
            $q->param('collections_paleolngdec'=>'YES');
            $q->param('collections_paleolatdec'=>'YES');
        }
    }
    if ($q->param('collections_coords')) {
        if ($q->param('collections_coords_format') eq 'DMS') {
            $q->param('collections_lngdeg'=>'YES');
            $q->param('collections_lngmin'=>'YES');
            $q->param('collections_lngsec'=>'YES');
            $q->param('collections_lngdir'=>'YES');
            $q->param('collections_latdeg'=>'YES');
            $q->param('collections_latmin'=>'YES');
            $q->param('collections_latsec'=>'YES');
            $q->param('collections_latdir'=>'YES');
        } else {
            $q->param('collections_latdec'=>'YES');
            $q->param('collections_lngdec'=>'YES');
        }
    }

    if ( $q->param('split_subgenera') eq 'YES') {
        $q->param('occurrences_subgenus_name'=>'YES');
    } 

    # Get the right lat/lng/paleolat/paleolng fields
    if ($q->param("collections_regionalbed")) {
        $q->param("collections_regionalbedunit"=>"YES");
    }
    if ($q->param("collections_localbed")) {
        $q->param("collections_localbedunit"=>"YES");
    }
    
    # Need to get the species_name field as well
    if ($q->param('output_data') =~ /species/) {
        $q->param('occurrences_species_name'=>'YES');
    }

    # Get these related fields as well
    if ($q->param('occurrences_subgenus_name')) {
        $q->param('occurrences_subgenus_reso'=>'YES');
    }
    if ($q->param('occurrences_species_name')) {
        $q->param('occurrences_species_reso'=>'YES');
    }

    # Required fields
    $q->param("occurrences_genus_name"=>'YES'); 
    $q->param("occurrences_genus_reso"=>'YES');

    # Get EML values, check interval names
    if ($q->param('max_interval_name')) {
        if ($q->param('max_interval_name') =~ /[a-zA-Z]/) {
            my ($eml, $name) = TimeLookup::splitInterval($dbt,$q->param('max_interval_name'));
            my $ret = Validation::checkInterval($dbt,$eml,$name);
            if (!$ret) {
                push @form_errors, "There is no record of ".$q->param('max_interval_name')." in the database";
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
                push @form_errors, "There is no record of ".$q->param('min_interval_name')." in the database";
                $q->param('min_interval_name'=>'');
                $q->param('min_eml_interval'=>'');
            } else {
                $q->param('min_interval_name'=>$name);
                $q->param('min_eml_interval'=>$eml);
            }
        }
    }

    # Specimen measurements implied fields
    my @params = $q->param();
    foreach my $f (@params) {
        if ($f =~ /^specimens_/) {
            $q->param('include_specimen_fields'=>1);
            # Output won't make sense without this
            $q->param('specimen_part'=>'YES');
            $q->param('specimens_specimens_measured'=>'YES');
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
            push @form_errors, "The taxon name '$taxon' is ambiguous and belongs to multiple taxonomic hierarchies. Right the download script can't distinguish between these different cases. If this is a problem email <a href='mailto: alroy\@nceas.ucsb.edu'>John Alroy</a>.";
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
}

sub formatRow {
    my $self = shift;
    my $csv = $self->{'csv'};

    if ( $csv->combine ( @_ ) ) {
        return $csv->string();
    }
}

# renamed from getGenusNames to getTaxonString to reflect changes in how this works PS 01/06/2004
sub getTaxonString {
    my $self = shift;
    my $q = $self->{'q'};
    my $dbt = $self->{'dbt'};
    my $dbh = $self->{'dbh'};

    my @taxon_nos_unique;
    my $taxon_nos_string;
    my $genus_names_string;

    my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));

    my @sql_bits;
    my %taxon_nos_unique = ();
    foreach my $taxon (@taxa) {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon);
        $self->dbg("Found ".scalar(@taxon_nos)." taxon_nos for $taxon");
        if (scalar(@taxon_nos) == 0) {
            push @sql_bits, "genus_name LIKE ".$dbh->quote($taxon);
        } elsif (scalar(@taxon_nos) == 1) {
            my @all_taxon_nos = TaxaCache::getChildren($dbt,$taxon_nos[0]);
            # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
            @taxon_nos_unique{@all_taxon_nos} = ();
        } else { #result > 1
            #do nothing here, quit above
        }
    }
    if (%taxon_nos_unique) {
        push @sql_bits, "taxon_no IN (".join(", ",keys(%taxon_nos_unique)).")";
    }
    return @sql_bits;
}

sub dbg {
    my $self = shift;
    my $message = shift;

    if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

    return $DEBUG;                    # Either way, return the current DEBUG value
}


1;
