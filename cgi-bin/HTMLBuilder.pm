package HTMLBuilder;

use TimeLookup;
use CGI qw(escapeHTML);
use Class::Date qw(now);
use Data::Dumper;
use Reference;
use Person;
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD);
use strict;

# Package wide variable, little messy but other modules need to access it
%HTMLBuilder::hard_lists = (
    abund_unit=>['', 'specimens', 'individuals', 'elements', 'fragments', 'category', 'rank', 'grid-count', 'quadrats', '%-specimens', '%-individuals', '%-elements', '%-fragments', '%-quadrats', '%-volume', '%-area'], 

    release_date=>['immediate','three months','six months','one year','two years','three years','four years','five years'],
    # Not for any drop down yet, used in DL
    lithology_siliciclastic => ['"siliciclastic"','claystone','mudstone','"shale"','siltstone','sandstone','conglomerate'],
    lithology_mixed => ['"mixed carbonate-siliciclastic"','marl'],
    lithology_carbonate => ['lime mudstone','wackestone','packstone','grainstone','"reef rocks"','floatstone','rudstone','bafflestone','bindstone','framestone','"limestone"','dolomite','"carbonate"'],

    # For environment dropdown
    environment_general=>['marine indet.', 'terrestrial indet.'],
    environment_carbonate=> ['carbonate indet.', '', 'peritidal', 'shallow subtidal indet.', 'open shallow subtidal', 'lagoonal/restricted shallow subtidal', 'sand shoal', '', 'reef, buildup or bioherm', 'perireef or subreef', 'intrashelf/intraplatform reef', 'platform/shelf-margin reef', 'slope/ramp reef', 'basin reef', '', 'deep subtidal ramp', 'deep subtidal shelf', 'deep subtidal indet.', 'offshore ramp', 'offshore shelf', 'offshore indet.', '', 'slope', 'basinal (carbonate)', 'basinal (siliceous)'],
    environment_siliciclastic => ['marginal marine indet.', 'coastal indet.', '', 'estuary/bay', 'lagoonal', 'paralic indet.', '', 'delta plain', 'interdistributary bay', 'delta front', 'prodelta', 'deltaic indet.', '', 'foreshore', 'shoreface', 'transition zone/lower shoreface', 'offshore', 'coastal indet.', '', 'submarine fan', 'basinal (siliciclastic)', 'basinal (siliceous)', 'basinal (carbonate)', 'deep-water indet.'],
    environment_terrestrial=>['terrestrial indet.', '', 'fluvial indet.', 'alluvial fan', 'channel lag', 'coarse channel fill', 'fine channel fill', '"channel"', 'wet floodplain', 'dry floodplain', '"floodplain"', 'crevasse splay', 'levee', 'mire/swamp', 'fluvial-lacustrine indet.', 'delta plain', 'fluvial-deltaic indet.', '', 'lacustrine - large', 'lacustrine - small', 'pond', 'crater lake', 'lacustrine delta plain', 'lacustrine interdistributary bay', 'lacustrine delta front', 'lacustrine prodelta', 'lacustrine deltaic indet.', 'lacustrine indet.', '', 'dune', 'interdune', 'loess', 'eolian indet.', '', 'cave', 'fissure fill', 'sinkhole', 'karst indet.', '', 'tar', 'mire/swamp', 'spring', 'glacial'],
    zone_lacustrine=>['lacustrine - large', 'lacustrine - small', 'pond', 'crater lake', 'lacustrine delta plain', 'lacustrine interdistributary bay', 'lacustrine delta front', 'lacustrine prodelta', 'lacustrine deltaic indet.', 'lacustrine indet.'],
    zone_fluvial=>['fluvial indet.', 'alluvial fan', 'channel lag', 'coarse channel fill', 'fine channel fill', '"channel"', 'wet floodplain', 'dry floodplain', '"floodplain"', 'crevasse splay', 'levee', 'mire/swamp', 'fluvial-lacustrine indet.', 'delta plain', 'fluvial-deltaic indet.'],
    zone_karst=>['cave', 'fissure fill', 'sinkhole', 'karst indet.'],
    zone_other_terrestrial=>['dune', 'interdune', 'loess', 'eolian indet.','tar', 'spring', 'glacial'],
    zone_marginal_marine=>['marginal marine indet.','peritidal','lagoonal/restricted shallow subtidal','estuary/bay','lagoonal','paralic indet.','delta plain','interdistributary bay'],
    zone_reef=>['reef, buildup or bioherm','perireef or subreef','intrashelf/intraplatform reef','platform/shelf-margin reef','slope/ramp reef','basin reef'],
    zone_shallow_subtidal=>['shallow subtidal indet.','open shallow subtidal','delta front','foreshore','shoreface','sand shoal'],
    zone_deep_subtidal=>['transition zone/lower shoreface','deep subtidal ramp','deep subtidal shelf','deep subtidal indet.'],
    zone_offshore=>['offshore ramp','offshore shelf','offshore indet.','prodelta','offshore'],
    zone_slope_basin=>['slope','basinal (carbonate)','basinal (siliceous)','submarine fan','basinal (siliciclastic)','basinal (siliceous)','basinal (carbonate)','deep-water indet.'],
    # Map form parameters 
    mapsize=>[ '50%', '75%', '100%', '125%', '150%' ],
    projection=>[ 'Eckert IV', 'equirectangular', 'Mollweide', 'orthographic' ],
    mapshape=>[ 'landscape', 'square' ],
    mapfocus=>['standard (0,0)', 'Africa (10,20)', 'Antarctica (-90,0)', 'Arctic (90,0)', 'Asia (20,100)', 'Australia (-28,135)', 'Europe (50,10)', 'North America (35,-100)', 'Pacific (0,150)', 'South America (-20,-60)', 'Western Hemisphere (0,-75)'],
    mapscale=>['X 1', 'X 1.2', 'X 1.5', 'X 2', 'X 2.5', 'X 3', 'X 4', 'X 5', 'X 6',  'X 7', 'X 8', 'X 9', 'X 10', 'X 12', 'X 15', 'X 20', 'X 30', 'X 40'],
mapwidth=>['100%','90%','80%','75%','70%','60%','50%'],
    mapresolution=>[ 'coarse', 'medium', 'fine', 'very fine' ],
    gridsize=>['none', '45 degrees', '30 degrees', '22.5 degrees', '15 degrees', '10 degrees', '5 degrees', '2 degrees', '1 degree'],
    gridposition=>[ 'in front', 'in back' ],
    linethickness=>[ 'thin', 'medium', 'thick' ],
    mapcolors=>['white','black','gray','light gray','red','dark red','pink','deep pink','violet','orchid','magenta','dark violet','purple','slate blue','teal','cyan','turquoise','steel blue','sky blue','dodger blue','royal blue','blue','dark blue','lime','light green','sea green','green','dark green','olive drab','olive','orange red','dark orange','orange','gold','yellow','medium yellow','tan','sandy brown','chocolate','saddle brown','sienna','brown'],
    pointsize=>[ 'tiny', 'very small', 'small', 'medium', 'large', 'very large', 'huge', 'proportional'],
    pointshape=>[ 'circles', 'crosses', 'diamonds', 'squares', 'stars', 'triangles'],
    dotborder=>[ 'no', 'black', 'white' ],
    mapsearchfields=>[ 'research group', 'country', 'state/province', 'time interval', 'formation', 'lithology', 'paleoenvironment', 'taxon' ],
    research_group=>['', 'decapod','divergence', 'GCP', 'marine invertebrate', 'micropaleontology', 'paleobotany', 'paleoentomology', 'taphonomy', 'vertebrate', 'ETE', '5%', '1%', 'PACED', 'PGAP'],

    simplecolors=>['','black','gray','red','pink','purple','blue','green','orange','yellow'],
    linescalings=>['x 1','x 2','x 3','x 5','x 10','x 20','x 30','x 50','x 100'],

    # Taphonomy/ecology and vertebrate cology fields
    fr_habitat=>['','terrestrial','marine','freshwater','marine,terrestrial','marine,freshwater','freshwater,terrestrial'],
#        ['','terrestrial','marine','freshwater','fresh./marine'    ,'terr./marine'      ,'terr./fresh.']],
    mass_units=>['g','kg'],
    ecovert_life_habit=>[ '','fossorial','ground dwelling','scansorial','arboreal','volant','amphibious','aquatic'],
    ecovert_reproduction=>['', 'oviparous','ovoviviparous','viviparous'],
    ecovert_diet=>['','herbivore','frugivore','folivore','browser','grazer','granivore','omnivore','insectivore','carnivore','piscivore','durophage'],
    species_name=>['','indet.','sp.'],
    period_max=>['', 'Holocene', 'Neogene', 'Paleogene', 'Cretaceous', 'Jurassic', 'Triassic', 'Permian', 'Carboniferous', 'Devonian', 'Silurian', 'Ordovician', 'Cambrian', 'Neoproterozoic']
);

# Initializes all the autogenerated lists and what not
sub new {
    my ($class,$dbt,$s,$use_guest,$use_admin) = @_;
    my $self = {};

    # Some quickie functions
    my $months = sub {
        my @k = ("","December","November","October","September","August","July","June","May","April","March","February","January"); 
        my @v = ("",reverse(1..12)); return (\@k,\@v)
    };
    my $day_of_month = sub{my @d = ("",reverse(1..31)); return (\@d)};
    my $years = sub{my @d = ("",reverse(1998..now->year)); return (\@d)};

    # Normal lists - first try this, then revert to trying the hard_lists
    my $select_lists = {
        #period_max=>[\&_listFromHardList,'periods'],
        environment=>[\&_listFromList,
            '','-- General --','',@{$HTMLBuilder::hard_lists{'environment_general'}},
            '', '-- Carbonate marine --', '', @{$HTMLBuilder::hard_lists{'environment_carbonate'}},
            '', '-- Siliciclastic marine --', '', @{$HTMLBuilder::hard_lists{'environment_siliciclastic'}},
            '', '-- Terrestrial --','',@{$HTMLBuilder::hard_lists{'environment_terrestrial'}}], 
        lithadj=> [\&_listFromEnum,'collections','lithadj' ,'space_after'=>'paleosol/pedogenic,lag,very coarse,volcaniclastic,shelly/skeletal'],
        lithadj2=>[\&_listFromEnum,'collections','lithadj2','space_after'=>'paleosol/pedogenic,lag,very coarse,volcaniclastic,shelly/skeletal'],
        lithology1=>[\&_listFromEnum,'collections','lithology1','space_after'=>'not reported,breccia,marl,"carbonate",radiolarite,tar,siderite,quartzite'],
        lithology2=>[\&_listFromEnum,'collections','lithology2','space_after'=>'not reported,breccia,marl,"carbonate",radiolarite,tar,siderite,quartzite'],
        seq_strat=>[\&_listFromEnum,'collections','seq_strat','space_after'=>'late glacial'],
        common_body_parts=>[\&_listFromEnum,'collections','common_body_parts','space_after'=>'other,eggs,valves,appendages,nymphs,stem ossicles'],
        rare_body_parts=>[\&_listFromEnum,'collections','rare_body_parts','space_after'=>'other,eggs,valves,appendages,nymphs,stem ossicles'],
        type_body_part=>[\&_listFromEnum,'authorities','type_body_part','space_after'=>'other,egg,valve,appendages,nymph,stem ossicles'],
        ecotaph=>[\&_listFromTable,'ecotaph'],
        eml_interval=>[\&_listFromEnum,'intervals','eml_interval'],
        eml_max_interval=>[\&_listFromEnum,'intervals','eml_interval'],
        eml_min_interval=>[\&_listFromEnum,'intervals','eml_interval'],
        habitat=>[\&_listFromEnum,'inventories','habitat','space_after'=>'terrestrial,mangrove,freshwater'],
        inventory_method=>[\&_listFromEnum,'inventories','inventory_method'],
        inventory_size_unit=>[\&_listFromEnum,'inventories','inventory_size_unit'],
        mapbgcolor=>[\&_listFromHardList,'mapcolors'],
        platecolor=>[\&_listFromHardList,'mapcolors','unshift'=>'none'],
        crustcolor=>[\&_listFromHardList,'mapcolors','unshift'=>'none'],
        crustedgecolor=>[\&_listFromHardList,'mapcolors','unshift'=>'none'],
        gridcolor=>[\&_listFromHardList,'mapcolors'],
        latlngnocolor=>[\&_listFromHardList,'mapcolors'],
        coastlinecolor=>[\&_listFromHardList,'mapcolors'],
        borderlinecolor=>[\&_listFromHardList,'mapcolors','unshift'=>'none'],
        usalinecolor=>[\&_listFromHardList,'mapcolors','unshift'=>'none'],
        pointsize1=>[\&_listFromHardList,'pointsize'],
        pointshape1=>[\&_listFromHardList,'pointshape'],
        dotcolor=>[\&_listFromHardList,'mapcolors'],
        country=>[\&_countryList],
        time_scale=>[\&_timeScaleList],
        # hack
        preservation2=>[\&_listFromEnum,'authorities','preservation'],
        opinion_basis=>[\&_listFromEnum,'opinions','basis'],
        reference_basis=>[\&_listFromEnum,'refs','basis'],
        month=>[$months], day_of_month=>[$day_of_month], year=>[$years],

        # Specimen measurement form 
        specimen_side=>[\&_listFromEnum,'specimens','specimen_side'],
        specimen_coverage=>[\&_listFromEnum,'specimens','specimen_coverage'],
        measurement_source=>[\&_listFromEnum,'specimens','measurement_source'],
        length_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        width_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        height_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        diagonal_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        inflation_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        d13C_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        d18O_error_unit=>[\&_listFromEnum,'measurements','error_unit'],
        specimen_is_type=>[\&_listFromList, 'no','holotype','paratype','some paratypes'],
    };
   
    # This block 'installs' a code reference in $select_lists to generate the list on the fly when its needed
    my %table_enums = (
        'cladograms' => ['source'],
        'authorities' => ['taxon_rank','preservation','form_taxon','extant'],
        'opinions' => ['phylogenetic_status','diagnosis_given'],
        'refs'=> ['project_name','publication_type','basis','language'],
        'collections'=> ['access_level', 'latdir', 'lngdir', 'latlng_basis', 'gps_datum', 'altitude_unit', 'geogscale', 'direct_ma_unit', 'direct_ma_method', 'max_ma_unit', 'max_ma_method', 'min_ma_unit', 'min_ma_method', 'zone_type', 'localbedunit', 'localorder', 'regionalbedunit', 'regionalorder', 'stratscale', 'lithification', 'lithification2', 'minor_lithology', 'minor_lithology2', 'tectonic_setting', 'lagerstatten', 'concentration', 'orientation', 'preservation_quality', 'abund_in_sediment', 'art_whole_bodies', 'disart_assoc_maj_elems', 'disassoc_maj_elems', 'disassoc_minor_elems', 'sorting', 'fragmentation', 'bioerosion', 'encrustation', 'temporal_resolution', 'spatial_resolution', 'articulated_parts', 'associated_parts', 'collection_type', 'collection_size_unit', 'rock_censused_unit', 'museum'],
        'ecotaph'=>['composition1', 'composition2', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'ontogeny', 'grouping', 'taxon_environment', 'locomotion', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'vision', 'reproduction', 'dispersal1', 'dispersal2','body_mass_source','body_mass_type'],
        'occurrences'=>['genus_reso', 'subgenus_reso', 'species_reso', 'plant_organ', 'plant_organ2'],
        'scales'=>['continent','basis','scale_rank']
    );
    
    while(my ($table,$fields) = each %table_enums) {
        foreach my $f (@{$fields}) {
            $select_lists->{$f} = [\&_listFromEnum,$table,$f];
        }
    }
    
    $self->{'dbt'} = $dbt;
    $self->{'s'} = $s;
    $self->{'use_guest'} = $use_guest;
    $self->{'use_admin'} = $use_admin;
    $self->{'select_lists'} = $select_lists;
    $self->{'included'} = {};
    $self->{'template_cache'} = {};
    return bless($self,$class);
}

# This function rewrites HTML on the fly based on a hash of key/values it receives.
# The hash may be passed as \@values and \@keys (legacy format) or as a \%hash.
#
# Select lists are automatically populated from the database if need be based on their
# name= attribute.  If you want the list to be populated from to be separate form the
# name=, you can specify autofill= in the select header.  i.e. <select name='lithologies' autofill='lithology1'>
# would keep the name of the select lithologies but populate it with select_lists->{lithology1}
#
# Subsitution is performed on variables of the type %%var_name%%
# and variables can be passed in to selectively show or hide
# chunks of code using the syntax <div show="show_if_this_var_exists"></div>
# or <span ..ditto..></span> or <div hide="hide_if_this_var_exists"></div> or
# <span ..ditton..></span>. You can pass simple conditions as well in the show=,
# I.E. <div hide="type=add"> .. </div> will only the block of text if the $vars{type} is "add"
# <span id="var_name"></span> is alternate syntax, but %%var_name%% is probably easier
#
# The optional read_only array (or hash) is a list of elements that you want to be marked read_only, which places
# A span made up to look like an empty box in its place. The special value 'all' may be passed to make all inputs
# in the form just text.  I.E. $hbo->populateHTML($template,$vars,["all"]). Caveat: the form elements completely
# disappear, they aren't carried through as hiddens right now
sub populateHTML {
    my ($self, $template, $vars_ref, $legacy_keys_ref, $read_only) = @_;

    my $vars;
    if(ref($vars_ref) eq "HASH"){
        $vars = $vars_ref;
        $read_only = $legacy_keys_ref;
    } elsif(ref($vars_ref) eq "ARRAY") {
        my %vars;
        @vars{@{$legacy_keys_ref}}=(@{$vars_ref});
        $vars = \%vars;
    }

    my %read_only = ();
    if (ref($read_only) eq 'ARRAY') {
        $read_only{$_} = 1 for (@{$read_only});
    } elsif (ref($read_only) eq 'HASH') {
        %read_only = %$read_only;
    }

    # CGI.pm separates multiple values by byte 0 (NULL value)
    while (my ($k,$v) = each %$vars) {
        $vars->{$k} = join(",",split(/\0/,$v));
    }

    # Add in some other basic vars from session
    my $s = $self->{'s'};
    $vars->{'is_contributor'} = 1 if ($s->isDBMember());
    $vars->{'is_fossil_record'} = 1 if ($IS_FOSSIL_RECORD);

    # Basic sanitizing
    $template =~ s/\.htm(l)?$//;
    $template =~ s/[^\/A-Za-z0-9_-]//g;

    # Two steps: if we're a db member, first try members templates/ dir
    # Else if we're guest or have been forced to use guest templates, try those, else print no success
    my $file = "";

    if ($self->{'use_admin'}) {
        my $try_file = "../html/admin/".$template.".html";
        $file = $try_file if (-e $try_file);
    } else {
        if ($IS_FOSSIL_RECORD && $s->isDBMember() && !$self->{'use_guest'}) {
            my $try_file = "fr_templates/".$template.".html";
            $file = $try_file if (-e $try_file);
        } 
        if (!$file && $s->isDBMember() && !$self->{'use_guest'}) {
            my $try_file = "templates/".$template.".html";
            $file = $try_file if (-e $try_file);
        }
        if (!$file && $IS_FOSSIL_RECORD) {
            my $try_file = "fr_guest_templates/".$template.".html";
            $file = $try_file if (-e $try_file);
        } 
        if (!$file) {
            my $try_file = "guest_templates/".$template.".html";
            $file = $try_file if (-e $try_file);
        }
    }
    
    if ($file) {
        $self->{'template_cache'}{$file} = $self->parseTemplate($file) unless ($self->{'template_cache'}{$file});
        return $self->writeBlock($self->{'template_cache'}{$file},$vars,\%read_only,0);
    } else {
        return "Can not open template $template";
    }
}

# This handles actual parsing.  It basically parses the html in a tree of nodes of stuff we care about.
# Each node is called a "block" and stored in that variable.  The block is a simple struct with the following relevant fields:
# block = {
#   type => (root, sub, radio, checkbox, text, hidden, password, print, textarea, select, show, hide)
#   children=> reference to array of blocks contained in this one. for a textarea, the value is stored in this array, for a show/hide, elements you want to show and hide are stored here. for a select, options are stored here, etc.
#   tag => original html tag (may be same as type)
#   content => only relevant for print node, the content to print
#   parent => pointer back to parent
#   attribs {  extra html tag attributes, mostly globbed togetther in other - see parseAttribs
#       value : for a print node: value you want to print out, for a radio/checkbox/option: value= attrib
#       autofill : only relevant for  
#       other : misc html attributes such as cols, onClick, etc we don't care about but want to save
#   }
# } 
# For the block->{type} variable: 
#   "radio", "checkbox", "text", "hidden", "password" correspond to the <input type="xxx"> counterparts.  
#   "textarea" and "select" are their html counterparts as well.  
#   "sub" is a subsitution variable that corresponds to %%varname%% or <span id="varname"></span> in the html.  
#   "show" corresponds to the <div show="xxx"> type html, and should only be shown when xxx is passed in to populateHTML.  Same with "hide".  
#   "print" variables are just plain old html between the items of interest and should just be printed. 
#   "root" is the root of the tree, useful to tag it as a sanity check
sub parseTemplate {
    my $self = shift;
    my $filename = $_[0];

    open FH,"<$filename" or return undef;
    my $txt = join("",<FH>);

    my $root = {
        'type'=>'root',
        'children'=>[]
    };
    $root->{'parent'} = $root;
    # $node is the "focal" node.  Keeps track of where we currently are in the.  I.E. when we see a <select>
    # $node becomes <select> until </select> happens.  For something like <input>
    # don't change $node though, since <input> doesn't have a closing tag.
    my $node = $root;

    # pos = byte_offset into the file - keep track of how much of the original file we've read
    my $last_pos = 0;

    # Hack, simpler this way
    $txt =~ s/<span\s+id=(['"]?)(\w+)\1?>\s*<\/span>/%%$2%%/gis;
    $txt =~ s/\$exec_url/bridge.pl/gis;
    $txt =~ s/%%READ_URL%%/$READ_URL/gis;
    $txt =~ s/%%WRITE_URL%%/$WRITE_URL/gis;

    my %select_counter; #Keeps track of how many times we've seen a <select> with the same name
    # Since some groups of single selects are treated like multiple select boxes
    # Keep finding interesting tags - don't modify $txt or it'll reset pos()! (pos return current file char offset)
    while ($txt =~ m/(%%(\w+)%%|<(\/?)(!-- optional|!-- end|input|div|span|select|textarea|option)(?:>| (.*?)>))/gis) {
        my $content = $1;
        my $length = length($content);
        my $start = pos($txt) - $length;
        my $tag = lc($4);
        my $is_end = ($3 || $tag eq "!-- end" || $tag eq 'option');
        my $is_start = (!$3);
        my $rest = $5;
        my $has_closing_tag = ($tag =~ /^(?:!-- optional|div|span|select|textarea|option)$/) ? 1 : 0;

        # Save all the intermediate text we've skipped over
        if ($start > $last_pos) {
            my $skipped_text =  {
                'tag'=>'text',
                'type'=>'print',
                'children'=>[],
                'parent'=>$node,
                'content'=>substr($txt,$last_pos,($start-$last_pos))
            };
            #printMsg("skipped",$skipped_text);
            push @{$node->{'children'}}, $skipped_text;
        }
        # Have read up to here
        $last_pos = pos($txt);
       
        # Textarea, select, end we move up one but don't need to print anything
        # option we move up one if the node above isn't a select node (happens cause there is sometimes no option end tag)
        # Select we move up either 1 or 2 if the last option was closed or not
        if ($is_end) {
            my $ended_node = $node; # The node that just got ended - node will be
                                    # replaced by its parent just below
            if ($tag eq 'option') {
                $node = $node->{'parent'} if ($node->{'tag'} ne 'select');
            } elsif ($tag eq 'select') {
                $node = $node->{'parent'};
                $node = $node->{'parent'} if ($node->{'tag'} eq 'select');
            } else {
                $node = $node->{'parent'};
            }
            if ($tag =~ /span|div/) { 
                # We want to save end tags for div/span only, but only for div/span where the original <div>|<span>
                # tags didn't have a a show= attribute
                unless ($ended_node->{'type'} =~ /hide|show/ && $ended_node->{'tag'} =~ /span|div/) {
                    my $end_tag=  {
                        'tag'=>$tag,
                        'type'=>'print',
                        'children'=>[],
                        'content'=>$content,
                        'parent'=>$node
                    };
                    #printMsg($tag,$end_tag,"Ending");
                    push @{$node->{'children'}}, $end_tag;
                }
            } 
        } 
        if ($is_start) {
            my $new_node = {
                'tag'=>$tag,
                'children'=>[]
            };
            if ($2) {
                $new_node->{'type'} = 'sub';
                $new_node->{'name'} = $2;
            } elsif ($tag eq '!-- optional') {
                $new_node->{'type'} = 'show';
                if ($rest =~ m/^(\w+)\W/) {
                    $new_node->{'name'} = $1;
                }
            } else {
                my $attribs = $self->parseAttribs($rest);
                # Reformat <div show= and <span show= to be in more general 'show' format
                if ($tag =~ /^(?:div|span)$/ && $attribs->{'hide'}) {
                    $new_node->{'type'} = 'hide';
                    $new_node->{'name'} = $attribs->{'hide'};
                } elsif ($tag =~ /^(?:div|span)$/ && $attribs->{'show'}) {
                    $new_node->{'type'} = 'show';
                    $new_node->{'name'} = $attribs->{'show'};
                } elsif ($tag eq 'input') {
                    $new_node->{'type'} = ($attribs->{'type'} || 'text');
                    $new_node->{'name'} = ($attribs->{'id'} || $attribs->{'name'});
                } elsif ($tag eq 'select' || $tag eq 'textarea' || $tag eq 'option') {
                    $new_node->{'type'} = $tag;
                    $new_node->{'name'} = ($attribs->{'id'} || $attribs->{'name'});
                    if ($tag eq 'select') {
                        # We need to keep track of how times we've seen each selecte
                        # since we treat some clusters of single selects as a multiselect
                        if ($select_counter{$new_node->{'name'}}) {
                            $new_node->{'global_count'} = $select_counter{$new_node->{'name'}};
                            $new_node->{'my_count'} = ${$select_counter{$new_node->{'name'}}};
                            ${$select_counter{$new_node->{'name'}}} += 1;
                        } else {
                            my $new_count = 1;
                            $new_node->{'global_count'} = \$new_count;
                            $new_node->{'my_count'} = $new_count - 1;
                            $select_counter{$new_node->{'name'}} = \$new_count;
                        }
                    }
                } else {
                    $new_node->{'type'} = 'print';
                    $new_node->{'content'} = $content;
                }
                $new_node->{'attribs'} = $attribs;
            }
            $new_node->{'parent'} = $node;
            push @{$node->{'children'}}, $new_node;

            #printMsg($tag,$new_node);
            # Change our focal node
            if ($has_closing_tag) {
                $node = $new_node;
            } 
        }
    } 
    # Save last bit
    my $file_length = length($txt);
    if ($file_length > $last_pos) {
        my $skipped_text =  {
            'tag'=>'text',
            'type'=>'print',
            'children'=>[],
            'parent'=>$node,
            'content'=>substr($txt,$last_pos,($file_length-$last_pos))
        };
        #printMsg("skipped",$skipped_text);
        push @{$node->{'children'}}, $skipped_text;
    }
    return $root;
}

# Debug;
sub printMsg {
    my $tag = shift;
    my $node = shift;
    my $msg = (shift || "Adding");
    my $j = $node;
    $j = $j->{'parent'} if ($msg =~ /ending/i);
    while($j = $j->{'parent'}) {
        last if $j->{'type'} eq 'root';
        print "\t";
    }
    print "$msg node for $tag($node->{type}:$node->{name}) to parent ($node->{parent}{type}:$node->{parent}{name})\n";
}


# Handles writing of the parsed template handled by parseTemplate
# Pretty much does a depth first traversal, printing out stuff as it sees it.
# Depth is for debuggin
sub writeBlock {
    my ($self,$block,$vars,$read_only,$depth) = @_;
    my $attribs = $block->{'attribs'};
    my $html = "";

    #if ($vars->{'debug'}) {
    #    print "    "x$depth;
    #    print "Node ($block->{tag}:$block->{type}:$block->{name}) has ".scalar(@{$block->{'children'}})." children\n";
    #}
    $depth++;
    
    if ($block->{'type'} eq 'root') {
        foreach my $c (@{$block->{'children'}}) {
            $html .= $self->writeBlock($c,$vars,$read_only,$depth);
        }
    } if ($block->{'type'} eq 'print') {
        $html .= $block->{'content'};
        foreach my $c (@{$block->{'children'}}) {
            $html .= $self->writeBlock($c,$vars,$read_only,$depth);
        }
    } if ($block->{'type'} =~ /^(?:button|submit|hidden|text|password|file)$/) {
        my $value = '';
        if ($block->{'type'} eq 'hidden') {
            # For hidden attributes (like action) the default value takes precedence if it exists
            if ($attribs->{'value'} || lc($attribs->{'replace'}) eq 'no') {
                $value = escapeHTML($attribs->{'value'});
            } elsif (exists ($vars->{$block->{'name'}})) {
                $value = escapeHTML($vars->{$block->{'name'}});
            } 
        } else {
            # Else the passed in value takes precedence
            if (exists ($vars->{$block->{'name'}})) {
                $value = escapeHTML($vars->{$block->{'name'}});
            } else {
                $value = escapeHTML($attribs->{'value'});
            }
        }
        $html .= qq|<input type="$block->{type}"|;
        $html .= qq| name="|.escapeHTML($block->{name}).qq|"| if ($block->{'name'} ne '');
        $html .= qq| value="$value"| unless $block->{'type'} eq 'password';
        $html .= qq| $attribs->{other}| if ($attribs->{'other'});
        $html .= " />";
        if ($read_only->{'all'} || $read_only->{$block->{'name'}}) {
            if ($block->{'type'} eq 'hidden') {
                $html = "";
            } else {
                #my $new_value = sprintf("%-20s",$value); 
                #$new_value =~ s/ /&nbsp;/g;
                my $new_value = $value;
                $html = "<span class=\"readOnlyInput\">$new_value</span>";
            }
        } 
    } elsif ($block->{'type'} =~ /^(?:radio|checkbox)/) {
        my $value = $attribs->{'value'};
        my $checked = "";
        if ($vars->{$block->{'name'}}) {
            $attribs->{'other'} =~ s/\s*checked\s*/ /;
            my @all_v = split(/\s*,\s*/,$vars->{$block->{'name'}});
            if (! @all_v && $vars->{$block->{'name'}} ne "")	{
                push @all_v , $vars->{$block->{'name'}};
            }

            foreach (@all_v) {
                if ($value eq $_) {
                    $checked = "checked";
                }
            }
        }
        # The default value (CHECKED or not) will be in $attribs->{other}, so don't have to do anything
        $value = escapeHTML($value);
        $html .= qq|<input type="$block->{type}"|;
        $html .= qq| name="|.escapeHTML($block->{name}).qq|"| if ($block->{'name'} ne '');
        $html .= qq| value="$value"|;
        $html .= " ".$checked if $checked;
        $html .= " ".$attribs->{'other'} if ($attribs->{'other'});
        $html .= " />";
        if ($read_only->{'all'} || $read_only->{$block->{'name'}}) {
            my $checked_symbol = ($checked) ? "X" : "&nbsp; ";
            $html = "<span class=\"readOnlyCheckBox\">$checked_symbol</span>";
        }
    } elsif ($block->{'type'} eq 'select') {
        my $selected;
        if (${$block->{'global_count'}} > 1) {
            my @values = split(/\s*,\s*/,$vars->{$block->{'name'}});
            $selected = $values[$block->{'my_count'}];
        } else {
            $selected = $vars->{$block->{'name'}};
        }
        my @options = grep {$_->{'tag'} eq 'option'} @{$block->{'children'}};
        if (@options ) {
            my (@keys,@values);
            foreach my $o (@options) {
                my $key;
                foreach my $c (@{$o->{'children'}}) {
                    $key.= $self->writeBlock($c,$vars,$read_only,$depth);
                }
                $key =~ s/^\s*|\s*$//gs;
                my $value = (exists $o->{'attribs'}{'value'}) ? $o->{'attribs'}{'value'} : $key;
                push @keys,$key;
                push @values,$value;
                if ($o->{'attribs'}{'other'} =~ /selected/ && ! exists $vars->{$block->{'name'}}) {
                    $selected = $value;
                }
            }
            $html .= $self->htmlSelect($block->{'name'},\@keys,\@values,$selected,$attribs->{'other'});
        } else {
            my $select_list = $attribs->{'autofill'} || $block->{'name'};
            my ($keys,$values) = $self->getKeysValues($select_list);
            $html .= $self->htmlSelect($block->{'name'},$keys,$values,$selected,$attribs->{'other'});
        }
        if ($read_only->{'all'} || $read_only->{$block->{'name'}}) {
            #my $new_value = sprintf("%-20s",$selected); 
            #$new_value =~ s/ /&nbsp;/g;
            my $new_value = $selected;
            $html = "<span class=\"readOnlyInput\">".sprintf("%-20s",$new_value)."</span>";
        }
    } elsif ($block->{'type'} eq 'textarea') {
        my $value = '';
        if (exists ($vars->{$block->{'name'}})) {
            $value = escapeHTML($vars->{$block->{'name'}});
        } else {
            foreach my $c (@{$block->{'children'}}) {
                $value.= $self->writeBlock($c,$vars,$read_only,$depth);
            }
        } 
        $html .= qq|<textarea name="$block->{'name'}"|;
        $html .= " ".$attribs->{'other'} if ($attribs->{'other'});
        $html .= ">$value</textarea>";
        if ($read_only->{'all'} || $read_only->{$block->{'name'}}) {
            #my $new_value = sprintf("%-20s",$value); 
            #$new_value =~ s/ /&nbsp;/g;
            my $new_value = $value;
            $html = "<span class=\"readOnlyInput\">".sprintf("%-20s",$new_value)."</span>";
        }
    } elsif ($block->{'type'} eq 'sub') {
        $html .= $vars->{$block->{'name'}};
    } elsif ($block->{'type'} eq 'show') {
        my ($k,$v) = split(/=/,$block->{'name'});
        if ((!$v && $vars->{$k} ne '') || ($v && $vars->{$k} eq $v)) {
            foreach my $c (@{$block->{'children'}}) {
                $html .= $self->writeBlock($c,$vars,$read_only,$depth);
            }
        } 
    } elsif ($block->{'type'} eq 'hide') {
        my ($k,$v) = split(/=/,$block->{'name'});
        unless ((!$v && $vars->{$k} ne '') || ($v && $vars->{$k} eq $v)) {
            foreach my $c (@{$block->{'children'}}) {
                $html .= $self->writeBlock($c,$vars,$read_only,$depth);
            }
        }
    }
    return $html;
}

sub parseAttribs {
    my $self = shift;
    my $txt = shift;
    my %attribs;
    while ($txt =~ s/(?:^|\s)(autofill|show|hide|type|id|value|name|replace)=(['"])(.*?)\2//) {
        $attribs{$1} = $3;
    }
    while ($txt =~ s/(?:^|\s)(autofill|show|hide|type|id|value|name|replace)=(\w+)//) {
        $attribs{$1} = $2;
    }
    if ($txt !~ /^\s*$/) {
        $txt =~ s//^s*/;
        $attribs{'other'} = $txt;
    }
    if ($attribs{'id'}) {
        $attribs{'name'} = $attribs{'id'};
    }
    return \%attribs;
}

# One public interface to getting various lists, whether they be
# hardcoded or from the database. List context only. Returns
# refs to keys and values arrays
sub getKeysValues {
    my $self = shift;
    my $name = shift;
    if (exists $self->{'select_lists'}{$name}) {
        my ($keys,$values);
        my ($func,@args) = @{$self->{'select_lists'}{$name}};
        my @r = $func->($self,@args);
        if (scalar(@r) == 2) {
            $keys = $r[0];
            $values = $r[1];
        } elsif (scalar(@r) == 1) {
            $keys = $r[0];
            $values = $r[0];
        }
        return ($keys,$values);
    } elsif (exists $HTMLBuilder::hard_lists{$name}) {
        my (@keys,@values);
        @keys = @{$HTMLBuilder::hard_lists{$name}};
        if (ref $keys[0] eq 'ARRAY') {
            my ($keys_ref,$values_ref) = @keys;
            @keys = @{$keys_ref};
            @values = @{$values_ref};
        } else {
            @values = @keys;
        }
        return (\@keys,\@values);
    } else {
        return ([],[]);
    }
}

# Second public interface - for most simple lists, just returns it
# I.E. @environemnt = $hbo->getList('environment')
# Returns the second arrayref as a normal array from getKeysValues
sub getList {
    my $self = shift;
    my $name = shift;
    my $distinct = shift;
    # remove duplicates (a problem with the environment pulldown)
    if ( $distinct )	{
        my ($keys,$values) = $self->getKeysValues($name);
        my %seen;
        my @newValues;
        foreach my $v (@$values) {
            unless ($seen{$v}) {
                push @newValues, $v;
                $seen{$v}++;
            }
        }
        return @newValues;
    } else	{
        #return @{ $self->getKeysValues($name) };
        my ($k,$v) = $self->getKeysValues($name);
        return @{$v};
    }
}

sub htmlSelect {
    # Don't need the object ref, just shift it off
    shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
	
	my $name = $_[0];
	my @keys = @{$_[1]};
	my @values = @{$_[2]};
	my $toSelect = $_[3];
	my $other = $_[4];

	my $html = "<select name=\"$name\" $other>\n";


    for(my $i=0;$i<scalar(@keys);$i++) {
        my $selected = ($toSelect ne '' && $values[$i] eq $toSelect) ? " SELECTED" : "";
        my $value = escapeHTML($values[$i]);
        my $key = $keys[$i];
		$html .= qq|  <option value="$value"$selected>$key</option>\n|;
	}
	$html .= "</select>";
	
	return $html;
}

sub radioSelect {
    # Don't need the object ref, just shift it off
    shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
	
	my $name = $_[0];
	my @keys = @{$_[1]};
	my @values = @{$_[2]};
	my $toSelect = $_[3];

	my $html = "";

    for(my $i=0;$i<scalar(@keys);$i++) {
        my $selected = ($toSelect ne '' && $values[$i] eq $toSelect) ? " checked default" : "";
        my $value = escapeHTML($values[$i]);
        my $key = $keys[$i];
		$html .= qq|  <input type="radio" name="$name" value="$value"$selected />$key<br />|;
	}
	
	return $html;
}



# Create the thin line boxes
# arg 1 = title, arg2 = content
sub htmlBox {
    shift if ref $_[0];
    my $html = '<div class="displayPanel" align="left">'
             . qq'<span class="displayPanelHeader">$_[0]</span>'
             . qq'<div class="displayPanelContent">'
             . qq'<div class="displayPanelText">$_[1]'
             . '</div></div></div>';
    return $html;
}

# This only shown for internal errors
sub htmlError {
    my ($self,$message) = @_;

	# print $q->header( -type => "text/html" );
    print $self->stdIncludes("std_page_top");
	print $message;
    print $self->stdIncludes("std_page_bottom");
	exit 1;
}

# This is a wrapper to put the goodies into the standard page bottom
# or top.  Pass it the name of a template such as "std_page_top" and 
# it will return the HTML code.
sub stdIncludes {
    my ($self,$page,$vars) = @_;
    $vars ||= {};

    my $s = $self->{'s'};
    my $dbt = $self->{'dbt'};

    if ($self->{included}{$page}) {
        return "";
    } else {
        $self->{included}{$page} = 1;
    }

    if ($s->isDBMember()) {
        $vars->{reference} = 'none';
        my $reference_no = $s->get('reference_no');
        if ($reference_no) {
            $vars->{reference_no} = $reference_no;
            $vars->{reference} = Reference::formatShortRef($dbt,$reference_no,'no_inits'=>1);
        }
        $vars->{enterer} = $s->get("enterer") || "none"; 
    }

	return $self->populateHTML($page,$vars);
}

sub _listFromTable {
    my ($self,$name) = @_;
    my $dbt = $self->{'dbt'};
    my @columns = grep {!/_no$|^created$|^modified$/} $dbt->getTableColumns($name);
    unshift @columns, "";
    return (\@columns);
}

sub _listFromList {
    my ($self,@values) = @_;
    return (\@values);
}

sub _listFromHardList {
    my ($self,$name,%options) = @_;
    my @list = @{$HTMLBuilder::hard_lists{$name}};
    unshift @list,$options{'unshift'} if (exists $options{'unshift'});
    
    return \@list;
}

sub _listFromEnum {
    my ($self,$table,$field,%options) = @_;
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;

    my $sql = "SHOW COLUMNS FROM `$table` LIKE '$field'";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $row = $sth->fetchrow_arrayref();
    if ($row) {
        my %space_after;
        $space_after{$_} = 1 for split(/\s*,\s*/,$options{'space_after'});
        my $def = $row->[1];
        $def =~ s/^enum\('|^set\('|'\)$//g;
        my @values;
        foreach (split(/','/,$def)) {
            push @values,$_;
            push @values, '' if ($space_after{$_});
        }
        if (lc($row->[2]) eq 'yes') { # NULL=YES, add blank value
            unshift @values, '' unless $values[0] eq '';
        }
        return (\@values);
    } else {
        die "bad call to listFromEnum";
    }
}

# JA 29.2.04
sub _timeScaleList {
    my $self = shift;
    my $dbt = $self->{'dbt'};

	# get the time scales and their ID numbers
	my $sql = "SELECT scale_no,scale_name FROM scales";
	my @timescalerefs = @{$dbt->getData($sql)};

    my @sorted = 
        sort {$b->{'scale_name'} =~ /Gradstein .:/ <=> $a->{'scale_name'} =~ /Gradstein .:/ ||
              $a->{'scale_name'} cmp $b->{'scale_name'}} @timescalerefs;
    for (my $i=0;$i<@sorted;$i++) {
        # Insert a space after gradstein 7 (scale_no 73)
        if ($sorted[$i]->{'scale_no'} == 73) { 
            splice(@sorted,$i+1,0,{'scale_no'=>'','scale_name'=>''});
            last;
        }
    }

    my @k = map {$_->{'scale_name'}} @sorted;
    my @v = map {$_->{'scale_no'}} @sorted;
    unshift @k, "","PBDB 10 m.y. bins","";
    unshift @v, "","PBDB 10 m.y. bins","";
    return \@k,\@v;
}

sub _countryList {
        return (['','United States', 'Argentina', 'Australia', 'Canada', 'China', 'France', 'Germany', 'Italy', 'Japan', 'Mexico', 'New Zealand', 'Russian Federation', 'Spain', 'United Kingdom', 'Afghanistan', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo-Brazzaville', 'Congo-Kinshasa', 'Cook Islands', 'Costa Rica', "Cote D'Ivoire", 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'East Timor', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'France, Metropolitan', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakstan', 'Kenya', 'Kiribati', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macau', 'Macedonia, the Former Yugoslav Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova', 'Monaco', 'Mongolia', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'Netherlands Antilles', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'North Korea', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Reunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Helena', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia and Montenegro', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'South Korea', 'Sri Lanka', 'Sudan', 'Suriname', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela', 'Vietnam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Western Sahara', 'Yemen', 'Zaire', 'Zambia', 'Zimbabwe']);
}

1;
