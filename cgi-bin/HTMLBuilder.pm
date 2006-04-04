package HTMLBuilder;
use strict;

use SelectList;
use TextField;
use Checkbox;
use Radiodial;
use TextArea;
use Hidden;
use Anchor;
use Class::Date qw(date localdate gmdate now);
use Debug;
use Person;

use CGI;

my $stuff;
my %SELECT_LISTS;

sub new {
    my $class = shift;
    my $templateDir = shift;
    my $dbt = shift;
    my $exec_url = shift;
    my $self = {};
    bless $self, $class;
    $self->{'SELECT_LISTS'} = \%SELECT_LISTS;
    $self->{'CACHED_TEMPLATES'} = {};
    $self->{'template_dir'} = $templateDir;
    $self->{'exec_url'} = $exec_url;
    $self->{'dbt'} = $dbt;
    return $self;
}

# Note: geogscale has labels different from values <OPTION value="small collection">small collection (&lt; 10 x 10 m)<OPTION value=outcrop>outcrop (&lt; 1 x 1 km)<OPTION value="local area">local area (&lt; 100 x 100 km)<OPTION value=basin>basin (&gt; 100 x 100 km)</OPTION>

%SELECT_LISTS = (assigned_to=>["Ederer", "Alroy"],
                    severity=>["Cosmetic", "Annoying", "Important", "Critical"],
					taxon_rank=>['subspecies', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade'],
					parent_taxon_rank=>['', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade'],
					type_taxon_rank=>['', 'specimen number', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade'],
                    current_status=>["active", "deferred", "retired"],
					species_name=>['', 'indet.', 'sp.'],
					project_name=>['', 'decapod', 'divergence', 'ETE', '5%', '1%', 'PACED', 'PGAP'],
					publication_type=>['', 'journal article', 'book/book chapter', 'serial monograph', 'Ph.D. thesis', 'M.S. thesis', 'abstract', 'guidebook','news article', 'unpublished'],
					classification_quality=>['authoritative', 'standard', 'second hand'],
					release_date=>['immediate','three months','six months','one year','two years','three years','four years','five years'],
					access_level=>['the public','database members', 'group members', 'authorizer only'],
					latdir=>['North','South'],
					lngdir=>['West', 'East'],
					latlng_basis=>['','stated in text','based on nearby landmark','based on political unit','estimated from map','unpublished field data'],
					gps_datum=>['','NAD27 CONUS','NAD83','WGS72','WGS84'],
					altitude_unit=>['', 'meters', 'feet'],
					geogscale=>['', 'hand sample', 'small collection', 'outcrop', 'local area', 'basin'],
					emlperiod_max=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					period_max=>['', 'Holocene', 'Neogene', 'Paleogene', 'Cretaceous', 'Jurassic', 'Triassic', 'Permian', 'Carboniferous', 'Devonian', 'Silurian', 'Ordovician', 'Cambrian', 'Neoproterozoic'],
					emlperiod_min=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					period_min=>['', 'Holocene', 'Neogene', 'Paleogene',  'Cretaceous', 'Jurassic', 'Triassic', 'Permian', 'Carboniferous', 'Devonian', 'Silurian', 'Ordovician', 'Cambrian', 'Neoproterozoic'],
					emlepoch_max=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					emlepoch_min=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					emlintage_max=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					emlintage_min=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					emllocage_max=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					emllocage_min=>['', 'Late/Upper', 'Middle - Late/Upper', 'Middle', 'Early/Lower - Middle', 'Early/Lower'],
					localbedunit=>['','m','cm','ft'],
					localorder=>['', 'bottom to top', 'top to bottom', 'no particular order'],
					regionalbedunit=>['','m','cm','ft'],
					regionalorder=>['', 'bottom to top', 'top to bottom', 'no particular order'],
					stratscale=>['', 'bed', 'group of beds', 'member', 'formation', 'group'],
					lithification=>['', 'unlithified', 'poorly lithified', 'lithified', 'metamorphosed'],
					lithification2=>['', 'unlithified', 'poorly lithified', 'lithified', 'metamorphosed'],
					#lithology1=>['', 'claystone', 'mudstone', '"shale"', 'siltstone', 'sandstone', 'conglomerate', '', 'marl', '', 'lime mudstone', 'wackestone', 'packstone', 'grainstone', '"reef   rocks"', '"limestone"', 'dolomite', '"carbonate"', '', 'chert', 'phosphorite', 'ironstone', 'siderite'],
					#lithology2=>['', 'claystone', 'mudstone', '"shale"', 'siltstone', 'sandstone', 'conglomerate', '', 'marl', '', 'lime mudstone', 'wackestone', 'packstone', 'grainstone', '"reef   rocks"', '"limestone"', 'dolomite', '"carbonate"', '', 'chert', 'phosphorite', 'ironstone', 'siderite'],
					#lithadj=>['', 'desiccation cracks', 'current ripples', 'dunes', 'hummocky CS', 'wave ripples', '"cross stratification"', 'planar lamination', 'tool marks', 'flute casts', 'deformed bedding', 'grading', 'burrows', 'bioturbation', '', 'condensed', 'firmground', 'hardground', 'lag', '', 'argillaceous', 'silty', 'sandy', 'conglomeratic', 'pebbly', '', 'calcareous', 'carbonaceous', 'cherty/siliceous', 'concretionary', 'dolomitic', 'ferruginous', 'glauconitic', 'hematitic', 'pyritic', 'phosphatic', 'sideritic', '', 'flat-pebble', 'intraclastic', 'oncoidal', 'ooidal', 'peloidal', 'shelly/skeletal', '', 'black', 'gray', 'green', 'red or brown', 'yellow'],
					#lithadj2=>['', 'desiccation cracks', 'current ripples', 'dunes', 'hummocky CS', 'wave ripples', '"cross stratification"', 'planar lamination', 'tool marks', 'flute casts', 'deformed bedding', 'grading', 'burrows', 'bioturbation', '', 'condensed', 'firmground', 'hardground', 'lag', '', 'argillaceous', 'silty', 'sandy', 'conglomeratic', 'pebbly', '', 'calcareous', 'carbonaceous', 'cherty/siliceous', 'concretionary', 'dolomitic', 'ferruginous', 'glauconitic', 'hematitic', 'pyritic', 'phosphatic', 'sideritic', '', 'flat-pebble', 'intraclastic', 'oncoidal', 'ooidal', 'peloidal', 'shelly/skeletal', '', 'black', 'gray', 'green', 'red or brown', 'yellow'],
                    lithology_siliciclastic => ['"siliciclastic"','claystone','mudstone','"shale"','siltstone','sandstone','conglomerate'],
                    lithology_mixed => ['"mixed carbonate-siliciclastic"','marl'],
                    lithology_carbonate => ['lime mudstone','wackestone','packstone','grainstone','"reef rocks"','floatstone','rudstone','bafflestone','bindstone','framestone','"limestone"','dolomite','"carbonate"'],
					lithology1=>['', '"siliciclastic"', 'claystone', 'mudstone', '"shale"', 'siltstone', 'sandstone', 'conglomerate', '', '"mixed carbonate-siliciclastic"', 'marl', '', 'lime mudstone', 'wackestone', 'packstone', 'grainstone', '"reef rocks"', 'floatstone', 'rudstone', 'bafflestone', 'bindstone', 'framestone', '"limestone"', 'dolomite', '"carbonate"', '', "coal",'peat', 'lignite', 'subbituminous coal', 'bituminous coal', 'anthracite', 'coal ball', 'tar', '', 'amber', 'chert', 'evaporite', 'phosphorite', 'ironstone', 'siderite', '', 'phyllite', 'slate', 'schist', 'quartzite', '', '"volcaniclastic"', 'ash', 'tuff'],
                    lithology2=>['', '"siliciclastic"', 'claystone', 'mudstone', '"shale"', 'siltstone', 'sandstone', 'conglomerate', '', '"mixed carbonate-siliciclastic"', 'marl', '', 'lime mudstone', 'wackestone', 'packstone', 'grainstone', '"reef rocks"', 'floatstone', 'rudstone', 'bafflestone', 'bindstone', 'framestone', '"limestone"', 'dolomite', '"carbonate"', '', "coal",'peat', 'lignite', 'subbituminous coal', 'bituminous coal', 'anthracite', 'coal ball', 'tar', '', 'amber', 'chert', 'evaporite', 'phosphorite', 'ironstone', 'siderite', '', 'phyllite', 'slate', 'schist', 'quartzite', '', '"volcaniclastic"', 'ash', 'tuff'],
                    lithadj=>['', 'lenticular', 'tabular', 'desiccation cracks', 'current ripples', 'dunes', 'hummocky CS', 'wave ripples', '"cross stratification"', 'planar lamination', 'tool marks', 'flute casts', 'deformed bedding', 'grading', 'burrows', 'bioturbation', 'pedogenic', '', 'condensed', 'firmground', 'hardground', 'lag', '', 'argillaceous', 'micaceous', 'silty', 'sandy', 'conglomeratic', 'pebbly', '', 'very fine', 'fine', 'medium', 'coarse', 'very coarse', '', 'bentonitic', 'calcareous', 'carbonaceous', 'cherty/siliceous','concretionary', 'diatomaceous', 'dolomitic', 'ferruginous', 'glauconitic', 'gypsiferous', 'hematitic', 'pyritic', 'phosphatic', 'sideritic', 'stromatolitic', 'tuffaceous', 'volcaniclastic', '', 'flat-pebble', 'intraclastic', 'oncoidal', 'ooidal', 'peloidal', 'shelly/skeletal', '', 'black', 'blue', 'brown', 'gray', 'green', 'red', 'red or brown', 'white', 'yellow'], 
                    lithadj2=>['', 'lenticular', 'tabular', 'desiccation cracks', 'current ripples', 'dunes', 'hummocky CS', 'wave ripples', '"cross stratification"', 'planar lamination', 'tool marks', 'flute casts', 'deformed bedding', 'grading', 'burrows', 'bioturbation', 'pedogenic', '', 'condensed', 'firmground', 'hardground', 'lag', '', 'argillaceous', 'micaceous', 'silty', 'sandy', 'conglomeratic', 'pebbly', '', 'very fine', 'fine', 'medium', 'coarse', 'very coarse', '', 'bentonitic', 'calcareous', 'carbonaceous', 'cherty/siliceous','concretionary', 'diatomaceous', 'dolomitic', 'ferruginous', 'glauconitic', 'gypsiferous', 'hematitic', 'pyritic', 'phosphatic', 'sideritic', 'stromatolitic', 'tuffaceous', 'volcaniclastic', '', 'flat-pebble', 'intraclastic', 'oncoidal', 'ooidal', 'peloidal', 'shelly/skeletal', '', 'black', 'blue', 'brown', 'gray', 'green', 'red', 'red or brown', 'white', 'yellow'], 
					# General
					environment=>['', '--General--', '', 'marine indet.', 'terrestrial indet.',
					# Carbonate
					'', '--Carbonate--', '', 'carbonate indet.', 'peritidal', 'shallow subtidal indet.', 'open shallow subtidal', 'lagoonal/restricted shallow subtidal', 'sand shoal', 'reef, buildup or bioherm', 'deep subtidal ramp', 'deep subtidal shelf', 'deep subtidal indet.', 'offshore ramp', 'offshore shelf', 'offshore indet.', 'slope', 'basinal (carbonate)', 'basinal (siliceous)',
					# Siliciclastic
					'', '--Siliciclastic--', '', 'marginal marine indet.', 'paralic indet.', 'estuarine/bay', 'lagoonal', '', 'coastal indet.', 'foreshore', 'shoreface', 'transition zone/lower shoreface', 'offshore', '', 'deltaic indet.', 'delta plain', 'interdistributary bay', 'delta front', 'prodelta', '', 'deep-water indet.', 'submarine fan', 'basinal (siliciclastic)',
					# Terrestrial
					'', '--Terrestrial--', 'fluvial-lacustrine indet.', '', 'fluvial indet.', '"channel"', 'channel lag', 'coarse channel fill', 'fine channel fill', '"floodplain"', 'wet floodplain', 'dry floodplain', 'levee', 'crevasse splay', '', 'lacustrine indet.', 'lacustrine - large', 'lacustrine - small', 'pond', 'crater lake', '', 'karst indet.', 'fissure fill', 'cave', 'sinkhole', '', 'eolian indet.', 'dune', 'interdune', 'loess', '', 'fluvial-deltaic indet.', 'deltaic indet.', 'delta plain', 'interdistributary bay', '', 'alluvial fan', 'estuary', 'glacial', 'mire/swamp', 'spring', 'tar'],
					tectonic_setting=>['', 'rift', '', 'passive margin', '', 'back-arc basin', 'cratonic basin', 'deep ocean basin', 'forearc basin', 'foreland basin', 'intermontane basin', 'intramontane basin', 'piggyback basin', 'pull-apart basin', '', 'volcanic basin', 'impact basin', '', 'non-subsiding area'],
                    # These used for making lists to insert into html, derived from environment list above
					environment_general=>['marine indet.', 'terrestrial indet.'],
                    environment_carbonate=> ['carbonate indet.', 'peritidal', 'shallow subtidal indet.', 'open shallow subtidal', 'lagoonal/restricted shallow subtidal', 'sand shoal', 'reef, buildup or bioherm', 'deep subtidal ramp', 'deep subtidal shelf', 'deep subtidal indet.', 'offshore ramp', 'offshore shelf', 'offshore indet.', 'slope', 'basinal (carbonate)', 'basinal (siliceous)'],
                    environment_siliciclastic => ['marginal marine indet.', 'paralic indet.', 'estuarine/bay', 'lagoonal', 'coastal indet.', 'foreshore', 'shoreface', 'transition zone/lower shoreface', 'offshore', 'deltaic indet.', 'delta plain', 'interdistributary bay', 'delta front', 'prodelta', 'deep-water indet.', 'submarine fan', 'basinal (siliciclastic)'],
                     environment_terrestrial=>['terrestrial indet.','fluvial-lacustrine indet.',  'fluvial indet.', '\"channel\"', 'channel lag', 'coarse channel fill', 'fine channel fill', '\"floodplain\"', 'wet floodplain', 'dry floodplain', 'levee', 'crevasse splay', 'lacustrine indet.', 'lacustrine - large', 'lacustrine - small', 'pond', 'crater lake', 'karst indet.', 'fissure fill', 'cave', 'sinkhole', 'eolian indet.', 'dune', 'interdune', 'loess', 'fluvial-deltaic indet.', 'deltaic indet.', 'delta plain', 'interdistributary bay', 'alluvial fan', 'estuary', 'glacial', 'mire/swamp', 'spring', 'tar'],
					seq_strat=>['', 'transgressive', 'regressive', '', 'transgressive systems tract', 'highstand systems tract', 'lowstand systems tract', '', 'parasequence boundary', 'transgressive surface', 'maximum flooding surface', 'sequence boundary'],
					lagerstatten=>['', 'conservation', 'concentrate'],
					concentration=>['', 'dispersed', '', 'concentrated', '-single event', '-multiple events', '-seasonal', '-lag', '-hiatal', '-bonebed'],
					orientation=>['', 'life position', 'random', 'preferred'],
					preservation_quality=>['', 'excellent', 'good', 'medium', 'poor', 'variable'],
                    abund_in_sediment=>['','abundant','common','few','rare'],
					art_whole_bodies=>['', 'none', 'some', 'many', 'all'],
					disart_assoc_maj_elems=>['', 'none', 'some', 'many', 'all'],
					disassoc_maj_elems=>['', 'none', 'some', 'many', 'all'],
					disassoc_minor_elems=>['', 'none', 'some', 'many', 'all'],
					sorting=>['', 'very poor', 'poor', 'medium', 'well', 'very well'],
					fragmentation=>['', 'none', 'occasional', 'frequent', 'extreme'],
					bioerosion=>['', 'none', 'occasional', 'frequent', 'extreme'],
					encrustation=>['', 'none', 'occasional', 'frequent', 'extreme'],
					temporal_resolution=>['', 'snapshot', 'time-averaged', 'condensed'],
					spatial_resolution=>['', 'autochthonous', 'parautochthonous', 'allochthonous'],
					collection_type=>['', 'archaeological', 'biostratigraphic', 'paleoecologic', 'taphonomic', 'taxonomic', 'general faunal/floral'],
					collection_size_unit=>['', 'specimens', 'individuals'],
					rock_censused_unit=>['', 'cm (line intercept)', 'cm2 (area)', 'cm3 (volume)', 'g', 'kg', '# of surfaces (quadrat)'],
					museum=>['', 'AMNH', 'BAS', 'BGS', 'BMNH', 'BPI', 'BRU', 'CAS', 'CIT', 'CM', 'DMNH', 'FMNH', 'GSC', 'IGNS', 'IUPR', 'IVPP', 'LACM', 'MACN', 'MCZ', 'MfN', 'MLP', 'MNHN', 'MNHN (La Paz)', 'NIGPAS', 'NMMNH', 'NYSM', 'OSU', 'OU', 'OUM', 'PIN', 'PRI', 'ROM', 'SDSM', 'SGOPV', 'SM', 'SMF', 'SUI', 'TMM', 'TMP', 'UCM', 'UCMP', 'UF', 'UMMP', 'UNM', 'UNSM', 'USNM', 'UW', 'UWBM', 'YPM'],
					genus_reso=>['', 'aff.', 'cf.', 'ex gr.', 'n. gen.', 'sensu lato', '?', '"', 'informal', 'informal aff.', 'informal cf.'],
					subgenus_reso=>['', 'aff.', 'cf.', 'ex gr.', 'n. subgen.', 'sensu lato', '?', '"', 'informal', 'informal aff.', 'informal cf.'],
					species_reso=>['', 'aff.', 'cf.', 'ex gr.', 'n. sp.', 'sensu lato', '?', '"', 'informal', 'informal aff.', 'informal cf.'],
					abund_unit=>['', 'specimens', '%-specimens', 'individuals', '%-individuals', '%-volume', '%-area', 'grid-count', 'rank', 'category', '% of quadrats', '# of quadrats'],
					plant_organ=>['', 'unassigned', 'leaf', 'seed/fruit', 'axis', 'plant debris', 'marine palyn', 'microspore', 'megaspore', 'flower', 'seed repro', 'non-seed repro', 'wood', 'sterile axis', 'fertile axis', 'root', 'cuticle', 'multi organs'],
					plant_organ2=>['', 'unassigned', 'leaf', 'seed/fruit', 'axis', 'plant debris', 'marine palyn', 'microspore', 'megaspore', 'flower', 'seed repro', 'non-seed repro', 'wood', 'sterile axis', 'fertile axis', 'root', 'cuticle'],

					ecology1=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					ecology2=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					ecology3=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					ecology4=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					ecology5=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					ecology6=>['','composition1','composition2','entire_body','body_part','adult_length','adult_width','adult_height','adult_area','adult_volume','maximum_body_mass','minimum_body_mass','body_mass_type','body_mass_source','body_mass_comment','body_mass_estimate','thickness','architecture','form','reinforcement','folds','ribbing','spines','internal_reinforcement','polymorph','ontogeny','grouping','clonal','taxon_environment','locomotion','attached','epibiont','life_habit','depth_habitat','diet1','diet2','reproduction','attached','brooding','dispersal1','dispersal2'],
					composition1=>['', 'aragonite','"calcite"','high Mg calcite','low Mg calcite','hydroxyapatite','phosphatic','calcified cartilage','silica','agglutinated','chitin','lignin','"sclero-protein"','cutan/cutin','other','no hard parts'],
					composition2=>['', 'aragonite','"calcite"','high Mg calcite','low Mg calcite','hydroxyapatite','phosphatic','calcified cartilage','silica','agglutinated','chitin','lignin','"sclero-protein"','cutan/cutin','other'],
					adult_length=>['', '< 0.0001','0.0001 to 0.001','0.001 to 0.01','0.01 to 0.1','0.1 to < 1.0','1.0 to < 10','10 to < 100','100 to < 1000','1000 to < 10^4','10^4 to < 10^5','10^5 to < 10^6','10^6 to < 10^7','10^7 to < 10^8','10^8 to < 10^9','10^9 to < 10^10','10^10 to < 10^11','10^11 or more'],
					adult_width=>['', '< 0.0001','0.0001 to 0.001','0.001 to 0.01','0.01 to 0.1','0.1 to < 1.0','1.0 to < 10','10 to < 100','100 to < 1000','1000 to < 10^4','10^4 to < 10^5','10^5 to < 10^6','10^6 to < 10^7','10^7 to < 10^8','10^8 to < 10^9','10^9 to < 10^10','10^10 to < 10^11','10^11 or more'],
					adult_height=>['', '< 0.0001','0.0001 to 0.001','0.001 to 0.01','0.01 to 0.1','0.1 to < 1.0','1.0 to < 10','10 to < 100','100 to < 1000','1000 to < 10^4','10^4 to < 10^5','10^5 to < 10^6','10^6 to < 10^7','10^7 to < 10^8','10^8 to < 10^9','10^9 to < 10^10','10^10 to < 10^11','10^11 or more'],
					adult_area=>['', '< 0.0001','0.0001 to 0.001','0.001 to 0.01','0.01 to 0.1','0.1 to < 1.0','1.0 to < 10','10 to < 100','100 to < 1000','1000 to < 10^4','10^4 to < 10^5','10^5 to < 10^6','10^6 to < 10^7','10^7 to < 10^8','10^8 to < 10^9','10^9 to < 10^10','10^10 to < 10^11','10^11 or more'],
					adult_volume=>['', '< 0.0001','0.0001 to 0.001','0.001 to 0.01','0.01 to 0.1','0.1 to < 1.0','1.0 to < 10','10 to < 100','100 to < 1000','1000 to < 10^4','10^4 to < 10^5','10^5 to < 10^6','10^6 to < 10^7','10^7 to < 10^8','10^8 to < 10^9','10^9 to < 10^10','10^10 to < 10^11','10^11 or more'],
					thickness=>['', 'thin','intermediate','thick'],
					architecture=>['', 'porous','compact or dense'],
					form=>['', 'sheet','blade','inflated sheet','inflated blade','roller-shaped','spherical'],
					folds=>['', 'none','minor','major'],
					ribbing=>['', 'none','minor','major'],
					spines=>['', 'none','minor','major'],
					internal_reinforcement=>['', 'none','minor','major'],
					ontogeny=>['', 'accretion','molting','addition of parts','modification of parts','replacement of parts'],
					grouping=>['', 'colonial','gregarious','solitary'],
					taxon_environment=>['', 'coastal', 'inner shelf', 'outer shelf', 'oceanic', 'oligotrophic', 'mesotrophic', 'eutrophic', 'hypersaline','marine','brackish','freshwater','terrestrial'],
					locomotion=>['', 'stationary','facultatively mobile','passively mobile','actively mobile'],
					ecovert_life_habit=>['','fossorial','ground dwelling','scansorial','arboreal','volant','amphibious','aquatic'],
					life_habit=>['','boring','infaunal','semi-infaunal','epifaunal','nektobenthic','nektonic','planktonic','fossorial','ground dwelling','scansorial','arboreal','volant','amphibious','herbaceous','arborescent','aquatic'],
					depth_habitat=>['','surface', 'thermocline', 'subthermocline', 'deep'],
					ecovert_diet1=>['','herbivore','frugivore','folivore','browser','grazer','granivore','omnivore','insectivore','carnivore','piscivore','durophage'],
					ecovert_diet2=>['','herbivore','frugivore','folivore','browser','grazer','granivore','omnivore','insectivore','carnivore','piscivore','durophage'],
					diet1=>['','chemoautotroph','"photoautotroph"','C3 autotroph','C4 autotroph','CAM autotroph','chemosymbiotic','photosymbiotic','herbivore','frugivore','folivore','browser','grazer','granivore','omnivore','insectivore','carnivore','piscivore','durophage','parasite','suspension feeder','deposit feeder','detritivore','saprophage','coprophage'],
					diet2=>['','chemoautotroph','"photoautotroph"','C3 autotroph','C4 autotroph','CAM autotroph','chemosymbiotic','photosymbiotic','herbivore','frugivore','folivore','browser','grazer','granivore','omnivore','insectivore','carnivore','piscivore','durophage','parasite','suspension feeder','deposit feeder','detritivore','saprophage','coprophage'],
					ecovert_reproduction=>['', 'oviparous','ovoviviparous','viviparous'],
					reproduction=>['', 'oviparous','ovoviviparous','viviparous','alternating','homosporous','heterosporous','seeds','fruits'],
					dispersal1=>['', 'direct/internal','water','wind','animal'],
					dispersal2=>['', 'planktonic','non-planktonic','wind-dispersed','animal-dispersed','mobile','gravity'],
                    minimum_body_mass_unit=>['g','kg'],
                    maximum_body_mass_unit=>['g','kg'],
                    body_mass_estimate_unit=>['g','kg'],
                    body_mass_type=>['','quantitative', 'qualitative', 'unknown'],
                    body_mass_source=>['','published', 'unpublished'],
					research_group=>['', 'decapod','divergence', 'marine invertebrate', 'micropaleontology', 'paleobotany', 'paleoentomology', 'taphonomy', 'vertebrate', 'ETE', '5%', '1%', 'PACED', 'PGAP'],
					eml_interval=>['', 'Late/Upper', 'late Late', 'middle Late', 'early Late', 'Middle', 'late Middle', 'middle Middle', 'early Middle', 'Early/Lower', 'late Early', 'middle Early', 'early Early'],
					eml_max_interval=>['', 'Late/Upper', 'late Late', 'middle Late', 'early Late', 'Middle', 'late Middle', 'middle Middle', 'early Middle', 'Early/Lower', 'late Early', 'middle Early', 'early Early'],
					eml_min_interval=>['', 'Late/Upper', 'late Late', 'middle Late', 'early Late', 'Middle', 'late Middle', 'middle Middle', 'early Middle', 'Early/Lower', 'late Early', 'middle Early', 'early Early'],
					continent=>['global', 'Africa', 'Antarctica', 'Asia', 'Australia', 'Europe', 'New Zealand', 'North America', 'South America'],
					basis=>['archaeological', 'geomagnetic', 'paleontological'],
					scale_rank=>['eon/eonothem', 'era/erathem', 'period/system', 'subperiod/system', 'epoch/series', 'subepoch/series', 'age/stage', 'subage/stage', 'chron/zone'],
					mapsize=>[ '50%', '75%', '100%', '125%', '150%' ],
					projection=>[ 'Eckert', 'Mollweide', 'orthographic', 'rectilinear' ],
					mapfocus=>['standard (0,0)', 'Africa (10,20)', 'Antarctica (-90,0)', 'Arctic (90,0)', 'Asia (20,100)', 'Australia (-28,135)', 'Europe (50,10)', 'North America (35,-100)', 'Pacific (0,150)', 'South America (-10,-50)'],
					mapscale=>['X 1', 'X 1.2', 'X 1.5', 'X 2', 'X 2.5', 'X 3', 'X 4', 'X 5', 'X 6',  'X 7', 'X 8', 'X 9', 'X 10'],
					mapresolution=>[ 'coarse', 'medium', 'fine', 'very fine' ],
					mapbgcolor=>[ 'white', 'black', 'gray', 'light gray', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					crustcolor=>[ 'none', 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					gridsize=>['30 degrees', '15 degrees', '10 degrees', 'none'],
					gridcolor=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					latlngnocolor=>[ 'none', 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					gridposition=>[ 'in front', 'in back' ],
					linethickness=>[ 'thin', 'medium', 'thick' ],
					coastlinecolor=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					borderlinecolor=>[ 'none', 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					usalinecolor=>[ 'none', 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					pointsize1=>[ 'tiny', 'small', 'medium', 'large', 'huge', 'proportional'],
					pointsize2=>[ 'tiny', 'small', 'medium', 'large', 'huge', 'proportional'],
					pointsize3=>[ 'tiny', 'small', 'medium', 'large', 'huge', 'proportional'],
					pointsize4=>[ 'tiny', 'small', 'medium', 'large', 'huge', 'proportional'],
					pointshape1=>[ 'circles', 'crosses', 'diamonds', 'squares', 'stars', 'triangles'],
					pointshape2=>[ 'circles', 'crosses', 'diamonds', 'squares', 'stars', 'triangles'],
					pointshape3=>[ 'circles', 'crosses', 'diamonds', 'squares', 'stars', 'triangles'],
					pointshape4=>[ 'circles', 'crosses', 'diamonds', 'squares', 'stars', 'triangles'],
					dotcolor1=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					dotcolor2=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					dotcolor3=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					dotcolor4=>[ 'black', 'gray', 'light gray', 'white', 'dark red', 'red', 'pink', 'brown', 'light brown', 'ochre', 'orange', 'light orange', 'yellow', 'light yellow', 'green', 'light green', 'turquoise', 'jade', 'teal', 'dark blue', 'blue', 'light blue', 'sky blue', 'lavender', 'violet', 'light violet', 'purple' ],
					dotborder1=>[ 'no', 'black', 'white' ],
					dotborder2=>[ 'no', 'black', 'white' ],
					dotborder3=>[ 'no', 'black', 'white' ],
					dotborder4=>[ 'no', 'black', 'white' ],
					mapsearchfields2=>[ 'research group', 'country', 'state/province', 'time interval', 'formation', 'lithology', 'paleoenvironment', 'taxon' ],
					mapsearchfields3=>[ 'research group', 'country', 'state/province', 'time interval', 'formation', 'lithology', 'paleoenvironment', 'taxon' ],
					mapsearchfields4=>[ 'research group', 'country', 'state/province', 'time interval', 'formation', 'lithology', 'paleoenvironment', 'taxon' ],
                    country=>['','United States', 'United Kingdom', 'China', 'Canada', 'France', 'Australia', 'Russian Federation', 'Afghanistan', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Cote D'Ivoire", 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'East Timor', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'France, Metropolitan', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libyan Arab Jamahiriya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macau', 'Macedonia, the Former Yugoslav Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'Netherlands Antilles', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Reunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Helena', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia and Montenegro', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela', 'Vietnam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Western Sahara', 'Yemen', 'Zaire', 'Zambia', 'Zimbabwe'],
                    language=>['','Chinese','English','French','German','Italian','Japanese','Portugese','Russian','Spanish','other'],

                    # Specimen measurement form 
                    specimen_side=>['','left','right','left?','right?','dorsal','ventral','both'],
                    length_error_unit=>['1 s.d.','2 s.d.','95% CI'],
                    width_error_unit=>['1 s.d.','2 s.d.','95% CI'],
                    height_error_unit=>['1 s.d.','2 s.d.','95% CI'],
                    diagonal_error_unit=>['1 s.d.','2 s.d.','95% CI'],
                    inflation_error_unit=>['1 s.d.','2 s.d.','95% CI'],
                    specimen_is_type=>['no','yes, the holotype','yes, a paratype','yes, some paratypes'],
                    specimen_coverage=>['','all','some'],
                    measurement_source=>['','text','table','picture','graph','direct measurement']
				);

# This is a mess fortunately, I only have one or two of these right now.
# Not sure how I will make this easier to configure.  I'll think of something.
#my %NESTED_SELECTS = (environments=>[{

my $rowCount = 0;






# pass this a hashref
# and it will escapeHTML on all 
# fields, except the ones that look like they're already html, or the 
# ones which have "NOESCAPE" in the key name.
#
# Should do this before displaying the values in a form.
sub escapeHTMLOnFields {
	my $ref = shift;
	
	if (!$ref) {
		return;
	}
	
	foreach my $key (keys(%$ref)) {
		my $val = $ref->{$key};
		
		# only compile once.
		# we don't want to HTML escape field which are already 
		# formatted as HTML!  So we'll do a simple check to see if they
		# have anything that looks like an HTML tag in them..
		#
		# Also, we'll not escape any key which has "NOESCAPE" somewhere in the
		# key name.
		if (($key =~ m/NOESCAPE/) || 
			($val =~ m/ (?:< (?:[-A-Za-z"=0-9%._]|\s)* >)+ /xgo) ) {
			#Debug::dbPrint("I think this is HTML: " . $ref->{$key});
		} else {
			$ref->{$key} = CGI::escapeHTML($ref->{$key});
		}
	}
}



# rjp, 3/2004
#
# same as buildSelect(),
# but pass it a name of a list in the
# hard coded %SELECT_LISTS which is defined at
# the top of this HTMLBuilder file.
#
# Optionally pass the second argument as the default value
# to select.
#
# Returns HTML formatted select statement.
sub buildSelectWithHardList {
	my $self = shift;

	my $listName = shift;
	my $toSelect = shift;

	my $temp = $SELECT_LISTS{$listName};

	return $self->buildSelect($temp, $listName, $toSelect);
}



# rjp, 3/2004
#
# Pass this an arrayref, a name for the select,
# and an optional choice to select.
#
# It will return an HTML formatted select statement
# with the approriate one selected.
sub buildSelect {
    if (UNIVERSAL::isa($_[0],'HTMLBuilder')) {
        # If called from an object oriented interface, 
        # don't need the object ref, just shift it off
        shift;
    }
	
	my $name = $_[0];
	my @keys = @{$_[1]};
	my @values = @{$_[2]};
	my $toSelect = $_[3];
	
	my $html = "<select name=\"$name\" id=\"$name\">\n";

    for(my $i=0;$i<scalar(@keys);$i++) {
        my $selected = ($values[$i] eq $toSelect) ? "SELECTED" : "";
		$html .= "<option value=\"$values[$i]\" $selected>$keys[$i]</option>\n";	
	}
	$html .= "</select>\n";
	
	return $html;
}

# Creates a popup menu of taxon ranks (a select).
#
# Pass it a rank to select as the first argument,
# and optionally pass it an array ref of ranks which you want to have excluded
# from the list as the second argument.
sub rankPopupMenu {
	my $self = shift;
	my $rankToSelect = shift;
	my $toExcludeRef = shift; # optional
	
	my @toExclude;
	if ($toExcludeRef) {
		@toExclude = @$toExcludeRef;
	}
	
	my @ranks = ('subspecies', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade');
	
	my $html;
	my $selected = "";
	
	foreach my $rank (@ranks) {
		# add each rank unless it's in the exclude list.
		if ($rank eq $rankToSelect) {
			$selected = "selected";
		} else {
			$selected = '';
		}

		$html .= "<OPTION $selected>$rank</OPTION>" unless PBDBUtil::isIn(\@toExclude, $rank);
		
	}
	
	return $html;
}


# rjp, 3/2004..
# pass it a template name, and a hash ref of field names as keys 
# and values to substitute as values.  Similar to the older populateHTML, but
# much simpler, and doesn't do as much.  It also makes non-editable fields non-editable
# using an HTML tag (readonly, or disabled) instead of making them underlined text
# like the older populateHTML did.
#
# Note: all values to substitute will be assumed to take the format %%fieldname%% in
# the template file, but when passing the field name, the %% are not necessary.
#

## New Note: the automatic hidden fields feature has been disabled due to some problems.
## if re-enabling it, be very careful because it may break forms such as
## the add_enter_authority or add_enter_opinion.  3/2004.
# Note: any fields which don't exist in the template will be created as hidden input fields.
##

#
# You can optionally pass an array ref of tag names to make non-editable.  This would typically
# apply to <input> and <textarea>, although it will search for any HTML tag with a matching
# name or id. 
# For example,
# if the input is <INPUT name="myname" id="myname" value="avalue">, then you would pass
# "myname" in the array and the method will search and replace the entire input tag with
# underlined text (non-editable).
#
# Can also remove optional sections.
# Optional sections are denoted in the template by:
#
# %%START_OPTIONAL_userdefinedname%%
# optional section here
# %%END_OPTIONAL_userdefinedname%%
#
# By default, this routine will leave all optional sections in the code,
# and will automatically remove the surrounding tags.
# 
# If you want to *delete* an optional section, then pass it
# in the hash with the key = "OPTIONAL_userdefinedname" and the value = 0 (false).
#
# returns fully populated HTML string.
sub newPopulateHTML {
	my $self = shift;
	my $templateName = shift;	# template to populate (without .html extension)
	my $hashRef = shift;		# hash of fields and values to populate with
	my $nonEditableRef = shift;		# array of input fields to make non-editable
    my $dbt = $self->{'dbt'};
	
	#Debug::dbPrint("nonEditables = ");
	#Debug::printArray($nonEditableRef);
	
	
	if (! $hashRef) {
		Debug::logError("improper hashref in HTMLBuilder::newPopulateHTML.");
		return;
	}
		
	my %fields = %$hashRef;
	
	# escape the fields we're populating with for proper html encoding.
	escapeHTMLOnFields(\%fields);
	
	#Debug::dbPrint("in newPopulateHTML, fields = ");
	#Debug::printHash(\%fields);
	
	# read the template from the templates or guest_templates folder.
	my $html = $self->newReadTemplate($templateName);
	if (! $html) {
		return "<HTML><BODY><DIV class=\"warning\">Error: No template '" . $templateName ."' found...</DIV></BODY></HTML>";	
	}

	
	
	# loop through all keys in the passed hash and
	# replace field names with their values
	my $hiddenInputs;
	
	# do a preliminary loop through all keys in the fields hash
	# and see if we should add any keys such as a nicely formatted
	# reference entry, or a taxon_rank popup.
	foreach my $key (keys(%fields)) {
	
		if ($key eq 'taxon_rank') {
			# if it's name is taxon_rank, then we'll look for any fields
			# in the template called "taxon_rank_popup" and fill them with
			# a popup menu of ranks.
			
			# if the rank is 'subspecies' or 'species', then we should
			# only show the appropriate one in the list.
			#
			# however, if the rank is genus or higher, then we show everything
			# from genus on up.
			my $rank = $fields{taxon_rank};
			my @toExclude;
			
			# grab the entire list of ranks..
			@toExclude = @{$SELECT_LISTS{taxon_rank}};

			if (($rank eq 'subspecies') || ($rank eq 'species')) {  
				# then we don't want to exclude it.
				@toExclude = @{PBDBUtil::deleteElementFromArray(\@toExclude, $rank)};
			} else {
				@toExclude = ('subspecies', 'species');
			}
			
			Debug::dbPrint("toExclude = @toExclude");
			
			$fields{taxon_rank_popup} = $self->rankPopupMenu($rank, \@toExclude);
		}
	}
	
	# now do the main loop through the keys which will search and replace values
	# in the template.
	foreach my $key (keys(%fields)) {
		
		if ($key =~ m/OPTIONAL/i) {
			# this is an optional section key - see note in the method documentation.

			if (! $fields{$key}) {
				# if the value for this optional key is false, then we should
				# delete the entire optional section.
				
				# Note, the .*? is important, the ? forces this to be LAZY instead
				# of greedy so we don't go too far..
				
				$html =~ s/[%][%]START_$key[%][%].*?[%][%]END_$key[%][%]//sg;
			}
			next;
		}
		
		# replace all fields in the template which are of the form
		# %%fieldname%% with their replacement values.
		my $numReplacements = ($html =~ s/([%][%]$key[%][%])/$fields{$key}/sg);
		#if ($numReplacements <= 0) {
			#print "$key<BR>";
			# if we didn't match anything, then
			# create it as a hidden input, if it doesn't already exist somewhere.
		#	if ($html !~ m/$key/) {
		#		$hiddenInputs .= "<INPUT type=\"hidden\" name=\"$key\" id=\"$key\" value=\"" .
		#		$fields{$key} . "\">\n";
		#	}
		#}
	}
	
	
	
	# now we should also make hiddens out of any field that the user wants
	# to make non-editable.  This will allow us to still have access to their values.
	if ($nonEditableRef) {
	    my $validChars = '(?:[-A-Za-z"\'=0-9%._]|\s)*';
		foreach my $key (@$nonEditableRef) {
			# only add it to the list if we haven't already done so.. This is to 
			# prevent duplicates which will screw up the form processing script.
			if ($hiddenInputs !~ m/["]$key["]/g) {
		        if ($html !~ / ( < input (?: $validChars hidden $validChars ) ) ( $validChars (?:name|id) \s* = \s* ["]?$key["]? $validChars value \s* = \s* ["]?.*["]?  $validChars > ) /isx) {
    				$hiddenInputs .= "<INPUT type=\"hidden\" name=\"$key\" id=\"$key\" value=\"" .
	    			$fields{$key} . "\">\n";
                } 
			}
		}
	}
	
	
	# replace the %%hiddens%% with hiddensgohere so we can go ahead and
	# replace all the %% signs and fill in the non-editables before actually printing the hiddens.
	$html =~ s/[%][%]hiddens[%][%]/hiddensgohere/;
	
	# now, we'll replace any leftover %%fields%% in the template with blanks
	# so they don't show up...
	$html =~ s/[%][%][A-Za-z0-9_.-]+[%][%]//g;
	
	# now we'll make any fields non-editable that were passed in the @nonEditableInputs
	# array. This is optional.

	#Debug::printArray($nonEditableRef);
	
	if ($nonEditableRef) {
		$html = makeNonEditableInputs($html, $nonEditableRef);
	}
	
	
	# now, we'll add the hiddens (fields which didn't exist in the template,
	# but which were present in the list passed to us).
	$html =~ s/hiddensgohere/$hiddenInputs/;
	
	
	return $html;
}


# Pass this an HTML string and an array ref
# of tag names to make non editable..  
#
# It returns the modified HTML string.
#
# See the description for newPopulateHTML() for more details on this.
# 
# Mainly used by the new populateHTML method.
# rjp, 2/2004
sub makeNonEditableInputs {
	my $html = shift;
	my $nameRef = shift;
	
	if (!$nameRef) { return $html; };
	
	# static box to replace non-editable fields with.
	my $toReplace = "<SPAN class=\"nonEditable\">";
	my $end = "</SPAN>";
	
	
	# note, very important, we have to make sure that it doesn't match
	# hidden fields... That would screw stuff up.
	
	# <input type="hidden" id="taxon_name" name="taxon_name" value="%%taxon_name%%">
	# <input type="text" id="taxon_name_corrected" name="taxon_name_corrected" size="%%taxon_size%%" value="%%taxon_name%%">
		
	# This matches an optional HTML element such as size="5"
	# 	(?:[A-Za-z"=0-9%._]+ \s+)* 
	
	# This makes sure that we're not replacing a hidden field:
	#	(?! (?:[A-Za-z"=0-9%._]|\s)+ hidden (?:[A-Za-z"=0-9%._]|\s)+ > ) 
		
	# Note that (?: phrase) is the same as using regular parenthesis, but it doesn't
	# capture the value which makes it faster.
		
	# Note: the x modifier ignores whitespace in the pattern, so we
	# can space it out to make it more legible
		
	my $validChars = '(?:[-A-Za-z"\'=0-9%._]|\s)*';

	foreach my $key (@$nameRef) {
		#Debug::dbPrint("nonEditable: $key");
		# first, search for textarea fields and replace them with disabled boxes.
		# <textarea id="comments" name="comments" rows=2 cols=70>type Eurasian E. caballus</textarea>
		$html =~ s/ ( <textarea \s+ ) ( $validChars (?:name|id) \s* = \s* ["]?$key["]? $validChars > ) /$1 disabled readonly $2/igsx;
		
		# next, search for select boxes...		
		$html =~ s/( <select \s+ ) ( $validChars (?:name|id) \s* = \s* ["]?$key["]? $validChars > ) /$1 disabled readonly $2/igsx; 
		
		# Next search for other fields such as input and replace them with static boxes.
		$html =~ s/ ( < input (?! $validChars hidden $validChars > ) ) ( $validChars (?:name|id) \s* = \s* ["]?$key["]? $validChars value \s* = \s* ["]?.*["]?  $validChars > ) /$1 disabled readonly $2/igsx;

		# search for radio and checkboxes.
		$html =~ s/ ( < input (?= $validChars (?: radio|checkbox) $validChars > ) $validChars ) (  name = ["]?$key["]?  $validChars > ) /$1 disabled $2/igsx;	
	}

	return $html;
}



# rjp, 2/2004
#
# pass it a template name and it will return it
# note, it will either get it from the templates or the guest_templates
# directory depending on whether or not the user is logged in.
#
# returns 0 if it can't find the template.
sub newReadTemplate {
	my $self = shift;
	my $template = shift;
	
	if ($template eq '') {
		return 0; 
	}
	
	my $dir = $self->{'template_dir'}."/$template" . ".html";
	
	my $success = open(TEMPLATE, "<$dir");
	unless ($success) {
		# if we can't open the file...
		return 0;
	}
	
	# read the entire file in at once.
	my $string = do { local $/, <TEMPLATE> };
	
	close(TEMPLATE);
	
	return $string;
}







sub populateHTML {
  # Get the template name, the row (list of values)
  # and a list of fieldnames to scan for
  my ($self, $htmlTemplateName, $row, $fieldNames, $prefkeys) = @_;
  
  #Debug::dbPrint("start of populateHTML");
  #Debug::dbPrint("template = $htmlTemplateName, row = $row, fieldNames = $fieldNames, prefKeys = $prefKeys");
  
  my @row;
  my @fieldNames;
  if(UNIVERSAL::isa($row, "HASH")){
		while(my ($key,$value) = each %$row){
			push(@row, $value);
			push(@fieldNames, $key);
		}
  }
  elsif(UNIVERSAL::isa($row, "ARRAY")){
	  @row = @$row;
	  @fieldNames = @$fieldNames;
  }
  #print "look: " . join(',', @fieldNames);

  my $htmlTemplateString = $self->getTemplateString($htmlTemplateName,\@$prefkeys);
  
  # Substitute in the application URL if it is supplied
  my $exec_url = $self->{exec_url};

  $htmlTemplateString =~ s/(<.+?)\$exec_url/$1$exec_url/gim;

  # Do substitutions
  my $fieldNum = 0;
  foreach my $fieldName (@fieldNames) {
    my $val = $row[$fieldNum];
	# insert spaces after commas for the set members
	if($fieldName =~ /(lithadj*|research_group|pres_mode|coll_meth|museum|feed_pred_traces|assembl_comps|project_name)/){ 
		$val =~ s/,/, /g;	
	}
    
	my @split_val = split(/(<.*?>)/,$val);
	foreach my $token (@split_val) {
		if($token !~ /[<>]/){
			$token =~ s/"/&quot;/g;
		}
	}
	$val = join('',@split_val);

	# Reformat modified field JA 27.6.02
	if ($fieldName eq "modified")	{
		$val = date($val);
	}

	# genera with species equal to 'indet.' shoudn't be italicized
	if($htmlTemplateName eq 'taxa_display_row' && $fieldName eq 'species_name'){
		if($val eq 'indet.'){
			$htmlTemplateString =~ s/<i>//g;
			$htmlTemplateString =~ s/<\/i>//g;
		}	
	}

    # Do div tags (eventually, other tags should support show/hide attributes)
	# revised by JA 16.7.02
	if ($val ne "")	{
		# If we have a value, replace the div tags with just what's between
		# them (which could be <span> tags).
		$htmlTemplateString =~ s/(<optional_$fieldName>)(.*?)(<\/optional_$fieldName>)/$2/gis;
		$htmlTemplateString =~ s/(<div show="$fieldName">)(.*?)(<\/div>)/$2/gim;
	} else	{
		# Otherwise, remove the div tags, and everything inbetween (like <span>
		# tags), completely
		$htmlTemplateString =~ s/(<optional_$fieldName>)(.*?)(<\/optional_$fieldName>)//gis;
		$htmlTemplateString =~ s/<div show="$fieldName">.*?<\/div>//gim;

        # Do spans with show
        # Remove the <span> tags and everything inbetween if there is no
        # corresponding value.
        $htmlTemplateString =~ s/<span show="$fieldName">.*?<\/span>//gim;
	}
    

    # Do span tags with id
	# Else, replace span tags with just the value
    $htmlTemplateString =~ s/<span\s+id="$fieldName">.*?<\/span>/$val/gim if $val ne "";

	# Variable substitution (of form %%variable%%) -- tone
    if ( $fieldName =~ /^%%.*%%$/ ) { 
		$htmlTemplateString =~ s/$fieldName/$val/gim; 
	}

    my $keepMatching = 1;
    while($keepMatching)
    {
      $keepMatching = 0;
      # Do select tags
      if($htmlTemplateString =~ /(<select\s+id="$fieldName"(.*?)>)/im){
        my $otherstuff = $2;

		# Get a list of select lists with this name
		my @selLists = ($htmlTemplateString =~ /(<select\s+id="$fieldName".*?>)/img);
				
		# If the list has a length greater than 1, then this is an enumeration field
		if(@selLists > 1){
			my @selVals = split(/\s*,\s*/, $val);
			
			my $selListCount = 0;
			foreach my $selList (@selLists)
			{
				# Make a new SelectList instance
				my $sl = new SelectList;
		        # Set the name
		        $sl->setName($fieldName);
		        # If an array having this field name exists, use it
		        if(defined $SELECT_LISTS{$fieldName})
		        {
		          $sl->setList(@{$SELECT_LISTS{$fieldName}});
		        }
				# Set the size attribute if it has one
				if($selList =~ /size="?(\d+)"?/)
				{
					my $size = $1;
  					$sl->setSize($size);
				}
				# Set other main tag attributes if any (like class=)
				if($selList =~ /<select\s+id="$fieldName"(.*?)>/)
				{
					if($1){
  						$sl->setMainTagStuff($1);
					}
				}
                if($selList =~ /<select\s+id="$fieldName"(.*?)>/)
                {
                    if($1){
                        my $stuff = $1;
                        $sl->setMainTagStuff($stuff);
                    }
                }

		        $sl->setSelected($selVals[$selListCount]) if $selVals[$selListCount];
		        $sl->setAllowNulls(0);
		        my $htmlString = $sl->toHTML();
						
				#print "stuff: $stuff\nhtmlString: $htmlString\n\n";
						
				$htmlTemplateString =~ s/\Q$selList/$htmlString/im;
						
				$selListCount++;
			}
			next;
		}
				
        # Make a new SelectList instance
        my $sl = new SelectList;
        # Set the name
        $sl->setName($fieldName);
    	# Set other stuff
	    $sl->setMainTagStuff($otherstuff) if($otherstuff);
        # If an array having this field name exists, use it
        if(defined $SELECT_LISTS{$fieldName})
        {
          $sl->setList(@{$SELECT_LISTS{$fieldName}});
        }
		# Set the size attribute if it has one
	
		
		if ($stuff =~ /size="?(\d+)"?/)
		{
			my $size = $1;
			$sl->setSize($size);
		}
        $sl->setSelected($val);
        $sl->setAllowNulls(0);
        my $selectString = $sl->toHTML();
        $htmlTemplateString =~ s/\Q$stuff/$selectString/gim;
        $keepMatching = 1;
      }

      # Do <input> tags
      # To Do: This should match any valid input tag, not just one with id="" next to <input
      elsif ( $htmlTemplateString =~ /(<input\s+(type="{0,1}.*?"{0,1}\s+){0,1}id="{0,1}$fieldName"{0,1}.*?>)/im)
      {
        my $stuff = $1;
        #print "\n\n<!-- $stuff -->\n\n";
        # Checkboxes
        if ( $stuff =~ /type="?checkbox"?/im ) {
			# Get a list of checkboxes with this name
			my @checkboxes = ($htmlTemplateString =~ /(<input\s+id="?$fieldName"?.*?>)/img);
			# If the list has a length greater than 1, then this is an enumeration field
			if ( @checkboxes > 1 ) {
				my %CHECK_VALS;
				my @checkVals = split(/\s*,\s*/, $val);
				foreach my $checkVal (@checkVals) {
					$CHECK_VALS{$checkVal} = 1;
				}
				
				my $cbCount = 0;
				foreach my $checkbox (@checkboxes) {
					my ($formVal) = $checkbox =~ /value="(.+?)"/;
					$formVal = 1 unless $formVal;
					
					my $cb = Checkbox->new();
					$cb->setName($fieldName);
					$cb->setChecked(1) if $CHECK_VALS{$formVal};
					$cb->setValue($formVal);
					my $htmlString = '';
					# tone - took out value of NULL in the next line value attribute
					$htmlString .= qq|<input type=hidden name="$fieldName" value="">| if $cbCount == 0;
					$htmlString .= $cb->toHTML();
							
					#print "stuff: $stuff\nhtmlString: $htmlString\n\n";
							
					$htmlTemplateString =~ s/\Q$checkbox/$htmlString/im;
					# Get the value
							
					$cbCount++;
				}
				next;
			}
					
			#print "$fieldName has " . @checkboxes . "<br>";
					
			my ($formVal) = $stuff =~ /value="(.+?)"/;
			$formVal = 1 unless $formVal;
					
			my $cb = Checkbox->new();
			$cb->setName($fieldName);
			$cb->setChecked(1) if $val eq $formVal;
			$cb->setValue($formVal);
			my $htmlString = $cb->toHTML();
					
			#print "stuff: $stuff\nhtmlString: $htmlString\n\n";
					
			$htmlTemplateString =~ s/\Q$stuff/$htmlString/im;
        }
        # Do radios
		# Each radio gets the current field name for the name,
		# and the current val for the value
        elsif($stuff =~ /type="?radio"?/im)
        {
			# Get the value from the form
			$stuff =~ /value="?(.+?)"?/im;
			my $formVal = $1;

			my $rd = Radiodial->new();
			$rd->setName($fieldName);
			$rd->setValue($formVal);

			# print "val: $val formVal: $formVal<BR>\n";
			# Is there a db value?
			if ( defined ( $val ) ) {
				$rd->setChecked(1) if $val eq $formVal;
			} else {
				# See if there is a default value
				if ( $stuff =~ /checked/im ) {
					$rd->setChecked(1);
				}
			}

			my $htmlString = $rd->toHTML();
			$htmlTemplateString =~ s/\Q$stuff/$htmlString/im;
        }
        # Do submit buttons
        elsif ( $stuff =~ /type="?submit"?/im )
        {
        }
        # Do hiddens
        elsif($stuff =~ /type="?hidden"?/im)
        {
          my $hn = Hidden->new();
          $hn->setValue($val);
          $hn->setName($fieldName);
          my $htmlString = $hn->toHTML();
          $htmlTemplateString =~ s/\Q$stuff/$htmlString/gim;
        }
        # Do text fields
        else
        #else( $stuff =~ /type="?text"?/i || $stuff !~ /type/i )
        {
			my $tf = TextField->new();
			$tf->setText($val);
			$tf->setName($fieldName);

			if( $stuff =~ /size="?(\d+)"?/im)       { $tf->setSize($1); }
			if( $stuff =~ /maxlength="?(\d+)"?/im)  { $tf->setMaxLength($1); }
			if( $stuff =~ /disabled/im )            { $tf->setDisabled(); }

            my $otherstuff = $stuff;
            $otherstuff =~ s/(value|size|maxlength|type|id|name)="(.*?)"(\s|>)//igm;
            $otherstuff =~ s/(value|size|maxlength|type|id|name)=(.*?)(\s|>)//igm;
            $otherstuff =~ s/disabled//im;   
            if ($otherstuff =~ /\s+(.*?)>/)  { $tf->setMainTagStuff($1)};

			my $htmlString = $tf->toHTML();
			$htmlTemplateString =~ s/\Q$stuff/$htmlString/gim;
        }
        $keepMatching = 1;
      }

      # Do textareas
      elsif($htmlTemplateString =~ /(<textarea\s+id="?$fieldName"?.*?>)/im)
      {
        my $stuff = $1;
        
        $stuff =~ /cols="?(\d+)"?/im;
        my $formCols = $1;
        $stuff =~ /rows="?(\d+)"?/im;
        my $formRows = $1;
        
        my $ta = TextArea->new();
        $ta->setName($fieldName);
        $ta->setText($val);
        $ta->setWrapVirtual(1) if $stuff =~ /wrap="?virtual"?/im;
        $ta->setRows($formRows);
        $ta->setCols($formCols);
        my $htmlString = $ta->toHTML();
        $htmlTemplateString =~ s/\Q$stuff/$htmlString/gim;
        
        $keepMatching = 1;
      }

      # Do anchor tags
      # (To Do: Get path from ???)
      elsif($htmlTemplateString =~ /(<a\s+id="?$fieldName"?.*?>)/im)
      {
        my $stuff = $1;
        
        my $ah = Anchor->new();
        $ah->setHref("$exec_url?action=$val");
        my $htmlString = $ah->toHTML();
        $htmlTemplateString =~ s/\Q$stuff/$htmlString/gim;
        
        $keepMatching = 0;
      }
    }
    $fieldNum++;
  }
  
  # This is a global, so be careful
  $rowCount++;
  return $htmlTemplateString;
}

# Reads in the HTML template
sub getTemplateString {
	my ($self, $templateName, $prefkeys) = @_;
	
	my $templateString;
	my $htmlTemplateDir = $self->{'template_dir'};
	my %pref;
	for my $p (@$prefkeys)	{
		$pref{$p} = "yes";
	}

	my $templateFile = "$htmlTemplateDir/" . ${templateName};
	if (${templateName} !~ /\.ftp$/ && ${templateName} !~ /\.pdf$/ &&
		${templateName} !~ /\.eps$/ && ${templateName} !~ /\.gif$/ )	{
		$templateFile .= ".html";
	}

	if( $self->{'CACHED_TEMPLATES'}{$templateName} ) {
		$templateString = $self->{'CACHED_TEMPLATES'}{$templateName};
	} elsif(open(HTMLTEMPLATEFILE, $templateFile)) {
		while(<HTMLTEMPLATEFILE>) {
			if ($_ =~ /<!-- OPTIONAL/)	{
				my ($a,$b) = split /OPTIONAL /,$_,2;
				($a,$b) = split / -->/,$b,2;
				if ( $a ne "genus_and_species_only" && $pref{$a} ne "yes" || ( $a eq "genus_and_species_only" && $pref{$a} eq "yes" ) )	{
					while ($_ !~ / END $a /)	{
						$_ = <HTMLTEMPLATEFILE>;
					}
				}
			}
			$templateString .= $_;
		}
		$self->{'CACHED_TEMPLATES'}{$templateName} = $templateString;
		close HTMLTEMPLATEFILE;
	} else {
		return $templateFile . "<br>";
		#return join(',', @row) . "<br>\n";
	}
	return $templateString
}

                                    


1;
