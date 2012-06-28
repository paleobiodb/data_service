#!/opt/local/bin/perl

# This is the main controller for PBDB, everything passes through here

use lib qw(.);
use strict;	

# CPAN modules
use CGI qw(escapeHTML);
use URI::Escape;
use Text::CSV_XS;
use CGI::Carp qw(fatalsToBrowser);
use Class::Date qw(date localdate gmdate now);
use POSIX qw(ceil floor);
use DBI;

# PBDB modules
use HTMLBuilder;
use DBConnection;
use DBTransactionManager;
use Session;

# Autoloaded libs
use Person;
use PBDBUtil;
use Permissions;
use Reclassify;
use Reference;

use Collection;
use Images;
use TaxonInfo;
use TimeLookup;
use Ecology;
use Measurement;
use TaxaCache;
use TypoChecker;
use FossilRecord;
use Cladogram;
use Review;

# god awful Poling modules
use Taxon;
use Opinion;
use Validation;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HOST_URL $HTML_DIR $DATA_DIR $IS_FOSSIL_RECORD $TAXA_TREE_CACHE $DB $PAGE_TOP $PAGE_BOTTOM $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO);

#*************************************
# some global variables 
#*************************************
# 
# Some of these variable names are used throughout the code
# $q		: The CGI object - used for getting parameters from HTML forms.
# $s		: The session object - used for keeping track of users, see Session.pm
# $hbo		: HTMLBuilder object, used for populating HTML templates with data. 
# $dbt		: DBTransactionManager object, used for querying the database.
# $dbt->dbh	: Connection to the database, see DBConnection.pm
#

# Create the CGI, Session, and some other objects.
my $q = new CGI;

# Make a Transaction Manager object
my $dbt = new DBTransactionManager();

# Make the session object
my $s = new Session($dbt,$q->cookie('session_id'));

if ( $q->param('a') )	{
	$q->param('action' => $q->param('a') );
}

if ( $DB ne "eco" && $HOST_URL !~ /flatpebble|paleodb\.science\.mq\.edu\.au/ && $q->param('action') eq "login" )	{
	print $q->redirect( -url=>"http://paleodb.science.mq.edu.au/cgi-bin/bridge.pl?action=menu&user=Contributor" );
}

if ($ENV{'REMOTE_ADDR'} =~ /^188.186.181|^123.8.131.44/){exit;}

my $sql = "SHOW PROCESSLIST";
my $p = $dbt->dbh->do( $sql );
if ( $p >= 10 )	{
	if ( PBDBUtil::checkForBot() == 1 || $p >= 20 )	{
		exit;
	}
}

# Make the HTMLBuilder object - it'll use whatever template dir is appropriate
my $use_guest = (!$s->isDBMember()) ? 1 : 0;
if ( $q->param('action') eq "home" )	{
	$use_guest = 1;
}
my $hbo = HTMLBuilder->new($dbt,$s,$use_guest,'');

# process the action
# rjp, 2/2004
# Grab the action from the form.  This is what subroutine we should run.
my $action = ($q->param("action") || "menu");
	
# The right combination will allow me to conditionally set the DEBUG flag
if ($s->get("enterer") eq "J. Sepkoski" && 
    $s->get("authorizer") eq "J. Alroy" ) {
        $Debug::DEBUG = 1;
}
    
# figure out what to do with the action
if ($action eq 'displayDownloadNeptuneForm' &&  $HOST_URL !~ /flatpebble\.nceas/)	{
    print $q->redirect( -url=>'http://flatpebble.nceas.ucsb.edu/cgi-bin/bridge.pl?action='.$action);
} elsif ($action eq 'logout')	{
    logout();
    return;
} elsif ($action eq 'processLogin')	{
    processLogin();
    return;
} else	{
    if ($q->param('output_format') eq 'xml' ||
        $q->param('action') =~ /XML$/) {
        print $q->header(-type => "text/xml", 
                     -Cache_Control=>'no-cache',
                     -expires =>"now" );
    } elsif ($q->param('action') =~ /json/i )	{
        print $q->header(-type => "application/json", 
                     -Cache_Control=>'no-cache',
                     -expires =>"now" );
    } else {
        print $q->header(-type => "text/html", 
                     -Cache_Control=>'no-cache',
                     -expires =>"now" );
    }
    execAction($action);
    exit;
}

# call the proper subroutine
sub execAction {
    my $action = shift;
    $action =~ s/[^a-zA-Z0-9_]//g;
    # collection_no is a key param used all over the place and developers
    #  hitting us for JSON or XML might get its name wrong JA 28.6.12
    for my $param ( 'collection','coll','PaleoDB_collection' )	{
        if ( $q->param($param) > 0 )	{
            $q->param('collection_no' => $q->param($param));
        }
    }
    $action = \&{$action}; # Hack so use strict doesn't break
    &$action;
    exit;
}


sub processLogin {
    my $authorizer = $q->param('authorizer_reversed');
    my $enterer = $q->param('enterer_reversed');
    if ($IS_FOSSIL_RECORD) {
        $enterer = $authorizer;
    }
    my $password = $q->param('password');

    if ( $authorizer =~ /,/ )	{
        $authorizer = Person::reverseName($authorizer);
    }
    if ( $enterer =~ /,/ )	{
        $enterer = Person::reverseName($enterer);
    }

    my $cookie = $s->processLogin($authorizer,$enterer,$password);

    if ($cookie) {
        # The following two cookies are for setting the select lists
        # on the login page.

        my $cookieEnterer= $q->cookie(
                -name    => 'enterer_reversed',
                -value   => $q->param('enterer_reversed'),
                -expires => '+1y',
                -path    => "/",
                -secure  => 0);
        my $cookieAuthorizer = $q->cookie(
                -name    => 'authorizer_reversed',
                -value   => $q->param('authorizer_reversed'),
                -expires => '+1y',
                -path    => "/",
                -secure  => 0);

        print $q->header(-type => "text/html", 
                         -cookie => [$cookie, $cookieEnterer, $cookieAuthorizer],
                         -expires =>"now" );

        # the hbo object still thinks the user is a guest, so reset it
        $hbo = HTMLBuilder->new($dbt,$s,0,'');

        my $action = "menu";
        # Destination
        if ($q->param("destination") ne "") {
            $action = $q->param("destination");
        } 
        execAction($action);
    } else { # no cookie
        # failed login:  (bad password, etc.)
        my $errorMessage;

        if (!$authorizer) {
            $errorMessage = "<li> The authorizer name is required. ";
        } 
        if (!$enterer) {
            $errorMessage .= "<li> The enterer name is required. ";
        }
        if (($authorizer && $authorizer !~ /\./) ||
            ($enterer && $enterer !~ /\./)) {
            $errorMessage .= "<li> Note that the format for names is <i>Smith, A.</i> ";
        } elsif ( $authorizer =~ /[A-Za-z]\. [A-Za-z]\. / || $enterer =~ /[A-Za-z]\. [A-Za-z]\. / )	{
            $errorMessage .= "<li> Please don't enter your middle initial. ";
        }
        if (!$password) {
            $errorMessage .= "<li> The password is required. ";
        }
        if (!$errorMessage )	{
            $errorMessage .= "<li> The authorizer name, enterer name, or password is invalid. ";
        }

        print $q->header(-type => "text/html", 
                         -Cache_Control=>'no-cache',
                         -expires =>"now" );

        # return them to the login page with a message about the bad login.
        login(Debug::printErrors(["Sorry, your login failed. $errorMessage"]));
        exit;
    }
}

# Logout
# Clears the SESSION_DATA table of this session.
sub logout	{
	my $session_id = $q->cookie("session_id");
	my $dbh = $dbt->dbh;

	if ( $session_id ) {
		my $sql = "DELETE FROM session_data WHERE session_id = ".$dbh->quote($session_id);
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	}

	print $q->redirect( -url=>$WRITE_URL."?user=Contributor" );
	exit;
}

# Displays the login page
sub login	{
	my $message = shift;
	my $destination = shift;

	# logged in user has clicked the login link on the public home page...
	if ( $s->isDBMember() )	{
		menu( );
		exit;
	}

	my %vars = $q->Vars();
	$vars{'message'} = $message;
	$vars{'authorizer_reversed'} ||= $q->cookie("authorizer_reversed");
	$vars{'enterer_reversed'} ||= $q->cookie("enterer_reversed");
	$vars{'destination'} ||= $destination;

	print $hbo->stdIncludes($PAGE_TOP);
	print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('login_box', \%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayPreferencesPage {
    if (!$s->isDBMember()) {
        login( "Please log in first.");
        return;
    }
    print $hbo->stdIncludes($PAGE_TOP);
    Session::displayPreferencesPage($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub setPreferences {
    if (!$s->isDBMember()) {
        login( "Please log in first.");
        return;
    }
    print $hbo->stdIncludes($PAGE_TOP);
    Session::setPreferences($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

# displays the main menu page for the data enterers
sub menu	{
	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue(); 
	} else {
	
		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue();
		if ( $queue{'action'} ) {
	
			# Set each parameter
			foreach my $parm ( keys %queue ) {
				$q->param($parm => $queue{$parm});
			}

	 		# Run the command
			execAction($queue{'action'});
		}
	}

	if ($s->isDBMember()) {
		print $hbo->stdIncludes($PAGE_TOP);
		my $access;
		if ( $s->get('role') =~ /authorizer|student|technician/ )	{
			$access = "full";
		}
		if ( $DB ne "eco" )	{
			print $hbo->populateHTML('menu',[$access],['access']);
		} else	{
		# print header separately because links are reused elsewhere
			print qq|
<div style="margin-left: auto; margin-right: auto; margin-bottom: 2em; text-align: center;"><p class="pageTitle">Data entry functions</p></div>

<div class="displayPanel" style="width: 20em; margin-left: auto; margin-right: auto; margin-bottom: 1em; font-size: 1.1em;">

|;
			print $hbo->populateHTML('eco_menu',[$access],['access']);
			print "\n</div>\n\n";
		}
		print $hbo->stdIncludes($PAGE_BOTTOM);
	} else	{
        	if ($q->param('user') eq 'Contributor') {
			login( "Please log in first.","menu" );
		} else	{
			home();
		}
	}
}




# well, displays the home page
sub home	{
	my $error = shift;
	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue(); 
	} else {

		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue();
		if ( $queue{action} ) {
			# Set each parameter
			foreach my $parm ( keys %queue ) {
				$q->param ( $parm => $queue{$parm} );
			}
	
	 		# Run the command
            execAction($queue{'action'}); # Hack so use strict doesn't back
		}
	}

	if ( $DB eq "eco" )	{
		print $hbo->stdIncludes($PAGE_TOP);
		print $hbo->populateHTML('eco_home',{error => $error});
		print $hbo->stdIncludes($PAGE_BOTTOM);
		return;
	}

	sub lastEntry	{
		my $thing = shift;
		my $entry;
		if ( $thing->{day_now} == $thing->{day_created} )	{
			$entry = 60 * ( $thing->{hour_now} - $thing->{hour_created} )  + $thing->{minute_now} - $thing->{minute_created};
		} elsif ( $thing->{day_now} == $thing->{day_created} + 1 )	{
			$entry = 60 * $thing->{hour_now} + 60 * ( 24 - $thing->{hour_created} ) + $thing->{minute_now} - $thing->{minute_created};
		}
		if ( $entry < 60 )	{
			$entry .= " minutes ago";
			$entry =~ s/^1 minutes ago/one minute ago/;
			$entry =~ s/^0 minutes ago/this very minute/;
		} elsif ( $entry )	{
			$entry = int($entry / 60)." hours ago";
			$entry =~ s/^1 hours/one hour/;
		# hopefully this will never happen
		} else	{
			$entry = ($thing->{day_now} - $thing->{day_created})." days ago";
		}
		return $entry;
	}

	# Get some populated values
	my $sql = "SELECT * FROM statistics";
	my $row = ${$dbt->getData($sql)}[0];
	for my $f ( 'reference','taxon','collection','occurrence')	{
		$row->{$f."_total"} =~ s/(\d)(\d{6})$/$1,$2/;
		$row->{$f."_total"} =~ s/(\d)(\d{3})$/$1,$2/;
	}

	# PAPERS IN PRESS
	my $limit = 3;
	if ( $ENV{'HTTP_USER_AGENT'} =~ /Mobile/i )	{
		$limit = 1;
	}
	$sql = "SELECT CONCAT(authors,'. ',title,'. <i>',journal,'.</i> \[#',pub_no,'\]') AS cite FROM pubs WHERE created<now()-interval 1 week ORDER BY pub_no DESC LIMIT $limit";
	my @pubs;
	push @pubs , $_->{cite} foreach @{$dbt->getData($sql)};
	$row->{in_press} = '<div class="small" style="text-indent: -0.5em; margin-left: 0.5em;margin-bottom: 0.25em;">'.join(qq|</div>\n<div class="small" style="text-indent: -0.5em; margin-left: 0.5em; margin-bottom: 0.25em;">|,@pubs)."</div>";

	# MOST RECENTLY ENTERED COLLECTION
	# attempting any kind of join here would be brutal, just don't do it
	# the time computation is awful but is needed because MySQL's date
	#  subtraction functions seem to be buggy
	my $sql = "SELECT to_days(now()) day_now,to_days(created) day_created,hour(now()) hour_now,hour(created) hour_created,minute(now()) minute_now,minute(created) minute_created,reference_no,enterer_no,collection_no,collection_name,country,max_interval_no,min_interval_no FROM collections WHERE (release_date<now() OR access_level='the public') ORDER BY collection_no DESC LIMIT 1";
	my $coll = @{$dbt->getData($sql)}[0];

	$sql = "SELECT interval_no,interval_name FROM intervals WHERE interval_no IN (".$coll->{max_interval_no}.",".$coll->{min_interval_no}.")";
	my %interval_name;
	$interval_name{$_->{interval_no}} = $_->{interval_name} foreach @{$dbt->getData($sql)};
	my $first_interval = ( $coll->{min_interval_no} > 0 ) ? $interval_name{$coll->{max_interval_no}}." to ".$interval_name{$coll->{min_interval_no}} : $interval_name{$coll->{max_interval_no}};
	$row->{latest_collection} = "<a href=\"$READ_URL?a=basicCollectionSearch&amp;collection_no=$coll->{collection_no}\">".$coll->{collection_name}."</a>";
	$row->{last_timeplace} = $first_interval." of ".$coll->{country};

	$row->{last_coll_entry} = lastEntry($coll);
	$sql = "SELECT CONCAT(first_name,' ',last_name) AS name FROM person WHERE person_no=".$coll->{enterer_no};
	$row->{last_coll_enterer} = ${$dbt->getData($sql)}[0]->{name};
	$row->{last_coll_ref} = "<a href=\"$READ_URL?a=displayReference&reference_no=$coll->{reference_no}\">".Reference::formatShortRef(${$dbt->getData('SELECT * FROM refs WHERE reference_no='.$coll->{reference_no})}[0])."</a>";

	# MOST RECENTLY ENTERED SPECIES (must have reasonable data)
	$sql = "SELECT to_days(now()) day_now,to_days(a.created) day_created,hour(now()) hour_now,hour(a.created) hour_created,minute(now()) minute_now,minute(a.created) minute_created,a.reference_no,a.enterer_no,taxon_name,a.taxon_no,type_locality,type_specimen,type_body_part,r.author1last,r.author2last,r.otherauthors,r.pubyr FROM authorities a,refs r,$TAXA_TREE_CACHE t WHERE a.reference_no=r.reference_no AND ref_is_authority='YES' AND a.taxon_no=t.taxon_no AND t.taxon_no=synonym_no AND taxon_rank='species' AND type_body_part IS NOT NULL ORDER BY a.taxon_no DESC LIMIT 1";
	my $sp = @{$dbt->getData($sql)}[0];
	$row->{latest_species} = "<i><a href=\"$READ_URL?a=basicTaxonInfo&amp;taxon_no=$sp->{taxon_no}\">$sp->{taxon_name}</a></i>";
	$row->{latest_species} .= " <a href=\"$READ_URL?a=displayReference&reference_no=$sp->{reference_no}\">".Reference::formatShortRef($sp)."</a>";
	my $class_hash = TaxaCache::getParents($dbt,[$sp->{taxon_no}],'array_full');
	my @class_array = @{$class_hash->{$sp->{taxon_no}}};
	my $sp = Collection::getClassOrderFamily($dbt,\$sp,\@class_array);
	$row->{last_species_entry} = lastEntry($sp);
	$row->{latest_species} .= ( $sp->{common_name} ) ? " [".$sp->{common_name}."]" : "";
	$sql = "SELECT CONCAT(first_name,' ',last_name) AS name FROM person WHERE person_no=".$sp->{enterer_no};
	$row->{last_species_enterer} = ${$dbt->getData($sql)}[0]->{name};
	$row->{type_specimen} = ( $sp->{type_specimen} )  ? "&bull; Type specimen ".$sp->{type_specimen}."<br>" : "";
	if ( $sp->{type_locality} > 0 )	{
		$sql = "SELECT collection_name FROM collections WHERE collection_no=".$sp->{type_locality};
		$row->{type_locality} = "&bull; Type locality <a href=\"$READ_URL?a=basicCollectionSearch&amp;collection_no=".$sp->{type_locality}."\">".${$dbt->getData($sql)}[0]->{collection_name}."</a><br>";
	}

	# RANDOM GENUS LINKS
	my $offset = int(rand(1200));
	$sql = "SELECT taxon_name,a.taxon_no,rgt-lft+1 width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND t.taxon_no=synonym_no AND taxon_rank='genus' AND rgt>lft+1 AND ((t.taxon_no+$offset)/1200)=floor((t.taxon_no+$offset)/1200) LIMIT 20";
	my @genera = sort { $a->{width} <=> $b->{width} } @{$dbt->getData($sql)};
	my ($characters,$clear);
	for my $g ( @genera )	{
		my $fontsize = sprintf "%.1fem",log( $g->{'width'} ) / 2;
		my $blue = sprintf "#%x%x%x%xFF",0+int(rand(10)),int(rand(16)),0+int(rand(10)),int(rand(16));
		$blue =~ s/ /0/g;
		my $padding = (0.3 + int(rand(60)) / 10)."em";
		$characters += length( $g->{'taxon_name'} ) + $padding;
		if ( $characters > 24 )	{
			$characters = 0;
			$clear = "right";
		} elsif ( $clear eq "right" || ! $clear )	{
			$clear = "left";
		} else	{
			$clear = "none";
		}
		$row->{'random_names'} .= "<div style=\"float: left; clear: $clear; padding: 0.3em; padding-left: $padding; font-size: $fontsize;\"><a href=\"$READ_URL?action=basicTaxonInfo&amp;taxon_no=$g->{'taxon_no'}\" style=\"color: $blue\">".$g->{'taxon_name'}."</a></div>\n";
	}

	# TOP CONTRIBUTORS THIS MONTH
	$row->{'enterer_names'} = Person::homePageEntererList($dbt);

	print $hbo->stdIncludes($PAGE_TOP);
	if ( $ENV{'HTTP_USER_AGENT'} !~ /Mobile/i )	{
		print $hbo->populateHTML('home', $row);
	} else	{
		print $hbo->populateHTML('mobile_home', $row);
	}
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# calved off from sub home because it might be useable later on JA 22.3.12
sub mostRecentData	{

	# display the most recently entered collections that have
	#  distinct combinations of references and enterers (the latter is
	#  usually redundant)
	my $sql = "SELECT reference_no,enterer_no,collection_no,collection_name,floor(plate/100) p FROM collections WHERE (release_date<now() OR access_level='the public') GROUP BY reference_no,enterer_no ORDER BY collection_no DESC LIMIT 46";
	my %continent = (1 => 'North America', 2 => 'South America', 3 => 'Europe', 4 => 'Europe', 5 => 'Asia', 6 => 'Asia', 7 => 'Africa', 8 => 'Oceania', 9 => 'Oceania');
	my $lastcontinent;
my @colls; # place holder
my $row; # place holder
	@colls = sort { $continent{$a->{p}} cmp $continent{$b->{p}} } @colls;
	for my $coll ( @colls )	{
		if ( ! $continent{$coll->{p}} )	{
			next;
		}
		if ( $continent{$coll->{p}} ne $lastcontinent )	{
			if ( $lastcontinent )	{
				$row->{collection_links} .= "</div>\n";
			}
			$lastcontinent = $continent{$coll->{p}};
			$row->{collection_links} .= qq|<div class="medium">$lastcontinent</div>\n<div style="padding-top: 0.5em; padding-bottom: 0.5em;">\n|;
		}
		$row->{collection_links} .= qq|<div class="verysmall collectionLink"><a class="homeBodyLinks" href="$READ_URL?action=basicCollectionSearch&amp;collection_no=$coll->{collection_no}">$coll->{collection_name}</a></div>\n|;
	}
	$row->{'collection_links'} .= "</div>\n";

	my %groupnames = ('Dinosauria' => 'Dinosaurs','Reptilia' => 'Other reptiles','Mammalia'=> 'Mammals','Vertebrata' => 'Other vertebrates','Insecta' => 'Insects', 'Metazoa' => 'Other invertebrates');
	my @groups = keys %groupnames;
	$sql = "SELECT lft,rgt,taxon_name FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND t.taxon_no=spelling_no AND taxon_name IN ('".join("','",@groups)."') ORDER BY lft DESC";
	my @grouprefs = @{$dbt->getData($sql)};

	# something similar for new "cool species" (recently published, type
	#  body part known, etc.)
	$sql = "SELECT taxon_name,a.taxon_no,lft,rgt,a.reference_no FROM authorities a,refs r,$TAXA_TREE_CACHE t WHERE a.reference_no=r.reference_no AND ref_is_authority='YES' AND r.pubyr>=year(now())-10 AND a.taxon_no=t.taxon_no AND t.taxon_no=synonym_no AND taxon_rank='species' AND type_body_part IS NOT NULL ORDER BY a.taxon_no DESC LIMIT 100";
	my @spp = @{$dbt->getData($sql)};
	my %refseen;
	my @toprint;
	for my $s ( @spp )	{
		if ( ! $refseen{$s->{'reference_no'}} )	{
			$refseen{$s->{'reference_no'}}++;
			push @toprint , $s;
			if ( $#toprint + 1 == 51 )	{
				last;
			}
		}
	}

	my %printed;
	for my $g ( @grouprefs )	{
		if ( $g ne $grouprefs[0] )	{
			$row->{'taxon_links'} .= "</div>\n";
		}
		$row->{'taxon_links'} .= qq|<div class="medium">$groupnames{$g->{taxon_name}}</div>\n<div style="padding-top: 0.5em; padding-bottom: 0.5em;">\n|;
		for my $s ( @toprint )	{
			if ( $s->{lft} > $g->{lft} && $s->{rgt} < $g->{rgt} && ! $printed{$s->{taxon_no}} )	{
				$printed{$s->{taxon_no}}++;
				$row->{'taxon_links'} .= qq|<div class="verysmall collectionLink"><a class="homeBodyLinks" href="$READ_URL?action=basicTaxonInfo&amp;taxon_no=$s->{'taxon_no'}">$s->{'taxon_name'}</a></div>\n|;
			}
		}
	}
	$row->{'taxon_links'} .= "</div>\n";

}



# Shows the form for requesting a map
sub displayBasicMapForm {
	my %vars = ( 'mapsize'=>'100%', 'pointsize1'=>'medium', 'pointshape1'=>'circles', 'dotcolor1'=>'gold', 'dotborder1'=>'no' );
	print $hbo->stdIncludes($PAGE_TOP);
	print $hbo->populateHTML('basic_map_form', \%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# Shows the form for requesting a map
sub displayMapForm {

	# List fields that should be preset
	my %vars = ( 'mapsize'=>'100%', 'projection'=>'equirectangular', 'maptime'=>'', 'mapfocus'=>'standard (0,0)', 'mapscale'=>'X 1', 'mapwidth'=>'100%', 'mapresolution'=>'fine', 'mapbgcolor'=>'sky blue', 'crustcolor'=>'olive drab', 'crustedgecolor'=>'white', 'gridsize'=>'30', 'gridcolor'=>'light gray', 'gridposition'=>'in back', 'linethickness'=>'medium', 'latlngnocolor'=>'none', 'coastlinecolor'=>'dark green', 'borderlinecolor'=>'green', 'usalinecolor'=>'green', 'pointsize1'=>'large', 'pointshape1'=>'circles', 'dotcolor1'=>'gold', 'dotborder1'=>'no', 'mapsearchfields2'=>'', 'pointsize2'=>'large', 'pointshape2'=>'squares', 'dotcolor2'=>'blue', 'dotborder2'=>'no', 'mapsearchfields3'=>'', 'pointsize3'=>'large', 'pointshape3'=>'triangles', 'dotcolor3'=>'red', 'dotborder3'=>'no', 'mapsearchfields4'=>'', 'pointsize4'=>'large', 'pointshape4'=>'diamonds', 'dotcolor4'=>'green', 'dotborder4'=>'no' );

	# Prefs have higher precedence;
	my %pref = $s->getPreferences();
	my ($setFieldNames) = $s->getPrefFields();
	foreach my $p (@{$setFieldNames}) {
        #these prefs are for collection entry form, don't display the here
        if ($p !~ /environment|research_group|formation|country|state|interval_name|lithology|period_max/) {
            $vars{$p} = $pref{$p} if $pref{$p};
        }
	}
    $vars{'enterer_me'} = $s->get('enterer_reversed');
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    # Lastly resubmission takes highest precedence
    my $q_vars = $q->Vars();
    while (my ($k,$v) = each %{$q_vars}) {
        $vars{$k} = $v if $v;
    }

	# Spit out the HTML
	print $hbo->stdIncludes($PAGE_TOP);
    print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('js_map_checkform');
    print $hbo->populateHTML('map_form', \%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub displayMapOnly	{
	require Map;

	my $m = Map->new($q,$dbt,$s);
	my ($file,$errors,$warnings) = $m->buildMap();
}

sub displayMapResults {
    return if PBDBUtil::checkForBot();
    require Map;

    $|=1; # Freeflowing data

    logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP );

    my @errors;
    if ($q->param('interval_name_preset')) {
        my $interval_name = $q->param('interval_name_preset');
        my $t = new TimeLookup($dbt);
        $q->param('interval_name'=>$interval_name);
        my ($eml,$name) = TimeLookup::splitInterval($interval_name);
        my $interval_no = $t->getIntervalNo($eml,$name);
        if (!$interval_no) {
            push @errors, "Bad interval name";
        }
        
        $q->delete('interval_name_preset');
        my $h = $t->lookupIntervals([$interval_no]);
        my $itv = $h->{$interval_no};
        my $map_age = sprintf("%.0f",(($itv->{'base_age'} + $itv->{'top_age'})/2));
        if (!$map_age) {
            push @errors, "No age range for interval";
        }
        $q->param('maptime'=>$map_age);

    }
    if ($q->param('taxon_name_preset') && !$q->param('taxon_name')){
        $q->param('taxon_name'=>$q->param('taxon_name_preset'));
        $q->delete('taxon_name_preset');
    }

    if ($q->param('mapcolors'))	{
        my %settings;
        if ($q->param('mapcolors') eq 'green on white') {
            %settings = (
                mapbgcolor=>'white',crustcolor=>'olive drab', crustedgecolor=>'none',
                usalinecolor=>'green', borderlinecolor=>'green', autoborders=>'yes',
                gridsize=>'30',gridcolor=>'light gray',gridposition=>'in back',
                coastlinecolor=>'dark green'
            );
        } elsif ($q->param('mapcolors') eq 'gray on white') {
            %settings = (
                mapbgcolor=>'white',crustcolor=>'light gray', crustedgecolor=>'none',
                usalinecolor=>'light gray', borderlinecolor=>'light gray', autoborders=>'yes',
                gridsize=>'30',gridcolor=>'light gray',gridposition=>'in back',
                coastlinecolor=>'black'
            );
        } elsif ($q->param('mapcolors') eq 'green on blue') {
            %settings = (
                mapbgcolor=>'sky blue',crustcolor=>'olive drab', crustedgecolor=>'white',
                usalinecolor=>'green', borderlinecolor=>'green', autoborders=>'yes',
                gridsize=>'30',gridcolor=>'light gray',gridposition=>'in back',
                coastlinecolor=>'dark green'
            );
        } else { # outlines only default
            %settings = (
                mapbgcolor=>'white',crustcolor=>'none', crustedgecolor=>'none',
                usalinecolor=>'light gray', borderlinecolor=>'light gray', autoborders=>'yes',
                gridsize=>'30',gridcolor=>'light gray',gridposition=>'in back',
                coastlinecolor=>'black'
            );
        }
        while(my ($k,$v) = each %settings) {
            $q->param($k=>$v);
        }
    }

	my $m = Map->new($q,$dbt,$s);
	my ($file,$errors,$warnings) = $m->buildMap();
    if (ref $errors && @$errors) {
        print '<div align="center">'.Debug::printErrors($errors).'</div>';
    } else {
        if (ref $warnings && @$warnings) {
            print '<div align="center">'.Debug::printWarnings($warnings).'</div>';
        }
   
        open(MAP, $file) or die "couldn't open $file ($!)";
        while(<MAP>){
            print;
        }
        close MAP;
    }
	print $hbo->stdIncludes($PAGE_BOTTOM);
}


# This crappy code based off of TaxonInfo::doMap. Hence the calls there.  This
# needs to be done so its all abstracted correctly in the Map module and called
# on Map object creation, maybe later PS 12/14/2005
sub displayMapOfCollection {
    require Map;
    return unless $q->param('collection_no') =~ /^\d+$/;
    return if PBDBUtil::checkForBot();

    logRequest($s,$q);
    my $sql = "SELECT c.collection_no,c.lngdeg,c.latdeg,c.lngdir,c.latdir,c.collection_name,c.country,c.state,concat(i1.eml_interval,' ',i1.interval_name) max_interval, concat(i2.eml_interval,' ',i2.interval_name) min_interval "
            . " FROM collections c "
            . " LEFT JOIN intervals i1 ON c.max_interval_no=i1.interval_no"
            . " LEFT JOIN intervals i2 ON c.min_interval_no=i2.interval_no"
            . " WHERE c.collection_no=".$q->param('collection_no');
    my $coll = ${$dbt->getData($sql)}[0];

    my $latdeg = $coll->{latdeg};
    my $lngdeg = $coll->{lngdeg};
    $latdeg *= -1 if ($coll->{latdir} eq 'South');
    $lngdeg *= -1 if ($coll->{lngdir} eq 'West');
    # we need to get the number of collections out of dataRowsRef
    #  before figuring out the point size
    require Map;
    $q->param("simple_map"=>'YES');
    $q->param('mapscale'=>'x 5');
    $q->param('maplat'=>$latdeg);
    $q->param('maplng'=>$lngdeg);
    $q->param('pointsize1'=>'auto');
    $q->param('autoborders'=>'yes');
    my $m = Map->new($q,$dbt,$s);
    my ($map_html_path,$errors,$warnings) = $m->buildMap();


    if ($q->param("display_header") eq 'NO') {
        print $hbo->stdIncludes("blank_page_top") 
    } else {
        print $hbo->stdIncludes($PAGE_TOP) 
    }

    if ($coll->{'collection_no'}) {
        # get the max/min interval names
        my $time_place = $coll->{'collection_name'}.": ";
        if ($coll->{'max_interval'} ne $coll->{'min_interval'} && $coll->{'min_interval'}) {
            $time_place .= "$coll->{max_interval} - $coll->{min_interval}";
        } else {
            $time_place .= "$coll->{max_interval}";
        }
        if ($coll->{'state'} && $coll->{'country'} eq "United States") {
            $time_place .= ", $coll->{state}";
        } elsif ($coll->{'country'}) {
            $time_place .= ", $coll->{country}";
        }
        print '<div align="center"><p class="pageTitle">'.$time_place.'</p></div>';
    }
    print '<div align="center">';
    # MAP USES $q->param("taxon_name") to determine what it's doing.
    if ( $map_html_path )   {
        if($map_html_path =~ /^\/public/){
            # reconstruct the full path the image.
            $map_html_path = $HTML_DIR.$map_html_path;
        }
        open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
        while(<MAP>){
            print;
        }
        close MAP;
    } else {
        print "<i>No distribution data are available</i>";
    }
    
    # trim the path down beyond apache's root so we don't have a full
    # server path in our html.
    if ( $map_html_path )   {
        $map_html_path =~ s/.*?(\/public.*)/$1/;
        print "<input type=hidden name=\"map_num\" value=\"$map_html_path\">";
    }  
    print '</div>';

    
    if ($q->param("display_header") eq 'NO') {
        print $hbo->stdIncludes("blank_page_bottom") 
    } else {
        print $hbo->stdIncludes($PAGE_BOTTOM) 
    }
}

sub displaySimpleMap {
    print $hbo->stdIncludes($PAGE_TOP);
	$q->param("simple_map"=>'YES');
    return if PBDBUtil::checkForBot();

	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape1', 'dotcolor1', 'dotborder1');
	my %user_prefs = $s->getPreferences();
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}

	
    # now actually draw the map
    require Map;
    print '<div align="center"><p class="pageTitle">Map</p></div>';
    print '<div align="center">';

    # we need to get the number of collections out of dataRowsRef
    #  before figuring out the point size
    my ($map_html_path,$errors,$warnings);
    $q->param("simple_map"=>'YES');
    $q->param('mapscale'=>'auto');
    $q->param('autoborders'=>'yes');
    $q->param('pointsize1'=>'auto');
    my $m = Map->new($q,$dbt,$s);
    ($map_html_path,$errors,$warnings) = $m->buildMap();

    # MAP USES $q->param("taxon_name") to determine what it's doing.
    if ($map_html_path)   {
        if($map_html_path =~ /^\/public/){
            # reconstruct the full path the image.
            $map_html_path = $HTML_DIR.$map_html_path;
        }
        open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
        while(<MAP>){
            print;
        }
        close MAP;
    } else {
        print "<i>No distribution data are available</i>";
    }  
    print "</div>";
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub displayDownloadForm {

	my %vars = $q->Vars();
	$vars{'authorizer_me'} = $s->get("authorizer_reversed");
	$vars{'enterer_me'} = $s->get("authorizer_reversed");

	my $last;
	if ( $s->isDBMember() )	{
		$vars{'row_class_1a'} = '';
		$vars{'row_class_1b'} = ' class="lightGray"';
		my $dbh = $dbt->dbh;
		if ( $q->param('restore_defaults') )	{
			my $sql = "UPDATE person SET last_action=last_action,last_download=NULL WHERE person_no=".$s->get('enterer_no');
			$dbh->do($sql);
		} else	{
			$last = ${$dbt->getData("SELECT last_download FROM person WHERE person_no=".$s->get('enterer_no'))}[0]->{'last_download'};
			if ( $last )	{
				$vars{'has_defaults'} = 1;
				my @pairs = split '/',$last;
				for my $p ( @pairs )	{
					my ($k,$v) = split /=/,$p;
					$vars{$k} = $v;
				}
			}
		}
	} else	{
		$vars{'row_class_1a'} = ' class="lightGray"';
		$vars{'row_class_1b'} = '';
	}

	print $hbo->stdIncludes($PAGE_TOP);
	print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('download_form',\%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayBasicDownloadForm {
	my %vars = $q->Vars();
	my $last;
	if ( $s->get('enterer_no') > 0 )	{
		$last = ${$dbt->getData("SELECT last_download FROM person WHERE person_no=".$s->get('enterer_no'))}[0]->{'last_download'};
		my @pairs = split '/',$last;
		for my $p ( @pairs )	{
			my ($k,$v) = split /=/,$p;
			$vars{$k} = $v;
		}
	}
	print $hbo->stdIncludes($PAGE_TOP );
	print $hbo->populateHTML('basic_download_form',\%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayDownloadResults {
    require Download;
    logRequest($s,$q);

	print $hbo->stdIncludes( $PAGE_TOP );

	my $m = Download->new($dbt,$q,$s,$hbo);
	$m->buildDownload( );

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub emailDownloadFiles	{
	require Download;

	print $hbo->stdIncludes( $PAGE_TOP );

	my $m = Download->new($dbt,$q,$s,$hbo);
	$m->emailDownloadFiles();

	print $hbo->stdIncludes($PAGE_BOTTOM);

}

# JA 28.7.08
sub displayDownloadMeasurementsForm	{
	my %vars = $q->Vars();
	$vars{'error_message'} = shift;
	print $hbo->stdIncludes($PAGE_TOP);
	print PBDBUtil::printIntervalsJava($dbt,1);
	print $hbo->populateHTML('download_measurements_form',\%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayDownloadMeasurementsResults	{
	return if PBDBUtil::checkForBot();
	require Download;
	logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	Measurement::displayDownloadMeasurementsResults($q,$s,$dbt,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayDownloadNeptuneForm {
    my %vars;
    if ($s->isDBMember()) {
        $vars{'row_class_1'} = '';
        $vars{'row_class_2'} = ' class="lightGray"';
    } else {
        $vars{'row_class_1'} = ' class="lightGray"';
        $vars{'row_class_2'} = '';
    }
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('download_neptune_form',\%vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}       
    
sub displayDownloadNeptuneResults {
    require Neptune;
    print $hbo->stdIncludes( $PAGE_TOP );
    Neptune::displayNeptuneDownloadResults($q,$s,$hbo,$dbt);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}  

sub displayDownloadTaxonomyForm {
   
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    print $hbo->stdIncludes($PAGE_TOP);
    print Person::makeAuthEntJavascript($dbt);
    print $hbo->populateHTML('download_taxonomy_form',\%vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}       

sub getTaxonomyXML {
    return if PBDBUtil::checkForBot();
    logRequest($s,$q);
    require DownloadTaxonomy;
    DownloadTaxonomy::getTaxonomyXML($dbt,$q,$s,$hbo);
}

sub displayDownloadTaxonomyResults {
    return if PBDBUtil::checkForBot();
    require DownloadTaxonomy;

    logRequest($s,$q);
    print $hbo->stdIncludes( $PAGE_TOP );
    if ($q->param('output_data') =~ /ITIS/i) {
        DownloadTaxonomy::displayITISDownload($dbt,$q,$s);
    } else { 
        DownloadTaxonomy::displayPBDBDownload($dbt,$q,$s);
    }
                                              
    print $hbo->stdIncludes($PAGE_BOTTOM);
}  

sub displayReportForm {
	print $hbo->stdIncludes( $PAGE_TOP );
	print $hbo->populateHTML('report_form');
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayReportResults {
	require Report;

	logRequest($s,$q);

	print $hbo->stdIncludes( $PAGE_TOP );

	my $r = Report->new($dbt,$q,$s);
	$r->buildReport();

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayMostCommonTaxa	{
	my $dataRowsRef = shift;
	require Report;

	logRequest($s,$q);

	print $hbo->stdIncludes( $PAGE_TOP );

	my $r = Report->new($dbt,$q,$s);
	$r->findMostCommonTaxa($dataRowsRef);

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayCountForm	{
	print $hbo->stdIncludes( $PAGE_TOP );
	require Person;
	print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('taxon_count_form');
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub fastTaxonCount	{
	return if PBDBUtil::checkForBot();
	logRequest($s,$q);

	print $hbo->stdIncludes( $PAGE_TOP );

	require Report;
	Report::fastTaxonCount($dbt,$q,$s,$hbo);

	print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub countNames	{
	require Report;

	logRequest($s,$q);

	print $hbo->stdIncludes( $PAGE_TOP );

	my $r = Report->new($dbt,$q,$s);
	$r->countNames();

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayCurveForm {
    my $std_page_top = $hbo->stdIncludes($PAGE_TOP);
    print $std_page_top;

	my $html = $hbo->populateHTML('curve_form');
    if ($q->param("input_data") =~ /neptune/) {
        $html =~ s/<option selected>10 m\.y\./<option>10 m\.y\./;
        if ($q->param("input_data") =~ /neptune_pbdb/) {
            $html =~ s/<option>Neptune-PBDB PACMAN/<option selected>Neptune-PBDB PACMAN/;
        } else {
            $html =~ s/<option>Neptune PACMAN/<option selected>Neptune PACMAN/;
        }
    }
    if ($q->param("yourname") && !$s->isDBMember()) {
        my $yourname = $q->param("yourname");
        $html =~ s/<input name=yourname/<input name=yourname value="$yourname"/;
    }
    print $html;

    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayCurveResults	{
	require Curve;

	logRequest($s,$q);

	my $std_page_top = $hbo->stdIncludes($PAGE_TOP);
	print $std_page_top;

	my $c = Curve->new($q, $s, $dbt );
	$c->buildCurve($hbo);

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# Show a generic page
sub displayPage {
	my $page = shift;
	if ( ! $page ) { 
		# Try the parameters
		$page = $q->param("page"); 
		if ( ! $page ) {
			$hbo->htmlError( "displayPage(): Unknown page..." );
		}
	}

	# Spit out the HTML
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print $hbo->stdIncludes( $PAGE_TOP );
	}
	print $hbo->populateHTML($page,[],[]);
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print $hbo->stdIncludes( $PAGE_BOTTOM );
	}
}

sub displaySearchRefs {
    my $error = shift;
    print $hbo->stdIncludes( $PAGE_TOP );
    Reference::displaySearchRefs($dbt,$q,$s,$hbo,$error);
    print $hbo->stdIncludes( $PAGE_BOTTOM );
}

sub selectReference {
	$s->setReferenceNo($q->param("reference_no") );
	menu( );
}

# Wrapper to displayRefEdit
sub editCurrentRef {
	my $reference_no = $s->get("reference_no");
	if ( $reference_no ) {
		$q->param("reference_no"=>$reference_no);
		displayReferenceForm();
	} else {
		$q->param("type"=>"edit");
		Reference::displaySearchRefs($dbt,$q,$s,$hbo,"Please choose a reference first" );
	}
}

sub displayRefResults {
	return if PBDBUtil::checkForBot();
	logRequest($s,$q);
	Reference::displayRefResults($dbt,$q,$s,$hbo);
}

sub getReferencesXML {
    logRequest($s,$q);
    Reference::getReferencesXML($dbt,$q,$s,$hbo);
}

sub getTitleWordOdds	{
	logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	Reference::getTitleWordOdds($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayReferenceForm {
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		return;
	}

	print $hbo->stdIncludes($PAGE_TOP);
	Reference::displayReferenceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayReference {
	print $hbo->stdIncludes($PAGE_TOP);
	Reference::displayReference($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processReferenceForm {
	print $hbo->stdIncludes($PAGE_TOP);
	Reference::processReferenceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}


# 7.11.09 JA
# hacked to keep eco DB searches away from taxon pages because there won't be
#  enough taxon-specific info to make taxon pages useful for quite some time
#  27.5.11 JA
sub quickSearch	{

	my $qs = $q->param('quick_search');
	$qs =~ s/\./%/g;
	$qs =~ s/  / /g;
	$q->param('quick_search' => $qs);

	my $taxa_skipped;
	( $DB eq "eco" ) ? $taxa_skipped = 1 : "";

	# case 1 or 2: search string cannot be a taxon name, so search elsewhere
	my $nowDate = now();
	my ($date,$time) = split / /,$nowDate;
	my ($yyyy,$mm,$dd) = split /-/,$date,3;
	if ( $qs =~ /[^A-Za-z% ]/ || $qs =~ / .* / )	{
	# case 1: string looks like author/year, so try references
		my @words = split / /,$qs;
		if ( $words[$#words] >= 1758 && $words[$#words] <= $yyyy )	{
			$q->param('name_pattern' => 'equals');
			$q->param('name' => $words[0]);
			$q->param('year_relation' => 'in');
			$q->param('year' => $words[$#words]);
			displayRefResults();
			exit;
		}
	# case 2: otherwise or if that fails, try collections
		my $found = Collection::basicCollectionSearch($dbt,$q,$s,$hbo);
		$found == 1 ? exit : "";
	# if basicCollectionSearch finds any match it should exit somehow before
	#   this point, so try a common name search as a desperation measure
		if ( $qs !~ /[^A-Za-z' ]/ && $DB ne "eco" )	{
			TaxonInfo::basicTaxonInfo($q,$s,$dbt,$hbo);
			exit;
		}
	}
	elsif ( $DB ne "eco" )	{
		my $sql = "SELECT count(*) c FROM authorities WHERE taxon_name LIKE '".$qs."'";
    		my $t = ${$dbt->getData($sql)}[0];
	# case 3: string is formatted correctly and matches at least one name,
	#  so search taxa only
		if ( $t->{'c'} > 0 )	{
			TaxonInfo::basicTaxonInfo($q,$s,$dbt,$hbo);
			exit;
		}
	# case 4: search is formatted correctly but does not directly match
	#  any name, so first try collections and then try taxa again (which
	#  will yield some kind of a match somehow)
		else	{
			my $found = Collection::basicCollectionSearch($dbt,$q,$s,$hbo);
			$found == 1 ? exit : "";
			TaxonInfo::basicTaxonInfo($q,$s,$dbt,$hbo);
			exit;
		}
	} else	{
		my $found = Collection::basicCollectionSearch($dbt,$q,$s,$hbo,$taxa_skipped);
		( $found == 1 ) ? exit : "";
	}

	# if we don't have any idea what they're driving at, send them home
	# this point should only ever be reached if nothing works whatsoever
	#  and no error message is returned by anything else, which is only
	#  ever likely to happen if basicTaxonInfo isn't called
	print $hbo->stdIncludes( $PAGE_TOP );
	home('<div class="large" style="margin-bottom: 2em;"><i>Sorry - your search failed to recover any data records.</i></div>');
	print $hbo->stdIncludes( $PAGE_BOTTOM );

	return;

}


# 5.4.04 JA
# print the special search form used when you are adding a collection
# uses some code lifted from displaySearchColls
sub displaySearchCollsForAdd	{

	if (!$s->isDBMember()) {
		login( "Please log in first.");
		return;
	}

	# Have to have a reference #, unless we are just searching
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		# Come back here... requeue our option
		$s->enqueue("action=displaySearchCollsForAdd" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	# Some prefilled variables like lat/lng/time term
	my %pref = $s->getPreferences();

	# Spit out the HTML
	print $hbo->stdIncludes( $PAGE_TOP );
	if ( $DB ne "eco" )	{
		print  $hbo->populateHTML('search_collections_for_add_form' , \%pref);
	} else	{
		print  $hbo->populateHTML('search_inventories_for_add_form' , \%pref);
	}
	print $hbo->stdIncludes( $PAGE_BOTTOM );

}


sub displaySearchColls {
	my $error = shift;
	# Get the type, passed or on queue
	my $type = $q->param("type");
	if ( ! $type ) {
		# QUEUE
		my %queue = $s->unqueue();
		$type = $queue{type};
	}

	# Have to have a reference #, unless we are just searching
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no && $type !~ /^(?:basic|analyze_abundance|view|edit|reclassify_occurrence|count_occurrences|most_common)$/) {
		# Come back here... requeue our option
		$s->enqueue("action=displaySearchColls&type=$type" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	# Show the "search collections" form
	my %vars = ();
	$vars{'enterer_me'} = $s->get('enterer_reversed');
	$vars{'action'} = "displayCollResults";
	$vars{'type'} = $type;
	$vars{'error'} = $error;

	$vars{'links'} = qq|
<p><span class="mockLink" onClick="javascript: checkForm(); document.collForm.submit();"><b>Search collections</b></span>
|;

	if ( $type eq "view" || ! $type )	{
		$vars{'links'} = qq|
<p><span class="mockLink" onClick="javascript: checkForm(); document.collForm.basic.value = 'yes'; document.collForm.submit();"><b>Search for basic info</b></span> -
<span class="mockLink" onClick="javascript: document.collForm.basic.value = ''; document.collForm.submit();"><b>Search for full details</b></span></p>
|;
	} elsif ($type eq 'occurrence_table') {
		$vars{'reference_no'} = $reference_no;
		$vars{'limit'} = 20;
	}

	# Spit out the HTML
	print $hbo->stdIncludes($PAGE_TOP);
	print Person::makeAuthEntJavascript($dbt);
	if ( $DB ne "eco" )	{
		$vars{'page_title'} = "Collection search form";
		print PBDBUtil::printIntervalsJava($dbt,1);
		print $hbo->populateHTML('search_collections_form', \%vars);
	} else	{
		$vars{'page_title'} = "Inventory search form";
		print $hbo->populateHTML('inventory_search_form', \%vars);
	}
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub basicCollectionSearch	{
	print $hbo->stdIncludes($PAGE_TOP);
	Collection::basicCollectionSearch($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}


# User submits completed collection search form
# System displays matching collection results
# Called during collections search, and by displayReIDForm() routine.
sub displayCollResults {

	# dataRows might be passed in by basicCollectionSearch
	my $dataRows = shift;
	my $ofRows;
	if ( $dataRows )	{
		$ofRows = scalar(@$dataRows);
	}

	return if PBDBUtil::checkForBot();

	if ( ! $s->get('enterer') && $q->param('type') eq "reclassify_occurrence" )    {
		print $hbo->stdIncludes( $PAGE_TOP );
		print "<center>\n<p class=\"pageTitle\">Sorry!</p>\n\n";
		print "<p>You can't reclassify occurrences unless you <a href=\"$WRITE_URL?action=menu&amp;user=Contributor\">log in</a> first.</p>\n</center>\n";
		print $hbo->stdIncludes($PAGE_BOTTOM);
		exit;
	}

	logRequest($s,$q);

	my $limit = $q->param('limit') || 30 ;
	my $rowOffset = $q->param('rowOffset') || 0;

	# limit passed to permissions module
	my $perm_limit;

	# effectively don't limit the number of collections put into the
	#  initial set to examine when adding a new one
	if ( $q->param('type') eq "add" )	{
#		$perm_limit = 1000000;
		$perm_limit = $limit + $rowOffset;
	} else {
		if ($q->param("type") =~ /occurrence_table|occurrence_list|count_occurrences|most_common/ ||
            $q->param('taxon_name') && ($q->param('type') eq "reid" ||
                                        $q->param('type') eq "reclassify_occurrence")) {
            # We're passing the collection_nos directly to the functions, so pass all of them                                            
			$perm_limit = 1000000000;
		} else {
			$perm_limit = $limit + $rowOffset;
		}
	}

	my $type;
	if ( $q->param('type') ) {
		$type = $q->param('type');			# It might have been passed (ReID)
	} else {
		# QUEUE
		my %queue = $s->unqueue();		# Most of 'em are queued
		$type = $queue{type};
		if ( ! $type )	{
			$type = "view";
		}
	}

    my $exec_url = ($type =~ /view/) ? $READ_URL : $WRITE_URL;

    my $action =  
          ($type eq "add") ? "displayCollectionDetails"
        : ($type eq "edit" && $DB ne "eco") ? "displayCollectionForm"
        : ($type eq "edit" && $DB eq "eco") ? "inventoryForm"
        : ($type eq "view" && $DB ne "eco") ? "displayCollectionDetails"
        : ($type eq "view" && $DB eq "eco") ? "inventoryInfo"
        : ($type eq "edit_occurrence") ? "displayOccurrenceAddEdit"
        : ($type eq "occurrence_list") ? "displayOccurrenceListForm"
        : ($type eq "analyze_abundance") ? "rarefyAbundances"
        : ($type eq "reid") ? "displayOccsForReID"
        : ($type eq "reclassify_occurrence") ?  "startDisplayOccurrenceReclassify"
        : ($type eq "most_common") ? "displayMostCommonTaxa"
        : "displayCollectionDetails";

	# GET COLLECTIONS
	# Build the SQL
	# which function to use depends on whether the user is adding a collection
	my $sql;
    
	my ($warnings,$occRows) = ([],[]);

	if ( $q->param('type') eq "add" )	{
		# you won't have an in list if you are adding
		($dataRows,$ofRows) = processCollectionsSearchForAdd();
	} elsif ( ! $dataRows )	{
		my %options = $q->Vars();
		my $fields = ["authorizer","country", "state", "max_interval_no", "min_interval_no","collection_aka","collectors","collection_dates"];
		if ( $DB eq "eco" )	{
			$fields = ["authorizer_no","country", "state", "habitat", "inventory_aka","inventory_method","inventoried_by","years"];
		}
		if ($q->param('output_format') eq 'xml') {
			push @$fields, "latdeg","latmin","latsec","latdir","latdec","lngdeg","lngmin","lngsec","lngdir","lngdec";
		}
		if ($type eq "reclassify_occurrence" || $type eq "reid") {
	# Want to not get taxon_nos when reclassifying. Otherwise, if the taxon_no is set to zero, how will you find it?
			$options{'no_authority_lookup'} = 1;
			$options{'match_subgenera'} = 1;
		}
		$options{'limit'} = $perm_limit;
	# Do a looser match against old ids as well
		$options{'include_old_ids'} = 1;
	# Even if we have a match in the authorities table, still match against the bare occurrences/reids  table
		$options{'include_occurrences'} = 1;
		if ($q->param("taxon_list")) {
			my @in_list = split(/,/,$q->param('taxon_list'));
			$options{'taxon_list'} = \@in_list if (@in_list);
		}
		if ($type eq "count_occurrences")	{
			$options{'count_occurrences'} = 1;
		}
		if ($type eq "most_common")	{
			$options{'include_old_ids'} = 0;
		}

		$options{'calling_script'} = "displayCollResults";
		($dataRows,$ofRows,$warnings,$occRows) = Collection::getCollections($dbt,$s,\%options,$fields);
	}

	# DISPLAY MATCHING COLLECTIONS
	my @dataRows;
	if ( $dataRows )	{
		@dataRows = @$dataRows;
	}
	my $displayRows = scalar(@dataRows);	# get number of rows to display

	if ( $type eq 'occurrence_table' && @dataRows) {
		my @colls = map {$_->{$COLLECTION_NO}} @dataRows;
		displayOccurrenceTable(\@colls);
		exit;
	} elsif ( $type eq 'count_occurrences' && @dataRows) {
		Collection::countOccurrences($dbt,$hbo,\@dataRows,$occRows);
		exit;
	} elsif ( $type eq 'most_common' && @dataRows) {
		displayMostCommonTaxa(\@dataRows);
		exit;
	} elsif ( $displayRows > 1  || ($displayRows == 1 && $type eq "add")) {
		# go right to the chase with ReIDs if a taxon_rank was specified
		if ($q->param('taxon_name') && ($q->param('type') eq "reid" ||
                                        $q->param('type') eq "reclassify_occurrence")) {
			# get all collection #'s and call displayOccsForReID
			my @colls;
			foreach my $row (@dataRows) {
				push(@colls , $row->{$COLLECTION_NO});
			}
			if ($q->param('type') eq 'reid')	{
				displayOccsForReID(\@colls);
			} else	{
				Reclassify::displayOccurrenceReclassify($q,$s,$dbt,$hbo,\@colls);
			}
			exit;
		}

	# need to grab the authorizer names, these are no built in to the
	#  inventories table
		my %lookup;
		if ( $DB eq "eco" )	{
			my @auth_nos = map { $_->{'authorizer_no'} } @dataRows;
			my $sql = "SELECT person_no,name FROM person WHERE person_no IN (".join(',',@auth_nos).")";
			my @prefs = @{$dbt->getData($sql)};
			$lookup{$_->{'person_no'}} = $_->{'name'} foreach @prefs;
		}

		print $hbo->stdIncludes( $PAGE_TOP );

        # Display header link that says which collections we're currently viewing
        if (@$warnings) {
            print "<div align=\"center\">".Debug::printWarnings($warnings)."</div>";
        }

        print "<center>";
        if ($ofRows > 1) {
		print "<p class=\"pageTitle\">There are $ofRows matches\n";
		if ($ofRows > $limit) {
			print " - here are";
			if ($rowOffset > 0) {
				print " rows ".($rowOffset+1)." to ";
				my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
				print $printRows;
			} else {
				print " the first ";
				my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
				print $printRows;
				print " rows";
			}
		}
		print "</p>\n";
	} elsif ( $ofRows == 1 ) {
		print "<p class=\"pageTitle\">There is exactly one match</p>\n";
	} else	{
		print "<p class=\"pageTitle\">There are no matches</p>\n";
	}
	print "</center>\n";

	print qq|<div class="displayPanel" style="margin-left: auto; margin-right: auto; padding: 0.5em; padding-left: 1em;">
	<table class="small" border="0" cellpadding="4" cellspacing="0">|;

	# print columns header
	if ( $DB ne "eco" )	{
		print qq|<tr>
<th>Collection</th>
<th align=left>Authorizer</th>
<th align=left nowrap>Collection name</th>
<th align=left>Reference</th>
|;
		print "<th align=left>Distance</th>\n" if ($type eq 'add');
		print "</tr>\n\n";
	} else	{
		print qq|<tr>
<th align=left>Contributor</th>
<th align=left nowrap>Inventory name</th>
<th align=left>Location</th>
<th align=left>Habitat</th>
<th align=left nowrap>Survey methods</th>
<th align=left>Reference</th>
|;
		print "<th align=left>Distance</th>\n" if ($type eq 'add');
		print "</tr>\n\n";
	}
 
        # Make non-editable links not highlighted  
        my ($p,%is_modifier_for); 
        if ($type eq 'edit' && $DB ne "eco") { 
            $p = Permissions->new($s,$dbt);
            %is_modifier_for = %{$p->getModifierList()};
        }

		# Loop through each data row of the result set
        my %seen_ref;
        my %seen_interval;
        for(my $count=$rowOffset;$count<scalar(@dataRows) && $count < $rowOffset+$limit;$count++) {
            my $dataRow = $dataRows[$count];
			# Get the reference_no of the row
            my $reference;
            if ($seen_ref{$dataRow->{'reference_no'}}) {
                $reference = $seen_ref{$dataRow->{'reference_no'}};
            } else {
                my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=".$dataRow->{'reference_no'};
                my $ref = ${$dbt->getData($sql)}[0];
                # Build the reference string
                $reference = Reference::formatShortRef($ref,'alt_pubyr'=>1, 'link_id'=>1);
                $seen_ref{$dataRow->{'reference_no'}} = $reference;
            }

	# Build a short descriptor of the collection's time place
	# first part JA 7.8.03
	my $timeplace;

            if ($seen_interval{$dataRow->{'max_interval_no'}." ".$dataRow->{'min_interval_no'}}) {
                $timeplace = $seen_interval{$dataRow->{'max_interval_no'}." ".$dataRow->{'min_interval_no'}}." - ";
            } elsif ( $dataRow->{'max_interval_no'} > 0 )	{
                my @intervals = ();
                push @intervals, $dataRow->{'max_interval_no'} if ($dataRow->{'max_interval_no'});
                push @intervals, $dataRow->{'min_interval_no'} if ($dataRow->{'min_interval_no'} && $dataRow->{'min_interval_no'} != $dataRow->{'max_interval_no'});
                my $max_lookup;
                my $min_lookup;
                if (@intervals) {
                    my $t = new TimeLookup($dbt);
                    my $lookup = $t->lookupIntervals(\@intervals,['interval_name','ten_my_bin']);
                    $max_lookup = $lookup->{$dataRow->{'max_interval_no'}};
                    if ($dataRow->{'min_interval_no'} && $dataRow->{'min_interval_no'} != $dataRow->{'max_interval_no'}) {
                        $min_lookup = $lookup->{$dataRow->{'min_interval_no'}};
                    } 
                }
                $timeplace .= "<nobr>" . $max_lookup->{'interval_name'} . "</nobr>";
                if ($min_lookup) {
                    $timeplace .= "/<nobr>" . $min_lookup->{'interval_name'} . "</nobr>"; 
                }
                if ($max_lookup->{'ten_my_bin'} && (!$min_lookup || $min_lookup->{'ten_my_bin'} eq $max_lookup->{'ten_my_bin'})) {
                    $timeplace .= " - <nobr>$max_lookup->{'ten_my_bin'}</nobr> ";
                }
                $timeplace .= " - ";
            }

			$timeplace =~ s/\/(Lower|Upper)//g;

			# rest of timeplace construction JA 20.8.02
			if ( $dataRow->{"state"} && $dataRow->{"country"} eq "United States" )	{
				$timeplace .= $dataRow->{"state"};
			} else	{
				$timeplace .= $dataRow->{"country"};
			}

			# should it be a dark row, or a light row?  Alternate them...
 			if ( $count % 2 == 0 ) {
				print "<tr class=\"darkList\">";
 			} else {
				print "<tr>";
			}


            if ( $DB ne "eco" )	{ 
            if ($type ne 'edit' || 
                $type eq 'edit' && ($s->get("superuser") ||
                                   ($s->get('authorizer_no') && $s->get("authorizer_no") == $dataRow->{'authorizer_no'}) ||
                                    $is_modifier_for{$dataRow->{'authorizer_no'}})) {
                if ( $q->param('basic') =~ /yes/i && $type eq "view" )	{
                    print "<td align=center valign=top><a href=\"$exec_url?action=basicCollectionSearch&amp;$COLLECTION_NO=$dataRow->{$COLLECTION_NO}";
                } else	{
                    print "<td align=center valign=top><a href=\"$exec_url?action=$action&amp;$COLLECTION_NO=$dataRow->{$COLLECTION_NO}";
                }

                # for collection edit:
                if($q->param('use_primary')){
                    print "&use_primary=yes";
                }
                
                # These may be useful to displayOccsForReID
                if($q->param('genus_name')){
                    print "&genus_name=".$q->param('genus_name');
                }
                
                if($q->param('species_name')){
                    print "&species_name=".$q->param('species_name');
                }
                if ($q->param('occurrences_authorizer_no')) {
                    print "&occurrences_authorizer_no=".$q->param('occurrences_authorizer_no');
                }
                print "\">$dataRow->{$COLLECTION_NO}</a></td>";
            } else {	
                # Don't link it if if we're in edit mode and we don't have permission
                print "<td align=center valign=top>$dataRow->{$COLLECTION_NO}</td>";
            }
            }


            my $collection_names = $dataRow->{'collection_name'};
            if ($dataRow->{'collection_aka'} || $dataRow->{'collectors'} ||$dataRow->{'collection_dates'}) {
                $collection_names .= " (";
            }
            if ($dataRow->{'collection_aka'}) {
                $collection_names .= "= $dataRow->{collection_aka}";
                if ($dataRow->{'collectors'} ||$dataRow->{'collection_dates'}) {
                    $collection_names .= " / ";
                }
            }
            if ($dataRow->{'collectors'} ||$dataRow->{'collection_dates'}) {
                $collection_names .= "coll.";
            }
            if ($dataRow->{'collectors'}) {
                my $collectors = " ";
                $collectors .= $dataRow->{'collectors'};
                $collectors =~ s/ \(.*\)//g;
                $collectors =~ s/ and / \& /g;
                $collectors =~ s/(Dr\.)(Mr\.)(Prof\.)//g;
                $collectors =~ s/\b[A-Za-z]([A-Za-z\.]|)\b//g;
                $collectors =~ s/\.//g;
                $collection_names .= $collectors;
            }
            if ($dataRow->{'collection_dates'}) {
                my $years = " ";
                $years .= $dataRow->{'collection_dates'};
                $years =~ s/[A-Za-z\.]//g;
                $years =~ s/([^\-]) \b[0-9]([0-9]|)\b/$1/g;
                $years =~ s/^( |),//;
                $collection_names .= $years;
            }
            if ($dataRow->{'collection_aka'} || $dataRow->{'collectors'} ||$dataRow->{'collection_dates'}) {
                $collection_names .= ")";
            }

            if ($dataRow->{'old_id'}) {
                $timeplace .= " - old id";
            }
            if ( $DB ne "eco" )	{
                print "<td valign=top nowrap>$dataRow->{authorizer}</td>\n";
                print qq|<td valign="top" style="padding-left: 0.5em; text-indent: -0.5em;"><span style="padding-right: 1em;">${collection_names}</span> <span class="tiny"><i>${timeplace}</i></span></td>
|;
            } else	{
                $dataRow->{'authorizer'} = $lookup{$dataRow->{'authorizer_no'}};
		$dataRow->{'inventory_name'} = "<a href=\"$exec_url?action=displayCollResults&amp;type=$type&amp;$COLLECTION_NO=$dataRow->{$COLLECTION_NO}\">" . $dataRow->{'inventory_name'} . "</a>";
                $dataRow->{'inventory_method'} =~ s/,/, /g;
                for my $field ( 'authorizer','inventory_name','country','habitat','inventory_method' )	{
                    print "<td valign=top nowrap>$dataRow->{$field}</td>\n";
                }
            }
            print "<td valign=top nowrap>$reference</td>\n";
            print "<td valign=top align=center>".int($dataRow->{distance})." km </td>\n" if ($type eq 'add');
            print "</tr>";
  		}

        print "</table>\n</div>\n";
    } elsif ( $displayRows == 1 ) { # if only one row to display...
		$q->param($COLLECTION_NO=>$dataRows[0]->{$COLLECTION_NO});
                if ( ( $q->param('basic') =~ /yes/i || $DB eq "eco" ) && $type eq "view" )	{
			if ( $DB ne "eco" )	{
				Collection::basicCollectionInfo($dbt,$q,$s,$hbo);
			} else	{
				Collection::inventoryInfo($dbt,$q,$s,$hbo);
			}
			return;
		}
		# Do the action directly if there is only one row
		execAction($action);
    } else {
		# If this is an add,  Otherwise give an error
		if ( $type eq "add" ) {
			if ( $DB ne "eco" )	{
				displayCollectionForm();
			} else	{
				inventoryForm();
			}
			return;
		} else {
			my $error = "<center>\n<p style=\"margin-top: -1em;\">Your search produced no matches: please try again</p>";
			displaySearchColls($error);
			exit;
		}
    }

    ###
    # Display the footer links
    ###
    print "<center><p>";

    # this q2  var is necessary because the processCollectionSearch
    # method alters the CGI object's internals above, and deletes some fields 
    # so, we create a new CGI object with everything intact
    my $q2 = new CGI; 
    my @params = $q2->param;
    my $getString = "rowOffset=".($rowOffset+$limit);
    foreach my $param_key (@params) {
        if ($param_key ne "rowOffset") {
            if ($q2->param($param_key) ne "") {
                $getString .= "&".uri_escape($param_key)."=".uri_escape($q2->param($param_key));
            }
        }
    }

    if (($rowOffset + $limit) < $ofRows) {
        my $numLeft;
        if (($rowOffset + $limit + $limit) > $ofRows) { 
            $numLeft = "the last " . ($ofRows - $rowOffset - $limit);
        } else {
            $numLeft = "the next " . $limit;
        }
        print "<a href=\"$exec_url?$getString\"><b>View $numLeft matches</b></a> - ";
    } 

	if ( $type eq "add" )	{
		print "<a href='$exec_url?action=displaySearchCollsForAdd&type=add'>Do another search</a>";
	} else	{
		print "<a href='$exec_url?action=displaySearchColls&type=$type'>Do another search</a>";
	}

    print "</center></p>";
    # End footer links


	if ( $type eq "add" ) {
		print qq|<form action="$exec_url">\n|;

		# stash the lat/long coordinates to be populated on the
		#  entry form JA 6.4.04
		my @coordfields = ("latdeg","latmin","latsec","latdec","latdir",
				"lngdeg","lngmin","lngsec","lngdec","lngdir");
		if ( $DB eq "eco" )	{
			@coordfields = ("lat","lng","survey_method");
		}
		for my $cf (@coordfields)	{
			if ( $q->param($cf) )	{
				print "<input type=\"hidden\" name=\"$cf\" value=\"";
				print $q->param($cf) . "\">\n";
			}
		}

		if ( $DB ne "eco" )	{
			print qq|<input type="hidden" name="action" value="displayCollectionForm">
|;
		} else	{
			print qq|<input type="hidden" name="action" value="inventoryForm">
|;
		}
		if ( $DB ne "eco" )	{
			print qq|<center>\n<input type=submit value="Add a new collection">|;
		} else	{
			print qq|<center>\n<input type=submit value="Add a new inventory">|;
		}
		print "</center>\n</form>\n";
	}
		
	print $hbo->stdIncludes($PAGE_BOTTOM);

} # end sub displayCollResults


sub getOccurrencesXML {
    require Download;
    require XML::Generator;
    logRequest($s,$q);

    my $rowOffset = $q->param('rowOffset') || 0;
    my $limit = $q->param('limit') ? $q->param('limit') : '';

    # limit passed to permissions module
    my $perm_limit = ($limit) ? $limit + $rowOffset : 100000000;

    $q->param('max_interval_name'=>$q->param("max_interval"));
    $q->param('min_interval_name'=>$q->param("min_interval"));
    $q->param('collections_coords'=>'YES');
    $q->param('collections_coords_format'=>'decimal');
    if ($q->param('xml_format') =~ /points/i) { 
        $q->param('output_data'=>'collections');
    } else {
        $q->param('sp'=>'YES');
        $q->param('indet'=>'YES');
        $q->param('collections_collection_name'=>'YES');
        $q->param('collections_collection_environment'=>'YES');
        $q->param('collections_pres_mode'=>'YES');
        $q->param('collections_reference_no'=>'YES');
        $q->param('collections_country'=>'YES');
        $q->param('collections_state'=>'YES');
        $q->param('collections_geological_group'=>'YES');
        $q->param('collections_formation'=>'YES');
        $q->param('collections_member'=>'YES');
        $q->param('collections_ma_max'=>'YES');
        $q->param('collections_ma_min'=>'YES');
        $q->param('collections_max_interval_no'=>'YES');
        $q->param('collections_min_interval_no'=>'YES');
        $q->param('collections_paleocoords'=>'YES');
        $q->param('collections_paleocoords_format'=>'decimal');
        $q->param('occurrences_occurrence_no'=>'YES');
        $q->param('occurrences_subgenus_name'=>'YES');
        $q->param('occurrences_species_name'=>'YES');
        $q->param('occurrences_plant_organ'=>'YES');
        $q->param('occurrences_plant_organ2'=>'YES');
        $q->param('occurrences_stratcomments'=>'YES');
        $q->param('occurrences_geology_comments'=>'YES');
        $q->param('occurrences_collection_comments'=>'YES');
        $q->param('occurrences_taxonomy_comments'=>'YES');
    }

    my $d = new Download($dbt,$q,$s,$hbo);
    my ($dataRows,$allDataRows,$dataRowsSize) = $d->queryDatabase();
    my @dataRows = @$dataRows;

    my $last_record = scalar(@dataRows);
    if ($limit && (($rowOffset+$limit) < $last_record)) {
        $last_record = $rowOffset + $limit;
    } 

    print "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" standalone=\"yes\"?>\n";

    my $t = new TimeLookup($dbt);
    my $time_lookup;
    if ($q->param('xml_format') !~ /points/i) { 
        $time_lookup = $t->lookupIntervals([],['period_name','epoch_name','stage_name']);
    }

    my $g = XML::Generator->new(escape=>'always',conformance=>'strict',empty=>'args');

    if ($q->param('xml_format') =~ /points/i) { 
        print "<points total=\"$dataRowsSize\">\n";
    } else {
        print "<occurrences total=\"$dataRowsSize\">\n";
    }
#    print "<size>".scalar(@dataRows)."</size>";
    for (my $i = $rowOffset; $i< $last_record;$i++) {
        my $row = $dataRows[$i];

        if ($q->param('xml_format') =~ /points/i) { 
            print $g->p(
                $g->col($row->{'collection_no'}),
                $g->lat($row->{'c.latdec'}),
                $g->lng($row->{'c.lngdec'})
            );
        } else {
            if (!$row->{'c.min_interval_no'} && $row->{'c.max_interval_no'}) {
                $row->{'c.min_interval_no'} = $row->{'c.max_interval_no'};
            }

            my ($period_max,$period_min,$epoch_max,$epoch_min,$stage_max,$stage_min);
            my $max_lookup = $time_lookup->{$row->{'c.max_interval_no'}};
            my $min_lookup = $time_lookup->{$row->{'c.min_interval_no'}};
            # Period lookup
            $period_max = $max_lookup->{'period_name'};
            $period_min = $min_lookup->{'period_name'};
            if (!$period_max) {$period_max = "";}
            if (!$period_min) {$period_min= "";}

            # Epoch lookup
            $epoch_max = $max_lookup->{'epoch_name'};
            $epoch_min = $min_lookup->{'epoch_name'};
            if (!$epoch_max) {$epoch_max = "";}
            if (!$epoch_min) {$epoch_min= "";}

            # Stage lookup
            $stage_max = $max_lookup->{'stage_name'};
            $stage_min = $min_lookup->{'stage_name'};
            if (!$stage_max) {$stage_max = "";}
            if (!$stage_min) {$stage_min= "";}

            my $taxon_name = $row->{'o.genus_name'};
            if ($q->param('lump_genera') ne 'YES') {
                if ($row->{'o.subgenus_name'}) {
                    $taxon_name .= " ($row->{'o.subgenus_name'})";
                }
                $taxon_name .= " $row->{'o.species_name'}";
            }

            my $plant_organs = $row->{'o.plant_organ'};
            if ($row->{'o.plant_organ2'}) {
                $plant_organs .= ",".$row->{'o.plant_organ2'}; 
            }

            print $g->occurrence(
                $g->occurrence_no($row->{'o.occurrence_no'}),
                $g->collection_no($row->{'collection_no'}),
                $g->reference_no($row->{'c.reference_no'}),
                $g->latitude($row->{'c.latdec'}),
                $g->longitude($row->{'c.lngdec'}),
                $g->paleolatitude($row->{'c.paleolatdec'}),
                $g->paleolongitude($row->{'c.paleolngdec'}),
                $g->age_max($row->{'c.ma_max'}),
                $g->age_min($row->{'c.ma_min'}),
                $g->collection_name($row->{'c.collection_name'}),
                $g->environment($row->{'c.environment'}),
                $g->preservation($row->{'c.pres_mode'}),
                $g->group($row->{'c.geological_group'}),
                $g->formation($row->{'c.formation'}),
                $g->member($row->{'c.member'}),
                $g->country($row->{'c.country'}),
                $g->state($row->{'c.state'}),
                $g->taxon_name($taxon_name),
                $g->time_period_max($period_max),
                $g->time_period_min($period_min),
                $g->time_epoch_max($epoch_max),
                $g->time_epoch_min($epoch_min),
                $g->time_stage_max($stage_max),
                $g->time_stage_min($stage_min),
                $g->plant_organ($plant_organs),
                $g->strat_comments($row->{'o.stratcomments'}),
                $g->geology_comments($row->{'o.geology_comments'}),
                $g->collection_comments($row->{'o.collection_comments'}),
                $g->taxonomy_comments($row->{'o.taxonomy_comments'})
            );
        }
        print "\n";
    }
    if ($q->param('xml_format') =~ /points/i) { 
        print "</points>\n";
    } else {
        print "</occurrences>\n";
    }
}

# JA 23.6.12
# hey, it's something
sub jsonTaxon	{
	my $t = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$q->param('name')},['all']);
	my $author = TaxonInfo::formatShortAuthor($t);
	my $parent_hash = TaxaCache::getParents($dbt,[$t->{'taxon_no'}],'array_full');
	my @parent_array = @{$parent_hash->{$t->{'taxon_no'}}};
	my $cof = Collection::getClassOrderFamily($dbt,'',\@parent_array);
	print qq|{ "PaleoDB_no": "$t->{'taxon_no'}", "author": "$author", "common_name": "$t->{'common_name'}", "extant": "$t->{'extant'}", "rank": "$t->{'taxon_rank'}", "family": "$cof->{'family'}", "order": "$cof->{'order'}", "class": "$cof->{'class'}" }|;
}

# JA 27.6.12
sub jsonCollection	{
	Collection::jsonCollection($dbt,$q,$s);
}

# JA 5-6.4.04
# compose the SQL to find collections of a certain age within 100 km of
#  a coordinate (required when the user wants to add a collection)
sub processCollectionsSearchForAdd	{

	my $dbh = $dbt->dbh;
	return if PBDBUtil::checkForBot();
	require Map;

	# some generally useful trig stuff needed by processCollectionsSearchForAdd
	my $PI = 3.14159265;

	my $sql;

	if ( $DB ne "eco" )	{
	# get a list of interval numbers that fall in the geological period
		my $t = new TimeLookup($dbt);
		$sql = "SELECT interval_no FROM intervals WHERE interval_name LIKE ".$dbh->quote($q->param('period_max'));
		my $period_no = ${$dbt->getData($sql)}[0]->{'interval_no'};
		my @intervals = $t->mapIntervals($period_no);
		$sql = "SELECT c.collection_no, c.collection_aka, c.authorizer_no, p1.name authorizer, c.collection_name, c.access_level, c.research_group, c.release_date, DATE_FORMAT(release_date, '%Y%m%d') rd_short, c.country, c.state, c.latdeg, c.latmin, c.latsec, c.latdec, c.latdir, c.lngdeg, c.lngmin, c.lngsec, c.lngdec, c.lngdir, c.max_interval_no, c.min_interval_no, c.reference_no FROM collections c LEFT JOIN person p1 ON p1.person_no = c.authorizer_no WHERE ";
		$sql .= "c.max_interval_no IN (" . join(',', @intervals) . ") AND ";
	} else	{
		$sql = "SELECT i.* FROM inventories i LEFT JOIN person p1 ON p1.person_no=i.authorizer_no WHERE FIND_IN_SET('".$q->param('inventory_method')."',inventory_method) AND ";
	}


	# convert the submitted lat/long values
	my ($lat,$lng,$latlng_format);
	if ( $DB ne "eco" )	{
		($lat,$latlng_format) = Collection::fromMinSec($q->param('latdeg'),$q->param('latmin'),$q->param('latsec'));
		($lng,$latlng_format) = Collection::fromMinSec($q->param('lngdeg'),$q->param('lngmin'),$q->param('lngsec'));
	} elsif ( $q->param('latsec') ne "" )	{
		$lat = sprintf("%s%c %d' %d\"",$q->param('latdeg'),186,$q->param('latmin'),$q->param('latsec'));
		$lng = sprintf("%s%c %d' %d\"",$q->param('lngdeg'),186,$q->param('lngmin'),$q->param('lngsec'));
	} elsif ( $q->param('latmin') ne "" )	{
		$lat = sprintf("%s%c %d'",$q->param('latdeg'),186,$q->param('latmin'));
		$lng = sprintf("%s%c %d'",$q->param('lngdeg'),186,$q->param('lngmin'));
	} elsif ( $q->param('latdec') )	{
		$lat = $q->param('latdeg').".".$q->param('latdec');
		$lng = $q->param('lngdeg').".".$q->param('lngdec');
	} else	{
		$lat = $q->param('latdeg');
		$lng = $q->param('lngdeg');
	}
	
	# west and south are negative
	if ( $q->param('latdir') =~ /S/ )	{
		$lat = "-".$lat;
	}
	if ( $q->param('lngdir') =~ /W/ )	{
		$lng = "-".$lng;
	}
	my $mylat = $lat;
	my $mylng = $lng;
	# these are needed to stash values as hiddens
	if ( $DB eq "eco" )	{
		$q->param('lat' => $lat);
		$q->param('lng' => $lng);
	}

	# convert the coordinates to decimal values
	# maximum latitude is center point plus 100 km, etc.
	# longitude is a little tricky because we have to deal with cosines
	# it's important to use floor instead of int because they round off
	#  negative numbers differently
	my $maxlat = floor($lat + 100 / 111);
	my $minlat = floor($lat - 100 / 111);
	my $maxlng = floor($lng + ( (100 / 111) / cos($lat * $PI / 180) ) );
	my $minlng = floor($lng - ( (100 / 111) / cos($lat * $PI / 180) ) );

	# create an inlist of lat/long degree values for hitting the
	#  collections table
	if ( $DB ne "eco" )	{

	# reset the limits if you go "north" of the north pole etc.
	# note that we don't have to get complicated with resetting, say,
	#  the minlat when you limit maxlat because there will always be
	#  enough padding
	# if you're too close to lat 0 or lng 0 there's no problem because
	#  you'll just repeat some values like 1 or 2 in the inlist, but we
	#  do need to prevent looking in just one hemisphere
	# if you have a "wraparound" like this you need to look in both
	#  hemispheres anyway, so don't add a latdir= or lngdir= clause
	if ( $maxlat >= 90 )	{
		$maxlat = 89;
	} elsif ( $minlat <= -90 )	{
		$minlat = -89;
	} elsif ( ( $maxlat > 0 && $minlat > 0 ) || ( $maxlat < 0 && $minlat < 0 ) )	{
		$sql .= "c.latdir='" . $q->param('latdir') . "' AND ";
	}
	if ( $maxlng >= 180 )	{
		$maxlng = 179;
	} elsif ( $minlng <= -180 )	{
		$minlng = -179;
	} elsif ( ( $maxlng > 0 && $minlng > 0 ) || ( $maxlng < 0 && $minlng < 0 ) )	{
		$sql .= "c.lngdir='" . $q->param('lngdir') . "' AND ";
	}

	my $inlist;
	for my $l ($minlat..$maxlat)	{
		$inlist .= abs($l) . ",";
	}
	$inlist =~ s/,$//;
	$sql .= "c.latdeg IN (" . $inlist . ") AND ";

	$inlist = "";
	for my $l ($minlng..$maxlng)	{
		$inlist .= abs($l) . ",";
	}
	$inlist =~ s/,$//;
	$sql .= "c.lngdeg IN (" . $inlist . ")";

	}
	# for the inventories table, simply use the built-in decimal values
	else	{
		$sql .= "i.lat >= $minlat AND i.lat <= $maxlat AND i.lng >= $minlng AND i.lng <= $maxlng";
	}

	if ($q->param('sortby') eq $COLLECTION_NO) {
		$sql .= " ORDER BY c.$COLLECTION_NO";
	} elsif ($q->param('sortby') =~ /collection_name|inventory_name/) {
		$sql .= " ORDER BY c.".$q->param('sortby');
	}

	my @dataRows = ();

	if ( $DB ne "eco" )	{
		my $sth = $dbt->dbh->prepare($sql);
		$sth->execute();
		my $p = Permissions->new ($s,$dbt);
		my $limit = 10000000;
		my $ofRows = 0;
		$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );
	} else	{
		@dataRows = @{$dbt->getData($sql)};
	}


	# make sure collections really are within 100 km of the submitted
	#  lat/long coordinate JA 6.4.04
	my @tempDataRows;

    # have to recompute this
    for my $dr (@dataRows)	{
        my ($lat,$lng);
        if ( $DB ne "eco" )	{
        # compute the coordinate
        $lat = $dr->{'latdeg'};
        $lng = $dr->{'lngdeg'};
        if ( $dr->{'latmin'} )	{
            $lat = $lat + ( $dr->{'latmin'} / 60 ) + ( $dr->{'latsec'} / 3600 );
        } else	{
            $lat = $lat . "." . $dr->{'latdec'};
        }
    
        if ( $dr->{'lngmin'} )	{
            $lng = $lng + ( $dr->{'lngmin'} / 60 ) + ( $dr->{'lngsec'} / 3600 );
        } else	{
            $lng = $lng . "." . $dr->{'lngdec'};
        }

        # west and south are negative
        if ( $dr->{'latdir'} =~ /S/ )	{
            $lat = $lat * -1;
        }
        if ( $dr->{'lngdir'} =~ /W/ )	{
            $lng = $lng * -1;
        }
        } else	{
            $lat = $dr->{'lat'};
            $lng = $dr->{'lng'};
        }

        # if the points are less than 100 km apart, save
        #  the collection
        my $distance = 111 * Map::GCD($mylat,$lat,abs($mylng-$lng));
        if ( $distance < 100 )	{
            $dr->{'distance'} = $distance;
            push @tempDataRows, $dr;
        } 
    }

	if ($q->param('sortby') eq 'distance')	{
		@tempDataRows = sort {$a->{'distance'} <=> $b->{'distance'}  ||
		$a->{'collection_no'} <=> $b->{'collection_no'}} @tempDataRows;
	}

	return (\@tempDataRows,scalar(@tempDataRows));
}


sub displayCollectionForm {
    # Have to be logged in
    if (!$s->isDBMember()) {
        login("Please log in first.");
        exit;
    }
    print $hbo->stdIncludes($PAGE_TOP);
    Collection::displayCollectionForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processCollectionForm {
    if (!$s->isDBMember()) {
        login("Please log in first.");
        exit;
    }
    print $hbo->stdIncludes($PAGE_TOP);
    Collection::processCollectionForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);

}

sub displayCollectionDetails {
    logRequest($s,$q);
    Collection::displayCollectionDetails($dbt,$q,$s,$hbo);
}

sub rarefyAbundances {
    return if PBDBUtil::checkForBot();
    logRequest($s,$q);

    print $hbo->stdIncludes($PAGE_TOP);
    Collection::rarefyAbundances($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayCollectionEcology	{
	return if PBDBUtil::checkForBot();
	logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	Collection::displayCollectionEcology($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub explainAEOestimate	{
	return if PBDBUtil::checkForBot();
	print $hbo->stdIncludes($PAGE_TOP);
	Collection::explainAEOestimate($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_TOP);
}

# PS 11/7/2005
#
# Generic opinions earch handling form.
# Flow of this is a little complicated
#
sub submitOpinionSearch {
    print $hbo->stdIncludes($PAGE_TOP);
    if ($q->param('taxon_name')) {
        $q->param('goal'=>'opinion');
        processTaxonSearch($dbt,$hbo,$q,$s);
    } else {
        $q->param('goal'=>'opinion');
        Opinion::displayOpinionChoiceForm($dbt,$s,$q);
    }
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

# JA 17.8.02
#
# Generic authority search handling form, used as a front end for:
#  add/edit authority, add/edit opinion, add image, add ecology data, search by ref no
#
# Edited by rjp 1/22/2004, 2/18/2004, 3/2004
# Edited by PS 01/24/2004, accept reference_no instead of taxon_name optionally
#
sub submitTaxonSearch {
    print $hbo->stdIncludes($PAGE_TOP);
    processTaxonSearch($dbt, $hbo, $q, $s);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processTaxonSearch {
    my ($dbt, $hbo, $q, $s) = @_;
    my $dbh = $dbt->dbh;
	# check for proper spacing of the taxon..
	my $errors = Errors->new();
	$errors->setDisplayEndingMessage(0); 

    if ($q->param('taxon_name')) {
        if (! Taxon::validTaxonName($q->param('taxon_name'))) {
            $errors->add("Ill-formed taxon name.  Check capitalization and spacing.");
        }
    }

	# Try to find this taxon in the authorities table

    my %options;
    if ($q->param('taxon_name')) {
        $options{'taxon_name'} = $q->param('taxon_name');
    } else {
        if ($q->param("authorizer_reversed")) {
            my $sql = "SELECT person_no FROM person WHERE name LIKE ".$dbh->quote(Person::reverseName($q->param('authorizer_reversed')));
            my $authorizer_no = ${$dbt->getData($sql)}[0]->{'person_no'};
            if (!$authorizer_no) {
                $errors->add($q->param('authorizer_reversed')." is not a valid authorizer. Format like 'Sepkoski, J.'");
            } else {
                $options{'authorizer_no'} = $authorizer_no;
            }
        }
        if ($q->param('created_year')) {
            my ($yyyy,$mm,$dd) = ($q->param('created_year'),$q->param('created_month'),$q->param('created_day'));
            my $date = sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd);
            $options{'created'}=$date;
            $options{'created_before_after'}=$q->param('created_before_after');
        }
        $options{'author'} = $q->param('author');
        $options{'pubyr'} = $q->param('pubyr');
        $options{'reference_no'} = $q->param('reference_no');
    }
    if (scalar(%options) == 0) {
        $errors->add("You must fill in at least one field");
    }

	if ($errors->count()) {
		print $errors->errorMessage();
		return;
	}

    # Denormalize with the references table automatically
    $options{'get_reference'} = 1;
    # Also match against subgenera if the user didn't explicity state the genus
    $options{'match_subgenera'} = 1;
    # If we have multiple versions of a name (i.e. Cetacea) but they're really the
    # same taxa that's been ranged differently, then don't treat it as a homonym, use the original rank
    unless ($q->param('goal') eq 'authority') {
        $options{'remove_rank_change'} = 1;
    }
    
    my $goal = $q->param('goal');
    my $taxon_name = $q->param('taxon_name');
    my $next_action = 
          ($goal eq 'authority')  ? 'displayAuthorityForm' 
        : ($goal eq 'opinion')    ? 'displayOpinionChoiceForm'
        : ($goal eq 'cladogram')  ? 'displayCladogramChoiceForm'
        : ($goal eq 'image')      ? 'displayLoadImageForm'
        : ($goal eq 'ecotaph')    ? 'startPopulateEcologyForm'
        : ($goal eq 'ecovert')    ? 'startPopulateEcologyForm'
        : croak("Unknown goal given in submit taxon search");

    if ( $goal eq 'authority' || $goal eq 'opinion' )	{
        $options{'ignore_common_name'} = "YES";
    }
    if ( ( $goal eq 'authority' || $goal eq 'opinion' ) && $q->param('taxon_name') =~ / \(.*\)/ )	{
        $options{'match_subgenera'} = "";
    }
    
    my @results = TaxonInfo::getTaxa($dbt,\%options,['*']);
        
    # If there were no matches, present the new taxon entry form immediately
    # We're adding a new taxon
    if (scalar(@results) == 0) {
        if ($q->param('goal') eq 'authority') {
            # Try to see if theres any near matches already existing in the DB
            if ($q->param('taxon_name')) {
                my ($g,$sg,$sp) = Taxon::splitTaxon($q->param('taxon_name'));
                my ($oldg,$oldsg,$oldsp);
                my @typoResults = ();
                unless ($q->param("skip_typo_check")) {
                # give a free pass if the name is "plausible" because its
                #  parts all exist in the authorities table JA 21.7.08
                # disaster could ensue if the parts are actually typos,
                #  but let's cross our fingers
                # perhaps getTaxa could be adapted for this purpose, but
                #  it's a pretty simple piece of code
                    my $sql = "SELECT taxon_name tn FROM authorities WHERE taxon_name='$g' OR taxon_name LIKE '$g %' OR taxon_name LIKE '% ($sg) %' OR taxon_name LIKE '% $sp'";
                    my @partials = @{$dbt->getData($sql)};
                    for my $p ( @partials )	{
                        if ( $p->{tn} eq $g )	{
                            $oldg++;
                        }
                        if ( $p->{tn} =~ /^$g / )	{
                            $oldg++;
                        }
                        if ( $p->{tn} =~ / \($sg\) / )	{
                            $oldsg++;
                        }
                        if ( $p->{tn} =~ / $sp$/ )	{
                            $oldsp++;
                        }
                    }
                    if ( $oldg == 0 || ( $sg && $oldsg == 0 ) || $oldsp == 0 )	{
                        $sql = "SELECT count(*) c FROM occurrences WHERE genus_name LIKE ".$dbh->quote($g)." AND taxon_no>0";
                        if ($sg) {
                            $sql .= " AND subgenus_name LIKE ".$dbh->quote($sg);
                        }
                        if ($sp) {
                            $sql .= " AND species_name LIKE ".$dbh->quote($sp);
                        }
                        my $exists_in_occ = ${$dbt->getData($sql)}[0]->{c};
                        unless ($exists_in_occ) {
                            my @results = keys %{TypoChecker::taxonTypoCheck($dbt,$q->param('taxon_name'),"",1)};
                            my ($g,$sg,$sp) = Taxon::splitTaxon($q->param('taxon_name'));
                            foreach my $typo (@results) {
                                my ($t_g,$t_sg,$t_sp) = Taxon::splitTaxon($typo);
                            # if the genus exists, we only want typos including
                            # it JA 16.3.11
                                if ( $oldg && $g ne $t_g )	{
                                    next;
                                }
                                if ($sp && !$t_sp) {
                                    $typo .= " $sp";
                                }
                                push @typoResults, $typo;
                            }
                        }
                    }
                }

                if (@typoResults) {
                    print "<div align=\"center\">\n";
    		        print "<p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">'<i>" . $q->param('taxon_name') . "</i>' was not found</p>\n<br>\n";
                    print "<div class=\"displayPanel medium\" style=\"width: 36em; padding: 1em;\">\n";
                    print "<p><div align=\"left\"><ul>";
                    my $none = "None of the above";
                    if ( $#typoResults == 0 )	{
                        $none = "Not the one above";
                    }
                    foreach my $name (@typoResults) {
                        my @full_rows = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$name},['*']);
                        if (@full_rows) {
                            foreach my $full_row (@full_rows) {
                                my ($name,$authority) = Taxon::formatTaxon($dbt,$full_row,'return_array'=>1);
                                print "<li><a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_no=$full_row->{taxon_no}\">$name</a>$authority</li>";
                            }
                        } else {
                            print "<li><a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_name=$name\">$name</a></li>";
                        }
                    }
                    # route them to a genus form instead if the genus doesn't
                    #   exist JA 24.10.11
                    if ( $oldg == 0 && $sp )	{
                        print "<li><a href=\"$WRITE_URL?action=submitTaxonSearch&goal=authority&taxon_name=$g&amp;skip_typo_check=1\">$none</a> - create a new record for this genus";
                    } else	{
                        print "<li><a href=\"$WRITE_URL?action=submitTaxonSearch&goal=authority&taxon_name=".$q->param('taxon_name')."&amp;skip_typo_check=1\">$none</a> - create a new taxon record";
                    }
                    print "</ul>";

                    print "<div align=left class=small style=\"width: 500\">";
                    print "<p>The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, some approximate matches were found and are listed above.  If none of the names above match, please enter a new taxon record.";
                    print "</div></p>";
                    print "</div>";
                    print "</div>";
                } else {
                    if (!$s->get('reference_no')) {
                        $s->enqueue($q->query_string());
                        displaySearchRefs("Please choose a reference before adding a new taxon",1);
                        exit;
                    }
                    $q->param('taxon_no'=> -1);
                    Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);
                }
            } else {
                print "<div align=\"center\"><p class=\"pageTitle\">No taxonomic names found</p></div>";
            }
        } else {
            # Try to see if theres any near matches already existing in the DB
            my @typoResults = ();
            if ($q->param('taxon_name')) {
                @typoResults = TypoChecker::typoCheck($dbt,'authorities','taxon_name','taxon_no,taxon_name,taxon_rank','',$q->param('taxon_name'),1);
            }

            if (@typoResults) {
                print "<div align=\"center\">";
    		    print "<p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">'<i>" . $q->param('taxon_name') . "</i>' was not found</p>\n<br>\n";
                print "<div class=\"displayPanel medium\" style=\"width: 36em; padding: 1em;\">\n";
                print "<div align=\"left\"><ul>";
                foreach my $row (@typoResults) {
                    my $full_row = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['*']);
                    my ($name,$authority) = Taxon::formatTaxon($dbt,$full_row,'return_array'=>1);
                    print "<li><a href=\"$WRITE_URL?action=$next_action&amp;goal=$goal&amp;taxon_name=$full_row->{taxon_name}&amp;taxon_no=$row->{taxon_no}\">$name</a>$authority</li>";
                }
                print "</ul>";

                print qq|<div align=left class="small">\n<p>|;
                if ( $#typoResults > 0 )	{
                    print "The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, some approximate matches were found and are listed above.  If none of them are what you're looking for, please <a href=\"$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=".$q->param('taxon_name')."\">enter a new authority record</a> first.";
                } else	{
                    print "The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, an approximate match was found and is listed above.  If it is not what you are looking for, please <a href=\"$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=".$q->param('taxon_name')."\">enter a new authority record</a> first.";
                }
                print "</div></p>";
                print "</div>";
                print "</div>";
            } else {
                if ($q->param('taxon_name')) {
                    push my @errormessages , "The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.<br>Please <a href=\"$WRITE_URL?action=submitTaxonSearch&goal=authority&taxon_name=".$q->param('taxon_name')."\">enter</a> an authority record for this taxon first.";
                    print "<div align=\"center\" class=\"large\">".Debug::printWarnings(\@errormessages)."</div>";
                } else {
                    print "<div align=\"center\" class=\"large\">No taxonomic names were found that match the search criteria.</div>";
                }
            }
            return;
        }
    # One match - good enough for most of these forms
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'authority') {
        $q->param("taxon_no"=>$results[0]->{'taxon_no'});
        $q->param('called_by'=> 'processTaxonSearch');
        Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'cladogram') {
        $q->param("taxon_no"=>$results[0]->{'taxon_no'});
        Cladogram::displayCladogramChoiceForm($dbt,$q,$s,$hbo);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'opinion') {
        $q->param("taxon_no"=>$results[0]->{'taxon_no'});
        Opinion::displayOpinionChoiceForm($dbt,$s,$q);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'image') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Images::displayLoadImageForm($dbt,$q,$s); 
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'ecotaph') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Ecology::populateEcologyForm($dbt, $hbo, $q, $s, $WRITE_URL);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'ecovert') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Ecology::populateEcologyForm($dbt, $hbo, $q, $s, $WRITE_URL);
	# We have more than one matches, or we have 1 match or more and we're adding an authority.
    # Present a list so the user can either pick the taxon,
    # or create a new taxon with the same name as an exisiting taxon
	} else	{
		print "<div align=\"center\">\n";
        if ($q->param("taxon_name")) { 
    		print "<p class=\"pageTitle\" style=\"margin-top: 1em;\">Which '<i>" . $q->param('taxon_name') . "</i>' do you mean?</p>\n<br>\n";
        } else {
		if ( $s->isDBMember() )	{
    			print "<p class=\"pageTitle\">Select a taxon to edit</p>\n";
		} else	{
    			print "<p class=\"pageTitle\">Taxonomic names from ".Reference::formatShortRef($dbt,$q->param("reference_no"))."</p>\n";
        	}
        }

        # now create a table of choices
		print "<div class=\"displayPanel medium\" style=\"width: 40em; padding: 1em; padding-right: 2em; margin-top: -1em;\">";
        print "<div align=\"left\"><ul>\n";
        my $checked = (scalar(@results) == 1) ? "CHECKED" : "";
        foreach my $row (@results) {
            # Check the button if this is the first match, which forces
            #  users who want to create new taxa to check another button
            my ($name,$authority) = Taxon::formatTaxon($dbt, $row,'return_array'=>1);
            if ( $s->isDBMember() )	{
                print qq|<li><a href=\"$WRITE_URL?action=$next_action&amp;goal=$goal&amp;taxon_name=$taxon_name&amp;taxon_no=$row->{taxon_no}\">|;
                print "$name</a>$authority</li>\n";
            } else	{
                print "<li>$name$authority</li>\n";
            }
        }

        # always give them an option to create a new taxon as well
        if ($q->param('goal') eq 'authority' && $q->param('taxon_name')) {
            print qq|<li><a href=\"$WRITE_URL?action=$next_action&amp;goal=$goal&amp;taxon_name=$taxon_name&amp;taxon_no=-1\">|;
            if ( scalar(@results) == 1 )	{
                print "No, not the one above ";
            } else	{
                print "None of the above ";
            }
            print "</a>";
            print "- create a new taxon record</li>\n";
        }
        
		print "</ul></div>";

        # we print out difference buttons for two cases:
        #  1: using a taxon name. give them an option to add a new taxon, so button is Submit
        #  2: this is from a reference_no. No option to add a new taxon, so button is Edit
        if ($q->param('goal') eq 'authority') {
            if ($q->param('taxon_name')) {
		        print "<p align=\"left\"><div class=\"verysmall\" style=\"margin-left: 2em; text-align: left;\">";
                print "You have a choice because there may be multiple biological species<br>&nbsp;&nbsp;(e.g., a plant and an animal) with identical names.<br>\n";
		        print "Create a new taxon only if the old ones were named by different people in different papers.<br></div></p>\n";
            } else {
            }
        } else {
            print "<p align=\"left\"><div class=\"verysmall\" style=\"margin-left: 2em; text-align: left;\">";
            print "You have a choice because there may be multiple biological species<br>&nbsp;&nbsp;(e.g., a plant and an animal) with identical names.<br></div></p>\n";
        }
		print "<p align=\"left\"><div class=\"verysmall\" style=\"margin-left: 2em; text-align: left;\">";
        if (!$q->param('reference_no')) {
		    print "You may want to read the <a href=\"javascript:tipsPopup('/public/tips/taxonomy_FAQ.html')\">FAQ</a>.</div></p>\n";
        }

		print "</div>\n</div>\n";
	}
}

##############
## Authority stuff

# startTaxonomy separated out into startAuthority and startOpinion 
# since they're really separate things but were incorrectly grouped
# together before.  For opinions, always pass the original combination and spelling number
# for authorities, just pass what the user types in
# PS 04/27/2004

# Called when the user clicks on the "Add/edit taxonomic name" or 
sub displayAuthorityTaxonSearchForm {
    if ($q->param('use_reference') eq 'new') {
        $s->setReferenceNo(0);
    }
    print $hbo->stdIncludes($PAGE_TOP);
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    print Person::makeAuthEntJavascript($dbt);

    $vars{'page_title'} = "Search for names to add or edit";
    $vars{'action'} = "submitTaxonSearch";
    $vars{'taxonomy_fields'} = "YES";
    $vars{'goal'} = "authority";

    print $hbo->populateHTML('search_taxon_form', \%vars);

    print $hbo->stdIncludes($PAGE_BOTTOM);
}

# rjp, 3/2004
#
# The form to edit an authority
sub displayAuthorityForm {
    if ( $q->param('taxon_no') == -1) {
        if (!$s->get('reference_no')) {
            $s->enqueue($q->query_string());
			displaySearchRefs("You must choose a reference before adding a new taxon" );
			exit;
        }
	} 
    print $hbo->stdIncludes($PAGE_TOP);
	Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub submitAuthorityForm {
    print $hbo->stdIncludes($PAGE_TOP);
	Taxon::submitAuthorityForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayClassificationTableForm {
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	} 
    if (!$s->get('reference_no')) {
        $s->enqueue('action=displayClassificationTableForm');
		displaySearchRefs("You must choose a reference before adding new taxa" );
		exit;
	}
    print $hbo->stdIncludes($PAGE_TOP);
	FossilRecord::displayClassificationTableForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayClassificationUploadForm {
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	} 
    if (!$s->get('reference_no')) {
        $s->enqueue('action=displayClassificationUploadForm');
		displaySearchRefs("You must choose a reference before adding new taxa" );
		exit;
	}
    print $hbo->stdIncludes($PAGE_TOP);
	FossilRecord::displayClassificationUploadForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub submitClassificationTableForm {
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	} 
    print $hbo->stdIncludes($PAGE_TOP);
	FossilRecord::submitClassificationTableForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitClassificationUploadForm {
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	} 
    print $hbo->stdIncludes($PAGE_TOP);
	FossilRecord::submitClassificationUploadForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

## END Authority stuff
##############

##############
## Opinion stuff

# "Add/edit taxonomic opinion" link on the menu page. 
# Step 1 in our opinion editing process
sub displayOpinionSearchForm {
    if ($q->param('use_reference') eq 'new') {
        $s->setReferenceNo(0);
    }
    print $hbo->stdIncludes($PAGE_TOP);
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');
    print Person::makeAuthEntJavascript($dbt);

    $vars{'page_title'} = "Search for opinions to add or edit";
    $vars{'action'} = "submitOpinionSearch";
    $vars{'taxonomy_fields'} = "YES";

    print $hbo->populateHTML('search_taxon_form', \%vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

# PS 01/24/2004
# Changed from displayOpinionList to just be a stub for function in Opinion module
# Step 2 in our opinion editing process. now that we know the taxon, select an opinion
sub displayOpinionChoiceForm {
    if ($q->param('use_reference') eq 'new') {
        $s->setReferenceNo(0);
    }
	print $hbo->stdIncludes($PAGE_TOP);
    Opinion::displayOpinionChoiceForm($dbt,$s,$q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub reviewOpinionsForm	{
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	}
	Opinion::reviewOpinionsForm($dbt,$hbo,$s,$q);
}

sub reviewOpinions	{
	if (!$s->isDBMember()) {
		login( "Please log in first.");
		exit;
	}
	Opinion::reviewOpinions($dbt,$hbo,$s,$q);
}

# rjp, 3/2004
#
# Displays a form for users to add/enter opinions about a taxon.
# It grabs the taxon_no and opinion_no from the CGI object ($q).
sub displayOpinionForm {
	if ($q->param('opinion_no') != -1 && $q->param("opinion_no") !~ /^\d+$/) {
		return;	
	}

	if ($q->param('opinion_no') == -1) {
        if (!$s->get('reference_no') || $q->param('use_reference') eq 'new') {
            # Set this to prevent endless loop
            $q->param('use_reference'=>'');
            $s->enqueue($q->query_string()); 
            displaySearchRefs("You must choose a reference before adding a new opinion");
            exit;
        }
	}
	
	print $hbo->stdIncludes($PAGE_TOP);
	Opinion::displayOpinionForm($dbt, $hbo, $s, $q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitOpinionForm {
	print $hbo->stdIncludes($PAGE_TOP);
	Opinion::submitOpinionForm($dbt,$hbo, $s, $q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub entangledNamesForm	{
	my $error = shift;
	print $hbo->stdIncludes($PAGE_TOP);
	Taxon::entangledNamesForm($dbt,$hbo,$s,$q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub disentangleNames	{
	print $hbo->stdIncludes($PAGE_TOP);
	Taxon::disentangleNames($dbt,$hbo,$s,$q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitTypeTaxonSelect {
	print $hbo->stdIncludes($PAGE_TOP);
	Taxon::submitTypeTaxonSelect($dbt, $s, $q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub badNameForm	{
	my %vars;
	$vars{'error'} = $_[0];
	if ( $vars{'error'} )	{
		$vars{'error'} = '<p class="small" style="margin-left: 1em; margin-bottom: 1.5em; margin-top: 1em; text-indent: -1em;">' . $vars{'error'} . ". Please try again.</p>\n\n";
	}
	print $hbo->stdIncludes($PAGE_TOP);
	print $hbo->populateHTML('bad_name_form', \%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub badNames	{
	print $hbo->stdIncludes($PAGE_TOP);
	Opinion::badNames($dbt,$hbo,$s,$q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

## END Opinion stuff
##############

##############
## Editing list stuff
sub displayPermissionListForm {
	print $hbo->stdIncludes($PAGE_TOP);
    Permissions::displayPermissionListForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitPermissionList {
	print $hbo->stdIncludes($PAGE_TOP);
    Permissions::submitPermissionList($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
} 

sub submitHeir{
	print $hbo->stdIncludes($PAGE_TOP);
    Permissions::submitHeir($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
} 

##############
## Occurrence misspelling stuff

sub searchOccurrenceMisspellingForm {
    if (!$s->isDBMember()) {
        # have to be logged in
        $s->enqueue("action=searchOccurrenceMisspellingForm" );
        login( "Please log in first." );
        exit;
    }
	print $hbo->stdIncludes($PAGE_TOP);
	TypoChecker::searchOccurrenceMisspellingForm ($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub occurrenceMisspellingForm {
	print $hbo->stdIncludes($PAGE_TOP);
	TypoChecker::occurrenceMisspellingForm ($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitOccurrenceMisspelling {
	print $hbo->stdIncludes($PAGE_TOP);
	TypoChecker::submitOccurrenceMisspelling($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

## END occurrence misspelling stuff
##############

##############
## Reclassify stuff

sub startStartReclassifyOccurrences	{
	Reclassify::startReclassifyOccurrences($q, $s, $dbt, $hbo);
}

sub startDisplayOccurrenceReclassify	{
	Reclassify::displayOccurrenceReclassify($q, $s, $dbt, $hbo);
}

sub startProcessReclassifyForm	{
	Reclassify::processReclassifyForm($q, $s, $dbt, $hbo);
}

## END Reclassify stuff
##############

##############
## Taxon Info Stuff

sub beginTaxonInfo{
    print $hbo->stdIncludes( $PAGE_TOP );
    if ($IS_FOSSIL_RECORD) {
        FossilRecord::displaySearchTaxaForm($dbt,$q,$s,$hbo);
    } else {
        TaxonInfo::searchForm($hbo, $q);
    }
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub checkTaxonInfo {
    logRequest($s,$q);
    if ( $q->param('match') eq "all" )	{
        print $hbo->stdIncludes( $PAGE_TOP );
        $q->param('taxa' => @{TaxonInfo::getMatchingSubtaxa($dbt,$q,$s,$hbo)} );
        if ( ! $q->param('taxa') )	{
            TaxonInfo::searchForm($hbo,$q,1);
        } else	{
            TaxonInfo::checkTaxonInfo($q, $s, $dbt, $hbo);
        }
        print $hbo->stdIncludes($PAGE_BOTTOM);
        exit;
    } elsif ( $q->param('match') eq "random" )	{
        # infinite loops are bad
        TaxonInfo::getMatchingSubtaxa($dbt,$q,$s,$hbo);
        $q->param('match' => '');
    }
    print $hbo->stdIncludes( $PAGE_TOP );
    if ($IS_FOSSIL_RECORD) {
         FossilRecord::submitSearchTaxaForm($dbt,$q,$s,$hbo);
    } else {
        TaxonInfo::checkTaxonInfo($q, $s, $dbt, $hbo);
    }
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayTaxonInfoResults {
    print $hbo->stdIncludes( $PAGE_TOP );
	TaxonInfo::displayTaxonInfoResults($dbt,$s,$q,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

# JA 3.11.09
sub basicTaxonInfo	{
	TaxonInfo::basicTaxonInfo($q,$s,$dbt,$hbo);
}

## END Taxon Info Stuff
##############

sub beginFirstAppearance	{
	print $hbo->stdIncludes( $PAGE_TOP );
	TaxonInfo::beginFirstAppearance($hbo, $q, '');
	print $hbo->stdIncludes( $PAGE_BOTTOM );
}

sub displayFirstAppearance	{
	print $hbo->stdIncludes( $PAGE_TOP );
	TaxonInfo::displayFirstAppearance($q, $s, $dbt, $hbo);
	print $hbo->stdIncludes( $PAGE_BOTTOM );
}

sub displaySearchFossilRecordTaxaForm {
    print $hbo->stdIncludes( $PAGE_TOP );
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitSearchFossilRecordTaxa {
    logRequest($s,$q);
    print $hbo->stdIncludes( $PAGE_TOP );
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayFossilRecordCurveForm {
    print $hbo->stdIncludes( $PAGE_TOP );
	FossilRecord::displayFossilRecordCurveForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub submitFossilRecordCurveForm {
    print $hbo->stdIncludes( $PAGE_TOP );
	FossilRecord::submitFossilRecordCurveForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

### End Module Navigation
##############


##############
## Scales stuff JA 7.7.03
sub searchScale	{
	require Scales;
	print $hbo->stdIncludes($PAGE_TOP);
	Scales::searchScale($dbt, $hbo, $s, $q);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub processShowForm	{
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
	Scales::processShowEditForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub processViewScale	{
    require Scales;
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
	Scales::processViewTimeScale($dbt, $hbo, $q, $s);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub processEditScale	{
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
	Scales::processEditScaleForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub displayTenMyBinsDebug {
    return if PBDBUtil::checkForBot();
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
    Scales::displayTenMyBinsDebug($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub submitSearchInterval {
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
    Scales::submitSearchInterval($dbt, $hbo, $q);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub displayInterval {
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
    Scales::displayInterval($dbt, $hbo, $q);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub displayTenMyBins {
    return if PBDBUtil::checkForBot();
    require Scales;
    print $hbo->stdIncludes($PAGE_TOP);
    Scales::displayTenMyBins($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

## END Scales stuff
##############


##############
## Images stuff
sub startImage{
    my $goal='image';
    my $page_title ='Search for the taxon with an image to be added';

    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('search_taxon_form',[$page_title,'submitTaxonSearch',$goal],['page_title','action','goal']);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayLoadImageForm{
    print $hbo->stdIncludes($PAGE_TOP);
	Images::displayLoadImageForm($dbt, $q, $s);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processLoadImage{
	if (!$s->isDBMember()) {
		login( "Please log in first");
		exit;
	} 
	print $hbo->stdIncludes($PAGE_TOP);
	Images::processLoadImage($dbt, $q, $s);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub searchGallery	{
	print $hbo->stdIncludes($PAGE_TOP);
	print $hbo->populateHTML('search_taxoninfo_form' , ['Image gallery search form','',1,1], ['page_title','page_subtitle','gallery_form','basic_fields']);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub gallery	{
	Images::gallery($q,$s,$dbt,$hbo);
}

sub displayImage {
    if ($q->param("display_header") eq 'NO') {
        print $hbo->stdIncludes("blank_page_top") 
    } else {
        print $hbo->stdIncludes($PAGE_TOP) 
    }
    my $image_no = int($q->param('image_no'));
    if (!$image_no) {
        print "<div align=\"center\">".Debug::printErrors(["No image number specified"])."</div>";
    } else {
        my $height = $q->param('maxheight');
        my $width = $q->param('maxwidth');
        Images::displayImage($dbt,$image_no,$height,$width);
    }
    if ($q->param("display_header") eq 'NO') {
        print $hbo->stdIncludes("blank_page_bottom"); 
    } else {
        print $hbo->stdIncludes($PAGE_BOTTOM); 
    }
}
## END Image stuff
##############


##############
## Ecology stuff
sub startStartEcologyTaphonomySearch{
    my $goal='ecotaph';
    my $page_title ='Search for the taxon you want to describe';

    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('search_taxon_form',[$page_title,'submitTaxonSearch',$goal],['page_title','action','goal']);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub startStartEcologyVertebrateSearch{
    my $goal='ecovert';
    my $page_title ='Search for the taxon you want to describe';

    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('search_taxon_form',[$page_title,'submitTaxonSearch',$goal],['page_title','action','goal']);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub startPopulateEcologyForm	{
    print $hbo->stdIncludes($PAGE_TOP);
	Ecology::populateEcologyForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub startProcessEcologyForm	{
    print $hbo->stdIncludes($PAGE_TOP);
	Ecology::processEcologyForm($dbt, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
## END Ecology stuff
##############

##############
## Specimen measurement stuff
sub displaySpecimenSearchForm	{
	print $hbo->stdIncludes($PAGE_TOP);
	if (!$s->get('reference_no'))	{
		$s->enqueue('action=displaySpecimenSearchForm');
		displaySearchRefs("You must choose a reference before adding measurements" );
		exit;
	}
	print $hbo->populateHTML('search_specimen_form',[],[]);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub submitSpecimenSearch{
    print $hbo->stdIncludes($PAGE_TOP);
    Measurement::submitSpecimenSearch($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displaySpecimenList {
    print $hbo->stdIncludes($PAGE_TOP);
    Measurement::displaySpecimenList($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub populateMeasurementForm{
    print $hbo->stdIncludes($PAGE_TOP);
    Measurement::populateMeasurementForm($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processMeasurementForm {
    print $hbo->stdIncludes($PAGE_TOP);
    Measurement::processMeasurementForm($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

## END Specimen measurement stuff
##############



##############
## Strata stuff
sub displayStrata {
    require Strata;
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    Strata::displayStrata($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displaySearchStrataForm {
    require Strata;
    print $hbo->stdIncludes($PAGE_TOP);
    Strata::displaySearchStrataForm($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}  

sub displaySearchStrataResults{
    require Strata;
    print $hbo->stdIncludes($PAGE_TOP);
    Strata::displaySearchStrataResults($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}  
## END Strata stuff
##############

##############
## PrintHierarchy stuff
sub classificationForm	{
	return if PBDBUtil::checkForBot();
	require PrintHierarchy;
	logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	PrintHierarchy::classificationForm($hbo, $s);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub classify	{
	return if PBDBUtil::checkForBot();
	PrintHierarchy::classify($dbt, $hbo, $s, $q);
}
## END PrintHierarchy stuff
##############

##############
## SanityCheck stuff
sub displaySanityForm	{
	my $error_message = shift;
	print $hbo->stdIncludes($PAGE_TOP);
	print $hbo->populateHTML('sanity_check_form',$error_message);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub startProcessSanityCheck	{
	return if PBDBUtil::checkForBot();
	require SanityCheck;
	logRequest($s,$q);
    
	print $hbo->stdIncludes($PAGE_TOP);
	SanityCheck::processSanityCheck($q, $dbt, $hbo, $s);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
## END SanityCheck stuff
##############

##############
## PAST stuff
sub PASTQueryForm {
    require PAST;
    print $hbo->stdIncludes($PAGE_TOP);
    PAST::queryForm($dbt,$q,$hbo,$s);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub PASTQuerySubmit {
    require PAST;
    print $hbo->stdIncludes($PAGE_TOP);
    PAST::querySubmit($dbt,$q,$hbo,$s);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}
## End PAST stuff
##############


sub displayOccurrenceAddEdit {
	my $dbh = $dbt->dbh;

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if (!$s->isDBMember()) {
		login( "Please log in first.",'displayOccurrenceAddEdit');
		exit;
	} 
	if (! $s->get('reference_no')) {
		$s->enqueue($q->query_string());
		displaySearchRefs("Please select a reference first"); 
		exit;
	} 

	my $collection_no = $q->param($COLLECTION_NO);
	# No collection no is passed in, search for one
	if ( ! $collection_no ) { 
		$q->param('type'=>'edit_occurrence');
		displaySearchColls();
		exit;
	}

	# Grab the collection name for display purposes JA 1.10.02
	my $sql;
	if ( $DB ne "eco" )	{
		$sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
	} else	{
		$sql = "SELECT inventory_name FROM inventories WHERE inventory_no=$collection_no";
	}
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $collection_name = ${$sth->fetchrow_arrayref()}[0];
	$sth->finish();

	print $hbo->stdIncludes( $PAGE_TOP );

	# get the occurrences right away because we need to make sure there
	#  aren't too many to be displayed
	if ( $DB ne "eco" )	{
		$sql = "SELECT * FROM occurrences WHERE collection_no=$collection_no ORDER BY occurrence_no ASC";
	} else	{
		$sql = "SELECT * FROM inventory_entries WHERE inventory_no=$collection_no ORDER BY entry_no ASC";
	}
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my $p = Permissions->new($s,$dbt);
	my @all_data = $p->getReadWriteRowsForEdit($sth);

	# first check to see if there are too many rows to display, in which
	#  case display links going to different batches of occurrences and
	#  then bomb out JA 26.7.04
	# don't do this if the user already has gone through one of those
	#  links, so rows_to_display has a useable value
	if ( $#all_data > 49 && $q->param("rows_to_display") !~ / to / )	{
		print "<center><p class=\"pageTitle\">Please select the rows you wish to edit</p></center>\n\n";
		print "<center>\n";
		print "<table><tr><td>\n";
		print "<ul>\n";
        my ($startofblock,$endofblock);
		for my $rowset ( 1..100 )	{
			$endofblock = $rowset * 50;
			$startofblock = $endofblock - 49;
			if ( $#all_data >= $endofblock )	{
				print "<li><a href=\"$WRITE_URL?action=displayOccurrenceAddEdit&collection_no=$collection_no&rows_to_display=$startofblock+to+$endofblock\">Rows <b>$startofblock</b> to <b>$endofblock</b></a>\n";
			}
			if ( $#all_data < $endofblock + 50 )	{
				$startofblock = $endofblock + 1;
				$endofblock = $#all_data + 1;
				print "<li><a href=\"$WRITE_URL?action=displayOccurrenceAddEdit&collection_no=$collection_no&rows_to_display=$startofblock+to+$endofblock\">Rows <b>$startofblock</b> to <b>$endofblock</b></a>\n";
				last;
			}
		}
		print "</ul>\n\n";
		print "</td></tr></table>\n";
		print "</center>\n";
		print $hbo->stdIncludes( $PAGE_BOTTOM );
		exit;
	}

	# which rows should be displayed?
	my $firstrow = 0;
	my $lastrow = $#all_data;
	if ( $q->param("rows_to_display") =~ / to / )	{
		($firstrow,$lastrow) = split / to /,$q->param("rows_to_display");
		$firstrow--;
		$lastrow--;
	}

	my %pref = $s->getPreferences();
	if ($DB ne "eco" )	{
		print $hbo->populateHTML('js_occurrence_checkform');
	} else	{
		print $hbo->populateHTML('js_eco_entry_checkform');
	}

	print qq|<form method=post action="$WRITE_URL" onSubmit='return checkForm();'>\n|;
	print qq|<input name="action" value="processEditOccurrences" type=hidden>\n|;
	print qq|<input name="list_collection_no" value="$collection_no" type=hidden>\n|;

	my @optional = ('editable_collection_no','subgenera','genus_and_species_only','abundances','plant_organs');
	my $header_vars = {
		'collection_no'=>$collection_no,
		'collection_name'=>$collection_name
	};
	if ( $DB eq "eco" )	{
		@optional = ('counts');
		$header_vars = {
			'inventory_no'=>$collection_no,
			'collection_name'=>$collection_name,
			'counts'=>'yes'
		};
		$pref{'counts'} = 'yes';
	}
	$header_vars->{$_} = $pref{$_} for (@optional);

	print $hbo->populateHTML('occurrence_header_row', $header_vars);

    # main loop
    # each record is represented as a hash
    my $gray_counter = 0;
    foreach my $all_data_index ($firstrow..$lastrow){
    	my $occ_row = $all_data[$all_data_index];
		# This essentially empty reid_no is necessary as 'padding' so that
		# any actual reid number (see while loop below) will line up with 
		# its row in the form, and ALL rows (reids or not) will be processed
		# properly by processEditOccurrences(), below.
        $occ_row->{'reid_no'} = '0';
        $occ_row = formatTaxonNameInput($occ_row);
   
        # Copy over optional fields;
        $occ_row->{$_} = $pref{$_} for (@optional);

        # Read Only
        my $occ_read_only = ($occ_row->{'writeable'} == 0) ? "all" : ""; 
        $occ_row->{'darkList'} = ($occ_read_only eq 'all' && $gray_counter%2 == 0) ? "darkList" : "";
        #    print qq|<input type=hidden name="row_token" value="row_token">\n|;
        print $hbo->populateHTML("occurrence_edit_row", $occ_row, [$occ_read_only]);

        my @reid_rows;
        if ( $DB ne "eco" )	{
            my $sql = "SELECT * FROM reidentifications WHERE occurrence_no=" .  $occ_row->{'occurrence_no'};
            @reid_rows = @{$dbt->getData($sql)};
        }
        foreach my $re_row (@reid_rows) {
            $re_row = formatTaxonNameInput($re_row);
            # Copy over optional fields;
            $re_row->{$_} = $pref{$_} for (@optional);

            # Read Only
            my $re_read_only = $occ_read_only;
            $re_row->{'darkList'} = $occ_row->{'darkList'};
            
            my $reidHTML = $hbo->populateHTML("reid_edit_row", $re_row, [$re_read_only]);
            # Strip away abundance widgets (crucial because reIDs never may
            #  have abundances) JA 30.7.02
#            $reidHTML =~ s/<td><input id="abund_value"(.*?)><\/td>/<td><input type=hidden name="abund_value"><\/td>/;
#            $reidHTML =~ s/<td><select id="abund_unit"(.*?)>(.*?)<\/select><\/td>/<td><input type=hidden name="abund_unit"><\/td>/;
#            $reidHTML =~ s/<td align=right><select name="genus_reso">/<td align=right><nobr><b>reID<\/b><select name="genus_reso">/;
#            $reidHTML =~ s/<td /<td class=tiny /g;
            # The first one needs to be " = (species ..."
#            $reidHTML =~ s/<div id="genus_reso">/<div class=tiny>= /;
#            $reidHTML =~ s//<input class=tiny /g;
#            $reidHTML =~ s/<select /<select class=tiny /g;
            print $reidHTML;
        }
        $gray_counter++;
    }

	# Extra rows for adding
	my $blank;
	$blank = {
		'collection_no'=>$collection_no,
		'reference_no'=>$s->get('reference_no'),
		'occurrence_no'=>-1,
		'taxon_name'=>$pref{'species_name'}
	};
	if ( $DB eq "eco" )	{
		$blank = {
			'inventory_no'=>$collection_no,
			'reference_no'=>$s->get('reference_no'),
			'entry_no'=>-1,
			'counts'=>'yes'
		};
	}
	if ( $blank->{'species_name'} eq " " )	{
		$blank->{'species_name'} = "";
	}
    

	# Copy over optional fields;
	$blank->{$_} = $pref{$_} for (@optional,'species_name');
        
	# Figure out the number of blanks to print
	my $blanks = $pref{'blanks'} || 10;

	for ( my $i = 0; $i<$blanks ; $i++) {
#		print qq|<input type=hidden name="row_token" value="row_token">\n|;
		print $hbo->populateHTML("occurrence_entry_row", $blank);
	}

	print "</table><br>\n";
	print "<p>Delete entries by erasing the taxon name.</p>\n";
	print qq|<center><p><input type=submit value="Save changes">|;
	printf " to collection %s's taxonomic list</p></center>\n",$collection_no;
	print "</div>\n\n</form>\n\n";

	print $hbo->stdIncludes( $PAGE_BOTTOM );
} 

# JA 5.7.07
sub formatTaxonNameInput	{
    my $occ_row = shift;

    if ( $occ_row->{'genus_reso'} )	{
        if ( $occ_row->{'genus_reso'} =~ /"/ )	{
            $occ_row->{'taxon_name'} = '"';
        } elsif ( $occ_row->{'genus_reso'} =~ /informal/ )	{
            $occ_row->{'taxon_name'} = '<';
        } else	{
            $occ_row->{'taxon_name'} = $occ_row->{'genus_reso'} . " ";
        }
    }
    $occ_row->{'taxon_name'} .=  $occ_row->{'genus_name'};
    if ( $occ_row->{'genus_reso'} =~ /"/ )	{
        $occ_row->{'taxon_name'} .= '"';
    } elsif ( $occ_row->{'genus_reso'} =~ /informal/ )	{
        $occ_row->{'taxon_name'} .= '>';
    }
    if ( $occ_row->{'subgenus_name'} )	{
        $occ_row->{'taxon_name'} .=  " ";
        if ( $occ_row->{'subgenus_reso'} )	{
            if ( $occ_row->{'subgenus_reso'} =~ /"/ )	{
                $occ_row->{'subgenus_name'} = '"' . $occ_row->{'subgenus_name'} . '"';
            } elsif ( $occ_row->{'subgenus_reso'} =~ /informal/ )	{
                $occ_row->{'subgenus_name'} = '<' . $occ_row->{'subgenus_name'} . '>';
            } else	{
                $occ_row->{'taxon_name'} .= $occ_row->{'subgenus_reso'} . " ";
            }
        }
        $occ_row->{'taxon_name'} .=  "(" . $occ_row->{'subgenus_name'} . ")";
    }
    $occ_row->{'taxon_name'} .=  " ";
    if ( $occ_row->{'species_reso'} )	{
        if ( $occ_row->{'species_reso'} =~ /"/ )	{
            $occ_row->{'species_name'} = '"' . $occ_row->{'species_name'};
        } elsif ( $occ_row->{'species_reso'} =~ /informal/ )	{
            $occ_row->{'species_name'} = '<' . $occ_row->{'species_name'};
        } else	{
            $occ_row->{'taxon_name'} .= $occ_row->{'species_reso'} . " ";
        }
    }
    $occ_row->{'taxon_name'} .=  $occ_row->{'species_name'};
    if ( $occ_row->{'species_reso'} =~ /"/ )	{
        $occ_row->{'taxon_name'} .= '"';
    } elsif ( $occ_row->{'species_reso'} =~ /informal/ )	{
        $occ_row->{'taxon_name'} .= '>';
    }

    return ($occ_row);
}

#
# Sanity checks/error checks?
# Hit enter, capture and do addrow
#
sub displayOccurrenceTable {
    my @all_collections = @{$_[0]};
	# Have to be logged in
	if (!$s->isDBMember()) {
		login( "Please log in first.",'displayOccurrenceTable' );
		exit;
	}
	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		$s->enqueue($q->query_string());
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}	

    # Get modifier as well
    my $p = new Permissions($s,$dbt);
    my $can_modify = $p->getModifierList();
    $can_modify->{$s->get('authorizer_no')} = 1;

    my $lower_limit = int($q->param("offset")) || 0;
    my $limit = int($q->param("limit")) || 20;
    my $upper_limit = ($lower_limit + $limit);
    if ($upper_limit > @all_collections) {
        $upper_limit = @all_collections;
    }

    my @collections = map {int} @all_collections[$lower_limit .. ($upper_limit-1)];
    my @other_colls = ();
    if (0 < $lower_limit) {
        @other_colls = map {int} @all_collections[0 .. $lower_limit-1];
    }

    my %taxon_names = ();
    my %taxon_nos = ();

    my $sql = "SELECT 0 reid_no, o.occurrence_no, o.collection_no, o.reference_no, o.authorizer_no, p1.name authorizer, o.taxon_no, o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name, o.abund_value, o.abund_unit FROM occurrences o LEFT JOIN person p1 ON p1.person_no=o.authorizer_no WHERE collection_no IN (".join(",",@collections).")";
    my @occs = @{$dbt->getData($sql)};

    if (@occs < @collections && @other_colls) {
        my $sql = "SELECT 0 reid_no, o.occurrence_no, o.collection_no, o.reference_no, o.authorizer_no, p1.name authorizer, o.taxon_no, o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name, o.abund_value, o.abund_unit FROM occurrences o LEFT JOIN person p1 ON p1.person_no=o.authorizer_no WHERE collection_no IN (".join(",",@other_colls).")";
        push @occs, @{$dbt->getData($sql)};

    }    

    my %count_by_abund_unit;
    my %min_occ_no;
    foreach my $row (@occs) {
        my %hash = %$row;
        # Make sure the resos come last since we don't want that to affect hte sort below
        # DON'T change the ordering of taxon_key, this ordering has to match up with the javascript and split functions
        # throughout this whoel process
        my $taxon_key = join("-_",@hash{"genus_name","subgenus_name","species_name","genus_reso","subgenus_reso","species_reso"});
        $taxon_names{$taxon_key}{$row->{'collection_no'}} = $row;
        if (!$min_occ_no{$taxon_key} || $row->{occurrence_no} < $min_occ_no{$taxon_key}) {
            $min_occ_no{$taxon_key} = $row->{occurrence_no};
        }
        
        $taxon_nos{$taxon_key}{$row->{'taxon_no'}} = 1 if ($row->{'taxon_no'} > 0);
        $count_by_abund_unit{$row->{'abund_unit'}}++ if ($row->{'abund_unit'});
   }

    # This takes advantage of a bug in IE 6 in which absolutely positioned elements get treated
    # as fixed position elements when height:100% and overflow-y:auto are added to the body
    # Note that the browser can't be rendering in "quirks" mode so the doctype must be XHTML
    # (use a different header)
    my $extra_header = <<EOF;
<script src="/JavaScripts/occurrence_table.js" type="text/javascript" language="JavaScript"></script>
<style type="text/css">
body {
    margin:10px; 
    top:0px; 
    left:10px; 
    padding:0 0 0 0; 
    border:0; 
    height:100%; 
    overflow-y:auto; 
}
#occurrencesTableHeader {
    display:block; 
    top:0px; 
    left:10px; 
    position:fixed; 
    border-bottom:2px solid gray; 
    padding:0px; 
    text-align:center; 
    background-color:#FFFFFF;
    z-index: 9;
}
#occurrencesTableHeader th,#occurrencesTableHeader td {
    border-right: 1px solid gray;
    border-bottom: 1px solid gray; 
}
* html #occurrencesTableHeader {position:absolute;}
</style>
<!--[if lte IE 6]>
   <style type="text/css">
   /*<![CDATA[*/ 
html {overflow-x:auto; overflow-y:hidden;}
   /*]]>*/
   </style>
<![endif]-->
EOF
    print $hbo->populateHTML('blank_page_top',{'extra_header'=>$extra_header});
    print qq|<form method="post" action="$WRITE_URL" onSubmit="return handleSubmit();">|;
    print '<input type="hidden" name="action" value="processOccurrenceTable" />';
    # this field is read by the javascript but not used otherwise
    print qq|<input type="hidden" name="reference_no" value="$reference_no" />|;

    foreach my $collection_no (@collections) {
        print qq|<input type="hidden" name="collection_nos" value="$collection_no" />\n|;
    }

    # Fixed position header
    # We're make an assumption here, that there will generally only be one abundance unit for the page
    # and everything gets synced to that one -- we prepopulate the form with that abundance unit, or if
    # where no abundance unit (a new sheet or only presences and not abundances records), then we
    # default to specimens
    my $selected_abund_unit = 'specimens';
    my $max_count = 1;
    while(my ($abund_unit,$count) = each %count_by_abund_unit) {
        if ($count > $max_count && $abund_unit) {
            $max_count = $count;
            $selected_abund_unit = $abund_unit;
        }
    }
    my $abund_select = $hbo->htmlSelect('abund_unit',$hbo->getKeysValues('abund_unit'),$selected_abund_unit,'class="small"');
    my $reference = "$reference_no (".Reference::formatShortRef($dbt,$reference_no).")";
    print '<div id="occurrencesTableHeader">';
    print '<table border=0 cellpadding=0 cellspacing=0>'."\n";
    print '<tr>';
    print '<td valign="bottom"><div class="fixedLabel">'.
          qq|<div class="small" align="left">Please see the <a href="#" onClick="tipsPopup('/public/tips/occurrence_table_tips.html');">tip sheet</a></div><br />|.
          '<div align="left" style="height: 160px; overflow: hidden;" class="small">'.
          '<b>New cells:</b><br />'.
          '&nbsp;Reference: '.$reference."<br />".
          '&nbsp;Abund. unit: '.$abund_select."<br />".
          '<b>Current cell: </b><br />'.
          '<div id="cell_info"></div>'.
          '</div>'.
          '<input type="submit" name="submit" value="Submit table" /><br /><br />'.
          '</div></td>';
    foreach my $collection_no (@collections) {
        my $collection_name = escapeHTML(generateCollectionLabel($collection_no));
        print '<td class="addBorders"><div class="fixedColumn">'.
            qq|<a target="_blank" href="$READ_URL?action=basicCollectionSearch&amp;collection_no=$collection_no"><img border="0" src="/public/collection_labels/$collection_no.png" alt="$collection_name"/></a>|.
            "</div></td>";
    }
    print "</tr>\n";
    print "</table></div>";

 
    print '<div style="height: 236px">&nbsp;</div>';
    print '<table border=0 cellpadding=0 cellspacing=0 id="occurrencesTable">'."\n";
    my @sorted_names;
    if ($q->param('taxa_order') eq 'alphabetical') {
        @sorted_names = sort keys %taxon_names;
    } else {
        @sorted_names = sort {$min_occ_no{$a} <=> $min_occ_no{$b}} keys %taxon_names;
    }
    for(my $i=0;$i<@sorted_names;$i++) {
        my $taxon_key = $sorted_names[$i];
        my @taxon_nos = (); 
        if (exists ($taxon_nos{$taxon_key})) {
            @taxon_nos = keys %{$taxon_nos{$taxon_key}} ;
        }
        my %hash = ();
        @hash{"genus_name","subgenus_name","species_name","genus_reso","subgenus_reso","species_reso"} = split("-_",$taxon_key);
        my $show_name = Collection::formatOccurrenceTaxonName(\%hash);
        $show_name =~ s/<a href/<a target="_blank" href/;
        my $class = ($i % 2 == 0) ? 'class="darkList"' : '';
        print '<tr '.$class.'><td class="fixedLabel"><div class="fixedLabel">'.
            $show_name.
            qq|<input type="hidden" name="row_num" value="$i" />|.
            qq|<input type="hidden" name="taxon_key_$i" value="|.escapeHTML($taxon_key).qq|" />|;
        foreach my $taxon_no (@taxon_nos) {
           print qq|<input type="hidden" name="taxon_no_$i" value="$taxon_no" />|; 
        }
        print "</div></td>";
        $class = ($i % 2 == 0) ? 'fixedInputDark' : 'fixedInput';
        for (my $j=0;$j<@collections;$j++) {
            my $collection_no = $collections[$j];
            my $occ = $taxon_names{$taxon_key}{$collections[$j]};
            my ($abund_value,$abund_unit,$key_type,$key_value,$occ_reference_no,$readonly,$authorizer);
            $readonly = 0;
            if ($occ) {
                if ($occ->{'abund_value'}) {
                    $abund_value = $occ->{'abund_value'};
                } else {
                    $abund_value = "x";
                }
                if ($occ->{'reid_no'}) {
                    $key_type = "reid_no";
                    $key_value = "$occ->{reid_no}";
                } else {
                    $key_type = "occurrence_no";
                    $key_value = "$occ->{occurrence_no}";
                }
#                $abund_unit = $occ->{'abund_unit'};
                $occ_reference_no = $occ->{'reference_no'};
                if (!$can_modify->{$occ->{'authorizer_no'}}) {
                    $readonly = 1;
                }
                $authorizer=$occ->{'authorizer'}
            } else {
#                $abund_unit = "DEFAULT";
                $key_type = "occurrence_no";
                $key_value = "-1";
                $occ_reference_no = $reference_no;
            }
          
            my $style="";
            my $editCellJS = "editCell($i,$collection_no); ";
            if ($readonly) {
                $style = 'style="color: red;"';
                $editCellJS = "";
            }
            my $esc_show_name = escapeHTML($show_name);
            # The span is necessary to act as a container and prevent wrapping
            # The &nbsp; fixes a Safari bug where the onClick doesn't trigger unless the TD has somethiing in it

            print qq|<td class="fixedColumn" onClick="cellInfo($i,$collection_no,$occ_reference_no,$readonly,'$authorizer');$editCellJS"><div class="fixedColumn"><span class="fixedSpan" id="dummy_${i}_${collection_no}" $style>$abund_value &nbsp;|;
            print qq|<input type="hidden" id="abund_value_${i}_${collection_no}" name="abund_value_${i}_${collection_no}" size="4" value="$abund_value" class="$class" $style /></span>|;
            print qq|<input type="hidden" id="${key_type}_${i}_${collection_no}" name="${key_type}_${i}_${collection_no}" value="$key_value"/>|;
            print qq|</div></td>\n|;
                  
        }
        print "</tr>\n";
    }
    print "</table>";

    my %prefs = $s->getPreferences();
    # Can dynamically add rows using javascript that modified the DOM -- see occurrence_table.js
    print "<table>";
    print '<tr><th></th><th class="small">Genus</th>';
    if ($prefs{'subgenera'} || $prefs{'genus_and_species_only'}) {
        print '<th></th><th class="small">Subgenus</th>';
    }
    print '<th></th><th class="small">Species</th></tr>';
    print "<tr>".
        '<td>'.$hbo->htmlSelect("genus_reso",$hbo->getKeysValues('genus_reso'),'','class="small"').'</td>'.
        '<td><input name="genus_name" class="small" /></td>';
    if ($prefs{'subgenera'} || $prefs{'genus_and_species_only'}) {
        print '<td>'.$hbo->htmlSelect("subgenus_reso",$hbo->getKeysValues('subgenus_reso'),'','class="small"').'</td>'.
        '<td><input name="subgenus_name" class="small" /></td>';
    }
    print '<td>'.$hbo->htmlSelect("species_reso",$hbo->getKeysValues('species_reso'),'','class="small"').'</td>'.
        '<td><input name="species_name" class="small" value="'.$prefs{species_name}.'" /></td>'.
        '</tr><tr>'.
        '<td colspan=6 align=right><input type="button" name="addRow" value="Add row" onClick="insertOccurrenceRow();" /></td>'.
        '</tr>';
    print "</table>";

    print "<br /><br />";

    print '<div align="center"><div style="width: 640px">';
    if (@all_collections > @collections) {
        print "<b>";
        print "Showing collections ".($lower_limit + 1)." to $upper_limit of ".scalar(@all_collections).".";
        if (@all_collections > $upper_limit) {
            my $query = "offset=".($upper_limit);
            foreach my $p ($q->param()) {
                if ($p ne 'offset' &&  $p ne 'next_page_link') {
                    $query .= "&amp;$p=".$q->param($p);
                }
            }
            my $remaining = ($limit + $upper_limit >= @all_collections) ? (@all_collections - $upper_limit) : $limit;
            my $verb = ($limit + $upper_limit >= @all_collections) ? "last" : "next";
            if ($remaining > 1) {
                $remaining= "$remaining collections";
            } else {
                $remaining = "collection";
            }
            print qq|<a href="$WRITE_URL?$query"> Get $verb $remaining</a>.|;

            # We save this so we can go to the next page easily on form submission
            my $next_page_link = uri_escape(qq|<b><a href="$WRITE_URL?$query"> Edit $verb $remaining</a></b>|);
            print qq|<input type="hidden" name="next_page_link" value="$next_page_link">|;
        }
        print "</b>";
    }
    print '</div></div>';
    print "</form>";
    print "<br /><br />";

    print $hbo->stdIncludes('blank_page_bottom');
}

# JA 19-20.5.09
sub displayOccurrenceListForm	{

	my $dbh = $dbt->dbh;

	if (!$s->isDBMember()) {
		login( "Please log in first." );
		exit;
	}
	if (! $s->get('reference_no')) {
		$s->enqueue($q->query_string());
		displaySearchRefs("Please select a reference first"); 
		exit;
	}
 
	my %vars;
	my $collection_no = $q->param($COLLECTION_NO);
	my $sql = "(SELECT o.genus_reso,o.genus_name,o.species_reso,o.species_name FROM occurrences o LEFT JOIN reidentifications r ON o.occurrence_no=r.occurrence_no WHERE o.$COLLECTION_NO=$collection_no AND r.reid_no IS NULL) UNION (SELECT r.genus_reso,r.genus_name,r.species_reso,r.species_name FROM occurrences o LEFT JOIN reidentifications r ON o.occurrence_no=r.occurrence_no WHERE o.$COLLECTION_NO=$collection_no AND r.most_recent='YES') ORDER BY genus_name,species_name";
	if ( $DB eq "eco" )	{
		$sql = "SELECT genus_name,species_name FROM inventory_entries WHERE $COLLECTION_NO=$collection_no ORDER BY genus_name,species_name";
	}
	my @occs = @{$dbt->getData($sql)};

	if ( @occs )	{
		$vars{'old_occurrences'} = "You can only add occurrences with this form. The existing ones are: ";
		my @ids;
		for my $o ( @occs )	{
			$o->{'genus_reso'} =~ s/informal|"//;
			$o->{'species_reso'} =~ s/informal|"//;
			my ($gr,$gn,$sr,$sn) = ($o->{'genus_reso'},$o->{'genus_name'},$o->{'species_reso'},$o->{'species_name'});

			my $id = $gn;
			if ( $gr )	{
				$id = $gr." ".$id;
			}
			if ( $sr )	{
				$id .= " ".$sr;
			}
			$id .= " ".$sn;
			if ( $sn !~ /indet\./ )	{
				$id = "<i>".$id."</i>";
			}
			push @ids , $id;
		}
		$vars{'old_occurrences'} .= join(', ',@ids);
	}

	if ( $DB ne "eco" )	{
		my $sql = "SELECT collection_name FROM $COLLECTIONS WHERE $COLLECTION_NO=$collection_no";
		$vars{'collection_name'} = ${$dbt->getData($sql)}[0]->{'collection_name'};
	} else	{
		my $sql = "SELECT inventory_name FROM $COLLECTIONS WHERE $COLLECTION_NO=$collection_no";
		$vars{'collection_name'} = ${$dbt->getData($sql)}[0]->{'inventory_name'};
	}

	print $hbo->stdIncludes($PAGE_TOP);
	print $hbo->populateHTML('js_occurrence_checkform');

	print qq|<form method=post action="$WRITE_URL" onSubmit='return checkForm();'>\n|;
	$vars{$COLLECTION_NO} = $collection_no;
	$vars{'coll_or_inventory'} = $collection_no;
	$vars{'collection_no_field'} = $COLLECTION_NO;
	$vars{'collection_no_field2'} = $COLLECTION_NO;
	$vars{'list_collection_no'} = $collection_no;
	$vars{'reference_no'} = $s->get('reference_no');
	print $hbo->populateHTML('occurrence_list_form',\%vars);

	print $hbo->stdIncludes($PAGE_BOTTOM);

   
}

sub processOccurrenceTable {

    if (!$s->isDBMember()) {
        login( "Please log in first." );
        exit;
    }
   
    my @row_tokens = $q->param('row_num');
    my @collections = $q->param('collection_nos');
    my $collection_list = join(",",@collections);
    my $global_abund_unit = $q->param("abund_unit");
    my $session_ref = $s->get('reference_no');
    if (!$global_abund_unit) {
        print "ERROR: no abund_unit specified";
        die;
    }
    if (!$session_ref) {
        print "ERROR: no session reference";
        die;
    }
    my $p = new Permissions($s,$dbt);
    my $can_modify = $p->getModifierList();
    $can_modify->{$s->get('authorizer_no')} = 1;

    print $hbo->stdIncludes('std_page_top');
    print '<div align="center"><p class="pageTitle">Occurrence table entry results</p></div>';
    print qq|<form method="post" action="$WRITE_URL">|;
    print '<input type="hidden" name="action" value="startProcessReclassifyForm">';
    print '<div align="center"><table cellpadding=3 cellspacing=0 border=0>';
    my $changed_rows = 0;
    my $seen_homonyms = 0;
    foreach my $i (@row_tokens) {
        my $taxon_key = $q->param("taxon_key_$i");
        my @taxon_nos = $q->param("taxon_no_$i");
        my ($genus_name,$subgenus_name,$species_name,$genus_reso,$subgenus_reso,$species_reso) = split("-_",$taxon_key);
        my (@deleted,@updated,@inserted,@uneditable);
        my $total_occs = 0;
        
        my $taxon_no;
        my @homonyms = ();
        my $manual_resolve_homonyms = 0;
        if (@taxon_nos == 1) {
            # If taxon_nos == 1: good to go. Note that taxon_nos is derived from what actually exists already in the DB,
            #  so if theres a homonym but only one version of the name is used it'll just reuse that name.  Likewise
            $taxon_no = $taxon_nos[0];
        } elsif (@taxon_nos > 1) {
            # If taxon_nos > 1: then there are multiple versions of the same
            # name in the sheet. It would be bad to overwrite any taxons classification arbitrarily
            # so we have a link for the user to manually classify that taxon by setting $manual_resolve_homonyms
            #  non-homonyms may have no taxon_no set if its a new entry - do a lookup in that case.
            @homonyms= @taxon_nos;
            $manual_resolve_homonyms = 1;
        } elsif (@taxon_nos == 0) {
            # If taxon_nos < 1: This can be because the taxon is new or because there are multiple versions of the
            # name, none of which have been classified.  Give an option to classify if homonyms exist
            $taxon_no = Taxon::getBestClassification($dbt,$genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name);
            if (!$taxon_no) {
                my @matches = Taxon::getBestClassification($dbt,$genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name);
                if (@matches) {
                    @homonyms = map {$_->{'taxon_no'}} @matches;
                    $seen_homonyms++;
                } # Else doesn't exist in the DB
            }
        }
        my @occurrences = ();
        foreach my $collection_no (@collections) {
            my $abund_value = $q->param("abund_value_${i}_${collection_no}");
            my $abund_unit = $global_abund_unit;
            my $primary_key_value = $q->param("occurrence_no_${i}_${collection_no}");
            my $primary_key = "occurrence_no";
            my $table = 'occurrences';
            if ($primary_key !~ /^occurrence_no$|^reid_no$/) {
                print "ERROR: invalid primary key type";
                next;
            }

            my $in_form = ($abund_value !~ /^\s*$/) ? 1 : 0;
            my $in_db = ($primary_key_value > 0) ? 1 : 0;

            if (lc($abund_value) eq 'x') {
                $abund_value = '';
                $abund_unit = '';
            } 
            
            my $db_row;
            if ($in_db) {
                my $sql = "SELECT * FROM $table WHERE $primary_key=$primary_key_value";
                $db_row = ${$dbt->getData($sql)}[0];
                if (!$db_row) {
                    die "Can't find db row $table.$primary_key=$primary_key_value";
                }
            }

            my %record = (
                'collection_no'=>$collection_no,
                'abund_value'=>$abund_value,
                'abund_unit'=>$abund_unit,
                'genus_reso'=>$genus_reso,
                'genus_name'=>$genus_name,
                'subgenus_reso'=>$subgenus_reso,
                'subgenus_name'=>$subgenus_name,
                'species_reso'=>$species_reso,
                'species_name'=>$species_name
            );
            if ($taxon_no) {
                $record{'taxon_no'} = $taxon_no;
            }

            if (!$in_db) {
                $record{'reference_no'} = $session_ref;
            }

            if ($in_db) {
                my $authorizer_no = $db_row->{'authorizer_no'};
                unless ($can_modify->{$authorizer_no}) {
                    push @uneditable,$collection_no;
                    $total_occs++;
                    next;
                }
            }
        
            if ($in_form && $in_db) {
                # Do an update
                dbg("UPDATING TAXON:$taxon_key COLLECTION:$collection_no $table.$primary_key=$primary_key_value");
                dbg("Record:".DumpHash(\%record));
                my $result = $dbt->updateRecord($s,$table,$primary_key,$primary_key_value,\%record);
                if ($result > 0) { 
                    push @updated,$collection_no; 
                }
                push @occurrences, $primary_key_value;
                $total_occs++;
            } elsif ($in_form && !$in_db) {
                # Do an insert
                dbg("INSERTING TAXON:$taxon_key COLLECTION:$collection_no $table");
                dbg("Record:".DumpHash(\%record));
                my ($result,$occurrence_no) = $dbt->insertRecord($s,$table,\%record);
                push @inserted,$collection_no; 
                if ($result) {
                    push @occurrences, $occurrence_no;
                }
                $total_occs++;
                # Add secondary ref
                Collection::setSecondaryRef($dbt,$collection_no,$session_ref);
            } elsif (!$in_form && $in_db) {
                # Do a delete
                dbg("DELETING TAXON:$taxon_key COLLECTION:$collection_no $table.$primary_key=$primary_key_value");
                dbg("Record:".DumpHash(\%record));
                $dbt->deleteRecord($s,$table,$primary_key,$primary_key_value);
                push @deleted,$collection_no; 
            } 
        }

        my $taxon_name = Collection::formatOccurrenceTaxonName({
            'genus_name'=>$genus_name,
            'genus_reso'=>$genus_reso,
            'subgenus_name'=>$subgenus_reso,
            'subgenus_reso'=>$subgenus_name,
            'species_reso'=>$species_reso,
            'species_name'=>$species_name
        });
        
        my $classification_select = "";
        if ( @homonyms) {
            if ($manual_resolve_homonyms) {
            } else {
                my @taxon_nos = ("0+unclassified");
                my @descriptions = ("leave unclassified");
                foreach my $taxon_no (@homonyms) {
                    my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no},['taxon_no','taxon_rank','taxon_name','author1last','author2last','otherauthors','pubyr']);
                    my $authority = Taxon::formatTaxon($dbt,$t);
                    push @descriptions, $authority; 
                    push @taxon_nos, $taxon_no."+".$authority;
                }
                $classification_select .= qq|<input type="hidden" name="occurrence_list" value="|.join(",",@occurrences).qq|">|;
                $classification_select .= qq|<input type="hidden" name="old_taxon_no" value="0">|;
                $classification_select .= qq|<input type="hidden" name="occurrence_description" value="|.escapeHTML($taxon_name).qq|">|;
                $classification_select .= $hbo->htmlSelect('taxon_no',\@descriptions,\@taxon_nos);
            }
        }
        if (@inserted || @updated || @deleted || !$total_occs || $classification_select || $manual_resolve_homonyms) {
            my $row = "<tr><td>$taxon_name</td><td>$classification_select</td><td>";
            if (@inserted) {
                my $s = (@inserted == 1) ? "" : "s";
                $row .= "Added to ".scalar(@inserted)." collection$s. "; 
            }
            if (@updated) {
                my $s = (@updated == 1) ? "" : "s";
                $row .= "Updated in ".scalar(@updated)." collection$s. ";
            }
            if (@deleted) {
                my $s = (@deleted == 1) ? "" : "s";
                $row .= "Removed from ".scalar(@deleted)." collection$s. ";
            }
            if (!$total_occs) {
                if (@deleted) {
                    $row .= "All occurrences of this taxon were removed. ";
                } else {
                    $row .= "No occurrences of this taxon were entered. ";
                }
            } 
            if ($manual_resolve_homonyms) {
                my $simple_taxon_name = $genus_name;
                $simple_taxon_name .= " ($subgenus_name)" if ($subgenus_name);
                $simple_taxon_name .= " ".$species_name;
                $row .= qq|Multiple versions of this name exist and must be <a target="_new" href="$WRITE_URL?action=startDisplayOccurrenceReclassify&collection_list=$collection_list&taxon_name=$simple_taxon_name">manually classified</a>. |;
            }
            $row .= "</td></tr>";
            print $row;
            $changed_rows++;
        }
    }
    if (!$changed_rows) {
        print "<tr><td>No rows were changed</td></tr>";
    }
    if ($seen_homonyms) {
        print qq|<tr><td colspan="3" align="center"><br><input type="submit" name="submit" value="Classify taxa"></td></tr>|;
        print qq|<tr><td colspan="3">|.Debug::printWarnings(['Multiple versions of some names exist in the database.  Please select the version wanted and hit "Classify taxa"']).qq|</td></tr>|;
    }
    print "</table>";
    print "</div>";
    print "</form>";
    print '<div align="center"><p>';
    print qq|<b><a href="$WRITE_URL?action=displaySearchColls&type=occurrence_table">Edit more occurrences</a></b>|;
    if ($q->param('next_page_link')) {
        print " - ".uri_unescape($q->param("next_page_link"));
    }
    print '</p></div>';
    print $hbo->stdIncludes('std_page_bottom');
}

sub DumpHash { 
    my @k = sort keys %{$_[0]}; 
    my $t = ''; 
    $t .= "$_=$_[0]->{$_}," for @k;
    return $t;
}

sub generateCollectionLabel {
    my $collection_no = int(shift); 
    return unless $collection_no;

    require GD;
    my $sql = "SELECT collection_name FROM collections WHERE collection_no=".int($collection_no);
    my $collection_name = ${$dbt->getData($sql)}[0]->{'collection_name'};
    PBDBUtil::autoCreateDir("$HTML_DIR/public/collection_labels");
    my $file = $HTML_DIR."/public/collection_labels/$collection_no.png";
    my $txt = "#$collection_no: $collection_name";

    my $font= "$DATA_DIR/fonts/sapirsan.ttf";
    my $font_size = 10;
    my $x = $font_size+2;
    my $height = 240;
    my $y = $height-3;
    my $num_lines = 3;
    my $angle = 1.57079633;# Specified in radians = .5*pi

    my $width = ($font_size+1)*$num_lines+3;
    my $im = new GD::Image($width,$height,1);
    my $white = $im->colorAllocate(255,255,255); # Allocate background color first
    my $black = $im->colorAllocate(0,0,0);
    $im->transparent($white);
    $im->filledRectangle(0,0,$width-1,$height-1,$white);

    my @words = split(/[\s-]+/,$txt);
    my $line_count = 1;
    foreach my $word (@words) {
        # This first call to stringFT is to GD::Image - this doesn't draw anything
        # but instead gets the @bounds back quickly so we know whether or now to 
        # wrap to the next line
        my @bounds = GD::Image->stringFT($black,$font,$font_size,$angle,$x,$y,$word);
#        print "Bounds are: ".join(",",@bounds)." for $word<BR>";
        if ($bounds[3] < 0) {
            #bounds[3] is the top left y coordinate or some such. if its < 0, then this
            # strin gis running off the image so break to next line
            $x += $font_size + 1;
            last if ($line_count > $num_lines);
            $y = $height - 3;
            my @bounds = $im->stringFT($black,$font,$font_size,$angle,$x,$y,$word);
            $y = $bounds[3] - int($font_size/3);
        } else {
            my @bounds = $im->stringFT($black,$font,$font_size,$angle,$x,$y,$word);
            $y = $bounds[3] - int($font_size);
        }
    }

    open IMG,">$file";
    print IMG $im->png; 
    close IMG;
    return $collection_name;
}


# This function now handles inserting/updating occurrences, as well as inserting/updating reids
# Rewritten PS to be a bit clearer, handle deletions of occurrences, and use DBTransationManager
# for consistency/simplicity.
sub processEditOccurrences {
	my $dbh = $dbt->dbh;
	if (!$s->isDBMember()) {
		login( "Please log in first." );
		exit;
	}
                                
	# list of the number of rows to possibly update.
	my @rowTokens;

	# parse freeform all-in-one-textarea lists passed in by
	#  displayOccurrenceListForm JA 19-20.5.09
	my $collection_no;
	if ( $q->param('row_token') )	{
		@rowTokens = $q->param('row_token');
	} elsif ( $q->param('taxon_list') )	{
		my $taxon_list = $q->param('taxon_list');
		# collapse down multiple delimiters, if any
		$taxon_list =~ s/[^A-Za-z0-9 <>\.\"\?\*#\/][^A-Za-z0-9 <>\.\"\?\(\)\*#\/]/=/g;
		my @lines = split /[^A-Za-z0-9 <>\.\"\?\(\)\*#\/]/,$taxon_list;
		my (@names,@comments,@colls,@refs,@occs,@reids);
		for my $l ( 0..$#lines )	{
			if ( $lines[$l] !~ /[A-Za-z0-9]/ )	{
				next;
			}
			if ( $lines[$l] =~ /^[\*#\/]/ && $#names == $#comments + 1 )	{
				$lines[$l] =~ s/^[\*#\/]//g;
				push @comments , $lines[$l];
			} elsif ( $lines[$l] =~ /^[\*#\/]/ )	{
				$lines[$l] =~ s/^[\*#\/]//;
				$comments[$#comments] .= "\n".$lines[$l];
			} else	{
				push @names , $lines[$l];
				while ( $#names > $#comments + 1 )	{
					push @comments , "";
				}
			}
		}
		push @colls , $q->param($COLLECTION_NO) foreach @names;
		push @refs , $q->param('reference_no') foreach @names;
		push @rowTokens , "row_token" foreach @names;
		push @occs , -1 foreach @names;
		push @reids , -1 foreach @names;
		$q->param('taxon_name' => @names);
		$q->param('comments' => @comments);
		$q->param($COLLECTION_NO => @colls);
		$q->param('reference_no' => @refs);
		$q->param($OCCURRENCE_NO => @occs);
		$q->param('reid_no' => @reids);
	} else	{
	    $collection_no = $q->param($COLLECTION_NO);
        }

	# Get the names of all the fields coming in from the form.
	my @param_names = $q->param();

	# list of required fields
	my @required_fields = ($COLLECTION_NO, "taxon_name", "reference_no");
	my @warnings = ();
	my @occurrences = ();
	my @occurrences_to_delete = ();

        my @genera = ();
        my @subgenera = ();
        my @species = ();
        my @latin_names = ();
        my @resos = ("\?","aff\.","cf\.","ex gr\.","n\. gen\.","n\. subgen\.","n\. sp\.","sensu lato");

	my @matrix;

	# loop over all rows submitted from the form

	for (my $i = 0;$i < @rowTokens; $i++)	{

        # Flatten the table into a single row, for easy manipulation
        my %fields = ();
        foreach my $param (@param_names) {
            my @vars = $q->param($param);
            if (scalar(@vars) == 1) {
                $fields{$param} = $vars[0];
            } else {
                $fields{$param} = $vars[$i];
            }
        }

        my $rowno = $i + 1;

        # extract the genus, subgenus, and species names and resos
        #  JA 5.7.07
        if ( $fields{'taxon_name'} )	{
            my $name = $fields{'taxon_name'};

        # first some free passes for breaking the rules by putting stuff
        #  at the end
        # n. gen. n. sp. at the end
            if ( $name =~ /n\. gen\. n\. sp\.$/ )	{
                $name =~ s/n\. gen\. n\. sp\.$//;
                $fields{'genus_reso'} = "n. gen.";
                $fields{'species_reso'} = "n. sp.";
            }
        # n. sp. or sensu lato after a species name at the end
            elsif ( $name =~ / [a-z]+ (n\. sp\.|sensu lato)$/ )	{
                if ( $name =~ /sensu lato$/ )	{
                    $fields{'species_reso'} = "sensu lato";
                } else	{
                    $fields{'species_reso'} = "n. sp.";
                }
                $name =~ s/ (n\. sp\.|sensu lato)$//;
            }
        # a bad idea, but some users may put n. sp. before the species name
            elsif ( $name =~ / n\. sp\./ )	{
                $fields{'species_reso'} = "n. sp.";
                $name =~ s/ n\. sp\.//;
            }
        # users may want to enter n. sp. as a qualifier for a sp., in which
        #  case they will probably write out n. sp. followed by nothing
        # this tests for a genus or subgenus name immediately beforehand
            $name =~ s/([A-Z][a-z]+("|\)|"\)|))( n\. sp\.)$/$1 n. sp. sp./;

        # hack: stash the informals and replace them with dummy values
            my %informal;
            my $foo;
            if ( $name =~ /^</ )	{
                ($informal{'genus'},$foo) = split />/,$name;
                $informal{'genus'} =~ s/<//;
                $name =~ s/^<[^>]*> /Genus /;
            }
            if ( $name =~ / <.*> / )	{
                ($informal{'subgenus'},$foo) = split />/,$name;
                ($foo,$informal{'subgenus'}) = split /</,$informal{'subgenus'};
                $name =~ s/ <.*> / \(Subgenus\) /;
            }
            if ( $name =~ />$/ )	{
                ($foo,$informal{'species'}) = split /</,$name;
                $informal{'species'} =~ s/>//;
                $name =~ s/ <.*>/ species/;
            }
            $name =~ s/^ //;
            $name =~ s/ $//;
            my @words = split / /,$name;
            for my $reso ( @resos )	{
                if ( $words[0]." ".$words[1] eq $reso )	{
                    $fields{'genus_reso'} = $reso;
                    splice @words , 0 , 2;
                } elsif ( $words[0] eq $reso )	{
                    $fields{'genus_reso'} = shift @words;
                    last;
                }
            }
            $fields{'genus_name'} = shift @words;
            $fields{'species_name'} = pop @words;
            for my $reso ( @resos )	{
                if ( $words[$#words-1]." ".$words[$#words] eq $reso )	{
                    $fields{'species_reso'} = $reso;
                    splice @words , 0 , 2;
                }  elsif ( $words[$#words] eq $reso )	{
                    $fields{'species_reso'} = pop @words;
                    last;
                }
            }
            # there is either nothing left, or a subgenus
            if ( $#words > -1 )	{
                $fields{'subgenus_name'} = pop @words;
            }
            if ( $#words > -1 )	{
                for my $reso ( @resos )	{
                    if ( $words[0]." ".$words[1] eq $reso )	{
                        $fields{'subgenus_reso'} = $reso;
                        shift @words , 2;
                    } elsif ( $words[0] eq $reso )	{
                        $fields{'subgenus_reso'} = shift @words;
                        last;
                    }
                }
            }
            $fields{'subgenus_name'} =~ s/\(//;
            $fields{'subgenus_name'} =~ s/\)//;
            for my $f ( "genus","subgenus","species" )	{
                if ( $fields{$f.'_name'} =~ /"/ )	{
                    $fields{$f.'_reso'} = '"';
                    $fields{$f.'_name'} =~ s/"//g;
                }
                if ( $informal{$f} )	{
                    $fields{$f.'_name'} = $informal{$f};
                    $fields{$f.'_reso'} = 'informal';
                }
                $fields{$f.'_reso'} =~ s/\\//;
            }
            push @genera , $fields{'genus_name'};
            push @subgenera , $fields{'subgenus_name'};
            push @species , $fields{'species_name'};
            if ( $fields{'species_name'} =~ /^[a-z]*$/ )	{
                if ( $fields{'subgenus_name'} =~ /^[A-Z][a-z]*$/ )	{
                    push @latin_names , $fields{'genus_name'} ." (". $fields{'subgenus_name'} .") ". $fields{'species_name'};
                } else	{
                    push @latin_names , $fields{'genus_name'} ." ". $fields{'species_name'};
                }
            } else	{
                if ( $fields{'subgenus_name'} =~ /^[A-Z][a-z]*$/ )	{
                    push @latin_names , $fields{'genus_name'} ." (". $fields{'subgenus_name'} . ")";
                } else	{
                    push @latin_names , $fields{'genus_name'};
                }
            }
            $fields{'latin_name'} = $latin_names[$#latin_names];
        }


        if ( $fields{$COLLECTION_NO} > 0 )	{
            $collection_no = $fields{$COLLECTION_NO}
        }

	%{$matrix[$i]} = %fields;

	# end of first pass
	}

	# check for duplicates JA 2.4.08
	# this section replaces the old occurrence-by-occurrence check that
	#  used checkDuplicates; it's much faster and uses more lenient
	#  criteria because isolated duplicates are handled by the JavaScript
	my $sql ="SELECT genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name,taxon_no FROM $OCCURRENCES WHERE $COLLECTION_NO=" . $collection_no;
	if ( $DB eq "eco" )	{
		$sql ="SELECT genus_name,species_name,taxon_no FROM $OCCURRENCES WHERE $COLLECTION_NO=" . $collection_no;
	}
	my @occrefs = @{$dbt->getData($sql)};
	my %taxon_no;
	if ( $#occrefs > 0 )	{
		my $newrows;
		my %newrow;
		for (my $i = 0;$i < @rowTokens; $i++)	{
			if ( $matrix[$i]{'genus_name'} =~ /^[A-Z][a-z]*$/ && $matrix[$i]{$OCCURRENCE_NO} == -1 )	{
				$newrow{ $matrix[$i]{'genus_reso'} ." ". $matrix[$i]{'genus_name'} ." ". $matrix[$i]{'subgenus_reso'} ." ". $matrix[$i]{'subgenus_name'} ." ". $matrix[$i]{'species_reso'} ." ". $matrix[$i]{'species_name'} }++;
				$newrows++;
			}
		}
		if ( $newrows > 0 )	{
			my $dupes;
			for my $or ( @occrefs )	{
				if ( $newrow{ $or->{'genus_reso'} ." ". $or->{'genus_name'} ." ". $or->{'subgenus_reso'} ." ". $or->{'subgenus_name'} ." ". $or->{'species_reso'} ." ". $or->{'species_name'} } > 0 )	{
					$dupes++;
				}
			}
			if ( $newrows == $dupes && $newrows == 1 )	{
				push @warnings , "Nothing was entered or updated because the new occurrence was a duplicate";
				@rowTokens = ();
			} elsif ( $newrows == $dupes )	{
				push @warnings , "Nothing was entered or updated because all the new records were duplicates";
				@rowTokens = ();
			} elsif ( $dupes >= 3 )	{
				push @warnings , "Nothing was entered or updated because there were too many duplicate entries";
				@rowTokens = ();
			}
		}
		# while we're at it, store the taxon_no JA 20.7.08
		# do this here and not earlier because taxon_no is not
		#  stored in the entry form
		for my $or ( @occrefs )	{
			if ( $or->{'taxon_no'} > 0 && $or->{'genus_reso'} !~ /informal/ )	{
				my $latin_name;
				if ( $or->{'species_name'} =~ /^[a-z]*$/ && $or->{'species_reso'} !~ /informal/ )	{
					if ( $or->{'subgenus_name'} =~ /^[A-Z][a-z]*$/ && $or->{'subgenus_reso'} !~ /informal/ )	{
						$latin_name = $or->{'genus_name'} ." (". $or->{'subgenus_name'} .") ". $or->{'species_name'};
					} else	{
						$latin_name = $or->{'genus_name'} ." ". $or->{'species_name'};
					}
				} else	{
					if ( $or->{'subgenus_name'} =~ /^[A-Z][a-z]*$/ && $or->{'subgenus_reso'} !~ /informal/ )	{
						$latin_name = $or->{'genus_name'} ." (". $or->{'subgenus_name'} . ")";
					} else	{
						$latin_name = $or->{'genus_name'};
					}
				}
				$taxon_no{$latin_name} = $or->{'taxon_no'};
			}
		}
	}

	# get as many taxon numbers as possible at once JA 2.4.08
	# this greatly speeds things up because we now only need to use
	#  getBestClassification as a last resort
	my $sql = "SELECT taxon_name,taxon_no,count(*) c FROM authorities WHERE taxon_name IN ('" . join('\',\'',@latin_names) . "') GROUP BY taxon_name";
	my @taxonrefs = @{$dbt->getData($sql)};
	for my $tr ( @taxonrefs )	{
		if ( $tr->{'c'} == 1 )	{
			$taxon_no{$tr->{'taxon_name'}} = $tr->{'taxon_no'};
		} elsif ( $tr->{'c'} > 1 )	{
			$taxon_no{$tr->{'taxon_name'}} = -1;
		}
	}

	# finally, check for n. sp. resos that appear to be duplicates and
	#  insert a type_locality number if there's no problem JA 14-15.12.08
	# this is not 100% because it will miss cases where a species was
	#  entered with "n. sp." using two different combinations
	# a couple of (fast harmless) checks in the section section are
	#  repeated here for simplicity
	my (@to_check,%dupe_colls);
	for (my $i = 0;$i < @rowTokens; $i++)	{
		my %fields = %{$matrix[$i]};
		if ( $fields{'genus_name'} eq "" && $fields{$OCCURRENCE_NO} < 1 )	{
			next;
		}
        	if ( $fields{'reference_no'} !~ /^\d+$/ && $fields{'genus'} =~ /[A-Za-z]/ )	{
            		next; 
        	}
        	if ( $fields{$COLLECTION_NO} !~ /^\d+$/ )	{
            		next; 
        	}
	# guess the taxon no by trying to find a single match for the name
	#  in the authorities table JA 1.4.04
	# see Reclassify.pm for a similar operation
	# only do this for non-informal taxa
	# done here and not in the last pass because we need the taxon_nos
		if ( $taxon_no{$fields{'latin_name'}} > 0 )	{
			$fields{'taxon_no'} = $taxon_no{$fields{'latin_name'}};
		} elsif ( $taxon_no{$fields{'latin_name'}} eq "" )	{
			$fields{'taxon_no'} = Taxon::getBestClassification($dbt,\%fields);
		} else	{
			$fields{'taxon_no'} = 0;
		}
		if ( $fields{'taxon_no'} > 0 && $fields{'species_reso'} eq "n. sp." )	{
			push @to_check , $fields{'taxon_no'};
		}
		%{$matrix[$i]} = %fields;
	}
	if ( @to_check )	{
		# pre-processing is faster than a join
		$sql = "SELECT taxon_no,taxon_name,type_locality FROM authorities WHERE taxon_no IN (".join(',',@to_check).") AND taxon_rank='species'";
		my @species = @{$dbt->getData($sql)};
		if ( @species )	{
			@to_check = ();
			push @to_check , $_->{'taxon_no'} foreach @species;
			$sql = "(SELECT taxon_no,collection_no FROM occurrences WHERE collection_no!=$collection_no AND taxon_no in (".join(',',@to_check).") AND species_reso='n. sp.') UNION (SELECT taxon_no,collection_no FROM reidentifications WHERE collection_no!=$collection_no AND taxon_no in (".join(',',@to_check).") AND species_reso='n. sp.')";
			if ( $DB eq "eco" )	{
				$sql = "SELECT taxon_no,inventory_no FROM inventory_entries WHERE inventory_no!=$collection_no AND taxon_no in (".join(',',@to_check).") AND species_reso='n. sp.'";
			}
			my @dupe_refs = @{$dbt->getData($sql)};
			if ( @dupe_refs )	{
				$dupe_colls{$_->{'taxon_no'}} .= ", ".$_->{$COLLECTION_NO} foreach @dupe_refs;
				for (my $i = 0;$i < @rowTokens; $i++)	{
					my %fields = %{$matrix[$i]};
					if ( ! $dupe_colls{$fields{'taxon_no'}} || ! $fields{'taxon_no'} )	{
						next;
					}
					$dupe_colls{$fields{'taxon_no'}} =~ s/^, //;
					if ( $dupe_colls{$fields{'taxon_no'}} =~ /^[0-9]+$/ )	{
						push @warnings, "<a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_no=$fields{'taxon_no'}\"><i>$fields{'genus_name'} $fields{'species_name'}</i></a> has already been marked as new in collection $dupe_colls{$fields{'taxon_no'}}, so it won't be recorded as such in this one";
					} elsif ( $dupe_colls{$fields{'taxon_no'}} =~ /, [0-9]/ )	{
						$dupe_colls{$fields{'taxon_no'}} =~ s/(, )([0-9]*)$/ and $2/;
						push @warnings, "<i>$fields{'genus_name'} $fields{'species_name'}</i> has already been marked as new in collections $dupe_colls{$fields{'taxon_no'}}, so it won't be recorded as such in this one";
					}
				}
			}
			my @to_update;
			for my $s ( @species )	{
				if ( ! $dupe_colls{$s->{'taxon_no'}} && $s->{'type_locality'} < 1 )	{
					push @to_update , $s->{'taxon_no'};
				} elsif ( ! $dupe_colls{$s->{'taxon_no'}} && $s->{'type_locality'} > 0 && $s->{'type_locality'} != $collection_no )	{
					push @warnings, "The type locality of <a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_no=$s->{'taxon_no'}\"><i>$s->{'taxon_name'}</i></a> has already been marked as new in collection $s->{'type_locality'}, which seems incorrect";
				}
			}
			if ( @to_update )	{
				$sql = "UPDATE authorities SET type_locality=$collection_no,modified=modified WHERE taxon_no IN (".join(',',@to_update).")";
				$dbh->do($sql);
				Taxon::propagateAuthorityInfo($dbt,$_) foreach @to_update;
			}
		}

	}

	# last pass, update/insert loop
	for (my $i = 0;$i < @rowTokens; $i++)	{

	my %fields = %{$matrix[$i]};
	my $rowno = $i + 1;

	if ( $fields{'genus_name'} eq "" && $fields{$OCCURRENCE_NO} < 1 )	{
		next;
	}

		# check that all required fields have a non empty value
        if ( $fields{'reference_no'} !~ /^\d+$/ && $fields{'genus'} =~ /[A-Za-z]/ )	{
            push @warnings, "There is no reference number for row $rowno, so it was skipped";
            next; 
        }
        if ( $fields{$COLLECTION_NO} !~ /^\d+$/ )	{
            push @warnings, "There is no collection number for row $rowno, so it was skipped";
            next; 
        }
	my $taxon_name = Collection::formatOccurrenceTaxonName(\%fields);

        if ($fields{'genus_name'} =~ /^\s*$/) {
            if ($fields{$OCCURRENCE_NO} =~ /^\d+$/ && $fields{'reid_no'} != -1) {
                # THIS IS AN UPDATE: CASE 1 or CASE 3. We will be deleting this record, 
                # Do nothing for now since this is handled below;
            } else {
                # THIS IS AN INSERT: CASE 2 or CASE 4. Just do nothing, this is a empty row
                next;  
            }
        } else {
            if (!Validation::validOccurrenceGenus($fields{'genus_reso'},$fields{'genus_name'})) {
                push @warnings, "The genus ($fields{'genus_name'}) in row $rowno is blank or improperly formatted, so it was skipped";
                next; 
            }
            if ($fields{'subgenus_name'} !~ /^\s*$/ && !Validation::validOccurrenceGenus($fields{'subgenus_reso'},$fields{'subgenus_name'})) {
                push @warnings, "The subgenus ($fields{'subgenus_name'}) in row $rowno is improperly formatted, so it was skipped";
                next; 
            }
            if ($fields{'species_name'} =~ /^\s*$/ || !Validation::validOccurrenceSpecies($fields{'species_reso'},$fields{'species_name'})) {
                push @warnings, "The species ($fields{'species_name'}) in row $rowno is blank or improperly formatted, so it was skipped";
                next; 
            }
        }

        if ($fields{$OCCURRENCE_NO} =~ /^\d+$/ && $fields{$OCCURRENCE_NO} > 0 &&
            (($fields{'reid_no'} =~ /^\d+$/ && $fields{'reid_no'} > 0) || ($fields{'reid_no'} == -1))) {
            # We're either updating or inserting a reidentification
            my $sql = "SELECT reference_no FROM $OCCURRENCES WHERE $OCCURRENCE_NO=$fields{$OCCURRENCE_NO}";
            my $occurrence_reference_no = ${$dbt->getData($sql)}[0]->{'reference_no'};
            if ($fields{'reference_no'} == $occurrence_reference_no) {
                push @warnings, "The occurrence of taxon $taxon_name in row $rowno and its reidentification have the same reference number";
                next;
            }
            # don't insert a new reID using a ref already used to reID
            #   this occurrence
            if ( $fields{'reid_no'} == -1 )	{
                my $sql = "SELECT reference_no FROM reidentifications WHERE occurrence_no=$fields{'occurrence_no'}";
                my @reidrows = @{$dbt->getData($sql)};
                my $isduplicate;
                for my $reidrow ( @reidrows )	{
                    if ($fields{'reference_no'} == $reidrow->{reference_no}) {
                        push @warnings, "This reference already has been used to reidentify the occurrence of taxon $taxon_name in row $rowno";
                       $isduplicate++;
                       next;
                    }
                }
                if ( $isduplicate > 0 )	{
                   next;
                }
            }
        }
        
		# CASE 1: UPDATE REID
        if ($fields{'reid_no'} =~ /^\d+$/ && $fields{'reid_no'} > 0 &&
            $fields{$OCCURRENCE_NO} =~ /^\d+$/ && $fields{$OCCURRENCE_NO} > 0) {

            # CASE 1a: Delete record
            if ($fields{'genus_name'} =~ /^\s*$/) {
                $dbt->deleteRecord($s,'reidentifications','reid_no',$fields{'reid_no'});
            } 
            # CASE 1b: Update record
            else {
                # ugly hack: make sure taxon_no doesn't change unless
                #  genus_name or species_name did JA 1.4.04
                my $old_row = ${$dbt->getData("SELECT * FROM reidentifications WHERE reid_no=$fields{'reid_no'}")}[0];
                die ("no reid for $fields{reid_no}") if (!$old_row);
                if ($old_row->{'genus_name'} eq $fields{'genus_name'} &&
                    $old_row->{'subgenus_name'} eq $fields{'subgenus_name'} &&
                    $old_row->{'species_name'} eq $fields{'species_name'}) {
                    delete $fields{'taxon_no'};
                }

                $dbt->updateRecord($s,'reidentifications','reid_no',$fields{'reid_no'},\%fields);

                if($old_row->{'reference_no'} != $fields{'reference_no'}) {
                    dbg("calling setSecondaryRef (updating ReID)<br>");
                    unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{$COLLECTION_NO}, $fields{'reference_no'})){
                           Collection::setSecondaryRef($dbt,$fields{$COLLECTION_NO},$fields{'reference_no'});
                    }
                }
            }
            setMostRecentReID($dbt,$fields{$OCCURRENCE_NO});
            push @occurrences, $fields{$OCCURRENCE_NO};
        }
		# CASE 2: NEW REID
		elsif ($fields{$OCCURRENCE_NO} =~ /^\d+$/ && $fields{$OCCURRENCE_NO} > 0 && 
               $fields{'reid_no'} == -1) {
            # Check for duplicates
            my @keys = ("genus_reso","genus_name","subgenus_reso","subgenus_name","species_reso","species_name",$OCCURRENCE_NO);
            my %vars = map{$_,$dbh->quote($_)} @fields{@keys};

            my $dupe_id = $dbt->checkDuplicates("reidentifications", \%vars);

            if ( $dupe_id ) {
                push @warnings, "Row ". ($i + 1) ." may be a duplicate";
            }
#            } elsif ( $return ) {
            $dbt->insertRecord($s,'reidentifications',\%fields);

            unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{$COLLECTION_NO}, $fields{'reference_no'}))	{
               Collection::setSecondaryRef($dbt,$fields{$COLLECTION_NO}, $fields{'reference_no'});
            }
#            }
            setMostRecentReID($dbt,$fields{$OCCURRENCE_NO});
            push @occurrences, $fields{$OCCURRENCE_NO};
        }
		
		# CASE 3: UPDATE OCCURRENCE
		elsif($fields{$OCCURRENCE_NO} =~ /^\d+$/ && $fields{$OCCURRENCE_NO} > 0) {
            # CASE 3a: Delete record
            if ($fields{'genus_name'} =~ /^\s*$/) {
                # We push this onto an array for later processing because we can't delete an occurrence
                # With reids attached to it, so we want to let any reids be deleted first
                my $old_row = ${$dbt->getData("SELECT * FROM $OCCURRENCES WHERE $OCCURRENCE_NO=$fields{$OCCURRENCE_NO}")}[0];
                push @occurrences_to_delete, [$fields{$OCCURRENCE_NO},Collection::formatOccurrenceTaxonName($old_row),$i];
            } 
            # CASE 3b: Update record
            else {
                # ugly hack: make sure taxon_no doesn't change unless
                #  genus_name or species_name did JA 1.4.04
                my $old_row = ${$dbt->getData("SELECT * FROM $OCCURRENCES WHERE $OCCURRENCE_NO=$fields{$OCCURRENCE_NO}")}[0];
                die ("no reid for $fields{reid_no}") if (!$old_row);
                if ($old_row->{'genus_name'} eq $fields{'genus_name'} &&
                    $old_row->{'subgenus_name'} eq $fields{'subgenus_name'} &&
                    $old_row->{'species_name'} eq $fields{'species_name'}) {
                    delete $fields{'taxon_no'};
                }

                $dbt->updateRecord($s,$OCCURRENCES,$OCCURRENCE_NO,$fields{$OCCURRENCE_NO},\%fields);

                if($old_row->{'reference_no'} != $fields{'reference_no'}) {
                    dbg("calling setSecondaryRef (updating occurrence)<br>");
                    unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{$COLLECTION_NO}, $fields{'reference_no'}))	{
                           Collection::setSecondaryRef($dbt,$fields{$COLLECTION_NO}, $fields{'reference_no'});
                    }
                }
            }
            push @occurrences, $fields{$OCCURRENCE_NO};
		} 
        # CASE 4: NEW OCCURRENCE
        elsif ($fields{$OCCURRENCE_NO} == -1) {
            # previously, a check here for duplicates generated error
            #  messages but (1) was incredibly slow and (2) apparently
            #  didn't work, so there is now a batch check above instead

            my ($result, $occurrence_no) = $dbt->insertRecord($s,$OCCURRENCES,\%fields);
            if ($result && $occurrence_no =~ /^\d+$/) {
                push @occurrences, $occurrence_no;
            }

            unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{$COLLECTION_NO}, $fields{'reference_no'}))	{
                   Collection::setSecondaryRef($dbt,$fields{$COLLECTION_NO}, $fields{'reference_no'});
            }
        }
    }

    # Now handle the actual deletion
    foreach my $o (@occurrences_to_delete) {
        my ($occurrence_no,$taxon_name,$line_no) = @{$o};
        if ( $DB ne "eco" )	{
            my $sql = "SELECT COUNT(*) c FROM reidentifications WHERE occurrence_no=$occurrence_no";
            my $reid_cnt = ${$dbt->getData($sql)}[0]->{'c'};
            $sql = "SELECT COUNT(*) c FROM specimens WHERE occurrence_no=$occurrence_no";
            my $measure_cnt = ${$dbt->getData($sql)}[0]->{'c'};
            if ($reid_cnt) {
                push @warnings, "'$taxon_name' on line $line_no can't be deleted because there are reidentifications based on it";
            }
            if ($measure_cnt) {
                push @warnings, "'$taxon_name' on line $line_no can't be deleted because there are measurements based on it";
            }
            if ($reid_cnt == 0 && $measure_cnt == 0) {
                $dbt->deleteRecord($s,$OCCURRENCES,$OCCURRENCE_NO,$occurrence_no);
            }
        } else	{
            $dbt->deleteRecord($s,$OCCURRENCES,$OCCURRENCE_NO,$occurrence_no);
        }
        
    }

	print $hbo->stdIncludes( $PAGE_TOP );

	print qq|<div align="center"><p class="large" style="margin-bottom: 1.5em;">|;
	my $sql = "SELECT collection_name AS coll FROM collections WHERE collection_no=$collection_no";
	my $coll_or_inventory = "collection";
	if ( $DB eq "eco" )	{
		$sql = "SELECT inventory_name AS coll FROM inventories WHERE inventory_no=$collection_no";
		$coll_or_inventory = "inventory";
	}
	print ${$dbt->getData($sql)}[0]->{'coll'};
	print "</p></div>\n\n";

	# Links to re-edit, etc
	my $links = "<div align=\"center\" style=\"padding-top: 1em;\">";
	if ($q->param('form_source') eq 'new_reids_form') {
        # suppress link if there is clearly nothing more to reidentify
        #  JA 3.8.07
        # this won't work if exactly ten occurrences have been displayed
        if ( $#rowTokens < 9 )	{
            $links .= "<a href=\"$WRITE_URL?action=displayCollResults&type=reid&taxon_name=".$q->param('search_taxon_name')."&collection_no=".$q->param("list_collection_no")."&page_no=".$q->param('page_no')."\"><nobr>Reidentify next 10 occurrences</nobr></a> - ";
        }
        $links .= "<a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\"><nobr>Reidentify different occurrences</nobr></a>";
    } else {
        if ($q->param('list_collection_no')) {
            my $collection_no = $q->param("list_collection_no");
            $links .= qq|<a href="$WRITE_URL?action=displayOccurrenceAddEdit&$COLLECTION_NO=$collection_no"><nobr>Edit this taxonomic list</nobr></a> - |;
            $links .= "<nobr><a href=\"$WRITE_URL?action=displayOccurrenceListForm&$COLLECTION_NO=$collection_no\">Paste in more names</a> - ";
            if ( $DB ne "eco" )	{
                $links .= "<a href=\"$WRITE_URL?action=startStartReclassifyOccurrences&$COLLECTION_NO=$collection_no\"><nobr>Reclassify these IDs</nobr></a> - ";
            	$links .= "<a href=\"$WRITE_URL?action=displayCollectionForm&$COLLECTION_NO=$collection_no\"><nobr>Edit the $coll_or_inventory record</nobr></a><br>";
            } else	{
            	$links .= "<a href=\"$WRITE_URL?action=inventoryForm&$COLLECTION_NO=$collection_no\"><nobr>Edit the $coll_or_inventory record</nobr></a><br>";
            }
        }
        $links .= "<nobr><a href=\"$WRITE_URL?action=displaySearchCollsForAdd&type=add\">Add</a> or ";
        $links .= "<a href=\"$WRITE_URL?action=displaySearchColls&type=edit\">edit another $coll_or_inventory</a> - </nobr>";
        $links .= "<nobr><a href=\"$WRITE_URL?action=displaySearchColls&type=edit_occurrence\">Add/edit</a>";
        if ( $DB ne "eco" )	{
            $links .= ", <nobr><a href=\"$WRITE_URL?action=displaySearchColls&type=occurrence_list\">paste in</a>, or ";
            $links .= "<a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\">reidentify IDs for a different $coll_or_inventory</a></nobr>";
        } else	{
            $links .= " or <nobr><a href=\"$WRITE_URL?action=displaySearchColls&type=occurrence_list\">paste in</a> IDs for a different $coll_or_inventory</nobr>";
        }
    }
    $links .= "</div><br>";

	# for identifying unrecognized (new to the db) genus/species names.
	# these are the new taxon names that the user is trying to enter, do this before insert
	my @new_genera = TypoChecker::newTaxonNames($dbt,\@genera,'genus_name');
	my @new_subgenera =  TypoChecker::newTaxonNames($dbt,\@subgenera,'subgenus_name');
	my @new_species =  TypoChecker::newTaxonNames($dbt,\@species,'species_name');

	print qq|<div style="padding-left: 1em; padding-right: 1em;>"|;

    my $return;
    if ($q->param('list_collection_no')) {
        my $collection_no = $q->param("list_collection_no");
        my $coll = ${$dbt->getData("SELECT $COLLECTION_NO,reference_no FROM $COLLECTIONS WHERE $COLLECTION_NO=$collection_no")}[0];
    	$return = Collection::buildTaxonomicList($dbt,$hbo,$s,{$COLLECTION_NO=>$collection_no, 'hide_reference_no'=>$coll->{'reference_no'},'new_genera'=>\@new_genera, 'new_subgenera'=>\@new_subgenera, 'new_species'=>\@new_species, 'do_reclassify'=>1, 'warnings'=>\@warnings, 'save_links'=>$links });
    } else {
    	$return = Collection::buildTaxonomicList($dbt,$hbo,$s,{'occurrence_list'=>\@occurrences, 'new_genera'=>\@new_genera, 'new_subgenera'=>\@new_subgenera, 'new_species'=>\@new_species, 'do_reclassify'=>1, 'warnings'=>\@warnings, 'save_links'=>$links });
    }
    if ( ! $return )	{
        print $links;
    } else	{
        print $return;
    }

    print "\n</div>\n<br>\n";

	print $hbo->stdIncludes( $PAGE_BOTTOM );
}



 #  3.* System processes new reference if user entered one.  System
#       displays search occurrences and search collections forms
#     * User searches for a collection in order to work with
#       occurrences from that collection
#     OR
#     * User searches for genus names of occurrences to work with
sub displayReIDCollsAndOccsSearchForm {
	# Have to be logged in
	if (!$s->isDBMember()) {
		login( "Please log in first.",'displayReIDCollsAndOccsSearchForm');
		exit;
	}
	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		$s->enqueue($q->query_string());
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}	


    my %vars = $q->Vars();
    $vars{'enterer_me'} = $s->get('enterer_reversed');
    $vars{'submit'} = "Search for reidentifications";
    $vars{'page_title'} = "Reidentifications search form";
    $vars{'action'} = "displayCollResults";
    $vars{'type'} = "reid";

	# Spit out the HTML
	print $hbo->stdIncludes( $PAGE_TOP );
    print PBDBUtil::printIntervalsJava($dbt,1);
    print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('search_occurrences_form',\%vars);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayOccsForReID {

    my $dbh = $dbt->dbh;
	my $collection_no = $q->param('collection_no');
    my $taxon_name = $q->param('taxon_name');
	my $where = "";

	#dbg("genus_name: $genus_name, subgenus_name: $subgenus_name, species_name: $species_name");

	my $current_session_ref = $s->get("reference_no");
	# make sure they've selected a reference
	# (the only way to get here without a reference is by doing 
	# a coll search right after logging in).
	unless($current_session_ref){
		$s->enqueue($q->query_string());
		displaySearchRefs();	
		exit;
	}

	my $collNos = shift;
	my @colls;
	if($collNos){
		@colls = @{$collNos};
	}

	my $printCollDetails = 0;

	print $hbo->stdIncludes( $PAGE_TOP );
	print $hbo->populateHTML('js_occurrence_checkform');
    
	my $pageNo = $q->param('page_no');
	if ( ! $pageNo ) { 
		$pageNo = 1;
	}


	my $reference_no = $current_session_ref;
	my $ref = Reference::getReference($dbt,$reference_no);
	my $formatted_primary = Reference::formatLongRef($ref);
	my $refString = "<b><a href=\"$READ_URL?action=displayReference&reference_no=$reference_no\">$reference_no</a></b> $formatted_primary<br>";

	# Build the SQL
	my @where = ();
	my $printCollectionDetails = 0;
	# Don't build it directly from the genus_name or species_name, let dispalyCollResults
	# DO that for us and pass in a set of collection_nos, for consistency, then filter at the end

	if (! @colls && $q->param('collection_no')) {
		push @colls , $q->param('collection_no');
	}

	if (@colls) {
		$printCollectionDetails = 1;
		push @where, "collection_no IN (".join(',',@colls).")";
		my ($genus,$subgenus,$species) = Taxon::splitTaxon($q->param('taxon_name'));
		if ( $genus )	{
			my $names = $dbh->quote($genus);
			if ($subgenus) {
				$names .= ", ".$dbh->quote($subgenus);
			}
			push @where, "(genus_name IN ($names) OR subgenus_name IN ($names))";
		}
		push @where, "species_name LIKE ".$dbh->quote($species) if ($species);
	} elsif ($collection_no) {
		push @where, "collection_no=$collection_no";
	} else {
		push @where, "0=1";
	}

	# some occs are out of primary key order, so order them JA 26.6.04
	my $sql = "SELECT * FROM occurrences WHERE ".join(" AND ",@where);
	if ( $q->param('sort_occs_by') )	{
		$sql .= " ORDER BY ".$q->param('sort_occs_by');
		if ( $q->param('sort_occs_order') eq "desc" )	{
			$sql .= " DESC";
		}
	}
	my $limit = 1 + 10 * $pageNo;
	$sql .= " LIMIT $limit";

	dbg("$sql<br>");
	my @results = @{$dbt->getData($sql)};

	my $rowCount = 0;
	my %pref = $s->getPreferences();
	my @optional = ('editable_collection_no','subgenera','genus_and_species_only','abundances','plant_organs','species_name');
    if (@results) {
        my $header_vars = {
            'ref_string'=>$refString,
            'search_taxon_name'=>$taxon_name,
            'list_collection_no'=>$collection_no
        };
        $header_vars->{$_} = $pref{$_} for (@optional);
		print $hbo->populateHTML('reid_header_row', $header_vars);

	splice @results , 0 , ( $pageNo - 1 ) * 10;
        foreach my $row (@results) {
            my $html = "";
            # If we have 11 rows, skip the last one; and we need a next button
            $rowCount++;
            last if $rowCount > 10;

            # Print occurrence row and reid input row
            $html .= "<tr>\n";
            $html .= "    <td align=\"left\" style=\"padding-top: 0.5em;\">".$row->{"genus_reso"};
            $html .= " ".$row->{"genus_name"};
            if ($pref{'subgenera'} eq "yes")	{
                $html .= " ".$row->{"subgenus_reso"};
                $html .= " ".$row->{"subgenus_name"};
            }
            $html .= " " . $row->{"species_reso"};
            $html .= " " . $row->{"species_name"} . "</td>\n";
            $html .= " <td>". $row->{"comments"} . "</td>\n";
            if ($pref{'plant_organs'} eq "yes")	{
                $html .= "    <td>" . $row->{"plant_organ"} . "</td>\n";
                $html .= "    <td>" . $row->{"plant_organ2"} . "</td>\n";
            }
            $html .= "</tr>";
            if ($current_session_ref == $row->{'reference_no'}) {
                $html .= "<tr><td colspan=20><i>The current reference is the same as the original reference, so this taxon may not be reidentified.</i></td></tr>";
            } else {
                my $vars = {
                    'collection_no'=>$row->{'collection_no'},
                    'occurrence_no'=>$row->{'occurrence_no'},
                    'reference_no'=>$current_session_ref
                };
                $vars->{$_} = $pref{$_} for (@optional);
                $html .= $hbo->populateHTML('reid_entry_row',$vars);
            }

            # print other reids for the same occurrence

            $html .= "<tr><td colspan=100>";
            my ($table,$classification) = Collection::getReidHTMLTableByOccNum($dbt,$hbo,$s,$row->{'occurrence_no'}, 0);
            $html .= "<table>".$table."</table>";
            $html .= "</td></tr>\n";
            #$sth2->finish();
            
            my $ref = Reference::getReference($dbt,$row->{'reference_no'});
            my $formatted_primary = Reference::formatShortRef($ref);
            my $refString = "<a href=\"$READ_URL?action=displayReference&reference_no=$row->{reference_no}\">$row->{reference_no}</a></b>&nbsp;$formatted_primary";

            $html .= "<tr><td colspan=20 class=\"verysmall\" style=\"padding-bottom: 0.75em;\">Original reference: $refString<br>\n";
            # Print the collections details
            if ( $printCollectionDetails) {
                my $sql = "SELECT collection_name,state,country,formation,period_max FROM collections WHERE collection_no=" . $row->{'collection_no'};
                my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
                $sth->execute();
                my %collRow = %{$sth->fetchrow_hashref()};
                $html .= "Collection:";
                my $details = " <a href=\"$READ_URL?action=basicCollectionSearch&collection_no=$row->{'collection_no'}\">$row->{'collection_no'}</a>"." ".$collRow{'collection_name'};
                if ($collRow{'state'} && $collRow{'country'} eq "United States")	{
                     $details .= " - " . $collRow{'state'};
                }
                if ($collRow{'country'})	{
                    $details .= " - " . $collRow{'country'};
                }
                if ($collRow{'formation'})	{
                    $details .= " - " . $collRow{'formation'} . " Formation";
                }
                if ($collRow{'period_max'})	{
                    $details .= " - " . $collRow{'period_max'};
                }
                $html .= "$details </td>";
                $html .= "</tr>";
                $sth->finish();
            }
        
            #$html .= "<tr><td colspan=100><hr width=100%></td></tr>";
            if ($rowCount % 2 == 1) {
                $html =~ s/<tr/<tr class=\"darkList\"/g;
            } else	{
                $html =~ s/<tr/<tr class=\"lightList\"/g;
            }
            print $html;

        }
    }

	print "</table>\n";
	$pageNo++;
	if ($rowCount > 0)	{
		print qq|<center><p><input type=submit value="Save reidentifications"></center></p>\n|;
		print qq|<input type="hidden" name="page_no" value="$pageNo">\n|;
		print qq|<input type="hidden" name="sort_occs_by" value="|;
		print $q->param('sort_occs_by'),"\">\n";
		print qq|<input type="hidden" name="sort_occs_order" value="|;
		print $q->param('sort_occs_order'),"\">\n";
	} else	{
		print "<center><p class=\"pageTitle\">Sorry! No matches were found</p></center>\n";
		print "<p align=center>Please <a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\">try again</a> with different search terms</p>\n";
	}
	print "</form>\n";
	print "\n<table border=0 width=100%>\n<tr>\n";

	# Print prev and next  links as appropriate

	# Next link
	if ( $rowCount > 10 ) {
		print "<td align=center>";
		print qq|<b><a href="$WRITE_URL?action=displayCollResults&type=reid|;
		print qq|&taxon_name=$taxon_name|;
		print qq|&collection_no=$collection_no|;
		print qq|&sort_occs_by=|,$q->param('sort_occs_by');
		print qq|&sort_occs_order=|,$q->param('sort_occs_order');
		print qq|&page_no=$pageNo">Skip to the next 10 occurrences</a></b>\n|;
		print "</td></tr>\n";
		print "<tr><td class=small align=center><i>Warning: if you go to the next page without saving, your changes will be lost</i></td>\n";
	}

	print "</tr>\n</table><p>\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);
}


# Marks the most_recent field in the reidentifications table to YES for the most recent reid for
# an occurrence, and marks all not-most-recent to NO.  Needed for collections search for Map and such
# PS 8/15/2005
sub setMostRecentReID {
    my $dbt = shift;
    my $dbh = $dbt->dbh;
    my $occurrence_no = shift;

    if ($occurrence_no =~ /^\d+$/) {
        my $sql = "SELECT re.* FROM reidentifications re, refs r WHERE r.reference_no=re.reference_no AND re.occurrence_no=".$occurrence_no." ORDER BY r.pubyr DESC, re.reid_no DESC";
        my @results = @{$dbt->getData($sql)};
        if ($results[0]->{'reid_no'}>0) {
            $sql = "UPDATE reidentifications SET modified=modified, most_recent='YES' WHERE reid_no=".$results[0]->{'reid_no'};
            my $result = $dbh->do($sql);
            dbg("set most recent: $sql");
            if (!$result) {
                carp "Error setting most recent reid to YES for reid_no=$results[0]->{reid_no}";
            } else	{
                $sql = "UPDATE occurrences SET modified=modified, reid_no=".$results[0]->{'reid_no'}." WHERE occurrence_no=".$occurrence_no;
                my $result = $dbh->do($sql);
            }
                
            my @older_reids;
            for(my $i=1;$i<scalar(@results);$i++) {
                push @older_reids, $results[$i]->{'reid_no'};
            }
            if (@older_reids) {
                $sql = "UPDATE reidentifications SET modified=modified, most_recent='NO' WHERE reid_no IN (".join(",",@older_reids).")";
                $result = $dbh->do($sql);
                dbg("set not most recent: $sql");
                if (!$result) {
                    carp "Error setting most recent reid to NO for reid_no IN (".join(",",@older_reids).")"; 
                }
            }
        }
    }
}

# ------------------------ #
# Person pages
# ------------------------ #
sub showEnterers {
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    print Person::showEnterers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub showAuthorizers {
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    print Person::showAuthorizers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub showFeaturedAuthorizers {
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    print Person::showFeaturedAuthorizers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub showInstitutions {
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    print Person::showInstitutions($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub publications	{
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    my %vars;
    $vars{'publications'} = Person::publications($dbt,$hbo);
    print $hbo->populateHTML('publications', \%vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


# ------------------------ #
# Confidence Intervals JSM #
# ------------------------ #

sub displaySearchSectionResults{
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::displaySearchSectionResults($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displaySearchSectionForm{
    require Confidence;
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::displaySearchSectionForm($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayTaxaIntervalsForm{
    require Confidence;
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::displayTaxaIntervalsForm($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayTaxaIntervalsResults{
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::displayTaxaIntervalsResults($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub buildListForm {
    return if PBDBUtil::checkForBot();
    require Confidence;
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::buildList($q, $s, $dbt,$hbo,{});
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub displayStratTaxaForm{
    return if PBDBUtil::checkForBot();
    require Confidence;
    print $hbo->stdIncludes($PAGE_TOP);
    Confidence::displayStratTaxa($q, $s, $dbt);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub showOptionsForm {
    return if PBDBUtil::checkForBot();
    require Confidence;
	print $hbo->stdIncludes($PAGE_TOP);
	Confidence::optionsForm($q, $s, $dbt);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub calculateTaxaInterval {
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	Confidence::calculateTaxaInterval($q, $s, $dbt);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub calculateStratInterval {
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
	print $hbo->stdIncludes($PAGE_TOP);
	Confidence::calculateStratInterval($q, $s, $dbt);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

## Cladogram stuff

sub displayCladeSearchForm	{
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('search_clade_form');
    print $hbo->stdIncludes($PAGE_BOTTOM);

	#print $hbo->stdIncludes($PAGE_TOP);
    #Cladogram::displayCladeSearchForm($dbt,$q,$s,$hbo);
	#print $hbo->stdIncludes($PAGE_BOTTOM);
}
#sub processCladeSearch	{
#	print $hbo->stdIncludes($PAGE_TOP);
#    Cladogram::processCladeSearch($dbt,$q,$s,$hbo);
#	print $hbo->stdIncludes($PAGE_BOTTOM);
#}
sub displayCladogramChoiceForm	{
	print $hbo->stdIncludes($PAGE_TOP);
    Cladogram::displayCladogramChoiceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub displayCladogramForm	{
	print $hbo->stdIncludes($PAGE_TOP);
    Cladogram::displayCladogramForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub submitCladogramForm {
	print $hbo->stdIncludes($PAGE_TOP);
    Cladogram::submitCladogramForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}
sub drawCladogram	{
	print $hbo->stdIncludes($PAGE_TOP);
    my $cladogram_no = $q->param('cladogram_no');
    my $force_redraw = $q->param('force_redraw');
    my ($pngname, $caption, $taxon_name) = Cladogram::drawCladogram($dbt,$cladogram_no,$force_redraw);
    if ($pngname) {
        print qq|<div align="center"><h3>$taxon_name</h3>|;
        print qq|<img src="/public/cladograms/$pngname"><br>$caption|;
        print qq|</div>|;
    }
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# JA 17.1.10
sub displayReviewForm {
	print $hbo->stdIncludes($PAGE_TOP);
	Review::displayReviewForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub processReviewForm {
	print $hbo->stdIncludes($PAGE_TOP);
	Review::processReviewForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub listReviews	{
	print $hbo->stdIncludes($PAGE_TOP);
	Review::listReviews($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub showReview	{
	print $hbo->stdIncludes($PAGE_TOP);
	Review::showReview($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# Displays taxonomic opinions and names associated with a reference_no
# PS 10/25/2004
sub displayTaxonomicNamesAndOpinions {
    print $hbo->stdIncludes( $PAGE_TOP );
    my $ref = Reference->new($dbt,$q->param('reference_no'));
    if ($ref) {
        $q->param('goal'=>'authority');
        if ( $q->param('display') ne "opinions" )	{
            processTaxonSearch($dbt, $hbo, $q, $s);
        }
        if ( $q->param('display') ne "authorities" )	{
            Opinion::displayOpinionChoiceForm($dbt,$s,$q);
        }
    } else {
        print "<div align=\"center\">".Debug::printErrors(["No valid reference supplied"])."</div>";
    }
    print $hbo->stdIncludes($PAGE_BOTTOM);
}



sub logRequest {
    my ($s,$q) = @_;
    
    if ( $HOST_URL !~ /paleobackup\.nceas\.ucsb\.edu/ && $HOST_URL !~ /paleodb\.org/ )  {
        return;
    }
    my $status = open LOG, ">>/var/log/apache2/request_log";
    if (!$status) {
        $status = open LOG, ">>/var/log/httpd/request_log";
    }
    if (!$status) {
        carp "Could not open request_log";
    } else {
        my $date = now();

        my $ip = $ENV{'REMOTE_ADDR'};
        $ip ||= 'localhost';

        my $user = $s->get('enterer');
        if (!$user) { $user = 'Guest'; }

        my $postdata = "";
        my @fields = $q->param();
        foreach my $field (@fields) {
            my @values = $q->param($field);
            foreach my $value (@values) {
                if ($value !~ /^$/) {
                    # Escape these to make it easier to parse later
                    $value =~ s/&/\\A/g;
                    $value =~ s/\\/\\\\/g;
                    $postdata .= "$field=$value&";
                }
            }
        } 
        $postdata =~ s/&$//;
        $postdata =~ s/[\n\r\t]/ /g;

        # make the file "hot" to ensure that the buffer is flushed properly.
        # see http://perl.plover.com/FAQs/Buffering.html for more info on this.
        my $ofh = select LOG;
        $| = 1;
        select $ofh;

        my $line = "$ip\t$date\t$user\t$postdata\n";
        print LOG $line;
    }
}

# These next functin simply provide simple links to all of our taxon/collection pages
# so they can be indexed by search engines
sub listCollections {
	print $hbo->stdIncludes ($PAGE_TOP);
    my $sql = "SELECT MAX(collection_no) max_id FROM collections";
    my $page = int($q->param("page"));

    my $max_id = ${$dbt->getData($sql)}[0]->{'max_id'};
   
    for(my $i=0;$i*200 < $max_id;$i++) {
        if ($page == $i) {
            print "$i ";
        } else {
            print "<a href=\"$READ_URL?action=listCollections&page=$i\">$i</a> ";
        }
    }
    print "<BR><BR>";
    my $start = $page*200;
    for (my $i=$start; $i<$start+200 && $i <= $max_id;$i++) {
        print "<a href=\"$READ_URL?action=basicCollectionSearch&collection_no=$i\">$i</a> ";
    }

	print $hbo->stdIncludes ($PAGE_BOTTOM);
}

sub listTaxa {
	print $hbo->stdIncludes ($PAGE_TOP);
    
    my $sql = "SELECT MAX(taxon_no) max_id FROM authorities";
    my $page = int($q->param("page"));

    my $max_id = ${$dbt->getData($sql)}[0]->{'max_id'};
   
    for(my $i=0;$i*200 < $max_id;$i++) {
        if ($page == $i) {
            print "$i ";
        } else {
            print "<a href=\"$READ_URL?action=listCollections&page=$i\">$i</a> ";
        }
    }
    print "<BR><BR>";
    my $start = $page*200;
    for (my $i=$start; $i<$start+200 && $i <= $max_id;$i++) {
        print "<a href=\"$READ_URL?action=basicTaxonInfo&taxon_no=$i\">$i</a> ";
    }

	print $hbo->stdIncludes ($PAGE_BOTTOM);
}

sub inventoryForm	{
	Collection::inventoryForm($dbt,$q,$s,$hbo);
	return;
}

sub inventoryInfo	{
	logRequest($s,$q);
	Collection::inventoryInfo($dbt,$q,$s,$hbo);
}



