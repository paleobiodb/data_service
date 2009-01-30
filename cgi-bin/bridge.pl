#!/usr/local/bin/perl

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

# god awful Poling modules
use Taxon;
use Opinion;
use Validation;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HOST_URL $HTML_DIR $DATA_DIR $IS_FOSSIL_RECORD $TAXA_TREE_CACHE);

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

# don't let users into the contributors' area unless they're on the main site
#  or backup server (as opposed to a mirror site) JA 3.8.04
if ( $HOST_URL !~ /paleobackup\.nceas\.ucsb\.edu/ && $HOST_URL !~ /paleodb\.org/ )	{
	 $q->param("user" => "Guest");
}

# Make the HTMLBuilder object - it'll use whatever template dir is appropriate
my $use_guest = ($q->param('user') =~ /^guest$/i) ? 1 : 0;
my $hbo = HTMLBuilder->new($dbt,$s,$use_guest,'');

# process the action
processAction();

# ____________________________________________________________________________________
# --------------------------------------- subroutines --------------------------------
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# rjp, 2/2004
sub processAction {
	# Grab the action from the form.  This is what subroutine we should run.
	my $action = ($q->param("action") || "displayMenuPage");
	
    # The right combination will allow me to conditionally set the DEBUG flag
    if ($s->get("enterer") eq "J. Sepkoski" && 
        $s->get("authorizer") eq "J. Alroy" ) {
            $Debug::DEBUG = 1;
    }
    
	# figure out what to do with the action
    if ($action eq 'displayDownloadNeptuneForm' &&  $HOST_URL !~ /paleodb\.org/) {
	    print $q->redirect( -url=>'http://paleodb.org/cgi-bin/bridge.pl?action='.$action);
    } elsif ($action eq 'logout') {
    	logout();
        return;
    } elsif ($action eq 'processLogin') {
        processLogin();
        return;
    } else {
        if ($q->param('output_format') eq 'xml' ||
            $q->param('action') =~ /XML$/) {
            print $q->header(-type => "text/xml", 
                         -Cache_Control=>'no-cache',
                         -expires =>"now" );
        } else {
            print $q->header(-type => "text/html", 
                         -Cache_Control=>'no-cache',
                         -expires =>"now" );
        } 

        dbg("<p><font color='red' size='+1' face='arial'>You are in DEBUG mode!</font><br> Cookie []<BR> Action [$action] Authorizer [".$s->get("authorizer")."] Enterer [".$s->get("enterer")."]<BR></p>");
        #dbg("@INC");
        dbg($q->Dump);
        execAction($action);
	}
    exit;
}

# Exec actions with this so we can peform some sanity check or do whatever on them
# trap this in an eval {} later so we can dump out all relevant data on crash in the
# future
sub execAction {
    my $action = shift;
    $action =~ s/[^a-zA-Z0-9_]//g;
    $action = \&{$action}; # Hack so use strict doesn't break
    # Run the action (ie, call the proper subroutine)
    &$action;
    #if ($@) {
    #    print $@."<br>";
    #    foreach (my $i = 0;$i< 5;$i++) {
    #        my ($package, $filename, $line, $subroutine) = caller($i);
    #        if ($package) {
    #            print "$line: $package:$subroutine<br>";
    #        } 
    #    }
    #}
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

        my $action = "displayMenuPage";
        # Destination
        if ($q->param("destination") ne "") {
            $action = $q->param("destination");
        } 
        execAction($action);
    } else { # no cookie
        # failed login:  (bad password, etc.)
        my $errorMessage;

        if (!$authorizer) {
            $errorMessage = "The authorizer name is required. ";
        } 
        if (!$enterer) {
            $errorMessage .= "The enterer name is required. ";
        }
        if (($authorizer && $authorizer !~ /\./) ||
            ($enterer && $enterer !~ /\./)) {
            $errorMessage .= "Note that the format for names is <i>Smith, A.</i> ";
        }    
        if (!$password) {
            $errorMessage .= "The password is required. ";
        }
        if (!$errorMessage )	{
            $errorMessage .= "The authorizer name, enterer name, or password is invalid. ";
        }

        print $q->header(-type => "text/html", 
                         -Cache_Control=>'no-cache',
                         -expires =>"now" );

        # return them to the login page with a message about the bad login.
        displayLoginPage(Debug::printErrors(["Sorry, your login failed. $errorMessage"]));
        exit;
    }
}

# Logout
# Clears the SESSION_DATA table of this session.
sub logout {
	my $session_id = $q->cookie("session_id");
    my $dbh = $dbt->dbh;

	if ( $session_id ) {
		my $sql =	"DELETE FROM session_data WHERE session_id = ".$dbh->quote($session_id);
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	}

	print $q->redirect( -url=>$WRITE_URL."?user=Contributor" );
	exit;
}

# Displays the login page
sub displayLoginPage {
	my $message = shift;
    my $destination = shift;
	
    my %vars = $q->Vars();
    $vars{'message'} = $message;
    $vars{'authorizer_reversed'} ||= $q->cookie("authorizer_reversed");
    $vars{'enterer_reversed'} ||= $q->cookie("enterer_reversed");
    $vars{'destination'} ||= $destination;
	
	print $hbo->stdIncludes("std_page_top");
    print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('login_box', \%vars);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayPreferencesPage {
    print $hbo->stdIncludes("std_page_top");
    Session::displayPreferencesPage($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub setPreferences {
    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first.");
        return;
    }
    print $hbo->stdIncludes("std_page_top");
    Session::setPreferences($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

# displays the main menu page for the data enterers
sub displayMenuPage	{
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

    if ($s->isDBMember() && $q->param('user') ne 'Guest') {
	    print $hbo->stdIncludes("std_page_top");
	    print $hbo->populateHTML('menu');
	    print $hbo->stdIncludes("std_page_bottom");
    } else {
        if ($q->param('user') eq 'Contributor') {
		    displayLoginPage( "Please log in first.","displayMenuPage" );
        } else {
            displayHomePage();
        }
    }
}




# well, displays the home page
sub displayHomePage {
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

	# Get some populated values
	my $sql = "SELECT * FROM statistics";
	my $row = ${$dbt->getData($sql)}[0];

	# display the eight most recently entered collections that have
	#  distinct combinations of references and enterers (the latter is
	#  usually redundant)
	my $nowDate = now();
	$nowDate = $nowDate-'1M';
	my ($date,$time) = split / /,$nowDate;
	my ($yyyy,$mm,$dd) = split /-/,$date,3;
	my $sql = "SELECT reference_no,enterer_no,collection_no,collection_name FROM collections WHERE created>".$yyyy.$mm.$dd."000000 ORDER BY collection_no DESC";
	my @colls = @{$dbt->getData($sql)};
	my %entererseen;
	my $printed;
	for my $coll ( @colls )	{
		if ( $entererseen{$coll->{reference_no}.$coll->{enterer_no}} < 1 )	{
			$entererseen{$coll->{reference_no}.$coll->{enterer_no}}++;
			$row->{collection_links} .= qq|<div class="verysmall collectionLink"><a class="homeBodyLinks" href="$READ_URL?action=displayCollectionDetails&amp;collection_no=$coll->{collection_no}">$coll->{collection_name}</a></div>\n|;
			$printed++;
			if ( $printed == 25 )	{
				last;
			}
		}
	}

	$row->{'enterer_names'} = Person::homePageEntererList($dbt);

	print $hbo->stdIncludes("std_page_top");
	print $hbo->populateHTML('home', $row);
	print $hbo->stdIncludes("std_page_bottom");
}



# Shows the form for requesting a map
sub displayBasicMapForm {
	my %vars = ( 'pointsize1'=>'large', 'pointshape1'=>'circles', 'dotcolor1'=>'gold', 'dotborder1'=>'no' );
	print $hbo->stdIncludes("std_page_top");
	print $hbo->populateHTML('basic_map_form', \%vars);
	print $hbo->stdIncludes("std_page_bottom");
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
	print $hbo->stdIncludes("std_page_top");
    print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('js_map_checkform');
    print $hbo->populateHTML('map_form', \%vars);
	print $hbo->stdIncludes("std_page_bottom");
}



sub displayMapResults {
    require Map;

    $|=1; # Freeflowing data

    logRequest($s,$q);
	print $hbo->stdIncludes("std_page_top" );

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
        my $map_age = sprintf("%.0f",(($itv->{'lower_boundary'} + $itv->{'upper_boundary'})/2));
        if (!$map_age) {
            push @errors, "No age range for interval";
        }
        $q->param('maptime'=>$map_age);

    }
    if ($q->param('taxon_name_preset') && !$q->param('taxon_name')){
        $q->param('taxon_name'=>$q->param('taxon_name_preset'));
        $q->delete('taxon_name_preset');
    }

    if ($q->param('mapcolors')) {
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
        } else { # Green on blue default
            %settings = (
                mapbgcolor=>'sky blue',crustcolor=>'olive drab', crustedgecolor=>'white',
                usalinecolor=>'green', borderlinecolor=>'green', autoborders=>'yes',
                gridsize=>'30',gridcolor=>'light gray',gridposition=>'in back',
                coastlinecolor=>'dark green'
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
	print $hbo->stdIncludes("std_page_bottom");
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
        print $hbo->stdIncludes("std_page_top") 
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
        print $hbo->stdIncludes("std_page_bottom") 
    }
}

sub displaySimpleMap {
    print $hbo->stdIncludes("std_page_top");
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
    print $hbo->stdIncludes("std_page_bottom");
}


sub displayDownloadForm {
    
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get("authorizer_reversed");
    $vars{'enterer_me'} = $s->get("authorizer_reversed");

    if ($s->isDBMember()) {
        $vars{'row_class_1a'} = '';
        $vars{'row_class_1b'} = ' class="lightGray"';
    } else {
        $vars{'row_class_1a'} = ' class="lightGray"';
        $vars{'row_class_1b'} = '';
    }

	print $hbo->stdIncludes("std_page_top");
	print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('download_form',\%vars);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayBasicDownloadForm {
	my %vars = $q->Vars();
	print $hbo->stdIncludes("std_page_top" );
	print $hbo->populateHTML('basic_download_form',\%vars);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayDownloadResults {
    require Download;
    logRequest($s,$q);

	print $hbo->stdIncludes( "std_page_top" );

	my $m = Download->new($dbt,$q,$s,$hbo);
	$m->buildDownload( );

	print $hbo->stdIncludes("std_page_bottom");
}

sub emailDownloadFiles	{
	require Download;

	print $hbo->stdIncludes( "std_page_top" );

	my $m = Download->new($dbt,$q,$s,$hbo);
	$m->emailDownloadFiles();

	print $hbo->stdIncludes("std_page_bottom");

}

# JA 28.7.08
sub displayDownloadMeasurementsForm	{
	my %vars;
	$vars{'error_message'} = shift;
	print $hbo->stdIncludes("std_page_top");
	print PBDBUtil::printIntervalsJava($dbt,1);
	print $hbo->populateHTML('download_measurements_form',\%vars);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayDownloadMeasurementsResults	{
	return if PBDBUtil::checkForBot();
	require Download;
	logRequest($s,$q);
	print $hbo->stdIncludes("std_page_top");
	Measurement::displayDownloadMeasurementsResults($q,$s,$dbt);
	print $hbo->stdIncludes("std_page_bottom");
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
    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('download_neptune_form',\%vars);
    print $hbo->stdIncludes("std_page_bottom");
}       
    
sub displayDownloadNeptuneResults {
    require Neptune;
    print $hbo->stdIncludes( "std_page_top" );
    Neptune::displayNeptuneDownloadResults($q,$s,$hbo,$dbt);
    print $hbo->stdIncludes("std_page_bottom");
}  

sub displayDownloadTaxonomyForm {
   
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    print $hbo->stdIncludes("std_page_top");
    print Person::makeAuthEntJavascript($dbt);
    print $hbo->populateHTML('download_taxonomy_form',\%vars);
    print $hbo->stdIncludes("std_page_bottom");
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
    print $hbo->stdIncludes( "std_page_top" );
    if ($q->param('output_data') =~ /ITIS/i) {
        DownloadTaxonomy::displayITISDownload($dbt,$q,$s);
    } else { 
        DownloadTaxonomy::displayPBDBDownload($dbt,$q,$s);
    }
                                              
    print $hbo->stdIncludes("std_page_bottom");
}  

sub displayReportForm {
	print $hbo->stdIncludes( "std_page_top" );
	print $hbo->populateHTML('report_form');
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayReportResults {
	require Report;

	logRequest($s,$q);

	print $hbo->stdIncludes( "std_page_top" );

	my $r = Report->new($dbt,$q,$s);
	$r->buildReport();

	print $hbo->stdIncludes("std_page_bottom");
}

sub displayMostCommonTaxa	{
	my $dataRowsRef = shift;
	require Report;

	logRequest($s,$q);

	print $hbo->stdIncludes( "std_page_top" );

	my $r = Report->new($dbt,$q,$s);
	$r->findMostCommonTaxa($dataRowsRef);

	print $hbo->stdIncludes("std_page_bottom");
}

sub displayCurveForm {
    my $std_page_top = $hbo->stdIncludes("std_page_top");
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

    print $hbo->stdIncludes("std_page_bottom");
}

sub displayCurveResults {
    require Curve;

    logRequest($s,$q);

    my $std_page_top = $hbo->stdIncludes("std_page_top");
    print $std_page_top;

	my $c = Curve->new($q, $s, $dbt );
	$c->buildCurve();

    print $hbo->stdIncludes("std_page_bottom");
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
		print $hbo->stdIncludes( "std_page_top" );
	}
	print $hbo->populateHTML($page,[],[]);
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print $hbo->stdIncludes("std_page_bottom");
	}
}

sub displaySearchRefs {
    my $error = shift;
    print $hbo->stdIncludes( "std_page_top" );
    Reference::displaySearchRefs($dbt,$q,$s,$hbo,$error);
    print $hbo->stdIncludes( "std_page_bottom" );
}

sub selectReference {
	$s->setReferenceNo($q->param("reference_no") );
	displayMenuPage( );
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

	print $hbo->stdIncludes("std_page_top");
    Reference::displayRefResults($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
				
}

sub getReferencesXML {
    logRequest($s,$q);
    Reference::getReferencesXML($dbt,$q,$s,$hbo);
}

sub displayReferenceForm {
    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first.");
        return;
    }

	print $hbo->stdIncludes("std_page_top");
    Reference::displayReferenceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayReference {
	print $hbo->stdIncludes("std_page_top");
    Reference::displayReference ($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub processReferenceForm {
	print $hbo->stdIncludes("std_page_top");
    Reference::processReferenceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

# 5.4.04 JA
# print the special search form used when you are adding a collection
# uses some code lifted from displaySearchColls
sub displaySearchCollsForAdd	{

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
	print $hbo->stdIncludes( "std_page_top" );
	print  $hbo->populateHTML('search_collections_for_add_form' , \%pref);
	print $hbo->stdIncludes("std_page_bottom");

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
	if ( ! $reference_no && $type !~ /^(?:analyze_abundance|view|edit|reclassify_occurrence|count_occurrences|most_common)$/) {
		# Come back here... requeue our option
		$s->enqueue("action=displaySearchColls&type=$type" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	# Show the "search collections" form
    my %vars = ();
    $vars{'enterer_me'} = $s->get('enterer_reversed');
    $vars{'page_title'} = "Collection search form";
    $vars{'action'} = "displayCollResults";
    $vars{'type'} = $type;
    $vars{'submit'} = "Search collections";
    $vars{'error'} = $error;

	if ($type eq 'occurrence_table') {
		$vars{'reference_no'} = $reference_no;
		$vars{'limit'} = 20;
	}

	# Spit out the HTML
	print $hbo->stdIncludes( "std_page_top" );
	print Person::makeAuthEntJavascript($dbt);
	print PBDBUtil::printIntervalsJava($dbt,1);
	print $hbo->populateHTML('search_collections_form', \%vars);
	print $hbo->stdIncludes("std_page_bottom");
}


# User submits completed collection search form
# System displays matching collection results
# Called during collections search, and by displayReIDForm() routine.
sub displayCollResults {
	return if PBDBUtil::checkForBot();
	if ( ! $s->get('enterer') && $q->param('type') eq "reclassify_occurrence" )    {
		print $hbo->stdIncludes( "std_page_top" );
		print "<center>\n<p class=\"pageTitle\">Sorry!</p>\n\n";
		print "<p>You can't reclassify occurrences unless you <a href=\"$WRITE_URL?action=displayMenuPage&amp;user=Contributor\">log in</a> first.</p>\n</center>\n";
		print $hbo->stdIncludes("std_page_bottom");
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
		if ($q->param("type") =~ /occurrence_table|count_occurrences|most_common/ ||
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
	}

    my $exec_url = ($type =~ /view/) ? $READ_URL : $WRITE_URL;

    my $action =  
          ($type eq "add") ? "displayCollectionDetails"
        : ($type eq "edit") ? "displayCollectionForm"
        : ($type eq "view") ? "displayCollectionDetails"
        : ($type eq "edit_occurrence") ? "displayOccurrenceAddEdit"
        : ($type eq "analyze_abundance") ? "rarefyAbundances"
        : ($type eq "reid") ? "displayOccsForReID"
        : ($type eq "reclassify_occurrence") ?  "startDisplayOccurrenceReclassify"
        : ($type eq "most_common") ? "displayMostCommonTaxa"
        : "displayCollectionDetails";
	
	# Build the SQL
	# which function to use depends on whether the user is adding a collection
	my $sql;
    
    my ($dataRows,$ofRows,$warnings,$occRows) = ([],'',[],[]);
	if ( $q->param('type') eq "add" )	{
		# you won't have an in list if you are adding
		($dataRows,$ofRows) = processCollectionsSearchForAdd();
	} else	{
        my $fields = ["authorizer","country", "state", "period_max", "period_min", "epoch_max", "epoch_min", "intage_max", "intage_min", "locage_max", "locage_min", "max_interval_no", "min_interval_no","collection_aka","collectors","collection_dates"];
        if ($q->param('output_format') eq 'xml') {
            push @$fields, "latdeg","latmin","latsec","latdir","latdec","lngdeg","lngmin","lngsec","lngdir","lngdec";
        }
        my %options = $q->Vars();
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
#        $options{'lithologies'} = $options{'lithology1'} if (!$options{'lithologies'}); delete $options{'lithology1'};
#        $options{'lithadjs'} = $options{'lithadj'}; delete $options{'lithadj'};
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

		($dataRows,$ofRows,$warnings,$occRows) = Collection::getCollections($dbt,$s,\%options,$fields);
	}

	my @dataRows = @$dataRows;
	my $displayRows = scalar(@dataRows);	# get number of rows to display

	if ( $type eq 'occurrence_table' && @dataRows) {
		my @colls = map {$_->{'collection_no'}} @dataRows;
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
				push(@colls , $row->{collection_no});
			}
            if ($q->param('type') eq 'reid') {
			    displayOccsForReID(\@colls);
            } else {
			    Reclassify::displayOccurrenceReclassify($q,$s,$dbt,$hbo,\@colls);
            }
			exit;
		}
		
		print $hbo->stdIncludes( "std_page_top" );
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

	print qq|<table class="small" style="margin-left: 1em; margin-right: 1em; border: 1px solid lightgray;" border="0" cellpadding="4" cellspacing="0">|;
 
		# print columns header
		print "<tr>";
		print "<th>Collection</th>";
		print "<th align=left>Authorizer</th>";
		print "<th align=left nowrap>Collection name</th>";
		print "<th align=left>Reference</th>";
        print "<th align=left>Distance</th>" if ($type eq 'add');
		print "</tr>";
 
        # Make non-editable links not highlighted  
        my ($p,%is_modifier_for); 
        if ($type eq 'edit') { 
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
                $timeplace = $seen_interval{$dataRow->{'max_interval_no'}." ".$dataRow->{'min_interval_no'}};
            } else {
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
            }

			$timeplace =~ s/\/(Lower|Upper)//g;

			# rest of timeplace construction JA 20.8.02
			$timeplace .= "</b> - ";
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
	
            if ($type ne 'edit' || 
                $type eq 'edit' && ($s->get("superuser") ||
                                   ($s->get('authorizer_no') && $s->get("authorizer_no") == $dataRow->{'authorizer_no'}) ||
                                    $is_modifier_for{$dataRow->{'authorizer_no'}})) {
		  	    print "<td align=center valign=top><a href=\"$exec_url?action=$action&collection_no=$dataRow->{collection_no}";
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
                print "\">$dataRow->{collection_no}</a></td>";
            } else {	
                # Don't link it if if we're in edit mode and we don't have permission
                print "<td align=center valign=top>$dataRow->{collection_no}</td>";
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
            print "<td valign=top nowrap>$dataRow->{authorizer}</td>";
            print qq|<td valign="top" style="padding-left: 0.5em; text-indent: -0.5em;"><span style="padding-right: 1em;">${collection_names}</span> <span class="tiny"><i>${timeplace}</i></span></td>|;
            print "<td valign=top nowrap>$reference</td>";
            print "<td valign=top align=center>".int($dataRow->{distance})." km </td>" if ($type eq 'add');
            print "</tr>";
  		}

        print "</table>\n";
    } elsif ( $displayRows == 1 ) { # if only one row to display...
		$q->param(collection_no=>$dataRows[0]->{'collection_no'});
		# Do the action directly if there is only one row
        execAction($action);
    } else {
		# If this is an add,  Otherwise give an error
		if ( $type eq "add" ) {
			displayCollectionForm();
			return;
		} else {
			my $error = "<center>\n<p style=\"margin-top: -1em;\">Your search produced no matches: please try again</p>";
			displaySearchColls($error);
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
        print "<a href=\"$exec_url?$getString\"><b>Get $numLeft collections</b></a> - ";
    } 

	if ( $type eq "add" )	{
		print "<a href='$exec_url?action=displaySearchCollsForAdd&type=add'><b>Do another search</b></a>";
	} else	{
		print "<a href='$exec_url?action=displaySearchColls&type=$type'><b>Do another search</b></a>";
	}

    print "</center></p>";
    # End footer links
    

	if ( $type eq "add" ) {
		print qq|<form action="$exec_url">\n|;

		# stash the lat/long coordinates to be populated on the
		#  entry form JA 6.4.04
		my @coordfields = ("latdeg","latmin","latsec","latdec","latdir",
				"lngdeg","lngmin","lngsec","lngdec","lngdir");
		for my $cf (@coordfields)	{
			if ( $q->param($cf) )	{
				print "<input type=\"hidden\" name=\"$cf\" value=\"";
				print $q->param($cf) . "\">\n";
			}
		}

		print qq|<input type="hidden" name="action" value="displayCollectionForm">\n|;
		print qq|<center>\n<input type=submit value="Add a new collection">\n|;
		print qq|</center>\n</form>\n|;
	}
		
	print $hbo->stdIncludes("std_page_bottom");

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



# JA 5-6.4.04
# compose the SQL to find collections of a certain age within 100 km of
#  a coordinate (required when the user wants to add a collection)
sub processCollectionsSearchForAdd	{

    my $dbh = $dbt->dbh;
    return if PBDBUtil::checkForBot();
    require Map;

    # some generally useful trig stuff needed by processCollectionsSearchForAdd
    my $PI = 3.14159265;


	# get a list of interval numbers that fall in the geological period
    my $t = new TimeLookup($dbt);
    my $sql = "SELECT interval_no FROM intervals WHERE interval_name LIKE ".$dbh->quote($q->param('period_max'));
    my $period_no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	my @intervals = $t->mapIntervals($period_no);

	my $sql = "SELECT c.collection_no, c.collection_aka, c.authorizer_no, p1.name authorizer, c.collection_name, c.access_level, c.research_group, c.release_date, DATE_FORMAT(release_date, '%Y%m%d') rd_short, c.country, c.state, c.latdeg, c.latmin, c.latsec, c.latdec, c.latdir, c.lngdeg, c.lngmin, c.lngsec, c.lngdec, c.lngdir, c.max_interval_no, c.min_interval_no, c.reference_no FROM collections c LEFT JOIN person p1 ON p1.person_no = c.authorizer_no WHERE ";
	$sql .= "c.max_interval_no IN (" . join(',', @intervals) . ") AND ";

	# convert the coordinates to decimal values

	my $lat = $q->param('latdeg');
	my $lng = $q->param('lngdeg');
	if ( $q->param('latmin') )	{
		$lat = $lat + ( $q->param('latmin') / 60 ) + ( $q->param('latsec') / 3600 );
	} elsif ( $q->param('latdec') ) {
		$lat = $lat . "." . $q->param('latdec');
	}
		
	if ( $q->param('lngmin') )	{
		$lng = $lng + ( $q->param('lngmin') / 60 ) + ( $q->param('lngsec') / 3600 );
	} elsif ( $q->param('lngdec') ) {
		$lng = $lng . "." . $q->param('lngdec');
	}

	# west and south are negative
	if ( $q->param('latdir') =~ /S/ )	{
		$lat = $lat * -1;
	}
	if ( $q->param('lngdir') =~ /W/ )	{
		$lng = $lng * -1;
	}
    my $mylat = $lat;
    my $mylng = $lng;

	# maximum latitude is center point plus 100 km, etc.
	# longitude is a little tricky because we have to deal with cosines
	# it's important to use floor instead of int because they round off
	#  negative numbers differently
	my $maxlat = floor($lat + 100 / 111);
	my $minlat = floor($lat - 100 / 111);
	my $maxlng = floor($lng + ( (100 / 111) / cos($lat * $PI / 180) ) );
	my $minlng = floor($lng - ( (100 / 111) / cos($lat * $PI / 180) ) );

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

    if ($q->param('sortby') eq 'collection_no') {
	    $sql .= " ORDER BY c.collection_no";
    } elsif ($q->param('sortby') eq 'collection_name') {
	    $sql .= " ORDER BY c.collection_name";
    }

    dbg("process collections search for add: $sql");

    my $sth = $dbt->dbh->prepare($sql);
    $sth->execute();
    my $p = Permissions->new ($s,$dbt);

    # See if rows okay by permissions module
    my @dataRows = ();
    my $limit = 10000000;
    my $ofRows = 0;
    $p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );


	# make sure collections really are within 100 km of the submitted
	#  lat/long coordinate JA 6.4.04
    my @tempDataRows;
    # have to recompute this
    for my $dr (@dataRows)	{
        # compute the coordinate
        my $lat = $dr->{'latdeg'};
        my $lng = $dr->{'lngdeg'};
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

        # if the points are less than 100 km apart, save
        #  the collection
        #if ( $mylat == $lat && $mylng == $lng )	{
        #    push @tempDataRows, $dr;
        #    $ofRows++;
        #}
        my $distance = 111 * Map::GCD($mylat,$lat,abs($mylng-$lng));
        if ( $distance < 100 )	{
            $dr->{'distance'} = $distance;
            push @tempDataRows, $dr;
        } 
    }

    if ($q->param('sortby') eq 'distance') {
        @tempDataRows = sort {$a->{'distance'} <=> $b->{'distance'} 
                                                ||
                              $a->{'collection_no'} <=> $b->{'collection_no'}} @tempDataRows;
    }

	return (\@tempDataRows,scalar(@tempDataRows));
}


sub displayCollectionForm {
    # Have to be logged in
    if (!$s->isDBMember()) {
        displayLoginPage("Please log in first.");
        exit;
    }
    print $hbo->stdIncludes("std_page_top");
    Collection::displayCollectionForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub processCollectionForm {
    if (!$s->isDBMember()) {
        displayLoginPage("Please log in first.");
        exit;
    }
    print $hbo->stdIncludes("std_page_top");
    Collection::processCollectionForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");

}

sub displayCollectionDetails {
    logRequest($s,$q);
    Collection::displayCollectionDetails($dbt,$q,$s,$hbo);
}

sub rarefyAbundances {
    return if PBDBUtil::checkForBot();
    logRequest($s,$q);

    print $hbo->stdIncludes("std_page_top");
    Collection::rarefyAbundances($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayCollectionEcology	{
	return if PBDBUtil::checkForBot();
	logRequest($s,$q);
	print $hbo->stdIncludes("std_page_top");
	Collection::displayCollectionEcology($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub explainAEOestimate	{
	return if PBDBUtil::checkForBot();
	print $hbo->stdIncludes("std_page_top");
	Collection::explainAEOestimate($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_top");
}

# PS 11/7/2005
#
# Generic opinions earch handling form.
# Flow of this is a little complicated
#
sub submitOpinionSearch {
    print $hbo->stdIncludes("std_page_top");
    if ($q->param('taxon_name')) {
        $q->param('goal'=>'opinion');
        processTaxonSearch($dbt,$hbo,$q,$s);
    } else {
        $q->param('goal'=>'opinion');
        Opinion::displayOpinionChoiceForm($dbt,$s,$q);
    }
    print $hbo->stdIncludes("std_page_bottom");
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
    print $hbo->stdIncludes("std_page_top");
    processTaxonSearch($dbt, $hbo, $q, $s);
    print $hbo->stdIncludes("std_page_bottom");
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
    
    my @results = TaxonInfo::getTaxa($dbt,\%options,['*']);
        
    # If there were no matches, present the new taxon entry form immediately
    # We're adding a new taxon
    if (scalar(@results) == 0) {
        if ($q->param('goal') eq 'authority') {
            # Try to see if theres any near matches already existing in the DB
            if ($q->param('taxon_name')) {
                my @typoResults = ();
                unless ($q->param("skip_typo_check")) {
                    my ($g,$sg,$sp) = Taxon::splitTaxon($q->param('taxon_name'));
                # give a free pass if the name is "plausible" because its
                #  parts all exist in the authorities table JA 21.7.08
                # disaster could ensue if the parts are actually typos,
                #  but let's cross our fingers
                # perhaps getTaxa could be adapted for this purpose, but
                #  it's a pretty simple piece of code
                    my $sql = "SELECT taxon_name tn FROM authorities WHERE taxon_name='$g' OR taxon_name LIKE '$g %' OR taxon_name LIKE '% ($sg) %' OR taxon_name LIKE '% $sp'";
                    my @partials = @{$dbt->getData($sql)};
                    my ($oldg,$oldsg,$oldsp);
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
                        $sql = "SELECT count(*) c FROM occurrences WHERE genus_name LIKE ".$dbh->quote($g);
                        if ($sg) {
                            $sql .= " AND subgenus_name LIKE ".$dbh->quote($sg);
                        }
                        if ($sp) {
                            $sql .= " AND species_name LIKE ".$dbh->quote($sp);
                        }
                        my $exists_in_occ = ${$dbt->getData($sql)}[0]->{c};
                        unless ($exists_in_occ) {
                            my @results = keys %{TypoChecker::taxonTypoCheck($dbt,$q->param('taxon_name'))};
                            my ($g,$sg,$sp) = Taxon::splitTaxon($q->param('taxon_name'));
                            foreach my $typo (@results) {
                                my ($t_g,$t_sg,$t_sp) = Taxon::splitTaxon($typo);
                                if ($sp && !$t_sp) {
                                    $typo .= " $sp";
                                }
                                push @typoResults, $typo;
                            }
                        }
                    }
                }

                if (@typoResults) {
                    print "<div align=\"center\"><table><tr><td align=\"center\">";
    		        print "<p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">'<i>" . $q->param('taxon_name') . "</i>' was not found</p>\n<br>\n";
                    print "<div class=\"displayPanel medium\" style=\"padding: 1em;\">\n";
                    print "<p><div align=\"left\"><ul>";
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
                    print "<li><a href=\"$WRITE_URL?action=submitTaxonSearch&goal=authority&taxon_name=".$q->param('taxon_name')."&amp;skip_typo_check=1\">None of the above</a> - create a <b>new</b> taxon record";
                    print "</ul>";

                    print "<div align=left class=small style=\"width: 500\">";
                    print "<p>The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, some approximate matches were found and are listed above.  If none of the names above match, please enter a new taxon record";
                    print "</div></p>";
                    print "</div>";
                    print "</td></tr></table></div>";
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
                print "<div align=\"center\"><table><tr><td align=\"center\">";
    		    print "<p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">'<i>" . $q->param('taxon_name') . "</i>' was not found</p>\n<br>\n";
                print "<div class=\"displayPanel medium\" style=\"padding: 1em;\">\n";
                print "<p><div align=\"left\"><ul>";
                foreach my $row (@typoResults) {
                    my $full_row = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['*']);
                    my ($name,$authority) = Taxon::formatTaxon($dbt,$full_row,'return_array'=>1);
                    print "<li><a href=\"$WRITE_URL?action=$next_action&amp;goal=$goal&amp;taxon_name=$full_row->{taxon_name}&amp;taxon_no=$row->{taxon_no}\">$name</a>$authority</li>";
                }
                print "</ul>";

                print "<div align=left class=small style=\"width: 500\">";
                if ( $#typoResults > 0 )	{
                    print "<p>The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, some approximate matches were found and are listed above.  If none of them are what you're looking for, please <a href=\"$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=".$q->param('taxon_name')."\">enter a new authority record</a> first.";
                } else	{
                    print "<p>The taxon '" . $q->param('taxon_name') . "' doesn't exist in the database.  However, an approximate match was found and is listed above.  If it is not what you are looking for, please <a href=\"$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=".$q->param('taxon_name')."\">enter a new authority record</a> first.";
                }
                print "</div></p>";
                print "</div>";
                print "</td></tr></table></div>";
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
        print "<table><tr><td align=\"center\">";
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
		print "<div class=\"displayPanel medium\" style=\"padding: 1em; padding-right: 2em; margin-top: -1em;\">";
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
            print "- create a <b>new</b> taxon record</li>\n";
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

        print "</div>";
        print "</td></tr></table>";
		print "</div>\n";
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
    print $hbo->stdIncludes("std_page_top");
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    print Person::makeAuthEntJavascript($dbt);
    print $hbo->populateHTML('search_authority_form',\%vars);

    print $hbo->stdIncludes("std_page_bottom");
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
    print $hbo->stdIncludes("std_page_top");
	Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes("std_page_bottom");
}


sub submitAuthorityForm {
    print $hbo->stdIncludes("std_page_top");
	Taxon::submitAuthorityForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayClassificationTableForm {
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first.");
		exit;
	} 
    if (!$s->get('reference_no')) {
        $s->enqueue('action=displayClassificationTableForm');
		displaySearchRefs("You must choose a reference before adding new taxa" );
		exit;
	}
    print $hbo->stdIncludes("std_page_top");
	FossilRecord::displayClassificationTableForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayClassificationUploadForm {
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first.");
		exit;
	} 
    if (!$s->get('reference_no')) {
        $s->enqueue('action=displayClassificationUploadForm');
		displaySearchRefs("You must choose a reference before adding new taxa" );
		exit;
	}
    print $hbo->stdIncludes("std_page_top");
	FossilRecord::displayClassificationUploadForm($dbt, $hbo, $s, $q);	
    print $hbo->stdIncludes("std_page_bottom");
}


sub submitClassificationTableForm {
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first.");
		exit;
	} 
    print $hbo->stdIncludes("std_page_top");
	FossilRecord::submitClassificationTableForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes("std_page_bottom");
}

sub submitClassificationUploadForm {
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first.");
		exit;
	} 
    print $hbo->stdIncludes("std_page_top");
	FossilRecord::submitClassificationUploadForm($dbt,$hbo, $s, $q);
    print $hbo->stdIncludes("std_page_bottom");
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
    print $hbo->stdIncludes("std_page_top");
    my %vars = $q->Vars();
    $vars{'authorizer_me'} = $s->get('authorizer_reversed');

    print Person::makeAuthEntJavascript($dbt);
    print $hbo->populateHTML('search_opinion_form', \%vars);
    print $hbo->stdIncludes("std_page_bottom");
}

# PS 01/24/2004
# Changed from displayOpinionList to just be a stub for function in Opinion module
# Step 2 in our opinion editing process. now that we know the taxon, select an opinion
sub displayOpinionChoiceForm {
    if ($q->param('use_reference') eq 'new') {
        $s->setReferenceNo(0);
    }
	print $hbo->stdIncludes("std_page_top");
    Opinion::displayOpinionChoiceForm($dbt,$s,$q);
	print $hbo->stdIncludes("std_page_bottom");
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
	
	print $hbo->stdIncludes("std_page_top");
	Opinion::displayOpinionForm($dbt, $hbo, $s, $q);
	print $hbo->stdIncludes("std_page_bottom");
}

sub submitOpinionForm {
	print $hbo->stdIncludes("std_page_top");
	Opinion::submitOpinionForm($dbt,$hbo, $s, $q);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayUntangleSearchForm {
	print $hbo->stdIncludes("std_page_top");
	my %vars = $q->Vars();
	print $hbo->populateHTML('search_untangle_form', \%vars);
	print $hbo->stdIncludes("std_page_bottom");
}

sub displayUntangleForm {
	my $error = shift;
	print $hbo->stdIncludes("std_page_top");
	Opinion::displayUntangleForm($dbt,$hbo, $s, $q, $error);
	print $hbo->stdIncludes("std_page_bottom");
}

sub processUntangleForm {
	print $hbo->stdIncludes("std_page_top");
	Opinion::processUntangleForm($dbt,$hbo, $s, $q);
	print $hbo->stdIncludes("std_page_bottom");
}

sub submitTypeTaxonSelect {
	print $hbo->stdIncludes("std_page_top");
	Taxon::submitTypeTaxonSelect($dbt, $s, $q);
	print $hbo->stdIncludes("std_page_bottom");
}

## END Opinion stuff
##############

##############
## Editing list stuff
sub displayPermissionListForm {
	print $hbo->stdIncludes("std_page_top");
    Permissions::displayPermissionListForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub submitPermissionList {
	print $hbo->stdIncludes("std_page_top");
    Permissions::submitPermissionList($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
} 

sub submitHeir{
	print $hbo->stdIncludes("std_page_top");
    Permissions::submitHeir($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
} 

##############
## Occurrence misspelling stuff

sub searchOccurrenceMisspellingForm {
    if (!$s->isDBMember()) {
        # have to be logged in
        $s->enqueue("action=searchOccurrenceMisspellingForm" );
        displayLoginPage( "Please log in first." );
        exit;
    } 
	print $hbo->stdIncludes("std_page_top");
	TypoChecker::searchOccurrenceMisspellingForm ($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub occurrenceMisspellingForm {
	print $hbo->stdIncludes("std_page_top");
	TypoChecker::occurrenceMisspellingForm ($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}

sub submitOccurrenceMisspelling {
	print $hbo->stdIncludes("std_page_top");
	TypoChecker::submitOccurrenceMisspelling($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
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
sub randomTaxonInfo{
    return if PBDBUtil::checkForBot();

    my $dbh = $dbt->dbh;
    my $sql;
    my $lft;
    my $rgt;
    if ( $q->param('taxon_name') =~ /^[A-Za-z]/ )	{
        my $sql = "SELECT lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name=".$dbh->quote($q->param('taxon_name'))." ORDER BY rgt-lft DESC";
        my $taxref = ${$dbt->getData($sql)}[0];
        if ( $taxref )	{
            $lft = $taxref->{lft};
            $rgt = $taxref->{rgt};
        }
    } elsif ( $q->param('common_name') =~ /^[A-Za-z]/ )	{
        my $sql = "SELECT lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND common_name=".$dbh->quote($q->param('common_name'))." ORDER BY rgt-lft DESC";
        my $taxref = ${$dbt->getData($sql)}[0];
        if ( $taxref )	{
            $lft = $taxref->{lft};
            $rgt = $taxref->{rgt};
        }
    }
    my @orefs;
    if ( $lft > 0 && $rgt > 0 )	{
        my $morewhere;
        if ( $q->param('type_body_part') )	{
            $morewhere = " AND type_body_part='".$q->param('type_body_part')."'";
        }
        if ( $q->param('preservation') )	{
            $morewhere .= " AND preservation='".$q->param('preservation')."'";
        }
        $sql = "SELECT DISTINCT(o.taxon_no) taxon_no FROM occurrences o,authorities a,$TAXA_TREE_CACHE t WHERE o.taxon_no=a.taxon_no AND taxon_rank='species' AND a.taxon_no=t.taxon_no AND (lft BETWEEN $lft AND $rgt) AND (rgt BETWEEN $lft AND $rgt) $morewhere";
        @orefs = @{$dbt->getData($sql)};
    }
    if ( $q->param('match') eq "all" )	{
        my @taxa;
        push @taxa , $_->{taxon_no} foreach @orefs;
        return \@taxa;
    } else	{
        my $x = int(rand($#orefs + 1));
        $q->param('taxon_no' => $orefs[$x]->{taxon_no});
        # DON'T SET THIS TO 1
        #$q->param('is_real_user' => 1);
        # infinite loops are bad
        $q->param('match' => '');
        if ( $q->param('action') eq "checkTaxonInfo" )	{
            return;
        } else	{
            checkTaxonInfo();
        }
    }
}

sub beginTaxonInfo{
    print $hbo->stdIncludes( "std_page_top" );
    if ($IS_FOSSIL_RECORD) {
        FossilRecord::displaySearchTaxaForm($dbt,$q,$s,$hbo);
    } else {
        TaxonInfo::searchForm($hbo, $q);
    }
    print $hbo->stdIncludes("std_page_bottom");
}

sub checkTaxonInfo {
    logRequest($s,$q);
    if ( $q->param('match') eq "all" )	{
        print $hbo->stdIncludes( "std_page_top" );
        $q->param('taxa' => @{randomTaxonInfo()} );
        if ( ! $q->param('taxa') )	{
            TaxonInfo::searchForm($hbo,$q,1);
        } else	{
            TaxonInfo::checkTaxonInfo($q, $s, $dbt, $hbo);
        }
        print $hbo->stdIncludes("std_page_bottom");
        exit;
    } elsif ( $q->param('match') eq "random" )	{
        # infinite loops are bad
        randomTaxonInfo();
        $q->param('match' => '');
    }
    print $hbo->stdIncludes( "std_page_top" );
    if ($IS_FOSSIL_RECORD) {
         FossilRecord::submitSearchTaxaForm($dbt,$q,$s,$hbo);
    } else {
        TaxonInfo::checkTaxonInfo($q, $s, $dbt, $hbo);
    }
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayTaxonInfoResults {
    print $hbo->stdIncludes( "std_page_top" );
	TaxonInfo::displayTaxonInfoResults($dbt,$s,$q);
    print $hbo->stdIncludes("std_page_bottom");
}

## END Taxon Info Stuff
##############

sub beginFirstAppearance	{
	print $hbo->stdIncludes( "std_page_top" );
	TaxonInfo::beginFirstAppearance($hbo, $q, '');
	print $hbo->stdIncludes( "std_page_bottom" );
}

sub displayFirstAppearance	{
	print $hbo->stdIncludes( "std_page_top" );
	TaxonInfo::displayFirstAppearance($q, $s, $dbt, $hbo);
	print $hbo->stdIncludes( "std_page_bottom" );
}

sub displaySearchFossilRecordTaxaForm {
    print $hbo->stdIncludes( "std_page_top" );
    print $hbo->stdIncludes("std_page_bottom");
}

sub submitSearchFossilRecordTaxa {
    logRequest($s,$q);
    print $hbo->stdIncludes( "std_page_top" );
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayFossilRecordCurveForm {
    print $hbo->stdIncludes( "std_page_top" );
	FossilRecord::displayFossilRecordCurveForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}
sub submitFossilRecordCurveForm {
    print $hbo->stdIncludes( "std_page_top" );
	FossilRecord::submitFossilRecordCurveForm($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

### End Module Navigation
##############


##############
## Scales stuff JA 7.7.03
sub startScale	{
    require Scales;
    print $hbo->stdIncludes("std_page_top");
	Scales::startSearchScale($dbt, $s, $WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}
sub processShowForm	{
    require Scales;
    print $hbo->stdIncludes("std_page_top");
	Scales::processShowEditForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}
sub processViewScale	{
    require Scales;
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
	Scales::processViewTimeScale($dbt, $hbo, $q, $s);
    print $hbo->stdIncludes("std_page_bottom");
}
sub processEditScale	{
    require Scales;
    print $hbo->stdIncludes("std_page_top");
	Scales::processEditScaleForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}
sub displayTenMyBinsDebug {
    return if PBDBUtil::checkForBot();
    require Scales;
    print $hbo->stdIncludes("std_page_top");
    Scales::displayTenMyBinsDebug($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}
sub submitSearchInterval {
    require Scales;
    print $hbo->stdIncludes("std_page_top");
    Scales::submitSearchInterval($dbt, $hbo, $q);
    print $hbo->stdIncludes("std_page_bottom");
}
sub displayInterval {
    require Scales;
    print $hbo->stdIncludes("std_page_top");
    Scales::displayInterval($dbt, $hbo, $q);
    print $hbo->stdIncludes("std_page_bottom");
}
sub displayTenMyBins {
    return if PBDBUtil::checkForBot();
    require Scales;
    print $hbo->stdIncludes("std_page_top");
    Scales::displayTenMyBins($dbt,$q,$s,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}
sub displayFullScale {
    require Scales;
    print $hbo->stdIncludes("std_page_top");
    Scales::displayFullScale($dbt, $hbo);
    print $hbo->stdIncludes("std_page_bottom");
}
sub dumpAllIntervals {
    return if PBDBUtil::checkForBot();
    my $t = new TimeLookup($dbt);
    $t->getBoundaries();
    my $dmp = $t->_dumpGraph();
    $dmp =~ s/(Interval (\d+):)/<a name="i$2">$1<\/a>/g;
    $dmp =~ s/(\d{1,3})(:\w+\W)/<a href="#i$1">$1<\/a>$2/g;
    print "<hr><pre>AAAAAAAA";
    print $dmp;
    print "</pre>";
}

## END Scales stuff
##############


##############
## Images stuff
sub startImage{
    my $goal='image';
    my $page_title ='Search for the taxon with an image to be added';

    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayLoadImageForm{
    print $hbo->stdIncludes("std_page_top");
	Images::displayLoadImageForm($dbt, $q, $s);
    print $hbo->stdIncludes("std_page_bottom");
}

sub processLoadImage{
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first");
		exit;
	} 
    print $hbo->stdIncludes("std_page_top");
	Images::processLoadImage($dbt, $q, $s);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayImage {
    if ($q->param("display_header") eq 'NO') {
        print $hbo->stdIncludes("blank_page_top") 
    } else {
        print $hbo->stdIncludes("std_page_top") 
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
        print $hbo->stdIncludes("std_page_bottom"); 
    }
}
## END Image stuff
##############


##############
## Ecology stuff
sub startStartEcologyTaphonomySearch{
    my $goal='ecotaph';
    my $page_title ='Search for the taxon you want to describe';

    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print $hbo->stdIncludes("std_page_bottom");
}
sub startStartEcologyVertebrateSearch{
    my $goal='ecovert';
    my $page_title ='Search for the taxon you want to describe';

    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print $hbo->stdIncludes("std_page_bottom");
}
sub startPopulateEcologyForm	{
    print $hbo->stdIncludes("std_page_top");
	Ecology::populateEcologyForm($dbt, $hbo, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}
sub startProcessEcologyForm	{
    print $hbo->stdIncludes("std_page_top");
	Ecology::processEcologyForm($dbt, $q, $s, $WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}
## END Ecology stuff
##############

##############
## Specimen measurement stuff
sub displaySpecimenSearchForm {
    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('search_specimen_form',[],[]);
    print $hbo->stdIncludes("std_page_bottom");
}

sub submitSpecimenSearch{
    print $hbo->stdIncludes("std_page_top");
    Measurement::submitSpecimenSearch($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displaySpecimenList {
    print $hbo->stdIncludes("std_page_top");
    Measurement::displaySpecimenList($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}

sub populateMeasurementForm{
    print $hbo->stdIncludes("std_page_top");
    Measurement::populateMeasurementForm($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}

sub processMeasurementForm {
    print $hbo->stdIncludes("std_page_top");
    Measurement::processMeasurementForm($dbt,$hbo,$q,$s,$WRITE_URL);
    print $hbo->stdIncludes("std_page_bottom");
}

## END Specimen measurement stuff
##############



##############
## Strata stuff
sub displayStrata {
    require Strata;
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    Strata::displayStrata($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displaySearchStrataForm {
    require Strata;
    print $hbo->stdIncludes("std_page_top");
    Strata::displaySearchStrataForm($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}  

sub displaySearchStrataResults{
    require Strata;
    print $hbo->stdIncludes("std_page_top");
    Strata::displaySearchStrataResults($q,$s,$dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}  
## END Strata stuff
##############

##############
## PrintHierarchy stuff
sub startStartPrintHierarchy	{
    require PrintHierarchy;
    print $hbo->stdIncludes("std_page_top");
	PrintHierarchy::startPrintHierarchy($hbo, $s);
    print $hbo->stdIncludes("std_page_bottom");
}
sub startProcessPrintHierarchy	{
    return if PBDBUtil::checkForBot();
    require PrintHierarchy;
    logRequest($s,$q);
    
    print $hbo->stdIncludes("std_page_top");
	PrintHierarchy::processPrintHierarchy($q, $s, $dbt, $hbo);
    print $hbo->stdIncludes("std_page_bottom");
}
## END PrintHierarchy stuff
##############

##############
## SanityCheck stuff
sub displaySanityForm	{
	my $error_message = shift;
	print $hbo->stdIncludes("std_page_top");
	print $hbo->populateHTML('sanity_check_form',$error_message);
	print $hbo->stdIncludes("std_page_bottom");
}
sub startProcessSanityCheck	{
	return if PBDBUtil::checkForBot();
	require SanityCheck;
	logRequest($s,$q);
    
	print $hbo->stdIncludes("std_page_top");
	SanityCheck::processSanityCheck($q, $dbt, $hbo, $s);
	print $hbo->stdIncludes("std_page_bottom");
}
## END SanityCheck stuff
##############

##############
## PAST stuff
sub PASTQueryForm {
    require PAST;
    print $hbo->stdIncludes("std_page_top");
    PAST::queryForm($dbt,$q,$hbo,$s);
    print $hbo->stdIncludes("std_page_bottom");
}
sub PASTQuerySubmit {
    require PAST;
    print $hbo->stdIncludes("std_page_top");
    PAST::querySubmit($dbt,$q,$hbo,$s);
    print $hbo->stdIncludes("std_page_bottom");
}
## End PAST stuff
##############


sub displayOccurrenceAddEdit {
    my $dbh = $dbt->dbh;
	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first.",'displayOccurrenceAddEdit');
		exit;
	} 
    if (! $s->get('reference_no')) {
        $s->enqueue($q->query_string());
        displaySearchRefs("Please select a reference first"); 
        exit;
	} 

	my $collection_no = $q->param("collection_no");
    # No collection no is passed in, search for one
	if ( ! $collection_no ) { 
        $q->param('type'=>'edit_occurrence');
		displaySearchColls();
        exit;
    }

	# Grab the collection name for display purposes JA 1.10.02
	my $sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $collection_name = ${$sth->fetchrow_arrayref()}[0];
	$sth->finish();

	print $hbo->stdIncludes( "std_page_top" );

	# get the occurrences right away because we need to make sure there
	#  aren't too many to be displayed
	$sql = "SELECT * FROM occurrences WHERE collection_no=$collection_no ORDER BY occurrence_no ASC";
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
		print $hbo->stdIncludes("std_page_bottom");
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
    my @optional = ('subgenera','genus_and_species_only','abundances','plant_organs');
	print $hbo->populateHTML('js_occurrence_checkform');

	print qq|<form method=post action="$WRITE_URL" onSubmit='return checkForm();'>\n|;
	print qq|<input name="action" value="processEditOccurrences" type=hidden>\n|;
	print qq|<input name="list_collection_no" value="$collection_no" type=hidden>\n|;
	print "<table>";

    my $header_vars = {
        'collection_no'=>$collection_no,
        'collection_name'=>$collection_name,
    };
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

        my $sql = "SELECT * FROM reidentifications WHERE occurrence_no=" .  $occ_row->{'occurrence_no'};
        my @reid_rows = @{$dbt->getData($sql)};
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
    my $blank = {
        'collection_no'=>$collection_no,
        'reference_no'=>$s->get('reference_no'),
        'occurrence_no'=>-1,
        'taxon_name'=>$pref{'species_name'}
    };
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
	print qq|<center><p><input type=submit value="Save changes">|;
    printf " to collection %s's taxonomic list</p></center>\n",$collection_no;
	print "</form>";

	print $hbo->stdIncludes("std_page_bottom");
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
		displayLoginPage( "Please log in first.",'displayOccurrenceTable' );
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
            qq|<a target="_blank" href="$READ_URL?action=displayCollectionDetails&amp;collection_no=$collection_no"><img border="0" src="/public/collection_labels/$collection_no.png" alt="$collection_name"/></a>|.
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

sub processOccurrenceTable {

    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first." );
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
		displayLoginPage( "Please log in first." );
		exit;
	}
                                
	# Get the names of all the fields coming in from the form.
	my @param_names = $q->param();

	# list of the number of rows to possibly update.
	my @rowTokens = $q->param('row_token');

	# list of required fields
	my @required_fields = ("collection_no", "taxon_name", "reference_no");
	my @warnings = ();
	my @occurrences = ();
	my @occurrences_to_delete = ();

        my @genera = ();
        my @subgenera = ();
        my @species = ();
        my @latin_names = ();
        my @resos = ("\?","aff\.","cf\.","ex gr\.","n\. gen\.","n\. subgen\.","n\. sp\.","sensu lato");

	my @matrix;
	my $collection_no;

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


        if ( $fields{'collection_no'} > 0 )	{
            $collection_no = $fields{'collection_no'}
        }

	%{$matrix[$i]} = %fields;

	# end of first pass
	}

	# check for duplicates JA 2.4.08
	# this section replaces the old occurrence-by-occurrence check that
	#  used checkDuplicates; it's much faster and uses more lenient
	#  criteria because isolated duplicates are handled by the JavaScript
	my $sql ="SELECT genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name,taxon_no FROM occurrences WHERE collection_no=" . $collection_no;
	my @occrefs = @{$dbt->getData($sql)};
	my %taxon_no;
	if ( $#occrefs > 0 )	{
		my $newrows;
		my %newrow;
		for (my $i = 0;$i < @rowTokens; $i++)	{
			if ( $matrix[$i]{'genus_name'} =~ /^[A-Z][a-z]*$/ && $matrix[$i]{'occurrence_no'} == -1 )	{
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
				push @warnings , "Nothing was entered or updated because all the new occurrences were duplicates";
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
		if ( $fields{'genus_name'} eq "" && $fields{'occurrence_no'} < 1 )	{
			next;
		}
        	if ( $fields{'reference_no'} !~ /^\d+$/ && $fields{'genus'} =~ /[A-Za-z]/ )	{
            		next; 
        	}
        	if ( $fields{'collection_no'} !~ /^\d+$/ )	{
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
			my @dupe_refs = @{$dbt->getData($sql)};
			if ( @dupe_refs )	{
				$dupe_colls{$_->{'taxon_no'}} .= ", ".$_->{'collection_no'} foreach @dupe_refs;
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

	if ( $fields{'genus_name'} eq "" && $fields{'occurrence_no'} < 1 )	{
		next;
	}

		# check that all required fields have a non empty value
        if ( $fields{'reference_no'} !~ /^\d+$/ && $fields{'genus'} =~ /[A-Za-z]/ )	{
            push @warnings, "There is no reference number for row $rowno, so it was skipped";
            next; 
        }
        if ( $fields{'collection_no'} !~ /^\d+$/ )	{
            push @warnings, "There is no collection number for row $rowno, so it was skipped";
            next; 
        }
	my $taxon_name = Collection::formatOccurrenceTaxonName(\%fields);

        if ($fields{'genus_name'} =~ /^\s*$/) {
            if ($fields{'occurrence_no'} =~ /^\d+$/ && $fields{'reid_no'} != -1) {
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

        if ($fields{'occurrence_no'} =~ /^\d+$/ && $fields{'occurrence_no'} > 0 &&
            (($fields{'reid_no'} =~ /^\d+$/ && $fields{'reid_no'} > 0) || ($fields{'reid_no'} == -1))) {
            # We're either updating or inserting a reidentification
            my $sql = "SELECT reference_no FROM occurrences WHERE occurrence_no=$fields{'occurrence_no'}";
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
            $fields{'occurrence_no'} =~ /^\d+$/ && $fields{'occurrence_no'} > 0) {

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
                    unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{'collection_no'}, $fields{'reference_no'})){
                           Collection::setSecondaryRef($dbt,$fields{'collection_no'},$fields{'reference_no'});
                    }
                }
            }
            setMostRecentReID($dbt,$fields{'occurrence_no'});
            push @occurrences, $fields{'occurrence_no'};
        }
		# CASE 2: NEW REID
		elsif ($fields{'occurrence_no'} =~ /^\d+$/ && $fields{'occurrence_no'} > 0 && 
               $fields{'reid_no'} == -1) {
            # Check for duplicates
            my @keys = ("genus_reso","genus_name","subgenus_reso","subgenus_name","species_reso","species_name","occurrence_no");
            my %vars = map{$_,$dbh->quote($_)} @fields{@keys};

            my $dupe_id = $dbt->checkDuplicates("reidentifications", \%vars);

            if ( $dupe_id ) {
                push @warnings, "Row ". ($i + 1) ." may be a duplicate";
            }
#            } elsif ( $return ) {
            $dbt->insertRecord($s,'reidentifications',\%fields);

            unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{'collection_no'}, $fields{'reference_no'}))	{
               Collection::setSecondaryRef($dbt,$fields{'collection_no'}, $fields{'reference_no'});
            }
#            }
            setMostRecentReID($dbt,$fields{'occurrence_no'});
            push @occurrences, $fields{'occurrence_no'};
        }
		
		# CASE 3: UPDATE OCCURRENCE
		elsif($fields{'occurrence_no'} =~ /^\d+$/ && $fields{'occurrence_no'} > 0) {
            # CASE 3a: Delete record
            if ($fields{'genus_name'} =~ /^\s*$/) {
                # We push this onto an array for later processing because we can't delete an occurrence
                # With reids attached to it, so we want to let any reids be deleted first
                my $old_row = ${$dbt->getData("SELECT * FROM occurrences WHERE occurrence_no=$fields{'occurrence_no'}")}[0];
                push @occurrences_to_delete, [$fields{'occurrence_no'},Collection::formatOccurrenceTaxonName($old_row),$i];
            } 
            # CASE 3b: Update record
            else {
                # ugly hack: make sure taxon_no doesn't change unless
                #  genus_name or species_name did JA 1.4.04
                my $old_row = ${$dbt->getData("SELECT * FROM occurrences WHERE occurrence_no=$fields{'occurrence_no'}")}[0];
                die ("no reid for $fields{reid_no}") if (!$old_row);
                if ($old_row->{'genus_name'} eq $fields{'genus_name'} &&
                    $old_row->{'subgenus_name'} eq $fields{'subgenus_name'} &&
                    $old_row->{'species_name'} eq $fields{'species_name'}) {
                    delete $fields{'taxon_no'};
                }

                $dbt->updateRecord($s,'occurrences','occurrence_no',$fields{'occurrence_no'},\%fields);

                if($old_row->{'reference_no'} != $fields{'reference_no'}) {
                    dbg("calling setSecondaryRef (updating occurrence)<br>");
                    unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{'collection_no'}, $fields{'reference_no'}))	{
                           Collection::setSecondaryRef($dbt,$fields{'collection_no'}, $fields{'reference_no'});
                    }
                }
            }
            push @occurrences, $fields{'occurrence_no'};
		} 
        # CASE 4: NEW OCCURRENCE
        elsif ($fields{'occurrence_no'} == -1) {
            # previously, a check here for duplicates generated error
            #  messages but (1) was incredibly slow and (2) apparently
            #  didn't work, so there is now a batch check above instead

            my ($result, $occurrence_no) = $dbt->insertRecord($s,'occurrences',\%fields);
            if ($result && $occurrence_no =~ /^\d+$/) {
                push @occurrences, $occurrence_no;
            }

            unless(Collection::isRefPrimaryOrSecondary($dbt, $fields{'collection_no'}, $fields{'reference_no'}))	{
                   Collection::setSecondaryRef($dbt,$fields{'collection_no'}, $fields{'reference_no'});
            }
        }
    }

    # Now handle the actual deletion
    foreach my $o (@occurrences_to_delete) {
        my ($occurrence_no,$taxon_name,$line_no) = @{$o};
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
            $dbt->deleteRecord($s,'occurrences','occurrence_no',$occurrence_no);
        }
        
    }

	print $hbo->stdIncludes( "std_page_top" );

	print qq|<div align="center"><p class="large" style="margin-bottom: 1.5em;">|;
	my $sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
	print ${$dbt->getData($sql)}[0]->{'collection_name'};
	print "</p></div>\n\n";

	# Links to re-edit, etc
    my $links = "<div align=\"center\">";
    if ($q->param('form_source') eq 'new_reids_form') {
        # suppress link if there is clearly nothing more to reidentify
        #  JA 3.8.07
        # this won't work if exactly ten occurrences have been displayed
        if ( $#rowTokens < 9 )	{
            $links .= "<a href=\"$WRITE_URL?action=displayCollResults&type=reid&taxon_name=".$q->param('search_taxon_name')."&collection_no=".$q->param("list_collection_no")."&last_occ_num=".$q->param('last_occ_num')."\"><nobr>Reidentify next 10 occurrences</nobr></a> - ";
        }
        $links .= "<a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\"><nobr>Reidentify different occurrences</nobr></a>";
    } else {
        if ($q->param('list_collection_no')) {
            my $collection_no = $q->param("list_collection_no");
            $links .= "<a href=\"$WRITE_URL?action=startStartReclassifyOccurrences&collection_no=$collection_no\"><nobr>Reclassify these occurrences</nobr></a> - ";
            $links .= "<a href=\"$WRITE_URL?action=displayCollectionForm&collection_no=$collection_no\"><nobr>Edit the collection record</nobr></a><br>";
        }
        $links .= "<nobr><a href=\"$WRITE_URL?action=displaySearchCollsForAdd&type=add\">Add</a> or ";
        $links .= "<a href=\"$WRITE_URL?action=displaySearchColls&type=edit\">edit another collection</a> - </nobr>";
        $links .= "<nobr><a href=\"$WRITE_URL?action=displaySearchColls&type=edit_occurrence\">Add/edit</a> or ";
        $links .= "<a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\">reidentify different occurrences</a></nobr>";
    }
    $links .= "</div><br>";

	# for identifying unrecognized (new to the db) genus/species names.
	# these are the new taxon names that the user is trying to enter, do this before insert
	my @new_genera = TypoChecker::newTaxonNames($dbt,\@genera,'genus_name');
	my @new_subgenera =  TypoChecker::newTaxonNames($dbt,\@subgenera,'subgenus_name');
	my @new_species =  TypoChecker::newTaxonNames($dbt,\@species,'species_name');

	print qq|<div style="padding-left: 1em; padding-right: 1em;"|;

    if ($q->param('list_collection_no')) {
        my $collection_no = $q->param("list_collection_no");
        my $coll = ${$dbt->getData("SELECT collection_no,reference_no FROM collections WHERE collection_no=$collection_no")}[0];
    	print Collection::buildTaxonomicList($dbt,$hbo,$s,{'collection_no'=>$collection_no, 'hide_reference_no'=>$coll->{'reference_no'},'new_genera'=>\@new_genera, 'new_subgenera'=>\@new_subgenera, 'new_species'=>\@new_species, 'do_reclassify'=>1, 'warnings'=>\@warnings, 'save_links'=>$links });
    } else {
    	print Collection::buildTaxonomicList($dbt,$hbo,$s,{'occurrence_list'=>\@occurrences, 'new_genera'=>\@new_genera, 'new_subgenera'=>\@new_subgenera, 'new_species'=>\@new_species, 'do_reclassify'=>1, 'warnings'=>\@warnings, 'save_links'=>$links });
    }

    print "\n</div>\n<br>\n";

	print $hbo->stdIncludes("std_page_bottom");
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
		displayLoginPage( "Please log in first.",'displayReIDCollsAndOccsSearchForm');
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
	print $hbo->stdIncludes( "std_page_top" );
    print PBDBUtil::printIntervalsJava($dbt,1);
    print Person::makeAuthEntJavascript($dbt);
	print $hbo->populateHTML('search_occurrences_form',\%vars);
	print $hbo->stdIncludes("std_page_bottom");
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

	my $onFirstPage = 0;
	my $printCollDetails = 0;

	print $hbo->stdIncludes( "std_page_top" );
	print $hbo->populateHTML('js_occurrence_checkform');
    
	my $lastOccNum = $q->param('last_occ_num');
	if ( ! $lastOccNum ) { 
		$lastOccNum = -1;
		$onFirstPage = 1; 
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
    if (@colls) {
		$printCollectionDetails = 1;
		push @where, "collection_no IN(".join(',',@colls).")";
        my ($genus,$subgenus,$species) = Taxon::splitTaxon($q->param('taxon_name'));
        my $names = $dbh->quote($genus);
        if ($subgenus) {
            $names .= ", ".$dbh->quote($subgenus);
        }
        push @where, "(genus_name IN ($names) OR subgenus_name IN ($names))";
        push @where, "species_name LIKE ".$dbh->quote($species) if ($species);
	} elsif ($collection_no) {
		push @where, "collection_no=$collection_no";
	} else {
        push @where, "0=1";
    }

	push @where, "occurrence_no > $lastOccNum";

	# some occs are out of primary key order, so order them JA 26.6.04
    my $sql = "SELECT * FROM occurrences WHERE ".join(" AND ",@where).
           " ORDER BY occurrence_no LIMIT 11";

	dbg("$sql<br>");
    my @results = @{$dbt->getData($sql)};

	my $rowCount = 0;
	my %pref = $s->getPreferences();
    my @optional = ('subgenera','genus_and_species_only','abundances','plant_organs','species_name');
    if (@results) {
        my $header_vars = {
            'ref_string'=>$refString,
            'search_taxon_name'=>$taxon_name,
            'list_collection_no'=>$collection_no
        };
        $header_vars->{$_} = $pref{$_} for (@optional);
		print $hbo->populateHTML('reid_header_row', $header_vars);

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
                my $details = " <a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$row->{'collection_no'}\">$row->{'collection_no'}</a>"." ".$collRow{'collection_name'};
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

            $lastOccNum = $row->{'occurrence_no'};
        }
    }

	print "</table>\n";
	if ($rowCount > 0)	{
		print qq|<center><p><input type=submit value="Save reidentifications"></center></p>\n|;
		print qq|<input type="hidden" name="last_occ_num" value="$lastOccNum">\n|;
	} else	{
		print "<center><p class=\"pageTitle\">Sorry! No matches were found</p></center>\n";
		print "<p align=center><b>Please <a href=\"$WRITE_URL?action=displayReIDCollsAndOccsSearchForm\">try again</a> with different search terms</b></p>\n";
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
        print qq|&last_occ_num=$lastOccNum">Skip to the next 10 occurrences</a></b>\n|;
		print "</td></tr>\n";
		print "<tr><td class=small align=center><i>Warning: if you go to the next page without saving, your changes will be lost</i></td>\n";
	}

	print "</tr>\n</table><p>\n";

	print $hbo->stdIncludes("std_page_bottom");
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
        if (@results) {
            $sql = "UPDATE reidentifications SET modified=modified, most_recent='YES' WHERE reid_no=".$results[0]->{'reid_no'};
            my $result = $dbh->do($sql);
            dbg("set most recent: $sql");
            if (!$result) {
                carp "Error setting most recent reid to YES for reid_no=$results[0]->{reid_no}";
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
sub displayEnterers {
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    print Person::displayEnterers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayAuthorizers {
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    print Person::displayAuthorizers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayFeaturedAuthorizers {
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    print Person::displayFeaturedAuthorizers($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayInstitutions {
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    print Person::displayInstitutions($dbt,$IS_FOSSIL_RECORD);
    print $hbo->stdIncludes("std_page_bottom");
}

# ------------------------ #
# Confidence Intervals JSM #
# ------------------------ #

sub displaySearchSectionResults{
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    Confidence::displaySearchSectionResults($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displaySearchSectionForm{
    require Confidence;
    print $hbo->stdIncludes("std_page_top");
    Confidence::displaySearchSectionForm($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayTaxaIntervalsForm{
    require Confidence;
    print $hbo->stdIncludes("std_page_top");
    Confidence::displayTaxaIntervalsForm($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayTaxaIntervalsResults{
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
    print $hbo->stdIncludes("std_page_top");
    Confidence::displayTaxaIntervalsResults($q, $s, $dbt,$hbo);
    print $hbo->stdIncludes("std_page_bottom");
}

sub buildListForm {
    return if PBDBUtil::checkForBot();
    require Confidence;
    print $hbo->stdIncludes("std_page_top");
    Confidence::buildList($q, $s, $dbt,$hbo,{});
    print $hbo->stdIncludes("std_page_bottom");
}

sub displayStratTaxaForm{
    return if PBDBUtil::checkForBot();
    require Confidence;
    print $hbo->stdIncludes("std_page_top");
    Confidence::displayStratTaxa($q, $s, $dbt);
    print $hbo->stdIncludes("std_page_bottom");
}

sub showOptionsForm {
    return if PBDBUtil::checkForBot();
    require Confidence;
	print $hbo->stdIncludes("std_page_top");
	Confidence::optionsForm($q, $s, $dbt);
	print $hbo->stdIncludes("std_page_bottom");
}

sub calculateTaxaInterval {
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
	print $hbo->stdIncludes("std_page_top");
	Confidence::calculateTaxaInterval($q, $s, $dbt);
	print $hbo->stdIncludes("std_page_bottom");
}

sub calculateStratInterval {
    return if PBDBUtil::checkForBot();
    require Confidence;
    logRequest($s,$q);
	print $hbo->stdIncludes("std_page_top");
	Confidence::calculateStratInterval($q, $s, $dbt);
	print $hbo->stdIncludes("std_page_bottom");
}

## Cladogram stuff

sub displayCladeSearchForm	{
    print $hbo->stdIncludes("std_page_top");
    print $hbo->populateHTML('search_clade_form');
    print $hbo->stdIncludes("std_page_bottom");

	#print $hbo->stdIncludes("std_page_top");
    #Cladogram::displayCladeSearchForm($dbt,$q,$s,$hbo);
	#print $hbo->stdIncludes("std_page_bottom");
}
#sub processCladeSearch	{
#	print $hbo->stdIncludes("std_page_top");
#    Cladogram::processCladeSearch($dbt,$q,$s,$hbo);
#	print $hbo->stdIncludes("std_page_bottom");
#}
sub displayCladogramChoiceForm	{
	print $hbo->stdIncludes("std_page_top");
    Cladogram::displayCladogramChoiceForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}
sub displayCladogramForm	{
	print $hbo->stdIncludes("std_page_top");
    Cladogram::displayCladogramForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}
sub submitCladogramForm {
	print $hbo->stdIncludes("std_page_top");
    Cladogram::submitCladogramForm($dbt,$q,$s,$hbo);
	print $hbo->stdIncludes("std_page_bottom");
}
sub drawCladogram	{
	print $hbo->stdIncludes("std_page_top");
    my $cladogram_no = $q->param('cladogram_no');
    my $force_redraw = $q->param('force_redraw');
    my ($pngname, $caption, $taxon_name) = Cladogram::drawCladogram($dbt,$cladogram_no,$force_redraw);
    if ($pngname) {
        print qq|<div align="center"><h3>$taxon_name</h3>|;
        print qq|<img src="/public/cladograms/$pngname"><br>$caption|;
        print qq|</div>|;
    }
	print $hbo->stdIncludes("std_page_bottom");
}


# Displays taxonomic opinions and names associated with a reference_no
# PS 10/25/2004
sub displayTaxonomicNamesAndOpinions {
    print $hbo->stdIncludes( "std_page_top" );
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
    print $hbo->stdIncludes("std_page_bottom");
}



sub logRequest {
    my ($s,$q) = @_;
    
    if ( $HOST_URL !~ /paleobackup\.nceas\.ucsb\.edu/ && $HOST_URL !~ /paleodb\.org/ )  {
        return;
    }
    my $status = open LOG, ">>/var/log/httpd/request_log";
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
	print $hbo->stdIncludes ("std_page_top");
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
        print "<a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$i\">$i</a> ";
    }

	print $hbo->stdIncludes ("std_page_bottom");
}

sub listTaxa {
	print $hbo->stdIncludes ("std_page_top");
    
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
        print "<a href=\"$READ_URL?action=checkTaxonInfo&taxon_no=$i\">$i</a> ";
    }

	print $hbo->stdIncludes ("std_page_bottom");
}

