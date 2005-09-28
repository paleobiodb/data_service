#!/usr/bin/perl

#-d:ptkdb

# bridge.pl is the starting point for all parts of PBDB system.  Everything passes through
# here to start with. 

#use strict;	# eventually, should have use strict set.. rjp 2/2004.

use CGI;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Carp;
use HTMLBuilder;
use DBConnection;
use DBI;
use Session;
use Class::Date qw(date localdate gmdate now);
use DataRow;
use AuthorNames;
use BiblioRef;
use Map;
use Download;
use Curve;
use Permissions;
use PBDBUtil;
use DBTransactionManager;
use Reclassify;
use Scales;
use Images;
use TaxonInfo;
use TimeLookup;
use Ecology;
use Strata;
use PrintHierarchy;
use URI::Escape;
use Report;
use Text::CSV_XS;
use POSIX qw(ceil floor);
use Confidence;
use Measurement;

# god awful Poling modules
#use Occurrence; - entirely deprecated, only ever called by Collection.pm
#use Collection; - entirely deprecated, replicates buildTaxonomicList
use Taxon;
use Opinion;

use Validation;
use Debug;


#*************************************
# some global variables (to bridge.pl)
#*************************************
# 
# Some of these variable names are used throughout the code
# $sql 		: Generally is any SQL string, although it may also be an DBTransactionManager object.
# $q		: The CGI object - used for getting parameters from HTML forms.
# $s		: The session object - used for keeping track of users, see Session.pm
# $hbo		: HTMLBuilder object, used for populating HTML templates with data. 
# $dbh		: Connection to the database, see DBConnection.pm.
# $dbt		: DBTransactionManager object, used for querying the database.
#
# rjp, 2/2004.


my $DEBUG = 0;		# Shows debug information regarding the page if set to 1

# a constant value returned by a mysql insert record indicating a duplicate row already exists
my $DUPLICATE = 2;	

my $sql="";					# Any SQL string

# Paths from the Apache environment variables (in the httpd.conf file).
my $HOST_URL = $ENV{'BRIDGE_HOST_URL'};
my $BRIDGE_HOME = $HOST_URL . "/cgi-bin/bridge.pl";
my $TEMPLATE_DIR = "./templates";
my $GUEST_TEMPLATE_DIR = "./guest_templates";
my $HTML_DIR = $ENV{'BRIDGE_HTML_DIR'};
my $OUTPUT_DIR = "public/data";


# some generally useful trig stuff needed by processCollectionsSearchForAdd
my $PI = 3.14159265;
sub acos { atan2( sqrt(1 - $_[0] * $_[0]), $_[0] ) }
# returns great circle distance given two latitudes and a longitudinal offset
sub GCD { ( 180 / $PI ) * acos( ( sin($_[0]*$PI/180) * sin($_[1]*$PI/180) ) + ( cos($_[0]*$PI/180) * cos($_[1]*$PI/180) * cos($_[2]*$PI/180) ) ) }


# Create the CGI, Session, and some other objects.
my $q = CGI->new();

# Get the URL pointing to bridge
# WARNING (JA 13.6.02): must do this before making the HTMLBuilder object!
my $exec_url = $q->url();

# this cleans all of the CGI parameters by removing HTML, escaping quotes, etc.
# added by rjp, 2/2004
# don't do this for reclassification because italics need to be passed through
#  JA 1.4.04
if ( $q->param('action') ne "startProcessReclassifyForm" )	{
	Validation::cleanCGIParams($q);
}

# Get the database connection
my $dbh = DBConnection::connect();

# Make a Transaction Manager object
my $dbt = DBTransactionManager->new($dbh);

# Make the HTMLBuilder object with the private template directory..  
my $hbo = HTMLBuilder->new($TEMPLATE_DIR, $dbt, $exec_url );

my $s = Session->new($dbt);

my $action = "";

# don't let users into the contributors' area unless they're on the main site
#  or backup server (as opposed to a mirror site) JA 3.8.04
if ( $HOST_URL !~ /paleobackup\.nceas\.ucsb\.edu/ && $HOST_URL !~ /paleodb\.org/ )	{
	 $q->param("user" => "Guest");
}


# process the action
processAction();

# ____________________________________________________________________________________
# --------------------------------------- subroutines --------------------------------
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# rjp, 2/2004
sub processAction {
	my $cookie;
		
	# Grab the action from the form.  This is what subroutine we should run.
	$action = $q->param("action");
	
	if ( $q->param("user") eq "Guest" || ! $q->param("user") )	{
		$action = "displayHomePage" unless ( $action );  # set default action to home page
	} else	{
		$action = "displayMenuPage" unless ( $action );  # set default action to menu page
	}

	
	# need to know (below) if we did a processLogin and then changed the action
	my $old_action = "";

	# figure out what to do with the action
	
	if ($action eq 'logout') { 
		logout();
		return;
	}

	if ($action eq "displayLogin") {
		displayLoginPage();
		return;
	}

	
	# Process Login
	if ($action eq "processLogin") {
        my $authorizer = $q->param('authorizer_reversed');
        my $enterer = $q->param('enterer_reversed');
        my $password = $q->param('password');
        $authorizer = Person::reverseName($authorizer);
        $enterer = Person::reverseName($enterer);


		$cookie = $s->processLogin($q,$authorizer,$enterer,$password);

		if ($cookie) {
			my $cf = CookieFactory->new();
            # The following two cookies are for setting the select lists
            # on the login page.
            my $cookieEnterer = $cf->buildCookie("enterer_reversed", $q->param("enterer_reversed"));
            my $cookieAuthorizer = $cf->buildCookie("authorizer_reversed", $q->param("authorizer_reversed"));
            
            print $q->header(-type => "text/html", 
                             -cookie => [$cookie, $cookieEnterer, $cookieAuthorizer],
                             -expires =>"now" );

			# Destination
			if ($q->param("destination") ne "") {
				$action = $q->param("destination");
			}
			if ($action eq "processLogin") {
				$action = "displayMenuPage";
				$old_action = "processLogin";
			}
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
			
			$q->param("user" => "Guest");
			$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbh, $exec_url );
			
			# return them to the login page with a message about the bad login.
			displayLoginPage("<div class=\"warning\">Sorry, your login failed. $errorMessage</div>");
			
			return;
		}
		
		# if we make it to here, then we can continue...
	}

	
	# Guest page? 
	if ($q->param("user") eq "Guest") {
		# Change the HTMLBuilder object so that the templates
		# will come from the guest directory instead of the private directory.
		$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbt, $exec_url );
	}
	
	# Validate User
	my $temp_cookie = $q->cookie('session_id');
	if (! $cookie) { # if we didn't just make a cookie, then grab it from the session.
		$cookie = $s->validateUser($dbh, $q->cookie('session_id'));
	}
	Debug::dbPrint("processAction $action authorizer ".$s->get('authorizer')." enterer ".$s->get('enterer')." session ".$q->cookie('session_id')." ip $ENV{REMOTE_ADDR}");
		
	if (!$cookie) {
		if ($q->param("user") eq "Contributor") {			
			displayLoginPage();
			return;
		} else {
			$q->param("user" => "Guest");
			$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbh, $exec_url );
		}
	}


    # The right combination will allow me to conditionally set the DEBUG flag
    if ($s->get("enterer") eq "J. Sepkoski" && 
        $s->get("authorizer") eq "J. Alroy" ) {
            $DEBUG = 1;
    }
	
	
	# Record the date of the action in the person table JA 27/30.6.02
	if ($s->isDBMember()) {
		my $sql = "UPDATE person SET last_action=NOW() WHERE person_no=".$s->get('enterer_no');
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	}
	
	# print out the HTML headers..  We have to do this differently for
	# the displayLogin routine because it turns off the cache control for some reason...(rjp)
	unless ($action eq 'displayLogin' or $old_action eq 'processLogin') {
        print $q->header(-type => "text/html", 
                         -Cache_Control=>'no-cache',
                         -expires =>"now" );
	}
	
	
	dbg("<p><font color='red' size='+1' face='arial'>You are in DEBUG mode!</font><br> Cookie [$cookie]<BR> Action [$action] Authorizer [".$s->get("authorizer")."] Enterer [".$s->get("enterer")."]<BR></p>");
	#dbg("@INC");
	dbg($q->Dump);

	# Run the action (ie, call the proper subroutine)
	&$action;
}




# Logout
# Clears the SESSION_DATA table of this session.
sub logout {

	my $session_id = $q->cookie("session_id");

	if ( $session_id ) {
		$sql =	"DELETE FROM session_data ".
		 		" WHERE session_id = '".$q->cookie("session_id")."' ";
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	}

	print $q->redirect( -url=>$BRIDGE_HOME."?user=Contributor" );
	exit;
}

# Displays the login page
# original Ederer function messed up by Poling and revised by JA 13.4.04
sub displayLoginPage {	
	my $message = shift;
	
	print $q->header( -type => "text/html", -Cache_Control=>'no-cache');
	
	my $authorizer = $q->param("authorizer_reversed") ? $q->param("authorizer_reversed") : $q->cookie("authorizer_reversed");
	my $enterer = $q->param("enterer_reversed") ? $q->param("enterer_reversed") : $q->cookie("enterer_reversed");
	my $destination = $q->param("destination");

	my $javaScript = &makeAuthEntJavaScript();

	my $html = $hbo->populateHTML('login_box', [$authorizer,$enterer] , ['authorizer_reversed','enterer_reversed' ] );
	$html =~ s/%%message%%/$message/;
	$html =~ s/%%destination%%/$destination/;
	$html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/;
	
	print &stdIncludes ("std_page_top");

	print $html;

	print &stdIncludes ("std_page_bottom");
	
	exit;
}

# Poling code calved off from displayLoginPage by JA 13.4.04
sub makeAuthEntJavaScript	{
	####
	## We need to build a list of the enterers and authorizers for 
	## the java script to use for autocompletion.
	####
	my $authListRef = Person::listOfAuthorizers($dbt,1);
	my $entListRef = Person::listOfEnterers($dbt,1);
	
	my $authList;
	my $entList;
	foreach my $p (@$authListRef) {
		$authList .= "\"" . $p->{'reversed_name'} . "\", ";  # reversed name
	}
	$authList =~ s/,\s*$//; # remove last comma and space if present.


	foreach my $p (@$entListRef) {
		$entList .= "\"" . $p->{'reversed_name'} . "\", ";  # reversed name
	}
	$entList =~ s/,\s*$//; # remove last comma and space if present.

	my $javaScript = '<SCRIPT language="JavaScript" type="text/javascript">
	// returns an array of enterer names
	function entererNames() {
		var names = new Array(' . $entList . ');
		return names;
	}
		
	// returns an array of enterer names
	function authorizerNames() {
		var names = new Array(' . $authList . ');
		return names;
	} 
	</SCRIPT>
	';

	return $javaScript;

}

# Display the preferences page JA 25.6.02
sub displayPreferencesPage {
	my $select = "";
	my $destination = $q->param("destination");

	$s->enqueue( $dbh, "action=$destination" );

	my %pref = getPreferences($s->get('enterer_no'));

	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	# Populate the form
    my @rowData;
	my @fieldNames = @{$setFieldNames};
	push @fieldNames , @{$shownFormParts};
	for my $f (@fieldNames)	{
		if ($pref{$f} ne "")	{
			push @rowData,$pref{$f};
		}
		else	{
			push @rowData,"";
		}
	}

	# Show the preferences entry page
    print stdIncludes( "std_page_top" );
	print $hbo->populateHTML('preferences', \@rowData, \@fieldNames);
    print stdIncludes("std_page_bottom");
	exit;
}

# Get the current preferences JA 25.6.02
sub getPreferences	{
	my $person_no = shift;
	my %pref;

	my $sql = "SELECT preferences FROM person WHERE person_no=".int($person_no);

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @row = $sth->fetchrow_array();
	$sth->finish();
	my @prefvals = split / -:- /,$row[0];
	for my $p (@prefvals)	{
		if ($p =~ /=/)	{
			my ($a,$b) = split /=/,$p,2;
			$pref{$a} = $b;
		}
		else	{
			$pref{$p} = "yes";
		}
	}
	return %pref;
}

# Made a separate function JA 29.6.02
sub getPrefFields	{

	# translations of fields in database tables, where needed
	my %cleanSetFieldNames = ("blanks" => "blank occurrence rows",
		"research_group" => "research group",
		"latdeg" => "latitude", "lngdeg" => "longitude",
		"geogscale" => "geographic resolution",
		"max_interval" => "time interval",
		"stratscale" => "stratigraphic resolution",
		"lithology1" => "primary lithology",
		"environment" => "paleoenvironment",
		"collection_type" => "collection purpose",
		"assembl_comps" => "assemblage components",
		"pres_mode" => "preservation mode",
		"coll_meth" => "collection type",
		"geogcomments" => "location details",
		"stratcomments" => "stratigraphic comments",
		"lithdescript" => "complete lithology description",
		"mapsize" => "image size",
		"maptime" => "reconstruction date",
		"mapfocus" => "map focus", "mapscale" => "magnification",
		"mapresolution" => "resolution",
		"mapbgcolor" => "background/ocean color",
		"crustcolor" => "continental crust color",
		"gridsize" => "grid line spacing", "gridcolor" => "grid line color",
		"latlngnocolor" => "lat/long number color",
		"coastlinecolor" => "coastline color",
		"borderlinecolor" => "international border color",
		"usalinecolor" => "USA state border color",
		"pointsize1" => "point size",
		"pointshape1" => "point shape",
		"dotcolor1" => "point color",
		"dotborder1" => "point borders" );
	# list of fields in tables
	my @setFieldNames = ("blanks", "research_group", "country", "state",
			"latdeg", "latdir", "lngdeg", "lngdir", "geogscale",
			"max_interval",
			"formation", "stratscale", "lithology1", "environment",
			"collection_type", "assembl_comps", "pres_mode", "coll_meth",
		# occurrence fields
			"species_name",
		# comments fields
			"geogcomments", "stratcomments", "lithdescript",
		# map form fields
			"mapsize", "projection", "maptime", "mapfocus",
			"mapscale", "mapresolution", "mapbgcolor", "crustcolor",
			"gridsize", "gridcolor", "latlngnocolor",
			"coastlinecolor", "borderlinecolor", "usalinecolor",
			"pointsize1", "pointshape1",
			"dotcolor1", "dotborder1");
	for my $fn (@setFieldNames)	{
		if ($cleanSetFieldNames{$fn} eq "")	{
			my $cleanFN = $fn;
			$cleanFN =~ s/_/ /g;
			$cleanSetFieldNames{$fn} = $cleanFN;
		}
	}
	# options concerning display of forms, not individual fields
	my @shownFormParts = ("collection_search", "genus_and_species_only",
		"taphonomy", "subgenera", "abundances", "plant_organs");
	return (\@setFieldNames,\%cleanSetFieldNames,\@shownFormParts);

}

# Set new preferences JA 25.6.02
sub setPreferences	{
    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first." );
        exit;
    }

    print stdIncludes( "std_page_top" );
	print "<table width='100%'><tr><td colspan='2' align='center'><h3>Your current preferences</h3></td><tr><td align='center'>\n";
	print "<table align=center cellpadding=4>\n";

	# assembl_comps: separate with commas
	my @formVals = $q->param('assembl_comps');
	# Zorch first cell (always a null value for some reason)
	shift @formVals;
	my $numSetValues = @formVals;
	if ( $numSetValues ) {
		$q->param(assembl_comps => join(',', @formVals) );
	}

 	my $sql = "UPDATE person SET preferences='";
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
 # prepare the SQL to update the prefs
	for my $i (0..$#{$setFieldNames})	{
		my $f = ${$setFieldNames}[$i];
 		if ( $q->param($f))	{
			my $val = $q->param($f);
			$val =~ s/'/\\'/g;
 			$sql .= " -:- $f=" . $val;
		}
	}

	if ($q->param("latdir"))	{
		$q->param(latdeg => $q->param("latdeg") . " " . $q->param("latdir") );
	}
	if ($q->param("lngdir"))	{
		$q->param(lngdeg => $q->param("lngdeg") . " " . $q->param("lngdir") );
	}

	print "<tr><td valign=\"top\" width=\"33%\">\n";
	print "<b>Displayed sections</b><br>\n";
	for my $f (@{$shownFormParts})	{
		my $cleanName = $f;
		$cleanName =~ s/_/ /g;
 		if ( $q->param($f) )	{
 			$sql .= " -:- " . $f;
			print "<i>Show</i> $cleanName<br>\n";
 		} else	{
			print "<i>Do not show</i> $cleanName<br>\n";
		}
	}
	# Are any comments stored?
	my $commentsStored;
	for my $i (0..$#{$setFieldNames})	{
		my $f = ${$setFieldNames}[$i];
		if ($q->param($f) && $f =~ /comm/)	{
			$commentsStored = 1;
		}
	}

	print "</td>\n<td valign=\"top\" width=\"33%\">\n";
	print "<b>Prefilled values</b><br>\n";
	for my $i (0..$#{$setFieldNames})	{
		my $f = ${$setFieldNames}[$i];
		if ($f =~ /^geogcomments$/)	{
			print "</td></tr>\n<tr><td align=\"left\" colspan=3>\n";
			if ($commentsStored)	{
				print "<b>Comment fields</b><br>\n";
 			}
 		}
		elsif ($f =~ /mapsize/)	{
			print "</td></tr>\n<tr><td valign=\"top\" width=\"33%\">\n";
			print "<b>Map view</b><br>\n";
		}
		elsif ($f =~ /(formation)|(coastlinecolor)/)	{
			print "</td><td valign=\"top\" width=\"33%\">\n<br>";
		}
 		if ( $q->param($f) && $f !~ /^eml/ && $f !~ /^l..dir$/)	{
			my @letts = split //,${$cleanSetFieldNames}{$f};
			$letts[0] =~ tr/[a-z]/[A-Z]/;
			print join '',@letts , " = <i>" . $q->param($f) . "</i><br>\n";
 		}
	}
	print "</td></tr></table>\n";

	$sql =~ s/' -:- /'/;
    my $enterer_no = int($s->get('enterer_no'));

 	$sql .= "' WHERE person_no=$enterer_no";
 	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
 	$sth->execute();
 	$sth->finish();

	print "<p>\n<a href=\"$exec_url?action=displayPreferencesPage\"><b>Set preferences</b></a></td></tr></table><p>\n";
	my %continue = $s->unqueue($dbh);
	if($continue{action}){
		print "<center><p>\n<a href=\"$exec_url?action=$continue{action}\"><b>Continue</b></a><p></center>\n";
	}
    print stdIncludes("std_page_bottom");
	exit;

}

# JA 29.2.04
sub buildTimeScalePulldown	{
	my $html = shift;			# HTML page into which we substitute

	# get the time scales and their ID numbers
	my $sql = "SELECT scale_no,scale_name FROM scales";
	my @timescalerefs = @{$dbt->getData($sql)};

	# alphabetize the time scale names
	my %tsid;
	for my $ts ( @timescalerefs )	{
		$tsid{$ts->{scale_name}} = $ts->{scale_no};
	}
	my @tsnames = keys %tsid;
	@tsnames = sort @tsnames;

	# move the Harland global scales to the start of the list
	my @harlandnames;
	my @temptsnames;
	for my $ts ( @tsnames )	{
		if ( $ts =~ /Harland .:/ )	{
			push @harlandnames, $ts;
		} else	{
			push @temptsnames, $ts;
		}
	}
	@tsnames = @harlandnames;
	push @tsnames, @temptsnames;

	# build the select list
	my $timescale = "<select name=\"time_scale\">\n<option selected>\n";
	# add the 10 m.y. bin option at the head of the list
	$timescale .= "<option>PBDB 10 m.y. bins\n<option>\n";
	for my $ts ( @tsnames )	{
		$timescale .= "<option value=\"$tsid{$ts}\">$ts\n";
	}
	$timescale .= "</select>\n";


	# add a blank after the Harland periods
	$timescale =~ s/<option value="6">Harland 6: Stages/<option value="6">Harland 6: Stages\n<option>\n/;

	# put the select list into the web page
	$$html =~ s/%%time scale%%/$timescale/;
}

# displays the main menu page for the data enterers
sub displayMenuPage	{
	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue( $dbh ); 
	} else {
	
		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue( $dbh );
		if ( $queue{action} ) {
	
			# Set each parameter
			foreach my $parm ( keys %queue ) {
				$q->param ( $parm => $queue{$parm} );
			}
	
			# Run the command
			&{$queue{action}};
			exit;
		}
	}
	
	print stdIncludes("std_page_top");
	print $hbo->populateHTML('menu', [],[]);
	print stdIncludes("std_page_bottom");


}




# well, displays the home page
sub displayHomePage {
	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue( $dbh ); 
	} else {

		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue( $dbh );
		if ( $queue{action} ) {
			# Set each parameter
			foreach my $parm ( keys %queue ) {
				$q->param ( $parm => $queue{$parm} );
			}
	
			# Run the command
			&{$queue{action}};
			exit;
		}
	}

	# Get some populated values
	$sql = "SELECT * FROM statistics";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
   	$sth->execute();
	my $rs = $sth->fetchrow_hashref();
    my (@rowData,@fieldNames);
	push ( @rowData,	$rs->{reference_total},
						$rs->{collection_total}, 
						$rs->{occurrence_total}, 
						$rs->{enterer_total}, 
						$rs->{institution_total}, 
						$rs->{country_total}, '', '' );
	push ( @fieldNames,	"reference_total",
						"collection_total", 
						"occurrence_total", 
						"enterer_total", 
						"institution_total", 
						"country_total", "main_menu", "login" );
	$sth->finish();

	print stdIncludes("std_page_top");
	print $hbo->populateHTML('home', \@rowData, \@fieldNames);
	print stdIncludes("std_page_bottom");
}



# Shows the form for requesting a map
sub displayMapForm {

	# List fields that should be preset
	my @fieldNames = ( 'research_group', 'country', 'period_max', 'lithology1', 'environment', 'mapsize', 'projection', 'maptime', 'mapfocus', 'mapscale', 'mapresolution', 'mapbgcolor', 'crustcolor', 'gridsize', 'gridcolor', 'gridposition', 'linethickness', 'latlngnocolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointsize1', 'pointshape1', 'dotcolor1', 'dotborder1', 'mapsearchfields2', 'pointsize2', 'pointshape2', 'dotcolor2', 'dotborder2', 'mapsearchfields3', 'pointsize3', 'pointshape3', 'dotcolor3', 'dotborder3', 'mapsearchfields4', 'pointsize4', 'pointshape4', 'dotcolor4', 'dotborder4' );
	# Set default values
	my @row = ( '', '', '', '', '', '100%', 'rectilinear', '0', 'Europe', 'X 1', 'medium', 'white', 'none', '30 degrees', 'gray', 'in back', 'medium', 'none', 'black', 'none', 'none', 'medium', 'circles', 'red', 'black', '', 'medium', 'squares', 'blue', 'black', '', 'medium', 'triangles', 'yellow', 'black', '', 'medium', 'diamonds', 'green', 'black' );
	
	# Read preferences if there are any JA 8.7.02
	my %pref = getPreferences($s->get('enterer_no'));
	# Get the enterer's preferences
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	for my $p (@{$setFieldNames})	{
		if ($pref{$p} ne "")	{
			#these prefs are for collection entry form, don't display the here
			if ($p !~ /environment|research_group|formation|country|state|interval_name|lithology|period_max/) {
				unshift @row,$pref{$p};
				unshift @fieldNames,$p;
			}
		}
	}

	%pref = getPreferences($s->get('enterer_no'));
	my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('map_form', \@row, \@fieldNames, \@prefkeys);


    my $javaScript = &makeAuthEntJavaScript();
    $html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/; 

	my $authorizer_reversed = $s->get("authorizer_reversed");
	$html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;
	my $enterer_reversed = $s->get("enterer_reversed");
	$html =~ s/%%enterer_reversed%%/$enterer_reversed/;

	# Spit out the HTML
	print stdIncludes("std_page_top" );
	print $hbo->populateHTML('js_map_checkform');
	print $html;
	print stdIncludes("std_page_bottom");
}



sub displayMapResults {
	print stdIncludes("std_page_top" );

	my $m = Map::->new( $dbh, $q, $s, $dbt, $hbo);
	my $file = $m->buildMap();

    open(MAP, $file) or die "couldn't open $file ($!)";
    while(<MAP>){
        print;
    }
    close MAP;

	print stdIncludes("std_page_bottom");
}



sub displayDownloadForm {
	print stdIncludes( "std_page_top" );
	my $html = $hbo->populateHTML( 'download_form', [ '', '', '', '', '', '','','','','' ], [ 'research_group', 'country','environment','lithology1','ecology1','ecology2','ecology3','ecology4','ecology5','ecology6' ] );
    my $javaScript = &makeAuthEntJavaScript();
    $html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/; 
    my $authorizer_reversed = $s->get("authorizer_reversed");
    $html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;
    my $enterer_reversed = $s->get("enterer_reversed");
    $html =~ s/%%enterer_reversed%%/$enterer_reversed/;  
    
	buildTimeScalePulldown ( \$html );
	print $html;

	print stdIncludes("std_page_bottom");
}

sub displayDownloadResults {

	print stdIncludes( "std_page_top" );

	my $m = Download->new( $dbh, $dbt, $q, $s, $hbo );
	$m->buildDownload( );

	print stdIncludes("std_page_bottom");
}

sub displayReportForm {

	print stdIncludes( "std_page_top" );

	print $hbo->populateHTML( 'report_form', [ '' ], [ 'research_group' ] );

	print stdIncludes("std_page_bottom");
}

sub displayReportResults {

	print stdIncludes( "std_page_top" );

	my $r = Report->new( $dbh, $q, $s, $dbt );
	$r->buildReport();

	print stdIncludes("std_page_bottom");
}

sub displayCurveForm {

	print stdIncludes( "std_page_top" );

	print $hbo->populateHTML( 'curve_form', [ '', '', '', '' ] , [ 'research_group', 'collection_type', 'lithology1', 'lithology2' ] );

	print stdIncludes("std_page_bottom");
}

sub displayCurveResults {

	print stdIncludes( "std_page_top" );

	my $c = Curve->new( $dbh, $q, $s, $dbt );
	$c->buildCurve();

	print stdIncludes("std_page_bottom");
}

# JA 9.8.04
sub displayTenMyBins	{

	print stdIncludes("std_page_top");

	print "<center><h2>10 m.y.-Long Sampling Bins</h2></center>\n\n";

	print "These bin definitions are used by the <a href=\"$exec_url?action=displayCurveForm\">diversity curve generator</a>.\n\n";

	my @binnames = TimeLookup::getTenMYBins();

	@_ = TimeLookup::processBinLookup($dbh,$dbt,"boundaries");
	#%topma = %{$_[0]};
	my %basema = %{$_[1]};
	my %binning = %{$_[2]};

	my $intervalsql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intervalrefs = @{$dbt->getData($intervalsql)};
	my (%intervalname,%intervalalias);

	for my $ir (@intervalrefs)	{
		$intervalname{$ir->{interval_no}} = $ir->{interval_name};
		$intervalalias{$intervalname{$ir->{interval_no}}} = $ir->{interval_name};
		if ( $ir->{eml_interval} =~ /[A-Za-z]/ )	{
			$intervalname{$ir->{interval_no}} = $ir->{eml_interval} . " " . $ir->{interval_name};
			if ( $ir->{eml_interval} !~ /middle/i )	{
				$intervalalias{$intervalname{$ir->{interval_no}}} = $ir->{interval_name} . $ir->{eml_interval};
			} else	{
				$intervalalias{$intervalname{$ir->{interval_no}}} = $ir->{interval_name} . "F";
			}
		}
	}

	print "<hr>\n\n";

	print "<table><tr>\n";
	print "<td valign=top><b>Bin name</b></td>  <td valign=top><b>Age&nbsp;at&nbsp;base&nbsp;(Ma)</b></td>  <td valign=top><b>Included intervals</b></td></tr>\n";

	for my $bn (@binnames)	{
		my $temp = $bn;
		$temp =~ s/ /&nbsp;/g;
		print "<tr><td valign=top>$temp</td>\n";
		printf "<td align=center valign=top>%.1f</td>\n",$basema{$bn};
		print "<td class=tiny>";
		my @namestoprint;
        my @ints;
		for my $in ( keys %binning )	{
			if ( $binning{$in} eq $bn )	{
				push @namestoprint , $intervalname{$in};
                push @ints,$in;
			}
		}
		@namestoprint = sort { $intervalalias{$a} cmp $intervalalias{$b} } @namestoprint;
		my $printed = 0;
		for my $nametoprint ( @namestoprint )	{
			if ( $printed > 0 )	{
				print ", ";
			}
			print $nametoprint;
			$printed++;
		}
		print "</td></tr>\n\n";
	}
	print "</table>\n<p>\n\n";

	print stdIncludes("std_page_bottom");

}

# Show a generic page
sub displayPage {
	my $page = shift;
	if ( ! $page ) { 
		# Try the parameters
		$page = $q->param("page"); 
		if ( ! $page ) {
			htmlError( "displayPage(): Unknown page..." );
		}
	}

	# Spit out the HTML
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print stdIncludes( "std_page_top" );
	}
	print $hbo->populateHTML($page,[],[]);
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print stdIncludes("std_page_bottom");
	}
}


# This is a wrapper to put the goodies into the standard page bottom
# or top.  Pass it the name of a template such as "std_page_top" and 
# it will return the HTML code.
sub stdIncludes {
	my $page = shift;
	my $reference = buildReference( $s->get("reference_no"), "bottom" );
	my $enterer;

	ENTERER: {
		# Already was logged in
		if ( $s->get("enterer") ne "" ) { $enterer = $s->get("enterer"); last; }
		# Just logged in
		if ( $q->param("enterer") ne "" ) { $enterer = $q->param("enterer"); last; }
		# Don't know
		$enterer = "none";
	}

	return $hbo->populateHTML(	$page, 
								[ $reference, $enterer ], 
								[ "%%reference%%", "%%enterer%%" ] );
}


# Shows the search form
# modified by rjp, 3/2004
# JA: Poling completely fucked this up and I restored it from backup 13.4.04
sub displaySearchRefs {

	my $message = shift;		    # Message telling them why they are here
    my $noHeader = (shift || 0);
	my $type = $q->param("type");

	my @row;
	my @fields = ( "action" );

	my $html = "";

	print &stdIncludes ( "std_page_top" ) unless ($noHeader);

	TYPE: {
		if ( $type eq "select" ) { push ( @row, "displayRefResults" ); last; }
		if ( $type eq "edit" ) { push ( @row, "displaySelectRefForEditPage" ); last; }
		if ( $type eq "add" ) { push ( @row, "displayRefResultsForAdd" ); last; }

		# Unspecified
		push ( @row, "displayRefResults" );
	}

	# Prepend the message and the type
	unshift ( @row, $message, $type );
	unshift ( @fields, "%%message%%", "type" );

	# If we have a default reference_no set, show another button.
	# Don't bother to show if we are in select mode.
	unshift ( @fields, "%%use_current%%" );
	my $reference_no = $s->get("reference_no");
	if ( $reference_no && $type ne "add" ) {
		unshift ( @row, "<input type='submit' name='use_current' value='Use current reference ($reference_no)'>\n" );
	} else {
		unshift ( @row, "" );
	}

# Users editing collections may want to have their current reference
	# swapped with the primary reference of the collection to be edited.
	unshift ( @fields, "%%use_primary%%" );
	if ($q->param('action') eq "startEditCollection" ) {
		unshift ( @row, "<input type='submit' name='use_primary' value=\"Use collection's reference\">\n" ); } else {
		unshift ( @row, "" );
	}

	unshift @row,"";
	unshift @fields,"project_name";

	$html .= $hbo->populateHTML("search_refs_form", \@row, \@fields);

	# insert the JavaScript
	# WARNING: this can't be done by means of populateHTML because
	#  quotes would be turned into &quot; tags
	my $javaScript = &makeAuthEntJavaScript();
	$html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/;
    my $authorizer_reversed = $s->get("authorizer_reversed");
    $html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;
    my $enterer_reversed = $s->get("enterer_reversed");
    $html =~ s/%%enterer_reversed%%/$enterer_reversed/;  

	print $html;

	print &stdIncludes ("std_page_bottom") unless ($noHeader);

}


# Print out the number of refs found by a ref search JA 26.7.02
sub describeRefResults	{
	my $numRows = shift;
	my $overlimit = shift;
    my $refsearchstring = shift;
	
	print "<center><h3>Your search $refsearchstring produced ";
	if ($numRows > 30 || $numRows < $overlimit)	{
		if ($overlimit > 0)	{
			print "$overlimit";
		} else	{
			print "$numRows";
		}
		print " matches</h3><h4>Here are ";
		if ($numRows == 30)	{
			print "the first 30";
		} elsif ($numRows < $overlimit)	{
			print "references ", $numRows - 29, " through ",$numRows;
		} else	{
			print "the remaining " . $numRows % 30 . " references";
		}
		print "</h4>";
	} elsif ( $numRows == 1)	{
		print "exactly one match</h3>";
	} else	{
		print "$numRows matches</h3>";
	}
	print "</center>\n";

}




sub printGetRefsButton	{

	my $numRows = shift;
	my $overlimit = shift;

	if ($overlimit > 30 && $overlimit > $numRows)	{
		my $oldSearchTerms;
		my @oldParams = ("name", "year", "reftitle", "reference_no",
						 "enterer_reversed", "project_name", "refsortby",
						 "refsSeen", "authorizer_reversed");
		for my $parameter (@oldParams)	{
			if ($q->param($parameter))	{
				$oldSearchTerms .= "&" . $parameter . "=" . $q->param($parameter);
			}
		}
		print qq|<center><p><a href="$exec_url?action=displayRefResults&type=select$oldSearchTerms"><b>Get the next 30 references</b></a> - |;
	} else	{
		print  "<center><p>";
	}

	my $authname = $s->get('authorizer');
	$authname =~ s/\. //;
	print qq|<a href="$HOST_URL/$OUTPUT_DIR/$authname.refs"><b>Download all the references</b></a> - \n|;

}



# This shows the actual references.
sub displayRefResults {
    my ($sth,$overlimit,$refsearchstring,$md);
	my @rows;
	my $numRows;

	# use_primary is true if the user has clicked on the "Current reference" link at
	# the top or bottom of the page.  Basically, don't bother doing a complicated 
	# query if we don't have to.
	unless($q->param('use_primary')) {
		($sth, $overlimit,$refsearchstring) = RefQuery($q);
	    $md = MetadataModel->new($sth);
		
		@rows = @{$sth->fetchall_arrayref()};
		$numRows = @rows;
	} else {
		$q->param('use_primary' => "yes");
		$numRows = 1;
	}


	if ( $numRows == 1 ) {
		# Do the action, don't show results...

		# Set the reference_no
		unless($q->param('use_primary') || $q->param('type') eq 'view'){
			$s->setReferenceNo( $dbh, ${$rows[0]}[3] );
		}

		# QUEUE
		my %queue = $s->unqueue( $dbh );
		$q->param( "type" => $queue{type} );		# Store the type, just in case
		$q->param( "collection_no" => $queue{collection_no} );
		my $action = $queue{action};

		# Get all query params that may have been stuck on the queue
		# back into the query object:
		foreach my $key (keys %queue){
			$q->param($key => $queue{$key});
		}

		# if there's an action, go straight back to it without showing the ref
		if ( $action)	{
			&{$action};			# Run the action
		} else	{  
		
			# otherwise, display a page showing the ref JA 10.6.02
			print stdIncludes( "std_page_top" );
			print "<h3>Here is the full reference...</h3>\n";
			print "<table border=0 cellpadding=4 cellspacing=0>\n";
		    my $drow = DataRow->new($rows[0], $md);
			# Args: data row, selectable, row, rowcount, suppress_colls
			# Suppress collections so I can insert other stuff and then
			# call it below.
		   	print makeRefString( $drow, 0, 1, 1, 1);
			print "<tr><td colspan=\"3\">&nbsp;</td>";
			print "<td><font size=-1>\n";
			print "<table border=0 cellpadding=0 cellspacing=0>";
			# Now the pubyr:
			# This spacing is to match up with the collections, below
			if(@{$rows[0]}[16]){
				print "<tr><td>Publication type:&nbsp;<i>".${$rows[0]}[16]."</i></font></td></tr>";
			}
			# Now the comments:
			if(@{$rows[0]}[17]){
				print "<tr><td>Comments:&nbsp;<i>".${$rows[0]}[17]."</i></font></td></tr>";
			}
			# getCollsWithRef creates a new <tr> for the collections.
			my $refColls = getCollsWithRef(${$rows[0]}[3], 0, 0);
			# remove the cells that cause this to line up since we're putting
			# it in a subtable.
			$refColls =~ s/^<tr>\n<td colspan=\"3\">&nbsp;<\/td>/<tr>/;
			print $refColls;
			print "</table>\n";
			print "</table><p>\n";
			print stdIncludes( "std_page_bottom" );
		}
		#	if ( ! $action ) { $action = "displayMenuPage"; } # bad Tone code
		return;		# Out of here!
	}


	print stdIncludes( "std_page_top" );

	if ( $numRows ) {
		describeRefResults($numRows,$overlimit,$refsearchstring);

		print qq|<FORM method="POST" action="$exec_url"'>\n|;

		print qq|<input type=hidden name="action" value="selectReference">\n|;

		my $row = 1;

		# Print the references found
		print "<table border=0 cellpadding=5 cellspacing=0>\n";

		# Only print the last 30 rows that were found JA 26.7.02
		foreach my $rowref ( @rows ) {
		    my $drow = DataRow->new($rowref, $md);
			if ( $row + 30 > $q->param('refsSeen') )	{
				# Don't show radio buttons if Guest
		   		print &makeRefString( $drow, $s->isDBMember() , $row, $numRows );

                if ( $row % 2 != 0 ) {
                    print "<tr class=\"darkList\">";
                } else {
                    print "<tr>";
                }
			}
			$row++;
		}
		print "</table>\n";

		$sth->finish();

		if($s->isDBMember()){
			print qq|<input type=submit value="Select reference"></form>\n|;
		}

		printGetRefsButton($numRows,$overlimit);

	} else {
		print "<center>\n<h3>Your search $refsearchstring produced no matches</h3>\n";
		print "<p>Please try again with fewer search terms.</p>\n</center>\n";
		print "<center>\n<p>";
	}

	print qq|<a href="$exec_url?action=displaySearchRefs&type=select"><b>Do another search</b></a>\n|;
	if($s->isDBMember()){
		print qq| - <a href="$exec_url?action=displayRefAdd"><b>Enter a new reference</b></a>\n|; 
	}
	print "</p></center><br>\n";

	print stdIncludes("std_page_bottom");
}



sub displayRefResultsForAdd {

	my ($sth, $overlimit,$refsearchstring) = RefQuery($q);
	my $md = MetadataModel->new($sth);

	my @rows = @{$sth->fetchall_arrayref()};
	my $numRows = @rows;
  
	if ( ! $numRows ) {
		# No matches?  Great!  Get them where the were going.
		displayRefAdd();
		exit;
	}

	print stdIncludes( "std_page_top" );

	describeRefResults($numRows,$overlimit,$refsearchstring);

	print qq|<FORM method="POST" action="$exec_url"'>\n|;

	# This is view only... you may not select
	print qq|<input type=hidden name="action" value="displayRefAdd">\n|;

	# carry search terms over for populating form
	foreach my $s_param ("name","year","reftitle","project_name") {
		if($q->param($s_param)){
			print "<input type=hidden name=\"$s_param\" value=\"".
				  $q->param($s_param)."\">\n";
		}
	}

	# Print the references found
	print "<table border=0 cellpadding=5 cellspacing=0>\n";
	my $row = 1;
	foreach my $rowref ( @rows ) {

	    my $drow = DataRow->new($rowref, $md);
	    #print $drow->getValue('reference_no') . "<BR>";
		# This is view only... you may not select
    	print makeRefString( $drow, 0, $row, $numRows );
		$row++;
	}
	print "</table>\n";
	$sth->finish();

	printGetRefsButton($numRows,$overlimit);

	print qq|<a href="$exec_url?action=displaySearchRefs&type=add"><b>Do another search</b></a></p>\n|;

	print qq|<p><input type=submit value="Add reference"></center>\n</p>\n|;

	print stdIncludes("std_page_bottom");
}


# Given the sth of a single row result, find the reference_no.
# Then set the SESSION_DATA variable.
sub setReferenceNoFromSth {
	my $sth = shift;
	my $values = shift;

	if ( $sth->rows ) {
		my @names = @{$sth->{NAME}};
		for ( my $i=0; $i<$#names; $i++ ) {
			if ( $names[$i] eq "reference_no" ) {
				$s->setReferenceNo( $dbh, ${$values}[$i] );
			}
		}
	}
}

sub selectReference {

	$s->setReferenceNo( $dbh, $q->param("reference_no") );
	displayMenuPage( );
}

sub displayRefAdd {
	my @fieldNames = (	"publication_type", 
						"%%new_message%%" );
	my @row = ( "", 
				"<p>If the reference is <b>new</b>, please fill out the following form.</p>" );

	print stdIncludes( "std_page_top" );
	print $hbo->populateHTML('js_reference_checkform');

	# Pre-populate the form with the search terms:
	my %query_hash = ("name" => "author1last",
					  "year" => "pubyr",
					  "reftitle" => "reftitle",
					  "project_name" => "project_name");

	foreach my $s_param (keys %query_hash){
		if($q->param($s_param)){
			push(@row, $q->param($s_param));
			push(@fieldNames, $query_hash{$s_param});
		}
	}
    push (@row, 'standard','English','processNewRef');
    push (@fieldNames, 'classification_quality','language','action');
	print $hbo->populateHTML("enter_ref_form", \@row, \@fieldNames);
	print stdIncludes("std_page_bottom");
}

#  * User (presumably has a ref that's not in the results, and)
#    submits completed enter ref form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processNewRef {
	my $reentry = shift;
	my $reference_no=0;
	my $return;

	print stdIncludes( "std_page_top" );
	dbg("processNewRef reentry:$reentry<br>");

	if($reentry){
		$reference_no = $reentry;
		$return = 1; # equivalent to 'success' from insertRecord
		dbg("reentry TRUE<br>");
	}
	else{
		dbg("reentry FALSE, calling insertRecord()<br>");

        $return = checkFraud();
        if (!$return) {
            $return = insertRecord('refs', 'reference_no', \$reference_no, '5', 'author1last' );
            if ( ! $return ) { return $return; }
        } else {
            if ($return eq 'Gupta') {
                print qq|<center><h3><font color='red'>WARNING: Data published by V. J. Gupta have been called into question by Talent et al. 1990, Webster et al. 1991, Webster et al. 1993, and Talent 1995. Please hit the back button, copy the comment below to the reference title, and resubmit.  Do NOT enter
any further data from the reference.<br><br> "DATA NOT ENTERED: SEE |.$s->get('authorizer').qq| FOR DETAILS"|;
                print "</font></h3></center>\n";
            } else {
                print qq|<center><h3><font color='red'>WARNING: Data published by M. M. Imam have been called into question by <a href='http://www.up.ac.za/organizations/societies/psana/Plagiarism_in_Palaeontology-A_New_Threat_Within_The_Scientific_Community.pdf'>J. Aguirre 2004</a>. Please hit the back button, copy the comment below to the reference title, and resubmit.  Do NOT enter any further data from the reference.<br><br> "DATA NOT ENTERED: SEE |.$s->get('authorizer').qq| FOR DETAILS"|;
            }
            return 0;
        }
	}

	print "<center><h3><font color='red'>Reference record ";
	if ( $return == $DUPLICATE ) {
   		print "already ";
	}
	print "added</font></h3><center>";

	# Set the reference_no
	$s->setReferenceNo( $dbh, $reference_no );

    $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$reference_no;  
    
    dbg( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();

    my $rowref = $sth->fetchrow_arrayref();
    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($rowref, $md);
    my $retVal = makeRefString($drow);
    print '<table>' . $retVal . '</table>';

    print qq|<center><p><a href="$exec_url?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p>\n|;
	print qq|<p><a href="$exec_url?action=displaySearchCollsForAdd&type=add"><b>Add a new collection</b></a></p></center>|;
    print stdIncludes("std_page_bottom");
}


#  * User submits completed refs search form
#  * System displays matching reference results
sub displaySelectRefForEditPage
{

	my ($sth, $overlimit,$refsearchstring) = RefQuery($q);
	my $md = MetadataModel->new($sth);
	
	my @rowrefs = @{$sth->fetchall_arrayref()};
	my $numRows = @rowrefs;

	if ( $numRows == 1 ) {
		# Grab the default ref if no ref no was supplied in a form
		if ($q->param("reference_no") <= 0)	{
			my $drow = DataRow->new($rowrefs[0], $md);
			if ( $drow->getValue('reference_no') )	{
				$q->param("reference_no" => $drow->getValue('reference_no'));
			}
			# Otherwise grab the default ref
			if ($q->param("reference_no") <= 0)	{
				$q->param("reference_no" => $s->get("reference_no") );
			}
		}
		displayRefEdit();
		exit;
	}

	print stdIncludes( "std_page_top" );

	if ( $numRows > 0) {

		describeRefResults($numRows,$overlimit,$refsearchstring);

		print qq|<form method="POST" action="$exec_url">\n|;
		print qq|<input type=hidden name="action" value="displayRefEdit">\n|;

		print "<table border=0 cellpadding=5 cellspacing=0>\n";
		my $matches;
		my $row = 1;
		foreach my $rowref (@rowrefs) {
			my $drow = DataRow->new($rowref, $md);
			my $retVal = makeRefString($drow, 1, $row, $numRows);
			print $retVal;
			#$matches++ if $selectable;
			$matches++;
			$row++;
		}
		print "</table>";
		if ($matches > 0)	{
			print qq|<input type=submit value="Edit selected">\n|;
		}
		print "</form>";

		printGetRefsButton($numRows,$overlimit);

	} else {
		print "<center><h3>Your search $refsearchstring produced no matches</h3>\n";
		print "<p>Please try again with fewer search terms.</p></center>\n";
		print "<center><p>";
	}

	print qq|<a href="$exec_url?action=displaySearchRefs&type=edit"><b>Search for another reference</b></a></p></center><br>\n|;

	print stdIncludes("std_page_bottom");
}

# Wrapper to displayRefEdit
sub editCurrentRef {
	my $reference_no = $s->get("reference_no");
	if ( $reference_no ) {
		$q->param("reference_no"=>$reference_no);
		displayRefEdit( );
	} else {
		$q->param("type"=>"edit");
		displaySearchRefs( "Please choose a reference first" );
	}
}

# The reference_no must be passed in the params.
sub displayRefEdit
{
	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=displayRefEdit" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	my $reference_no = $q->param('reference_no');
	if ( $reference_no ) {
		$s->setReferenceNo( $dbh, $reference_no );
	} else {
		# Have to have one!
		$s->enqueue( $dbh, "action=displayRefEdit" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	print stdIncludes( "std_page_top" );
	print $hbo->populateHTML('js_reference_checkform');

	$sql =	"SELECT * FROM refs WHERE reference_no=$reference_no";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my @row = $sth->fetchrow_array();
	my @fieldNames = @{$sth->{NAME}};
	$sth->finish();

	# Tack on a few extras
	push (@fieldNames, 'action','%%new_message%%');
	push (@row, 'processReferenceEditForm','');

	print $hbo->populateHTML('enter_ref_form', \@row, \@fieldNames);

	print stdIncludes("std_page_bottom");
}

#  * User submits completed reference form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processReferenceEditForm {

	print stdIncludes( "std_page_top" );
	  
	my $refID = updateRecord('refs', 'reference_no', $q->param('reference_no'));
    print "<center><h3><font color='red'>Reference record updated</font></h3></center>\n";
		
    $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$refID;

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my $rowref = $sth->fetchrow_arrayref();
    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($rowref, $md);
    print '<table>' . makeRefString($drow) . '</table>';
	# my $bibRef = BiblioRef->new($drow);
	# print '<table>' . $bibRef->toString() . '</table>';
		
    print qq|<center><p><a href="$exec_url?action=displaySearchRefs&type=edit"><b>Edit another reference</b></a></p></center><br>\n|;
	print stdIncludes("std_page_bottom");
}


# 5.4.04 JA
# print the special search form used when you are adding a collection
# uses some code lifted from displaySearchColls
sub displaySearchCollsForAdd	{

	# Have to have a reference #, unless we are just searching
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		# Come back here... requeue our option
		$s->enqueue( $dbh, "action=displaySearchCollsForAdd&type=add" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	# use some preferences JA 20.10.04
	my %pref = getPreferences($s->get('enterer_no'));

	my $html = $hbo->populateHTML('search_collections_for_add_form' , [ '' , $pref{'latdeg'} , '' , '' , '' , $pref{'latdir'} , $pref{'lngdeg'} , '' , '' , '' , $pref{'lngdir'} ] , [ 'period_max' , 'latdeg' , 'latmin' , 'latsec' , 'latdec' , 'latdir',  'lngdeg' , 'lngmin' , 'lngsec' , 'lngdec' , 'lngdir' ] );

	# Spit out the HTML
	print stdIncludes( "std_page_top" );
	print $html;
	print stdIncludes("std_page_bottom");

}


sub displaySearchColls {

	# Get the type, passed or on queue
	my $type = $q->param("type");
	if ( ! $type ) {
		# QUEUE
		my %queue = $s->unqueue( $dbh );
		$type = $queue{type};
	}

	# Have to have a reference #, unless we are just searching
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no && $type ne "view" && $type ne "reclassify_occurrence" && !$q->param('use_primary') ) {
		# Come back here... requeue our option
		$s->enqueue( $dbh, "action=displaySearchColls&type=$type" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}	

	# add				result list links view collection details
	# view				result list links view collection details
	# edit				result list links go directly to edit the collection
	# edit_occurrence	result list links go to edit occurrence page

	# Show the "search collections" form
	my %pref = getPreferences($s->get('enterer_no'));
	my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('search_collections_form', [ '', '', '', '', '', '','' ], [ 'research_group', 'eml_max_interval', 'eml_min_interval', 'lithadj', 'lithology1', 'lithadj2', 'lithology2', 'environment',$type ], \@prefkeys);

    my $javaScript = &makeAuthEntJavaScript();
    $html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/; 
    my $authorizer_reversed = $s->get("authorizer_reversed");
    $html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;
    my $enterer_reversed = $s->get("enterer_reversed");
    $html =~ s/%%enterer_reversed%%/$enterer_reversed/;  

	# propagate this through the next server hit.
	if($q->param('use_primary')){
		$html =~ s/%%use_primary%%/yes/;
	}
	else{
		$html =~ s/%%use_primary%%//;
	}

	# Set the type
	$html =~ s/%%type%%/$type/;

	# Spit out the HTML
	print stdIncludes( "std_page_top" );
    printIntervalsJava(1);
	print $html;
	print stdIncludes("std_page_bottom");
}


# User submits completed collection search form
# System displays matching collection results
# Called during collections search, and by displayReIDForm() routine.
sub displayCollResults {
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
		if ($q->param('taxon_name') && ($q->param('type') eq "reid" ||
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
		my %queue = $s->unqueue( $dbh );		# Most of 'em are queued
		$type = $queue{type};
	}

    my $permission_type = "read";
	# We create different links depending on their destination, using the hidden type field.
	if ( $type eq "add" )		{ $action = "displayCollectionDetails"; $permission_type = "read"; }
	elsif ( $type eq "edit" )	{ $action = "displayEditCollection"; $permission_type = "write"; }
	elsif ( $type eq "view" )	{ $action = "displayCollectionDetails"; $permission_type= "read"; }
	elsif ( $type eq "edit_occurrence" )   { $action = "displayOccurrenceAddEdit"; $permission_type= "read"; }
	elsif ( $type eq "reid" )	{ $action = "displayOccsForReID"; $permission_type= "read"; }
	# added by JA 31.3.04
	elsif ( $type eq "reclassify_occurrence" )	{ $action = "startDisplayOccurrenceReclassify"; $permission_type= "write";}
	# type is unknown, so use defaults.
	else { $action = "displayCollectionDetails"; $permission_type = "read"; }

	
	# Build the SQL
	# which function to use depends on whether the user is adding a collection
	my $sql;
    
    my ($dataRows,$ofRows);
	if ( $q->param('type') eq "add" )	{
		# you won't have an in list if you are adding
		($dataRows,$ofRows) = processCollectionsSearchForAdd();
	} else	{
        my $fields = ["authorizer","country", "state", "period_max", "period_min", "epoch_max", "epoch_min", "intage_max", "intage_min", "locage_max", "locage_min", "max_interval_no", "min_interval_no","collection_aka"];  
        my %options = $q->Vars();
        if ($type eq "reclassify_occurrence") {
            # Want to not get taxon_nos when reclassifying. Otherwise, if the taxon_no is set to zero, how will you find it?
            $options{'no_authority_lookup'} = 1;
        }
        $options{'limit'} = $perm_limit;
        $options{'permission_type'} = $permission_type;
        if ($q->param("taxon_list")) {
            my @in_list = split(/,/,$q->param('taxon_list'));
            $options{'taxon_list'} = \@in_list if (@in_list);
        }
		($dataRows,$ofRows) = processCollectionsSearch($dbt,\%options,$fields);
	}

    my @dataRows = @$dataRows;
    my $displayRows = scalar(@dataRows);	# get number of rows to display

    if ( $displayRows > 1  || ($displayRows == 1 && $type eq "add")) {
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
			    Reclassify::displayOccurrenceReclassify($q,$s,$dbh,$dbt,\@colls);
            }
			exit;
		}
		
		print stdIncludes( "std_page_top" );
        # Display header link that says which collections we're currently viewing

        print "<center>";
        if ($ofRows > 1) {
            print "<h3>Your search produced $ofRows matches</h3>\n";
            if ($ofRows > $limit) {
                print "<h4>Here are";
                if ($rowOffset > 0) {
                    print " rows ".($rowOffset+1)." to ";
                    my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
                    print $printRows;
                    print "</h4>\n";
                } else {
                    print " the first ";
                    my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
                    print $printRows;
                    print " rows</h4>\n";
                }
            }
		} elsif ( $ofRows == 1 ) {
            print "<h3>Your search produced exactly one match</h3>\n";
		} else	{
            print "<h3>Your search produced no matches</h3>\n";
		}
		print "</center>\n";
		print "<br>\n";

	  	print "<table width='100%' border=0 cellpadding=4 cellspacing=0>\n";
 
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
        for(my $count=$rowOffset;$count<scalar(@dataRows) && $count < $rowOffset+$limit;$count++) {
            my $dataRow = $dataRows[$count];
			# Get the reference_no of the row
            $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=".$dataRow->{'reference_no'};
            my $ref = ${$dbt->getData($sql)}[0];
			# Build the reference string
            my $reference = Reference::formatShortRef($ref,'alt_pubyr'=>1, 'link_id'=>1);

			# Build a short descriptor of the collection's time place
			# first part JA 7.8.03
			my $timeplace;

			my $tsql = "SELECT interval_name FROM intervals WHERE interval_no=" . $dataRow->{max_interval_no};
			my $maxintname = @{$dbt->getData($tsql)}[0];
			$timeplace = $maxintname->{interval_name};
			if ( $dataRow->{min_interval_no} > 0 )	{
				$tsql = "SELECT interval_name FROM intervals WHERE interval_no=" . $dataRow->{min_interval_no};
				my $minintname = @{$dbt->getData($tsql)}[0];
				$timeplace .= "/" . $minintname->{interval_name};
			}

			# rest of timeplace construction JA 20.8.02

			$timeplace .= " - ";
			if ( $dataRow->{"state"} )	{
				$timeplace .= $dataRow->{"state"};
			} else	{
				$timeplace .= $dataRow->{"country"};
			}
			$timeplace = "</b> <span class=tiny>" . $timeplace . "</span>";

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
                print "\">$dataRow->{collection_no}</a></td>";
            } else {	
                # Don't link it if if we're in edit mode and we don't have permission
                print "<td align=center valign=top>$dataRow->{collection_no}</td>";
            }
			

            my $collection_names = $dataRow->{'collection_name'};
            if ($dataRow->{'collection_aka'}) {
                $collection_names .= " <span class=\"tiny\">(aka $dataRow->{collection_aka})</span>";
            }
            print "<td valign=top nowrap>$dataRow->{authorizer}</td>";
            print "<td valign=top><b>${collection_names}$timeplace</td>";
            print "<td valign=top nowrap>$reference</td>";
            print "<td valign=top align=center>".int($dataRow->{distance})." km </td>" if ($type eq 'add');
            print "</tr>";
  		}

        print "</table>\n";
    } elsif ( $displayRows == 1 ) { # if only one row to display...
		$q->param(collection_no=>$dataRows[0]->{'collection_no'});
		# Do the action directly if there is only one row
		&$action;
		exit;
    } else {
		# If this is an add,  Otherwise give an error
		if ( $type eq "add" ) {
			displayEnterCollPage();
			return;
		} else {
			print stdIncludes( "std_page_top" );
			print "<center>\n<h3>Your search produced no matches</h3>";
			print "<p>Please try again with fewer search terms.</p>\n</center>\n";
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

		print qq|<input type="hidden" name="action" value="displayEnterCollPage">\n|;
		print qq|<center>\n<input type=submit value="Add a new collection">\n|;
		print qq|</center>\n</form>\n|;
	}
		
	print stdIncludes("std_page_bottom");

} # end sub displayCollResults


# JA 5-6.4.04
# compose the SQL to find collections of a certain age within 100 km of
#  a coordinate (required when the user wants to add a collection)
sub processCollectionsSearchForAdd	{

	my $sql = "SELECT c.collection_no, c.collection_aka, c.authorizer_no, p1.name authorizer, c.collection_name, c.access_level, c.research_group, c.release_date, DATE_FORMAT(release_date, '%Y%m%d') rd_short, c.country, c.state, c.latdeg, c.latmin, c.latsec, c.latdec, c.latdir, c.lngdeg, c.lngmin, c.lngsec, c.lngdec, c.lngdir, c.max_interval_no, c.min_interval_no, c.reference_no FROM collections c LEFT JOIN person p1 ON p1.person_no = c.authorizer_no WHERE ";

	# get a list of interval numbers that fall in the geological period
	my ($inlistref,$bestbothscale) = TimeLookup::processLookup($dbh,$dbt,'',$q->param('period_max'),'','','intervals');
	@intervals = @{$inlistref};

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

    main::dbg("process collections search for add: $sql");

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
        my $distance = 111 * GCD($mylat,$lat,abs($mylng-$lng));
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


# This function has been generalized to use by a number of different modules
# as a generic way of getting back collection results, including maps, collection search, confidence, and taxon_info
# These are simple options, corresponding to database fields, that can be passed in:
# These are more complicated options that can be passed in:
#   taxon_list: A list of taxon_nos to filter by (i.e. as passed by TaxonInfo)
#   most_recent: Only use the most recent reid of an occurrence when filtering by taxon_name,taxon_no
#   calling_script: Name of the script which called this function, only used for error message generation
#   no_authority_lookup: Don't hit the authorities table when lookup up a taxon name , only the occurrences/reids tables
# PS 08/11/2005
sub processCollectionsSearch {
    my $dbt = $_[0];
	my %options = %{$_[1]};
    my @fields = @{$_[2]};
	
    # Set up initial values
    my (@where,@occ_where,@reid_where,@tables,@from,@left_joins,@groupby,@errors);
    @tables = ("collections c");

    # There fields must always be here
	@from = ("c.authorizer_no","c.collection_no","c.collection_name","c.access_level","c.release_date","c.reference_no","DATE_FORMAT(release_date, '%Y%m%d') rd_short","c.research_group");
    
    # Now add on any requested fields
    foreach my $field (@fields) {
        if ($field eq 'authorizer') {
            push @from, "p1.name authorizer"; 
            push @left_joins, "LEFT JOIN person p1 ON p1.person_no = c.authorizer_no";
        } elsif ($field eq 'enterer') {
            push @from, "p2.name enterer"; 
            push @left_joins, "LEFT JOIN person p2 ON p2.person_no = c.enterer_no";
        } elsif ($field eq 'modifier') {
            push @from, "p3.name modifier"; 
            push @left_joins, "LEFT JOIN person p3 ON p3.person_no = c.modifier_no";
        } else {
            push @from, "c.$field";
        }
    }

    # Only necessary if we're doing a union
    if ($options{'sortby'}) {
        push @from, "c.$options{sortby}";
    }

	# Handle wildcards
	my $comparator = "=";
	my $wildcardToken = "";
	if ($options{'wild'} eq 'Y') {
 	 	$comparator = " LIKE ";
  		$wildcardToken = "%";
	}

	# First try the authorites table, else we just hit up the occurrences+reids table
    # Reworked so you can optionally only use the mostRecentReid in determining if an occurrence
    # matches a taxon_name PS 08/11/2005
	# Also searches reIDs table JA 16.8.02
	# Handles species name searches JA 19-20.8.02
    if ($options{'taxon_list'}) { 
        # taxon_list will generally be a list of numbers, but may be both is passed
        # in from Confidence.pm
        my ($taxon_sql,$occ_genus_sql,$reid_genus_sql,$occ_taxon_sql,$reid_taxon_sql,$both);
        foreach my $taxon_no_or_name (@{$options{'taxon_list'}}) {
            if ($taxon_no_or_name =~ /^\d+$/) {
                $taxon_sql .= $taxon_no_or_name . ", ";
            } else {
                my ($genus,$species) = split(/ /,$taxon_no_or_name);
                if ($genus) {
                    $occ_genus_sql .= " (o.genus_name LIKE ".$dbh->quote($genus);
                    $reid_genus_sql .= " (re.genus_name LIKE ".$dbh->quote($genus);
                    if ($species) {
                        $occ_genus_sql .= " AND o.species_name LIKE ".$dbh->quote($species);
                        $reid_genus_sql .= " AND re.species_name LIKE ".$dbh->quote($species);
                    }
                    $occ_genus_sql .= ")";
                    $reid_genus_sql .= ")";
                }
            }
        }
        $taxon_sql =~ s/, $//;
        if ($taxon_sql) {
            $occ_taxon_sql = "o.taxon_no IN ($taxon_sql)";
            $reid_taxon_sql = "re.taxon_no IN ($taxon_sql)";
        }
        if ($taxon_sql && $occ_genus_sql) {
            $both = " OR ";
        } else {
            $both = "";
        }
        
        push @where, "o.collection_no=c.collection_no";
        push @tables, "occurrences o";
        push @occ_where,"($occ_taxon_sql $both $occ_genus_sql)";
        push @reid_where," ($reid_taxon_sql $both $reid_genus_sql)";

    } elsif ($options{'taxon_name'}) {
        # Parse these values regardless
        my ($genus,$subgenus,$species);

        # Fix up the genus name and set the species name if there is a space 
        my @taxon_bits = split(/\s+/,$options{'taxon_name'});
        if (scalar(@taxon_bits) == 3) {
            ($genus,$subgenus,$species) = @taxon_bits;
            $subgenus =~ s/[\(\)]//g;
        } elsif (scalar(@taxon_bits) == 2) {
            ($genus,$species) = @taxon_bits;
        } elsif (scalar(@taxon_bits) == 1) {
            ($genus) = @taxon_bits;
        }

        # Set for displayOccsForReID
        $q->param('species_name' => $species) if ($species);
        $q->param('g_name' => $genus) if ($genus);

        my @taxon_nos;
        if (! $options{'no_authority_lookup'}) {
            my $taxon_sql = "SELECT taxon_no FROM authorities WHERE taxon_name LIKE ".$dbh->quote($options{'taxon_name'});
            @taxon_nos = map {$_->{taxon_no}} @{$dbt->getData($taxon_sql)}; 
        }

        # taxon is a homonym... make sure we get all versions of the homonym
        if (@taxon_nos) {
            # 1 or more authorities exists in the authorities table, parse them
            my %taxon_nos_unique = ();
            foreach my $taxon_no (@taxon_nos) {
                if ($options{'no_taxonomy_recurse'}) {
                    # In this case, just get alternative spellings, not the full tree of children
                    my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE child_no=$taxon_no";
                    my @results = @{$dbt->getData($sql)};
                    foreach my $row (@results) {
                        $taxon_nos_unique{$row->{'child_spelling_no'}} = 1;
                    }
                    $sql = "SELECT DISTINCT child_no FROM opinions WHERE child_spelling_no=$taxon_no";
                    @results = @{$dbt->getData($sql)};
                    foreach my $row (@results) {
                        $taxon_nos_unique{$row->{'child_no'}} = 1;
                    }
                } else {
                    my @all_taxon_nos = PBDBUtil::taxonomic_search($dbt,$taxon_no);
                    # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
                    @taxon_nos_unique{@all_taxon_nos} = ();
                }
            }
            my $taxon_nos_string = join(", ", keys %taxon_nos_unique);
            if (!$taxon_nos_string) {
                $taxon_nos_string = '-1';
                push @errors, "Could not find any collections matching taxononomic name entered.";
            }
                                                
            push @occ_where, "o.taxon_no IN ($taxon_nos_string)";
            push @reid_where, "re.taxon_no IN ($taxon_nos_string)";
        } else {
            # It doesn't exist in the authorities table, so now hit the occurrences table directly 
            if ( $genus)	{
                push @occ_where, "o.genus_name LIKE ".$dbh->quote($genus.$wildcardToken);
                push @reid_where, "re.genus_name LIKE ".$dbh->quote($genus.$wildcardToken);
            }
            if ( $subgenus)	{
                push @occ_where, "o.subgenus_name LIKE ".$dbh->quote($subgenus.$wildcardToken);
                push @reid_where, "re.subgenus_name LIKE ".$dbh->quote($subgenus.$wildcardToken);
            }
            if ( $species )	{
                push @occ_where, "o.species_name LIKE ".$dbh->quote($species.$wildcardToken);
                push @reid_where, "re.species_name LIKE ".$dbh->quote($species.$wildcardToken);
            }
        }
        push @where, "o.collection_no=c.collection_no";
        push @tables, "occurrences o";
	}

    # Handle time terms
	if ( $options{'max_interval'} || $options{'min_interval'}) {
        #These seeminly pointless four lines are necessary if this script is called from Download or whatever.
        # if the $q->param($var) is not set (undef), the parameters array passed into processLookup doesn't get
        # set properly, so make sure they can't be undef PS 04/10/2005
        my $eml_max = ($options{'eml_max_interval'} || '');
        my $max = ($options{'max_interval'} || '');
        my $eml_min = ($options{'eml_min_interval'} || '');
        my $min = ($options{'min_interval'} || '');
        if ($max =~ /[a-zA-Z]/ && !Validation::checkInterval($dbt,$eml_max,$max)) {
            push @errors, "There is no record of $eml_max $max in the database";
        }
        if ($min =~ /[a-z][A-Z]/ && !Validation::checkInterval($dbt,$eml_min,$min)) {
            push @errors, "There is no record of $eml_min $min in the database";
        }
 		my ($intervals,$bestbothscale) = TimeLookup::processLookup($dbh, $dbt, $eml_max,$max,$eml_min,$min,'intervals');
        my $val = join(",",@$intervals);
        if (!$val) {
            $val = "-1";
		    push @errors, "Please enter a valid time term or broader time range";
        }
        push @where, "c.max_interval_no IN ($val) AND c.min_interval_no IN (0,$val)";
	}
                                        
	# Handle half/quarter degrees for long/lat respectively passed by Map.pm PS 11/23/2004
    if ( $options{"coordres"} eq "half") {
		if ($options{"latdec_range"} eq "00") {
			push @where, "((latmin >= 0 AND latmin <15) OR " 
 						. "(latdec regexp '^(0|1|2\$|(2(0|1|2|3|4)))') OR "
                        . "(latmin IS NULL AND latdec IS NULL))";
		} elsif($options{"latdec_range"} eq "25") {
			push @where, "((latmin >= 15 AND latmin <30) OR "
 						. "(latdec regexp '^(4|3|(2(5|6|7|8|9)))'))";
		} elsif($options{"latdec_range"} eq "50") {
			push @where, "((latmin >= 30 AND latmin <45) OR "
 						. "(latdec regexp '^(5|6|7\$|(7(0|1|2|3|4)))'))";
		} elsif ($options{'latdec_range'} eq "75") {
			push @where, "(latmin >= 45 OR (latdec regexp '^(9|8|(7(5|6|7|8|9)))'))";
		}

		if ( $options{'lngdec_range'} eq "50" )	{
			push @where, "(lngmin>=30 OR (lngdec regexp '^(5|6|7|8|9)'))";
		} elsif ($options{'lngdec_range'} eq "00") {
			push @where, "(lngmin<30 OR (lngdec regexp '^(0|1|2|3|4)') OR (lngmin IS NULL AND lngdec
IS NULL))";
		}
    # assume coordinate resolution is 'full', which means full/half degress for long/lat
    # respectively 
	} else {
		if ( $options{'latdec_range'} eq "50" )	{
			push @where, "(latmin>=30 OR (latdec regexp '^(5|6|7|8|9)'))";
		} elsif ($options{'latdec_range'} eq "00") {
			push @where, "(latmin<30 OR (latdec regexp '^(0|1|2|3|4)') OR (latmin IS NULL AND latdec
IS NULL))";
		}
	}

    # Handle period
	if ($options{'period'}) {
		my $periodName = $dbh->quote($wildcardToken . $options{'period'} . $wildcardToken);
		push @where, "(period_min LIKE " . $periodName . " OR period_max LIKE " . $periodName . ")";
	}
	
	# Handle intage
	if ($options{'intage'}) {
		my $intageName = $dbh->quote($wildcardToken . $options{'intage'} . $wildcardToken);
		push @where, "(intage_min LIKE " . $intageName . " OR intage_max LIKE " . $intageName . ")";
	}
	
	# Handle locage
	if ($options{'locage'}) {
		my $locageName = $dbh->quote($wildcardToken . $options{'locage'} . $wildcardToken);
		push @where, "(locage_min LIKE " . $locageName . " OR locage_max LIKE " . $locageName . ")";
	}
	
	# Handle epoch
	if ($options{'epoch'}) {
		my $epochName = $dbh->quote($wildcardToken . $options{'epoch'} . $wildcardToken);
		push @where, "(epoch_min LIKE " . $epochName . " OR epoch_max LIKE " . $epochName . ")";
	}

    # Handle authorizer/enterer/modifier
    if ($options{'authorizer_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($options{'authorizer_reversed'});
        $options{'authorizer_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{authorizer_reversed} is not a valid authorizer. Format like 'Sepkoski, J.'" if (!$options{'authorizer_no'});
    }

    if ($options{'enterer_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($options{'enterer_reversed'});
        $options{'enterer_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{enterer_reversed} is not a valid enterer. Format like 'Sepkoski, J.'" if (!$options{'enterer_no'});
        
    }

    if ($options{'modifier_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($options{'modifier_reversed'});
        $options{'modifier_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{modifier_reversed} is not a valid modifier. Format like 'Sepkoski, J.'" if (!$options{'modifier_no'});
    }

	# Handle modified date
	if ($options{'modified_since'} || $options{'year'})	{
        my ($yyyy,$mm,$dd);
        if ($options{'modified_since'}) {
            my $nowDate = now();
            if ( "yesterday" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'1D';
            } elsif ( "two days ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'2D';
            } elsif ( "three days ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'3D';
            } elsif ( "last week" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'7D';
            } elsif ( "two weeks ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'14D';
            } elsif ( "three weeks ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'21D';
            } elsif ( "last month" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'1M';
            }
            my ($date,$time) = split / /,$nowDate;
            ($yyyy,$mm,$dd) = split /-/,$date,3;
        } elsif ($options{'year'}) {
            $yyyy = $options{'year'};
            $mm = $options{'month'};
            $dd = $options{'date'};
        }  

        my $val = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
        if ( $options{'beforeafter'} eq "created after" )  {
            push @where, "created > $val";
        } elsif ( $q->param("beforeafter") eq "created before" )    {
            push @where, "created < $val";
        } elsif ( $q->param("beforeafter") eq "modified after" )    {
            push @where, "modified > $val";
        } elsif ( $q->param("beforeafter") eq "modified before" )   {
            push @where, "modified < $val";
        } 
	}
	
	# Handle collection name (must also search collection_aka field) JA 7.3.02
	if ( $options{'collection_names'} ) {
		my $val = $dbh->quote($wildcardToken . $options{'collection_names'} . $wildcardToken);
		push(@where, "(collection_name LIKE $val OR collection_aka LIKE $val)");
	}
	
    # Handle localbed, regionalbed
    if ($options{'regionalbed'} && $options{'regionalbed'} =~ /^[0-9.]+$/) {
        my $min = int($options{'regionalbed'});
        my $max = $min + 1;
        push @where,"regionalbed >= $min","regionalbed <= $max";
    }
    if ($options{'localbed'} && $options{'localbed'} =~ /^[0-9.]+$/) {
        my $min = int($options{'localbed'});
        my $max = $min + 1;
        push @where ,"localbed >= $min","localbed <= $max";
    }

    # Maybe special environment terms
    if ( $options{'environment'}) {
        my $environment;
        if ($options{'environment'} =~ /General/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_general'}});
        } elsif ($options{'environment'} =~ /Terrestrial/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_terrestrial'}});
        } elsif ($options{'environment'} =~ /Siliciclastic/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_siliciclastic'}});
        } elsif ($options{'environment'} =~ /Carbonate/) {
            $environment = join(",", map {"'".$_."'"} @{$hbo->{'SELECT_LISTS'}{'environment_carbonate'}});
        } else {
            $environment = $dbh->quote($options{'environment'});
        }
        if ($environment) {
            push @where, "c.environment IN ($environment)";
        }
    }
		
	# research_group is now a set -- tone 7 jun 2002
	if($options{'research_group'}) {
        my $research_group_sql = PBDBUtil::getResearchGroupSQL($dbt,$options{'research_group'});
        push @where, $research_group_sql if ($research_group_sql);
	}
    
	if (int($options{'reference_no'})) {
		push @where, " (c.reference_no=".int($options{'reference_no'})." OR sr.reference_no=".int($options{'reference_no'}).") ";
    }

    # Do a left join on secondary refs if we have to
    # PS 11/29/2004
    if ($options{'research_group'} =~ /^(?:decapod|ETE|5%|1%|PACED|PGAP)$/ || int($options{'reference_no'})) {
        push @left_joins, "LEFT JOIN secondary_refs sr ON sr.collection_no=c.collection_no";
    }

        
	# note, we have one field in the collection search form which is unique because it can
	# either be geological_group, formation, or member.  Therefore, it has a special name, 
	# group_formation_member, and we'll have to deal with it separately.
	# added by rjp on 1/13/2004
	if ($options{"group_formation_member"}) {
        my $val = $dbh->quote($wildcardToken.$options{"group_formation_member"}.$wildcardToken);
		push(@where, "(c.geological_group LIKE $val OR c.formation LIKE $val OR c.member LIKE $val)");
	}

    # This field is only passed by section search form PS 12/01/2004
    if (exists $options{"section_name"} && $options{"section_name"} eq '') {
        push @where, "((c.regionalsection IS NOT NULL AND c.regionalsection != '' AND c.regionalbed REGEXP '^[0-9.]+\$') OR (c.localsection IS NOT NULL AND c.localsection != '' AND c.localbed REGEXP '^[0-9.]+\$'))";
    } elsif ($options{"section_name"}) {
        my $val = $dbh->quote($wildcardToken.$options{"section_name"}.$wildcardToken);
        push @where, "((c.regionalsection  LIKE  $val AND c.regionalbed REGEXP '^[0-9.]+\$') OR (c.localsection  LIKE  $val AND c.localbed REGEXP '^[0-9.]+\$'))"; 
    }                

    # This field is only passed by links created in the Strata module PS 12/01/2004
	if ($options{"lithologies"}) {
        my $val = $dbh->quote($options{"lithologies"});
		push @where, "(c.lithology1=$val OR c.lithology2=$val)"; 
	}


    # get the column info from the table
    my $sth = $dbh->column_info(undef,'pbdb','collections','%');
    
	# Compose the WHERE clause
	# loop through all of the possible fields checking if each one has a value in it
    while (my $row = $sth->fetchrow_hashref()) {
        my $field = $row->{'COLUMN_NAME'};
        my $type = $row->{'TYPE_NAME'};
        my $is_nullable = ($row->{'IS_NULLABLE'} eq 'YES') ? 1 : 0;
        my $is_primary =  $row->{'mysql_is_pri_key'};
            
        # These are special cases handled above in code, so skip them
        next if ($field =~ /^(?:environment|localbed|regionalbed|research_group|reference_no|max_interval_no|min_interval_no)$/);

		if (exists $options{$field} && $options{$field} ne '') {
            my $value = $options{$field};
            if ($value eq "NOT_NULL_OR_EMPTY") {
                push @where, "(c.$field IS NOT NULL AND c.$field !='')";
            } elsif ($value eq "NULL_OR_EMPTY") {
                push @where, "(c.$field IS NULL OR c.$field ='')";
			} elsif ( $type =~ /ENUM/i) {
				# It is in a pulldown... no wildcards
				push @where, "c.$field=".$dbh->quote($value);
			} elsif ( $type =~ /SET/i) {
                # Its a set, use the special set syntax
		        push @where, "FIND_IN_SET(".$dbh->quote($value).", c.$field)";
			} elsif ( $type =~ /INT/i) {
                # Don't need to quote ints, however cast them to int a security measure
                push @where, "c.$field=".int($value);
			} else {
                # Assuming character, datetime, etc. 
				push @where, "c.$field $comparator ".$dbh->quote($wildcardToken.$value.$wildcardToken);
			}
		}
	}


    # Commented out PS 08/16 - don't see when this would ever get executed
	# if first search failed and wild cards were used, try again
	#  stripping first wildcard JA 22.2.02
	#if ( !@terms && $wildcardToken ne "")	{
	#	foreach $fieldName (@fieldNames) {
	#			
	#		$fieldCount++;
	#		if ( my $val = $q->param($fieldName)) {
	#			$val =~ s/"//g;
	#			$val = qq|"$val$wildcardToken"| if $fieldTypes[$fieldCount] == 0;
	#			push(@terms, "collections.$fieldName$comparator$val");
	#		}
	#	}
	#}

    # Print out an errors that may have happened.
    # htmlError print header/footer and quits as well

    if (!scalar(@where)) {
        push @errors, "No search terms were entered";
    }
    
	if (@errors) {
        my $plural = (scalar(@errors) > 1) ? "s" : "";
        my $message = "<br><div align=center><table width=600 border=0>" .
              "<tr><td class=darkList><font size='+1'><b> Error$plural</b></font></td></tr>" .
              "<tr><td>";
        $message .= "<li class='medium'>$_</li>" for (@errors);
        $message .= "</td></tr></table>";
        if ( $options{"calling_script"} eq "Map" )	{
            $message .= "<a href=\"bridge.pl?action=displayMapForm\"><b>Try again</b></a>";
        } elsif ( $options{"calling_script"} eq "Confidence" )	{
            $message .= "<a href=\"bridge.pl?action=displaySearchSectionForm\"><b>Try again</b></a>";
        } elsif ( $options{"type"} eq "add" )	{
            $message .= "<a href=\"bridge.pl?action=displaySearchCollsForAdd&type=add\"><b>Try again</b></a>";
        } else	{
            $message .= "<a href=\"bridge.pl?action=displaySearchColls&type=$options{type}\"><b>Try again</b></a>";
        }
        $message .= "</div><br>";
        if ($options{'calling_script'} !~ /Map|Confidence|TaxonInfo/) {
            print stdIncludes( "std_page_top" ) 
        }
        if ($options{'calling_script'} !~ /TaxonInfo/) {
            print $message;
            print stdIncludes("std_page_bottom");     
        } else {
            return ([],0);
        }
        exit 1;
	}


    # Cover all our bases
    if (scalar(@left_joins) || scalar(@tables) > 1 || $options{'taxon_list'} || $options{'taxon_name'}) {
        push @groupby,"c.collection_no";
    }

    if ($options{'taxon_list'} || $options{'taxon_name'}) {
        # Reworked PS  08/15/2005
        # Instead of doing a left join on the reids table, we achieve the close to the same effect
        # with a union of the (collections,occurrences left join reids where reid_no IS NULL) UNION (collections,occurrences,reids).
        # but for the first SQL in the union, we use o.taxon_no, while in the second we use re.taxon_no
        # This has the advantage in that it can use indexes in each case, thus is super fast (rather than taking ~5-8s for a full table scan)
        # Just doing a simple left join does the full table scan because an OR is needed (o.taxon_no IN () OR re.taxon_no IN ())
        # and because you can't use indexes for tables that have been LEFT JOINED as well
        # By hitting the occ/reids tables separately, it also has the advantage in that it excludes occurrences that
        # have been reid'd into something that no longer is the taxon name and thus shouldn't show up.
        my @left_joins1 = (@left_joins,'LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no');

        my @where1 = (@where,@occ_where);
        if ($options{'most_recent'}) {
            # This term very important.  sql1 deal with occs with NO reid, sql2 deals with only reids
            # So if an occ is reid'd, we only one to count the most recent reid -- thus we discount the original
            # off in the search.
            push @where1,"re.reid_no IS NULL";
        } 
        my $sql1 = "SELECT ".join(",",@from).
               " FROM " .join(",",@tables)." ".join (" ",@left_joins1).
               " WHERE ".join(" AND ",@where1);
        $sql1 .= " GROUP BY ".join(",",@groupby) if (@groupby);

        my @where2 = ('re.occurrence_no=o.occurrence_no',@where,@reid_where);
        if ($options{'most_recent'}) {
            push @where2, "re.most_recent='YES'";
        }
        my @tables2 = (@tables,'reidentifications re');
        my $sql2 = "SELECT ".join(",",@from).
               " FROM " .join(",",@tables2)." ".join (" ",@left_joins).
               " WHERE ".join(" AND ",@where2);
        $sql2 .= " GROUP BY ".join(",",@groupby) if (@groupby);
        $sql = "($sql1) UNION ($sql2)";   
    } else {
        $sql = "SELECT ".join(",",@from).
               " FROM " .join(",",@tables)." ".join (" ",@left_joins).
               " WHERE ".join(" AND ",@where);
        $sql .= " GROUP BY ".join(",",@groupby) if (@groupby);  
    }
	# Handle sort order
    if ($options{'sortby'}) {
        my $sortBy = $options{'sortby'};
        $sortBy =~ s/[^a-zA-Z0-9_]//g;
	    $sortBy .= " DESC" if ($options{'sortorder'} =~ /desc/i);
        $sql .= " ORDER BY $sortBy";
    }
    dbg("Collections sql: $sql");

    $sth = $dbt->dbh->prepare($sql);
    $sth->execute();
    my $p = Permissions->new ($s,$dbt); 

    # See if rows okay by permissions module
    my @dataRows = ();
    my $limit = (int($options{'limit'})) ? int($options{'limit'}) : 10000000;
    my $ofRows = 0;
    $p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );        

    return (\@dataRows,$ofRows);
} # end sub processCollectionsSearch




#  * User selects a collection from the displayed list
#  * System displays selected collection
sub displayCollectionDetails {
	my $collection_no = $q->param('collection_no');
	
	$sql = "SELECT p1.name authorizer, p2.name enterer, p3.name modifier, c.* FROM collections c LEFT JOIN person p1 ON p1.person_no=c.authorizer_no LEFT JOIN person p2 ON p2.person_no=c.enterer_no LEFT JOIN person p3 ON p3.person_no=c.modifier_no WHERE collection_no=" . $collection_no;
    dbg("Main SQL: $sql");
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
		
	my @fieldNames = @{$sth->{NAME}};
	my $numFields  = $sth->{NUM_OF_FIELDS};
	
	my @fieldTypes = @{$sth->{mysql_type}};
	#for($i = 0;$i < $numFields;$i++){
	#	if ( $fieldTypes[$i] == 254)
	#	{
	#	  print $fieldNames[$i] . ": " . $fieldTypes[$i] . "<br>";
	#	}
	#}
	my @row = $sth->fetchrow_array();
	$sth->finish();
  
	# Get the name of the authorizer
	my $fieldCount = 0;
	my ($authorizer_no, $refNo);
	
    foreach my $tmpVal (@fieldNames) {
		if ( $tmpVal eq 'authorizer_no') {
			$authorizer_no = $row[$fieldCount];
		} elsif ( $tmpVal eq 'reference_no') {
			$refNo = $row[$fieldCount];
		} elsif ( $tmpVal eq 'collection_subset' && $row[$fieldCount] ne "") {
			my $collno = $row[$fieldCount];
			my $linkFront = "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collno\">";
			$row[$fieldCount] = $linkFront . $collno . "</a> ";
		}
		$fieldCount++;
	}

    # Get the reference
    $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$refNo;
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my $refRowRef = $sth->fetchrow_arrayref();

    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($refRowRef, $md);
    my $bibRef = BiblioRef->new($drow);
    $sth->finish();
    my $refString = $bibRef->toString();
    push(@row, $refString);
    push(@fieldNames, 'reference_string');

	# get the secondary_references
	push(@row, PBDBUtil::getSecondaryRefsString($dbh,$collection_no,0,0));
	push(@fieldNames, 'secondary_reference_string');

	# Get any subset collections JA 25.6.02
	$sql = "SELECT collection_no FROM collections where collection_subset=" . $collection_no;
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my @subrowrefs = @{$sth->fetchall_arrayref()};
    $sth->finish();
    my $subString;
    for $subrowref (@subrowrefs)	{
      my @subrow = @{$subrowref};
      my $collno = $subrow[0];
      my $linkFront = "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collno\">";
      $subString .= $linkFront . $collno . "</a> ";
    }
    push(@row, $subString);
    push(@fieldNames, 'subset_string');
	
	# get the max/min interval names
	my ($r,$f) = getMaxMinNamesAndDashes(\@row,\@fieldNames);
	@row = @{$r};
	@fieldNames = @{$f};

    # have the regional section link to a local section search
    my ($regionalsection_idx, $regionalsection_link);
    for(my $index=0;$index < $#fieldNames;$index++) {
        $regionalsection_idx = $index if ($fieldNames[$index] eq "regionalsection");
    }
    if ($row[$regionalsection_idx]) {
        $regionalsection_link = "<a href=\"$exec_url?action=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=regional"
                         . "&input=".uri_escape($row[$regionalsection_idx])."\">$row[$regionalsection_idx]</a>";
    }
    $row[$regionalsection_idx] = $regionalsection_link if ($regionalsection_link);
    
    # have the local section link to a local section search
    my ($localsection_idx, $localsection_link);
    for(my $index=0;$index < $#fieldNames;$index++) {
        $localsection_idx = $index if ($fieldNames[$index] eq "localsection");
    }
    if ($row[$localsection_idx]) {
        $localsection_link = "<a href=\"$exec_url?action=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=local"
                         . "&input=".uri_escape($row[$localsection_idx])."\">$row[$localsection_idx]</a>";
    }
    $row[$localsection_idx] = $localsection_link if ($localsection_link);


    # have the geological group/formation/members hyperlink to strata search
    my ($group_idx, $formation_idx, $member_idx, $group_link, $formation_link, $member_link);
    for(my $index=0;$index < $#fieldNames;$index++) {
        $group_idx = $index if ($fieldNames[$index] eq "geological_group");
        $formation_idx = $index if ($fieldNames[$index] eq "formation");
        $member_idx = $index if ($fieldNames[$index] eq "member");
    }
    if ($row[$group_idx]) {
        $group_link = "<a href=\"$exec_url?action=displayStrataSearch"
                         . "&group_formation_member=".uri_escape($row[$group_idx])."\">$row[$group_idx]</a>";
    }
    if ($row[$formation_idx]) {   
        $formation_link = "<a href=\"$exec_url?action=displayStrataSearch"
                             . "&group_hint=".uri_escape($row[$group_idx])
                             . "&group_formation_member=".uri_escape($row[$formation_idx])."\">$row[$formation_idx]</a>";
    }
    if ($row[$member_idx]) {
        $member_link = "<a href=\"$exec_url?action=displayStrataSearch"
                          . "&formation_hint=".uri_escape($row[$formation_idx])
                          . "&group_formation_member=".uri_escape($row[$member_idx])."\">$row[$member_idx]</a>";
    }
    $row[$group_idx] = $group_link if ($group_link);
    $row[$formation_idx] = $formation_link if ($formation_link);
    $row[$member_idx] = $member_link if ($member_link);

    print stdIncludes( "std_page_top" );
	
    print $hbo->populateHTML('collection_display_fields', \@row, \@fieldNames);
		
    # If the viewer is the authorizer (or it's me), display the record with edit buttons
	print '<p><div align="center"><table><tr>';
    if ($s->isDBMember() && (($authorizer_no eq $s->get('authorizer_no')) || ($s->get('authorizer') eq "J. Alroy"))) {
		print '<td>'.$hbo->populateHTML('collection_display_buttons', \@row, \@fieldNames).'</td>';
    }
    if ($s->isDBMember()) {
	    print '<td>'.$hbo->populateHTML('collection_display_buttons2', [$q->param('collection_no')],['prefill_collection_no']).'</td>';
    }
	print '</tr></table></div></p>';
	
	print "<HR>\n";
	
	# rjp, 1/2004.  This is the new routine which handles all reids instead
	# of just the first and the last.  To rever to the old way, comment
	# out the following three lines, and uncomment the fourth.
	# JA: HTMLFormattedTaxonomicList is totally redundant and is therefore
	#  deprecated
	#my $collection = Collection->new(\%GLOBALVARS);
	#$collection->setWithCollectionNumber($collection_no);
	#my $taxa_list = $collection->HTMLFormattedTaxonomicList();
	
	my $taxa_list = buildTaxonomicList($collection_no, $refNo);
	
	
	print $taxa_list;

	print '<p><div align="center"><table><tr>';
	if ( $taxa_list =~ /Abundance/ )	{
		print "<td>".$hbo->populateHTML('rarefy_display_buttons', \@row, \@fieldNames)."</td>";
	}

	print "<td>".$hbo->populateHTML('ecology_display_buttons', \@row, \@fieldNames)."</td>";

	if($authorizer_no eq $s->get('authorizer_no') || $s->get('authorizer') eq "J. Alroy")	{
		print "<td>".$hbo->populateHTML('occurrence_display_buttons', \@row, \@fieldNames)."</td>";;
	}
	if($taxa_list ne "" && $s->isDBMember()) {
		print "<td>".$hbo->populateHTML('reid_display_buttons', \@row, \@fieldNames)."</td>";
	}
	print "</tr></table></div></p>";

	print stdIncludes("std_page_bottom");
} # end sub displayCollectionDetails()


# written around excised chunk of code from above JA 30.7.03
# first part gets interval names matching numbers in intervals table;
# second part figures out whether to display dashes in time interval fields
sub getMaxMinNamesAndDashes	{

	my $r = shift;
	my $f = shift;

	my @row = @{$r};
	my @fieldNames = @{$f};

	# get the interval names by querying the intervals table JA 11.7.03
	# also get the E/M/Ls JA 17.7.03
	my ($max_interval_no,$min_interval_no,$fieldCount);
    $fieldCount = "";
	for my $tmpVal (@fieldNames) {
		if ( $tmpVal eq 'max_interval_no') {
			$max_interval_no = $row[$fieldCount];
		} elsif ( $tmpVal eq 'min_interval_no' )	{
			$min_interval_no = $row[$fieldCount];
		}
		if ( $max_interval_no && $min_interval_no )	{
			last;
		}
		$fieldCount++;
	}
	if ( $max_interval_no )	{
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $max_interval_no;
		unshift @row, @{$dbt->getData($sql)}[0]->{eml_interval};
		unshift @row, @{$dbt->getData($sql)}[0]->{interval_name};
	} else	{
		unshift @row, '';
		unshift @row, '';
	}
	unshift @fieldNames, 'eml_max_interval';
	unshift @fieldNames, 'max_interval';

	if ( $min_interval_no )	{
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $min_interval_no;
		unshift @row, @{$dbt->getData($sql)}[0]->{eml_interval};
		unshift @row, @{$dbt->getData($sql)}[0]->{interval_name};
	} else	{
		unshift @row, '';
		unshift @row, '';
	}
	unshift @fieldNames, 'eml_min_interval';
	unshift @fieldNames, 'min_interval';

	# check whether we have period/epoch/locage/intage max AND/OR min:
	for my $term ("_interval","epoch_","intage_","locage_","period_"){
		my $max = 0;
		my $min = 0;
		for my $index (0..scalar(@fieldNames)){
			if($fieldNames[$index] eq "max".$term && $row[$index]){
				$max = 1;
			}
			elsif($fieldNames[$index] eq "min".$term && $row[$index]){
				$min = 1;
			}
			if($fieldNames[$index] eq $term."max" && $row[$index]){
				$max = 1;
			}
			elsif($fieldNames[$index] eq $term."min" && $row[$index]){
				$min = 1;
			}
		}
		# Do this regardless:
		my $termtitle = $term."title";
		if ( $term =~ /_interval/ )	{
			$termtitle = "title".$term;
		}
		push(@fieldNames,$termtitle);
		if($max || $min){
			# There is no corresponding span, so this is just a placeholder, 
			# but necessary so that htmlbuilder doesn't wipe the contents of 
			# the div tags.
			push(@row, "dummy"); 

			if($max && $min){
				if ( $term =~ /_interval/ )	{
					push(@fieldNames,"dash".$term);
				} else	{
					push(@fieldNames,$term."dash");
				}
				push(@row, " - ");
			}
			elsif($min && !$max){
				if ( $term =~ /_interval/ )	{
					push(@fieldNames, "min_only".$term);
				} else	{
					push(@fieldNames, $term."min_only");
				}
				push(@row, "<p class=\"small\">(minimum)</p>");
			}
		}
		# This will cause the whole "*_title" section to be erased.
		else{
			push(@row,"");
		}
	}

	return (\@row, \@fieldNames);

}





# builds the list of occurrences shown in places such as the collections form
# must pass it the collection_no
# reference_no (optional or not?? - not sure).
#
# optional arguments:
#
# gnew_names	:	reference to array of new genus names the user is entering (from the form)
# subgnew_names	:	reference to array of new subgenus names the user is entering
# snew_names	:	reference to array of new species names the user is entering
sub buildTaxonomicList {
	my $collection_no = shift;
	my $collection_refno = shift;
	
	my $gnew_names = shift;				
	my $subgnew_names = shift;			
	my $snew_names = shift;	
	

	# dereference arrays.
	my @gnew_names = @{$gnew_names};
	my @subgnew_names = @{$subgnew_names};
	my @snew_names = @{$snew_names};
	
	my $new_found = 0;		# have we found new taxa?  (ie, not in the database)
	my $return = "";

	# This is the taxonomic list part
	$sql =	"SELECT abund_value, ".
			"       abund_unit, ".
			"       genus_name, ".
			"       genus_reso, ".
			"       subgenus_name, ".
			"       subgenus_reso, ".
			"       species_name, ".
			"       species_reso, ".
			"       comments, ".
			"       reference_no, ".
			"       occurrence_no, ".
			"       taxon_no ".
			"  FROM occurrences ".
			" WHERE collection_no = $collection_no";

	my @rowrefs = @{$dbt->getData($sql)};

	if (@rowrefs) {
		my @grand_master_list = ();

		# loop through each row returned by the query
		foreach my $rowref (@rowrefs) {
			my $output = '';
			my %grand_master_hash = ();
			my %classification = ();

			# For sorting, later
			$grand_master_hash{occurrence_no}	= 	$rowref->{occurrence_no};
			$grand_master_hash{comments} 		= 	$rowref->{comments};
			$grand_master_hash{abund_value}		= 	$rowref->{abund_value};
			$grand_master_hash{reference_no}	= 	$rowref->{reference_no};

            # If we have specimens
            my $sql_s = "SELECT count(*) c FROM specimens WHERE occurrence_no=$rowref->{occurrence_no}";
            my $specimens_measured = ${$dbt->getData($sql_s)}[0]->{'c'};
            if ($specimens_measured) {
                my $pl = ($specimens_measured > 1) ? 's' : '';
                $rowref->{comments} .= " (<a href=\"bridge.pl?action=displaySpecimenList&occurrence_no=$rowref->{occurrence_no}\">$specimens_measured measurement$pl</a>)";
            }
			
			# if the user submitted a form such as adding a new occurrence or 
			# editing an existing occurrence, then we'll bold face any of the
			# new taxa which we don't already have in the database.
			
			# check for unrecognized genus names
			foreach my $nn (@gnew_names){
				if ($rowref->{genus_name} eq  $nn) {
					$rowref->{genus_name} = "<b>".$rowref->{genus_name}."</b>";
					$new_found = 1;
				}
			}

			# check for unrecognized subgenus names
			foreach my $nn (@subgnew_names){
				if($rowref->{subgenus_name} eq $nn){
					$rowref->{subgenus_name} = "<b>".$rowref->{subgenus_name}."</b>";
					$new_found = 1;
				}
			}

			# check for unrecognized species names
			foreach my $nn (@snew_names){
				if($rowref->{species_name} eq $nn){
					$rowref->{species_name}="<b>".$rowref->{species_name}."</b>";
					$new_found = 1;
				}
			}

			# tack on the author and year if the taxon number exists
			# JA 19.4.04
			if ( $rowref->{taxon_no} )	{
			# If the authority is a PBDB ref, retrieve and print it
				my $sql = "SELECT * FROM authorities WHERE taxon_no=" . $rowref->{taxon_no};
				my %taxData = %{@{$dbt->getData($sql)}[0]};
			# JA 23.4.04: don't do this is the species name looks
			#  like a species but the taxon's rank is not species
				if ( $rowref->{species_name} =~ /(indet\.)|(sp\.)/ || $taxData{taxon_rank} eq "species" )	{
					if ( $taxData{'ref_is_authority'} )	{
						$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=";
						$sql .= $taxData{'reference_no'};
						my %refRow = %{@{$dbt->getData($sql)}[0]};
						$rowref->{authority} = Reference::formatShortRef(\%refRow );
					}
				# Otherwise, use what authority info can be found
					else	{
						$rowref->{authority} = Reference::formatShortRef(\%taxData );
					}
				}
			}

			my $formatted_reference = '';

			# if the occurrence's reference differs from the collection's, print it
			my $newrefno = $rowref->{'reference_no'};
			if ($newrefno != $collection_refno)	{
				$rowref->{reference_no} = buildReference($newrefno,"list");
			} else {
				$rowref->{reference_no} = '';
			}

			my $arg = "";
			my $reidTable = "";
			my @occrow = ();
			my @occFieldNames = ();
			
			# put all keys and values from the current occurrence
			# into two separate arrays.
			while((my $key, my $value) = each %{$rowref}){
				push(@occFieldNames, $key);
				push(@occrow, $value);
			}
	
			# get the most recent reidentification of this occurrence.  
			my $mostRecentReID = PBDBUtil::getMostRecentReIDforOcc($dbt,$rowref->{occurrence_no});
			
			# if the occurrence has been reidentified at least once, then 
			# display the original and reidentifications.
			if ($mostRecentReID) {
				$output = $hbo->populateHTML("taxa_display_row", \@occrow, \@occFieldNames );
				
				# rjp, 1/2004, change this so it displays *all* reidentifications, not just
				# the last one.
	# JA 2.4.04: this was never implemented by Poling, who instead went
	#  renegade and wrote the entirely redundant HTMLFormattedTaxonomicList;
	#  the correct way to do it was to pass in $rowref->{occurrence_no} and
	#  isReidNo = 0 instead of $mostRecentReID and isReidNo = 1
		
                %classification = ();
				$output .= getReidHTMLTableByOccNum($rowref->{occurrence_no}, 0, \%classification);
				
				$grand_master_hash{'class_no'} = ($classification{'class'}{'taxon_no'} or 1000000);
				$grand_master_hash{'order_no'} = ($classification{'order'}{'taxon_no'} or 1000000);
				$grand_master_hash{'family_no'} = ($classification{'family'}{'taxon_no'} or 1000000);
			}
		# otherwise this occurrence has never been reidentified
			else {
				
		# get the classification (by PM): changed 2.4.04 by JA to
		#  use the occurrence number instead of the taxon name
				$class_hash = Classification::get_classification_hash($dbt,'class,order,family',[$rowref->{'taxon_no'}],'array');
                # Index 0 is class, 1 is order, 2 is family. add -1 to split to keep empty trailing fields
                @class_array = @{$class_hash->{$rowref->{'taxon_no'}}};
                %classification = ();
                foreach my $taxon (@class_array) {
                    $classification{$taxon->{'taxon_rank'}} = $taxon;
                }
                # If its a higher order name and indet, like "Omomyidae indet." get the higher order name as well
                if ($rowref->{'taxon_no'}) {
                    my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$rowref->{'taxon_no'});
                    if ($taxon->{'taxon_rank'} =~ /^(?:family|order|class)$/) {
                        $classification{$taxon->{'taxon_rank'}} = $taxon;
                    }
                }
                
				# for sorting, later
				$grand_master_hash{'class_no'} = ($classification{'class'}{'taxon_no'} or 1000000);
				$grand_master_hash{'order_no'} = ($classification{'order'}{'taxon_no'} or 1000000);
				$grand_master_hash{'family_no'} = ($classification{'family'}{'taxon_no'} or 1000000);

				if (%classification) {
					push(@occrow, "bogus");
					push(@occFieldNames, 'higher_taxa');
					push(@occrow, $classification{'class'}{'taxon_name'});
					push(@occFieldNames, 'class');
					push(@occrow, $classification{'order'}{'taxon_name'});
					push(@occFieldNames, 'order');
					push(@occrow, $classification{'family'}{'taxon_name'});
					push(@occFieldNames, 'family');
				}

				$output = $hbo->populateHTML("taxa_display_row", \@occrow, \@occFieldNames );

				if (%classification) {
					pop(@occrow);
					pop(@occrow);
					pop(@occrow);
					pop(@occrow);
					pop(@occFieldNames);
					pop(@occFieldNames);
					pop(@occFieldNames);
					pop(@occFieldNames);
				}
			}

			# If genus is informal, don't link.
			$output =~ s/(<i>|<\/i>)//g;
			if($output =~ /informal(.*)?<genus>/){
				# do nothing
			}
			# If species is informal, link only the genus.
			elsif($output =~ /<genus>(.*)?<\/genus>(.*)?informal/s){
				$output =~ s/<r_genus>(.*)?<\/r_genus> <genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$2"><i>$1$2$3$4<\/i><\/a>/g;
			}
            # N sp. id the only species reso that comes after the species
			elsif($output =~ /n\. sp\.\s*<species>/s){
				$output =~ s/<r_genus>(.*)?<\/r_genus> <genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$2"><i>$1$2$3$4<\/i><\/a>/g;
				$output =~ s/(n\. sp\.)(.*)?<\/i><\/a>/$2 $1<\/a>/g;
			}
			# shouldn't be any <i> tags for indet's.
			elsif($output =~ /<species>indet/s){
				$output =~ s/<r_genus>(.*)?<\/r_genus> <genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$2"><i>$1$2$3$4<\/i><\/a>/g;
				$output =~ s/<i>(.*)?indet(\.{0,1})<\/i><\/a>/$1indet$2<\/a>/;
			}
			else{
				# match multiple rows as a single (use the 's' modifier)
				$output =~ s/<r_genus>(.*)?<\/r_genus> <genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$2+$4"><i>$1 $2$3$4<\/i><\/a>/g;
			}
			# ---------------------------------

			# Clean up abundance values (somewhat messy, but works, and better
			#   here than in populateHTML) JA 10.6.02
			$output =~ s/(>1 specimen)s|(>1 individual)s/$1$2/g;
			
            $grand_master_hash{html} = $output;
			push(@grand_master_list, \%grand_master_hash);
		}

		# Look at @grand_master_list to see every record has class_no, order_no,
		# family_no,  reference_no, abundance_unit and comments. 
		# If ALL records are missing any of those, don't print the header
		# for it.
		my ($class_nos, $order_nos, $family_nos, $reference_nos, 
			$abund_values, $comments) = (0,0,0,0,0,0);
		foreach my $row (@grand_master_list){
			$class_nos++ if($row->{class_no} && $row->{class_no} != 1000000);
			$order_nos++ if($row->{order_no} && $row->{order_no} != 1000000);
			$family_nos++ if($row->{family_no} && $row->{family_no} != 1000000);
			$reference_nos++ if($row->{reference_no} && $row->{reference_no} != $collection_refno);
			$abund_values++ if($row->{abund_value});
			$comments++ if($row->{comments});
		}
			
		$sql = "SELECT collection_name FROM collections ".
			   "WHERE collection_no=$collection_no";
		my @coll_name = @{$dbt->getData($sql)};

		# Taxonomic list header
		$return = "
		<div align='center'>
		<h3>Taxonomic list for " . $coll_name[0]->{collection_name} .
		" (PBDB collection $collection_no)</h3>";

		if ($new_found) {
			$return .= "<h3><font color=red>WARNING!</font> Taxon names in ".
					   "<b>bold</b> are new to the occurrences table.</h3><p>Please make ".
					   "sure the spelling is correct. If it isn't, DON'T hit the back button; hit the \"Edit occurrences\" button below.</p>";
		}

		$return .= "<table border=\"0\" cellpadding=\"3\" cellspacing=\"0\"><tr>";

		if($class_nos == 0){
			$return .= "<td nowrap></td>";
		}
		else{
			$return .= "<td nowrap><u>Class</u></td>";
		}
		if($order_nos == 0){
			$return .= "<td></td>";
		}
		else{
			$return .= "<td><u>Order</u></td>";
		}
		if($family_nos == 0){
			$return .= "<td></td>";
		}
		else{
			$return .= "<td><u>Family</u></td>";
		}

		# if ALL taxa have no genus or species, we have no list,
		# so always print this.
		$return .= "<td><u>Taxon</u></td>";

		if($reference_nos == 0){
			$return .= "<td></td>";
		}
		else{
			$return .= "<td><u>Reference</u></td>";
		}
		if($abund_values == 0){
			$return .= "<td></td>";
		}
		else{
			$return .= "<td><u>Abundance</u></td>";
		}
		if($comments == 0){
			$return .= "<td></td>";
		}
		else{
			$return .= "<td><u>Comments</u></td></tr>";
		}

		# Sort:
		my @sorted = sort{ $a->{class_no} <=> $b->{class_no} ||
						   $a->{order_no} <=> $b->{order_no} ||
						   $a->{family_no} <=> $b->{family_no} ||
						   $a->{occurrence_no} <=> $b->{occurrence_no}} @grand_master_list;

        # Don't do any more sorting if all occs had NO other classification info.
        unless($class_nos == 0 && $order_nos == 0 && $family_nos == 0 ){
            # Now sort the ones that had no class or order or family by occ_no.
            my @occs_to_sort = ();
            while($sorted[-1]->{class_no} == 1000000 &&
                  $sorted[-1]->{order_no} == 1000000 &&
                  $sorted[-1]->{family_no} == 1000000){
                push(@occs_to_sort, pop @sorted);
            }

            # Put occs in order, AFTER the sorted occ with the closest smaller
            # number.  First check if our occ number is one greater than any 
            # existing sorted occ number.  If so, place after it.  If not, find
            # the distance between it and all other occs less than it and then
            # place it after the one with the smallest distance.
            while(my $single = pop @occs_to_sort){
                my $slot_found = 0;
                my @variances = ();
                # First, look for the "easy out" at the endpoints.
                # Beginning?
            # HMM, if $single is less than $sorted[0] we don't want to put
            # it at the front unless it's less than ALL $sorted[$x].
                #if($single->{occurrence_no} < $sorted[0]->{occurrence_no} && 
                #	$sorted[0]->{occurrence_no} - $single->{occurrence_no} == 1){
                #	unshift @sorted, $single;
                #}
                # Can I just stick it at the end?
                if($single->{occurrence_no} > $sorted[-1]->{occurrence_no} &&
                    $single->{occurrence_no} - $sorted[-1]->{occurrence_no} == 1){
                    push @sorted, $single;
                }
                # Somewhere in the middle
                else{
                    for(my $index = 0; $index < @sorted-1; $index++){
                        if($single->{occurrence_no} > 
                                        $sorted[$index]->{occurrence_no}){ 
                            # if we find a variance of 1, bingo!
                            if($single->{occurrence_no} -
                                    $sorted[$index]->{occurrence_no} == 1){
                                splice @sorted, $index+1, 0, $single;
                                $slot_found=1;
                                last;
                            }
                            else{
                                # store the (positive) variance
                                push(@variances, $single->{occurrence_no}-$sorted[$index]->{occurrence_no});
                            }
                        }
                        else{ # negative variance
                            push(@variances, 1000000);
                        }
                    }
                    # if we didn't find a variance of 1, place after smallest
                    # variance.
                    if(!$slot_found){
                        # end variance:
                        if($sorted[-1]->{occurrence_no}-$single->{occurrence_no}>0){
                            push(@variances,$sorted[-1]->{occurrence_no}-$single->{occurrence_no});
                        }
                        else{ # negative variance
                            push(@variances, 1000000);
                        }
                        # insert where the variance is the least
                        my $smallest = 1000000;
                        my $smallest_index = 0;
                        for(my $counter=0; $counter<@variances; $counter++){
                            if($variances[$counter] < $smallest){
                                $smallest = $variances[$counter];
                                $smallest_index = $counter;
                            }
                        }
                        # NOTE: besides inserting according to the position
                        # found above, this will insert an occ less than all other
                        # occ numbers at the very front of the list (the condition
                        # in the loop above will never be met, so $smallest_index
                        # will remain zero.
                        splice @sorted, $smallest_index+1, 0, $single;
                    }
                }
            }
        }

		my $sorted_html = '';
		for(my $index = 0; $index < @sorted; $index++){
			# Color the background of alternating rows gray JA 10.6.02
			if($index % 2 == 0 && @sorted > 2){
				#$sorted[$index]->{html} =~ s/<td/<td class='darkList'/g;
				$sorted[$index]->{html} =~ s/<tr/<tr class='darkList'/g;
			}
			$sorted_html .= $sorted[$index]->{html};
		}
		$return .= $sorted_html;

		$return .= "</table>
					</div>";

	}
	return $return;
} # end sub buildTaxonomicList()



# note: rjp 1/2004 - I *think* this gets an HTML formatted table
# of reidentifications for a particular taxon
# to be used in the taxon list of the collections page.
# JA 2.4.04: yes, stupid, of course that's what it does
#
# pass it an occurrence number or reid number 
# the second parameter tells whether it's a reid_no (true) or occurrence_no (false).
sub getReidHTMLTableByOccNum {
	my $occNum = shift;
	my $isReidNo = shift;

	$sql =	"SELECT genus_reso, ".
			" genus_name, ".
			" subgenus_reso, ".
			" subgenus_name, ".
			" species_reso, ".
			" species_name, ".
			" reidentifications.comments as comments, ".
			" reidentifications.reference_no as reference_no, ".
			" pubyr, ".
			" taxon_no ".
			" FROM reidentifications,refs ";
	if ($isReidNo) {
		$sql .= " WHERE reid_no = $occNum";
	} else {
		$sql .= " WHERE occurrence_no = $occNum";
	}
	$sql .= " AND refs.reference_no=reidentifications.reference_no";

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
  
	my @fieldNames = @{$sth->{NAME}};
	my @rows = @{$sth->fetchall_arrayref()};

	# JA 2.4.04: if there are multiple results, sort them by the
	#  reference's publication year (column 8)
	# syntax here is ugly because we are sorting on elements of a
	#  referenced array
	@rows = sort { ${$a}[8] <=> ${$b}[8] } @rows;

	my $retVal = "";
	
	# NOT SURE ABOUT THIS LOOP. DON'T WE JUST WANT ONE (THE MOST RECENT)?
	# note, rjp, 12/23/2004 - No, we want all of them.
	# JA 2.4.04: whether we get all of them depends on whether isReidNo
	#  is true (no, just one) or false (yes, all of them)
	foreach my $rowRef ( @rows ) {
		my @row = @{$rowRef};

		# get the taxonomic authority JA 19.4.04
		# the taxon no is in the second to last field
		my $authority = "";
		if ( $row[-1] )	{
		# If the authority is a PBDB ref, retrieve and print it
		# JA 23.4.04: don't do this is the species name (column 5) looks
		#  like a species but the taxon's rank is not species
			my $sql = "SELECT * FROM authorities WHERE taxon_no=" . $row[-1];
			my %taxData = %{@{$dbt->getData($sql)}[0]};
			if ( $row[5] =~ /(indet\.)|(sp\.)/ || $taxData{taxon_rank} eq "species" )	{
				if ( $taxData{'ref_is_authority'} )	{
					$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=";
					$sql .= $taxData{'reference_no'};
					my %refRow = %{@{$dbt->getData($sql)}[0]};
					$authority = Reference::formatShortRef(\%refRow );
				}
			# Otherwise, use what authority info can be found
				else	{
					$authority = Reference::formatShortRef(\%taxData );
				}
			}
		}

		# format the reference (PM)
		# JA: -3 means fourth to last element in the array
		$row[-3] = buildReference($row[-3],"list");

        my $class_hash = Classification::get_classification_hash($dbt,'class,order,family',[$row[9]],'array');
        # Index 0 is class, 1 is order, 2 is family. add -1 to split to keep empty trailing fields
        my @class_array = @{$class_hash->{$row[9]}};
        my %classification = ();
        foreach my $taxon (@class_array) {
            $classification{$taxon->{'taxon_rank'}} = $taxon;
        }
        # If its a higher order name and indet, like "Omomyidae indet." get the higher order name as well
        if ($row[9]) {
            my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$row[9]);
            if ($taxon->{'taxon_rank'} =~ /^(?:family|order|class)$/) {
                $classification{$taxon->{'taxon_rank'}} = $taxon;
            }
        }
        
        %{$_[0]} = %classification;

		# JA 2.4.04: changed this so it only works on the most
		#  recently published reID
		if ( $rowRef == $rows[$#rows] )	{
		if(%classification) {
			push(@row, "bogus");
			push(@fieldNames, 'higher_taxa');
			push(@row, $classification{'class'}{'taxon_name'});
			push(@fieldNames, 'class');
			push(@row, $classification{'order'}{'taxon_name'});
			push(@fieldNames, 'order');
			push(@row, $classification{'family'}{'taxon_name'});
			push(@fieldNames, 'family');
		}
		}

		if ( $authority )	{
			push @row , $authority;
			push @fieldNames , "authority";
		}

		$retVal .= $hbo->populateHTML("reid_taxa_display_row", \@row,\@fieldNames);

		if ( $rowRef == $rows[$#rows] )	{
            if(%classification) {
                pop(@row);
                pop(@row);
                pop(@row);
                pop(@row);
                pop(@fieldNames);
                pop(@fieldNames);
                pop(@fieldNames);
                pop(@fieldNames);
            }
		}
		if ( $authority )	{
			pop(@row);
			pop(@fieldNames);
		}
	}

	return $retVal;
}


# JA 21.2.03
sub rarefyAbundances	{

	print stdIncludes("std_page_top");

	$sql = "SELECT abund_value FROM occurrences WHERE collection_no=";
	$sql .= $q->param(collection_no);
	$sql .= " AND abund_value>0";
	
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @ids;
	my $abundsum;
	my $abundmax;
    my $ntaxa;
    my @abund;
	while ( my @abundrow = @{$sth->fetchrow_arrayref()} )	{
		push @abund , $abundrow[0];
		$abundsum = $abundsum + $abundrow[0];
		if ( $abundrow[0] > $abundmax )	{
			$abundmax = $abundrow[0];
		}
		$ntaxa++;
		for my $i (1..$abundrow[0])	{
			push @ids , $ntaxa;
		}
	}
	$sth->finish();

	# compute Berger-Parker, Shannon-Wiener, and Simpson indices
	my $bpd = $abundmax / $abundsum;
	my $swh;
	my $simpson;
	for my $a ( @abund )	{
		my $p = $a / $abundsum;
		$swh = $swh + ( $p * log($p) );
		$simpson = $simpson + $p**2;
	}
	$swh = $swh * -1;
	$simpson = 1 - $simpson;
	# Lande 1996 correction
	if ( $ntaxa > 1 )	{
		$simpson = $simpson * $ntaxa / ( $ntaxa - 1 );
	} else	{
		$simpson = 0;
	}
	# compute Fisher's alpha using May 1975 eqns. 3.12 and F.13
	my $alpha = 100;
	my $lastalpha;
	while ( abs($alpha - $lastalpha) > 0.001 )	{
		$lastalpha = $alpha;
		$alpha = $ntaxa / log(1 + ($abundsum / $alpha));
	}
	# compute PIelou's J index
	my $pj = $swh / log($ntaxa);
	# compute Buzas-Gibson index
	my $bge = exp($swh) / $ntaxa;

	# abundances have to be sorted and transformed to frequencies
	#  in order to test the distribution against the log series JA 14.5.04
	@abund = sort { $b <=> $a } @abund;
	my @freq;
	for my $i (0..$ntaxa-1)	{
		$freq[$i] = $abund[$i] / $abundsum;
	}

	# now we need to get freq i out of alpha and gamma (Euler's constant)
	# start with May 1975 eqn. F.10
	#  i = -a log(a * freq i) - gamma, so
	#  (i + gamma)/-a = log(a * freq i), so
	#  exp((i +gamma)/-a) / a = freq i
	my $gamma = 0.577215664901532860606512090082;

	# note that we only get the right estimates if we start i at 0
	my $estfreq;
	my $sumestfreq;
	my $sumfreq;
	my $logseriesksd;
	for my $i (0..$ntaxa-1)	{
		my $estfreq = ($i + $gamma) / (-1 * $alpha);
		$estfreq = exp($estfreq) / $alpha;
		$sumestfreq = $sumestfreq + $estfreq;
		$sumfreq = $sumfreq + $freq[$i];
		my $freqdiff = abs($sumfreq - $sumestfreq);
		if ( $freqdiff > $logseriesksd )	{
			$logseriesksd = $freqdiff;
		}
	}

	print "<center><h3>Diversity statistics for ", $q->param(collection_name), " (PBDB collection ", $q->param(collection_no), ")</h3></center>\n\n";

	print "<center><table><tr><td align=\"left\">\n";
	printf "<p>Total richness: <b>%d taxa</b><br>\n",$ntaxa;
	printf "Shannon-Wiener <i>H</i>: <b>%.3f</b><br>\n",$swh;
	printf "Simpson's <i>D</i>*: <b>%.3f</b><br>\n",$simpson;
	printf "Berger-Parker <i>d</i>: <b>%.3f</b><br>\n",$bpd;
	printf "Fisher's <i>alpha</i>**: <b>%.2f</b><br>\n",$alpha;
	printf "Kolmogorov-Smirnov <i>D</i>, data vs. log series***: <b>%.3f</b>",$logseriesksd;
	if ( $logseriesksd > 1.031 / $ntaxa**0.5 )	{
		print " (<i>p</i> < 0.01)<br>\n";
	} elsif ( $logseriesksd > 0.886 / $ntaxa**0.5 )	{
		print " (<i>p</i> < 0.05)<br>\n";
	} else	{
		print " (not significant)<br>\n";
	}
	printf "Pielou's <i>J</i> (evenness): <b>%.3f</b><br>\n",$pj;
	printf "Buzas-Gibson <i>E</i> (evenness): <b>%.3f</b></p>\n",$bge;
	print "<div class=small><p>* = with Lande 1996 correction<br>\n** = solved recursively based on richness and total abundance<br>\n*** = test of whether the distribution differs from a log series</p></div></center>\n";
	print "</td></tr></table>\n";

	# rarefy the abundances
	my $maxtrials = 200;
    my @sampledTaxa;
	for my $trial (1..$maxtrials)	{
		my @tempids = @ids;
		my @seen = ();
		my $running = 0;
		for my $n (0..$#ids)	{
			my $x = int(rand() * ($#tempids + 1));
			my $id = splice @tempids, $x, 1;
			$sampledTaxa[$n] = $sampledTaxa[$n] + $running;
			if ( $seen[$id] < $trial )	{
				$sampledTaxa[$n]++;
				$running++;
			}
			push @{$richnesses[$n]} , $running;
			$seen[$id] = $trial;
		}
	}

	my @slevels = (1,2,3,4,5,7,10,15,20,25,30,35,40,45,50,
	      55,60,65,70,75,80,85,90,95,100,
	      150,200,250,300,350,400,450,500,550,600,650,
	      700,750,800,850,900,950,1000,
	      1500,2000,2500,3000,3500,4000,4500,5000,5500,
	      6000,6500,7000,7500,8000,8500,9000,9500,10000);
	for my $sl (@slevels)	{
		$isalevel[$sl] = "Y";
	}

	print "<hr><center><h3>Rarefaction curve for ", $q->param(collection_name), " (PBDB collection ", $q->param(collection_no), ")</h3></center>\n\n";

	open OUT,">$HTML_DIR/$OUTPUT_DIR/rarefaction.csv";
	print "<center><table>\n";
	print "<tr><td><u>Specimens</u></td><td><u>Species (mean)</u></td><td><u>Species (median)</u></td><td><u>95% confidence limits</u></td></tr>\n";
	print OUT "Specimens\tSpecies (mean)\tSpecies (median)\tLower CI\tUpper CI\n";
	for my $n (0..$#ids)	{
		if ( $n == $#ids || $isalevel[$n+1] eq "Y" )	{
			my @distrib = sort { $a <=> $b } @{$richnesses[$n]};
			printf "<tr><td align=center>%d</td> <td align=center>%.1f</td> <td align=center>%d</td> <td align=center>%d - %d</td></tr>\n",$n + 1,$sampledTaxa[$n] / $maxtrials,$distrib[99],$distrib[4],$distrib[195];
			printf OUT "%d\t%.1f\t%d\t%d\t%d\n",$n + 1,$sampledTaxa[$n] / $maxtrials,$distrib[99],$distrib[4],$distrib[195];
		}
	}
	close OUT;
	print "</table></center>\n<p>\n\n";
	print "<p><i>Results are based on 200 random sampling trials.\n";
	print "The data can be downloaded from a <a href=\"$HOST_URL/$OUTPUT_DIR/rarefaction.csv\">tab-delimited text file</a>.</i></p></center>\n\n";

	print stdIncludes("std_page_bottom");

}

# JA 20,21,28.9.04
# shows counts of taxa within ecological categories for an individual
#  collection
# WARNING: assumes you only care about life habit and diet
# Download.pm uses some similar calculations but I see no easy way to
#  use a common function
sub displayCollectionEcology	{
	print stdIncludes("std_page_top");

    my @ranks = @{$hbo->{'SELECT_LISTS'}{'taxon_rank'}};
    my %rankToKey = ();
    foreach my $rank (@ranks) {
        my $rank_abbrev = $rank;
        $rank_abbrev =~ s/species/s/;
        $rank_abbrev =~ s/genus/g/;
        $rank_abbrev =~ s/tribe/t/;
        $rank_abbrev =~ s/family/f/;
        $rank_abbrev =~ s/order/o/;
        $rank_abbrev =~ s/class/c/;
        $rank_abbrev =~ s/phylum/p/;
        $rank_abbrev =~ s/kingdom/f/;
        $rank_abbrev =~ s/unranked clade/uc/;
        $rankToKey{$rank} = $rank_abbrev;
    }

    # Get all occurrences for the collection using the most currently reid'd name
    my $collection_no = int($q->param('collection_no'));
    my $collection_name = $q->param('collection_name');

    print "<div align=center><h3>$collection_name (PBDB collection number $collection_no)</h3></div>";

	my $sql = "(SELECT o.genus_name,o.species_name,o.taxon_no FROM occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE o.collection_no=$collection_no AND re.reid_no IS NULL)".
           " UNION ".
	       "(SELECT re.genus_name,re.species_name,o.taxon_no FROM occurrences o,reidentifications re WHERE o.occurrence_no=re.occurrence_no AND o.collection_no=$collection_no AND re.most_recent='YES')";
    
	my @occurrences = @{$dbt->getData($sql)};

    # First get a list of all the parent taxon nos
	my @taxon_nos = map {$_->{'taxon_no'}} @occurrences;
	my $parents = Classification::get_classification_hash($dbt,'all',\@taxon_nos,'array');
    # We only look at these categories for now
	my @categories = ("life_habit", "diet1", "diet2","minimum_body_mass","maximum_body_mass","body_mass_estimate");
    my $ecology = Ecology::getEcology($dbt,$parents,\@categories,'get_basis');

	if (!%$ecology) {
		print "<center><h3>Sorry, there are no ecological data for any of the taxa</h3></center>\n\n";
		print "<center><p><b><a href=\"$exec_url?action=displayCollectionDetails&collection_no=" . $q->param('collection_no') . "\">Return to the collection record</a></b></p></center>\n\n";
		print stdIncludes("std_page_bottom");
		return;
	} 

    # Convert units for display
    foreach my $taxon_no (keys %$ecology) {
        foreach ('minimum_body_mass','maximum_body_mass','body_mass_estimate') {
            if ($ecology->{$taxon_no}{$_}) {
                if ($ecology->{$taxon_no}{$_} < 1) {
                    $ecology->{$taxon_no}{$_} = Ecology::kgToGrams($ecology->{$taxon_no}{$_});
                    $ecology->{$taxon_no}{$_} .= ' g';
                } else {
                    $ecology->{$taxon_no}{$_} .= ' kg';
                }
            }
        } 
    }
   
	# count up species in each category and combined categories
    my (%cellsum,%colsum,%rowsum);
	for my $row (@occurrences)	{
        my ($col_key,$row_key);
		if ( $ecology->{$row->{'taxon_no'}}{'life_habit'}) {
            $col_key = $ecology->{$row->{'taxon_no'}}{'life_habit'};
        } else {
            $col_key = "?";
        }
        
		if ( $ecology->{$row->{'taxon_no'}}{'diet2'})	{
            $row_key = $ecology->{$row->{'taxon_no'}}{'diet1'}.'/'.$ecology->{$row->{'taxon_no'}}{'diet2'};
		} elsif ( $ecology->{$row->{'taxon_no'}}{'diet1'})	{
            $row_key = $ecology->{$row->{'taxon_no'}}{'diet1'};
        } else {
            $row_key = "?";
        }

        $cellsum{$col_key}{$row_key}++;
		$colsum{$col_key}++;
        $rowsum{$row_key}++;
	}

	print "<div align=\"center\"><h3>Assignments of taxa to categories</h3>";
	print "<table cellspacing=0 border=0 cellpadding=4 class=dataTable>";

    # Header generation
	print "<tr><th class=dataTableColumnLeft>Taxon</th>";
	print "<th class=dataTableColumn>Diet</th>";
	print "<th class=dataTableColumn>Life habit</th>";
	print "<th class=dataTableColumn>Body mass</th>";
	print "</tr>\n";

    # Table body
    my %all_rank_keys = ();
	for my $row (@occurrences) {
		print "<tr>";
        if (($row->{'taxon_rank'} && $row->{'taxon_rank'} !~ /species/) ||
            ($row->{'species_name'} =~ /indet/)) {
            print "<td class=dataTableCellLeft>$row->{genus_name} $row->{species_name}</td>";
        } else {
            print "<td class=dataTableCellLeft><i>$row->{genus_name} $row->{species_name}</i></td>";
        }

        # Basis is the rank of the taxon where this data came from. i.e. family/class/etc.
        # See Ecology::getEcology for further explanation
        my ($value,$basis);

        # Handle diet first
        if ($ecology->{$row->{'taxon_no'}}{'diet2'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'diet1'}."/".$ecology->{$row->{'taxon_no'}}{'diet2'};
            $basis = $ecology->{$row->{'taxon_no'}}{'diet1'.'basis'}
        } elsif ($ecology->{$row->{'taxon_no'}}{'diet1'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'diet1'};
            $basis = $ecology->{$row->{'taxon_no'}}{'diet1'.'basis'}
        } else {
            ($value,$basis) = ("?","");
        }
        $all_rank_keys{$basis} = 1;
        print "<td class=dataTableCell>$value<span class='superscript'>$rankToKey{$basis}</span></td>";

        # Then life habit
        if ($ecology->{$row->{'taxon_no'}}{'life_habit'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'life_habit'};
            $basis = $ecology->{$row->{'taxon_no'}}{'life_habit'.'basis'}
        } else {
            ($value,$basis) = ("?","");
        }
        $all_rank_keys{$basis} = 1;
        print "<td class=dataTableCell>$value<span class='superscript'>$rankToKey{$basis}</span></td>";

        # Now body mass
        if ($ecology->{$row->{'taxon_no'}}{'body_mass_estimate'}) {
            $value1 = $ecology->{$row->{'taxon_no'}}{'body_mass_estimate'};
            $basis1 = $ecology->{$row->{'taxon_no'}}{'body_mass_estimate'.'basis'};
            $value2 = "";
            $basis2 = "";
        } elsif ($ecology->{$row->{'taxon_no'}}{'minimum_body_mass'}) {
            $value1 = $ecology->{$row->{'taxon_no'}}{'minimum_body_mass'};
            $basis1 = $ecology->{$row->{'taxon_no'}}{'minimum_body_mass'.'basis'};
            $value2 = $ecology->{$row->{'taxon_no'}}{'maximum_body_mass'};
            $basis2 = $ecology->{$row->{'taxon_no'}}{'maximum_body_mass'.'basis'};
        } else {
            ($value1,$basis1,$value2,$basis2) = ("?","","","");
        }
        $all_rank_keys{$basis1} = 1;
        $all_rank_keys{$basis2} = 1; 
        print "<td class=dataTableCell>$value1<span class='superscript'>$rankToKey{$basis1}</span>";
        print " - $value2<span class='superscript'>$rankToKey{$basis2}</span>" if ($value2);
        print "</td>";

		print "</tr>\n";
	}
    # now print out keys for superscripts above
    print "<tr><td colspan=4>";
    my $html = "Source: ";
    foreach my $rank (@ranks) {
        if ($all_rank_keys{$rank}) {
            $html .= "$rankToKey{$rank} = $rank, ";
        }
    }
    $html =~ s/, $//;
    print $html;
    print "</td></tr>";
	print "</table>";
    print "</div>";

    # Summary information
	print "<p>";
	print "<div align=\"center\"><h3>Counts within categories</h3>";
	print "<table border=0 cellspacing=0 cellpadding=4 class=dataTable>";
    print "<tr><td class=dataTableTopULCorner>&nbsp;</td><th class=dataTableTop colspan=".scalar(keys %colsum).">Life Habit</th></tr>";
    print "<tr><th class=dataTableULCorner>Diet</th>";
	for my $habit (sort keys %colsum) {
        print "<td class=dataTableRow align=center>$habit</td>";
	}
	print "<td class=dataTableRow><b>Total<b></tr>";

	for my $diet (sort keys %rowsum) {
		print "<tr>";
		print "<td class=dataTableRow>$diet</td>";
		for my $habit ( sort keys %colsum ) {
			print "<td class=dataTableCell align=right>";
			if ( $cellsum{$habit}{$diet} ) {
				printf("%d",$cellsum{$habit}{$diet});
			} else {
                print "&nbsp;";
            }
			print "</td>";
		}
        print "<td class=dataTableCell align=right><b>$rowsum{$diet}</b></td>";
		print "</tr>\n";
	}
	print "<tr><td class=dataTableColumn><b>Total</b></td>";
	for my $habit (sort keys %colsum) {
		print "<td class=dataTableCell align=right>";
		if ($colsum{$habit}) {
			print "<b>$colsum{$habit}</b>";
		} else {
            print "&nbsp;";
        }
		print "</td>";
	}
	print "<td class=dataTableCell align=right><b>".scalar(@occurrences)."</b></td></tr>\n";
	print "</table>\n";
    print "</div>";

	print "<div align=\"center\"><p><b><a href=\"$exec_url?action=displayCollectionDetails&collection_no=".$q->param('collection_no')."\">Return to the collection record</a></b> - ";
	print "<b><a href=\"$exec_url?action=displaySearchColls&type=view\">Search for other collections</a></b></p></div>\n\n";
	print stdIncludes("std_page_bottom");

}


sub displayEnterCollPage {

	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=displayEnterCollPage" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	# Need to build the research_group checkboxes

    my (@htmlFields,@htmlValues,%pref);
    if ($q->param('prefill_collection_no') =~ /^\d+$/) {
        dbg("Prefilling form with ".$q->param('prefill_collection_no'));
        $sql = "SELECT * FROM collections WHERE collection_no=" . $q->param('prefill_collection_no');
        my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
        $sth->execute();
        my @fieldNames = @{$sth->{NAME}};
        my $row = $sth->fetchrow_hashref();
        if ($row) {
            foreach my $field (@fieldNames) {
                if ($field !~ /^(authorizer|enterer|modifier|authorizer_no|enterer_no|modifier_no|created|modified|collection_no)/) {
                    push @htmlFields,$field;
                    push @htmlValues,$row->{$field};
                }
            }
            # get the max/min interval names
            my ($v,$f) = getMaxMinNamesAndDashes(\@htmlValues,\@htmlFields);
            @htmlValues = @{$v};
            @htmlFields = @{$f};

            $reference_no = $row->{'reference_no'};
            # Get the reference data
            $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$reference_no;

            $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
            $sth->execute();

            my $md = MetadataModel->new($sth);
            $refRowRef = $sth->fetchrow_arrayref();
            my $drow = DataRow->new($refRowRef, $md);
            my $bibRef = BiblioRef->new($drow);
            $sth->finish();

            $refRowString = "<table>" . $bibRef->toString() . '</table>';

            unshift(@htmlFields, 'ref_string');
            unshift(@htmlValues, $refRowString);

	        %pref = getPreferences($s->get('enterer_no'));
                  
            $sth->finish();
        } else { 
            die "Tried to prefill entry form with non-existant collection no: ".$q->param('prefill_collection_no');
        }
    } else {
        # Have to have a reference #
        my $reference_no = $s->get("reference_no");
        if ( ! $reference_no ) {
            $s->enqueue( $dbh, "action=displayEnterCollPage" );
            displaySearchRefs( "Please choose a reference first" );
            exit;
        }	

        # Get the reference data
        $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$reference_no;
        $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
        $sth->execute();

        my $md = MetadataModel->new($sth);
        $refRowRef = $sth->fetchrow_arrayref();
        my $drow = DataRow->new($refRowRef, $md);
        my $bibRef = BiblioRef->new($drow);
        $sth->finish();

        $refRowString = "<table>" . $bibRef->toString() . '</table>';

        # Get the field names
        $sql = "SELECT * FROM collections WHERE collection_no=0";
        dbg( "$sql" );
        my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
        $sth->execute();
        my @fieldNames = @{$sth->{NAME}};
        $sth->finish();
        # Tack a few extra fields
        push @htmlValues, '' for @fieldNames;
        @htmlFields = @fieldNames;
        unshift(@htmlFields,    'authorizer',
            'enterer',
            'reference_no',
            'ref_string',
            'country',
            'eml_max_interval',
            'max_interval',
            'eml_min_interval',
            'min_interval' );
        unshift(@htmlValues,   $s->get('authorizer'),
            $s->get('enterer'),
            $reference_no,
            $refRowString,
            '',
            '',
            '',
            '',
            '' );

        %pref = getPreferences($s->get('enterer_no'));
        # Get the enterer's preferences JA 25.6.02
        my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = getPrefFields();
        for my $p (@{$setFieldNames})	{
            if ($pref{$p} ne "")	{
                unshift @htmlValues,$pref{$p};
                unshift @htmlFields,$p;
            }
        }

        # carry over the lat/long coordinates the user entered while doing
        #  the mandatory collection search JA 6.4.04
        my @coordfields = ("latdeg","latmin","latsec","latdec","latdir",
                "lngdeg","lngmin","lngsec","lngdec","lngdir");
        for my $cf (@coordfields)	{
            if ( $q->param($cf) )	{
                unshift @htmlValues,$q->param($cf);
                unshift @htmlFields,$cf;
            }
        }
    }

    # Remove these from being displayed
    unshift(@htmlFields,'secondary_reference_string','session_reference_string');
    unshift(@htmlValues,'','');
   
# this is hack to disable the usual 100% height table that wraps around
#  the entire page JA 1.9.05
	my $std_page_top = stdIncludes("std_page_top");
	$std_page_top =~ s/ height="100%"//;
	print $std_page_top;
	print "</td></tr></table>\n\n";
	print "</td></tr></table>\n\n";

	print printIntervalsJava();

    push(@htmlValues, '<input type="hidden" name="action" value="processEnterCollectionForm">');
    push(@htmlFields, 'page_target');

    push(@htmlValues, "Collection entry form");
    push(@htmlFields, 'page_title');

    push(@htmlValues, '<input type=submit name="enter_button" value="Enter collection and exit">');
    push(@htmlFields, 'page_submit_button');

    push(@htmlValues, stdIncludes("std_page_bottom"));
    push(@htmlFields, 'page_footer');

    # Output the main part of the page
    my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML("collection_form", \@htmlValues, \@htmlFields, \@prefkeys);

    print $html;

}

# print Javascript to limit entry of time interval names
# WARNING: if "Early/Late Interval" is submitted but only "Interval"
#  is present in the intervals table, the submission will be rejected
# the CheckIntervalNames is used for form validation, the intervalNames is used
# for autocompletion.  They're slightly different in that checkIntervalNames is interested in
# fully qualified names (i.e. early X) while we don't care about the early/middle/late for the intervalNames
sub printIntervalsJava  {
    my $include_ten_my_bins = shift;
    my $sql = "SELECT eml_interval,interval_name FROM intervals";
    my @results = @{$dbt->getData($sql)};
    
    my %intervals_seen;
    foreach $row (@results)  {
        if (!$intervals_seen{$row->{'interval_name'}}) {
            $intervals .= "'$row->{interval_name}', ";
            $intervals_seen{$row->{'interval_name'}} = 1;
        }
    }
    $intervals =~ s/, $//;
                                                                                                                                                             
print <<EOF;
<script language="JavaScript" type="text/javascript">
<!-- Begin
function intervalNames() {
    var intervals = new Array($intervals);
    return intervals;
}

function checkIntervalNames(require_field) {
    var frm = document.forms[0];
    var badname1 = "";
    var badname2 = "";
    var alertmessage = "";
    var eml1 = frm.eml_max_interval.options[frm.eml_max_interval.selectedIndex].value;
    var time1 = frm.max_interval.value;
    var eml2 = frm.eml_min_interval.options[frm.eml_min_interval.selectedIndex].value;
    var time2 = frm.min_interval.value;
    var emltime1 = eml1 + time1;
    var emltime2 = eml2 + time2;
    
    var isInt = /^[0-9.]+\$/;
    if ( time1 == "" || isInt.test(time1))   {
        if (require_field) {
            var noname ="WARNING!\\n" +
                    "The maximum interval field is required.\\n" +
                    "Please fill it in and submit the form again.\\n" +
                    "Hint: epoch names are better than nothing.\\n";
            alert(noname);
            return false;
        } else {
            return true;
        }
    } 
EOF
    for my $i (1..2) {
        my $check = "    if(";
        for my $row ( @results) {
            # this is kind of ugly: we're just not going to let users
            #  enter a time term that has double quotes because that
            #  would break the JavaScript
            if ( $row->{'interval_name'} !~ /"/ )   {
                $check .= qq| emltime$i != "| . $row->{'eml_interval'} . $row->{'interval_name'} . qq|" &&\n|;
            }
        }
        if ($include_ten_my_bins) {
            my @binnames = TimeLookup::getTenMYBins();
            foreach my $binname (@binnames) {
                $check .= qq| emltime$i != "|.$binname. qq|" &&\n|;
            }
        }
        if ($i == 1) {
            chop($check); chop($check); chop($check);#remove trailing &&\n
        } else {
            $check .= qq|time$i != ""|;
        }
        $check .= ") {\n";
        $check .= "        badname$i += \"YES\";\n";
        $check .= "    }\n";
        print $check;
    }
print <<EOF;
                                                                                                                                                             
    if ( badname1 != "" || badname2 != "" ) {
        alertmessage = "WARNING!\\n";
    }
                                                                                                                                                             
    if ( badname1 != "" && badname2 != "" ) {
        alertmessage += eml1 + " " + time1 +
                        " and " + eml2 + " " + time2 +
                        " aren't official time terms.\\n";
        alertmessage += "Please correct them and submit the form again.\\n";
    } else if ( badname1 != "" ) {
        alertmessage += eml1 + " " + time1;
        alertmessage += " isn't an official time term.\\n" +
                        "Please correct it and submit the form again.\\n";
    } else if ( badname2 != "" ) {
        alertmessage += eml2 + " " + time2;
        alertmessage += " isn't an official time term.\\n" +
                        "Please correct it and submit the form again.\\n";
    }
    if ( alertmessage != "" ) {
        alertmessage += "Hint: try epoch names instead.";
        alert(alertmessage);
        return false;
    }
    return true;
}
// END -->
</script>
EOF
    return;
}


# Set the release date
# originally written by Ederer; made a separate function by JA 26.6.02
sub setReleaseDate	{

	my $releaseDate;
	my $releaseDateString = $q->param('release_date');
	{
	#local $Class::Date::DATE_FORMAT="%Y%m%d%H%M%S";
		$releaseDate = now;
		if ( $q->param('created') )	{
			$releaseDate = date( $q->param('created') );
		}
	}
	if ( $releaseDateString eq 'immediate')	{
		#$releaseDate = $releaseDate;
	}
	elsif ( $releaseDateString eq 'three months')	{
		$releaseDate = $releaseDate+'3M';
	}
	elsif ( $releaseDateString eq 'six months')	{
		$releaseDate = $releaseDate+'6M';
	}
	elsif ( $releaseDateString eq 'one year')	{
		$releaseDate = $releaseDate+'1Y';
	}
	elsif ( $releaseDateString eq 'two years') {
		$releaseDate = $releaseDate+'2Y';
	}
	elsif ( $releaseDateString eq 'three years')	{
		$releaseDate = $releaseDate+'3Y';
	}
	elsif ( $releaseDateString eq 'four years')	{
			$releaseDate = $releaseDate+'4Y';
	}
	elsif ( $releaseDateString eq 'five years')	{
		$releaseDate = $releaseDate+'5Y';
	}
	#print "<pre>Release Date: " . $releaseDate . "</pre><br>";

	$q->param(release_date=>"$releaseDate");
}

#  * User submits completed collection entry form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processEnterCollectionForm {
    print stdIncludes( "std_page_top" );

	unless($q->param('max_interval'))	{
		print "<center><h3>The time interval field is required!</h3>\n<p>Please go back and specify the time interval for this collection</p></center>";
		print stdIncludes("std_page_bottom");
		print "<br><br>";
		return;
	}

	# figure out the release date, enterer, and authorizer
	setReleaseDate();

	# change interval names into numbers by querying the intervals table
	# JA 11-12.7.03
	if ( $q->param('max_interval') )	{
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('max_interval') . "'";
		if ( $q->param('eml_max_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_max_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(max_interval_no => $no);
	}
	if ( $q->param('min_interval') )	{
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('min_interval') . "'";
		if ( $q->param('eml_min_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_min_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
	my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(min_interval_no => $no);
	}
	# bomb out if no such interval exists JA 28.7.03
	if ( $q->param('max_interval_no') < 1 )	{
		print "<center><h3>You can't enter an unknown time interval name</h3>\n<p>Please go back, check the time scales, and enter a valid name</p></center>";
		print stdIncludes("std_page_bottom");
		return;
	}

                                                                                                                                          
    #set paleolat, paleolng if we can PS 11/07/2004
    dbg("paleocoords part");
    my ($paleolat, $paleolng);
    if ($q->param('lngdeg') >= 0 && $q->param('lngdeg') =~ /\d+/ &&
        $q->param('latdeg') >= 0 && $q->param('latdeg') =~ /\d+/)
    {
        my ($f_latdeg, $f_lngdeg);
        if ($q->param('lngmin') =~ /\d+/ && $q->param('lngmin') >= 0 && $q->param('lngmin') < 60)  {
            $f_lngdeg = $q->param('lngdeg') + ($q->param('lngmin')/60) + ($q->param('lngsec')/3600);
        } else {
            $f_lngdeg = $q->param('lngdeg') . "." .  int($q->param('lngdec'));
        }
        if ($q->param('latmin') =~ /\d+/ && $q->param('latmin') >= 0 && $q->param('latmin') < 60)  {
            $f_latdeg = $q->param('latdeg') + ($q->param('latmin')/60) + ($q->param('latsec')/3600);
        } else {
            $f_latdeg = $q->param('latdeg') . "." .  int($q->param('latdec'));
        }
        dbg("f_lngdeg $f_lngdeg f_latdeg $f_latdeg");
        if ($q->param('lngdir') =~ /West/)  {
                $f_lngdeg = $f_lngdeg * -1;
        }
        if ($q->param('latdir') =~ /South/) {
                $f_latdeg = $f_latdeg * -1;
        }

        my $max_interval_no = ($q->param('max_interval_no')) ? $q->param('max_interval_no') : 0;
        my $min_interval_no = ($q->param('min_interval_no')) ? $q->param('min_interval_no') : 0;
        ($paleolng, $paleolat) = PBDBUtil::getPaleoCoords($dbh, $dbt,$max_interval_no,$min_interval_no,$f_lngdeg,$f_latdeg);
        dbg("have paleocoords paleolat: $paleolat paleolng $paleolng");
        if ($paleolat ne "" && $paleolng ne "") {
            $q->param("paleolng"=>$paleolng);
            $q->param("paleolat"=>$paleolat);
        }
    }
   
 
	my $recID;
	my $return = insertRecord( 'collections', 'collection_no', \$recID, '99', 'period_max' );
	if ( ! $return ) { return $return; }

	print "<center><h3><font color='red'>Collection record ";
	if ( $return == $DUPLICATE ) {
		print "already ";
	}
	print "added</font></h3></center>";
 
    
	$sql = "SELECT * FROM collections WHERE collection_no=$recID";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
  	$sth->execute();
    my @fields = @{$sth->{NAME}};
	my @row = $sth->fetchrow_array();
    $sth->finish();
 
  	# Get the reference for this collection
	my $refColNum;
	my $curColNum = 0;
	foreach my $colName (@fields)
	{
		if ( $colName eq 'reference_no')
		{
			$refColNum = $curColNum;
			my $reference_no = $row[$refColNum];
			$s->setReferenceNo($dbh, $reference_no);

            $sql = "SELECT r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.publication_type FROM refs r WHERE r.reference_no=".$reference_no;
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			@refRow = $sth->fetchrow_array();
			my @refFieldNames = @{$sth->{NAME}};
			$sth->finish();
			$refRowString = $hbo->populateHTML('reference_display_row', \@refRow, \@refFieldNames);
			
			push(@row, $refRowString);
			push(@fields, 'reference_string');
			# We won't ever have a secondary ref yet, but we want HTMLBuilder
			# to nuke the headline in the template, so give it an empty value.
			push(@row, "");
			push(@fields, 'secondary_reference_string');

			last;
		}
		$curColNum++;
	}

	# get the max/min interval names
	my ($r,$f) = getMaxMinNamesAndDashes(\@row,\@fields);
	@row = @{$r};
	@fields = @{$f};
 
    print $hbo->populateHTML('collection_display_fields', \@row, \@fields);
	print '<p><div align="center"><table><tr>';
    print '<td>'.$hbo->populateHTML('collection_display_buttons', \@row, \@fields).'</td>';
	print '<td>'.$hbo->populateHTML('collection_display_buttons2', [$recID],['prefill_collection_no']).'</td>';
    print "</tr></table></div></p>";
	print '<div align="center">';
    print $hbo->populateHTML('occurrence_display_buttons', \@row, \@fields);
	print '</div>';
	print qq|<center><b><p><a href="$exec_url?action=displaySearchCollsForAdd&type=add">Add another collection with the same reference</a></p></b>|;
	#print qq| <b><p><a href="$exec_url?action=displayEnterCollPage&prefill_collection_no=$recID">Add another collection with fields prefilled based on this collection</a></p></b></center>|;
 
	print stdIncludes("std_page_bottom");
}

# This subroutine intializes the process to get to the Edit Collection page
sub startEditCollection {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection

	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=startEditCollection" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	elsif ( $q->param("collection_no") ) {
		$s->enqueue( $dbh, "action=displayEditCollection&collection_no=".$q->param("collection_no") );
	} else {
		$s->enqueue( $dbh, "action=displaySearchColls&type=edit" );
	}

	$q->param( "type" => "select" );
	displaySearchRefs( ); 
}

# This subroutine intializes the process to get to the Add Collection page
# hacked to queue displaySearchCollsForAdd JA 5.4.04
sub startAddCollection {

	# clear the queue because you never add a collection en route to
	#  doing something else JA 6.4.04
	$s->clearQueue( $dbh );

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=startAddCollection" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	$s->enqueue( $dbh, "action=displaySearchCollsForAdd&type=add" );

	$q->param( "type" => "select" );
	displaySearchRefs( ); 
}

# This subroutine intializes the process to get to the Add/Edit Occurrences page
sub startAddEditOccurrences {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=startAddEditOccurrences" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	elsif ( $q->param("collection_no") ) {
		$s->enqueue( $dbh, "action=displayOccurrenceAddEdit&collection_no=".$q->param("collection_no") );
	} else {
		$s->enqueue( $dbh, "action=displaySearchColls&type=edit_occurrence" );
	}

	$q->param( "type" => "select" );
	displaySearchRefs(); 
}

# This subroutine intializes the process to get to the reID entry page
sub startReidentifyOccurrences {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection or genus search
	
	$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );

	$q->param( "type" => "select" );
	displaySearchRefs();
}


# JA 17.8.02
#
# Generic authority handling form, used as a front end for:
#  add/edit authority, add/edit opinion, add image, add ecology data, search by ref no
#
# Edited by rjp 1/22/2004, 2/18/2004, 3/2004
# Edited by PS 01/24/2004, accept reference_no instead of taxon_name optionally
#
sub submitTaxonSearch {
    print stdIncludes("std_page_top");
    processTaxonSearch($dbh, $dbt, $hbo, $q, $s, $exec_url);
    print stdIncludes("std_page_bottom");
}
sub processTaxonSearch {
    my ($dbh, $dbt, $hbo, $q, $s, $exec_url) = @_;
	# check for proper spacing of the taxon..
	my $errors = Errors->new();
	$errors->setDisplayEndingMessage(0); 

    if ($q->param('taxon_name')) {
        if ( (Validation::looksLikeBadSubgenus($q->param('taxon_name'))) ||
             ((Validation::taxonRank($q->param('taxon_name')) eq 'invalid')) ) {
            $errors->add("Ill-formed taxon.  Check capitalization and spacing.");
        }
    } elsif (!$q->param('reference_no')) {
        $errors->add("No taxon name or reference passed.");
    }
    
	if ($errors->count()) {
		print $errors->errorMessage();
		return;
	}
	# Try to find this taxon in the authorities table
    my (@results);
    if ($q->param('taxon_name')) {
        @results = TaxonInfo::getTaxon($dbt,'taxon_name'=>$q->param('taxon_name'),'get_reference'=>1);
    } elsif ($q->param('reference_no')) {
        @results = TaxonInfo::getTaxon($dbt,'reference_no'=>$q->param('reference_no'),'get_reference'=>1);
    }
        
    # If there were no matches, present the new taxon entry form immediately
    # We're adding a new taxon
    if (scalar(@results) == 0) {
        if ($q->param('goal') eq 'authority') {
            if ($q->param('taxon_name')) {
                if ($s->get('reference_no') && $q->param('use_reference') eq 'current') {
                    $q->param('taxon_no'=> -1);
                    Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);
                } else {
                    my $toQueue = "action=displayAuthorityForm&skip_ref_check=1&taxon_no=-1&taxon_name=".$q->param('taxon_name');
                    $s->enqueue( $dbh, $toQueue );
                    $q->param( "type" => "select" );
                    main::displaySearchRefs("Please choose a reference before adding a new taxon",1);
                }
            } else {
                print "<div align=\"center\"><h3>No taxonomic names found for this reference</h3></div>";
            }
        } else {
            print "<div class=\"warning\">The taxon '" . $q->param('taxon_name') . "' doesn't exist in our database.  Please <a href=\"bridge.pl?action=submitTaxonSearch&goal=authority&taxon_name=".$q->param('taxon_name')."\">enter</a> authority record for this taxon first.</DIV>";
            return;
        }
    # One match - good enough for most of these forms
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'opinion') {
        $q->param("taxon_no"=>$results[0]->{'taxon_no'});
        Opinion::displayOpinionChoiceForm($dbt,$s,$q);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'image') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Images::displayLoadImageForm($dbt,$q,$s); 
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'ecotaph') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Ecology::populateEcologyForm($dbh, $dbt, $hbo, $q, $s, $exec_url);
    } elsif (scalar(@results) == 1 && $q->param('goal') eq 'ecovert') {
        $q->param('taxon_no'=>$results[0]->{'taxon_no'});
        Ecology::populateEcologyForm($dbh, $dbt, $hbo, $q, $s, $exec_url);
	# We have more than one matches, or we have 1 match or more and we're adding an authority.
    # Present a list so the user can either pick the taxon,
    # or create a new taxon with the same name as an exisiting taxon
	} else	{
		print "<div align=\"center\">\n";
        print "<table><tr><td align=\"center\">";
        if ($q->param("taxon_name")) { 
    		print "<h3>Which '<i>" . $q->param('taxon_name') . "</i>' do you mean?</h3>\n<br>\n";
        } else {
    		print "<h3>Select a taxon to edit:</h3>\n";
        }
        $action = ($q->param('goal') eq 'authority')  ? 'displayAuthorityForm' 
                : ($q->param('goal') eq 'opinion')    ? 'displayOpinionChoiceForm'
                : ($q->param('goal') eq 'image')      ? 'displayLoadImageForm'
                : ($q->param('goal') eq 'ecotaph')    ? 'startPopulateEcologyForm'
                : ($q->param('goal') eq 'ecovert')    ? 'startPopulateEcologyForm'
                : croak("Unknown goal given in submit taxon search");
        print "<form method=\"POST\" action=\"bridge.pl\">\n";
        print "<input type=hidden name=\"action\" value=\"$action\">\n";
        if ($q->param('goal') eq 'ecotaph' || $q->param('goal') eq 'ecovert') {
            print "<input type=hidden name=\"goal\" value=\"".$q->param('goal')."\">\n";
        }
		print "<input type=hidden name=\"taxon_name\" value=\"".$q->param('taxon_name')."\">\n";
        print "<input type=hidden name=\"use_reference\" value=\"".$q->param('use_reference')."\">\n" if ($q->param('use_reference'));

        # now create a table of choices
		print "<table>\n";
        my $checked = (scalar(@results) == 1) ? "CHECKED" : "";
        foreach my $row (@results) {
            # Check the button if this is the first match, which forces
            #  users who want to create new taxa to check another button
            print qq|<tr><td align="center"><input type="radio" name="taxon_no" value="$row->{taxon_no}" $checked></td>|;
            print "<td>".Taxon::formatAuthorityLine($dbt, $row)."</td></tr>";
        }

        # always give them an option to create a new taxon as well
        if ($q->param('goal') eq 'authority' && $q->param('taxon_name')) {
            print "<tr><td align=\"right\"><input type=\"radio\" name=\"taxon_no\" value=\"-1\"></td>\n<td>";
            if ( scalar(@results) == 1 )	{
                print "No, not the one above ";
            } else	{
                print "None of the above ";
            }
            print "- create a <b>new</b> taxon record</i></td></tr>\n";
        }
        
		print "</table>";

        # we print out difference buttons for two cases:
        #  1: using a taxon name. give them an option to add a new taxon, so button is Submit
        #  2: this is from a reference_no. No option to add a new taxon, so button is Edit
        if ($q->param('goal') eq 'authority') {
            if ($q->param('taxon_name')) {
		        print "<p><input type=submit value=\"Submit\"></p>\n</form>\n";
		        print "<p align=\"left\"><span class=\"tiny\">";
                print "You have a choice because there may be multiple biological species (e.g., a plant and an animal) with identical names.<br>\n";
		        print "Create a new taxon only if the old ones were named by different people in different papers.<br></span></p>\n";
            } else {
		        print "<p><input type=submit value=\"Edit\"></p>\n</form>\n";
            }
        } else {
            print "<p><input type=submit value=\"Submit\"></p>\n</form>\n";
            print "<p align=\"left\"><span class=\"tiny\">";
            print "You have a choice because there may be multiple biological species (e.g., a plant and an animal) with identical names.<br></span></p>\n";
        }
		print "<p align=\"left\"><span class=\"tiny\">";
        if (!$q->param('reference_no')) {
		    print "You may want to read the <a href=\"javascript:tipsPopup('/public/tips/taxonomy_tips.html')\">tip sheet</a>.</span></p>\n<br>";
        }

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
    my $page_title = 'Please enter the full name of the taxon you want to add or revise';
    my $goal= 'authority';

    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal,$q->param("use_reference")],['page_title','goal','use_reference']);
    print stdIncludes("std_page_bottom");
}

# rjp, 3/2004
#
# The form to edit an authority
sub displayAuthorityForm {
    if ($q->param('taxon_no') == -1) {
		if (!$q->param('skip_ref_check')) {
			# if skip_ref_check is not set, then we should prompt them for a reference_no 
			# set the skip_ref_check in the queued action so we don't endlessly loop 
			my $toQueue = "action=displayAuthorityForm&skip_ref_check=1&taxon_name=".$q->param('taxon_name')."&taxon_no=" . $q->param('taxon_no');
			$s->enqueue( $dbh, $toQueue );
			$q->param( "type" => "select" );
			displaySearchRefs("You must choose a reference before adding a new taxon." );
			return;
		}
	} 
    print stdIncludes("std_page_top");
	Taxon::displayAuthorityForm($dbt, $hbo, $s, $q);	
    print stdIncludes("std_page_bottom");
}


sub submitAuthorityForm {
    print stdIncludes("std_page_top");
	Taxon::submitAuthorityForm($dbt,$hbo, $s, $q);
    print stdIncludes("std_page_bottom");
}

## END Authority stuff
##############

##############
## Opinion stuff

# "Add/edit taxonomic opinion" link on the menu page. 
# Step 1 in our opinion editing process
sub displayOpinionTaxonSearchForm {
    my $page_title = 'Please enter the full name of the taxon you to add or edit an opinion for';
    my $goal = 'opinion';

    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form', [$page_title,$goal,$q->param('use_reference')], ['page_title','goal','use_reference']);
    print stdIncludes("std_page_bottom");
}

# PS 01/24/2004
# Changed from displayOpinionList to just be a stub for function in Opinion module
# Step 2 in our opinion editing process. now that we know the taxon, select an opinion
sub displayOpinionChoiceForm {
	print stdIncludes("std_page_top");
    Opinion::displayOpinionChoiceForm($dbt,$s,$q);
	print stdIncludes("std_page_bottom");
}

# rjp, 3/2004
#
# Displays a form for users to add/enter opinions about a taxon.
# It grabs the taxon_no and opinion_no from the CGI object ($q).
sub displayOpinionForm {
	if (! $q->param('opinion_no')) {
		return;	
	}

	if ($q->param('opinion_no') <= 0 && ! $q->param('skip_ref_check')) {
		# opinion_no == -1, so that means they're adding a new opinion,
		# and hence, we should prompt them for a reference_no each time.
		# Set the skip_ref_check in the queued action so we don't go into an infinite 
		# loop of asking for references.
		
		my $toQueue = 'action=displayOpinionForm&skip_ref_check=1&opinion_no=-1&child_no='.$q->param('child_no')."&child_spelling_no=".$q->param('child_spelling_no');
		$s->enqueue( $dbh, $toQueue );
	
		$q->param( "type" => "select" );
		displaySearchRefs("You must choose a reference before adding a new opinion");
		return;
	}
	
	print stdIncludes("std_page_top");
	Opinion::displayOpinionForm($dbt, $hbo, $s, $q);
	print stdIncludes("std_page_bottom");

}

sub submitOpinionForm {
	print stdIncludes("std_page_top");
	Opinion::submitOpinionForm($dbt,$hbo, $s, $q);
	print stdIncludes("std_page_bottom");
}

sub submitTypeTaxonSelect {
	print stdIncludes("std_page_top");
	Taxon::submitTypeTaxonSelect($dbt, $s, $q);
	print stdIncludes("std_page_bottom");
}

## END Opinion stuff
##############

##############
## Editing list stuff
sub displayPermissionListForm {
    Permissions::displayPermissionListForm($dbt,$q,$s,$hbo);
}

sub submitPermissionList {
    Permissions::submitPermissionList($dbt,$q,$s,$hbo);
} 

##############
## Reclassify stuff

sub startStartReclassifyOccurrences	{
	Reclassify::startReclassifyOccurrences($q, $s, $dbh, $dbt);
}

sub startDisplayOccurrenceReclassify	{
	Reclassify::displayOccurrenceReclassify($q, $s, $dbh, $dbt);
}

sub startProcessReclassifyForm	{
	Reclassify::processReclassifyForm($q, $s, $dbh, $dbt, $exec_url);
}

## END Reclassify stuff
##############

##############
## Taxon Info Stuff
sub beginTaxonInfo{
    print main::stdIncludes( "std_page_top" );
    TaxonInfo::searchForm($hbo, $q);
    print main::stdIncludes("std_page_bottom");
}

sub checkTaxonInfo{
    print main::stdIncludes( "std_page_top" );
	TaxonInfo::checkTaxonInfo($q, $dbh, $s, $dbt, $hbo);
    print main::stdIncludes("std_page_bottom");
}

sub displayTaxonInfoResults {
    print main::stdIncludes( "std_page_top" );
	TaxonInfo::displayTaxonInfoResults($dbt,$s,$q);
    print main::stdIncludes("std_page_bottom");
}

### End Module Navigation
##############

## END Taxon Info Stuff
##############

##############
## Scales stuff JA 7.7.03
sub startScale	{
	Scales::startSearchScale($dbh, $dbt, $s, $exec_url);
}
sub processShowForm	{
	Scales::processShowEditForm($dbh, $dbt, $hbo, $q, $s, $exec_url);
}
sub processViewScale	{
	Scales::processViewTimeScale($dbt, $hbo, $q, $s, $exec_url);
}
sub processEditScale	{
	Scales::processEditScaleForm($dbt, $hbo, $q, $s, $exec_url);
}
## END Scales stuff
##############


##############
## Images stuff
sub startImage{
    my $goal='image';
    my $page_title ='Please enter the full name of the taxon you want to add an image for';

    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print stdIncludes("std_page_bottom");
}

sub displayLoadImageForm{
    print stdIncludes("std_page_top");
	Images::displayLoadImageForm($dbt, $q, $s);
    print stdIncludes("std_page_bottom");
}

sub processLoadImage{
    print stdIncludes("std_page_top");
	Images::processLoadImage($dbt, $q, $s);
    print stdIncludes("std_page_bottom");
}
## END Image stuff
##############


##############
## Ecology stuff
sub startStartEcologyTaphonomySearch{
    my $goal='ecotaph';
    my $page_title ='Please enter the full name of the taxon you want to describe';

    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print stdIncludes("std_page_bottom");
}
sub startStartEcologyVertebrateSearch{
    my $goal='ecovert';
    my $page_title ='Please enter the full name of the taxon you want to describe';

    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_taxon_form',[$page_title,$goal],['page_title','goal']);
    print stdIncludes("std_page_bottom");
}
sub startPopulateEcologyForm	{
    print stdIncludes("std_page_top");
	Ecology::populateEcologyForm($dbh, $dbt, $hbo, $q, $s, $exec_url);
    print stdIncludes("std_page_bottom");
}
sub startProcessEcologyForm	{
    print stdIncludes("std_page_top");
	Ecology::processEcologyForm($dbh, $dbt, $q, $s, $exec_url);
    print stdIncludes("std_page_bottom");
}
## END Ecology stuff
##############

##############
## Specimen measurement stuff
sub displaySpecimenSearchForm {
    print stdIncludes("std_page_top");
    print $hbo->populateHTML('search_specimen_form',[],[]);
    print stdIncludes("std_page_bottom");
}

sub submitSpecimenSearch{
    print stdIncludes("std_page_top");
    Measurement::submitSpecimenSearch($dbt,$hbo,$q,$s,$exec_url);
    print stdIncludes("std_page_bottom");
}

sub displaySpecimenList {
    print stdIncludes("std_page_top");
    Measurement::displaySpecimenList($dbt,$hbo,$q,$s,$exec_url);
    print stdIncludes("std_page_bottom");
}

sub populateMeasurementForm{
    print stdIncludes("std_page_top");
    Measurement::populateMeasurementForm($dbh,$dbt,$hbo,$q,$s,$exec_url);
    print stdIncludes("std_page_bottom");
}

sub processMeasurementForm {
    print stdIncludes("std_page_top");
    Measurement::processMeasurementForm($dbh,$dbt,$hbo,$q,$s,$exec_url);
    print stdIncludes("std_page_bottom");
}

## END Specimen measurement stuff
##############



##############
## Strata stuff
sub displayStrataSearch {
    Strata::displayStrataSearch($dbh,$dbt,$hbo,$q,$s);
}
## END Strata stuff
##############

##############
## PrintHierarchy stuff
sub startStartPrintHierarchy	{
	PrintHierarchy::startPrintHierarchy($dbh, $hbo);
}
sub startProcessPrintHierarchy	{
	PrintHierarchy::processPrintHierarchy($dbh, $q, $dbt, $exec_url);
}
## END PrintHierarchy stuff
##############

sub displayEditCollection {
	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=displayEditCollection" );
		displayLoginPage( "Please log in first." );
		exit;
	}


	
	my $collection_no = $q->param('collection_no');
	$sql = "SELECT c.reference_no,c.* FROM collections c WHERE c.collection_no=" . $collection_no;
	dbg( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @fieldNames = @{$sth->{NAME}};
	my @row = $sth->fetchrow_array();
	$sth->finish();

    # Check to make sure that we have write permission
    my $authorizer_no;
    for(my $i=0;$i<scalar(@fieldNames);$i++) {
        if ($fieldNames[$i] eq 'authorizer_no') {
            $authorizer_no=$row[$i]; last;
        }
    }

    my $p = Permissions->new($s,$dbt);
    my %is_modifier_for = %{$p->getModifierList()};
    unless ($s->get("superuser") ||
            ($s->get('authorizer_no') && $s->get("authorizer_no") == $authorizer_no) ||
            $is_modifier_for{$authorizer_no}) {
        my $authorizer = Person::getPersonName($dbt,$authorizer_no);
        htmlError("You may not edit this record because it is owned by a different authorizer ($authorizer)");
        exit;
    }

	if($q->param('use_primary')){
		$s->put('reference_no', $row[0]);
	}

	my $session_ref = $s->get('reference_no');

# this is hack to disable the usual 100% height table that wraps around
#  the entire page JA 1.9.05
	my $std_page_top = stdIncludes("std_page_top");
	$std_page_top =~ s/ height="100%"//;
	print $std_page_top;
	print "</td></tr></table>\n\n";
	print "</td></tr></table>\n\n";

	# Get the reference for this collection
	my $curColNum = 0;
	my $reference_no;
	foreach my $colName (@fieldNames) {
		if ( $colName eq 'reference_no') {
			$reference_no = $row[$curColNum];
			last;
		}
		$curColNum++;
	}
	# Current primary ref
	my $refRowString = getCurrRefDisplayStringForColl($dbh, $collection_no, $reference_no);
	push(@row, $refRowString);
	push(@fieldNames, 'ref_string');

	# Secondary refs, followed by current ref
	$refRowString = PBDBUtil::getSecondaryRefsString($dbh,$collection_no,1,1);

    # secondary_references
	push(@row, $refRowString);
	push(@fieldNames, 'secondary_reference_string');

	# Clear the variable for use below
	$refRowString = "";

	# Check if current session ref is at all associated with the collection
	# If not, list it beneath the sec. refs. (with radio button for selecting
	# as the primary ref, as with the secondary refs below).
	unless(PBDBUtil::isRefPrimaryOrSecondary($dbh,$collection_no,$session_ref)){
		# This part allows current session ref to be selected as primary
		$refRowString = "<table border=0 cellpadding=8><tr><td>".
						 "<table border=0 cellpadding=2 cellspacing=0><tr>".
						 "</td></tr><tr class='darkList'><td valign=top>".
						 #"</td></tr><tr><td valign=top>".
						 "<input type=radio name=secondary_reference_no value=".
						 $session_ref."></td><td></td>";
		my $sr = getCurrRefDisplayStringForColl($dbh, $collection_no,$session_ref);
		# put the radio on the same line as the ref
		$sr =~ s/<tr>//;
		$refRowString .= $sr."</table></td></tr></table>";
		# Now, set up the current session ref to be added as a secondary even
		# if it's not picked as a primary (it's currently neither).
		$refRowString .= "\n<input type=hidden name=add_session_ref_as_2ndary ".
						 "value=$session_ref>\n";
	}

    # get the session reference
    push(@row, $refRowString);
    push(@fieldNames, 'session_reference_string');

	# get the max/min interval names
	my ($r,$f) = getMaxMinNamesAndDashes(\@row,\@fieldNames);
	@row = @{$r};
	@fieldNames = @{$f};

	print printIntervalsJava();

    push(@row, '<input type="hidden" name="action" value="processEditCollectionForm">');
    push(@fieldNames, 'page_target');
                                                                                                                                                             
    push(@row, "Collection no ".$collection_no);
    push(@fieldNames, 'page_title');
                                                                                                                                                             
    push(@row, '<input type=submit name="update_button" value="Update collection and exit">');
    push(@fieldNames, 'page_submit_button');
                                                                                                                                                             
    #push(@row, stdIncludes("std_page_bottom"));
    push(@row, stdIncludes('std_page_bottom'));
    push(@fieldNames, 'page_footer');

    # Output the main part of the page
    my %pref = getPreferences($s->get('enterer_no'));
    my @prefkeys = keys %pref;

    my $html = $hbo->populateHTML("collection_form", \@row, \@fieldNames, \@prefkeys);

    print $html;
}


sub processEditCollectionForm {
	# Save the old one in case a new one comes in
	my $reference_no = $q->param("reference_no");
	my $collection_no = $q->param("collection_no");
	my $secondary = $q->param('secondary_reference_no');

	print stdIncludes( "std_page_top" );

	unless($q->param('max_interval'))	{
		print "<center><h3>The time interval field is required!</h3>\n<p>Please go back and specify the time interval for this collection</p></center>";
		print stdIncludes("std_page_bottom");
		print "<br><br>";
		return;
	}

	# SECONDARY REF STUFF...
	# If a radio button was checked, we're changing a secondary to the primary
	if($q->param('secondary_reference_no')){
		# The updateRecord() logic will take care of putting in the new primary
		# reference for the collection
		$q->param(reference_no => $secondary);
		# Now, put the old primary ref into the secondary ref table
		PBDBUtil::setSecondaryRef($dbh, $collection_no, $reference_no);
		# and remove the new primary from the secondary table
		if(PBDBUtil::isRefSecondary($dbh,$collection_no,$secondary)){
			PBDBUtil::deleteRefAssociation($dbh, $collection_no, $secondary);
		}
	}
	# If the current session ref isn't being made the primary, and it's not
	# currently a secondary, add it as a secondary ref for the collection 
	# (this query param doesn't show up if session ref is already a 2ndary.)
	if(defined $q->param('add_session_ref_as_2ndary')){
		my $session_ref = $q->param('add_session_ref_as_2ndary');
		my $sess_ref_is_sec = PBDBUtil::isRefSecondary($dbh,$collection_no,
														$session_ref);
		if(($session_ref != $secondary) && (!$sess_ref_is_sec)){
			PBDBUtil::setSecondaryRef($dbh, $collection_no, $session_ref);
		}
	}
	# Delete secondary ref associations
	my @refs_to_delete = $q->param("delete_ref");
	dbg("secondary ref associations to delete: @refs_to_delete<br>");
	if(scalar @refs_to_delete > 0){
		foreach my $ref_no (@refs_to_delete){
			# check if any occurrences with this ref are tied to the collection
			if(PBDBUtil::refIsDeleteable($dbh, $collection_no, $ref_no)){
				# removes secondary_refs association between the numbers.
				dbg("removing secondary ref association (col,ref): $collection_no, $ref_no<br>");
				PBDBUtil::deleteRefAssociation($dbh, $collection_no, $ref_no);
			}
		}
	}

	# change interval names into numbers by querying the intervals table
	# JA 11-12.7.03
	if ( $q->param('max_interval') )	{
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('max_interval') . "'";
		if ( $q->param('eml_max_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_max_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(max_interval_no => $no);
	}
	if ( $q->param('min_interval') )	{
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('min_interval') . "'";
		if ( $q->param('eml_min_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_min_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(min_interval_no => $no);
	} else	{
		# WARNING: assumes that you do have a non-zero max interval no
		$q->param(min_interval_no => '0');
	}
	# bomb out if no such interval exists JA 28.7.03
	if ( $q->param('max_interval_no') < 1 )	{
		print "<center><h3>You can't enter an unknown time interval name</h3>\n<p>Please go back, check the time scales, and enter a valid name</p></center>";
		print stdIncludes("std_page_bottom");
		return;
	}
                                                                                                                                          
    #set paleolat, paleolng if we can PS 11/07/2004
    dbg("paleocoords part");
    my ($paleolat, $paleolng);
    if ($q->param('lngdeg') >= 0 && $q->param('lngdeg') =~ /\d+/ &&
        $q->param('latdeg') >= 0 && $q->param('latdeg') =~ /\d+/)
    {
        my ($f_latdeg, $f_lngdeg);
        if ($q->param('lngmin') =~ /\d+/ && $q->param('lngmin') >= 0 && $q->param('lngmin') < 60)  {
            $f_lngdeg = $q->param('lngdeg') + ($q->param('lngmin')/60) + ($q->param('lngsec')/3600);
        } else {
            $f_lngdeg = $q->param('lngdeg') . "." .  int($q->param('lngdec'));
        }
        if ($q->param('latmin') =~ /\d+/ && $q->param('latmin') >= 0 && $q->param('latmin') < 60)  {
            $f_latdeg = $q->param('latdeg') + ($q->param('latmin')/60) + ($q->param('latsec')/3600);
        } else {
            $f_latdeg = $q->param('latdeg') . "." .  int($q->param('latdec'));
        }
        dbg("f_lngdeg $f_lngdeg f_latdeg $f_latdeg");
        if ($q->param('lngdir') =~ /West/)  {
                $f_lngdeg = $f_lngdeg * -1;
        }
        if ($q->param('latdir') =~ /South/) {
                $f_latdeg = $f_latdeg * -1;
        }

        my $max_interval_no = ($q->param('max_interval_no')) ? $q->param('max_interval_no') : 0;
        my $min_interval_no = ($q->param('min_interval_no')) ? $q->param('min_interval_no') : 0;
        ($paleolng, $paleolat) = PBDBUtil::getPaleoCoords($dbh, $dbt,$max_interval_no,$min_interval_no,$f_lngdeg,$f_latdeg); 
        dbg("have paleocoords paleolat: $paleolat paleolng $paleolng");
        if ($paleolat ne "" && $paleolng ne "") {
            $q->param("paleolng"=>$paleolng);
            $q->param("paleolat"=>$paleolat);
        }
    }

    

    unless($q->param('fossilsfrom1'))	{
      $q->param(fossilsfrom1=>'NULL');
    }
    unless($q->param('fossilsfrom2'))	{
      $q->param(fossilsfrom2=>'NULL');
    }
	# added by JA 26.6.02
	$sql = "SELECT created FROM collections WHERE collection_no=";
	$sql .= $q->param('collection_no');
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
  	$sth->execute();
	my @row = $sth->fetchrow_array();
	$q->param(created => $row[0]);
    $sth->finish();
	# Why is this here? Maybe it should be called only if the release date
	# is not already set.
	setReleaseDate();

	# Updates here 
	my $recID = updateRecord( 'collections', 'collection_no', $q->param('collection_no') );
 
    print "<center><h3><font color='red'>Collection record updated</font></h3></center>\n";

	# Select the updated data back out of the database.
	$sql = "SELECT * FROM collections WHERE collection_no=$recID";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
  	$sth->execute();
	dbg( "$sql<HR>" );
	my @fields = @{$sth->{NAME}};
	@row = $sth->fetchrow_array();
	$sth->finish();
    
	# get the max/min interval names
	my ($r,$f) = getMaxMinNamesAndDashes(\@row,\@fields);
	@row = @{$r};
	@fields = @{$f};

	my $curColNum = 0;
	foreach my $colName (@fields){
		if($colName eq 'reference_no'){
			$reference_no = $row[$curColNum];
			last;
		}
		$curColNum++;
	}
	my $refRowString = getCurrRefDisplayStringForColl($dbh, $recID, $reference_no);
	push(@row, $refRowString);
	push(@fields, 'reference_string');

	# get the secondary_references
	push(@row, PBDBUtil::getSecondaryRefsString($dbh,$collection_no,0,0));
	push(@fields, 'secondary_reference_string');

    
    print $hbo->populateHTML('collection_display_fields', \@row, \@fields);
	print '<p><div align="center"><table><tr>';
    print '<td>'.$hbo->populateHTML('collection_display_buttons', \@row, \@fields).'</td>';
	print '<td>'.$hbo->populateHTML('collection_display_buttons2', [$recID],['prefill_collection_no']).'</td>';
    print "</tr></table></div></p>";
	print '<div align="center">';
    print $hbo->populateHTML('occurrence_display_buttons', \@row, \@fields);
	print '</div>';
    
	print qq|<center><b><p><a href="$exec_url?action=displaySearchColls&type=edit">Edit another collection using the same reference</a></p></b></center>|;
	print qq|<center><b><p><a href="$exec_url?action=displaySearchColls&type=edit&use_primary=yes">Edit another collection using its own reference</a></p></b></center>|;
	#print qq| <b><p><a href="$exec_url?action=displayEnterCollPage&prefill_collection_no=$recID">Add a collection with fields prefilled based on this collection</a></p></b></center>|;
	print qq|<center><b><p><a href="$exec_url?action=displaySearchCollsForAdd&type=add">Add a collection with the same reference</a></p></b></center>|;

	print stdIncludes("std_page_bottom");
}

## getCurrRefDisplayStringForColl($dbh, $collection_no, $reference_no, $session)
#   Description:    builds the reference display row
#
#   Parameters:     $collection_no  the collection to which the ref belongs
#                   $reference_no   the reference for which the string is
#                                   being built.
#                   $session        session object
#
#   Returns:        reference_display_row as processed by HTMLBuilder
##
sub getCurrRefDisplayStringForColl{
    my $dbh = shift;
    my $collection_no = shift;
    my $reference_no = shift;

    my $sql = "SELECT r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.publication_type FROM refs r WHERE r.reference_no=$reference_no";
    
    my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my @refRow = $sth->fetchrow_array();
    my @refFieldNames = @{$sth->{NAME}};
    $sth->finish();
    my $refRowString = $hbo->populateHTML('reference_display_row', \@refRow, \@refFieldNames);

    return $refRowString;
}


sub displayOccurrenceAddEdit {

	my $collection_no = $q->param("collection_no");
	if ( ! $collection_no ) { htmlError( "No collection_no specified" ); }

	# Grab the collection name for display purposes JA 1.10.02
	my $sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $collection_name = ${$sth->fetchrow_arrayref()}[0];
	$sth->finish();

	print stdIncludes( "std_page_top" );

	# get the occurrences right away because we need to make sure there
	#  aren't too many to be displayed
	$sql = "SELECT * FROM occurrences WHERE collection_no=$collection_no";
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
		print "<center><h3>Please select the rows you wish to edit</h3></center>\n\n";
		print "<center>\n";
		print "<table><tr><td>\n";
		print "<ul>\n";
        my ($startofblock,$endofblock);
		for my $rowset ( 1..100 )	{
			$endofblock = $rowset * 50;
			$startofblock = $endofblock - 49;
			if ( $#all_data >= $endofblock )	{
				print "<li><a href=\"$exec_url?action=displayOccurrenceAddEdit&collection_no=$collection_no&rows_to_display=$startofblock+to+$endofblock\">Rows <b>$startofblock</b> to <b>$endofblock</b></a>\n";
			}
			if ( $#all_data < $endofblock + 50 )	{
				$startofblock = $endofblock + 1;
				$endofblock = $#all_data + 1;
				print "<li><a href=\"$exec_url?action=displayOccurrenceAddEdit&collection_no=$collection_no&rows_to_display=$startofblock+to+$endofblock\">Rows <b>$startofblock</b> to <b>$endofblock</b></a>\n";
				last;
			}
		}
		print "</ul>\n\n";
		print "</td></tr></table>\n";
		print "</center>\n";
		print stdIncludes("std_page_bottom");
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

	my %pref = getPreferences($s->get('enterer_no'));
	my @prefkeys = keys %pref;

	my $html = $hbo->populateHTML('js_occurrence_checkform');

	# only print the JavaScript involving subgenera if the user
	#  wants to see them
	if ( $pref{'subgenera'} eq "yes" )	{
		$html =~ s/\/\/ check subgenera/\/\/ check subgenera
				if ( frm.subgenus_name[i].value != "" ) {
					errors += checkName ( "Subgenus", frm.subgenus_name[i].value, frm.subgenus_reso[i], i );
				}
/;
		$html =~ s/\/\/ check subgenus/\/\/ check subgenus
                                if ( frm.subgenus_name.value != "" ) {
                                        errors += checkName ( "Subgenus", frm.subgenus_name.value, frm.subgenus_reso, 1 );
                                }
/; 
	}

	print $html;

	print qq|<form method=post action="$exec_url" onSubmit='return checkForm();'>\n|;
	print qq|<input name="action" value="processEditOccurrences" type=hidden>\n|;
	print "<table>";

    my (@tempRow,@tempFieldName);
	push @tempRow, $collection_no;
	push @tempFieldName, "collection_no";

	push @tempRow, $collection_name;
	push @tempFieldName, "collection_name";

	print $hbo->populateHTML('occurrence_header_row', \@tempRow, \@tempFieldName, \@prefkeys);

    # main loop
    # each record is represented as a hash
    my $gray_counter = 0;
    foreach my $all_data_index ($firstrow..$lastrow){
	my $hash_ref = $all_data[$all_data_index];
		# This essentially empty reid_no is necessary as 'padding' so that
		# any actual reid number (see while loop below) will line up with 
		# its row in the form, and ALL rows (reids or not) will be processed
		# properly by processEditOccurrences(), below.
        $hash_ref->{'reid_no'} = '0';
        my @row = values %$hash_ref;
        my @names = keys %$hash_ref;

        my $occHTML = "";
        # Read Only
        if($hash_ref->{'writeable'} == 0){
            # processEditOccurrences uses 'row_token' 
            # to determine which rows to update
            if($gray_counter%2==0){
                $occHTML = $hbo->populateHTML("occurrence_read_only_row_gray", \@row, \@names, \@prefkeys);
            }
            else{
                $occHTML = $hbo->populateHTML("occurrence_read_only_row", \@row, \@names, \@prefkeys);
            }
        }
        else{
            print qq|<input type=hidden name="row_token" value="row_token">\n|;
            $occHTML = $hbo->populateHTML("occurrence_edit_row", \@row, \@names, \@prefkeys);
        }
		print $occHTML;

        $sql = "SELECT * FROM reidentifications WHERE occurrence_no=" .  $hash_ref->{'occurrence_no'};
		my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
        while(my $hr = $sth2->fetchrow_hashref()) {
            my @re_row = values %$hr;
            my @re_names = keys %$hr;
#			print "<tr><td></td><td colspan=3 class=tiny><i>reidentified as</i></td></tr>\n";
            my $reidHTML = "";
            # Read Only
            if($hash_ref->{'writeable'} == 0){
                if($gray_counter%2==0){
                    $reidHTML = $hbo->populateHTML("occurrence_read_only_row_gray", \@re_row, \@re_names, \@prefkeys);
                }
                else{
                    $reidHTML = $hbo->populateHTML("occurrence_read_only_row", \@re_row, \@re_names, \@prefkeys);
                }
            }
            else{
                print qq|<input type=hidden name="row_token" value="row_token">\n|;
                $reidHTML = $hbo->populateHTML("occurrence_edit_row", \@re_row, \@re_names, \@prefkeys);
            }
            # Strip away abundance widgets (crucial because reIDs never may
            #  have abundances) JA 30.7.02
            $reidHTML =~ s/<td><input id="abund_value" size=4 maxlength=255><\/td>/<td><input type=hidden name="abund_value"><\/td>/;
            $reidHTML =~ s/<td><select id="abund_unit"><\/select><\/td>/<td><input type=hidden name="abund_unit"><\/td>/;
            $reidHTML =~ s/<td align=right><select name="genus_reso">/<td align=right><nobr><b>reID<\/b><select name="genus_reso">/;
            $reidHTML =~ s/<td /<td class=tiny /g;
            # The first one needs to be " = (species ..."
            $reidHTML =~ s/<div id="genus_reso">/<div class=tiny>= /;
            $reidHTML =~ s/<input /<input class=tiny /g;
            $reidHTML =~ s/<select /<select class=tiny /g;
            print $reidHTML;
        }
        $sth2->finish();
        $gray_counter++;
    }

	# Extra rows for adding
	# Set collection_no, authorizer, enterer, and other hidden goodies
	my @fieldNames = (	'collection_no', 
						'reference_no', 
						'genus_reso',
						'subgenus_reso',
						'species_reso',
						'plant_organ', 
						'plant_organ2');
	my @row = ( $collection_no, 
				$s->get("reference_no"),
				'', '', '', '', '');

	# Read the users' preferences related to occurrence entry/editing
	%pref = getPreferences($s->get('enterer_no'));
	@prefkeys = keys %pref;

	# Figure out the number of blanks to print
	my $blanks = $pref{'blanks'};
	if ( ! $blanks ) { $blanks = 10; }

	# Set the default species name (indet. or sp.) JA 9.7.02
	# JA 21.4.04: have to ALWAYS do this or the id= tag in the form
	#  won't be stripped, which causes the JavaScript to get confused
	#  about row numbering
	unshift @row, $pref{'species_name'};
	unshift @fieldNames, "species_name";

	for ( my $i = 0; $i<$blanks ; $i++) {
		print qq|<input type=hidden name="row_token" value="row_token">\n|;
		print $hbo->populateHTML("occurrence_entry_row", \@row, \@fieldNames, \@prefkeys);
	}

	print "</table><br>\n";
	print qq|<center><p><input type=submit value="Save changes">|;
    printf " to collection %s's taxonomic list</p></center>\n",$collection_no;
	print "</form>";

	print stdIncludes("std_page_bottom");
}

sub processEditOccurrences {
    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first." );
        exit;
    }
                                
	# Get the names of all the fields coming in from the form.
	my @param_names = $q->param();
	# list of the number of rows to possibly update.
	my @rowTokens = $q->param('row_token');

	# make a hash of parameter names => arrays of values
	my %all_params = ();
	foreach my $name (@param_names){
		$all_params{$name} = [$q->param($name)];
	}

	# for identifying unrecognized (new to the db) genus/species names.
	# these are the new taxa names that the user is trying to enter
	my @gnew_names = (); # genus
	my @subgnew_names = (); # subgenus
	my @snew_names = (); # species
	my $gnew_names_ref = $all_params{'genus_name'};
	my $subgnew_names_ref = $all_params{'subgenus_name'};
	my $snew_names_ref = $all_params{'species_name'};
	# get all genus names in order to check for a new name
	if($gnew_names_ref){
		push(@gnew_names, PBDBUtil::newTaxonNames($dbh,$gnew_names_ref,'genus'));
	}
	if($subgnew_names_ref){
		push(@subgnew_names, PBDBUtil::newTaxonNames($dbh,$subgnew_names_ref,'subgenus'));
	}
	if($snew_names_ref){
		push(@snew_names, PBDBUtil::newTaxonNames($dbh,$snew_names_ref,'species'));
	}

	# list of required fields
	my @required_fields = ("collection_no", "genus_name", "species_name", "reference_no");
	# hashes of which fields per table are integral (vs. text) type
	# and which may contain NULL values.
	%reidIntFields = ();
	%reidNullFields = ();
	%occsIntFields = ();
	%occsNullFields = ();

	# loop over all rows submitted from the form
	ROW:
	for(my $index = 0;$index < @rowTokens; $index++){
		# check that all required fields have a non empty value
		foreach my $field_check (@required_fields){
			if(!exists($all_params{$field_check}) || 
							${$all_params{$field_check}}[$index] eq ""){
				next ROW;
			}
		}
		my $sql = "";

	# guess the taxon no by trying to find a single match for the name
	#  in the authorities table JA 1.4.04
	# see Reclassify.pm for a similar operation
	# only do this for non-informal taxa
		if ( $all_params{'genus_reso'}[$index] !~ /informal/ )	{
			$taxon_name = $all_params{'genus_name'}[$index];
			if ( $all_params{'species_reso'}[$index] !~ /informal/ && $all_params{'species_name'}[$index] ne "sp." && $all_params{'species_name'}[$index] ne "indet." )	{
				$taxon_name .= " " . $all_params{'species_name'}[$index];
			}
			$sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
			$sql .= $taxon_name . "'";
			my @taxnorefs = @{$dbt->getData($sql)};
			# exactly one match?
			if ( $#taxnorefs == 0 )	{
				$all_params{'taxon_no'}[$index] = $taxnorefs[0]->{taxon_no};
			}
			# no match? try genus alone
			elsif ( ! @taxnorefs && $taxon_name =~ / / )	{
				($taxon_name) = split / /,$taxon_name;
				$sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
				$sql .= $taxon_name . "'";
				@taxnorefs = @{$dbt->getData($sql)};
				if ( $#taxnorefs == 0 )	{
					$all_params{'taxon_no'}[$index] = $taxnorefs[0]->{taxon_no};
				} else	{
					$all_params{'taxon_no'}[$index] = 0;
				}
			} else	{
				$all_params{'taxon_no'}[$index] = 0;
			}
		} else {
			$all_params{'taxon_no'}[$index] = 0;
        }

		# back to our normal code, pre-existing the above

		# CASE 1
		# if we have a reid_no and an occurrence_no, we're updating a reid
		if(exists($all_params{reid_no}) 
		   && ${$all_params{reid_no}}[$index] > 0
		   && exists($all_params{occurrence_no})
		   && ${$all_params{occurrence_no}}[$index] > 0){

			my $reid_no = ${$all_params{reid_no}}[$index];

			# select the old data record to merge in the new data record.
			$sql = "SELECT * FROM reidentifications where reid_no=".$reid_no;
			my $sth = $dbh->prepare($sql) or die "Error preparing sql $sql ($!)";
			$sth->execute() or die "Error executing $sql ($!)";
			# Get the names of the fields that are integral typed
			if(!defined(%reidIntFields)){
				# contains string names
				my @names = @{$sth->{'NAME'}};
				# contains boolean
				my @nullables = @{$sth->{'NULLABLE'}};
				# contains boolean
				my @types = @{$sth->{'mysql_is_num'}};
				for(my $i=0; $i<@names; $i++){
					$reidIntFields{$names[$i]} = $types[$i];
					$reidNullFields{$names[$i]} = $nullables[$i];
				}
			}
			my $results_ref = $sth->fetchrow_hashref();
			# Error checking
			if(!defined($results_ref)){
				htmlError("ERROR in processEditOccurrences: $sth->errstr<br>");
			}
			$sth->finish();
			my %results = %{$results_ref};

			# remove keys we don't directly mess with from the result set
			# NOTE: for some reason Muhl originally excluded
			#  authorizer and enterer, causing corruption of data;
			#  fixed this 3.4.04 JA
			delete($results{authorizer_no});
			delete($results{enterer_no});
			delete($results{modifier_no});
			delete($results{authorizer});
			delete($results{enterer});
			delete($results{modifier});
			delete($results{created});
			delete($results{modified});
			delete($results{modified_temp});
			delete($results{reid_no});
            delete($results{most_recent});
			
			my $something_changed = 0;
			my @update_strings = ();

			# compare form data with database data
			FIELD:
			foreach my $key (keys %results){
				# compare ints to ints... 
				if($reidIntFields{$key} == 1){
					# if the form value is different than the db value, use it.
					unless($results{$key} == ${$all_params{$key}}[$index]){
						$something_changed = 1;
						$results{$key} = ${$all_params{$key}}[$index];
						# Note: there will be no empty integral values
						# since they are all required
						push(@update_strings,"$key=$results{$key}");
					}
				}
				# strings with strings...
				else{
					# if the form value is different than the db value, use it.
					unless($results{$key} eq ${$all_params{$key}}[$index]){
						$something_changed = 1;
						$results{$key} = ${$all_params{$key}}[$index];
						# Deal with empty values
						if($results{$key} eq ""){
							# can't wipe out required fields:
							for(my $j = 0; $j < @required_fields; $j++){
								if($required_fields[$j] eq $key){
									next FIELD;
								}
							}
							if($reidNullFields{$key} == 1){
								push(@update_strings,"$key=NULL");
							}
							else{
								push(@update_strings,"$key=''");
							}
						}
						else{
							push(@update_strings,"$key=".$dbh->quote($results{$key}));
						}
					}
				}
			}

			# if no values were different, don't update
			if(!$something_changed){
				next ROW;
			}


			# 'modifier' is set to enterer:
			my $modifier = $dbh->quote($s->get('enterer'));
			push(@update_strings,"modifier=$modifier");
			my $modifier_no = $s->get('enterer_no');
			push(@update_strings,"modifier_no=$modifier_no");

			# ugly hack: make sure taxon_no doesn't change unless
			#  genus_name or species_name did JA 1.4.04
			# this could lead to a bizarre case in which only
			#  the modifier name changes, but who cares?
			my $changed_taxon = "";
			for my $us ( @update_strings )	{
				if ( $us =~ /genus_name/ || $us =~ /species_name/ )	{
					$changed_taxon = 1;
					last;
				}
			}
			if ( ! $changed_taxon )	{
				for my $i ( 0..$#update_strings )	{
					if ( $update_strings[$i] =~ /taxon_no/ )	{
						splice @update_strings, $i, 1;
						last;
					}
				}
			}

			# Update the reidentification row
			$sql = "UPDATE reidentifications SET ".
					join(',', @update_strings) .
					" WHERE reid_no=$reid_no";
			dbg("REID UPDATE SQL: $sql<br>");

			# Prepare, execute
			$dbh->do($sql) || die ("$sql<hr>$!");	

            # Reset which reid is marked as the most recent is the reference changes
            for my $i ( 0..$#update_strings )	{
				if ($update_strings[$i] =~ /reference_no/ && $results{'occurrence_no'}) {
                    setMostRecentReID($dbt,$results{'occurrence_no'});
                }
            }

			# If @update_strings contains 'reference_no',
			# check if new ref is primary or secondary for the
			# given collection, and if not, add the coll_no<->ref_no
			# association to the secondary_refs table.
			my $flattened = join(" ",@update_strings);
			if($flattened =~ /reference_no/){
				dbg("calling setSecondaryRef (updating ReID)<br>");
				unless(PBDBUtil::isRefPrimaryOrSecondary($dbh, $results{collection_no}, $results{reference_no})){
					PBDBUtil::setSecondaryRef($dbh,$results{collection_no},
											  $results{reference_no});
				}
			}
		}
		
		# CASE 2
		# if we just have an occurrence_no, we're updating an occurrence
		elsif(exists($all_params{occurrence_no})
				&& ${$all_params{occurrence_no}}[$index] > 0){

			$sql = "SELECT * FROM occurrences where occurrence_no=".
					${$all_params{occurrence_no}}[$index];
			my $sth = $dbh->prepare($sql) or die "Error preparing sql $sql ($!)";
			$sth->execute() or die "Error executing $sql ($!)";
			# Get the names of the fields that are integral typed
			if(!defined(%occsIntFields)){
				# contains string names
				my @names = @{$sth->{'NAME'}};
				# contains boolean
				my @nullables = @{$sth->{'NULLABLE'}};
				# contains boolean
				my @types = @{$sth->{'mysql_is_num'}};
				for(my $i=0; $i<@names; $i++){
					$occsIntFields{$names[$i]} = $types[$i];
					$occsNullFields{$names[$i]} = $nullables[$i];
				}
			}
			my $results_ref = $sth->fetchrow_hashref();
			# Error checking
			if(!defined($results_ref)){
				htmlError("ERROR in processEditOccurrences: $sth->errstr<br>");
			}
			$sth->finish();
			my %results = %{$results_ref};

			# remove keys we don't directly mess with from the result set
			# NOTE: for some reason Muhl originally excluded
			#  authorizer and enterer, causing corruption of data;
			#  fixed this 3.4.04 JA
			delete($results{authorizer});
			delete($results{enterer});
			delete($results{modifier});
			delete($results{authorizer_no});
			delete($results{enterer_no});
			delete($results{modifier_no});
			delete($results{created});
			delete($results{modified});
			delete($results{occurrence_no});
            delete($results{upload});
			
			my $something_changed = 0;
			my @update_strings = ();

			# compare form data with database data
			FIELD2:
			foreach my $key (keys %results){
				# compare ints to ints... 
				if($occsIntFields{$key} == 1){
					# if the form value is different than the db value, use it.
					unless($results{$key} == ${$all_params{$key}}[$index]){
						$something_changed = 1;
						$results{$key} = ${$all_params{$key}}[$index];
						# Note: there will be no empty integral values
						# since they are all required
						push(@update_strings,"$key=$results{$key}");
					}
				}
				# strings with strings...
				else{
					# if the form value is different than the db value, use it.
					unless($results{$key} eq ${$all_params{$key}}[$index]){
						$something_changed = 1;
						$results{$key} = ${$all_params{$key}}[$index];
						# Deal with empty values
						if($results{$key} eq ""){
							# can't wipe out required fields:
							for(my $j = 0; $j < @required_fields; $j++){
								if($required_fields[$j] eq $key){
									next FIELD2;
								}
							}
							if($occsNullFields{$key} == 1){
								push(@update_strings,"$key=NULL");
							}
							else{
								push(@update_strings,"$key=''");
							}
						}
						else{
							push(@update_strings,"$key=".$dbh->quote($results{$key}));
						}
					}
				}
			}

			# if no values were different, don't update
			if(!$something_changed){
				next ROW;
			}

			# 'modifier' is set to enterer:
			my $modifier = $dbh->quote($s->get('enterer'));
			push(@update_strings,"modifier=$modifier");
			my $modifier_no = $s->get('enterer_no');
			push(@update_strings,"modifier_no=$modifier_no");


			# ugly hack: make sure taxon_no doesn't change unless
			#  genus_name or species_name did JA 1.4.04
			# this could lead to a bizarre case in which only
			#  the modifier name changes, but who cares?
			my $changed_taxon = "";
			for my $us ( @update_strings )	{
				if ( $us =~ /genus_name/ || $us =~ /species_name/ )	{
					$changed_taxon = 1;
					last;
				}
			}
			if ( ! $changed_taxon )	{
				for my $i ( 0..$#update_strings )	{
					if ( $update_strings[$i] =~ /taxon_no/ )	{
						splice @update_strings, $i, 1;
						last;
					}
				}
			}

			# update the occurrence row
			$sql = "UPDATE occurrences SET ".
					join(',', @update_strings) .
					" WHERE occurrence_no=".
					${$all_params{occurrence_no}}[$index];
			dbg("OCCURRENCE UPDATE SQL: $sql<br>");

			# Prepare, execute
			$dbh->do( $sql ) || die ( "$sql<HR>$!" );	

			# If @update_strings contains 'reference_no',
			# check if new ref is primary or secondary for the
			# given collection, and if not, add the coll_no<->ref_no
			# association to the secondary_refs table.
			my $flattened = join(" ",@update_strings);
			if($flattened =~ /reference_no/){
				dbg("calling setSecondaryRef (updating occurrence)<br>");
				unless(PBDBUtil::isRefPrimaryOrSecondary($dbh, $results{collection_no}, $results{reference_no})){
					PBDBUtil::setSecondaryRef($dbh,$results{collection_no},
											  $results{reference_no});
				}
			}
		}

		# CASE 3
		# if we have neither an occurrence_no nor a reid_no, we're 
		# inserting an occurrence.	
		else{
			# If this is the first row, we don't know yet which fields have
			# to be quoted for insert (which are strings and which are ints)
			# Get the names of the fields that are integral typed
			if(!defined(%occsIntFields)){
				$sql = "SELECT * FROM occurrences where occurrence_no=0";
				my $sth = $dbh->prepare($sql) or die "Error preparing sql $sql ($!)";
				$sth->execute() or die "Error executing $sql ($!)";
				# contains string names
				my @names = @{$sth->{'NAME'}};
				# contains boolean
				my @nullables = @{$sth->{'NULLABLE'}};
				# contains boolean
				my @types = @{$sth->{'mysql_is_num'}};
				for(my $i=0; $i<@names; $i++){
					$occsIntFields{$names[$i]} = $types[$i];
					$occsNullFields{$names[$i]} = $nullables[$i];
				}
				$sth->finish();
			}

			my @insert_names = ();
			my @insert_values = ();

			# stuff names / values in the above arrays for checkDuplicates()
			foreach my $val (keys %all_params){
				next if $val =~ /^(?:row_token|action|authorizer|enterer|authorizer_no|enterer_no)$/;
				if(${$all_params{$val}}[$index]){
					push(@insert_names, $val);
					if($occsIntFields{$val} == 1){
						push(@insert_values, ${$all_params{$val}}[$index]);
					}
					else{
						push(@insert_values, $dbh->quote(${$all_params{$val}}[$index]));
					}
				}
			}
			push(@insert_names, 'created');
			push(@insert_values, "'".now()."'");
			push(@insert_names, 'authorizer','enterer','authorizer_no', 'enterer_no');
    		push(@insert_values, $dbh->quote($s->get('authorizer')), $dbh->quote($s->get('enterer')),$s->get('authorizer_no'),$s->get('enterer_no'));
			# Check for duplicates
			my $return = checkDuplicates("occurrence_no", \${$all_params{occurrence_no}}[$index], "occurrences", \@insert_names, \@insert_values );
			if(!$return){return $return;}
			if($return != $DUPLICATE){
				$sql = "INSERT INTO occurrences (".
						join(', ', @insert_names) .
						") VALUES (" .
						join(', ', @insert_values) . ")";
				dbg("$sql<hr>$!");
				$dbh->do($sql) || die("$sql<hr>$!");
			}

			# If @insert_names contains 'reference_no',
			# check if new ref is primary or secondary for the
			# given collection, and if not, add the coll_no<->ref_no
			# association to the secondary_refs table.
			my $flattened = join(" ",@insert_names);
			if($flattened =~ /reference_no/){
				dbg("calling setSecondaryRef inserting occurrence<br>");
				unless(PBDBUtil::isRefPrimaryOrSecondary($dbh,${$all_params{collection_no}}[$index],${$all_params{reference_no}}[$index] )){
					PBDBUtil::setSecondaryRef($dbh,
										  ${$all_params{collection_no}}[$index],
										  ${$all_params{reference_no}}[$index]);
				}
			}
		}
	}

	print stdIncludes( "std_page_top" );

	# Show the rows for this collection to the user
	my $collection_no = ${$all_params{collection_no}}[0];

	print buildTaxonomicList( $collection_no, 0,\@gnew_names,\@subgnew_names,\@snew_names );

	# Show a link to re-edit
	print "
	<p><center><b>
	<a href='$exec_url?action=displayOccurrenceAddEdit&collection_no=$collection_no'>Add/edit&nbsp;this&nbsp;collection's&nbsp;occurrences</a> -
	<a href='$exec_url?action=startStartReclassifyOccurrences&collection_no=$collection_no'><b>Reclassify&nbsp;this&nbsp;collection's&nbsp;occurrences</b></a> -
	<a href='$exec_url?action=displayEditCollection&collection_no=$collection_no'>Edit&nbsp;the&nbsp;main&nbsp;collection&nbsp;record</a> -
	<a href='$exec_url?action=displaySearchColls&type=edit_occurrence'>Add/edit&nbsp;occurrences&nbsp;for&nbsp;a&nbsp;different&nbsp;collection</a> -
	<a href='$exec_url?action=displaySearchCollsForAdd&type=add'>Add&nbsp;another&nbsp;collection</a></b><p></center>
";

	print stdIncludes("std_page_bottom");
}


 #  3.* System processes new reference if user entered one.  System
#       displays search occurrences and search collections forms
#     * User searches for a collection in order to work with
#       occurrences from that collection
#     OR
#     * User searches for genus names of occurrences to work with
sub displayReIDCollsAndOccsSearchForm
{

	# Have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );
		displayLoginPage( "Please log in first." );
		exit;
	}
	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );
		displaySearchRefs( "Please choose a reference first" );
		exit;
	}	

	print stdIncludes( "std_page_top" );
  
	print "<h4>You may now reidentify either a set of occurrences matching a genus or higher taxon name, or all the occurrences in one collection.</h4>";
  
	# Display the collection search form
	%pref = getPreferences($s->get('enterer_no'));
	my @prefkeys = keys %pref;
	my $html = $hbo->populateHTML('search_collections_form', ['', '', 'displayReIDForm', $reference_no,'',$q->param('type'),'','','',''], ['authorizer', 'enterer', 'action', 'reid_reference_no', 'lithadj', 'lithology1','type','lithadj2', 'lithology2','environment','eml_min_interval','eml_max_interval'], \@prefkeys);

    my $javaScript = &makeAuthEntJavaScript();
    $html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/; 

	# Set the Enterer & Authorizer
	my $enterer_reversed = $s->get("enterer_reversed");
	$html =~ s/%%enterer_reversed%%/$enterer_reversed/;
	my $authorizer_reversed = $s->get("authorizer_reversed");
	$html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;

	# Spit out the HTML
    printIntervalsJava(1);
	print $html;
  
	print '</form>';
	print stdIncludes("std_page_bottom");
}

sub displayReIDForm {
	$q->param("type" => "reid");
	displayCollResults();
}

sub displayOccsForReID
{
	my $genus_name = $q->param('g_name');
	my $species_name = $q->param('species_name');

	if(!$genus_name && !$species_name){
		$genus_name = $q->param('genus_name');
	}

	my $collection_no = $q->param('collection_no');
	my $sql = "";
	my $where = "";

	dbg("genus_name: $genus_name, species_name: $species_name<br>");

	my $current_session_ref = $s->get("reference_no");
	# make sure they've selected a reference
	# (the only way to get here without a reference is by doing 
	# a coll search right after logging in).
	unless($current_session_ref){
		displaySearchRefs();	
		$s->enqueue( $dbh, "action=displayOccsForReID&collection_no=$collection_no" );
		exit();
	}

	my $collNos = shift;
	my @colls;
	if($collNos){
		@colls = @{$collNos};
	}

	my $onFirstPage = 0;
	my $printCollDetails = 0;

	print stdIncludes( "std_page_top" );
	print $hbo->populateHTML('js_occurrence_checkform');
    
	my $lastOccNum = $q->param('last_occ_num');
	if ( ! $lastOccNum ) { 
		$lastOccNum = -1;
		$onFirstPage = 1; 
	}

	# Build the SQL
    my @where = ();
	if($genus_name ne '' or $species_name ne ''){
		$printCollectionDetails = 1;

        push @where, "genus_name=".$dbh->quote($genus_name) if ($genus_name);
        push @where, "species_name=".$dbh->quote($species_name) if ($species_name);
		
		if (@colls > 0) {
			push @where, "collection_no IN(".join(',',@colls).")";
		} elsif ($collection_no > 0) {
			push @where, "collection_no=$collection_no";
		}
	} elsif ($collection_no) {
		push @where, "collection_no=$collection_no";
	}

	push @where, "occurrence_no > $lastOccNum";

	# some occs are out of primary key order, so order them JA 26.6.04
    $sql = "SELECT * FROM occurrences WHERE ".join(" AND ",@where).
           " ORDER BY occurrence_no LIMIT 11";

	dbg("$sql<br>");
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Get the current reference data
    $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$s->get('reference_no');
	dbg("$sql");
	my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth2->execute();
	my $array_ref_of_hash_refs = $sth->fetchall_arrayref({});
	my @array_of_hash_refs = @{$array_ref_of_hash_refs};
	dbg("got ".@array_of_hash_refs." occs for reid<br>");

    my $md = MetadataModel->new($sth2);
	my $refRow = $sth2->fetchrow_arrayref();
    my $drow = DataRow->new($refRow, $md);
    my $bibRef = BiblioRef->new($drow);
	$sth2->finish();
	my $refString = $bibRef->toString();

	my $rowCount = 0;
	my %pref = getPreferences($s->get('enterer_no'));
	my @prefkeys = keys %pref;
	foreach my $rowRef (@array_of_hash_refs)
	{
		# If we have 11 rows, skip the last one; and we need a next button
		$rowCount++;
		last if $rowCount > 10;

		my @prefkeys;
		if ($rowCount == 1)	{
			print $hbo->populateHTML('reid_header_row', [ $refString ], [ 'ref_string' ], \@prefkeys);
		}

		my %row = %{$rowRef};

		# Print occurrence row and reid input row
		print "<tr>\n";
		print "    <td align=right>".$row{"genus_reso"}."</td>\n";
		print "    <td>".$row{"genus_name"}."</td>\n";
		if ($pref{'subgenera'} eq "yes")	{
			print "    <td align=right>".$row{"subgenus_reso"}."</td>\n";
			print "    <td>".$row{"subgenus_name"}."</td>\n";
		}
		print "    <td align=right>".$row{"species_reso"}."</td>\n";
		print "    <td>".$row{"species_name"}."</td>\n";
		print "    <td>" . $row{"comments"} . "</td>\n";
		if ($pref{'plant_organs'} eq "yes")	{
			print "    <td>" . $row{"plant_organ"} . "</td>\n";
			print "    <td>" . $row{"plant_organ2"} . "</td>\n";
		}
		print "</tr>";
		print $hbo->populateHTML('reid_entry_row', [$row{'occurrence_no'}, $row{'collection_no'}, $s->get('authorizer'), $s->get('enterer') ], ['occurrence_no', 'collection_no', 'authorizer', 'enterer'], \@prefkeys);

		# print other reids for the same occurrence
		$sql = "SELECT * FROM reidentifications WHERE occurrence_no=" . $row{'occurrence_no'};
		$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();

		print "<tr><td colspan=100>";
		print getReidHTMLTableByOccNum($row{'occurrence_no'}, 0);
		print "</td></tr>\n";
		$sth2->finish();

		# Print the reference details
        $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=".$row{'reference_no'};
		$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
		my $md = MetadataModel->new($sth2);
		my $refRow = $sth2->fetchrow_arrayref();
		my $drow = DataRow->new($refRow, $md);
		# my $bibRef = BiblioRef->new($drow, $md);

		print "
<tr>
	<td colspan=100>
	<table border=0 cellspacing=0 cellpadding=0>
	<tr>
		<td colspan='4'>
		<font size=-1><b>Original reference</b>:</font>
		</td>
	</tr>
";
		# Last parameter is "suppress_colls" so collection numbers aren't shown.
		my $ref_string = makeRefString($drow,0,0,0,1);
		print $ref_string;
		# print $bibRef->toString();

		print "</table></td></tr>";

		$sth2->finish();

		# Print the collections details
		if ( $printCollectionDetails) {

			$sql = "SELECT collection_name,state,country,formation,period_max FROM collections WHERE collection_no=" . $row{'collection_no'};
			$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	    	$sth2->execute();
	    	my %collRow = %{$sth2->fetchrow_hashref()};
	    	print "<tr>";
	    	print "  <td colspan=100>";
			print "<font size=-1><b>Collection</b>:<br>";
			my $details = $collRow{'collection_name'};
			if ($collRow{'state'})	{
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
	    	print "$details</font>  </td>";
	    	print "</tr>";
	    	$sth2->finish();
		}
    
		print "<tr><td colspan=100><hr width=100%></td></tr>";
		$lastOccNum = $row{'occurrence_no'};
	}

	print "</table>\n";
	if ($rowCount > 0)	{
		print qq|<center><input type=submit value="Save reidentifications"></center><br>\n|;
	} else	{
		print "<center><h3>Sorry! No matches were found</h3></center>\n";
		print "<p align=center><b>Please <a href=\"$exec_url?action=displayReIDCollsAndOccsSearchForm\">try again</a> with different search terms</b></p>\n";
	}
	print "</form>\n";
	print "\n<table border=0 width=100%>\n<tr>\n";

	# Print prev and next  links as appropriate

	# Next link
	if ( $rowCount > 10 ) {
		print "<td align=center>";
		print qq|<b><a href="$exec_url?action=displayOccsForReID|;
		print qq|&g_name=$genus_name| if $genus_name;
		print qq|&species_name=$species_name| if $species_name;
		print qq|&collection_no=$collection_no&last_occ_num=$lastOccNum"> View next 10 occurrences</a></b>\n|;
		print "</td></tr>\n";
		print "<tr><td class=small align=center><i>Warning: if you go to the next page without saving, your changes will be lost</i></td>\n";
	}

	print "</tr>\n</table><p>\n";

	print stdIncludes("std_page_bottom");
}

#  5. * System processes reidentifications.  System displays
#       reidentification form.  Occurrences which were reidentified
#       in the previous step may not be reidentified in this step
sub processNewReIDs {
    if (!$s->isDBMember()) {
        displayLoginPage( "Please log in first." );
        exit;
    }

	print stdIncludes( "std_page_top" );
    
	# Get the database metadata
	$sql = "SELECT * FROM reidentifications WHERE reid_no=0";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	# Get a list of field names
	my @fieldNames = @{$sth->{NAME}};
	# Get a list of field types
	my @fieldTypes = @{$sth->{mysql_is_num}};
	# Get the number of fields
	my $numFields = $sth->{NUM_OF_FIELDS};
	# Get the null constraints
	my @nullables = @{$sth->{NULLABLE}};
	# Get the types
	my @types = @{$sth->{mysql_type_name}};
	# Get the primary key data
	my @priKeys = @{$sth->{mysql_is_pri_key}};
	#print join(',', @types);
	$sth->finish();

	my @requiredFields = ("occurrence_no", "collection_no", "genus_name", "species_name", "reference_no");
	for my $i (0..$numFields)	{
		for my $j (0..$#requiredFields)	{
			if ($fieldNames[$i] eq $requiredFields[$j])	{
				$isRequired[$i] = "Y";
				last;
			}
		}
	}
		
		# Iterate over the rows, and commit each one
		my @successfulRows;
		my @rowTokens = $q->param('row_token');
		my $numRows = @rowTokens;
		my $skipped;

		if ( @rowTokens )	{
			print "<center><h3>Submitted reidentifications</h3></center>\n\n";
		}
		else	{
			print "<center><h3>No reidentifications were submitted</h3></center>\n\n";
		}
		print "<table align=center>\n";

		ROW:
		for(my $i = 0;$i < $numRows;$i++)
		{
			my @fieldList;
			my @row;
		# added 11.7.02 JA (omitted by Garot!)
			push(@fieldList, 'created');
			push(@row, '"' . now() . '"');


		# need these to check whether to add secondary refs
		#  (see below) JA 6.5.04
			my $collection_no;
			my $reference_no = $s->get('reference_no');
            my ($genus_name,$genus_reso,$species_reso,$species_name);
			for(my $j = 0;$j < $numFields;$j++)
			{
				my $fieldName = $fieldNames[$j];
				my @tmpVals = $q->param($fieldName);
				my $curVal = $tmpVals[$i];

				if ( $fieldName eq "collection_no" )	{
					$collection_no = $curVal;
				}
                $genus_reso = $curVal if ($fieldName eq 'genus_reso');
                $genus_name= $curVal if ($fieldName eq 'genus_name');
                $species_reso = $curVal if ($fieldName eq 'species_reso');
                $species_name = $curVal if ($fieldName eq 'species_name');
				
				next if  $fieldName eq 'reference_no';
				# Skip rows that don't have a required data item
				unless ( ! $isRequired[$j] || ($types[$j] eq 'timestamp') || $priKeys[$j] || $curVal ) {
					# explain why the row was skipped unless
					#  the missing field is the genus name,
					#  in which case it's obvious
					#  JA 26.60.04
					if ( $fieldName ne "genus_name" )	{
						my $temp = $fieldName;
						$temp =~ s/_/ /g;
						$temp =~ s/no$/number/g;
						my $tempi = $i + 1;
						$tempString = "<tr><td>";
						$tempString .= "<b>Row $tempi</b>: <i>skipped</i> because it is missing the $temp<p>\n";
						$tempString .= "</td></tr>\n\n";
						$skipped++;
						push(@submittedRow, $tempString);
					}
  					next ROW;
				}
 
				if ( $curVal) {
					my $val = $curVal;
					$val = $dbh->quote($val) if $fieldTypes[$j] == 0;
					
					push(@row, $val);
					push(@fieldList, $fieldName);
				}
				elsif ( defined $q->param($fieldName))
				{
					push(@row, 'NULL');
					push(@fieldList, $fieldName);
				}
				#print "<br>$i $fieldName: $val";
			}

            
            # guess the taxon no by trying to find a single match for the name
            #  in the authorities table JA 1.4.04
            # see Reclassify.pm for a similar operation
            # only do this for non-informal taxa
            # missing from here, added in 03/23/2005 PS
            my $taxon_no = 0;
            if ( $genus_reso !~ /informal/ )  {
                my @taxon_nos;
                # Try Genus+species first, if possible
                if ( $species_name !~ /^(indet\.|sp\.)/i && $species_reso !~ /informal/) {
                    @taxon_nos = TaxonInfo::getTaxonNos($dbt,"$genus_name $species_name");
                    if (scalar(@taxon_nos) == 1) {
                        $taxon_no = $taxon_nos[0];
                    }
                }    
                # Lastly try just Genus. If taxon_nos > 0, we found 1 match, or too many
                # In either case, we're hosed
                if ( !scalar(@taxon_nos) ) {
                    @taxon_nos = TaxonInfo::getTaxonNos($dbt,$genus_name);
                    if (scalar(@taxon_nos) == 1) {
                        $taxon_no = $taxon_nos[0];
                    }    
                } 
            }
            #print "sn $species_name sr $species_reso gn $genus_name gr $genus_reso t# $taxon_no<br>";
			push(@fieldList, 'taxon_no');
			push(@row, $taxon_no);
 
			push(@fieldList, 'authorizer','enterer','authorizer_no', 'enterer_no','reference_no');
    		push(@row, $dbh->quote($s->get('authorizer')), $dbh->quote($s->get('enterer')),$s->get('authorizer_no'),$s->get('enterer_no'),$s->get('reference_no'));

			# Check for duplicates
			my $return = checkDuplicates( "reid_no", \$recID, "reidentifications", \@fieldList, \@row );
			if ( ! $return ) { return $return; }


			if ( $return != $DUPLICATE ) {
				$sql = "INSERT INTO reidentifications (" . join(',', @fieldList).") VALUES (".join(', ', @row) . ")" || die $!;
				$sql =~ s/\s+/ /gms;
				dbg( "$sql<HR>" );
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
      
				$recID = $dbh->{'mysql_insertid'};
				unless(PBDBUtil::isRefPrimaryOrSecondary($dbh, $collection_no, $reference_no))	{
					PBDBUtil::setSecondaryRef($dbh,$collection_no, $reference_no);
				}

                my $occurrence_no = 0;
                for($i=0;$i<scalar(@fieldList);$i++) {
                    if ($fieldList[$i] eq 'occurrence_no') {
                        $occurrence_no=$row[$i];
                        last;
                    }
                }
                # This marks the most recent reid for an occurrence as 'most_recent' in the database
                # so that scripts can properly get only most recent reids when they need them.
                if ($occurrence_no) {
                    setMostRecentReID($dbt,$occurrence_no);
                }
			}

			$sql = "SELECT occurrence_no,genus_reso,genus_name,species_reso,species_name,comments,collection_no FROM reidentifications WHERE reid_no=$recID";
			dbg ( "$sql<HR>" );
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			my @retrievedRow = $sth->fetchrow_array();
			$sth->finish();

			if ( $retrievedRow[4] ne "indet." )	{
				$retrievedRow[2] = "<i>" . $retrievedRow[2] . "</i>";
				$retrievedRow[4] = "<i>" . $retrievedRow[4] . "</i>";
			}
			my $reidname = $retrievedRow[1] . " " . $retrievedRow[2] . " " . $retrievedRow[3] . " " . $retrievedRow[4];

			$sql = "SELECT genus_reso,genus_name,species_reso,species_name FROM occurrences WHERE occurrence_no=" . $retrievedRow[0];
			my $occref = @{$dbt->getData($sql)}[0];
			if ( $occref->{species_name} ne "indet." )	{
				$occref->{genus_name} = "<i>" . $occref->{genus_name} . "</i>";
				$occref->{species_name} = "<i>" . $occref->{species_name} . "</i>";
			}
			my $occname = $occref->{genus_reso} . " " . $occref->{genus_name} . " " . $occref->{species_reso} . " " . $occref->{species_name};

			my $rowString = "<tr>";
			my $tempi = $i + 1;
			$rowString .= "<td><b>Row $tempi</b>: ";

		# standard report
			if ( $return != $DUPLICATE ) {
				$rowString .= $occname . " in collection " . $retrievedRow[6] . " reidentified as " . $reidname;
				if ( $retrievedRow[5] )	{
					$rowString .= "</td></tr>\n<tr><td>&nbsp;&nbsp;&nsbp;Comments: " . $retrievedRow[5];
				}
		# handle duplicate rows
			} else	{
				$rowString .= "<i>skipped</i> because it is a duplicate";
			}

			$rowString .= "<p></td></tr><tr>";
			push(@submittedRow, $rowString);
		}

	foreach $submittedRow (@submittedRow)	{
  		print $submittedRow;
   	}
   	print "</table><p>\n";
	print "<p align=center><b><a href=\"$exec_url?action=displayReIDCollsAndOccsSearchForm\">Reidentify more occurrences</a></b></p>\n";

	print stdIncludes("std_page_bottom");
}

sub displayProjectStatusPage
{
	  print stdIncludes( "std_page_top" );
	  print $hbo->populateHTML('project_status_page');
	  print stdIncludes("std_page_bottom");
}

# Marks the most_recent field in the reidentifications table to YES for the most recent reid for
# an occurrence, and marks all not-most-recent to NO.  Needed for collections search for Map and such
# PS 8/15/2005
sub setMostRecentReID {
    my $dbt = shift;
    my $dbh = $dbt->dbh;
    my $occurrence_no = shift;

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

sub updateRecord {

	# WARNING: guests never should get to this point anyway, but seem to do
	#  so by some combination of logging in, getting to a submit form, and
	#  going to the public page and therefore logging out, but then hitting
	#  the back button and getting back to the submit form
	# Have to be logged in
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first." );
		exit;
	}

	my $tableName = shift;
	my $idName = shift;
	my $id = shift;

	my $nowString = now();
	$q->param(modified=>"$nowString") unless $q->param('modified');
	
	# Get the database metadata
	$sql = "SELECT * FROM $tableName WHERE reference_no=0";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my @fieldNames = @{$sth->{NAME}};			# Field names
	my @fieldTypes = @{$sth->{mysql_is_num}};	# Field types to know if quotes are needed
	my @fieldTypeCodes = @{$sth->{mysql_type}};
	# Get the null constraints
	my @nullables = @{$sth->{NULLABLE}};
	$sth->finish();

	# Set a few defaults
	$q->param(modifier => $s->get("enterer"));			# This is an absolute
	# Set the pubtitle to the pull-down pubtitle unless it's set in the form
	$q->param(pubtitle => $q->param("pubtitle_pulldown")) unless $q->param("pubtitle");

    $q->param('modifier_no'=>$s->get('enterer_no'));

	my @updates;
	my @fields;
	my @vals;
	my $fieldCount = -1;
	foreach my $i ( 0..$#fieldNames ) {
		my $fieldName = $fieldNames[$i];
		$fieldCount++;
		# Skip the ID field
		next if $fieldName eq $idName;

		# Skip fields that aren't defined in the form
		next unless defined $q->param($fieldName);

		# Get the value from the form
		my $val = $q->param($fieldName);

		# If this is an enum, separate with commas
		my @formVals = $q->param($fieldName);
		if ( $fieldTypeCodes[$fieldCount] == 254) {
			#print "<b>Here: $fieldName: " . join(',', @formVals) . "</b><br><br>";
			$val = join(',', @formVals) if @formVals > 1;
		}

		# Add quotes if this is character data
		$val = $dbh->quote($val) unless $fieldTypes[$fieldCount];
			if ( $val !~ /\w/ )	{
				if ( $nullables[$i] )	{
						$val = "NULL";
				} else	{
						$val = "''";
				}
			}
#		$val = "''" unless $val =~ /\w/;
#		$val = 'NULL' unless $val =~ /\w/;

		push(@updates, "$fieldName=$val");
	}

	# Update the record
	my $updateString = "UPDATE $tableName SET ";
	$updateString .= join(',', @updates) . " WHERE $idName=$id";
	$updateString =~ s/\s+/ /gms;
	dbg($updateString);

	# Trying to find why the modifier is sometimes coming through 
	# blank.  This should stop it.
	if ( $updateString !~ /modifier/ ) { htmlError( "modifier not specified" ); }

	$dbh->do( $updateString ) || die ( "$updateString<HR>$!" );
  
	return $id;
}

# inserts a record
# Returns:
#	0 = failed
#	1 = success
#	2 = duplicate found
sub insertRecord {

	# WARNING: guests never should get to this point anyway, but seem to do
	#  so by some combination of logging in, getting to a submit form, and
	#  going to the public page and therefore logging out, but then hitting
	#  the back button and getting back to the submit form
	# Have to be logged in
	if (!$s->isDBMember()) {
		displayLoginPage( "Please log in first." );
		exit;
	}

	my $tableName = shift;
	my $idName = shift;
	my $primary_key = shift;
	my $matchlimit = shift;
	my $searchField = shift;
	my $fields_ref = shift;
	my $vals_ref = shift;

	my @fields;
	my @vals;

	# This 'unless' wraps most of the method. If we have data in fields_ref
	# and vals_ref, we're re-entering this method from processCheckNearMatch
	# after finding a potential conflict entering a new record, and the 
	# user said to go ahead and add the reference anyway.  See the INSERT, 
	# below, for final details...
	dbg( "fields_ref:$fields_ref,vals_ref:$vals_ref<br>");
	unless($fields_ref && $vals_ref){
		dbg("insertRecord: first pass<br>");

		my $searchVal = $q->param($searchField);

		# Created data, unless specified.
		my $nowString = now();
		$q->param(created=>"$nowString") unless $q->param('created');

		# Get the database metadata
		$sql = "SELECT * FROM $tableName WHERE $idName=0";
		dbg( "$sql<HR>" );
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();

		my @fieldNames = @{$sth->{NAME}};
		my @fieldTypes = @{$sth->{mysql_is_num}};	# To see which fields need quotes
		my @fieldTypeCodes = @{$sth->{mysql_type}};
		# Get the null constraints
		my @nullables = @{$sth->{NULLABLE}};
		$sth->finish();

		# Set a few defaults
		$q->param('enterer'       => $s->get('enterer'));			# This is an absolute
        $q->param('enterer_no'    => $s->get('enterer_no'));
		$q->param('authorizer'    => $s->get('authorizer'));		# This is an absolute
        $q->param('authorizer_no' => $s->get('authorizer_no'));
		# Set the pubtitle to the pull-down pubtitle unless it's set in the form
		$q->param(pubtitle => $q->param('pubtitle_pulldown')) unless $q->param("pubtitle");

		my $fieldCount = 0;
		# Loop over all database fields
		foreach my $i ( 0..$#fieldNames ) {
			my $fieldName = $fieldNames[$i];
			my $val;

			if ( defined $q->param($fieldName) ) {

				$val = $q->param($fieldName);

				push(@fields, $fieldName);
				
				# Set: separate with commas
				my @formVals = $q->param($fieldName);
				# 254 is the code for both sets and enums, and this should NOT
				# be done for enums!!!
				if ( $fieldTypeCodes[$fieldCount] == 254 ) {
					my $numSetValues = @formVals;
					if ( $numSetValues ) {
						my @valid = ();
						foreach my $item (@formVals){
							if($item ne ""){
								push(@valid, $item);
							}
						}
						$val = join(',', @valid);
					}
				}
		  
				# Add quotes if this is character data
				$val = $dbh->quote($val) unless $fieldTypes[$fieldCount];
	#dbg ( "fn:[$fieldName] val:[$val] type:[".$fieldTypes[$fieldCount]."] tc:[".$fieldTypeCodes[$fieldCount]."]" );
				if ( $val !~ /\w/ )	{
					if ( $nullables[$i] )	{
							$val = "NULL";
					} else	{
							$val = "''";
					}
				}
				
				push ( @vals, $val );
			}
			$fieldCount++;
		}
		dbg("insert VALS: @vals<br>");

		# Check for a duplicate and bomb if one is found
		my $return = checkDuplicates( $idName, $primary_key, $tableName, \@fields, \@vals );
		if ( ! $return || $return == $DUPLICATE ) { return $return; }

		# Check for near matches
		# NOTE: the below method now handles matches by giving the user a choice
		# of 'cancel' or 'continue' if a match is found. See method for details.
		checkNearMatch( $matchlimit, $idName, $tableName, $searchField, $searchVal, \@fields, \@vals );

	} # END 'unless($fields_ref && $vals_ref)' - see top of method

	if($fields_ref && $vals_ref){
		dbg("insertRecord: reentry from checkNearMatch<br>");
		@fields = @{$fields_ref};
		@vals = @{$vals_ref};
	}
	dbg("fields: @fields<br>vals: @vals<br>");

	# Insert the record
	my $valstring = join ',',@vals;
	$sql = "INSERT INTO $tableName (" . join(',', @fields) . ") VALUES (" . $valstring . ")";
	$sql =~ s/\s+/ /gs;
	dbg("$sql<HR>");
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );

	# Retrieve and display the record
	my $recID = $dbh->{'mysql_insertid'};
	dbg("inserted record ID: $recID<br>");

	# Once again, deal with the re-entry scenario (checkNearMatch found some
	# matches, but the user chose to go ahead and enter the data anyway)
	if($fields_ref && $vals_ref && ($tableName eq "refs")){
		processNewRef($recID);
	}
	# NOTE: we can also be in a reentry scenario (via processCheckNearMatch)
	# with the taxonomy scripts (authorities or opinions tables) but the
	# taxonomy scripts handle the display when this method returns to them.

	$$primary_key = $recID;
	# record ID is returned so printTaxonomicOpinions can tell
	#  if an opinion is new
	return $recID;
}

## checkNearMatch
#
# 	Description:		Check for records that match at least some number of 
#						fields
#
#	Arguments:			$matchlimit		threshold of column matches to consider
#										whole record a 'match.'
#						$idName			name of primary key of table
#						$tableName		db table in which to look for matches
#						$searchField	table column on which to search
#						$searchVal		column value to search against
#						$fields			names of fields from form (from 
#										submission to insertRecord that called
#										this method).	
#						$vals			values from form (as above)
#
#	Returns:
##			
sub checkNearMatch {

	my $matchlimit = shift;
	my $idName = shift;
	my $tableName = shift;
	my $searchField = shift;
	my $searchVal = shift;
	my $fields = shift;
	my $vals = shift;

	# Escape quotes in names like O'Sullivan
	$searchVal =~ s/(\w+)'{1}/$1\\'/;

	my $sql = "SELECT * FROM $tableName WHERE " . $searchField;
	$sql .= "='" . $searchVal . "'";
	dbg("checkNearMatch SQL:$sql<br>");
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Look for matches in the returned rows
	my @complaints;
	while (my $rowRef = $sth->fetchrow_hashref())	{
		my %row = %{$rowRef};
		my $fieldMatches;
		for ( my $i=0; $i<$#{$fields}; $i++ ) {
			# Strip single quotes, which won't be in the database
			my $v = $$vals[$i];
			$v =~ s/'//g;
			if ( $$fields[$i] !~ /^authorizer/ &&
           		 $$fields[$i] !~ /^enterer/ && $$fields[$i] !~ /^modifier/ &&
				 $$fields[$i] ne "created" &&
           		 $$fields[$i] ne "modified" && $$fields[$i] ne "release_date" &&
				 $$fields[$i] ne "comments" ) {
				if ( $v eq $row{$$fields[$i]} && $v ne "")	{
					$fieldMatches++;
				}
			}
		}
		if ($fieldMatches >= $matchlimit)	{
			push @complaints,$row{$idName};
#			$complaint .= "$fieldMatches fields are the same in record $row{$idName}<p>\n";
		}
#print "$idName $row{$idName} $fieldMatches $matchlimit<p>\n";
	}

	if (@complaints)	{
		# Print out the possible matches
		my $warning = "Your new record may duplicate one of the following old ones.";
        print "<CENTER><H3><FONT COLOR='red'>Warning:</FONT> $warning</H3></CENTER>\n";                                                                         
		print "<table><tr><td>\n";
		# Figure out what fields to show
		my @display;
		# Just show the whole thing if this is a ref or occ
		@display = @{$sth->{NAME}};
		# Be more narrowminded if this is a coll
		if ($tableName eq "collections")	{
			@display = ("collection_no", "collection_name", "country", "state",
						"formation", "period_max");
		}
		$sth->finish();

		for $complaint (@complaints)	{
			my $sql = "SELECT * FROM $tableName WHERE " . $idName;
			$sql .= "='" . $complaint . "'";
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			my $rowRef = $sth->fetchrow_hashref();
			my %row = %{$rowRef};
			# Do some cleanup if this is a ref
			if ($tableName eq "refs")	{
				# If otherauthors is filled in, we do an et al.
				if ( $row{'otherauthors'} )	{
					$row{'author1last'} .= " et al.";
					$row{'author2init'} = '';
					$row{'author2last'} = '';
					$row{'otherauthors'} = '';
				}
				# If there is a second author...
				elsif ( $row{'author2last'} )	{
					$row{'author1last'} .= " and ";
				}
			}
			my @rowData;
			for my $d (@display)	{
				push @rowData,$row{$d};
			}
			if ($tableName eq "refs")	{
				print $hbo->populateHTML('reference_display_row', \@rowData, \@display);
			}
			elsif($tableName eq "opinions"){
				my $sql="SELECT taxon_name FROM authorities WHERE taxon_no=".
						$row{parent_no};
				my @results = @{$dbt->getData($sql)};
				print "<table><tr>";
				print "<td>$row{status} $results[0]->{taxon_name}: ".
					  "$row{author1last} $row{pubyr} ";

				$sql="SELECT p1.name as name1, p2.name as name2, ".
						"p3.name as name3 ".
						"FROM person as p1, person as p2, person as p3 WHERE ".
						"p1.person_no=$row{authorizer_no} ".
						"AND p2.person_no=$row{enterer_no} ".
						"AND p3.person_no=$row{modifier_no}";
				@results = @{$dbt->getData($sql)};

				print "<font class=\"tiny\">[".
					  "$results[0]->{name1}/$results[0]->{name2}/".
					  "$results[0]->{name3}]".
					  "</font></td>";
				print "</tr></table>";
			}
			elsif($tableName eq "authorities"){
				print "<table><tr>";
				print "<td> $row{taxon_name}: $row{author1last} $row{pubyr} ";

				my $sql="SELECT p1.name as name1, p2.name as name2, ".
						"p3.name as name3 ".
						"FROM person as p1, person as p2, person as p3 WHERE ".
						"p1.person_no=$row{authorizer_no} ".
						"AND p2.person_no=$row{enterer_no} ".
						"AND p3.person_no=$row{modifier_no}";
				my @results = @{$dbt->getData($sql)};

				print "<font class=\"tiny\">[".
					  "$results[0]->{name1}/$results[0]->{name2}/".
					  "$results[0]->{name3}]".
					  "</font></td>";
				print "</tr></table>";
			}
			$sth->finish();
		}
		print "</td></tr></table>\n";
	}

	if(scalar @complaints){
		my @fields = @{$fields};
		my $flat_fields = join(',',@fields);
		my @vals = @{$vals};
		my $flat_vals = join(',',@vals);
		$flat_vals =~ s/"/&quot;/g;
		print "<center><p><b>What would you like to do?</b></p></center>";
		print "<form method=POST action=$exec_url>";
		print "<input type=hidden name=\"action\" value=\"processCheckNearMatch\">";
		print "<input type=hidden name=\"tablename\" value=\"$tableName\">";
		print "<input type=hidden name=\"idname\" value=\"$idName\">";
		print "<input type=hidden name=\"searchField\" value=\"$searchField\">";
		print "<input type=hidden name=\"fields\" value=\"$flat_fields\">";
		print "<input type=hidden name=\"vals\" value=\"$flat_vals\">";
		print "<center><input type=submit name=\"whattodo\" value=\"Cancel\">&nbsp;";
		print "<input type=submit name=\"whattodo\" value=\"Continue\"></form>";
		if($tableName eq "refs"){
			print qq|<p><a href="$exec_url?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p></center><br>\n|;
		}
		print stdIncludes("std_page_bottom");

		# we don't want control to return to insertRecord() (which called this
		# method and will insert the record after control returns to it after
		# calling this method, thus potentially creating a duplicate record if
		# the user chooses to continue.
		# Terminate this server session, and wait for user's response.
		exit;
	}
}

## processCheckNearMatch
#
#	Description:		either calls insertRecord with existing data (from a
#						previous call to insertRecord) or cuts the process 
#						short and just provides links to continue down a
#						different path.
#
#	Arguments:
#
#	Returns:			
##
sub processCheckNearMatch{
	my $fields = $q->param('fields');
	my @fields = split(',',$fields);
	my $vals = $q->param('vals');
	my @vals = split(',',$vals);
	my $what_to_do = $q->param('whattodo');
	my $table_name = $q->param('tablename');
	my $idName = $q->param('idName');
	my $searchField = $q->param('searchField');

	if($what_to_do eq 'Continue'){
		# these are mostly dummy vars, except tablename and the last two.
		insertRecord($table_name, $idName, 0, 5, $searchField, \@fields, \@vals);
	}
	else{
		print stdIncludes("std_page_top");
		print "<center><h3>Record addition canceled</h3>";
		if($table_name eq "refs"){
			print qq|<p><a href="$exec_url?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p></center><br>\n|;
		}
		elsif($table_name eq "opinions" || $table_name eq "authorities"){
			print qq|<p><a href="$exec_url?action=displayAuthorityTaxonSearchForm"><b>Add more taxonomic information.</b></a></p></center><br>\n|;
		}
		print stdIncludes("std_page_bottom");
	}
}

# Check for duplicates before inserting a record
sub checkDuplicates {
	my $idName = shift;
	my $primary_key = shift;
	my $tableName = shift;
	my $fields = shift;
	my $vals = shift;

	$sql = "";
	for (my $i=0; $i<$#{$fields}; $i++ ) {
		# The primary key isn't known until after the insert.
		# The created date would be different by a few seconds.
		# Also the release_date
		# Also the "modified" date! Added by JA 12.6.02
		# Also "comments" (relevant to occs and reids) JA 28.6.02
		if ( $$fields[$i] !~ /^(:?$idName|created|modified|release_date|comments|authorizer|enterer|modifier|authorizer_no|enterer_no|modifier_no)$/ ) {
			# Tack on the field and value; take care of NULLs.
			if ( $$vals[$i] eq "NULL" ) {
				$sql .= $$fields[$i]." IS NULL";
			} else {
				$sql .= $$fields[$i]." = ".$$vals[$i];
			}
			if ( $sql && $i != $#{$fields} - 1 ) { $sql .= " AND "};
		}
	}
	$sql =~ s/ AND $//;
	$sql = "SELECT $idName FROM $tableName WHERE ".$sql;
	dbg("checkDuplicates SQL:$sql<HR>");

	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	if ( $sth->rows ) {
		# Duplicate entry found!
		my @row = $sth->fetchrow_array();
		$$primary_key = $row[0];
		return $DUPLICATE;
	}
	1;
}

sub dbg {
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

# JA 25.2.02
# Given a single row of data, send it back as a table row
sub makeRefString	{
	my $drow = shift;
	my $selectable = shift;
	my $row = shift;
	my $rowcount = shift;
	my $supress_colls = shift;

	my $retRefString  = "";
	my $bibRef = BiblioRef->new( $drow );

	$retRefString = $bibRef->toString( $selectable, $row, $rowcount );
	if($supress_colls){
		return $retRefString;
	}
	my $tempRefNo = $bibRef->get("_reference_no");
	
	# getCollsWithRef creates a new <tr> for the collections.
	$retRefString .= getCollsWithRef($tempRefNo, $row, $rowcount);

	return $retRefString;
}

# JA 23.2.02
sub getCollsWithRef	{
	my $tempRefNo = shift;
	my $row = shift;
	my $rowcount = shift;
	my $retString = "";

    # Set up output
	if ($row % 2 != 0 && $rowcount > 1)	{
		$retString .= "<tr class='darkList'>\n";
	} else	{
		$retString .= "<tr>\n";
    }    
	$retString .= "<td colspan=\"3\">&nbsp;</td>\n<td><font size=-1>\n";

    
    # Handle Authorities
    $sql = "SELECT count(*) c FROM authorities WHERE reference_no=$tempRefNo";
    my $authority_count = ${$dbt->getData($sql)}[0]->{'c'};

    if ($authority_count) {
        $retString .= qq|<b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$tempRefNo">|;
        my $plural = ($authority_count == 1) ? "" : "s";
        $retString .= "$authority_count taxonomic name$plural";
        $retString .= qq|</a></b>, |;
    }
    
    # Handle Opinions
    $sql = "SELECT count(*) c FROM opinions WHERE reference_no=$tempRefNo";
    my $opinion_count = ${$dbt->getData($sql)}[0]->{'c'};

    if ($opinion_count) {
        $retString .= qq|<b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$tempRefNo">|;
        if ($opinion_count) {
            my $plural = ($opinion_count == 1) ? "" : "s";
            $retString .= "$opinion_count taxonomic opinion$plural";
        }
        $retString .= qq|</a></b>, |;
    }      

    # Handle Collections
	# make sure displayed collections are readable by this person JA 24.6.02


	# primary ref in first SELECT, secondary refs in second SELECT
    # the '1 is primary' and '0 is_primary' is a cool trick - alias the value 1 or 0 to column is_primary
    # any primary referneces will have a  virtual column called "is_primary" set to 1, and secondaries will not have it.  PS 04/29/2005
    $sql = "(SELECT collection_no,authorizer_no,collection_name,access_level,research_group,release_date,DATE_FORMAT(release_date, '%Y%m%d') rd_short, 1 is_primary FROM collections where reference_no=$tempRefNo)";
    $sql .= " UNION ";
    $sql .= "(SELECT c.collection_no, c.authorizer_no, c.collection_name, c.access_level, c.research_group, release_date, DATE_FORMAT(c.release_date,'%Y%m%d') rd_short, 0 is_primary FROM collections c, secondary_refs s WHERE c.collection_no = s.collection_no AND s.reference_no=$tempRefNo) ORDER BY collection_no";

    $sth = $dbh->prepare($sql);
    $sth->execute();

	my $p = Permissions->new($s,$dbt);
    $results = [];
	if($sth->rows) {
	    my $limit = 999;
	    my $ofRows = 0;
        $p->getReadRows($sth,$results,$limit,\$ofRows);
    }

    my $collection_count = scalar(@$results);
    if ($collection_count == 0) {
        $retString .= "No collections";
    } else {
        my $plural = ($collection_count == 1) ? "" : "s";
        $retString .= qq|<b><a href="bridge.pl?action=displayCollResults&type=view&wild=N&reference_no=$tempRefNo">$collection_count collection$plural</a> </b> ( |;
        foreach $row (@$results) {
			my $coll_link = qq|<a href="bridge.pl?action=displayCollectionDetails&collection_no=$row->{collection_no}">$row->{collection_no}</a>|;
            if ($row->{'is_primary'}) {
                $coll_link = "<b>".$coll_link."</b>";
            }
            $retString .= $coll_link . " ";
        }
        $retString .= ")";
    } 
    

    $retString .= "</font></td></tr>";

	return $retString;
}



# Greg Ederer function that is our standard method for querying the refs table
# completely messed up by Poling 3.04 and restored by JA 10.4.04
sub RefQuery {
	my $q = shift;
    
    my $csv = Text::CSV_XS->new();
    my $sth;

	# Use current reference button?
	if ($q->param('use_current')) {
		# if so, grab the current reference info.
		my $sql = "SELECT p1.name authorizer, p2.name enterer, p3.name modifier, r.reference_no, r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.publication_type,r.comments,r.language,r.created,r.modified FROM refs r".
                  " LEFT JOIN person p1 ON p1.person_no=r.authorizer_no ".
                  " LEFT JOIN person p2 ON p2.person_no=r.enterer_no".
                  " LEFT JOIN person p3 ON p3.person_no=r.modifier_no".
                  " WHERE reference_no = ?";
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute($s->get('reference_no'));
		return $sth;
	}

	# do these really need to be globals?  rjp 12/29/03
	# I'm changing them to locals, rjp, 3/15/2004.
	my $name = $q->param('name');
	my $pubyr = $q->param('year');
	my $reftitle = $q->param('reftitle');
	my $pubtitle = $q->param('pubtitle');
	my $refno = $q->param('reference_no');

	# build a string that will tell the user what they asked for
	my $refsearchstring = qq|$name| if $name;
	$refsearchstring .= qq| $pubyr| if $pubyr;
	$refsearchstring .= qq| $reftitle| if $reftitle;
	$refsearchstring .= qq| $pubtitle| if $pubtitle;
	$refsearchstring .= qq| reference $refno| if $refno;
	$refsearchstring =~ s/^ //;
	my $overlimit;

	if ( $refsearchstring ) { $refsearchstring = "for '$refsearchstring' "; }

	if ( $refsearchstring ne "" || $q->param('authorizer_reversed') || $q->param('enterer_reversed') || $q->param('project_name') ) {
        my $tables = "refs r, person p1, person p2".
                     " LEFT JOIN person p3 ON p3.person_no=r.modifier_no";
        # This exact order is very important due to work around with inflexible earlier code
        my $from = "p1.name authorizer, p2.name enterer, p3.name modifier, r.reference_no, r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.publication_type,r.comments,r.language,r.created,r.modified";
        my @where = ("r.authorizer_no=p1.person_no","r.enterer_no=p2.person_no");

		if ($name) {
			push @where,"(r.author1last LIKE ".$dbh->quote('%'.$name.'%').
                        " OR r.author1last LIKE ".$dbh->quote('%'.$name.'%').
                        " OR r.otherauthors LIKE ".$dbh->quote('%'.$name.'%').')';
		}
        push @where, "r.pubyr LIKE ".$dbh->quote($pubyr) if ($pubyr);
        push @where, "r.reftitle LIKE ".$dbh->quote('%'.$reftitle.'%') if ($reftitle);
        push @where, "r.pubtitle LIKE ".$dbh->quote('%'.$pubtitle.'%') if ($pubtitle);
        push @where, "r.reference_no=".int($refno) if ($refno);
        push @where, "FIND_IN_SET(".$dbh->quote($q->param('project_name')).",r.project_name)" if ($q->param('project_name'));
        
		if ( $q->param('authorizer_reversed')) {
            push @where, "p1.reversed_name LIKE ".$dbh->quote($q->param('authorizer_reversed')) if ($q->param('authorizer_reversed'));
		}
		if ( $q->param('enterer_reversed')) {
            push @where, "p2.reversed_name LIKE ".$dbh->quote($q->param('enterer_reversed')) if ($q->param('enterer_reversed'));
		}
	
        my $sql = "SELECT $from FROM $tables WHERE ".join(" AND ",@where);
    
		my $orderBy = " ORDER BY ";
		my $refsortby = $q->param('refsortby');

		# order by clause is mandatory
		if ($refsortby eq 'year') {
			$orderBy .= "r.pubyr, ";
		} elsif ($refsortby eq 'publication') {
			$orderBy .= "r.pubtitle, ";
		} elsif ($refsortby eq 'authorizer') {
			$orderBy .= "p1.reversed_name, ";
		} elsif ($refsortby eq 'enterer') {
			$orderBy .= "p2.reversed_name, ";
		} elsif ($refsortby eq 'entry date') {
			$orderBy .= "r.reference_no, ";
		}
		
		if ($refsortby)	{
			$orderBy .= "r.author1last, r.author1init, r.author2last, r.pubyr";
		}

		# only append the ORDER clause if something is in it,
		#  which we know because it doesn't end with "BY "
		if ( $orderBy !~ /BY $/ )	{
			$orderBy =~ s/, $//;
			$sql .= $orderBy;
		}

        dbg("RefQuery SQL".$sql);

		# Execute the ref query
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my @refFields = @{$sth->{NAME}};
		my @rows = @{$sth->fetchall_arrayref()};
		
		# If too many refs were found, set a limit
		if (@rows > 30)	{
			$overlimit = @rows;
			$q->param('refsSeen' => 30 + $q->param('refsSeen') );
			$sql = $sql . " LIMIT " .$q->param('refsSeen');
		}
	
		
		# Dump the refs to a flat file JA 1.7.02
		my $authname = $s->get('authorizer');
		$authname =~ s/\. //;
		open REFOUTPUT,">$HTML_DIR/$OUTPUT_DIR/$authname.refs";

        if ($csv->combine(@refFields)) {
            print REFOUTPUT $csv->string(),"\n";
        }
		for my $rowRef (@rows)	{
			if ($csv->combine(@$rowRef))	{
				print REFOUTPUT $csv->string(),"\n";
			}
		}
		close REFOUTPUT;
		
		# Rerun the query
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
	} else {
		print stdIncludes("std_page_top");
		print "<center><h4>Sorry! You can't do a search without filling in at least one field</h4>\n";
		print "<p><a href='$exec_url?action=displaySearchRefs&type=".$q->param("type")."'><b>Do another search</b></a></p></center>\n";
		print stdIncludes("std_page_bottom");
		exit(0);
	}
	
	return ($sth, $overlimit,$refsearchstring);
}


# This only shown for internal errors
sub htmlError {
	my $message = shift;

	# print $q->header( -type => "text/html" );
    print stdIncludes( "std_page_top" );
	print $message;
    print stdIncludes("std_page_bottom");
	exit 1;
}

# Build the reference ( at the bottom of the page )
sub buildReference {
	my $reference_no = shift;
	my $outputtype = shift; # possible values list/bottom
	my $reference = "";

	if ( $reference_no ) {
		$sql = "SELECT reference_no,author1init,author1last,author2init,author2last,otherauthors,pubyr FROM refs where reference_no = $reference_no";
		#dbg ( "$sql" );
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		if ( $sth->rows ) {
			my $rs = $sth->fetchrow_hashref ( );

			# First author always shown
			if ($outputtype eq "bottom")	{
				$reference = $reference_no." (";
			}
			elsif ($outputtype eq "list")	{
				$reference = "<a href=bridge.pl?action=displayRefResults&type=view&reference_no=$reference_no>";
			}
			$reference .= $rs->{author1last};

			AUTHOR: {

				# If otherauthors is filled in, we do an et al.
				if ( $rs->{otherauthors} )	{
					$reference .= " et al."; last;
				}
				# If there is a second author...
				elsif ( $rs->{author2last} )	{
					$reference .= " and ".$rs->{author2last};
					last;
				}
				# Otherwise, it is just the author, so do nothing extra
			}
			$reference .= " ".$rs->{pubyr};
			if ($outputtype eq "bottom")	{
				$reference .= ")";
			}
			elsif ($outputtype eq "list")	{
				$reference =~ s/ /&nbsp;/g;
				$reference =~ s/<a&nbsp;href/<a href/g;
				$reference .= "</a>";
			}
		}	
	} else {
		$reference = "none";
	}

	return $reference;
}

# ------------------------ #
# Confidence Intervals JSM #
# ------------------------ #

sub displaySearchSectionResults{
    print stdIncludes("std_page_top");
    Confidence::displaySearchSectionResults($q, $s, $dbt,$hbo);
    print stdIncludes("std_page_bottom");
}

sub displaySearchSectionForm{
    print stdIncludes("std_page_top");
    Confidence::displaySearchSectionForm($q, $s, $dbt,$hbo);
    print stdIncludes("std_page_bottom");
}

sub displayTaxaIntervalsForm{
    print stdIncludes("std_page_top");
    Confidence::displayTaxaIntervalsForm($q, $s, $dbt,$hbo);
    print stdIncludes("std_page_bottom");
}

sub displayTaxaIntervalsResults{
    print stdIncludes("std_page_top");
    Confidence::displayTaxaIntervalsResults($q, $s, $dbt,$hbo);
    print stdIncludes("std_page_bottom");
}

sub buildListForm {
    print stdIncludes("std_page_top");
    Confidence::buildList($q, $s, $dbt,$hbo);
    print stdIncludes("std_page_bottom");
}

sub displayStratTaxaForm{
    print stdIncludes("std_page_top");
    Confidence::displayStratTaxa($q, $s, $dbt);
    print stdIncludes("std_page_bottom");
}

sub showOptionsForm {
	print stdIncludes("std_page_top");
	Confidence::optionsForm($q, $s, $dbt);
	print stdIncludes("std_page_bottom");
}

sub calculateTaxaInterval {
	print stdIncludes("std_page_top");
	Confidence::calculateTaxaInterval($q, $s, $dbt);
	print stdIncludes("std_page_bottom");
}

sub calculateStratInterval {
	print stdIncludes("std_page_top");
	Confidence::calculateStratInterval($q, $s, $dbt);
	print stdIncludes("std_page_bottom");
}

# Displays taxonomic opinions and names associated with a reference_no
# PS 10/25/2004
sub displayTaxonomicNamesAndOpinions {
    print stdIncludes( "std_page_top" );
    my $ref = Reference->new($dbt,$q->param('reference_no'));
    if ($ref) {
        my $html = $ref->formatAsHTML();
        $html =~ s/<b>\d+<\/b>//g; #Remove the reference_no
        print "<center><h3>Showing taxonomic names and opinions from reference: ".$html."</h3></center><br>";

        $q->param('goal'=>'authority');
        processTaxonSearch($dbh, $dbt, $hbo, $q, $s, $exec_url);
        Opinion::displayOpinionChoiceForm($dbt,$s,$q);
    } else {
        print "<div align=\"center\" class=\"errorMessage\"><h3>An error has occurred.  No valid reference supplied</h3></div>\n";
    }
    print stdIncludes("std_page_bottom");
}
# check for the presence of the nefarious V.J. Gupta or M.M. Imam
sub checkFraud{
    dbg("checkFraud called". $q->param('author1last'));

    if ($q->param('reftitle') =~ /DATA NOT ENTERED: SEE (.*) FOR DETAILS/) {
        dbg("found gupta/imam bypassed by finidng data not entered");
        return 0;
    }
    
    if ($q->param('author1init') =~ /V/i &&
        $q->param('author1last') =~ /^Gupta/i)  {
        dbg("found gupta in author1");    
        return 'Gupta';
    }
    if ($q->param('author1init') =~ /M/i &&
        $q->param('author1last') =~ /^Imam/i)  {
        dbg("found imam in author1");    
        return 'Imam';
    }
    if ($q->param('otherauthors') =~ /V[J. ]+Gupta/i) {
        dbg("found gupta in other authors");    
        return 'Gupta';
    }
    if ($q->param('otherauthors') =~ /M[M. ]+Imam/i) {
        dbg("found imam in other authors");    
        return 'Imam';
    }
    if ($q->param('author2init') =~ /V/i &&
        $q->param('author2last') =~ /^Gupta/i)  {
        dbg("found gupta in author2");    
        return 'Gupta';
    }
    if ($q->param('author2init') =~ /M/i &&
        $q->param('author2last') =~ /^Imam/i)  {
        dbg("found imam in author2");    
        return 'Imam';
    }
    return 0;
}

