#!/usr/bin/perl
use CGI;
use CGI::Carp qw(fatalsToBrowser);
use HTMLBuilder;
use DBI;
use Session;
use Class::Date qw(date localdate gmdate now);
use DataRow;
use Locale::Country;
use Locale::SubCountry;
use AuthorNames;
use BiblioRef;
use FileHandle;
use Map;
use Download;
use Report;
use Curve;
use Permissions;
use PBDBUtil;
use TaxonInfo;
use DBTransactionManager;

require "connection.pl";	# Contains our database connection info

my $DEBUG = 0;				# Shows debug information regarding the page
							#   if set to 1

my $DUPLICATE = 2;

# A few declarations
my $sql="";					# Any SQL string
my $return="";				# Generic return value
my $rs;						# Generic recordset
my $HOST_URL = $ENV{BRIDGE_HOST_URL};
my $BRIDGE_HOME = $HOST_URL . "/cgi-bin/bridge.pl";
my $TEMPLATE_DIR = "./templates";
my $GUEST_TEMPLATE_DIR = "./guest_templates";
my $HTML_DIR = $ENV{BRIDGE_HTML_DIR};
my $OUTPUT_DIR = "public/data";

# Make a few objects
my $q = CGI->new();
my $s = Session->new();
$csv = Text::CSV_XS->new();

# Get the URL pointing to this executable
# WARNING (JA 13.6.02): must do this before making the HTMLBuilder object!
my $exec_url = $q->url();

# Make the HTMLBuilder object
my $hbo = HTMLBuilder->new( $TEMPLATE_DIR, $dbh, $exec_url );

# Figure out the action
my $action = $q->param("action");
$action = "displayMenuPage" unless ( $action );
# need to know (below) if we did a processLogin and then changed the action
my $old_action = "";

# Get the database connection
my $dbh = DBI->connect("DBI:mysql:database=$db;host=$host", $user, $password, {RaiseError => 1});

# Make a Global Transaction Manager object
my $dbt = DBTransactionManager->new($dbh, $s);

# Need to do this before printing anything else out, if debugging.
#print $q->header('text/html') if $DEBUG;

# Logout?
# Run before debugging information
if ( $action eq "logout" ) { &logout(); }

# Send Password must be run before login
if ( $action eq "sendPassword" ) { 
	&sendPassword();
	print $q->redirect( -url=>$BRIDGE_HOME );
	exit;
}

# Login?
LOGIN: {
	# Display Login page
	if($action eq "displayLogin"){
		displayLoginPage();
		last;
	}

	# Process Login
	if($action eq "processLogin"){
		$cookie = $s->processLogin ( $dbh, $q ); 
		if($cookie){
			my $cf = CookieFactory->new();
			# The following two cookies are for setting the select lists
			# on the login page.
			my $cookieEnterer = $cf->buildCookie("enterer",
												 $q->param("enterer"));
			my $cookieAuthorizer = $cf->buildCookie("authorizer",
													$q->param("authorizer"));
			print $q->header(-type => "text/html", 
							 -cookie => [$cookie, 
										 $cookieEnterer, $cookieAuthorizer],
							 -expires =>"now" );

			# Destination
			if($q->param("destination") ne ""){
				$action = $q->param("destination");
			}
			if($action eq "processLogin"){
				$action = "displayMenuPage";
				$old_action = "processLogin";
			}
		}
		else{
			# failed login:  (bad password, etc)
			$action = "displayHomePage";
			$q->param("user" => "Guest");
			$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbh, $exec_url );
		}
		last; 
	}

	# Guest page? 
	if($q->param("user") eq "Guest"){
		# Change the HTMLBuilder object
		$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbh, $exec_url );
		last;
	}

	# Validate User
	my $temp_cookie = $q->cookie('session_id');
	$cookie = $s->validateUser($dbh, $q->cookie('session_id'));
	if(!$cookie){
		if($q->param("user") eq "Contributor"){
			displayLoginPage();
		}
		else{
			$hbo = HTMLBuilder->new( $GUEST_TEMPLATE_DIR, $dbh, $exec_url );
		}
	}
}

if(!$DEBUG){
	# The right combination will allow me to conditionally set the DEBUG flag
	if($s->get("enterer") eq "J. Sepkoski" && 
									$s->get("authorizer") eq "J. Alroy" ) {
		$DEBUG = 1;
	}
}

# Record the date of the action in the person table JA 27/30.6.02
if ($s->get("enterer") ne "" && $s->get("enterer") ne "Guest")	{
	my $nowString = now();
	my $sql = "UPDATE person SET last_action='" . $nowString;
	$sql .= "' WHERE name='" . $s->get("enterer") . "'";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );
}

unless($action eq 'displayLogin' or $old_action eq 'processLogin'){
	print $q->header('text/html');
}

dbg("<p><font color='red' size='+1' face='arial'>You are in DEBUG mode!</font><br> Cookie [$cookie]<BR> Action [$action] DB [$db] Authorizer [".$s->get("authorizer")."] Enterer [".$s->get("enterer")."]<BR></p>");

dbg($q->Dump);

# ACTION
&$action;

# --------------------------------------- subroutines --------------------------------

# Logout
# Clears the SESSION_DATA table of this session.
sub logout {

	my $session_id = $q->cookie("session_id");

	if ( $session_id ) {
		$sql =	"DELETE FROM SESSION_DATA ".
		 		" WHERE session_id = '".$q->cookie("session_id")."' ";
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	}

	print $q->redirect( -url=>$BRIDGE_HOME."?user=Contributor" );
	exit;
}

# sendPassword
sub sendPassword {
	my $authorizer = $q->param("authorizer");

	if ( $authorizer ) {
		$sql =	"SELECT * ".
				"  FROM person ".
				" WHERE authorizer = '$authorizer' ";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		if ( $sth->rows ) {
			$rs = $sth->fetchrow_hashref ( );

			if ( $rs->{plaintext} ) {
				my $body = "\n\nThe password for ".$rs->{name}." is ".$rs->{plaintext}.".\n";
				my $subject = "Your password";
				&sendMessage ( $rs->{email}, $subject, $body );
			}
		}
		$sth->finish();
	}
}

# This is a poor way to do this.  In the future it should
# be replaced with something better.
sub sendMessage {

	my $to = shift;
	my $subject  = shift;
	my $body  = shift;
	my $sm = FileHandle->new();
	my $sendmail = "/usr/sbin/sendmail -t";
	my $from = "root\@flatpebble.nceas.ucsb.edu";

	open ( $sm, "| $sendmail") || die "Cannot open $sendmail: $!";

	print $sm "Subject: $subject\n";
	print $sm "To: $to\n";
	print $sm "From: $from\n";
	print $sm "Content-type: text/plain\n\n";
	print $sm $body;

	close ( $sm );
}

# Displays the login page
sub displayLoginPage {
	my $select = "";
	my $authorizer = $q->cookie("authorizer");
	my $enterer = $q->cookie("enterer");
	my $destination = $q->param("destination");
    my $html = $hbo->getTemplateString ('login_box');

	# Authorizer
	buildAuthorizerPulldown ( \$html, $authorizer, 1 );

	# Enterer
	buildEntererPulldown ( \$html, $enterer, 1 );

	# Set the destination
	$html =~ s/%%destination%%/$destination/;

	# Show the login page
	print $q->header( -type => "text/html", -Cache_Control=>'no-cache'); 
    print &stdIncludes ( "std_page_top" );
	print $html;
    print &stdIncludes ("std_page_bottom");
	exit;
}

# Display the preferences page JA 25.6.02
sub displayPreferencesPage	{
	my $select = "";
	my $authorizer = $q->cookie("authorizer");
	my $enterer = $q->cookie("enterer");
	my $destination = $q->param("destination");

	$s->enqueue( $dbh, "action=$destination" );

	my %pref = &getPreferences($enterer);

	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	# Populate the form
	my @fieldNames = @{$setFieldNames};
	push @fieldNames , @{$shownFormParts};
	for $f (@fieldNames)	{
		if ($pref{$f} ne "")	{
			push @rowData,$pref{$f};
		}
		else	{
			push @rowData,"";
		}
	}

	# Show the preferences entry page
    print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('preferences', \@rowData, \@fieldNames);
    print &stdIncludes ("std_page_bottom");
	exit;
}

# Get the current preferences JA 25.6.02
sub	getPreferences	{
	my $enterer = shift;
	my %pref;

	my $sql = "SELECT preferences FROM person WHERE name='";
	$sql .= $enterer . "'";

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @row = $sth->fetchrow_array();
	$sth->finish();
	my @prefvals = split / -:- /,$row[0];
	for $p (@prefvals)	{
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

	my %cleanSetFieldNames = ("blanks" => "blank occurrence rows",
		"research_group" => "research group",
		"latdeg" => "latitude", "lngdeg" => "longitude",
		"geogscale" => "geographic resolution",
		"period_max" => "period max", "epoch_max" => "epoch max",
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
		"coastlinecolor" => "coastline color",
		"borderlinecolor" => "international border color",
		"usalinecolor" => "USA state border color",
		"pointsize" => "point size",
		"pointshape" => "point shape",
		"dotcolor" => "point color",
		"dotborder" => "point borders" );
	my @setFieldNames = ("blanks", "research_group", "country", "state",
			"latdeg", "latdir", "lngdeg", "lngdir", "geogscale",
			"emlperiod_max", "period_max", "emlepoch_max", "epoch_max",
			"formation", "stratscale", "lithology1", "environment",
			"collection_type", "assembl_comps", "pres_mode", "coll_meth",
		# occurrence fields
			"species_name",
		# comments fields
			"geogcomments", "stratcomments", "lithdescript",
		# map form fields
			"mapsize", "projection", "maptime", "mapfocus",
			"mapscale", "mapresolution", "mapbgcolor", "crustcolor",
			"gridsize", "gridcolor", "coastlinecolor",
			"borderlinecolor", "usalinecolor",
			"pointsize", "pointshape",
			"dotcolor", "dotborder");
	for $fn (@setFieldNames)	{
		if ($cleanSetFieldNames{$fn} eq "")	{
			my $cleanFN = $fn;
			$cleanFN =~ s/_/ /g;
			$cleanSetFieldNames{$fn} = $cleanFN;
		}
	}
	@shownFormParts = ("collection_search", "taphonomy",
		"subgenera", "abundances", "plant_organs");
	return (\@setFieldNames,\%cleanSetFieldNames,\@shownFormParts);

}

# Set new preferences JA 25.6.02
sub setPreferences	{

    print &stdIncludes ( "std_page_top" );
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

	my $enterer = $q->cookie("enterer");
 	my $sql = "UPDATE person SET preferences='";
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
 # prepare the SQL to update the prefs
	for $i (0..$#{$setFieldNames})	{
		$f = ${$setFieldNames}[$i];
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
	if ($q->param("emlperiod_max"))	{
		$q->param(period_max => $q->param("emlperiod_max") . " " . $q->param("period_max") );
	}
	if ($q->param("emlepoch_max"))	{
		$q->param(epoch_max => $q->param("emlepoch_max") . " " . $q->param("epoch_max") );
	}

	print "<tr><td valign=\"top\" width=\"33%\">\n";
	print "<b>Displayed sections</b><br>\n";
	for $f (@{$shownFormParts})	{
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
	for $i (0..$#{$setFieldNames})	{
		$f = ${$setFieldNames}[$i];
		if ($q->param($f) && $f =~ /comm/)	{
			$commentsStored = 1;
		}
	}

	print "</td>\n<td valign=\"top\" width=\"33%\">\n";
	print "<b>Prefilled values</b><br>\n";
	for $i (0..$#{$setFieldNames})	{
		$f = ${$setFieldNames}[$i];
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
 	$sql .= "' WHERE name='";
 	$sql .= $enterer;
 	$sql .= "'";
 	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
 	$sth->execute();
 	$sth->finish();

	print "<p>\n<a href=\"$exec_url?action=displayPreferencesPage\"><b>Reset preferences</b></a></td></tr></table><p>\n";
	my %continue = $s->unqueue($dbh);
	if($continue{action}){
		print "<center><p>\n<a href=\"$exec_url?action=$continue{action}\"><b>Continue</b></a><p></center>\n";
	}
    print &stdIncludes ("std_page_bottom");
	exit;

}

# Given an html page, substitute the enterer data
sub buildEntererPulldown {
	my $html = shift;			# HTML page into which we substitute
	my $enterer = shift;		# Default value
	my $active = shift;

	# Get the active enterers
	$sql =	"SELECT name as enterer, reversed_name FROM person ";
	if ( $active ) { $sql .= " WHERE active = 1 "; }
	$sql .= " ORDER BY reversed_name";

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my $select = "<OPTION value=''>Select enterer...</OPTION>\n";
	while ( $rs = $sth->fetchrow_hashref ( ) ) {
		if ( $enterer eq $rs->{enterer} ) {
			$select .= "<OPTION value='".$rs->{enterer}."' selected>".$rs->{reversed_name}."<\/OPTION>\n";
		} else {
			$select .= "<OPTION value='".$rs->{enterer}."'>".$rs->{reversed_name}."<\/OPTION>\n";
		}
	}
	$$html =~ s/<select name="enterer">/$&\n$select/;
}

# Given an html page, substitute the authorizer data
sub buildAuthorizerPulldown {
	my $html = shift;			# HTML page into which we substitute
	my $authorizer = shift;		# Default value
	my $active = shift;

	# Get the active authorizers
	$sql =	"SELECT name as authorizer, reversed_name FROM person WHERE is_authorizer = 1 ";
	if ( $active ) { $sql .= " AND active = 1 "; }
	$sql .= " ORDER BY reversed_name";

	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my $select = "<OPTION value=''>Select authorizer...</OPTION>\n";
	while ( $rs = $sth->fetchrow_hashref ( ) ) {
		if ( $authorizer eq $rs->{authorizer} ) {
			$select .= "<OPTION value='".$rs->{authorizer}."' selected>".$rs->{reversed_name}."<\/OPTION>\n";
		} else {
			$select .= "<OPTION value='".$rs->{authorizer}."'>".$rs->{reversed_name}."<\/OPTION>\n";
		}
	}
	$$html =~ s/<select name="authorizer">/$&\n$select/;
}

sub displayMenuPage	{
	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue ( $dbh ); 
	} else {

		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue ( $dbh );
		if ( $queue{action} ) {
			# Set each parameter
			foreach my $parm ( %queue ) {
				$q->param ( $parm => $queue{$parm} );
			}
	
			# Run the command
			&{$queue{action}};
			exit;
		}
	}
	print &stdIncludes ("std_page_top");
	print $hbo->populateHTML('menu', \@rowData, \@fieldNames);
	print &stdIncludes ("std_page_bottom");

}

sub displayHomePage {

	# Clear Queue?  This is highest priority
	if ( $q->param("clear") ) {
		$s->clearQueue ( $dbh ); 
	} else {

		# QUEUE
		# See if there is something to do.  If so, do it first.
		my %queue = $s->unqueue ( $dbh );
		if ( $queue{action} ) {
			# Set each parameter
			foreach my $parm ( %queue ) {
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
	push ( @rowData,	$rs->{reference_total},
						$rs->{collection_total}, 
						$rs->{occurrence_total}, 
						$rs->{enterer_total}, '', '' );
	push ( @fieldNames,	"reference_total",
						"collection_total", 
						"occurrence_total", 
						"enterer_total", "main_menu", "login" );
	$sth->finish();


	print &stdIncludes ("std_page_top");
	print $hbo->populateHTML('home', \@rowData, \@fieldNames);
	print &stdIncludes ("std_page_bottom");
}

# Shows the form for requesting a map
sub displayMapForm {

	# List fields that should be preset
	my @fieldNames = ( 'research_group', 'country', 'period_max', 'lithology1', 'environment', 'mapsize', 'projection', 'maptime', 'mapfocus', 'mapscale', 'mapresolution', 'mapbgcolor', 'crustcolor', 'gridsize', 'gridcolor', 'gridposition', 'linethickness', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointsize', 'pointshape', 'dotcolor', 'dotborder', 'mapsearchfields2', 'pointsize2', 'pointshape2', 'dotcolor2', 'dotborder2', 'mapsearchfields3', 'pointsize3', 'pointshape3', 'dotcolor3', 'dotborder3', 'mapsearchfields4', 'pointsize4', 'pointshape4', 'dotcolor4', 'dotborder4' );
	# Set default values
	my @row = ( '', '', '', '', '', '100%', 'rectilinear', '0', 'Europe', 'X 1', 'medium', 'white', 'none', '30 degrees', 'gray', 'in back', 'medium', 'black', 'none', 'none', 'medium', 'circles', 'red', 'black', '', 'medium', 'squares', 'blue', 'black', '', 'medium', 'triangles', 'yellow', 'black', '', 'medium', 'diamonds', 'green', 'black' );
	
	# Read preferences if there are any JA 8.7.02
	%pref = &getPreferences($s->get('enterer'));
	# Get the enterer's preferences
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	for $p (@{$setFieldNames})	{
		if ($pref{$p} ne "")	{
			unshift @row,$pref{$p};
			unshift @fieldNames,$p;
		}
	}

	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML ('map_form', \@row, \@fieldNames, \@prefkeys);

	buildAuthorizerPulldown ( \$html );
	buildEntererPulldown ( \$html );

	my $authorizer = $s->get("authorizer");
	$html =~ s/%%authorizer%%/$authorizer/;
	my $enterer = $s->get("enterer");
	$html =~ s/%%enterer%%/$enterer/;

	# Spit out the HTML
	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('js_map_checkform');
	print $html;
	print &stdIncludes ("std_page_bottom");
}

sub displayMapResults {

	print &stdIncludes ( "std_page_top" );

	my $m = Map->new( $dbh, $q, $s, $dbt );
	$m->buildMap ( );

	print &stdIncludes ("std_page_bottom");
}

sub displayDownloadForm {
	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML( 'download_form', '', [ 'country' ] );
	print &stdIncludes ("std_page_bottom");
}

sub displayDownloadResults {

	print &stdIncludes ( "std_page_top" );

	my $m = Download->new( $dbh, $dbt, $q, $s );
	$m->buildDownload ( );

	print &stdIncludes ("std_page_bottom");
}

sub displayReportForm {
	print $hbo->populateHTML( 'report_form', '', [ 'research_group' ] );
}

sub displayReportResults {

	print &stdIncludes ( "std_page_top" );

	my $r = Report->new( $dbh, $q );
	$r->buildReport ( );

	print &stdIncludes ("std_page_bottom");
}

sub displayCurveForm {
    displayPage ('curve_form');
}

sub displayCurveResults {

	print &stdIncludes ( "std_page_top" );

	my $c = Curve->new( $q, $s );
	$c->buildCurve ( );

	print &stdIncludes ("std_page_bottom");
}

# Show a generic page
sub displayPage {
	my $page = shift;
	if ( ! $page ) { 
		# Try the parameters
		$page = $q->param("page"); 
		if ( ! $page ) {
			&htmlError ( "$0.displayPage(): Unknown page..." );
		}
	}

	# Spit out the HTML
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print &stdIncludes ( "std_page_top" );
	}
	print $hbo->populateHTML( $page );
	if ( $page !~ /\.eps$/ && $page !~ /\.gif$/ )	{
		print &stdIncludes ("std_page_bottom");
	}
}

# This is a wrapper to put the goodies into the standard page bottom
sub stdIncludes {

	my $page = shift;
	my $reference = &buildReference ( $s->get("reference_no"), "bottom" );
	my $enterer;

	ENTERER: {
		# Already was logged in
		if ( $s->get("enterer") ne "" ) { $enterer = $s->get("enterer"); last; }
		# Just logged in
		if ( $q->param("enterer") ne "" ) { $enterer = $q->param("enterer"); last; }
		# Not logged in
		if ( $q->cookie("enterer") ne "" ) { $enterer = $q->cookie("enterer"); last; }
		# Don't know
		$enterer = "none";
	}

	return $hbo->populateHTML (	$page, 
								[ $reference, $enterer ], 
								[ "%%reference%%", "%%enterer%%" ] );
}

# Shows the search form
sub displaySearchRefs {

	my $message = shift;			# Message telling them why they are here
	my $type = $q->param("type");

	my @row;
	my @fields = ( "action" );
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

	unshift @row,"";
	unshift @fields,"authorizer";
	unshift @row,"";
	unshift @fields,"project_name";

	print &stdIncludes ( "std_page_top" );
	my $html = "";

	unless($q->param('user') eq "Guest"){
		$html .= stdIncludes("js_pulldown_me");
	}

	$html .= $hbo->populateHTML("search_refs_form", \@row, \@fields);
	buildEntererPulldown ( \$html, $enterer, 1 );
	my $enterer = $s->get("enterer");
	$html =~ s/%%enterer%%/$enterer/;
	my $authorizer = $s->get("authorizer");
	$html =~ s/%%authorizer%%/$authorizer/;
	print $html;

	print &stdIncludes ("std_page_bottom");
}

# Print out the number of refs found by a ref search JA 26.7.02
sub describeRefResults	{

	my $numRows = shift;
	my $overlimit = shift;

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
						 "enterer", "project_name", "refsortby",
						 "refsSeen");
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

# This shows the actual references
sub displayRefResults {

	my $overlimit = &RefQuery();
  
	my @rows = @{$sth->fetchall_arrayref()};
	my $numRows = @rows;

	if ( $numRows == 1 ) {
		# Do the action, don't show results...

		# Set the reference_no
		$s->setReferenceNo ( $dbh, ${@rows[0]}[3] );		# Why isn't the primary key the first column?
		# print "reference_no is ".${@rows[0]}[3]."<BR>\n";


		# QUEUE
		my %queue = $s->unqueue ( $dbh );
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
			&{$action};									# Run the action
		}
		# otherwise, display a page showing the ref JA 10.6.02
		else	{
			print &stdIncludes ( "std_page_top" );
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
			if(${@rows[0]}[19]){
				print "<tr><td>Publication type:&nbsp;<i>".${@rows[0]}[19]."</i></font></td></tr>";
			}
			# Now the comments:
			if(${@rows[0]}[20]){
				print "<tr><td>Comments:&nbsp;<i>".${@rows[0]}[20]."</i></font></td></tr>";
			}
			# getCollsWithRef creates a new <tr> for the collections.
			my $refColls = getCollsWithRef(${@rows[0]}[3], $row, $rowcount);
			# remove the cells that cause this to line up since we're putting
			# it in a subtable.
			$refColls =~ s/^<tr>\n<td colspan=\"3\">&nbsp;<\/td>/<tr>/;
			print $refColls;
			print "</table>\n";
			print "</table><p>\n";
			print &stdIncludes ( "std_page_bottom" );
		}
#		if ( ! $action ) { $action = "displayMenuPage"; } # bad Tone code
		return;										# Out of here!
	}

	print &stdIncludes ( "std_page_top" );

	if ( $numRows ) {

		&describeRefResults($numRows,$overlimit);

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
		   		print &makeRefString( $drow, ( ! $s->guest( ) ), $row, $numRows );
			}
			$row++;
		}
		print "</table>\n";

		$sth->finish();

		print qq|<input type=submit value="Select reference">\n| if ( ! $s->guest( ) );

		&printGetRefsButton($numRows,$overlimit);

	} else {
		print "<center>\n<h3>Your search $refsearchstring produced no matches</h3>\n";
		print "<p>Please try again with fewer search terms.</p>\n</center>\n";
		print "<center>\n<p>";
	}

	print qq|<a href="$exec_url?action=displaySearchRefs&type=select"><b>Do another search</b></a></p></center><br>\n|;

	print &stdIncludes ("std_page_bottom");
}

sub displayRefResultsForAdd {

	my $overlimit = &RefQuery();

	my @rows = @{$sth->fetchall_arrayref()};
	my $numRows = @rows;
  
	if ( ! $numRows ) {
		# No matches?  Great!  Get them where the were going.
		displayRefAdd();
		exit;
	}

	print &stdIncludes ( "std_page_top" );

	&describeRefResults($numRows,$overlimit);

	print qq|<FORM method="POST" action="$exec_url"'>\n|;

	# This is view only... you may not select
	print qq|<input type=hidden name="action" value="displayRefAdd">\n|;

	# carry search terms over for populating form
	foreach my $s_param ("name","year","reftitle","project_name"){
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
		# This is view only... you may not select
    	print &makeRefString ( $drow, 0, $row, $numRows );
		$row++;
	}
	print "</table>\n";
	$sth->finish();

	&printGetRefsButton($numRows,$overlimit);

	print qq|<a href="$exec_url?action=displaySearchRefs&type=add"><b>Do another search</b></a></p>\n|;

	print qq|<p><input type=submit value="Add reference"></center>\n</p>\n|;

	print &stdIncludes ("std_page_bottom");
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
				$s->setReferenceNo ( $dbh, ${$values}[$i] );
			}
		}
	}
}

sub selectReference {

	$s->setReferenceNo ( $dbh, $q->param("reference_no") );
	displayMenuPage ( );
}

sub displayRefAdd
{
	my @fieldNames = (	"publication_type", 
						"authorizer",
						"enterer",
						"%%new_message%%" );
	my @row = ( "", 
				$s->get('authorizer'), 
				$s->get('enterer'), 
				"<p>If the reference is <b>new</b>, please fill out the following form.</p>" );

	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('js_reference_checkform');

	print qq|<FORM method="POST" action="$exec_url" onSubmit='return checkForm();'>\n|;
	print qq|<input type=hidden name="action" value="processNewRef">\n|;

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
	print $hbo->populateHTML("enter_ref_form", \@row, \@fieldNames);
	print &stdIncludes ("std_page_bottom");
}

#  * User (presumably has a ref that's not in the results, and)
#    submits completed enter ref form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processNewRef {
	my $reentry = shift;
	my $reference_no=0;
	my $return;

	print &stdIncludes ( "std_page_top" );
	dbg("processNewRef reentry:$reentry<br>");

	if($reentry){
		$reference_no = $reentry;
		$return = 1; # equivalent to 'success' from insertRecord
		dbg("reentry TRUE<br>");
	}
	else{
		dbg("reentry FALSE, calling insertRecord()<br>");
		$return = insertRecord('refs', 'reference_no', \$reference_no, '5', 'author1last' );
		if ( ! $return ) { return $return; }
	}

	print "<center><h3><font color='red'>Reference record ";
	if ( $return == $DUPLICATE ) {
   		print "already ";
	}
	print "added</font></h3><center>";

	# Set the reference_no
	$s->setReferenceNo ( $dbh, $reference_no );

    $sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
    dbg( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();

    my $rowref = $sth->fetchrow_arrayref();
    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($rowref, $md);
    $retVal = &makeRefString($drow);
    print '<table>' . $retVal . '</table>';

    print qq|<center><p><a href="$exec_url?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p>\n|;
	print qq|<p><a href="$exec_url?action=displaySearchColls&type=add"><b>Add a new collection</b></a></p></center>|;
    print &stdIncludes ("std_page_bottom");
}


#  * User submits completed refs search form
#  * System displays matching reference results
sub displaySelectRefForEditPage
{

	my $overlimit = &RefQuery();
	
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
		&displayRefEdit ();
		exit;
	}

	print &stdIncludes ( "std_page_top" );

	if ( $numRows > 0) {

		&describeRefResults($numRows,$overlimit);

		print qq|<form method="POST" action="$exec_url">\n|;
		print qq|<input type=hidden name="action" value="displayRefEdit">\n|;

		print "<table border=0 cellpadding=5 cellspacing=0>\n";
		my $matches;
		my $row = 1;
		foreach my $rowref (@rowrefs)
		{
			my $drow = DataRow->new($rowref, $md);
			my $selectable = 1 if ( $s->get('authorizer') eq $drow->getValue('authorizer') || $s->get('authorizer') eq 'J. Alroy');
			$retVal = &makeRefString ( $drow, $selectable, $row, $numRows );
			print $retVal;
			$matches++ if $selectable;
			$row++;
		}
		print "</table>";
		if ($matches > 0)	{
			print qq|<input type=submit value="Edit selected">\n|;
		}
		print "</form>";

		&printGetRefsButton($numRows,$overlimit);

	} else {
		print "<center><h3>Your search $refsearchstring produced no matches</h3>\n";
		print "<p>Please try again with fewer search terms.</p></center>\n";
		print "<center><p>";
	}

	print qq|<a href="$exec_url?action=displaySearchRefs&type=edit"><b>Search for another reference</b></a></p></center><br>\n|;

	print &stdIncludes ("std_page_bottom");
}

# Wrapper to displayRefEdit
sub editCurrentRef {
	my $reference_no = $s->get("reference_no");
	if ( $reference_no ) {
		$q->param("reference_no"=>$reference_no);
		displayRefEdit ( );
	} else {
		$q->param("type"=>"edit");
		&displaySearchRefs ( "Please choose a reference first" );
	}
}

# The reference_no must be passed in the params.
sub displayRefEdit
{
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=displayRefEdit" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	my $reference_no = $q->param('reference_no');
	if ( $reference_no ) {
		$s->setReferenceNo ( $dbh, $reference_no );
	} else {
		# Have to have one!
		$s->enqueue( $dbh, "action=displayRefEdit" );
		&displaySearchRefs ( "Please choose a reference first" );
		exit;
	}

	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('js_reference_checkform');

	$sql =	"SELECT * ".
			"  FROM refs ".
			" WHERE reference_no=$reference_no";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my @row = $sth->fetchrow_array();
	my @fieldNames = @{$sth->{NAME}};
	$sth->finish();

	# Tack on a few extras
	#push ( @fieldNames, 'authorizer', 'enterer', '%%new_message%%' );
	#push ( @row, $s->get('authorizer'), $s->get('enterer'), '');

    if($row[0] eq ""){
        $row[0] = $s->get('authorizer');
    }
    if($row[1] eq ""){
        $row[1] = $s->get('enterer');
    }

	print qq|<form method="POST" action="$exec_url" onSubmit='return checkForm();'>\n|;
	print qq|<input type=hidden name="action" value="processReferenceEditForm"\n|;
	print $hbo->populateHTML('enter_ref_form', \@row, \@fieldNames);

	print &stdIncludes ("std_page_bottom");
}

#  * User submits completed reference form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processReferenceEditForm {

	print &stdIncludes ( "std_page_top" );
	  
	$refID = updateRecord('refs', 'reference_no', $q->param('reference_no'));
    print "<center><h3><font color='red'>Reference record updated</font></h3></center>\n";
		
    $sql = "SELECT * FROM refs WHERE reference_no=$refID";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my $rowref = $sth->fetchrow_arrayref();
    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($rowref, $md);
    print '<table>' . &makeRefString($drow) . '</table>';
	# my $bibRef = BiblioRef->new($drow);
	# print '<table>' . $bibRef->toString() . '</table>';
		
    print qq|<center><p><a href="$exec_url?action=displaySearchRefs&type=edit"><b>Edit another reference</b></a></p></center><br>\n|;
	print &stdIncludes ("std_page_bottom");
}


sub displaySearchColls {

	# Get the type, passed or on queue
	my $type = $q->param("type");
	if ( ! $type ) {
		# QUEUE
		my %queue = $s->unqueue ( $dbh );
		$type = $queue{type};
	}

	# Have to have a reference #, unless we are just searching
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no && $type ne "view" ) {
		# Come back here... requeue our option
		$s->enqueue ( $dbh, "action=displaySearchColls&type=$type" );
		&displaySearchRefs ( "Please choose a reference first" );
		exit;
	}	

	# add				result list links view collection details
	# view				result list links view collection details
	# edit				result list links go directly to edit the collection
	# edit_occurrence	result list links go to edit occurrence page

	# Show the "search collections" form
	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('search_collections_form', [ '', '', '', '','' ], [ 'research_group', 'lithology1', 'lithology2', 'environment',$type ], \@prefkeys);
	buildAuthorizerPulldown ( \$html );
	buildEntererPulldown ( \$html );

	# Set the Enterer
	my $enterer = $s->get("enterer");
	$html =~ s/%%enterer%%/$enterer/;
	my $authorizer = $s->get("authorizer");
	$html =~ s/%%authorizer%%/$authorizer/;

	# Set the type
	$html =~ s/%%type%%/$type/;

	# Spit out the HTML
	print &stdIncludes ( "std_page_top" );
	print $html;
	print &stdIncludes ("std_page_bottom");
}

#  * User submits completed collection search form
#  * System displays matching collection results
sub displayCollResults {
	my $in_list = shift; # for taxon info script

	my $limit = $q->param("limit");
	if ( ! $limit ) { $limit = 10; }
	my $ofRows = 0;
	my $method = "getReadRows";					# Default is readable rows
	my $p = Permissions->new ( $s );

	# Build the SQL
    $sql = processCollectionsSearch($in_list);

	# Run it
	my $sth = $dbh->prepare( $sql );
	$sth->execute();

	if ( $q->param ( "type" ) ) {
		$type = $q->param ( "type" );			# It might have been passed (ReID)
	} else {
		# QUEUE
		my %queue = $s->unqueue ( $dbh );		# Most of 'em are queued
		$type = $queue{type};
	}

	# We create different links depending on their destination
	TYPE: {
		if ( $type eq "add" )	{ $action = "displayCollectionDetails"; $method = "getReadRows"; last; }
		if ( $type eq "edit" )	{ $action = "displayEditCollection"; $method = "getWriteRows"; last; }
		if ( $type eq "view" )	{ $action = "displayCollectionDetails"; $method = "getReadRows"; last; }
        # PZM 09/17/02 changed to 'getReadRows' from 'getWriteRows'
        # because we will be displaying both readable and writeable
        # data in 'displayOccurrenceAddEdit'
        if ( $type eq "edit_occurrence" )   { $action = "displayOccurrenceAddEdit"; $method =
"getReadRows"; last; }
		if ( $type eq "reid" )	{ $action = "displayOccsForReID"; $method = "getReadRows"; last; }

		# Unknown
		$action = "displayCollectionDetails";
		$method = "getReadRows";
	}

	# Get rows okayed by permissions module
	$p->$method ( $sth, \@dataRows, $limit, \$ofRows );
	#$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );
    my $displayRows = @dataRows;				# This is the actual number of rows displayed

    # the taxon info script displays the rows differently than say, 
    # the displayCollectionDetails method.
    if ($q->param("taxon_info_script") eq "yes"){
    	return \@dataRows;
    }

    if ( $displayRows > 1  || ($displayRows == 1 && $type eq "add")) {
		# go right to the chase with ReIDs if a taxon_rank was specified
		if($q->param('type') eq "reid" && $q->param('taxon_rank') ne 'Higher-taxon'){
			# get all collection #'s and call displayOccsForReID
			my @reidColls;
			foreach my $res (@dataRows){
				push(@reidColls, $res->{collection_no});
			}
			displayOccsForReID(\@reidColls);	
			exit;
		}
		# get the enterer's preferences (needed to determine the number
		#  of displayed blanks) JA 1.8.02
		%pref = &getPreferences($s->get('enterer'));

		print &stdIncludes ( "std_page_top" );
		print "<center><h3>Your search produced ";
		if ( $displayRows != $ofRows ) {
			print "$ofRows matches.  Here are the first $displayRows.";
		} else {
			print "$displayRows matches";
		}
		print "</h3></center>\n";

		print "<br>\n";
	  	print "<table width='100%' border=0 cellpadding=4 cellspacing=0>\n";
 
		# Create columns header
		print "
<tr>
	<th>Collection</th>
	<th align=left>Authorizer</th>
	<th align=left nowrap>Collection name</th>
	<th align=left colspan=2>Reference</th>
</tr>
";

		# Loop through each data row of the result set
		my $count = 0;
		foreach my $dataRow (@dataRows) {

			# Get the reference_no of the row
	        $sql = "SELECT * FROM refs WHERE reference_no=".$dataRow->{"reference_no"};
			my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	        $sth->execute();

	        my $refrowref = $sth->fetchrow_arrayref();
	        my $refmd = MetadataModel->new($sth);
	        my $refDataRow = DataRow->new($refrowref, $refmd);
	        my $an = AuthorNames->new($refDataRow);

			# Build the reference string

	        my $referenceNo = "<b>".$dataRow->{"reference_no"}."</b>&nbsp;&nbsp;";
	        my $reference = $an->getAuthor1Last();
			if ( $an->getOtherAuthors() )	{
				$reference .= " et al.";
			} else	{
	        	$reference .= " & " . $an->getAuthor2Last() if $an->getAuthor2Last();
			}
	        $reference .= " (" . $refDataRow->getValue("pubyr") . ")";

			# Build a short descriptor of the collection's time place
			# JA 20.8.02

			my $timeplace;
			my @timeterms = ( "locage", "intage", "epoch", "period" );
			for my $tt (@timeterms)	{
				if ( ! $timeplace )	{
					$timeplace = $dataRow->{$tt."_max"};
					if ( $dataRow->{$tt."_min"} && $dataRow->{$tt."_min"} ne $dataRow->{$tt."_max"} )	{
						$timeplace .= "/" . $dataRow->{$tt."_min"};
					}
				}
				if ( $timeplace )	{
					last;
				}
			}

			$timeplace .= " - ";
			if ( $dataRow{"state"} )	{
				$timeplace .= $dataRow->{"state"};
			} else	{
				$timeplace .= $dataRow->{"country"};
			}
			$timeplace = "</b> <span class=tiny>" . $timeplace . "</span>";

			# Shorthand
		  	my $collection_no = $dataRow->{"collection_no"};
		  	my $authorizerName = $dataRow->{"authorizer"};
			$authorizerName =~ s/ /&nbsp;/g;

#dbg ( join( "][", keys ( %{$dataRow} ) ) );
#dbg ( join( "][", values ( %{$dataRow} ) ) );

			# The LINK 
 			if ( $count % 2 == 0 ) {
				print "<tr bgcolor=\"#E0E0E0\">";
 			} else {
				print "<tr>";
			}
		  	print "
	<td align=center valign=top><a href='$exec_url?action=$action&collection_no=$collection_no";
			# These may be useful to displayOccsForReID
			if($q->param('genus_name')){
				print "&genus_name=".$q->param('genus_name');
			}
			if($q->param('species_name')){
				print "&species_name=".$q->param('species_name');
			}
			print "&blanks='".$pref{'blanks'}."'>$collection_no</a></td>
	<td valign=top>$authorizerName</td>
	<td valign=top><b>" . $dataRow->{"collection_name"} . $timeplace . "</td>
	<td align=right valign=top>$referenceNo</td>
	<td valign=top>$reference</td>
</tr>
";
			$count++;
  		}
		print "</table>\n";
    }
    elsif ( $displayRows == 1 )
    {
		my $dataRow = $dataRows[0];
		my $collection_no = $dataRow->{'collection_no'};
		$q->param(collection_no=>"$collection_no");
		# Do the action directly if there is only one row
		&$action;
		exit;
    } else {
		# If this is an add, then way cool.  Otherwise give an error
		if ( $type eq "add" ) {
			&displayEnterCollPage();
			return;
		} else {
			print &stdIncludes ( "std_page_top" );
			print "<center>\n<h3>Your search produced no matches</h3>";
			print "<p>Please try again with fewer search terms.</p>\n</center>\n";
		}
    }

	print "<center><p><a href='$exec_url?action=displaySearchColls&type=$type'><b>Do another search</b></a></p></center>\n";

	if ( $type eq "add" ) {
		print qq|<form action="$exec_url">\n|;
		print qq|<input type="hidden" name="action" value="displayEnterCollPage">\n|;
		print qq|<center>\n<input type=submit value="Add a new collection">\n|;
		print qq|</center>\n</form>\n|;
	}
		
	print &stdIncludes ("std_page_bottom");
}

# NOTE: this routine is used only by displayCollResults
sub processCollectionsSearch {
	my $in_list = shift;  # for taxon info script

	# This is a list of all pulldowns in the collection search.  
	# These cannot use the LIKE wildcard, i.e. they must be
	# exact matches regardless of the request.
	my %pulldowns = (	"authorizer"		=> 1,
						"enterer"			=> 1,
						"research_group"	=> 1,
						"period"			=> 1,
						"lithology1"		=> 1,
						"lithology2"		=> 1,
						"environment"		=> 1 );

					
	my $genus_name = $q->param('genus_name');

	# If a genus name is requested, query the occurrences table to get
	#   a list of useable collections
	# WARNING: wild card searches in this case DO require exact matches
	#   at the beginning of the genus name
	# Also searches reIDs table JA 16.8.02
	# Handles species name searches JA 19-20.8.02
	if($genus_name){
		# Fix up the genus name and set the species name if there is a space 
		my $genus;
		my $sub_genus;
		my $species;
		if($genus_name =~ / /){
			# Look for a subgenus in parentheses
			if($genus_name =~ /\([A-Z][a-z]+\)/ && 
							($q->param('taxon_rank') ne 'species')){
				$genus_name =~ /([A-Z][a-z]+)\s\(([A-Z][a-z]+)\)\s?([a-z]*)/;
				($genus, $sub_genus, $species) = ($1, $2, $3);
				# These param reassignments maybe useful to displayOccsForReID
				$q->param('species_name' => $species);
				$q->param('g_name' => $genus);
			}
			else{
				($genus,$species) = split / /,$q->param('genus_name');
				$q->param('species_name' => $species);
				$q->param('g_name' => $genus);
			}
		} elsif ( $q->param('taxon_rank') eq "species" )	{
			$species = $q->param('genus_name');
			$q->param('species_name' => $species);
		} else	{ # this is for genus only...
			$genus = $q->param('genus_name');
		}
		my @tables = ("occurrences","reidentifications");
		for my $tableName (@tables)	{
			$sql =	"SELECT collection_no,count(*) ".
					"  FROM " . $tableName . " WHERE ";

			if ( $q->param("wild") =~ /Y/ ) {
				$relationString = " LIKE '";
				$wildCard = "%'";
			} else	{
				$relationString = "='";
				$wildCard = "'";
			}
			if ( $genus )	{
				if($q->param("taxon_rank") eq "Higher taxon" ||
						$q->param("taxon_rank") eq "Higher-taxon"){
					$sql .= "genus_name IN (";
					if($in_list eq ""){
						dbg("RE-RUNNING TAXONOMIC SEARCH in bridge<br>");
						$in_list = PBDBUtil::taxonomic_search(
												$q->param('genus_name'), $dbt);
					}
					$sql .= $in_list . ") ";
				}
				else{
					$sql .= "genus_name" . $relationString . $genus . $wildCard;
				}
				if ( $sub_genus || $species )	{
					$sql .= " AND ";
				}
			}
			if ( $sub_genus )	{
				$sql .= "subgenus_name" . $relationString.$subgenus.$wildCard;
				if ( $species )	{
					$sql .= " AND ";
				}
			}
			if ( $species )	{
				$sql .= "species_name" . $relationString . $species . $wildCard;
			}

			$sql .= " GROUP BY collection_no";
			dbg ( "$sql<HR>" );
			$sth = $dbh->prepare($sql);
			$sth->execute();
			my @result = @{$sth->fetchall_arrayref()};
			for $r (0..$#result)	{
				push @okcolls, @{$result[$r]}[0] ;
			}
			if ($#okcolls == -1)	{
				push @okcolls , 0;
			}

			$sth->finish();
		}
	}

	# Get the database metadata
	$sql = "SELECT * FROM collections WHERE collection_no=0";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Get a list of field names
	my @fieldNames = @{$sth->{NAME}};
	# Get a list of field types
	my @fieldTypes = @{$sth->{mysql_is_num}};
	# Get the number of fields
	my $numFields = $sth->{NUM_OF_FIELDS};
	$sth->finish();
    
	# Handle wildcards
	my $comparator = "=";
	my $wildcardToken = "";
	if ( $q->param("wild") eq 'Y') {
 	 	$comparator = " LIKE ";
  		$wildcardToken = "%";
	}

		my @terms;
		# Handle modified date
		if ( $q->param('modified_since'))	{
		  push(@terms, "modified>" . $q->param('modified_since'));
		}
		# Handle collection name (must also search aka field) JA 7.3.02
		if ( $q->param('collection_names')) {
		  my $collectionName = $dbh->quote($wildcardToken . $q->param('collection_names') . $wildcardToken);
		  push(@terms, "(collection_name$comparator" . $collectionName . " OR collection_aka$comparator" . $collectionName . ")");
		}
		# Handle period
		if ( $q->param('period')) {
		  my $periodName = $dbh->quote($wildcardToken . $q->param('period') . $wildcardToken);
		  push(@terms, "(period_min$comparator" . $periodName . " OR period_max$comparator" . $periodName . ")");
		}
		# Handle intage
		if ( $q->param('intage')) {
		  my $intageName = $dbh->quote($wildcardToken . $q->param('intage') . $wildcardToken);
		  push(@terms, "(intage_min$comparator" . $intageName . " OR intage_max$comparator" . $intageName . ")");
		}
		# Handle locage
		if ( $q->param('locage')) {
		  my $locageName = $dbh->quote($wildcardToken . $q->param('locage') . $wildcardToken);
		  push(@terms, "(locage_min$comparator" . $locageName . " OR locage_max$comparator" . $locageName . ")");
		}
		# Handle epoch
		if ( $q->param('epoch')) {
		  my $epochName = $dbh->quote($wildcardToken . $q->param('epoch') . $wildcardToken);
		  push(@terms, "(epoch_min$comparator" . $epochName . " OR epoch_max$comparator" . $epochName . ")");
		}
		# Handle lithology
		if ( $q->param('lithology'))	{
		  my $lithologyName = $dbh->quote($wildcardToken . $q->param('lithology') . $wildcardToken);
		  push(@terms, "(lithology1$comparator" . $lithologyName . " OR lithology2$comparator" . $lithologyName . ")");
		}
		# research_group is now a set -- tone 7 jun 2002
		my $resgrp = $q->param('research_group');
		if($resgrp && $resgrp =~ /(^ETE$)|(^5%$)|(^PGAP$)/){
			my $resprojstr = PBDBUtil::getResearchProjectRefsStr($dbh,$q);
				if($resprojstr ne ""){
					push(@terms, " reference_no IN (" . $resprojstr . ")");
				}   
		}   
		elsif($resgrp){
			push ( @terms, "FIND_IN_SET('".$q->param("research_group")."', research_group)" );
		}
		# Remove it from further consideration
		$q->param("research_group" => "");

		# Compose the WHERE clause
		my $fieldCount = -1;
		foreach $fieldName ( @fieldNames ) {
			$fieldCount++;
			if ( my $val = $q->param($fieldName) ) {
				$val =~ s/"//g;
				if ( $pulldowns{$fieldName} ) {
					# It is in a pulldown... no wildcards
					push(@terms, "$fieldName = '$val'");
				} else {
					$val = qq|"$wildcardToken$val$wildcardToken"| if $fieldTypes[$fieldCount] == 0;
					push(@terms, "$fieldName$comparator$val");
				}

			}
		}

		# if first search failed and wild cards were used, try again
		#  stripping first wildcard JA 22.2.02
		if ( !@terms && $wildcardToken ne "")	{
			foreach $fieldName (@fieldNames) {
				$fieldCount++;
				if ( my $val = $q->param($fieldName)) {
					$val =~ s/"//g;
					$val = qq|"$val$wildcardToken"| if $fieldTypes[$fieldCount] == 0;
					push(@terms, "$fieldName$comparator$val");
				}
			}
		}
		
		if ( ! @terms ) {
			if ( $q->param("genus_name") ) {
				push @terms,"collection_no is not NULL";
			} else {
				my $message =	"
<center>
<h4>Please specify at least one search term</h4>
<p>
<a href='?action=displaySearchColls&type=".$q->param("type")."'><b>Try again</b></a>
</p>
</center>
";
				&htmlError ( $message );
			}
		}
		
		# Compose the columns list
		my @columnList = (	"collection_no",
							"authorizer",
							"collection_name",
							"access_level",
							"research_group",
							"release_date",
							"DATE_FORMAT(release_date, '%Y%m%d') rd_short",
			"country", "state", "period_max", "period_min", "epoch_max", "epoch_min", "intage_max", "intage_min", "locage_max", "locage_min");
		
		# Handle extra columns
		push(@columnList, $q->param('column1')) if $q->param('column1');
		push(@columnList, $q->param('column2')) if $q->param('column2');
		
		# Handle sort order
		my $sortString = "";
		$sortString = "ORDER BY " . $q->param('sortby') if $q->param('sortby');
		$sortString .= " DESC" if $sortString && $q->param('sortorder') eq 'desc';

		# Handle limit
		my $limitString = "";
		#	$limitString = " LIMIT " . $q->param('limit') if $q->param('limit');
		
		# Compose the SELECT query
		$sql =	"SELECT ".join(', ', @columnList, 'reference_no').
				"  FROM collections ".
				" WHERE ".join(' AND ', @terms);
		
		if ( $q->param('genus_name') ) {
			if (@terms)	{
				$sql .= " AND collection_no IN ( " . join ( ", ", @okcolls )." ) ";
			} else	{
				$sql .= " collection_no IN ( " . join ( ", ", @okcolls )." ) ";
			}
		}

		# Sort and limit
		$sql .= " $sortString $limitString";

		dbg ( "$sql<HR>" );

		return $sql;
}


#  * User selects a collection from the displayed list
#  * System displays selected collection
sub displayCollectionDetails {

	my $collection_no = $q->param('collection_no');
	$sql = "SELECT * FROM collections WHERE collection_no=" . $collection_no;
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
	my $authorizer;
    my $refNo;
    foreach my $tmpVal (@fieldNames) {
		if ( $tmpVal eq 'authorizer') {
			$authorizer = $row[$fieldCount];
		} elsif ( $tmpVal eq 'reference_no') {
			$refNo = $row[$fieldCount];
		} elsif ( $tmpVal eq 'collection_subset' && $row[$fieldCount] ne "") {
			my $collno = $row[$fieldCount];
			my $linkFront = "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collno\">";
			$row[$fieldCount] = $linkFront . $collno . "</a> ";
		}
		last if $authorizer && $refNo;
		$fieldCount++;
	}

    # Get the reference
    $sql = "SELECT * FROM refs WHERE reference_no=$refNo";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my $refRowRef = $sth->fetchrow_arrayref();

    my $md = MetadataModel->new($sth);
    my $drow = DataRow->new($refRowRef, $md);
    my $bibRef = BiblioRef->new($drow);

    my $refFieldNamesRef = $sth->{NAME};
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
      my $collno = @subrow[0];
      my $linkFront = "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collno\">";
      $subString .= $linkFront . $collno . "</a> ";
    }
    push(@row, $subString);
    push(@fieldNames, 'subset_string');

    print &stdIncludes ( "std_page_top" );
    print $hbo->populateHTML('collection_display_fields', \@row, \@fieldNames);
    # If the viewer is the authorizer (or it's me), display the record with edit buttons
    if ( $authorizer eq $s->get('authorizer') || $s->get('authorizer') eq 'J. Alroy') {
		print $hbo->populateHTML('collection_display_buttons', \@row, \@fieldNames);
    }

	print "<HR>\n";
	my $taxa_list = buildTaxonomicList($collection_no, $refNo);
	print $taxa_list;

	if($authorizer eq $s->get('authorizer') || $s->get('authorizer') eq 'J. Alroy')	{
		print $hbo->populateHTML('occurrence_display_buttons', \@row, \@fieldNames);
	}
	if($taxa_list ne "" && $q->param("user") ne "Guest"){
		print $hbo->populateHTML('reid_display_buttons', \@row, \@fieldNames);
	}

	print &stdIncludes ("std_page_bottom");
}

sub buildTaxonomicList {
	my $collection_no = shift;
	my $collection_refno = shift;
	my $gnew_names = shift;
	my @gnew_names = @{$gnew_names};
	my $subgnew_names = shift;
	my @subgnew_names = @{$subgnew_names};
	my $snew_names = shift;
	my @snew_names = @{$snew_names};
	my $new_found = 0;
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
			"       occurrence_no ".
			"  FROM occurrences ".
			" WHERE collection_no=$collection_no";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $md = MetadataModel->new($sth);

	my @rowrefs = @{$sth->fetchall_arrayref()};
	$sth->finish();

	if ( @rowrefs ) {
		my @occFieldNames = $md->getFieldNames();
		my $output;
		my $genus;
		my $newreference;
		my $abundance;
		my $comments;
		my $genus_index = -1;
		my $subgenus_index = -1;
		my $species_index = -1;

		# helps in setting bold genus/species names
		for($i=0; $i<@occFieldNames; $i++){
			if($occFieldNames[$i] eq 'genus_name'){
				$genus_index = $i;
			}
			elsif($occFieldNames[$i] eq 'subgenus_name'){
				$subgenus_index = $i;
			}
			elsif($occFieldNames[$i] eq 'species_name'){
				$species_index = $i;
			}
		}

		my $count = 0;
		foreach my $rowref ( @rowrefs ) {
			# check for unrecognized genus names
			foreach my $nn (@gnew_names){
				if($rowref->[$genus_index] eq $nn){
					$rowref->[$genus_index] = "<b>".$rowref->[$genus_index]."</b>";
					$new_found = 1;
				}
			}

			# check for unrecognized subgenus names
			foreach my $nn (@subgnew_names){
				if($rowref->[$subgenus_index] eq $nn){
					$rowref->[$subgenus_index] = "<b>".$rowref->[$subgenus_index]."</b>";
					$new_found = 1;
				}
			}

			# check for unrecognized species names
			foreach my $nn (@snew_names){
				if($rowref->[$species_index] eq $nn){
					$rowref->[$species_index]="<b>".$rowref->[$species_index]."</b>";
					$new_found = 1;
				}
			}

			my $drow = DataRow->new($rowref, $md);

			$genus = "Genus and species" if $drow->getValue('genus_name') || $drow->getValue('species_name');
			$abundance = 'Abundance' if $drow->getValue('abund_value');
			$comments = 'Comments' if $drow->getValue('comments');
			# replace the ref no with a formatted ref JA 10.6.02
			# WARNING: uses buildReference function that was written to
			#   be used in the bottom navigation bar
			my $newrefno = $drow->getValue('reference_no');
			if ($newrefno != $collection_refno)	{
				$drow->setValue("reference_no",&buildReference($newrefno,"list"));
				$newreference = 'Reference';
			}
			else	{
				$drow->setValue("reference_no",'');
			}

			my @occrow = @{$rowref};

			$formattedrow = $hbo->populateHTML("taxa_display_row", \@occrow, \@occFieldNames );

			# Link taxa to the TaxonInfo script
			# ---------------------------------
			# If genus is informal, don't link.
			$formattedrow =~ s/(<i>|<\/i>)//g;
			if($formattedrow =~ /informal(.*)?<genus>/){
				# do nothing
			}
			# If species is informal, link only the genus.
			elsif($formattedrow =~ /<genus>(.*)?<\/genus>(.*)?informal/s){
				$formattedrow =~ s/<genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$1&taxon_rank=Genus&user=Contributor"><i>$1$2$3<\/i><\/a>/s;
			}
			elsif($formattedrow =~ /<species>indet/s){
				# shouldn't be any <i> tags for indet's.
				$formattedrow =~ s/<genus>(.*)?<\/genus>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$1&taxon_rank=Higher+taxon&user=Contributor">$1 indet.<\/a>/;
				$formattedrow =~ s/<species>(.*)?<\/species>//;
			}
			else{
				# match multiple rows as a single (use the 's' modifier)
				$formattedrow =~ s/<genus>(.*)?<\/genus>(.*)?<species>(.*)?<\/species>/<a href="\/cgi-bin\/bridge.pl?action=checkTaxonInfo&taxon_name=$1+$3&taxon_rank=Genus+and+species&user=Contributor"><i>$1$2$3<\/i><\/a>/s;
			}
			# ---------------------------------

			$formattedrow .= getReidHTMLTableByOccNum(pop(@occrow),$collection_refno);
			# if there's a link in here somewhere, there must be a new ref
			#   (kludgy, but saves the trouble of passing newreference back
			#   and forth)
			if ($formattedrow =~ /a href/)	{
				$newreference = 'Reference';
			}
			# Color the background of alternating rows gray JA 10.6.02
			if ( $count % 2 == 0 && @rowrefs > 2) {
				$formattedrow =~ s/<td/<td bgcolor='#E0E0E0'/g;
			}
            $output .= $formattedrow;
			$count++;
		}

		$sql = "SELECT collection_name FROM collections ".
			   "WHERE collection_no=$collection_no";
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my @coll_name = @{$sth->fetchrow_arrayref()};
		$sth->finish();

		# Taxonomic list header
		$return = "
<div align='center'>
<h3>Taxonomic list for $coll_name[0] (PBDB collection $collection_no)</h3>";

		if($new_found){
			$return .= "<h4><font color=red>WARNING!</font> Taxon names in ".
					   "<b>bold</b> are new to the database.<br>Please make ".
					   "sure the spelling is correct.</h4>";
		}

		$return .=
"<table border=\"0\" cellpadding=\"3\" cellspacing=\"0\">
<tr>
	<td nowrap><u>$genus</u></td>
	<td><u>$newreference</u></td>
	<td><u>$abundance</u></td>
	<td><u>$comments</u></td>
</tr>
";
		# Clean up abundance values (somewhat messy, but it works, and better
		#   here than in populateHTML) JA 10.6.02
		$output =~ s/(>1 specimen)s|(>1 individual)s/$1$2/g;
        
		$return .= $output;

		$return .= "
</table>
</div>
";

	}
	return $return;
}

sub getReidHTMLTableByOccNum {

	my $occNum = shift;
	my $collection_refno = shift;

	$sql =	"SELECT genus_reso, ".
			"       genus_name, ".
			"       subgenus_reso, ".
			"       subgenus_name, ".
			"       species_reso, ".
			"       species_name, ".
			"       comments, ".
			"       reference_no ".
			"  FROM reidentifications ".
			" WHERE occurrence_no=$occNum";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
  
	my @fieldNames = @{$sth->{NAME}};
	@rows = @{$sth->fetchall_arrayref()};

	my $retVal = "";
	foreach my $rowRef ( @rows ) {
		my @row = @{$rowRef};
		# format genus_name
		if($row[0]){
			$row[1] = "<font size=-1><i>&nbsp;&nbsp;&nbsp;&nbsp;=&nbsp;".$row[0]."&nbsp;".$row[1]."</i></font>";
			shift @row;
			shift @fieldNames;
		}
		else{
			$row[1] = "<font size=-1><i>&nbsp;&nbsp;&nbsp;&nbsp;=&nbsp;".$row[1]."</i></font>";
		}
		# format the reference
		$row[-1] = buildReference($row[-1],"list");
		$retVal .= $hbo->populateHTML("taxa_display_row", \@row,\@fieldNames);
	}

	return $retVal;
}


sub displayEnterCollPage {

	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=displayEnterCollPage" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		$s->enqueue( $dbh, "action=displayEnterCollPage" );
		&displaySearchRefs ( "Please choose a reference first" );
		exit;
	}	

	# Get the field names
	$sql = "SELECT * FROM collections WHERE collection_no=0";
	dbg ( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @fieldNames = @{$sth->{NAME}};
	$sth->finish();

	# Get the reference data
	$sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

    my $md = MetadataModel->new($sth);
	$refRowRef = $sth->fetchrow_arrayref();
    my $drow = DataRow->new($refRowRef, $md);
    my $bibRef = BiblioRef->new($drow);
	@refFieldNames = @{$sth->{NAME}};
	$sth->finish();

	$refRowString = "<table>" . $bibRef->toString() . '</table>';

	# Need to build the research_group checkboxes

	# Tack a few extra fields
	my @row;
	unshift(@fieldNames,	'authorizer', 
							'enterer', 
							'reference_no',
							'ref_string',
							'country' );
	unshift(@row,	$s->get('authorizer'), 
					$s->get('enterer'), 
					$reference_no,
					$refRowString,
					'' );

	%pref = &getPreferences($s->get('enterer'));
	# Get the enterer's preferences JA 25.6.02
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	for $p (@{$setFieldNames})	{
		if ($pref{$p} ne "")	{
			unshift @row,$pref{$p};
			unshift @fieldNames,$p;
		}
	}

	# Output the page
	print &stdIncludes ( "std_page_top" );
	my @prefkeys = keys %pref;
	print $hbo->populateHTML('enter_coll_form', \@row, \@fieldNames, \@prefkeys);
	print &stdIncludes ("std_page_bottom");
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
		print &stdIncludes ( "std_page_top" );

	&setReleaseDate();
		$q->param(enterer=>$s->get("enterer"));
		$q->param(authorizer=>$s->get("authorizer"));
		
    unless($q->param('period_max'))
    {
      print "<h3>The period field is required!</h3>\nPlease go back and specify the period for this collection";
      print &stdIncludes ("std_page_bottom");
      print "<br><br>";
      return;
    }
    
	my $recID;
	$return = insertRecord( 'collections', 'collection_no', \$recID, '99', 'period_max' );
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
			$s->setReferenceNo ( $dbh, $reference_no );

			$sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			@refRow = $sth->fetchrow_array();
			@refFieldNames = @{$sth->{NAME}};
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
 
    print $hbo->populateHTML('collection_display_fields', \@row, \@fields);
    print $hbo->populateHTML('collection_display_buttons', \@row, \@fields);
    print $hbo->populateHTML('occurrence_display_buttons', \@row, \@fields);
	print qq|<center><b><p><a href="$exec_url?action=displaySearchColls&type=add">Enter another collection with the same reference</a></p></b></center>|;
 
	print &stdIncludes ("std_page_bottom");
}

# This subroutine intializes the process to get to the Edit Collection page
sub startEditCollection {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection

	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=startEditCollection" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	elsif ( $q->param("collection_no") ) {
		$s->enqueue( $dbh, "action=displayEditCollection&collection_no=".$q->param("collection_no") );
	} else {
		$s->enqueue( $dbh, "action=displaySearchColls&type=edit" );
	}

	$q->param( "type" => "select" );
	&displaySearchRefs ( ); 
}

# This subroutine intializes the process to get to the Add Collection page
sub startAddCollection {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=startAddCollection" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	$s->enqueue( $dbh, "action=displaySearchColls&type=add" );

	$q->param( "type" => "select" );
	&displaySearchRefs ( ); 
}

# This subroutine intializes the process to get to the Add/Edit Occurrences page
sub startAddEditOccurrences {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection
	
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=startAddEditOccurrences" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	elsif ( $q->param("collection_no") ) {
		$s->enqueue( $dbh, "action=displayOccurrenceAddEdit&collection_no=".$q->param("collection_no") );
	} else {
		$s->enqueue( $dbh, "action=displaySearchColls&type=edit_occurrence" );
	}

	$q->param( "type" => "select" );
	&displaySearchRefs ( ); 
}

# This subroutine intializes the process to get to the reID entry page
sub startReidentifyOccurrences {

	# 1. Need to ensure they have a ref
	# 2. Need to get a collection or genus search
	
	$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );

	$q->param( "type" => "select" );
	&displaySearchRefs ( );
}

##############
## Taxon Info Stuff
sub beginTaxonInfo{
	TaxonInfo::startTaxonInfo($q);
}

sub checkTaxonInfo{
	TaxonInfo::checkStartForm($q, $dbh, $s, $dbt);
}

sub displayTaxonInfoResults{
	TaxonInfo::displayTaxonInfoResults($q, $dbh, $s, $dbt);
}
## END Taxon Info Stuff
##############

# JA 13.8.02
sub startTaxonomy	{

	# 1. Need to ensure they have a ref
	# 2. Need to perform a taxonomy search

	# if there's no selected taxon you'll have to search for one
	if ( ! $q->param('taxon_name') || ! $q->param('taxon_no') )	{
		$s->enqueue( $dbh, "action=displayTaxonomySearchForm" );
	# otherwise go right to the edit page
	} else	{
		my $temp = "action=displayTaxonomyEntryForm&taxon_name=";
		$temp .= $q->param('taxon_name');
		$temp .= "&taxon_no=" . $q->param('taxon_no');
		$s->enqueue( $dbh, $temp );
	}
	$q->param( "type" => "select" );
	&displaySearchRefs ( "Please choose a reference first" );
}

sub displayEditCollection {
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=displayEditCollection" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	
	my $session_ref = $s->get('reference_no');

	print &stdIncludes ("std_page_top");

	my $collection_no = $q->param('collection_no');
	$sql = "SELECT * FROM collections WHERE collection_no=" . $collection_no;
	dbg ( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @fieldNames = @{$sth->{NAME}};
	my @row = $sth->fetchrow_array();
	$sth->finish();

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
	my $refRowString = getCurrRefDisplayStringForColl($dbh, $collection_no, 
												  $reference_no);
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
						 "</td></tr><tr bgcolor=E0E0E0><td valign=top>".
						 "<input type=radio name=secondary_reference_no value=".
						 $session_ref."></td><td></td>";
		$sr = getCurrRefDisplayStringForColl($dbh, $collection_no,$session_ref);
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

	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
	print $hbo->populateHTML('edit_coll_form', \@row, \@fieldNames, \@prefkeys);
    
	print &stdIncludes ("std_page_bottom");
}


sub processEditCollectionForm {
	# Save the old one in case a new one comes in
	my $reference_no = $q->param("reference_no");
	my $collection_no = $q->param("collection_no");
	my $secondary = $q->param('secondary_reference_no');

	print &stdIncludes ( "std_page_top" );

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
	&setReleaseDate();

	# Updates here 
	my $recID = &updateRecord ( 'collections', 'collection_no', $q->param('collection_no') );
 
    print "<center><h3><font color='red'>Collection record updated</font></h3></center>\n";

	# Select the updated data back out of the database.
	$sql = "SELECT * FROM collections WHERE collection_no=$recID";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
  	$sth->execute();
	&dbg ( "$sql<HR>" );
    my @fields = @{$sth->{NAME}};
	my @row = $sth->fetchrow_array();
    $sth->finish();
    
	my $curColNum = 0;
	my $reference_no;
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
    print $hbo->populateHTML('collection_display_buttons', \@row, \@fields);
    print $hbo->populateHTML('occurrence_display_buttons', \@row, \@fields);
    
	print qq|<center><b><p><a href="$exec_url?action=displaySearchColls&type=edit">Edit another collection using the same reference</a></p></b></center>|;
	print qq|<center><b><p><a href="$exec_url?action=displaySearchColls&type=add">Add a collection with the same reference</a></p></b></center>|;

	print &stdIncludes ("std_page_bottom");
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

    my $sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
    my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my @refRow = $sth->fetchrow_array();
    my @refFieldNames = @{$sth->{NAME}};
    $sth->finish();
    my $refRowString = $hbo->populateHTML('reference_display_row', \@refRow, \@refFieldNames);

    return $refRowString;
}


sub processNewOccurrences
{
	my $recID;

	print &stdIncludes ( "std_page_top" );

	# Get the database metadata
	$sql = "SELECT * FROM occurrences WHERE occurrence_no=0";
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

	my @requiredFields = ("authorizer", "enterer", "collection_no",
							"genus_name", "species_name", "reference_no");
	for $i (0..$numFields)	{
		for $j (0..$#requiredFields)	{
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

	ROW:
	# Loop over each row
	for(my $i=0;$i<$numRows;$i++)
	{
		my @fieldList;
		my @row;
			
		push(@fieldList, 'created');
		push(@row, '"' . now() . '"');

		# Loop over each field in the row
		for(my $j=0;$j<$numFields;$j++)
		{
			my $fieldName = $fieldNames[$j];

			# Here's an indirect way to get the value
			my @tmpVals = $q->param($fieldName);
			my $curVal = $tmpVals[$i];
			
			# Skip rows that don't have a required data item
			unless ( ! $isRequired[$j] || ($types[$j] eq 'timestamp') || $priKeys[$j] || $curVal ) {
#			unless($nullables[$j] || ($types[$j] eq 'timestamp') || $priKeys[$j] || $curVal) {
				next ROW;
			}

			if ( $curVal ) {
				$curVal = $dbh->quote($curVal) if $fieldTypes[$fieldCount] == 0;

 				push(@row, $curVal);
				push(@fieldList, $fieldName);
			}
			elsif ( defined $q->param($fieldName))
			{
				push(@row, '');
				push(@fieldList, $fieldName);
			}
		}
#			$sql = "INSERT INTO occurrences (" . join(',', @fieldList) . ") VALUES (" . join(', ', @row) . ")" || die $!;
#			$sql =~ s/\s+/ /gms;
#print "$sql\n"; exit(0);

		# Check for duplicates
		$return = checkDuplicates ( "occurrence_no", \$recID, "occurrences", \@fieldList, \@row );
		if ( ! $return ) { return $return; }

		if ( $return != $DUPLICATE ) {
			$sql = "INSERT INTO occurrences (" . join(',', @fieldList) . ") VALUES (" . join(', ', @row) . ")" || die $!;
			$sql =~ s/\s+/ /gms;
			dbg ( "$sql<HR>" );
			$dbh->do( $sql ) || die ( "$sql<HR>$!" );
			$recID = $dbh->{'mysql_insertid'};
		}

		$sql = "SELECT * FROM occurrences WHERE occurrence_no=$recID";
		dbg ( "$sql<HR>" );
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my @retrievedRow = $sth->fetchrow_array();
		$sth->finish();
		my $rowString = "<tr><td>" . join("</td><td>", @retrievedRow) . "</td></tr>";
		push(@successfulRows, $rowString);
	}
		
	print "<table>";
	foreach $successfulRow (@successfulRows) {
  		print $successfulRow;
    }
    print "</table>";
		
	print &stdIncludes ("std_page_bottom");
}


sub displayOccurrenceAddEdit {

	my $collection_no = $q->param("collection_no");
	if ( ! $collection_no ) { htmlError ( "No collection_no specified" ); }

	# Grab the collection name for display purposes JA 1.10.02
	$sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $collection_name = ${$sth->fetchrow_arrayref()}[0];
	$sth->finish();

	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('js_occurrence_checkform');

	$sql = "SELECT * FROM occurrences WHERE collection_no=$collection_no";
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	print qq|<form method=post action="$exec_url" onSubmit='return checkForm();'>\n|;
	print qq|<input name="action" value="processEditOccurrences" type=hidden>\n|;
	print "<table>";

	push my @tempRow, $collection_no;
	push my @tempFieldName, "collection_no";

	push @tempRow, $collection_name;
	push @tempFieldName, "collection_name";

	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
	print $hbo->populateHTML('occurrence_header_row', \@tempRow, \@tempFieldName, \@prefkeys);

    # of records, each represented as a hash
    my $p = Permissions->new($s);
    my @all_data = $p->getReadWriteRowsForEdit($sth);
    my $gray_counter = 0;
    foreach my $hash_ref (@all_data){
        $hash_ref->{'authorizer'} = $s->get('authorizer');
        $hash_ref->{'enterer'} = $s->get('enterer');
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
		$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
        while(my $hr = $sth2->fetchrow_hashref()) {
            $hr->{'authorizer'} = $s->get('authorizer');
            $hr->{'enterer'} = $s->get('enterer');
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
						'authorizer', 
						'enterer',
						'genus_reso',
						'subgenus_reso',
						'species_reso',
						'plant_organ', 
						'plant_organ2');
	my @row = ( $collection_no, 
				$s->get("reference_no"),
				$s->get("authorizer"), 
				$s->get("enterer"),
				'', '', '', '', '');

	# Read the users' preferences related to occurrence entry/editing
	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;

	# Figure out the number of blanks to print
	my $blanks = $pref{'blanks'};
	if ( ! $blanks ) { $blanks = 10; }

	# Set the default species name (indet. or sp.) JA 9.7.02
	if ($pref{'species_name'} ne "")	{
		unshift @row, $pref{'species_name'};
		unshift @fieldNames, "species_name";
	}

	for ( $i = 0; $i<$blanks ; $i++) {
		print qq|<input type=hidden name="row_token" value="row_token">\n|;
		print $hbo->populateHTML("occurrence_entry_row", \@row, \@fieldNames, \@prefkeys);
	}

	print "</table><br>\n";
	print qq|<center><p><input type=submit value="Save changes">|;
    printf " to collection %s's taxonomic list</p></center>\n",$collection_no;
	print "</form>";

	print &stdIncludes ("std_page_bottom");
}

sub processEditOccurrences {
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
	my @required_fields = ("authorizer", "enterer", "collection_no", "genus_name", "species_name", "reference_no");
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
			delete($results{created});
			delete($results{modified});
			delete($results{modifier});
			delete($results{modified_temp});
			delete($results{reid_no});
			
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
						# replace every single quote with two single quotes.
						# Note: it seems to behave the same without the \Q too.
						${$all_params{$key}}[$index] =~ s/\Q'/''/g;
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
							push(@update_strings,"$key='$results{$key}'");
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

			# Update the reidentification row
			$sql = "UPDATE reidentifications SET ".
					join(',', @update_strings) .
					" WHERE reid_no=$reid_no";
			dbg("REID UPDATE SQL: $sql<br>");

			# Prepare, execute
			$dbh->do($sql) || die ("$sql<hr>$!");	

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
			delete($results{created});
			delete($results{modified});
			delete($results{modifier});
			delete($results{occurrence_no});
			
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
						# replace every single quote with two single quotes.
						# Note: it seems to behave the same without the \Q too.
						${$all_params{$key}}[$index] =~ s/\Q'/''/g;
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
							push(@update_strings,"$key='$results{$key}'");
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
				next if $val eq 'row_token';
				next if $val eq 'action';
				if(${$all_params{$val}}[$index]){
					push(@insert_names, $val);
					if($occsIntFields{$val} == 1){
						push(@insert_values, ${$all_params{$val}}[$index]);
					}
					else{
						# escape all inner single quotes (like don't) BEFORE
						# quoting the string.
						${$all_params{$val}}[$index] =~ s/\Q'/''/g;
						push(@insert_values, "'".${$all_params{$val}}[$index]."'");
					}
				}
			}
			push(@insert_names, 'created');
			push(@insert_values, "'".now()."'");

			# Check for duplicates
			$return = checkDuplicates ( "occurrence_no", \${$all_params{occurrence_no}}[$index], "occurrences", \@insert_names, \@insert_values );
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

	print &stdIncludes ( "std_page_top" );

	# Show the rows for this collection to the user
	my $collection_no = ${$all_params{collection_no}}[0];

	print &buildTaxonomicList ( $collection_no, 0,\@gnew_names,\@subgnew_names,\@snew_names );

	# Show a link to re-edit
	print "
	<p align='center'><b>
	<a href='$exec_url?action=displayOccurrenceAddEdit&collection_no=$collection_no'>Edit occurrences for this collection</a>
	<br><a href='$exec_url?action=displayEditCollection&collection_no=$collection_no'>Edit the main collection record</a>
	<br><a href='$exec_url?action=displaySearchColls&type=edit_occurrence'>Add/edit occurrences for a different collection with the current reference</a>
	<br><a href='$exec_url?action=displaySearchColls&type=add'>Enter another collection with the same reference</a></p></b>
";

	print &stdIncludes ("std_page_bottom");
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
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	if ( ! $reference_no ) {
		$s->enqueue( $dbh, "action=displayReIDCollsAndOccsSearchForm" );
		&displaySearchRefs ( "Please choose a reference first" );
		exit;
	}	

	print &stdIncludes ( "std_page_top" );
  
	print "<h4>You may now reidentify either a set of occurrences matching a genus or higher taxon name, or all the occurrences in one collection.</h4>";
  
	# Display the collection search form
	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
	my $html = $hbo->populateHTML('search_collections_form', ['', '', 'displayReIDForm', $reference_no,'',$q->param('type'),'',''], ['authorizer', 'enterer', 'action', 'reid_reference_no','lithology1','type','lithology2','environment'], \@prefkeys);

	buildAuthorizerPulldown ( \$html );
	buildEntererPulldown ( \$html );

	# Set the Enterer & Authorizer
	my $enterer = $s->get("enterer");
	$html =~ s/%%enterer%%/$enterer/;
	my $authorizer = $s->get("authorizer");
	$html =~ s/%%authorizer%%/$authorizer/;

	# Spit out the HTML
	print $html;
  
	print '</form>';
	print &stdIncludes ("std_page_bottom");
}

sub displayReIDForm {

    # If this is a genus/species search, go right to the reid form.
	#if($q->param("taxon_rank") ne ''){
		$q->param("type" => "reid");
	    &displayCollResults ();
	#}
	# Not sure why this is here, which of course means I'm not sure why this
	# entire method exists...  11/26/02 PM
    #else{ #( $q->param('genus_name') ne '') {
	#	&displayOccsForReID();
	#}
    #} else {
#
#    	# Must be a collection search
#		$q->param("type" => "reid");
#	    &displayCollResults ();
#    }
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

	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('js_occurrence_checkform');
    
	my $lastOccNum = $q->param('last_occ_num');
	if ( ! $lastOccNum ) { 
		$lastOccNum = -1;
		$onFirstPage = 1; 
	}

	# Build the SQL
	if($genus_name ne '' or $species_name ne ''){
		$printCollectionDetails = 1;

		$sql = "SELECT * FROM occurrences ";
		if ( $genus_name ) {
			$where = buildWhere ( $where, "genus_name='$genus_name'" );
		}
		if ( $species_name ) {
			$where = buildWhere ( $where, "species_name='$species_name'" );
		}
		if(@colls > 0){
			$where = buildWhere($where,"collection_no IN(".join(',',@colls).")");
		}
		elsif($collection_no > 0){
			$where = buildWhere($where, "collection_no=$collection_no");
		}
	}
	elsif($collection_no){
		$sql = "SELECT * FROM occurrences ";
		$where = buildWhere($where, "collection_no=$collection_no");
	}
	$where = buildWhere ( $where, "occurrence_no > $lastOccNum LIMIT 11" );

	# Tack it all together
	$sql .= $where;
 
	dbg("$sql<br>");
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Get the current reference data
	$sql = "SELECT * FROM refs WHERE reference_no=".$s->get("reference_no");
	dbg("$sql<br>");
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
	foreach my $rowRef (@array_of_hash_refs)
	{
		# If we have 11 rows, skip the last one; and we need a next button
		$rowCount++;
		last if $rowCount > 10;

		my @prefkeys;
		if ($rowCount == 1)	{
			%pref = &getPreferences($s->get('enterer'));
			@prefkeys = keys %pref;
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
		print $hbo->populateHTML('reid_entry_row', [$row{'occurrence_no'}, $row{'collection_no'}, $row{'authorizer'}, $row{'enterer'} ], ['occurrence_no', 'collection_no', 'authorizer', 'enterer'], \@prefkeys);

		# print other reids for the same occurrence
		$sql = "SELECT * FROM reidentifications WHERE occurrence_no=" . $row{'occurrence_no'};
		$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();

		print "<tr><td colspan=100>";
		print getReidHTMLTableByOccNum($row{'occurrence_no'},0);
		print "</td></tr>\n";
		$sth2->finish();

		# Print the reference details
		$sql = "SELECT * FROM refs WHERE reference_no=" . $row{'reference_no'};
		$sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
		my $md = MetadataModel->new($sth2);
		my $refRow = $sth2->fetchrow_arrayref();
		my $drow = DataRow->new($refRow, $md);
		# my $bibRef = BiblioRef->new($drow, $md);

		print "
<tr>
	<td colspan=100>
	<img src='/images/spacer.gif' width='50' height='1' alt='' border='0' align='left'>
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

		print "
	</table>

	</td>
</tr>
";
		$sth2->finish();

		# Print the collections details
		if ( $printCollectionDetails) {

			$sql = "SELECT * FROM collections WHERE collection_no=" . $row{'collection_no'};
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
		print "</td>";
	}

	print "</tr>\n</table><p>\n";

	print &stdIncludes ("std_page_bottom");
}

#  5. * System processes reidentifications.  System displays
#       reidentification form.  Occurrences which were reidentified
#       in the previous step may not be reidentified in this step
sub processNewReIDs {
	my $redID;

	print &stdIncludes ( "std_page_top" );
    
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

	my @requiredFields = ("authorizer", "enterer", "occurrence_no",
				"collection_no", "genus_name", "species_name", "reference_no");
	for $i (0..$numFields)	{
		for $j (0..$#requiredFields)	{
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
		ROW:
		for(my $i = 0;$i < $numRows;$i++)
		{
			my @fieldList;
			my @row;
		# added 11.7.02 JA (omitted by Garot!)
			push(@fieldList, 'created');
			push(@row, '"' . now() . '"');
			for(my $j = 0;$j < $numFields;$j++)
			{
				my $fieldName = $fieldNames[$j];
				my @tmpVals = $q->param($fieldName);
				my $curVal = $tmpVals[$i];
				
				next if  $fieldName eq 'reference_no';
				# Skip rows that don't have a required data item
				unless ( ! $isRequired[$j] || ($types[$j] eq 'timestamp') || $priKeys[$j] || $curVal ) {
					#print "Skipping row $i because of missing $fieldName<br>";
  					next ROW;
				}
 
				if ( $curVal) {
					my $val = $curVal;
					$val =~ s/\Q'/''/g;
					# $val =~ s/\Q"/""/g;  # tone -- commented out
					$val = "'$val'" if $fieldTypes[$fieldCount] == 0;
					
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
 
			push(@fieldList, 'modifier');;
			push(@row, $dbh->quote($s->get('enterer')));
#			push(@fieldList, 'authorizer', 'enterer', 'modifier');;
#			push(@row, $dbh->quote($s->get('authorizer')), $dbh->quote($s->get('enterer')), $dbh->quote($s->get('enterer')));

			push ( @fieldList, 'reference_no');
			push ( @row, $s->get('reference_no'));
 
			# Check for duplicates
			$return = checkDuplicates ( "reid_no", \$recID, "reidentifications", \@fieldList, \@row );
			if ( ! $return ) { return $return; }

			if ( $return != $DUPLICATE ) {
				$sql = "INSERT INTO reidentifications (" . join(',', @fieldList).") VALUES (".join(', ', @row) . ")" || die $!;
				$sql =~ s/\s+/ /gms;
				dbg ( "$sql<HR>" );
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
      
				$recID = $dbh->{'mysql_insertid'};
				if ($#successfulRows == -1)	{
					print "<center><h3><font color='red'>Newly entered reidentifications</font></h3></center>\n";
				}
			} else {
				print "<center><h3>The following reidentifications already were entered</h3></center>\n";
			}

			$sql = "SELECT * FROM reidentifications WHERE reid_no=$recID";
			dbg ( "$sql<HR>" );
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			my @retrievedRow = $sth->fetchrow_array();
			splice @retrievedRow, 0, 3;
			splice @retrievedRow, $#retrievedRow - 5, 6;
			my @colnames = ("Reid no", "Occurrence", "Collection",
              "Reference", "Corrected name", "", "", "", "Comments" );
			$sth->finish();
			my $rowString = "<tr><td>";
			for $i (0..$#retrievedRow)	{
				if ($retrievedRow[$i] ne "" ||
					$colnames[$i] eq "Corrected name")	{
					if ($colnames[$i] ne "")	{
						if ($colnames[$i] eq "Corrected name" ||
							$colnames[$i] eq "Comments")	{
							$rowString .= "</td></tr>\n<tr><td colspan=4>";
						}
						else	{
							$rowString .= "</td><td>";
						}
						$rowString .= "<b>" . $colnames[$i] . ":</b>&nbsp;";
					}
					$rowString .= $retrievedRow[$i] . " ";
				}
			}
			$rowString .= "<p></td></tr><tr>";
			push(@successfulRows, $rowString);
		}

	print "<table align=center>\n";
	foreach $successfulRow (@successfulRows)	{
  		print $successfulRow;
   	}
   	print "</table><p>\n";
	print "<p align=center><b><a href=\"$exec_url?action=displayReIDCollsAndOccsSearchForm\">Reidentify more occurrences</a></b></p>\n";

	print &stdIncludes ("std_page_bottom");
}

# JA 13.8.02
sub displayTaxonomySearchForm	{

	print &stdIncludes ("std_page_top");

	print $hbo->populateHTML('search_taxonomy_form');

	print &stdIncludes ("std_page_bottom");
}

# JA 17.8.02
sub processTaxonomySearch	{

	my $taxon = $q->param('taxon_name');

	# Try to find this taxon in the authorities table

	my $sql = "SELECT * FROM authorities WHERE taxon_name='" . $taxon . "'";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $matches;
	my $html;
	while ( my %authorityRow = %{$sth->fetchrow_hashref()} )	{
		$matches++;
		$html .= "<tr><td><input type=radio name=taxon_no value=";
		$html .= $authorityRow{'taxon_no'};
		# Check the button if this is the first match, which forces
		#  users who want to create new taxa to check another button
		if ( $matches == 1 )	{
			$html .= " checked";
		}
		$html .= "> </td><td>";
		$html .= &formatAuthorityLine( \%authorityRow );
		# Print the name
		$html .= "</td></tr>\n";
	}
	$sth->finish();

	# If there were no matches, present the entry form immediately
	if ( $matches == 0 )	{
		&displayTaxonomyEntryForm();
	}
	# Otherwise, print a form so the user can pick the taxon
	else	{
		print &stdIncludes ("std_page_top");
		print "<center>\n";
		print "<h3>Which \"$taxon\" do you mean?</h3>\n";
		print "<form method=\"POST\" action=\"bridge.pl\">\n";
		print "<input type=hidden name=\"action\" value=\"displayTaxonomyEntryForm\">\n";
		print "<input type=hidden name=\"taxon_name\" value=\"$taxon\">\n";
		print "<table>\n";
		print $html;
		print "<tr><td><input type=radio name=taxon_no value=''> </td><td>None of the above - create a <b>new</b> taxon record</i></td></tr>\n";
		print "</table><p>\n";
		print "<input type=submit value=\"Submit\">\n</form>\n";
		print "</center>\n";
		print &stdIncludes ("std_page_bottom");
	}

}

# JA 17,20.8.02

sub formatAuthorityLine	{

	my $taxDataRef = shift;
	my %taxData = %{$taxDataRef};

	my $authLine;

	# Print the name
	if ( $taxData{'taxon_rank'} =~ /(species)|(genus)/ )	{
		$authLine .= "<i>";
	}
	$authLine .= "$taxData{'taxon_name'} ";
	if ( $taxData{'taxon_rank'} =~ /(species)|(genus)/ )	{
		$authLine .= "</i>";
	}
	# If the authority is a PBDB ref, retrieve and print it
	if ( $taxData{'ref_is_authority'} )	{
		my $sql = "SELECT * FROM refs WHERE reference_no=";
		$sql .= $taxData{'reference_no'};
		my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
		my %refRow = %{$sth2->fetchrow_hashref()} ;
		$authLine .= &formatShortRef( \%refRow );
		$sth2->finish();
	}
	# Otherwise, use what authority info can be found
	else	{
		$authLine .= &formatShortRef( \%taxData );
	}

	return $authLine;
}

# JA 13-20.8.02
sub displayTaxonomyEntryForm	{

	print &stdIncludes ("js_tipsPopup");

	my $taxon = $q->param('taxon_name');
	my $author;

	print &stdIncludes ("std_page_top");

	# If the taxon already is known, get data about it from
	#  the authorities table
	my %authorityRow;
	if ( $q->param('taxon_no') )	{
		my $sql = "SELECT * FROM authorities WHERE taxon_no=" . $q->param('taxon_no');
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		%authorityRow = %{$sth->fetchrow_hashref()};
		$sth->finish();

		# Retrieve the authorizer name (needed below)
		my $sql = "SELECT name FROM person WHERE person_no=";
		$sql .= $authorityRow{'authorizer_no'};
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		$authorityRow{'authorizer'} = ${$sth->fetchrow_arrayref()}[0];
		$sth->finish();

		# Retrieve the type taxon name or type specimen name, as appropriate
		if ( $authorityRow{'type_taxon_no'} )	{
			my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=";
			$sql .= $authorityRow{'type_taxon_no'};
			my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			$authorityRow{'type_taxon_name'} = ${$sth->fetchrow_arrayref()}[0];
			$sth->finish();
		}
		elsif ( $authorityRow{'type_specimen'} )	{
			$authorityRow{'type_taxon_name'} = $authorityRow{'type_specimen'};
		}
	}

	# Print the entry form

	# Determine the fields and values to be populated by populateHTML
	my @authorityFields = ( "taxon_no", "taxon_rank", "type_taxon_name", "ref_is_authority", "author1init", "author1last", "author2init", "author2last", "otherauthors", "pubyr", "comments");
	for $f ( @authorityFields )	{
		if ( $f ne "taxon_rank" || $authorityRow{$f} ne "" )	{
			push @authorityVals, $authorityRow{$f};
		} elsif ( $taxon =~ / / )	{
			push @authorityVals, "species";
		} else	{
			push @authorityVals, "genus";
		}
	}

	# Retrieve the taxon's primary ref or (failing that) the current ref
	#   so it can be displayed

	my $sql;
	if ( $authorityRow{'reference_no'} )	{
		$sql = "SELECT * FROM refs WHERE reference_no=" . $authorityRow{'reference_no'};
	} else	{
		$sql = "SELECT * FROM refs WHERE reference_no=" . $s->get("reference_no");
	}
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my %refHash = %{$sth->fetchrow_hashref()};
	$sth->finish();

	# If the authority is the primary ref for the record, then retrieve
	#  the ref data from the refs table
	# Don't do this for comments because those would apply to the ref,
	#  not the naming event
	if ( $authorityRow{'ref_is_authority'} )	{
		for my $r ( @authorityFields )	{
			if ( $refHash{$r} && $r ne "comments" )	{
				$authorityRow{$r} = $refHash{$r};
			}
		}
	}
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @refRow = $sth->fetchrow_array();
	my @refFieldNames = @{$sth->{NAME}};
	$sth->finish();
	my $refRowString = '<table>' . $hbo->populateHTML('reference_display_row', \@refRow, \@refFieldNames) . '</table>';

	# Add gray background and make font small
	$refRowString =~ s/<tr>/<tr bgcolor='#E0E0E0'>/;
	$refRowString =~ s/<td/<td class='small' /;
	push @authorityVals, $refRowString;
	push @authorityFields , 'ref_string';

	my @opinionFields = ( "opinion_author1init", "opinion_author1last", "opinion_author2init", "opinion_author2last", "opinion_otherauthors", "opinion_pubyr", "opinion_comments");
	for $f ( @opinionFields )	{
		push @authorityFields, $f;
		push @authorityVals, $authorityRow{$f};
	}

	# If this is a species, prepopulate the "Valid" name field in the
	#   status section
	if ( $taxon =~ / / )	{
		unshift @authorityVals, $taxon;
	} else	{
		unshift @authorityVals, '';
	}
	unshift @authorityFields, 'parent_taxon_name';

	$html = &stdIncludes("js_taxonomy_checkform");
	$html .= $hbo->populateHTML ('enter_taxonomy_top', [ $authorityRow{'taxon_no'}, $authorityRow{'type_taxon_no'} ] , [ 'taxon_no', 'type_taxon_no' ] );
	$html .= $hbo->populateHTML('enter_tax_ref_form', \@authorityVals , \@authorityFields );
	$html .= $hbo->populateHTML('enter_taxonomy_form', \@authorityVals , \@authorityFields );

	# Remove widgets if the current authorizer does not own the record and
	#  the existing data are non-null
	for $f ( @authorityFields )	{
		if ( $authorityRow{$f} &&
			 $s->get('authorizer') ne $authorityRow{'authorizer'} )	{
			$html =~ s/<input name="$f".*?>/<u>$authorityRow{$f}<\/u>/;
			$html =~ s/<select name="$f".*?<\/select>/<u>$authorityRow{$f}<\/u>/;
			$html =~ s/<textarea name="$f".*?<\/textarea>/<u>$authorityRow{$f}<\/u>/;
		}
	}

	# If the taxon already is known, look for known opinions about it
	if ( $authorityRow{'taxon_name'} )	{
		my $opinion = &printTaxonomicOpinions( $taxon, 0 );

		if ( $opinion )	{
			my $preOpinion = "<hr>\n<center><h4>Previously entered opinions on the status of $taxon</h4></center>\n\n<center><table><tr><td>\n";
			my $postOpinion = "</td></tr></table></center>";
			$opinion = $preOpinion . $opinion . $postOpinion;
			$html =~ s/<!-- OPINIONS -->/$opinion/g;
		}
	}

	# Customize the status fields
	if ( $taxon =~ / / )	{
		# Following substitution also changes JavaScript
		$html =~ s/Type taxon's name/Type specimen/g;
		$html =~ s/Valid/Valid species/;
		$html =~ s/, classified as belonging to/, combined or recombined as/;
		$html =~ s/value="belongs_to"/value="recombined"/;
	}
	else	{
		$html =~ s/Type taxon's name:<\/b>/Type taxon's name:<\/b><br><span class=tiny>e.g., "<i>Homo sapiens<\/i>"<\/span>/g;
		$html =~ s/Valid/Valid higher taxon/;
	}

	# Substitute in the taxon name
	$html =~ s/\$taxon_name/$taxon/g;

	print $html;

	print &stdIncludes ("std_page_bottom");
}

# JA 15-22,27.8.02
sub processTaxonomyEntryForm	{

# Figure out the parent taxon name
	# The parent of a synonym is the senior synonym
	if ( $q->param('taxon_status') eq "invalid1" )	{
		$q->param(parent_taxon_name => $q->param('parent_taxon_name2') );
		# senior synonym's rank is just the taxon's rank
		$q->param(parent_taxon_rank => $q->param('taxon_rank') );
	}

	# Set the parent name for valid species
	# Don't set the parent taxon rank if the parent is a genus,
	#  because that information will be taken from the form
	if ( $q->param('taxon_name') =~ / / && $q->param('taxon_status') !~ /invalid/ )	{
		# If no parent name is given, use the genus name
		if ( ! $q->param('parent_taxon_name') )	{
			my ($genus,$species) = split / /,$q->param('taxon_name');
			$q->param(parent_taxon_name => $genus );
		}
		# If a parent name is given...
		else	{
			my ($genus,$species) = split / /,$q->param('parent_taxon_name');
			# Use the genus name if the original combination is valid
			if ( $q->param('taxon_name') eq $q->param('parent_taxon_name') )	{
				$q->param(parent_taxon_name => $genus );
			}
			# Otherwise, use the new combination's name and set the
			#  parent genus name parameter
			else	{
				$q->param(parent_genus_taxon_name => $genus );
				# new combination's rank is just the taxon's rank
				$q->param(parent_taxon_rank => $q->param('taxon_rank') );
			}
		}
	}

# If an unrecognized type or parent taxon name was entered, stash the form data
#  and ask if the user wants to add the name to the authorities table
# WARNING: unrecognized parent names won't be dealt with immediately if
#  unrecognized type taxon names are seen first

	if ( $q->param('type_taxon_name') && $q->param('taxon_name') !~ / / )	{
		$matches = &checkNewTaxon('type', 'YES');
		if ( $matches == 1 ) {
		# So far so good, but parent name still needs checking
			if ( $q->param('parent_taxon_name') )	{
				$matches = &checkNewTaxon('parent', 'YES');
			}
		# Go straight to the results display page if everything checked out
		# If not, the user will have gotten a form by now anyway
			if ( $matches == 1 ) {
				&displayTaxonomyResults();
			}
		}
	}
	elsif ( $q->param('parent_taxon_name') )	{
	 # Check the genus name if this is a recombination
		if ( $q->param('parent_genus_taxon_name') )	{
			$matches = &checkNewTaxon('parent_genus', 'YES');
		# If the genus is OK, check the recombination itself, but don't
		#  waste time getting extra (redundant) authority info
			if ( $matches == 1 ) {
				$matches = &checkNewTaxon('parent', 'NO');
				if ( $matches < 2 ) {
					&displayTaxonomyResults();
				}
			}
		}
	 # Display results if this is a "valid as originally combined" species
		elsif ( $q->param('parent_taxon_name') eq $q->param('taxon_name') )	{
			&displayTaxonomyResults();
		}
	 # Otherwise check the full name
		else	{
			$matches = &checkNewTaxon('parent', 'YES');
			if ( $matches == 1 ) {
				&displayTaxonomyResults();
			}
		}
	}
	# Go straight to the results display page if there are no problem names
	else	{
		&displayTaxonomyResults();
	}

}

sub checkNewTaxon	{

	my ($stemName,$moreData) = @_;
	$new_taxon_name = $stemName . "_taxon_name";
	$new_taxon_no = $stemName . "_taxon_no";

		$sql = "SELECT * FROM authorities WHERE taxon_name='";
		$sql .= $q->param($new_taxon_name) . "'";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my $matches;
		my $matchList;
		my %taxonRow;
		while ( %taxonRow = %{$sth->fetchrow_hashref()} )	{
			$matches++;
			$lastNo = $taxonRow{'taxon_no'};
			my $authority = "<tr><td><input type=\"radio\" name=\"$new_taxon_no\"";
			$authority .= " value=\"". $taxonRow{'taxon_no'} . "\"> ";
			$authority .= &formatAuthorityLine( \%taxonRow );
			$authority .= "</td></tr>\n";
			$matchList .= $authority;
		}
		$sth->finish();
		my @refParams;
		# Stash all the query params as hidden values
		# WARNING: some fields don't correspond to database fields, e.g.,
		#  type_taxon_name, parent_taxon_name2, and parent_genus_taxon_name
		if ( $matches != 1 )	{
			print &stdIncludes ("std_page_top");
			@insertParams = ("taxon_rank", "type_taxon_name",
					"author1init", "author1last",
					"author2init", "author2last", "otherauthors",
					"pubyr", "comments");
			my @taxonParams = ("taxon_no", "taxon_name",
					"ref_is_authority",
					"taxon_status", "parent_taxon_name", "parent_taxon_rank",
					"synonym", "parent_taxon_name2",
					"parent_genus_taxon_name", "nomen",
					"ref_has_opinion",
					"opinion_author1init", "opinion_author1last",
					"opinion_author2init", "opinion_author2last",
					"opinion_otherauthors",
					"opinion_pubyr", "opinion_comments");
			push @taxonParams, @insertParams;
			print "<form method=\"POST\" action=\"bridge.pl\" onSubmit=\"return checkForm();\">\n";
			for my $p ( @taxonParams )	{
				if ( $q->param($p) )	{
					print "<input type=hidden name=\"$p\" value=\"";
					print $q->param($p) , "\">\n";
				}
			}
		}
		# Can't find the taxon? Get some info on it and submit it
		if ( $matches == 0 && $moreData eq "YES" )	{
			my $new_name = $q->param($new_taxon_name);
			my $html = $hbo->populateHTML('enter_authority_top', [ $new_name ], [ 'taxon_name' ] );
			$html =~ s/missing_taxon_name/$new_name/;
			if ( $stemName =~ /type/ )	{
				if ( $new_name =~ / / )	{
					$html =~ s/no authority data for/no authority data for the type species/;
				} else	{
					$html =~ s/no authority data for/no authority data for the type taxon/;
				}
			} else	{
				$html =~ s/no authority data for/no authority data for the parent taxon/;
			}
			print $html;
			# Print the ref info part of the form, which has no populated vals
			#  other than the taxon rank pulldown, the usual ref fields, and
			#  the current ref
			my @tempParams = ( "ref_is_authority", "author1init", "author1last",
					"author2init", "author2last", "otherauthors",
					"pubyr", "comments");
			my @tempVals = ( "YES", "", "",  "", "", "",  "", "" );
			unshift @tempParams, 'taxon_rank';
			if ( $taxon =~ / / )	{
				unshift @tempVals, "species";
			} else	{
				unshift @tempVals, "genus";
			}

			# Display the current ref
			my $sql = "SELECT * FROM refs WHERE reference_no=";
			$sql .= $s->get("reference_no");
			my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			my @refRow = $sth->fetchrow_array();
			my @refFieldNames = @{$sth->{NAME}};
			$sth->finish();
			my $refRowString = '<table>' . $hbo->populateHTML('reference_display_row', \@refRow, \@refFieldNames) . '</table>';

			# Add gray background and make font small
			$refRowString =~ s/<tr>/<tr bgcolor='#E0E0E0'>/;
			$refRowString =~ s/<td/<td class='small' /;
			push @tempVals, $refRowString;
			push @tempParams, 'ref_string';

			$html = $hbo->populateHTML ("enter_tax_ref_form", \@tempVals, \@tempParams );

			# Suppress the type taxon field because this name would have
			#  to be checked against the authorities table, and life is
			#  already way too complicated!
			$html =~ s/Type taxon's name://;
			$html =~ s/<input name="type_taxon_name" size=50>//;
			$html =~ s/<input id="type_taxon_name" size=50>//;

			# Make sure all the "new" ref field names are modified so
			#  they can be retrieved when the form data are processed
			for my $p ( @insertParams )	{
				my $newp = $stemName . "_" . $p;
				$html =~ s/name="$p/name="$newp/;
			}
			$html =~ s/\$taxon_name/$new_name/;
			print $html;
			print "<center><input type=submit value=\"Submit\"></center>\n</form>\n";
			print &stdIncludes ("std_page_bottom");
		} elsif ( $matches > 1 )	{
			print "<input type=hidden name=\"action\" value=\"displayTaxonomyResults\">";
			print "<center>\n<h4>There are several taxa called \"";
			print $q->param($new_taxon_name), "\"</h4>\n";
			print "<p>Please select one and hit submit.</p>\n";
			print "<table>\n";
			print $matchList;
			print "</table>\n";
			print "<input type=submit value=\"Submit\">\n</center>\n</form>\n";
			print &stdIncludes ("std_page_bottom");
		} elsif ( $matches == 1 ) {
	# Save the taxon number if there's only one match
			$q->param($new_taxon_no => $lastNo );
		}

	return $matches;
}


# JA 13-18,27.8.02
sub displayTaxonomyResults	{

	print &stdIncludes ("std_page_top");

# Process the form data relevant to the authorities table

	# Update or insert the authority data, as appropriate
	# Assumption here is that you definitely want to create the taxon name
	#  if it doesn't exist yet
	my $taxon_no;
	if ( $q->param('taxon_no') > 0 )	{
		updateRecord( 'authorities', 'taxon_no', $q->param('taxon_no') );
	} else	{
		$q->param(reference_no => $s->get('reference_no') );
		insertRecord( 'authorities', 'taxon_no', \$taxon_no, '9', 'taxon_name' );
		$q->param(taxon_no => $taxon_no);
		$taxon_is_new = 1;
	}

	# if species is valid as originally combined, parent no = species no
	if ( $q->param('parent_taxon_name') eq $q->param('taxon_name') )	{
		$q->param(parent_taxon_no => $q->param('taxon_no') );
	}

	my @insertFields = ("taxon_no","taxon_rank",
				"type_taxon_no", "type_specimen",
				"ref_is_authority", "author1init", "author1last",
				"author2init", "author2last", "otherauthors",
				"pubyr", "comments");

# Process authority data on a new type and/or parent taxon name, if
#  either was submitted on a previous page

	if ( ( $q->param('parent_taxon_name') && ! $q->param('parent_taxon_no') ) ||
		 ( $q->param('parent_genus_taxon_name') &&
		   ! $q->param('parent_genus_taxon_no') ) ||
		 ( $q->param('type_taxon_name') && ! $q->param('type_taxon_no') &&
		   $q->param('taxon_name') !~ / / ) )	{
		# Save the data for the focal taxon
		my %savedParams;
		my $savedName = $q->param('taxon_name');
		for my $f ( @insertFields )	{
			$savedParams{$f} = $q->param($f);
		}

		if ( $q->param('type_taxon_name') && ! $q->param('type_taxon_no') &&
			 $q->param('taxon_name') !~ / / )	{
			$my_taxon_no = &insertSwappedTaxon('type_', \@insertFields);
			$savedParams{'type_taxon_no'} = $my_taxon_no;
		}

		# Do exactly the same thing for the parent taxon
		if ( $q->param('parent_taxon_name') && ! $q->param('parent_taxon_no') )	{
			$my_taxon_no = &insertSwappedTaxon('parent_', \@insertFields);
			$q->param( parent_taxon_no => $my_taxon_no );
		}

		# Do exactly the same thing for the parent genus (relevant only
		#   if a species is recombined into a "new" genus)
		if ( $q->param('parent_genus_taxon_name') && ! $q->param('parent_genus_taxon_no') )	{
			$my_taxon_no = &insertSwappedTaxon('parent_genus_', \@insertFields);
			$q->param( parent_genus_taxon_no => $my_taxon_no );
		}

		# Reset the saved parameters
		$q->param(taxon_name => $savedName );
		for my $f ( @insertFields )	{
			if ( $savedParams{$f} )	{
				$q->param($f => $savedParams{$f} );
			} else	{
				$q->param($f => '' );
			}
		}
	}

	# Fix up the type taxon/specimen fields now that all the taxa
	#  definitely are in the authorities table
	my @prefixes = ("", "parent_");
	for my $pf ( @prefixes )	{
		my $tnm = $pf . "taxon_name";
		my $tno = $pf . "taxon_no";
		my $ttnm = $pf . "type_taxon_name";
		my $ttno = $pf . "type_taxon_no";
		my $ts = $pf . "type_specimen";
		if ( $q->param($ttnm) )	{
			# For a higher taxon, the type taxon's ID number should have
			#  been set above
			my $sql;
			if ( $q->param($tnm) !~ / / )	{
				$sql = "UPDATE authorities SET type_taxon_no=";
				$sql .= $q->param($ttno);
				$sql .= " WHERE taxon_no=";
				$sql .= $q->param($tno);
			}
			# For a species, shift the name into the type specimen field
			else	{
				$q->param($ts => $q->param($ttnm) );
				$sql = "UPDATE authorities SET type_specimen='";
				$sql .= $q->param($ts);
				$sql .= "' WHERE taxon_no=";
				$sql .= $q->param($tno);
			}
			my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			$sth->finish();
		}
	}


# Process the form data relevant to the opinions table

	# Set values implied by selection of radio buttons
	if ( $q->param('taxon_status') eq "belongs_to" )	{
		$q->param(status => 'belongs to');
	} elsif ( $q->param('taxon_status') eq "recombined" )	{
		$q->param(status => 'recombined as');
		# If the parent name is not a species, the species simply has
		#  been assigned to a genus, so the status is "belongs to"
		if ( $q->param('parent_taxon_name') !~ / / )	{
			$q->param(status => 'belongs to');
		}
	} elsif ( $q->param('taxon_status') eq "invalid1" )	{
		$q->param(status => $q->param('synonym') );
	} elsif ( $q->param('taxon_status') eq "invalid2" )	{
		$q->param(status => $q->param('nomen') );
	}

	# If any status has been recorded, retrieve the ID number
	#  of the child and parent taxa
	if ( $q->param('status') )	{
		$q->param(child_no => $q->param('taxon_no') );
		$q->param(parent_no => $q->param('parent_taxon_no') );

#		my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
#		$sql .= $q->param('taxon_name') . "'";
#		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
#		$sth->execute();
#		$q->param(child_no => ${$sth->fetchrow_arrayref()}[0] );
#		$sth->finish();

#		my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
#		$sql .= $q->param('parent_taxon_name') . "'";
#		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
#		$sth->execute();
#		$q->param(parent_no => ${$sth->fetchrow_arrayref()}[0] );
#		$sth->finish();

		if ( $q->param('parent_genus_taxon_name') )	{
#			my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
#			$sql .= $q->param('parent_taxon_name') . "'";
#			my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
#			$sth->execute();
#			$q->param(parent_no => ${$sth->fetchrow_arrayref()}[0] );
#			$sth->finish();
		}
	}

	# Set ref info for the opinion, because same field names are used
	#  in the auth and opinions tables
	if ( $q->param('opinion_author1last') )	{
		$q->param(author1init => $q->param('opinion_author1init') );
		$q->param(author1last => $q->param('opinion_author1last') );
		$q->param(author2init => $q->param('opinion_author2init') );
		$q->param(author2last => $q->param('opinion_author2last') );
		$q->param(otherauthors => $q->param('opinion_otherauthors') );
		$q->param(pubyr => $q->param('opinion_pubyr') );
		$q->param(comments => $q->param('opinion_comments') );
	} else	{
		$q->param(author1init => '');
		$q->param(author1last => '');
		$q->param(author2init => '');
		$q->param(author2last => '');
		$q->param(otherauthors => '');
		$q->param(pubyr => '');
		$q->param(comments => '');
	}

	# If a new opinion was submitted, insert it
	my @lastOpinions;
	if ( $q->param('status') )	{
#printf "C(%d) P(%d)<br>",$q->param('child_no'),$q->param('parent_no');
#printf "TAX(%s = %d),P(%s = %d),G(%s = %d)<br>",$q->param('taxon_name') ,$q->param('taxon_no'), $q->param('parent_taxon_name'), $q->param('parent_taxon_no') , $q->param(parent_genus_taxon_name), $q->param('parent_genus_taxon_no');
		my $opinion_no;
		# Make sure the ref no was set (won't happen if the child taxon
		#  already existed)
		$q->param(reference_no => $s->get('reference_no') );
		push @lastOpinions , insertRecord( 'opinions', 'opinion_no', \$opinion_no, '9', 'parent_no' );
	}

	# If a species was recombined, create another opinion to record
	#  that the new combination belongs to the new genus
	if ( $q->param('parent_genus_taxon_no') )	{
		$q->param(child_no => $q->param('parent_taxon_no') );
		$q->param(parent_no => $q->param('parent_genus_taxon_no') );
		# The relation is no longer "recombined as"
		$q->param(status => 'belongs to');
#printf "2C(%d) P(%d)<br>",$q->param('child_no'),$q->param('parent_no');
#printf "2TAX(%s = %d),P(%s = %d),G(%s = %d)<br>",$q->param('taxon_name') ,$q->param('taxon_no'), $q->param('parent_taxon_name'), $q->param('parent_taxon_no') , $q->param(parent_genus_taxon_name), $q->param('parent_genus_taxon_no');
		my $opinion_no;
		# Don't save another copy of the comments
		$q->param(comments => '');
		push @lastOpinions , insertRecord( 'opinions', 'opinion_no', \$opinion_no, '9', 'parent_no' );
	}

# Print out the results

	my $taxon = $q->param('taxon_name');

	my $opinion = &printTaxonomicOpinions( $taxon, \@lastOpinions );
	if ( $opinion )	{
		if ( $taxon_is_new )	{
			print "<center><h4>$taxon has been entered into the Database</h4></center>\n\n";
			print "<center><p>Known opinions on the status of $taxon</center></p>\n\n<center><table><tr><td>";
		}
		else	{
			print "<center><h4>Known opinions on the status of $taxon</h4></center>\n\n<center><table><tr><td>";
		}
		print $opinion;
		print "</td></tr></table></center>";
	} elsif ( $taxon_is_new )	{
		print "<center><h4>$taxon has been entered into the Database</h4></center>\n\n";
	} else	{
		print "<center><h4>Revised data for $taxon have been entered into the Database</h4></center>\n\n";
	}

	my $cleanTaxonName = $q->param('taxon_name');
	$cleanTaxonName =~ s/ /\+/g;

	print "<p align=center><b><a href=\"$exec_url?action=displayTaxonomyEntryForm&taxon_name=" . $cleanTaxonName;
	print "&taxon_no=";
	print $q->param('taxon_no');
	print "\">Add more data about " . $q->param('taxon_name') . "</a></b> - ";
	print "<b><a href=\"$exec_url?action=displayTaxonomySearchForm\">Add more data about another taxon</a></b></p>\n";
	print &stdIncludes ("std_page_bottom");

}

# JA 27.8.02
sub insertSwappedTaxon	{

	my ($stemName, $insertFields) = @_;
	my @insertFields = @$insertFields;

	# Swap the type taxon name onto the taxon_name query param
	$q->param(taxon_name => $q->param($stemName . 'taxon_name') );

	# Swap the type taxon ref params into the focal taxon's query params
	for my $f ( @insertFields )	{
		my $pr = $stemName . $f;
		if ( $q->param($pr) )	{
			$q->param($f => $q->param($pr) );
		} else	{
			$q->param($f => '' );
		}
	}
	my $temp_taxon = $q->param('taxon_name');

	# Add the taxon
	# WARNING: by using taxon_no (the primary key) as the fifth field,
	# this fools checkNearTaxon into always adding the record
	insertRecord( 'authorities', 'taxon_no', \$my_taxon_no, '9', 'taxon_no' );

	print "<center><h4>$temp_taxon has been entered into the Database</h4></center>\n\n";

	return $my_taxon_no;
}

# JA 13-15.8.02
sub printTaxonomicOpinions	{

	my ($taxon, $lastOpinions) = @_;
	my @lastOpinions = @$lastOpinions;
	my $author;
	my $opinion;

	# Retrieve the child taxon's number from the authorities table
	my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='";
	$sql .= $taxon . "'";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my $child_no = ${$sth->fetchrow_arrayref()}[0];
	$sth->finish();

	my $sql = "SELECT * FROM opinions WHERE child_no=" . $child_no;
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Print all the opinions
	my $matches;
	while ( my %taxonRow = %{$sth->fetchrow_hashref()} )	{

		if ( $matches == 1 )	{
			$opinion .= "<ul>\n";
		}

		$opinion .= "<li>";

		# Highlight the brand-new opinion if there is one
		if ( $lastOpinions[0] == $taxonRow{'opinion_no'} && $lastOpinions[0] > 0 )	{
			$opinion .= "<font color=\"red\">";
		} elsif ( $lastOpinions[1] == $taxonRow{'opinion_no'} && $lastOpinions[1] > 0 )	{
			$opinion .= "<font color=\"red\">";
		}

		# Reword the status for a species still in its original combination
		# WARNING: this is a temporary fix made necessary by failure to
		#  map old combinations into new combinations in addition to genera
		if ( $taxonRow{'status'} eq "recombined as" )	{
			$taxonRow{'status'} = "placed in";
		}

		$opinion .= $taxonRow{'status'};

		# Retrieve the parent taxon's name and rank from the authorities table
		my $sql = "SELECT taxon_name,taxon_rank FROM authorities WHERE taxon_no=";
		$sql .= $taxonRow{'parent_no'};
		my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
		my ($parent_taxon_name,$parent_taxon_rank) = @{$sth2->fetchrow_arrayref()};
		$sth2->finish();

		if ( $parent_taxon_name ne "" )	{
			if ( $parent_taxon_rank =~ /species/ ||
				 $parent_taxon_rank =~ /genus/ )	{
				$opinion .= "<i>";
			}
			$opinion .=  " ". $parent_taxon_name;
			if ( $parent_taxon_rank =~ /species/ ||
				 $parent_taxon_rank =~ /genus/ )	{
				$opinion .= "</i>";
			}
		}
		if ( $lastOpinions[0] == $taxonRow{'opinion_no'} && $lastOpinions[0] > 0 )	{
			$opinion .= "</font>";
		} elsif ( $lastOpinions[1] == $taxonRow{'opinion_no'} && $lastOpinions[1] > 0 )	{
			$opinion .= "</font>";
		}
		$opinion .= ": ";

		# If the opinion data are from a secondary report, print the
		#  second-hand data for the original report
		if ( $taxonRow{'author1last'} )	{
			$opinion .= &formatShortRef( \%taxonRow );
		}

		# Get the PBDB ref info
		my $sql = "SELECT * FROM refs WHERE reference_no=";
		$sql .= $taxonRow{'reference_no'};
		my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth2->execute();
		my %refRow = %{$sth2->fetchrow_hashref()};
		$sth2->finish();

		# If the data are from a secondary report, add some formatting
		if ( $taxonRow{'author1last'} )	{
			$opinion .= " (see also ";
		}
		$opinion .= "<a href=$exec_url?action=displayRefResults&reference_no=";
		$opinion .= $taxonRow{'reference_no'} . ">";
		$opinion .= &formatShortRef( \%refRow );
		$opinion .= "</a>";
		if ( $taxonRow{'author1last'} )	{
			$opinion .= ")";
		}

		$opinion .= "\n";
	}

	if ( $matches > 0 )	{
		$opinion .= "</ul>\n";
	}

	$sth->finish();

	return $opinion;

}

# JA 16-17.8.02
sub formatShortRef	{

	my $refDataRef = shift;
	my %refData = %{$refDataRef};

	my $shortRef = $refData{'author1init'} . " " . $refData{'author1last'};
	if ( $refData{'otherauthors'} )	{
		$shortRef .= " et al.";
	} elsif ( $refData{'author2last'} )	{
		$shortRef .= " and " . $refData{'author2init'} . " ". $refData{'author2last'};
	}
	$shortRef .= " " . $refData{'pubyr'};

	return $shortRef;

}

sub displayEnterAuthoritiesForm
{
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=displayEnterAuthoritiesForm" );
		&displayLoginPage ( "Please log in first." );
		exit;
	}
		my $reference_no;
	  
		# If the nice user created a new reference, commit it
		# and get it's reference_no
		if ( $q->param('submit') ne "Use reference")
		{
			$return = insertRecord('refs', 'reference_no', \$reference_no, '5', 'author1last' );
			if ( ! $return ) { return $return; }

			print "<center><h3><font color='red'>Reference record ";
			if ( $return == $DUPLICATE ) {
   				print "already ";
			}
			print "added</font></h3></center>\n";
		} else {
			$reference_no = $q->param('reference_no');
		}
		$s->setReferenceNo ( $dbh, $reference_no );

		print &stdIncludes ( "std_page_top" );
 
		print '<table>';
		print qq|<form method=POST action="$exec_url">\n|;
		print qq|<input type=hidden name=action value=processNewAuthorities>\n|;
		print qq|<input type=hidden name=reference_no value="$reference_no">\n|;
		print $hbo->populateHTML('authority_header_row');
		for($i = 0;$i < 20;$i++)
		{
 			print $hbo->populateHTML('authority_entry_row', ['genus', 'species', ''], ['taxon_rank', 'type_taxon_rank', 'body_part']);
		}
		print '</table>';
		print '<input type=submit value="Add authorities">';
		
	  print &stdIncludes ("std_page_bottom");
}

#  5. * System processes new authorities.  System displays
#       authorities form.
sub processNewAuthorities
{
		print &stdIncludes ( "std_page_top" );
    
		# Get the database metadata
		$sql = "SELECT * FROM authorities WHERE taxon_no=0";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		# Get a list of field names
		my @fieldNames = @{$sth->{NAME}};
		# Get a list of field types
		my @fieldTypes = @{$sth->{mysql_is_num}};
		# Get the number of fields
		my $numFields = $sth->{NUM_OF_FIELDS};
		# Get the null constraints - OBSOLETE, BUT NOT REPLACED YET!!
		my @nullables = @{$sth->{NULLABLE}};
		# Get the types
		my @types = @{$sth->{mysql_type_name}};
		# Get the primary key data
		my @priKeys = @{$sth->{mysql_is_pri_key}};
		#print join(',', @types);
		$sth->finish();
		
		# This hash contains references to arrays of hashref rows
		my %T_NAME_IN_DB_ROWS;
		my @successfulRows;
		my @rowTokens = $q->param('row_token');
		my $numRows = @rowTokens;
		print "Num Rows: $numRows";
		# Iterate over the rows, and commit each one
		ROW:
		for(my $i = 0;$i < $numRows;$i++)
		{
			my @fieldList;
			my @row;
		# added 11.7.02 JA (omitted by Garot!)
			push(@fieldList, 'created');
			push(@row, '"' . now() . '"');
			for(my $j = 0;$j < $numFields;$j++)
			{
				my $fieldName = $fieldNames[$j];
				my @tmpVals = $q->param($fieldName);
				my $curVal = $tmpVals[$i];
				
				next if  $fieldName eq 'reference_no';
				# Skip rows that don't have a required data item
				unless($nullables[$j] || ($types[$j] eq 'timestamp') || $priKeys[$j] || $curVal)
				{
					print "Skipping row $i because of missing $fieldName<br>";
  					next ROW;
				}

				# Check for duplicate taxon name
				if ( $fieldName eq 'taxon_name')
				{
					$sql = "SELECT * FROM authorities WHERE taxon_name='$curVal'";
					$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
					$sth->execute();
					my @rowrefs = @{$sth->fetchall_hashref()};
					foreach my $rowref (@rowrefs)
					{
						push(@{$T_NAME_IN_DB_ROWS{$curVal}}, $rowref);
					}
				}
        
				#if ( $q->param($fieldName))
				if ( $curVal)
				{
					my $val = $curVal;
					$val =~ s/\Q'/''/g;
					# $val =~ s/\Q"/""/g;	# tone -- commented out
					#$val =~ s/%/\\%/g;
					$val = "'$val'" if $fieldTypes[$fieldCount] == 0;
					
 					push(@row, $val);
					push(@fieldList, $fieldName);
				}
				elsif ( defined $q->param($fieldName))
				{
					push(@row, 'NULL');
					push(@fieldList, $fieldName);
				}
				print "<br>$i $fieldName: $val";
			}

			push ( @fieldList, 'reference_no');
			push ( @row, $q->param('reference_no'));
 
			# Check for duplicates
			$return = checkDuplicates ( "taxon_no", \$recID, "authorities", \@fieldList, \@row );
			if ( ! $return ) { return $return; }

			if ( $return != $DUPLICATE ) {
				$sql = "INSERT INTO authorities (" . join(',', @fieldList) . ") VALUES (" . join(', ', @row) . ")" || die $!;
				$sql =~ s/\s+/ /gms;
				dbg ( "$sql<HR>" );
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
 
				my $recID = $dbh->{'mysql_insertid'};
			}

			$sql = "SELECT * FROM authorities WHERE taxon_no=$recID";
			dbg ( "$sql<HR>" );
			$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
			$sth->execute();
			my @retrievedRow = $sth->fetchrow_array();
			$sth->finish();
			my $rowString = "<tr><td>" . join("</td><td>", @retrievedRow) . "</td></tr>";
			push(@successfulRows, $rowString);
		}
		
	print "<table>\n";
	foreach $successfulRow (@successfulRows)	{
  		print $successfulRow;
   	}
   	print "</table>\n";
		
	print &stdIncludes ("std_page_bottom");
}

sub displaySearchDLGenusNamesForm
{
	print &stdIncludes ( "std_page_top" );
	print $hbo->populateHTML('search_genus_names');
	print &stdIncludes ("std_page_bottom");
}

sub displayGenusNamesDLResults
{
	my $class_name = $q->param('class_name');
	$sql = "SELECT genus_name FROM jab_class_genus_index WHERE class_name='$class_name'";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @rows = @{$sth->fetchall_arrayref()};
	
	print &stdIncludes ( "std_page_top" );
	
	my $numRows = @rows;
	if ( $numRows > 0)
	{
		my $fileName = $s->get('authorizer');
		$fileName .= '_' . $class_name;
		$fileName =~ s/\.//g;
		$fileName =~ s/\s+/_/g;
		
		open(OUTFILE, ">/home/httpd/html/dl/$fileName.csv") || die $!;
		
		foreach my $rowref (@rows)
		{
			my @row = @{$rowref};
			print OUTFILE $row[0] . "\n";
		}
		
		close OUTFILE;
		
		print qq|<h3>$numRows genus names in the Class $class_name were found</h3>Please download your file: <a href="/dl/$fileName.csv">$fileName.csv</a>\n|;
 	}
	else
	{
		print "<h1>No genus names for class $class_name</h1>";
	}
	
	print &stdIncludes ("std_page_bottom");
}


sub authorityRow
{
	print &stdIncludes ( "std_page_top" );
	print '<form><table>' . $hbo->populateHTML('authority_entry_row') . '</table></form>';
	print &stdIncludes ("std_page_bottom");
}

sub displayTaxonGeneralForm
{
		my $reference_no;
	  
		# If the nice user created a new reference, commit it
		# and get it's reference_no
		if ( $q->param('submit') ne "Use reference")
		{
			$return = insertRecord('refs', 'reference_no', \$reference_no, '5', 'author1last' );
			if ( ! $return ) { return $return; }

			print "<center><h3><font color='red'>Reference record ";
			if ( $return == $DUPLICATE ) {
   				print "already ";
			}
			print "added</font></h3></center>\n";
		} else {
			$reference_no = $q->param('reference_no');
		}
		$s->setReferenceNo ( $dbh, $reference_no );
	  
		print $hbo->populateHTML('taxon_general_page_top');
	  
		# Display the reference
		$sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my $refRowRef = $sth->fetchrow_arrayref();
		my $refFieldNamesRef = $sth->{NAME};
		$sth->finish();
		print $hbo->populateHTML('reference_display_row', $refRowRef, $refFieldNamesRef);

		print '<table>';
		print qq|<form method=POST action="$exec_url">\n|;
		print qq|<input type=hidden name=action value=processNewAuthorities>\n|;
		print qq|<input type=hidden name=reference_no value="$reference_no">\n|;
		for($i = 0;$i < 10;$i++)
		{
 			print $hbo->populateHTML('taxon_general_entry_row', ['genus', 'species', ''], ['taxon_rank', 'type_taxon_rank', 'body_part']);
		}
		print '</table>';
		print '<input type=submit value="Process taxa">';
		
	  print &stdIncludes ("std_page_bottom");
	  
}

sub displayProjectStatusPage
{
	  print &stdIncludes ( "std_page_top" );
	  print $hbo->populateHTML('project_status_page');
	  print &stdIncludes ("std_page_bottom");
}

sub displaySubmitBugForm
{
	  print &stdIncludes ( "std_page_top" );
	  print $hbo->populateHTML('bug_report_form', [$s->get('enterer'), 'Cosmetic'], ['enterer', 'severity']);
	  print &stdIncludes ("std_page_bottom");
}

sub processBugReport
{
		$q->param(enterer=>$s->get('enterer'));
		$q->param(date_submitted=>now());
		$return = insertRecord('bug_reports', 'bug_id', 0);
		if ( ! $return ) { return $return; }
    
		print &stdIncludes ( "std_page_top" );
		print "<h3>Your bug report has been added</h3>";
		print &stdIncludes ("std_page_bottom");
}

sub displayBugs {
	$sql = "SELECT * FROM bug_reports";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	  $sth->execute();
	  my $fieldNamesRef = $sth->{NAME};
	  my @rowrefs = @{$sth->fetchall_arrayref()};
	  $sth->finish();
	  
	  print &stdIncludes ( "std_page_top" );
	  print "<table border=0>";
	  foreach my $rowref (@rowrefs)
	  {
	    #print join(', ', @{$fieldNamesRef});
	    print $hbo->populateHTML('bug_display_row', $rowref, $fieldNamesRef);
	  }
	  print "</table>";
	  print &stdIncludes ("std_page_bottom");
}

sub updateRecord {

	# WARNING: guests never should get to this point anyway, but seem to do
	#  so by some combination of logging in, getting to a submit form, and
	#  going to the public page and therefore logging out, but then hitting
	#  the back button and getting back to the submit form
	# Have to be logged in
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=updateRecord" );
		&displayLoginPage ( "Please log in first." );
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
	if( ! $q->param('enterer') ){
		$q->param(enterer => $s->get("enterer"));
	}
	if( ! $q->param('authorizer') ){
		$q->param(authorizer => $s->get("authorizer"));
	}
	$q->param(modifier => $s->get("enterer"));			# This is an absolute
	# Set the pubtitle to the pull-down pubtitle unless it's set in the form
	$q->param(pubtitle => $q->param("pubtitle_pulldown")) unless $q->param("pubtitle");

	&setPersonValues( $tableName );

	my $updateString = "UPDATE $tableName SET ";
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
	my $updateString = $updateString . join(',', @updates) . " WHERE $idName=$id";
	$updateString =~ s/\s+/ /gms;
	dbg($updateString);

	# Trying to find why the modifier is sometimes coming through 
	# blank.  This should stop it.
	if ( $updateString !~ /modifier/ ) { &htmlError ( "modifier not specified" ); }

if ( $s->get("authorizer") eq "M. Uhen" ) { print "$updateString<br>\n"; } 
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
	if ($s->get('enterer') eq "Guest" || $s->get('enterer') eq "")	{
		$s->enqueue( $dbh, "action=insertRecord" );
		&displayLoginPage ( "Please log in first." );
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
	unless($fields_ref && $vals_ref){

		my $searchVal = $q->param($searchField);

		# Created data, unless specified.
		my $nowString = now();
		$q->param(created=>"$nowString") unless $q->param('created');

		# Get the database metadata
		$sql = "SELECT * FROM $tableName WHERE $idName=0";
		dbg ( "$sql<HR>" );
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();

		my @fieldNames = @{$sth->{NAME}};
		my @fieldTypes = @{$sth->{mysql_is_num}};	# To see which fields need quotes
		my @fieldTypeCodes = @{$sth->{mysql_type}};
		# Get the null constraints
		my @nullables = @{$sth->{NULLABLE}};
		$sth->finish();

		# Set a few defaults
		$q->param(enterer => $s->get('enterer'));			# This is an absolute
		$q->param(authorizer => $s->get('authorizer'));		# This is an absolute
		$q->param(modifier => $s->get('enterer')) unless $q->param('modifier');
		# Set the pubtitle to the pull-down pubtitle unless it's set in the form
		$q->param(pubtitle => $q->param('pubtitle_pulldown')) unless $q->param("pubtitle");

		&setPersonValues( $tableName );

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
				if ( $fieldTypeCodes[$fieldCount] == 254 ) {
					my $numSetValues = @formVals;
					if ( $numSetValues ) {
						$val = join(',', @formVals);
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
		$return = checkDuplicates ( $idName, $primary_key, $tableName, \@fields, \@vals );
		if ( ! $return || $return == $DUPLICATE ) { return $return; }

		# Check for near matches
		# NOTE: the below method now handles matches by giving the user a choice
		# of 'cancel' or 'continue' if a match is found. See method for details.
		checkNearMatch ( $matchlimit, $idName, $tableName, $searchField, $searchVal, \@fields, \@vals );

	} # END 'unless($fields_ref && $vals_ref)' - see top of method

	if($fields_ref && $vals_ref){
		@fields = @{$fields_ref};
		@vals = @{$vals_ref};
	}
	dbg("fields: @fields<br>vals: @vals<br>");

	# Insert the record
	my $valstring = join ',',@vals;
	$valstring =~ s/"/\\"/;
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
	# Show entered rec (and other, related recs?) and give a link to
	# do more of the same
	elsif($fields_ref && $vals_ref && ($tableName eq "opinions")){
		my $sql="SELECT * FROM opinions WHERE opinion_no=$recID";
		my @results = @{$dbt->getData($sql)};

		my $sql="SELECT * FROM authorities WHERE taxon_no=".
				$results[0]->{parent_no};
		my @other_results = @{$dbt->getData($sql)};

		print stdIncludes("std_page_top");
		print "<center><b>Record added:</b>";
		print "<table><tr>";
		print "<td>$results[0]->{status} $other_results[0]->{taxon_name} ".
			  "$results[0]->{author1last} $results[0]->{pubyr} ";

		my $sql="SELECT p1.name as name1, p2.name as name2, ".
				"p3.name as name3 ".
				"FROM person as p1, person as p2, person as p3 WHERE ".
				"p1.person_no=$results[0]->{authorizer_no} ".
				"AND p2.person_no=$results[0]->{enterer_no} ".
				"AND p3.person_no=$results[0]->{modifier_no}";
		my @results = @{$dbt->getData($sql)};

		print "<font class=\"tiny\">[".
			  "$results[0]->{name1}/$results[0]->{name2}/".
			  "$results[0]->{name3}]".
			  "</font></td>";
		print "</tr></table>";
		print "<p><a href=\"/cgi-bin/bridge.pl?action=startTaxonomy\">Enter more taxonomic information</a></center>";

		print stdIncludes("std_page_bottom");
	}
	elsif($fields_ref && $vals_ref && ($tableName eq "authorities")){
        my $sql="SELECT * FROM authorities WHERE taxon_no=$recID";
        my @results = @{$dbt->getData($sql)};

		print stdIncludes("std_page_top");
		print "<center><b>Record added:</b>";
        print "<table><tr>";
        print "<td>$results[0]->{taxon_rank} $results[0]->{taxon_name} ".
              "$results[0]->{author1last} $results[0]->{pubyr} ";

        print "</td></tr></table>";
		print "<p><a href=\"/cgi-bin/bridge.pl?action=startTaxonomy\">Enter more taxonomic information</a></center>";
		print stdIncludes("std_page_bottom");
	}
	else{
		print stdIncludes("std_page_top");
		print "<center><b>Record added.</b></center>";
		print stdIncludes("std_page_bottom");
	}

	$$primary_key = $recID;
	return 1;
}

# JA 15.8.02
sub setPersonValues	{

	my $tableName = shift;

	# If the table stores person numbers instead of names, retrieve them
	if ( $tableName eq "authorities" || $tableName eq "opinions" )	{
		my @personTypes = ("authorizer", "enterer", "modifier");
		for my $personType ( @personTypes )	{
			my $pt = "";
			$pt = $s->get($personType);
			if ( $personType eq "modifier" )	{
				$pt = $s->get('enterer');
			}
			if ( $pt )	{
				my $sql = "SELECT person_no FROM person WHERE name='";
				$sql .= $pt . "'";
				my $sth2 = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
				$sth2->execute();
				$ptno = $personType . "_no";
				$q->param($ptno => ${$sth2->fetchrow_arrayref()}[0] );
				$sth2->finish();
			}
		}
	}

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
sub checkNearMatch ()	{

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
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Look for matches in the returned rows
	my @complaints;
	while (my $rowRef = $sth->fetchrow_hashref())	{
		my %row = %{$rowRef};
		my $fieldMatches;
		for ( $i=0; $i<$#{$fields}; $i++ ) {
			# Strip single quotes, which won't be in the database
			my $v = $$vals[$i];
			$v =~ s/'//g;
			if ( $$fields[$i] ne "authorizer" &&
           		 $$fields[$i] ne "enterer" && $$fields[$i] ne "modifier" &&
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
		print "<center><h3><font color='red'>WARNING!</font> Your new record may duplicate one of the following old ones</h3><center>\n";
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
			for $d (@display)	{
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
		print "<p><b>What would you like to do?</b></p>";
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
		print "<center><h3>Record Addition Canceled</h3>";
		if($table_name eq "refs"){
			print qq|<p><a href="$exec_url?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p></center><br>\n|;
		}
		elsif($table_name eq "opinions" || $table_name eq "authorities"){
			print qq|<p><a href="$exec_url?action=startTaxonomy"><b>Add more taxonomic information.</b></a></p></center><br>\n|;
		}
		print stdIncludes("std_page_bottom");
	}
}

# Check for duplicates before inserting a record
sub checkDuplicates () {
	my $idName = shift;
	my $primary_key = shift;
	my $tableName = shift;
	my $fields = shift;
	my $vals = shift;

	$sql = "";
	for ( $i=0; $i<$#{$fields}; $i++ ) {
		# The primary key isn't known until after the insert.
		# The created date would be different by a few seconds.
		# Also the release_date
		# Also the "modified" date! Added by JA 12.6.02
		# Also "comments" (relevant to occs and reids) JA 28.6.02
		if ( $$fields[$i] ne $idName && $$fields[$i] ne "created" &&
             $$fields[$i] ne "modified" && $$fields[$i] ne "release_date" &&
			 $$fields[$i] ne "comments" ) {
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
	dbg ( "$sql<HR>" );

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
	my $bibRef = BiblioRef->new ( $drow );

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

	# make sure displayed collections are readable by this person JA 24.6.02
	$limit = 999;
	my $ofRows = 0;
	my $ofRows2 = 0;
	#my $method = "getReadRows";					# Default is readable rows
	my $p = Permissions->new ( $s );

	# NOTE:  "release_date" seems redundant, and "collection_name" seems
	# unnecessary.  PM 10/16/02
	my @columnList = (	"collection_no", "authorizer",
						"collection_name", "access_level",
						"research_group", "release_date",
						"DATE_FORMAT(release_date, '%Y%m%d') rd_short" );

	# primary ref
	$sql = "SELECT ". join (', ', @columnList) . " FROM collections WHERE reference_no=$tempRefNo";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# secondary ref
	$sql = "SELECT col.collection_no, col.authorizer, col.access_level, ".
		  "col.research_group,DATE_FORMAT(col.release_date,'%Y%m%d') rd_short ".
		  "FROM collections AS col, secondary_refs ".
		  "WHERE col.collection_no = secondary_refs.collection_no ".
		  "AND secondary_refs.reference_no=$tempRefNo";
	my $sth2= $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth2->execute();
	

	my $displayRows;
	my $displayRows2;
	# Get rows okayed by permissions module
	my @dataRows = ();
	my @dataRows2 = ();
	if($sth->rows || $sth2->rows){
		# primary
		$p->getReadRows($sth, \@dataRows, $limit, \$ofRows);
    	$displayRows = @dataRows; # This is the actual number of rows displayed
		# secondary
		$p->getReadRows($sth2, \@dataRows2, $limit, \$ofRows2);
    	$displayRows2 = @dataRows2;# This is the actual number of rows displayed

		# Make sure the background color matches what was set back when
		#  bibRef->toString was used in makeRefString
		if ($row % 2 != 0 && $rowcount > 1)	{
			$retString .= "<tr bgcolor='#E0E0E0'>\n";
		}
		else	{
			$retString .= "<tr>\n";
		}
		# I think this matches up with the radio,ref#,name cols from BiblioRef.
		$retString .= "<td colspan=\"3\">&nbsp;</td>\n<td><font size=-1>\n";
	}
	if( ($displayRows == 0 && $displayRows2 == 0) 
							&& ($sth->rows || $sth2->rows) ){
		$retString .= "<b>WARNING: collections have been created using this reference.\n";
		$retString .= "</font>\n</td>\n</tr>\n";
	}
	elsif($displayRows > 0 || $displayRows2 > 0){
		# Build the collections line

		my $CollString = "";
		my $count = 0;
		my $exec_url = $q->url();
	
		# primary
		foreach my $dataRow (@dataRows) {
			$dataRow->{'primary_bold'} = 1;
		}
		# put them together (primary and secondary)
		push(@dataRows,@dataRows2);
		@dataRows = sort {$a->{collection_no} <=> $b->{collection_no}} @dataRows;
		# secondary
		foreach my $elem (@dataRows) {
			my $collno = $elem->{"collection_no"};
			my $linkFront = "";

			if($elem->{'primary_bold'} == 1){
				$linkFront = "<b>";
			}

			$linkFront .= "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collno\">";
			$CollString .= $linkFront . $elem->{"collection_no"} . "</a> ";

			if($elem->{'primary_bold'} == 1){
				$CollString .= "</b>";
			}

			$count++;
		}
		if($count){
			if ( $count == 1 )	{
				$retString .= "Collection: ";
			} else {
				$retString .= "Collections: ";
			}
		}

		$retString .= "
	$CollString
	</font>
	</td>
</tr>
";
	}

	return $retString;
}

# JA 25.2.02
sub RefQuery	{

	# Use current reference button?
	if ( $q->param("use_current") ) {
		$sql = "SELECT * FROM refs WHERE reference_no = ".$s->get("reference_no");
		dbg ( "Using current: $sql<HR>" );
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		$md = MetadataModel->new($sth);
		@fieldNames = @{$sth->{NAME}};
		return;
	}

	$name = $q->param('name');
	$pubyr = $q->param('year');
	$reftitle = $q->param('reftitle');
	$refno = $q->param('reference_no');
	$refsearchstring = qq|$name| if $name;
	$refsearchstring .= qq| $pubyr| if $pubyr;
	$refsearchstring .= qq| $reftitle| if $reftitle;
	$refsearchstring .= qq| reference $refno| if $refno;
	$refsearchstring =~ s/^ //;
	my $overlimit;

	if ( $refsearchstring ) { $refsearchstring = "for '$refsearchstring' "; }

	if ( $refsearchstring ne "" || $q->param('enterer') ||
		 $q->param('project_name') )	{
		$sql =	"SELECT * FROM refs ";

		my $where = "";

		if ( $name ) {
			$where = buildWhere ( $where,	" ( author1last LIKE '%$name%' OR ".
											"   author2last LIKE '%$name%' OR ".
											"   otherauthors LIKE '%$name%' ) " );
		}
		if ( $pubyr ) { $where = &buildWhere ( $where, "pubyr='$pubyr'" ); }
		if ( $reftitle ) { $where = &buildWhere ( $where, " ( reftitle LIKE '%$reftitle%' OR reftitle LIKE '$reftitle%' )" ); }
		if ( $refno ) { $where = &buildWhere ( $where, "reference_no=$refno" ); }
		if ( $q->param('enterer') ) { $where = &buildWhere ( $where, "enterer='".$q->param('enterer')."'" ); }
		if ( $q->param('project_name') ) { $where = &buildWhere ( $where, "project_name='".$q->param('project_name')."'" ); }

		# sort the results in any of multiple ways JA 26-27.7.02
		# default is first author
		if ($q->param('refsortby'))	{
			$where .= " ORDER BY ";
		}
		if ($q->param('refsortby') eq "year")	{
			$where .= "pubyr, ";
		} elsif ($q->param('refsortby') eq "publication")	{
			$where .= "pubtitle, ";
		} elsif ($q->param('refsortby') eq "authorizer")	{
			$where .= "authorizer, ";
		} elsif ($q->param('refsortby') eq "enterer")	{
			$where .= "enterer, ";
		} elsif ($q->param('refsortby') eq "entry date")	{
			$where .= "reference_no, ";
		}
		if ($q->param('refsortby'))	{
			$where .= "author1last, author1init, author2last, pubyr";
		}
		$sql .= $where;
		$sql =~ s/\s+/ /gms;
		dbg ( "$sql<HR>" );

	# Execute the ref query
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		my @rows = @{$sth->fetchall_arrayref()};
	# If too many refs were found, set a limit
		if (@rows > 30)	{
			$overlimit = @rows;
			$q->param('refsSeen' => 30 + $q->param('refsSeen') );
			$sql .= " LIMIT " . $q->param('refsSeen');
		}
	# Dump the refs to a flat file JA 1.7.02
		my $authname = $s->get('authorizer');
		$authname =~ s/\. //;
		open REFOUTPUT,">$HTML_DIR/$OUTPUT_DIR/$authname.refs";
		for my $rowRef (@rows)	{
			my @row = @{$rowRef};
			if ($csv->combine(@row))	{
				print REFOUTPUT $csv->string,"\n";
			}
		}
		close REFOUTPUT;
	# Rerun the query
		$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		$md = MetadataModel->new($sth);
		@fieldNames = @{$sth->{NAME}};
	}
	else {
		print &stdIncludes ("std_page_top");
		print "<center><h4>Sorry! You can't do a search without filling in at least one field</h4>\n";
		print "<p><a href='$exec_url?action=displaySearchRefs&type=".$q->param("type")."'><b>Do another search</b></a></p></center>\n";
		print &stdIncludes ("std_page_bottom");
		exit(0);
	}
	return $overlimit;
}

# This only shown for internal errors
sub htmlError {
	my $message = shift;

	# print $q->header( -type => "text/html" );
    print &stdIncludes ( "std_page_top" );
	print $message;
    print &stdIncludes ("std_page_bottom");
	exit 1;
}

# Builds a WHERE clause smartly
sub buildWhere {
	my $where = shift;
	my $clause = shift;

	if ( $where ) {
		$where .= " AND $clause ";
	} else {
		$where = " WHERE $clause ";
	}
	return $where;
}

# Build the reference ( at the bottom of the page )
sub buildReference {
	my $reference_no = shift;
	my $outputtype = shift; # possible values list/bottom
	my $reference = "";

	if ( $reference_no ) {
		$sql = "SELECT * FROM refs where reference_no = $reference_no";
		dbg ( "$sql<HR>" );
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		if ( $sth->rows ) {
			$rs = $sth->fetchrow_hashref ( );

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
