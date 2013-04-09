# 
# TaxonInfo.pm
# 
# The purpose of this module is to produce HTML code to describe a taxon,
# including the body of the comprehensive page describing a given taxon.


package TaxonInfo;

use Taxonomy;
use TimeLookup;
use Data::Dumper;
use Collection;
use Reference;
use PrintHierarchy;
use Ecology;
use Images;
use Measurement;
use Debug qw(dbg);
use PBDBUtil;
use Constants qw($HOST_URL $WRITE_URL $SQL_DB $IS_FOSSIL_RECORD $PAGE_TOP $PAGE_BOTTOM $HTML_DIR $TAXA_TREE_CACHE $TAXA_LIST_CACHE);

use strict;
use Carp qw(carp croak);

# JA: rjp did a big complicated reformatting of the following and I can't
#  confirm that no damage was done in the course of it
# him being an idiot, I think it's his fault that all the HTML originally was
#  embedded instead of being in a template HTML file that's passed to
#  populateHTML
# possibly Muhl's fault because he uses populateHTML nowhere in this module
# I fixed this 21.10.04
sub searchForm {
    
    my ($hbo, $q, $message) = @_;
    
	my $page_title = "Taxonomic name search form";

	if ( defined $message )
	{
	    $message = "No results found" unless $message =~ /^[a-zA-Z]/;
	    $page_title = "<p class=\"medium\">$message (please search again)</p>";
	}
    
	my @ranks = $hbo->getList('taxon_rank');
	shift @ranks;
	my $rank_select = "<select name=\"taxon_rank\"><option>".join('</option><option>',@ranks)."</option></select>\n";
	$rank_select =~ s/>species</ selected>species</;
	print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,'',$rank_select], ['page_title','page_subtitle','taxon_rank_select']);
}

# checkTaxonInfo ( taxonomy, q, s, hbo )
# 
# Process HTML parameters for displaying full taxonomic info.  Check for
# parameters 'taxon_no', 'taxon_name', and 'search_again'.  Depending upon the
# value of these parameters, call 'displayFullTaxonInfo' or 'displayTaxonNoResults'

sub checkTaxonInfo {
    my ($dbt, $taxonomy, $q, $s, $hbo) = @_;
    my $dbh = $dbt->dbh;
    
    my ($is_real_user,$not_bot) = (0,1);
    if ($q->request_method() eq 'POST' || $q->param('is_real_user') || $s->isDBMember()) {
        $is_real_user = 1;
        $not_bot = 1;
    }
    if (PBDBUtil::checkForBot()) {
        $is_real_user = 0;
        $not_bot = 0;
    }
    
    # If we are given a taxon_no value, use that.
    
    if ( my $taxon_no = $q->param('taxon_no') )
    {
        displayFullTaxonInfo($dbt, $taxonomy, $taxon_no, $q, $s, $is_real_user, $not_bot, $hbo);
    } 
    
    # If we are given a taxon_name value, use that.
    
    if ( $q->param('taxon_name') || $q->param('search_again') )
    {
	my $name = $q->param('taxon_name') || $q->param('search_again');
	
	my $taxon_no = $taxonomy->getTaxaByName($name, { id => 1 });
	
	# If we didn't find the taxon in the authorities table, check occurrences/reids
	
	unless ( $taxon_no )
	{
	    $taxon_no = searchTaxonOccurrences($name);
	}
	
	# If we have a taxon number, display info about that taxon.
	
	if ( $taxon_no )
	{
	    displayFullTaxonInfo($dbt, $taxonomy, $taxon_no, $q, $s, $is_real_user, $not_bot, $hbo);
	}
	
	# Otherwise, "no results"
	
	else
	{
	    displayTaxonNoResults($dbt, $name, $hbo);
	}
    }
    
    # If we don't have either, redisplay the search form.
    
    else
    {
        searchForm($hbo, $q, 1); # param for not printing header with form
        return;
    }
}


# displayFullTaxonInfo ( taxonomy, taxon_no, q, s, not_bot, hbo )
# 
# Display all of the information that we have about the given taxon, specified
# by taxon_no.  If 'not_bot' is true, then we believe this to be a query from
# a real person rather than a bot.  The 'hbo' parameter should be a reference
# to an HTMLBuilder object.

sub displayFullTaxonInfo {

    my ($dbt, $taxonomy, $base_taxon_no, $q, $s, $is_real_user, $not_bot, $hbo) = @_;
    
    my $dbh = $dbt->dbh;
    
    # Start by getting the necessary informationa about the current spelling
    # of the most senior synonym of the specified taxon.  We actually display
    # info about the senior synonym, regardless of the taxon_no actually
    # passed in.
    
    my $focal_taxon = $taxonomy->getRelatedTaxon($base_taxon_no, 'senior',
						 { select => 'spelling',
						   fields => ['ref', 'discussion',
							      'link', 'lft', 'locality'] });
    
    unless ( $focal_taxon )
    {
	print "<div align=\"center\">".Debug::printErrors(["taxon number $base_taxon_no doesn't exist in the database"])."</div>";
	return;
    }
    
    my $taxon_no = $focal_taxon->{taxon_no};
    my $spelling_no = $taxon_no;
    my $taxon_name = $focal_taxon->{taxon_name};
    my $common_name = $focal_taxon->{common_name};
    my $taxon_rank = $focal_taxon->{taxon_rank};
    
    my @parent_taxa = $taxonomy->getTaxa($focal_taxon, 'all_parents');
    
    # Don't show all of the info for taxa with too many descendants.
    
    my $count_children = $taxonomy->countTaxa($focal_taxon, 'all_children');
    
    my $limit_display = !$is_real_user && $count_children > 1000;
    
    # Figure out which modules should be displayed
    
    print "<div>\n";
    
    # Determine the display name
    
    my $display_name = $focal_taxon->{taxon_name};
    
    if ( $focal_taxon->{common_name} =~ /[A-Za-z]/ )	{
        $display_name .= " ($focal_taxon->{common_name})";
    }
    
    elsif ( $taxon_rank !~ /genus|species/ and not $taxonomy->taxonIsUsed($taxon_no) )
    {
	$display_name .= " (disused)";
    }
    
    elsif ( $focal_taxon->{status} =~ /nomen/ )
    {
	$display_name .= " ($focal_taxon->{status})";
    }
    
    # Start the taxon info display with the tab pane for selecing from among
    # the eight modules into which the taxon info is divided
    
    print '
<script src="/JavaScripts/common.js" language="JavaScript" type="text/javascript"></script>

<div align="center">
  <table class="panelNavbar" cellpadding="0" cellspacing="0" border="0">
  <tr>
    <td id="tab1" class="tabOff" onClick="switchToPanel(1,8);">
      Basic info</td>
    <td id="tab2" class="tabOff" onClick="switchToPanel(2,8);">
      Taxonomic history</td>
    <td id="tab3" class="tabOff" onClick = "switchToPanel(3,8);">
      Classification</td>
    <td id="tab4" class="tabOff" onClick = "switchToPanel(4,8);">
      Relationships</td>
  </tr>
  <tr>
    <td id="tab5" class="tabOff" onClick="switchToPanel(5,8);">
      Morphology</td>
    <td id="tab6" class="tabOff" onClick = "switchToPanel(6,8);">
      Ecology and taphonomy</td>
    <td id="tab7" class="tabOff" onClick = "switchToPanel(7,8);">
      Map</td>
    <td id="tab8" class="tabOff" onClick = "switchToPanel(8,8);">
      Age range and collections</td>
  </tr>
  </table>
</div>
';

    my ($htmlCOF,$htmlClassification) = ('', '');
    
    print qq|
<div align="center" style="margin-bottom: -1.5em;">
<p class="pageTitle" style="white-space: nowrap; margin-bottom: 0em;">$display_name</p>
<p class="medium">$htmlCOF</p>
</div>

<div style="position: relative; top: -3.5em; left: 43em; margin-bottom: -2.5em; width: 8em; z-index: 8;">
<form method="POST" action="">
<input type="hidden" name="action" value="checkTaxonInfo">
<input type="text" name="search_again" value="Search again" size="14" onFocus="textClear(search_again);" onBlur="textRestore(search_again);" style="font-size: 0.7em;">
</form>
</div>

|;

    
    print '<script language="JavaScript" type="text/javascript">
    hideTabText(2);
    hideTabText(3);
    hideTabText(4);
    hideTabText(5);
    hideTabText(6);
    hideTabText(7);
    hideTabText(8);
</script>';
    
    # Basic info
    
    displayModule1($dbt, $taxonomy, $focal_taxon, $q, $s, $is_real_user);
    
    # Taxonomic history
    
    displayModule2($dbt, $taxonomy, $focal_taxon, $is_real_user);
    
    # Classification
    
    displayModule3($dbt, $taxonomy, $focal_taxon, \@parent_taxa, $is_real_user) if $not_bot;
    
    # Relationships
    
    displayModule4($dbt, $taxonomy, $focal_taxon, $q, $is_real_user) if $not_bot;
    
    # Morphology
    
    displayModule5($dbt, $taxonomy, $focal_taxon, $is_real_user, $limit_display) if $not_bot;
    
    # Ecology and taphonomy
    
    displayModule6($dbt, $taxonomy, $focal_taxon, $is_real_user, $limit_display) if $not_bot;
    
    # Map
    
    my $collectionsSet = getCollectionsSet($dbt, $taxonomy, $q, $s, $taxon_no, $taxon_name)
	if $is_real_user;
    
    displayModule7($dbt, $taxonomy, $q, $s, $collectionsSet, $limit_display) if $not_bot;
    
    # Collections
    
    displayModule8($dbt, $taxonomy, $focal_taxon, $q, $s, $collectionsSet, 
		   $display_name, $is_real_user) if $not_bot;
    
    # Finish up
    
    if ( ! $q->param('show_panel') )	{
        print "<script language=\"JavaScript\" type=\"text/javascript\">switchToPanel(1,8);</script>\n";
    } else	{
        print "<script language=\"JavaScript\" type=\"text/javascript\">switchToPanel(".$q->param('show_panel').",8);</script>\n";
    }
    print "</div>"; # Ends div class="small" declared at start
}


sub displayModule1 {
    
    my ($dbt, $taxonomy, $focal_taxon, $q, $s, $is_real_user) = @_;
    
    my $taxon_no = $focal_taxon->{taxon_no};
    
    print '<div id="panel1" class="panel">';
    my $width = "52em;";
    unless ( $focal_taxon )
    {
	$width = "44em;";
    }
    
    print displayThumbs($dbt, $taxonomy, $focal_taxon);
    
    print displayDiscussion($dbt, $focal_taxon);
    
    print qq|<div align="center" class="small" style="margin-left: 1em; margin-top: 1em;">
<div style="width: $width;">
<div class="displayPanel" style="margin-top: -1em; margin-bottom: 2em; padding-left: 1em; text-align: left;">
<span class="displayPanelHeader">Taxonomy</span>
<div align="left" class="small displayPanelContent" style="padding-left: 1em; padding-bottom: 1em;">
|;
    
    my $basicSynonymy = getSynonymyParagraph($dbt, $taxonomy, $focal_taxon, $is_real_user);
    
    print $basicSynonymy;
    print "</div>\n</div>\n\n";
    
    my $entered_name = $q->param('entered_name') || $q->param('taxon_name') || $focal_taxon->{taxon_name};
    my $entered_no = $q->param('entered_no') || $q->param('taxon_no');
    print "<p>";
    print "<div>";
    print "<center>";
    
    print displayRelatedTaxa($dbt, $taxonomy, $focal_taxon, $is_real_user);
    
    print "</center>\n";
    if($s->isDBMember() && $s->get('role') =~ /authorizer|student|technician/) {
	# Entered Taxon
	if ($entered_no) {
	    print "<a href=\"$WRITE_URL?a=displayAuthorityForm&amp;taxon_no=$entered_no\">";
	    print "<b>Edit taxonomic data for $entered_name</b></a> - ";
	} else {
	    print "<a href=\"$WRITE_URL?a=submitTaxonSearch&amp;goal=authority&amp;taxon_no=-1&amp;taxon_name=$entered_name\">";
	    print "<b>Enter taxonomic data for $entered_name</b></a> - ";
	}
	
	if ($entered_no) {
	    print "<a href=\"$WRITE_URL?a=displayOpinionChoiceForm&amp;taxon_no=$entered_no\"><b>Edit taxonomic opinions about $entered_name</b></a> -<br> ";
	    print "<a href=\"$WRITE_URL?a=startPopulateEcologyForm&amp;taxon_no=$taxon_no\"><b>Add/edit ecological/taphonomic data</b></a> - ";
	}
	
	print "<a href=\"$WRITE_URL?a=startImage\">".
	    "<b>Enter an image</b></a>\n";
    }
    
    print "</div>\n";
    print "</p>";
    print "</div>\n</div>\n</div>\n\n";
}


sub displayModule2 {

    my ($dbt, $taxonomy, $focal_taxon, $is_real_user) = @_;
    
    print qq|<div id="panel2" class="panel";">
<div align="center" class="small"">
|;
    
    # If there are any junior synonyms, display a "synonymy paragraph" for
    # each of them.
    
    my @juniors = $taxonomy->getTaxa($focal_taxon, 'juniors');
    
    if ( @juniors )
    {
print qq|<div class="displayPanel" style="margin-bottom: 2em; padding-top: -1em; width: 42em; text-align: left;">
<span class="displayPanelHeader">Synonyms</span>
<div align="center" class="small displayPanelContent">
<table><tr><td><ul>
|;

	my @paragraphs;
	
	foreach my $t (@juniors)
	{
	    push @paragraphs, getSynonymyParagraph($dbt, $taxonomy, $t, $is_real_user)
		if $t->{classification_no} == $focal_taxon->{orig_no};
	}
	
	foreach my $p ( sort { lc($a) cmp lc($b) } @paragraphs )
	{
	    print "<li style=\"padding-bottom: 1.5em;\">$p</li>\n";
	}
	
	print "</ul></td></tr></table>\n</div>\n</div>\n";
    }
    
    print displaySynonymyList($dbt, $taxonomy, $focal_taxon, $is_real_user);
    
    if ( $focal_taxon )
    {
	print "<p>Is something missing? <a href=\"?a=displayPage&amp;page=join_us\">Join the Paleobiology Database</a> and enter the data</p>\n";
    } else	{
	print "<p>Please <a href=\"?a=displayPage&amp;page=join_us\">join the Paleobiology Database</a> and enter some data</p>\n";
    }
    
    print "</div>\n</div>\n</div>\n";
}


sub displayModule3 {

    my ($dbt, $taxonomy, $focal_taxon, $parent_taxa, $is_real_user) = @_;
    
    print qq^<div id="panel3" class="panel">
<div align="center">
^;
    
    my ($class, $order, $family) = $taxonomy->getClassOrderFamily($focal_taxon, $parent_taxa);
    
    my $htmlCOF = $class if $class;
    
    if ( $order )
    {
	$htmlCOF .= ' - ' if $htmlCOF;
	$htmlCOF .= $order;
    }
    
    if ( $family )
    {
	$htmlCOF .= ' - ' if $htmlCOF;
	$htmlCOF .= $family;
    }
    
    print $htmlCOF, "\n";
    
    my $htmlClassification = displayTaxonClassification($dbt, $taxonomy, $focal_taxon, 
							 $parent_taxa, $is_real_user);
	
    print $htmlClassification, "\n";
    
    print "</div>\n</div>\n\n";
}


sub displayModule4 {

    my ($dbt, $taxonomy, $focal_taxon, $q, $is_real_user) = @_;
    
    print qq^<div id="panel4" class="panel">
<div align="center" class="small">
^;
    
    if ($is_real_user) {
	displayTaxonRelationships($dbt, $taxonomy, $q, $focal_taxon);
    } else {
	print qq|<form method="POST" action="">|;
	foreach my $f ($q->param()) {
	    print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">\n";
	}
	print "<input type=\"hidden\" name=\"show_panel\" value=\"4\">\n";
	print "<input type=\"submit\" name=\"submit\" value=\"Show relationships\">";
	print "</form>\n";
    }
    
    print "</div>\n</div>\n";
}


sub displayModule5 {

    my ($dbt, $taxonomy, $focal_taxon, $is_real_user, $limit_display) = @_;
    
    print qq^<div id="panel5" class="panel">
<div align="center" class="small" "style="margin-top: -2em;">
^;
    
    print displayDiagnoses($dbt, $taxonomy, $focal_taxon);
    print displayMeasurements($dbt, $taxonomy, $focal_taxon) unless $limit_display;
    
    print "</div>\n</div>\n";
}


sub displayModule6 {

    my ($dbt, $taxonomy, $focal_taxon, $is_real_user, $limit_display) = @_;
    
    print qq^<div id="panel6" class="panel">
<div align="center" clas="small">
^;
    
    print displayEcology($dbt, $taxonomy, $focal_taxon) unless $limit_display;
    
    print "</div>\n</div>\n";
}


sub displayModule7 {

    my ($dbt, $taxonomy, $q, $s, $collections) = @_;
    
    print qq^<div id="panel7" class="panel">
<div align="center" style="margin-top: -1em;">
^;
    
    if ( $collections ) 
    {
	displayMap($dbt, $taxonomy, $q, $s, $collections);
    }
    
    else
    {
	print qq|<form method="POST" action="">|;
	foreach my $f ($q->param()) {
	    print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">\n";
	}
	print "<input type=\"hidden\" name=\"show_panel\" value=\"7\">\n";
	print "<input type=\"submit\" name=\"submit\" value=\"Show map\">";
	print "</form>\n";
    }
    
    print "</div>\n</div>\n";
}


sub displayModule8 {
    
    my ($dbt, $taxonomy, $focal_taxon, $q, $s, $collectionsSet, $display_name, $is_real_user) = @_;
    
    my $taxon_no = $focal_taxon->{taxon_no};
    my $type_locality = $focal_taxon->{type_locality};
    
    print qq^<div id="panel8" class="panel">\n^;
    
    if ($is_real_user)
    {
	print Collection::generateCollectionTable($dbt, $taxonomy, $s, $collectionsSet, $display_name, $taxon_no, '', $is_real_user, $type_locality);
    }
    
    else
    {
	print '<div align="center">';
	print qq|<form method="POST" action="">|;
	foreach my $f ($q->param()) {
	    print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">\n";
	}
	print "<input type=\"hidden\" name=\"show_panel\" value=\"8\">\n";
	print "<input type=\"submit\" name=\"submit\" value=\"Show age range and collections\">";
	print "</form>\n";
	print "</div>\n";
    }
    
    print "</div>\n";
}

# getCollectionsSet ( )
# 
# Get a list of all collections containing the specified taxon or any of its children.

sub getCollectionsSet {
    my ($dbt, $taxonomy, $taxon_no) = @_;
    
    my $fields = ['country','state','max_interval_no','min_interval_no','latdeg','latdec','latmin','latsec','latdir','lngdeg','lngdec','lngmin','lngsec','lngdir','seq_strat'];
    
    # Pull the colls from the DB;
    my %options = ();
    $options{'permission_type'} = 'read';
    $options{'calling_script'} = 'TaxonInfo';
    $options{'base_taxon'} = $taxon_no;
    
    # These fields passed from strata module,etc
    #foreach ('group_formation_member','formation','geological_group','member','taxon_name') {
    #    if (defined($q->param($_))) {
    #        $options{$_} = $q->param($_);
    #    }
    #}
    my ($dataRows) = Collection::getCollections($dbt, $taxonomy, \%options, $fields);
    return $dataRows;
}


# heavily rewriten to switch from using htmlTaxaTree to using classify
#   JA 27.2.12
#   MM 13.10.12

sub displayTaxonRelationships {
    
    my ($dbt, $taxonomy, $q, $focal_taxon) = @_;
    
    my $taxon_no = $focal_taxon->{taxon_no};
    my $taxon_name = $focal_taxon->{taxon_name};
    my $parent_no;
    
    # First, figure out the taxon from which we are starting.  If a valid
    # taxon_no is given, find its parent.  Otherwise, find the best match
    # using the given genus and species name.
    
    if ( $taxon_no < 1 && $taxon_name =~ / / )
    {
        my ($genus,$subgenus,$species,$subspecies) = $taxonomy->splitTaxonName($taxon_name);
	my $guess = $taxonomy->getTaxaBestMatch(['', $genus, '', $subgenus, '', $species]);
	$parent_no = $guess->{taxon_no} if defined $guess;
    }
    
    else
    {
	$parent_no = $taxonomy->getRelatedTaxon($taxon_no, 'parent', { id => 1 });
    }
    
    my $html;
    my @cladograms;
    print qq|<div class="displayPanel" align="left" style="width: 52em; margin-top: 0em; padding-top: 0em;">
    <span class="displayPanelHeader" class="large">Classification of relatives</span>
|;
    if ( $parent_no )	{

    # print a classification of the grandparent and its children down
    #  three (or two) taxonomic levels (one below the focal taxon's)
    # JA 23-24.11.08
	print "<div class=\"displayPanelContent\">\n<div align=\"center\" class=\"medium\" style=\"padding-bottom: 1em;\"><i>\n\n";
        $q->param('parent_no' => $parent_no);
        $q->param('boxes_only' => 'YES');
        my $subtaxa = PrintHierarchy::classify($dbt, $taxonomy, undef, undef, $q);
	print "</div></div>\n\n";
	
	my @synonyms = $taxonomy->getTaxa($taxon_no, 'synonyms', { id => 1 });
	
        my $parent_list = join(',', @synonyms);
	
        my $sql = "(SELECT DISTINCT cladogram_no FROM cladograms c WHERE c.taxon_no IN ($parent_list))".
              " UNION ".
              "(SELECT DISTINCT cladogram_no FROM cladogram_nodes cn WHERE cn.taxon_no IN ($parent_list))";
    	@cladograms = @{$dbt->getData($sql)};

    } else	{
        print "<div class=\"displayPanelContent\">\n<div align=\"center\" class=\"medium\"><i>No data on relationships are available</i></div></div>";
    }

    print "</div>\n\n";

        print qq|<div class="displayPanel" align="left" style="width: 52em; margin-top: 2em; padding-bottom: 1em;">
    <span class="displayPanelHeader" class="large">Cladograms</span>
        <div class="displayPanelContent">
|;
    if (@cladograms) {
        print "<div align=\"center\" style=\"margin-top: 1em;\">";
        foreach my $row (@cladograms) {
            my $cladogram_no = $row->{cladogram_no};
            my ($pngname, $caption, $taxon_name) = Cladogram::drawCladogram($dbt,$cladogram_no);
            if ($pngname) {
                print qq|<img src="/public/cladograms/$pngname"><br>$caption<br><br>|;
            }
        }
        print "</div>";
    } else {
          print "<div align=\"center\"><i>No cladograms are available</i></div>\n\n";
    }
    print "</div>\n</div>\n\n";

} 


# displayThumbs ( )
# 
# Display a list of thumbnails for the given taxa and a selection of its children.

sub displayThumbs {
    
    my ($dbt, $taxonomy, $focal_taxon) = @_;
    my $images_per_row = 6;
    my $thumbnail_size = 100;
    
    my $dbh = $dbt->dbh;
    
    # First, figure out how many images there are.
    
    my $auth_table = $taxonomy->{auth_table};
    my $tree_table = $taxonomy->{tree_table};
    
    my ($image_count) = $dbh->selectrow_array("
	SELECT count(distinct i.image_no)
	FROM images as i JOIN $auth_table as a using (taxon_no)
		JOIN $tree_table as t using (orig_no)
		JOIN $tree_table as t2 on t.lft >= t2.lft and t.lft <= t2.rgt
	WHERE t2.orig_no = $focal_taxon->{orig_no}");
    
    # If the image count is less than or equal to 12 (two rows) just display
    # them all.  Otherwise, display the first 12.  (someday, we may want to
    # change this to a random sample).
    
    my $base_taxon_no = $focal_taxon->{taxon_no};
    
    my $thumbs = $dbh->selectall_arrayref("
	SELECT i.image_no, i.caption, i.path_to_image, i.width, i.height
	FROM images as i JOIN $auth_table as a using (taxon_no)
		JOIN $tree_table as t using (orig_no)
		JOIN $tree_table as t2 on t.lft >= t2.lft and t.lft <= t2.rgt
		JOIN $auth_table as a2 on t2.orig_no = a2.orig_no
	WHERE a2.taxon_no = $base_taxon_no
	LIMIT 12", { Slice => {} });
    
    my $output = '';
    
    if ($thumbs) {
        my $output .= '<div align="center" style="margin-left: 2em; margin-bottom: 2em;">';
        my $output .= "<table border=0 cellspacing=8 cellpadding=0>";
        for (my $i = 0;$i < scalar(@$thumbs);$i+= $images_per_row) {
            my $output = "<tr>";
            for( my $j = $i;$j<$i+$images_per_row;$j++) {
                my $output .= "<td>";
                my $thumb = $thumbs->[$j];
                if ($thumb) {
                    my $thumb_path = $thumb->{path_to_image};
                    $thumb_path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
                    my $caption = $thumb->{'caption'};
                    my $width = ($thumb->{'width'}  || 300);
                    my $height = ($thumb->{'height'}  || 400);
                    my $maxwidth = $width;
                    my $maxheight = $height;
                    if ( $maxwidth > 300 || $maxheight > 400 )	{
                        $maxheight = 400;
                        $maxwidth = 400 * $width / $height;
                    }
                    $height =  $maxheight + 150;
                    $width = $maxwidth;
                    if ( $width < 300 )	{
                        $width = 300;
                    }
                    $width += 80;
                    my $t_width=$thumbnail_size;
                    my $t_height=$thumbnail_size;
                    if ($thumb->{'width'} && $thumb->{'height'}) {
                        if ($thumb->{'width'} > $thumb->{'height'}) {
                            $t_width = $thumbnail_size;
                            $t_height = int($thumb->{'height'}/$thumb->{'width'}*$thumbnail_size);
                        } else {
                            $t_width = int($thumb->{'width'}/$thumb->{'height'}*$thumbnail_size);
                            $t_height = $thumbnail_size;
                        }
                    }

                    my $output .= "<a href=\"javascript: imagePopup('?a=displayImage&amp;image_no=$thumb->{image_no}&amp;maxheight=$maxheight&amp;maxwidth=$maxwidth&amp;display_header=NO',$width,$height)\">";
                    my $output .= "<img src=\"$thumb_path\" border=1 vspace=3 width=$t_width height=$t_height alt=\"$caption\">";
                    my $output .= "</a>";

                } else {
                    my $output .= "&nbsp;";
                }
                my $output .= "</td>";
            }
            my $output .= "</tr>";
        }
        my $output .= "</td></tr></table>";
        my $output .= "</div>\n";
    }
    
    return $output;
} 


# displayDiscussion ( )
# 
# JA 5.9.11 MM 13.10.12
# If this taxon has been discussed, figure out who it is.

sub displayDiscussion {

    my ($dbt, $focal_taxon) = @_;
    
    my $discussion = $focal_taxon->{discussion};
    my $taxon_name = $focal_taxon->{taxon_name};
    
    return unless $discussion;
    
    my $discussant = $focal_taxon->{discussant};
    my $email = $focal_taxon->{discussant_email};
    
    $discussion =~ s/(\[\[)([A-Za-z ]+|)(taxon )([0-9]+)(\|)/<a href="?a=basicTaxonInfo&amp;taxon_no=$4">/g;
    $discussion =~ s/(\[\[)([A-Za-z0-9\'\. ]+|)(ref )([0-9]+)(\|)/<a href="?a=displayReference&amp;reference_no=$4">/g;
    $discussion =~ s/(\[\[)([A-Za-z0-9\'"\.\-\(\) ]+|)(coll )([0-9]+)(\|)/<a href="?a=basicCollectionSearch&amp;collection_no=$4">/g;
    $discussion =~ s/\]\]/<\/a>/g;
    $discussion =~ s/\n\n/<\/p>\n<p>/g;
    
    $email =~ s/\@/\' \+ \'\@\' \+ \'/;
    
    print qq|<div align="center" class="small" style="margin-left: 1em; margin-top: 1em;">
<div style="width: 52em;">
<div class="displayPanel" style="margin-bottom: 3em; padding-left: 1em; padding-top: -1em; padding-bottom: 1.5em; text-align: left;">
<span class="displayPanelHeader">Discussion</span>
<div align="center" class="small displayPanelContent" style="text-align: left;">
<p>$discussion</p>
|;
    
    if ( $discussant ne "" )	{
	print qq|<script language="JavaScript" type="text/javascript">
    <!-- Begin
    window.onload = showMailto;
    function showMailto( )      {
        document.getElementById('mailto').innerHTML = '<a href="' + 'mailto:' + '$email?subject=$taxon_name">$discussant</a>';
    }
    // End -->
</script>

<p class="verysmall">Send comments to <span id="mailto">me</span><p>
|;
	
    }
    
    print "</div></div></div></div>\n";
}
    

# displayMap ( )
# 
# Given a set of collections, display a map showing the geographic locus of the
# specified taxon.

sub displayMap {
    my ($dbt,$taxonomy,$q,$s,$collectionsSet)  = @_;
    require Map;
    
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape1', 'dotcolor1', 'dotborder1');
	my %user_prefs = $s->getPreferences();
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}

	# we need to get the number of collections out of dataRowsRef
	#  before figuring out the point size
    my ($map_html_path,$errors,$warnings);
#    if (ref $in_list && @$in_list) {
        $q->param("simple_map"=>'YES');
        $q->param('mapscale'=>'auto');
        $q->param('autoborders'=>'yes');
        $q->param('pointsize1'=>'auto');
        my $m = Map->new($dbt,$taxonomy,$q,$s);
        ($map_html_path,$errors,$warnings) = $m->buildMap('dataSet'=>$collectionsSet);
#    }

    # MAP USES $q->param("taxon_name") to determine what it's doing.
    if ( $map_html_path )	{
        if($map_html_path =~ /^\/public/){
            # reconstruct the full path the image.
            $map_html_path = $HTML_DIR.$map_html_path;
        }
	unless ( open(MAP, $map_html_path) )
	{
	    carp "couldn't open $map_html_path ($!)";
	    print qq|<div class="displayPanel" align="left" style="width: 36em; margin-top: 0em; padding-bottom: 1em;">
<span class="displayPanelHeader" class="large">Map</span>
<div class="displayPanelContent">
  <div align="center"><i>Error: could not create map</i></div>
</div>
</div>
|;

	}
        while(<MAP>){
            print;
        }
        close MAP;
    } else {
        print qq|<div class="displayPanel" align="left" style="width: 36em; margin-top: 0em; padding-bottom: 1em;">
<span class="displayPanelHeader" class="large">Map</span>
<div class="displayPanelContent">
  <div align="center"><i>No distribution data are available</i></div>
</div>
</div>
|;
    }
}


# JA 23.9.11
# replaces Schroeter's much more complicated getIntervalsData with a simple
#  database hit
sub getIntervalData	{
	my ($dbt,$colls) = @_;
	my %is_no;
	$is_no{$_->{'max_interval_no'}}++ foreach @$colls;
	$is_no{$_->{'min_interval_no'}}++ foreach @$colls;
	delete $is_no{0};
	my $sql = "SELECT TRIM(CONCAT(i.eml_interval,' ',i.interval_name)) AS interval_name,i.interval_no,base_age,top_age FROM intervals i,interval_lookup l WHERE i.interval_no=l.interval_no AND i.interval_no IN (".join(',',keys %is_no).")";
	return @{$dbt->getData($sql)};
}

# JA 23.9.11
# replaces Schroeter's old, hard-fought calculateAgeRange function, which was
#  vastly more complicated
sub getAgeRange	{
	my ($dbt,$colls) = @_;
	my @coll_nos = map { $_ ->{'collection_no'} } @$colls;
	if ( ! @coll_nos )	{
		return;
	}

	# get the youngest base age of any collection including this taxon
	# ultimately, the range's top must be this young or younger
	my $sql = "SELECT base_age AS maxtop FROM collections,interval_lookup WHERE max_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos).") ORDER BY base_age ASC";
	my $maxTop = ${$dbt->getData($sql)}[0]->{'maxtop'};

	# likewise the oldest top age
	# the range's base must be this old or older
	# the top is the top of the max_interval for collections having
	#  no separate max and min ages, but is the top of the min_interval
	#  for collections having different max and min ages
	my $sql = "SELECT top_age AS minbase FROM ((SELECT top_age FROM collections,interval_lookup WHERE min_interval_no=0 AND max_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos).")) UNION (SELECT top_age FROM collections,interval_lookup WHERE min_interval_no>0 AND min_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos)."))) AS ages ORDER BY top_age DESC";
	my $minBase = ${$dbt->getData($sql)}[0]->{'minbase'};

	# now get the range top
	# note that the range top is the top of some collection's min_interval
	$sql = "SELECT MAX(top_age) top FROM ((SELECT top_age FROM collections,interval_lookup WHERE min_interval_no=0 AND max_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos).") AND top_age<$maxTop) UNION (SELECT top_age FROM collections,interval_lookup WHERE min_interval_no>0 AND min_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos).") AND top_age<$maxTop)) AS tops";
	my $top = ${$dbt->getData($sql)}[0]->{'top'};

	# and the range base
	$sql = "SELECT MIN(base_age) base FROM collections,interval_lookup WHERE max_interval_no=interval_no AND collection_no IN (".join(',',@coll_nos).") AND base_age>$minBase";
	my $base = ${$dbt->getData($sql)}[0]->{'base'};

	my (%is_max,%is_min);
	for my $c ( @$colls )	{
		$is_max{$c->{'max_interval_no'}}++;
		if ( $c->{'min_interval_no'} > 0 )	{
			$is_min{$c->{'min_interval_no'}}++;
		} else	{
			$is_min{$c->{'max_interval_no'}}++;
		}
	}

	# get the ID of the shortest interval whose base is equal to the
	#  range base and explicitly includes an occurrence
	$sql = "SELECT interval_no FROM interval_lookup WHERE interval_no IN (".join(',',keys %is_max).") AND base_age=$base ORDER BY top_age DESC LIMIT 1";
	my $oldest_interval_no = ${$dbt->getData($sql)}[0]->{'interval_no'};

	# ditto for the shortest interval defining the top
	# only the ID number is needed
	$sql = "SELECT interval_no FROM interval_lookup WHERE interval_no IN (".join(',',keys %is_min).") AND top_age=$top ORDER BY base_age ASC LIMIT 1";
	my $youngest_interval_no = ${$dbt->getData($sql)}[0]->{'interval_no'};

	return($base,$top,$oldest_interval_no,$minBase,$youngest_interval_no);
}


# displayTaxonClassification ( )
# 
# Given a taxon number, generate HTML code for a table (two columns) giving
# its full classification.
#

sub displayTaxonClassification {
    
    my ($taxonomy, $taxon, $parent_list, $is_real_user) = @_;
    
    my $dbh = $taxonomy->{dbh};
    
    # the classification variables refer to the taxa derived from the taxon_no we're using for classification
    # purposes.  If we found an exact match in the authorities table this classification_no wil
    # be the same as the original combination taxon_no for an authority. If we passed in a Genus+species
    # type combo but only the genus is in the authorities table, the classification_no will refer
    # to the genus
    
    unless ( $taxon && $parent_list )
    {
	my $output =qq|
<div class="small displayPanel" style="width: 42em;">
<div class="displayPanelContent">
<p><i>No classification data are available</i></p>
|;
	return $output;
    }
    
    # Now extract the relevant information from this taxon.
    
    my $taxon_no = $taxon->{'taxon_no'};
    my $taxon_name = $taxon->{'taxon_name'};
    my $taxon_rank = $taxon->{'taxon_rank'};
    my $classification_no = $taxon_no;
    my $classification_name = $taxon_name;
    my $classification_rank = $taxon_rank;
    
    my ($genus,$subgenus,$species,$subspecies) = $taxonomy->splitTaxonName($taxon_name);
    
    # Determine the full classification of the taxon (all parent taxa)
    
    my @table_rows = ();
    my ($output);
    
    my ($subspecies_no,$species_no,$subgenus_no,$genus_no) = (0,0,0,0);
    # Set for focal taxon
    my $subspecies_row = $taxon_no if ($taxon_rank eq 'subspecies');
    $species_no = $taxon_no if ($taxon_rank eq 'species');
    $subgenus_no = $taxon_no if ($taxon_rank eq 'subgenus');
    $genus_no = $taxon_no if ($taxon_rank eq 'genus');
    foreach my $row (@$parent_list) {
	# Set for all possible higher taxa
	# Handle species/genus separately below.  The reason for this is the "loose" classification that
	# the PBDB does.  Taxon::getBestClassification will find a proximate match if we can't
	# find an exact match in the database.  Because of this, some of the lower level names
	# (genus,subgenus,species,subspecies) may not match up exactly from what the user entered
	$subspecies_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'subspecies');
	$species_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'species');
	$subgenus_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'subgenus');
	$genus_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'genus');
	
	if ($row->{'taxon_rank'} !~ /species|genus/) {
	    push (@table_rows,[$row->{taxon_rank},
			       $row->{taxon_name},
			       $row->{taxon_name},
			       $row->{taxon_no},
			       $row]);
	}
	last if ($row->{taxon_rank} eq 'kingdom');
    }
    if ($genus_no) {
	unshift @table_rows, ['genus',$genus,$genus,$genus_no,$genus_no];
    } elsif ($classification_no) {
	unshift @table_rows, [$classification_rank,$classification_name,$classification_name,$classification_no];
    }
    if ($subgenus) {
	unshift @table_rows, ['subgenus',"$genus ($subgenus)",$subgenus,$subgenus_no];
    }
    if ($species) {
	my $species_name = "$genus $species";
	if ($subgenus) {
	    $species_name = "$genus ($subgenus) $species";
	} 
	unshift @table_rows, ['species',"$species_name",$species,$species_no];
    }
    if ($subspecies) {
	unshift @table_rows, ['subspecies',"$taxon_name",$subspecies,$subspecies_no];
    }
    
    #
    # Print out the table in the reverse order that we initially made it
    #
    # the html actually returned by the function
    $output =qq|
<div class="small displayPanel">
<div class="displayPanelContent">
<table><tr><td valign="top">
<table><tr valign="top"><th>Rank</th><th>Name</th><th>Author</th></tr>
|;

    my $class = '';
    for(my $i = scalar(@table_rows)-1;$i>=0;$i--) {
	if ( $i == int((scalar(@table_rows) - 2) / 2) )	{
	    $output .= "\n</td></tr></table>\n\n";
	    $output .= "\n</td><td valign=\"top\" style=\"width: 2em;\"></td><td valign=\"top\">\n\n";
	    $output .= "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
	}
	$class = $class eq '' ? 'class="darkList"' : '';
	$output .= "<tr $class>";
	my($taxon_rank,$taxon_name,$show_name,$taxon_no) = @{$table_rows[$i]};
	if ($taxon_rank eq 'unranked clade') {
	    $taxon_rank = "&mdash;";
	}
	my $authority;
	if ($taxon_no) {
	    $authority = $taxonomy->getTaxon($taxon_no, { fields => 'oldattr'});
	}
	my $pub_info = Reference::formatShortRef($authority);
	if ($authority->{'ref_is_authority'} =~ /yes/i) {
	    $pub_info = "<a href=\"?a=displayReference&amp;reference_no=$authority->{reference_no}&amp;is_real_user=$is_real_user\">$pub_info</a>";
	}
	my $orig_no = getRelatedTaxon($taxon_no, 'orig', { id => 1 });
	if ($orig_no != $taxon_no) {
	    $pub_info = "(".$pub_info.")" if $pub_info !~ /^\s*$/;
	} 
	my $link;
	if ($taxon_no) {
	    $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_no=$taxon_no&amp;is_real_user=$is_real_user">$show_name</a>|;
	} else {
	    $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_name=$taxon_name&amp;is_real_user=$is_real_user">$show_name</a>|;
	}
	$output .= qq|<td align="center">$taxon_rank</td>|.
	    qq|<td align="center">$link</td>|.
		qq|<td align="center" style="white-space: nowrap">$pub_info</td>|; 
	$output .= '</tr>';
    }
    $output .= "</table>";
    $output .= "</td></tr></table>\n\n";
    $output .= "<p class=\"small\" style=\"margin-left: 2em; margin-right: 2em; text-align: left;\">If no rank is listed, the taxon is considered an unranked clade in modern classifications. Ranks may be repeated or presented in the wrong order because authors working on different parts of the classification may disagree about how to rank taxa.</p>\n\n";
    
    $output .= "</div>\n</div>\n\n";
    
    return $output;
}


# displayRelatedTaxa ( )
# 
# Given a taxon number, generate HTML code for a table listing related taxa.
# 
# Separated out from classification section PS 09/22/2005

sub displayRelatedTaxa {
    
    my ($dbt, $taxonomy, $taxon_no, $is_real_user) = @_;
    
    my $dbh = $dbt->dbh;
    
    # First make sure that we have a valid taxon number.
    
    my $taxon = $taxonomy->getTaxon($taxon_no, { fields => 'type' });
    
    unless ( $taxon )
    {
	return '';
    }
    
    # Then extract information from it.
    
    my $taxon_name = $taxon->{taxon_name};
    my $taxon_rank = $taxon->{taxon_rank};
    
    my ($genus,$subgenus,$species,$subspecies) = $taxonomy->splitTaxonName($taxon_name);
    
    my $output = "";
    
    # Begin getting sister/child taxa
    # PS 01/20/2004 - rewrite: Use getChildren function
    # First get the children
    
    my $parent_taxon = $taxonomy->getRelatedtaxon($taxon_no, 'parent');
    
    my @child_taxa_links;
    
    # This section generates links for children, ordered alphabetically by name.
    
    my @children = $taxonomy->getTaxa($taxon_no, 'children', { order => 'name' });
    
	@children = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @children;
	if (@children) {
	    my $sql = "SELECT type_taxon_no FROM authorities WHERE taxon_no=$taxon_no";
	    my $type_taxon_no = ${$dbt->getData($sql)}[0]->{'type_taxon_no'};
	    foreach my $record (@children) {
		my @syn_links;                                                         
		my @synonyms = @{$record->{'synonyms'}};
		push @syn_links, $_->{'taxon_name'} for @synonyms;
		my $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}&amp;is_real_user=$is_real_user">$record->{taxon_name}|;
		$link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
		$link .= "</a>";
		if ($type_taxon_no && $type_taxon_no == $record->{'taxon_no'}) {
		    $link .= " <small>(type $record->{taxon_rank})</small>";
		}
		push @child_taxa_links, $link;
	    }
	}
    
    # Get sister taxa as well
    # PS 01/20/2004
    my @sister_taxa_links;
    
    if ($parent_taxon) 
    {
	my @sisters = $taxonomy->getTaxa($parent_taxon, 'children', { order => 'name'});
	foreach my $record (@sisters)
	{
	    next if $record->{orig_no} == $taxon->{orig_no};
	    next if $record->{taxon_rank} ne $taxon->{taxon_rank};
	    
	    my $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}&amp;is_real_user=$is_real_user">$record->{taxon_name}|;
	    
	    my @synonyms = $taxonomy->getTaxa($record, 'synonyms', 
					      { order => 'name', exclude_self => 1 });
	    
	    if ( @synonyms )
	    {
		my @names = map { $_->{taxon_name} } @synonyms;
		$link .= " (syn. ".join(", ",@names).")";
	    }
	    $link .= "</a>";
	    push @sister_taxa_links, $link;
        }
    }

    # Check for additional links through the occurence records
    my (@possible_sister_taxa_links,@possible_child_taxa_links);
    if ($taxon_name) {
        my ($sql,$whereClause,@results);
        my ($genus,$subgenus,$species,$subspecies) = $taxonomy->splitTaxonName($taxon_name);
        my @names = ();
        if ($genus) {
            push @names, $dbh->quote($genus);
        }
        if ($subgenus) {
            push @names, $dbh->quote($subgenus);
        }
        if (@names) {
            my $genus_sql = "a.genus_name IN (".join(",",@names).")";
            my $subgenus_sql = " a.subgenus_name  IN (".join(",",@names).")";
            my ($occ_genus_no_sql,$reid_genus_no_sql) = ("","");
            #$occ_genus_no_sql = " OR a.taxon_no=$parents[0]->{taxon_no}" if (@parents);
            #$reid_genus_no_sql = " OR a.taxon_no=$parents[0]->{taxon_no}" if (@parents);
            # Note that the table aliased to "a" and "b" is switched up.  The table we want to dislay names for and do matches
            # against is "a" and the non-important table is "b"
            my $sql  = "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences a LEFT JOIN reidentifications b ON a.occurrence_no=b.occurrence_no LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE b.reid_no IS NULL AND $genus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences b, reidentifications a LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE a.occurrence_no=b.occurrence_no AND a.most_recent='YES' AND $genus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences a LEFT JOIN reidentifications b ON a.occurrence_no=b.occurrence_no LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE b.reid_no IS NULL AND $subgenus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences b, reidentifications a LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE a.occurrence_no=b.occurrence_no AND a.most_recent='YES' AND $subgenus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " ORDER BY genus_name,subgenus_name,species_name";
            dbg("Get from occ table: $sql");
            @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                next if ($row->{'species_name'} =~ /^sp(p)*\.|^indet\.|s\.\s*l\./);
                my ($g,$sg,$sp) = $taxonomy->splitTaxonName($row->{'taxon_name'});
                my $match_level = 0;
                if ($row->{'taxon_name'}) {
                    $match_level = $taxonomy->computeMatchLevel($row->{'genus_name'},$row->{'subgenus_name'},$row->{'species_name'},$g,$sg,$sp);
                }
                if ($match_level < 20) { # For occs with only a genus level match, or worse
                    my $occ_name = $row->{'genus_name'};
                    if ($row->{'subgenus'}) {
                        $occ_name .= " ($row->{subgenus})";
                    }
                    $occ_name .= " ".$row->{'species_name'};
                    if ($species) {
                        if ($species ne $row->{'species_name'}) {
                            my $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_name=$occ_name&amp;is_real_user=$is_real_user">$occ_name</a>|;
                            push @possible_sister_taxa_links, $link;
                        }
                    } else {
                        my $link = qq|<a href="?a=checkTaxonInfo&amp;taxon_name=$occ_name&amp;is_real_user=$is_real_user">$occ_name</a>|;
                        push @possible_child_taxa_links, $link;
                    }
                }
            }
        }
    }
   
    # Generate the table
    
    my @letts = split //,$taxon_name;
    my $initial = $letts[0];
    if (@child_taxa_links) {
        my $rank = ($taxon_rank eq 'species') ? 'Subspecies' :
                   ($taxon_rank eq 'genus') ? 'Species' :
                                                    'Subtaxa';
$output .= qq|<div class="displayPanel" align="left" style="margin-bottom: 2em; padding-left: 1em; padding-bottom: 1em;">
  <span class="displayPanelHeader">$rank</span>
  <div class="displayPanelContent">
|;
        $_ =~ s/$taxon_name /$initial. /g foreach ( @child_taxa_links );
        $output .= join(", ",@child_taxa_links);
        $output .= qq|  </div>
</div>|;
    }

    if (@possible_child_taxa_links) {
$output .= qq|<div class="displayPanel" align="left" style="margin-bottom: 2em; padding-left: 1em; padding-bottom: 1em;">
  <span class="displayPanelHeader">Species lacking formal opinion data</span>
  <div class="displayPanelContent">
|;
        # the GROUP BY apparently fails if there are both occs and reIDs
        @possible_child_taxa_links = sort { $a cmp $b } @possible_child_taxa_links;
        $_ =~ s/>$taxon_name />$initial. /g foreach ( @possible_child_taxa_links );
        $_ =~ s/=$taxon_name /=$taxon_name\+/g foreach ( @possible_child_taxa_links );
        $output .= join(", ",@possible_child_taxa_links);
        $output .= qq|  </div>
</div>|;
    }

    if (@sister_taxa_links) {
        my $rank = ($taxon_rank eq 'species') ? 'species' :
                   ($taxon_rank eq 'genus') ? 'genera' :
                                                    'taxa';
$output .= qq|<div class="displayPanel" align="left" style="margin-bottom: 2em; padding-left: 1em; padding-bottom: 1em;">
  <span class="displayPanelHeader">Sister $rank</span>
  <div class="displayPanelContent">
|;
        $_ =~ s/$genus /$initial. /g foreach ( @sister_taxa_links );
        $output .= join(", ",@sister_taxa_links);
        $output .= qq|  </div>
</div>|;
    }
    
    if (@possible_sister_taxa_links) {
$output .= qq|<div class="displayPanel" align="left" style="margin-bottom: 2em; padding-left: 1em; padding-bottom: 1em;">
  <span class="displayPanelHeader">Sister species lacking formal opinion data</span>
  <div class="displayPanelContent">
|;
        $_ =~ s/>$genus />$initial. /g foreach ( @possible_sister_taxa_links );
        $output .= join(", ",@possible_sister_taxa_links);
        $output .= qq|  </div>
</div>|;
    }

    if (ref $taxon and $taxon->{orig_no}) {
        $output .= '<p><b><a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download authority and opinion data</a></b> - <b><a href=# onClick="javascript: document.doViewClassification.submit()">View classification of included taxa</a></b>';
        $output .= "<form method=\"POST\" action=\"\" name=\"doDownloadTaxonomy\">";
        $output .= '<input type="hidden" name="action" value="displayDownloadTaxonomyResults">';
        $output .= '<input type="hidden" name="taxon_no" value="'.$taxon->{orig_no}.'">';
        $output .= "</form>\n";
        $output .= "<form method=\"POST\" action=\"\" name=\"doViewClassification\">";
        $output .= '<input type="hidden" name="action" value="classify">';
        $output .= '<input type="hidden" name="taxon_no" value="'.$taxon->{orig_no}.'">';
        $output .= "</form>\n";
        
    }
    
    # Return the resulting HTML.
    
    return $output;
}


our %synmap1 = ('original spelling' => 'revalidated',
		'recombination' => 'recombined as ',
		'correction' => 'corrected as ',
		'rank change' => 'reranked as ',
		'reassigment' => 'reassigned as ',
		'misspelling' => 'misspelled as ');

my %synmap2 = ('belongs to' => 'revalidated ',
	       'replaced by' => 'replaced with ',
	       'nomen dubium' => 'considered a nomen dubium ',
	       'nomen nudum' => 'considered a nomen nudum ',
	       'nomen vanum' => 'considered a nomen vanum ',
	       'nomen oblitum' => 'considered a nomen oblitum ',
	       'homonym of' => ' considered a homonym of ',
	       'misspelling of' => 'misspelled as ',
	       'invalid subgroup of' => 'considered an invalid subgroup of ',
	       'subjective synonym of' => 'synonymized subjectively with ',
	       'objective synonym of' => 'synonymized objectively with ');

# updated by rjp, 1/22/2004
# gets paragraph displayed in places like the
# taxonomic history, for example, if you search for a particular taxon
# and then check the taxonomic history box at the left.
#

sub getSynonymyParagraph {
    
    my ($dbt, $taxonomy, $base_taxon, $is_real_user) = @_;
    
    my $text = "";
    
    # We start by getting all of the opinions by which the specified taxon has
    # been classified.
    
    my @opinions = $taxonomy->getOpinions('child', $base_taxon, { fields => ['child', 'parent'] });
    
    my $best_opinion = $opinions[0];
    
    # We also re-fetch the taxon itself, to make sure that we have the
    # necessary fields.
    
    my $taxon = $taxonomy->getTaxon($base_taxon, { fields => ['oldattr', 'tt', 'specimen',
							      'comments', 'discussion'] });
    
    # Now figure out who named it, if that information is in the database.
    
    if ( $taxon->{author1last} )
    {
	my $ref = $taxon->{ref_is_authority} ? 
	    Reference::formatShortRef($taxon,'alt_pubyr'=>1,'show_comments'=>1,'link_id'=>1) :
		    Reference::formatShortRef($taxon,'alt_pubyr'=>1,'show_comments'=>1);
	
	$text .= "<i><a href=\"?a=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a></i> was named by $ref.";
    }
    
    # Otherwise, we specify the currently accepted rank anonymously.  We need
    # to scan the opinions in this case to determine whether the rank has ever
    # been changed.
    
    else
    {
	my $rank = $taxon->{taxon_rank};
	my $article = $rank =~ /^[aeiou]/ ? "an" : "a";
	my $rankchanged;
	
	for my $row ( @opinions )
	{
	    $rankchanged = 1 if $row->{'spelling_reason'} =~ /rank/;
	}
	
	if ( $rankchanged )
	{
	    $text .= "<a href=\"?a=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a> was named as $article $rank. ";
	}
	
	else
	{
	    $text .= "<a href=\"?a=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a> is $article $rank. ";
	}
    }
    
    # Is it extant?  Is it a form taxon?
    
    if ($taxon->{'extant'} =~ /y/i) {
        $text .= "It is extant. ";
    } elsif (! $taxon->{'preservation'} && $taxon->{'extant'} =~ /n/i) {
        $text .= "It is not extant. ";
    }
    
    if ($taxon->{'form_taxon'} =~ /y/i) {
            $text .= "It is considered to be a form taxon. ";
    }
    
    my @spelling_nos = $taxonomy->getTaxa($taxon, 'spellings', { id => 1 });
    
    my ($typeInfo,$typeLocality) = displayTypeInfo($dbt,join(',',@spelling_nos),$taxon,$is_real_user,'checkTaxonInfo',1);
    $text .= $typeInfo;
    
    my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE type_taxon_no IN (".join(",",@spelling_nos).")";
    my @type_for = @{$dbt->getData($sql)};
    if (@type_for) {
        $text .= "It is the type $taxon->{'taxon_rank'} of ";
        foreach my $row (@type_for) {
            my $taxon_name = $row->{'taxon_name'};
            if ($row->{'taxon_rank'} =~ /genus|species/) {
                $taxon_name = "<i>".$taxon_name."</i>";
            }
            $text .= "<a href=\"?a=checkTaxonInfo&amp;taxon_no=$row->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon_name</a>, ";
        }
        $text =~ s/, $/. /;
    }

   my %phyly = ();
    foreach my $row (@opinions) {
        if ($row->{'phylogenetic_status'}) {
            push @{$phyly{$row->{'phylogenetic_status'}}},$row;
        }
    }
    my @phyly_list = keys %phyly;
    if (@phyly_list) {
        my $para_text = " It was considered ";
        @phyly_list = sort {$phyly{$a}->[-1]->{'pubyr'} <=> $phyly{$b}->[-1]->{'pubyr'}} @phyly_list;
        foreach my $phylogenetic_status (@phyly_list) {
            $para_text .= " $phylogenetic_status by ";
            my $parent_block = $phyly{$phylogenetic_status};
            $para_text .= printReferenceList($parent_block,$best_opinion);
            $para_text .= ", ";
        }
        $para_text =~ s/, $/\./;
        my $last_comma = rindex($para_text,",");
        if ($last_comma >= 0) {
            substr($para_text,$last_comma,1," and ");
        }
        $text .= $para_text;
    }

    $text .= "<br><br>";
    
    # We want to group opinions together that have the same spelling/parent
    # We do this by creating a double array - $syns[$group_index][$child_index]
    # where all children having the same parent/spelling will have the same group index
    # the hashs %(syn|rc)_group_index keep track of what the $group_index is for each clump
    my (@syns,@nomens,%syn_group_index,%rc_group_index);
    my $list_revalidations = 0;
	# If something
	foreach my $row (@opinions) {
		# put all syn's referring to the same taxon_name together
        if ($row->{'status'} =~ /subgroup|synonym|homonym|replaced|misspell/) {
            if (!exists $syn_group_index{$row->{'parent_spelling_no'}}) {
                $syn_group_index{$row->{'parent_spelling_no'}} = scalar(@syns);
            }
            my $index = $syn_group_index{$row->{'parent_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /nomen/) {
	        # Combine all adjacent like status types @nomens
	        # (They're chronological: nomen, reval, reval, nomen, nomen, reval, etc.)
            my $index;
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$#nomens][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /corr|rank|recomb/ || $row->{'spelling_reason'} =~ /^corr|^rank|^recomb|^reass/) {
            if (!exists $rc_group_index{$row->{'child_spelling_no'}}) {
                $rc_group_index{$row->{'child_spelling_no'}} = scalar(@syns);
            }
            my $index = $rc_group_index{$row->{'child_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif (($row->{'status'} =~ /belongs/ && $list_revalidations && $row->{'spelling_reason'} !~ /^recomb|^corr|^rank|^reass/)) {
            # Belongs to's are only considered revalidations if they come
            # after a recombined as, synonym, or nomen *
            my $index;
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$#nomens][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
        }
    }
    
    # Now combine the synonyms and nomen/revalidation arrays, with the nomen/revalidation coming last
    my @synonyms = (@syns,@nomens);
    
    # Exception to above:  the most recent opinion should appear last. Splice it to the end
    if (@synonyms) {
        my $oldest_pubyr = 0;
        my $oldest_group = 0; 
        for(my $i=0;$i<scalar(@synonyms);$i++){
            my @group = @{$synonyms[$i]};
            if ($group[$#group]->{'pubyr'} > $oldest_pubyr) {
                $oldest_group = $i; 
                $oldest_pubyr = $group[$#group]->{'pubyr'};
            }
        }
        my $most_recent_group = splice(@synonyms,$oldest_group,1);
        push @synonyms,$most_recent_group;
    }
	
    # Loop through unique parent number from the opinions table.
    # Each parent number is a hash key whose value is an array ref of records.
    foreach my $group (@synonyms)
    {
        my $first_row = ${$group}[0];
        if ($first_row->{'status'} =~ /belongs/) {
            if ($first_row->{'spelling_reason'} eq 'rank change') {
                my $child = $taxonomy->getTaxon($first_row->{'child_no'});
                my $spelling = $taxonomy->getTaxon($first_row->{'child_spelling_no'});
                if ($child->{'taxon_rank'} =~ /genus/) {
		            $text .= "; it was reranked as ";
                } else {
                    $text .= "; it was reranked as the $spelling->{taxon_rank} ";
                }
            } elsif ( $synmap1{$first_row->{'spelling_reason'}} ne "revalidated" || $first_row ne ${$group}[0] ) {
		        $text .= "; it was ".$synmap1{$first_row->{'spelling_reason'}};
            }
        } else {
		    $text .= "; it was ".$synmap2{$first_row->{'status'}};
        }
	
        if ($first_row->{'status'} !~ /nomen/) {
            my $taxon_no;
            if ($first_row->{'status'} =~ /subgroup|synonym|replaced|homonym|misspelled/) {
                $taxon_no = $first_row->{'parent_spelling_no'};
            } elsif ($first_row->{'status'} =~ /misspell/) {
                $taxon_no = $first_row->{'child_spelling_no'};
            } elsif ($first_row->{'spelling_reason'} =~ /correct|recomb|rank|reass|missp/) {
                $taxon_no = $first_row->{'child_spelling_no'};
            }
            if ($taxon_no) {
                my $taxon = $taxonomy->getTaxon($taxon_no, { fields => 'oldattr' });
                if ($taxon->{'taxon_rank'} =~ /genus|species/) {
			        $text .= "<i>".$taxon->{'taxon_name'}."</i>";
                } else {
			        $text .= $taxon->{'taxon_name'};
                }
                if ($first_row->{'status'} eq 'homonym of') {
                    my $pub_info = Reference::formatShortRef($taxon);
                    $text .= ", $pub_info";
                }
            }
        }
	
	if ( $first_row->{'status'} !~ /belongs/ || $synmap1{$first_row->{'spelling_reason'}} ne "revalidated" || $first_row ne ${$group}[0] )
	{
	    if ($first_row->{'status'} eq 'misspelling of') {
		$text .= " according to ";
	    } else {
		$text .= " by ";
	    }
	    $text .= printReferenceList($group,$best_opinion);
	}
    }
    
    if ( $text ne "" )
    {
        # Capitalize first 'it' and make sure the string ends with a period.
        $text .= '.' unless $text =~ /\.\s*$/;
	$text =~ s/;\s+it/It/;
    }
    
    my %parents = ();
    foreach my $row (@opinions) {
        if ($row->{'status'} =~ /belongs/) {
            if ($row->{'parent_spelling_no'}) { # Fix for bad opinions. See Asinus, Equus some of the horses
                push @{$parents{$row->{'parent_spelling_no'}}},$row;
            }
        }
    }
    $text =~ s/<br><br>\s*\.\s*$//i;
    my @parents_ordered = sort {$parents{$a}[-1]->{'pubyr'} <=> $parents{$b}[-1]->{'pubyr'} } keys %parents;
    if (@parents_ordered && $taxon->{'taxon_rank'} !~ /species/) {
        $text .= "<br><br>";
        #my $taxon_name = $taxon->{'taxon_name'};
        #if ($taxon->{'taxon_rank'} =~ /genus|species/) {
        #    $taxon_name = "<i>$taxon_name</i>";
        #}
        #$text .= "<a href=\"?a=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon_name</a> was assigned ";
        $text .= "It was assigned";
        for(my $j=0;$j<@parents_ordered;$j++) {
            my $parent_no = $parents_ordered[$j];
            my $parent = getTaxa($dbt,{'taxon_no'=>$parent_no});
            my @parent_array = @{$parents{$parent_no}};
            $text .= " and " if ($j==$#parents_ordered && @parents_ordered > 1);
            my $parent_name = $parent->{'taxon_name'};
            if ($parent->{'taxon_rank'} =~ /genus|species/) {
                $parent_name = "<i>$parent_name</i>";
            }
            $text .= " to <a href=\"?a=checkTaxonInfo&amp;taxon_no=$parent->{taxon_no}&amp;is_real_user=$is_real_user\">$parent_name</a> by ";
            $text .= printReferenceList(\@parent_array,$best_opinion);
            $text .= "; ";
        }
        $text =~ s/; $/\./;
    }
    
    return $text;
}


# Only used in the above function, just a simple utility to print out a formatted
# list of references

sub printReferenceList {
    
    my ($reflist, $best_opinion) = @_;
    
    my $text = " ";
    
    foreach my $ref (@$reflist)
    {
	if ($ref->{'ref_has_opinion'} =~ /yes/i) {
	    if ( $ref->{'opinion_no'} eq $best_opinion )	{
		$text .= "<b>";
	    }
	    $text .= Reference::formatShortRef($ref,'alt_pubyr'=>1,'show_comments'=>1, 'link_id'=>1);
	    if ( $ref->{'opinion_no'} eq $best_opinion )	{
		$text .= "</b>";
	    }
	    $text .= ", ";
	} else {
	    if ( $ref->{'opinion_no'} eq $best_opinion )	{
		$text .= "<b>";
	    }
	    $text .= Reference::formatShortRef($ref,'alt_pubyr'=>1,'show_comments'=>1);
	    if ( $ref->{'opinion_no'} eq $best_opinion )	{
		$text .= "</b>";
	    }
	    $text .= ", ";
	}
    }
    $text =~ s/, $//;
    my $last_comma = rindex($text,",");
    if ($last_comma >= 0) {
	substr($text,$last_comma,1," and ");
    }
    
    return $text;
}


# split out as a function 4.11.09
sub displayTypeInfo	{

    my $dbt = shift;
    my $spellings = shift;
    my $taxon = shift;
    my $is_real_user = shift;
    my $taxonInfoGoal = shift;
    my $preface = shift;
    my $text;

    if ($taxon->{'taxon_rank'} =~ /species/) {
        if ( $taxon->{'type_specimen'} || $taxon->{'type_body_part'} || $taxon->{'part_details'} || $taxon->{'type_locality'} )	{
            if ($taxon->{'type_specimen'})	{
                if ( $preface )	{
                    $text .= "Its type specimen is ";
                }
                $text .= "$taxon->{type_specimen}";
                if ($taxon->{'type_body_part'}) {
                    my $an = ($taxon->{'type_body_part'} =~ /^[aeiou]/) ? "an" : "a";
                    $text .= ", " if ($taxon->{'type_specimen'});
                    if ( $taxon->{type_body_part} =~ /teeth|postcrania|vertebrae|limb elements|appendages|ossicles/ )	{
                        $an = "a set of";
                    }
                    $text .= "$an $taxon->{type_body_part}";
                }
                if ($taxon->{'part_details'}) {
                    $text .= " ($taxon->{part_details})";
                }
                $text .= ". ";
            }
            # don't report preservation for extant taxa
            if ($taxon->{'preservation'} && $taxon->{'extant'} !~ /y/i) {
                my %p = ("body (3D)" => "3D body fossil", "compression" => "compression fossil", "soft parts (3D)" => "3D fossil preserving soft parts", "soft parts (2D)" => "compression preserving soft parts", "amber" => "inclusion in amber", "cast" => "cast", "mold" => "mold", "impression" => "impression", "trace" => "trace fossil", "not a trace" => "not a trace fossil");
                my $preservation = $p{$taxon->{'preservation'}};
                if ($preservation =~ /^[aieou]/) {
                    $preservation = "an $preservation";
                } elsif ($preservation !~ /^not/ ) {
                    $preservation = "a $preservation";
                }
                if ($taxon->{'type_specimen'} && $taxon->{'type_body_part'})	{
                    $text =~ s/\. $/, /;
                    $text .= "and it is $preservation. ";
                } elsif ($taxon->{'type_specimen'})	{
                    $text =~ s/\. $/ /;
                    $text .= "and is $preservation. ";
                } else	{
                    $text .= "It is $preservation. ";
                }
            }
            if ($taxon->{'type_locality'} > 0)	{
                my $sql = "SELECT i.interval_name AS max,IF (min_interval_no>0,i2.interval_name,'') AS min,IF (country='United States',state,country) AS place,collection_name,formation,lithology1,fossilsfrom1,lithology2,fossilsfrom2,environment FROM collections c,intervals i,intervals i2 WHERE collection_no=".$taxon->{'type_locality'}." AND i.interval_no=max_interval_no AND (min_interval_no=0 OR i2.interval_no=min_interval_no)";
                my $coll_row = ${$dbt->getData($sql)}[0];
                $coll_row->{'lithology1'} =~ s/not reported//;
                my $strat = $coll_row->{'max'};
                if ( $coll_row->{'min'} )	{
                    $strat .= "/".$coll_row->{'min'};
                }
                my $fm = $coll_row->{'formation'};
                if ( $fm )	{
                    $fm = "the $fm Formation";
                }
                if ( $coll_row->{'fossilsfrom1'} eq "YES" && $coll_row->{'fossilsfrom2'} ne "YES" )	{
                    $coll_row->{'lithology2'} = "";
                } elsif ( $coll_row->{'fossilsfrom1'} ne "YES" && $coll_row->{'fossilsfrom2'} eq "YES" )	{
                    $coll_row->{'lithology1'} = "";
                }
                my $lith = $coll_row->{'lithology1'};
                if ( $coll_row->{'lithology2'} )	{
                    $lith .= "/" . $coll_row->{'lithology2'};
                }
                if ( ! $lith )	{
                    $lith = "horizon";
                }
                if ( $coll_row->{'environment'} )	{
                    if ( $strat =~ /^[AEIOU]/ )	{
                        $strat = "an ".$strat;
                    } else	{
                        $strat = "a ".$strat;
                    }
                    if ( $fm ) { $fm = "in $fm of"; } else { $fm = "in"; }
                    $lith = $coll_row->{'environment'}." ".$lith;
                } else	{
                    $strat = "the ".$strat." of ";
                }
                $lith =~ s/ indet\.//;
                $lith =~ s/"//g;
                $coll_row->{'place'} =~ s/,.*//;
                $coll_row->{'place'} =~ s/Libyan Arab Jamahiriya/Libya/;
                $coll_row->{'place'} =~ s/Syrian Arab Republic/Syria/;
                $coll_row->{'place'} =~ s/Lao People's Democratic Republic/Laos/;
                $coll_row->{'place'} =~ s/(United Kingdom|Russian Federation|Czech Republic|Netherlands|Dominican Republic|Bahamas|Philippines|Netherlands Antilles|United Arab Emirates|Marshall Islands|Congo|Seychelles)/the $1/;
                $text .= "Its type locality is <a href=\"?a=basicCollectionSearch&amp;collection_no=".$taxon->{'type_locality'}."&amp;is_real_user=$is_real_user\">".$coll_row->{'collection_name'}."</a>, which is in $strat $lith $fm $coll_row->{'place'}. ";
            }
        }
    } else {
        my $sql = "SELECT taxon_no,type_taxon_no FROM authorities WHERE type_taxon_no != 0 AND taxon_no IN (".$spellings.")";
        my $tt_row = ${$dbt->getData($sql)}[0];
        if ($tt_row) {
            my $type_taxon = getTaxa($dbt,{'taxon_no'=>$tt_row->{'type_taxon_no'}});
            my $type_taxon_name = $type_taxon->{'taxon_name'};
            if ($type_taxon->{'taxon_rank'} =~ /genus|species/) {
                $type_taxon_name = "<i>".$type_taxon_name."</i>";
            }
            if ( $preface )	{
                $text .= "Its type is ";
            }
            $text .= "<a href=\"?a=$taxonInfoGoal&amp;taxon_no=$type_taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$type_taxon_name</a>. ";  
        }
    }

    return ($text,$taxon->{'type_locality'});
}


# JA 1.8.03
sub displayEcology {
    
    my ($dbt, $taxonomy, $focal_taxon) = @_;
    
    my $output .= qq|<div class="small displayPanel" align="left" style="width: 46em; margin-top: 0em; padding-top: 1em; padding-bottom: 1em;">
<div align="center" class="displayPanelContent">
|;

    unless ( $focal_taxon )
    {
	$output .= qq|<i>No ecological data are available</i>
</div>
</div>
|;
	return $output;
    }
    
    my $taxon_no = re $focal_taxon ? $focal_taxon->{taxon_no} : $focal_taxon;
    
    # get the field names from the ecotaph table
    my @ecotaphFields = $dbt->getTableColumns('ecotaph');

    # get the values for the focal taxon and all its ancestors
    my $eco_hash = Ecology::getEcology($dbt,$focal_taxon,\@ecotaphFields, { get_basis => 1 });
    
    my $ecotaphVals = $eco_hash->{$taxon_no};
    
    if ( ! $ecotaphVals )
    {
	$output .= qq|<i>No ecological data are available</i>
</div>
</div>
|;
	return $output;
    } 
    
    # Convert units for display
    foreach ('minimum_body_mass','maximum_body_mass','body_mass_estimate')
    {
	if ($ecotaphVals->{$_}) {
	    if ($ecotaphVals->{$_} < 1) {
		$ecotaphVals->{$_} = Ecology::kgToGrams($ecotaphVals->{$_});
		$ecotaphVals->{$_} .= ' g';
	    } else {
		$ecotaphVals->{$_} .= ' kg';
	    }
	}
    } 
    
    my @references = @{$ecotaphVals->{'references'}};     
    
    my @ranks = ('subspecies', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade');
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
    my %all_ranks = ();
    
    $output .= "<table cellpadding=\"4\">";
    $output .= "<tr>";
    my $cols = 0;
    foreach my $i (0..$#ecotaphFields)	{
	my $name = $ecotaphFields[$i];
	my $nextname = $ecotaphFields[$i+1];
	my $n = $name;
	my @letts = split //,$n;
	$letts[0] =~ tr/[a-z]/[A-Z]/;
	$n = join '',@letts;
	$n =~ s/_/ /g;
	$n =~ s/Taxon e/E/;
	if ( $n =~ /1/ && $ecotaphVals->{$nextname} !~ /2/ )	{
	    $n =~ s/1//;
	}
	$n =~ s/1$/&nbsp;1/g;
	$n =~ s/2$/&nbsp;2/g;
	if ( $ecotaphVals->{$name} && $name !~ /_no$/ )	{
	    my $v = $ecotaphVals->{$name};
	    my $rank = $ecotaphVals->{$name."basis"};
	    $all_ranks{$rank} = 1; 
	    $v =~ s/,/, /g;
	    if ( $cols == 2 || $name =~ /^comments$/ || $name =~ /^created$/ || $name =~ /^size_value$/ || $name =~ /1$/ )	{
		$output .= "</tr>\n<tr>\n";
		$cols = 0;
	    }
	    $cols++;
	    my $colspan = ($name =~ /comments/) ? "colspan=2" : "";
	    my $rank_note = "<span class=\"superscript\">$rankToKey{$rank}</span>";
	    if ($name =~ /created|modified/) {
		$rank_note = "";
	    }
	    $output .= "<td $colspan valign=\"top\"><table cellpadding=0 cellspacing=0 border=0><tr><td align=\"left\" valign=\"top\"><span class=\"fieldName\">$n:</span>&nbsp;</td><td valign=\"top\">${v}${rank_note}</td></tr></table></td> \n";
	}
    }
    $output .= "</tr>" if ( $cols > 0 );
    # now print out keys for superscripts above
    $output .= "<tr><td colspan=2>";
    my $html = "<span class=\"fieldName\">Source:</span> ";
    foreach my $rank (@ranks) {
	if ($all_ranks{$rank}) {
	    $html .= "$rankToKey{$rank} = $rank, ";
	}
    }
    $html =~ s/, $//;
    $output .= $html;
    $output .= "</td></tr>"; 
    $output .= "<tr><td colspan=2><span class=\"fieldName\">";
    if (scalar(@references) == 1) {
	$output .= "Reference: ";
    } elsif (scalar(@references) > 1) {
	$output .= "References: ";
    }
    $output .= "</span>";
    for(my $i=0;$i<scalar(@references);$i++) {
	my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$references[$i]";
	my $ref = ${$dbt->getData($sql)}[0];
	$references[$i] = Reference::formatShortRef($ref,'link_id'=>1);
    }

    $output .= join(", ",@references);
    $output .= "</td></tr>";
    $output .= "</table>\n";
    
    $output .= "\n</div>\n</div>\n";
    return $output;
}


# PS 6/27/2005
sub displayMeasurements {
    
    my ($dbt, $taxonomy, $focal_taxon) = @_;
    
    # Specimen level data:
    my @specimens;
    my $specimen_count;
    
    if ($focal_taxon->{taxon_rank} =~ /genus|species/)
    {
	# If the rank is genus or lower we want the big aggregate list of all taxa
	unless ( $focal_taxon->{child_no_list} )
	{
	    my @child_nos = $taxonomy->getTaxa($focal_taxon, 'all_children', { id => 1, exclude_self => 1 });
	    $focal_taxon->{child_no_list} = \@child_nos;
	}
	
	@specimens = Measurement::getMeasurements($dbt, $taxonomy, 
						  taxon_list => $focal_taxon->{child_no_list}, 
						  get_global_specimens => 1);
    } 
    
    elsif ( $focal_taxon )
    {
	# If the rank is higher than genus, then that rank is too big to be meaningful.  
	# In that case we only want the taxon itself (and its synonyms and alternate names), not the big recursively generated list
	# i.e. If they entered Nasellaria, get Nasellaria indet., or Nasellaria sp. or whatever.
	# get alternate spellings of focal taxon. 
	my @synonym_nos = $taxonomy->getTaxa($focal_taxon, 'synonyms', { id => 1 });
	@specimens = Measurement::getMeasurements($dbt, $taxonomy, taxon_list => \@synonym_nos, 
						  get_global_specimens => 1);
    }

    # Returns a triple index hash with index <part><dimension type><whats measured>
    #  Where part can be leg, valve, etc, dimension type can be length,width,height,circumference,diagonal,diameter,inflation 
    #   and whats measured can be average, min,max,median,error
    my $p_table_ref = Measurement::getMeasurementTable(\@specimens);
    my %p_table = %{$p_table_ref};

    my $mass_string;

    my $str .= qq|<div class="displayPanel" align="left" style="width: 36em;">
<span class="displayPanelHeader" class="large">Measurements</span>
<div align="center" class="displayPanelContent">
|;

    if (@specimens) {

        my %errorSeen = ();
        my %partHeader = ();
        $partHeader{'average'} = "mean";
        my $defaultError = "";
        for my $part ( keys %p_table )	{
	    next unless ref $p_table{$part} eq 'HASH';
            my %m_table = %{$p_table{$part}};
            foreach my $type (('length','width','height','circumference','diagonal','diameter','inflation')) {
                if (exists ($m_table{$type})) {
                    if ( $m_table{$type}{'min'} )	{
                        $partHeader{'min'} = "minimum";
                    }
                    if ( $m_table{$type}{'max'} )	{
                        $partHeader{'max'} = "maximum";
                    }
                    if ( $m_table{$type}{'median'} )	{
                        $partHeader{'median'} = "median";
                    }
                    if ( $m_table{$type}{'error_unit'} )	{
                        $partHeader{'error'} = "error";
                        $errorSeen{$m_table{$type}{'error_unit'}}++;
                        my @errors = keys %errorSeen;
                        if ( $#errors == 0 )	{
                            $m_table{$type}{'error_unit'} =~ s/^1 //;
                            $defaultError = $m_table{$type}{'error_unit'};
                        } else	{
                            $defaultError = "";
                        }
                    }
                }
            }
        }

        # estimate body mass if possible JA 18.7.07
        # code is here and not earlier because we need the parts list first
        my @m = Measurement::getMassEstimates($dbt,$taxonomy,$focal_taxon->{taxon_no},$p_table_ref);
        my @part_list = @{$m[0]};
        my @masses = @{$m[2]};
        my @eqns = @{$m[3]};
        my @refs = @{$m[4]};
        my $grandmean = $m[5];
        my $grandestimates = $m[6];

        for my $i ( 0..$#masses )	{
            my $reference = Reference::formatShortRef($dbt,$refs[$i],'no_inits'=>1,'link_id'=>1);
            $mass_string .= "<tr><td>&nbsp;";
            $mass_string .= formatMass($masses[$i]);
            $mass_string .= '</td>';
            $mass_string .= "<td><span class=\"small\">&nbsp;$eqns[$i]</span></td><td><span class=\"small\">$reference</span></td></tr>";
        }

        if ( $mass_string )	{
            if ( $#masses > 0 )	{
                $mass_string .= '<tr><td colspan="3">mean: '.formatMass( exp( $grandmean / $grandestimates ) )."</td></tr>\n";
            }
            $mass_string = qq|<div class="displayPanel" align="left" style="width: 36em; margin-bottom: 2em;">
<span class="displayPanelHeader" class="large">Body mass estimates</span>
<div align="center" class="displayPanelContent">
<table cellspacing="6"><tr><th align="center">estimate</th><th align="center">equation</th><th align="center">reference</th></tr>
$mass_string
</table>
</div>
</div>
|;
        }

        my $temp;
        my $spacing = "5px";
        if ( ! $partHeader{'min'} )	{
            $spacing = "8px";
        }
        $str .= "<table cellspacing=\"$spacing;\"><tr><th>part</th><th align=\"left\">N</th><th>$partHeader{'average'}</th><th>$partHeader{'min'}</th><th>$partHeader{'max'}</th><th>$partHeader{'median'}</th><th>$defaultError</th><th></th></tr>";
        for my $part ( @part_list )	{
            if ( ! $p_table{$part} )	{
                next;
            }
            my %m_table = %{$p_table{$part}};
            $temp++;

            foreach my $type (('length','width','height','circumference','diagonal','diameter','inflation')) {
                if (exists ($m_table{$type})) {
                    if ( $m_table{$type}{'average'} <= 0 )	{
                        next;
                    }
                    $str .= "<tr><td>$part $type</td>";
                    $str .= "<td>$m_table{specimens_measured}</td>";
                    foreach my $column (('average','min','max','median','error')) {
                        my $value = $m_table{$type}{$column};
                        if ( $value <= 0 && $partHeader{$column} ) {
                            $str .= "<td align=\"center\">-</td>";
                        } elsif ( ! $partHeader{$column} ) {
                            $str .= "<td align=\"center\"></td>";
                        } else {
                            if ( $value < 1 )	{
                                $value = sprintf("%.3f",$value);
                            } elsif ( $value < 10 )	{
                                $value = sprintf("%.2f",$value);
                            } else	{
                                $value = sprintf("%.1f",$value);
                            }
                            $str .= "<td align=\"center\">$value</td>";
                        }
                    }
                    $str .= qq|<td align="center" style="white-space: nowrap;">|;
                    if ( $m_table{$type}{'error'} && ! $defaultError ) {
                        $m_table{$type}{error_unit} =~ s/^1 //;
                        $str .= qq|$m_table{$type}{error_unit}|;
                    }
                    $str .= '</td></tr>';
                }
            }
        }
        $str .= "</table><br>\n";
    } else {
        $str .= "<div align=\"center\" style=\"padding-bottom: 1em;\"><i>No measurements are available</i>\n</div>\n";
    }
    $str .= qq|</div>
</div>
|;

    if ( $mass_string )	{
        $str = $mass_string . $str;
    }

    return $str;
}


# JA 7.12.10
sub formatMass	{
	my $mass = shift;
	if ( $mass < 1000 )	{
		$mass = sprintf "%.1f g",$mass;
	} elsif ( $mass < 10000 )	{
		$mass = sprintf "%.2f kg",$mass / 1000;
	} elsif ( $mass > 10000 && $mass < 1000000 )	{
		$mass = sprintf "%.1f kg",$mass / 1000;
	} elsif ( $mass < 10000000 )	{
		$mass = sprintf "%.2f tons",$mass / 1000000;
	} else	{
		$mass = sprintf "%.1f tons",$mass / 1000000;
	}
	return $mass;
}



sub displayDiagnoses {
    
    my ($dbt, $taxonomy, $focal_taxon) = @_;
    
    my $taxon_no = $focal_taxon->{taxon_no};
    
    my $str = qq|<div class="displayPanel" align="left" style="width: 36em; margin-top: 2em; margin-bottom: 2em; padding-bottom: 1em;">
<span class="displayPanelHeader" class="large">Diagnosis</span>
<div class="displayPanelContent">
|;
    
    my @diagnoses = ();
    if ($focal_taxon) {
        @diagnoses = getDiagnoses($dbt, $taxonomy, $focal_taxon);

        if (@diagnoses) {
            $str .= "<table cellspacing=5>\n";
            $str .= "<tr><th>Reference</th><th>Diagnosis</th></tr>\n";
            foreach my $row (@diagnoses) {
                $str .= "<tr><td valign=top><span style=\"white-space: nowrap\">$row->{reference}</span>";
                if ($row->{'is_synonym'}) {
                    if ($row->{'taxon_rank'} =~ /species|genus/) {
                        $str .= " (<i>$row->{taxon_name}</i>)";
                    } else {
                        $str .= " ($row->{taxon_name})";
                    }
                } 
                $row->{diagnosis} =~ s/\n/<br>/g;
                $str .= "</td><td>$row->{diagnosis}<td></tr>";
            }
            $str .= "</table>\n";
        } 
    } 
    if ( ! $taxon_no || ! @diagnoses ) {
        $str .= "<div align=\"center\"><i>No diagnoses are available</i></div>";
    }
    $str .= "\n</div>\n</div>\n";
    return $str;
}


# JA 11-12,14.9.03
# rewritten and shortened 16.7.07 JA
# new version assumes you only ever want to know who named or classified the
#  taxon and its synonyms, and not who assigned something to one of them
# Rewritten slightly MM 2012-12-05

sub displaySynonymyList	{
    
    my ($dbt, $taxonomy, $focal_taxon, $is_real_user) = @_;
    
    # taxon_no must be an original combination
    
    my $taxon_no = $focal_taxon->{taxon_no};
    my $output = "";
    
    $output .= qq|<div align="left" class="displayPanel" style="width: 42em; margin-top: 0em;">
<span class="displayPanelHeader" style="text-align: left;">Synonymy list</span>
<div align="center" class="small displayPanelContent" style="padding-top: 0em; padding-bottom: 1em;">
|;

    unless ($taxon_no) {
	$output .= "<div align=\"center\" style=\"padding-top: 0.75em;\"><i>No taxonomic opinions are available</i></div>";
	$output .= "</table>\n</div>\n</div>\n";
	return $output;
    }
    
    # Find all spellings of the focal taxon plus its junior synonyms.
    
    
    my @taxa = $taxonomy->getTaxa('spellings', $focal_taxon);
    
    push @taxa, $taxonomy->getTaxa('juniors', $focal_taxon, { spelling => 'all' });
    
    # Get all opinions with any of these taxa as children.
    
    my @opinions = $taxonomy->getOpinions('children', \@taxa);
    
    # do some initial formatting and create a name lookup hash
    my %spelling = ();
    my %rank = ();
    my %synline = ();
    for my $ar ( @taxa )
    {
	$spelling{$ar->{taxon_no}} = $ar->{taxon_name};
	$rank{$ar->{taxon_no}} = $ar->{taxon_rank};
	if ( $ar->{taxon_no} == $ar->{orig_no} )
	{
	    my $synkey = buildSynLine($ar, $is_real_user);
	    $synline{$synkey}{TAXON} = $ar->{taxon_name};
	    $synline{$synkey}{YEAR} = $ar->{pubyr};
	    $synline{$synkey}{AUTH} = $ar->{author1last} . " " . $ar->{author2last};
	    $synline{$synkey}{PAGES} = $ar->{pages};
	}
    }
    
    # go through the opinions only now that you have the names
    for my $or ( @opinions )
    {
	if ( $or->{status} =~ /belongs to/ )
	{
	    $or->{taxon_name} = $spelling{$or->{child_spelling_no}};
	    $or->{taxon_rank} = $rank{$or->{child_spelling_no}};
	    my $synkey = buildSynLine($or, $is_real_user);
	    $synline{$synkey}{TAXON} = $or->{taxon_name};
	    $synline{$synkey}{YEAR} = $or->{pubyr};
	    $synline{$synkey}{AUTH} = $or->{author1last} . " " . $or->{author2last};
	    $synline{$synkey}{PAGES} = $or->{pages};
	}
    }
    
    # sort the synonymy list by pubyr
    my @synlinekeys = sort { $synline{$a}->{YEAR} <=> $synline{$b}->{YEAR} || $synline{$a}->{AUTH} cmp $synline{$b}->{AUTH} || $synline{$a}->{PAGES} <=> $synline{$b}->{PAGES} || $synline{$a}->{TAXON} cmp $synline{$b}->{TAXON} } keys %synline;
    
    # print each line of the synonymy list
    $output .= qq|<table cellspacing=5>
<tr><th>Year</th><td>Name and author</th></tr>
|;
    
    my $lastline;
    
    foreach my $synline ( @synlinekeys )
    {
	if ( $synline{$synline}->{YEAR} . $synline{$synline}->{AUTH} . $synline{$synline}->{TAXON} ne $lastline )
	{
	    $output .= "<tr>$synline</td></tr>\n";
	}
	$lastline = $synline{$synline}->{YEAR} . $synline{$synline}->{AUTH} . $synline{$synline}->{TAXON};
    }
    
    $output .= "</table>\n</div>\n</div>\n";
    
    return $output;
}


sub buildSynLine {
    
    my ($refdata, $is_real_user) = @_;
    my $synkey = "";
    
    if ( $refdata->{pubyr} )
    {
	$synkey = "<td valign=\"top\">" . $refdata->{pubyr} . "</d><td valign=\"top\">";
	if ( $refdata->{taxon_rank} =~ /genus|species/ )	{
	    $synkey .= "<i>";
	}
	$synkey .= $refdata->{taxon_name};
	if ( $refdata->{taxon_rank} =~ /genus|species/ )	{
	    $synkey .= "</i>";
	}
	$synkey .= " ";
	my $authorstring = $refdata->{author1last};;
	if ( $refdata->{otherauthors} )	{
	    $authorstring .= " et al.";
	} elsif ( $refdata->{author2last} )	{
	    $authorstring .= " and " . $refdata->{author2last};
	}
	if ( $refdata->{ref_is_authority} eq "YES" || $refdata->{ref_has_opinion} eq "YES" )	{
	    $authorstring = "<a href=\"?a=displayReference&amp;reference_no=$refdata->{reference_no}&amp;is_real_user=$is_real_user\">" . $authorstring . "</a>";
	}
	$synkey .= $authorstring;
    }
    if ( $refdata->{pages} )	{
	if ( $refdata->{pages} =~ /[ -]/ )	{
	    $synkey .= " pp. " . $refdata->{pages};
	} else	{
	    $synkey .= " p. " . $refdata->{pages};
	}
    }
    if ( $refdata->{figures} )	{
	if ( $refdata->{figures} =~ /[ -]/ )	{
	    $synkey .= " figs. " . $refdata->{figures};
	} else	{
	    $synkey .= " fig. " . $refdata->{figures};
	}
    }
    
    return $synkey;
}
	

# JA 10.1.09
sub beginFirstAppearance	{
	my ($hbo,$q,$error_message) = @_;
	print $hbo->populateHTML('first_appearance_form', [$error_message], ['error_message']);
	if ( $error_message )	{
		exit;
	}
}

# JA 10-13.1.09
sub displayFirstAppearance	{
	my ($q,$s,$dbt,$taxonomy,$hbo) = @_;
	
	my ($search_name, $use_common);
	
	if ( $q->param('taxon_name') )	{
		if ( $q->param('taxon_name') !~ /^[A-Z][a-z]*(| )[a-z]*$/ )	{
			my $error_message = "The name '".$q->param('taxon_name')."' is formatted incorrectly.";
			beginFirstAppearance($hbo,$q,$error_message);
		}
		if ( $q->param('common_name') )	{
			my $error_message = "Please enter either a scientific or common name, not both.";
			beginFirstAppearance($hbo,$q,$error_message);
		}
		$search_name = $q->param('taxon_name');
		$use_common = 0;
	} elsif ( $q->param('common_name') )	{
		if ( $q->param('common_name') =~ /[^A-Za-z ]/ )	{
			my $error_message = "A common name can't include anything but letters.";
			beginFirstAppearance($hbo,$q,$error_message);
		}
		$search_name = $q->param('common_name');
		$use_common = 'only';
	} else	{
		my $error_message = "No search term was entered.";
		beginFirstAppearance($hbo,$q,$error_message);
	}
	
	# If any taxa were specified for exclusion, create an exclusion object
	# to be used in subsequent queries.
	
	my $exclusion;
	if ( $q->param('exclude') )
	{
	    my @names = split /[\s,]+/, $q->param('exclude');
	    my @excluded_taxa = $taxonomy->getTaxaByName(\@names);
	    my $exclusion = $taxonomy->generateExclusion(\@excluded_taxa);
	}
	
	# Now fetch our focal taxon.  If more than one match is found, get the
	# biggest one.
	
	my ($focal_taxon, @other_matches) = 
	    $taxonomy->getTaxaByName($search_name, { common => $use_common,
						     senior => 1,
						     order => 'size.desc',
						     fields => ['oldattr', 'lft', 'extant'] });
	
	unless ( $focal_taxon )
	{
		my $error_message = "$search_name is not in the system.";
		beginFirstAppearance($hbo,$q,$error_message);
	}
	
	# Figure out the name and attribution of the taxon.
	
	my $name = $focal_taxon->{taxon_name};
	
	if ( $focal_taxon->{common_name} )
	{
	    $name .= " ($focal_taxon->{common_name})";
	}
	
	my $authors = $focal_taxon->{'author1last'};
	if ( $focal_taxon->{'otherauthors'} )	{
		$authors .= " <i>et al.</i>";
	} elsif ( $focal_taxon->{'author2last'} )	{
		$authors .= " and ".$focal_taxon->{'author2last'};
	}
	$authors .= " ".$focal_taxon->{'pubyr'};
	
	# If this is a higher taxon, make sure it includes at least one
	# child taxon.
	
	if ( $focal_taxon->{'lft'} == $focal_taxon->{'rgt'} - 1 && $focal_taxon->{'taxon_rank'} !~ /genus|species/ )
	{
	    my $error_message = "$name $authors includes no classified subtaxa.";
	    beginFirstAppearance($hbo,$q,$error_message);
	}
	
	# Generate the result page.
	
	print $hbo->stdIncludes("std_page_top");
	
	# MAIN TABLE HITS
	
	my @allsubtaxa = $taxonomy->getTaxa('all_children', $focal_taxon, 
					    { exclude => $exclusion,
					      fields => ['lft', 'preservation', 'extant'] });
	
	my @subtaxa;
	
	if ( $q->param('taxonomic_precision') =~ /species|genus|family/ )
	{
	    my @ranks = ('subspecies','species');
	    if ( $q->param('taxonomic_precision') =~ /genus or species/ )	{
		push @ranks , ('subgenus','genus');
	    } elsif ( $q->param('taxonomic_precision') =~ /family/ )	{
		push @ranks , ('subgenus','genus','tribe','subfamily','family');
	    }
	    
	    my @extra_filters;
	    
	    if ( $q->param('types_only') =~ /yes/i )
	    {
		push @extra_filters,  "a.type_locality > 0";
	    }
	    
	    if ( $q->param('type_body_part') )
	    {
		my $parts;
		if ( $q->param('type_body_part') =~ /multiple teeth/i )	{
		    $parts = "'skeleton','partial skeleton','skull','partial skull','maxilla','mandible','teeth'";
		} elsif ( $q->param('type_body_part') =~ /skull/i )	{
		    $parts = "'skeleton','partial skeleton','skull','partial skull'";
		} elsif ( $q->param('type_body_part') =~ /skeleton/i )	{
		    $parts = "'skeleton','partial skeleton'";
		}
		push @extra_filters, "a.type_body_part IN ($parts)" if $parts;
	    }
	    
	    @subtaxa = $taxonomy->getTaxa('all_children', $focal_taxon,
					  { exclude => $exclusion,
					    rank => \@ranks,
					    extra_filters => \@extra_filters,
					    fields => ['lft', 'preservation', 'extant'] });
	    
	} 
	
	else
	{
	    @subtaxa = @allsubtaxa;
	}
	
	my $extant = $focal_taxon->{is_extant};
	
	# TRACE FOSSIL REMOVAL
	
	# this is a fast, elegant algorithm for determining simple
	#  inheritance of a value (preservation) from parent to child
	if ( $q->param('traces') !~ /yes/i )	{
		my %istrace;
		for my $i ( 0..$#allsubtaxa )	{
			my $s = $allsubtaxa[$i];
			if ( $s->{'preservation'} eq "trace" )	{
				$istrace{$s->{'taxon_no'}}++;
		# find parents by descending
		# overall parent is innocent until proven guilty
			} elsif ( ! $s->{'preservation'} && $s->{'lft'} >= $focal_taxon->{'lft'} )	{
				my $j = $i-1;
			# first part means "not parent"
				while ( ( $allsubtaxa[$j]->{'rgt'} < $s->{'lft'} || ! $allsubtaxa[$j]->{'preservation'} ) && $j > 0 )	{
					$j--;
				}
				if ( $allsubtaxa[$j]->{'preservation'} eq "trace" )	{
					$istrace{$s->{'taxon_no'}}++;
				}
			}
		}
		my @nontraces;
		for $s ( @subtaxa )	{
			if ( ! $istrace{$s->{'taxon_no'}} )	{
				push @nontraces , $s;
			}
		}
		@subtaxa = @nontraces;
	}

	# COLLECTION SEARCH

	my %options = ();
	if ( $q->param('types_only') =~ /yes/i )	{
		for my $s ( @subtaxa )	{
			if ( $s->{'type_locality'} > 0 ) { $options{'collection_list'} .= ",".$s->{'type_locality'}; }
		}
		$options{'collection_list'} =~ s/^,//;
		push @{$options{'species_reso'}} , "n. sp.";
	}

	# similarly, we could use getCollectionsSet but it would be overkill
	my $fields = ['max_interval_no','min_interval_no','collection_no','collection_name','country','state','geogscale','formation','member','stratscale','lithification','minor_lithology','lithology1','lithification2','minor_lithology2','lithology2','environment'];

	if ( ! $q->param('Africa') || ! $q->param('Antarctica') || ! $q->param('Asia') || ! $q->param('Australia') || ! $q->param('Europe') || ! $q->param('North America') || ! $q->param('South America') )	{
		for my $c ( 'Africa','Antarctica','Asia','Australia','Europe','North America','South America' )	{
			if ( $q->param($c) )	{
				$options{'country'} .= ":".$c;
			}
		}
		$options{'country'} =~ s/^://;
	}

	my (@in_list);
	push @in_list , $_->{'taxon_no'} foreach @subtaxa;

	$options{'permission_type'} = 'read';
	$options{'taxon_list'} = \@in_list;
	$options{'geogscale'} = $q->param('geogscale');
	$options{'stratscale'} = $q->param('stratscale');
	if ( $q->param('minimum_age') > 0 )	{
		$options{'max_interval'} = 999;
		$options{'min_interval'} = $q->param('minimum_age');
	}

	my ($colls) = Collection::getCollections($dbt,$s,\%options,$fields);
	if ( ! @$colls )	{
		my $error_message = "No occurrences of $name match the search criteria";
		beginFirstAppearance($hbo,$q,$error_message);
	}

	my @intervals = getIntervalData($dbt,$colls);
	my %interval_hash;
	$interval_hash{$_->{'interval_no'}} = $_ foreach @intervals;

	if ( $q->param('temporal_precision') )	{
		my @newcolls;
        	for my $coll (@$colls) {
			if ( $interval_hash{$coll->{'max_interval_no'}}->{'base_age'} -  $interval_hash{$coll->{'max_interval_no'}}->{'top_age'} <= $q->param('temporal_precision') )	{
				push @newcolls , $coll;
			}
		}
		@$colls = @newcolls;
	}
	if ( ! @$colls )	{
		my $error_message = "No occurrences of $name have sufficiently precise age data";
		beginFirstAppearance($hbo,$q,$error_message);
	}
	my $ncoll = scalar(@$colls);

	print "<div style=\"text-align: center\"><p class=\"medium pageTitle\">First appearance data for $name $authors</p></div>\n";
	print "<div class=\"small\" style=\"padding-left: 2em; padding-right: 2em;  padding-bottom: 4em;\">\n";
	
	my $other_count = scalar(@other_matches);
	if ( $other_count == 1 )
	{
		print "<p class=\"small\">Warning: a different but smaller taxon in the system has the name $name.</p>";
	} 
	elsif ( $other_count > 1 )
	{
	    
		print "<p class=\"small\">Warning: $other_count smaller taxa in the system have the name $name.</p>";
	}
	
	print "<div class=\"displayPanel\">\n<span class=\"displayPanelHeader\" style=\"font-size: 1.2em;\">Basic data</span>\n<div class=\"displayPanelContents\">\n";

	# CROWN GROUP CALCULATION
	
	if ( $focal_taxon->{is_extant} )
	{
	    my $crown = $taxonomy->getRelatedTaxon('crown_group', $focal_taxon);
	    
	    if ( $crown->{orig_no} == $focal_taxon->{orig_no} )
	    {
		my @extant_children = $taxonomy->getTaxa('children', $focal_taxon, { extant => 1 });
		my $extant_string = join(', ', map { $_->{taxon_name} } @extant_children);
		
		print "<p style=\"padding-left: 1em; text-indent: -1em;\"><i>$name is itself a crown group, being the immediate parent of the following extant taxa: $extant_string</i></p>\n";
	    }
	    
	    elsif ( $crown )
	    {
		my $crown_name = $crown->{taxon_name};
		my $crown_no = $crown->{taxon_no};
		my $paramlist;
		for my $p ( $q->param() )	{
		    if ( $q->param($p) && $p ne "taxon_name" && $p ne "common_name" )	{
			$paramlist .= "&amp;".$p."=".$q->param($p);
		    }
		}
		print "<p>The crown group of $name is <a href=\"?taxon_no=$crown_no$paramlist\">$crown_name</a> (click to compute its first appearance)</p>\n";
	    }
	    
	    else
	    {
		my $exclude = "";
		if ( $q->param('exclude') )
		{
		    $exclude = " (other than the ones you excluded)";
		}
		print "<p><i>$name has no subtaxa marked in our system as extant$exclude, so its crown group cannot be determined</i></p>\n";
	    } 
	}
	
	else
	{
	    print "<p><i>$name is entirely extinct, so it has no crown group</i></p>\n";
	}

	# AGE RANGE/CONFIDENCE INTERVAL CALCULATION

	my ($lb,$ub,$max_no,$minfirst,$min_no) = getAgeRange($dbt,$colls);
	my ($first_interval_top,@firsts,@rages,@ages,@gaps);
	my $TRIALS = int( 10000 / scalar(@$colls) );
        for my $coll (@$colls) {
		my ($collmax,$collmin,$last_name) = ("","","");
		$collmax = $interval_hash{$coll->{'max_interval_no'}}->{'base_age'};
		# IMPORTANT: the collection's max age is truncated at the
		#   taxon's max first appearance
		if ( $collmax > $lb )	{
			$collmax = $lb;
		}
		if ( $coll->{'min_interval_no'} == 0 )	{
			$collmin = $interval_hash{$coll->{'max_interval_no'}}->{'top_age'};
			$last_name = $interval_hash{$coll->{'max_interval_no'}}->{'interval_name'};
		} else	{
			$collmin = $interval_hash{$coll->{'min_interval_no'}}->{'top_age'};
			$last_name = $interval_hash{$coll->{'min_interval_no'}}->{'interval_name'};
		}
		$coll->{'maximum Ma'} = $collmax;
		$coll->{'minimum Ma'} = $collmin;
		$coll->{'midpoint Ma'} = ( $collmax + $collmin ) / 2;
		if ( $minfirst == $collmin )	{
			if ( $coll->{'state'} && $coll->{'country'} eq "United States" )	{
				$coll->{'country'} = "US (".$coll->{'state'}.")";
			}
			$first_interval_top = $last_name;
			push @firsts , $coll;
		}
	# randomization to break ties and account for uncertainty in
	#  age estimates
		for my $t ( 1..$TRIALS )	{
			push @{$rages[$t]} , rand($collmax - $collmin) + $collmin;
		}
	}

	my $first_interval_base = $interval_hash{$max_no}->{interval_name};
	my $last_interval = $interval_hash{$min_no}->{interval_name};
	if ( $first_interval_base =~ /an$/ )	{
		$first_interval_base = "the ".$first_interval_base;
	}
	if ( $first_interval_top =~ /an$/ )	{
		$first_interval_top = "the ".$first_interval_top;
	}
	if ( $last_interval =~ /an$/ )	{
		$last_interval = "the ".$last_interval;
	}

	my $agerange = $lb - $ub;;
	if ( $q->param('minimum_age') > 0 )	{
		$agerange = $lb - $q->param('minimum_age');
	}
	for my $t ( 1..$TRIALS )	{
		@{$rages[$t]} = sort { $b <=> $a } @{$rages[$t]};
	}
	for my $i ( 0..$#{$rages[1]} )	{
		my $x = 0;
		for my $t ( 1..$TRIALS )	{
			$x += $rages[$t][$i];
		}
		push @ages , $x / $TRIALS;
	}
	for my $i ( 0..$#ages-1 )	{
		push @gaps , $ages[$i] - $ages[$i+1];
	}
	# shortest to longest
	@gaps = sort { $a <=> $b } @gaps;

	# AGE RANGE/CI OUTPUT

	if ( $options{'country'} )	{
		my $c = $options{'country'};
		$c =~ s/:/, /g;
		$c =~ s/(, )([A-Za-z ]*)$/ and $2/;
		print "<p>Continents: $c</p>\n";
	}

	printf "<p>Maximum first appearance date: bottom of $first_interval_base (%.1f Ma)</p>\n",$lb;
	printf "<p>Minimum first appearance date: top of $first_interval_top (%.1f Ma)</p>\n",$minfirst;
	if ( $extant eq "no" )	{
		printf "<p>Minimum last appearance date: top of $last_interval (%.1f Ma)</p>\n",$ub;
	}
	print "<p>Total number of collections: $ncoll</p>\n";
	printf "<p>Collections per Myr between %.1f and %.1f Ma: %.2f</p>\n",$lb,$lb - $agerange,$ncoll / $agerange;
	if ( $ncoll > 1 )	{
		printf "<p>Average gap between collections: %.2f Myr</p>\n",$agerange / ( $ncoll - 1 );
		printf "<p>Gap between two oldest collections: %.2f Myr</p>\n",$ages[0] - $ages[1];
	}

	print "</div>\n</div>\n\n";

	# begin more-than-one collection calculations
	if ( $ncoll > 1 )	{
	print "<div class=\"displayPanel\" style=\"margin-top: 2em;\">\n<span class=\"displayPanelHeader\" style=\"font-size: 1.2em;\">Confidence intervals on the first appearance</span>\n<div class=\"displayPanelContents\">\n";

	print "<div style=\"margin-left: 1em;\">\n";

	printf "<p style=\"text-indent: -1em;\">Based on assuming continuous sampling (Strauss and Sadler 1987, 1989): 50%% = %.2f Ma, 90%% = %.2f Ma, 95%% = %.2f Ma, and 99%% = %.2f Ma<br>\n", Strauss($lb, $ncoll, 0.50, 0.90, 0.95, 0.99);
	
	printf "<p style=\"text-indent: -1em;\">Based on percentiles of gap sizes (Marshall 1994): 50%% = %s, 90%% = %s, 95%% = %s, and 99%% = %s<br>\n", percentile($lb, \@gaps, 0.50, 0.90, 0.95, 0.99);

	printf "<p style=\"text-indent: -1em;\">Based on the oldest gap (Solow 2003): 50%% = %.2f Ma, 90%% = %.2f Ma, 95%% = %.2f Ma, and 99%% = %.2f Ma<br>\n", Solow($lb, \@ages, 0.50, 0.90, 0.95, 0.99);

	print "<div id=\"note_link\" class=\"small\" style=\"margin-bottom: 1em; padding-left: 1em;\"><span class=\"mockLink\" onClick=\"document.getElementById('CI_note').style.display='inline'; document.getElementById('note_link').style.display='none';\"> > <i>Important: please read the explanatory notes.</span></i></div>\n";
	print qq|<div id="CI_note" style="display: none;"><div class="small" style="margin-left: 1em; margin-right: 2em; background-color: ghostwhite;">
<p style="margin-top: 0em;">All three confidence interval (CI) methods assume that there are no errors in identification, classification, temporal correlation, or time scale calibration. Our database is founded on published literature that often contains such errors, and we are not always able to correct them although (for example) we standardize taxonomy using synonymy tables. Our sampling of the literature is also variably complete, and it may not include all published early occurrences.</p>
<p>The first CI two methods also assume that distribution of gap sizes does not change through time. The Strauss and Sadler method assumes more specifically that the gaps are randomly placed in time, so they follow a Dirichlet distribution. The percentile-based estimates assume nothing about the underlying distribution. They are computed by rank-ordering the N observed gaps and taking the average of the two gaps that span the percentile matching the appropriate CI (see Marshall 1994). These are gaps k and k + 1 where k < (1 - CI) N < k + 1. So, if there are 100 gaps then the 95% CI matches the 5th longest.</p>
<p style="margin-bottom: 0em;">Intuitively, it might seem that percentiles underestimate the CIs when sample sizes are small. Marshall (1994) therefore proposed generating CIs on top of the nonparametric CIs. The CIs on CIs express the chance that 1 to k gaps are longer than 1 - CI' of all possible gaps, CI and CI' potentially being different (say, 50% and 5%). However, possible gaps are not of interest: one wants to know about a single real gap in the fossil record. The chance that 1 to k gaps are longer than this record is just k/N, the original CI. Therefore, CIs based on Marshall's method are not reported here.</p>
<p>Solow's method has a computational problem: the size of the oldest gap cannot be computed when the oldest occurrences have the same range of age estimates (because they fall in the same geological time interval). To break ties, the point age estimate of each collection is randomized repeatedly within its age range to produce an average estimate of the oldest gap's size. The same randomization procedure is applied to all age estimates prior to computing the above-mentioned percentiles. The raw and randomized values are both reported in the download file.
</p>
</div></div></div>
|;

	# TIME VS. GAP SIZE TEST

	# convert to ranks by manipulating an array of objects
	my @gapdata;
	for my $i ( 0..$#ages-1 )	{
		$gapdata[$i]->{'age'} = $ages[$i];
		$gapdata[$i]->{'gap'} = $ages[$i] - $ages[$i+1];
	}
	@gapdata = sort { $b->{'age'} <=> $a->{'age'} } @gapdata;
	for my $i ( 0..$#ages-1 )	{
		$gapdata[$i]->{'agerank'} = $i;
	}
	@gapdata = sort { $b->{'gap'} <=> $a->{'gap'} } @gapdata;
	for my $i ( 0..$#ages-1 )	{
		$gapdata[$i]->{'gaprank'} = $i;
	}

	my ($n,$mx,$my,$sx,$sy,$cov);
	$n = $#ages;
	if ( $n > 9 )	{
		for my $i ( 0..$#ages-1 )	{
			$mx += $gapdata[$i]->{'agerank'};
			$my += $gapdata[$i]->{'gaprank'};
		}
		$mx /= $n;
		$my /= $n;
		for my $i ( 0..$#ages-1 )	{
			$sx += ($gapdata[$i]->{'agerank'} - $mx)**2;
			$sy += ($gapdata[$i]->{'gaprank'} - $my)**2;
			$cov += ($gapdata[$i]->{'agerank'} - $mx) * ( $gapdata[$i]->{'gaprank'} - $my);
		}
		$sx = sqrt( $sx / ( $n - 1 ) );
		$sy = sqrt( $sy / ( $n - 1 ) );
		my $r = $cov / ( ( $n - 1 ) * $sx * $sy );
		my $t = $r / sqrt( ( 1 - $r**2 ) / ( $n - 2 ) );
	# for n > 9, the p < 0.001 critical values range from 3.291 to 4.587
		my ($direction,$size) = ("positive","small");
		if ( $r < 0 )	{
			$direction = "negative";
			$size = "large";
		}
		if ( $t > 3.291 )	{
			printf "<p style=\"padding-left: 1em; text-indent: -1em;\">WARNING: there is a very significant $direction rank-order correlation of %.3f between time in Myr and gap size, so the continuous sampling- and percentile-based confidence interval estimates are far too small (try setting a higher minimum age)</p>\n",$r;
	# and the p < 0.01 values range from 2.576 to 3.169
		} elsif ( $t > 2.576 )	{
			printf "<p style=\"padding-left: 1em; text-indent: -1em;\">WARNING: there is a significant $direction rank-order correlation of %.3f between time in Myr and gap size, so the continuous sampling- and percentile-based confidence interval estimates are too small (try setting a higher minimum age)</p>\n",$r;
	# and the p < 0.05 values range from 1.960 to 2.228
		} elsif ( $t > 1.960 )	{
			printf "<p style=\"padding-left: 1em; text-indent: -1em;\">WARNING: there is a $direction rank-order correlation of %.3f between time in Myr and gap size, so the continuous sampling- and percentile-based confidence interval estimates are probably too small (try setting a higher minimum age)</p>\n",$r;
		}
	}

	print "</div>\n</div>\n\n";
	
	} # end more-than-one collection calculations

	# COLLECTION DATA OUTPUT

	print "<div class=\"displayPanel\" style=\"margin-top: 2em;\">\n<span class=\"displayPanelHeader\" style=\"font-size: 1.2em;\">First occurrence details</span>\n<div class=\"displayPanelContents\">\n";

	# getCollections won't return multiple occurrences per collection, so...
	my @collnos;
	push @collnos , $_->{'collection_no'} foreach @$colls;
	my $reso;
	# not returning occurrences means that getCollections can't apply this
	#  filter consistently
	if ( $q->param('types_only') =~ /yes/i )	{
		$reso = " AND species_reso='n. sp.'";
	}
	
	my $temp_table = $taxonomy->getTaxa('all_children', $focal_taxon, { return => 'id_table',
									    fields => 'orig,senior' });
	
	my @occs;
	
	if ( $temp_table )
	{
	    my $coll_list = join(',', @collnos);
	    
	    my $sql = "
	        SELECT r.taxon_no, a.taxon_name, a.taxon_rank, sa.taxon_name as senior_name,
		       r.collection_no, r.occurrence_no, r.reid_no
		FROM reidentifications as r JOIN authorities as a on a.taxon_no = r.taxon_no
			JOIN $temp_table as t on t.orig_no = a.orig_no
			JOIN authorities as sa on sa.taxon_no = t.senior_no
		WHERE r.collection_no in ($coll_list) and most_recent='YES' $reso
		GROUP BY r.collection_no, r.taxon_no
		UNION
		SELECT o.taxon_no, a.taxon_name, a.taxon_rank, o.collection_no, o.occurrence_no, 0
		FROM occurrences as o JOIN authorities as a on a.taxon_no = o.taxon_no
			JOIN $temp_table as t on t.orig_no = a.orig_no
			JOIN authorities as sa on sa.taxon_no = t.senior_no
		WHERE o.collection_no in ($coll_list) and most_recent='YES' $reso
		GROUP BY o.collection_no, o.taxon_no";
	    
	    @occs = @{$dbt->getData($sql)};
	}
	
	# print data to output file JA 17.1.09
	my $name = ($s->get("enterer")) ? $s->get("enterer") : "Guest";
	my $filename = PBDBUtil::getFilename($name) . "-appearances.txt";;
	print "<p><a href=\"/public/downloads/$filename\">Download the full data set</a></p>\n\n";
	open OUT , ">$HTML_DIR/public/downloads/$filename";
	@$colls = sort { $b->{'midpoint Ma'} <=> $a->{'midpoint Ma'} || $b->{'maximum Ma'} <=> $a->{'maximum Ma'} || $b->{'minimum Ma'} <=> $a->{'minimum Ma'} || $a->{'country'} cmp $b->{'country'} || $a->{'state'} cmp $b->{'state'} || $a->{'formation'} cmp $b->{'formation'} || $a->{'collection_name'} cmp $b->{'collection_name'} } @$colls;
	splice @$fields , 0 , 2 , ('maximum Ma','minimum Ma','midpoint Ma','randomized Ma','randomized gap','taxa');
	print OUT join("\t",@$fields),"\n";

	my %ids;
	$ids{$_->{'occurrence_no'}}++ foreach @occs;

	my %includes;
	for $_ ( @occs )	{
		if ( $ids{$_->{'occurrence_no'}} == 1 || $_->{'reid_no'} > 0 )	{	
			push @{$includes{$_->{'collection_no'}}} , $_->{senior_name};
		}
	}
	
	for my $i ( 0..scalar(@$colls)-1 )	{
		my $coll = $$colls[$i];
		$coll->{'randomized Ma'} = $ages[$i];
		if ( $i < scalar(@$colls) - 1 )	{
			$coll->{'randomized gap'} = $ages[$i] - $ages[$i+1];
		# this transform should standardized the gap sizes if indeed
		#  the sampling probability falls exponentially through time
		#  (which it generally does not do cleanly)
			#$coll->{'randomized gap'} = log((1/$coll->{'randomized gap'})*$ages[$i]);
		} else	{
			$coll->{'randomized gap'} = "NA";
		}
		my %seen;
		$seen{$_}++ foreach @{$includes{$coll->{'collection_no'}}};
		$coll->{'taxa'} = join(', ',keys %seen);
		$coll->{'taxa'} =~ s/  / /g;
		$coll->{'taxa'} =~ s/  / /g;
		$coll->{'taxa'} =~ s/^ //g;
		$coll->{'taxa'} =~ s/ $//g;
		$coll->{'taxa'} =~ s/ ,/,/g;
		for my $f ( @$fields )	{
			my $val = $coll->{$f};
			if ( $coll->{$f} =~ / / )	{
				$val = '"'.$val.'"';
			}
			if ( $f =~ /randomized/ && $coll->{$f} ne "NA" )	{
				printf OUT "%.3f",$coll->{$f};
			} elsif ( $coll->{$f} =~ /^[0-9]+(\.|)[0-9]*$/ && $f !~ /_no$/ )	{
				printf OUT "%.2f",$coll->{$f};
			} else	{
				print OUT "$val";
			}
			if ( $$fields[scalar(@$fields)-1] eq $f )	{
				print OUT "\n";
			} else	{
				print OUT "\t";
			}
		}
	}

	for my $o ( @occs )	{
		if ( $o->{'taxon_rank'} =~ /genus|species/ )	{
			$o->{'taxon_name'} = "<i>".$o->{'taxon_name'}."</i>";
		}
	}

	if ( $#firsts == 0 )	{
		if ( $firsts[0]->{'formation'} )	{
			$firsts[0]->{'formation'} .= " Formation ";
		}
		my $agerange = $interval_hash{$firsts[0]->{'max_interval_no'}}->{'interval_name'};
		if ( $firsts[0]->{'min_interval_no'} > 0 )	{
			$agerange .= " - ".$interval_hash{$firsts[0]->{'min_interval_no'}}->{'interval_name'};
		}
		my @includes;
		for my $o ( @occs )	{
			if ( $o->{'collection_no'} == $firsts[0]->{'collection_no'} && ( $ids{$o->{'occurrence_no'}} == 1 || $o->{'reid_no'} > 0 ) )	{
				push @includes , $o->{'taxon_name'};
			}
		}
		print "<p style=\"padding-left: 1em; text-indent: -1em;\">The collection documenting the first appearance is <a href=\"?a=basicCollectionSearch&amp;collection_no=$firsts[0]->{'collection_no'}\">$firsts[0]->{'collection_name'}</a> ($agerange $firsts[0]->{'formation'} of $firsts[0]->{'country'}: includes ".join(', ',@includes).")</p>\n";
	} else	{
		@firsts = sort { $a->{'collection_name'} cmp $b->{'collection_name'} } @firsts;
		print "<p class=\"large\" style=\"margin-bottom: -1em;\">Collections including first appearances</p>\n";
		print "<table cellpadding=\"0\" cellspacing=\"0\" style=\"padding: 1.5em;\">\n";
		my @fields = ('collection_no','collection_name','country','formation');
		print "<tr valign=\"top\">\n";
		for my $f ( @fields )	{
			my $fn = $f;
			$fn =~ s/^[a-z]/\U$&/;
			$fn =~  s/_/ /g;
			$fn =~ s/ no$//;
			print "<td><div style=\"padding: 0.5em;\">$fn</div></td>\n";
		}
		print "<td style=\"padding: 0.5em;\">Age (Ma)</td>\n";
		print "</tr>\n";
		my $i;
		for my $coll ( @firsts )	{
			$i++;
			my $classes = (($#firsts > 1) && ($i/2 > int($i/2))) ? qq|"small darkList"| : qq|"small"|;
			print "<tr valign=\"top\" class=$classes style=\"padding: 3.5em;\">\n";
			my $collno = $coll->{'collection_no'};
			$coll->{'collection_no'} = "&nbsp;&nbsp;<a href=\"?a=basicCollectionSearch&amp;collection_no=$coll->{'collection_no'}\">".$coll->{'collection_no'}."</a>";
			if ( $coll->{'state'} && $coll->{'country'} eq "United States" )	{
				$coll->{'country'} = "US (".$coll->{'state'}.")";
			}
			if ( ! $coll->{'formation'} )	{
				$coll->{'formation'} = "-";
			}
			for my $f ( @fields )	{
				print "<td style=\"padding: 0.5em;\">$coll->{$f}</td>\n";
			}
			printf "<td style=\"padding: 0.5em;\">%.1f to %.1f</td>\n",$interval_hash{$coll->{'max_interval_no'}}->{'base_age'},$interval_hash{$coll->{'max_interval_no'}}->{'top_age'};
			print "</tr>\n";
			my @includes = ();
			for my $o ( @occs )	{
				if ( $o->{'collection_no'} == $collno && ( $ids{$o->{'occurrence_no'}} == 1 || $o->{'reid_no'} > 0 ) )	{
					push @includes , $o->{'taxon_name'};
				}
			}
			print "<tr valign=\"top\" class=$classes><td></td><td style=\"padding-bottom: 0.5em;\" colspan=\"6\">includes ".join(', ',@includes)."</td></tr>\n";
		}
		print "</table>\n\n";
	}
	print "</div>\n</div>\n";
	print "<div style=\"padding-left: 6em;\"><a href=\"?a=beginFirstAppearance\">Search again</a> - <a href=\"?a=displayTaxonInfoResults&amp;taxon_no=$focal_taxon->{'taxon_no'}\">See more details about $name</a></div>\n";
	print "</div>\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);
	return;
}


# CI COMPUTATIONS

sub percentile {
    my ($lb, $gaps, @c) = @_;
    
    my @result;
    
    foreach my $c (@c)
    {
	my $i = int($c * ( $#$gaps + 1 ) );
	my $j = $i + 1;
	if ( $i == $c * ( $#$gaps + 1 ) )	{
	    $j = $i;
	}
	if ( $j > $#$gaps )	{
	    push @result, "NA";
	}
	push @result, sprintf "%.2f Ma",( $lb + ( $gaps->[$i] + $gaps->[$j] ) / 2 );
    }
    
    return @result;
}

sub Strauss {
    my ($lb, $ncoll, @c) = @_;
    
    return map { $lb * ( ( 1 - $_ )**( -1 /( $ncoll - 1 ) ) ) } @c;
}

sub Solow {
    my ($lb, $ages, @c) = @_;
    
    return map { $lb + ( $_ / ( 1 - $_ ) ) * ( $ages->[0] - $ages->[1] ) } @c;
}

# Bayesian CIs can computed using an equation related to Marshall's, but it
# yields CIs that are identical to percentiles converted from ranks.  Let t =
# the true gap in Myr between the oldest fossil and the actual first
# appearance, X = the overall distribution of possible gaps, Y = the observed
# gap size distribution, N = the number of observed gaps, and s = a possible
# percentile score of t within X.
# 
# We will evaluate only N equally spaced values of s between 0 and 100%.
# We want the posterior probability that t is between the kth and k+1th values out of N for each value of k.
# We will sum the probabilities to find the 50, 90, 95, and 99% CIs.
# 
# For each k we find the conditional probability Pk,i that exactly k out of N
# observations will be greater than t given that t's percentile score is
# closest to the ith s value, which is simply a binomial probability (see
# Marshall 1994).  Instead of summing across possible values of k, which would
# yield 1 for each value of i, we sum the conditionals across values of i
# (which by definition have equal priors) to produce a total tail probability
# for each k.
# 
# The grand total across all values of k is just N.
# The posterior probability of each k is therefore its sum divided by N.
# This method yields equal posteriors for all possible ranks, justifying
# transformation of ranks into percentiles.

sub BayesCI	{
    my $n = shift;
    
    my %fact = ();
    for my $i ( 1..$n )	{
	$fact{$i} = $fact{$i-1} + log( $i );
    }
    
    for my $k ( 0..int($n/1) )	{
	my $conditional = 0;
	for my $i ( 1..$n+1 )	{
	    my $p = ( $i - 0.5 ) / ( $n + 1 );
	    my $kp = $k * log( $p ) + ( $n - $k ) * log( 1 - $p );
	    $kp += $fact{$n};
	    $kp -= $fact{$k};
	    $kp -= $fact{$n - $k};
	    $conditional += exp( $kp );
	}
	my $posterior = $conditional / ( $n + 1 );
	#printf "$k %.3f<br>",$posterior;
    }
}


# JA 3-5.11.09
sub basicTaxonSearch {

    my ($q, $s, $dbt, $taxonomy, $hbo) = @_;    #$$$$

    my ($is_real_user,$not_bot) = (1,1);
    if (! $q->request_method() eq 'POST' && ! $q->param('is_real_user') && ! $s->isDBMember()) {
	$is_real_user = 0;
	$not_bot = 0;
    }
    if (PBDBUtil::checkForBot()) {
	$is_real_user = 0;
	$not_bot = 0;
    }
    if ( $is_real_user > 0 )	{
	main::logRequest($s,$q);
    }
    
    # Short cut if we were given a specific taxon number.
    
    my $exclusion;
    my $match = $q->param('match');
    my $taxon_no = $q->param('taxon_no');
    
    if ( $taxon_no > 0 and not $match eq 'all' and not $match eq 'random' )
    {
	my $focal_taxon = $taxonomy->getRelatedTaxon('senior', $taxon_no);
	displayBasicTaxonInfo($dbt, $taxonomy, $s, $hbo, $is_real_user, $focal_taxon);
	return;
    }
    
    # Otherwise, try to figure out which taxon is wanted.
    
    my $taxon_name = $q->param('taxon_name') || $q->param('quick_search' ) ||
	$q->param('search_again');
    my $common_name = $q->param('common_name');
    
    $taxon_name =~ s/ spp?\.$//;
    $taxon_name =~ s/\./%/;
    
    # Determine the other specified criteria.
    
    my %search_options;
    
    if ( $q->param('taxon_rank') )
    {
	$search_options{rank} = $q->param('taxon_rank');
    }
    
    my $validity = $q->param('validity');
    
    if ( $validity )
    {
	$search_options{status} = 'valid' if $validity eq 'valid';
	$search_options{status} = 'all' if $validity eq 'valid or invalid';
	$search_options{status} = 'invalid' if $validity eq 'invalid';
    }
    
    my $author = $q->param('author');
    
    if ( $author )
    {
	$author =~ s/[^A-Za-z '-]//g;
	$author =~ s/'/\\'/g;
	$search_options{author} = $author;
    }
    
    my $pubyr = $q->param('pubyr');
    
    if ( $pubyr )
    {
	$pubyr =~ s/[^0-9]//g;
	$search_options{pubyr} = $pubyr;
    }
    
    my $type_part = $q->param('type_body_part');
    
    if ( $type_part )
    {
	$search_options{type_body_part} = $type_part;
    }
    
    my $preservation = $q->param('preservation');
    
    if ( $preservation )
    {
	$search_options{preservation} = $preservation;
    }
    
    my $exclude = $q->param('exclude_taxon');
    
    if ( $exclude )
    {
	my @taxon_nos = $taxonomy->getTaxaByName($exclude);
	$exclusion = $taxonomy->generateExclusion(\@taxon_nos);
    }
    
    # Now find the requested taxon or taxa.  If we were asked for a random
    # subtaxon, or for all subtaxa, then just take the first match on the name.
    
    if ( $match eq 'random' or $match eq 'all' )
    {
	my $focal_taxon;
	
	# If a taxon number was specified, use that.
	# First try the taxon name, if specified
	
	if ( $taxon_no > 0 )
	{
	    $focal_taxon = $taxonomy->getTaxon($taxon_no);
	}
	
	# Otherwise, if a name was specified, search for that.  Use the
	# biggest one if there is more than one match.
	
	elsif ( $taxon_name )
	{
	    ($focal_taxon) = $taxonomy->getTaxaByName($taxon_name, { order => 'size.desc' });
	}
	
	# Otherwise, search for the common name if specified.  Again, use the
	# biggest one if there is more than one match.
	
	elsif ( $common_name )
	{
	    ($focal_taxon) = $taxonomy->getTaxaByName($common_name, { common => 1, order => 'size.desc' });
	}
	
	# If we haven't found anything, redo the query form.
	
	unless ( $focal_taxon )
	{
	    searchForm($hbo, $q, "No results found");
	}
	
	# Otherwise, find all of the subtaxa.  Use the specified search criteria.
	
	my @taxon_nos = $taxonomy->getTaxa('all_children', $taxon_no, 
					   { %search_options, exclude => $exclusion,
					     limit => 501,
					     return => 'id' });
	
	# If a random subtaxon was requested, choose one.
	
	if ( $match eq 'random' )
	{
	    my $choose = int(rand($#taxon_nos + 1));
	    
	    displayBasicTaxonInfo($dbt, $taxonomy, $s, $hbo, $is_real_user, $taxon_nos[$choose]);
	}
	
	# If we got more than 500 results, tell the user to search again.
	
	elsif ( @taxon_nos > 500 )
	{
	    searchForm($hbo, $q, "Too many results found");
	}
	
	# Otherwise, show all of the results.
	
	else
	{
	    listTaxonChoices($taxonomy, \@taxon_nos, $focal_taxon->{family});
	}
	
	return;
    }
    
    # Otherwise, try to match the name if specified.  Filter by any search
    # options that were specified (see above).  If we can't find any taxon id
    # numbers, look for occurrences instead.
    
    my @taxon_nos;
    
    if ( $taxon_name )
    {
	@taxon_nos = $taxonomy->getTaxaByName($taxon_name, 
						 { %search_options, 
						   limit => 501,
						   return => 'id' });
	
	unless (@taxon_nos)
	{
	    my ($g,$s) = split / /,$taxon_name;
	    my $name_clause = "genus_name='".$g."'";
	    my $name_clause = "(genus_name='".$g."' OR subgenus_name='".$g."')";
	    if ( $s )	{
		$name_clause .= " AND species_name='".$s."'";
	    }
	    my $sql = "SELECT count(*) c FROM occurrences WHERE $name_clause";
	    my $occ = ${$dbt->getData($sql)}[0];
	    if ( ! $occ )
	    {
		$sql = "SELECT count(*) c FROM reidentifications WHERE $name_clause";
		$occ = ${$dbt->getData($sql)}[0];
	    }
	    
	    if ( $occ )
	    {
		displayBasicTaxonInfo($dbt, $taxonomy, $s, $hbo, $is_real_user, undef, $taxon_name);
		return;
	    }
	}
    }
    
    # Otherwise, if the common name was specified, try that.
    
    elsif ( $common_name )
    {
	@taxon_nos = $taxonomy->getTaxaByName($common_name,
					      { %search_options, 
						limit => 501,
						common => 1, 
						return => 'id' });
    }
    
    # Otherwise, look for all taxa which satisfy the search criteria.
    
    else
    {
	@taxon_nos = $taxonomy->getTaxa('all_taxa', undef, 
					{ %search_options,
					  limit => 501,
					  return => 'id' });
    }
    
    # If got zero results, let the user search again.
    
    if ( @taxon_nos == 0 )
    {
	searchForm($hbo, $q, "No results found");
	return;
    }
    
    # If we got one result, show it.
    
    elsif ( @taxon_nos == 1 )
    {
	displayBasicTaxonInfo($dbt, $taxonomy, $s, $hbo, $is_real_user, $taxon_nos[0]);
	return;
    }
    
    # If we got 500 or fewer, show a list.
    
    elsif ( @taxon_nos <= 500 )
    {
	listTaxonChoices($taxonomy, \@taxon_nos);
    }
    
    # Otherwise, we got too many results so let the user search again.
    
    else
    {
	searchForm($hbo, $q, "Too many results found");
    }
}
 

# $$$   

# moved over from bridge.pl JA 8.4.12
# originally called randomTaxonInfo and then hijacked to also get all names in
#  a group if those are requested instead
# originally wrote this to only recover names tied to an occurrence; revised to
#   get all names in the group, period JA 20.3.11
sub getMatchingSubtaxa	{

    my ($dbt, $q, $s, $hbo, $taxonomy) = @_;
    
    my $dbh = $dbt->dbh;
    my $sql;
    my $lft;
    my $rgt;	return if PBDBUtil::checkForBot();
    
    return if PBDBUtil::checkForBot();
    
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
	my @trefs;
	if ( $lft > 0 && $rgt > 0 )	{
		# default is valid names only as currently spelled
		my $tables = "authorities a,$TAXA_TREE_CACHE t";
		my $join = "a.taxon_no=t.taxon_no AND t.taxon_no=synonym_no";
		if ( $q->param('author') =~ /^[A-Za-z]/ || $q->param('pubyr') > 1700 )	{
			$tables .= ",refs r";
			$join .= " AND a.reference_no=r.reference_no";
		}
		# invalid only
		if ( $q->param('taxon_rank') =~ /[a-z]/ && $q->param('validity') =~ /^invalid$/i )	{
				$join = "a.taxon_no=t.taxon_no AND t.taxon_no=spelling_no AND t.taxon_no!=synonym_no";
		# either one
		} elsif ( $q->param('taxon_rank') =~ /[a-z]/ && $q->param('validity') =~ /invalid/i )	{
				$join = "a.taxon_no=t.taxon_no AND t.taxon_no=spelling_no";
		}
		my $morewhere;
		if ( $q->param('author') =~ /^[A-Za-z]/ )	{
			my $author = $q->param('author');
			$author =~ s/[^A-Za-z '\-]//g;
			$author =~ s/'/\\'/g;
			$morewhere .= " AND ((ref_is_authority='yes' AND (r.author1last='$author' OR r.author2last='$author')) OR (ref_is_authority!='yes' AND (a.author1last='$author' OR a.author2last='$author')))";
		}
		if ( $q->param('pubyr') > 1700 )	{
			my $pubyr = $q->param('pubyr');
			$pubyr =~ s/[^0-9]//g;
			$morewhere .= " AND ((ref_is_authority='yes' AND r.pubyr=$pubyr) OR (ref_is_authority!='yes' AND a.pubyr=$pubyr))";
		}
		if ( $q->param('taxon_rank') )	{
			$morewhere .= " AND taxon_rank='".$q->param('taxon_rank')."'";
		} else	{
			$morewhere .= " AND taxon_rank='species'";
		}
		if ( $q->param('type_body_part') )	{
			$morewhere = " AND type_body_part='".$q->param('type_body_part')."'";
		}
		if ( $q->param('preservation') )	{
			$morewhere .= " AND preservation='".$q->param('preservation')."'";
		}
		if ( $q->param('exclude_taxon') )	{
			$sql = "SELECT lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name=".$dbh->quote($q->param('exclude_taxon'))." ORDER BY rgt-lft DESC";
			my $exclude = ${$dbt->getData($sql)}[0];
			$morewhere .= " AND (lft<".$exclude->{lft}." OR rgt>".$exclude->{rgt}.")";
		}
		$sql = "SELECT a.taxon_no FROM $tables WHERE $join AND (lft BETWEEN $lft AND $rgt) AND (rgt BETWEEN $lft AND $rgt) $morewhere";
		@trefs = @{$dbt->getData($sql)};
	}
	if ( $q->param('match') eq "all" )	{
		my @taxa;
		push @taxa , $_->{taxon_no} foreach @trefs;
		return \@taxa;
	}
	# otherwise select a taxon at random
	else	{
		@trefs = @{$dbt->getData("SELECT a.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND t.taxon_no=synonym_no AND taxon_rank IN ('species')")};
		my $x = int(rand($#trefs + 1));
		$q->param('taxon_no' => $trefs[$x]->{taxon_no});
		# infinite loops are bad
		$q->param('match' => '');
		basicTaxonInfo($q,$s,$dbt,$hbo);
		exit;
	}
}


# $$$ must print header and footer, also take family name if provided

# calved off from checkTaxonInfo JA 8.4.12
sub listTaxonChoices	{

	my ($dbt,$resultsRef) = @_;
	my @results = @{$resultsRef};
	@results = sort { $a->{taxon_name} cmp $b->{taxon_name} } @results;
	print "<div align=\"center\"><p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">Please select a taxonomic name</p>\n";
	if ( scalar @results >= 10 )	{
		print "<p class=\"small\">The total number of matches is ".scalar @results."</p>\n";
	} else	{
		print "<br>\n";
	}
	print qq|<div class="displayPanel" align="center" style="width: 36em; padding-top: 1.5em;">
<div class="displayPanelContent">
<table>
<tr>
|;

	my $classes = qq|"medium"|;
	for my $i ( 0..$#results )	{
		my $authorityLine = Taxon::formatTaxon($dbt,$results[$i]);
		if ($#results > 2)	{
			$classes = ($i/2 == int($i/2)) ? qq|"small darkList"| : "small";
		}
		# the width term games browsers
		print qq|<td class=$classes style="width: 1em; padding: 0.25em; padding-left: 1em; padding-right: 1em; white-space: nowrap;">&bull; <a href="?a=basicTaxonInfo&amp;taxon_no=$results[$i]->{taxon_no}" style="color: black;">$authorityLine</a></td>|;
		print "</tr>\n<tr>";
	}
	print qq|</tr>
<tr><td align="center" colspan=3><br>
</td></tr></table></div>
</div>
</div>
|;

}

sub displayBasicTaxonInfo {

    my ($dbt, $taxonomy, $s, $hbo, $is_real_user, $taxon_no, $taxon_name, $warning) = @_;
    
    # First get the necessary info, if available.
    
    my $dbh = $taxonomy->{dbh};
    my $auth_table = $taxonomy->{auth_table};
    
    my ($focal_taxon, @parent_taxa, $taxon_rank, $common_name, $cof);
    my ($typeInfo, $typeLocality);
    
    my $indent = 'style="padding-left: 1em; text-indent: -1em;"';
    
    # We might have been passed a taxon_no, or maybe only a taxon_name.
    
    if ( $taxon_no )
    {
	$focal_taxon = $taxonomy->getRelatedTaxon('senior', $taxon_no, 
					      { fields => ['link', 'oldattr', 'ref', 'specimen',
							   'discussion', 'tt', 'lft'] });
	
	@parent_taxa = $taxonomy->getTaxa('all_parents', $taxon_no, 
				      { exclude_self => 1, seniors => 1 });
	
	$taxon_name = $focal_taxon->{taxon_name};
	$taxon_rank = $focal_taxon->{taxon_rank};
	$common_name = $focal_taxon->{common_name};
	$taxon_no = $focal_taxon->{taxon_no};
	
	unless ( $common_name )
	{
	    foreach my $t (@parent_taxa)
	    {
		if ( $t->{common_name} )
		{
		    $common_name = $t->{common_name};
		    last;
		}
	    }
	}
    }
    
    my $page_title = { title => "Paleobiology Database: $taxon_name" };
    
    print $hbo->stdIncludes($PAGE_TOP, $page_title);
    
    my $header = $taxon_name;
    
    if ( $focal_taxon )
    {
	if ( $taxon_rank =~ /genus|species/ )
	{
	    $header = "<i>$taxon_name</i> ";
	}
	
	elsif ( $taxon_rank eq 'Unranked clade' )
	{
	    $header = "Clade $taxon_name";
	}
	
	else
	{
	    $header = "$taxon_rank $taxon_name";
	}
	
	$header = "&dagger;$header" if $focal_taxon->{extant} =~ /yes/i;
	
	my $author = formatShortAuthor($focal_taxon);
	
	if ( $focal_taxon->{'ref_is_authority'} =~ /y/i )
	{
	    $author = "<a href=\"?a=displayReference&amp;reference_no=$focal_taxon->{'reference_no'}&amp;is_real_user=$is_real_user\">".$author."</a>";
	}
	
	$header .= $author;
	
	$header .= " ($common_name)" if $common_name;
    }
    
    print qq|<div align="center" class="medium" style="margin-left: 1em; margin-top: 3em;">
<div class="displayPanel" style="margin-top: -1em; margin-bottom: 2em; text-align: left; width: 54em;">
<span class="displayPanelHeader">$header</span>
<div align="left" class="small displayPanelContent" style="padding-left: 1em; padding-bottom: 1em;">
|;
    
    # CLASS/ORDER/FAMILY SECTION
    
    if ( $focal_taxon )
    {
	my ($class, $order, $family) = $taxonomy->getClassOrderFamily($focal_taxon, \@parent_taxa);
	
	my @parent_links;
	
	push @parent_links, "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$focal_taxon>{class_no}\">$class</a>"
	    if $class;
	push @parent_links, "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$focal_taxon>{order_no}\">$order</a>"
	    if $order;
	push @parent_links, "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$focal_taxon>{family_no}\">$family</a>"
	    if $family;
	
	if ( @parent_links )	{
	    print "<p class=\"small\" style=\"margin-top: -0.25em; margin-bottom: 0.75em; margin-left: 1em;\">".join(' - ',@parent_links)."</p>\n\n";
	}
    }
    
    if ( $warning )
    {
	print "<p class=\"medium\"><i>$warning</i></p>\n\n";
    }
    
    # VERBAL DISCUSSION
    # JA 5.9.11

    if ( $focal_taxon->{discussion} )
    {
	my $discussion = $focal_taxon->{discussion};
	$discussion =~ s/(\[\[)([A-Za-z ]+|)(taxon )([0-9]+)(\|)/<a href="?a=basicTaxonInfo&amp;taxon_no=$4">/g;
	$discussion =~ s/(\[\[)([A-Za-z0-9\'\. ]+|)(ref )([0-9]+)(\|)/<a href="?a=displayReference&amp;reference_no=$4">/g;
	$discussion =~ s/(\[\[)([A-Za-z0-9\'"\.\-\(\) ]+|)(coll )([0-9]+)(\|)/<a href="?a=basicCollectionSearch&amp;collection_no=$4">/g;
	$discussion =~ s/\]\]/<\/a>/g;
	$discussion =~ s/\n\n/<\/p>\n<p>/g;
	$focal_taxon->{'email'} =~ s/\@/\' \+ \'\@\' \+ \'/;
	print qq|<p style="margin-bottom: -0.5em; font-size: 1.0em;">$discussion</p>
|;
	if ( $focal_taxon->{discussant} ne "" )
	{
			print qq|<script language="JavaScript" type="text/javascript">
    <!-- Begin
    window.onload = showMailto;
    function showMailto( )	{
        document.getElementById('mailto').innerHTML = '<a href="' + 'mailto:' + '$focal_taxon->{email}?subject=$taxon_name">$focal_taxon->{discussant}</a>';
    }
    // End -->
</script>

<p class="verysmall">Send comments to <span id="mailto"></span><p>
|;
	}
    }
    
    # IMAGE AND SYNONYM SECTIONS
    
    my (@junior_spellings, @junior_synonyms, @alt_spellings, @all_spellings);
    
    if ( $focal_taxon )
    {
	my (@all_spellings) = $taxonomy->getTaxa('synonyms', $focal_taxon, 
					     { fields => ['link', 'oldattr', 'ref', 'tt', 'specimen'], 
					       spelling => 'all', order => 'name' });
	
	foreach my $t ( @all_spellings )
	{
	    push @alt_spellings, $t if $t->{taxon_no} != $t->{spelling_no} 
		and $t->{orig_no} == $focal_taxon->{orig_no}
		    and $t->{taxon_name} != $taxon_name;
	    push @junior_spellings, $t if $t->{orig_no} != $focal_taxon->{orig_no}
		and $t->{taxon_name} != $taxon_name;
	    push @junior_synonyms, $t if $t->{orig_no} != $focal_taxon->{orig_no}
		and $t->{taxon_no} == $t->{spelling_no};
	}
	
	my $thumbs = $dbh->selectall_arrayref("
		SELECT i.image_no, i.caption, i.path_to_image, i.width, i.height
		FROM images as i JOIN $auth_table as a using (taxon_no)
		WHERE a.taxon_no = $focal_taxon->{orig_no}
		LIMIT 12", { Slice => {} });
	
	if ( ref $thumbs eq 'ARRAY' )
	{
	    for my $thumb ( @$thumbs )
	    {
		my $aspect = $thumb->{'height'} / $thumb->{'width'};
		my $divwidth =  int( 500 / ( $#$thumbs + 2 * $aspect**2 ) );
		if ( $divwidth > $thumb->{'width'} )	{
		    $divwidth = $thumb->{'width'};
		}
		if ( $divwidth > 200 )	{
		    $divwidth = 200;
		}
		my $blowup = 300 / sqrt( $thumb->{'width'} * $thumb->{'height'} );
		my ($height,$width) = ( int( $blowup*$thumb->{'height'} ) , int( $blowup*$thumb->{'width'} ) );
		my $thumb_path = $thumb->{path_to_image};
		$thumb_path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
		print '<div style="float: left; clear: none; margin-right: 10px;">';
		printf "<a href=\"javascript: imagePopup('?a=displayImage&amp;image_no=$thumb->{image_no}&amp;maxheight=%d&amp;maxwidth=%d&amp;display_header=NO',%d,%d)\">",$height,$width,$width + 80,$height + 150;
		print "<img src=\"$thumb_path\" border=1 vspace=3 width=$divwidth alt=\"$thumb->{caption}\">";
		print "</a></div>\n\n";
	    }
	    print "<div style=\"clear: both;\"></div>\n\n";
	}
	
	my $noun = $focal_taxon->{taxon_rank} =~ /species/ ? "combination" : "spelling";
	
	if ( @alt_spellings )
	{
	    my $word = (@alt_spellings > 1) ? $noun : "${noun}s";
	    my $list = join(', ', map { italicize($_) } @alt_spellings);
	    
	    print "<p>Alternative $word: $list</p>\n\n";
	}
	
	my (@syns, @invalids);
	
	for my $s ( @junior_synonyms )
	{
	    if ( $s->{status} eq 'objective synonym of' )
	    {
		$s->{note} = ' [objective synonym]';
		push @syns, $s;
	    }
	    
	    elsif ( $s->{status} eq 'subjective synonym of' )
	    {
		$s->{note} = '';
		push @syns, $s
	    }
	    
	    elsif ( $s->{status} eq 'replaced by' )
	    {
		$s->{note} = ' [replaced name]';
		push @syns, $s
	    }
	    
	    else
	    {
		my $status = $s->{status};
		$status =~ s/ of$//;
		$s->{note} = " [$status]";
		push @invalids, $s;
	    }
	}
	
	if ( @syns > 0 )
	{
	    my $list = join(', ', map { italicize($_) . ' ' . formatShortAuthor($_) . $s->{note} } @syns );
	    print "<p $indent>Synonyms: $list</p>\n\n";
	}
	
	if ( @invalids > 0 )
	{
	    my $list = join(', ', map { italicize($_) . ' ' . formatShortAuthor($_) . $s->{note} } @syns );
	    print "<p $indent>Invalid subtaxa: $list</p>\n\n";
	}
	
	# FULL AUTHORITY REFERENCE

	if ( $focal_taxon->{ref_is_authority} =~ /y/i )	{
		print "<p $indent>Full reference: ".Reference::formatLongRef($focal_taxon)."</p>\n\n";
	}

	# PARENT SECTION
	
	my $parent_taxon = $taxonomy->getRelatedTaxon('parent', $focal_taxon, { fields => ['ref', 'oldattr'] });
	
	my $belongs = ( $taxon_rank =~ /species/ ) ? "Belongs to" : "Parent taxon:";
	
	if ( $parent_taxon )
	{
	    print "<p style=\"clear: left;\">$belongs <a href=\"?a=basicTaxonInfo&amp;taxon_no=$parent_taxon->{'taxon_no'}\">".italicize($parent_taxon)."</a>";
	    print " according to ".Reference::formatShortRef($parent_taxon,'link_id'=>1);
	    print "</p>\n\n";
	    
	    my (@other_opinions) = $taxonomy->getOpinions('child', $focal_taxon, { fields => ['ref'] });
	    my (@other_refs);
	    my (%seen_ref) = ( $parent_taxon->{reference_no} => 1 );
	    
	    foreach my $o (@other_opinions)
	    {
		push @other_refs, Reference::formatShortRef($o, link_id => 1)
		    unless $seen_ref{$o->{reference_no}};
		$seen_ref{$o->{reference_no}} = 1;
	    }
	    
	    if ( @other_refs )
	    {
		my $lastref = pop @other_refs;
		my $list = join(', ', @other_refs);
		my $and = $list ? ' and ' : '';
		print "<p $indent>See also $list$and$lastref</p>\n\n";
	    }
	}
	
	# SISTERS SECTION
	
	my @sisters = $taxonomy->getTaxa('children', $parent_taxon, { exclude => $focal_taxon });
	
	if ( @sisters )
	{
	    my $word = (@sisters > 1) ? 'taxa' : 'taxon';

	    my @list = map { "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$_->{'taxon_no'}\">".italicize($_)."</a>, " }
		@sisters;
	    my $list = join(', ', @list);
	    
	    print "<p style=\"margin-left: 1em;\"><span style=\"margin-left: -1em; text-indent: -0.5em;\">Sister $word: $list</p>\n\n";
	}
	
	# CHILDREN SECTION
	
	my @child_taxa = $taxonomy->getTaxa('children', $focal_taxon);
	
	if ( @child_taxa || $taxon_rank !~ /species/ )
	{
	    my ($list, $classification_script);
	    
	    if ( @child_taxa )
	    {
		$list = join(', ',
		   map { "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$_->{'taxon_no'}\">".italicize($_)."</a>, " }
			     @child_taxa );
		$classification_script = qq|
<p><a href=# onClick="javascript: document.doViewClassification.submit()">View classification</a></span></p>
<form method="POST" action="" name="doViewClassification">
<input type="hidden" name="action" value="classify">
<input type="hidden" name="taxon_no" value="$taxon_no">
</form>
|;
	    }
	    
	    else
	    {
		$list = "<i>none</i>";
		$classification_script = '';
	    }
	    
	    print qq|
<p style=\"margin-left: 1em;\"><span style=\"margin-left: -1em; text-indent: -0.5em;\">Subtaxa: $list</span></p>
$classification_script|;
	}
	
	# TYPE SECTION

	my (@spellings) = map { $_->{taxon_no} } $focal_taxon, @alt_spellings;
	
	($typeInfo,$typeLocality) = displayTypeInfo($dbt,join(',', @spellings), $focal_taxon, 1, 'basicTaxonInfo');

	if ( $typeInfo )
	{
	    if ( $typeInfo !~ /\. [A-Za-z]/ )
	    {
		$typeInfo =~ s/[\.] //;
	    }
	    if ($taxon_rank =~ /species/)
	    {
		unless ( @junior_synonyms )
		{
		    print "<p $indent>Type specimen: $typeInfo</p>\n\n";
		} 
		else
		{
		    print "<p $indent>Type specimens:\n</p>\n<ul style=\"margin-top: -0.5em;\">";
		    print "<li><i>$taxon_name</i>: ".$typeInfo."</li>\n";
		    foreach my $t ( @junior_synonyms )
		    {
			my ($synTypeInfo,$synTypeLocality) = printTypeInfo($dbt,$t->{taxon_no},$t,1,'basicTaxonInfo');
			print "<li><i>$t->{taxon_name}</i>: ".$synTypeInfo."</li>\n";
		    }
		    print "</ul>\n";
		}
	    } 
	    else
	    {
		print "<p $indent>Type: $typeInfo</p>\n\n";
	    }
	}
	
	# ECOLOGY SECTION

	if ( $taxon_no && $SQL_DB eq "pbdb" )
	{
	    my $eco_hash = Ecology::getEcology($dbt, $taxonomy, $focal_taxon,
					       ['locomotion','life_habit','diet1','diet2'],'get_basis');
	    my $ecotaphVals = $eco_hash->{$taxon_no};
	    
	    if ( $ecotaphVals )	{
		print "<p>Ecology:";
		# it's really annoying how often this gets printed
		$ecotaphVals->{'locomotion'} =~ s/actively mobile//;
		for my $e ( 'locomotion','life_habit','diet1' )	{
		    if ( $ecotaphVals->{$e} )	{
			print " ".$ecotaphVals->{$e};
		    }
		}
		if ( $ecotaphVals->{'diet1'} && $ecotaphVals->{'diet2'} )	{
		    print "-".$ecotaphVals->{'diet2'};
		}
		print "</p>\n\n";
	    }
	}
	
	# MEASUREMENT AND BODY MASS SECTIONS
	# JA 24.11.10
	# added body mass and simplified by calling getMassEstimates 9.12.10

	my @specimens;
	my $specimen_count;
	if ( $taxon_no && $taxon_rank eq "species" && $SQL_DB eq "pbdb" )
	{
		@specimens = Measurement::getMeasurements($dbt, $taxonomy, taxon_list => \@all_spellings,
							  get_global_specimens => 1);
		if ( @specimens )	{
			my $p_table = Measurement::getMeasurementTable(\@specimens);
			my $orig = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
			my $ss = TaxonInfo::getSeniorSynonym($dbt,$orig);
			my @m = Measurement::getMassEstimates($dbt,$ss,$p_table,'skip area');
			if ( @{$m[1]} )	{
				print "<p $indent>Average measurements (in mm): ".join(', ',@{$m[1]});
				print "</p>\n\n";
			}
			if ( $m[5] && $m[6] )	{
				my @eqns = @{$m[3]};
				s/^[A-Za-z]+ // foreach @eqns;
				my %perpart;
				$perpart{$_}++ foreach @eqns;
				@eqns = keys %perpart;
				@eqns = sort @eqns;
				if ( $#eqns > 0 )	{
					$eqns[$#eqns] = "and ".$eqns[$#eqns];
				}
				if ( $#eqns > 1 )	{
					$eqns[$_] .= "," foreach ( 0..$#eqns-1 );
				}
				print "<p $indent>Estimated body mass: ".formatMass( exp( $m[5]/$m[6] ) )." based on ".join(' ',@eqns);
				print "</p>\n\n";
			}
		}
	}
    }
    
    # DISTRIBUTION SECTION
    
    my @occs;
    if ( $is_real_user > 0 && $SQL_DB eq "pbdb" && (not defined $focal_taxon
						    or $focal_taxon->{'rgt'} - $focal_taxon->{'lft'} < 20000 ) )
    {
		# taxon_string is needed for maps and taxon_param for links
		my $taxon_string = $taxon_no;
		my $taxon_param = "taxon_no=".$taxon_no;
		if ( ! $taxon_string )	{
			$taxon_string = $taxon_name;
			$taxon_param = "taxon_name=".$taxon_name;
		}
		$taxon_string =~ s/ /_/g;

		my $collection_fields = "c.collection_no,collection_name,max_interval_no,min_interval_no,country,state";
		my $sql;
		
		if ( $focal_taxon )
		{
		    my $temp_table = $taxonomy->getTaxa('all_children', $focal_taxon, 
						    { return => 'id_table', select => 'all' });
		    
		    $sql = "
		        (SELECT $collection_fields, count(distinct(o.collection_no)) as c, count(distinct(o.occurrence_no)) as o
			FROM collections as c JOIN occurrences as o using (collection_no)
				JOIN $temp_table as t on o.taxon_no = t.taxon_no
				LEFT JOIN reidentifications as re on o.occurrence_no = re.occurrence_no
			WHERE re.reid_no IS NULL
			GROUP BY c.max_interval_no, c.min_interval_no, country, state)
			UNION
			(SELECT $collection_fields, count(distinct(c.collection_no)) as c, count(distinct(re.occurrence_no)) as o
			FROM collections as c JOIN reidentifications as re using (collection_no)
				JOIN $temp_table as t on re.taxon_no = t.taxon_no
			WHERE re.most_recent='YES'
			GROUP BY c.max_interval_no, c.min_interval_no, country, state)";
		} 
		
		else
		{
		    my ($g,$s) = split / /,$taxon_name;
		    my $name_clause = "(o.genus_name='".$g."' OR o.subgenus_name='".$g."')";
		    if ( $s )
		    {
			$name_clause .= " AND o.species_name='".$s."'";
		    }
		    
		    my $name_clause2 = $name_clause;
		    $name_clause2 =~ s/o\./re\./g;
		    
		    $sql = "
			(SELECT $collection_fields, count(distinct(c.collection_no)) as c, count(distinct(o.occurrence_no)) as o
			FROM collections as c JOIN occurrences as o using(collection_no)
				LEFT JOIN reidentifications as re ON o.occurrence_no = re.occurrence_no
			WHERE $name_clause AND re.reid_no IS NULL
			GROUP BY c.max_interval_no, c.min_interval_no, country, state)
			UNION
			(SELECT $collection_fields, count(distinct(c.collection_no)) as c, count(distinct(re.occurrence_no)) as o
			FROM collections as c JOIN reidentifications as re using (collection_no)
			WHERE $name_clause2 AND re.most_recent='YES'
			GROUP BY c.max_interval_no, c.min_interval_no, country, state)";
		}
		
		@occs = @{$dbt->getData($sql)};
		
		$sql = "SELECT l.interval_no,i1.interval_name period,i2.interval_name epoch,base_age base FROM interval_lookup l,intervals i1,intervals i2 WHERE period_no=i1.interval_no AND epoch_no=i2.interval_no";
		my @intervals = @{$dbt->getData($sql)};
		my (%epoch,%period,%own,%base);
		for my $i ( @intervals )	{
			$epoch{$i->{'interval_no'}} = $i->{'epoch'};
			$period{$i->{'interval_no'}} = $i->{'period'};
			# it doesn't matter which subinterval is used
			$base{$i->{'epoch'}} = $i->{'base'};
			$base{$i->{'period'}} = $i->{'base'};
		}
		$sql = "SELECT i.interval_no,interval_name own,base_age base FROM interval_lookup l,intervals i WHERE l.interval_no=i.interval_no";
		my @intervals2 = @{$dbt->getData($sql)};
		for my $i ( @intervals2 )	{
			$own{$i->{'interval_no'}} = $i->{'own'};
			$base{$i->{'own'}} = $i->{'base'};
		}

		print "<p>Distribution:";
		if ( $#occs == 0 && $occs[0]->{'c'} == 1 )	{
			my $o = $occs[0];
			print qq| found only at <a href="?a=basicCollectionSearch&amp;collection_no=$o->{collection_no}">$o->{'collection_name'}</a>|;
			if ( $typeLocality == 0 )	{
				my $place = ( $o->{'country'} =~ /United States|Canada/ ) ? $o->{'state'} : $o->{'country'};
				$place =~ s/United King/the United King/;
				my $time = ( $period{$o->{'max_interval_no'}} =~ /Paleogene|Neogene/ ) ? $epoch{$o->{'max_interval_no'}} : $period{$o->{'max_interval_no'}};
				$time .= ( $period{$o->{'min_interval_no'}} =~ /Paleogene|Neogene/ ) ? " to ".$epoch{$o->{'min_interval_no'}} : "";
				print qq| ($time of $place)|;
			}
			print "</p>\n\n";
		} elsif ( @occs )	{
			my ($ctotal,$ototal,%bycountry,%bystate);
			for my $o ( @occs )	{
				$ctotal += $o->{'c'};
				$ototal += $o->{'o'};
				if ( $period{$o->{'max_interval_no'}} =~ /Paleogene|Neogene/ )	{
					if ( $epoch{$o->{'max_interval_no'}} eq $epoch{$o->{'min_interval_no'}} || $o->{'min_interval_no'} == 0 || ! $epoch{$o->{'min_interval_no'}} )	{
						$bycountry{$epoch{$o->{'max_interval_no'}}}{$o->{'country'}} += $o->{'c'};
						$bystate{$epoch{$o->{'max_interval_no'}}}{$o->{'country'}}{$o->{'state'}} += $o->{'c'};
					} else	{
						$bycountry{$epoch{$o->{'max_interval_no'}}." to ".$epoch{$o->{'min_interval_no'}}}{$o->{'country'}} += $o->{'c'};
						$bystate{$epoch{$o->{'max_interval_no'}}." to ".$epoch{$o->{'min_interval_no'}}}{$o->{'country'}}{$o->{'state'}} += $o->{'c'};
					}
				} elsif ( $period{$o->{'max_interval_no'}} )	{
					if ( $period{$o->{'max_interval_no'}} eq $period{$o->{'min_interval_no'}} || $o->{'min_interval_no'} == 0 || ! $period{$o->{'min_interval_no'}} )	{
						$bycountry{$period{$o->{'max_interval_no'}}}{$o->{'country'}} += $o->{'c'};
						$bystate{$period{$o->{'max_interval_no'}}}{$o->{'country'}}{$o->{'state'}} += $o->{'c'};
					} else	{
						$bycountry{$period{$o->{'max_interval_no'}}." to ".$period{$o->{'min_interval_no'}}}{$o->{'country'}} += $o->{'c'};
						$bystate{$period{$o->{'max_interval_no'}}." to ".$period{$o->{'min_interval_no'}}}{$o->{'country'}}{$o->{'state'}} += $o->{'c'};
					}
				} else	{
					$bycountry{$own{$o->{'max_interval_no'}}}{$o->{'country'}} += $o->{'c'};
					$bystate{$own{$o->{'max_interval_no'}}}{$o->{'country'}}{$o->{'state'}} += $o->{'c'};
				}
			}
			my @intervals = keys %bycountry;
			for my $i ( @intervals )	{
				if ( ! $base{$i} )	{
					my ($x,$y) = split / /,$i;
					$base{$i} = $base{$x} - 0.01;
				}
			}
			@intervals = sort { $base{$a} <=> $base{$b} } @intervals;
			print "</p>\n\n";
			print "<div style=\"margin-left: 2em;\">\n";
			my $printed;
			for my $i ( @intervals )	{
				print "<p $indent>&bull; $i of ";
				my @countries = keys %{$bycountry{$i}};
				@countries = sort @countries;
				my $list;
				for my $c ( @countries )	{
					my @states = keys %{$bystate{$i}{$c}};
					@states = sort @states;
					for my $j ( 0..$#states )	{
						if ( ! $states[$j] )	{
							splice @states , $j , 1;
							last;
						}
					}
					my ($max_interval,$min_interval) = split/ to /,$i;
					my $country = $c;
					my $shortcountry = $country;
					$shortcountry =~ s/Libyan Arab Jamahiriya/Libya/;
					$shortcountry =~ s/Syrian Arab Republic/Syria/;
					$shortcountry =~ s/Lao People's Democratic Republic/Laos/;
					$shortcountry =~ s/(United Kingdom|Russian Federation|Czech Republic|Netherlands|Dominican Republic|Bahamas|Philippines|Netherlands Antilles|United Arab Emirates|Marshall Islands|Congo|Seychelles)/the $1/;
					$shortcountry =~ s/, .*//;
					my $min_interval_where;
					if ( $min_interval )	{
						$min_interval_where = "&amp;min_interval_no=$min_interval";
					}
					if ( $country !~ /United States|Canada/ || ! @states )	{
						$list .= "<a href=\"?a=displayCollResults&amp;$taxon_param&amp;max_interval=$max_interval$min_interval_where&amp;country=$country&amp;is_real_user=$is_real_user&amp;basic=yes&amp;type=view&amp;match_subgenera=1\">$shortcountry</a> (".$bycountry{$i}{$c};
					} else	{
						for my $j ( 0..$#states )	{
							$states[$j] = "<a href=\"?a=displayCollResults&amp;$taxon_param&amp;max_interval=$max_interval$min_interval_where&amp;country=$country&amp;state=$states[$j]&amp;is_real_user=$is_real_user&amp;basic=yes&amp;type=view&amp;match_subgenera=1\">$states[$j]</a>";
						}
						$list .= "$country ($bycountry{$i}{$c}";
						$list .= ": ".join(', ',@states);
					}
					$printed++;
					if ( $printed == 1 && $bycountry{$i}{$c} == 1 )	{
						$list .= " collection";
					} elsif ( $printed == 1 && $bycountry{$i}{$c} > 1 )	{
						$list .= " collections";
					}
					$list .= "), ";
				}
				$list =~ s/, $//;
				print "$list</p>\n";
			}
			if ( $ctotal > 1 && $ctotal < $ototal )	{
				print "<p>Total: $ctotal collections including $ototal occurrences</p>\n\n";
			} elsif ( $ctotal > 1 && $ctotal == $ototal )	{
				print "<p>Total: $ctotal collections each including a single occurrence</p>\n\n";
			}
			print "</div>\n\n";
		}
		
		# don't print anything for really big groups, users shouldn't
		#  expect to see occurrences anyway JA 13.7.12
		elsif ( $focal_taxon->{'rgt'} - $focal_taxon->{'lft'} >= 20000 )
		{
		}
		
		elsif ( $focal_taxon )
		{
		    print " <i>there are no occurrences of $taxon_name in the database</i></p>\n\n";
		}
		
		else
		{
		    print "</p>\n\n<p><i>There is no taxonomic or distributional information about '$taxon_name' in the database</i></p>\n\n";
		}
	    }

	# MAP SECTION

	if ( $is_real_user and @occs )
	{
	    require GD;
	    my $taxon_string = $taxon_no || $taxon_name;
	    my $taxon_param = "=$taxon_string";
	    $taxon_string =~ s/ /_/g;
	    my $im = new GD::Image(1,1,1);
	    my $GIF_DIR = $HTML_DIR."/public/maps";
	    open(PNG,">$GIF_DIR/taxon".$taxon_string.".png");
	    binmode(PNG);
	    print PNG $im->png;
	    close PNG;
	    chmod 0664, "$GIF_DIR/taxon".$taxon_string.".png";
	    
	    print qq|
<script language="Javascript" type="text/javascript">
<!--

var swapID;
var eraseID;
function requestMap()	{
	document.getElementById('taxonImage').src = '$HOST_URL/?a=displayMapOnly&amp;display_header=NO&amp;$taxon_param';
	document.getElementById('mapLink').innerHTML = '';
	document.getElementById('moreMapLinkText').innerHTML = '';
	document.getElementById('pleaseWait').innerHTML = '<i>Please wait for the map to be generated</i>';
	swapID = setInterval( "swapInMap()" , 2000 );
	// there's no way to erase the message before the image is fully loaded,
	//  so check frequently to see if it has been
	eraseID = setInterval( "erasePleaseWait()" , 100 );
	return(true);
}

function swapInMap()	{
	document.getElementById('pleaseWait').innerHTML += ' .';
	document.getElementById('taxonImage').src = '/public/maps/taxon$taxon_string.png?' + (new Date()).getTime();
	if ( document.getElementById('taxonImage').clientWidth > 100 )	{
		clearInterval( swapID );
	}
	return(true);
}

function erasePleaseWait()	{
	if ( document.getElementById('taxonImage').clientWidth > 100 )	{
		document.getElementById('pleaseWait').style.display = 'none';
		clearInterval( eraseID );
	}
	return(true);
}

// -->
</script>

<p><img id="taxonImage" src="/public/maps/taxon$taxon_string.png"><span id="mapLink" onClick="requestMap();" class="mockLink"><i>Click here</span><span id="moreMapLinkText"> to see a distribution map</span></i><span id="pleaseWait"></span></p>

|;

	}

	if ( $is_real_user and ( @occs || $taxon_no ) )
	{
		if ( $taxon_no && $SQL_DB eq "pbdb" )	{
			print "<p><a href=\"?a=checkTaxonInfo&amp;taxon_no=$taxon_no&amp;is_real_user=1\">Show more details</a></p>\n\n";
		} elsif ( $SQL_DB eq "pbdb" )	{
			print "<p><a href=\"?a=checkTaxonInfo&amp;taxon_name=$taxon_name&amp;is_real_user=1\">Show more details</a></p>\n\n";
		}
		if ( $s->isDBMember() && $taxon_no && $s->get('role') =~ /authorizer|student|technician/ )	{
			print "<p><a href=\"$WRITE_URL?a=displayAuthorityForm&amp;taxon_no=$taxon_no\">Edit ".italicize($focal_taxon)."</a></p>\n\n";
			print "<p><a href=\"$WRITE_URL?a=displayOpinionChoiceForm&amp;taxon_no=$taxon_no\">Add/edit taxonomic opinions about ".italicize($focal_taxon)."</a></p>\n\n";
		}
	}
	print "</div>\n</div>\n\n";

	print qq|
<form method="POST" action="" onSubmit="return checkName(1,'search_again');">
<input type="hidden" name="action" value="basicTaxonInfo">
|;
	if ( $taxon_no )	{
		print qq|<input type="hidden" name="last_taxon" value="$taxon_no">
|;
	}
	print qq|
<span class="small">
<input type="text" name="search_again" value="Search again" size="24" onFocus="textClear(search_again);" onBlur="textRestore(search_again);" style="font-size: 1.0em;">
</span>
</form>

|;

	print "<br>\n\n";
	print "</div>\n\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);
}


# JA 3.11.09
sub formatShortAuthor	{
	my $taxon = shift;
	my $authors = $taxon->{'author1last'};
	if ( $taxon->{'otherauthors'} =~ /[A-Z]/ )	{
		$authors .= " et al.";
	} elsif ( $taxon->{'author2last'} =~ /[A-Z]/ )	{
		$authors .= " and ".$taxon->{'author2last'};
	}
	$authors .= " ".$taxon->{'pubyr'};
	return $authors;
}

# JA 3.11.09
sub italicize	{
	my $taxon = shift;
	my $name = $taxon->{'taxon_name'};
	if ( $taxon->{'taxon_rank'} =~ /genus|species/ )	{
		$name = "<i>".$name."</i>";
	}
	return $name;
}


# This will return all diagnoses for a particular taxon, for all its spellings, and
# for all its junior synonyms. The diagnoses are passed back as a sorted array of hashrefs ordered by
# pubyr of the opinion.  Each hashref has the following keys:
#  taxon_no: spelling_no for the opinion for which the diagnosis exists
#  reference: formated reference for the diagnosis
#  diagnosis: text of the diagnosis field
#  is_synonym: boolean denoting whether this is a 
#  taxon_name: spelling_name for the opinion fo rwhich the diagnosis exists
# Example usage:
#   $taxon = getTaxa($dbt,{'taxon_name'=>'Calippus'});
#   @diagnoses = getDiagnoses($dbt,$taxon->{taxon_no});
#   foreach $d (@diagnoses) {
#       print "$d->{reference}: $d->{diagnosis}";
#   }

sub getDiagnoses {
    
    my ($dbt, $taxonomy, $focal_taxon) = @_;
    
    return unless $focal_taxon;
    
    my (@synonyms) = $taxonomy->getTaxa('synonyms', $focal_taxon);
    my (@opinions) = $taxonomy->getOpinions('child', \@synonyms, { fields => ['child', 'oldattr'] });
    my (@diagnoses);
    
    foreach my $o (@opinions)
    {
	my $reference = Reference::formatShortRef($o);
	
	my $diagnosis = {
                    taxon_no => $o->{child_spelling_no},
                    taxon_name => $o->{child_name},
                    taxon_rank => $o->{child_rank},
                    reference => $reference,
                    pubyr => $o->{pubyr},
                    opinion_no => $o->{opinion_no},
                    diagnosis => $o->{diagnosis},
                    is_synonym => $o->{child_no} != $focal_taxon->{orig_no}
                };
	
	push @diagnoses, $diagnosis;
    }
    
    return sort {if ($a->{pubyr} && $b->{pubyr}) {$a->{pubyr} <=> $b->{pubyr}}
		 else {$a->{opinion_no} <=> $b->{opinion_no}}} @diagnoses;
    
}


1;
