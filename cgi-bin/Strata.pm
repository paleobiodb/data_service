package Strata;

use TaxonInfo;
use Permissions;
use URI::Escape;

# written by PS  12/01/2004


# Not imp'd yet
sub startStrataSearch	{
	my $dbh = shift;
	my $hbo = shift;
	my $s = shift;

	# print the search form
	print main::stdIncludes("std_page_top");
	#my $form = $hbo->populateHTML('search_strata_form','','');
	#print $form;
	print main::stdIncludes("std_page_bottom");
	return;
}

# Not imp'd yet
sub populateStrataForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;

	my @fieldNames;
	my @fieldValues;
    
	for my $field ( @fields )	{
		push @fieldNames, $field;
		push @fieldValues, '';
	}

    my @fields = ();
	# populate the form

	#print main::stdIncludes("std_page_top");
	#my $form = $hbo->populateHTML(strata_form, \@fieldValues, \@fieldNames);
	#print $form;
	#print main::stdIncludes("std_page_bottom");

	return;

}

# print out info for a geological group, formation, or member, including:
#   1. group/formation/member name
#   2. lithologies present, formation present if group, members present if formation
#   3. paleoenvironments
#   4. age range
#   5. what collections are in it, binned into timerange then country bins
sub displayStrataSearch{
	my $dbh = shift;
	my $dbt = shift;
    my $hbo = shift;
	my $q = shift;
    my $s = shift;

	# print the header
	print main::stdIncludes("std_page_top");

    $sql = "SELECT collection_no,access_level,DATE_FORMAT(release_date, '%Y%m%d') rd_short,research_group," 
         . "geological_group,formation,member,lithology1,lithology2,environment FROM collections "
         . "WHERE member = ? OR geological_group = ? OR formation = ?";

    main::dbg("sql: $sql<br>");

    # Get rows okayed by permissions module
    my $sth = $dbh->prepare($sql);
    $sth->execute($q->param('search_term'),$q->param('search_term'),$q->param('search_term'));
    my $p = Permissions->new( $s );
    my $limit = 100000;
    my (@dataRows, $ofRows);
    $p->getReadRows( $sth, \@dataRows, $limit, \$ofRows );
    
    main::dbg("rows returned by permissions module: $ofRows<br>");

    my ($is_geological_group, $is_formation, $is_member, %lith_count, %environment_count, %formations, %members);
    $is_geological_group = 0; $is_formation = 0; $is_member = 0; %lith_count = (); %environment_count = (); %formations = (); %members = ();
    
    foreach $href (@dataRows) {
        # group hierarchy data
        if (lc($q->param('search_term')) eq lc($href->{'geological_group'})) {
            $is_geological_group = 1;
            $formations{$href->{'formation'}} += 1;
        }
        if (lc($q->param('search_term')) eq lc($href->{'formation'})) {
            $is_formation = 1; 
            $members{$href->{'member'}} += 1;
        }
        if (lc($q->param('search_term')) eq lc($href->{'member'})) {
            $is_member = 1;
        }

        # lithology data
        my $lith1 = $href->{'lithology1'};
        my $lith2 = $href->{'lithology2'};
        $lith_count{$lith1} += 1 if ($lith1);
        $lith_count{$lith2} += 1 if ($lith2 && ($lith1 ne $lith2));

        # environment data
        my $environment = $href->{'environment'};
        $environment =~ s/(^")|("$)//g;
        $environment_count{$environment} += 1 if ($environment);

        main::dbg("c_no: $href->{collection_no} l1: $href->{lithology1} l2: $href->{lithology2} e: $href->{environment}");
        main::dbg(" f: $href->{formation} g: $href->{geological_group} m: $href->{member} <br>");
    }
    main::dbg("$is_member $is_formation $is_geological_group<hr>");

    # Display Header
    my $in_strata_type = "";
    $in_strata_type .= "Member" if ($is_member);
    $in_strata_type .= ", Formation" if ($is_formation);
    $in_strata_type .= ", Group" if ($is_geological_group);
    $in_strata_type =~ s/^, //g;
   
    my $collTxt = ($ofRows == 0) ? "No collections found" 
                : ($ofRows == 1) ? "1 collection total"
                                 : "$ofRows collections total";
    print qq|<div align="center"><h2>|;
    print $q->escapeHTML(ucfirst($q->param('search_term'))) . " " . $in_strata_type;
    print qq|</h2>($collTxt)</div></br>|;

    # Display formations in groups, members in formations
    my ($cnt,$plural,$coll_link,$html);
    if ($is_geological_group) {
        $html = "<p><b>Formations in the ". ucfirst($q->param('search_term'))." Group:</b> ";
        foreach $formation (sort(keys(%formations))) {
            $cnt = $formations{$formation};
            #$plural = ($cnt == 1) ? "" : "s";
            $coll_link = "";
            if ($formation) {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&geological_group=|
                         . uri_escape($q->param('search_term'))
                         . qq|&formation=|.uri_escape($formation).qq|">$formation</a>|;
            } else {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&geological_group=|
                         . uri_escape($q->param('search_term'))
                         . qq|&formation=NULL_OR_EMPTY">unknown</a>|;
            }
            $html .=  "$coll_link ($formations{$formation}), ";
        }
        $html =~ s/, $//g;
        $html .= "</p>\n";
        print $html;
    }

    if ($is_formation) {
        $html = "<p><b>Members in the ".ucfirst($q->param('search_term'))." Formation:</b> ";
        foreach $member (sort(keys(%members))) {
            $cnt = $members{$member};
            $coll_link = "";
            if ($member) {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&formation=|
                         . uri_escape($q->param('search_term'))
                         . qq|&member=|.uri_escape($member).qq|">$member</a>|;
            } else {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&formation=|
                         . uri_escape($q->param('search_term'))
                         . qq|&member=NULL_OR_EMPTY">unknown</a>|;
            } 
            $html .= "$coll_link ($members{$member}), ";
        }
        $html =~ s/, $//g;
        $html .= "</p>\n";
        print $html;
    }    

    # Display lithologies present
    my @lith_list = @{$hbo->{SELECT_LISTS}{lithology1}};
        $html = "<p><b>Lithologies:</b> ";
    if (%lith_count) {
        foreach $lithology (@lith_list) {
            if ($lith_count{$lithology}) {
                 $cnt = $lith_count{$lithology};
                 $coll_link = qq|<a href="$exec_url?action=displayCollResults| 
                            . "&group_formation_member=".uri_escape($q->param('search_term'))
                            . qq|&lithologies=|.uri_escape($lithology).qq|">$lithology</a>|;
                
                $html .= "$coll_link ($lith_count{$lithology}), ";
            }      
        }
        $html =~ s/, $//g;
    } else { 
        $html .= "<i>unknown</i>";
    }
    $html .= "</p>\n";
    print $html;

    # Display environments present
    my @env_list = @{$hbo->{SELECT_LISTS}{environment}};
    $html = "<p><b>Paleoenvironments:</b> ";
    if (%environment_count) {
        foreach $environment (@env_list) {
            if ($environment_count{$environment}) {
                $coll_link = qq|<a href="$exec_url?action=displayCollResults| 
                              . "&group_formation_member=".uri_escape($q->param('search_term'))
                              . qq|&environment=|.uri_escape($environment).qq|">$environment</a>|;
                $html .= "$coll_link ($environment_count{$environment}), ";
            }
        }
        $html =~ s/, $//g;
    } else { 
        $html .= "<i>unknown</i>";
    }
    $html .= "</p>\n";
    print $html;

    # Display age range/Show what collections are in it 
    # Set this q parameter so processCollectionsSearch (called from doCollections) builds correct SQL query
    $q->param("group_formation_member"=>$q->param('search_term'));
    print TaxonInfo::doCollections($q->url(), $q, $dbt, $dbh, '',"for_strata_module");

	print main::stdIncludes("std_page_bottom");
	return;
}

1;
