package Strata;

use TaxonInfo;
use Permissions;
use URI::Escape;
if ($main::DEBUG) {
    use Data::Dumper;
}

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
#   * group/formation/member name
#   * formation present if group, members present if formation
#   * lithologies present, paleoenvironments present
#   * age range
#   * what collections are in it, binned into timerange then country bins
sub displayStrataSearch{
	my $dbh = shift;
	my $dbt = shift;
    my $hbo = shift;
	my $q = shift;
    my $s = shift;

    $sql = "SELECT collection_no,access_level,DATE_FORMAT(release_date, '%Y%m%d') rd_short,research_group," 
         . "geological_group,formation,member,lithology1,lithology2,environment FROM collections "
         . "WHERE (member=".$dbh->quote($q->param('search_term'))
              . " OR geological_group=".$dbh->quote($q->param('search_term'))
              . " OR formation=".$dbh->quote($q->param('search_term')).")";
    if ($q->param("group")) { 
        $sql .= " AND geological_group=".$dbh->quote($q->param('group'));
    }
    if ($q->param("formation")) { 
        $sql .= " AND formation=".$dbh->quote($q->param('formation')); 
    }    

    main::dbg("sql: $sql<br>");

    # Get rows okayed by permissions module
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $p = Permissions->new( $s );
    my $limit = 100000;
    my (@dataRows, $ofRows);
    $p->getReadRows( $sth, \@dataRows, $limit, \$ofRows );
    
    main::dbg("rows returned by permissions module: $ofRows<br>");

    # Do conflict checking beforehand, see function definition for explanation
    my $conflict_found = checkConflict(\@dataRows,$q);

    # Will help narrow parameters, see if the conflict is resolved
    #   we do this instead of just narrowing it down
    #   from the beginning, because initally we want a broad search (i.e. ogallala is
    #   is a formation and group, so we want to display both)
    # Also important for having TaxonInfo::doCollections return what we want
    if ($conflict_found) {
        if ($conflict_found eq "different groups") {
            if ($q->param("group_hint")) {
                $q->param("geological_group" => $q->param("group_hint"));
            }
        } elsif ($conflict_found eq "different formations") {
            if ($q->param("formation_hint")) {
                $q->param("formation" => $q->param("formation_hint"));
            }    
        } elsif ($conflict_found eq "different lines") {
            if ($q->param("formation_hint")) {
                $q->param("formation" => $q->param("formation_hint"));
            }    
            if ($q->param("group_hint")) {
                $q->param("geological_group" => $q->param("group_hint"));
            }
        }
    }    

    # check again, see if conflict resolved.
    # this wouldn't happen if we were linked from the collSearch, as a hint
    # is always passed, but just if it does, let user supply missing options
    $conflict_found = checkConflict(\@dataRows,$q);
    if ($conflict_found) {
        displayStrataChoice($q, $conflict_found, \@dataRows);
	    print main::stdIncludes("std_page_bottom");
        exit;    
    }

    # build data structures, looped through later
    my ($is_group, $is_formation, $is_member, %lith_count, %environment_count);
    my ($row_count, %c_formations, %c_members);
    $is_group = 0; $is_formation = 0; $is_member = 0; %lith_count = (); %environment_count = ();
    $row_count = 0; %c_formations = (); %c_members = ();

    foreach my $row (@dataRows) {
        if ($q->param('formation')) {
            next unless (lc($q->param('formation')) eq lc($row->{'formation'}));
        }    
        if ($q->param('geological_group')) {
            next unless (lc($q->param('geological_group')) eq lc($row->{'geological_group'}));
        }    
        $row_count++;
        # group hierarchy data, c_ denote "children of this formation or group"
        if (lc($q->param('search_term')) eq lc($row->{'geological_group'})) {
            $is_group = 1;
            $c_formations{$row->{'formation'}} += 1;
        }
        if (lc($q->param('search_term')) eq lc($row->{'formation'})) {
            $is_formation = 1; 
            $c_members{$row->{'member'}} += 1;
        }
        if (lc($q->param('search_term')) eq lc($row->{'member'})) {
            $is_member = 1;
        }

        # lithology data
        my $lith1 = $row->{'lithology1'};
        my $lith2 = $row->{'lithology2'};
        $lith_count{$lith1} += 1 if ($lith1);
        $lith_count{$lith2} += 1 if ($lith2 && ($lith1 ne $lith2));

        # environment data
        my $environment = $row->{'environment'};
        $environment_count{$environment} += 1 if ($environment);

        #main::dbg("c_no: $row->{collection_no} l1: $row->{lithology1} l2: $row->{lithology2} e: $row->{environment}");
        #main::dbg(" f: $row->{formation} g: $row->{geological_group} m: $row->{member} <br>");
    }
    main::dbg("is mbr: $is_member is form: $is_formation is grp: $is_group<hr>");
    main::dbg("c_form: <pre>" . Dumper(%c_formations) . "</pre>");
    main::dbg("c_mbr: <pre>" . Dumper(%c_members) . "</pre>");

    # Display Header
	print main::stdIncludes("std_page_top");

    my $in_strata_type = "";
    $in_strata_type .= "Member" if ($is_member);
    $in_strata_type .= ", Formation" if ($is_formation);
    $in_strata_type .= ", Group" if ($is_group);
    $in_strata_type =~ s/^, //g;
   
    my $collTxt = ($row_count == 0) ? "No collections found" 
                : ($row_count == 1) ? "1 collection total"
                                 : "$row_count collections total";
    print qq|<div align="center"><h2>|;
    print $q->escapeHTML(ucfirst($q->param('search_term'))) . " " . $in_strata_type;
    print qq|</h2>($collTxt)</div></br>|;

    # Display formations in groups, members in formations
    my ($cnt,$plural,$coll_link,$html);
    if ($is_group) {
        $html = "<p><b>Formations in the ". ucfirst($q->param('search_term'))." Group:</b> ";
        foreach $formation (sort(keys(%c_formations))) {
            $cnt = $c_formations{$formation};
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
            $html .=  "$coll_link ($c_formations{$formation}), ";
        }
        $html =~ s/, $//g;
        $html .= "</p>\n";
        print $html;
    }

    if ($is_formation) {
        $html = "<p><b>Members in the ".ucfirst($q->param('search_term'))." Formation:</b> ";
        foreach $member (sort(keys(%c_members))) {
            $cnt = $c_members{$member};
            $coll_link = "";
            if ($member) {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&formation=|
                           . uri_escape($q->param('search_term'));
                $coll_link .= "&geological_group=".uri_escape($q->param('geological_group')) if $q->param('geological_group');
                $coll_link .= qq|&member=|.uri_escape($member).qq|">$member</a>|;
            } else {
                $coll_link =  qq|<a href="$exec_url?action=displayCollResults&formation=|
                           . uri_escape($q->param('search_term'));
                $coll_link .= "&geological_group=".uri_escape($q->param('geological_group')) if $q->param('geological_group');
                $coll_link .= qq|&member=NULL_OR_EMPTY">unknown</a>|;
            } 
            $html .= "$coll_link ($c_members{$member}), ";
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
                $cnt = $lith_count->{$lithology};
                $coll_link = qq|<a href="$exec_url?action=displayCollResults| 
                            . "&group_formation_member=".uri_escape($q->param('search_term'));
                $coll_link .= "&formation=".uri_escape($q->param('formation')) if $q->param('formation');
                $coll_link .= "&geological_group=".uri_escape($q->param('geological_group')) if $q->param('geological_group');
                $coll_link .= qq|&lithologies=|.uri_escape($lithology).qq|">$lithology</a>|;
                
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
                              . "&group_formation_member=".uri_escape($q->param('search_term'));
                $coll_link .= "&formation=".uri_escape($q->param('formation')) if $q->param('formation');
                $coll_link .= "&geological_group=".uri_escape($q->param('grup')) if $q->param('geological_group');
                $coll_link .= qq|&environment=|.uri_escape($environment).qq|">$environment</a>|;
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

    print "<p>&nbsp;</p>";

	print main::stdIncludes("std_page_bottom");
	return;
}

###
# A conflict occurs in the following cases:
#   1. different_groups: Formation belongs to two different groups
#   2. different_formations: Member belongs to two different formations
#   3. different_lines: Formation with certain group, Member with a different formation
###
sub checkConflict {
    my $dataRows = shift;
    my $q = shift;

    my (%p_formations, %p_groups, %gp_groups);
    %p_formations = (); %p_groups = (); %gp_groups = ();

    foreach my $row (@{$dataRows}) {
        if ($q->param('formation')) {
            next unless (lc($q->param('formation')) eq lc($row->{'formation'}));
        }    
        if ($q->param('geological_group')) {
            next unless (lc($q->param('geological_group')) eq lc($row->{'geological_group'}));
        }    
        # group hierarchy data
        # the p_* arrays denote parents, the gp_* array lists/counts grandparents and are used for
        #   checking for conflicts.  i.e. a member that belongs to two different formations
        #   so if theres no conflicts the p_* and g_* should have only 1 non-null element in them
        if (lc($q->param('search_term')) eq lc($row->{'formation'})) {
            $p_groups{$row->{'geological_group'}} += 1;
        }
        if (lc($q->param('search_term')) eq lc($row->{'member'})) {
            $p_formations{$row->{'formation'}} += 1;
            $gp_groups{$row->{'formation'}}{$row->{'geological_group'}} += 1;
        }
    }
    main::dbg("p_form: <pre>" . Dumper(%p_formations) . "</pre>");
    main::dbg("p_grp: <pre>" . Dumper(%p_groups) . "</pre>");
    main::dbg("gp_grp: <pre>" . Dumper(%gp_groups) . "</pre>");

    my ($p_formation_cnt, $p_group_cnt, $conflict_found);
    $p_formation_cnt = 0; $p_group_cnt = 0;
    foreach my $fm (keys %p_formations) {
        $p_formation_cnt += 1 if ($fm);
    }             
    foreach my $grp (keys %p_groups) {
        $p_group_cnt += 1 if ($grp);
    }             
    main::dbg("p grp cnt: $p_group_cnt p fm cnt: $p_formation_cnt");
    $conflict_found = "different lines" if ($p_formation_cnt > 0 && $p_group_cnt > 0);
    $conflict_found = "different groups" if ($p_group_cnt > 1);
    $conflict_found = "different formations" if ($p_formation_cnt > 1);
    return $conflict_found;    
}


# In the case of an unresolvable conflict, display a choice to the user
sub displayStrataChoice() {
    my $q = shift; 
    my $conflict_reason = shift;
    my $dataRows = shift;
    my (%formation_links,%group_links);
    %formation_links = ();
    %group_links = ();

    foreach $row (@{$dataRows}) {
        if (lc($q->param('search_term')) eq lc($row->{'formation'})) {
            $group_links{$row->{'geological_group'}} += 1 if ($row->{'geological_group'});
        }    
        if (lc($q->param('search_term')) eq lc($row->{'member'})) {
            $formation_links{$row->{'formation'}} += 1 if ($row->{'formation'});
        }
    }    

    main::dbg("In display strata choice for reason: $conflict_reason");
	print main::stdIncludes("std_page_top");
    print "<center>";
    my $count = 0;
    if ($conflict_reason eq "different groups") {
        print "The ".$q->param('search_term')." formation belongs to multiple groups.  Please select the one you want: <p>";
        foreach $grp (keys %group_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"$exec_url?action=displayStrataSearch"
                . "&geological_group=".uri_escape($grp)
                . "&search_term=".uri_escape($q->param('search_term'))."\">$grp</a></b>";
        }          
        print "</p>";
    } elsif ($conflict_reason eq "different formations") {
        print "The ".$q->param('search_term')." member belongs to multiple formations.  Please select the one you want: <p>";
        foreach $fm (keys %formation_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"$exec_url?action=displayStrataSearch"
                . "&formation=".uri_escape($fm)
                . "&search_term=".uri_escape($q->param('search_term'))."\">$fm</a></b> ";
        }          
        print "</p>";
    } elsif ($conflict_reason eq "different lines") {
        print "The term ".$q->param('search_term')." is ambiguous and belongs to multiple formations or groups.  Please select the one you want: <p>";
        foreach $fm (keys %formation_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"$exec_url?action=displayStrataSearch"
                . "&formation=".uri_escape($fm)
                . "&search_term=".uri_escape($q->param('search_term'))."\">$fm (formation)</a></b> ";
        }          
        foreach $grp (keys %group_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"$exec_url?action=displayStrataSearch"
                . "&geological_group=".uri_escape($grp)
                . "&search_term=".uri_escape($q->param('search_term'))."\">$grp (group)</a></b><br> ";
        }          
        print "</p>";
    }
    print "</center>";

    print "<p>&nbsp;</p>";

	print main::stdIncludes("std_page_bottom");
    exit;    
}

1;
