package Strata;

use strict;
use TaxonInfo;
use URI::Escape;
use Data::Dumper;

# written by PS  12/01/2004


# print out info for a geological group, formation, or member, including:
#   * group/formation/member name
#   * formation present if group, members present if formation
#   * lithologies present, paleoenvironments present
#   * age range
#   * what collections are in it, binned into timerange then country bins
sub displayStrata {
    my ($q,$s,$dbt,$hbo) = @_;
    my $dbh = $dbt->dbh;

    # Gets teh datas
    my %options = $q->Vars();
    $options{'permission_type'} = 'read';                                                                                                                   
    $options{'calling_script'} = 'Strata';
    my @fields = ('geological_group','formation','member','lithology1','lithology2','environment');
    my ($dataRows,$ofRows) = main::processCollectionsSearch($dbt,\%options,\@fields);

    # Do conflict checking beforehand, see function definition for explanation
    my $conflict_found = checkConflict($dataRows,$q);

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
            } elsif ($q->param("group_hint")) {
                $q->param("geological_group" => $q->param("group_hint"));
            }
        } elsif ($conflict_found eq "different lines") {
            if ($q->param("formation_hint")) {
                $q->param("formation" => $q->param("formation_hint"));
            }    
            if ($q->param("group_hint")) {
                $q->param("geological_group" => $q->param("group_hint"));
            }
        }

        # check again, see if conflict resolved.
        # this wouldn't happen if we were linked from the collSearch, as a hint
        # is always passed, but just if it does, let user supply missing options
        $conflict_found = checkConflict($dataRows,$q);
        if ($conflict_found) {
            displayStrataChoice($q, $conflict_found, $dataRows);
            return;    
        }
    }    

    # build data structures, looped through later
    my ($is_group, $is_formation, $is_member, %lith_count, %environment_count);
    my ($row_count, %c_formations, %c_members, %p_groups, %p_formations);
    my %formation_uc;
    my %member_uc;
    my %group_uc;
    $is_group = 0; $is_formation = 0; $is_member = 0; %lith_count = (); %environment_count = ();
    $row_count = 0; %c_formations = (); %c_members = ();

    foreach my $row (@$dataRows) {
        if ($q->param('formation')) {
            if ($q->param("formation") eq "NULL_OR_EMPTY") {
                next if ($row->{'formation'});
            } else {
                next unless (lc($q->param('formation')) eq lc($row->{'formation'}));
            }
        }    
        if ($q->param('geological_group')) {
            if ($q->param("geological_group") eq "NULL_OR_EMPTY") {
                next if ($row->{'geological_group'});
            } else {
                next unless (lc($q->param('geological_group')) eq lc($row->{'geological_group'}));
            }
        }    
        $row_count++;
        # group hierarchy data, c_ denote "children of this formation or group"
        # P denotes we'll print links to these guys
        if (lc($q->param('group_formation_member')) eq lc($row->{'geological_group'})) {
            $is_group = 1;
            $c_formations{$row->{'formation'}} += 1;
        }
        if (lc($q->param('group_formation_member')) eq lc($row->{'formation'})) {
            $is_formation = 1; 
            $c_members{lc($row->{'member'})} += 1;
            $p_groups{lc($row->{'geological_group'})} = 1 if ($row->{'geological_group'});
        }
        if (lc($q->param('group_formation_member')) eq lc($row->{'member'})) {
            $is_member = 1;
            $p_formations{lc($row->{'formation'})} = 1 if ($row->{'formation'});
            $p_groups{lc($row->{'geological_group'})} = 1 if ($row->{'geological_group'});
        }

        # mysql isn't case sensitive but perl is, which is a problem since the cases often differ for things like "lower" and "upper"
        # So we want to store all the counts using only lowercased versions of stuff, but we want to store the uppercase "real" spelling
        # in the _uc arrays for printing back out later
        $member_uc{lc($row->{'member'})} = ($row->{'member'}) if (lc($row->{'member'}) ne $row->{'member'});
        $formation_uc{lc($row->{'formation'})} = ($row->{'formation'}) if (lc($row->{'formation'}) ne $row->{'formation'});
        $group_uc{lc($row->{'geological_group'})} = $row->{'geological_group'} if (lc($row->{'geological_group'}) ne $row->{'geological_group'});

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

    my $in_strata_type = "";
    $in_strata_type .= "Member" if ($is_member);
    $in_strata_type .= ", Formation" if ($is_formation);
    $in_strata_type .= ", Group" if ($is_group);
    $in_strata_type =~ s/^, //g;
   
    print qq|<div align="center"><h2>|;
    my $name = ucfirst($q->param('group_formation_member'));
    if ($name =~ /^(lower part|upper part|lower|middle|upper|bottom|top|medium|base|basal|uppermost)(\s+to\s+(lower part|upper part|lower|middle|upper|bottom|top|medium|base|basal|uppermost))*$/i) {
        if ($is_member) {
            my @formations = keys %p_formations;
            my $formation = $formations[0];
            $formation = ($formation_uc{$formation}) ? $formation_uc{$formation} : $formation; # get correct capitalization
            if ($formation) {
                $name = "$name $formation";
            } else {
                my @groups = sort keys %p_groups;
                my $group = $groups[0];
                if ($group) {
                    $group = ($group_uc{$group}) ? $group_uc{$group} : $group; # get correct capitalization
                    $name = "$name $group";
                }
            }
        }  elsif ($is_formation) {
            my @groups = sort keys %p_groups;
            my $group = $groups[0];
            if ($group) {
                $group = ($group_uc{$group}) ? $group_uc{$group} : $group; # get correct capitalization
                $name = "$name $group";
            }
        }
    }
    $name =~ s/ (formation|group|member|fm\.|gp\.|mbr\.|grp\.)$//ig;
    print $q->escapeHTML($name)." ".$in_strata_type."</h2>";

    print "<div style=\"text-align: left\">";
    # Display formations in groups, members in formations
    my ($cnt,$plural,$coll_link,$html);
    if ($is_formation || $is_member) {
        my @groups = sort keys %p_groups;
        if (@groups) {
            my $html = "<p><b>Group:</b> ";
            foreach my $g_lc (@groups) {
                my $g = ($group_uc{$g_lc}) ? $group_uc{$g_lc} : $g_lc;
                $html .=  qq|<a href="bridge.pl?action=displayStrata&geological_group=|.uri_escape($g)
                      . "&group_formation_member=".uri_escape($g)."\">$g</a>, ";
            }
            $html =~ s/, $//;
            $html .= '</p>';
            print $html;
        }
    }
    if ($is_member) {
        my @formations = sort keys %p_formations;
        if (@formations) {
            my $html = "<p><b>Formation:</b> ";
            foreach my $fm_lc (@formations) {
                my $fm = ($formation_uc{$fm_lc}) ? $formation_uc{$fm_lc} : $fm_lc;
                $html .=  qq|<a href="bridge.pl?action=displayStrata&formation=|.uri_escape($fm)
                      . "&group_formation_member=".uri_escape($fm)."\">$fm</a>, ";
            }
            $html =~ s/, $//;
            $html .= '</p>';
            print $html;
        }
    }

    if ($is_group) {
        my $html = "<p><b>Formations in the $name Group:</b> ";
        foreach my $fm_lc (sort(keys(%c_formations))) {
            $cnt = $c_formations{$fm_lc};
            $coll_link = "";
            my $fm = ($formation_uc{$fm_lc}) ? $formation_uc{$fm_lc} : $fm_lc;
            if ($fm) {
                $coll_link =  qq|<a href="bridge.pl?action=displayCollResults&geological_group=|
                         . uri_escape($q->param('group_formation_member'))
                         . qq|&formation=|.uri_escape($fm).qq|">$fm</a>|;
            } else {
                $coll_link =  qq|<a href="bridge.pl?action=displayCollResults&geological_group=|
                         . uri_escape($q->param('group_formation_member'))
                         . qq|&formation=NULL_OR_EMPTY"><i>unknown</i></a>|;
            }
            $html .=  "$coll_link ($c_formations{$fm}), ";
        }
        $html =~ s/, $//g;
        $html .= "</p>\n";
        print $html;
    }

    if ($is_formation) {
        my $html = "<p><b>Members in the $name Formation:</b> ";
        foreach my $mbr_lc (sort(keys(%c_members))) {
            $cnt = $c_members{$mbr_lc};
            $coll_link = "";
            my $mbr = ($member_uc{$mbr_lc}) ? $member_uc{$mbr_lc} : $mbr_lc;
            if ($mbr) {
                $coll_link =  qq|<a href="bridge.pl?action=displayCollResults&formation=|
                           . uri_escape($q->param('group_formation_member'));
                $coll_link .= "&geological_group=".uri_escape($q->param('geological_group')) if $q->param('geological_group');
                $coll_link .= qq|&member=|.uri_escape($mbr).qq|">$mbr</a>|;
            } else {
                $coll_link =  qq|<a href="bridge.pl?action=displayCollResults&formation=|
                           . uri_escape($q->param('group_formation_member'));
                $coll_link .= "&geological_group=".uri_escape($q->param('geological_group')) if $q->param('geological_group');
                $coll_link .= qq|&member=NULL_OR_EMPTY"><i>unknown</i></a>|;
            } 
            $html .= "$coll_link ($c_members{$mbr_lc}), ";
        }
        $html =~ s/, $//g;
        $html .= "</p>\n";
        print $html;
    } 

    # Display lithologies present
    my @lith_list = $hbo->getList('lithology1');
    $html = "<p><b>Lithologies:</b> ";
    if (%lith_count) {
        foreach my $lithology (@lith_list) {
            if ($lith_count{$lithology}) {
                $cnt = $lith_count{$lithology};
                $coll_link = qq|<a href="bridge.pl?action=displayCollResults| 
                            . "&group_formation_member=".uri_escape($q->param('group_formation_member'));
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
    my @env_list = $hbo->getList('environment',1);
    $html = "<p><b>Paleoenvironments:</b> ";
    if (%environment_count) {
        foreach my $environment (@env_list) {
            if ($environment_count{$environment}) {
                $coll_link = qq|<a href="bridge.pl?action=displayCollResults| 
                              . "&group_formation_member=".uri_escape($q->param('group_formation_member'));
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
    print TaxonInfo::doCollections($q->url(), $q, $dbt, $dbh, '', '', "for_strata_module");

    print "<p>&nbsp;</p>";
    print "</div>";

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

    my %p_formations = (); my %p_groups = (); my %gp_groups = ();

    foreach my $row (@{$dataRows}) {
        if ($q->param('formation')) {
            if ($q->param("formation") eq "NULL_OR_EMPTY") {
                next if ($row->{'formation'});
            } else {
                next unless (lc($q->param('formation')) eq lc($row->{'formation'}));
            }
        } 
        if ($q->param('geological_group')) {
            if ($q->param("geological_group") eq "NULL_OR_EMPTY") {
                next if ($row->{'geological_group'});
            } else {
                next unless (lc($q->param('geological_group')) eq lc($row->{'geological_group'}));
            }
        }    
        # group hierarchy data
        # the p_* arrays denote parents, the gp_* array lists/counts grandparents and are used for
        #   checking for conflicts.  i.e. a member that belongs to two different formations
        #   so if theres no conflicts the p_* and g_* should have only 1 non-null element in them
        if (lc($q->param('group_formation_member')) eq lc($row->{'formation'})) {
            $p_groups{$row->{'geological_group'}} += 1;
        }
        if (lc($q->param('group_formation_member')) eq lc($row->{'member'})) {
            $p_formations{$row->{'formation'}} += 1;
            $gp_groups{$row->{'formation'}}{$row->{'geological_group'}} += 1;
        }
    }
    main::dbg("p_form: <pre>" . Dumper(%p_formations) . "</pre>");
    main::dbg("p_grp: <pre>" . Dumper(%p_groups) . "</pre>");
    main::dbg("gp_grp: <pre>" . Dumper(%gp_groups) . "</pre>");

    my ($p_formation_cnt, $p_group_cnt, $conflict_found);
    $p_formation_cnt = 0; $p_group_cnt = 0;
    #if (!$q->param('geological_group')) {
        foreach my $fm (keys %p_formations) {
            $p_formation_cnt += 1 if ($fm);
        }
    #}
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
sub displayStrataChoice {
    my $q = shift; 
    my $conflict_reason = shift;
    my $dataRows = shift;
    my %formation_links = ();
    my %group_links = ();

    foreach my $row (@{$dataRows}) {
        if ($q->param('formation')) {
            next unless (lc($q->param('formation')) eq lc($row->{'formation'}));
        }    
        if ($q->param('geological_group')) {
            next unless (lc($q->param('geological_group')) eq lc($row->{'geological_group'}));
        }    
        if (lc($q->param('group_formation_member')) eq lc($row->{'formation'})) {
            $group_links{$row->{'geological_group'}} += 1 if ($row->{'geological_group'});
        }    
        if (lc($q->param('group_formation_member')) eq lc($row->{'member'})) {
            $formation_links{$row->{'formation'}} += 1 if ($row->{'formation'});
        }
    }    

    main::dbg("In display strata choice for reason: $conflict_reason");
    print "<center>";
    my $count = 0;
    if ($conflict_reason eq "different groups") {
        print "The ".$q->param('group_formation_member')." formation belongs to multiple groups.  Please select the one you want: <p>";
        foreach my $grp (keys %group_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"bridge.pl?action=displayStrata"
                . "&geological_group=".uri_escape($grp)
                . "&group_formation_member=".uri_escape($q->param('group_formation_member'))."\">$grp</a></b>";
        }          
        print "</p>";
    } elsif ($conflict_reason eq "different formations") {
        print "The ".$q->param('group_formation_member')." member belongs to multiple formations.  Please select the one you want: <p>";
        foreach my $fm (sort keys %formation_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"bridge.pl?action=displayStrata"
                . "&formation=".uri_escape($fm)
                . "&group_formation_member=".uri_escape($q->param('group_formation_member'))."\">$fm</a></b> ";
        }          
        print "</p>";
    } elsif ($conflict_reason eq "different lines") {
        print "The term ".$q->param('group_formation_member')." is ambiguous and belongs to multiple formations or groups.  Please select the one you want: <p>";
        foreach my $fm (sort keys %formation_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"bridge.pl?action=displayStrata"
                . "&formation=".uri_escape($fm)
                . "&group_formation_member=".uri_escape($q->param('group_formation_member'))."\">$fm (formation)</a></b> ";
        }          
        foreach my $grp (sort keys %group_links) {
            print " - " if ($count++) != 0;
            print "<b><a href=\"bridge.pl?action=displayStrata"
                . "&geological_group=".uri_escape($grp)
                . "&group_formation_member=".uri_escape($q->param('group_formation_member'))."\">$grp (group)</a></b><br> ";
        }          
        print "</p>";
    }
    print "</center>";

    print "<p>&nbsp;</p>";
}

#
# Search strata stuff
#
# PS 02/04/2005
sub displaySearchStrataForm {
    my ($q,$s,$dbt,$hbo) = @_;
    my $dbh = $dbt->dbh;
   
    my $vars = $q->Vars();
    $vars->{'enterer_me'} = $s->get("enterer_reversed");
    $vars->{'page_title'} = "Stratigraphic unit search form";
    $vars->{'action'} = "displaySearchStrataResults";
    $vars->{'submit'} = "Search strata";
    # Show the "search collections" form

    # Set the Enterer
    main::printIntervalsJava(1);
    print main::makeAuthEntJavaScript();
    print $hbo->populateHTML('search_collections_form',$vars)
}
   
#
# Search strata stuff
#
# Displays a search page modeled after the collections search to search for local/regional sections
# PS 02/04/2005
sub displaySearchStrataResults {
    my ($q,$s,$dbt,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $limit = $q->param('limit') || 30;
    $limit = $limit*2; # two columns
    my $rowOffset = $q->param('rowOffset') || 0;

    # Build the SQL

    my $fields = ['max_interval_no','min_interval_no','state','country','localbed','localsection','localbedunit','regionalbed','regionalsection','regionalbedunit','geological_group','formation','member'];
    my %options = $q->Vars();
    $options{'permission_type'} = 'read';
    $options{'limit'} = 10000000;
    $options{'calling_script'} = 'Confidence';
#    $options{'lithologies'} = $options{'lithology1'}; delete $options{'lithology1'};
#    $options{'lithadjs'} = $options{'lithadj'}; delete $options{'lithadj'}; 
    if (!$options{'group_formation_member'}) {
        $options{'group_formation_member'} = 'NOT_NULL_OR_EMPTY';
    }
    my ($dataRows,$ofRows) = main::processCollectionsSearch($dbt,\%options,$fields);
    # Schwartzian tranform to be able to sort case insensitively and without quotes
    my @dataRows = 
        map {$_->[0]}
        sort {$a->[1] cmp $b->[1]}
        map {[$_,eval{my $j = lc($_->{'geological_group'}.$_->{'formation'});if ($j =~ s/^(["'\?\s]+)//) {$j .= $1;};$j}]}
        @$dataRows;

    # get the enterer's preferences (needed to determine the number
    # of displayed blanks) JA 1.8.02

    my $t = new TimeLookup($dbt);
    my @period_order = $t->getScaleOrder('69');
    # Convert max_interval_no to a period like 'Quaternary'
    my $int2period = $t->getScaleMapping('69','names');

    # We need to group the collections here in the code rather than SQL so that
    # we can get a list of max_interval_nos.  There should generaly be only 1 country.
    my @tableRows = ();
    my ($last_group,$last_formation);
    if (@dataRows) {
        $last_group= $dataRows[0]->{'geological_group'};
        $last_formation = $dataRows[0]->{'formation'};
        my (%period_list,%country_list,%member_list);
        for(my $i=0;$i<scalar(@dataRows)+1;$i++) {
            # The +1 is important, go one over so we print the last row correctly
            my $row = $dataRows[$i];
            if ($i == scalar(@dataRows) ||
                $last_group ne $row->{'geological_group'} ||
                $last_formation ne $row->{'formation'}) {
                my ($time_str, $place_str);
                my $link;
                if ($last_group) { 
                    $link .= "<a href=\"bridge.pl?action=displayStrata"
                           . "&geological_group=".uri_escape($last_group)
                           . "&group_formation_member=".uri_escape($last_group)
                           . "\">$last_group</a>";
                }
                if ($last_group && $last_formation) { 
                    $link .= "/";
                }
                if ($last_formation) {
                    $link .= "<a href=\"bridge.pl?action=displayStrata"
                           . "&geological_group=".uri_escape($last_group)
                           . "&formation=".uri_escape($last_formation)
                           . "&group_formation_member=".uri_escape($last_formation)
                           . "\">$last_formation</a>";
                }
                if (!$last_group && !$last_formation) {
                    $link .= "<i>unknown</i>";
                }
                # Tack on members
                $link .= "<small> - ";
                foreach my $member (sort values %member_list) {
                    $link .= "<a href=\"bridge.pl?action=displayStrata"
                           . "&geological_group=".uri_escape($last_group)
                           . "&formation=".uri_escape($last_formation)
                           . "&member=".uri_escape($member)
                           . "&group_formation_member=".uri_escape($member)
                           . "\">$member</a>, ";
                }
                $link =~ s/, $//;
                $link .= " - " if ($link !~ /-\s*$/);
               
                # Tack on period
                foreach my $period (@period_order) {
                    $link .= $period.", " if ($period_list{$period});
                }
                $link =~ s/, $//;
                $link .= " - " if ($link !~ /-\s*$/);

                # Tack on country
                foreach my $country (sort keys %country_list) {
                    $link .= $country.", ";
                }
                $link =~ s/, $//;
                $link .= "</small>";

                push @tableRows, $link;
                %period_list = ();
                %country_list = ();
                %member_list = ();
            }
            # We go over by one in the count, so don't set these for the last $i
            unless ($i == scalar(@dataRows)) {
                $country_list{$row->{'country'}} = 1;
                $period_list{$int2period->{$row->{'max_interval_no'}}} = 1;
                $member_list{lc($row->{'member'})} = $row->{'member'} if ($row->{'member'});
                $last_group = $row->{'geological_group'};
                $last_formation = $row->{'formation'};
            }
        }
    }

    my $ofRows = scalar(@tableRows);
    if ($ofRows > 1 || ($ofRows == 1 && !$last_formation && !$last_group)) {
        # Display header link that says which collections we're currently viewing
        print "<center>";
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
        print "</center>\n";
        print "<br>\n";
        print "<table width='100%' border=0 cellpadding=4 cellspacing=0>\n";

        # print columns header
        print '<tr><th align=left nowrap>Group/formation name</th>';
        if ($rowOffset + $limit/2 < $ofRows) { 
            print '<th align=left nowrap>Group/formation name</th>';
        }    
        print '</tr>';
   
        # print each of the rows generated above
        for(my $i=$rowOffset;$i<$ofRows && $i < $rowOffset+$limit/2;$i++) {
            # should it be a dark row, or a light row?  Alternate them...
            if ( $i % 2 == 0 ) {
                print "<tr class=\"darkList\">";
            } else {
                print "<tr>";
            }
            print "<td>$tableRows[$i]</td>";
            if ($i+$limit/2 < $ofRows) {
                print "<td>".$tableRows[$i+$limit/2]."</td>";
            } else {
                print "<td>&nbsp;</td>";
            }
            print "</tr>\n";
        }
 
        print "</table>\n";
    } elsif ($ofRows == 1 ) { # if only one row to display, cut to next page in chain
        print "<center>\n<h3>Your search produced exactly one match</h3></center>";
        my $highest = ($last_group) ? $last_group : $last_formation;
        my $my_q = new CGI({
                         'group_formation_member'=>$highest,
                         'geological_group'=>$last_group,
                         'formation'=>$last_formation,
                         'member'=>''});
        displayStrata($my_q,$s,$dbt,$hbo);
        return;
    } else {
        print "<center>\n<h3>Your search produced no matches</h3>";
        print "<p>Please try again with fewer search terms.</p>\n</center>\n";
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
            if ($q2->param($param_key) ne "" || $param_key eq 'section_name') {
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
        print "<a href='bridge.pl?$getString'><b>Get $numLeft units</b></a> - ";
    }
    print "<a href='bridge.pl?action=displaySearchSectionForm'><b>Do another search</b></a>";

    print "</center></p>";
    # End footer links
}
   


1;
