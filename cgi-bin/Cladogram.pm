# JA 26-31.7, 6.8.07
package Cladogram;

use GD;
use Reference;
use Data::Dumper;
use Debug qw(dbg);
use Constants qw($DATA_DIR $HTML_DIR $WRITE_URL $READ_URL);

use strict;

my $FONT = "$DATA_DIR/fonts/Vera.ttf";
my $FONT2 = "$DATA_DIR/fonts/VeraBd.ttf";


sub displayCladogramChoiceForm {
    my ($dbt,$q,$s,$hbo) = @_;

    my $taxon_no = $q->param('taxon_no');
    my $session_ref = $s->get('reference_no');
    return unless $taxon_no =~ /^\d+$/;

    my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no});

	# if there are no matches, call displayCladeSearchForm and insert
	#  a message saying "No matches were found, please search again"

    my $sql = "SELECT c.cladogram_no, a.taxon_name, c.reference_no FROM cladograms c, authorities a WHERE a.taxon_no=c.taxon_no ";
    my @where = ();
    if ($taxon_no) {
        push @where, "c.taxon_no IN ($taxon_no)";
    }
    if (@where) {
        $sql .= " AND ".join(", ",@where);
    } #else {
      #  displayCladeSearchForm($dbt,$q,$s,$hbo,"No search terms were entered. Please search again");
      #  return;
    #}

    my @results = @{$dbt->getData($sql)};
    if (@results == 0) {
        $q->param('cladogram_no'=>'-1');
        displayCladogramForm($dbt,$q,$s,$hbo);
        return;
    } else {
        # If we have one exact match (matches reference and taxon), use that
        foreach my $row (@results) {
            if ($row->{'reference_no'} == $session_ref && $row->{'taxon_no'} == $taxon_no) {
                $q->param('cladogram_no'=>$row->{'cladogram_no'});
                displayCladogramForm($dbt,$q,$s,$hbo);
                return;
            }
        }

        # Else if we have more than one match, or 1 match and its from a diff. reference
        # Gives the users a choice to edit an old cladogram or enter a new one
        my $blank_rows = $q->param('blank_rows');
        my $html = qq|<div align="center">|;
        $html .= qq|<p class="pageTitle">Edit or enter a new cladogram</p>|;
        $html .= qq|<table><tr><td>|;
        $html .= qq|<div class="displayPanel" style="padding: 1em;"><div align="left"><ul style="padding-left: 1em;">|;
        foreach my $row (@results) {
            my $short_ref = Reference::formatShortRef($dbt,$row->{'reference_no'});
            $html .= qq|<li style="padding-bottom: 0.75em;"><a href="$WRITE_URL?action=displayCladogramForm&blank_rows=$blank_rows&cladogram_no=$row->{cladogram_no}">$row->{taxon_name}</a>, $short_ref</li>|;
        }
        $html .= qq|<li><a href="$WRITE_URL?action=displayCladogramForm&blank_rows=$blank_rows&cladogram_no=-1&taxon_no=$taxon_no">Create a new cladogram for $taxon->{taxon_name}</a></li>|;
        $html .= qq|</ul></div></div>|;
        $html .= qq|</td></tr></table></div>|;

        print $html;
    }
}

sub displayCladogramForm {
    my ($dbt,$q,$s,$hbo,$errors) = @_;

    my $taxon_no = $q->param('taxon_no');
    my $cladogram_no = $q->param('cladogram_no');
    my $session_ref = $s->get('reference_no');

    if ($cladogram_no !~ /^-1|\d+$/) {
        print("Cladogram no not set");
        return;
    }
    if ($taxon_no !~ /^\d+$/ && $cladogram_no == -1) {
        print ("Taxon no not set");
        return;
    }

    my $isNewEntry = ($cladogram_no > 0) ? 0 : 1;
    my $isResubmit = (ref $errors && @$errors) ? 1 : 0;

    my $db_row;
    if (!$isNewEntry) {
        my $sql = "SELECT * FROM cladograms WHERE cladogram_no=$cladogram_no";
        $db_row = ${$dbt->getData($sql)}[0];
        $taxon_no = $db_row->{taxon_no};
    }

    my $reference_no = ($isNewEntry) ? $session_ref : $db_row->{reference_no};

    if ($isNewEntry && !$session_ref) {
        $s->enqueue("action=displayCladogramForm&cladogram_no=-1&taxon_no=$taxon_no");
        main::displaySearchRefs("Please choose a reference before entering a new cladogram",1);
        return;
    }

    my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no});
    my $vars = {};
    my @clades = ();
    my @bootstraps = ();
    my @outgroups = ();
    if ($isResubmit) {
        my %vars_copy = $q->Vars();
        my @clade_count = grep {/^clade\d+$/} $q->param;
        my $num_rows = scalar(@clade_count);
        foreach my $i (0 .. ($num_rows-1)) {
            push @outgroups, $q->param('outgroup'.$i) || "";
            push @bootstraps,$q->param('bootstrap'.$i) || "";
            push @clades,$q->param('clade'.$i) || "";
        }
        $vars = \%vars_copy;
        $vars->{'error_message'} = "<div align=\"center\">".Debug::printErrors($errors)."</div><br>";
    } elsif (!$isNewEntry) {
        my $sql = "SELECT * FROM cladograms WHERE cladogram_no=$cladogram_no";
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            die("Could not read cladogram $cladogram_no");
        }
        $vars = $row;
    
        my $sql = "SELECT cn.*, a.taxon_name FROM cladogram_nodes cn LEFT JOIN authorities a ON cn.taxon_no=a.taxon_no WHERE cladogram_no=$cladogram_no ORDER BY entry_order";
        my @rows = @{$dbt->getData($sql)};
        my %parents = ();
        my %lookup_by_node_no = ();
        my %seen_parent = ();
        my @ordered_parents;
        my %children = ();
        foreach my $row (@rows) {
            $parents{$row->{'node_no'}} = $row->{'parent_no'};
            $lookup_by_node_no{$row->{'node_no'}} = $row;
            push @{$children{$row->{'parent_no'}}}, $row;
            if (!$seen_parent{$row->{'parent_no'}}) {
                push @ordered_parents, $row->{'parent_no'};
                $seen_parent{$row->{'parent_no'}} = 1;
            } 

        }

        my $i = 0;
        my $anon_count = 1;
        foreach my $parent_no (@ordered_parents) {
            my $children_ref = $children{$parent_no};
            my $parent_name;
            if ($lookup_by_node_no{$parent_no}) {
                my $parent = $lookup_by_node_no{$parent_no};
                if (!$parent->{'taxon_name'}) {
                    $parent->{'taxon_name'} = $anon_count;
                    $anon_count++;
                }
                $parent_name = $parent->{'taxon_name'};
            } else {
                next;
            }
            my @child_names = ();
            foreach my $child (@$children_ref) {
                if (!$child->{'taxon_name'}) {
                    $child->{'taxon_name'} = $anon_count;
                    $anon_count++;
                }
                if ($child->{'plesiomorphic'}) {
                    $child->{'taxon_name'} .= "*";
                }
                push @child_names, $child->{'taxon_name'};
            }
            $clades[$i] = "$parent_name = ".join(" ",@child_names);
            $bootstraps[$i] = $lookup_by_node_no{$parent_no}->{'bootstrap'};
            $outgroups[$i] = $lookup_by_node_no{$parent_no}->{'outgroup'};
            $i++;
        }
    }

    my $short_ref = Reference::formatShortRef($dbt,$reference_no,'link_id'=>1);

    $vars->{'taxon_no'}     = $taxon_no;
    $vars->{'taxon_name'}   = $taxon->{'taxon_name'};
    $vars->{'short_reference'} = $short_ref;
    $vars->{'cladogram_no'} = $cladogram_no;
    $vars->{'reference_no'} = $reference_no;


    my $blank_rows = $q->param('blank_rows') || 10;

    my $num_rows = scalar(@clades) < $blank_rows? $blank_rows : scalar(@clades);

    my $table_rows = "";
    for(my $i=0;$i<$num_rows;$i++) {
        my $clade = $clades[$i] || "".($i+1)." = ";
        my $bootstrap = $bootstraps[$i];
        my $outgroup_checked = $outgroups[$i] ? "CHECKED" : "";
        $table_rows .= qq|<tr>|
                     . qq|<td><input type="checkbox" name="outgroup$i" id="outgroup$i" $outgroup_checked value="YES">yes</td>|
                     . qq|<td><input type="text" size="4" name="bootstrap$i" id="bootstrap$i" value="$bootstrap"></td>|
                     . qq|<td><input type="text" size="40" name="clade$i" id="clade$i" value="$clade"></td>|
                     . qq|</tr>|;
    }

    $vars->{'table_rows'} = $table_rows;

    if ($isNewEntry) {
        $vars->{'page_title'} = "Cladogram entry form";
    } else {
        $vars->{'page_title'} = "Cladogram edit form";
    }

    print $hbo->populateHTML('enter_cladogram_form',$vars);
}

sub submitCladogramForm {
    my ($dbt,$q,$s,$hbo) = @_;

    my $taxon_no = $q->param('taxon_no');
    my $cladogram_no = $q->param('cladogram_no');

    return unless $taxon_no =~ /^\d+$/;
    return unless $cladogram_no =~ /^-1|\d+$/;

    my $isNewEntry = ($cladogram_no > 0) ? 0 : 1;

    my %cladogram = $q->Vars;

    my %homonym_resolve = parseHomonymResolveTable($q);

    my (@outgroups,@bootstraps,@clades);
    my @clade_count = grep {/^clade\d+$/} $q->param;
    my $num_rows = scalar(@clade_count);
    foreach my $i (0 .. ($num_rows-1)) {
        push @outgroups, $q->param('outgroup'.$i) || "";
        push @bootstraps,$q->param('bootstrap'.$i) || "";
        push @clades,$q->param('clade'.$i) || "";
    }
    
    my @errors = ();#("Dummy Error");
    my @homonym_errors = ();
    my @missing_errors = ();

	# source must be entered
    my $reference_no = $q->param('reference_no');
    if ($reference_no !~ /^\d+$/) {
        push @errors, "No reference_no set";
    }

	# first do some minimal sanity checking in case the JavaScript
	#  didn't catch something
    my %node_lookup = ();
    my %seen_parent = ();
    my %seen_child = ();
    my $seen_child_order = 1;
    my $seen_parent_order = 1;
    for (my $i = 0;$i< @clades;$i++) {
        my $clade = $clades[$i];
        my $bootstrap = $bootstraps[$i];
        my $outgroup = $outgroups[$i];
#        my $node_no = $node_nos[$i];

        next if ($clade =~ /^\d+\s*=\s*$/ || $clade =~ /^\s*$/);

        my ($parent_no,$parent_name,$child_nos,$child_names,$plesiomorphics) = parseClade($dbt,$clade,\%homonym_resolve,\@errors,\@missing_errors,\@homonym_errors);
        if (!$node_lookup{$parent_name}) {
            $node_lookup{$parent_name} = {'taxon_name'=>$parent_name,'children'=>[]};
        }
        my $parent = $node_lookup{$parent_name};
        $parent->{'bootstrap'} = $bootstrap;
        $parent->{'outgroup'} = $outgroup;
        $parent->{'taxon_no'} = $parent_no unless $parent->{'taxon_no'};

        if (@$child_nos < 2) {
            push @errors, "$parent_name has less than two things assigned to it";
        }

#        print Dumper($parent_no)."<br>";
#        print Dumper($parent_name)."<br>";
#        print Dumper($child_nos)."<br>";
#        print Dumper($child_names)."<br>";
#        print Dumper($plesiomorphics)."<br>";

        # check for duplicate clade names
        if ($seen_parent{$parent_name}) {
            push @errors, "$parent_name appears twice as a clade";
        } else {
            $seen_parent{$parent_name} = $seen_parent_order;
            $seen_parent_order++;
        }
        
        for (my $j=0;$j<@$child_nos;$j++) {
            my $child_no = $child_nos->[$j];
            my $child_name = $child_names->[$j];
            
            my $plesiomorphic = $plesiomorphics->[$j];
            if (!$node_lookup{$child_name}) {
                $node_lookup{$child_name} = {'taxon_name'=>$child_name,'children'=>[]};
            }
       
            my $child = $node_lookup{$child_name}; 
            $child->{'plesiomorphic'} = $plesiomorphic;
            $child->{'taxon_no'} = $child_no;
            push @{$parent->{'children'}},$child;
        	# check for taxa of any kind assigned to the same clade twice
        	# check for taxa of any kind assigned to more than one clade

            if ($seen_child{$child_name}) {
                push @errors, "$child_name was assigned twice";
            } else {
                $seen_child{$child_name} = $seen_child_order;
                $seen_child_order++;
            }
        }
    }

    my @roots;
    foreach my $parent_name (keys %seen_parent) {
        if (! $seen_child{$parent_name}) {
            push @roots, $parent_name;
            dbg("<pre>".Dumper($node_lookup{$parent_name})."</pre>");
        }
    }
    # check to see that exactly one clade is assigned to nothing
    if (@roots > 1) {
        push @errors, "Multiple clades are not assigned anywhere: ".join(", ", @roots);
    } elsif (@roots < 1) {
        push @errors, "There is no root clade";
    }


	# bootstrap values should all be integers between 1 and 100
    for (my $i = 0;$i< @clades;$i++) {
        my $bootstrap = $bootstraps[$i];
        my $bootstrap_error = 0;
        if ($bootstrap) {
            if ($bootstrap !~ /^\d+$/) {
                $bootstrap_error = 1;
            } elsif ($bootstrap < 1 || $bootstrap > 100) {
                $bootstrap_error = 1;
            }
        }
        if ($bootstrap_error) {
            push @errors, "Bootstrap values must be integers between 1 and 100";
        }
    }

    if (@homonym_errors) {
        my $text;
        if (@homonym_errors > 1) {
            $text .= "Some names were ambiguous";
        } else {
            $text .= "The following name was ambiguous";
        }
        $text .= ". Please choose the version you want:<br>";
        foreach my $homonym_select (@homonym_errors) {
            $text .= "$homonym_select<br>";
        }
        push @errors, $text;
    }

    if (@missing_errors) {
        my $text;
        if (@missing_errors > 1) {
            $text .= "Please add authority data for the following taxa: ";
        } else {
            $text .= "Please add authority data for "; 
        }
        foreach my $name (@missing_errors) {
            $text .= qq|<a target="_NEW" href="$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=$name">$name</a>, |;
        }
        $text =~ s/, $//;
        push @errors, $text;
    }

    if (@errors) {
        displayCladogramForm($dbt,$q,$s,$hbo,\@errors);
        return;
    } 


    my $result;
    if ($isNewEntry) {
        ($result,$cladogram_no) = $dbt->insertRecord($s,'cladograms',\%cladogram);
        if (!$result) {
            die("Error inserting cladogram into database");
        }
    } else {
        $result = $dbt->updateRecord($s,'cladograms','cladogram_no',$cladogram_no,\%cladogram);
        if (!$result) {
            die("Error updating cladogram in database");
        }
    }


    my %db_nodes = ();
    if (!$isNewEntry) {
        my $sql = "SELECT cn.*,a.taxon_name FROM cladogram_nodes cn LEFT JOIN authorities a ON cn.taxon_no=a.taxon_no WHERE cn.cladogram_no=$cladogram_no ORDER BY entry_order";
        my @results = @{$dbt->getData($sql)};
        my $anon_count = 1;
        foreach my $row (@results) {
            if (!$row->{'taxon_name'}) {
                $row->{'taxon_name'} = $anon_count;
                $anon_count++;
            }
            $db_nodes{$row->{'taxon_name'}} = $row;
        }
    }


    # Insert the root node first then all its children, etc. 
    # we put the node_no just inserted on the q along with a child, so the child
    # knows which node_no to use as its parent_no
    my @q = ();
    push @q, [0,$node_lookup{$roots[0]}];
    while (@q) {
        my $next = pop @q;
        my ($parent_no,$node) = @$next;

        $node->{'cladogram_no'} = $cladogram_no;
        $node->{'parent_no'} = $parent_no;
        $node->{'entry_order'} = $seen_child{$node->{taxon_name}} || 0;
        if ($node->{'taxon_no'} !~ /^\d+$/) {
            $node->{'taxon_no'} = 0;
        }

        my ($result,$node_no);
        if ($db_nodes{$node->{'taxon_name'}}) {
            my $db_row = $db_nodes{$node->{'taxon_name'}};
            $db_row->{'seen_db_row'} = 1;
            $node_no = $db_row->{'node_no'};
            $node->{'node_no'} = $node_no;
            $result = $dbt->updateRecord($s,'cladogram_nodes','node_no',$node_no,$node);
        } else {
            ($result,$node_no) = $dbt->insertRecord($s,'cladogram_nodes',$node);
        }

        foreach my $next_node (@{$node->{'children'}}) {
            push @q, [$node_no,$next_node];
        }
    }

    if (%db_nodes) {
        while (my ($k,$v) = each %db_nodes) {
            if (! $v->{'seen_db_row'}) {
                $dbt->deleteRecord($s,'cladogram_nodes','node_no',$v->{'node_no'});
            }
        }
    }

    my ($pngname,$caption,$taxon_name) = drawCladogram($dbt,$cladogram_no,1);

    my $verb = ($isNewEntry) ? "entered" : "edited";
    print "<div align=\"center\">";
    print "<p class=\"pageTitle\">The cladogram for $taxon_name was successfully $verb</p>";
    print "<br>";

    print "<img src=\"/public/cladograms/$pngname\"><br>";
    print "$caption";
    print "<br>";
    print "<a href=\"$WRITE_URL?action=displayCladogramForm&cladogram_no=$cladogram_no\">Edit this cladogram</a> - ";
    print "<a href=\"$WRITE_URL?action=displayCladeSearchForm\">Enter another cladogram</a>";

    print "</div>";


	# call displayCladeSearchForm and populate it with a congratulations
	#  message ("The cladogram for ... was successfully entered"), plus
	# "tree: ..." immediately following, with an image generated
	#  by formatTreeData
}

sub parseHomonymResolveTable {
    my $q = shift;

    my @params = $q->param;
    my %table = ();
    foreach my $p (@params) {
        if ($p =~ /^taxon_no_(.*)$/) {
            my $taxon_name = $1;
            my $taxon_no = $q->param($p);
            $table{$taxon_name} = $taxon_no;
        }
    }
    return %table;
}

# hits the cladograms and nodes table and returns a string in NHX (similar
#  to NEXUS) format that describes the topology of the cladogram
sub _formatTreeData	{
    my ($rows) = @_;
    my @rows = @$rows;
    
    my %node_lookup = ();
    my $anon_count = 1;
    foreach my $row (@rows) {
        $row->{'children'} = [];
        if (!$row->{'taxon_name'}) {
            $row->{'taxon_name'} = $anon_count;
            $anon_count++;
        }
        $node_lookup{$row->{'node_no'}} = $row;
    }
    my $root;
    foreach my $row (@rows) {
        if ($row->{'parent_no'}) {
            my $parent = $node_lookup{$row->{'parent_no'}};
            push @{$parent->{'children'}},$row;
        } else {
            $root = $row;
        }
    }

    my $string = formatTreeString($root);
    return $string;
}

# format the data like this:
# taxon_name = ((A:1,B:0)C:1,D:1));
# where A = terminal taxon name, D = parent node name, and the
#  numbers indicate branch lengths: 1 = default, 0 = value if
#  plesiomorphic = YES
# if bootstrap values like 98 exist, add them like this:
#  ((A:1,B:0)D:1[&&NSX:B=98],C:1));
sub formatTreeString {
    my $node = shift;
    
    my $formatted_text = '';
    if ($node->{'children'} && @{$node->{'children'}}) {
        my @string_bits = ();
        foreach my $child (@{$node->{'children'}}) {
            push @string_bits, formatTreeString($child);
        }
        $formatted_text .= "(".join(",",@string_bits).")";
    } 
    
    my $is_plesiomorphic = ($node->{'plesiomorphic'}) ? "1" : "0";
    my $formatted_name = $node->{'taxon_name'};
    $formatted_name =~ s/ /_/g;
    $formatted_text .= $formatted_name .":".$is_plesiomorphic;
    if ($node->{'bootstrap'}) {
        $formatted_text .= "[&&NSX:B=$node->{bootstrap}]";
    }
    return $formatted_text;
}
    # cladograms table should include following fields:

    # authorizer_no
    # enterer_no
    # modifier_no
    # cladogram_no
    # reference_no
    # taxon_no
    # pages text
    # figures text
    # source enum('','text','illustration','supertree','most parsimonious tree','consensus tree','likelihood tree')
    # comments (user entered)
    # created
    # modified
    # upload enum('','YES');

    # nodes table fields:

    # node_no
    # taxon_no
    # parent_no
    # outgroup enum('','YES');
    # plesiomorphic enum('','YES');
    # bootstrap int default null

    # parent_no points to another node_no in the same cladogram, and is zero
    #  for the root
    # bootstrap is a percentage between 0 and 100 with no fractional values
    # created/modified/upload and authorizer_no etc. excluded because these are
    #  all (more or less) properties of entire cladograms

sub parseClade {
	# extract apparent taxon names and check their formatting
    my ($dbt,$clade,$homonym_resolve,$errors,$missing_errors,$homonym_errors) = @_;

    # Split on assignment separator first
    my ($parent_name,$children) = split(/\s*=\s*/,$clade,2);

	# replace all apparent separators with commas; these include
	#  . : ; - + &
    $children =~ s/[.:;-=& ]+/,/g;

	# also collapse down all spaces and replace them with commas, but
	#  do not do this in the [A-Z][a-z]+ [a-z] case, because this should
	#  indicate a genus-species combination
    $children =~ s/([A-Z][a-z*]+),([a-z*]+)/$1 $2/g;
    my @child_names = split(/,/,$children);
    my @plesiomorph = ();

	# all such names should either be integers not starting with zero,
	#  or taxon names in the form "Equidae" or "Equus caballus"
	# a trailing * after a proper taxon name is fine, but if this is seen,
	#  strip it and set plesiomorphic = YES for this taxon
	# add errors saying "'Equidae' is misformatted" if needed
    my @child_nos;
    foreach my $name (@child_names) {
        if ($name =~ s/\*//) {
            push @plesiomorph, "YES";
        } else {
            push @plesiomorph, "";
        }

        my $taxon_no = "";
        if ($name =~ /^\d+$/) {
            $taxon_no = -1 * $name;
        } else {
            if ($name =~ /^[A-Z][a-z]+ [a-z]+$/ || $name =~ /^[A-Z][a-z]+$/) {
                my @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$name,'remove_rank_change'=>1},['*']);
                if (@taxa < 1) {
                    push @$missing_errors, $name;
                } elsif (@taxa > 1) {
                    if ($homonym_resolve->{$name}) {
                        $taxon_no = $homonym_resolve->{$name};
                    } else {
                        push @$homonym_errors, homonymChoice($dbt,\@taxa,$name);
                    }
                } else {
                    $taxon_no = $taxa[0]->{'taxon_no'};
                }
            } else {
                push @$errors, "$name is misformatted";
            }
        }
        push @child_nos, $taxon_no;
    }

    my $parent_no;
    if ($parent_name =~ /^\d+$/) {
        $parent_no = -1 * $parent_name;
    } else {
        my @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$parent_name,'remove_rank_change'=>1},['*']);
        if (@taxa < 1) {
            push @$missing_errors, $parent_name;
        } elsif (@taxa > 1) {
            if ($homonym_resolve->{$parent_name}) {
                $parent_no = $homonym_resolve->{$parent_name};
            } else {
                push @$homonym_errors, homonymChoice($dbt,\@taxa,$parent_name);
            }
        } else {
            $parent_no = $taxa[0]->{'taxon_no'};
        }
    }

    return ($parent_no,$parent_name,\@child_nos,\@child_names,\@plesiomorph);
}

sub homonymChoice {
    my ($dbt,$choices,$taxon_name,$selected_no) = @_;
    $taxon_name =~ s/ /_/g;
    my $html = "<select name=\"taxon_no_$taxon_name\">";
    foreach my $c (@$choices) {
        # have to format the authority data
        my $authority = Taxon::formatTaxon($dbt,$c);
        $html .= qq|<option value="$c->{taxon_no}"|;
        if ($c->{taxon_no} eq $selected_no) {
            $html .= " selected";
        }
        $html .= ">$authority</option>\n";
    }
    $html .= "</select>";
    return $html;
}

sub drawCladogram {
    my ($dbt,$cladogram_no,$force_redraw) = @_;

    return undef unless ($cladogram_no =~ /^\d+$/);

	my $pngname = "cladogram_$cladogram_no.png";
	my $cladogram_png = $HTML_DIR."/public/cladograms/$pngname";
    if (! -e $cladogram_png || $force_redraw) {
        generateCladogram($dbt,$cladogram_no);
    }
    my $sql = "SELECT c.taxon_no,c.caption,a.taxon_name FROM cladograms c LEFT JOIN authorities a ON c.taxon_no=a.taxon_no WHERE c.cladogram_no=$cladogram_no";
    my $row = ${$dbt->getData($sql)}[0];
    my $caption = $row->{'caption'};
    my $taxon_name = $row->{'taxon_name'};

    return ($pngname,$caption,$taxon_name);
}


sub generateCladogram	{
    my ($dbt,$cladogram_no) = @_;

    return undef unless $cladogram_no =~ /^\d+$/;

    my $sql = "SELECT c.* FROM cladograms c WHERE c.cladogram_no=$cladogram_no";
    my $cladogram = ${$dbt->getData($sql)}[0];

    $sql = "SELECT cn.*,a.taxon_name FROM cladogram_nodes cn LEFT JOIN authorities a ON cn.taxon_no=a.taxon_no WHERE cn.cladogram_no=$cladogram_no ORDER BY entry_order";
    my @rows = @{$dbt->getData($sql)};
    my @taxon_name;
    my @taxon_no;
    my @plesiomorphic;
    my @bootstrap;
    my @parent;
    my @nodes;
    my @is_terminal = ();
    my $index= 0;
    foreach my $row (@rows) {
        push @taxon_name, $row->{'taxon_name'} || "";
        push @taxon_no, $row->{'taxon_no'} || "";
        push @bootstrap, $row->{'bootstrap'} || "";
        push @plesiomorphic, $row->{'plesiomorphic'} || "";
        push @nodes,$row->{'node_no'};
        push @parent, $row->{'parent_no'};
        push @is_terminal, 1;
    }


    my %node_index = ();
    for(my $i=0;$i<@nodes;$i++) {
        $node_index{$nodes[$i]} = $i;
    }

    my $num_nodes = scalar(@nodes);

    my $focal_taxon_no = $cladogram->{'taxon_no'};
    
    #my @taxon = ("N. eurystyle","N. gidleyi","","Neohipparion leptode","","N. trampasense","","N. affine","Neohipparion","M. republicanus","Pseudhipparion","","","M. coloradense","","Pseudoparablastomeryx olcotti","Hipparionini","M. insignis","Equinae");
	#my @parent = (2,2,4,4,6,6,8,8,12,11,11,12,14,14,16,16,18,18,0);
	#my @plesiomorphic = ('','YES','','','','YES','','','','','','','','','','','','','');
	#my @bootstrap = ('','','99','','87','','100','','','','','35','100','','','','68','','');

#    print "TAXON: ".Dumper(\@taxon_name)."<BR>";
#    print "PARENT: ".Dumper(\@parent)."<BR>";
#    print "PL: ".Dumper(\@plesiomorphic)."<BR>";
#    print "BS: ".Dumper(\@bootstrap)."<BR>";
#    print "NODE: ".Dumper(\@nodes)."<BR>";

	my @depth;
	for my $i ( 0..$num_nodes-1 )	{
		$depth[$i] = 0;
	}

	# the depth of each node is one more than the number of resolved nodes
	#  it includes
	my %depth_to;
	for my $i ( 0..$#nodes )	{
		my $z = $node_index{$nodes[$i]};
		my $d = 1;
		while ( $parent[$z] > 0 )	{
			my $parent_index = $node_index{$parent[$z]};
			$depth_to{$i}{$parent_index} = $d;
			$depth[$parent_index]++;
			$d++;
			$z = $parent_index;
		}
	}

	# nodes with depth = 0 are terminals
	my $terminals = 0;
	my @terminal_no;
	my $maxdepth = 0;
	my @clade_no;
	my $clades;
	for my $i ( 0..$num_nodes-1 )	{
		if ( $depth[$i] == 0 )	{
			$terminals++;
			$terminal_no[$i] = $terminals;
		} else	{
			# this line squeezes the cladogram a bit for
			#  aesthetic reasons
			$depth[$i] = $depth[$i]**0.9;
			$clades++;
			$clade_no[$i] = $clades;
			if ( $depth[$i] > $maxdepth )	{
				$maxdepth = $depth[$i];
			}
		}
	}
	for my $i ( 0..$num_nodes-1 )	{
		if ( $clade_no[$i] )	{
			$clade_no[$i] = $clades - $clade_no[$i] + 1;
		}
	}

    # Reorder the terminals
    # the first terminal is still first
    # at each step, add the first terminal found that has the minimal
    #  distance to any of the ancestors of the last terminal added

    my @terminal_indices;
    my @clade_indices;
    for my $i ( 0..$num_nodes-1 )	{
        if ($terminal_no[$i] > 0) {
            push @terminal_indices, $i;
        } else {
            push @clade_indices, $i;
        }
    }
#    print Dumper(\%depth_to)."<br>";
    my $first_terminal = shift @terminal_indices;
    $terminal_no[$first_terminal] = 1;
    my $terminal_cnt = 2;
    my $last_terminal = $first_terminal;
    my @chosen;
    for my $i (@terminal_indices) {
        my $next_terminal = -1;
        my $min_dist = 9999;
        for my $j (@terminal_indices) {
            for my $h (@clade_indices) {
                if ($depth_to{$last_terminal}{$h} > 0 && $depth_to{$last_terminal}{$h} < $min_dist && $depth_to{$j}{$h} > 0 && $chosen[$j] == 0) {
                    $min_dist = $depth_to{$last_terminal}{$h};
                    $next_terminal = $j;
                }
            }
        }
        if ($next_terminal > 0) {
            $terminal_no[$next_terminal] = $terminal_cnt;
            $terminal_cnt++;
            $last_terminal = $next_terminal;
            $chosen[$next_terminal] = 1;
        }
    }
#    print Dumper(\@terminal_no)."<br>";
            
	# the vertical position of each internal node is the arithmetic
	#  mean of all its terminals' terminal numbers
	my @subterminals;
	my @sumterminalnos;
	my @height;

	for my $i ( 0..$num_nodes-1 )	{
		if ( $terminal_no[$i] > 0 )	{
			my $z = $i;
			while ( $parent[$z] > 0 )	{
				my $pi = $node_index{$parent[$z]};
				$subterminals[$pi]++;
				$sumterminalnos[$pi] += $terminal_no[$i];
				$z = $pi;
			}
		}
	}
	for my $i ( 0..$num_nodes-1 )	{
		if ( $subterminals[$i] > 0 )	{
			$height[$i] = $sumterminalnos[$i] / $subterminals[$i];
		} else	{
			$height[$i] = $terminal_no[$i];
		}
	}

	# the cladograms look "right" when the scaling numbers are equal
	#  because the lines branch from each other at 90 degrees
	my $height_scale = 24;
	my $width_scale = 12;
	my $maxletts = 1;
	foreach my $t ( @taxon_name ) {
		my $num_letts = length($t);
		if ($num_letts > $maxletts) {
			$maxletts = $num_letts;
		}
	}
    
    
	# the multiplier constant is specific to the font
	my $border = int( $maxletts * 8.25 );
	my $imgheight = $height_scale * ( $terminals + 1 );
	my $imgwidth = ( $width_scale * ( $maxdepth + 1 ) ) + $border + 10;
	my $im = GD::Image->new($imgwidth,$imgheight,1);
	
	my $unantialiased = $im->colorAllocate(-1,-1,-1);
	my $orangeunantialiased = $im->colorAllocate(-255,-127,-63);
	my $white = $im->colorAllocate(255,255,255);
	my $black = $im->colorAllocate(0,0,0);
	my $lightgray = $im->colorAllocate(193,193,193);

	# this line is said to work in the standard GD.pm manual, but does not
	#$im->transparent($white);
	$im->interlaced('true');
	# so we clear the image the hard way
	$im->filledRectangle(0,0,$imgwidth,$imgheight,$white);
	# also add a frame
	$im->rectangle(0,0,$imgwidth - 1,$imgheight - 1,$lightgray);

	# might want to mess with this sometime
	#$im->setThickness(1);
	$im->setAntiAliased($black);
	for my $i ( 0..$num_nodes-1 )	{
		my $pi = $node_index{$parent[$i]};
		if ( $terminal_no[$i] > 0 )	{
			# focal taxon's name is bold orange
			if ( $taxon_no[$i] == $focal_taxon_no )	{
				$im->stringFT($orangeunantialiased,$FONT2,10,0,$imgwidth - $border + 8,( $terminal_no[$i] * $height_scale ) + 5,$taxon_name[$i]);
			} else	{
				$im->stringFT($unantialiased,$FONT,10,0,$imgwidth - $border + 8,( $terminal_no[$i] * $height_scale ) + 5,$taxon_name[$i]);
			}
			$im->line($imgwidth - $border,$terminal_no[$i] * $height_scale,$imgwidth - $border - ( $depth[$pi] * $width_scale ),$height[$pi] * $height_scale,gdAntiAliased);
			# small circle indicates an automorphic
			#  (=  non-plesiomorphic) taxon
			if ( ! $plesiomorphic[$i] )	{
				$im->filledArc($imgwidth - $border,( $terminal_no[$i] * $height_scale ),5,5,0,360,gdAntiAliased);
			}

		}
		# connect internal nodes
		elsif ( $parent[$i] )	{
			my $nodex = $imgwidth - $border - ( $depth[$i] * $width_scale );
			my $nodey = $height[$i] * $height_scale;
			$im->line($nodex,$nodey,$imgwidth - $border - ( $depth[$pi] * $width_scale ),$height[$pi] * $height_scale,gdAntiAliased);
		}
	}

	# draw node numbers and write caption
	my $printednodes = 0;
	my $caption;
	# debugging line
	for my $i ( reverse 0..$num_nodes-1 )	{
		my $nodex = $imgwidth - $border - ( $depth[$i] * $width_scale );
		my $nodey = $height[$i] * $height_scale;
		if ( $terminal_no[$i] == 0 && $taxon_name[$i] ne "" )	{
			$printednodes++;
			if ( $taxon_no[$i] == $focal_taxon_no )	{
			    $caption .= "<span style=\"color: orange;\">$printednodes = $taxon_name[$i]</span>, ";
            } else {
			    $caption .= "$printednodes = $taxon_name[$i], ";
            }
			# tweaks specific to this font
			my $xoffset = 3;
			if ( $clade_no[$i] =~ /1$/ )	{
				$xoffset = 2;
			} elsif ( $clade_no[$i] =~ /4$/ )	{
				$xoffset = 4;
			}
			if ( $clade_no[$i] < 10 )	{
				$im->filledArc($nodex,$nodey,15,15,0,360,$white);
				# debugging line
				#$im->arc($nodex,$nodey,15,15,0,360,$black);
				$im->stringFT($unantialiased,$FONT,10,0,$nodex - $xoffset,$nodey + 5,$printednodes);
			} else	{
				$im->filledArc($nodex,$nodey,19,15,0,360,$white);
				# debugging line
				#$im->arc($nodex,$nodey,19,15,0,360,$black);
				$im->stringFT($unantialiased,$FONT,10,0,$nodex - $xoffset - 2,$nodey + 5,$printednodes);
			}
		}
		# print bootstrap proportions
		if ( $terminal_no[$i] == 0 && $bootstrap[$i] > 0 )	{
			# there needs to be a white box in the background in
			#  case bending of lines would cause an overlap
			# has to be positioned very exactly
			if ( $bootstrap[$i] < 100 )	{
				$im->filledRectangle($nodex - 9,$nodey - 16,$nodex + 11,$nodey - 7, $white);
				$im->stringFT($unantialiased,$FONT,6,0,$nodex - 7,$nodey - 7,$bootstrap[$i] . "%");
			} else	{
				$im->filledRectangle($nodex - 10,$nodey - 16,$nodex + 12,$nodey - 7, $white);
				$im->stringFT($unantialiased,$FONT,6,0,$nodex - 9,$nodey - 7,$bootstrap[$i] . "%");
			}
		}
	}

	if ( $caption )	{
		$caption =~ s/, $//;
		$caption = "<nobr>Key: " . $caption;
	}
	$caption = "\n<div style=\"width: 20em; text-align: center;\"><p style=\"text-align: left;\">\n" . $caption . "</nobr>";
	$caption .= "<br>\n<nobr>Reference: ".Reference::formatShortRef($dbt,$cladogram->{reference_no},'no_inits'=>1,'link_id'=>1) . "</nobr><br>\n";
	$caption .= "<nobr>Download as: <a href=\"/public/cladograms/cladogram_$cladogram_no.nhx\">NHX</a>, <a href=\"/public/cladograms/cladogram_$cladogram_no.png\">PNG</a></nobr></p></div>";

    my $dbh = $dbt->dbh;
    $sql = "UPDATE cladograms SET modified=modified,caption=".$dbh->quote($caption)." WHERE cladogram_no=$cladogram_no";
    $dbh->do($sql);

	# test directory and file name
	my $pngname = "cladogram_$cladogram_no.png";
	my $png_file = $HTML_DIR."/public/cladograms/$pngname";
	open PNG,">$png_file";
	binmode(PNG);
	print PNG $im->png;
	close PNG;
	chmod 0664, "$png_file";

	my $nhx_text = _formatTreeData(\@rows);
	my $txtname = "cladogram_$cladogram_no.nhx";
	my $txt_file = $HTML_DIR."/public/cladograms/$txtname";
	open TXT, ">$txt_file";
	print TXT $nhx_text;
	close TXT;
	chmod 0664, "$txt_file";
}


# outline only
# pass in a "backbone" (more reliable) cladogram and a "secondary" cladogram
#  taken directly from the database, then merge them and pass back the
#  merged cladogram
# the backbone is gotten either from the database or from previous calls of
#   this function
sub mergeCladograms	{

	# shift data corresponding to fields such as parent_no from nodes table
	# these could be packaged as a hash of arrays such as:
	#  $backbone{$parent_no[$i]}

	# find set of terminal taxa found in both cladograms
	# all taxon_nos have to be converted into synonym_nos taken from
	#  taxa_tree_cache
	# record the node_nos of the terminals in a hash where the secondary
	#  node_no is the key and the backbone node_no is the value (i.e.,
	#  a mapping hash)

	# return if there are no overlapping taxa at all

	# compute two trimmed cladograms each only including the terminals
	#  whose taxon_nos are in both the backbone and secondary cladogram
	# there could be trouble if some taxa are terminals in one cladogram
	#  but parent nodes in another, not sure what to do about this

	# compare each parent in the secondary cladogram to those in the
	#  backbone and record whether it conflicts (is not matched) because
	#  it includes a different set of terminals
	# if there is no conflict, record the matching parent_nos in the
	#  mapping hash

	# having figured that out, for each conflicting node find the next
	#  highest parent node that does not conflict, and record that
	#  relationship in the mapping hash
	# now every node in the secondary cladogram has a mapping value

	# having dealt with the overlapping nodes, handle the ones only found
	#  in the secondary cladogram
	# for each one:
	#  if the parent_no does not map to one in the backbone, leave it alone
	#  if it does, translate the parent_no into the node_no in the backbone
	#   using the mapping hash 
	#  after deciding, add the taxon's node_no and its parent_no to
	#   the backbone; the addition order shouldn't matter

	# there should be no conflicts at this point, because each taxon found
	#  in both cladograms maps to a node_no originally found in the
	#  backbone, whereas the others each map to a node_no that was unique
	#  to its own cladogram, so there are no duplicate node_nos

	# return the merged cladogram

}

1;

