package Measurement;
use Data::Dumper;
use CGI::Carp;
use TaxaCache;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $TAXA_TREE_CACHE);


# written by PS 6/22/2005 - 6/24/2005
# Handle display and processing of form to enter measurements for specimens

#my @specimen_fields= ('specimens_measured', 'specimen_coverage','specimen_id','specimen_side','specimen_part','measurement_source','length_median','length_min','length_max','length_error','length_error_unit','width_median','width_min','width_max','width_error','width_error_unit','height_median','height_min','height_max','height_error','height_error_unit','circumference_median','circumference_min','circumference_max','circumference_error','circumference_error_unit','diagonal_median','diagonal_min','diagonal_max','diagonal_error','diagonal_error_unit','inflation_median','inflation_min','inflation_max','inflation_error','inflation_error_unit','comments','length','width','height','diagonal','inflation');

my @specimen_fields   =('specimens_measured', 'specimen_coverage','specimen_id','specimen_side','specimen_part','measurement_source','magnification','is_type','comments');
my @measurement_types =('mass','length','width','height','circumference','diagonal','inflation');
my @measurement_fields=('average','median','min','max','error','error_unit');

#
# Displays a list of potential specimens from the search.  
#
sub submitSpecimenSearch {
    my  ($dbt,$hbo,$q,$s) = @_;
    my $dbh = $dbt->dbh;

    if ( ! $q->param('taxon_name') && ! $q->param('comment') && ! int($q->param('collection_no')) ) {
        push my @error , "You must enter a taxonomic name, comment, or collection number";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    }

    # Grab the data from the database, filtering by either taxon_name and/or collection_no
    my $sql1 = "SELECT c.collection_no, c.collection_name, IF(c.country IN ('Canada','United States'),c.state,c.country) place, i.interval_name max,IF(min_interval_no>0,i2.interval_name,'') min,o.occurrence_no,o.genus_reso o_genus_reso,o.genus_name o_genus_name,o.species_reso o_species_reso,o.species_name o_species_name,o.comments o_comments, '' re_genus_reso,'' re_genus_name,'' re_species_reso,'' re_species_name,'' re_comments, count(DISTINCT specimen_no) cnt FROM (occurrences o, collections c, intervals i) LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no LEFT JOIN intervals i2 ON c.min_interval_no=i2.interval_no LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE o.collection_no=c.collection_no AND c.max_interval_no=i.interval_no AND re.reid_no IS NULL ";
    my $sql2 = "SELECT c.collection_no, c.collection_name, IF(c.country IN ('Canada','United States'),c.state,c.country) place, i.interval_name max,IF(min_interval_no>0,i2.interval_name,'') min,o.occurrence_no,o.genus_reso o_genus_reso,o.genus_name o_genus_name,o.species_reso o_species_reso,o.species_name o_species_name,o.comments o_comments, re.genus_reso re_genus_reso,re.genus_name re_genus_name,re.species_reso re_species_reso,re.species_name re_species_name,re.comments re_comments, count(DISTINCT specimen_no) cnt FROM (reidentifications re, occurrences o, collections c, intervals i) LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no LEFT JOIN intervals i2 ON c.min_interval_no=i2.interval_no WHERE re.occurrence_no=o.occurrence_no AND o.collection_no=c.collection_no AND c.max_interval_no=i.interval_no AND most_recent='YES' ";

    my $where = "";
    my @taxa;
    if ($q->param('taxon_name')) {
        if ( $q->param('match_type') =~ /exact|combinations only/ )	{
            my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='".$q->param('taxon_name')."'";
            @taxa = @{$dbt->getData($sql)};
        } else	{
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$q->param('taxon_name'),'match_subgenera'=>1});
        }
    } 
    my $taxon_nos;        
    if (@taxa) {
        my (@taxon_nos,%all_taxa);
        if ( $q->param('match_type') !~ /exact/ )	{
            foreach (@taxa) {
                if ($_->{'taxon_rank'} =~ /species|genus/) {
                    @taxon_nos = TaxaCache::getChildren($dbt,$_->{'taxon_no'});
                    @all_taxa{@taxon_nos} = ();
                } else {
                    if ( $q->param('match_type') !~ /combinations only/ )	{
                        @taxon_nos = TaxonInfo::getAllSynonyms($dbt,$_->{'taxon_no'});
                    } else	{
                        @taxon_nos = TaxonInfo::getAllSpellings($dbt,$_->{'taxon_no'});
                    }
                    @all_taxa{@taxon_nos} = ();
                    @all_taxa{$_->{'taxon_no'}} = 1; 
                }
            }
            @taxon_nos = keys %all_taxa;
        } else	{
            push @taxon_nos , $_->{'taxon_no'} foreach @taxa;
        }
        $taxon_nos = join(",",@taxon_nos);
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
    } elsif ($q->param('taxon_name')) {
        my @taxon_bits = split(/\s+/,$q->param('taxon_name'));
        $sql1 .= " AND o.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
        $sql2 .= " AND re.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
        if (scalar(@taxon_bits) > 1) {
            $sql1 .= " AND o.species_name LIKE ".$dbh->quote($taxon_bits[1]);
            $sql2 .= " AND re.species_name LIKE ".$dbh->quote($taxon_bits[1]);
        }
    }
    if ($q->param('comment')) {
        $sql1 .= " AND o.comments LIKE '%".$q->param('comment')."%'";
        $sql2 .= " AND (o.comments LIKE '%".$q->param('comment')."%' OR re.comments LIKE '%".$q->param('comment')."%')";
        #$sql2 .= " AND re.comments LIKE '%".$q->param('comment')."%'";
    }
    if ($q->param('collection_no')) {
        $sql1 .= " AND o.collection_no=".int($q->param('collection_no'));
        $sql2 .= " AND o.collection_no=".int($q->param('collection_no'));
    }
    $sql1 .= " GROUP BY o.occurrence_no";
    $sql2 .= " GROUP BY o.occurrence_no";
    $sql .= "($sql1) UNION ($sql2) ORDER BY collection_no";

    dbg("SQL is $sql");       

    my @results = @{$dbt->getData($sql)};

    # if we only have a taxon_name, get taxa not tied to any occurrence as well
    my @results_taxa_only;
    if ($taxon_nos && !$q->param('collection_no')) {
        my $sql = "SELECT a.taxon_no,a.taxon_rank,a.taxon_name, count(DISTINCT specimen_no) cnt FROM authorities a LEFT JOIN specimens s ON s.taxon_no=a.taxon_no WHERE a.taxon_no IN ($taxon_nos) GROUP BY a.taxon_no ORDER by a.taxon_name";
        dbg("SQL for authorities only is $sql");
        @results_taxa_only = @{$dbt->getData($sql)};
    }

    if (scalar(@results) == 0 && $q->param('collection_no')) {
        push my @error , "Occurrences of this taxon could not be found";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    } elsif (scalar(@results) == 1 && $q->param('collection_no')) {
        $q->param('occurrence_no'=>$results[0]->{'occurrence_no'});
        displaySpecimenList($dbt,$hbo,$q,$s);
    } elsif (scalar(@results) == 0 && scalar(@results_taxa_only) == 0) {
        push my @error , "Occurrences or taxa matching these search terms could not be found";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    } else {
        print "<form method=\"POST\" action=\"$READ_URL\">";
        print "<input type=\"hidden\" name=\"action\" value=\"displaySpecimenList\">";
        print qq|<div align="center" class="small" style="margin: 1em;">|;
        print "<table cellspacing=\"0\" cellpadding=\"6\" style=\"border: 1px solid lightgray;\">";
        print "<tr><th><span style=\"margin-right: 1em\">Occurrence</span></th><th>Collection</th><th>Measurements</th></tr>";
#        print "<tr><td>Collection name</td><td></td></tr>";
        my $last_collection_no = -1;
        my %coll_names = ();
        $coll_names{$_->{'collection_name'}} = 1 for (@results);
        my $coll_count = scalar(keys(%coll_names));
        my $class = (scalar($coll_count) > 0) ? '' : 'class="darkList"';
        sub reso	{
            my $reso = shift;
            my $quote;
            if ( $reso eq '"' )	{
                $quote = '"';
            } else	{
                $reso .= " ";
            }
            return ($reso,$quote);
        }
        foreach my $row (@results) {
            my $specimens = ($row->{'cnt'} >= 1) ? $row->{'cnt'} : 'none';
            my $taxon_name;
            my ($reso,$quote) = reso($row->{'o_genus_reso'});
            my ($reso2,$quote2) = reso($row->{'o_species_reso'});
            if ( $reso eq '"' && $reso2 eq '"' )	{
                $quote = "";
                $reso2 = "";
            }
            $taxon_name = $reso.$row->{'o_genus_name'}.$quote." ".$reso2.$row->{'o_species_name'}.$quote2;
            if ($row->{re_genus_name}) {
                my ($reso,$quote) = reso($row->{'re_genus_reso'});
                my ($reso2,$quote2) = reso($row->{'re_species_reso'});
                if ( $reso eq '"' && $reso2 eq '"' )	{
                    $quote = "";
                    $reso2 = "";
                }
                $taxon_name .= "<br>\n&nbsp;= ".$reso.$row->{'re_genus_name'}.$quote." ".$reso2.$row->{'re_species_name'}.$quote2;
            }
            my $comments;
            if ( $row->{'o_comments'} || $row->{'re_comments'} )	{
                $comments = "<br>\n<div class=\"verysmall\" style=\"width: 20em; white-space: normal;\">$row->{'o_comments'}";
                if ( $row->{'o_comments'} && $row->{'re_comments'} )	{
                    $comments .= "/".$row->{'re_comments'};
                } elsif ( $row->{'re_comments'} )	{
                    $comments .= $row->{'re_comments'};
                }
                $comments .= "</div>\n";
            }
            if ($last_collection_no != $row->{'collection_no'}) {
                $class = ($class eq '') ? $class='class="darkList"' : '';
            }
            print "<tr $class><td><a href=\"$READ_URL?action=displaySpecimenList&occurrence_no=$row->{occurrence_no}\"><nobr><span class=\"small\">$taxon_name</a>$comments</nobr></a></td>";
            if ($last_collection_no != $row->{'collection_no'}) {
                $last_collection_no = $row->{'collection_no'};
                my $interval = $row->{max};
                if ( $row->{min} )	{
                    $interval .= " - ".$row->{min};
                }
                print "<td><span class=\"small\" style=\"margin-right: 1em;\"><a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$row->{collection_no}\">$row->{collection_name}</a></span><br><span class=\"verysmall\">$row->{collection_no}: $interval, $row->{place}</span></td>";
            } else {
                print "<td></td>";
            }
            print "<td align=\"center\">$specimens</td></tr>";
        }
        foreach my $row (@results_taxa_only) {
            $class = ($class eq '') ? $class='class="darkList"' : '';
            my $specimens = ($row->{'cnt'} >= 1) ? $row->{'cnt'} : 'none';
            my $taxon_name;
            if ($row->{'taxon_rank'} =~ /species/) {
                $taxon_name = $row->{'taxon_name'};
            } elsif ($row->{'taxon_rank'} =~ /genus/) {
                $taxon_name = $row->{'taxon_name'}." sp.";
            } else {
                $taxon_name = $row->{'taxon_name'}." indet.";
            }
            print "<tr $class><td><a href=\"$READ_URL?action=displaySpecimenList&taxon_no=$row->{taxon_no}\">$taxon_name</a></td><td>unknown collection</td><td align=\"center\">$specimens</td></tr>";

        }
        print qq|</table>
</div>
</form>
|;
    }

}

#
# Displays a list of specimens associated with a specific occurrence.  If there are no specimens
# currently associated with the occurrence, just go directly to the add occurrence form, otherwise
# give them an option to edit an old one, or add a new set of measurements
#
sub displaySpecimenList {
    my ($dbt,$hbo,$q,$s,$called_from) = @_;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
		push my @error , "There is no occurrence or classification of this taxon";
		print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
        carp "populateMeasurementForm called with no occurrence_no/taxon_no by ".$s->get('enterer_no');
		exit;
	}

    #my $sql = "SELECT * FROM specimens WHERE occurrence_no=".int($q->param('occurrence_no'));
    #my @results = @{$dbt->getData($sql)};
    my @results;
    my $taxon_name;
    my $collection;
    if ($q->param('occurrence_no')) {
        @results = getMeasurements($dbt,'occurrence_no'=>int($q->param('occurrence_no')));
        $sql = "SELECT collection_no,genus_name,species_name,occurrence_no FROM occurrences WHERE occurrence_no=".int($q->param("occurrence_no"));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            carp "Error is displaySpecimenList, could not find ".$q->param("occurrence_no")." in the database";
            print "An error has occurred, could not find occurrence in database";
            return;
        }
        $collection = "(collection $row->{collection_no})";
        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }
    } else {
        my $sql = "";
        my $taxon_no = int($q->param('taxon_no'));
        if ($q->param('types_only')) {
            $sql = "(SELECT s.*,m.*,s.taxon_no FROM specimens s, measurements m WHERE s.specimen_no=m.specimen_no AND s.taxon_no=$taxon_no AND s.is_type='holotype')"
                 . " UNION "
                 . "(SELECT s.*,m.*,s.taxon_no FROM specimens s, measurements m, occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND o.taxon_no=$taxon_no AND re.reid_no IS NULL AND s.is_type='holotype')"
                 . " UNION "
                 . "(SELECT s.*,m.*,s.taxon_no FROM specimens s, measurements m, occurrences o, reidentifications re WHERE s.occurrence_no=o.occurrence_no AND o.occurrence_no=re.occurrence_no AND s.specimen_no=m.specimen_no AND re.taxon_no=$taxon_no AND re.most_recent='YES' AND s.is_type='holotype')"
        } else {
            $sql = "SELECT s.*,m.*,s.taxon_no FROM specimens s, measurements m WHERE s.specimen_no=m.specimen_no AND s.taxon_no=$taxon_no";
        }
        dbg("sql is $sql");
        @results = @{$dbt->getData($sql)};

        my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>int($q->param('taxon_no'))});
        if ($taxon->{'taxon_rank'} =~ /species/) {
            $taxon_name = $taxon->{'taxon_name'};
        } elsif ($taxon->{'taxon_rank'} =~ /genus/) {
            $taxon_name = $taxon->{'taxon_name'}." sp.";
        } else {
            $taxon_name = $taxon->{'taxon_name'}." indet.";
        } 

    }

    my $panelheader = "";
    my $contentstyle = qq|style="padding-top: 1em;"|;
    if ( $called_from ne "processMeasurementForm" )	{
        $panelheader = '<span class="displayPanelHeader">'.$taxon_name.' '.$collection.'</span>';
        $contentstyle = qq|style="padding-top: 1.5em;"|;
    }
    print qq|<center>
<div class="displayPanel" align="center" style="width: 36em; margin-top: 2em; padding-bottom: 3em;">
  $panelheader
  <div class="displayPanelContent" $contentstyle>
|;
    print "<form name=\"specimenList\" method=\"POST\" action=\"$WRITE_URL\">\n";
    print "<input type=hidden name=\"action\" value=\"populateMeasurementForm\">\n";
    if ($q->param('types_only')) {
        print "<input type=hidden name=\"types_only\" value=\"".$q->param('types_only')."\">";
    }
    if ($q->param('occurrence_no')) {
        print "<input type=hidden name=\"occurrence_no\" value=\"".$q->param('occurrence_no')."\">";
    } else {
        print "<input type=hidden name=\"taxon_no\" value=\"".$q->param('taxon_no')."\">";
    }
    # default value
    print "<input type=hidden name=\"specimen_no\" value=\"-2\">";
    print qq|
<script language="JavaScript" type="text/javascript">
<!--
function submitForm ( )
{
  document.specimenList.submit() ;
}
-->
</script>
|;

    # now create a table of choices
    print "<table>\n";
#        my $checked = (scalar(@results) == 1) ? "CHECKED" : "";

    %specimens = ();
    %types = ();
    %parts = ();
    my $specimen_ids;
    foreach my $row (@results) {
        $specimens{$row->{specimen_no}}{$row->{measurement_type}} = $row->{real_average};
        $specimens{$row->{specimen_no}}{'specimens_measured'} = $row->{specimens_measured};
        $specimens{$row->{specimen_no}}{'specimen_part'} = $row->{specimen_part};
        if ( $row->{specimen_id} )	{
            $specimens{$row->{specimen_no}}{'specimen_id'} = $row->{specimen_id};
            $specimen_ids++;
        }
        $types{$row->{measurement_type}}++;
        $parts{$row->{specimen_part}}++ if ($row->{specimen_part});
    }

    $specimen_count = scalar(keys(%specimens));

    if ($specimen_count > 0) {
        print "<tr><th></th>";
        if ( $specimen_ids > 0 )	{
            print "<th>specimen #</th>";
        }
        print "<th>part</th>" if (%parts);
        print "<th>count</th>";
        foreach my $type (@measurement_types) {
            if ($types{$type}) {
                print "<th>$type</th>";
            }
        }
        print "</tr>";
    } else {
        if ($q->param('occurrence_no')) {
            print "<tr><th colspan=7 align=\"center\">There are no measurements for this occurrence<br><br></td></tr>";
        } else {
            print "<tr><th colspan=7 align=\"center\">There are no measurements for $taxon_name<br><br></td></tr>";
        }
    }

    my $checked;
    $checked = "CHECKED" if ($specimen_count == 1);
    my $types_only;
    $types_only = "types_only=$q->param('types_only')" if ($q->param('types_only'));
    # fixed order for mammal teeth
    my @teeth = ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");
    my %tooth;
    for my $t ( 0..$#teeth )	{
        $tooth{$teeth[$t]} = $t;
    }
    foreach $specimen_no (sort {$tooth{$specimens{$a}->{specimen_part}} <=> $tooth{$specimens{$b}->{specimen_part}} || $a <=> $b} keys %specimens) {
        my $row = $specimens{$specimen_no};
        # Check the button if this is the first match, which forces
        #  users who want to create new measurement to check another button
        if ( $q->param('occurrence_no') )	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&occurrence_no=| . $q->param('occurrence_no') . qq|&specimen_no=$specimen_no"><span class="measurementBullet">&#149;</span></td>|;
        } else	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&taxon_no=| . $q->param('taxon_no') . qq|&specimen_no=$specimen_no"><span class="measurementBullet">&#149;</span></td>|;
        }
        if ( $specimen_ids > 0 )	{
            print qq|<td>$row->{specimen_id}</td>|;
        }
        print qq|<td align="center">$row->{specimen_part}</td>| if (%parts);
        print qq|<td align="center">$row->{specimens_measured}</td>|;
        foreach my $type (@measurement_types) {
            if ($types{$type}) {
                print "<td align=\"center\">$row->{$type}</td>";
            }
        }
        print "</a>";
        print "</tr>";
    }

    # always give them an option to create a new measurement as well
    if ( $q->param('occurrence_no') )	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&occurrence_no=| . $q->param('occurrence_no') . qq|&specimen_no=-1"><span class="measurementBullet">&#149;</span></td>|;
        } else	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&taxon_no=| . $q->param('taxon_no') . qq|&specimen_no=-1"><span class="measurementBullet">&#149;</span></td>|;
        }
    print "<td colspan=\"6\">&nbsp;Add a new average measurement</i></td></tr>\n";
    print qq|<tr><td align="center" valign="top"><a href="javascript:submitForm('')"><div class="measurementBullet" style="position: relative; margin-top: -0.1em;">&#149;</div></td>|;
    print "<td colspan=\"6\" valign=\"top\">&nbsp;Add <input type=\"text\" name=\"specimens_measured\" value=\"10\" size=3>new individual measurements</i><br>";
    print qq|
  <div style=\"padding-left: 2em;\">
  default specimen #: <input name="default_no" size="10"><br>
  default side:
  <select name="default_side">
  <option value=""></option>
  <option value="left">left</option>
  <option value="right">right</option>
  <option value="left?">left?</option>
  <option value="right?">right?</option>
  <option value="dorsal">dorsal</option>
  <option value="ventral">ventral</option>
  <option value="both">both</option>
  </select><br>
  measurements:
  <input type="checkbox" name="length" checked> length 
  <input type="checkbox" name="width" checked> width
  <input type="checkbox" name="height"> height<br>
  <input type="checkbox" name="circumference" style="margin-left: 11em;"> circumference
  <input type="checkbox" name="diagonal"> diagonal
  <br>default part: <input name="default_part" size="10"><br>
  default type:
  <select name="default_type">
  <option value="no">no</option>
  <option value="holotype">holotype</option>
  <option value="paratype">paratype</option>
  </select><br>
  default source:
  <select name="default_source">
  <option value=""></option>
  <option value="text">text</option>
  <option value="table">table</option>
  <option value="picture">picture</option>
  <option value="graph">graph</option>
  <option value="direct measurement">direct measurement</option>
  </select>
  <br>default magnification: <input name="default_magnification" size="10"><br>
  </div>
</td></tr>
|;

if ( $called_from eq "processMeasurementForm" )	{
	return;
}

print qq|</table>
</div>
</div>
</center>
|;

}

sub populateMeasurementForm {
    my ($dbt,$hbo,$q,$s) = @_;
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
    if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
        push my @error , "There is no matching taxon or occurrence";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
        carp "populateMeasurementForm called with no occurrence_no/taxon_no by ".$s->get('enterer_no');
        exit;
    }   
    
	# get the taxon's name
    my ($taxon_name,$old_field,$old_no,$collection,$extant);
    if ($q->param('occurrence_no')) {
        $old_field = "occurrence_no";
        $old_no = $q->param('occurrence_no');
        my $sql = "SELECT c.collection_name, o.collection_no, o.genus_name, o.species_name, o.occurrence_no, o.taxon_no FROM collections c, occurrences o WHERE c.collection_no=o.collection_no AND o.occurrence_no=".int($q->param('occurrence_no'));
        my $row = ${$dbt->getData($sql)}[0];

        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }  

        $collection = "($row->{'collection_name'})";

        if (!$row || !$taxon_name || !$collection) {
            push my @error , "The occurrence of this taxon could not be found";
            print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
            carp("processMeasurementForm: no row found for occurrence_no ".$q->param('occurrence_no'));
            return;
        }
    } else {
        $old_field = "taxon_no";
        $old_no = int($q->param('taxon_no'));
        my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>int($q->param('taxon_no'))},['taxon_rank','taxon_name','extant']);
        if ($taxon->{'taxon_rank'} =~ /species/) {
            $taxon_name = $taxon->{'taxon_name'};
        } elsif ($taxon->{'taxon_rank'} =~ /genus/) {
            $taxon_name = $taxon->{'taxon_name'}." sp.";
        } else {
            $taxon_name = $taxon->{'taxon_name'}." indet.";
        }
        # check for extant taxa only if there is no occurrence because
        #  occurrences are only of fossils, in principle
        if ($taxon->{'extant'} =~ /yes/i) {
            $extant = "yes";
        }
    }

    #Prepare fields to be use in the form ahead
    my @values = ();
    my @fields = ();

    if ($q->param('specimen_no') < 0) {
        # This is a new entry
        if (!$s->get('reference_no')) {
            # Make them choose a reference first
            $s->enqueue($q->query_string());
            main::displaySearchRefs("Please choose a reference before adding specimen measurement data",1);
            return;
        } else {
            if ($q->param('specimen_no') == -1) {
                # get the data from the last record of this taxon entered
                #  JA 10.5.07
                my $fieldstring = join(',',@specimen_fields);
                $fieldstring =~ s/is_type/is_type AS specimen_is_type/;
                $sql = "SELECT $fieldstring FROM specimens WHERE $old_field=$old_no ORDER BY specimen_no DESC";
                $row = ${$dbt->getData($sql)}[0];
                push @fields,$_ for @specimen_fields;
                s/is_type/specimen_is_type/ foreach @fields;
                if ( ! $row->{'specimens_measured'} )	{
                    $row->{'specimens_measured'} = 1;
                }
                for my $f ( @fields )	{
                    push @values,  $row->{$f};
                }
                foreach my $type (@measurement_types) {
                    foreach my $f (@measurement_fields) {
                        push @fields, $type."_".$f;
                        if ( $row->{$f} )	{
                            push @values, $row->{$f};
                        } else	{
                            push @values, '';
                        }
                    }
                }
                if ( $extant eq "yes" )	{
                    push @fields, "mass_unit";
                    push @values , "g";
                }

                my $sql = "SELECT author1init,author1last,author2init,author2last,pubyr,reftitle,pubtitle,pubvol,firstpage,lastpage FROM refs WHERE reference_no=".$s->get('reference_no');
                my $ref = ${$dbt->getData($sql)}[0];

	        push @fields,('reference','occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','types_only');
	        push @values,(Reference::formatLongRef($ref),int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,int($q->param('types_only')));
	        print $hbo->populateHTML('specimen_measurement_form_general', \@values, \@fields);
            } elsif ($q->param('specimen_no') == -2) {
		push (@fields,'occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','specimen_coverage');
		push (@values,int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,'');
		my $column_names;
		my $inputs;
                for my $c ( 'length','height','width','circumference','diagonal' )	{
			if ( $q->param($c) )	{
				my @c = split //,$c;
				$c[0] =~ tr/[a-z]/[A-Z]/;
				my $cn = join('',@c);
				$cn =~ s/Circumference/Circumf./;
				$column_names .= qq|<th><span class="small">$cn</span></th>|;
				$inputs .= qq|  <td><input type="text" name="|.$c.qq|_average" value="" size=7 class="tiny"></td>
|;
			}
		}
                #@table_rows = ('specimen_id','length','width','height','diagonal','specimen_side','specimen_part','measurement_source','magnification','is_type');
                my $table_rows = "";
                for (1..$q->param('specimens_measured')) {
                    $table_rows .= $hbo->populateHTML('specimen_measurement_form_row',[$q->param('default_no'),$inputs,$q->param('default_side'),$q->param('default_part'),$q->param('default_type'),$q->param('default_source'),$q->param('default_magnification')],['specimen_id','inputs','specimen_side','specimen_part','specimen_is_type','measurement_source','magnification']);
                }
                push @fields,('column_names','table_rows');
                push @values,($column_names,$table_rows);
	        my $html = $hbo->populateHTML('specimen_measurement_form_individual', \@values, \@fields);
                print $html;
            }
        }
    } elsif ($q->param('specimen_no') > 0) {
        # query the specimen table for the old data
        $sql = "SELECT * FROM specimens WHERE specimen_no=".int($q->param('specimen_no'));
        $row = ${$dbt->getData($sql)}[0];

        #Query the measurements table for the old data
        $sql = "SELECT * FROM measurements WHERE specimen_no=".int($q->param('specimen_no'));
        my @measurements = @{$dbt->getData($sql)};

        # Get the measurement data. types can be "length","width",etc. fields can be "average","max","min",etc.
        my %m_table = (); # Measurement table, only used right below
        $m_table{$_->{'measurement_type'}} = $_ for @measurements;

        # add mass_unit to force display of mass fields and convert
        #  high g values to kg 
        if ( $extant eq "yes" ) {
            push @fields, "mass_unit";
            if ( $m_table{'mass'}{'average'} >= 100 ) {
                push @values , "kg";
                foreach my $f (@measurement_fields) {
                    if ( $m_table{'mass'}{$f} > 0) {
                        $m_table{'mass'}{$f} /= 1000;
                    } 
                } 
            } else {
                push @values , "g";
            }
        }

        foreach my $type (@measurement_types) {
            foreach my $f (@measurement_fields) {
                push @fields, $type."_".$f;
                if (exists $m_table{$type}) {
                    push @values, $m_table{$type}{$f};
                } else {
                    push @values, '';
                }
            }
        }
        
	for my $field ( @specimen_fields )	{
		push @fields,$field;
		if ( $row->{$field} )	{
			push @values, $row->{$field};
		} else {
			push @values, '';
		}
	}
        # This is an edit, use fields from the DB
        push @fields, 'specimen_is_type';
        if ($row->{'is_type'} eq 'holotype') {
            push @values, 'holotype';
        } elsif ($row->{'is_type'} eq 'some paratypes') {
            push @values, 'some paratypes';
        } elsif ($row->{'is_type'} eq 'paratype') {
            push @values, 'paratype';
        } else {
            push @values, 'no';
        }      

        my $sql = "SELECT author1init,author1last,author2init,author2last,pubyr,reftitle,pubtitle,pubvol,firstpage,lastpage FROM refs WHERE reference_no=".$row->{'reference_no'};
        my $ref = ${$dbt->getData($sql)}[0];

        # if the refs don't agree, the user might want to switch the record to the
        #  current ref
        if ( $row->{'reference_no'} != $s->get('reference_no') )	{
            push @fields , 'switch_to_ref';
            push @values , '<br><div style="margin-top: 0.5em;"><input type="checkbox" name="switch_ref" value="'.$s->get('reference_no').'"> switch this data record to your current reference ('.$s->get('reference_no').')'."</div>\n";
        }

	# if this is a general taxon-specific record, the user might want to swap it
	#   to a specific occurrence
	# exact match on genus and species is required to avoid printing tons of
	#  irrelevant records
	if ( $row->{'occurrence_no'} == 0 )	{
		my ($g,$s) = split / /,$taxon_name;
		my $sql = "SELECT collection_name,occurrence_no FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND genus_name='$g' AND species_name='$s'";
        	@colls = @{$dbt->getData($sql)};
		if ( @colls )	{
			push @fields , 'occurrences';
			my $occ_list = "<div style=\"margin-top: 0.5em;\">... and/or switch this record to ";
			if ( $#colls == 0 )	{
				$occ_list .= "<input type=\"checkbox\" name=\"switch_occ\" value=\"$colls[0]->{'occurrence_no'}\">$colls[0]->{'collection_name'}<br>\n";
			} elsif ( $#colls <= 9 )	{
				$occ_list .= "one of the following collections:<br>";
				$occ_list .= "<input type=\"radio\" name=\"switch_occ\" value=\"$_->{'occurrence_no'}\" style=\"margin-left: 3em;\">$_->{'collection_name'}<br>\n" foreach @colls;
			} else	{
				$occ_list .= "collection number <input name=\"switch_coll\" size=6>\n<input type=\"hidden\" name=genus_name value=\"$g\">\n<input type=\"hidden\" name=species_name value=\"$s\">\n";
			}
			$occ_list .= "</div>\n";
			push @values , $occ_list;
		}
	}

        # some additional fields not from the form row
	    push (@fields, 'reference','occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','types_only');
	    push (@values, sprintf("%s",Reference::formatLongRef($ref)),int($q->param('occurrence_no')),int($q->param('taxon_no')),int($row->{'reference_no'}),$row->{'specimen_no'},$taxon_name,$collection,int($q->param('types_only')));

	    print $hbo->populateHTML('specimen_measurement_form_general', \@values, \@fields);
    }

}

sub processMeasurementForm	{
    my ($dbt,$hbo,$q,$s) = @_;
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
    if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
        push my @error , "There is no matching taxon or occurrence";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
        carp "populateMeasurementForm called with no occurrence_no/taxon_no by ".$s->get('enterer_no');
        exit;
    }

    my $taxon_no = $q->param('taxon_no');

    # get the taxon's name
    my ($taxon_name,$collection,$newcoll,$badcoll);
    if ($q->param('occurrence_no')) {
        my $sql = "SELECT o.taxon_no, o.collection_no, o.genus_name, o.species_name, o.occurrence_no FROM occurrences o WHERE o.occurrence_no=".int($q->param('occurrence_no'));
        my $row = ${$dbt->getData($sql)}[0];
        $taxon_no = $row->{'taxon_no'};
        $sql = "SELECT taxon_no FROM reidentifications WHERE occurrence_no=".$row->{'occurrence_no'}." AND most_recent='YES'";
        my $reid_taxon = ${$dbt->getData($sql)}[0]->{'taxon_no'};
        if ( $reid_taxon > 0 )	{
            $taxon_no = $reid_taxon;
        }

        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }

        $collection = " in collection $row->{'collection_no'}";

        if (!$row || !$taxon_name || !$collection) {
            push my @error , "The occurrence of this taxon could not be found";
            print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
            carp("processMeasurementForm: no row found for occurrence_no ".$q->param('occurrence_no'));
            return;
        }
    } else {
        my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>int($q->param('taxon_no'))});
        if ($taxon->{'taxon_rank'} =~ /species/) {
            $taxon_name = $taxon->{'taxon_name'};
        } elsif ($taxon->{'taxon_rank'} =~ /genus/) {
            $taxon_name = $taxon->{'taxon_name'}." sp.";
        } else {
            $taxon_name = $taxon->{'taxon_name'}." indet.";
        } 
    } 


    my @specimen_ids = $q->param('specimen_id');
    my @param_list = $q->param();

    my $inserted_row_count = 0;
    for(my $i=0;$i<scalar(@specimen_ids);$i++) {
        my %fields = ();

        # This is the part where we rearrange the data into a flat single dimensional
        # hash that contains all the data from single row.  i.e. if $i = 3, the hash
        # will contain the length,width etc from row 3, as well as the intransients like occurrence_no
        foreach my $param (@param_list) {
            my @vars = $q->param($param);
            if (scalar(@vars) == 1) {
                $fields{$param} = $vars[0];
            } else {
                $fields{$param} = $vars[$i];
            }
        }

        # Make sure at least one of these fields is set
        if (! ($fields{'length_average'} || $fields{'width_average'} || $fields{'height_average'} || $fields{'circumference_average'} || $fields{'diagonal_average'}) ) {
            next;
        }

        # kg values have to be rescaled as grams
        if ($fields{'mass_average'} > 0 && $fields{'mass_unit'} eq "kg") {
            for my $f ( @measurement_fields )	{
                $fields{'mass_'.$f} *= 1000;
            }
        }
        # if ecotaph no exists, update the record

        if ($fields{'specimen_is_type'} =~ /holotype/) {
            $fields{'is_type'} = 'holotype';
        } elsif ($fields{'specimen_is_type'} =~ /paratypes/) {
            $fields{'is_type'} = 'some paratypes';
        } elsif ($fields{'specimen_is_type'} =~ /paratype/) {
            $fields{'is_type'} = 'paratype';
        } else {
            $fields{'is_type'} = '';
        }

        my $specimen_no;

        # prevent duplication by resubmission of form for "new" specimen JA 21.12.10
        # this is pretty conservative...
        if ( $fields{'specimen_no'} <= 0 )	{
            $sql = "SELECT specimen_no FROM specimens WHERE ((specimen_id='".$fields{'specimen_id'}."' AND specimen_id IS NOT NULL) OR (specimen_id IS NULL AND taxon_no=".$fields{'taxon_no'}." AND taxon_no>0) OR (specimen_id IS NULL AND occurrence_no=".$fields{'occurrence_no'}." AND occurrence_no>0)) AND BINARY specimen_part='".$fields{'specimen_part'}."'";
            $fields{'specimen_no'} = ${$dbt->getData($sql)}[0]->{'specimen_no'};
        }

        if ( $fields{'specimen_no'} > 0 )	{
            delete $fields{'taxon_no'}; # keys, never update thse
            delete $fields{'occurrence_no'}; # keys, never update thse
            if ( $q->param('switch_ref') > 0 )	{
                $fields{'reference_no'} = $q->param('switch_ref');
            }
            if ( $q->param('switch_occ') > 0 )	{
                $fields{'occurrence_no'} = $q->param('switch_occ');
                $fields{'taxon_no'} = undef;
            } elsif ( $q->param('switch_coll') > 0 )	{
                $sql = "SELECT occurrence_no FROM occurrences WHERE collection_no=".$q->param('switch_coll')." AND genus_name='".$q->param('genus_name')."' AND species_name='".$q->param('species_name')."'";
                $fields{'occurrence_no'} = ${$dbt->getData($sql)}[0]->{'occurrence_no'};
                # the user might have screwed up the collection number...
                if ( $fields{'occurrence_no'} == 0 )	{
                    delete $fields{'occurrence_no'};
                } else	{
                    $fields{'taxon_no'} = undef;
		}
            }
            if ( $fields{'occurrence_no'} > 0 )	{
                $sql = "SELECT collection_name FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND occurrence_no=".$fields{'occurrence_no'};
                $newcoll = ${$dbt->getData($sql)}[0]->{'collection_name'};
            } elsif ( $q->param('switch_coll') > 0 )	{
                $sql = "SELECT collection_name FROM collections c WHERE collection_no=".$q->param('switch_coll');
                $badcoll = ${$dbt->getData($sql)}[0]->{'collection_name'};
            }
            $result = $dbt->updateRecord($s,'specimens','specimen_no',$fields{'specimen_no'},\%fields);
            $specimen_no = $fields{specimen_no};

            if ($result) {
                $sql = "SELECT * FROM measurements WHERE specimen_no=".int($fields{'specimen_no'});
                my @measurements = @{$dbt->getData($sql)};

                my %in_db= (); # Find rows from DB
                $in_db{$_->{'measurement_type'}} = $_ for @measurements;

                my %in_cgi = (); # Find rows from filled out form
                foreach my $type (@measurement_types) {
                    foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                        if ($fields{$type."_".$f}) {
                            $in_cgi{$type}{$f} = $fields{$type."_".$f};
                        } 
                    }
                }

                foreach my $type (@measurement_types) {
                    if ($in_db{$type} && $in_cgi{$type}) {
                        # If the record exists both the form and db, its an update
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            if (!$in_cgi{$type}{$f}) {
                                $in_cgi{$type}{$f} = "";
                            }
                            if ($fields{'magnification'} =~ /^[0-9.]+$/) {
                                if ($in_cgi{$type}{$f}) {
                                    $in_cgi{$type}{'real_'.$f}=$in_cgi{$type}{$f}/$fields{'magnification'};
                                } else {
                                    $in_cgi{$type}{'real_'.$f}="";
                                }
                            } else {
                                $in_cgi{$type}{'real_'.$f}=$in_cgi{$type}{$f};
                            }
                        }
                        if ( $in_cgi{$type}{'real_error'} )	{
                            $in_cgi{$type}{'error_unit'} = $fields{$type."_error_unit"};
                        }
                        dbg("UPDATE, TYPE $type: ".Dumper($in_cgi{$type}));
#                        $row->{'error_unit'} = $q->param($type."_error_unit");
                        #$dbt->insertRecord($s,'measurements',$row);
                        $dbt->updateRecord($s,'measurements','measurement_no',$in_db{$type}{'measurement_no'},$in_cgi{$type});
                    } elsif ($in_db{$type}) {
                        # Else if it exists only in the database now, delete it 
                        $sql = "DELETE FROM measurements WHERE measurement_no=".$in_db{$type}{'measurement_no'} . " LIMIT 1";
                        dbg("DELETING type $type: $sql");
                        $dbt->getData($sql);
                    } elsif ($in_cgi{$type}) {
                        # Else if its in the form and NOT in the DB, add it
                        dbg("INSERT, TYPE $type: ".Dumper($row));
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            if ($fields{'magnification'} =~ /^[0-9.]+$/) {
                                $in_cgi{$type}{'real_'.$f}=$in_cgi{$type}{$f}/$fields{'magnification'};
                            } else {
                                $in_cgi{$type}{'real_'.$f}=$in_cgi{$type}{$f};
                            }
                        }
#                        $in_cgi{$type}{'error_unit'}=$q->param($type."_error_unit");
                        $in_cgi{$type}{'measurement_type'}=$type;
                        $in_cgi{$type}{'specimen_no'}= $fields{'specimen_no'};
                        if ( $in_cgi{$type}{'real_error'} )	{
                            $in_cgi{$type}{'error_unit'}=$fields{$type."_error_unit"};
                        }
                        $dbt->insertRecord($s,'measurements',$in_cgi{$type});
    #                    $in_cgi{$type}{'measurement_type'}=$type;
    #                    $in_cgi{$type}{'error_unit'}=$q->param($type."_error_unit");
    #                    $in_cgi{$type}{'specimen_no'}= $q->param('specimen_no');
    #                    $dbt->insertRecord($s,'measurements',$in_cgi{$type});
                    }
                }

                print "<center><p class=\"pageTitle\">$taxon_name$collection (revised data)</p></center>\n";
            } else {
                print "Error updating database table row, please contact support";
                carp "Error updating row in Measurement.pm: ".$result;
            }
        } else {
            # Set the reference_no
            $fields{'reference_no'} = $s->get('reference_no');
            # Make sure one of these gets set to NULL
            if ($q->param('occurrence_no')) {
                $fields{'taxon_no'} = undef;
            } else {
                $fields{'occurrence_no'} = undef;
            }
            my $result;
            ($result,$specimen_no) = $dbt->insertRecord($s,'specimens',\%fields);

            if ($result) {
                # Get the measurement data. types can be "length","width",etc. fields can be "average","max","min",etc.
                my %m_table = (); # Measurement table, only used right below
                foreach my $type (@measurement_types) {
                    foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                        if ($fields{$type."_".$f}) {
                            $m_table{$type}{$f} = $fields{$type."_".$f};
                        } 
                    }
                }

                # Now insert a row into the measurements table for each type of measurement
                while(my($type,$row)=each %m_table) {
                    foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                        next if (!$row->{$f});
                        if ($fields{'magnification'} =~ /^[0-9.]+$/) {
                            $row->{'real_'.$f} = $row->{$f}/$fields{'magnification'}; 
                        } else {
                            $row->{'real_'.$f} = $row->{$f};
                        }
                    }
                    dbg("INSERT, TYPE $type: ".Dumper($row));
                    $row->{'measurement_type'} = $type;
                    $row->{'specimen_no'} = $specimen_no;
                    $row->{'error_unit'}=$fields{$type."_error_unit"};
                    $dbt->insertRecord($s,'measurements',$row);
                }

                $inserted_row_count++;
            } else {
                print "Error inserting database table row, please contact support";
                carp "Error inserting row in Measurement.pm: ".$result;
            }
        }
        if ($specimen_no) {
            syncWithAuthorities($dbt,$s,$hbo,$specimen_no);
        }
    }

    if ($inserted_row_count) {
        print "<center><p class=\"pageTitle\">$taxon_name$collection (new data)</p></center>\n";
    }

    if ( $q->param('switch_occ') > 0 || $q->param('switch_coll') > 0 )	{
        if ( $newcoll )	{
            print "<div style=\"width: 32em; margin-left: auto; margin-right: auto; text-indent: -1em;\"><p class=\"verysmall\">The data record you updated has been switched successfully to $newcoll. You can search for it by clicking the 'another collection' link.</p></div>\n";
        } elsif ( $badcoll )	{
            print "<div style=\"width: 32em; margin-left: auto; margin-right: auto; text-indent: -1em;\"><p class=\"verysmall\">The data record was not switched because $taxon_name is not present in $badcoll. Try entering a different collection number.</p></div>\n";
        }
    }

	displaySpecimenList($dbt,$hbo,$q,$s,'processMeasurementForm');
	if ($q->param('occurrence_no')) {
		my ($temp,$collection_no) = split / /,$collection;
		$collection_no =~ s/\)//;
    		print qq|<tr><td valign="top"><a href="$WRITE_URL?action=submitSpecimenSearch&collection_no=$collection_no"><span class="measurementBullet">&#149;</span></a></td><td colspan="6" valign="center">&nbsp;Add a measurement of another occurrence in this collection</td></tr>
|;
	}
	print qq|<tr><td valign="top"><a href="$WRITE_URL?action=submitSpecimenSearch&taxon_name=$taxon_name"><span class="measurementBullet">&#149;</span></a></td><td colspan="6" valign="center">&nbsp;Add a measurement of $taxon_name in another collection</td></tr>
|;
	print qq|<tr><td><a href="$READ_URL?action=checkTaxonInfo&taxon_name=$taxon_name"><span class="measurementBullet">&#149;</span></a></td><td colspan="6" valign="center">&nbsp;Get general info about this taxon</td></tr>
<tr><td><a href="$WRITE_URL?action=displaySpecimenSearchForm"><span class="measurementBullet">&#149;</span></a></td><td colspan="6" valign="center">&nbsp;Add a measurement of another taxon</td></tr>
</table>
</div>
</div>
</center>
|;

    # cache average of all body mass estimates for this taxon's senior synonym JA 7.12.10
    # note that we do not compute mass estimates for junior synonyms by themselves, so likewise
    #  we (1) store combined data only, and (2) store these data under the names of the senior
    #  synonyms only
    my $orig = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    my $ss = TaxonInfo::getSeniorSynonym($dbt,$orig);
    my @in_list = TaxonInfo::getAllSynonyms($dbt,$ss);
    my @specimens = getMeasurements($dbt,'taxon_list'=>\@in_list,'get_global_specimens'=>1);
    my $p_table = getMeasurementTable(\@specimens);
    my @m = getMassEstimates($dbt,$ss,$p_table);
    if ( $m[5] && $m[6] )	{
        my $mean = $m[5] / $m[6];
        @in_list = TaxonInfo::getAllSpellings($dbt,$ss);
        my $sql = "UPDATE $TAXA_TREE_CACHE SET mass=$mean WHERE taxon_no IN (".join(',',@in_list).")";
        $dbh->do($sql);
    }

}


# rewrote this heavily JA 10.8.07
# all authorities data manually entered before that point are suspect
sub syncWithAuthorities {
    my ($dbt,$s,$hbo,$specimen_no) = @_;

    my $sql = "SELECT s.taxon_no,specimens_measured,specimen_id,specimen_part,magnification,is_type,type_specimen,type_body_part,part_details FROM specimens s,authorities a WHERE specimen_no=$specimen_no AND s.taxon_no=a.taxon_no";
    my $row = ${$dbt->getData($sql)}[0];

    # bomb out if the specimens data don't clearly pertain to the type or
    #  the authorities table has complete data already
    # we don't want to mess around if there's conflict, because (for example)
    #  the type specimen per se may include additional body parts on top of
    #  the one that has been measured
    if ( ! $row || $row->{'taxon_no'} < 1 || $row->{'specimens_measured'} != 1 || $row->{'is_type'} ne "holotype" || ( $row->{'magnification'} && $row->{'magnification'} != 1 ) || ( $row->{'type_specimen'} && $row->{'type_body_part'} && $row->{'part_details'} ) )	{
        return;
    } else	{
        my $taxon_no = $row->{'taxon_no'};
        my %generic_parts = ();
        foreach my $p ($hbo->getList('type_body_part'))	{
            $generic_parts{$p} = 1;
        }
        if ( $row->{'specimen_part'} )	{
            if ( $generic_parts{$row->{'specimen_part'}} && ! $row{'type_body_part'} ) {
                $fields{'type_body_part'} = $row->{'specimen_part'};
            } elsif ( ! $row{'part_details'} )	{
                $fields{'part_details'} = $row->{'specimen_part'};
            }
        }
        if ( ! $row{'type_specimen'} )	{
            $fields{'type_specimen'} = $row->{'specimen_id'};
        }
        $dbt->updateRecord($s,'authorities','taxon_no',$taxon_no,\%fields);
    }
}

# General purpose function for getting occurrences with data.  Pass in 2 arguments:
#   Argument 1 is $dbt object
#   Argument 2 is hash array of options
#   i.e. getSpecimens($dbt,'collection_no'=>1111,'taxon_name'=>'Calippus')
#   Possible values for options:
#      taxon_no: a taxon_no.  Will call getChildren on this no
#      taxon_name: Will find all taxon_nos this corresponds to, and combine the 
#          getChildren calls for all of them.  If a taxon_no is not found, then
#          search against the occurrences/reids table
#      taxon_list: an array ref of taxon_nos, like $in_list in TaxonInfo
#      collection_no: a collection_no
#      get_global_specimens: include measurements for which the occurrence is not known and
#          only the taxon_no is known. used in TaxonInfo and in limited cases in Download
#   Returns a straight array of what the DB results
sub getMeasurements {
    my $dbt = shift;
    my $dbh = $dbt->dbh;
    my %options = @_;


    my ($sql1,$sql2,$sql3,$where) = ("","","");

    $sql1 = "SELECT s.*,m.*,o.taxon_no FROM (specimens s, occurrences o, measurements m) LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.reid_no IS NULL";
    $sql2 = "SELECT s.*,m.*,re.taxon_no FROM specimens s, occurrences o, measurements m, reidentifications re WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.occurrence_no=o.occurrence_no AND re.most_recent='YES'";
    $sql3 = "SELECT s.*,m.*,a.taxon_no FROM specimens s, authorities a, measurements m WHERE a.taxon_no=s.taxon_no AND s.specimen_no=m.specimen_no";

    my $clause_found = 0;
    if ($options{'taxon_list'}) {
        my $taxon_nos = join(",",@{$options{'taxon_list'}});
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
        $sql3 .= " AND a.taxon_no IN ($taxon_nos)";
        $clause_found = 1;
    } elsif ($options{'taxon_name'} || $options{'taxon_no'}) {
        my @taxa;
        if ($options{'taxon_name'}) {
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$options{'taxon_name'}},['taxon_no']);
        } else {
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$options{'taxon_no'}},['taxon_no']);
        }
        if (@taxa) {
            my (@taxon_nos,%all_taxa);
            foreach (@taxa) {
                @taxon_nos = TaxaCache::getChildren($dbt,$_->{'taxon_no'});
                @all_taxa{@taxon_nos} = ();
            }
            @taxon_nos = keys %all_taxa;
            my $taxon_nos = join(",",@taxon_nos);
            $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
            $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
            $sql3 .= " AND a.taxon_no IN ($taxon_nos)";
        } elsif ($options{'taxon_name'}) {
            my @taxon_bits = split(/\s+/,$options{'taxon_name'});
            $sql1 .= " AND o.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
            $sql2 .= " AND re.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
            if (scalar(@taxon_bits) > 1) {
                $sql1 .= "AND o.species_name LIKE ".$dbh->quote($taxon_bits[1]);
                $sql2 .= "AND re.species_name LIKE ".$dbh->quote($taxon_bits[1]);
            }
        }
        $clause_found = 1;
    } elsif ($options{'collection_no'}) {
        $sql1 .= " AND o.collection_no=".int($options{'collection_no'});
        $sql2 .= " AND o.collection_no=".int($options{'collection_no'});
        $clause_found = 1;
    } elsif ($options{'occurrence_list'}) {
        $sql1 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
        $sql2 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
        $clause_found = 1;
    } elsif ($options{'occurrence_no'}) {
        $sql1 .= " AND o.occurrence_no =".int($options{'occurrence_no'});
        $sql2 .= " AND o.occurrence_no =".int($options{'occurrence_no'});
        $clause_found = 1;
    }

    if ($options{'get_global_specimens'} && $sql3 =~ /taxon_no IN/) {
        $sql = "($sql1) UNION ($sql2) UNION ($sql3)";
    } else {
        $sql = "($sql1) UNION ($sql2)";
    } #else {
      #  $sql = $sql1;
    #}
    dbg("SQL is $sql");

    if ($clause_found) {
        my @results = @{$dbt->getData($sql)};
        return @results;
    } else {
        return ();
    }
}

# Pass in a joined specimen/measurement table, as returned by the getMeasurements function above.
# This will give back a triple hash of aggregate data, in the form:
# $table{part}{what is measured}{stat type} where stat_type can be : min,max,a_mean,average,median,error
#  and what is measured can be: width,length,height,diagonal,inflation and part can be leg or arm,etc
# By convention, the triple hash ref is called p_table (parts table) and the double sub-hash is m_table (measurements table)
# See TaxonInfo or Download for examples of this function being called. i.e.:
# @results = getMeasurements($dbt,'collection_no'=>1234);
# $p_table = getMeasurementTable(\@results);
# $m_table = $p_table->{'leg'};
# $total_measured = $m_table->{'specimens_measured'}
# $average_width_leg = $m_table->{'width'}{'average'}
sub getMeasurementTable {
    my @measurements = @{$_[0]};

    my %p_table;
    my $sp_count = 0;
    my %types = ();
    my %seen_specimens = ();
    my %unique_specimen_nos = ();

    # Do a simple reorganization of flat database data into triple indexed hash described above
    foreach my $row (@measurements) {
        if (!$seen_specimens{$row->{'specimen_no'}}) {
            $p_table{$row->{'specimen_part'}}{'specimens_measured'} += $row->{'specimens_measured'};
            $unique_specimen_nos{$row->{'specimen_part'}}++;
            $seen_specimens{$row->{'specimen_no'}} = 1;
        }
        $types{$row->{'measurement_type'}}++;
        my $part_type;
        if (! exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}) {
            $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}} = {};
        } 
        $part_type = $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}};
#        $p_table{'a_mean'}{$row->{'measurement_type'}} += $row->{'specimens_measured'} * $row->{'real_average'};
        # Note that "average" is the geometric mean - a_mean (arithmetic mean) is not used right now
        $part_type->{'specimens_measured'} += $row->{'specimens_measured'};
        $part_type->{'average'} += $row->{'specimens_measured'} * log($row->{'real_average'});
        if ($row->{'specimens_measured'} == 1) {
            unless ($part_type->{'average_only'}) {
                if ( $part_type->{'min'} == 0  || ( $row->{'real_average'} > 0 && $row->{'real_average'} < $part_type->{'min'} ) )	{
                    $part_type->{'min'} = $row->{'real_average'};
                }
                if ( $row->{'real_average'} > $part_type->{'max'} )	{
                    $part_type->{'max'} = $row->{'real_average'};
                }
            }
        } else {
            if ( $part_type->{'min'} == 0 || ( $row->{'real_min'} > 0 && $row->{'real_min'} < $part_type->{'min'} ) )	{
                $part_type->{'min'} = $row->{'real_min'};
            }
            if ( $row->{'real_max'} > $part_type->{'max'} )	{
                $part_type->{'max'} = $row->{'real_max'};
            }
            if ($row->{'real_average'} =~ /\d/ && $row->{'real_min'} !~ /\d/ && $row->{'real_max'} !~ /\d/) {
                $part_type->{'average_only'} = 1;
            }
        }
    }

    while (my ($part,$m_table) = each %p_table) {
        foreach my $type (keys %types) {
            if ($m_table->{$type}{'specimens_measured'}) {
                $m_table->{$type}{'average'} = exp($m_table->{$type}{'average'}/$m_table->{$type}{'specimens_measured'});
                # if any averages were used in finding the min and max, the
                #  values are statistically bogus and should be erased
                # likewise if the sample size is 1
                if ( $m_table->{$type}{'average_only'} == 1 || $m_table->{$type}{'specimens_measured'} == 1 )	{
                    $m_table->{$type}{'min'} = "";
                    $m_table->{$type}{'max'} = "";
                }
            }
        }
   
        my @values = ();
        my $can_compute = 0; # Can compute median, and error (std dev)
        my $is_group = 0; # Is it aggregate group data or a bunch of singles?
        if ($unique_specimen_nos{$part} == 1) {
            if ($m_table->{'specimens_measured'} > 1) {
                $can_compute = 1;
                $is_group = 1;
            }
        } elsif ($unique_specimen_nos{$part} >= 1 && $unique_specimen_nos{$part} == $m_table->{'specimens_measured'}) {
            # This will only happen if the specimens_measured for each row is 1 above
            $can_compute = 1;
        }
    
        if ($can_compute) {
            my @measurements_for_part = ();
            foreach my $row (@measurements) {
                if ($row->{'specimen_part'} eq $part) {
                    push @measurements_for_part,$row;
                }
            }
            if ($is_group) {
                foreach my $row (@measurements_for_part) {
                    $m_table->{$row->{'measurement_type'}}{'median'} = $row->{'real_median'};
                    $m_table->{$row->{'measurement_type'}}{'error'} = $row->{'real_error'};
                    $m_table->{$row->{'measurement_type'}}{'error_unit'} = $row->{'error_unit'};
                }
            } else {
                my %values_by_type;
                foreach my $row (@measurements_for_part) {
                    push @{$values_by_type{$row->{'measurement_type'}}},$row->{'real_average'};
                }
                while (my ($type,$values_array_ref) = each %values_by_type) {
                    @values = sort {$a <=> $b} @$values_array_ref;
                    if (@values) {
                        if (scalar(@values) % 2 == 0) {
                            my $middle_index = int(scalar(@values)/2);
                            my $median = ($values[$middle_index] + $values[$middle_index-1])/2;
                            $m_table->{$type}{'median'} = $median;
                        } else {
                            my $middle_index = int(scalar(@values/2));
                            $m_table->{$type}{'median'} = $values[$middle_index];
                        }
                    }
                    if (scalar(@values) > 1) {
                        $m_table->{$type}{'error'} = std_dev(@values);
                        $m_table->{$type}{'error_unit'} = "1 s.d.";
                    }
                }
            }
        }   
    }

    return \%p_table;
}

sub std_dev {
    my @set = @_;

    my $var = variance(@set);
    return ($var**(1/2));
}

sub variance {
    my @set = @_;

    my $mean = avg(@set);
    my $sum = 0;
    $sum += (($_ - $mean)**2) for @set;
    $sum = $sum/(scalar(@set)-1);
    return $sum;
}

sub avg {
    my @set = @_;
    my $sum = 0;
    $sum += $_ for @set;
    $sum = $sum/(scalar(@set));
    return $sum;
}

# JA 7.12.10
# stolen from TaxonInfo::displayMeasurements, but greatly simplified with double join on
#  taxa_tree_cache, also includes diameter and circumference, deals with multiple parent and/or
#  "minus" taxa in equations table, and also computes mean
sub getMassEstimates	{
	my ($dbt,$taxon_no,$p_table) = @_;
	my %distinct_parts = ();

	while (my($part,$m_table)=each %$p_table)	{
		if ( $part !~ /^(p|m)(1|2|3|4)$/i )	{
			$distinct_parts{$part}++;
		}
	}
	my @part_list = keys %distinct_parts;
	@part_list = sort { $a cmp $b } @part_list;
	# mammal tooth measurements should always be listed in this fixed order
	unshift @part_list , ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");

	# first get equations
	# join on taxa_tree_cache because we need to know which parents are
	#  the least inclusive
	# don't do this with a join on taxa_list_cache because that table
	#  is nightmarishly large
	# note that we are finding all equations including $taxon_no based either on
	#  taxon_no or minus_taxon_no, then finding the least inclusive of these groups
	#  (based on ORDER BY) and determining whether this group is a "minus" or not
	#  (based on SELECT FIND_IN_SET...)
	my $sql = "SELECT FIND_IN_SET(t2.taxon_no,e.minus_taxon_no) minus,taxon_name,t2.lft,e.reference_no,part,length,width,area,diameter,circumference,intercept FROM authorities a,equations e,refs r,$TAXA_TREE_CACHE t,$TAXA_TREE_CACHE t2 WHERE t.taxon_no=$taxon_no AND a.taxon_no=t2.taxon_no AND e.reference_no=r.reference_no AND t.lft>t2.lft AND t.rgt<t2.rgt AND (FIND_IN_SET(t2.taxon_no,e.taxon_no) OR FIND_IN_SET(t2.taxon_no,e.minus_taxon_no)) GROUP BY eqn_no ORDER BY t2.lft DESC,r.pubyr DESC";
	my @eqn_refs = @{$dbt->getData($sql)};

	my (@values,@masses,@eqns,@refs);
	my (%mean,%estimates);
	for my $part ( @part_list )	{
		my $m_table = %$p_table->{$part};
		if ( ! $m_table )	{
			next;
		}
		foreach my $type (('length','width','area','diameter','circumference')) {
			if ( $type eq "area" && $m_table->{length}{average} && $m_table->{width}{average} && $part =~ /^[PMpm][1234]$/ )	{
				$m_table->{area}{average} = $m_table->{length}{average} * $m_table->{width}{average};
			}
			if ( $m_table->{$type}{'average'} > 0 ) {
				my $value = $m_table->{$type}{'average'};
				if ( $value < 1 )	{
					$value = sprintf("%.3f",$value);
				} elsif ( $value < 10 )	{
					$value = sprintf("%.2f",$value);
				} else	{
					$value = sprintf("%.1f",$value);
				}
				push @values , "$part $type $value";
				my $last_lft = "";
				foreach my $eqn ( @eqn_refs )	{
					if ( $part eq $eqn->{'part'} && $eqn->{$type} && ! $eqn->{'minus'} )	{
						if ( $eqn->{'lft'} < $last_lft && $last_lft )	{
							last;
						}
						$last_lft = $eqn->{'lft'};
						my $mass = exp( ( log($m_table->{$type}{average}) * $eqn->{$type} ) + $eqn->{intercept} );
						$mean{$type.$part} += log($mass);
						$estimates{$type.$part}++;
						push @masses , $mass;
						push @eqns , "$eqn->{taxon_name} $part $type";
						push @refs , $eqn->{'reference_no'};
					}
				}
			}
		}
	}
	my ($grandmean,$grandestimates);
	for my $m ( keys %mean )	{
		$grandmean += $mean{$m} / $estimates{$m};
		$grandestimates++;
	}
	return (\@part_list,\@values,\@masses,\@eqns,\@refs,$grandmean,$grandestimates);
}
		
# JA 25-29.7.08
sub displayDownloadMeasurementsResults  {
	my $q = shift;
	my $s = shift;
	my $dbt = shift; 
	my $dbh = $dbt->dbh;

	if ( ! $q->param('taxon_name') ) 	{
		my $errorMessage = '<center><p class="medium"><i>You must enter the name of a taxonomic group.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	my $sep;
	if ( $q->param('output_format') eq "csv" )	{
		$sep = ",";
	} else	{
		$sep = "\t";
	}

	my $sql = "SELECT t.taxon_no,lft,rgt,rgt-lft width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND (taxon_name='".$q->param('taxon_name')."' OR common_name ='".$q->param('taxon_name')."') ORDER BY width DESC"; 
	# if there are multiple matches, we hope to get the right one by
	#  assuming the larger taxon is the legitimate one
	my @taxa = @{$dbt->getData($sql)};
	if ( ! @taxa ) 	{
		my $errorMessage = '<center><p class="medium"><i>The taxon '.$q->param('taxon_name').' is not in our database. Please try another name.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	my @fields = ('synonym_no','spelling_no','a.taxon_no','taxon_name');;
	if ( $q->param('authors') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.author1last,a.author1last) a1";
		push @fields , "IF(ref_is_authority='YES',r.author2last,a.author2last) a2";
		push @fields , "IF(ref_is_authority='YES',r.otherauthors,a.otherauthors) oa";
	}
	if ( $q->param('year') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.pubyr,a.pubyr) pubyr";
	}
	for my $f ( 'authorizer','type_specimen','type_body_part','type_locality','extant')	{
		if ( $q->param($f) =~ /y/i )	{
			push @fields , $f;
		}
	}

	# now extract and average the measurements as follows:
	#  (1) find species in the taxonomic group
	#  (2) get measurements of these species (getMeasurements)
	#  (3) save measurements of species found in a subset of collections
	#  (4) take averages (getMeasurementTable)

	# step 1

	# the query will get valid species only
	$sql = "SELECT ".join(',',@fields)." FROM authorities a,$TAXA_TREE_CACHE t WHERE taxon_rank='species' AND a.taxon_no=t.taxon_no AND lft>=".$taxa[0]->{lft}." AND rgt<=".$taxa[0]->{rgt}." ORDER BY taxon_name ASC";
	if ( $q->param('authors') =~ /y/i || $q->param('year') =~ /y/i )	{
		$sql = "SELECT ".join(',',@fields)." FROM authorities a,refs r,$TAXA_TREE_CACHE t WHERE taxon_rank='species' AND a.reference_no=r.reference_no AND a.taxon_no=t.taxon_no AND lft>=".$taxa[0]->{lft}." AND rgt<=".$taxa[0]->{rgt}." ORDER BY taxon_name ASC";
	}

	my @refs = @{$dbt->getData($sql)};

	# group synonyms for each valid species
	my %valid_no;
	if ( $q->param('replace_with_ss') =~ /y/i )	{
		$valid_no{$_->{taxon_no}} = $_->{synonym_no} foreach @refs;
	# or else don't, but make sure only one spelling is used per synonym
	} else	{
		$valid_no{$_->{taxon_no}} = $_->{spelling_no} foreach @refs;
	}
	my @taxon_list = keys %valid_no;

	# step 2

	my @measurements = getMeasurements($dbt,'taxon_list'=>\@taxon_list,'get_global_specimens'=>1);
	if ( ! @measurements ) 	{
		my $errorMessage = '<center><p class="medium"><i>We have no measurement data for species belonging to '.$q->param('taxon_name').'. Please try another taxon.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# step 3

	my $collections;
	if ( $q->param('collection_names') =~ /^[A-Za-z0-9]/i )	{
		$collections = $q->param('collection_names');
		$collections =~ s/[^A-Za-z0-9 :;,\.\-\(\)\'"]//g;
		if ( $collections =~ /^[0-9 ,]+$/ )	{
			$collections =~ s/ /,/g;
			while ( $collections =~ /,,/ )	{
				$collections =~ s/,,/,/g;
			}
			$collections = "c.collection_no IN (".$collections.")";
		} else	{
			$collections =~ s/\'/\\\'/g;
			$collections = "(collection_name LIKE ('%".$collections."%') OR collection_aka LIKE ('%".$collections."%') )";
		}
	}
	my $countries;
	for my $continent ( 'Africa','Antarctica','Asia','Australia','Europe','North America','South America' )	{
		if ( $q->param($continent) !~ /y/i )	{
			my $d = Download->new($dbt,$q,$s,$hbo);
			$countries = $d->getCountryString();
			last;
		}
	}
	my $continents;
	for my $continent ( 'Africa','Antarctica','Asia','Australia','Europe','North America','South America' )	{
		if ( $q->param($continent) =~ /y/i )	{
			$continents .= ", ".$continent;
		}
	}
	$continents =~ s/^, //;
	my $interval_nos;
	if ( $q->param('max_interval') =~ /^[A-Z][a-z]/i )	{
		require TimeLookup;
		my $t = new TimeLookup($dbt);
	# eml_max and min aren't on the form yet
		my ($intervals,$errors,$warnings) = $t->getRange('',$q->param('max_interval'),'',$q->param('min_interval'));
		$interval_nos = join(',',@$intervals);
	}
	my $strat_unit;
	if ( $q->param('group_formation_member') =~ /^[A-Z]/i )	{
		$strat_unit = $q->param('group_formation_member');
		$strat_unit =~ s/\'/\\\'/g;
		$strat_unit = "(geological_group='".$strat_unit."' OR formation='".$strat_unit."' OR member='".$strat_unit."')";
	}

	my %by_valid;
	if ( $collections || $countries || $interval_nos || $strat_unit )	{
	# it's actually faster to get the occurrences and reIDs separately
	#  from the measurements instead of doing a nightmare five-table
	#  join in getMeasurements
	# it's also faster get the reIDs and then hit occurrences with an out
	#  list instead of using a left join to get only the occurrences without
	#  reIDs, as done by getCollections
	# thank goodness we store collection_no in reidentifications, even
	#  though technically it's redundant
		my $sql1 = "SELECT occurrence_no,taxon_no FROM collections c,reidentifications re WHERE c.collection_no=re.collection_no AND taxon_no IN (".join(',',@taxon_list).")";
		my $sql2;
		if ( $collections )	{
			$sql2 .= " AND ".$collections;
		}
		if ( $interval_nos )	{
			$sql2 .= " AND max_interval_no IN (".$interval_nos.") AND min_interval_no IN (".$interval_nos.",0)";
		}
		if ( $countries )	{
			$sql2 .= " AND ".$countries;
		}
		if ( $strat_unit )	{
			$sql2 .= " AND ".$strat_unit;
		}
		my @occrefs = @{$dbt->getData($sql1.$sql2)};
		my $sql1 = "SELECT occurrence_no,taxon_no FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND taxon_no IN (".join(',',@taxon_list).")";
		my %temp;
		$temp{$_->{'occurrence_no'}}++ foreach @occrefs;
		if ( %temp )	{
			$sql1 .= "AND occurrence_no NOT IN (".join(',',keys %temp).")";
		}
		push @occrefs , @{$dbt->getData($sql1.$sql2)};
		if ( ! @occrefs )	{
			my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		my %avail;
		$avail{$_->{'taxon_no'}}++ foreach @occrefs;
		undef @occrefs;
		my %sampled;
	# which measured species are sampled anywhere in this collection set?
		for my $m ( @measurements )	{
			if ( $avail{$m->{'taxon_no'}} )	{
				$sampled{$valid_no{$m->{'taxon_no'}}}++;
			}
		}
		undef %avail;
	# go through it again because many measurements are not tied to any
	#  collection at all
	# we end up with all measurements grouped by valid species name
		for my $m ( @measurements )	{
			if ( $sampled{$valid_no{$m->{'taxon_no'}}} )	{
				push @{$by_valid{$valid_no{$m->{'taxon_no'}}}} , $m;
			}
		}
	} else	{
		for my $m ( @measurements )	{
			push @{$by_valid{$valid_no{$m->{'taxon_no'}}}} , $m;
		}
	}


	my @header_fields = ('species');
	my %name;
	$name{$_->{'taxon_no'}} = $_->{'taxon_name'} foreach @refs;
	my %authors;
	if ( $q->param('authors') =~ /y/i )	{
		for my $r ( @refs )	{
			$r->{'a1'} =~ s/,.*//;
			$r->{'a2'} =~ s/,.*//;
			if ( $r->{'oa'} ) { $r->{a2} = " et al."; }
			else { $r->{'a2'} =~ s/^([A-Za-z])/ and $1/; }
			$authors{$r->{'taxon_no'}} = $r->{'a1'}.$r->{'a2'};
			if ( ! $authors{$r->{'taxon_no'}} ) { $authors{$r->{'taxon_no'}} = "?" }
		}
		push @header_fields , "authors";
	}
	my %year;
	if ( $q->param('year') =~ /y/i )	{
		$year{$_->{'taxon_no'}} = $_->{'pubyr'} ? $_->{'pubyr'} : "?" foreach @refs;
		push @header_fields , "year published";
	}
	my %type;
	if ( $q->param('type_specimen') =~ /y/i )	{
		$type{$_->{'taxon_no'}} = $_->{'type_specimen'} ? $_->{'type_specimen'} : "?" foreach @refs;
		push @header_fields , "type specimen";
	}
	my %type_part;
	if ( $q->param('type_body_part') =~ /y/i )	{
		$type_part{$_->{'taxon_no'}} = $_->{'type_body_part'} ? $_->{'type_body_part'} : "?" foreach @refs;
		push @header_fields , "type body part";
	}
	my %locality;
	if ( $q->param('type_locality') =~ /y/i )	{
		$locality{$_->{'taxon_no'}} = $_->{'type_locality'} ? $_->{'type_locality'} : "?" foreach @refs;
		push @header_fields , "type locality number";
	}
	my %extant;
	if ( $q->param('extant') =~ /y/i )	{
		$extant{$_->{'taxon_no'}} = $_->{'extant'} ? $_->{'extant'} : "?" foreach @refs;
		push @header_fields , "extant";
	}
	push @header_fields , ('part','measurement');
	if ( $q->param('specimens_measured') =~ /y/i )	{
		push @header_fields , "specimens measured";
		push @columns , "specimens_measured";
	}
	push @header_fields , "mean";
	my @columns = ('average');
	for my $c ('min','max','median','error')	{
		if ( $q->param($c) =~ /y/i )	{
			push @header_fields , $c;
			push @columns , $c;
		}
	}
	if ( $q->param('error') =~ /y/i )	{
		push @header_fields , "error unit";
		push @columns , "error_unit";
	}

	# step 4

	my $OUT_HTTP_DIR = "/public/downloads";
	my $OUT_FILE_DIR = $HTML_DIR.$OUT_HTTP_DIR;
	my $name = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
	my $outfile = PBDBUtil::getFilename($name)."-size.txt";
	open OUT,">$OUT_FILE_DIR/$outfile";
	my $header = join($sep,@header_fields);
	print OUT $header,"\n";

	my %tables;
	my @with_data;
	foreach my $t ( @refs )	{
		if ( ( $q->param('replace_with_ss') =~ /y/i && $t->{taxon_no} == $t->{synonym_no} ) || ( $q->param('replace_with_ss') !~ /y/i && $t->{taxon_no} == $t->{spelling_no} ) )	{
			my $vn = $valid_no{$t->{taxon_no}};
			if ( ! $by_valid{$vn} )	{
				next;
			}
			my $p_table = getMeasurementTable(\@{$by_valid{$vn}});
			$tables{$vn} = $p_table;
			push @with_data , $vn;
		}
	}


	# much of this section is lifted from TaxonInfo::displayMeasurements
	# however, rewriting it would be a pain because that version focuses on
	#  creating an HTML table for exactly one taxon at a time
	my %records;
	my %specimens;
	my $rows;
	my @part_list;
	my %distinct_parts = ();
	for my $taxon_no ( @with_data )	{
		my $p_table = $tables{$taxon_no};
		while (my($part,$m_table)=each %$p_table)	{
			if ( $part !~ /^(p|m)(1|2|3|4)$/i )	{
				$distinct_parts{$part}++;
			}
		}
	}

	# this is slightly inefficient because getMeasurementTable has returned all parts,
	#  but that function is fast enough that it's not worth rewriting to only return
	#  part_list
	if ( $q->param('part_list') )	{
		@part_list = split /[^A-Za-z0-9 ]/,$q->param('part_list');
		s/^[ ]+// foreach @part_list;
		s/[ ]+$// foreach @part_list;
	} else	{
		@part_list = keys %distinct_parts;
		@part_list = sort { $a cmp $b } @part_list;
		unshift @part_list , ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");
	}

	my $types;
	for my $type (('length','width','height','circumference','diagonal','inflation'))	{
		if ( $q->param($type) =~ /y/i )	{
			$types++;
		}
	}
	my (%measured_parts,%measured_types);
	for my $taxon_no ( @with_data )	{
		my $measured_parts = 0;
		my $p_table = $tables{$taxon_no};
		for my $part ( @part_list )	{
			my $m_table = %$p_table->{$part};
			if ( $m_table )	{
				for my $type (('length','width','height','circumference','diagonal','inflation'))	{
					if ( $m_table->{$type} && $q->param($type) =~ /y/i && $m_table->{$type}{'average'} > 0 )	{
						$measured_parts{$taxon_no}++;
						$measured_types{$taxon_no}{$part}++;
					}
				}
			}
		}
	}

	my %printed_parts;
	for my $taxon_no ( @with_data )	{
		if ( $q->param('all_parts') =~ /y/i && $measured_parts{$taxon_no} < ( $#part_list + 1 ) * $types )	{
			next;
		}
		my $p_table = $tables{$taxon_no};
		for my $part ( @part_list )	{
			if ( $measured_types{$taxon_no}{$part} < $types )	{
				next;
			}
			$printed_parts{$taxon_no}++;
			my $m_table = %$p_table->{$part};
			if ( $m_table )	{
				$printed_part = $part;
				if ( $part eq "" )	{
					$printed_part = "unknown";
				}
				$records{$part}++;
				$specimens{$part} += $m_table->{'specimens_measured'};
				$rows++;
				for my $type (('length','width','height','circumference','diagonal','inflation'))	{
					if ( $m_table->{$type} && $q->param($type) =~ /y/i && $m_table->{$type}{'average'} > 0 )	{
						if ( $sep =~ /,/ )	{
							print OUT "\"$name{$taxon_no}\"",$sep;
						} else	{
							print OUT $name{$taxon_no},$sep;
						}
						if ( $q->param('authors') =~ /y/i && $authors{$taxon_no} =~ / / && $sep =~ /,/ )	{
							print OUT '"',$authors{$taxon_no},'"',$sep;
						} elsif ( $q->param('authors') =~ /y/i )	{
							print OUT $authors{$taxon_no},$sep;
						}
						if ( $q->param('year') =~ /y/i )	{
							print OUT $year{$taxon_no},$sep;
						}
						if ( $q->param('type_specimen') =~ /y/i )	{
							print OUT $type{$taxon_no},$sep;
						}
						if ( $q->param('type_body_part') =~ /y/i && $type_part{$taxon_no} =~ / / && $sep =~ /,/ )	{
							print OUT '"',$type_part{$taxon_no},'"',$sep;
						} elsif ( $q->param('type_body_part') =~ /y/i )	{
							print OUT $type_part{$taxon_no},$sep;
						}
						if ( $q->param('type_locality') =~ /y/i )	{
							print OUT $locality{$taxon_no},$sep;
						}
						if ( $q->param('extant') =~ /y/i )	{
							print OUT $extant{$taxon_no},$sep;
						}
						print OUT $printed_part,$sep,$type;
						if ( $q->param('specimens_measured') =~ /y/i )	{
							print OUT $sep,$m_table->{'specimens_measured'};
						}
						foreach my $column ( @columns )	{
							my $value = $m_table->{$type}{$column};
							print OUT $sep;
							if ( $column eq "error_unit" )	{
								print OUT $value;
							} elsif ( $value <= 0 )	{
								print OUT "NaN";
							} elsif ( $value < 1 )	{
								printf OUT "%.3f",$value;
							} elsif ( $value < 10 )	{
								printf OUT "%.2f",$value;
							} else	{
								printf OUT "%.1f",$value;
							}
		
						}
						print OUT "\n";
					}
				}
			}
		}
	}
	close OUT;
	if ( $rows < 1 )	{
		my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}
	print "<div style=\"margin-left: 10em; margin-bottom: 5em;\">\n\n";
	print "<p class=\"pageTitle\" style=\"margin-left: 8em;\">Download results</p>\n";
	print "<p class=\"darkList\" style=\"width: 30em; padding: 0.1em; padding-left: 3em;\">Summary</p>\n";
	print "<div style=\"margin-left: 4em;\">\n\n";
	print "<p style=\"width: 26em; margin-left: 1em; text-indent: -1em;\">Search: taxon = ",$q->param('taxon_name');
	if ( $q->param('collection_names') )	{
		print "; collection = ",$q->param('collection_names');
	}
	if ( $countries )	{
		if ( $continents =~ /, / )	{
			print "; continents = ",$continents;
		} else	{
			print "; continent = ",$continents;
		}
	}
	if ( $q->param('max_interval') )	{
		print "; interval = ",$q->param('max_interval');
	}
	if ( $q->param('min_interval') )	{
		print " to ",$q->param('min_interval');
	}
	if ( $q->param('group_formation_member') )	{
		print "; strat unit = ",$q->param('group_formation_member');
	}
	print "</p>\n";
	my @temp = keys %records;
	if ( $#temp == 0 )	{
		printf "<p>%d kind of body part</p>\n",$#temp+1;
	} else	{
		printf "<p>%d kinds of body parts</p>\n",$#temp+1;
	}
	my @temp = keys %printed_parts;
	printf "<p>%d species</p>\n",$#temp+1;
	if ( $rows == 1 )	{
		print "<p>$rows data record</p>\n";
	} else	{
		print "<p>$rows data records</p>\n";
	}
	print "<p>The data were saved to <a href=\"$OUT_HTTP_DIR/$outfile\">$outfile</a></p>\n";
	print "</div>\n\n";
	print "<p class=\"darkList\" style=\"width: 30em; margin-top: 3em; padding: 0.1em; padding-left: 3em;\">Data totals for each body part</p>\n";
	print "<table cellpadding=\"4\" style=\"margin-left: 6em;\">\n";
	print "<tr><td align=\"center\">part</td><td>species</td><td>specimens</td></tr>\n";
	for my $part ( @part_list )	{
		if ( $records{$part} )	{
			my $printed_part = $part;
			if ( $part eq "" )	{
				$printed_part = "unknown";
			}
			print "<tr><td style=\"padding-left: 1em;\">$printed_part</td> <td align=\"center\">$records{$part}</td> <td align=\"center\">$specimens{$part}</td></tr>\n";
		}
	}
	print "</table>\n";
	print "</div>\n";

}   


1;
