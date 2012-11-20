package Measurement;
use Data::Dumper;
use CGI::Carp;
use TaxaCache;
use Ecology;
use Reference;
use Download;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $TAXA_TREE_CACHE);


# written by PS 6/22/2005 - 6/24/2005
# Handle display and processing of form to enter measurements for specimens

my @specimen_fields   =('specimens_measured','specimen_coverage','specimen_id','specimen_side','sex','specimen_part','measurement_source','magnification','is_type','comments');
my @measurement_types =('mass','length','width','height','circumference','diagonal','diameter','inflation','d13C','d18O');
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
    my (@taxa,@results,$taxon_nos);
    if ($q->param('taxon_name')) {
        if ( $q->param('match_type') =~ /exact|combinations only/ )	{
            my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='".$q->param('taxon_name')."'";
            @taxa = @{$dbt->getData($sql)};
        } else	{
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$q->param('taxon_name'),'match_subgenera'=>1});
        }
    } 
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
    if ( $q->param('get_occurrences') !~ /^no/i )	{
        if ($q->param('comment')) {
            $sql1 .= " AND o.comments LIKE '%".$q->param('comment')."%'";
            $sql2 .= " AND (o.comments LIKE '%".$q->param('comment')."%' OR re.comments LIKE '%".$q->param('comment')."%')";
        }
        if ($q->param('collection_no')) {
            $sql1 .= " AND o.collection_no=".int($q->param('collection_no'));
            $sql2 .= " AND o.collection_no=".int($q->param('collection_no'));
        }
        $sql1 .= " GROUP BY o.occurrence_no";
        $sql2 .= " GROUP BY o.occurrence_no";
        $sql .= "($sql1) UNION ($sql2) ORDER BY collection_no";
        @results = @{$dbt->getData($sql)};
    }

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
    } elsif (scalar(@results) == 0 && scalar(@results_taxa_only) == 1) {
        $q->param('taxon_no'=>$results_taxa_only[0]->{'taxon_no'});
        displaySpecimenList($dbt,$hbo,$q,$s);
    } elsif (scalar(@results) == 0 && scalar(@results_taxa_only) == 0) {
        push my @error , "Occurrences or taxa matching these search terms could not be found";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    } else {
        my ($things,$collection_header,$knownUnknown) = ('occurrences and names','Collection','unknown');
        if( $q->param('get_occurrences') =~ /^no/i )	{
            $things = "taxonomic names";
            $collection_header = "";
            $knownUnknown = "";
        }
        print qq|
        <center><p class="pageTitle" style="font-size: 1.2em;">Matching $things</p></center>
        <form method="POST\" action="$READ_URL">
        <input type="hidden\" name="action" value="displaySpecimenList">
        <div align="center" class="small" style="margin: 1em;">
        <table cellspacing="0" cellpadding="6" style="border: 1px solid lightgray;">
        <tr><th><span style="margin-right: 1em">Occurrence</span></th><th>$collection_header</th><th>Measurements</th></tr>
|;
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
            print "<tr $class><td><a href=\"$READ_URL?action=displaySpecimenList&taxon_no=$row->{taxon_no}\">$taxon_name</a></td><td>$knownUnknown</td><td align=\"center\">$specimens</td></tr>";

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
    my ($dbt,$hbo,$q,$s,$called_from,$more_links) = @_;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
		push my @error , "There is no occurrence or classification of this taxon";
		print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
		exit;
	}

    my (@results,$taxon_name,$extant,$collection);
    if ($q->param('occurrence_no')) {
        @results = getMeasurements($dbt,{'occurrence_no'=>int($q->param('occurrence_no'))});
        $sql = "SELECT collection_no,genus_name,species_name,occurrence_no FROM occurrences WHERE occurrence_no=".int($q->param("occurrence_no"));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
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
        my $base_sql = "SELECT r.author1last,r.author2last,r.otherauthors,r.pubyr,s.*,m.*,s.taxon_no FROM refs r,specimens s, measurements m";
        if ($q->param('types_only')) {
            $sql = "($base_sql FROM specimens s, measurements m WHERE s.specimen_no=m.specimen_no AND s.taxon_no=$taxon_no AND s.is_type='holotype')"
                 . " UNION "
                 . "($base_sql, occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND o.taxon_no=$taxon_no AND re.reid_no IS NULL AND s.is_type='holotype')"
                 . " UNION "
                 . "($base_sql, reidentifications re WHERE s.occurrence_no=o.occurrence_no AND o.occurrence_no=re.occurrence_no AND s.specimen_no=m.specimen_no AND re.taxon_no=$taxon_no AND re.most_recent='YES' AND s.is_type='holotype')"
        } else {
            $sql = "$base_sql WHERE r.reference_no=s.reference_no AND s.specimen_no=m.specimen_no AND s.taxon_no=$taxon_no";
        }
        dbg("sql is $sql");
        @results = @{$dbt->getData($sql)};

        my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>int($q->param('taxon_no'))},[taxon_rank,taxon_name,extant]);
        if ($taxon->{'taxon_rank'} =~ /species/) {
            $taxon_name = $taxon->{'taxon_name'};
        } elsif ($taxon->{'taxon_rank'} =~ /genus/) {
            $taxon_name = $taxon->{'taxon_name'}." sp.";
        } else {
            $taxon_name = $taxon->{'taxon_name'}." indet.";
        }
        $extant = $taxon->{'extant'};
    }

    print qq|<center><div class="pageTitle" style="padding-top: 0.5em;"><span class="verysmall">$taxon_name $collection</span></div></center>
<center>
<div class="displayPanel verysmall" align="center" style="width: 40em; margin-top: 1em; padding-top: 1em;">
  $more_links
  <div class="displayPanelContent">
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

    my (%specimens,%types,%parts,$specimen_ids,$sexes);
    foreach my $row (@results) {
        $specimens{$row->{specimen_no}}{$row->{measurement_type}} = $row->{average};
        for my $field ( 'specimens_measured','specimen_part','author1last','author2last','otherauthors','pubyr' )	{
            $specimens{$row->{specimen_no}}{$field} = $row->{$field};
        }
        if ( $row->{specimen_id} )	{
            $specimens{$row->{specimen_no}}{'specimen_id'} = $row->{specimen_id};
            $specimens{$row->{specimen_no}}{'specimen_id'} .= ( $row->{sex} eq "female" ) ? ' (&#9792;)' : "";
            $specimens{$row->{specimen_no}}{'specimen_id'} .= ( $row->{sex} eq "male" ) ? ' (&#9794;)' : "";
            $specimen_ids++;
        } elsif ( $row->{sex} )	{
            $specimens{$row->{specimen_no}}{'specimen_id'} = ( $row->{sex} eq "female" ) ? '&#9792;' : "";
            $specimens{$row->{specimen_no}}{'specimen_id'} .= ( $row->{sex} eq "male" ) ? ' &#9794;' : "";
        }
        if ( $row->{sex} )	{
            $sexes++;
        }
        $types{$row->{measurement_type}}++;
        $parts{$row->{specimen_part}}++ if ($row->{specimen_part});
    }

    $specimen_count = scalar(keys(%specimens));

    if ($specimen_count == 0)	{
        if ($q->param('occurrence_no')) {
            print "<p class=\"small\">There are no measurements for this occurrence yet</p>\n";
        } else {
            print "<p class=\"small\">There are no measurements for $taxon_name yet</p>\n";
        }
    }

    print "<table style=\"margin-top: 0em;\">\n";

    # always give them an option to create a new measurement as well
    if ( $q->param('occurrence_no') )	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&occurrence_no=| . $q->param('occurrence_no') . qq|&specimen_no=-1"><span class="measurementBullet">&#149;</span></td>|;
        } else	{
            print qq|<tr><td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&taxon_no=| . $q->param('taxon_no') . qq|&specimen_no=-1"><span class="measurementBullet">&#149;</span></td>|;
        }
    print "<td colspan=\"6\">&nbsp;Add one new record (full form)</i></td></tr>\n";
    print qq|<tr><td align="center" valign="top"><a href="javascript:submitForm('')"><div class="measurementBullet" style="position: relative; margin-top: -0.1em;">&#149;</div></td>|;

    # anyone working on a large paper will enter the same type and source value
    #  almost every time, so grab the last one
    my (%checked,@part_values,$part_value,%part_used,%selected);
    my $sql = "SELECT occurrence_no,taxon_no,specimen_id,specimen_part,is_type,measurement_source,measurement_type FROM specimens s,measurements m WHERE s.specimen_no=m.specimen_no AND reference_no=".$s->get('reference_no')." ORDER BY s.specimen_no DESC LIMIT 100";
    my @last_entries = @{$dbt->getData($sql)};
    my ($old_records,$id);
    for $entry ( @last_entries )	{
        if ( $entry->{occurrence_no} != $last_entries[0]->{occurrence_no} || $entry->{taxon_no} != $last_entries[0]->{taxon_no} || $entry->{specimen_id} ne $last_entries[0]->{specimen_id} )	{
            last;
        }
        $old_records++;
        # a non-numeric ID may be a repeated bare museum code or what have you
        $id = ( $entry->{specimen_id} !~ /[0-9]/ ) ? $entry->{specimen_id} : "";
        $part_used{$entry->{specimen_part}}++;
        if ( $part_used{$entry->{specimen_part}} == 1 )	{
           unshift @part_values , $entry->{specimen_part};
        }
        $checked{$entry->{measurement_type}} = "checked";
    }
    $old_records--;
    $old_records = ( $old_records < 2 ) ? 10 : $old_records;

    # likewise, the same number of parts per taxon or occurrence may be
    #  entered every time
    print "<td colspan=\"6\" valign=\"top\">&nbsp;Add <input type=\"text\" name=\"specimens_measured\" value=\"$old_records\" size=3>new records</i><br>";

    $checked{'length'} = ( ! %checked ) ? "checked" : $checked{'length'};
    my $defaults = $last_entries[0];
    $selected{$defaults->{'is_type'}} = "selected";
    $selected{$defaults->{'measurement_source'}} = "selected";
    $part_value = 'value="'.join(', ',@part_values).'"';

    print qq|
  <div style=\"padding-left: 2em; padding-bottom: 1em;\">
  default specimen #: <input name="default_no" size="26" value="$id"><br>
  default side and sex:
  <select name="default_side">
  <option value=""></option>
  <option value="left">left</option>
  <option value="right">right</option>
  <option value="left?">left?</option>
  <option value="right?">right?</option>
  <option value="dorsal">dorsal</option>
  <option value="ventral">ventral</option>
  <option value="both">both</option>
  </select>
  <select name="default_sex"><option></option><option>female</option><option>male</option><option>both</option></select><br>
  default part: <input type="text" name="default_part" size="10" $part_value><br>
  sample size: <input type="text" name="N" size="10"><br>
  measurements:
  <input type="checkbox" name="length" $checked{'length'}> length 
  <input type="checkbox" name="width" $checked{'width'}> width
  <input type="checkbox" name="height" $checked{'height'}> height<br>
  <input type="checkbox" name="circumference" style="margin-left: 2em;" $checked{'circumference'}> circumference
  <input type="checkbox" name="diagonal" $checked{'diagonal'}> diagonal
  <input type="checkbox" name="diameter" $checked{'diameter'}> diameter<br>
|;
    if ( $extant =~ /y/i )	{
      print qq|
  <input type="checkbox" name="mass" style="margin-left: 2em;" $checked{'mass'}> mass\n|;
    }
    print qq|
  <input type="checkbox" name="d13C" style="margin-left: 2em;" $checked{'d13C'} > &delta;<sup>13</sup>C
  <input type="checkbox" name="d18O" style="margin-left: 2em;" $checked{'d18O'}> &delta;<sup>18</sup>O<br>
  default type:
  <select name="default_type">
  <option value="no">no</option>
  <option value="holotype" $selected{'holotype'}>holotype</option>
  <option value="paratype" $selected{'paratype'}>paratype</option>
  </select><br>
  default source:
  <select name="default_source">
  <option value=""></option>
  <option value="text" $selected{'text'}>text</option>
  <option value="table" $selected{'table'}>table</option>
  <option value="picture" $selected{'picture'}>picture</option>
  <option value="graph" $selected{'graph'}>graph</option>
  <option value="direct" $checked{'direct'}>direct</option>
  </select>
  <br>default magnification: <input name="default_magnification" size="10"><br>
  comments: <input type="text" name="default_comments" size="36"><br>
  </div>
</td></tr>
</table>
</div>
</div>
</center>
|;


    if ($specimen_count > 0) {
print qq|<center>
<div class="displayPanel verysmall" align="center" style="width: 40em; margin-top: 2em; padding-top: 1em; padding-bottom: 1em;">
<table class="verysmall" cellspacing="4">
<tr><td></td><td colspan="5" class="verylarge" style="padding-bottom: 0.5em;">Existing measurements</td></tr>
<tr>
<th></th>
|;
        if ( $specimen_ids > 0 )	{
            print "<th align=\"left\">specimen #</th>";
        } elsif ( $sexes > 0 )	{
            print "<th align=\"left\">sex</th>";
        }
        print "<th align=\"left\">part</th>" if (%parts);
        print "<th>count</th>";
        foreach my $type (@measurement_types) {
            if ($types{$type}) {
                print "<th>$type</th>";
            }
        }
        if ( $q->param('occurrence_no') == 0 )	{
            print "<th>reference</th>";
        }
        print "</tr>\n";
    }

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
        print "<tr>\n";
        if ( $q->param('occurrence_no') )	{
            print qq|<td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&occurrence_no=| . $q->param('occurrence_no') . qq|&specimen_no=$specimen_no">edit</a></td>|;
        } else	{
            print qq|<td align="center"><a href="$WRITE_URL?action=populateMeasurementForm&taxon_no=| . $q->param('taxon_no') . qq|&specimen_no=$specimen_no">edit</a></td>|;
        }
        if ( $specimen_ids > 0 || $sexes > 0 )	{
            print qq|<td>$row->{specimen_id}</td>|;
        }
        print qq|<td align="left">$row->{specimen_part}</td>| if (%parts);
        print qq|<td align="center">$row->{specimens_measured}</td>|;
        foreach my $type (@measurement_types) {
            if ($types{$type}) {
                print "<td align=\"center\">$row->{$type}</td>";
            }
        }
        my $auth = Reference::formatShortRef($row);
        $auth =~ s/ and / & /;
        print "<td>$auth</td>\n";
        print "</tr>";
    }
    if ($specimen_count > 0) {
        print "</table>\n</div>\n</center>\n";
    }
if ( $called_from eq "processMeasurementForm" )	{
	return;
}

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

    # prepare fields to be used in the form ahead
    my @values = ();
    my @fields = ();

    # new entry
    if ($q->param('specimen_no') < 0) {
        # users can only choose length and width position values right
        #  now because there's nothing to say about anything else
        #  JA 12.6.12
        my $sql = "SELECT measurement_type,position FROM specimens s,measurements m WHERE s.specimen_no=m.specimen_no AND position IS NOT NULL AND enterer_no=".$s->get('enterer_no')." AND measurement_type IN ('length','width') ORDER BY measurement_no ASC";
        my %last_position;
        $last_position{$_->{'measurement_type'}} = $_->{'position'} foreach @{$dbt->getData($sql)};
        if (!$s->get('reference_no')) {
            # Make them choose a reference first
            $s->enqueue($q->query_string());
            main::displaySearchRefs("Please choose a reference before adding specimen measurement data",1);
            return;
        } else {
            if ($q->param('specimen_no') == -1) {
                my @specimen_fields = ('taxon_no','specimens_measured','specimen_coverage','specimen_id','specimen_side','specimen_part','sex','measurement_source','magnification','is_type','comments');
                # get the data from the last record of this taxon entered
                #  JA 10.5.07
                # whoops, everything should come instead from the last record
                #  entered from the current ref JA 13.4.12
                my $fieldstring = "s.".join(',s.',@specimen_fields);
                $fieldstring =~ s/is_type/is_type AS specimen_is_type/;
                $sql = "SELECT $fieldstring,average AS last_average FROM specimens s,measurements m WHERE s.specimen_no=m.specimen_no AND reference_no=".$s->get('reference_no')." ORDER BY s.specimen_no DESC";
                $row = ${$dbt->getData($sql)}[0];
                # but if that fails, at least the part might be recoverable
                if ( ! $row )	{
                    $sql = "SELECT specimen_part FROM specimens WHERE $old_field=$old_no ORDER BY specimen_no DESC";
                    $row = ${$dbt->getData($sql)}[0];
                }
                # comments are usually taxon-specific
                elsif ( $q->param('taxon_no') != $row->{'taxon_no'} )	{
                    $row->{'comments'} = "";
                }
                if ( $row->{'specimen_part'} ne "body" )	{
                    push @fields,'length_position';
                    push @values,$last_position{'length'};
                    push @fields,'width_position';
                    push @values,$last_position{'width'};
                }
                push @fields,$_ for @specimen_fields;
                s/is_type/specimen_is_type/ foreach @fields;
                if ( ! $row->{'specimens_measured'} )	{
                    $row->{'specimens_measured'} = 1;
                }
                for my $f ( @specimen_fields )	{
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
                    push @values , ( $row->{'last_average'} >= 100 ) ? "kg" : "g";
                }

                my $sql = "SELECT author1init,author1last,author2init,author2last,pubyr,reftitle,pubtitle,pubvol,firstpage,lastpage FROM refs WHERE reference_no=".$s->get('reference_no');
                my $ref = ${$dbt->getData($sql)}[0];

	        push @fields,('reference','occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','types_only');
	        push @values,(Reference::formatLongRef($ref),int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,int($q->param('types_only')));
	        print $hbo->populateHTML('specimen_measurement_form_general', \@values, \@fields);
            } elsif ($q->param('specimen_no') == -2) {
                if ( $q->param('length') )	{
                    push @fields,'length_position';
                    push @values,$last_position{'length'};
                }
                if ( $q->param('width') )	{
                    push @fields,'width_position';
                    push @values,$last_position{'width'};
                }
		push (@fields,'occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','specimen_coverage');
		push (@values,int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,'');
		my ($column_names,$inputs);
		if ( $q->param('N') > 0 )	{
			$column_names = qq|<th><span class="small">N</span></th>|;
			$inputs = qq|  <td><input type="text" name="specimens_measured" value="|.$q->param('N').qq|" size="3" class="tiny"></td>\n|;
		} else	{
			$inputs = qq|  <input type="hidden" name="specimens_measured" value="1">\n|;
		}
		for my $c ( 'mass','length','height','width','circumference','diagonal','diameter','d13C','d18O')	{
			if ( $q->param($c) )	{
				my $cn = $c;
				if ( $c !~ /13C|18O/ )	{
					my @c = split //,$c;
					$c[0] =~ tr/[a-z]/[A-Z]/;
					$cn = join('',@c);
				}
				$cn =~ s/Circumference/Circumf./;
				$cn =~ s/Mass/Mass (g)/;
				$cn =~ s/d13C/&delta;<sup>13<\/sup>C/;
				$cn =~ s/d18O/&delta;<sup>18<\/sup>O/;
				my $average = $c."_average";
				$column_names .= qq|<th><span class="small">$cn</span></th>|;
				$inputs .= qq|  <td><input type="text" name="$average" size=7 class="tiny"></td>
|;
			}
		}
                my $table_rows = "";
                my @default_parts = split /, /,$q->param('default_part');
                my $s = 0;
                for (1..$q->param('specimens_measured')) {
                    $s = ( $s > $#default_parts ) ? 0 : $s;
                    $table_rows .= $hbo->populateHTML('specimen_measurement_form_row',[$q->param('default_no'),$inputs,$q->param('default_side'),$q->param('default_sex'),$default_parts[$s],$q->param('default_type'),$q->param('default_source'),$q->param('default_magnification'),$q->param('default_comments')],['specimen_id','inputs','specimen_side','sex','specimen_part','specimen_is_type','measurement_source','magnification','comments']);
                    $s++;
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

        for my $type (@measurement_types) {
            for my $f (@measurement_fields) {
                push @fields, $type."_".$f;
                if (exists $m_table{$type}) {
                    push @values, $m_table{$type}{$f};
                } else {
                    push @values, '';
                }
            }
        }
        # special handling needed because only length and width have meaningful
        #  values and _position isn't/shouldn't be in @measurement_fields
        # JA 12.6.12
        for my $type ( 'length','width' )	{
            if ( $m_table{$type}{'position'} )	{
                push @fields , $type."_position";
                push @values , $m_table{$type}{'position'};
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

        # allow users to swap records between occurrences or from general
        #  taxon-specific records to particular occurrences
	# exact match on genus and species is required to avoid printing tons of
	#  irrelevant records
	my ($g,$s) = split / /,$taxon_name;
	my $sql = "(SELECT collection_name,occurrence_no FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND genus_name='$g' AND species_name='$s') UNION (SELECT collection_name,occurrence_no FROM collections c,reidentifications r WHERE c.collection_no=r.collection_no AND genus_name='$g' AND species_name='$s') ORDER BY collection_name";
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
        return;
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

    my ($updated_row_count,$inserted_row_count);
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

        # make sure at least one of these fields is set
        my $averages;
        for my $dim ( @measurement_types )	{
            if ( $fields{$dim.'_average'} > 0 )	{
                $averages++;
            } else	{
                $fields{$dim.'_average'} =~ s/[^0-9\.]//g;
            }
        }
        if ( $averages == 0 )	{
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

        # whoops JA 3.6.12
        $fields{'specimen_id'} =~ s/'/\'/;
        $fields{'comments'} =~ s/'/\'/;

        my $specimen_no;

        # prevent duplication by resubmission of form for "new" specimen
        #  JA 21.12.10
        # this is pretty conservative...
        # made less conservative by requiring matches on the comments and
	#  all measurements when there is no specimen ID JA 7-8.9.11
        # and even less conservative (whoops) by requiring a taxon_no or
        #  occurrence_no match even when specimen_id is known JA 1.3.12
        # don't forget sex JA 2.5.12
        my $sex = ( $fields{'sex'} =~ /male/i ) ? "'".$fields{'sex'}."'" : "NULL";
        if ( $fields{'specimen_no'} <= 0 && $fields{'specimen_id'} =~ /[A-Za-z0-9]/ )	{
            $sql = "SELECT specimen_no FROM specimens WHERE ((taxon_no=".$fields{'taxon_no'}." AND taxon_no>0) OR (occurrence_no=".$fields{'occurrence_no'}." AND occurrence_no>0)) AND specimen_id='".$fields{'specimen_id'}."' AND specimen_id IS NOT NULL AND BINARY specimen_part='".$fields{'specimen_part'}."' AND sex=$sex LIMIT 1";
            $fields{'specimen_no'} = ${$dbt->getData($sql)}[0]->{'specimen_no'};
        } elsif ( $fields{'specimen_no'} <= 0 )	{
            $sql = "SELECT m.specimen_no,m.measurement_type,m.average,comments FROM specimens s,measurements m WHERE s.specimen_no=m.specimen_no AND ((specimen_id IS NULL AND taxon_no=".$fields{'taxon_no'}." AND taxon_no>0) OR (specimen_id IS NULL AND occurrence_no=".$fields{'occurrence_no'}." AND occurrence_no>0)) AND BINARY specimen_part='".$fields{'specimen_part'}."' AND sex=$sex LIMIT 1";
            my $match = ${$dbt->getData($sql)}[0];
            # preliminary match
            $fields{'specimen_no'} = $match->{'specimen_no'};
            # bomb out if there is any mismatch involving key fields
            if ( $fields{'comments'} ne $match->{'comments'} )	{
                delete $fields{'specimen_no'};
            }
            for my $type ( @measurement_types )	{
                if ( $fields{$type."_average"} != $match->{'average'} && $fields{$type."_average"} > 0 && $match->{'average'} > 0 && $type eq $match->{'measurement_type'} )	{
                    delete $fields{'specimen_no'};
                    last;
                }
            }
        }

        if ( $fields{'specimen_no'} > 0 )	{
            delete $fields{'taxon_no'}; # keys, never update these
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
                $in_db{$_->{'measurement_type'}} = $_ foreach @measurements;

                my %in_cgi = (); # Find rows from filled out form
                foreach my $type (@measurement_types) {
                    foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                        if ($fields{$type."_".$f}) {
                            $in_cgi{$type}{$f} = $fields{$type."_".$f};
                        } 
                    }
                }
                # check to see if the user is simply trying to switch
                #  measurement types on a single record JA 20.5.12
                my @dbs = keys %in_db;
                my @cgis = keys %in_cgi;
                if ( $#dbs == 0 && $#cgis == 0 && $q->param('full_form') =~ /y/i && $dbs[0] ne $cgis[0] )	{
                    $in_db{$cgis[0]} = $in_db{$dbs[0]};
                    $in_cgi{$cgis[0]}{'measurement_type'} = $cgis[0];
                    delete $in_db{$dbs[0]};
                }

                foreach my $type (@measurement_types)	{
                    # if the record exists in both the form and db, update
                    if ($in_db{$type} && $in_cgi{$type}) {
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            if (!$in_cgi{$type}{$f}) {
                                $in_cgi{$type}{$f} = "";
                            }
                            if ($fields{'magnification'} =~ /^[0-9.]+$/) {
                                if ($in_cgi{$type}{$f}) {
                                    $in_cgi{$type}{'real_'.$f} = $in_cgi{$type}{$f} / $fields{'magnification'};
                                } else {
                                    $in_cgi{$type}{'real_'.$f} = "";
                                }
                            } else {
                                $in_cgi{$type}{'real_'.$f} = $in_cgi{$type}{$f};
                            }
                        }
                        if ( $in_cgi{$type}{'real_error'} )	{
                            $in_cgi{$type}{'error_unit'} = $fields{$type."_error_unit"};
                        }
                        if ( $type eq "length" && $q->param('length_position') )	{
                            $in_cgi{$type}{'position'} = $q->param('length_position');
                        } elsif ( $type eq "width" && $q->param('width_position') )	{
                            $in_cgi{$type}{'position'} = $q->param('width_position');
                        }
                        $dbt->updateRecord($s,'measurements','measurement_no',$in_db{$type}{'measurement_no'},$in_cgi{$type});
                        $updated_row_count++;
                    # if the user submitted a full form and the specimen ID plus
                    #  part plus type is gone it must have been erased by hand,
                    #  so delete it
                    } elsif ( $in_db{$type} && $q->param('full_form') =~ /y/i )	{
                        $sql = "DELETE FROM measurements WHERE measurement_no=".$in_db{$type}{'measurement_no'} . " LIMIT 1";
                        $dbh->do($sql);
                    # otherwise do nothing because the user isn't actually
                    #  editing the specimen's existing measurements
                    } elsif ( $in_db{$type} )	{
                    # if it's in the form and not the DB, add it
                    } elsif ( $in_cgi{$type} )	{
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            if ($fields{'magnification'} =~ /^[0-9.]+$/) {
                                $in_cgi{$type}{'real_'.$f} = $in_cgi{$type}{$f}/$fields{'magnification'};
                            } else {
                                $in_cgi{$type}{'real_'.$f} = $in_cgi{$type}{$f};
                            }
                        }
                        $in_cgi{$type}{'measurement_type'} = $type;
                        $in_cgi{$type}{'specimen_no'} = $fields{'specimen_no'};
                        if ( $in_cgi{$type}{'real_error'} )	{
                            $in_cgi{$type}{'error_unit'} = $fields{$type."_error_unit"};
                        }
                        $dbt->insertRecord($s,'measurements',$in_cgi{$type});
                    }
                }

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
                    $row->{'measurement_type'} = $type;
                    $row->{'specimen_no'} = $specimen_no;
                    $row->{'error_unit'}=$fields{$type."_error_unit"};
                    if ( $type eq "length" && $q->param('length_position') )	{
                        $row->{'position'} = $q->param('length_position');
                    } elsif ( $type eq "width" && $q->param('width_position') )	{
                        $row->{'position'} = $q->param('width_position');
                    }
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

    if ( $updated_row_count > 0 )	{
        #print "<center><p class=\"pageTitle\">$taxon_name$collection (revised data)</p></center>\n";
    } elsif ( $inserted_row_count > 0 )	{
        #print "<center><p class=\"pageTitle\">$taxon_name$collection (new data)</p></center>\n";
    }

    if ( $q->param('switch_occ') > 0 || $q->param('switch_coll') > 0 )	{
        if ( $newcoll )	{
            print "<div style=\"width: 32em; margin-left: auto; margin-right: auto; text-indent: -1em;\"><p class=\"verysmall\">The data record you updated has been switched successfully to $newcoll. You can search for it by clicking the 'another collection' link.</p></div>\n";
        } elsif ( $badcoll )	{
            print "<div style=\"width: 32em; margin-left: auto; margin-right: auto; text-indent: -1em;\"><p class=\"verysmall\">The data record was not switched because $taxon_name is not present in $badcoll. Try entering a different collection number.</p></div>\n";
        }
    }

	my $more_links = qq|<div class="verysmall" style="margin-bottom: 0.5em;">add/edit a measurement of <a href="$WRITE_URL?action=displaySpecimenSearchForm">another taxon</a>|;
	if ( $collection_no > 0 )	{
		$more_links .= qq| or <a href="$WRITE_URL?action=submitSpecimenSearch&collection_no=$collection_no">another occurrence in this collection</a><br>|;
	}
	$more_links .= qq| or another <a href="$WRITE_URL?action=submitSpecimenSearch&taxon_name=$taxon_name">occurrence</a> of $taxon_name, or</div>
|;
	displaySpecimenList($dbt,$hbo,$q,$s,'processMeasurementForm',$more_links);
	if ($q->param('occurrence_no')) {
		my ($temp,$collection_no) = split / /,$collection;
		$collection_no =~ s/\)//;
	}
	print qq|
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
    my @specimens = getMeasurements($dbt,{'taxon_list'=>\@in_list,'get_global_specimens'=>1});
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
sub getMeasurements	{
    my $dbt = $_[0];
    my $dbh = $dbt->dbh;
    my %options = %{$_[1]};

    my ($sql1,$sql2,$sql3,$where) = ("","","");
    my @fields = ('s.*','m.*');
    my @tables = ('specimens s','measurements m');
    my @where;

    if ( $options{'refs'} )	{
        push @fields , 'r.author1last,r.author2last,r.otherauthors,r.pubyr';
        push @tables , 'refs r';
        push @where , ('s.reference_no=r.reference_no');
    }

    if ( join('',@{$options{'lengths'}}) =~ /unknown/ )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'lengths'}}).qq|') OR position IS NULL OR measurement_type!='length')|;
    } elsif ( $options{'lengths'} )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'lengths'}}).qq|') OR measurement_type!='length')|;
    }
    if ( join('',@{$options{'widths'}}) =~ /unknown/ )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'widths'}}).qq|') OR position IS NULL OR measurement_type!='width')|;
    } elsif ( $options{'widths'} )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'widths'}}).qq|') OR measurement_type!='width')|;
    }

    my @part_list;
    if ( $options{'part_list'} ne "" && $options{'part_list'} ne "no" )	{
        if ( $options{'part_list'} =~ /[^A-Za-z0-9 ]/ )	{
          @part_list = split /[^A-Za-z0-9 ]/,$options{'part_list'};
        } else	{
          @part_list = split /[^A-Za-z0-9]/,$options{'part_list'};
        }
        s/^[ ]+// foreach @part_list;
        s/[ ]+$// foreach @part_list;
        push @where , "BINARY specimen_part IN ('".join("','",@part_list)."')";
    }
    if ( $options{'sex'} =~ /male|unknown/i )	{
        if ( $options{'sex'} eq "female only" )	{
            push @where , "sex='female'";
        } elsif ( $options{'sex'} eq "male only" )	{
            push @where , "sex='male'";
        } elsif ( $options{'sex'} eq "unknown only" )	{
            push @where , "(sex='' OR sex IS NULL OR sex='both')";
        } elsif ( $options{'sex'} eq "exclude females" )	{
            push @where , "(sex='male' OR sex='both' OR sex IS NULL)";
        } elsif ( $options{'sex'} eq "exclude males" )	{
            push @where , "(sex='female' OR sex='both' OR sex IS NULL)";
        }
    }

    $sql1 = "SELECT ".join(',',@fields).",o.taxon_no FROM ".join(', ',@tables).", occurrences o";
    $sql2 = "SELECT ".join(',',@fields).",re.taxon_no FROM ".join(', ',@tables).", occurrences o, reidentifications re";
    $sql3 = "SELECT ".join(',',@fields).",a.taxon_no FROM ".join(', ',@tables).", authorities a";

    $sql1 .= " LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.reid_no IS NULL";
    $sql2 .= " WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.occurrence_no=o.occurrence_no AND re.most_recent='YES'";
    $sql3 .= " WHERE a.taxon_no=s.taxon_no AND s.specimen_no=m.specimen_no";

    if ( @where )	{
        $sql1 .= " AND ".join(' AND ',@where);
        $sql2 .= " AND ".join(' AND ',@where);
        $sql3 .= " AND ".join(' AND ',@where);
    }

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
        $sql1 .= " AND o.occurrence_no=".int($options{'occurrence_no'});
        $sql2 .= " AND o.occurrence_no=".int($options{'occurrence_no'});
        $clause_found = 1;
    }

    if ($options{'get_global_specimens'} && $sql3 =~ /taxon_no IN/) {
        $sql = "($sql1) UNION ($sql2) UNION ($sql3)";
    } else {
        $sql = "($sql1) UNION ($sql2)";
    }

    if ($clause_found) {
        my @results = @{$dbt->getData($sql)};
        # good luck coding this in SQL JA 12.5.12
        if ( $options{'precision'} )	{
            my @precise;
            if ( $options{'precision'} =~ /^[0-9]+(|\.[0-9]+) mm$/ )	{
                my $min_digits = $options{'precision'};
                $min_digits =~ s/^([0-9]+)(|\.)(|[0-9]+)( mm)/$3/;
                $min_digits = length($min_digits);
                for my $r ( @results )	{
                    my $value = $r->{'average'};
                    $value =~ s/^[0-9]+//;
                    $value =~ s/^\.//;
                    if ( length($value) >= $min_digits )	{
                        push @precise , $r;
                    }
                }
            } else	{
                my $min_pct = $options{'precision'};
                $min_pct =~ s/[^0-9\.]//g;
                $min_pct /= 100;
                for my $r ( @results )	{
                    my $value = $r->{'average'};
                    $value =~ s/^[0-9]+//;
                    $value =~ s/^\.//;
                    if ( 1 / 10**length($value) / $r->{'average'} <= $min_pct )	{
                        push @precise , $r;
                    }
                }
            }
            @results = @precise;
        }
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
# @results = getMeasurements($dbt,{'collection_no'=>1234});
# $p_table = getMeasurementTable(\@results);
# $m_table = $p_table->{'leg'};
# $total_measured = $m_table->{'specimens_measured'}
# $average_width_leg = $m_table->{'width'}{'average'}
sub getMeasurementTable {
    my @measurements = @{$_[0]};

    my %p_table;
    my $sp_count = 0;
    my %types = ();
    my (%seen_specimens,%seen_ref,%seen_part_ref);
    my %unique_specimen_nos = ();

    # Do a simple reorganization of flat database data into triple indexed hash described above
    foreach my $row (@measurements) {
        if (!$seen_specimens{$row->{'specimen_no'}}) {
            $p_table{$row->{'specimen_part'}}{'specimens_measured'} += $row->{'specimens_measured'};
            $unique_specimen_nos{$row->{'specimen_part'}}++;
            $seen_specimens{$row->{'specimen_no'}} = 1;
            # references are passed back in two ways because they are grouped
            #  differently depending on how the data are displayed
            $seen_ref{Reference::formatShortRef($row)}++;
            $seen_part_ref{$row->{'specimen_part'}}{Reference::formatShortRef($row)}++;
        }
        # needed to credit data contributors JA 8.9.11
        $p_table{$row->{'specimen_part'}}{'authorizer '.$row->{'authorizer_no'}}++;
        $p_table{$row->{'specimen_part'}}{'enterer '.$row->{'enterer_no'}}++;
        $p_table{$row->{'specimen_part'}}{'part_refs'} = join(', ',sort keys %{$seen_part_ref{$row->{'specimen_part'}}});
        $types{$row->{'measurement_type'}}++;
        my $part_type;
        if (! exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}) {
            $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}} = {};
        } 
        $part_type = $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}};
        # note that "average" is the geometric mean
	$row->{'position'} = ( $row->{'position'} ne "" ) ? $row->{'position'} : "unknown";
        if ( ! $part_type->{'position'} )	{
            $part_type->{'position'} = $row->{'position'};
        } elsif ( $part_type->{'position'} !~ /\b$row->{'position'}\b/ )	{
            $part_type->{'position'} .= ", ".$row->{'position'};
        }
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
        my $digits = $row->{'average'};
        if ( $digits =~ /\./ )	{
            $digits =~ s/^.*\.//;
        } else	{
            $digits = "";
        }
        if ( 1 / 10**length($digits) > $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} || ! $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} )	{
            $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} = 1 / 10**length($digits);
        }
    }
    $p_table{'all_refs'} = join(', ',sort keys %seen_ref);

    for my $part ( keys %p_table )	{
        my %m_table = %{$p_table{$part}};
        foreach my $type (keys %types) {
            if ($m_table{$type}{'specimens_measured'}) {
                $m_table{$type}{'average'} = exp($m_table{$type}{'average'}/$m_table{$type}{'specimens_measured'});
                # if any averages were used in finding the min and max, the
                #  values are statistically bogus and should be erased
                # likewise if the sample size is 1
                if ( $m_table{$type}{'average_only'} == 1 || $m_table{$type}{'specimens_measured'} == 1 )	{
                    $m_table{$type}{'min'} = "";
                    $m_table{$type}{'max'} = "";
                }
            }
        }
   
        my @values = ();
        my $can_compute = 0; # Can compute median, and error (std dev)
        my $is_group = 0; # Is it aggregate group data or a bunch of singles?
        if ($unique_specimen_nos{$part} == 1) {
            if ($m_table{'specimens_measured'} > 1) {
                $can_compute = 1;
                $is_group = 1;
            }
        } elsif ($unique_specimen_nos{$part} >= 1 && $unique_specimen_nos{$part} == $m_table{'specimens_measured'}) {
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
                    $m_table{$row->{'measurement_type'}}{'median'} = $row->{'real_median'};
                    $m_table{$row->{'measurement_type'}}{'error'} = $row->{'real_error'};
                    $m_table{$row->{'measurement_type'}}{'error_unit'} = $row->{'error_unit'};
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
                            $m_table{$type}{'median'} = $median;
                        } else {
                            my $middle_index = int(scalar(@values/2));
                            $m_table{$type}{'median'} = $values[$middle_index];
                        }
                    }
                    if (scalar(@values) > 1) {
                        $m_table{$type}{'error'} = std_dev(@values);
                        $m_table{$type}{'error_unit'} = "1 s.d.";
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
	my ($dbt,$taxon_no,$tableref,$skip_area) = @_;
	my %p_table = %{$tableref};
	my %distinct_parts = ();

	for my $part ( keys %p_table )	{
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
		my %m_table = %{$p_table{$part}};
		if ( ! %m_table )	{
			next;
		}
		foreach my $type (('length','width','area','diameter','circumference')) {
			if ( $type eq "area" && $m_table{length}{average} && $m_table{width}{average} && $part =~ /^[PMpm][1234]$/ && ! $skip_area )	{
				$m_table{area}{average} = $m_table{length}{average} * $m_table{width}{average};
			}
			if ( $m_table{$type}{'average'} > 0 ) {
				if ( $type ne "width" || ! $m_table{'length'}{'average'} || ! $m_table{'width'}{'average'} )	{
					my $value = $m_table{$type}{'average'};
					my $digits;
					if ( $value < 1 )	{
						$digits = "3f";
					} elsif ( $value < 10 )	{
						$digits = "2f";
					} else	{
						$digits = "1f";
					}
					$value = sprintf("%.$digits",$value);
					if ( $type ne "length" || ! $m_table{'width'}{'average'} )	{
						push @values , "$part $type $value";
					} else	{
						push @values , sprintf("$part $value x %.$digits",$m_table{'width'}{'average'});
					}
				}
				my $last_lft = "";
				foreach my $eqn ( @eqn_refs )	{
					if ( $part eq $eqn->{'part'} && $eqn->{$type} && ! $eqn->{'minus'} )	{
						if ( $eqn->{'lft'} < $last_lft && $last_lft )	{
							last;
						}
						$last_lft = $eqn->{'lft'};
						my $mass = exp( ( log($m_table{$type}{average}) * $eqn->{$type} ) + $eqn->{intercept} );
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
	my ($q,$s,$dbt,$hbo) = @_;
	my $dbh = $dbt->dbh;

	if ( ! $q->param('taxon_name') ) 	{
		my $errorMessage = '<center><p class="medium"><i>You must enter the name of a taxonomic group.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# who needs Text::CSV_XS? JA 18.6.12
	sub csv	{
		$_ = shift;
		$_ =~ s/^$/NA/;
		if ( $_ =~ /"/ )	{
			$_ =~ s/"/""/g;
		}
		if ( $_ =~ /[" ,]/ )	{
			$_ = '"'.$_.'"';
		}
		return $_;
	}

	my $sep;
	if ( $q->param('output_format') eq "csv" )	{
		$sep = ",";
	} else	{
		$sep = "\t";
	}

	# set up stuff needed to output collection data JA 18.6.12
	my @collection_fields = ('collection_no','collection_name','authorizer','enterer','country','state','county','latitude','longitude','paleolatitude','paleolongitude','period','epoch','stage','10_my_bin','formation','member','lithology','environment');

	if ( $q->param('coll_coord') )	{
		$q->param('coll_coord' => '');
		$q->param('coll_latitude' => 'YES');
		$q->param('coll_longitude' => 'YES');
	}
	if ( $q->param('coll_paleocoord') )	{
		$q->param('coll_paleocoord' => '');
		$q->param('coll_paleolatitude' => 'YES');
		$q->param('coll_paleolongitude' => 'YES');
	}

	my $names = $q->param('taxon_name');
	$names =~ s/[^A-Za-z ]//g;
	$names =~ s/  / /g;
	$names = join("','",split / /,$names);

	# if there are multiple matches, we hope to get the right one by
	#  assuming the larger taxon is the legitimate one
	my $sql = "SELECT t.taxon_no,lft,rgt,rgt-lft width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND (taxon_name IN ('".$names."') OR common_name IN ('".$names."')) ORDER BY width DESC";
	my @parents = @{$dbt->getData($sql)};
	if ( ! @parents ) 	{
		my $errorMessage = '<center><p class="medium"><i>The taxon '.$q->param('taxon_name').' is not in our database. Please try another name.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# same for "exclude" taxon JA 8.9.11
	# it doesn't matter if excluded taxa are extant
	my $exclude_clause;
	if ( $q->param('exclude') )	{
		$sql = "SELECT t.taxon_no,lft,rgt,rgt-lft width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND (taxon_name='".$q->param('exclude')."' OR common_name ='".$q->param('exclude')."') ORDER BY width DESC LIMIT 1"; 
		my $exclude = ${$dbt->getData($sql)}[0];
		if ( ! $exclude ) 	{
			my $errorMessage = '<center><p class="medium"><i>The taxon '.$q->param('exclude').' is not in our database. Please try another name.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		$exclude_clause = "AND (lft<$exclude->{'lft'} OR rgt>$exclude->{'rgt'})";
	}

	my @fields = ('synonym_no','spelling_no','a.taxon_no','taxon_name','taxon_rank');
	if ( $q->param('authors') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.author1last,a.author1last) a1";
		push @fields , "IF(ref_is_authority='YES',r.author2last,a.author2last) a2";
		push @fields , "IF(ref_is_authority='YES',r.otherauthors,a.otherauthors) oa";
	}
	if ( $q->param('year') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.pubyr,a.pubyr) pubyr";
	}
	for my $f ( 'type_specimen','type_body_part','type_locality','extant')	{
		if ( $q->param($f) =~ /y/i )	{
			push @fields , $f;
		}
	}

	# now extract, average, and output the measurements as follows:
	#  (1) find species in the taxonomic group
	#  (2) get measurements of these species (getMeasurements)
	#  (3) save measurements of species found in a subset of collections
	#  (4) get collection data to be printed to the raw measurements file
	#  (5) compose output file headers
	#  (6) take averages (getMeasurementTable)
	#  (7) print data to raw measurements file
	#  (8) print data to the average measurements file
	#  (9) print data to the table file

	# step 1

	my $extant_clause;
	if ( $q->param('extant_extinct') =~ /extant/i )	{
		$extant_clause = " AND extant='yes'";
	} elsif ( $q->param('extant_extinct') =~ /extinct/i )	{
		$extant_clause = " AND (extant='no' OR extant IS NULL)";
	}

	# grabs all synonyms and spellings
	my @brackets;
	push @brackets, "(lft>=$_->{lft} AND rgt<=$_->{rgt})" foreach @parents;
	$sql = "SELECT ".join(',',@fields)." FROM authorities a,$TAXA_TREE_CACHE t WHERE taxon_rank IN ('species','subspecies') AND a.taxon_no=t.taxon_no AND (".join(" OR ",@brackets).") $exclude_clause $extant_clause ORDER BY taxon_name ASC";
	if ( $q->param('authors') =~ /y/i || $q->param('year') =~ /y/i )	{
		$sql = "SELECT ".join(',',@fields)." FROM authorities a,refs r,$TAXA_TREE_CACHE t WHERE taxon_rank IN ('species','subspecies') AND a.reference_no=r.reference_no AND a.taxon_no=t.taxon_no AND (".join(" OR ",@brackets).") $exclude_clause $extant_clause ORDER BY taxon_name ASC";
	}

	my @taxa = @{$dbt->getData($sql)};
	if ( ! @taxa ) 	{
		my $errorMessage = '<center><p class="medium"><i>We have no measurement data for species belonging to '.$q->param('taxon_name').'. Please try another taxon.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# get the life habits so some categories can be excluded
	my $habits_checked;
	for my $p ( $q->param('life_habit') )	{
		$habits_checked++;
	}
	if ( $habits_checked < 9 )	{
		my %eco_lookup;
		for my $p ( @parents )	{
			my %temp_lookup = %{Ecology::fastEcologyLookup($dbt,'life_habit',$p->{lft},$p->{rgt})};
			$eco_lookup{$_} = $temp_lookup{$_} foreach keys %temp_lookup;
		}
		my (%use_habit,@with_habits);
		$use_habit{$_}++ foreach $q->param('life_habit');
		for my $t ( @taxa )	{
			if ( $use_habit{$eco_lookup{$t->{'taxon_no'}}} > 0 )	{
				push @with_habits , $t;
			}
		}
		if ( ! @with_habits ) 	{
			my $errorMessage = '<center><p class="medium"><i>We have no measurement data for species in the selected life habit categories. Adding more categories might help.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		@taxa = @with_habits;
	}

	# group synonyms for each valid species
	my %valid_no;
	if ( $q->param('replace_with_ss') =~ /y/i )	{
		$valid_no{$_->{taxon_no}} = $_->{synonym_no} foreach @taxa;
	# or else don't, but make sure only one spelling is used per synonym
	} else	{
		$valid_no{$_->{taxon_no}} = $_->{spelling_no} foreach @taxa;
	}

	# special handling for subspecies JA 1.5.12
	# this is a very minor issue only really relevant to Recent mammals
	my @subspp;
	for my $t ( @taxa )	{
		if ( $t->{'taxon_rank'} eq "subspecies" )	{
			push @subspp , $t->{'taxon_no'};
		}
	}
	if ( @subspp )	{
		$sql = "SELECT child_no,parent_no FROM authorities,taxa_list_cache WHERE taxon_no=parent_no AND child_no IN (".join(',',@subspp).") AND taxon_rank='species'";
		$valid_no{$_->{child_no}} = $_->{parent_no} foreach @{$dbt->getData($sql)};
	}

	my @taxon_list = keys %valid_no;

	# step 2: get measurements of the included species (getMeasurements)

	my %options;
	$options{'get_global_specimens'} = 1;
	if ( @taxon_list )	{
		$options{'taxon_list'} = \@taxon_list;
	}

	# recover desired position values from the length and width
	#  "measurements" checkboxes
	my @params = $q->param;
	my (@lengths,@widths);
	for my $p ( @params )	{
		if ( $p =~ /_length/ && $q->param('length') )	{
			my ($pos,$dim) = split /_/,$p;
			push @lengths , $pos;
		} elsif ( $p =~ /_width/ && $q->param('width') )	{
			my ($pos,$dim) = split /_/,$p;
			push @widths , $pos;
		}
	}
	if ( @lengths )	{
		$options{'lengths'} = \@lengths;
	}
	if ( @widths )	{
		$options{'widths'} = \@widths;
	}

	for my $p ( 'refs','part_list','precision' )	{
		if( $q->param($p) )	{
			$options{$p} = $q->param($p);
		}
	}
	if ( $q->param('sex') && $q->param('sex') ne "both" )	{
		$options{'sex'} = $q->param('sex');
	}
#print "$_/$options{$_}<br>" foreach keys %options;

	my @measurements = getMeasurements($dbt,\%options);
	if ( ! @measurements ) 	{
		my $errorMessage = '<center><p class="medium"><i>We have data records for this taxon but your options exclude them. Try broadening your search criteria.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}
#printf "TAXA".scalar(@measurements);

	# step 3: save measurements of species found in a subset of collections

	my ($resos,$collections,$countries,$continent_list,$continents_checked,$interval_nos,$strat_unit);
	my $download = Download->new($dbt,$q,$s,$hbo);
	# JA 18.6.12
	if ( $q->param('taxonomic_resolution') && $q->param('taxonomic_resolution') ne "all" )	{
		if ( $q->param('taxonomic_resolution') eq "identified to genus" )	{
			$resos = "species_name!='indet.' AND (genus_reso!='informal' OR genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "certainly identified to genus" )	{
			$resos = "species_name!='indet.' AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "identified to species" )	{
			$resos = "species_name NOT IN ('indet','sp.','spp.') AND (species_reso!='informal' OR species_reso IS NULL) AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "certainly identified to species" )	{
			$resos = "species_name NOT IN ('indet','sp.','spp.') AND (species_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') OR species_reso IS NULL) AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		}
	}
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
	my @continents = ( 'Africa','Antarctica','Asia','Australia','Europe','North America','South America' );
	for my $c ( @continents )	{
		if ( $q->param($c) !~ /y/i )	{
			$countries = $download->getCountryString();
			last;
		}
	}
	for my $c ( @continents )	{
		if ( $q->param($c) =~ /y/i )	{
			$continent_list .= ", ".$c;
			$continents_checked++;
		}
	}
	$continent_list =~ s/^, //;
	if ( $continents_checked == $#continents + 1 )	{
		($continent_list,$countries) = ("","");
	}
	if ( $q->param('max_interval') =~ /^[A-Z][a-z]/i )	{
		require TimeLookup;
		my $t = new TimeLookup($dbt);
	# eml_max and min aren't on the form yet
		my ($intervals,$errors,$warnings) = $t->getRange('',$q->param('max_interval'),'',$q->param('min_interval'));
		$interval_nos = join(',',@$intervals);
	}
	if ( $q->param('group_formation_member') =~ /^[A-Z]/i )	{
		$strat_unit = $q->param('group_formation_member');
		$strat_unit =~ s/\'/\\\'/g;
		$strat_unit = "(geological_group='".$strat_unit."' OR formation='".$strat_unit."' OR member='".$strat_unit."')";
	}
	# JA 18.6.12
	my $env_sql = $download->getEnvironmentString();
	my $pres_sql = $download->getPreservationModeString();

	my (%by_valid,%occ_used,%taxon_used);

	if ( $resos || $collections || $countries || $interval_nos || $strat_unit || $env_sql || $pres_sql )	{
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
		if ( $interval_nos )	{
			$sql2 .= " AND max_interval_no IN (".$interval_nos.") AND min_interval_no IN (".$interval_nos.",0)";
		}
		$sql2 .= ( $resos ) ? " AND ".$resos : "";
		$sql2 .= ( $collections ) ? " AND ".$collections : "";
		$sql2 .= ( $countries ) ? " AND ".$countries : "";
		$sql2 .= ( $strat_unit ) ? " AND ".$strat_unit : "";
		$sql2 .= ( $env_sql ) ? " AND ".$env_sql : "";
		$sql2 .= ( $pres_sql ) ? " AND ".$pres_sql : "";
		my @with_occs = @{$dbt->getData($sql1.$sql2)};
		my $sql1 = "SELECT occurrence_no,taxon_no FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND taxon_no IN (".join(',',@taxon_list).")";
		my %temp;
		$temp{$_->{'occurrence_no'}}++ foreach @with_occs;
		if ( %temp )	{
			$sql1 .= "AND occurrence_no NOT IN (".join(',',keys %temp).")";
		}
		push @with_occs , @{$dbt->getData($sql1.$sql2)};
		if ( ! @with_occs )	{
			my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		my %avail;
		$avail{$_->{'taxon_no'}}++ foreach @with_occs;
		$occ_used{$_->{'occurrence_no'}}++ foreach @with_occs;
		undef @with_occs;
	# which measured species are sampled anywhere in this collection set?
		for my $m ( @measurements )	{
			if ( $avail{$m->{'taxon_no'}} )	{
				$taxon_used{$valid_no{$m->{'taxon_no'}}}++;
			}
		}
		undef %avail;
	# go through it again because many measurements are not tied to any
	#  collection at all
	# we end up with all measurements grouped by valid species name
		for my $m ( @measurements )	{
			if ( $taxon_used{$valid_no{$m->{'taxon_no'}}} )	{
				my $vn = $valid_no{$m->{'taxon_no'}};
				$vn = ( $valid_no{$vn} > 0 ) ? $valid_no{$vn} : $vn;
				$m->{'valid_no'} = $vn;
				push @{$by_valid{$vn}} , $m;
			}
		}
	} else	{
		for my $m ( @measurements )	{
			# simple fix for extremely rare chaining case (could
			#  be created by lumping of subspecies, not only by
			#  conventional synonymies)
			my $vn = $valid_no{$m->{'taxon_no'}};
			$vn = ( $valid_no{$vn} > 0 ) ? $valid_no{$vn} : $vn;
			# needed to print valid names to raw measurements file
			$m->{'valid_no'} = $vn;
			push @{$by_valid{$vn}} , $m;
			$occ_used{$m->{'occurrence_no'}} = ( $m->{'occurrence_no'} ) ? 1 : "";
			$taxon_used{$valid_no{$m->{'taxon_no'}}} = ( $m->{'taxon_no'} ) ? 1 : "";
		}
	}

	# create faux "data records" for length x width etc. JA 19.6.12 
	# grouping is by unique specimen identifier (taxon_no or occurrence_no)
	#  plus specimen_id, if one exists
	# we have to compute only one LxW or LxWxH per thing even though things
	#  may have multiple lengths or widths
	if ( $q->param('area') || $q->param('volume') )	{
		my (%lengths,%widths,%heights);
		for my $m ( @measurements )	{
			my $id = ( $m->{'occurrence_no'} > 0 ) ? $m->{'occurrence_no'} : $m->{'taxon_no'};
			$id .= ( $m->{'specimen_id'} ) ? $m->{'specimen_id'} : "";
			if ( $m->{'measurement_type'} eq "length" )	{
				push @{$lengths{$id}} , $m;
			} elsif ( $m->{'measurement_type'} eq "width" )	{
				push @{$widths{$id}} , $m;
			} elsif ( $m->{'measurement_type'} eq "height" )	{
				push @{$heights{$id}} , $m;
			}
		}
		for my $id ( keys %lengths )	{
			my $faux = { };
			$faux->{$_} = $lengths{$id}[0]->{$_} foreach ( 'taxon_no','valid_no','occurrence_no','specimen_id','specimen_part','specimens_measured','authorizer_no','enterer_no' );
			my ($sum,$meanW,$meanH);
			$sum += log($_->{'average'}) foreach @{$lengths{$id}};
			my $meanL = $sum / scalar(@{$lengths{$id}});
			if ( $widths{$id} )	{
				$sum = 0;
				$sum += log($_->{'average'}) foreach @{$widths{$id}};
				$meanW = $sum / scalar(@{$widths{$id}});
				if ( $q->param('area') )	{
					if ( $faux->{'specimens_measured'} < $widths{$id}[0]->{'specimens_measured'} )	{
						$faux->{'specimens_measured'} = $widths{$id}[0]->{'specimens_measured'};
					}
					$faux->{'measurement_no'} = "NA";
					$faux->{'measurement_type'} = "area";
					$faux->{'average'} = sprintf "%.1f",exp($meanL + $meanW);
					$faux->{'real_average'} = $faux->{'average'};
					push @{$by_valid{$faux->{'valid_no'}}} , $faux;
push @measurements , $faux;
				}
			}
			if ( $widths{$id} && $heights{$id} && $q->param('volume') )	{
				$sum = 0;
				$sum += log($_->{'average'}) foreach @{$heights{$id}};
				$meanH = $sum / scalar(@{$heights{$id}});
				# don't try to recycle this, faux is a pointer
				my $faux = { };
				$faux->{$_} = $lengths{$id}[0]->{$_} foreach ( 'taxon_no','valid_no','occurrence_no','specimen_id','specimen_part','specimens_measured','authorizer_no','enterer_no' );
				if ( $faux->{'specimens_measured'} < $heights{$id}[0]->{'specimens_measured'} )	{
					$faux->{'specimens_measured'} = $heights{$id}[0]->{'specimens_measured'};
				}
				$faux->{'measurement_no'} = "NA";
				$faux->{'measurement_type'} = "volume";
				$faux->{'average'} = sprintf "%.1f",exp($meanL + $meanW + $meanH);
				$faux->{'real_average'} = $faux->{'average'};
				push @{$by_valid{$faux->{'valid_no'}}} , $faux;
push @measurements , $faux;
			}
		}
	}

	# step 4: get collection data to be printed to the raw measurements file
	# JA 18.6.12

	my (@raw_collection_fields,%collection_data);
	for my $p ( @collection_fields )	{
		if ( $q->param('coll_'.$p) )	{
			my $field = $p;
			$field =~ s/part_//;
			if ( $field =~ /period|epoch|stage|10_my_bin/ )	{
				next;
			}
			if ( $field =~ /collection_no|authorizer|enterer/ )	{
				push @raw_collection_fields , "c.".$field;
			} elsif ( $field eq "latitude" )	{
				push @raw_collection_fields , "lat AS latitude";
			} elsif ( $field eq "longitude" )	{
				push @raw_collection_fields , "lng AS longitude";
			} elsif ( $field eq "paleolatitude" )	{
				push @raw_collection_fields , "paleolat AS paleolatitude";
			} elsif ( $field eq "paleolongitude" )	{
				push @raw_collection_fields , "paleolng AS paleolongitude";
			} elsif ( $field eq "lithology" )	{
				push @raw_collection_fields , "IF(lithology2 IS NOT NULL AND lithology2!='',CONCAT(lithology1,'/',lithology2),lithology1) AS lithology";
			} else	{
				push @raw_collection_fields , $field;
			}
		}
	}
	if ( $q->param('coll_period') || $q->param('coll_epoch') || $q->param('coll_stage') || $q->param('coll_10_my_bin') )	{
		push @raw_collection_fields , ('max_interval_no','min_interval_no');
	}
	delete $occ_used{''};
	delete $occ_used{0};
	delete $taxon_used{''};
	delete $taxon_used{0};
	if ( @raw_collection_fields && keys %occ_used )	{
		my @tables = ('collections c','occurrences o');
		my (%interval_names,%lookups);
		if ( $q->param('coll_period') || $q->param('coll_epoch') || $q->param('coll_stage') || $q->param('coll_10_my_bin') )	{
			%interval_names = map { ( $_->{'interval_no'} , $_->{'interval_name'} ) } @{$dbt->getData("SELECT interval_no,IF(eml_interval!='' AND eml_interval IS NOT NULL,CONCAT(eml_interval,' ',interval_name),interval_name) AS interval_name FROM intervals")};
			%lookups = map { ( $_->{'interval_no'} , $_ ) } @{$dbt->getData("SELECT interval_no,period_no,epoch_no,stage_no,ten_my_bin FROM interval_lookup")};
		}
		$sql = "SELECT occurrence_no,".join(',',@raw_collection_fields)." FROM ".join(',',@tables)." WHERE c.collection_no=o.collection_no AND occurrence_no IN (".join(',',keys %occ_used).")";
		$collection_data{$_->{'occurrence_no'}} = $_ foreach @{$dbt->getData($sql)};
		for my $no ( keys %collection_data )	{
			my $max = $collection_data{$no}->{'max_interval_no'};
			my $min = $collection_data{$no}->{'min_interval_no'};
			$collection_data{$no}->{'period'} = $interval_names{$lookups{$max}->{period_no}};
			$collection_data{$no}->{'epoch'} = $interval_names{$lookups{$max}->{epoch_no}};
			$collection_data{$no}->{'stage'} = $interval_names{$lookups{$max}->{stage_no}};
			$collection_data{$no}->{'10_my_bin'} = $lookups{$max}->{ten_my_bin};
			if ( $min > 0 )	{
				$collection_data{$no}->{'period'} .= ( $collection_data{$no}->{'period'} ne $interval_names{$lookups{$min}->{period_no}} ) ? "/".$interval_names{$lookups{$min}->{period_no}} : "";
				$collection_data{$no}->{'epoch'} .= ( $collection_data{$no}->{'epoch'} ne $interval_names{$lookups{$min}->{epoch_no}} ) ? "/".$interval_names{$lookups{$min}->{epoch_no}} : "";
				$collection_data{$no}->{'stage'} .= ( $collection_data{$no}->{'stage'} ne $interval_names{$lookups{$min}->{stage_no}} ) ? "/".$interval_names{$lookups{$min}->{stage_no}} : "";
				$collection_data{$no}->{'10_my_bin'} = ( $collection_data{$no}->{'10_my_bin'} ne $lookups{$min}->{ten_my_bin} ) ? "" : $collection_data{$no}->{'10_my_bin'};
			}
		}
	}

	# step 5: compose output file headers
	# rewritten to deal with raw_header_fields JA 18.6.12

	my @header_fields = ('species');
	my @columns = ('average');
	my %authors;
	for my $param ( 'order','family' )	{
		if ( $q->param($param) )	{
			push @header_fields , $param;
		}
	}
	if ( $q->param('authors') =~ /y/i )	{
		for my $t ( @taxa )	{
			$t->{'a1'} =~ s/,.*//;
			$t->{'a2'} =~ s/,.*//;
			if ( $t->{'oa'} ) { $t->{a2} = " et al."; }
			else { $t->{'a2'} =~ s/^([A-Za-z])/ and $1/; }
			$authors{$t->{'taxon_no'}} = $t->{'a1'}.$t->{'a2'};
		}
		push @header_fields , "authors";
	}
	my %year;
	if ( $q->param('year') =~ /y/i )	{
		$year{$_->{'taxon_no'}} = $_->{'pubyr'} foreach @taxa;
		push @header_fields , "year published";
	}
	my %type;
	if ( $q->param('type_specimen') =~ /y/i )	{
		$type{$_->{'taxon_no'}} = $_->{'type_specimen'} foreach @taxa;
		push @header_fields , "type specimen";
	}
	my %type_part;
	if ( $q->param('type_body_part') =~ /y/i )	{
		$type_part{$_->{'taxon_no'}} = $_->{'type_body_part'} foreach @taxa;
		push @header_fields , "type body part";
	}
	my %locality;
	if ( $q->param('type_locality') =~ /y/i )	{
		$locality{$_->{'taxon_no'}} = $_->{'type_locality'} foreach @taxa;
		push @header_fields , "type locality number";
	}
	my %extant;
	if ( $q->param('extant') =~ /y/i )	{
		$extant{$_->{'taxon_no'}} = $_->{'extant'} foreach @taxa;
		push @header_fields , "extant";
	}

	# all the fields in the raw and averaged files are identical
	#  up to this point
	my @raw_header_fields = ('measurement_no',@header_fields);
	# collection attributes are only printed to the raw data file
	for my $p ( @collection_fields )	{
		if ( $q->param('coll_'.$p) )	{
			push @raw_header_fields , $p;
			if ( $q->param('authorizer') || $q->param('enterer') )	{
				$raw_header_fields[$#raw_header_fields] =~ s/(authorizer|enterer)/collection.$1/;
			}
		}
	}
	if ( $q->param('specimen_id') =~ /y/i )	{
		push @raw_header_fields , "specimen ID";
        }

	my @stat_fields;
	push @stat_fields , 'part';
	if ( $q->param('position') =~ /y/i )	{
		push @stat_fields , "position";
        }
	push @stat_fields, 'measurement';
	if ( $q->param('specimens_measured') =~ /y/i )	{
		push @stat_fields , "specimens measured";
	}
	push @stat_fields , "mean";
	for my $c ('min','max','median','error')	{
		if ( $q->param($c) =~ /y/i )	{
			push @stat_fields , $c;
			push @columns , $c;
		}
	}
	if ( $q->param('error') =~ /y/i )	{
		push @stat_fields , "error unit";
		push @columns , "error_unit";
	}
	for my $c ( 'authorizer','enterer' )	{
		if ( $q->param($c) =~ /y/i && ( $q->param('coll_authorizer') || $q->param('coll_enterer') ) )	{
			push @stat_fields , "measurement.".$c;
		} elsif ( $q->param($c) )	{
			push @stat_fields , $c;
		}
	}
	if ( $q->param('refs') =~ /y/i )	{
		push @stat_fields , "references";
	}
	push @header_fields , @stat_fields;
	push @raw_header_fields , @stat_fields;

	# step 6: take averages (getMeasurementTable)

	my %tables;
	my @with_data;
	foreach my $t ( @taxa )	{
		if ( ( $q->param('replace_with_ss') =~ /y/i && $t->{taxon_no} == $t->{synonym_no} ) || ( $q->param('replace_with_ss') !~ /y/i && $t->{taxon_no} == $t->{spelling_no} ) )	{
			my $vn = $valid_no{$t->{taxon_no}};
			if ( ! $by_valid{$vn} || $tables{$vn} )	{
				next;
			}
			my $p_table = getMeasurementTable(\@{$by_valid{$vn}});
			$tables{$vn} = $p_table;
			push @with_data , $vn;
		}
	}

	my %name;
	$name{$_->{'taxon_no'}} = $_->{'taxon_name'} foreach @taxa;
	@with_data = sort { $name{$a} cmp $name{$b} } @with_data;

	# this is pretty slow, but it's simple and reliable JA 30.5.12
	my (%order,%family);
	if ( $q->param('order') )	{
		$sql = "SELECT taxon_name,t1.taxon_no FROM $TAXA_TREE_CACHE t1,$TAXA_TREE_CACHE t2,authorities a WHERE t1.taxon_no IN (".join(',',@with_data).") AND t2.lft<t1.lft AND t2.rgt>t1.rgt AND a.taxon_no=t2.taxon_no AND taxon_rank='order' AND t2.taxon_no=t2.synonym_no";
		$order{$_->{taxon_no}} = $_->{taxon_name} foreach @{$dbt->getData($sql)};
	}
	if ( $q->param('family') )	{
		$sql = "SELECT taxon_name,t1.taxon_no FROM $TAXA_TREE_CACHE t1,$TAXA_TREE_CACHE t2,authorities a WHERE t1.taxon_no IN (".join(',',@with_data).") AND t2.lft<t1.lft AND t2.rgt>t1.rgt AND a.taxon_no=t2.taxon_no AND taxon_rank='family' AND t2.taxon_no=t2.synonym_no";
		$family{$_->{taxon_no}} = $_->{taxon_name} foreach @{$dbt->getData($sql)};
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
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( keys %p_table )	{
			if ( $part !~ /^(p|m)(1|2|3|4)$/i )	{
				$distinct_parts{$part}++;
			}
		}
	}

#FOO
#	if ( $q->param('part_list') )	{
#		@part_list = split /[^A-Za-z0-9 ]/,$q->param('part_list');
#		s/^[ ]+// foreach @part_list;
#		s/[ ]+$// foreach @part_list;
#	} else	{
		@part_list = keys %distinct_parts;
		@part_list = sort { $a cmp $b } @part_list;
#	if ( ! $q->param('part_list') )	{
		unshift @part_list , ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");
#}
#	}
#print join(' ',@part_list);

	my $types;
	for my $type ( @measurement_types,'area','volume' )	{
		if ( $q->param($type) =~ /y/i )	{
			$types++;
		}
	}
	my (%measured_parts,%measured_types);
	for my $taxon_no ( @with_data )	{
		my $measured_parts = 0;
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( @part_list )	{
			my %m_table = %{$p_table{$part}};
			if ( %m_table )	{
				for my $type ( @measurement_types,'area','volume' )	{
					if ( $m_table{$type} && $q->param($type) =~ /y/i && $m_table{$type}{'average'} > 0 )	{
						$measured_parts{$taxon_no}++;
						$measured_types{$taxon_no}{$part}++;
					}
				}
			}
		}
	}

	# needed to print credit lines
	my %person;
	my $sql = "SELECT name,person_no FROM person";
	$person{$_->{'person_no'}} = $_->{'name'} foreach @{$dbt->getData($sql)};

	# step 7: print data to raw measurements file JA 18.6.12

	my $OUT_HTTP_DIR = "/public/downloads";
	my $OUT_FILE_DIR = $HTML_DIR.$OUT_HTTP_DIR;
	my $person = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
	my $outfile = PBDBUtil::getFilename($person)."_raw_measurements.txt";
	my $outfile2 = PBDBUtil::getFilename($person)."_average_measurements.txt";
	my $outfile3 = PBDBUtil::getFilename($person)."_measurement_table.txt";


	@measurements = sort { $name{$a->{'valid_no'}} cmp $name{$b->{'valid_no'}} } @measurements;
	open OUT,">$OUT_FILE_DIR/$outfile";
	$_ = csv($_) foreach @raw_header_fields;
	my $header = join($sep,@raw_header_fields);
	print OUT $header,"\n";
	for my $m ( @measurements )	{
		if ( ! ( $occ_used{$m->{'occurrence_no'}} || $taxon_used{$valid_no{$m->{'taxon_no'}}} ) )	{
			next;
		}
		print OUT $m->{'measurement_no'},$sep,csv($name{$m->{'valid_no'}});
		if ( $q->param('order') ) { print OUT $sep,csv($order{$m->{'valid_no'}}); }
		if ( $q->param('family') ) { print OUT $sep,csv($family{$m->{'valid_no'}}); }
		if ( $q->param('authors') ) { print OUT $sep,csv($authors{$m->{'valid_no'}}); }
		if ( $q->param('year') ) { print OUT $sep,csv($year{$m->{'valid_no'}}); }
		if ( $q->param('type_specimen') ) { print OUT $sep,csv($type{$m->{'valid_no'}}); }
		if ( $q->param('type_body_part') ) { print OUT $sep,csv($type_part{$m->{'valid_no'}}); }
		if ( $q->param('type_locality') ) { print OUT $sep,csv($locality{$m->{'valid_no'}}); }
		if ( $q->param('extant') ) { print OUT $sep,csv($extant{$m->{'valid_no'}}); }
		for my $field ( @collection_fields )	{
			if ( $q->param('coll_'.$field) && $field =~ /latitude|longitude/ )	{
				print OUT $sep,csv(sprintf("%.1f",$collection_data{$m->{'occurrence_no'}}->{$field}));
			} elsif ( $q->param('coll_'.$field) )	{
				print OUT $sep,csv($collection_data{$m->{'occurrence_no'}}->{$field});
			}
		}
		if ( $q->param('specimen_id') ) { print OUT $sep,csv($m->{'specimen_id'}); }
		print OUT $sep,csv($m->{'specimen_part'});
		if ( $q->param('position') ) { print OUT $sep,csv($m->{'position'}); }
		print OUT $sep,csv($m->{'measurement_type'});
		if ( $q->param('specimens_measured') ) { print OUT $sep,csv($m->{'specimens_measured'}); }
		print OUT $sep,csv($m->{'average'});
		for my $stat ( 'median','min','max','error' )	{
			if ( $q->param($stat) )	{
				print OUT $sep,csv($m->{$stat});
			}
		}
		if ( $q->param('error') ) { print OUT $sep,csv($m->{'error_unit'}); }
		print OUT "\n";
	}
	close OUT;

	# step 8: print data to the average measurements file

	open OUT,">$OUT_FILE_DIR/$outfile2";
	$_ = csv($_) foreach @header_fields;
	my $header = join($sep,@header_fields);
	print OUT $header,"\n";

	my (%printed_parts,%total_authorized,%total_entered,%hasType,%matrix);
	for my $taxon_no ( @with_data )	{
		if ( $q->param('all_parts') =~ /y/i && $measured_parts{$taxon_no} < ( $#part_list + 1 ) * $types )	{
			next;
		}
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( @part_list )	{
			my (%authorized,%entered);
			for my $k ( keys %{$p_table{$part}} )	{
				if ( $k =~ /^auth/ )	{
					my $n = $k;
					$n =~ s/[^0-9]//g;
					$authorized{$person{$n}}++;
					$total_authorized{$person{$n}}++;
				} elsif ( $k =~ /^enter/ )	{
					my $n = $k;
					$n =~ s/[^0-9]//g;
					$entered{$person{$n}}++;
					$total_entered{$person{$n}}++;
				}
			}
			my %m_table = %{$p_table{$part}};
			if ( %m_table )	{
				$printed_part = $part;
				if ( $part eq "" )	{
					$printed_part = "unknown";
				}
				my $part_used;
				for my $type ( @measurement_types,'area','volume' )	{
					if ( $m_table{$type} && $q->param($type) =~ /y/i && $m_table{$type}{'average'} > 0 )	{
						$part_used++;
						$printed_parts{$taxon_no}++;
						print OUT csv($name{$taxon_no});
						if ( $q->param('order') )	{
							print OUT $sep,csv($order{$taxon_no});
						}
						if ( $q->param('family') )	{
							print OUT $sep,csv($family{$taxon_no});
						}
						if ( $q->param('authors') =~ /y/i )	{
							print OUT $sep,csv($authors{$taxon_no});
						}
						if ( $q->param('year') =~ /y/i )	{
							print OUT $sep,csv($year{$taxon_no});
						}
						if ( $q->param('type_specimen') =~ /y/i )	{
							print OUT $sep,csv($type{$taxon_no});
						}
						if ( $q->param('type_body_part') =~ /y/i )	{
							print OUT $sep,csv($type_part{$taxon_no});
						}
						if ( $q->param('type_locality') =~ /y/i )	{
							print OUT $sep,csv($locality{$taxon_no});
						}
						if ( $q->param('extant') =~ /y/i )	{
							print OUT $sep,csv($extant{$taxon_no});
						}
						if ( $q->param('position') =~ /y/i )	{
							print OUT $sep,csv($m_table{$type}{'position'});
						}
						print OUT $sep,$printed_part,$sep,$type;
						if ( $q->param('specimens_measured') =~ /y/i )	{
							print OUT $sep,$m_table{'specimens_measured'};
						}
						foreach my $column ( @columns )	{
							my $value = $m_table{$type}{$column};
							if ( $column eq "error_unit" )	{
							} elsif ( $m_table{$type}{'precision'} <= 0 )	{
								$value = "NaN";
							} elsif ( $m_table{$type}{'precision'} <= 0.001 )	{
								$value = sprintf "%.3f",$value;
							} elsif ( $m_table{$type}{'precision'} == 0.01 )	{
								$value = sprintf "%.2f",$value;
							} elsif ( $m_table{$type}{'precision'} == 0.1 )	{
								$value = sprintf "%.1f",$value;
							} elsif ( $value > 0 )	{
								$value = sprintf "%d",$value;
							}
							print OUT $sep,csv($value);
							if ( $column eq "average" && $value > 0 )	{
								$hasType{$type." ".$part}++;
								$matrix{$taxon_no}{$type}{$part} = $value;
							}
						}
						if ( $q->param('authorizer') =~ /y/i )	{
							my @names = keys %authorized;
							@names = sort @names;
							print OUT $sep,csv(join(', ',@names));
						}
						if ( $q->param('enterer') =~ /y/i )	{
							my @names = keys %entered;
							@names = sort @names;
							print OUT $sep,csv(join(', ',@names));
						}
						if ( $q->param('refs') =~ /y/i )	{
							print OUT $sep,csv($m_table{'part_refs'});
						}
						print OUT "\n";
					}
				}
				if ( $part_used )	{
					$records{$part}++;
					$specimens{$part} += $m_table{'specimens_measured'};
					$rows++;
				}
			}
		}
	}
	close OUT;

	# step 9: finally, print data to the table file

	open OUT,">$OUT_FILE_DIR/$outfile3";
	print OUT "species";
	for my $param ( 'order','family' )	{
		if ( $q->param($param) )	{
			print OUT "\t$param";
		}
	}
	for my $type ( @measurement_types,'area','volume' )	{
		for my $part ( @part_list )	{
			if ( $hasType{$type." ".$part} > 0 )	{
				my $column = $part." ".$type;
				$column =~ s/mass/mass (g)/;
				print OUT "\t",csv($column);
			}
		}
	}
	if ( $q->param('refs') =~ /y/i )	{
		print OUT "\treferences";
	}
	print OUT "\n";
	@with_data = keys %matrix;
	for my $taxon_no ( sort { $name{$a} cmp $name{$b} } @with_data )	{
		print OUT "$name{$taxon_no}";
		if ( $q->param('order') )	{
			print OUT "\t$order{$taxon_no}";
		}
		if ( $q->param('family') )	{
			print OUT "\t$family{$taxon_no}";
		}
		for my $type ( @measurement_types,'area','volume' )	{
			for my $part ( @part_list )	{
				if ( $hasType{$type." ".$part} > 0 )	{
					my $value = ( $matrix{$taxon_no}{$type}{$part} ) ? $matrix{$taxon_no}{$type}{$part} : "NA";
					print OUT "\t$value";
				}
			}
		}
		if ( $q->param('refs') =~ /y/i )	{
			my %p_table = %{$tables{$taxon_no}};
			print OUT "\t",csv($p_table{'all_refs'});
		}
		print OUT "\n";
	}
	close OUT;

	if ( $rows < 1 )	{
		my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}
	print "<div style=\"margin-left: 10em; margin-bottom: 5em; width: 35em;\">\n\n";
	print "<p class=\"pageTitle\" style=\"margin-left: 8em;\">Download results</p>\n";
	print "<p class=\"darkList\" style=\"width: 30em; padding: 0.1em; padding-left: 3em;\">Summary</p>\n";
	print "<div style=\"margin-left: 3em;\">\n\n";
	print "<p style=\"width: 26em; margin-left: 1em; text-indent: -1em;\">Search: taxon = ",$q->param('taxon_name');
	if ( $q->param('collection_names') )	{
		print "; collection = ",$q->param('collection_names');
	}
	if ( $countries )	{
		if ( $continent_list =~ /, / )	{
			print "; continents = ",$continent_list;
		} else	{
			print "; continent = ",$continent_list;
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
	print qq|<div>Output data files:
	<div style="margin-top: 0.5em; margin-left: 1em;"><a href="$OUT_HTTP_DIR/$outfile">$outfile</a><br>
	<a href="$OUT_HTTP_DIR/$outfile2">$outfile2</a><br>
	<a href="$OUT_HTTP_DIR/$outfile3">$outfile3</a>
	</div>
	</div>
|;

	print "<p style=\"margin-left: 1em; text-indent: -1em;\">Authorizers: ";
	my (@names,@bits) = (keys %total_authorized,());
	@names = sort { $total_authorized{$b} <=> $total_authorized{$a} } @names;
	push @bits , "$_ ($total_authorized{$_}&nbsp;records)" foreach @names;
	$_ =~ s/\(1&nbsp;records\)/(1&nbsp;record)/ foreach @bits;
	$_ =~ s/([A-Z]\.) /$1&nbsp;/ foreach @bits;
	print join(', ',@bits);
	print "</p>\n";

	print "<p style=\"margin-left: 1em; text-indent: -1em;\">Enterers: ";
	(@names,@bits) = (keys %total_entered,());
	@names = sort { $total_entered{$b} <=> $total_entered{$a} } @names;
	push @bits , "$_ ($total_entered{$_}&nbsp;records)" foreach @names;
	$_ =~ s/\(1&nbsp;records\)/(1&nbsp;record)/ foreach @bits;
	$_ =~ s/([A-Z]\.) /$1&nbsp;/ foreach @bits;
	print join(', ',@bits);
	print "</p>\n";
	print "</p>\n";

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
