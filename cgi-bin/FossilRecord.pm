package FossilRecord;

use Taxon;
use TimeLookup;
use Data::Dumper;
use Collection;
use Reference;
use TaxaCache;
use Ecology;
use Images;
use Measurement;
use Debug qw(dbg);
use PBDBUtil;
use TaxonInfo;
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD);

use strict;

my $ROWS_PER_PAGE = 50;

sub displaySearchTaxaForm {
    my ($dbt,$q,$s,$hbo,$search_again) = @_;

    my $page_title = "Taxonomic name search form"; 
    
	if ($search_again){
        $page_title = "<p class=\"medium\">No results found (please search again)</p>";
    } 
	print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,''], ['page_title','page_subtitle']);
}

# This is the front end for displayTaxonInfoResults - always use this instead if you want to 
# call from another script.  Can pass it a taxon_no or a taxon_name
sub submitSearchTaxaForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    if (!$q->param("taxon_no") && !$q->param('max_interval_name') && !$q->param("taxon_name") && !$q->param("common_name") && !$q->param("author") && !$q->param("pubyr")) {
        displaySearchTaxaForm($dbt,$q,$s,$hbo,1);
        return;
    }
	
    if ($q->param('taxon_no')) {
        # If we have is a taxon_no, use that:
        displayTaxon($dbt,$q,$s,$hbo);
    } elsif ($q->param('max_interval_name')) {
        my $range_type = $q->param('range_type') || 'exist';
        my $t = new TimeLookup($dbt);
        my ($eml_max,$max) = TimeLookup::splitInterval($q->param('max_interval_name'));
        my ($eml_min,$min) = TimeLookup::splitInterval($q->param('min_interval_name'));
        my ($intervals,$errors,$warnings) = $t->getRange($eml_max,$max,$eml_min,$min);
        my @taxon_nos = ();
        
        my ($pre,$range,$post,$unknown) = ({},{},{},{});
        if ($range_type =~ /exist/) {
            ($pre,$range,$post,$unknown) = $t->getCompleteRange($intervals);
        }
        my $pre_sql = join(",",keys(%$pre));
        my $post_sql = join(",",keys(%$post));
        my $range_sql = join(",",@$intervals);

        my $search_sql;
        if ($q->param('range_type') eq 'extinction') {
            $search_sql = "min_interval_no IN ($range_sql)";
        } elsif ($q->param('range_type') eq 'origination') {
            $search_sql = "max_interval_no IN ($range_sql)";
        } elsif ($q->param('range_type') eq 'span') {
            # Both originated and went extinct in the range
            $search_sql = "min_interval_no IN ($range_sql) AND max_interval_no IN ($range_sql)";
        } else {
            # Occurred in
            $search_sql = "(max_interval_no IN ($pre_sql) AND min_interval_no IN ($post_sql))"
                    . " OR (max_interval_no IN ($range_sql))"
                    . " OR (min_interval_no IN ($range_sql))"
        }

        my $sql = "SELECT a.taxon_no,a.taxon_name,a.taxon_rank,"
                . " TRIM(CONCAT(i1.eml_interval,' ',i1.interval_name)) AS max_interval_name,"
                . " TRIM(CONCAT(i2.eml_interval,' ',i2.interval_name)) AS min_interval_name"
                . " FROM taxa_tree_cache c" 
                . " LEFT JOIN authorities a ON c.synonym_no=a.taxon_no "
                . " LEFT JOIN intervals i1 ON i1.interval_no=c.max_interval_no "
                . " LEFT JOIN intervals i2 ON i2.interval_no=c.min_interval_no "
                . " WHERE ($search_sql)"
                . " ORDER BY a.taxon_name";
#                print $sql;

        my @results = @{$dbt->getData($sql)};
        print qq|<div align="center"><h3>Found |.scalar(@results).qq| names</h3>|;
        print qq|<table cellspacing=0 cellpadding=0 border=0>|;
        foreach my $row (@results) {
            my $max_name = $row->{'max_interval_name'};
            my $min_name = $row->{'min_interval_name'};
            if ($min_name eq $max_name) {
                $min_name = "" 
            } else {
                $min_name = " to ".$min_name;
            }
            print "<tr>";
            print "<td><a href=\"$READ_URL?action=checkTaxonInfo&taxon_no=$row->{taxon_no}\">$row->{taxon_name}</a></td>";
            print "<td>&nbsp;&nbsp;</td>";
            print "<td>$max_name $min_name</td>";
            print "</tr>";
        }
        print "</table>";
    } else {
        my $temp = $q->param('taxon_name');
        $temp =~ s/ sp\.//;
        $q->param('taxon_name' => $temp);
        my $options = {'match_subgenera'=>1,'remove_rank_change'=>1};
        foreach ('taxon_name','common_name','author','pubyr') {
            if ($q->param($_)) {
                $options->{$_} = $q->param($_);
            }
        }

        my @results = TaxonInfo::getTaxa($dbt,$options,['taxon_no','taxon_rank','taxon_name','common_name','author1last','author2last','otherauthors','pubyr','pages','figures','comments']);   

        if(scalar @results < 1 && $q->param('taxon_name')){
            displaySearchTaxaForm($dbt,$q,$s,$hbo,1);
            if($s->isDBMember()) {
                print "<center><p><a href=\"$WRITE_URL?action=submitTaxonSearch&amp;goal=authority&amp;taxon_name=".$q->param('taxon_name')."\"><b>Add taxonomic information</b></a></center>";
            }
        } elsif(scalar @results == 1){
            $q->param('taxon_no'=>$results[0]->{'taxon_no'});
            displayTaxon($dbt,$q,$s,$hbo);
        } else {
            # now create a table of choices and display that to the user
            print "<div align=\"center\"><h3>Please select a taxon</h3><br>";
            print qq|<div class="displayPanel" align="center" style="width: 30em; padding-top: 1.5em;">|;

            print qq|<form method="POST" action="$READ_URL">|;
            print qq|<input type="hidden" name="action" value="displayFossilRecordTaxon">|;
            
            print "<table>\n";
            print "<tr>";
            for(my $i=0; $i<scalar(@results); $i++) {
                my $authorityLine = Taxon::formatTaxon($dbt,$results[$i]);
                my $checked = ($i == 0) ? "CHECKED" : "";
                print qq|<td><input type="radio" name="taxon_no" value="$results[$i]->{taxon_no}" $checked> $authorityLine</td>|;
                print "</tr><tr>";
            }
            print "</tr>";
            print "<tr><td align=\"center\" colspan=3><br>";
            print "<input type=\"submit\" value=\"Get taxon info\">";
            print qq|</td></tr></table></form></div></div>|;
        }
    }
}

# By the time we're here, we're gone through checkTaxonInfo and one of these scenarios has happened
#   1: taxon_no is set: taxon is in the authorities table
#   2: taxon_name is set: NOT in the authorities table, but in the occs/reids table
# If neither is set, bomb out, we shouldn't be here
#   entered_name could also be set, for link display purposes. entered_name may not correspond 
#   to taxon_no, depending on if we follow a synonym or go to an original combination
sub displayTaxon {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $taxon_no = $q->param('taxon_no');
    return unless $taxon_no =~ /^\d+$/;

    my $is_real_user = 0;
    if ($q->request_method() eq 'POST' || $q->param('is_real_user') || $s->isDBMember()) {
        $is_real_user = 1;
    }
    if (PBDBUtil::checkForBot()) {
        $is_real_user = 0;
    }

    # Get most recently used name of taxon
    my ($taxon_name,$common_name,$taxon_rank);
    my $orig_taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    $taxon_no = TaxonInfo::getSeniorSynonym($dbt,$orig_taxon_no);
    # This actually gets the most correct name
    my $taxon = TaxonInfo::getMostRecentSpelling($dbt,$taxon_no);
    $taxon_name = $taxon->{'taxon_name'};
    $common_name = $taxon->{'common_name'};
    $taxon_rank = $taxon->{'taxon_rank'};

	# Get the sql IN list for a Higher taxon:
	my $in_list;
    my $quick = 0;;
    my $sql = "SELECT (rgt-lft) diff FROM taxa_tree_cache WHERE taxon_no=$taxon_no";
    my $diff = ${$dbt->getData($sql)}[0]->{'diff'};
    if (!$is_real_user && $diff > 1000) {
        $quick = 1;
        $in_list = [-1];
    } else {
        my @in_list=TaxaCache::getChildren($dbt,$taxon_no);
        $in_list=\@in_list;
    }

    print "<div class=\"small\">";
    my @modules_to_display = (1,2,3,4,5,6,7,8);

    my $display_name = $taxon_name;
    if ( $common_name =~ /[A-Za-z]/ )	{
        $display_name .= " ($common_name)";
    } 
    if ($taxon_no && $common_name !~ /[A-Za-z]/) {
        my $orig_ss = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
        my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$orig_ss);
        my $last_status = $mrpo->{'status'};

        my %disused;
        my $sql = "SELECT synonym_no FROM taxa_tree_cache WHERE taxon_no=$taxon_no";
        my $ss_no = ${$dbt->getData($sql)}[0]->{'synonym_no'};
        if ($taxon_rank !~ /genus|species/) {
            %disused = %{TaxonInfo::disusedNames($dbt,$ss_no)};
        }

        if ($disused{$ss_no}) {
            $display_name .= " (disused)";
        } elsif ($last_status =~ /nomen/) {
            $display_name .= " ($last_status)";
        }
    }

    print '
<script src="/JavaScripts/tabs.js" language="JavaScript" type="text/javascript"></script>                                                                                       
<div align=center>
  <table cellpadding=0 cellspacing=0 border=0 width=700>
  <tr>
    <td id="tab1" class="tabOff" style="white-space: nowrap;"
      onClick="showPanel(1);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(1)">Classification</td>
    <td id="tab2" class="tabOff" style="white-space: nowrap;"
      onClick="showPanel(2);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(2)">Taxonomic history</td>
    <td id="tab3" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(3);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(3)">Synonymy</td>
    <td id="tab4" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(4);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(4)">Ecology</td>
  </tr>
  </table>
</div>
';

    print "<div align=\"center\" style=\"margin-bottom: -1.5em;\"><h2>$display_name</h2></div>\n";

    
    print '<script language="JavaScript" type="text/javascript">
    hideTabText(2);
    hideTabText(3);
    hideTabText(4);
</script>';

    my %modules = ();
    $modules{$_} = 1 foreach @modules_to_display;


	# classification
	if($modules{1}) {
        print '<div id="panel1" class="panel">';
		print '<div align="center"><h3>Classification</h3></div>';
        #print '<div align="center" style=\"border-bottom-width: 1px; border-bottom-color: red; border-bottom-style: solid;\">';
        print '<div align="center">';
		print TaxonInfo::displayTaxonClassification($dbt, $taxon_no, $taxon_name,$is_real_user);

        my $entered_name = $q->param('entered_name') || $q->param('taxon_name') || $taxon_name;
        my $entered_no   = $q->param('entered_no') || $q->param('taxon_no');
        print "<p>";
        print "<div>";
        print "<center>";
        print TaxonInfo::displayRelatedTaxa($dbt, $taxon_no, $taxon_name,$is_real_user);
    	print "<a href=\"$READ_URL?action=beginTaxonInfo\">".
	    	  "<b>Get info on another taxon</b></a></center>\n";
        if($s->isDBMember()) {
            # Entered Taxon
            if ($entered_no) {
                print "<a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_no=$entered_no\">";
                print "<b>Edit taxonomic data for $entered_name</b></a> - ";
            } else {
                print "<a href=\"$WRITE_URL?action=submitTaxonSearch&amp;goal=authority&amp;taxon_no=-1&amp;taxon_name=$entered_name\">";
                print "<b>Enter taxonomic data for $entered_name</b></a> - ";
            }

            if ($entered_no) {
                print "<a href=\"$WRITE_URL?action=displayOpinionChoiceForm&amp;taxon_no=$entered_no\"><b>Edit taxonomic opinions about $entered_name</b></a> - ";
                print "<a href=\"$WRITE_URL.pl?action=startPopulateEcologyForm&amp;taxon_no=$taxon_no\"><b>Add/edit ecological/taphonomic data</b></a> - ";
            }
            
            print "<a href=\"$WRITE_URL?action=startImage\">".
                  "<b>Enter an image</b></a>\n";
        }

        print "</div>\n";
        print "</p>";
        print "</div>\n";
        print "</div>\n";
	}

    print '<script language="JavaScript" type="text/javascript">
    showPanel(1);
</script>';

	# synonymy
	if($modules{2}) {
        print '<div id="panel2" class="panel">';
		print "<div align=\"center\"><h3>Taxonomic history</h3></div>\n";
        print '<div align="center">';
        print TaxonInfo::displayTaxonHistory($dbt, $taxon_no, $is_real_user);
        print "</div>\n";
#		print "<div align=\"center\"><h3></h3></div>\n";
        if ($in_list && @$in_list) {
            print '<div align="center">';
            my $sql = "SELECT max_interval_no,min_interval_no FROM taxa_tree_cache WHERE taxon_no IN (".join(",",@$in_list).")";
            my $data = $dbt->getData($sql);
            my $interval_hash = TaxonInfo::getIntervalsData($dbt,$data);
            my ($lb,$ub,$minfirst,$max,$min) = TaxonInfo::calculateAgeRange($dbt,$data,$interval_hash);
            my $max_no = $max->[0];
            my $min_no = $min->[0];
            if ($max_no || $min_no) {
                $max = ($max_no) ? $interval_hash->{$max_no}->{interval_name} : "";
                $min = ($min_no) ? $interval_hash->{$min_no}->{interval_name} : "";
                my $range;
                if ($max ne $min) {
                    $range .= "<a href=\"$READ_URL?action=displayInterval&interval_no=$max_no\">$max</a> to <a href=\"$READ_URL?action=displayInterval&interval_no=$min_no\">$min</a>";
                } else {
                    $range .= "<a href=\"$READ_URL?action=displayInterval&interval_no=$max_no\">$max</a>";
                } 
                $range .= " <i>or</i> $lb to $ub Ma";

                print "<h3>Age range</h3>\n";
                print " $range <br>"; 
            }
            print "</div>";
        }
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(2); </script>';
	}
	if ($modules{3}) {
        print '<div id="panel3" class="panel">';
		print "<div align=\"center\"><h3>Synonymy</h3></div>\n";
        print '<div align="center">';
    	print TaxonInfo::displaySynonymyList($dbt, $taxon_no);
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(3); </script>';
	}
    if ($modules{4}) {
        print '<div id="panel4" class="panel">';
		print "<div align=\"center\"><h3>Ecology and taphonomy</h3></div>\n";
        print '<div align="center">';
#        unless ($quick) {
		    print TaxonInfo::displayEcology($dbt,$taxon_no,$in_list);
#        }
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(4); </script>';
    }
}

sub displayFossilRecordCurveForm {
    my ($dbt,$q,$s,$hbo) = @_;
}

sub submitFossiLRecordCurveForm {
    my ($dbt,$q,$s,$hbo) = @_;
}

sub displayClassificationUploadForm {
    my ($dbt,$hbo,$s,$q) = @_;
    my $vars = $q->Vars();

    my $enterer_no = $s->get('enterer_no');
    my $sql = "SELECT upload_id,file_name,created FROM uploads WHERE finished=0 AND enterer_no=$enterer_no";
    my @rows = @{$dbt->getData($sql)};
    if (@rows) {
        foreach my $row (@rows) {
            $vars->{in_progress} .= "<a href=\"$WRITE_URL?action=displayClassificationTableForm&upload_id=$row->{upload_id}\">$row->{file_name}, $row->{created}</a><br>";
        }
    } else {
        $vars->{in_progress} = "No uploads currently in progress";
    }
    print $hbo->populateHTML('classification_upload_form',$vars);
}

sub submitClassificationUploadForm {
    require Spreadsheet::ParseExcel;
    my ($dbt,$hbo,$s,$q) = @_;

    my $reference_no = $s->get('reference_no');
    die ("Reference not set") if (!$reference_no);
    
    my $file_name = $q->param('upload_file');
    dbg("FILE NAME: $file_name");
                                                                                                                                         
    # test that we actually got a file
    if(!$file_name) {
        die("ERROR RECEIVING FILE");
        exit;
    }

    my $oE = Spreadsheet::ParseExcel->new();
    
    my $oBook = $oE->Parse($file_name);
    my $oSheet = $oBook->{Worksheet}->[0];
    my $row_count= $oSheet->{MaxRow};

    my @cols = ("parent_spelling_name","child_spelling_name","author","pubyr","max_interval_name","min_interval_name","phylogenetic_status","habitat","first_occurrence","last_occurrence","comments");
    my $col_count = scalar(@cols);

    dbg("PARSED FILE: $row_count ROWS");

    seek($file_name,0,0);
    my $file_data;
    my $length = 0;
    my $buffer;
    while (my $bytes_read = read($file_name,$buffer,4096)) {
        $file_data .= $buffer;
        $length += $bytes_read;
    }

    my @errors = ();
    my @rows = ();
    for(my $i=$oSheet->{MinRow};$i<=$row_count;$i++) {
        my $row = {};
        for(my $j=0;$j<$col_count;$j++) {
            my $C = $oSheet->{Cells}->[$i][$j];
            my $col_name = $cols[$j];
            if ($C) {
                $row->{$col_name} = $C->Value;
            } else {
                $row->{$col_name} = "";
            }
            if ($col_name =~ /habitat|phylogenetic_status/) {
                $row->{$col_name} = lc($row->{$col_name});
            }
        }
        $row->{'line_no'} = $i;
        if ($row->{'child_spelling_name'}) {
            push @rows, $row;
        }
    }

    my $header = shift @rows;
    if ($header) {
        $q->param('parent_spelling_rank'=>lc($header->{'parent_spelling_name'}));
        $q->param('child_spelling_rank'=>lc($header->{'child_spelling_name'}));
    }
    if (!@errors) {
        local $Data::Dumper::Indent = 0;
        my $upload_state = {
            'parent_spelling_rank'=>lc($header->{'parent_spelling_name'}),
            'child_spelling_rank'=>lc($header->{'child_spelling_name'}),
            'reference_no'=>$reference_no,
            'position'=>0,
            'total_rows'=>scalar(@rows)
        };
        my $upload = {
            'reference_no'=>$reference_no,
            'finished'=>0,
            'file_name'=>$file_name,
            'state_data'=>Dumper($upload_state),
            'file_data'=>$file_data,
            'download_available'=>$q->param('download_available'),
            'comments'=>$q->param('comments')
        };
        my ($result,$id) = $dbt->insertRecord($s,'uploads',$upload);

        if ($id) {
            $q->param('upload_id'=>$id);

            for(my $i = 0;$i<@rows;$i++) {
                my $upload_row = {
                    'upload_id'=>$id,
                    'row_id'=>$i,
                    'raw_row'=>Dumper($rows[$i]),
                    'processed_row'=>'',
                    'processed'=>0
                };
                $rows[$i]->{'row_id'} = $i;
                my ($result,$row_id) = $dbt->insertRecord($s,'upload_rows',$upload_row,{'no_autoincrement'=>1});
            }
            # Make a copy before splicing, splicing alters the original array
            my @next_rows = @rows;
            @next_rows = splice(@next_rows,0,$ROWS_PER_PAGE);
            displayClassificationTableForm($dbt,$hbo,$s,$q,undef,undef,$upload_state,\@next_rows);
        }
    } else {
        print "<div align=\"center\">".Debug::printErrors(\@errors)."</div>";
        displayClassificationUploadForm($dbt,$hbo,$s,$q);
    }
}

sub displayClassificationTableForm {
    my ($dbt,$hbo,$s,$q,$errors,$resubmission,$upload,$upload_rows) = @_;

    my %errors;
    if ($errors) {
        %errors = %$errors;
    }

    if (%errors) {
        my $banner = "<span class=\"error_text\">Errors</span> or <span class=\"warning_text\">warnings</span> were encountered in processing this form.  Please correct all errors and optionally correct warnings and resubmit";
        print "<div align=\"center\"><h4>$banner</h4></div>";
        if ($errors{'general'}) {
            my @more_errors = map {"<span class=\"error_text\">$_->{message}</a>"} @{$errors{'general'}};
            print "<ul>";
            foreach my $error (@more_errors) {
                print "<li>$error</li>";
            }
            print "</ul>";
        }
    }

    my $parent_spelling_rank;
    my $child_spelling_rank;

    my $upload_id = int($q->param('upload_id'));
    if ($upload_id && !$upload) {
        $upload = getUploadState($dbt,$upload_id);
        $upload_rows = getUploadRows($dbt,$upload_id,$upload->{'position'},$ROWS_PER_PAGE);
    }

    my @rows;
    my $reference_no;
    if ($resubmission) {
        @rows = CGIToHashRefs($q);
        $parent_spelling_rank = $q->param('parent_spelling_rank'); 
        $child_spelling_rank = $q->param('child_spelling_rank'); 
        $reference_no = $q->param('reference_no');
    } elsif ($upload) {
        $parent_spelling_rank = $upload->{'parent_spelling_rank'};
        $child_spelling_rank = $upload->{'child_spelling_rank'};
        $reference_no = $upload->{'reference_no'};
        @rows = @$upload_rows;
    }  else {
        $parent_spelling_rank = $q->param('parent_spelling_rank'); 
        $child_spelling_rank = $q->param('child_spelling_rank'); 
        $reference_no = $s->get('reference_no');
        $parent_spelling_rank ||= 'family';
        $child_spelling_rank ||= 'genus';
    }
  
    my $rows_to_display;
    if ($upload) {
        $rows_to_display = scalar(@rows);
    } elsif ($resubmission) {
        $rows_to_display = scalar(@rows);
    } else {
        $rows_to_display = $ROWS_PER_PAGE;
    }

    my $processing_msg;
    if ($upload) {
        my $total = $upload->{'total_rows'};
        if ($total > $ROWS_PER_PAGE) {
            my $max = ($upload->{'position'} + $ROWS_PER_PAGE >= $total) ? $total : $upload->{'position'} + $ROWS_PER_PAGE;
            $processing_msg = ": processing rows ".($upload->{position} + 1)." to ".($max)." of $total";
        }
    }

    print qq|<div align="center"><h3>Fossil Record data entry form$processing_msg</h3></div>|;

    print qq|<p>Please see <a href="javascript:tipsPopup('/public/tips/fossil_record_fields.html')">tip sheet</a> for instructions and information about the data fields.</p>|; 
    
    print "<form method=\"POST\" action=\"$WRITE_URL\">";
    print "<input type=\"hidden\" name=\"action\" value=\"submitClassificationTableForm\">";
    print "<input type=\"hidden\" name=\"reference_no\" value=\"$reference_no\">";
    my $confirm_value = (%errors) ? 1 : 0;
    print "<input type=\"hidden\" name=\"confirm_submit\" value=\"$confirm_value\">";
    print "<div align=\"center\">";
    print "<table cellpadding=0 cellspacing=0>";
    my @parent_ranks = reverse grep {!/^$|subgenus|species|tribe|informal|clade|kingdom/} $hbo->getList('taxon_rank');
    my @child_ranks = @parent_ranks;
    my $parent_rank_select = $hbo->htmlSelect('parent_spelling_rank',\@parent_ranks,\@parent_ranks,$parent_spelling_rank);
    my $child_rank_select = $hbo->htmlSelect('child_spelling_rank',\@child_ranks,\@child_ranks,$child_spelling_rank);
    print "<tr><th>$parent_rank_select</th><th>$child_rank_select</th><th>Author(s)</th><th>Pub. yr.</th><th>First apperance</th><th>Last appearance</th><th>Phyl. status</th><th>Habitat</th></tr>";
   
    my @status_values = $hbo->getList('phylogenetic_status');
    my @habitat_values = $hbo->getList('fr_habitat');
    
    for(my $i = 0;$i < $rows_to_display ;$i++) {
        my $row = $rows[$i];
        my $row_class = ($i % 2 == 0) ? 'class="darkList"' : 'class="lightList"';

        my $phylogenetic_status_select = $hbo->htmlSelect('phylogenetic_status',\@status_values,\@status_values,$row->{'phylogenetic_status'});
        my $habitat_select = $hbo->htmlSelect('habitat',\@habitat_values,\@habitat_values,$row->{'habitat'});

        my $parent_select;
        if ($errors{"parent_$i"}) {
            my $data = $errors{"parent_$i"};
            $parent_select = $hbo->radioSelect('parent_spelling_no',$data->{choice_keys},$data->{choice_values},$data->{selected});
            $parent_select .= qq|<input type="hidden" name="parent_spelling_name" value="$row->{parent_spelling_name}">|;
        } else {
            $parent_select = qq|<input type="text" name="parent_spelling_name" value="$row->{parent_spelling_name}" size="16">|;
            $parent_select .= qq|<input type="hidden" name="parent_spelling_no" value="$row->{parent_spelling_no}">|;
        }

        my $child_select;
        if ($errors{"child_$i"}) {
            my $data = $errors{"child_$i"};
            $child_select = $hbo->radioSelect('child_spelling_no',$data->{choice_keys},$data->{choice_values},$data->{selected});
            $child_select .= qq|<input type="hidden" name="child_spelling_name" value="$row->{child_spelling_name}">|;
        } else {
            $child_select = qq|<input type="text" name="child_spelling_name" value="$row->{child_spelling_name}" size="16">|;
            $child_select .= qq|<input type="hidden" name="child_spelling_no" value="$row->{child_spelling_no}">|;
        }

        my $author_select;
        if ($errors{"author_$i"}) {
            my $data = $errors{"author_$i"};
            $author_select = $hbo->radioSelect('author_select',$data->{choice_keys},$data->{choice_values},$data->{selected});
        } else {
            $author_select = qq|<input type="hidden" name="author_select" value="$row->{author_select}">|;
        }

        if ($errors{"fill_author_$i"}) {
            my $data = $errors{"fill_author_$i"};
            $row->{'author'} = $data->{'author'};
            $row->{'pubyr'} = $data->{'pubyr'};
        }

        print qq|
<tr $row_class>
  <td class="padLeft padTop" valign="top">
    <input type="hidden" name="row_id" value="$row->{row_id}">
    $parent_select
  </td>
  <td class="padTop" valign="top">
    $child_select
  </td>
  <td class="padTop" valign="top"><input name="author" size="16" value="$row->{author}"></td>
  <td class="padTop" valign="top"><input name="pubyr" size="4" maxlength=4 value="$row->{pubyr}"></td>
  <td class="padTop" valign="top"><input name="max_interval_name" size="16" value="$row->{max_interval_name}"></td>
  <td class="padTop" valign="top"><input name="min_interval_name" size="16" value="$row->{min_interval_name}"></td>
  <td class="padTop" valign="top">$phylogenetic_status_select</td>
  <td class="padTop padRight" valign="top">$habitat_select</td>
</tr>
<tr $row_class>
  <td colspan="8" style="white-space: nowrap" class="small padLeft padRight padBottom" valign="top">
  <table cellpadding=0 cellspacing=0 border=0><tr>
    <td width="50%">First occurrence: <input name="first_occurrence" class="small" size="55" value="$row->{first_occurrence}"></td>
    <td width="50%">&nbsp;&nbsp;Last occurrence: <input name="last_occurrence" class="small" size="55" value="$row->{last_occurrence}"></td>
  </tr></table>
</tr>
<tr $row_class>
  <td colspan="8" style="white-space: nowrap" class="small padLeft padRight padBottom" valign="top">Comments: <input name="comments" class="small" size="120" value="$row->{comments}">
  </td>
</tr>|;
  #<td class="padTop" valign="top">$crown_select</td>
        if ($errors{"general_$i"}) {
            my @row_errors = @{$errors{"general_$i"}};
            print "<tr $row_class><td colspan=\"8\" class=\"padLeft padRight errorRow\" align=\"left\">";
            foreach my $error (@row_errors) {
                print "<li class=\"$error->{type}_text\">$error->{message}</li>";
            }
            print "</td></tr>";
        }
    }
    print "</table>";
    print "<input type=\"hidden\" name=\"upload_id\" value=\"$upload_id\">";
    print "<input type=\"submit\" name=\"submit\" value=\"Submit classification\">";
    print "</div>";
    print "</form>"
}

sub submitClassificationTableForm {
    my ($dbt,$hbo,$s,$q) = @_;

    # Perform error checking foreach each row before adding any data;
    my @rows = CGIToHashRefs($q);
    my %seen_children = ();
    my %seen_parent;
    my %errors = ();
    my @to_enter = ();

    my %rank_order = %Taxon::rankToNum;
    if ($rank_order{$rows[0]->{'parent_spelling_rank'}} < 
        $rank_order{$rows[0]->{'child_spelling_rank'}}) {
        push @{$errors{'general'}},{'type'=>'error','message'=>"Rank of child must be less than that of parent"};
    }
    if (!$rank_order{$rows[0]->{'parent_spelling_rank'}} ||
        !$rank_order{$rows[0]->{'child_spelling_rank'}}) {
        push @{$errors{'general'}},{'type'=>'error','message'=>"Missing parent rank or child rank"};
    }
    
    my @parent_ranks = reverse grep {!/^$|subgenus|species|tribe|informal|clade|kingdom/} $hbo->getList('taxon_rank');
    my @child_ranks = @parent_ranks;
    my @status_values = $hbo->getList('phylogenetic_status');
    my @habitat_values = $hbo->getList('fr_habitat');

    my %rank_ok   = map {($_,1)} @parent_ranks;
    my %status_ok = map {($_,1)} @status_values;
    my %habitat_ok = map {($_,1)} @habitat_values;
    
    for(my $i = 0;$i < @rows;$i++) {
        my $row = $rows[$i];
        next unless ($row->{'child_spelling_name'});
        my @row_errors = ();

        if ($seen_children{$row->{'child_spelling_name'}}) {
            my $line = $seen_children{$row->{'child_spelling_name'}};
            push @row_errors, {'type'=>'error', 'message'=>"'$row->{child_spelling_name}' was already classified on line $line.  You may only have one classification per taxon per reference."};
        } else {
            $seen_children{$row->{'child_spelling_name'}} = $i+1;
        }


        if (!$row->{'parent_spelling_name'}) {
            for(my $j = $i-1;$j >= 0;$j--) {
                my $prev_row = $rows[$j];
                if ($prev_row->{'parent_spelling_name'}) {
                    $row->{'parent_spelling_name'} = $prev_row->{'parent_spelling_name'};
                    $row->{'parent_spelling_no'} = $prev_row->{'parent_spelling_no'};
                }
            }
        }

        # If they leave the parent assignment row blank, use the previous one
        if (!$row->{'parent_spelling_name'}) {
            push @row_errors, {'type'=>'error','message'=>"No classification was given for $row->{child_spelling_name}"};
        }

        # Some simple parsing first;
        my $db_parent_name = "";
        if ($row->{'parent_spelling_no'} > 0) {
            my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_spelling_no}";
            $db_parent_name = ${$dbt->getData($sql)}[0]->{'taxon_name'};
        }
        if (! $row->{'parent_spelling_no'} || $db_parent_name ne $row->{parent_spelling_name}) {
            if ($row->{'parent_spelling_name'} =~ /^nomen dubium$|^nomen vanum$|^nomen oblitum|^nomen nudum$/i) {
                $row->{'parent_spelling_no'} = -2;
            } elsif (!$seen_parent{$row->{'parent_spelling_name'}}) {
                my ($parent_spelling_no,$general_err,$homonym_err) = parseTaxonName($dbt,$row->{'parent_spelling_name'},$row->{'parent_spelling_rank'},$row->{'parent_spelling_no'},'parent_spelling_no');
                $row->{'parent_spelling_no'} = $parent_spelling_no;
                push @row_errors, @$general_err;
                $errors{"parent_$i"} = $homonym_err if ($homonym_err);
                $seen_parent{$row->{'parent_spelling_name'}} = $row;
            } else {
                my $seen_row = $seen_parent{$row->{'parent_spelling_name'}};
                $row->{'parent_spelling_no'} = $seen_row->{'parent_spelling_no'}; 
            }
        }

        my $db_child_name;
        if ($row->{'child_spelling_no'} > 0) {
            my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_spelling_no}";
            $db_child_name = ${$dbt->getData($sql)}[0]->{'taxon_name'};
        }
        if (! $row->{'child_spelling_no'} || $db_child_name ne $row->{child_spelling_name}) {
            my ($child_spelling_no,$general_err,$homonym_err) = parseTaxonName($dbt,$row->{'child_spelling_name'},$row->{'child_spelling_rank'},$row->{'child_spelling_no'},'child_spelling_no');
            $row->{'child_spelling_no'} = $child_spelling_no;
            push @row_errors, @$general_err;
            $errors{"child_$i"} = $homonym_err if ($homonym_err);
        }

        if ($row->{'pubyr'} && $row->{'pubyr'} !~ /^(17|18|19|20)[0-9][0-9]$/) {
            push @row_errors, {'type'=>'error','message'=>'Publication year improperly formatted'};
        }

        if ($row->{'author'}) {
            # user provided authority data
            my ($a1init,$a1last,$a2init,$a2last,$otherauthors) = parseAuthorName($row->{'author'});
            if ($row->{'child_spelling_no'} > 0) {
                # Theres a pre existing taxon
                my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','pubyr','ref_is_authority','reference_no']); 
                # They are the same
                if ($t->{'author1last'} eq $a1last && 
                    $t->{'author2last'} eq $a2last &&
                    $t->{'pubyr'} eq $row->{'pubyr'}) {
                    # do nothing, 'perfect' enough match
                } elsif ($t->{'author1last'} eq '') {
                    # fill pbdb and warning?
                    $row->{'author_select'} = 'backfill';
                    #push @errors, {'type'=>'warning','message'=>''};
                } else {
                    if (!$row->{'author_select'}) {
                        my @choice_keys = ();
                        my @choice_values = ();
                        my $parent = TaxaCache::getParent($dbt,$t->{taxon_no});
                        my $pub_info = Reference::formatShortRef($t);
                        $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
                        my $higher_class;
                        if ($parent) {
                           $higher_class = $parent->{'taxon_name'}            
                        } else {
                            $higher_class = "unclassified $t->{taxon_rank}";
                        }
                        my $taxon_description = qq|$pub_info [$higher_class]|;
                        push @choice_keys,$taxon_description;
                        push @choice_values,$t->{taxon_no};
                        push @choice_keys,$row->{'author'};
                        push @choice_values, "-1";
                        my $auth_error = {
                            'type'=>'choice',
                            'choice_keys'=>\@choice_keys,
                            'choice_values'=>\@choice_values,
                            'choice_id'=>"author_select",
                            'selected'=>"-1"
                        };
                        push @row_errors, {'type'=>'error','message'=>"'$row->{child_spelling_name}' was found in the database, but with a different author."};
                        $errors{"author_$i"} = $auth_error;
                    }
                }
            } else {
                push @row_errors, {'type'=>'warning','message'=>"'$row->{child_spelling_name}' not found in the database, but a record will be created for it when you hit submit.  Please enter author and pub. yr. now if available."};
            }
        } else {
            # User provided no authority data
            if ($row->{'child_spelling_no'} > 0) {
                # Theres a pre existing taxon
                my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','pubyr','ref_is_authority','reference_no']); 
                # They are the same
                if ($t->{'author1last'} eq '') {
#                    push @row_errors, {'type'=>'warning','message'=>"'$row->{child_spelling_name}' found in the database but without the author and publication year, you may provide that now"};
                } else {
                    #$row->{'author_select'} = 'forwardfill';
                    my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','pubyr','ref_is_authority','reference_no']); 
                    push @row_errors, {'type'=>'warning','message'=>"Found existing taxon, prefilled authority data. Submit again if it's correct."};
                    $errors{"fill_author_$i"} = {'author'=>formatAuthor($t),'pubyr'=>$t->{'pubyr'}};
                }
            } else {
                # No taxon in pbdb, we're creating a new one
                push @row_errors, {'type'=>'warning','message'=>"'$row->{child_spelling_name}' not found in the database, but a record will be created for it when you hit submit.  Please enter author and pub. yr. now if available."};
            }
        }

        if ($row->{'max_interval_name'}) {
            my ($max_no,$err1) = parseIntervalName($dbt,$row->{'max_interval_name'});
            $row->{'max_interval_no'} = $max_no;
            push @row_errors, @$err1;
        }

        if ($row->{'min_interval_name'}) {
            my ($min_no,$err2) = parseIntervalName($dbt,$row->{'min_interval_name'});
            $row->{'min_interval_no'} = $min_no;
            push @row_errors, @$err2;
        }

        if (!($row->{'reference_no'} > 0)) {
            push @row_errors, {'type'=>'error','message'=>'reference_no is not set'};
        }
        if  ($row->{'child_spelling_no'} > 0 && $row->{'parent_spelling_no'} > 0 && $row->{'reference_no'} > 0) {
            my $sql = "SELECT opinion_no FROM opinions WHERE child_spelling_no=$row->{child_spelling_no} AND parent_spelling_no=$row->{parent_spelling_no} AND reference_no=$row->{reference_no} AND ref_has_opinion LIKE 'YES'";
            my @o =@{$dbt->getData($sql)};
            if (@o) {
                push @row_errors, "This opinion is already in the database (<a target=\"_NEW\" href=\"$WRITE_URL?action=displayOpinionForm&opinion_no=$o[0]->{opinion_no}view opinion</a>)";
            }
        }

        # Additional error checking - the !@row_errors, dont' display this error unless we have to
        if ($row->{'child_spelling_name'} !~ /^[A-Z][a-z]+$/)  {
            push @row_errors, {'type'=>'error','message'=>"'$row->{child_spelling_name}' is invalidly formatted"};
        }
        if ($row->{'parent_spelling_no'} != -2 && $row->{'parent_spelling_name'} !~ /^[A-Z][a-z]+$/)  {
            push @row_errors, {'type'=>'error','message'=>"'$row->{parent_spelling_name}' is invalidly formatted"};
        }
        if (! $row->{'parent_spelling_no'} && !@row_errors) {
            push @row_errors, {'type'=>'error','message'=>'parent_spelling_no is not set'};
        }
        if (! $row->{'child_spelling_no'} && !@row_errors) {
            push @row_errors, {'type'=>'error','message'=>'child_spelling_no is not set'};
        }

        if ($row->{'phylogenetic_status'} ne '' && !$status_ok{$row->{'phylogenetic_status'}}) {
            push @row_errors, {'type'=>'error','message'=>'Phylogenetic status not a valid value'}; 
        }
        if ($row->{'habitat'} ne '' && !$habitat_ok{$row->{'habitat'}}) {
            push @row_errors, {'type'=>'error','message'=>'Habitat not a valid value'}; 
        }


        if (@row_errors) {
            $errors{"general_$i"} = \@row_errors;
        }
        push @to_enter,$row;
    }

    # Handle redisplay of the form if there are errors
    if (%errors) {
        my $warning_count = 0;
        my $error_count = 0;
        while (my ($error_id,$errors_ref) = each %errors) {
            if (ref $errors_ref eq 'ARRAY') {
                foreach my $error (@$errors_ref) {
                    if ($error->{'type'} eq 'warning') {
                        $warning_count++;
                    } else {
                        $error_count++;
                    }
                }
            }
        }
        my $confirm_submit = $q->param('confirm_submit');
        if ($error_count || ($warning_count && !$confirm_submit)) {
            displayClassificationTableForm($dbt,$hbo,$s,$q,\%errors,1);
            return;
        }
    }

    my $upload_id = int($q->param('upload_id'));
    my $upload;
    if ($upload_id) {
        $upload = getUploadState($dbt,$upload_id);
    }
    my $upload_finished = 0; 
    if ($upload) {
        setUploadRows($dbt,$upload_id,\@to_enter);
        $upload->{'position'} += $ROWS_PER_PAGE;
        setUploadState($dbt,$upload_id,$upload);
        if ($upload->{'position'} >= $upload->{'total_rows'}) {
            $upload_finished = 1;
            my $all_to_enter = getUploadRows($dbt,$upload_id,0,$upload->{'total_rows'},'processed');
            @to_enter = @$all_to_enter;
        }
    } else {
        $upload_finished = 1;
    }

    if (! $upload_finished) {
        my $upload_rows = getUploadRows($dbt,$upload_id,$upload->{'position'},$ROWS_PER_PAGE);
        displayClassificationTableForm($dbt,$hbo,$s,$q,undef,undef,$upload,$upload_rows);
    } else {
        # Assuming error check is passed,  insert the data
        my @inserted_ids = ();
        %seen_parent = ();
        foreach my $row (@to_enter) {
            next if (!$row);
            next if (!$row->{'child_spelling_no'});
            next if (!$row->{'parent_spelling_no'});
#            print Dumper($row);
            $row->{'status'} = 'belongs to';
            if ($row->{'parent_spelling_no'} eq '-2') {
                $row->{'parent_no'} = 0;
                $row->{'parent_spelling_no'} = 0;
                $row->{'status'} = lc($row->{'parent_spelling_name'});
            } elsif ($row->{'parent_spelling_no'} eq '-1') {
                if ($seen_parent{$row->{'parent_spelling_name'}}) {
                    my $seen_row = $seen_parent{$row->{'parent_spelling_name'}}; 
                    $row->{'parent_spelling_no'} = $seen_row->{'parent_spelling_no'};
                } else {
                    my $taxon = {
                        'reference_no'=>$row->{'reference_no'},
                        'ref_is_authority'=>'',
                        'taxon_rank'=>$row->{'parent_spelling_rank'},
                        'taxon_name'=>$row->{'parent_spelling_name'},
                    };
                    my ($result,$id) = $dbt->insertRecord($s,'authorities',$taxon);
                    $row->{'parent_spelling_no'} = $id;
                    $seen_parent{$row->{'parent_spelling_name'}} = $row;
                    Taxon::setOccurrencesTaxonNoByTaxon($dbt,$s->get('authorizer_no'),$id);
                }
                $row->{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$row->{'parent_spelling_no'});
            } else {
                $row->{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$row->{'parent_spelling_no'});
            }
            if ($row->{'child_spelling_no'} eq '-1') {
                my ($a1init,$a1last,$a2init,$a2last,$otherauthors) = parseAuthorName($row->{'author'});
                my $taxon = {
                    'reference_no'=>$row->{'reference_no'},
                    'ref_is_authority'=>'',
                    'author1init'=>$a1init,
                    'author1last'=>$a1last,
                    'author2init'=>$a2init,
                    'author2last'=>$a2last,
                    'otherauthors'=>$otherauthors,
                    'pubyr'=>$row->{'pubyr'},
                    'taxon_rank'=>$row->{'child_spelling_rank'},
                    'taxon_name'=>$row->{'child_spelling_name'},
                    'first_occurrence'=>$row->{'first_occurrence'},
                    'last_occurrence'=>$row->{'last_occurrence'}
                };
                my ($result,$id) = $dbt->insertRecord($s,'authorities',$taxon);
                $row->{'child_spelling_no'} = $id;
                
                Taxon::setOccurrencesTaxonNoByTaxon($dbt,$s->get('authorizer_no'),$id);
            } elsif ($row->{'child_spelling_no'}) {
                my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','pubyr','ref_is_authority','reference_no']); 
                my $update = {
                    'first_occurrence'=>$row->{'first_occurrence'},
                    'last_occurrence'=>$row->{'last_occurrence'}
                };
                if (!$t->{'author1last'} && $row->{'author'}) {
                    my ($a1init,$a1last,$a2init,$a2last,$otherauthors) = parseAuthorName($row->{'author'});
                    $update->{'author1init'} = $a1init;
                    $update->{'author1last'} = $a1last;
                    $update->{'author2init'} = $a2init;
                    $update->{'author2last'} = $a2last;
                    $update->{'otherauthors'} = $otherauthors;
                    $update->{'pubyr'} = $row->{'pubyr'}
                }
                $dbt->updateRecord($s,'authorities','taxon_no',$row->{'child_spelling_no'},$update);
                Taxon::propagateAuthorityInfo($dbt,$row->{'child_spelling_no'},1);
            }
            if ($row->{'habitat'}) {
                my $sql = "SELECT ecotaph_no FROM ecotaph WHERE taxon_no=".$row->{child_spelling_no};
                my $eco = ${$dbt->getData($sql)}[0];
                if ($eco) {
                    $dbt->updateRecord($s,'ecotaph','ecotaph_no',$eco->{ecotaph_no},{'taxon_environment'=>$row->{'habitat'}});
                } else {
                    $dbt->insertRecord($s,'ecotaph',{'taxon_no'=>$row->{child_spelling_no},'reference_no'=>$row->{reference_no},'taxon_environment'=>$row->{'habitat'}});
                }
            }
            $row->{'child_no'} = TaxonInfo::getOriginalCombination($dbt,$row->{'child_spelling_no'});
            $row->{'ref_has_opinion'} = 'YES';
            my $spelling = {'taxon_no'=>$row->{'child_spelling_no'},'taxon_rank'=>$row->{'child_spelling_rank'},'taxon_name'=>$row->{'child_spelling_name'}};
            my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_no'}});
            $row->{'spelling_reason'} = Opinion::guessSpellingReason($child,$spelling) || 'original spelling';
            my ($result,$id) = $dbt->insertRecord($s,'opinions',$row);
            push @inserted_ids,$id;
        }

        if ($upload) {
            my $sql = "UPDATE uploads SET state_data=NULL,finish_date=NOW(),finished=1 WHERE upload_id=$upload_id";
            my $dbh = $dbt->dbh;
            $dbh->do($sql);
        }

        if (@inserted_ids) {
            print "<div align=\"center\"><h4>Inserted ".scalar(@inserted_ids)." classifications</h4></div>";
            print qq|<div align="center"><table><tr><td>|;
            foreach my $id (@inserted_ids) {
                my $o = Opinion->new($dbt,$id);
                my ($opinion,$authority) = $o->formatAsHTML('return_array'=>1);
                my $child_no = $o->get('child_no');
                my $child_spelling_no = $o->get('child_spelling_no');
                print qq|<li><a href="$WRITE_URL?action=displayOpinionForm&amp;child_no=$child_no&amp;child_spelling_no=$child_spelling_no&amp;opinion_no=$id">$opinion</a></li>|;
            }
            print "</div></table>";
        } else {
            print qq|<div align="center"><h4>No records were inserted</h4></div>|;
        }
    }
}

sub getUploadState {
    my ($dbt,$upload_id) = @_;
    my $sql = "SELECT reference_no,state_data FROM uploads WHERE upload_id=$upload_id AND finished=0";
    my $db_row = ${$dbt->getData($sql)}[0];
    if (! $db_row) { 
        die "There was an error in reading the upload out of the database, please contact the site admin";
    }
    my $VAR1;
    my $upload = eval $db_row->{state_data};
    $upload->{'reference_no'} = $db_row->{'reference_no'};
    return $upload;
}


sub setUploadState {
    my ($dbt,$upload_id,$upload) = @_;
    my $dbh = $dbt->dbh;
    local $Data::Dumper::Indent = 0;
    my $sql = "UPDATE uploads SET state_data=".$dbh->quote(Dumper($upload))." WHERE upload_id=".$upload_id;
    dbg($sql);
    my $result = $dbh->do($sql);
    if (!$result) {
        die "There was an error saving the progress of your upload in the database, please contact the site admin";
    }
}

sub getUploadRows {
    my ($dbt,$upload_id,$row_start,$num_rows,$type) = @_;

    my $row_end = $row_start + $num_rows;
    my $row_name = ($type eq 'processed') ? 'processed_row' : 'raw_row';
    my $sql = "SELECT row_id,$row_name FROM upload_rows WHERE upload_id=$upload_id AND row_id >= $row_start ORDER BY row_id LIMIT $num_rows";
    my @raw_next_rows = @{$dbt->getData($sql)};
    my @next_rows = ();
    foreach my $row (@raw_next_rows) {
        my $VAR1;
        my $real_row = eval $row->{$row_name};
        $real_row->{'row_id'} = $row->{'row_id'};
        push @next_rows, $real_row;
    }
    return \@next_rows;
}

sub setUploadRows {
    my ($dbt,$upload_id,$rows) = @_;
    my $dbh = $dbt->dbh;        
    local $Data::Dumper::Indent = 0;
        
    foreach my $row (@$rows) {
        my $sql = "UPDATE upload_rows SET processed_row=".$dbh->quote(Dumper($row))." WHERE upload_id=$upload_id AND row_id=$row->{row_id}";
        dbg($sql);
        my $result = $dbh->do($sql);
    }
}

sub CGIToHashRefs {
    my $q = shift;
    my @names = $q->param();
    my @rows = ();
    my $max_count = 0;
    foreach my $n (@names) {
        my @vals = $q->param($n);
        $max_count = ($max_count < scalar(@vals)) ? scalar(@vals) : $max_count;
    }
    foreach my $n (@names) {
        my @vals = $q->param($n);
        for(my $i = 0;$i<$max_count;$i++) {
            if (@vals == 1) {
                $rows[$i]->{$n} = $vals[0];
            } else {
                $rows[$i]->{$n} = $vals[$i];
            }
        }
    }

    return @rows;
}
        
sub parseTaxonName {
    my ($dbt,$name,$rank,$no,$id) = @_;
    my $dbh = $dbt->dbh;

    my @nos = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$name,'taxon_rank'=>$rank},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','ref_is_authority','pubyr']);
    my $taxon_no = 0;
    my @general_errors = ();
    my $homonym_error = undef;
    if (@nos == 0) {
        my @matches = TypoChecker::typoCheck($dbt,'authorities','taxon_name','taxon_no,taxon_name,taxon_rank','AND taxon_rank LIKE '.$dbh->quote($rank),$name);
        if (@matches) {
            my $typo_list = join(", ",map {"<a target=\"_NEW\" href=\"$READ_URL?action=displayFossilRecordTaxon&amp;taxon_no=$_->{taxon_no}\">$_->{taxon_name}</a>"} @matches);
            my $error = {
                'type'=>'warning',
                'message'=>"'$name' doesn't exist in the database, but the following close matches were found: $typo_list. If the name entered is correct, please submit again to create a new record for it."
            };
            push @general_errors, $error;
        } else {
#            my $error = {
#                'type'=>'warning',
#                'message'=>"$name doesn't exist in the database, please submit again to create a new record for it."
#            }
#            push @errors, $error;
        }
        $taxon_no = -1;
    } elsif (@nos == 1) {
        $taxon_no = $nos[0]->{'taxon_no'};
    } elsif (@nos > 1) {
        my @choice_keys = ();
        my @choice_values = ();
        foreach my $t (@nos) {
            my $taxon_description = describeTaxon($dbt,$t);
            push @choice_keys,$taxon_description;
            push @choice_values,$t->{taxon_no};
        }
        push @choice_keys, "None of these, create a new record";
        push @choice_values, "-1";
        $homonym_error = {
            'type'=>'choice',
            'choice_keys'=>\@choice_keys,
            'choice_values'=>\@choice_values,
            'choice_id'=>$id,
            'selected'=>$no
        };
        push @general_errors, {'type'=>'error','message'=>"'$name' is ambiguous, please select the one you want."};
    }

    return ($taxon_no,\@general_errors,$homonym_error);
}

sub parseIntervalName {
    my ($dbt,$full_name) = @_;
    my ($eml,$name) = TimeLookup::splitInterval($full_name);
    my $no = TimeLookup::getIntervalNo($dbt,$eml,$name);
    if ($no) {
        return ($no,[]);
    } else {
        my $error = {'type'=>'error','message'=>"Interval '$full_name' was not found in the database"};
        return (0,[$error]);
    }
}

sub parseAuthorName {
    my $auth = shift;
    my $an = new AuthorNames($auth);
    my $a1init = $an->{au1Init} || '';
    my $a1last = $an->{au1Last} || '';
    my $a2init = $an->{au2Init} || '';
    my $a2last = $an->{au2Last} || '';
    my $other  = $an->{auOther} || '';
    return ($a1init,$a1last,$a2init,$a2last,$other);
}

sub formatAuthor {
    my ($t) = @_;
    my $author = "";
    if ($t->{author1init}) {
        $author .= "$t->{author1init} ";
    }
    if ($t->{author1last}) {
        $author .= "$t->{author1last}";
    }
    if ($t->{author2init} || $t->{author2last}) {
        $author .= ", ";
    }
    if ($t->{author2init}) {
        $author .= "$t->{author2init} ";
    }
    if ($t->{author2last}) {
        $author .= "$t->{author2last}";
    }
    if ($t->{otherauthors}) {
        $author .= ", $t->{otherauthors}";
    }
    return $author;
}

sub describeTaxon {
    my ($dbt,$t) = @_;
    my $parent = TaxaCache::getParent($dbt,$t->{taxon_no});
    my $pub_info = Reference::formatShortRef($t);
    $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
    my $higher_class;
    if ($parent) {
        $higher_class = $parent->{'taxon_name'}            
    } else {
        $higher_class = "unclassified $t->{taxon_rank}";
    }
    my $taxon_description = qq|$t->{taxon_name}, $t->{taxon_rank}$pub_info [$higher_class]|;
    return $taxon_description;
}

sub listToTrees {
    my ($dbt,$taxa) = @_;
    my %taxa_lookup = ();
    my @taxon_nos = map {$_->{'taxon_no'}} @$taxa;
    my $parents_hash =  TaxaCache::getParents($dbt,\@taxon_nos,'array_full');
    my %nodes;
    my %root;
    foreach my $t (@$taxa) {
        my @parents = $parents_hash->{$t->{'taxon_no'}};
        my $last_c = $t;
        foreach my $p (@parents) {
            if ($nodes{$p->{'taxon_no'}}) {
                my $p = $nodes{$p->{'taxon_no'}};
                $p->{'children'}{$last_c->{'taxon_no'}} = $last_c;
            } else {
                $nodes{$p->{'taxon_no'}} = $p;
            }
            $last_c = $p;
        }
        $root{$last_c->{'taxon_no'}} = $last_c;
    }
    return values %root;
}

1;
