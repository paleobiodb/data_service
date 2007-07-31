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
use Class::Date qw(now);
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD $TAXA_TREE_CACHE);

use strict;

my $ROWS_PER_PAGE = 50;
my %habitat_order = ();

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
                . " FROM $TAXA_TREE_CACHE c" 
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
            print qq|<input type="hidden" name="action" value="checkTaxonInfo">|;
            
            print "<table>\n";
            print "<tr>";
            for(my $i=0; $i<scalar(@results); $i++) {
                my $authorityLine = Taxon::formatTaxon($dbt,$results[$i]);
                my $checked = ($i == 0) ? "CHECKED DEFAULT" : "";
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
    my $spelling_no = $taxon->{'taxon_no'};

	# Get the sql IN list for a Higher taxon:
	my $in_list;
    my $quick = 0;;
    my $sql = "SELECT (rgt-lft) diff FROM $TAXA_TREE_CACHE WHERE taxon_no=$taxon_no";
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
        my $sql = "SELECT synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$taxon_no";
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
        print TaxonInfo::displayRelatedTaxa($dbt, $taxon_no, $spelling_no, $taxon_name,$is_real_user);
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
            my $sql = "SELECT max_interval_no,min_interval_no FROM $TAXA_TREE_CACHE WHERE taxon_no IN (".join(",",@$in_list).")";
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
                $row->{$col_name} = $C->{Val};
            } else {
                $row->{$col_name} = "";
            }
            if ($col_name =~ /habitat|phylogenetic_status/) {
                $row->{$col_name} = lc($row->{$col_name});
            }
        }
        $row->{'row_id'} = $i;
        if ($row->{'child_spelling_name'}) {
            push @rows,$row;
        }
    }

    my $header = shift @rows;
    my ($parent_spelling_rank,$child_spelling_rank);
    if ($header) {
        $parent_spelling_rank = lc($header->{'parent_spelling_name'});
        $child_spelling_rank = lc($header->{'child_spelling_name'});
        $q->param('parent_spelling_rank'=>$parent_spelling_rank);
        $q->param('child_spelling_rank'=>$child_spelling_rank);
    }
    foreach my $row (@rows) {
        $row->{'child_spelling_rank'} = $child_spelling_rank;
        $row->{'parent_spelling_rank'} = $parent_spelling_rank;
        $row->{'reference_no'} = $reference_no;
    }
    if (!@errors) {
        local $Data::Dumper::Indent = 0;
        my $upload_state = {
            'parent_spelling_rank'=>$parent_spelling_rank,
            'child_spelling_rank'=>$child_spelling_rank,
            'seen_parent'=>{},
            'seen_child'=>{},
            'processed_count'=>0,
            'reference_no'=>$reference_no,
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
                    'row_id'=>$rows[$i]->{'row_id'}, # Same as line no
                    'raw_row'=>Dumper($rows[$i]),
                    'processed_row'=>'',
                    'processed'=>0
                };
                my ($result,$row_id) = $dbt->insertRecord($s,'upload_rows',$upload_row,{'no_autoincrement'=>1});
            }

            my ($general_errors,$row_errors) = validateBlock($dbt,$hbo,\@rows,{},{});
            my @next_rows = ();
            my @process_rows = ();
            foreach my $row (@rows) {
                if ($row_errors->{$row->{row_id}}) {
                    push @next_rows,$row;
                } else {
                    push @process_rows,$row;
                }
                last if (@next_rows >= $ROWS_PER_PAGE);
            }
            $upload_state->{'processed_count'} += scalar(@process_rows);
            setUploadState($dbt,$id,$upload_state);
            setUploadRows($dbt,$id,\@process_rows);

            displayClassificationTableForm($dbt,$hbo,$s,$q,$general_errors,$row_errors,undef,$upload_state,\@next_rows);
        }
    } else {
        print "<div align=\"center\">".Debug::printErrors(\@errors)."</div>";
        displayClassificationUploadForm($dbt,$hbo,$s,$q);
    }
}


sub displayClassificationTableForm {
    my ($dbt,$hbo,$s,$q,$general_errors,$row_errors,$resubmission,$upload,$upload_rows) = @_;
    my @general_errors = ref($general_errors) ? @$general_errors : ();
    my %row_errors = ref($row_errors) ? %$row_errors : ();

    if (@general_errors || %row_errors) {
        my $banner = "<span class=\"error_text\">Errors</span> or <span class=\"warning_text\">warnings</span> were encountered in processing this form.  Please correct all errors and optionally correct warnings and resubmit";
        print "<div align=\"center\"><h4>$banner</h4></div>";
        if (@general_errors) {
            my @more_errors = map {"<span class=\"error_text\">$_->{message}</a>"} @general_errors;
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
        $upload_rows = getUploadRows($dbt,$upload_id);
    }

    my @rows;
    my $reference_no;
    my $rows_to_display;
    if ($resubmission) {
        @rows = CGIToHashRefs($q);
        $rows_to_display = scalar(@rows);
        $parent_spelling_rank = $q->param('parent_spelling_rank'); 
        $child_spelling_rank = $q->param('child_spelling_rank'); 
        $reference_no = $q->param('reference_no');
    } elsif ($upload) {
        @rows = @$upload_rows;
        $rows_to_display = scalar(@rows);
        $parent_spelling_rank = $upload->{'parent_spelling_rank'};
        $child_spelling_rank = $upload->{'child_spelling_rank'};
        $reference_no = $upload->{'reference_no'};
    }  else {
        $rows_to_display = $ROWS_PER_PAGE;
        $parent_spelling_rank = $q->param('parent_spelling_rank'); 
        $child_spelling_rank = $q->param('child_spelling_rank'); 
        $reference_no = $s->get('reference_no');
        $parent_spelling_rank ||= 'family';
        $child_spelling_rank ||= 'genus';
    }
  
    my $processing_msg;
    if ($upload) {
#        print Dumper($upload);
        my $total = $upload->{'total_rows'};
        if ($total > $ROWS_PER_PAGE) {
            my $sql = "SELECT count(*) processed_count FROM upload_rows WHERE upload_id=$upload_id AND processed=1";
            my $processed_count = ${$dbt->getData($sql)}[0]->{'processed_count'};
            my $max = ($processed_count + $ROWS_PER_PAGE >= $total) ? $total : $processed_count + $ROWS_PER_PAGE;
            if ($processed_count > $total) {
                $processing_msg = ": processed $total rows, submit to upload";
            } else {
                $processing_msg = ": processing rows ".($upload->{processed_count} + 1)." to ".($max)." of $total";
            }
        }
    }

    print qq|<div align="center"><h3>Fossil Record data entry form$processing_msg</h3></div>|;

    print qq|<p>Please see <a href="javascript:tipsPopup('/public/tips/fossil_record_fields.html')">tip sheet</a> for instructions and information about the data fields.</p>|; 
    
    print "<form method=\"POST\" action=\"$WRITE_URL\">";
    print "<input type=\"hidden\" name=\"action\" value=\"submitClassificationTableForm\">";
    print "<input type=\"hidden\" name=\"reference_no\" value=\"$reference_no\">";
    my $confirm_value = (%row_errors) ? 1 : 0;
    print "<input type=\"hidden\" name=\"confirm_submit\" value=\"$confirm_value\">";
    print "<div align=\"center\">";
    print "<table cellpadding=0 cellspacing=0>";
    my @parent_ranks = reverse grep {!/^$|subgenus|species|tribe|informal|clade|kingdom/} $hbo->getList('taxon_rank');
    my @child_ranks = @parent_ranks;
    my $parent_rank_select = $hbo->htmlSelect('parent_spelling_rank',\@parent_ranks,\@parent_ranks,$parent_spelling_rank);
    my $child_rank_select = $hbo->htmlSelect('child_spelling_rank',\@child_ranks,\@child_ranks,$child_spelling_rank);
    print "<tr><th></th><th>$parent_rank_select</th><th>$child_rank_select</th><th>Author(s)</th><th>Year</th><th>First appearance</th><th>Last appearance</th><th>Phyl. status</th><th>Habitat</th></tr>";
   
    my @status_values = $hbo->getList('phylogenetic_status');
    my @habitat_values = $hbo->getList('fr_habitat');
    
    for(my $i = 0;$i < $rows_to_display ;$i++) {
        my $row = $rows[$i];
        my $row_id = ($resubmission || $upload) ? $row->{'row_id'} : $i + 1;
        my $row_class = ($i % 2 == 0) ? 'class="darkList"' : 'class="lightList"';

        my $phylogenetic_status_select = $hbo->htmlSelect("phylogenetic_status_$row_id",\@status_values,\@status_values,$row->{'phylogenetic_status'},'class="small"');
        my $habitat_select = $hbo->htmlSelect("habitat_$row_id",\@habitat_values,\@habitat_values,$row->{'habitat'},'class="small"');

        my $parent_select;
        if ($row_errors{$row_id}{"parent"}) {
            my $data = $row_errors{$row_id}{"parent"};
#            print "PARENT_SL".Dumper($data)."<br>";
            $parent_select = $hbo->radioSelect("parent_spelling_no_$row_id",$data->{choice_keys},$data->{choice_values},$data->{selected});
            $parent_select .= qq|<input type="hidden" name="parent_spelling_name_$row_id" value="$row->{parent_spelling_name}">|;
        } else {
            $parent_select = qq|<input type="text" name="parent_spelling_name_$row_id" value="$row->{parent_spelling_name}" size="14" class="small">|;
            $parent_select .= qq|<input type="hidden" name="parent_spelling_no_$row_id" value="$row->{parent_spelling_no}">|;
        }

        my $child_select;
        if ($row_errors{$row_id}{"child"}) {
            my $data = $row_errors{$row_id}{"child"};
#            print "CHILD_SEL".Dumper($data)."<br>";
            $child_select = $hbo->radioSelect("child_spelling_no_$row_id",$data->{choice_keys},$data->{choice_values},$data->{selected});
            $child_select .= qq|<input type="hidden" name="child_spelling_name_$row_id" value="$row->{child_spelling_name}">|;
        } else {
            $child_select = qq|<input type="text" name="child_spelling_name_$row_id" value="$row->{child_spelling_name}" size="14" class="small">|;
            $child_select .= qq|<input type="hidden" name="child_spelling_no_$row_id" value="$row->{child_spelling_no}">|;
        }

#        my $author_select;
#        if ($row_errors{$row_id}{"author"}) {
#            my $data = $row_errors{$row_id}{"author"};
#            print "AUTH_SEL".Dumper($data)."<br>";
#            $author_select = $hbo->radioSelect('author_select',$data->{choice_keys},$data->{choice_values},$data->{selected});
#        } else {
#         my $author_select = qq|<input type="hidden" name="author_select_$row_id" value="$row->{author_select}">|;
#        }

        if ($row_errors{$row_id}{"fill_author"}) {
            my $data = $row_errors{$row_id}{"fill_author"};
            $row->{'author'} = $data->{'author'};
            $row->{'pubyr'} = $data->{'pubyr'};
        }

        my $rowspan = 3;
        if ($row_errors{$row_id}{"general"}) {
            $rowspan = 4;
        }
        print qq|
<tr $row_class>
  <td rowspan=$rowspan valign="top" class="padTop padLeft">
    $row_id
  </td>
  <td class="padLeft padTop" valign="top">
    <input type="hidden" name="row_id" value="$row_id">
    $parent_select
  </td>
  <td class="padTop" valign="top">
    $child_select
  </td>
  <td class="padTop" valign="top"><input name="author_$row_id" size="16" value="$row->{author}" class="small"></td>
  <td class="padTop" valign="top"><input name="pubyr_$row_id" size="4" maxlength=4 value="$row->{pubyr}" class="small"></td>
  <td class="padTop" valign="top"><input name="max_interval_name_$row_id" size="14" value="$row->{max_interval_name}" class="small"></td>
  <td class="padTop" valign="top"><input name="min_interval_name_$row_id" size="14" value="$row->{min_interval_name}" class="small"></td>
  <td class="padTop" valign="top">$phylogenetic_status_select</td>
  <td class="padTop padRight" valign="top">$habitat_select</td>
</tr>
<tr $row_class>
  <td colspan="8" style="white-space: nowrap" class="small padLeft padRight padBottom" valign="top">
  <table cellpadding=0 cellspacing=0 border=0><tr>
    <td width="50%" nowrap>First occ.: <input name="first_occurrence_$row_id" class="small" size="50" value="$row->{first_occurrence}"></td>
    <td width="50%" nowrap>&nbsp;&nbsp;Last occ.: <input name="last_occurrence_$row_id" class="small" size="50" value="$row->{last_occurrence}"></td>
  </tr></table>
</tr>
<tr $row_class>
  <td colspan="8" style="white-space: nowrap" class="small padLeft padRight padBottom" valign="top">Comments: <input name="comments_$row_id" class="small" size="110" value="$row->{comments}">
  </td>
</tr>|;
  #<td class="padTop" valign="top">$crown_select</td>
        if ($row_errors{$row_id}{"general"}) {
            my @row_errors = @{$row_errors{$row_id}{"general"}};
            print "<tr $row_class><td colspan=\"8\" class=\"padLeft padRight errorRow\" align=\"left\">";
            # Errors printed first, then warnings
            foreach my $error (@row_errors) {
                if ($error->{type} eq 'error') {
                    print "<li class=\"$error->{type}_text\">$error->{message}</li>";
                } 
            }
            foreach my $error (@row_errors) {
                if ($error->{type} ne 'error') {
                    print "<li class=\"$error->{type}_text\">$error->{message}</li>";
                } 
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
    my %seen_child = ();
    my %seen_parent = ();
    my %row_errors = ();

    my $upload_id = int($q->param('upload_id'));
    my $upload;
    if ($upload_id) {
        $upload = getUploadState($dbt,$upload_id);
        %seen_child = %{$upload->{seen_child}};
        %seen_parent = %{$upload->{seen_parent}};
    }

    my ($general_errors,$row_errors) = validateBlock($dbt,$hbo,\@rows,\%seen_parent,\%seen_child);
    
    # Handle redisplay of the form if there are errors
#    print Dumper($general_errors,$row_errors);
    if (@$general_errors || %$row_errors) {
        my $warning_count = 0;
        my $error_count = 0;
        foreach my $error (@$general_errors) {
            if ($error->{'type'} eq 'warning') {
                $warning_count++;
            } else {
                $error_count++;
            }
        }
        while (my ($row_id,$row_ref) = each %$row_errors) {
            while(my ($error_name,$errors_row) = each %$row_ref) {
                if (ref $errors_row eq 'ARRAY') {
                    foreach my $error (@$errors_row) {
                        if ($error->{'type'} eq 'warning') {
                            $warning_count++;
                        } else {
                            $error_count++;
                        }
                    }
                }
            }
        }
        my $confirm_submit = $q->param('confirm_submit');
        if ($error_count || ($warning_count && !$confirm_submit)) {
            displayClassificationTableForm($dbt,$hbo,$s,$q,$general_errors,$row_errors,1,{},{});
            return;
        }
    }
    my @to_enter = @rows;

    my $upload_finished = 0; 
    if ($upload_id) {
        $upload->{'seen_child'} = \%seen_child;
        $upload->{'seen_parent'} = \%seen_parent;
        $upload->{'processed_count'} += scalar(@to_enter);
        setUploadState($dbt,$upload_id,$upload);
        setUploadRows($dbt,$upload_id,\@to_enter);
        if ($upload->{'processed_count'} >= $upload->{'total_rows'}) {
            $upload_finished = 1;
            my $all_to_enter = getUploadRows($dbt,$upload_id,'processed');
            @to_enter = @$all_to_enter;
        }
    } else {
        $upload_finished = 1;
    }

    if (! $upload_finished) {
        my $upload_rows = getUploadRows($dbt,$upload_id);
        my ($general_errors,$row_errors) = validateBlock($dbt,$hbo,$upload_rows,\%seen_parent,\%seen_child);
        displayClassificationTableForm($dbt,$hbo,$s,$q,$general_errors,$row_errors,0,$upload,$upload_rows);
    } else {
        # Assuming error check is passed,  insert the data
        my @inserted_ids = ();
        my %seen_parent = ();
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
                    my $parent_spelling_no = $seen_parent{$row->{'parent_spelling_name'}}; 
                    $row->{'parent_spelling_no'} = $parent_spelling_no;
                } else {
                    my $taxon = {
                        'reference_no'=>$row->{'reference_no'},
                        'ref_is_authority'=>'',
                        'taxon_rank'=>$row->{'parent_spelling_rank'},
                        'taxon_name'=>$row->{'parent_spelling_name'},
                    };
                    my ($result,$id) = $dbt->insertRecord($s,'authorities',$taxon);
                    $row->{'parent_spelling_no'} = $id;
                    $seen_parent{$row->{'parent_spelling_name'}} = $row->{'parent_spelling_no'};
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
                if ($t->{'ref_is_authority'} !~ /yes/i && $row->{'author'}) {
                    my $update = {};
                    my ($a1init,$a1last,$a2init,$a2last,$otherauthors) = parseAuthorName($row->{'author'});
                    $update->{'author1init'} = $a1init;
                    $update->{'author1last'} = $a1last;
                    $update->{'author2init'} = $a2init;
                    $update->{'author2last'} = $a2last;
                    $update->{'otherauthors'} = $otherauthors;
                    $update->{'pubyr'} = $row->{'pubyr'};
                    $dbt->updateRecord($s,'authorities','taxon_no',$row->{'child_spelling_no'},$update);
                    Taxon::propagateAuthorityInfo($dbt,$row->{'child_spelling_no'},1);
                }
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

            # Remove this, only applies to the taxa
            delete $row->{'pubyr'};

            my ($result,$id) = $dbt->insertRecord($s,'opinions',$row);
            push @inserted_ids,$id;
        }

        if ($upload) {
            my $sql = "UPDATE uploads SET state_data=NULL,finish_date=NOW(),finished=1 WHERE upload_id=$upload_id";
            my $dbh = $dbt->dbh;
            $dbh->do($sql);
        }

        if (@inserted_ids) {
            print "<div align=\"center\"><h4>Inserted ".scalar(@inserted_ids)." opinions</h4></div>";
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
        print "<br><br>";
        print qq|
            <div align="center">
            <ul class="small" style="text-align: left;">
              <li><a href="$WRITE_URL?action=displayClassificationUploadForm">Upload a spreadsheet</a></li>
              <li><a href="$WRITE_URL?action=displayClassificationTableForm">Enter a spreadsheet</a></li>
            <ul>
            </div>|;
    }
}

sub validateBlock {
    my ($dbt,$hbo,$rows,$seen_parent,$seen_child) = @_;
    my @rows = @$rows;

    my (@general_errors, %row_errors);
    my %rank_order = %Taxon::rankToNum;

    if ($rank_order{$rows[0]->{'parent_spelling_rank'}} < 
        $rank_order{$rows[0]->{'child_spelling_rank'}}) {
        push @general_errors,{'type'=>'error','message'=>"Rank of child must be less than that of parent"};
    }
    if (!$rank_order{$rows[0]->{'parent_spelling_rank'}} ||
        !$rank_order{$rows[0]->{'child_spelling_rank'}}) {
        push @general_errors,{'type'=>'error','message'=>"Missing parent rank or child rank"};
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

        # No behind the scenes autofill for now
        #if (!$row->{'parent_spelling_name'}) {
        #    for(my $j = $i-1;$j >= 0;$j--) {
        #        my $prev_row = $rows[$j];
        #        if ($prev_row->{'parent_spelling_name'}) {
        #            $row->{'parent_spelling_name'} = $prev_row->{'parent_spelling_name'};
        #            $row->{'parent_spelling_no'} = $prev_row->{'parent_spelling_no'};
        #        }
        #    }
        #}

        my $row_errors = validateRow($dbt,$hbo,$row,$seen_parent,$seen_child,\%status_ok,\%habitat_ok);
        while (my ($k,$v) = each %$row_errors) {
            $row_errors{$row->{row_id}}{$k} = $v;
        }
    }
    return (\@general_errors,\%row_errors);
}

sub validateRow {
    my ($dbt,$hbo,$row,$seen_parent,$seen_child,$status_ok,$habitat_ok) = @_;

    my %errors = ();
    my @row_errors = ();

    return unless ($row->{'child_spelling_name'});

    if ($seen_child->{$row->{'child_spelling_name'}}) {
        my $row_id = $seen_child->{$row->{'child_spelling_name'}};
        push @row_errors, {'type'=>'error', 'message'=>"'$row->{child_spelling_name}' was already classified on line $row_id.  You may only have one classification per taxon per reference"};
    } else {
        $seen_child->{$row->{'child_spelling_name'}} = $row->{'row_id'};
    }


    # If they leave the parent assignment row blank, use the previous one
    if (!$row->{'parent_spelling_name'}) {
        push @row_errors, {'type'=>'error','message'=>"No classification was given for $row->{child_spelling_name}"};
    }

    # Some simple parsing first;
    if ($row->{'parent_spelling_no'} > 0) {
        my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_spelling_no}";
        my $db_parent_name = ${$dbt->getData($sql)}[0]->{'taxon_name'};
        if ($db_parent_name ne $row->{parent_spelling_name}) {
            $row->{'parent_spelling_no'} = 0;
        }
    }
    if (! $row->{'parent_spelling_no'}) {
        if ($row->{'parent_spelling_name'} =~ /^nomen dubium$|^nomen vanum$|^nomen oblitum|^nomen nudum$/i) {
            $row->{'parent_spelling_no'} = -2;
        } elsif (!$seen_parent->{$row->{'parent_spelling_name'}}) {
            my ($parent_spelling_no,$general_err,$homonym_err) = parseParentName($dbt,$row->{'parent_spelling_name'},$row->{'parent_spelling_rank'},$row->{'parent_spelling_no'});
            $row->{'parent_spelling_no'} = $parent_spelling_no;
            push @row_errors, @$general_err;
            $errors{"parent"} = $homonym_err if ($homonym_err);
            $seen_parent->{$row->{'parent_spelling_name'}} = $parent_spelling_no;
        } else {
            my $parent_spelling_no = $seen_parent->{$row->{'parent_spelling_name'}};
            $row->{'parent_spelling_no'} = $parent_spelling_no; 
        }
    }

#    print Dumper($seen_parent)."<br>";

    if ($row->{'child_spelling_no'} > 0) {
        my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_spelling_no}";
        my $db_child_name = ${$dbt->getData($sql)}[0]->{'taxon_name'};
        if ($db_child_name ne $row->{child_spelling_name}) {
            $row->{'child_spelling_no'} = 0;
        }
    }
    if (! $row->{'child_spelling_no'}) {
        my ($child_spelling_no,$general_err,$homonym_err,$fill_author) = parseChildName($dbt,$row);
        $row->{'child_spelling_no'} = $child_spelling_no;
        push @row_errors, @$general_err;
        $errors{"child"} = $homonym_err if ($homonym_err);
        $errors{"fill_author"} = $fill_author if ($fill_author);
    }

    if ($row->{'pubyr'} && !$row->{'author'} || $row->{'author'} && !$row->{'pubyr'}) {
        push @row_errors, {'type'=>'error','message'=>'If pub. yr. is entered, enter author as well, and vice versa'};
    } 
    if ($row->{'pubyr'}) {
        my $this_yr = now()->year;
        if ($row->{'pubyr'} !~ /^\d\d\d\d$/) {
            push @row_errors, {'type'=>'error','message'=>'Publication year improperly formatted'};
        } elsif ($row->{'pubyr'} < 1700 || $row->{'pubyr'} > $this_yr) {
            push @row_errors, {'type'=>'error','message'=>'Publication year not valid'};
        }
    }


    if ($row->{'max_interval_name'}) {
        my ($max_no,$err1) = parseIntervalName($dbt,$row->{'max_interval_name'});
        $row->{'max_interval_no'} = $max_no;
        push @row_errors, @$err1;
    } elsif ($row->{'child_spelling_rank'} =~ /genus/) {
        push @row_errors, {'type'=>'error','message'=>'First interval is required'};
    }

    if ($row->{'min_interval_name'}) {
        my ($min_no,$err2) = parseIntervalName($dbt,$row->{'min_interval_name'});
        $row->{'min_interval_no'} = $min_no;
        push @row_errors, @$err2;
    } elsif ($row->{'child_spelling_rank'} =~ /genus/) {
        push @row_errors, {'type'=>'error','message'=>'Last interval is required'};
    }

    if  ($row->{'child_spelling_no'} > 0 && $row->{'parent_spelling_no'} > 0 && $row->{'reference_no'} > 0) {
        my $sql = "SELECT opinion_no FROM opinions WHERE child_spelling_no=$row->{child_spelling_no} AND parent_spelling_no=$row->{parent_spelling_no} AND reference_no=$row->{reference_no} AND ref_has_opinion LIKE 'YES'";
        my @o =@{$dbt->getData($sql)};
        if (@o) {
            push @row_errors, {'type'=>'error','message'=>"This opinion is already in the database (<a target=\"_NEW\" href=\"$WRITE_URL?action=displayOpinionForm&opinion_no=$o[0]->{opinion_no}\">view opinion</a>)"};
        }
    }

    # Additional error checking - the !@row_errors, dont' display this error unless we have to
    if ($row->{'child_spelling_name'} !~ /^$|^[A-Z][a-z]+$/)  {
        push @row_errors, {'type'=>'error','message'=>"'$row->{child_spelling_name}' is invalidly formatted"};
    }
    if ($row->{'parent_spelling_no'} != -2 && $row->{'parent_spelling_name'} !~ /^$|^[A-Z][a-z]+$/)  {
        push @row_errors, {'type'=>'error','message'=>"'$row->{parent_spelling_name}' is invalidly formatted"};
    }
    if (! $row->{'parent_spelling_no'} && !@row_errors) {
        push @row_errors, {'type'=>'error','message'=>'parent_spelling_no is not set'};
    }
    if (! $row->{'child_spelling_no'} && !@row_errors) {
        push @row_errors, {'type'=>'error','message'=>'child_spelling_no is not set'};
    }

    if ($row->{'phylogenetic_status'} ne '' && !$status_ok->{$row->{'phylogenetic_status'}}) {
        push @row_errors, {'type'=>'error','message'=>'Phylogenetic status not a valid value'}; 
    }
    if ($row->{'habitat'} ne '') {
        my ($habitat) = parseHabitat($hbo,$row->{'habitat'});
        $row->{'habitat'} = $habitat;
        if (!$habitat_ok->{$habitat}) {
            push @row_errors, {'type'=>'error','message'=>"Invalid option for habitat: $habitat"}; 
        }

        my $eco;
        if ($row->{'child_spelling_no'}) {
            my $sql = "SELECT * FROM ecotaph WHERE taxon_no=$row->{'child_spelling_no'}";
            $eco = ${$dbt->getData($sql)}[0];
        }

        if ($eco) {
            my $conflict = "";
            if ($eco->{'taxon_environment'} ne $habitat) {
                push @row_errors, {'type'=>'error','message'=>"Can not set habitat value, is currently $eco->{taxon_environment}, please manually fix"}; 
            }
        } 
    }

    if (@row_errors) {
        $errors{"general"} = \@row_errors;
    }
    return \%errors;
}


sub parseHabitat {
    my ($hbo,$habitat) = @_;
    my @RAW_VALS = map {lc($_)} split("\s*[,\0]\s*",$habitat);
    if (!%habitat_order) {
        my @habitat_values = $hbo->getList('taxon_environment');
        my $order = 1;
        foreach my $v (@habitat_values) {
            if ($v ne '') {
                $habitat_order{$v} = $order;
                $order++;
            }
        }
    }
    my @VALS;

#    print Dumper(\%habitat_order);
    my @errors;
    foreach my $v (@RAW_VALS) {
        if ($v) {
            if ($habitat_order{$v}) {
                push @VALS,$v;
            } else {
                push @errors, {'type'=>'error','message'=>"Habitat has invalid value $v"}; 
            }
        }
    }
    
    @VALS = sort {$habitat_order{$a} <=> $habitat_order{$b}} @VALS;
    $habitat = join(",",@VALS);
    return ($habitat,\@errors);
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
    my ($dbt,$upload_id,$type) = @_;

    my $sql;
    if ($type eq 'processed') {
        $sql = "SELECT row_id,processed_row row_data FROM upload_rows WHERE upload_id=$upload_id";
    } else {
        $sql = "SELECT row_id,raw_row row_data FROM upload_rows WHERE upload_id=$upload_id AND processed=0 LIMIT $ROWS_PER_PAGE";
    }
    my @raw_next_rows = @{$dbt->getData($sql)};
    my @next_rows = ();
    foreach my $row (@raw_next_rows) {
        my $VAR1;
        my $real_row = eval $row->{'row_data'};
        $real_row ||= {};
        $real_row->{'row_id'} = $row->{'row_id'};
        push @next_rows, $real_row;
    }
    return \@next_rows;
}

sub setUploadRows {
    my ($dbt,$upload_id,$rows) = @_;
#    print "SETUPLOADROWS CALLED: ".Dumper($rows);
    my $dbh = $dbt->dbh;        
    local $Data::Dumper::Indent = 0;
        
    foreach my $row (@$rows) {
        my $sql = "UPDATE upload_rows SET processed=1,processed_row=".$dbh->quote(Dumper($row))." WHERE upload_id=$upload_id AND row_id=$row->{row_id}";
        dbg($sql);
        my $result = $dbh->do($sql);
    }
}

sub CGIToHashRefs {
    my $q = shift;
    my @names = $q->param();
    #foreach my $n (@names) {
    #    my @vals = $q->param($n);
    #    $max_count = ($max_count < scalar(@vals)) ? scalar(@vals) : $max_count;
    #}
    my %seen_ids = ();
    foreach my $n (@names) {
#        my @vals = $q->param($n);
        if ($n =~ /^(.*)_(\d+)$/) {
            my $key = $1;
            my $row_id = $2;
            $seen_ids{$row_id}{$key} = $q->param($n);
        } 
    }
    foreach my $n (@names) {
        if ($n !~ /^row_id$|^(.*)_(\d+)$/) {
            foreach my $id (keys %seen_ids) {
                $seen_ids{$id}{$n} = $q->param($n);
            } 
        }
    }
    my @rows;
    foreach my $id (sort {$a <=> $b} keys %seen_ids) {
        my $row = $seen_ids{$id};
        $row->{'row_id'} = $id;
        push @rows, $seen_ids{$id};
    }

    return @rows;
}
        
sub parseChildName {
    my ($dbt,$row) = @_;
    my $name = $row->{'child_spelling_name'};
    my $rank = $row->{'child_spelling_rank'};
    my $no   = $row->{'child_spelling_no'};
    my $parent_spelling_no = $row->{'parent_spelling_no'};
    my $dbh = $dbt->dbh;


    my @taxa= ();
    if ($no > 0) {
        @taxa= TaxonInfo::getTaxa($dbt,{'taxon_no'=>$no},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','ref_is_authority','pubyr']);
    } else {
        my $restrict_rank = "";
        if ($rank =~ /genus/) {
            $restrict_rank = $rank;
        }
        @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$name,'taxon_rank'=>$restrict_rank},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','ref_is_authority','pubyr']);
    }


    my $has_common_ancestor = 0;
    my $is_same_authority = 0;
    for(my $i=0;$i<@taxa;$i++) {
        my $t = $taxa[$i];
        $is_same_authority = isSameAuthority($t,$row->{author},$row->{pubyr});
        $has_common_ancestor = hasCommonAncestor($dbt,$t->{taxon_no},$parent_spelling_no);

        if ($is_same_authority || $has_common_ancestor) {
            @taxa = ($t);
#            print "MATCHED $is_same_authority $has_common_ancestor: ".Dumper($t)."<br>";
            last;
        }
    }
    my $taxon_no = 0;
    my $taxon = {};
    my @general_errors = ();
    my $homonym_error = undef;
    my $fill_author = undef;
    if (@taxa == 0) {
        my @matches = TypoChecker::typoCheck($dbt,'authorities','taxon_name','taxon_no,taxon_name,taxon_rank','AND taxon_rank LIKE '.$dbh->quote($rank),$name);
        if (@matches) {
            my $typo_list = join(", ",map {"<a target=\"_NEW\" href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$_->{taxon_no}\">$_->{taxon_name}</a>"} @matches);
            my $error = {
                'type'=>'warning',
                'message'=>"'$name' not found in the database, but the following close matches were found: $typo_list. If the name entered is correct, please submit again to create a new record for it"
            };
            push @general_errors, $error;
        } else {
            my $error = {
                'type'=>'warning',
                'message'=>"'$name' not found in the database, please submit again to create a new record for it"
            };
            push @general_errors, $error;
        }
        $taxon_no = -1;
    } elsif (@taxa == 1) {
        $taxon_no = $taxa[0]->{'taxon_no'};
        $taxon = $taxa[0];
    } elsif (@taxa > 1) {
        my ($k,$v) = makeTaxonChoices($dbt,\@taxa);
        $homonym_error = {
            'type'=>'choice',
            'choice_keys'=>$k,
            'choice_values'=>$v,
            'choice_id'=>'child_spelling_no',
            'selected'=>$no
        };
        push @general_errors, {'type'=>'error','message'=>"'$name' is ambiguous, please select the one you want"};
    }

    if ($taxon_no > 0) {
        # Theres a pre existing taxon
        my $t = $taxon;
#        print Dumper($t);
        # They are the same
        if ($is_same_authority) {
            # Do nothing, are set
        } elsif ($t->{'author1last'} eq '') {
            # pbdb version has no authority data
            if ($has_common_ancestor) {
                $row->{'author_select'} = 'backfill';
            } else {
                #-- list out classification info, make sure its not a homonym
                my ($k,$v) = makeTaxonChoices($dbt,[$t]);
                $homonym_error = {
                    'type'=>'choice',
                    'choice_keys'=>$k,
                    'choice_values'=>$v,
                    'choice_id'=>'child_spelling_no',
                    'selected'=>$taxon_no
                };
                push @general_errors, {'type'=>'warning','message'=>"'$row->{child_spelling_name}' found in the database. Please check if it the same taxon and optionally fill in author and pub. yr."};
            }
        } elsif ($t->{'author1last'} ne '') {
            if ($row->{'author'}) {
                # Some sort of conflict in the names.  If pbdb ref_is_authority, give an error
                # Warning if ! $found_common_ancestor
                my ($k,$v) = makeTaxonChoices($dbt,[$t]);
                $homonym_error = {
                    'type'=>'choice',
                    'choice_keys'=>$k,
                    'choice_values'=>$v,
                    'choice_id'=>'child_spelling_no',
                    'selected'=>-1
                };
                my $overwrite_msg = "Correct the existing record or create a new record if it's a different taxon.";
                push @general_errors, {'type'=>'error','message'=>"'$row->{child_spelling_name}' $taxon_no was found in the database, but with a different author. $overwrite_msg"};
                #if (!$row->{'author_select'}) {
                #    push @row_errors, {'type'=>'error','message'=>"'$row->{child_spelling_name}' was found in the database, but with a different author"};
                #    $errors{"author"} = $auth_error;
                #}
            } else {
                #$row->{'author_select'} = 'forwardfill';
                push @general_errors, {'type'=>'warning','message'=>"Found existing taxon, prefilled authority data. Submit again if it's correct"};
                $fill_author = {'author'=>formatAuthor($t),'pubyr'=>$t->{'pubyr'}};
            }
        }
#        if (!$found_common_ancestor) {
#            -- print info and make sure they're the same
#        }
    } else {
    #    push @general_errors, {'type'=>'warning','message'=>"'$row->{child_spelling_name}' not found in the database, but a record will be created for it when you hit submit.  Please enter author and pub. yr. now if available"};
    }

    return ($taxon_no,\@general_errors,$homonym_error,$fill_author);
}

sub isSameAuthority {
    my ($t,$author,$pubyr) = @_;

    # user provided authority data
    my ($a1init,$a1last,$a2init,$a2last,$otherauthors) = parseAuthorName($author);

    if ($a1last && $a1last eq $t->{author1last} &&
        $a2last eq $t->{author2last} &&
        $pubyr eq $t->{pubyr}) {
        return 1;
    } else {
        return 0;
    }
}

sub hasCommonAncestor {
    my ($dbt,$taxon_no, $parent_no) = @_;
#    print "hasCommonAncestor($taxon_no,$parent_no) = ";

    my %all_p = ();
    my $sql = "(SELECT parent_no,parent_spelling_no FROM opinions WHERE child_no=$taxon_no)"
            . " UNION "
            . "(SELECT parent_no,parent_spelling_no FROM opinions WHERE child_spelling_no=$taxon_no)";

    my $results = $dbt->getData($sql);
    foreach my $row (@$results) {
        $all_p{$row->{parent_no}} = 1;
        $all_p{$row->{parent_spelling_no}} = 1;
    }
    my $p_hash = TaxaCache::getParents($dbt,[$taxon_no]);
    my @parents = $p_hash->{$taxon_no};
    foreach my $p (@parents) {
        $all_p{$p} = 1;
    }

    if ($all_p{$parent_no}) {
#        print "1<br>";
        return 1;
    } else {
#        print "0<br>";
        return 0;
    }
}

#
sub makeTaxonChoices {
    my $dbt = shift;
    my @taxa= @{$_[0]};
    my @choice_keys = ();
    my @choice_values = ();
    foreach my $t (@taxa) {
        my $taxon_description = describeTaxon($dbt,$t);
        push @choice_keys,$taxon_description;
        push @choice_values,$t->{taxon_no};
    }
    push @choice_keys, "None of these, create a new record";
    push @choice_values, "-1";
    return \@choice_keys,\@choice_values;
}

sub parseParentName {
    my ($dbt,$name,$rank,$no) = @_;
    my $dbh = $dbt->dbh;

    my @nos = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$name},['taxon_no','taxon_name','taxon_rank','author1init','author1last','author2init','author2last','otherauthors','ref_is_authority','pubyr']);
    my $taxon_no = 0;
    my @general_errors = ();
    my $homonym_error = undef;
    if (@nos == 0) {
        my @matches = TypoChecker::typoCheck($dbt,'authorities','taxon_name','taxon_no,taxon_name,taxon_rank','AND taxon_rank LIKE '.$dbh->quote($rank),$name);
        if (@matches) {
            my $typo_list = join(", ",map {"<a target=\"_NEW\" href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$_->{taxon_no}\">$_->{taxon_name}</a>"} @matches);
            my $error = {
                'type'=>'warning',
                'message'=>"'$name' doesn't exist in the database, but the following close matches were found: $typo_list. If the name entered is correct, please submit again to create a new record for it"
            };
            push @general_errors, $error;
        } else {
            my $error = {
                'type'=>'warning',
                'message'=>"'$name' not found in the database, please submit again to create a new record for it"
            };
            push @general_errors, $error;
        }
        $taxon_no = -1;
    } elsif (@nos == 1) {
        $taxon_no = $nos[0]->{'taxon_no'};
        if ($nos[0]->{taxon_rank} ne $rank) {
            my $error = {
                'type'=>'error',
                'message'=>"'$name' was found, but with rank $nos[0]->{taxon_rank}"
            };
            push @general_errors, $error;
        }
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
            'choice_id'=>'parent_spelling_no',
            'selected'=>$no
        };
        push @general_errors, {'type'=>'error','message'=>"'$name' is ambiguous, please select the one you want"};
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
