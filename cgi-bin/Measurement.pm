package Measurement;
use Data::Dumper;
use CGI::Carp;
use TaxaCache;
use Debug qw(dbg);

# written by PS 6/22/2005 - 6/24/2005
# Handle display and processing of form to enter measurements for specimens

#my @specimen_fields= ('specimens_measured', 'specimen_coverage','specimen_id','specimen_side','specimen_part','measurement_source','length_median','length_min','length_max','length_error','length_error_unit','width_median','width_min','width_max','width_error','width_error_unit','height_median','height_min','height_max','height_error','height_error_unit','diagonal_median','diagonal_min','diagonal_max','diagonal_error','diagonal_error_unit','inflation_median','inflation_min','inflation_max','inflation_error','inflation_error_unit','comments','length','width','height','diagonal','inflation');

my @specimen_fields   =('specimens_measured', 'specimen_coverage','specimen_id','specimen_side','specimen_part','measurement_source','magnification','comments');
my @measurement_types =('length','width','height','diagonal','inflation');
my @measurement_fields=('average','median','min','max','error','error_unit');

#
# Displays a list of potential specimens from the search.  
#
sub submitSpecimenSearch {
    my  ($dbt,$hbo,$q,$s,$exec_url) = @_;
    if ($s->isDBMember()) {
        $dbt->useRemote(1);
    }
    my $dbh = $dbt->dbh;

    if (!$q->param('taxon_name') && !int($q->param('collection_no'))) {
        push my @error , "You must enter a taxonomic name or a collection";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    }

    # Grab the data from the database, filtering by either taxon_name and/or collection_no
    my $sql1 = "SELECT c.collection_no, c.collection_name,o.occurrence_no,o.genus_name,o.species_name, count(DISTINCT specimen_no) cnt FROM (occurrences o, collections c) LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no WHERE o.collection_no=c.collection_no ";
    my $sql2 = "SELECT c.collection_no, c.collection_name,o.occurrence_no,o.genus_name,o.species_name, count(DISTINCT specimen_no) cnt FROM (reidentifications re, occurrences o, collections c) LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no WHERE re.occurrence_no=o.occurrence_no AND o.collection_no=c.collection_no ";
    my $where = "";
    my @taxa;
    if ($q->param('taxon_name')) {
        @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$q->param('taxon_name'),'match_subgenera'=>1});
    } 
    my $taxon_nos;        
    if (@taxa) {
        my (@taxon_nos,%all_taxa);
        foreach (@taxa) {
            if ($_->{'taxon_rank'} =~ /species|genus/) {
                @taxon_nos = TaxaCache::getChildren($dbt,$_->{'taxon_no'});
                @all_taxa{@taxon_nos} = ();
            } else {
                @taxon_nos = TaxonInfo::getAllSynonyms($dbt,$_->{'taxon_no'});
                @all_taxa{@taxon_nos} = ();
                @all_taxa{$_->{'taxon_no'}} = 1; 
            }
        }
        @taxon_nos = keys %all_taxa;
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
        displaySpecimenList($dbt,$hbo,$q,$s,$exec_url);
    } elsif (scalar(@results) == 0 && scalar(@results_taxa_only) == 0) {
        push my @error , "Occurrences or taxa matching these search terms could not be found";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
    } else {
        print "<form method=\"POST\" action=\"bridge.pl\">";
        print "<input type=\"hidden\" name=\"action\" value=\"displaySpecimenList\">";
        print "<div align=\"center\">";
        print "<table cellspacing=\"0\" cellpadding=\"6\">";
        print "<tr><th><span style=\"margin-right: 1em\">Taxon</span></th><th>Collection</th><th>Measurements</th></tr>";
#        print "<tr><td>Collection name</td><td></td></tr>";
        my $last_collection_no = -1;
        my %coll_names = ();
        $coll_names{$_->{'collection_name'}} = 1 for (@results);
        my $coll_count = scalar(keys(%coll_names));
        my $class = (scalar($coll_count) > 1) ? '' : 'class="darkList"';
        foreach my $row (@results) {
            my $specimens = ($row->{'cnt'} >= 1) ? $row->{'cnt'} : 'none';
            my $taxon_name;
            my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
            if ($reid_row) {
                $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
            } else {
                $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
            }
            if ($last_collection_no != $row->{'collection_no'}) {
                $class = ($class eq '') ? $class='class="darkList"' : '';
            }
            #print "<td><input type=\"radio\" name=\"occurrence_no\" value=\"$row->{occurrence_no}\"> $row->{genus_name} $row->{species_name}</td><td>$measurements</td></tr>";
            print "<tr $class><td><a href=\"bridge.pl?action=displaySpecimenList&occurrence_no=$row->{occurrence_no}\">$taxon_name</a></td>";
            if ($last_collection_no != $row->{'collection_no'}) {
                $last_collection_no = $row->{'collection_no'};
                print "<td><span style=\"margin-right: 1em;\"><a href=\"bridge.pl?action=displayCollectionDetails&collection_no=$row->{collection_no}\">$row->{collection_name}</a></span></td>";
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
            print "<tr $class><td><a href=\"bridge.pl?action=displaySpecimenList&taxon_no=$row->{taxon_no}\">$taxon_name</a></td><td>unknown collection</td><td align=\"center\">$specimens</td></tr>";

        }
        print "</table>";
        print "</div>";
        print "</form>";
        print "<br>";
    }

}

#
# Displays a list of specimens associated with a specific occurrence.  If there are no specimens
# currently associated with the occurrence, just go directly to the add occurrence form, otherwise
# give them an option to edit an old one, or add a new set of measurements
#
sub displaySpecimenList {
    my ($dbt,$hbo,$q,$s,$exec_url) = @_;
    if ($s->isDBMember()) {
        $dbt->useRemote(1);
    }

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
    
    print "<div align=\"center\">";
    print "<h4>Specimen list for $taxon_name $collection</h4>\n";
    print "<form method=\"POST\" action=\"bridge.pl\">\n";
    print "<input type=hidden name=\"action\" value=\"populateMeasurementForm\">\n";
    if ($q->param('types_only')) {
        print "<input type=hidden name=\"types_only\" value=\"".$q->param('types_only')."\">";
    }
    if ($q->param('occurrence_no')) {
        print "<input type=hidden name=\"occurrence_no\" value=\"".$q->param('occurrence_no')."\">";
    } else {
        print "<input type=hidden name=\"taxon_no\" value=\"".$q->param('taxon_no')."\">";
    }

    # now create a table of choices
    print "<table>\n";
#        my $checked = (scalar(@results) == 1) ? "CHECKED" : "";

    %specimens = ();
    %types = ();
    %parts = ();
    foreach my $row (@results) {
        $specimens{$row->{specimen_no}}{$row->{measurement_type}} = $row->{real_average};
        $specimens{$row->{specimen_no}}{'specimens_measured'} = $row->{specimens_measured};
        $specimens{$row->{specimen_no}}{'specimen_part'} = $row->{specimen_part};
        $specimens{$row->{specimen_no}}{'specimen_id'} = $row->{specimen_id};
        $types{$row->{measurement_type}}++;
        $parts{$row->{specimen_part}}++ if ($row->{specimen_part});
    }

    $specimen_count = scalar(keys(%specimens));

    if ($specimen_count > 0) {
        print "<tr><th></th><th>specimen #</th>";
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
    foreach $specimen_no (sort {$a <=> $b} keys %specimens) {
        my $row = $specimens{$specimen_no};
        # Check the button if this is the first match, which forces
        #  users who want to create new measurement to check another button
        print qq|<tr><td><input type="radio" name="specimen_no" value="$specimen_no" $checked></td>|;
        print qq|<td>$row->{specimen_id}</td>|;
        print qq|<td align=\"center\">$row->{specimen_part}</td>| if (%parts);
        print qq|<td align=\"center\">$row->{specimens_measured}</td>|;
        foreach my $type (@measurement_types) {
            if ($types{$type}) {
                print "<td align=\"center\">$row->{$type}</td>";
            }
        }
        print "</tr>";
    }

    # always give them an option to create a new measurement as well
    print "<tr><td><input type=\"radio\" name=\"specimen_no\" value=\"-1\"></td>";
    print "<td colspan=\"6\">Add a <b>new</b> average measurement</i></td></tr>\n";
    print "<tr><td valign=\"top\"><input type=\"radio\" name=\"specimen_no\" value=\"-2\"></td>";
    print "<td colspan=\"6\" valign=\"top\">Add <input type=\"text\" name=\"specimens_measured\" value=\"10\" size=3><b>new</b> individual measurements</i><br>";
    print qq|
  &nbsp;&nbsp;default side:
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
  &nbsp;&nbsp;default part: <input name="default_part" size="10"><br>
  &nbsp;&nbsp;default source:
  <select name="default_source">
  <option value=""></option>
  <option value="text">text</option>
  <option value="table">table</option>
  <option value="picture">picture</option>
  <option value="graph">graph</option>
  <option value="direct measurement">direct measurement</option>
  </select>\n|;
    print "</td></tr>\n";

    print "<tr><td align=\"center\" colspan=7><br><input type=\"Submit\" name=\"Submit\" value=\"Submit\"></td></tr>";
    print "</table></div>";
    #}
}

sub populateMeasurementForm {
    my ($dbt,$hbo,$q,$s,$exec_url) = @_;
    $dbt->useRemote(1);
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
    if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
        push my @error , "There is no matching taxon or occurrence";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
        carp "populateMeasurementForm called with no occurrence_no/taxon_no by ".$s->get('enterer_no');
        exit;
    }   
    
	# get the taxon's name
    my ($taxon_name,$collection);
    if ($q->param('occurrence_no')) {
        my $sql = "SELECT o.collection_no, o.genus_name, o.species_name, o.occurrence_no FROM occurrences o WHERE o.occurrence_no=".int($q->param('occurrence_no'));
        my $row = ${$dbt->getData($sql)}[0];

        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }  

        $collection = "(collection $row->{'collection_no'})";

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
                # Specimen count given a default value of 1 below
                push @fields,$_ for (grep(!/specimens_measured/,@specimen_fields));
                push @values, '' for @fields;
                foreach my $type (@measurement_types) {
                    foreach my $f (@measurement_fields) {
                        push @fields, $type."_".$f;
                        push @values, '';
                    }
                }
	            push (@fields,'occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','specimens_measured','specimen_is_type','types_only');
	            push (@values,int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,1,'',int($q->param('types_only')));
	            print $hbo->populateHTML('specimen_measurement_form_general', \@values, \@fields);
            } elsif ($q->param('specimen_no') == -2) {
	            push (@fields,'occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection','specimen_coverage');
	            push (@values,int($q->param('occurrence_no')),int($q->param('taxon_no')),$s->get('reference_no'),'-1',$taxon_name,$collection,'');
                #@table_rows = ('specimen_id','length','width','height','diagonal','specimen_side','specimen_part','measurement_source','magnification','is_type');
                my $table_rows = "";
                for (1..$q->param('specimens_measured')) {
                    $table_rows .= $hbo->populateHTML('specimen_measurement_form_row',[$q->param('default_side'),$q->param('default_part'),$q->param('default_source')],['specimen_side','specimen_part','measurement_source']);
                }
                push @fields,'table_rows';
                push @values,$table_rows;
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

        # some additional fields not from the form row
	    push (@fields, 'occurrence_no','taxon_no','reference_no','specimen_no','taxon_name','collection_no','types_only');
	    push (@values, int($q->param('occurrence_no')),int($q->param('taxon_no')),$row->{'reference_no'},$row->{'specimen_no'},$taxon_name,$collection_no,int($q->param('types_only')));
	    print $hbo->populateHTML('specimen_measurement_form_general', \@values, \@fields);
    }

}

sub processMeasurementForm	{
    my ($dbt,$hbo,$q,$s,$exec_url) = @_;
    $dbt->useRemote(1);
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
    if ( ! $q->param('occurrence_no') && ! $q->param('taxon_no')) {
        push my @error , "There is no matching taxon or occurrence";
        print "<center><p>".Debug::printWarnings(\@error)."</p></center>\n";
        carp "populateMeasurementForm called with no occurrence_no/taxon_no by ".$s->get('enterer_no');
        exit;
    }

    # get the taxon's name
    my ($taxon_name,$collection);
    if ($q->param('occurrence_no')) {
        my $sql = "SELECT o.collection_no, o.genus_name, o.species_name, o.occurrence_no FROM occurrences o WHERE o.occurrence_no=".int($q->param('occurrence_no'));
        my $row = ${$dbt->getData($sql)}[0];

        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }

        $collection = "(collection $row->{'collection_no'})";

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
        if (! ($fields{'length_average'} || $fields{'width_average'} || $fields{'height_average'} || $fields{'diagonal_average'}) ) {
            next;
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
        if ( $fields{'specimen_no'} > 0 )	{
            delete $fields{'taxon_no'}; # keys, never update thse
            delete $fields{'occurrence_no'}; # keys, never update thse
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
                        $in_cgi{$type}{'error_unit'} = $fields{$type."_error_unit"};
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
                        $in_cgi{$type}{'error_unit'}=$fields{$type."_error_unit"};
                        $dbt->insertRecord($s,'measurements',$in_cgi{$type});
    #                    $in_cgi{$type}{'measurement_type'}=$type;
    #                    $in_cgi{$type}{'error_unit'}=$q->param($type."_error_unit");
    #                    $in_cgi{$type}{'specimen_no'}= $q->param('specimen_no');
    #                    $dbt->insertRecord($s,'measurements',$in_cgi{$type});
                    }
                }

                print "<center><h4>The measurement data for $taxon_name $collection have been updated</h4></center>\n";
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
        print "<center><h4>Measurements for $taxon_name $collection has been added</h4></center>\n";
    }

    if ($q->param('occurrence_no')) {
	my ($temp,$collection_no) = split / /,$collection;
	$collection_no =~ s/\)//;
	print "<div align=\"center\"><table><tr><td><ul>\n".
		"<br><li><b><a href=\"$exec_url?action=populateMeasurementForm&specimen_no=-1&occurrence_no=".$q->param('occurrence_no')."\">Add another average or individual measurement of this occurrence</a></b></li>\n".
		"<br><li><b><a href=\"$exec_url?action=populateMeasurementForm&specimen_no=-2&specimens_measured=10&occurrence_no=".$q->param('occurrence_no')."\">Add up to 10 new individual measurements of this occurrence</a></b></li>\n".
		"<br><li><b><a href=\"$exec_url?action=displaySpecimenList&occurrence_no=".$q->param('occurrence_no')."\">Edit another measurement of this occurrence</a></b></li>\n".
    		"<br><li><b><a href=\"$exec_url?action=submitSpecimenSearch&collection_no=$collection_no\">Add a measurement of another occurrence in this collection</a></b></li>\n";
    } else {
	    print "<div align=\"center\"><table><tr><td><ul>\n".
		"<br><li><b><a href=\"$exec_url?action=populateMeasurementForm&specimen_no=-1&taxon_no=".$q->param('taxon_no')."\">Add another average or individual measurement of $taxon_name</a></b></li>\n".
		"<br><li><b><a href=\"$exec_url?action=populateMeasurementForm&specimen_no=-2&specimens_measured=10&taxon_no=".$q->param('taxon_no')."\">Add up to 10 new individual measurements of $taxon_name</a></b></li>\n".
		"<br><li><b><a href=\"$exec_url?action=displaySpecimenList&taxon_no=".$q->param('taxon_no')."&types_only=".$q->param('types_only')."\">Edit another measurement of $taxon_name</a></b></li>\n";
    }
    print "<br><li><b><a href=\"$exec_url?action=submitSpecimenSearch&taxon_name=$taxon_name\">Add a measurement of $taxon_name in another collection</a></b></li>\n".
	"<br><li><b><a href=\"$exec_url?action=checkTaxonInfo&taxon_name=$taxon_name\">Get general info about this taxon</a></b></li>\n".
	"<br><li><b><a href=\"$exec_url?action=displaySpecimenSearchForm\">Add a measurement of another taxon</a></b></li>\n".
	"</ul></td></tr></table></div>\n";
}


sub syncWithAuthorities {
    my ($dbt,$s,$hbo,$specimen_no) = @_;
    $dbt->useRemote(1);
    my $sql = "SELECT taxon_no,magnification,specimen_part,specimen_id FROM specimens WHERE specimen_no=$specimen_no";
    my $row = ${$dbt->getData($sql)}[0];
    if ($row && $row->{'taxon_no'} =~ /\d/ && $row->{'magnification'} <= 1) {
        my $taxon_no = $row->{'taxon_no'};
        my $sql = "(SELECT specimen_no FROM specimens s WHERE s.is_type='holotype' AND s.taxon_no=$taxon_no) UNION (SELECT specimen_no FROM specimens s, occurrences o left join reidentifications re on o.occurrence_no=re.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.is_type='holotype' AND o.taxon_no=$taxon_no AND re.reid_no IS NULL) UNION (SELECT specimen_no FROM specimens s, occurrences o, reidentifications re WHERE o.occurrence_no=re.occurrence_no AND s.occurrence_no=o.occurrence_no AND s.is_type='holotype' AND re.taxon_no=$taxon_no AND re.most_recent='YES')";
        my $cnt = scalar(@{$dbt->getData($sql)});
        if ($cnt <= 1) {
            my %generic_parts = ();
            foreach my $p ($hbo->getList('type_body_part')) {
                $generic_parts{$p} = 1;
            }
            if ($row->{'specimen_part'}) {
                if ($generic_parts{$row->{'specimen_part'}}) {
                    $fields{'type_body_part'} = $row->{'specimen_part'};
                } else {
                    $fields{'part_details'} = $row->{'specimen_part'};
                }
            }
            $fields{'type_specimen'} = $row->{'specimen_id'};
            $dbt->updateRecord($s,'authorities','taxon_no',$taxon_no,\%fields);
        }
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
    if ($options{'taxon_list'}) {
        my $taxon_nos = join(",",@{$options{'taxon_list'}});
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
        $sql3 .= " AND a.taxon_no IN ($taxon_nos)";
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
    } elsif ($options{'collection_no'}) {
        $sql1 .= " AND o.collection_no=".int($options{'collection_no'});
        $sql2 .= " AND o.collection_no=".int($options{'collection_no'});
    } elsif ($options{'occurrence_list'}) {
        $sql1 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
        $sql2 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
    } elsif ($options{'occurrence_no'}) {
        $sql1 .= " AND o.occurrence_no =".int($options{'occurrence_no'});
        $sql2 .= " AND o.occurrence_no =".int($options{'occurrence_no'});
    }

    if ($options{'get_global_specimens'} && $sql3 =~ /taxon_no IN/) {
        $sql = "($sql1) UNION ($sql2) UNION ($sql3)";
    } else {
        $sql = "($sql1) UNION ($sql2)";
    } #else {
      #  $sql = $sql1;
    #}
    dbg("SQL is $sql");

    my @results = @{$dbt->getData($sql)};
    return @results;
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
                if (!exists $part_type->{'min'} || $row->{'real_average'} < $part_type->{'min'}) {
                    $part_type->{'min'} = $row->{'real_average'};
                }
                if (!exists $part_type->{'max'} || $row->{'real_average'} > $part_type->{'max'}) {
                    $part_type->{'max'} = $row->{'real_average'};
                }
            }
        } else {
            if (!exists $part_type->{'min'} || $row->{'real_min'} < $part_type->{'min'}) {
                $part_type->{'min'} = $row->{'real_min'};
            }
            if (!exists $part_type->{'max'} || $row->{'real_max'} > $part_type->{'max'}) {
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
            }
        }
   
        my @values = ();
        my $can_compute = 0; # Can compute median, and error (std dev)
        my $is_group = 0; # Is it aggregate group data or a bunch of singles?
        if ($unique_specimen_nos{$part} == 1) {
            $can_compute = 1;
            if ($m_table->{'specimens_measured'} > 1) {
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

1;
