package Measurement;
use Data::Dumper;
use CGI::Carp;

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
    my $dbh = $dbt->dbh;

    if (!$q->param('taxon_name') && !int($q->param('collection_no'))) {
        print "<div align=\"center\"><h3>You must specify a taxonomic name or a collection number. Please go back and try again.<h3></div>";
    }

    # Grab the data from the database, filtering by either taxon_name and/or collection_no
    my $sql1 = "SELECT c.collection_no, o.occurrence_no, o.genus_name,o.species_name, count(DISTINCT specimen_no) cnt FROM occurrences o, collections c LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no WHERE o.collection_no=c.collection_no ";
    my $sql2 = "SELECT c.collection_no, o.occurrence_no, o.genus_name,o.species_name, count(DISTINCT specimen_no) cnt FROM reidentifications r, occurrences o, collections c LEFT JOIN specimens s ON o.occurrence_no=s.occurrence_no WHERE r.occurrence_no=o.occurrence_no AND o.collection_no=c.collection_no ";
    my $where = "";
    my @taxa;
    if ($q->param('taxon_name')) {
        @taxa = TaxonInfo::getTaxon($dbt,'taxon_name'=>$q->param('taxon_name'));
    } 
    if (@taxa) {
        my (@taxon_nos,%all_taxa);
        foreach (@taxa) {
            @taxon_nos = PBDBUtil::taxonomic_search($dbt,$_->{'taxon_no'});
            @all_taxa{@taxon_nos} = ();
        }
        @taxon_nos = keys %all_taxa;
        my $taxon_nos = join(",",@taxon_nos);
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND r.taxon_no IN ($taxon_nos)";
    } elsif ($q->param('taxon_name')) {
        my @taxon_bits = split(/\s+/,$q->param('taxon_name'));
        $sql1 .= " AND o.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
        $sql2 .= " AND r.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
        if (scalar(@taxon_bits) > 1) {
            $sql1 .= "AND o.species_name LIKE ".$dbh->quote($taxon_bits[1]);
            $sql1 .= "AND r.species_name LIKE ".$dbh->quote($taxon_bits[1]);
        }
    }
    if ($q->param('collection_no')) {
        $sql1 .= " AND o.collection_no=".int($q->param('collection_no'));
        $sql2 .= " AND o.collection_no=".int($q->param('collection_no'));
    }
    $sql1 .= " GROUP BY o.occurrence_no";
    $sql2 .= " GROUP BY o.occurrence_no";
    $sql .= "($sql1) UNION ($sql2) ORDER BY collection_no";

    main::dbg("SQL is $sql");       

    my @results = @{$dbt->getData($sql)};

    if (scalar(@results) == 0) {
        print "<div align=\"center\"><h3>Could not find any occurrences matching the criteria entered<h3></div>";
    } elsif (scalar(@results) == 1) {
        $q->param('occurrence_no'=>$results[0]->{'occurrence_no'});
        displaySpecimenList($dbt,$hbo,$q,$s,$exec_url);
    } else {
        print "<form method=\"POST\" action=\"bridge.pl\">";
        print "<input type=\"hidden\" name=\"action\" value=\"displaySpecimenList\">";
        print "<input type=\"hidden\" name=\"use_reference\" value=\"".$q->param('use_reference')."\">";
        print "<div align=\"center\">";
        print "<table cellspacing=0 cellpadding=3>";
        print "<tr><th><span style=\"margin-right: 1em\">Collection name</span></th><th>Taxon name</th><th>Measurements</th></tr>";
#        print "<tr><td>Collection name</td><td></td></tr>";
        my $last_collection_no = -1;
        my %coll_names = ();
        $coll_names{$_->{'collection_name'}} = 1 for (@results);
        my $coll_count = scalar(keys(%coll_names));
        my $class = (scalar($coll_count) > 1) ? '' : 'class="darkList"';
        foreach my $row (@results) {
            if ($last_collection_no != $row->{'collection_no'}) {
                $class = ($class eq '') ? $class='class="darkList"' : '';
                $last_collection_no = $row->{'collection_no'};
                print "<tr $class><td><span style=\"margin-right: 1em;\"><a href=\"bridge.pl?action=displayCollectionDetails&collection_no=$row->{collection_no}\">$row->{collection_no} $row->{collection_name}</a></span></td>";
            } else {
                print "<tr $class><td></td>";
            }
            my $specimens = ($row->{cnt} >= 1) ? $row->{'cnt'} : 'none';
            my $taxon_name;
            my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
            if ($reid_row) {
                $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
            } else {
                $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
            }
            #print "<td><input type=\"radio\" name=\"occurrence_no\" value=\"$row->{occurrence_no}\"> $row->{genus_name} $row->{species_name}</td><td>$measurements</td></tr>";
            print "<td><a href=\"bridge.pl?action=displaySpecimenList&use_reference=".$q->param('use_reference')."&occurrence_no=$row->{occurrence_no}\">$taxon_name</a></td><td>$specimens</td></tr>";
        }
        print "</table>";
        print "</div>";
        print "</form>";
    }

}

#
# Displays a list of specimens associated with a specific occurrence.  If there are no specimens
# currently associated with the occurrence, just go directly to the add occurrence form, otherwise
# give them an option to edit an old one, or add a new set of measurements
#
sub displaySpecimenList {
    my ($dbt,$hbo,$q,$s,$exec_url) = @_;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('occurrence_no')) {
		print "<center><h3>Sorry, occurrence number is unknown</h3></center>\n";
        carp "populateMeasurementForm called with no occurrence_no by ".$s->get('enterer_no');
		exit;
	}

    my $sql = "SELECT * FROM specimens WHERE occurrence_no=".int($q->param('occurrence_no'));
    my @results = @{$dbt->getData($sql)};
    if (scalar(@results) == 0) {
        populateMeasurementForm($dbt->dbh,$dbt,$hbo,$q,$s,$exec_url);
    } else {
        $sql = "SELECT collection_no,genus_name,species_name,occurrence_no FROM occurrences WHERE occurrence_no=".int($q->param("occurrence_no"));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            carp "Error is displaySpecimenList, could not find ".$q->param("occurrence_no")." in the database";
            print "An error has occurred";
            return;
        }
        my $taxon_name;
        my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
        if ($reid_row) {
            $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
        } else {
            $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
        }   
        
        print "<div align=\"center\">";
        print "<h3>Specimen list for $taxon_name (collection no $row->{collection_no}):</h3>\n";
        print "<form method=\"POST\" action=\"bridge.pl\">\n";
        print "<input type=hidden name=\"action\" value=\"populateMeasurementForm\">\n";
        print "<input type=hidden name=\"occurrence_no\" value=\"".$q->param('occurrence_no')."\">";
        print "<input type=\"hidden\" name=\"use_reference\" value=\"".$q->param('use_reference')."\">";

        # now create a table of choices
        print "<table>\n";
        my $checked = (scalar(@results) == 1) ? "CHECKED" : "";
        foreach my $row (@results) {
            # Check the button if this is the first match, which forces
            #  users who want to create new taxa to check another button
            print qq|<tr><td><input type="radio" name="specimen_no" value="$row->{specimen_no}" $checked>|;
            print formatMeasurement($dbt,$row)."</td>";
        }

        # always give them an option to create a new taxon as well
        print "<tr><td><input type=\"radio\" name=\"specimen_no\" value=\"-1\">";
        if ( scalar(@results) == 1 )    {
            print "No, not the one above ";
        } else  {
            print "None of the above ";
        }
        print "- add a <b>new</b> measurement</i></td></tr>\n";
        print "<tr><td align=\"center\"><br><input type=\"Submit\" name=\"Submit\" value=\"Submit\"></td></tr>";
        print "</table></div>";
    }
}

sub populateMeasurementForm {
    my ($dbh,$dbt,$hbo,$q,$s,$exec_url) = @_;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('occurrence_no')) {
		print "<center><h3>An error has occurred.  Occurrence is not given.</h3></center>\n";
        carp "populateMeasurementForm called with no occurrence_no by ".$s->get('enterer_no');
		exit;
	}

	# get the taxon's name
	my $sql = "SELECT o.collection_no, o.genus_name, o.species_name, o.occurrence_no FROM occurrences o WHERE o.occurrence_no=".int($q->param('occurrence_no'));
    my $row = ${$dbt->getData($sql)}[0];

    my $taxon_name;
    my $reid_row = PBDBUtil::getMostRecentReIDforOcc($dbt,$row->{'occurrence_no'},1);
    if ($reid_row) {
        $taxon_name = $reid_row->{'genus_name'}." ".$reid_row->{'species_name'};
    } else {
        $taxon_name = $row->{'genus_name'}." ".$row->{'species_name'};
    }  

    my $collection_no = $row->{'collection_no'};

    if (!$row || !$taxon_name || !$collection_no) {
		print "<center><h3>An error has occurred. Could not find occurrence in database.</h3></center>\n";
        carp("processMeasurementForm: no row found for occurrence_no ".$q->param('occurrence_no'));
        return;
    }

    #Prepare fields to be use in the form ahead
    my @values = ();
    my @fields = ();
    
	# query the specimen table for the old data
	$sql = "SELECT * FROM specimens WHERE specimen_no=".int($q->param('specimen_no'));
	$row = ${$dbt->getData($sql)}[0];

    if (!$row) {
        # This is a new entry
        if ($q->param('use_reference') eq 'current' && $s->get('reference_no')) {
            $q->param('skip_ref_check'=>1);
        }
        if (!$q->param('skip_ref_check') || !$s->get('reference_no')) {
                # Make them choose a reference first
                my $toQueue = "action=populateMeasurementForm&skip_ref_check=1&occurrence_no=".$q->param('occurrence_no');
                $s->enqueue( $dbh, $toQueue );
                $q->param( "type" => "select" );
                main::displaySearchRefs("Please choose a reference before adding specimen measurement data",1);
                return;
        } else {
            # Specimen count given a default value of 1 below
            push @fields,$_ for (grep(!/specimens_measured/,@specimen_fields));
            push @values, '' for @fields;
            foreach my $type (@measurement_types) {
                foreach my $f (@measurement_fields) {
                    push @fields, $type."_".$f;
                    push @values, '';
                }
            }
	        push (@fields,'occurrence_no','reference_no','specimen_no','taxon_name','collection_no','specimens_measured');
	        push (@values,int($q->param('occurrence_no')),$s->get('reference_no'),'-1',$taxon_name,$collection_no,1);
        }
    } else {
        # This is an edit, use fields from the DB

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
        # some additional fields not from the form row
	    push (@fields, 'occurrence_no','reference_no','specimen_no','taxon_name','collection_no');
	    push (@values, int($q->param('occurrence_no')),$row->{'reference_no'},$row->{'specimen_no'},$taxon_name,$collection_no);
    }

	print $hbo->populateHTML('specimen_measurement_form', \@values, \@fields);
}

sub processMeasurementForm	{
    my ($dbh,$dbt,$hbo,$q,$s,$exec_url) = @_;

	# can't proceed without a taxon no
	if (!$q->param('occurrence_no'))	{
		print "<center><h3>Sorry, the specimen measurement table can't be updated because the occurrence is unknown</h3></center>\n";
        carp "populateMeasurementForm called with no occurrence_no by ".$s->get('enterer_no');
		return;
	}
	# get the taxon's name
	my $sql = "SELECT o.collection_no, o.genus_name, o.species_name FROM occurrences o WHERE occurrence_no=".int($q->param('occurrence_no'));
    my $row = ${$dbt->getData($sql)}[0];

    my $taxon_name = "$row->{genus_name} $row->{species_name}";
    my $collection_no = $row->{'collection_no'};

    if (!$row || !$taxon_name || !$collection_no) {
		print "<center><h3>Sorry, the specimen measurement table can't be updated because the occurrence is unknown</h3></center>\n";
        carp("processMeasurementForm: no row found for occurrence_no ".$q->param('occurrence_no'));
        return;
    }
    
	# if ecotaph no exists, update the record
    my %fields = $q->Vars();

	if ( $q->param('specimen_no') > 0 )	{
        $result = $dbt->updateRecord($s,'specimens','specimen_no',$q->param('specimen_no'),\%fields);

        if ($result) {
            $sql = "SELECT * FROM measurements WHERE specimen_no=".int($q->param('specimen_no'));
            my @measurements = @{$dbt->getData($sql)};

            my %in_db= (); # Find rows from DB
            $in_db{$_->{'measurement_type'}} = $_ for @measurements;

            my %in_cgi = (); # Find rows from filled out form
            foreach my $type (@measurement_types) {
                foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                    if ($q->param($type."_".$f)) {
                        $in_cgi{$type}{$f} = $q->param($type."_".$f);
                    } 
                }
            }

            foreach my $type (@measurement_types) {
                if ($in_db{$type} && $in_cgi{$type}) {
                    # If the record exists both the form and db, its an update
                    while(my($type,$row)=each %in_cgi) {
                        main::dbg("UPDATE, TYPE $type: ".Dumper($row));
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            next if (!$row->{$f});
                            if ($q->param('magnification') =~ /^[0-9.]+$/) {
                                $row->{'real_'.$f}=$row->{$f}/$q->param('magnification');
                            } else {
                                $row->{'real_'.$f}=$row->{$f};
                            }
                        }
                        $in_cgi{$type}{'error_unit'} = $q->param($type."_error_unit");
#                        $row->{'error_unit'} = $q->param($type."_error_unit");
                        #$dbt->insertRecord($s,'measurements',$row);
                        $dbt->updateRecord($s,'measurements','measurement_no',$in_db{$type}{'measurement_no'},$in_cgi{$type});
                    }
                } elsif ($in_db{$type}) {
                    # Else if it exists only in the database now, delete it 
                    $sql = "DELETE FROM measurements WHERE measurement_no=".$in_db{$type}{'measurement_no'} . " LIMIT 1";
                    main::dbg("DELETING type $type: $sql");
                    $dbt->getData($sql);
                } elsif ($in_cgi{$type}) {
                    # Else if its in the form and NOT in the DB, add it
                    while(my($type,$row)=each %in_cgi) {
                        main::dbg("INSERT, TYPE $type: ".Dumper($row));
                        foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                            next if (!$row->{$f});
                            if ($q->param('magnification') =~ /^[0-9.]+$/) {
                                $row->{'real_'.$f}=$row->{$f}/$q->param('magnification');
                            } else {
                                $row->{'real_'.$f}=$row->{$f};
                            }
                        }
#                        $in_cgi{$type}{'error_unit'}=$q->param($type."_error_unit");
                        $row->{'measurement_type'}=$type;
                        $row->{'specimen_no'}= $q->param('specimen_no');
                        $row->{'error_unit'}=$q->param($type."_error_unit");
                        $dbt->insertRecord($s,'measurements',$row);
                    }
#                    $in_cgi{$type}{'measurement_type'}=$type;
#                    $in_cgi{$type}{'error_unit'}=$q->param($type."_error_unit");
#                    $in_cgi{$type}{'specimen_no'}= $q->param('specimen_no');
#                    $dbt->insertRecord($s,'measurements',$in_cgi{$type});
                }
            }

            print "<center><h3>Specimen measurement data for $taxon_name (collection no $collection_no) has been updated</h3></center>\n";
        } else {
            print "Error updating database table row, please contact support";
            carp "Error updating row in Measurement.pm: ".$result;
        }
	} else {
        # Set the reference_no
        $fields{'reference_no'} = $s->get('reference_no');
        $fields{'taxon_no'} = undef;
        my ($result,$specimen_no) = $dbt->insertRecord($s,'specimens',\%fields);

        if ($result) {
            # Get the measurement data. types can be "length","width",etc. fields can be "average","max","min",etc.
            my %m_table = (); # Measurement table, only used right below
            foreach my $type (@measurement_types) {
                foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                    if ($q->param($type."_".$f)) {
                        $m_table{$type}{$f} = $q->param($type."_".$f);
                    } 
                }
            }

            # Now insert a row into the measurements table for each type of measurement
            while(my($type,$row)=each %m_table) {
                foreach my $f (grep(!/error_unit/,@measurement_fields)) {
                    next if (!$row->{$f});
                    if ($q->param('magnification') =~ /^[0-9.]+$/) {
                        $row->{'real_'.$f} = $row->{$f}/$q->param('magnification'); 
                    } else {
                        $row->{'real_'.$f} = $row->{$f};
                    }
                }
                main::dbg("INSERT, TYPE $type: ".Dumper($row));
                $row->{'measurement_type'} = $type;
                $row->{'specimen_no'} = $specimen_no;
                $row->{'error_unit'}=$q->param($type."_error_unit");
                $dbt->insertRecord($s,'measurements',$row);
            }

            print "<center><h3>Specimen measurement data for $taxon_name (collection_no $collection_no) has been added</h3></center>\n";
        } else {
            print "Error inserting database table row, please contact support";
            carp "Error inserting row in Measurement.pm: ".$result;
        }
	}

	print "<div align=\"center\"><table><tr><td><ul>".
	      "<li><a href=\"$exec_url?action=populateMeasurementForm&skip_ref_check=1&specimen_no=-1&occurrence_no=".$q->param('occurrence_no')."\">Add another measurement of this occurrence</a></li>".
	      "<li><a href=\"$exec_url?action=displaySpecimenList&occurrence_no=".$q->param('occurrence_no')."\">Edit another measurement of this occurrence</a></li>".
	      "<li><a href=\"$exec_url?action=submitSpecimenSearch&use_reference=current&collection_no=$collection_no\">Add a measurement of another occurrence in this collection</a></li>".
	      "<li><a href=\"$exec_url?action=submitSpecimenSearch&use_reference=current&taxon_name=$taxon_name\">Add a measurement of $taxon_name in another collection</a></li>".
          "<li><a href=\"$exec_url?action=checkTaxonInfo&taxon_name=$taxon_name\">Get general info about this taxon</a></li>".
          "</ul></td></tr></table></div>";
}

sub formatMeasurement {
    my $dbt = shift;
    my $data = shift;
    my $s = "";

    $s .= " $data->{specimen_id} - " if ($data->{'specimen_id'});
    if ($data->{'specimens_measured'} != 1) {
        $s .= " $data->{specimens_measured} specimens - ";
    }

    $sql = "SELECT * FROM measurements WHERE specimen_no=$data->{specimen_no}";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        if ($row->{'average'}) {
            if ($data->{'magnification'}) {
                my $num = sprintf(" %.4f",$row->{'average'}/$data->{'magnification'});
                $num =~ s/0+$//;
                $s .= " $num x";
            } else {
                $s .= " $row->{average} x";
            }
        }
    }
    $s =~ s/x$//;
    $s .= " mm";
}


# General purpose function for getting occurrences with data.  Pass in 2 arguments:
#   Argument 1 is $dbt object
#   Argument 2 is hash array of options
#   i.e. getSpecimens($dbt,'collection_no'=>1111,'taxon_name'=>'Calippus')
#   Possible values for options:
#      taxon_no: a taxon_no.  Will performa  taxonomic_search on this no
#      taxon_name: Will find all taxon_nos this corresponds to, and combine the 
#          taxonomic_searchs for all of them.  If a taxon_no is not found, then
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


    my ($sql1,$sql2,$where) = ("","","");

    $sql1 = "SELECT s.*,m.*,o.taxon_no FROM specimens s, occurrences o, measurements m WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no";
    $sql2 = "SELECT s.*,m.*,a.taxon_no FROM specimens s, authorities a, measurements m WHERE a.taxon_no=s.taxon_no AND s.specimen_no=m.specimen_no";
    if ($options{'taxon_list'}) {
        my $taxon_nos = join(",",@{$options{'taxon_list'}});
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND a.taxon_no IN ($taxon_nos)";
    } elsif ($options{'taxon_name'} || $options{'taxon_no'}) {
        my @taxa;
        if ($options{'taxon_name'}) {
            @taxa = TaxonInfo::getTaxon($dbt,'taxon_name'=>$options{'taxon_name'});
        } else {
            @taxa = TaxonInfo::getTaxon($dbt,'taxon_no'=>$options{'taxon_no'});
        }
        if (@taxa) {
            my (@taxon_nos,%all_taxa);
            foreach (@taxa) {
                @taxon_nos = PBDBUtil::taxonomic_search($dbt,$_->{'taxon_no'});
                @all_taxa{@taxon_nos} = ();
            }
            @taxon_nos = keys %all_taxa;
            my $taxon_nos = join(",",@taxon_nos);
            $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
            $sql2 .= " AND a.taxon_no IN ($taxon_nos)";
        } elsif ($options{'taxon_name'}) {
            my @taxon_bits = split(/\s+/,$options{'taxon_name'});
            $sql1 .= " AND o.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
            if (scalar(@taxon_bits) > 1) {
                $sql1 .= "AND o.species_name LIKE ".$dbh->quote($taxon_bits[1]);
            }
        }
    } 
    if ($options{'collection_no'}) {
        $sql1 .= " AND o.collection_no=".int($options{'collection_no'});
    }
    if ($options{'occurrence_no'}) {
        $sql1 .= " AND o.occurrence_no =".int($options{'occurrence_no'});
    }

    if ($options{'get_global_specimens'} && $sql2 =~ /taxon_no IN/) {
        $sql = "($sql1) UNION ($sql2)";
    } else {
        $sql = $sql1;
    }
    main::dbg("SQL is $sql");

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
#        $p_table{'a_mean'}{$row->{'measurement_type'}} += $row->{'specimens_measured'} * $row->{'real_average'};
        # Note that "average" is the geometric mean - a_mean (arithmetic mean) is not used right now
        $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'average'} += $row->{'specimens_measured'} * log($row->{'real_average'});
        if ($row->{'specimens_measured'} == 1) {
            if (!exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'} || $row->{'real_average'} < $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'}) {
                $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'} = $row->{'real_average'};
            }
            if (!exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'} || $row->{'real_average'} > $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'max'}) {
                $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'max'} = $row->{'real_average'};
            }
        } else {
            if (!exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'} || $row->{'real_min'} < $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'}) {
                $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'min'} = $row->{'real_min'};
            }
            if (!exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'max'} || $row->{'real_max'} > $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'max'}) {
                $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'max'} = $row->{'real_max'};
            }
        }
    }    

    while (my ($part,$m_table) = each %p_table) {
        foreach my $type (keys %$m_table) {
            if ($m_table->{'specimens_measured'}) {
                $m_table->{$type}{'average'} = exp($m_table->{$type}{'average'}/$m_table->{'specimens_measured'});
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
            my @measurements_by_part = ();
            foreach my $row (@measurements) {
                if ($row->{'specimen_part'} eq $part) {
                    push @measurement_for_part,$row;
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
                    @values = sort @$values_array_ref;
                    if (@values) {
                        if (scalar(@values) % 2 == 0) {
                            my $middle_index = int(scalar(@values)/2);
                            my $median = ($values[$middle_index] + $values[$middle_index+1])/2;
                            $m_table->{$type}{'median'} = $median;
                        } else {
                            $m_table->{$type}{'median'} = $values[scalar(@values)/2];
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
