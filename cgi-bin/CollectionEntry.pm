# includes entry functions extracted from Collection.pm JA 4.6.13

package CollectionEntry;
use strict;

use PBDBUtil;
use Taxon;
use TaxonInfo;
use TimeLookup;
use TaxaCache;
use Person;
use Permissions;
use Class::Date qw(now date);
use Debug qw(dbg);
use URI::Escape;
use Debug;
#use Map;    
use Constants qw($WRITE_URL $HTML_DIR $HOST_URL $TAXA_TREE_CACHE $DB $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO $PAGE_TOP $PAGE_BOTTOM);

# this is a shell function that will have to be replaced with something new
#  because Collection::getCollections is going with Fossilworks JA 4.6.13
sub getCollections	{
	my $dbt = $_[0];
	my $s = $_[1];
	my $dbh = $dbt->dbh;
	my %options = %{$_[2]};
	my @fields = @{$_[3]};
	use Collection;
	return (Collection::getCollections($dbt,$s,\%options,\@fields));
}

# JA 4.6.13
# this is actually a near-complete rewrite of Collection::getClassOrderFamily
#  that uses a simpler algorithm and exists strictly to enable the detangling
#  of the codebases
# it's expecting a prefabricated array of objects including the parent names,
#  numbers, and ranks
sub getClassOrderFamily	{
	my ($dbt,$rowref_ref,$class_array_ref) = @_;
	my $rowref;
	if ( $rowref_ref )	{
		$rowref = ${$rowref_ref};
	}
	my @class_array = @{$class_array_ref};
	if ( $#class_array == 0 )	{
		return $rowref;
	}
	for my $t ( @class_array )	{
		if ( $t->{taxon_rank} =~ /^(class|order|family|common_name)$/ )	{
			my $rank = $t->{taxon_rank};
			$rowref->{$rank} = $t->{taxon_name};
			$rowref->{$rank."_no"} = $t->{taxon_no};
		}
	}
	return $rowref;
}

# This is a multi step process: 
# First populate our page variables with prefs, these have the lowest priority
# TBD CHeck for reerence no
sub displayCollectionForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $isNewEntry = ($q->param('collection_no') =~ /^\d+$/) ? 0 : 1;
    my $reSubmission = ($q->param('action') =~ /processCollectionForm/) ? 1 : 0;

    # First check to nake sure they have a reference no for new entries
    my $session_ref = $s->get('reference_no');
    if ($isNewEntry) {
        if (!$session_ref) {
            $s->enqueue($q->query_string() );
            main::displaySearchRefs( "Please choose a reference first" );
            exit;
        }  
    }

    # First get all three sources of data: form submision (%form), prefs (%prefs), and database (%row)
    my %vars = ();

    my %row = ();
    if (!$isNewEntry) {
        my $collection_no = int($q->param('collection_no'));
        my $sql = "SELECT * FROM collections WHERE collection_no=$collection_no";
        my $c_row = ${$dbt->getData($sql)}[0] or die "invalid collection no";
        %row = %{$c_row};
    }
    my %prefs =  $s->getPreferences();
    my %form = $q->Vars();


    if ($reSubmission) {
        %vars = %form;
    } if ($isNewEntry && int($q->param('prefill_collection_no'))) {
        my $collection_no = int($q->param('prefill_collection_no'));
        my $sql = "SELECT * FROM collections WHERE collection_no=$collection_no";
        my $row = ${$dbt->getData($sql)}[0] or die "invalid collection no";
        foreach my $field (keys(%$row)) {
            if ($field =~ /^(authorizer|enterer|modifier|authorizer_no|enterer_no|modifier_no|created|modified|collection_no)/) {
                delete $row->{$field};
            }
        }
        %vars = %$row;
        $vars{'reference_no'} = $s->get('reference_no');
    } elsif ($isNewEntry) {
        %vars = %prefs; 
        # carry over the lat/long coordinates the user entered while doing
        #  the mandatory collection search JA 6.4.04
        my @coordfields = ("latdeg","latmin","latsec","latdec","latdir","lngdeg","lngmin","lngsec","lngdec","lngdir");
        foreach my $cf (@coordfields) {
            $vars{$cf} = $form{$cf};
        }
        $vars{'reference_no'} = $s->get('reference_no');
    } else {
        %vars = %row;
    }

    ($vars{'sharealike'},$vars{'noderivs'},$vars{'noncommercial'}) = ('','Y','Y');
    if ( $vars{'license'} =~ /SA/ )	{
        $vars{'sharealike'} = 'Y';
    }
    if ( $vars{'license'} !~ /ND/ )	{
        $vars{'noderivs'} = '';
    }
    if ( $vars{'license'} !~ /NC/ )	{
        $vars{'noncommercial'} = '';
    }
    
    # always carry over optional fields
    $vars{'taphonomy'} = $prefs{'taphonomy'};
    $vars{'use_primary'} = $q->param('use_primary');

    my $ref = Reference::getReference($dbt,$vars{'reference_no'});
    my $formatted_primary = Reference::formatLongRef($ref);

    $vars{'ref_string'} = '<table cellspacing="0" cellpadding="2" width="100%"><tr>'.
    "<td valign=\"top\"><a href=\"?a=displayReference&reference_no=$vars{reference_no}\">".$vars{'reference_no'}."</a></b>&nbsp;</td>".
    "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
    "<td>$formatted_primary</td>".
    "</tr></table>";      

    if (!$isNewEntry) {
        my $collection_no = $row{'collection_no'};
        # We need to take some additional steps for an edit
        my $p = Permissions->new($s,$dbt);
        my $can_modify = $p->getModifierList();
        $can_modify->{$s->get('authorizer_no')} = 1;
        unless ($can_modify->{$row{'authorizer_no'}} || $s->isSuperUser) {
            my $authorizer = Person::getPersonName($dbt,$row{'authorizer_no'});
            print qq|<p class="warning">You may not edit this collection because you are not on the editing permission list of the authorizer ($authorizer)<br>
<a href="$WRITE_URL?a=displaySearchColls&type=edit"><br>Edit another collection</b></a>
|;
            exit;
        }

        # translate the release date field to populate the pulldown
        # I'm not sure if we never did this at all, or if something got
        #  broken at some point, but it was causing big problems JA 10.5.07

        if ( date($vars{'created'}) != date($vars{'release_date'}) )	{
            $vars{'release_date'} = getReleaseString($vars{'created'},$vars{'release_date'});
        }

        # Secondary refs, followed by current ref
        my @secondary_refs = ReferenceEntry::getSecondaryRefs($dbt,$collection_no);
        if (@secondary_refs) {
            my $table = '<table cellspacing="0" cellpadding="2" width="100%">';
            for(my $i=0;$i < @secondary_refs;$i++) {
                my $sr = $secondary_refs[$i];
                my $ref = Reference::getReference($dbt,$sr);
                my $formatted_secondary = Reference::formatLongRef($ref);
                my $class = ($i % 2 == 0) ? 'class="darkList"' : '';
                $table .= "<tr $class>".
                  "<td valign=\"top\"><input type=\"radio\" name=\"secondary_reference_no\" value=\"$sr\">".
                  "</td><td valign=\"top\" style=\"text-indent: -1em; padding-left: 2em;\"><b>$sr</b> ".
                  "$formatted_secondary <span style=\"color: red;\">$ref->{project_name} $ref->{project_ref_no}</span>";
                if(refIsDeleteable($dbt,$collection_no,$sr)) {
                    $table .= " <nobr>&nbsp;<input type=\"checkbox\" name=\"delete_ref\" value=$sr> remove<nobr>";
                }
                $table .= "</td></tr>";
            }
            $table .= "</table>";
            $vars{'secondary_reference_string'} = $table;
        }   

        # Check if current session ref is at all associated with the collection
        # If not, list it beneath the sec. refs. (with radio button for selecting
        # as the primary ref, as with the secondary refs below).
        if ($session_ref) {
            unless(isRefPrimaryOrSecondary($dbt,$collection_no,$session_ref)){
                my $ref = Reference::getReference($dbt,$session_ref);
                my $sr = Reference::formatLongRef($ref);
                my $table = '<table cellspacing="0" cellpadding="2" width="100%">'
                          . "<tr class=\"darkList\"><td valign=top><input type=radio name=secondary_reference_no value=$session_ref></td>";
                $table .= "<td valign=top><b>$ref->{reference_no}</b></td>";
                $table .= "<td>$sr</td></tr>";
                # Now, set up the current session ref to be added as a secondary even
                # if it's not picked as a primary (it's currently neither).
                $table .= "<tr class=\"darkList\"><td></td><td colspan=2><input type=checkbox name=add_session_ref value=\"YES\"> Add session reference as secondary reference</td></tr>\n";
                $table .= "</table>";
                $vars{'session_reference_string'} = $table;
            }
        }
    }

    # Get back the names for these
	if ( $vars{'max_interval_no'} )	{
		my $sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=".$vars{'max_interval_no'};
        my $interval = ${$dbt->getData($sql)}[0];
		$vars{'eml_max_interval'} = $interval->{eml_interval};
		$vars{'max_interval'} = $interval->{interval_name};
	}
	if ( $vars{'min_interval_no'} )	{
		my $sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=".$vars{'min_interval_no'};
        my $interval = ${$dbt->getData($sql)}[0];
		$vars{'eml_min_interval'} = $interval->{eml_interval};
		$vars{'min_interval'} = $interval->{interval_name};
	}

    $ref = Reference::getReference($dbt,$vars{'reference_no'});
    $formatted_primary = Reference::formatLongRef($ref);

	print PBDBUtil::printIntervalsJava($dbt);

    if ($isNewEntry) {
        $vars{'page_title'} =  "Collection entry form";
        $vars{'page_submit_button'} = '<input type=submit name="enter_button" value="Enter collection and exit">';
    } else {
        $vars{'page_title'} =  "Collection number ".$vars{'collection_no'};
        $vars{'page_submit_button'} = '<input type=submit name="edit_button" value="Edit collection and exit">';
        if ( $vars{'art_whole_bodies'} || $vars{'disart_assoc_maj_elems'} || $vars{'disassoc_maj_elems'} || $vars{'disassoc_minor_elems'} )	{
            $vars{'elements'} = 1;
        }
    }

    # Output the main part of the page
    print $hbo->populateHTML("collection_form", \%vars);
}


#  * User submits completed collection entry form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processCollectionForm {
	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;

	my $reference_no = $q->param("reference_no");
	my $secondary = $q->param('secondary_reference_no');

	my $collection_no = $q->param($COLLECTION_NO);

	my $isNewEntry = ($collection_no > 0) ? 0 : 1;
    
	# If a radio button was checked, we're changing a secondary to the primary
	if ($secondary)	{
		$q->param(reference_no => $secondary);
	}

	# there are three license checkboxes so users understand what they
	#  are doing, so combine the data JA 20.11.12
	my $license = 'CC BY';
	$license .= ( $q->param('noncommercial') ) ? '-NC' : '';
	$license .= ( $q->param('noderivs') ) ? '-ND' : '';
	$license .= ( $q->param('sharealike') ) ? '-SA' : '';
	$q->param('license' => $license);

	# change interval names into numbers by querying the intervals table
	# JA 11-12.7.03
	if ( $q->param('max_interval') )	{
		my $sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('max_interval') . "'";
		if ( $q->param('eml_max_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_max_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(max_interval_no => $no);
	}
	if ( $q->param('min_interval') )	{
		my $sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('min_interval') . "'";
		if ( $q->param('eml_min_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_min_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(min_interval_no => $no);
	} else	{
		$q->param(min_interval_no => 0);
	}

	# bomb out if no such interval exists JA 28.7.03
	if ( $q->param('max_interval_no') < 1 )	{
		print "<center><p>You can't enter an unknown time interval name</p>\n<p>Please go back, check the time scales, and enter a valid name</p></center>";
		return;
	}

    unless($q->param('fossilsfrom1')) {
      $q->param(fossilsfrom1=>'');
    }
    unless($q->param('fossilsfrom2')) {
      $q->param(fossilsfrom2=>'');
    }


    my $is_valid = validateCollectionForm($dbt,$q,$s);

    if ($is_valid) {

        #set paleolat, paleolng if we can PS 11/07/2004
        my ($paleolat, $paleolng, $pid);
        if ($q->param('lngdeg') >= 0 && $q->param('lngdeg') =~ /\d+/ &&
            $q->param('latdeg') >= 0 && $q->param('latdeg') =~ /\d+/)
        {
            my ($f_latdeg, $f_lngdeg) = ($q->param('latdeg'), $q->param('lngdeg') );
            if ($q->param('lngmin') =~ /\d+/ && $q->param('lngmin') >= 0 && $q->param('lngmin') < 60)  {
                $f_lngdeg += $q->param('lngmin')/60 + $q->param('lngsec')/3600;
            } elsif ($q->param('lngdec') > 0) {
                $f_lngdeg .= ".".$q->param('lngdec');
            }
            if ($q->param('latmin') =~ /\d+/ && $q->param('latmin') >= 0 && $q->param('latmin') < 60)  {
                $f_latdeg += $q->param('latmin')/60 + $q->param('latsec')/3600;
            } elsif ($q->param('latdec') > 0) {
                $f_latdeg .= ".".$q->param('latdec');
            }
            dbg("f_lngdeg $f_lngdeg f_latdeg $f_latdeg");
            if ($q->param('lngdir') =~ /West/)  {
                    $f_lngdeg = $f_lngdeg * -1;
            }
            if ($q->param('latdir') =~ /South/) {
                    $f_latdeg = $f_latdeg * -1;
            }
            # oh by the way, set type float lat and lng fields JA 26.11.11
            # one step on the way to ditching the old lat/long fields...
            $q->param('lat' => $f_latdeg);
            $q->param('lng' => $f_lngdeg);
            # set precision based on the latitude fields, assuming that the
            #  longitude fields are consistent JA 26.11.11
            if ( $q->param('latsec') =~ /[0-9]/ )	{
                $q->param('latlng_precision' => 'seconds');
            } elsif ( $q->param('latmin') =~ /[0-9]/ )	{
                $q->param('latlng_precision' => 'minutes');
            } elsif ( length($q->param('latdec')) > 0 && length($q->param('latdec')) < 9 )	{
                $q->param('latlng_precision' => length($q->param('latdec')));
            } elsif ( length($q->param('latdec')) > 0 )	{
                $q->param('latlng_precision' => 8);
            } else	{
                $q->param('latlng_precision' => 'degrees');
            }

            my $max_interval_no = ($q->param('max_interval_no')) ? $q->param('max_interval_no') : 0;
            my $min_interval_no = ($q->param('min_interval_no')) ? $q->param('min_interval_no') : 0;
            ($paleolng, $paleolat, $pid) = getPaleoCoords($dbt,$max_interval_no,$min_interval_no,$f_lngdeg,$f_latdeg);
            dbg("have paleocoords paleolat: $paleolat paleolng $paleolng");
            if ($paleolat ne "" && $paleolng ne "") {
                $q->param("paleolng"=>$paleolng);
                $q->param("paleolat"=>$paleolat);
                $q->param("plate"=>$pid);
            }
        }


        # figure out the release date, enterer, and authorizer
        my $created = now();
        if (!$isNewEntry) {
            my $sql = "SELECT created FROM $COLLECTIONS WHERE $COLLECTION_NO=$collection_no";
            my $row = ${$dbt->getData($sql)}[0];
            die "Could not fetch collection $collection_no from the database" unless $row;
            $created = $row->{created};
        }
        my $release_date = getReleaseDate($created, $q->param('release_date'));
        $q->param('release_date'=>$release_date);

        # Now final checking
        my %vars = $q->Vars;

        my ($dupe,$matches) = (0,0);
        if ($isNewEntry) {
            $dupe = $dbt->checkDuplicates($COLLECTIONS,\%vars);
#          $matches = $dbt->checkNearMatch($COLLECTIONS,$COLLECTION_NO,$q,99,"something=something?");
        }

        if ($dupe) {
            $collection_no = $dupe;
        } elsif ($matches) {
            # Nothing to do, page generation and form processing handled
            # in the checkNearMatch function
        } else {
            if ($isNewEntry) {
                my ($status,$coll_id) = $dbt->insertRecord($s,$COLLECTIONS, \%vars);
                $collection_no = $coll_id;
            } else {
                my $status = $dbt->updateRecord($s,$COLLECTIONS,$COLLECTION_NO,$collection_no,\%vars);
            }
        }

	# if numerical dates were entered, set the best-matching interval no
	my $ma;
	if ( $q->param('direct_ma') > 0 )	{
		my $no = setMaIntervalNo($dbt,$dbh,$collection_no,$q->param('direct_ma'),$q->param('direct_ma_unit'),$q->param('direct_ma'),$q->param('direct_ma_unit'));
	}
	elsif ( $q->param('max_ma') > 0 || $q->param('min_ma')> 0 )	{
		my $no = setMaIntervalNo($dbt,$dbh,$collection_no,$q->param('max_ma'),$q->param('max_ma_unit'),$q->param('min_ma'),$q->param('min_ma_unit'));
	} else	{
		setMaIntervalNo($dbt,$dbh,$collection_no);
	}
            
        # Secondary ref handling.  Handle this after updating the collection or it'll mess up
        if ($secondary) {
            # The updateRecord() logic will take care of putting in the new primary
            # reference for the collection
            # Now, put the old primary ref into the secondary ref table
            setSecondaryRef($dbt, $collection_no, $reference_no);
            # and remove the new primary from the secondary table
            deleteRefAssociation($dbt, $collection_no, $secondary);
        }
        # If the current session ref isn't being made the primary, and it's not
        # currently a secondary, add it as a secondary ref for the collection 
        # (this query param doesn't show up if session ref is already a 2ndary.)
        if($q->param('add_session_ref') eq 'YES'){
            my $session_ref = $s->get("reference_no");
            if($session_ref != $secondary) {
                setSecondaryRef($dbt, $collection_no, $session_ref);
            }
        }
        # Delete secondary ref associations
        my @refs_to_delete = $q->param("delete_ref");
        dbg("secondary ref associations to delete: @refs_to_delete<br>");
        if(scalar @refs_to_delete > 0){
            foreach my $ref_no (@refs_to_delete){
                # check if any occurrences with this ref are tied to the collection
                if(refIsDeleteable($dbt, $collection_no, $ref_no)){
                    # removes secondary_refs association between the numbers.
                    dbg("removing secondary ref association (col,ref): $collection_no, $ref_no<br>");
                    deleteRefAssociation($dbt, $collection_no, $ref_no);
                }
            }
        }

        my $verb = ($isNewEntry) ? "added" : "updated";
        print "<center><p class=\"pageTitle\" style=\"margin-bottom: -0.5em;\"><font color='red'>Collection record $verb</font></p><p class=\"medium\"><i>Do not hit the back button!</i></p></center>";

	my $coll;
       	my ($colls_ref) = getCollections($dbt,$s,{$COLLECTION_NO=>$collection_no},['authorizer','enterer','modifier','*']);
       	$coll = $colls_ref->[0];

        if ($coll) {
            
            # If the viewer is the authorizer (or it's me), display the record with edit buttons
            my $links = '<p><div align="center"><table><tr><td>';
            my $p = Permissions->new($s,$dbt);
            my $can_modify = $p->getModifierList();
            $can_modify->{$s->get('authorizer_no')} = 1;
            
            if ($can_modify->{$coll->{'authorizer_no'}} || $s->isSuperUser) {
                $links .= qq|<li><a href="$WRITE_URL?a=displayCollectionForm&collection_no=$collection_no">Edit this collection</a></li>|;
            }
            $links .= qq|<li><a href="$WRITE_URL?a=displayCollectionForm&prefill_collection_no=$collection_no">Add a collection copied from this one</a></li>|;
            if ($isNewEntry) {
                $links .= qq|<li><a href="$WRITE_URL?a=displaySearchCollsForAdd&type=add">Add another collection with the same reference</a></li>|;
            } else {
                $links .= qq|<li><a href="$WRITE_URL?a=displaySearchCollsForAdd&type=add">Add a collection with the same reference</a></li>|;
                $links .= qq|<li><a href="$WRITE_URL?a=displaySearchColls&type=edit">Edit another collection with the same reference</a></li>|;
                $links .= qq|<li><a href="$WRITE_URL?a=displaySearchColls&type=edit&use_primary=yes">Edit another collection using its own reference</a></li>|;
            }
            $links .= qq|<li><a href="$WRITE_URL?a=displayOccurrenceAddEdit&collection_no=$collection_no">Edit taxonomic list</a></li>|;
            $links .= qq|<li><a href="$WRITE_URL?a=displayOccurrenceListForm&collection_no=$collection_no">Paste in taxonomic list</a></li>|;
            $links .= qq|<li><a href="$WRITE_URL?a=displayCollResults&type=occurrence_table&reference_no=$coll->{reference_no}">Edit occurrence table for collections from the same reference</a></li>|;
            if ( $s->get('role') =~ /authorizer|student|technician/ )	{
                $links .= qq|<li><a href="$WRITE_URL?a=displayOccsForReID&collection_no=$collection_no">Reidentify taxa</a></li>|;
            }
            $links .= "</td></tr></table></div></p>";

            $coll->{'collection_links'} = $links;

            displayCollectionDetailsPage($dbt,$hbo,$q,$s,$coll);
        }
    }
}

# Set the release date
# originally written by Ederer; made a separate function by JA 26.6.02
sub getReleaseDate	{
	my ($createdDate,$releaseDateString) = @_;
	my $releaseDate = date($createdDate);

	if ( $releaseDateString eq 'three months')	{
		$releaseDate = $releaseDate+'3M';
	} elsif ( $releaseDateString eq 'six months')	{
		$releaseDate = $releaseDate+'6M';
	} elsif ( $releaseDateString eq 'one year')	{
		$releaseDate = $releaseDate+'1Y';
	} elsif ( $releaseDateString eq 'two years') {
		$releaseDate = $releaseDate+'2Y';
	} elsif ( $releaseDateString eq 'three years')	{
		$releaseDate = $releaseDate+'3Y';
	} elsif ( $releaseDateString eq 'four years')	{
        	$releaseDate = $releaseDate+'4Y';
	} elsif ( $releaseDateString eq 'five years')	{
		$releaseDate = $releaseDate+'5Y';
	}
	# Else immediate release
	return $releaseDate;
}

sub getReleaseString	{
	my ($created_date,$releaseDate) = @_;
	my $createdDate = date($created_date);
	my $releaseDate = date($releaseDate);
	my $releaseDateString = "immediate";

	if ( $releaseDate > $createdDate+'1M' && $releaseDate <= $createdDate+'3M' )	{
		$releaseDateString = 'three months';
	} elsif ( $releaseDate <= $createdDate+'6M' )	{
		$releaseDateString = 'six months';
	} elsif ( $releaseDate <= $createdDate+'1Y' )	{
		$releaseDateString = 'one year';
	} elsif ( $releaseDate <= $createdDate+'2Y' )	{
		$releaseDateString = 'two years';
	} elsif ( $releaseDate <= $createdDate+'3Y' )	{
		$releaseDateString = 'three years';
        } elsif ( $releaseDate <= $createdDate+'4Y' )	{
		$releaseDateString = 'four years';
	} elsif ( $releaseDate <= $createdDate+'5Y' )	{
		$releaseDateString = 'five years';
	}
	# Else immediate release
	return $releaseDateString;
}

# Make this more thorough in the future
sub validateCollectionForm {
	my ($dbt,$q,$s) = @_;
	my $is_valid = 1;
	unless($q->param('max_interval'))	{
		print "<center><p>The time interval field is required!</p>\n<p>Please go back and specify the time interval for this collection</p></center>";
		print "<br><br>";
		$is_valid = 0;
	}
	return $is_valid;
}


# JA 15.11.10
# records the narrowest interval that includes the direct Ma values entered on the collection form
# it's useful to know this because the enterer may have put in interval names that are either more
#  broad than necessary or in outright conflict with the numerical values
sub setMaIntervalNo	{
	my ($dbt,$dbh,$coll,$max,$max_unit,$min,$min_unit) = @_;
	my $sql;
	if ( $max < $min || ! $max || ! $min )	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=NULL WHERE collection_no=$coll";
		$dbh->do($sql);
		return 0;
	}

	# units matter! JA 25.3.11
	if ( $max_unit =~ /ka/i )	{
		$max /= 1000;
	} elsif ( $max_unit =~ /ybp/i )	{
		$max /= 1000000;
	}
	if ( $min_unit =~ /ka/i )	{
		$min /= 1000;
	} elsif ( $min_unit =~ /ybp/i )	{
		$min /= 1000000;
	}

	# users will want a stage name if possible
	$sql = "SELECT interval_no FROM interval_lookup WHERE base_age>$max AND top_age<$min AND stage_no>0 ORDER BY base_age-top_age";
	my $no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE base_age>$max AND top_age<$min AND subepoch_no>0 ORDER BY base_age-top_age";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE base_age>$max AND top_age<$min AND epoch_no>0 ORDER BY base_age-top_age";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE base_age>$max AND top_age<$min ORDER BY base_age-top_age";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no > 0 )	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=$no WHERE collection_no=$coll";
		$dbh->do($sql);
		return 1;
	} else	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=NULL WHERE collection_no=$coll";
		$dbh->do($sql);
		return 0;
	}
}


#  * User selects a collection from the displayed list
#  * System displays selected collection
sub displayCollectionDetails {
	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;
	# previously displayed a collection, but this function is only now
	#  used for entry results display, so bots shouldn't see anything
	#  JA 4.6.13
	if ( PBDBUtil::checkForBot() )	{
		return;
	}

	my $collection_no = int($q->param('collection_no'));

    # Handles the meat of displaying information about the colleciton
    # Separated out so it can be reused in enter/edit collection confirmation forms
    # PS 2/19/2006
    if ($collection_no !~ /^\d+$/) {
        print Debug::printErrors(["Invalid collection number $collection_no"]);
        return;
    }

	# grab the entire person table and work with a lookup hash because
	#  person is tiny JA 2.10.09
	my %name = %{PBDBUtil::getPersonLookup($dbt)};

	my $sql = "SELECT * FROM collections WHERE collection_no=" . $collection_no;
	my @rs = @{$dbt->getData($sql)};
	my $coll = $rs[0];
	$coll->{authorizer} = $name{$coll->{authorizer_no}};
	$coll->{enterer} = $name{$coll->{enterer_no}};
	$coll->{modifier} = $name{$coll->{modifier_no}};
	if (!$coll ) {
		print Debug::printErrors(["No collection with collection number $collection_no"]);
		return;
	}

    my $page_vars = {};
    if ( $coll->{'research_group'} =~ /ETE/ && $q->param('guest') eq '' )	{
        $page_vars->{ete_banner} = "<div style=\"padding-left: 0em; padding-right: 2em; float: left;\"><a href=\"http://www.mnh.si.edu/ETE\"><img alt=\"ETE\" src=\"/public/bannerimages/ete_logo.jpg\"></a></div>";
    }
    print $hbo->stdIncludes($PAGE_TOP, $page_vars);

    $coll = formatCoordinate($s,$coll);

    # Handle display of taxonomic list now
    # don't even let bots see the lists because they will index the taxon
    #  pages returned by TaxonInfo anyway JA 2.10.09
    my $taxa_list = buildTaxonomicList($dbt,$hbo,$s,{'collection_no'=>$coll->{'collection_no'},'hide_reference_no'=>$coll->{'reference_no'}});
    $coll->{'taxa_list'} = $taxa_list;

    my $links = "<div class=\"verysmall\">";

    # Links at bottom
    if ($s->isDBMember()) {
        $links .= '<p><div align="center">';
        my $p = Permissions->new($s,$dbt);
        my $can_modify = $p->getModifierList();
        $can_modify->{$s->get('authorizer_no')} = 1;

        if ($can_modify->{$coll->{'authorizer_no'}} || $s->isSuperUser) {  
            $links .= qq|<a href="$WRITE_URL?a=displayCollectionForm&collection_no=$collection_no">Edit collection</a> - |;
        }
        $links .=  qq|<a href="$WRITE_URL?a=displayCollectionForm&prefill_collection_no=$collection_no">Add a collection copied from this one</a>|;  
        $links .= "</div></p>";
    }
    $links .= "</div>\n";

    $coll->{'collection_links'} = $links;

    displayCollectionDetailsPage($dbt,$hbo,$q,$s,$coll);

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# split out of displayCollectionDetails JA 6.11.09
sub formatCoordinate	{

    my ($s,$coll) = @_;

    # if the user is not logged in, round off the degrees
    # DO NOT mess with this routine, because Joe Public must not be
    #  able to locate a collection in the field and pillage it
    # JA 10.5.07
    if ( ! $s->isDBMember() )	{
        if ( ! $coll->{'lngdec'} && $coll->{'lngmin'} )	{
            $coll->{'lngdec'} = ( $coll->{'lngmin'} / 60 ) + ( $coll->{'lngsec'}  / 3600 );
        } else	{
            $coll->{'lngdec'} = "0." . $coll->{'lngdec'};
        }
        if ( ! $coll->{'latdec'} && $coll->{'latmin'} )	{
            $coll->{'latdec'} = ( $coll->{'latmin'} / 60 ) + ( $coll->{'latsec'}  / 3600 );
        } else	{
            $coll->{'latdec'} = "0." . $coll->{'latdec'};
        }
        $coll->{'lngdec'} = int ( ( $coll->{'lngdec'} + 0.05 ) * 10 );
        $coll->{'latdec'} = int ( ( $coll->{'latdec'} + 0.05 ) * 10 );
        if ( $coll->{'lngdec'} == 10 )	{
            $coll->{'lngdeg'}++;
            $coll->{'lngdec'} = 0;
        }
        if ( $coll->{'latdec'} == 10 )	{
            $coll->{'latdeg'}++;
            $coll->{'latdec'} = 0;
        }
        $coll->{'lngmin'} = '';
        $coll->{'lngsec'} = '';
        $coll->{'latmin'} = '';
        $coll->{'latsec'} = '';
        $coll->{'geogcomments'} = '';
    }
    $coll->{'paleolatdir'} = "North";
    if ( $coll->{'paleolat'} < 0 )	{
        $coll->{'paleolatdir'} = "South";
    }
    $coll->{'paleolngdir'} = "East";
    if ( $coll->{'paleolng'} < 0 )	{
        $coll->{'paleolngdir'} = "West";
    }
    $coll->{'paleolat'} = sprintf "%.1f&deg;",abs($coll->{'paleolat'});
    $coll->{'paleolng'} = sprintf "%.1f&deg;",abs($coll->{'paleolng'});

    return $coll;
}

# JA 25.5.11
sub fromMinSec	{
	my ($deg,$min,$sec) = @_;
	$deg =~ s/[^0-9]//g;
	$min =~ s/[^0-9]//g;
	$sec =~ s/[^0-9]//g;
	my $dec = $deg + $min/60 + $sec/3600;
	my $format = "minutes";
	if ( $sec ne "" )	{
		$format = "seconds";
	}
	return ($dec,$format);
}

sub fromDecDeg {
    
    my ($deg, $frac) = @_;
    
    $deg =~ s/[^0-9]//g;
    $frac =~ s/[^0-9]//g;

    my $dec = "$deg.$frac";
    return ($dec);
}

# JA 25.5.11
sub toMinSec	{
	my ($deg,$dec) = split /\./,$_[0];
	$dec = ".".$dec;
	my $min = int($dec * 60);
	my $sec = int($dec *3600 - $min * 60);
	return ($deg,$min,$sec);
}

sub displayCollectionDetailsPage {
    my ($dbt,$hbo,$q,$s,$row) = @_;
    my $dbh = $dbt->dbh;
    my $collection_no = $row->{'collection_no'};
    return if (!$collection_no);

    # Get the reference
    if ($row->{'reference_no'}) {
        $row->{'reference_string'} = '';
        my $ref = Reference::getReference($dbt,$row->{'reference_no'});
        my $formatted_primary = Reference::formatLongRef($ref);
        $row->{'reference_string'} = '<table cellspacing="0" cellpadding="2" width="100%"><tr>'.
            "<td valign=\"top\"><a href=\"?a=displayReference&reference_no=$row->{reference_no}\">".$row->{'reference_no'}."</a></td>".
            "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
            "<td>$formatted_primary</td>".
            "</tr></table>";
        
        $row->{'secondary_reference_string'} = '';
        my @secondary_refs = ReferenceEntry::getSecondaryRefs($dbt,$collection_no);
        if (@secondary_refs) {
            my $table = "";
            $table .= '<table cellspacing="0" cellpadding="2" width="100%">';
            for(my $i=0;$i < @secondary_refs;$i++) {
                my $sr = $secondary_refs[$i];
                my $ref = Reference::getReference($dbt,$sr);
                my $formatted_secondary = Reference::formatLongRef($ref);
                my $class = ($i % 2 == 0) ? 'class="darkList"' : '';
                $table .= "<tr $class>".
                    "<td valign=\"top\"><a href=\"?a=displayReference&reference_no=$sr\">$sr</a></td>".
                    "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
                    "<td>$formatted_secondary</td>".
                    "</tr>";
            }
            $table .= "</table>";
            $row->{'secondary_reference_string'} = $table;
        }
    }


        my $sql;

	# Get any subset collections JA 25.6.02
	$sql = "SELECT collection_no FROM collections where collection_subset=" . $collection_no;
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my @subrowrefs = @{$sth->fetchall_arrayref()};
    $sth->finish();
    my @links = ();
    foreach my $ref (@subrowrefs)	{
      push @links, "<a href=\"?a=displayCollectionDetails&collection_no=$ref->[0]\">$ref->[0]</a>";
    }
    my $subString = join(", ",@links);
    $row->{'subset_string'} = $subString;

    my $sql1 = "SELECT DISTINCT authorizer_no, enterer_no, modifier_no FROM occurrences WHERE collection_no=" . $collection_no;
    my $sql2 = "SELECT DISTINCT authorizer_no, enterer_no, modifier_no FROM reidentifications WHERE collection_no=" . $collection_no;
    my @names = (@{$dbt->getData($sql1)},@{$dbt->getData($sql2)});
    my %lookup = %{PBDBUtil::getPersonLookup($dbt)};
    if (@names) {
        my %unique_auth = ();
        my %unique_ent = ();
        my %unique_mod = ();
        foreach (@names) {
            $unique_auth{$lookup{$_->{'authorizer_no'}}}++;
            $unique_ent{$lookup{$_->{'enterer_no'}}}++;
            $unique_mod{$lookup{$_->{'modifier_no'}}}++ if ($_->{'modifier'});
        }
        delete $unique_auth{$row->{'authorizer'}};
        delete $unique_ent{$row->{'enterer'}};
        delete $unique_mod{$row->{'modifier'}};
        $row->{'authorizer'} .= ", $_" for (keys %unique_auth);
        $row->{'enterer'} .= ", $_" for (keys %unique_ent);
        $row->{'modifier'} .= ", $_" for (keys %unique_mod);
        # many collections have no modifier, so the initial comma needs to be
        #  stripped off
        $row->{'modifier'} =~ s/^, //;
    }

	# get the max/min interval names
	$row->{'interval'} = '';
	if ( $row->{'max_interval_no'} ) {
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $row->{'max_interval_no'};
        my $max_row = ${$dbt->getData($sql)}[0];
        $row->{'interval'} .= qq|<a href="?a=displayInterval&interval_no=$row->{max_interval_no}">|;
        $row->{'interval'} .= $max_row->{'eml_interval'}." " if ($max_row->{'eml_interval'});
        $row->{'interval'} .= $max_row->{'interval_name'};
        $row->{'interval'} .= '</a>';
	} 

	if ( $row->{'min_interval_no'}) {
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $row->{'min_interval_no'};
        my $min_row = ${$dbt->getData($sql)}[0];
        $row->{'interval'} .= " - ";
        $row->{'interval'} .= qq|<a href="?a=displayInterval&interval_no=$row->{min_interval_no}">|;
        $row->{'interval'} .= $min_row->{'eml_interval'}." " if ($min_row->{'eml_interval'});
        $row->{'interval'} .= $min_row->{'interval_name'};
        $row->{'interval'} .= '</a>';

        if (!$row->{'max_interval_no'}) {
            $row->{'interval'} .= " <span class=small>(minimum)</span>";
        }
	} 
    my $time_place = $row->{'collection_name'}.": ";
    $time_place .= "$row->{interval}";
    if ($row->{'state'} && $row->{country} eq "United States") {
        $time_place .= ", $row->{state}";
    } elsif ($row->{'country'}) {
        $time_place .= ", $row->{country}";
    }
    if ( $row->{'collectors'} || $row->{'collection_dates'} ) {
        $time_place .= "<br><small>collected ";
        if ( $row->{'collectors'} ) {
            my $collectors = $row->{'collectors'};
            $time_place .= " by " .$collectors . " ";
        }
        if ( $row->{'collection_dates'} ) {
            my $years = $row->{'collection_dates'};
            $years =~ s/[A-Za-z\.]//g;
            $years =~ s/\b[0-9]([0-9]|)\b//g;
            $years =~ s/^( |),//;
            $time_place .= $years;
        }
        $time_place .= "</small>";
    }
    $row->{'collection_name'} = $time_place;

    my @intervals = ();
    push @intervals, $row->{'max_interval_no'} if ($row->{'max_interval_no'});
    push @intervals, $row->{'min_interval_no'} if ($row->{'min_interval_no'} && $row->{'min_interval_no'} != $row->{'max_interval_no'});
    my $max_lookup;
    my $min_lookup;
    if (@intervals) { 
        my $t = new TimeLookup($dbt);
        my $lookup = $t->lookupIntervals(\@intervals);
        $max_lookup = $lookup->{$row->{'max_interval_no'}};
        if ($row->{'min_interval_no'}) { 
            $min_lookup = $lookup->{$row->{'min_interval_no'}};
        } else {
            $min_lookup=$max_lookup;
        }
    }
    if ($max_lookup->{'base_age'} && $min_lookup->{'top_age'}) {
        my @boundaries = ($max_lookup->{'base_age'},$max_lookup->{'top_age'},$min_lookup->{'base_age'},$min_lookup->{'top_age'});
        @boundaries = sort {$b <=> $a} @boundaries;
        # Get rid of extra trailing zeros
        $boundaries[0] =~ s/(\.0|[1-9])(0)*$/$1/;
        $boundaries[-1] =~ s/(\.0|[1-9])(0)*$/$1/;
        $row->{'age_range'} = $boundaries[0]." - ".$boundaries[-1]." m.y. ago";
    } else {
        $row->{'age_range'} = "";
    }
    if ( $row->{'direct_ma'} )	{
        $row->{'age_estimate'} .= $row->{'direct_ma'};
        if ( $row->{'direct_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'direct_ma_error'};
        }
        $row->{'age_estimate'} .= " ".$row->{'direct_ma_unit'}." (" . $row->{'direct_ma_method'} . ")";
    }
    my $link;
    my $endlink;
    if ( $row->{'max_ma'} )	{
        if ( ! $row->{'min_ma'} )	{
            $row->{'age_estimate'} .= "maximum ";
        }
        $row->{'age_estimate'} .= $row->{'max_ma'};
        if ( $row->{'max_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'max_ma_error'};
        }
        if ( $row->{'min_ma'} && $row->{'max_ma_method'} ne $row->{'min_ma_method'} )	{
            $row->{'age_estimate'} .= " ".$row->{'max_ma_unit'}." ($link" . $row->{'max_ma_method'} . "$endlink)";
        }
    }
    if ( $row->{'min_ma'} && ( ! $row->{'max_ma'} || $row->{'min_ma'} ne $row->{'max_ma'} || $row->{'min_ma_method'} ne $row->{'max_ma_method'} ) )	{
        if ( ! $row->{'max_ma'} )	{
            $row->{'age_estimate'} .= "minimum ";
        } else	{
            $row->{'age_estimate'} .= " to ";
        }
        $row->{'age_estimate'} .= $row->{'min_ma'};
        if ( $row->{'min_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'min_ma_error'};
        }
        $row->{'age_estimate'} .= " ".$row->{'min_ma_unit'}." ($link" . $row->{'min_ma_method'} . "$endlink)";
    } elsif ( $row->{'age_estimate'} && $row->{'max_ma_method'} ne "" )	{
        $row->{'age_estimate'} .= " ".$row->{'max_ma_unit'}." ($link" . $row->{'max_ma_method'} . "$endlink)";
    }
    foreach my $term ("period","epoch","stage") {
        $row->{$term} = "";
        if ($max_lookup->{$term."_name"} &&
            $max_lookup->{$term."_name"} eq $min_lookup->{$term."_name"}) {
            $row->{$term} = $max_lookup->{$term."_name"};
        }
    }
    if ($max_lookup->{"ten_my_bin"} &&
        $max_lookup->{"ten_my_bin"} eq $min_lookup->{"ten_my_bin"}) {
        $row->{"ten_my_bin"} = $max_lookup->{"ten_my_bin"};
    } else {
        $row->{"ten_my_bin"} = "";
    }

    $row->{"zone_type"} =~ s/(^.)/\u$1/;

	# check whether we have period/epoch/locage/intage max AND/OR min:
    if ($s->isDBMember()) {
        foreach my $term ("epoch","intage","locage","period"){
            $row->{'legacy_'.$term} = '';
            if ($row->{$term."_max"}) {
                if ($row->{'eml'.$term.'_max'}) {
                    $row->{'legacy_'.$term} .= $row->{'eml'.$term.'_max'}." ";
                }
                $row->{'legacy_'.$term} .= $row->{$term."_max"};
            }
            if ($row->{$term."_min"}) {
                if ($row->{$term."_max"}) {
                    $row->{'legacy_'.$term} .= " - ";
                }
                if ($row->{'eml'.$term.'_min'}) {
                    $row->{'legacy_'.$term} .= $row->{'eml'.$term.'_min'}." ";
                }
                $row->{'legacy_'.$term} .= $row->{$term."_min"};
                if (!$row->{$term."_max"}) {
                    $row->{'legacy_'.$term} .= " <span class=small>(minimum)</span>";
                }
            }
        }
    }
    if ($row->{'legacy_period'} eq $row->{'period'}) {
        $row->{'legacy_period'} = '';
    }
    if ($row->{'legacy_epoch'} eq $row->{'epoch'}) {
        $row->{'legacy_epoch'} = '';
    }
    if ($row->{'legacy_locage'} eq $row->{'stage'}) {
        $row->{'legacy_locage'} = '';
    }
    if ($row->{'legacy_intage'} eq $row->{'stage'}) {
        $row->{'legacy_intage'} = '';
    }
    if ($row->{'legacy_epoch'} ||
        $row->{'legacy_period'} ||
        $row->{'legacy_intage'} ||
        $row->{'legacy_locage'}) {
        $row->{'legacy_message'} = 1;
    } else {
        $row->{'legacy_message'} = '';
    }

    if ($row->{'interval'} eq $row->{'period'} ||
        $row->{'interval'} eq $row->{'epoch'} ||
        $row->{'interval'} eq $row->{'stage'}) {
        $row->{'interval'} = '';
    }


    if ($row->{'collection_subset'}) {
        $row->{'collection_subset'} =  "<a href=\"?a=displayCollectionDetails&collection_no=$row->{collection_subset}\">$row->{collection_subset}</a>";
    }

    if ($row->{'regionalsection'}) {
        $row->{'regionalsection'} = "<a href=\"?a=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=regional&input=".uri_escape($row->{'regionalsection'})."\">$row->{regionalsection}</a>";
    }

    if ($row->{'localsection'}) {
        $row->{'localsection'} = "<a href=\"?a=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=local&input=".uri_escape($row->{'localsection'})."\">$row->{localsection}</a>";
    }
    if ($row->{'member'}) {
        $row->{'member'} = "<a href=\"?a=displayStrata&group_hint=".uri_escape($row->{'geological_group'})."&formation_hint=".uri_escape($row->{'formation'})."&group_formation_member=".uri_escape($row->{'member'})."\">$row->{member}</a>";
    }
    if ($row->{'formation'}) {
        $row->{'formation'} = "<a href=\"?a=displayStrata&group_hint=".uri_escape($row->{'geological_group'})."&group_formation_member=".uri_escape($row->{'formation'})."\">$row->{formation}</a>";
    }
    if ($row->{'geological_group'}) {
        $row->{'geological_group'} = "<a href=\"?a=displayStrata&group_formation_member=".uri_escape($row->{'geological_group'})."\">$row->{geological_group}</a>";
    }

    $row->{'modified'} = date($row->{'modified'});

    # textarea values often have returns that need to be rendered
    #  as <br>s JA 20.8.06
    for my $r ( keys %$row )	{
        if ( $r !~ /taxa_list/ && $r =~ /comment/ )	{
            $row->{$r} =~ s/\n/<br>/g;
        }
    }
    print $hbo->populateHTML('collection_display_fields', $row);

} # end sub displayCollectionDetails()


# builds the list of occurrences shown in places such as the collections form
# must pass it the collection_no
# reference_no (optional or not?? - not sure).
#
# optional arguments:
#
# gnew_names	:	reference to array of new genus names the user is entering (from the form)
# subgnew_names	:	reference to array of new subgenus names the user is entering
# snew_names	:	reference to array of new species names the user is entering
sub buildTaxonomicList {
	my ($dbt,$hbo,$s,$options) = @_;
	my %options = ();
	if ($options)	{
		%options = %{$options};
	}

	# dereference arrays.
	my @gnew_names = @{$options{'new_genera'}} if ($options{'new_genera'});
	my @subgnew_names = @{$options{'new_subgenera'}} if ($options{'new_subgenera'}) ;
	my @snew_names = @{$options{'new_species'}} if ($options{'new_species'});
	
	my $new_found = 0;		# have we found new taxa?  (ie, not in the database)
	my $return = "";

	# This is the taxonomic list part
	# join with taxa_tree_cache because lft and rgt will be used to
	#  order the list JA 13.1.07
	my $treefields = ", lft, rgt";
	my $sqlstart = "SELECT abund_value, abund_unit, genus_name, genus_reso, subgenus_name, subgenus_reso, plant_organ, plant_organ2, species_name, species_reso, comments, reference_no, occurrence_no, o.taxon_no taxon_no, collection_no";

	my $sqlmiddle;
	my $sqlend;
	if ($options{'collection_no'}) {
		$sqlmiddle = " FROM occurrences o ";
		$sqlend .= "AND collection_no=$options{'collection_no'}";
	} elsif ($options{'occurrence_list'} && @{$options{'occurrence_list'}}) {
		$sqlend .= "AND occurrence_no IN (".join(', ',@{$options{'occurrence_list'}}).") ORDER BY occurrence_no";
	} else	{
		$sqlend = "";
	}
	my $sql = $sqlstart . ", lft, rgt" . $sqlmiddle . ", $TAXA_TREE_CACHE t WHERE o.taxon_no=t.taxon_no " . $sqlend;
	my $sql2 = $sqlstart . $sqlmiddle . "WHERE taxon_no=0 " . $sqlend;

	my @warnings;
	if ($options{'warnings'}) {
		@warnings = @{$options{'warnings'}};
	}

	dbg("buildTaxonomicList sql: $sql");

	my @rowrefs;
	if ($sql) {
		@rowrefs = @{$dbt->getData($sql)};
		push @rowrefs , @{$dbt->getData($sql2)};
	}

	if (@rowrefs) {
		my @grand_master_list = ();
		my $are_reclassifications = 0;

		# loop through each row returned by the query
		foreach my $rowref (@rowrefs) {
			my $output = '';
			my %classification = ();


			# If we have specimens
			if ( $rowref->{'occurrence_no'} )	{
				my $sql_s = "SELECT count(*) c FROM specimens WHERE occurrence_no=$rowref->{occurrence_no}";
				my $specimens_measured = ${$dbt->getData($sql_s)}[0]->{'c'};
				if ($specimens_measured) {
    					my $s = ($specimens_measured > 1) ? 's' : '';
    					$rowref->{comments} .= " (<a href=\"?a=displaySpecimenList&occurrence_no=$rowref->{occurrence_no}\">$specimens_measured measurement$s</a>)";
				}
			}
			
			# if the user submitted a form such as adding a new occurrence or 
			# editing an existing occurrence, then we'll bold face any of the
			# new taxa which we don't already have in the database.
            # Bad bug: rewriting the data directly here fucked up all kinds of operations
            # below which expect the taxonomic names to be pure, just set some flags
            # and have stuff interpret them below PS 2006
			
			# check for unrecognized genus names
			foreach my $nn (@gnew_names){
				if ($rowref->{genus_name} eq  $nn) {
					$rowref->{new_genus_name} = 1;
					$new_found++;
				}
			}

			# check for unrecognized subgenus names
			foreach my $nn (@subgnew_names){
				if($rowref->{subgenus_name} eq $nn){
					$rowref->{new_subgenus_name} = 1;
					$new_found++;
				}
			}

			# check for unrecognized species names
			foreach my $nn (@snew_names){
				if($rowref->{species_name} eq $nn){
					$rowref->{new_species_name} = 1;
					$new_found++;
				}
			}

			# tack on the author and year if the taxon number exists
			# JA 19.4.04
			if ( $rowref->{taxon_no} )	{
				my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$rowref->{'taxon_no'}},['taxon_no','taxon_name','common_name','taxon_rank','author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority']);

				if ($taxon->{'taxon_rank'} =~ /species/ || $rowref->{'species_name'} =~ /^indet\.|^sp\./) {

					my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
					my $is_recomb = ($orig_no == $taxon->{'taxon_no'}) ? 0 : 1;
					$rowref->{'authority'} = Reference::formatShortRef($taxon,'no_inits'=>1,'link_id'=>$taxon->{'ref_is_authority'},'is_recombination'=>$is_recomb);
				}
			}

			my $formatted_reference = '';

			# if the occurrence's reference differs from the collection's, print it
			my $newrefno = $rowref->{'reference_no'};
			if ($newrefno != $options{'hide_reference_no'})	{
				$rowref->{reference_no} = Reference::formatShortRef($dbt,$newrefno,'no_inits'=>1,'link_id'=>1);
			} else {
				$rowref->{reference_no} = '';
			}
			
			# put all keys and values from the current occurrence
			# into two separate arrays.
			$rowref->{'taxon_name'} = formatOccurrenceTaxonName($rowref);
			$rowref->{'hide_collection_no'} = $options{'collection_no'};
	
			# get the most recent reidentification
			my $mostRecentReID;
			if ( $rowref->{'occurrence_no'} )	{
				$mostRecentReID = PBDBUtil::getMostRecentReIDforOcc($dbt,$rowref->{$OCCURRENCE_NO},1);
			}
			
			# if the occurrence has been reidentified at least once
			#  display the original and reidentifications.
			if ($mostRecentReID) {
				
				# rjp, 1/2004, change this so it displays *all* reidentifications, not just
				# the last one.
                # JA 2.4.04: this was never implemented by Poling, who instead
                #  went renegade and wrote the entirely redundant
		#  HTMLFormattedTaxonomicList; the correct way to do it was
		#  to pass in $rowref->{occurrence_no} and isReidNo = 0
                #  instead of $mostRecentReID and isReidNo = 1
	
				my $show_collection = '';
				my ($table,$classification,$reid_are_reclassifications) = getReidHTMLTableByOccNum($dbt,$hbo,$s,$rowref->{$OCCURRENCE_NO}, 0, $options{'do_reclassify'});
				$are_reclassifications = 1 if ($reid_are_reclassifications);
				$rowref->{'class'} = $classification->{'class'}{'taxon_name'};
				$rowref->{'order'} = $classification->{'order'}{'taxon_name'};
				$rowref->{'family'} = $classification->{'family'}{'taxon_name'};
				$rowref->{'common_name'} = ($classification->{'common_name'}{'taxon_no'});
				if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
					if ( $options{'do_reclassify'} )	{
						$rowref->{'class'} = qq|<span style="color: red;">unclassified</span>|;
					} else	{
						$rowref->{'class'} = "unclassified";
					}
				}
				if ( $rowref->{'class'} && $rowref->{'order'} )	{
					$rowref->{'order'} = "- " . $rowref->{'order'};
				}
				if ( $rowref->{'family'} && ( $rowref->{'class'} || $rowref->{'order'} ) )	{
					$rowref->{'family'} = "- " . $rowref->{'family'};
				}
				$rowref->{'parents'} = $hbo->populateHTML("parent_display_row", $rowref);
				$output = qq|<tr><td colspan="5" style="border-top: 1px solid #E0E0E0;"></td></tr>|;
				$output .= $hbo->populateHTML("taxa_display_row", $rowref);
				$output .= $table;
				
				$rowref->{'class_no'}  = ($classification->{'class'}{'taxon_no'} or 1000000);
				$rowref->{'order_no'}  = ($classification->{'order'}{'taxon_no'} or 1000000);
				$rowref->{'family_no'} = ($classification->{'family'}{'taxon_no'} or 1000000);
				$rowref->{'lft'} = ($classification->{'lft'}{'taxon_no'} or 1000000);
				$rowref->{'rgt'} = ($classification->{'rgt'}{'taxon_no'} or 1000000);
			}
    		# otherwise this occurrence has never been reidentified
			else {
	        	# get the classification (by PM): changed 2.4.04 by JA to
		        #  use the occurrence number instead of the taxon name
                if ($rowref->{'taxon_no'}) {
                    # Get parents
				    my $class_hash = TaxaCache::getParents($dbt,[$rowref->{'taxon_no'}],'array_full');
                    my @class_array = @{$class_hash->{$rowref->{'taxon_no'}}};
                    # Get Self as well, in case we're a family indet.
                    my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$rowref->{'taxon_no'}},['taxon_name','common_name','taxon_rank','pubyr']);
                    unshift @class_array , $taxon;
                    $rowref = getClassOrderFamily($dbt,\$rowref,\@class_array);
                    if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
                        $rowref->{'class'} = "unclassified";
                    }
                    $rowref->{'synonym_name'} = getSynonymName($dbt,$rowref->{'taxon_no'},$taxon->{'taxon_name'});
                } else {
                    if ($options{'do_reclassify'}) {
                        $rowref->{'show_classification_select'} = 1;
                        # Give these default values, don't want to pass in possibly undef values to any function or PERL might screw it up
                        my $taxon_name = $rowref->{'genus_name'}; 
                        $taxon_name .= " ($rowref->{'subgenus_name'})" if ($rowref->{'subgenus_name'});
                        $taxon_name .= " $rowref->{'species_name'}";
                        my @all_matches = Taxon::getBestClassification($dbt,$rowref);
                        if (@all_matches) {
                            $are_reclassifications = 1;
                            $rowref->{'classification_select'} = Reclassify::classificationSelect($dbt, $rowref->{$OCCURRENCE_NO},0,1,\@all_matches,$rowref->{'taxon_no'},$taxon_name);
                        }
                    }
                }
				$rowref->{'class_no'} ||= 1000000;
				$rowref->{'order_no'} ||= 1000000;
				$rowref->{'family_no'} ||= 1000000;
				$rowref->{'lft'} ||= 1000000;

				if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
					if ( $options{'do_reclassify'} )	{
						$rowref->{'class'} = qq|<span style="color: red;">unclassified</span>|;
					} else	{
						$rowref->{'class'} = "unclassified";
					}
				}
				if ( $rowref->{'class'} && $rowref->{'order'} )	{
					$rowref->{'order'} = "- " . $rowref->{'order'};
				}
				if ( $rowref->{'family'} && ( $rowref->{'class'} || $rowref->{'order'} ) )	{
					$rowref->{'family'} = "- " . $rowref->{'family'};
				}
				$rowref->{'parents'} = $hbo->populateHTML("parent_display_row", $rowref);
				$output = qq|<tr><td colspan="5" style="border-top: 1px solid #E0E0E0;"></td></tr>|;
				$output .= $hbo->populateHTML("taxa_display_row", $rowref);
			}

	# Clean up abundance values (somewhat messy, but works, and better
	#   here than in populateHTML) JA 10.6.02
			$output =~ s/(>1 specimen)s|(>1 individual)s|(>1 element)s|(>1 fragment)s/$1$2$3$4/g;
	
			$rowref->{'html'} = $output;
			push(@grand_master_list, $rowref);
		}

		# Look at @grand_master_list to see every record has class_no, order_no,
		# family_no,  reference_no, abundance_unit and comments. 
		# If ALL records are missing any of those, don't print the header
		# for it.
		my ($class_nos, $order_nos, $family_nos, $common_names, $lft_nos,
			$reference_nos, $abund_values, $comments) = (0,0,0,0,0,0,0);
		foreach my $row (@grand_master_list) {
			$class_nos++ if($row->{class_no} && $row->{class_no} != 1000000);
			$order_nos++ if($row->{order_no} && $row->{order_no} != 1000000);
			$family_nos++ if($row->{family_no} && $row->{family_no} != 1000000);
			$common_names++ if($row->{common_name});
			$lft_nos++ if($row->{lft} && $row->{lft} != 1000000);
			$reference_nos++ if($row->{reference_no} && $row->{reference_no} != $options{'hide_reference_no'});
			$abund_values++ if($row->{abund_value});
			$comments++ if($row->{comments});
		}
	
        if ($options{'collection_no'}) {
            my $sql = "SELECT c.collection_name,c.country,c.state,concat(i1.eml_interval,' ',i1.interval_name) max_interval, concat(i2.eml_interval,' ',i2.interval_name) min_interval " 
                    . " FROM collections c "
                    . " LEFT JOIN intervals i1 ON c.max_interval_no=i1.interval_no"
                    . " LEFT JOIN intervals i2 ON c.min_interval_no=i2.interval_no"
                    . " WHERE c.collection_no=$options{'collection_no'}";

            my $coll = ${$dbt->getData($sql)}[0];

            # get the max/min interval names
            my $time_place = $coll->{'collection_name'}.": ";
            if ($coll->{'max_interval'} ne $coll->{'min_interval'} && $coll->{'min_interval'}) {
                $time_place .= "$coll->{max_interval} - $coll->{min_interval}";
            } else {
                $time_place .= "$coll->{max_interval}";
            } 
            if ($coll->{'state'} && $coll->{country} eq "United States") {
                $time_place .= ", $coll->{state}";
            } elsif ($coll->{'country'}) {
                $time_place .= ", $coll->{country}";
            } 

        }

        # Taxonomic list header
        $return = "<div class=\"displayPanel\" align=\"left\">\n" .
                  "  <span class=\"displayPanelHeader\">Taxonomic list</span>\n" .
                  "  <div class=\"displayPanelContent\">\n" ;

		if ($new_found) {
            push @warnings, "Taxon names in <b>bold</b> are new to the occurrences table. Please make sure there aren't any typos. If there are, DON'T hit the back button; click the edit link below.";
		}
        if  ($are_reclassifications) {
            push @warnings, "Some taxa could not be classified because multiple versions of the names (such as homonyms) exist in the database.  Please choose which versions you mean and hit \"Classify taxa.\"";
        }

        if (@warnings) {
            $return .= "<div style=\"margin-left: auto; margin-right: auto; text-align: left;\">";
            $return .= Debug::printWarnings(\@warnings);
            $return .= "<br>";
            $return .= "</div>";
        }

        if ($are_reclassifications) {
            $return .= "<form action=\"$WRITE_URL\" method=\"post\">\n";
            $return .= "<input type=\"hidden\" name=\"action\" value=\"startProcessReclassifyForm\">\n"; 
            if ($options{$COLLECTION_NO}) {
                $return .= "<input type=\"hidden\" name=\"$COLLECTION_NO\" value=\"$options{$COLLECTION_NO}\">\n"; 
            }
        }

	$return .= "<table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" class=\"tiny\"><tr>";

	# Sort:
        my @sorted = ();
        if ($options{'occurrence_list'} && @{$options{'occurrence_list'}}) {
            # Should be sorted in SQL using the same criteria as was made to
            # build the occurrence list (in displayOccsForReID)  Right now this is by occurrence_no, which is being done in sql;
            @sorted = @grand_master_list;
        } else {
            # switched from sorting by taxon nos to sorting by lft rgt
            #  JA 13.1.07
            @sorted = sort{ $a->{lft} <=> $b->{lft} ||
                               $a->{rgt} <=> $b->{rgt} ||
                               $a->{$OCCURRENCE_NO} <=> $b->{$OCCURRENCE_NO} } @grand_master_list;
            #@sorted = sort{ $a->{class_no} <=> $b->{class_no} ||
            #                   $a->{order_no} <=> $b->{order_no} ||
            #                   $a->{family_no} <=> $b->{family_no} ||
            #                   $a->{occurrence_no} <=> $b->{occurrence_no} } @grand_master_list;
            unless ( $lft_nos == 0 )	{
            #unless($class_nos == 0 && $order_nos == 0 && $family_nos == 0 )
                # Now sort the ones that had no taxon_no by occ_no.
                my @occs_to_sort = ();
                while ( $sorted[-1]->{lft} == 1000000 )	{
                    push(@occs_to_sort, pop @sorted);
                }

            # Put occs in order, AFTER the sorted occ with the closest smaller
            # number.  First check if our occ number is one greater than any 
            # existing sorted occ number.  If so, place after it.  If not, find
            # the distance between it and all other occs less than it and then
            # place it after the one with the smallest distance.
                while(my $single = pop @occs_to_sort){
                    my $slot_found = 0;
                    my @variances = ();
                    # First, look for the "easy out" at the endpoints.
                    # Beginning?
                # HMM, if $single is less than $sorted[0] we don't want to put
                # it at the front unless it's less than ALL $sorted[$x].
                    #if($single->{occurrence_no} < $sorted[0]->{occurrence_no} && 
                    #	$sorted[0]->{occurrence_no} - $single->{occurrence_no} == 1){
                    #	unshift @sorted, $single;
                    #}
                    # Can I just stick it at the end?
                    if(($single->{$OCCURRENCE_NO} > $sorted[-1]->{$OCCURRENCE_NO}) &&
                       ($single->{$OCCURRENCE_NO} - $sorted[-1]->{$OCCURRENCE_NO} == 1)){
                        push @sorted, $single;
                    }
                    # Somewhere in the middle
                    else{
                        for(my $index = 0; $index < @sorted-1; $index++){
                            if($single->{$OCCURRENCE_NO} > 
                                            $sorted[$index]->{$OCCURRENCE_NO}){ 
                                # if we find a variance of 1, bingo!
                                if($single->{$OCCURRENCE_NO} -
                                        $sorted[$index]->{$OCCURRENCE_NO} == 1){
                                    splice @sorted, $index+1, 0, $single;
                                    $slot_found=1;
                                    last;
                                }
                                else{
                                    # store the (positive) variance
                                    push(@variances, $single->{$OCCURRENCE_NO}-$sorted[$index]->{$OCCURRENCE_NO});
                                }
                            }
                            else{ # negative variance
                                push(@variances, 1000000);
                            }
                        }
                        # if we didn't find a variance of 1, place after smallest
                        # variance.
                        if(!$slot_found){
                            # end variance:
                            if($sorted[-1]->{$OCCURRENCE_NO}-$single->{$OCCURRENCE_NO}>0){
                                push(@variances,$sorted[-1]->{$OCCURRENCE_NO}-$single->{$OCCURRENCE_NO});
                            }
                            else{ # negative variance
                                push(@variances, 1000000);
                            }
                            # insert where the variance is the least
                            my $smallest = 1000000;
                            my $smallest_index = 0;
                            for(my $counter=0; $counter<@variances; $counter++){
                                if($variances[$counter] < $smallest){
                                    $smallest = $variances[$counter];
                                    $smallest_index = $counter;
                                }
                            }
                            # NOTE: besides inserting according to the position
                            # found above, this will insert an occ less than all other
                            # occ numbers at the very front of the list (the condition
                            # in the loop above will never be met, so $smallest_index
                            # will remain zero.
                            splice @sorted, $smallest_index+1, 0, $single;
                        }
                    }
                }
            }
        }

		my $sorted_html = '';
		my $rows = $#sorted + 2;
		$sorted_html .= qq|
<script language="JavaScript" type="text/javascript">
<!-- Begin

window.onload = hideName;

function addLink(link_id,link_action,taxon_name)	{
	if ( ! /href/.test( document.getElementById(link_id).innerHTML ) )	{
		document.getElementById(link_id).innerHTML = '<a href="?a=basicTaxonInfo' + link_action + '&amp;is_real_user=1">' + taxon_name + '</a>';
	}
}

function hideName()	{
	for (var rowNum=1; rowNum<$rows; rowNum++)	{
		document.getElementById('commonRow'+rowNum).style.visibility = 'hidden';
	}
}

function showName()	{
	document.getElementById('commonClick').style.visibility = 'hidden';
	var commonName = document.getElementsByName("commonName");
	for ( i = 0; i<= commonName.length; i++ )       {
		commonName[i].style.visibility = "visible";
	}
	for (var rowNum=1; rowNum<$rows; rowNum++)	{
		document.getElementById('commonRow'+rowNum).style.visibility = 'visible';
	}
}

-->
</script>
|;
		my $lastparents;
		for(my $index = 0; $index < @sorted; $index++){
			# only the last row needs to have the rowNum inserted
			my $rowNum = $index + 1;
			my @parts = split /commonRow/,$sorted[$index]->{html};
			$parts[$#parts] = $rowNum . $parts[$#parts];
			$sorted[$index]->{html} = join 'commonRow',@parts;

#            $sorted[$index]->{html} =~ s/<td align="center"><\/td>/<td>$sorted[$index]->{occurrence_no}<\/td>/; DEBUG
			if ( $sorted[$index]->{'class'} . $sorted[$index]->{'order'} . $sorted[$index]->{'family'} ne $lastparents )	{
				$sorted_html .= $sorted[$index]->{'parents'};
				$lastparents = $sorted[$index]->{'class'} . $sorted[$index]->{'order'} . $sorted[$index]->{'family'};
			}
			$sorted_html .= $sorted[$index]->{html};
            
		}
		$return .= $sorted_html;

		$return .= qq|<tr><td colspan="5" align="right"><span onClick="showName();" id="commonClick" class="small">see common names</span></td>|;

		$return .= "</table>";
        if ($are_reclassifications) {
            $return .= "<br><input type=\"submit\" name=\"submit\" value=\"Classify taxa\">";
            $return .= "</form>"; 
        }

	$return .= "<div class=\"verysmall\">";
	$return .= '<p><div align="center">';

	if ( $options{'collection_no'} > 0 && ! $options{'save_links'} )	{
	# there used to be some links here to rarefyAbundances and
	#  displayCollectionEcology but these are going with Fossilworks
	#  4.6.13 JA

		if ($s->isDBMember()) {
			$return .= qq|<a href="$WRITE_URL?a=displayOccurrenceAddEdit&collection_no=$options{'collection_no'}">Edit taxonomic list</a>|;
			if ( $s->get('role') =~ /authorizer|student|technician/ )	{
				$return .= qq| - <a href="$WRITE_URL?a=displayOccsForReID&collection_no=$options{'collection_no'}">Reidentify taxa</a>|;
			}
		}
	} elsif ($s->isDBMember()) {
		$return .= $options{'save_links'};
	}
	$return .= "</div></p>\n</div>\n";

        $return .= "</div>";
        $return .= "</div>";
	} else {
        if (@warnings) {
            $return .= "<div align=\"center\">";
            $return .= Debug::printWarnings(\@warnings);
            $return .= "<br>";
            $return .= "</div>";
        }
    }


    # This replaces blank cells with blank cells that have no padding, so the don't take up
    # space - this way the comments field lines is indented correctly if theres a bunch of empty
    # class/order/family columns sort of an hack but works - PS
    $return =~ s/<td([^>]*?)>\s*<\/td>/<td$1 style=\"padding: 0\"><\/td>/g;
    #$return =~ s/<td(.*?)>\s*<\/td>/<td$1 style=\"padding: 0\"><\/td>/g;
	return $return;
} # end sub buildTaxonomicList()

sub formatOccurrenceTaxonName {
    my $row = shift;
    my $taxon_name = "";

    # Generate the link first
    my $link_id = $row->{'occurrence_no'};
    if ( $row->{'reid_no'} )	{
        $link_id = "R" . $row->{'reid_no'};
    }
    my $link_action;
    if ( $row->{'taxon_no'} > 0 )	{
        $link_action = $row->{'taxon_no'};
        $link_action = "&amp;taxon_no=" . uri_escape($link_action);
    } elsif ($row->{'genus_name'} && $row->{'genus_reso'} !~ /informal/) {
        $link_action = $row->{'genus_name'};

        if ($row->{'subgenus_name'} && $row->{'subgenus_reso'} !~ /informal/) {
            $link_action .= " ($row->{'subgenus_name'})";
        }
        if ($row->{'species_name'} && $row->{'species_reso'} !~ /informal/ && $row->{'species_name'} !~ /^indet\.|^sp\./) {
            $link_action .= " $row->{'species_name'}";
        }
        $link_action = "&amp;taxon_name=" . uri_escape($link_action);
    }


    if ($row->{'species_name'} !~ /^indet/ && $row->{'genus_reso'} !~ /informal/) {
        $taxon_name .= "<i>";
    }

    my $genus_name = $row->{'genus_name'};
    if ($row->{'new_genus_name'}) {
        $genus_name = "<b>".$genus_name."</b>";
    }
    # n. gen., n. subgen., n. sp. come afterwards
    # sensu lato always goes at the very end no matter what JA 3.3.07
    if ($row->{'genus_reso'} eq 'n. gen.' && $row->{'species_reso'} ne 'n. sp.') {
        $taxon_name .= "$genus_name n. gen.";
    } elsif ($row->{'genus_reso'} eq '"') {
        $taxon_name .= '"'.$genus_name;
        $taxon_name .= '"' unless ($row->{'subgenus_reso'} eq '"' || $row->{'species_reso'} eq '"');
    } elsif ($row->{'genus_reso'} && $row->{'genus_reso'} ne 'n. gen.' && $row->{'genus_reso'} ne 'sensu lato') {
        $taxon_name .= $row->{'genus_reso'}." ".$genus_name;
    } else {
        $taxon_name .= $genus_name;
    }

    if ($row->{'subgenus_name'}) {
        my $subgenus_name = $row->{'subgenus_name'};
        if ($row->{'new_subgenus_name'}) {
            $subgenus_name = "<b>".$subgenus_name."</b>";
        }
        $taxon_name .= " (";
        if ($row->{'subgenus_reso'} eq 'n. subgen.') {
            $taxon_name .= "$subgenus_name n. subgen.";
        } elsif ($row->{'subgenus_reso'} eq '"') {
            $taxon_name .= '"' unless ($row->{'genus_reso'} eq '"');
            $taxon_name .= $subgenus_name;
            $taxon_name .= '"' unless ($row->{'species_reso'} eq '"');
        } elsif ($row->{'subgenus_reso'}) {
            $taxon_name .= $row->{'subgenus_reso'}." ".$subgenus_name;
        } else {
            $taxon_name .= $subgenus_name;
        }
        $taxon_name .= ")";
    }

    $taxon_name .= " ";
    my $species_name = $row->{'species_name'};
    if ($row->{'new_species_name'}) {
        $species_name = "<b>".$species_name."</b>";
    }
    if ($row->{'species_reso'} eq '"') {
        $taxon_name .= '"' unless ($row->{'genus_reso'} eq '"' || $row->{'subgenus_reso'} eq '"');
        $taxon_name .= $species_name.'"';
    } elsif ($row->{'species_reso'} && $row->{'species_reso'} ne 'n. sp.' && $row->{'species_reso'} ne 'sensu lato') {
        $taxon_name .= $row->{'species_reso'}." ".$species_name;
    } else {
        $taxon_name .= $species_name;
    }
    #if ($row->{'species_reso'} ne 'n. sp.' && $row->{'species_reso'}) {
    #    $taxon_name .= " ".$row->{'species_reso'};
    #}
    #$taxon_name .= " ".$row->{'species_name'};

    if ($row->{'species_name'} !~ /^indet/ && $row->{'genus_reso'} !~ /informal/) {
        $taxon_name .= "</i>";
    }
    if ($link_id) {
        $taxon_name =~ s/"/&quot;/g;
        $taxon_name = qq|<span class="mockLink" id="$link_id" onMouseOver="addLink('$link_id','$link_action','$taxon_name')">$taxon_name</span>|;
    }
    
    if ($row->{'genus_reso'} eq 'sensu lato' || $row->{'species_reso'} eq 'sensu lato') {
        $taxon_name .= " sensu lato";
    }
    if ($row->{'species_reso'} eq 'n. sp.') {
        if ($row->{'genus_reso'} eq 'n. gen.') {
            $taxon_name .= " n. gen.,";
        }
        $taxon_name .= " n. sp.";
    }
    if ($row->{'plant_organ'} && $row->{'plant_organ'} ne 'unassigned') {
        $taxon_name .= " $row->{plant_organ}";
    }
    if ($row->{'plant_organ2'} && $row->{'plant_organ2'} ne 'unassigned') {
        $taxon_name .= ", " if ($row->{'plant_organ'} && $row->{'plant_organ'} ne 'unassigned');
        $taxon_name .= " $row->{plant_organ2}";
    }

    return $taxon_name;
}

# This is pretty much just used in a couple places above
sub getSynonymName {
    my ($dbt,$taxon_no,$current_taxon_name) = @_;
    return "" unless $taxon_no;

    my $synonym_name = "";

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    my ($ss_taxon_no,$status) = TaxonInfo::getSeniorSynonym($dbt,$orig_no,'','yes');
    my $is_synonym = ($ss_taxon_no != $orig_no && $status =~ /synonym/) ? 1 : 0;
    my $is_spelling = 0;
    my $spelling_reason = "";

    my $spelling = TaxonInfo::getMostRecentSpelling($dbt,$ss_taxon_no,{'get_spelling_reason'=>1});
    if ($spelling->{'taxon_no'} != $taxon_no && $current_taxon_name ne $spelling->{'taxon_name'}) {
        $is_spelling = 1;
        $spelling_reason = $spelling->{'spelling_reason'};
        $spelling_reason = 'original and current combination' if $spelling_reason eq 'original spelling';
        $spelling_reason = 'recombined as' if $spelling_reason eq 'recombination';
        $spelling_reason = 'corrected as' if $spelling_reason eq 'correction';
        $spelling_reason = 'spelled with current rank as' if $spelling_reason eq 'rank change';
        $spelling_reason = 'reassigned as' if $spelling_reason eq 'reassignment';
        if ( $status =~ /replaced|subgroup|nomen/ )	{
            $spelling_reason = $status;
            if ( $status =~ /nomen/ )	{
                $spelling_reason .= ' belonging to';
            }
        }
    }
    my $taxon_name = $spelling->{'taxon_name'};
    my $taxon_rank = $spelling->{'taxon_rank'};
    if ($is_synonym || $is_spelling) {
        if ($taxon_rank =~ /species|genus/) {
            $synonym_name = "<em>$taxon_name</em>";
        } else { 
            $synonym_name = $taxon_name;
        }
        $synonym_name =~ s/"/&quot;/g;
        if ($is_synonym) {
            $synonym_name = "synonym of <span class=\"mockLink\" id=\"syn$ss_taxon_no\" onMouseOver=\"addLink('syn$ss_taxon_no','&amp;taxon_no=$ss_taxon_no','$synonym_name')\">$synonym_name</span>";
        } else {
            $synonym_name = "$spelling_reason <span class=\"mockLink\" id=\"syn$ss_taxon_no\" onMouseOver=\"addLink('syn$ss_taxon_no','&amp;taxon_no=$ss_taxon_no','$synonym_name')\">$synonym_name</span>";
        }
    }
    return $synonym_name;
}


# Gets an HTML formatted table of reidentifications for a particular taxon
# pass it an occurrence number or reid_no
# the second parameter tells whether it's a reid_no (true) or occurrence_no (false).
sub getReidHTMLTableByOccNum {
	my ($dbt,$hbo,$s,$occNum,$isReidNo,$doReclassify) = @_;

	my $sql = "SELECT genus_reso, genus_name, subgenus_reso, subgenus_name, species_reso, species_name, plant_organ, re.comments as comments, re.reference_no as reference_no,  pubyr, taxon_no, occurrence_no, reid_no, collection_no FROM reidentifications re"
            . " LEFT JOIN refs r ON re.reference_no=r.reference_no ";
	if ($isReidNo) {
		$sql .= " WHERE reid_no = $occNum";
	} else {
		$sql .= " WHERE occurrence_no = $occNum";
	}
    $sql .= " ORDER BY r.pubyr ASC, re.reid_no ASC";
    my @results = @{$dbt->getData($sql)};
	my $html = "";
    my $classification = {};
    my $are_reclassifications = 0;

    # We always get all of them PS
	foreach my $row ( @results ) {
		$row->{'taxon_name'} = "&nbsp;&nbsp;&nbsp;&nbsp;= ".formatOccurrenceTaxonName($row);
        
		# format the reference (PM)
		$row->{'reference_no'} = Reference::formatShortRef($dbt,$row->{'reference_no'},'no_inits'=>1,'link_id'=>1);
       
		# get the taxonomic authority JA 19.4.04
		my $taxon;
		if ($row->{'taxon_no'}) {
			$taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['taxon_no','taxon_name','common_name','taxon_rank','author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority']);

			if ($taxon->{'taxon_rank'} =~ /species/ || $row->{'species_name'} =~ /^indet\.|^sp\./) {
				$row->{'authority'} = Reference::formatShortRef($taxon,'no_inits'=>1,'link_id'=>$taxon->{'ref_is_authority'});
			}
		}

        # Just a default value, so form looks correct
        # JA 2.4.04: changed this so it only works on the most recently published reID
        if ( $row == $results[$#results] )	{
            if ($row->{'taxon_no'}) {
                my $class_hash = TaxaCache::getParents($dbt,[$row->{'taxon_no'}],'array_full');
                my @class_array = @{$class_hash->{$row->{'taxon_no'}}};
                my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['taxon_name','taxon_rank','pubyr']);

		unshift @class_array , $taxon;
                $row = getClassOrderFamily($dbt,\$row,\@class_array);

		# row has the classification now, so stash it
		$classification->{'class'}{'taxon_name'} = $row->{'class'};
		$classification->{'order'}{'taxon_name'} = $row->{'order'};
		$classification->{'family'}{'taxon_name'} = $row->{'family'};

                # Include the taxon as well, it my be a family and be an indet.
                $classification->{$taxon->{'taxon_rank'}} = $taxon;

                $row->{'synonym_name'} = getSynonymName($dbt,$row->{'taxon_no'},$taxon->{'taxon_name'});
                # only $classification is being returned, so piggyback lft and
                #  rgt on it
                # I hate having to hit taxa_tree_cache with a separate SELECT,
                #  but you can't hit it until you already know there's a
                #  taxon_no you can use JA 23.1.07
                my $sql = "SELECT lft,rgt FROM $TAXA_TREE_CACHE WHERE taxon_no=" . $row->{'taxon_no'};
                my $lftrgtref = ${$dbt->getData($sql)}[0];
                $classification->{'lft'}{'taxon_no'} = $lftrgtref->{'lft'};
                $classification->{'rgt'}{'taxon_no'} = $lftrgtref->{'rgt'};
            } else {
                if ($doReclassify) {
                    $row->{'show_classification_select'} = 'YES';
                    my $taxon_name = $row->{'genus_name'}; 
                    $taxon_name .= " ($row->{'subgenus_name'})" if ($row->{'subgenus_name'});
                    $taxon_name .= " $row->{'species_name'}";
                    my @all_matches = Taxon::getBestClassification($dbt,$row);
                    if (@all_matches) {
                        $are_reclassifications = 1;
                        $row->{'classification_select'} = Reclassify::classificationSelect($dbt, $row->{$OCCURRENCE_NO},0,1,\@all_matches,$row->{'taxon_no'},$taxon_name);
                    }
                }
            }
		}
    
		$row->{'hide_collection_no'} = 1;
		$html .= $hbo->populateHTML("taxa_display_row", $row);
	}

	return ($html,$classification,$are_reclassifications);
}

## sub getPaleoCoords
#	Description: Converts a set of floating point coordinates + min/max interval numbers.
#	             determines the age from the interval numbers and returns the paleocoords.
#	Arguments:   $dbh - database handle
#				 $dbt - database transaction object	
#				 $max_interval_no,$min_interval_no - max/min interval no
#				 $f_lngdeg, $f_latdeg - decimal lontitude and latitude
#	Returns:	 $paleolng, $paleolat - decimal paleo longitude and latitutde, or undefined
#                variables if a paleolng/lat can't be found 
#
##
sub getPaleoCoords {
    my $dbt = shift;
    my $max_interval_no = shift;
    my $min_interval_no = shift;
    my $f_lngdeg = shift;
    my $f_latdeg = shift;

    my $dbh = $dbt->dbh;


    # Get time interval information
    my $t = new TimeLookup($dbt);
    my @itvs; 
    push @itvs, $max_interval_no if ($max_interval_no);
    push @itvs, $min_interval_no if ($min_interval_no && $max_interval_no != $min_interval_no);
    my $h = $t->lookupIntervals(\@itvs);

    my ($paleolat, $paleolng,$plng,$plat,$lngdeg,$latdeg,$pid); 
    if ($f_latdeg <= 90 && $f_latdeg >= -90  && $f_lngdeg <= 180 && $f_lngdeg >= -180 ) {
        my $colllowerbound =  $h->{$max_interval_no}{'base_age'};
        my $collupperbound;
        if ($min_interval_no)  {
            $collupperbound = $h->{$min_interval_no}{'top_age'};
        } else {        
            $collupperbound = $h->{$max_interval_no}{'top_age'};
        }
        my $collage = ( $colllowerbound + $collupperbound ) / 2;
        $collage = int($collage+0.5);
        if ($collage <= 600 && $collage >= 0) {
            dbg("collage $collage max_i $max_interval_no min_i $min_interval_no colllowerbound $colllowerbound collupperbound $collupperbound ");

            # Get Map rotation information - needs maptime to be set (to collage)
            # rotx, roty, rotdeg get set by the function, needed by projectPoints below
            my $map_o = new Map;
            $map_o->{maptime} = $collage;
            $map_o->readPlateIDs();
            $map_o->mapGetRotations();

            ($plng,$plat,$lngdeg,$latdeg,$pid) = $map_o->projectPoints($f_lngdeg,$f_latdeg);
            dbg("lngdeg: $lngdeg latdeg $latdeg");
            if ( $lngdeg !~ /NaN/ && $latdeg !~ /NaN/ )       {
                $paleolng = $lngdeg;
                $paleolat = $latdeg;
            } 
        }
    }

    dbg("Paleolng: $paleolng Paleolat $paleolat fx $f_lngdeg fy $f_latdeg plat $plat plng $plng pid $pid");
    return ($paleolng, $paleolat, $pid);
}


## setSecondaryRef($dbt, $collection_no, $reference_no)
# 	Description:	Checks if reference_no is the primary reference or a 
#					secondary reference	for this collection.  If yes to either
#					of those, nothing is done, and the method returns.
#					If the ref exists in neither place, it is added as a
#					secondary reference for the collection.
#
#	Parameters:		$dbh			the database handle
#					$collection_no	the collection being added or edited or the
#									collection to which the occurrence or ReID
#									being added or edited belongs.
#					$reference_no	the reference for the occ, reid, or coll
#									being updated or inserted.	
#
#	Returns:		boolean for running to completion.	
##
sub setSecondaryRef{
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }

	return if(isRefPrimaryOrSecondary($dbt, $collection_no, $reference_no));

	# If we got this far, the ref is not associated with the collection,
	# so add it to the secondary_refs table.
	my $sql = "INSERT IGNORE INTO secondary_refs (collection_no, reference_no) ".
		   "VALUES ($collection_no, $reference_no)";	

    my $dbh_r = $dbt->dbh;
    my $return = $dbh_r->do($sql);
	dbg("ref $reference_no added as secondary for collection $collection_no");
	return 1;
}

## refIsDeleteable($dbt, $collection_no, $reference_no)
#
#	Description		determines whether a reference may be disassociated from
#					a collection based on whether the reference has any
#					occurrences tied to the collection
#
#	Parameters		$dbh			database handle
#					$collection_no	collection to which ref is tied
#					$reference_no	reference in question
#
#	Returns			boolean
#
##
sub refIsDeleteable {
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }
	
	my $sql = "SELECT count(occurrence_no) cnt FROM occurrences ".
			  "WHERE collection_no=$collection_no ".
			  "AND reference_no=$reference_no";
    my $cnt = ${$dbt->getData($sql)}[0]->{'cnt'};

	if($cnt >= 1){
		dbg("Reference $reference_no has $cnt occurrences and is not deletable");
		return 0;
	} else {
		dbg("Reference $reference_no has $cnt occurrences and is deletable");
		return 1;
	}
}

## deleteRefAssociation($dbt, $collection_no, $reference_no)
#
#	Description		Removes association between collection_no and reference_no
#					in the secondary_refs table.
#
#	Parameters		$dbh			database handle
#					$collection_no	collection to which ref is tied
#					$reference_no	reference in question
#
#	Returns			boolean
#
##
sub deleteRefAssociation {
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }

	my $sql = "DELETE FROM secondary_refs where collection_no=$collection_no AND reference_no=$reference_no";
    dbg("Deleting secondary ref association $reference_no from collection $collection_no");
    my $dbh_r = $dbt->dbh;
    my $return = $dbh_r->do($sql);
	return 1;
}

## isRefPrimaryOrSecondary($dbt, $collection_no, $reference_no)
#
#	Description	Checks the collections and secondary_refs tables to see if
#				$reference_no is either the primary or secondary reference
#				for $collection
#
#	Parameters	$dbh			database handle
#				$collection_no	collection with which ref may be associated
#				$reference_no	reference to check for association.
#
#	Returns		positive value if association exists (1 for primary, 2 for
#				secondary), or zero if no association currently exists.
##	
sub isRefPrimaryOrSecondary{
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    my $dbh = $dbt->dbh;

	# First, see if the ref is the primary.
	my $sql = "SELECT reference_no from collections WHERE collection_no=$collection_no";

    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my %results = %{$sth->fetchrow_hashref()};
    $sth->finish();

	# If the ref is the primary, nothing need be done.
	if($results{reference_no} == $reference_no){
		dbg("ref $reference_no exists as primary for collection $collection_no");
		return 1;
	}

	# Next, see if the ref is listed as a secondary
	$sql = "SELECT reference_no from secondary_refs ".
			  "WHERE collection_no=$collection_no";

    $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results = @{$sth->fetchall_arrayref({})};
    $sth->finish();

	# Check the refs for a match
	foreach my $ref (@results){
		if($ref->{reference_no} == $reference_no){
		    dbg("ref $reference_no exists as secondary for collection $collection_no");
			return 2;
		}
	}

	# If we got this far, the ref is neither primary nor secondary
	return 0;
}


1;

