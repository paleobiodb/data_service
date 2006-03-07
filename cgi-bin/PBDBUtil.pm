package PBDBUtil;
use strict;

# This contains various miscellaneous functions that don't belong anywhere
# else or haven't been moved out yet
my $DEBUG = 0;

## debug($level, $message)
# 	Description:	print out diagnostic messages according to severity,
#			as determined by $level.
#	Parameters:	$level - debugging level
#			$message - message to print
##
sub debug{
    my $level = shift;
    my $message = shift;

    if(($level <= $DEBUG) && $message){ 
	    print "<font color='green'>$message</font><BR>\n";
    }

    return $DEBUG;
}

## getResearchGroupSQL($dbt, $research_group)
# 	Description:    Returns an SQL snippet to filter all collections corresponding to a project or research group
#                   Assumes that the secondary_refs table has been left joined against the collections table
#	Parameters:	$dbt - DBTransactionManager object
#				$research_group - can be a research_group, project_name, or both
#               $restricted_to - boolean (default 0) - if st to 1 and input is a research group,
#                   restrict so it includes collections that belong to that research group and it alone
#                   not collections that might belong to it and others
#	Returns:	SQL snippet, to be appended with AND
##
sub getResearchGroupSQL {
	my $dbt = shift;
	my $research_group = shift;
    my $restricted_to = shift;

    my @terms = ();
    if($research_group =~ /^(?:decapod|ETE|5%|1%|PACED|PGAP)$/){
        my $sql = "SELECT reference_no FROM refs WHERE ";
        if ($restricted_to) {
            $sql .= " FIND_IN_SET(".$dbt->dbh->quote($research_group).",project_name)";
        } else {
            $sql .= " project_name=".$dbt->dbh->quote($research_group);
        }
        my @results = @{$dbt->getData($sql)};
        my $refs = join(", ",map {$_->{'reference_no'}} @results);
        $refs = '-1' if (!$refs );
        if ($restricted_to) {
            # In the restricted to case the collections research group is only looked
            # at for these overlapping cases
            if ($research_group !~ /^(?:decapod|ETE|PACED)$/) {
                push @terms, "c.reference_no IN ($refs)";
            }
        } else {
            push @terms, "c.reference_no IN ($refs)";
            push @terms, "sr.reference_no IN ($refs)";
        }
    } 
    if($research_group =~ /^(?:decapod|ETE|marine invertebrate|micropaleontology|PACED|paleobotany|paleoentomology|taphonomy|vertebrate)$/) {
        if ($restricted_to) {
            push @terms, "c.research_group=".$dbt->dbh->quote($research_group);
        } else {
            push @terms, "FIND_IN_SET( ".$dbt->dbh->quote($research_group).", c.research_group ) ";
        }
    } 

    my $sql_terms;
    if (@terms) {
        $sql_terms = "(".join(" OR ",@terms).")";  
    }
    return $sql_terms;
}

## getSecondaryRefsString($dbh, $collection_no, $selectable, $deletable)
# 	Description:	constructs table rows of refs record data including
#					reference_no, reftitle, author info, pubyr and authorizer
#					and enterer.
#
#	Parameters:		$dbh			database handle
#					$collection_no	the collection number to which the 
#									references pertain.
#					$selectable		make this ref selectable (display a radio
#									button)	
#					$deletable		make this ref deletable (display a check
#									box)	
#
#	Returns:		table rows
##
sub getSecondaryRefsString{
    my $dbh = shift;
    my $collection_no = shift;
	my $selectable = shift;
	my $deletable = shift;
	
    my $sql = "SELECT refs.reference_no, refs.author1init, refs.author1last, ".
			  "refs.author2init, refs.author2last, refs.otherauthors, ".
              "refs.pubyr, refs.reftitle, refs.pubtitle, refs.pubvol, ".
			  "refs.pubno, refs.firstpage, refs.lastpage, refs.project_name ".
              "FROM refs, secondary_refs ".
              "WHERE refs.reference_no = secondary_refs.reference_no ".
              "AND secondary_refs.collection_no = $collection_no ".
			  "ORDER BY author1last, author1init, author2last, pubyr";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results = @{$sth->fetchall_arrayref({})};
    $sth->finish();
	unless(scalar @results > 0){ 
		return "";
	}

	# Authorname Formatting
	use AuthorNames;

	my $result_string = "<table border=0 cellpadding=2 cellspacing=0 width=\"100%\">";
	# Format each row from the database as a table row.
	my $row_color = 0;
    foreach my $ref (@results){
		# add in a couple of single-space cells around the reference_no
		# to match the formatting of Reference from BiblioRef.pm
		if($row_color % 2){
			$result_string .="<tr>";
		} else{
			$result_string .= "<tr class=\"darkList\">";
		}
		if($selectable){
			$result_string .= "<td width=\"1%\" valign=top><input type=radio name=secondary_reference_no value=" . $ref->{reference_no} . "></td>\n";
		}

		# Get all the authornames for formatting
		my %temp = ('author1init',$ref->{author1init},
					'author1last',$ref->{author1last},
					'author2init',$ref->{author2init},
					'author2last',$ref->{author2last},
					'otherauthors',$ref->{otherauthors}
					);
		my $an = AuthorNames->new(\%temp);

		$result_string .= "<td valign=top width=\"1%\"><b>$ref->{reference_no}</b></td>";

		if($ref->{project_name}){
			$result_string .= "<td width=\"1%\" valign=top><span style=\"color: red;\"".
							  ">&nbsp;$ref->{project_name}&nbsp;</span></td>";
		} else {
			$result_string .= "<td width=\"1%\" valign=top></td>";
        }

		$result_string .= "<td valign=top width=\"93%\">". $an->toString().".&nbsp;$ref->{pubyr}.&nbsp;";

		if($ref->{reftitle}){
			$result_string .= "$ref->{reftitle}.";
		}
		if($ref->{pubtitle}){
			$result_string .="&nbsp;<i>$ref->{pubtitle}</i>&nbsp;";
		}

		$result_string .= "<b>";

		if($ref->{pubvol}){
			$result_string .= "$ref->{pubvol}";
		}
		if($ref->{pubno}){
			 $result_string .= "($ref->{pubno})";
		}

		$result_string .= "</b>";

		if($ref->{firstpage}){
			$result_string .= ":$ref->{firstpage}";
		}
		if($ref->{lastpage}){
			$result_string .= "-$ref->{lastpage}";
		}

		$result_string .= "</td></tr>";
					
		# put in a checkbox for deletion if no occs with this ref are tied
		# to the collection
		if($deletable && refIsDeleteable($dbh,$collection_no,$ref->{reference_no})){
			if($row_color % 2){
				$result_string .= "<tr>";
			}	
			else{
				$result_string .= "<tr class='darkList'>";
			}
			$result_string .= "<td style=\"background-color:red;\"><input type=checkbox name=delete_ref value=$ref->{reference_no}></td><td colspan=\"3\">remove&nbsp;</td></tr>\n";
		}
        print "</tr>";
		$row_color++;
    }
	$sth->finish();
	$result_string .= "</table>";
	return $result_string;
}

## setSecondaryRef($dbh, $collection_no, $reference_no)
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
	my $dbh = shift;
	my $collection_no = shift;
	my $reference_no = shift;

	return if(isRefSecondary($dbh, $collection_no, $reference_no));

	# If we got this far, the ref is not associated with the collection,
	# so add it to the secondary_refs table.
	my $sql = "INSERT INTO secondary_refs (collection_no, reference_no) ".
		   "VALUES ($collection_no, $reference_no)";	
    my $sth = $dbh->prepare($sql);
    if($sth->execute() != 1){
		print "<font color=\"FF0000\">Failed to create secondary reference ".
			  "for collection $collection_no and reference $reference_no.<br>".
			  "Please notify the database administrator with this message.".
			  "</font><br>";
	}
	debug(1,"ref $reference_no added as secondary for collection $collection_no");
	return 1;
}

## refIsDeleteable($dbh, $collection_no, $reference_no)
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
sub refIsDeleteable{
	my $dbh = shift;
	my $collection_no = shift;
	my $reference_no = shift;
	
	my $sql = "SELECT count(occurrence_no) FROM occurrences ".
			  "WHERE collection_no=$collection_no ".
			  "AND reference_no=$reference_no";

	debug(1,"isDeleteable sql: $sql");
	my $sth = $dbh->prepare($sql) or print "SQL failed to prepare: $sql<br>";
	$sth->execute();
	my @rows = @{$sth->fetchall_arrayref({})};
	my %res = %{$rows[0]};
	my $num = $res{'count(occurrence_no)'};
	$sth->finish();
	if($num >= 1){
		debug(1,"Reference $reference_no has $num occurrences and is not deletable");
		return 0;
	}
	else{
		debug(1,"Reference $reference_no has $num occurrences and IS deletable");
		return 1;
	}
}

## deleteRefAssociation($dbh, $collection_no, $reference_no)
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
sub deleteRefAssociation{
	my $dbh = shift;
	my $collection_no = shift;
	my $reference_no = shift;

	my $sql = "DELETE FROM secondary_refs where collection_no=$collection_no ".
			  "AND reference_no=$reference_no";
	my $sth = $dbh->prepare($sql) or print "SQL failed to prepare: $sql<br>";
	my $res = $sth->execute();
	debug(1,"execute returned:$res.");
    if($res != 1){
		print "<font color=\"FF0000\">Failed to delete secondary ref for".
			  "collection $collection_no and reference $reference_no.<br>".
			  "Return code:$res<br>".
			  "Please notify the database administrator with this message.".                  "</font><br>";
		return 0;
	}
	$sth->finish();
	return 1;
}

## isRefPrimaryOrSecondary($dbh, $collection_no, $reference_no)
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
	my $dbh = shift;
	my $collection_no = shift;
	my $reference_no = shift;

	# First, see if the ref is the primary.
	my $sql = "SELECT reference_no from collections ".
			  "WHERE collection_no=$collection_no";

    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my %results = %{$sth->fetchrow_hashref()};
    $sth->finish();

	# If the ref is the primary, nothing need be done.
	if($results{reference_no} == $reference_no){
		debug(1,"ref $reference_no exists as primary for collection $collection_no");
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
		debug(1,"ref $reference_no exists as secondary for collection $collection_no");
			return 2;
		}
	}

	# If we got this far, the ref is neither primary nor secondary
	return 0;
}

## isRefSecondary($dbh, $collection_no, $reference_no)
#
#	Description	Checks the secondary_refs tables to see if
#				$reference_no is a secondary reference for $collection
#
#	Parameters	$dbh			database handle
#				$collection_no	collection with which ref may be associated
#				$reference_no	reference to check for association.
#
#	Returns		boolean
##	
sub isRefSecondary{
	my $dbh = shift;
	my $collection_no = shift;
	my $reference_no = shift;

	# Next, see if the ref is listed as a secondary
	my $sql = "SELECT reference_no from secondary_refs ".
			  "WHERE collection_no=$collection_no";

    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results = @{$sth->fetchall_arrayref({})};
    $sth->finish();

	# Check the refs for a match
	foreach my $ref (@results){
		if($ref->{reference_no} == $reference_no){
		debug(1,"ref $reference_no exists as secondary for collection $collection_no");
			return 1;
		}
	}

	# Not in secondary_refs table
	return 0;
}

## sub newTaxonNames
#	Description:	checks whether each of the names given to it are
#					currently in the database, returning an array of those
#					that aren't.
#
#	Arguments:		$dbh		database handle
#					$names		reference to an array of genus_names
#					$type		'genus_name', 'species_name' or 'subgenus_name'
#
#	Returns:		Array of names NOT currently in the database.
#
##
sub newTaxonNames{
	my $dbh = shift;
	my $names = shift;
	my $type = shift;

	my @names = @{$names};
	my @result = ();
	
	# This should be 'genus_name', 'species_name' or 'subgenus_name'
	$type .= '_name';

	# put each string in single quotes for the query
	foreach my $single (@names){
		$single = "\'$single\'";
	}
	my $sql = "SELECT count($type), $type FROM occurrences ".
			  "WHERE $type IN (".join(',',@names).") GROUP BY $type";
	my $sth = $dbh->prepare($sql) or die "Failure preparing sql: $sql ($!)";
	$sth->execute();
	my @res = @{$sth->fetchall_arrayref({})};
	$sth->finish();

	# remove the single quotes for comparison
	foreach my $single (@names){
		$single =~ /^'(.*)?'$/;
		$single = $1;
	}
	
	NAME:
	foreach my $check (@names){
		foreach my $check_res (@res){ 
			next NAME if(uc($check_res->{$type}) eq uc($check));
		}
		push(@result, $check);
	}
	
	return @result;
}

# Pass in a taxon_no and this function returns all taxa that are  a part of that taxon_no, recursively
# This function isn't meant to be called itself but is a recursive utility function for taxonomic_search
# deprecated, see taxonomic_search
sub new_search_recurse {
    # Start with a taxon_name:
    my $dbt = shift;
    my $passed = shift;
    my $parent_no = shift;
    my $parent_child_spelling_no = shift;
	$passed->{$parent_no} = 1 if ($parent_no);
	$passed->{$parent_child_spelling_no} = 1 if ($parent_child_spelling_no);
    return if (!$parent_no);

    # Get the children
    my $sql = "SELECT DISTINCT child_no FROM opinions WHERE parent_no=$parent_no";
    my @results = @{$dbt->getData($sql)};

    #my $debug_msg = "";
    if(scalar @results > 0){
        # Validate all the children
        foreach my $child (@results){
			# Don't revisit same child. Avoids loops in data structure, and speeds things up
            if (exists $passed->{$child->{child_no}}) {
                #print "already visited $child->{child_no}<br>";
                next;    
            }
            # (the taxon_nos in %$passed will always be original combinations since orig. combs always have all the belongs to links)
            my $parent_row = TaxonInfo::getMostRecentParentOpinion($dbt, $child->{'child_no'});

            if($parent_row->{'parent_no'} == $parent_no){
                my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE status IN  ('rank changed as','recombined as','corrected as') AND child_no=$child->{'child_no'}";
                my @results = @{$dbt->getData($sql)}; 
                foreach my $row (@results) {
                    if ($row->{'child_spelling_no'}) {
                        $passed->{$row->{'child_spelling_no'}}=1;
                    }
                }
                undef @results;
                new_search_recurse($dbt,$passed,$child->{'child_no'},$child->{'child_spelling_no'});
            } 
        }
    } 
}

##
# Recursively find all taxon_nos or genus names belonging to a taxon
# deprecated PS 10/10/2005 - use TaxaCache::getChildren instead
##
sub taxonomic_search{
	my $dbt = shift;
	my $taxon_name_or_no = (shift or "");
    my $taxon_no;

    # We need to resolve it to be a taxon_no or we're done    
    if ($taxon_name_or_no =~ /^\d+$/) {
        $taxon_no = $taxon_name_or_no;
    } else {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$taxon_name_or_no);
        if (scalar(@taxon_nos) == 1) {
            $taxon_no = $taxon_nos[0];
        }       
    }
    if (!$taxon_no) {
        return wantarray ? (-1) : "-1"; # bad... ambiguous name or none
    }
    # Make sure its an original combination
    $taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);

    my $passed = {};
    
    # get alternate spellings of focal taxon. all alternate spellings of
    # children will be found by the new_search_recurse function
    my $sql = "SELECT child_spelling_no FROM opinions WHERE status IN ('recombined as','corrected as','rank changed as') AND child_no=$taxon_no";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        $passed->{$row->{'child_spelling_no'}} = 1 if ($row->{'child_spelling_no'});
    }

    # get all its children
	new_search_recurse($dbt,$passed,$taxon_no);

    return (wantarray) ? keys(%$passed) : join(', ', keys(%$passed));
}


sub getMostRecentReIDforOcc{
	my $dbt = shift;
	my $occ = shift;
	my $returnTheRef = shift;

    my $sql = "SELECT re.genus_name, re.species_name, re.collection_no, re.reid_no, r.pubyr, re.created FROM reidentifications re, refs r WHERE r.reference_no=re.reference_no AND re.occurrence_no=".int($occ)." ORDER BY r.pubyr DESC, re.reid_no DESC LIMIT 1";  

	my @results = @{$dbt->getData($sql)};

	if(scalar @results < 1){
		return "";
	} else {
		if($returnTheRef) {
			return $results[0];
		} else {
			return $results[0]->{'reid_no'};
		}
	}
}

sub authorAndPubyrFromTaxonNo {
	my $dbt = shift;
	my $taxon_no = shift;
	my %return_vals = ();

    my $sql = "SELECT taxon_name, author1last, author2last, otherauthors, pubyr, reference_no, ".
              "ref_is_authority FROM authorities WHERE taxon_no=$taxon_no";
    my @auth_rec = @{$dbt->getData($sql)};
    # Get ref info from refs if 'ref_is_authority' is set
    if($auth_rec[0]->{ref_is_authority} =~ /YES/i){
        PBDBUtil::debug(2,"author and year from refs");
        if($auth_rec[0]->{reference_no}){
			$sql = "SELECT reference_no, author1last, author2last, otherauthors, pubyr FROM refs ".
				   "WHERE reference_no=".$auth_rec[0]->{reference_no};
			my @results = @{$dbt->getData($sql)};
			$return_vals{author1last} = $results[0]->{author1last};
			if ( $results[0]->{otherauthors} )	{
				$return_vals{author1last} .= " et al.";
			} elsif ( $results[0]->{author2last} )	{
				$return_vals{author1last} .= " and " . $results[0]->{author2last};
			}
			$return_vals{pubyr} = $results[0]->{pubyr};
        }
        $return_vals{'ref_is_authority'} = 'YES';
    }
    # If ref_is_authority is not set, use the authorname and pubyr in this
    # record.
    elsif($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
        PBDBUtil::debug(2,"author and year from authorities");
        $return_vals{author1last} = $auth_rec[0]->{author1last};
        if ( $auth_rec[0]->{otherauthors} )	{
            $return_vals{author1last} .= " et al.";
        } elsif ( $auth_rec[0]->{author2last} )	{
            $return_vals{author1last} .= " and " . $auth_rec[0]->{author2last};
        }
        $return_vals{pubyr} = $auth_rec[0]->{pubyr};
    }
    $return_vals{'reference_no'} = $auth_rec[0]->{'reference_no'};
	# This could be empty, so it's up to the caller to test the return vals.
	return \%return_vals;
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
    my $dbh = shift;
    my $dbt = shift;
    my $max_interval_no = shift;
    my $min_interval_no = shift;
    my $f_lngdeg = shift;
    my $f_latdeg = shift;

    use TimeLookup;
    use Map;    

    # Get time interval information
    @_ = TimeLookup::findBoundaries($dbh,$dbt);
    my %upperbound = %{$_[0]};
    my %lowerbound = %{$_[1]};
 

    my ($paleolat, $paleolng,$rx,$ry,$pid); 
    if ($f_latdeg <= 90 && $f_latdeg >= -90  && $f_lngdeg <= 180 && $f_lngdeg >= -180 ) {
        my $colllowerbound =  $lowerbound{$max_interval_no};
        my $collupperbound;
        if ($min_interval_no)  {
            $collupperbound = $upperbound{$min_interval_no};
        } else {        
            $collupperbound = $upperbound{$max_interval_no};
        }
        my $collage = ( $colllowerbound + $collupperbound ) / 2;
        $collage = int($collage+0.5);
        main::dbg("collage $collage max_i $max_interval_no min_i $min_interval_no colllowerbound $colllowerbound collupperbound $collupperbound ");

        # Get Map rotation information - needs maptime to be set (to collage)
        # rotx, roty, rotdeg get set by the function, needed by projectPoints below
        my $map_o = new Map;
        $map_o->{maptime} = $collage;
        $map_o->mapGetRotations();

        my ($lngdeg, $latdeg);
        ($lngdeg,$latdeg,$rx,$ry,$pid) = $map_o->projectPoints($f_lngdeg,$f_latdeg);
        main::dbg("lngdeg: $lngdeg latdeg $latdeg");
        if ( $lngdeg ne "NaN" && $latdeg ne "NaN" )       {
            $paleolng = $lngdeg;
            $paleolat = $latdeg;
        } 
    }

    main::dbg("Paleolng: $paleolng Paleolat $paleolat fx $f_lngdeg fy $f_latdeg rx $rx ry $ry pid $pid");
    return ($paleolng, $paleolat);
}

# Gets the childen of a taxon, sorted/output in various fashions
# Algorithmically, this behaves more or less identically to taxonomic_search,
# except its slower since it can potentially return much more data and is much more flexible
# Data is kept track of internally in a tree format. Additional data is kept track of as well
#  -- Alternate spellings get stored in a "spellings" field
#  -- Synonyms get stored in a "synonyms" field
# Separated 01/19/2004 PS. 
#  Inputs:
#   * 1st arg: $dbt
#   * 2nd arg: taxon name or taxon number
#   * 3nd arg: max depth: no of iterations to go down
#   * 4th arg: what we want the data to look like. possible values are:
#       tree: a tree-like data structure, more general and the format used internally
#       sort_hierarchical: an array sorted in hierarchical fashion, suitable for PrintHierarchy.pm
#       sort_alphabetical: an array sorted in alphabetical fashion, suitable for TaxonInfo.pm or Confidence.pm
# 
#  Outputs: an array of hash (record) refs
#    See 'my %new_node = ...' line below for what the hash looks like
sub getChildren {
    my $dbt = shift; 
    my $taxon_name_or_no = shift;
    my $max_depth = (shift || 1);
    my $return_type = (shift || "sort_hierarchical");

    # We need to resolve it to be a taxon_no or we're done
    my $taxon_no;
    if ($taxon_name_or_no =~ /^\d+$/) {
        $taxon_no = $taxon_name_or_no;
    } else {
        my @taxon_nos = TaxonInfo::getTaxonNos($taxon_no);
        if (scalar(@taxon_nos) == 1) {
            $taxon_no = $taxon_name_or_no;
        }    
    }
    if (!$taxon_no) {
        return undef; # bad... ambiguous name or none
    } 
    
    # described above, return'd vars
    my $tree_root = {'taxon_no'=>$taxon_no, 'taxon_name'=>'ROOT','children'=>[]};

    # The sorted records are sorted in a hierarchical fashion suitable for passing to printHierachy
    my @sorted_records = ();
    getChildrenRecurse($dbt, $tree_root, $max_depth, 1, \@sorted_records);
    #pop (@sorted_records); # get rid of the head
   
    if ($return_type eq 'tree') {
        return $tree_root;
    } elsif ($return_type eq 'sort_alphabetical') {
        @sorted_records = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @sorted_records;
        return \@sorted_records;
    } else { # default 'sort_hierarchical'
        return \@sorted_records;
    }
   
}

sub getChildrenRecurse { 
    my $dbt = shift;
    my $node = shift;
    my $max_depth = shift;
    my $depth = shift;
    my $sorted_records = shift;
    my $parent_is_synonym = (shift || 0);
    
    return if (!$node->{'taxon_no'});

    # find all children of this parent, do a join so we can do an order by on it
    my $sql = "SELECT DISTINCT child_no FROM opinions, authorities WHERE opinions.child_spelling_no=authorities.taxon_no AND parent_no=$node->{taxon_no} ORDER BY taxon_name";
    my @children = @{$dbt->getData($sql)};
    
    # Create the children and add them into the children array
    for my $row (@children) {
        # (the taxon_nos will always be original combinations since orig. combs always have all the belongs to links)
        # go back up and check each child's parent(s)
        my $parent_row = TaxonInfo::getMostRecentParentOpinion($dbt,$row->{'child_no'},1); 
        if ($parent_row->{'parent_no'}==$node->{'taxon_no'})	{
            # Get alternate spellings
            my $sql = "SELECT DISTINCT taxon_no, taxon_name, taxon_rank FROM opinions, authorities ".
                      "WHERE opinions.child_spelling_no=authorities.taxon_no ".
                      "AND child_no=$parent_row->{child_no} ".
                      "AND child_spelling_no!=$parent_row->{child_spelling_no} ".
                      "ORDER BY taxon_name"; 
            my $spellings= $dbt->getData($sql);

            # Create the node for the new child - note its taxon_no is always the original combination,
            # but its name/rank are from the corrected name/recombined name
            my $new_node = {'taxon_no'=>$parent_row->{'child_no'}, 
                            'taxon_name'=>$parent_row->{'child_name'},
                            'taxon_rank'=>$parent_row->{'child_rank'},
                            'depth'=>$depth,
                            'children'=>[],
                            'spellings'=>$spellings,
                            'synonyms'=>[]};
          
            # Populate the new node and place it in its right place
            if ( $parent_row->{'status'} =~ /^(?:bel|rec|cor|ran)/o ) {
                return if ($depth > $max_depth);
                # Hierarchical sort, in depth first order
                push @$sorted_records, $new_node if (!$parent_is_synonym);
                getChildrenRecurse($dbt,$new_node,$max_depth,$depth+1,$sorted_records);
                push @{$node->{'children'}}, $new_node;
            } elsif ($parent_row->{'status'} =~ /^(?:subj|homo|obje|repl)/o) {
                getChildrenRecurse($dbt,$new_node,$max_depth,$depth,$sorted_records,1);
                push @{$node->{'synonyms'}}, $new_node;
            }
        }
    }

    if (0) {
    print "synonyms for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'synonyms'}}); 
    print "\n<br>";
    print "spellings for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'spellings'}}); 
    print "\n<br>";
    print "children for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'children'}}); 
    print "\n<br>";
    }
}

# Generation of filenames standardized here to avoid security issues or
# potential weirdness. PS 3/6/2006
# If filetype == 1, use date/pid in randomizing filename.  Else use the ip
# Generally filetype == 1 is good, unless the files need to stick around and
# be reused for some reason (like in the download script)
sub getFilename {
    my $enterer = shift;
    my $filetype = shift;

    my $filename = "";
    if ($enterer eq '' || !$enterer) {
        if ($filetype == 1) {
            #  0    1    2     3     4    5     6     7     8
            my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
            my $date = sprintf("%d%02d%02d",($year+1900),$mon,$mday);
            $filename = "guest_".$date."_".$$;
        } else {
            my $ip = $ENV{'REMOTE_ADDR'}; 
            $ip =~ s/\./_/g;
            #my @bits = split(/\./,$ip);
            #my $longip = ($bits[0] << 24) | ($bits[1] << 16) | ($bits[2] << 8) | ($bits[3]);
            $filename = "guest_".$ip;
        }
    } else {
        #$enterer =~ s/['-]+/_/g;
        $enterer =~ s/[^a-zA-Z0-9_]//g;
        if (length($enterer) > 30) {
            $enterer = substr($enterer,0,30);
        }
        $filename = $enterer;
    }
    return $filename;
}

# pass this a number like "5" and it will return the name ("five").
# only works for numbers up through 19.  Above that and it will just return
# the original number.
#
sub numberToName {
    my $num = shift;

    my %numtoname = (  "0" => "zero", "1" => "one", "2" => "two",
                         "3" => "three", "4" => "four", "5" => "five",
                         "6" => "six", "7" => "seven", "8" => "eight",
                         "9" => "nine", "10" => "ten",
                         "11" => "eleven", "12" => "twelve", "13" => "thirteen",
                         "14" => "fourteen", "15" => "fifteen", "16" => "sixteen",
                         "17" => "seventeen", "18" => "eighteen", "19" => "nineteen");

    my $name;

    if ($num < 20) {
        $name = $numtoname{$num};
    } else {
        $name = $num;
    }

    return $name;
}   



# pass it an array ref and a scalar
# loops through the array to see if the scalar is a member of it.
# returns true or false value.
sub isIn {
    my $arrayRef = shift;
    my $val = shift;

    # if they don't exist
    if ((!$arrayRef) || (!$val)) {
        return 0;
    }

    foreach my $k (@$arrayRef) {
        if ($val eq $k) {
            return 1;
        }
    }

    return 0;
}
    

# Pass this an array ref and an element to delete.
# It returns a reference to a new array but *doesn't* modify the original.
# Does a string compare (eq)
sub deleteElementFromArray {
    my $ref = shift; 
    my $toDelete = shift;
    
    my @newArray;
    
    foreach my $element (@$ref) {
        if ($element ne $toDelete) {
            push(@newArray, $element);
        }
    }
    
    return \@newArray;
}   


# Utilitiy, no other place to put it PS 01/26/2004
sub printErrors{
    if (scalar(@_)) {
        my $plural = (scalar(@_) > 1) ? "s" : "";
        print "<br><div align=center><table width=600 border=0>" .
              "<tr><td class=darkList><font size='+1'><b> Error$plural</b></font></td></tr>" .
              "<tr><td>";
        print "<li class='medium'>$_</li>" for (@_);
        print "</td></tr></table></div><br>";
    }
}

1;
