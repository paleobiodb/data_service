package PBDBUtil;

use Globals;

### NOTE: SET UP EXPORTER AND EXPORT SUB NAMES.

# This package contains a collection of methods that are universally 
# useful to the pbdb codebase.
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

## getResearchProjectRefsStr($dbh, $q)
# 	Description:	returns a list of reference_no's from the refs table which
#					belong to a particular research project (not group).
#
#	Parameters:	$dbh - data base handle
#				$q	 - query object
#
#	Returns:	comma separated list of reference numbers, or empty string.
##
sub getResearchProjectRefsStr{
	my $dbh = shift;
	my $q   = shift;

    my $reflist = "";

    if ( $q->param('research_group') =~ /(^decapod$)|(^EJECT$)|(^ETE$)|(^5%$)|(^1%$)|(^PACED$)|(^PGAP$)/ ) {
        $sql = "SELECT reference_no FROM refs WHERE project_name LIKE '%";
        $sql .= $q->param('research_group') . "%'";

        my $sth = $dbh->prepare($sql);
        $sth->execute();
        my @refrefs = @{$sth->fetchall_arrayref()};
        $sth->finish();

        for $refref (@refrefs)  {
            $reflist .= "," . ${$refref}[0];
        }
        if ($reflist) {
            $reflist =~ s/^,//;
        } else {
            # in case of an empty list
            $reflist = "0";
        }    
    }
	return $reflist;
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

	my $result_string = "<table border=0 cellpadding=8 cellspacing=0 width=\"100%\"><tr><td width=\"100%\">";
	# Format each row from the database as a table row.
	my $row_color = 0;
    foreach my $ref (@results){
		# add in a couple of single-space cells around the reference_no
		# to match the formatting of Reference from BiblioRef.pm
		if($row_color % 2){
			$result_string .="<table border=0 cellpadding=0 cellspacing=0 width=\"100%\"><tr width=\"100%\">";
		}
		else{
			$result_string .= "<table border=0 cellpadding=0 cellspacing=0 width=\"100%\"><tr class='darkList' width=\"100%\">";
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

		$result_string .= "<td valign=top width=\"7%\"><small>".
						  "<b>$ref->{reference_no}</b></small></td>";

		if($ref->{project_name}){
			$result_string .= "<td width=\"1%\" valign=top><font color=\"red\"".
							  ">&nbsp;$ref->{project_name}&nbsp;</font></td>";
		}

		$result_string .= "<td rowspan=2 valign=top width=\"93%\">".
						  "<small>".$an->toString().
						  ".&nbsp;$ref->{pubyr}.&nbsp;";

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
			$result_string .= "<td bgcolor=red><input type=checkbox name=delete_ref value=$ref->{reference_no}></td><td><span class=tiny>remove&nbsp;</span></td></tr>\n";
		}
		$result_string .= "</table>\n";
		$row_color++;
    }
	$sth->finish();
	$result_string .= "</td></tr></table>";
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
	$sql = "INSERT INTO secondary_refs (collection_no, reference_no) ".
		   "VALUES ($collection_no, $reference_no)";	
    $sth = $dbh->prepare($sql);
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
			  "Return code:$res.<br>".
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

##
#
#
##
sub new_search_recurse{
    # Start with a taxon_name:
    my $seed_no = shift;
    my $dbt = shift;
	my $first_time = shift;
    my $sql = "";
    my @results = ();
    #my $validated_list = $seed_no;
	$passed{$seed_no} = 1;

    #print "\nPrimary seed: $seed_no";

    # select for taxon_no of seed in authorities:
    $sql = "SELECT taxon_no, child_no, opinions.pubyr, ".
		   "opinions.reference_no, opinion_no ".
           "FROM authorities, opinions ".
           "WHERE taxon_no = parent_no ".
           "AND taxon_no=$seed_no";
    @results = @{$dbt->getData($sql)};

    if(scalar @results > 0){
        my $seed_pubyr = get_real_pubyr($dbt, $results[0]);
		# THIS SHOULD NEVER HAPPEN
        #if($seed_pubyr == -1){
            # ERROR
        #    return $validated_list;
        #}
        #print ", Primary taxon_no: ".$results[0]->{taxon_no}."\n"; 

        # validate all the children
        foreach my $child (@results){
			# Don't revisit same child: this has to be done with the opinion
			# number (not the child_no) or the results will be incomplete.
			# Note: this is done mostly to avoid self referential data (and 
			# therefore deep recursion) but also speeds the whole thing up
			# A LOT!
            if(exists $visited_children{$child->{child_no}}){
                next;
            }
            else{
                $visited_children{$child->{child_no}} = 1;
            }
            #print "\tchild_no: ".$child->{child_no};
            my @other_results = ();
            $sql = "SELECT opinions.parent_no, opinions.pubyr, ".
                   "opinions.reference_no from opinions, authorities ".
                   "WHERE opinions.parent_no = authorities.taxon_no ".
                   "AND opinions.child_no=".$child->{child_no};
            # go back up and check each child's parent(s)
            @other_results = @{$dbt->getData($sql)};
            # find the most recent parent
            my $index = TaxonInfo::selectMostRecentParentOpinion($dbt, \@other_results , 1);
            my $most_recent = $other_results[$index]->{pubyr};
			## if this is the very first original seed, all children are good,
            # so don't do a pubyr check.
            if($first_time){
                $passed{$child->{child_no}} = 1;
                new_search_recurse($child->{child_no}, $dbt, 0);
            }
            # This adds current child and children with older parents, but
            # not children with parents younger than the seed parent.
            elsif($most_recent le $seed_pubyr){
                # Recursion: call self on all validated children
                #print "\nRECURSING WITH ".$child->{child_no}." SEED: $seed_no\n";
                $passed{$child->{child_no}} = 1;
                new_search_recurse($child->{child_no}, $dbt, 0);
            }
        }
    }
    else{
        #print "\nNO TAXON_NO FOUND FOR $seed_no\n";
		$visited_children{$seed_no} = 1;
    }
}


##
#
#
##
sub get_real_pubyr{
	my $dbt = shift;
    my $hash_rec = shift;
    my $year_string = (shift or "pubyr");
    my $ref_string = (shift or "reference_no");

	# if it's got a pubyr, cool
    if($hash_rec->{$year_string}){
        return $hash_rec->{$year_string};
    }
	# if there's no reference number, we're dead in the water.
    elsif(!$hash_rec->{$ref_string}){
        return "0";
    }
    # check the global cache
    elsif(exists $ref_pubyr{$hash_rec->{$ref_string}}){
        return $ref_pubyr{$hash_rec->{$ref_string}};
    }
	# hit the db
    else{
        my $sql = "SELECT pubyr FROM refs WHERE reference_no=".
                  $hash_rec->{$ref_string};
        my @results = @{$dbt->getData($sql)};
        if($results[0]->{pubyr}){
			$ref_pubyr{$hash_rec->{$ref_string}} = $results[0]->{pubyr};
            return $results[0]->{pubyr};
        }
    }
    # If there is no pubyr, return an error condition.
    return "0";
}

##
# Recursively find all taxon_nos or genus names belonging to a taxon
##
# Hacked to return taxon_nos as well, and an array if a user assigns return value to an array PS 12/29/2004
# Added fetching of recombinations PS 02/14/2005.  if var suppress_recombinations is set, don't do this (useful)
# in taxomic intervals, where we want to do that manually
sub taxonomic_search{
	my $name = shift;
	my $dbt = shift;
	my $taxon_no = (shift or "");
    my $return_taxon_nos = (shift or "");
    my $suppress_recombinations = (shift or "");

	my $sql = "";
	my @results = ();
	if($taxon_no eq ""){
		$sql = "SELECT taxon_no from authorities WHERE taxon_name=".$dbt->dbh->quote($name);
		@results = @{$dbt->getData($sql)};
		$taxon_no = $results[0]->{taxon_no};
	}

    # global to this method and methods called by it
    local %ref_pubyr = ();
    local %visited_children = ();
	local %passed = ();

	# We might not get a number or rank if this name isn't in authorities yet.
	if(! $taxon_no){
        if ($return_taxon_nos ne "") {
            return wantarray ? (-1) : "-1";
        } else {
    		return wantarray ? ($name) : "'$name'";
        }
	}
	new_search_recurse($taxon_no, $dbt, 1);
	my $results;

    # Dirty trick PS 01/10/2005 - if a taxon_no of 0 is passed in a occurrence query, any
    #  occurrence that doesn't have a taxon no gets added in (~90000) and any collection
    #  with one of these occurrences gets added in (~19000). So delete it. Not sure why
    #  this 0 gets passed back sometimes right now
    delete $passed{0};

    # Get recombination taxon nos also, and add those in PS 02/14/2005
    unless ($suppress_recombinations) {
        foreach $taxon_no (keys %passed) {
            $recomb_no = getCorrectedName($dbt,$taxon_no,'number');
            if ($recomb_no != $taxon_no) {
                PBDBUtil::debug(2,"recomb no is $recomb_no for taxon no $taxon_no");
                $passed{$recomb_no} = 1;
            }
        }
    }

    if ($return_taxon_nos ne "") {
        if (wantarray) {
            return keys %passed;
        } else {
            return join(', ', keys %passed);
        }
    } else {
        $results = join(', ',keys %passed);
    }
    $sql = "SELECT taxon_name FROM authorities WHERE taxon_no IN ($results)";
    @results = @{$dbt->getData($sql)};

    if (wantarray) {
        return @results;
    } else {
        foreach my $item (@results){
            $item = "'".$item->{taxon_name}."'";
        }
        return join(', ', @results);
    }
	
	return $results;
}

sub simple_array_push_unique{
    my $orig_ref = shift;
    my @orig = @{$orig_ref};
    my $new_ref = shift;
    my @new = @{$new_ref};
    my $duplicate = 0;

    foreach my $item (@new){
        my $duplicate = 0;
        foreach my $old (@orig){
            if($item == $old){
                $duplicate = 1;
                last;
            }
        }
        if($duplicate == 0){
            push(@orig, $item);
        }
    }
    return \@orig;
}

# JA: this is a Paul Muhl function, not to be confused with
#  Classification::get_classification_hash, that only is called in two
#  places in bridge.pl related to construction of taxonomic lists by
#  buildTaxonomicList
# extensive rewrite 2.4.04 by JA to accomodate taxon numbers instead of names
# DEPRECATED 01/11/2004 PS - functionality was almost identical to Classification::get_classification_hash, so use that
sub get_classification_hash{
	my $dbt = shift;
    my $taxon_no = shift;

 # don't even bother unless we know the taxon's ID number
    if ( $taxon_no < 1 )	{
      return;
    }

    # this might be a recombined species name, so we need the original
    #   combination or we won't be able to follow the opinion chain upwards
    #   JA 29.4.04
    $taxon_no = TaxonInfo::getOriginalCombination($dbt, $taxon_no);

#   my $taxon_name = shift;
#   $taxon_name =~ /(\w+)\s+(\w+)/;
#   my ($genus, $species) = ($1, $2);
#   if($species){
#       $rank = "species";
#   }
#   else{
        $rank = '';
#       #$rank = "genus";
#   }

    my $child_no = -1;
#   my $parent_no = -1;
    my $parent_no = $taxon_no;
    my %parent_no_visits = ();
    my %child_no_visits = ();
    my %classification = ();

    my $status = "";
    my $first_time = 1;
    # Loop at least once, but as long as it takes to get full classification
    while($parent_no){
            $child_no = $parent_no;

# following old PM section tried to guess the chid_no from the name
        # Keep $child_no at -1 if no results are returned.
#       my $sql = "SELECT taxon_no, taxon_rank FROM authorities WHERE ".
#                 "taxon_name='$taxon_name'";
#		if($rank){
#			$sql .= " AND taxon_rank = '$rank'";
#		}
#       my @results = @{$dbt->getData($sql)};
#       if(defined $results[0]){
            # Save the taxon_no for keying into the opinions table.
#           $child_no = $results[0]->{taxon_no};

# JA: still do need the following
            # Insurance for self referential / bad data in database.
            # NOTE: can't use the tertiary operator with hashes...
            # How strange...
            if(exists $child_no_visits{$child_no}){
                $child_no_visits{$child_no} += 1;
            }
            else{
                $child_no_visits{$child_no} = 1;
            }
            last if($child_no_visits{$child_no}>1);

#       }
        # no taxon number: if we're doing "Genus species", try to find a parent
        # for just the Genus, otherwise give up.
#       else{
#           if($genus && $species){
#               $sql_auth_inv = "SELECT taxon_no, taxon_rank ".
#                  "FROM authorities ".
#                  "WHERE taxon_name = '$genus'";
#               @results = @{$dbt->getData($sql_auth_inv)};
                # THIS IS LOOKING IDENTICAL TO ABOVE...
                # COULD CALL SELF WITH EMPTY SPECIES NAME AND AN EXIT...
#               if(defined $results[0]){
#                   $child_no = $results[0]->{taxon_no};
#					$rank = $results[0]->{taxon_rank};

#                   if($child_no_visits{$child_no}){
#                       $child_no_visits{$child_no} += 1;
#                   }
#                   else{
#                       $child_no_visits{$child_no} = 1;
#                   }
#                   last if($child_no_visits{$child_no}>1);
#               }
#           }
#           else{
#               last;
#           }
#       }

	# get the taxon_no and rank of the initial argument, in case it's a
        # higher taxon name with some c/o/f parents so we can sort better.
        # don't save the taxon_name in the hash because it will already be
        # displayed in the 'genus' field of the taxonomic list
        if($first_time and $child_no > 0 ){
            $sql = "SELECT taxon_rank FROM authorities WHERE taxon_no=" . $child_no;
            @results = @{$dbt->getData($sql)};
            $classification{$results[0]->{taxon_rank}."_no"} = $child_no;
        }

        # otherwise, give up...
        # JA: this should never happen given that the function now starts
        #  with a non-zero taxon no, but what the heck
        if($child_no < 1){
            return {};
        }

        # Now see if the opinions table has a parent for this child
        my $sql_opin =  "SELECT status, parent_no, pubyr, reference_no ".
                        "FROM opinions ".
                        "WHERE child_no=$child_no";
                      #  "WHERE child_no=$child_no AND status='belongs to'";
        @results = @{$dbt->getData($sql_opin)};

# JA: PM wrote the following in case the taxon being classified was a species,
#  there were no opinions on it, but there were opinions on its genus; this
#  is now a moot point because only explicit classification relationships are
#  now allowed
#       if($first_time && $rank eq "species" && scalar @results < 1){
#           my ($genus, $species) = split(/\s+/,$taxon_name);
#           my $last_ditch_sql = "SELECT taxon_no ".
#                                "FROM authorities ".
#                                "WHERE taxon_name = '$genus' ".
#                                "AND taxon_rank = 'Genus'";
#           @results = @{$dbt->getData($last_ditch_sql)};
#           my $child_no = $results[0]->{taxon_no};
#           if($child_no > 0){
#               $last_ditch_sql = "SELECT status, parent_no, pubyr, ".
#                                 "reference_no FROM opinions ".
#                                 "WHERE child_no=$child_no AND ".
#                                 "status='belongs to'";
#               @results = @{$dbt->getData($last_ditch_sql)};
#           }
#       }

        $first_time = 0;

        if(scalar @results){
            $parent_no=TaxonInfo::selectMostRecentParentOpinion($dbt,\@results);
                
            # Insurance for self referential or otherwise bad data in database.
            if($parent_no_visits{$parent_no}){
                $parent_no_visits{$parent_no} += 1;
            }       
            else{
                $parent_no_visits{$parent_no}=1;
            }           
            last if($parent_no_visits{$parent_no}>1);
                    
            if($parent_no){
                # Get the name and rank for the parent
                my $sql_auth = "SELECT taxon_name, taxon_rank ".
                           "FROM authorities ".
                           "WHERE taxon_no=$parent_no";
                @results = @{$dbt->getData($sql_auth)};
                if(scalar @results){
                    $auth_hash_ref = $results[0];
                    # reset name and rank for next loop pass
                    $rank = $auth_hash_ref->{"taxon_rank"};
                    $taxon_name = $auth_hash_ref->{"taxon_name"};
                    $classification{$rank} = $taxon_name;
                    $classification{$rank."_no"} = $parent_no;
                }       
                else{   
                    # No results might not be an error: 
                    # it might just be lack of data
                    # print "ERROR in sql: $sql_auth<br>";
                    last;
                }
            }                    
            # If we didn't get a parent or status ne 'belongs to'
            else{                
                $parent_no = 0;
            }
        }   
        else{   
            # No results might not be an error: it might just be lack of data
            # print "ERROR in sql: $sql_opin<br>";
            last;                 
        }       
    }       
    return \%classification;
}



sub getMostRecentReIDforOcc{
	my $dbt = shift;
	my $occ = shift;
	my $returnTheRef = shift;
	
	my $sql = "SELECT genus_name, species_name, collection_no, reid_no,pubyr, ".
			  "reidentifications.created ".
			  "FROM reidentifications, refs WHERE occurrence_no=$occ ".
			  "AND reidentifications.reference_no = refs.reference_no";
	my @results = @{$dbt->getData($sql)};

	if(scalar @results < 1){
		return "";
	}
	elsif(scalar @results == 1){
		if($returnTheRef){
			return $results[0];
		}
		else{
			return $results[0]->{reid_no};
		}
	}
	# find the most recent pubyr:
	else{
		my $most_recent = 0;
		for(my $index=0; $index<@results; $index++){
			if($results[$index]->{pubyr} > $most_recent){
				$most_recent = $index; 
			}
		}	
		if($returnTheRef){
			return $results[$most_recent];
		}
		else{
			return $results[$most_recent]->{reid_no};
		}
	}
}

sub authorAndPubyrFromTaxonNo{
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
			$sql = "SELECT author1last, author2last, otherauthors, pubyr FROM refs ".
				   "WHERE reference_no=".$auth_rec[0]->{reference_no};
			@results = @{$dbt->getData($sql)};
			$return_vals{author1last} = $results[0]->{author1last};
			if ( $results[0]->{otherauthors} )	{
				$return_vals{author1last} .= " et al.";
			} elsif ( $results[0]->{author2last} )	{
				$return_vals{author1last} .= " and " . $results[0]->{author2last};
			}
			$return_vals{pubyr} = $results[0]->{pubyr};
        }
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

    main::dbg("Paleolng: $paleolng Paleolat $paleolat x $lngdeg y $latdeg fx $f_lngdeg fy $f_latdeg collage $collage rx $rx ry $ry pid $pid");
    return ($paleolng, $paleolat);
}

# Gets the childen of a taxon, sorted in hierarchical fashion
# Separated 01/19/2004 PS. 
#  Inputs:
#   * 1st arg: $dbt
#   * 2nd arg: taxon name or taxon number
#   * 3nd arg: max depth: no of iterations to go down
#  Outputs: an array of hash (record) refs
#    See 'my %new_node = ...' line below for what the hash looks like
sub getChildren { 
    my $dbt = shift;
    my $taxon_name_or_no = shift;
    my $max_depth = (shift || 1);
    my $get_junior_synonyms = (shift || 0);

    use Data::Dumper;

    # described above, return'd vars
    my %tree_cache = ();
    my $tree_head;

    if ($taxon_name_or_no =~ /^\d+$/) {
        $taxon_no = $taxon_name_or_no;
    } else {
        @taxon_nos = TaxonInfo::getTaxonNos($taxon_no);
        if (scalar(@taxon_nos) == 1) {
            $taxon_no = $taxon_name_or_no;
        }    
    }
    if (!$taxon_no) {
        return undef; # bad... ambiguous name or no no
    } 
    my @parents = ($taxon_no);
    %tree_node = ('taxon_no'=>$taxon_no, 'children'=>[]);
    $tree_cache{$taxon_no} = \%tree_node;
    $tree_head = \%tree_node;

    # Max depth = max number of iterations
	for $level ( 1..$max_depth)	{
        # go through the parent list
		my @children = ();
		$childcount = 0;
		for $p ( @parents )	{
        	# find all children of this parent
			my $sql = "SELECT DISTINCT child_no,status FROM opinions WHERE status='belongs to' AND parent_no=" . $p;
			@childrefs = @{$dbt->getData($sql)};

			# now the hard part: make sure the most recent opinion
			#  on each child name is a "belongs to" and places
			#  the child in the parent we care about
			@goodrefs = ();
			for my $ref ( @childrefs )	{
                # Took out section with getOriginalCombination - belongs to can only be attached to
                # original combinations so we start off with original combination, don't need to backtrack PS 01/21/2004
                # rewrote this section 21.4.04 to use selectMostRecentParentOpinion
                # switched to selectMostRecentBelongsToOpinion JA 18.11.04
				$sql = "SELECT child_no,parent_no,status,reference_no,ref_has_opinion,pubyr FROM opinions WHERE child_no=" . $ref->{child_no};
				@crefs = @{$dbt->getData($sql)};

				my $currentref = TaxonInfo::selectMostRecentBelongsToOpinion($dbt, \@crefs, 1);
				$lastopinion = $currentref ->{status};
				$lastparent = $currentref->{parent_no};

                if ( $lastopinion =~ /belongs to/ && $lastparent == $p )	{
                    push @goodrefs , $ref;
                } else { 
                    if ($DEBUG) {
                        my $lp_name = @{$dbt->getData("SELECT taxon_name from authorities where taxon_no=$lastparent")}[0]->{'taxon_name'} if $lastparent;
                        my $ci_name = @{$dbt->getData("SELECT taxon_name from authorities where taxon_no=$ref->{child_no}")}[0]->{'taxon_name'};
                        debug(1,"Failed ($ref->{child_no}-$ci_name)-$lastopinion-($lastparent-$lp_name) does not belong to $p");
                    }
                }
            }

            for my $ref ( @goodrefs ){
                $childcount++;
                # rock 'n' roll: save the child name
                push @children,$ref->{child_no};
                
                my ($name,$syn,$rank);
                $name_ref = getCorrectedName($dbt,$ref->{child_no});
                $name = $name_ref->{taxon_name};
                $rank = $name_ref->{taxon_rank};
               
                # Save the child into the tree
                my $node = $tree_cache{$p};
                my %new_node = ('taxon_no'=>$ref->{child_no}, 
                                'taxon_name'=>$name,
                                'taxon_rank'=>$rank,
                                'level'=>$level,
                                'children'=>[]);
                if ($get_junior_synonyms) {
                    $new_node{'synonyms'} = getSynonyms($dbt,$ref->{child_no});
                } 
                push @{$node->{'children'}}, \%new_node;
                $tree_cache{$ref->{child_no}} = \%new_node;
            }
		}
        # replace the parent list with the child list
		@parents = @children;
	}

    # Perform he final sort
    my @sorted_records;
    getChildrenTraverseTree($tree_head, \@sorted_records);
    # Remove the head (is the parameter pased in)
    shift @sorted_records;
    #print "<pre>";
    #print "\n\nTH:\n ".Dumper($tree_head);
    #print "\n\nTC:\n ".Dumper(\%tree_cache);
    #print "\n\nSK:\n ".Dumper(\@sort_keys);
    #print "\n\nNAMES:\n ".Dumper(\%names);
    #print "\n\nRANKS:\n ".Dumper(\%ranks);
    ##print "\n\nLEVELS:\n ".Dumper(\%levels);
    #print "</pre>";
    return \@sorted_records;
}

# If the taxon no is an original combination, get the name of the thing its recombined to
sub getCorrectedName{
    my $dbt = shift;
    my $child_no = shift;
    my $return_type = shift; #default is to return row, optionally return taxon_no

    # will get what its renamed to.  A bit tricky, as you want to the most recent opinion, but normally the 'belongs to' comes first
    # so you have to use an order by so it comes second.  can't use filter the belongs to in the where clause cause the taxon may
    # genuinely belong to something else (not recomb). surround the select in () so aliased fields can be used in order by.
    my $sql = "(SELECT a.taxon_no, a.taxon_name, a.taxon_rank, (o.status in ('recombined as','corrected as','rank changed as')) as is_recomb, IF(o.pubyr, o.pubyr, r.pubyr) as pubyr, (r.publication_type='compendium') AS is_compendium" 
         . " FROM opinions o " 
         . " LEFT JOIN authorities a ON o.parent_no = a.taxon_no " 
         . " LEFT JOIN refs r ON r.reference_no=o.reference_no " 
         . " WHERE o.child_no=$child_no) " 
         . " ORDER BY is_compendium ASC, pubyr DESC, is_recomb DESC LIMIT 1";

    my @rows = @{$dbt->getData($sql)};
    if (@rows) {
        if ($rows[0]->{'is_recomb'}) {
            if ($return_type eq 'number') {
                return $rows[0]->{'taxon_no'};
            } else {
                return $rows[0];
            }
        } else {
            if ($return_type eq 'number') {
                return $child_no;
            } else {
                my @me;
                #no recomb, just return the name/no of the child
                $sql = "SELECT a.taxon_no, a.taxon_name, a.taxon_rank from authorities a WHERE a.taxon_no=$child_no";
                @me = @{$dbt->getData($sql)};
                return $me[0];
            }        
        }
    } else {
        #bad input, bad output
        return undef;        
    }
}

# INTERNAL USE: function to do depth first traversal
# Internal use for getChildren function only right now
sub getChildrenTraverseTree {
    my $node = shift;
    my $sort_keys = shift;

    my @children = @{$node->{'children'}};
    push @{$sort_keys}, $node;
    if (@children) {
        # Sort the children. The map function converts an array of nodes to an array of taxon nos 
        # Then sort based on those taxon nos using the sort_hash (the taxon names)
        #@children = sort {$sort_hash->{$a} cmp $sort_hash->{$b}} map {$_->{'taxon_no'}} @children;
        @children = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @children;
        getChildrenTraverseTree($_,$sort_keys) for @children;
    } 
}

# Trivial function get junior synonyms of a taxon_no. Returns array of hash references to db table rows (like getData does)
sub getSynonyms {
    my $dbt = shift;
    my $parent_no = shift;    
    my @synonyms;

    # Get children of parent
    my $sql = "SELECT DISTINCT child_no FROM opinions WHERE parent_no=$parent_no" 
            . " AND status IN ('subjective synonym of','objective synonym of')"; #add replaced by?
    @rows = @{$dbt->getData($sql)};
    foreach $row (@rows) {
        $sql = "SELECT a.taxon_no, a.taxon_name, a.taxon_rank, o.parent_no, o.pubyr, o.reference_no ".
               "FROM opinions o, authorities a ".
               "WHERE o.child_no = a.taxon_no ".
               "AND o.child_no=".$row->{'child_no'};
        # go back up and check each child's parent(s)
        @parents = @{$dbt->getData($sql)};

        $index = TaxonInfo::selectMostRecentParentOpinion($dbt,\@parents,1);
        if ($parents[$index]->{'parent_no'} == $parent_no) {
            push @synonyms,$parents[$index];
        }    
    }
    return \@synonyms;
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
