package PBDBUtil;

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

    if ( $q->param('research_group') =~ /(^ETE$)|(^5%$)|(^PGAP$)/ ) {
        $sql = "SELECT reference_no FROM refs WHERE project_name LIKE '%";
        $sql .= $q->param('research_group') . "%'";

        my $sth = $dbh->prepare($sql);
        $sth->execute();
        my @refrefs = @{$sth->fetchall_arrayref()};
        $sth->finish();

        for $refref (@refrefs)  {
            $reflist .= "," . ${$refref}[0];
        }
        $reflist =~ s/^,//;
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
			$result_string .= "<table border=0 cellpadding=0 cellspacing=0 width=\"100%\"><tr bgcolor=E0E0E0 width=\"100%\">";
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
				$result_string .= "<tr bgcolor=E0E0E0>";
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
	debug(1,"ref $reference_no added as secondary for collection $collection_no<br>");
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

	debug(1,"isDeleteable sql: $sql<br>");
	my $sth = $dbh->prepare($sql) or print "SQL failed to prepare: $sql<br>";
	$sth->execute();
	my @rows = @{$sth->fetchall_arrayref({})};
	my %res = %{$rows[0]};
	my $num = $res{'count(occurrence_no)'};
	$sth->finish();
	if($num >= 1){
		debug(1,"Reference $reference_no has $num occurrences and is not deletable<br>");
		return 0;
	}
	else{
		debug(1,"Reference $reference_no has $num occurrences and IS deletable<br>");
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
	debug(1,"execute returned:$res.<br>");
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
		debug(1,"ref $reference_no exists as primary for collection $collection_no<br>");
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
		debug(1,"ref $reference_no exists as secondary for collection $collection_no<br>");
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
		debug(1,"ref $reference_no exists as secondary for collection $collection_no<br>");
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
			next NAME if($check_res->{$type} eq $check);
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
    my $validated_list = $seed_no;

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
            my $most_recent = 0;
            # find the most recent parent
            foreach my $parent (@other_results){
                #print "\tchild's parent: ".$parent->{parent_no};
                # Get the pubyr for each parent from the authorities table
                $parent->{pubyr} = get_real_pubyr($dbt,$parent);
                if($parent->{pubyr} > $most_recent){
                    $most_recent = $parent->{pubyr};
                }
            }
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
#
#
##
sub taxonomic_search{
	my $name = shift;
	my $dbt = shift;
    my $sql = "SELECT taxon_no from authorities WHERE taxon_name='$name'";
    my @results = @{$dbt->getData($sql)};

    # global to this method and methods called by it
    local %ref_pubyr = ();
    local %visited_children = ();
	local %passed = ();

	# We might not get a number or rank if this name isn't in authorities yet.
	if(!$results[0]){
		return "'$name'";
	}

	new_search_recurse($results[0]->{taxon_no}, $dbt, 1);
	my $results = join(',', keys %passed);
	
    $sql = "SELECT taxon_name FROM authorities WHERE taxon_no IN ($results)";
    @results = @{$dbt->getData($sql)};

	foreach my $item (@results){
        $item = "'".$item->{taxon_name}."'";
	}

	$results = join(",",@results);
	
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

1;
