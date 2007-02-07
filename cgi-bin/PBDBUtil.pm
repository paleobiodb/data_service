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
    if($research_group =~ /^(?:decapod|divergence|ETE|5%|1%|PACED|PGAP)$/){
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
            if ($research_group !~ /^(?:decapod|divergence|ETE|PACED)$/) {
                push @terms, "c.reference_no IN ($refs)";
            }
        } else {
            push @terms, "c.reference_no IN ($refs)";
            push @terms, "sr.reference_no IN ($refs)";
        }
    } 
    if($research_group =~ /^(?:decapod|divergence|ETE|marine invertebrate|micropaleontology|PACED|paleobotany|paleoentomology|taphonomy|vertebrate)$/) {
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

	return if(isRefPrimaryOrSecondary($dbh, $collection_no, $reference_no));

	# If we got this far, the ref is not associated with the collection,
	# so add it to the secondary_refs table.
	my $sql = "INSERT IGNORE INTO secondary_refs (collection_no, reference_no) ".
		   "VALUES ($collection_no, $reference_no)";	
    my $sth = $dbh->prepare($sql);
    my $return = $sth->execute();
    #if($sth->execute() != 1){
	#	print "<font color=\"FF0000\">Failed to create secondary reference ".
	#		  "for collection $collection_no and reference $reference_no.<br>".
	#		  "Please notify the database administrator with this message.".
	#		  "</font><br>";
	#}
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
    #if($res != 1){
	#	print "<font color=\"FF0000\">Failed to delete secondary ref for".
	#		  "collection $collection_no and reference $reference_no.<br>".
	#		  "Return code:$res<br>".
	#		  "Please notify the database administrator with this message.".                  "</font><br>";
	#	return 0;
	#}
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


sub getMostRecentReIDforOcc{
	my $dbt = shift;
	my $occ = shift;
	my $returnTheRef = shift;

    my $sql = "SELECT re.*, r.pubyr FROM reidentifications re, refs r WHERE r.reference_no=re.reference_no AND re.occurrence_no=".int($occ)." ORDER BY r.pubyr DESC, re.reid_no DESC LIMIT 1";  

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

    require TimeLookup;
    require Map;    

    # Get time interval information
    my $t = new TimeLookup($dbt);
    my @itvs; 
    push @itvs, $max_interval_no if ($max_interval_no);
    push @itvs, $min_interval_no if ($min_interval_no && $max_interval_no != $min_interval_no);
    my $h = $t->lookupIntervals(\@itvs);

    my ($paleolat, $paleolng,$plng,$plat,$lngdeg,$latdeg,$pid); 
    if ($f_latdeg <= 90 && $f_latdeg >= -90  && $f_lngdeg <= 180 && $f_lngdeg >= -180 ) {
        my $colllowerbound =  $h->{$max_interval_no}{'lower_boundary'};
        my $collupperbound;
        if ($min_interval_no)  {
            $collupperbound = $h->{$min_interval_no}{'upper_boundary'};
        } else {        
            $collupperbound = $h->{$max_interval_no}{'upper_boundary'};
        }
        my $collage = ( $colllowerbound + $collupperbound ) / 2;
        $collage = int($collage+0.5);
        if ($collage <= 600 && $collage >= 0) {
            main::dbg("collage $collage max_i $max_interval_no min_i $min_interval_no colllowerbound $colllowerbound collupperbound $collupperbound ");

            # Get Map rotation information - needs maptime to be set (to collage)
            # rotx, roty, rotdeg get set by the function, needed by projectPoints below
            my $map_o = new Map;
            $map_o->{maptime} = $collage;
            $map_o->readPlateIDs();
            $map_o->mapGetRotations();

            ($plng,$plat,$lngdeg,$latdeg,$pid) = $map_o->projectPoints($f_lngdeg,$f_latdeg);
            main::dbg("lngdeg: $lngdeg latdeg $latdeg");
            if ( $lngdeg !~ /NaN/ && $latdeg !~ /NaN/ )       {
                $paleolng = $lngdeg;
                $paleolat = $latdeg;
            } 
        }
    }

    main::dbg("Paleolng: $paleolng Paleolat $paleolat fx $f_lngdeg fy $f_latdeg plat $plat plng $plng pid $pid");
    return ($paleolng, $paleolat);
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


1;
