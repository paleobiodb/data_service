package PBDBUtil;

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

## getSecondaryRefsString($dbh, $collection_no)
# 	Description:	constructs table rows of refs record data including
#					reference_no, reftitle, author info, pubyr and authorizer
#					and enterer.
#
#	Parameters:		$dbh			database handle
#					$collection_no	the collection number to which the 
#									references pertain.
#
#	Returns:		table rows
##
sub getSecondaryRefsString{
    my $dbh = shift;
    my $collection_no = shift;

    my $sql = "SELECT refs.reference_no, refs.author1init, refs.author1last, ".
			  "refs.author2init, refs.author2last, refs.otherauthors, ".
              "refs.pubyr, refs.authorizer, refs.enterer, refs.reftitle ".
              "FROM refs, secondary_refs ".
              "WHERE refs.reference_no = secondary_refs.reference_no ".
              "AND secondary_refs.collection_no = $collection_no";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results = @{$sth->fetchall_arrayref({})};
    $sth->finish();

	# Authorname Formatting
	use AuthorNames;

	my $result_string = "";
	# Format each row from the database as a table row.
	my $row_color = 0;
    foreach my $ref (@results){
		# add in a couple of single-space cells around the reference_no
		# to match the formatting of Reference from BiblioRef.pm
		if($row_color % 2){
			$result_string .= "<tr>";
		}
		else{
			$result_string .= "<tr bgcolor='E0E0E0'>";
		}
## ADAPT THIS FOR LATER...
#if ( $selectable ) {
#        $retVal .= "    <td width='5%' valign=top><input type='radio' name='reference_no' value='" . $self->{_reference_no} . "'></td>\n";
		$result_string .= "<td><small><b>$ref->{reference_no}</b></small></td>".
						  "<td colspan=3><small>$ref->{reftitle}</small>".
						  "</td></tr><tr><td></td>\n";

		# Get all the authornames for formatting
		my %temp = ('author1init',$ref->{author1init},
					'author1last',$ref->{author1last},
					'author2init',$ref->{author2init},
					'author2last',$ref->{author2last},
					'otherauthors',$ref->{otherauthors}
					);
		my $an = AuthorNames->new(\%temp);
		$result_string .= "<td><small>".$an->toString()."</small></td>".
						  "<td><small>$ref->{pubyr}</small></td>".
						  "<td><small>[$ref->{authorizer}/$ref->{enterer}]".
						  "</small></td></tr>\n";
    }
	$sth->finish();
	return $result_string;
}

1;
