package Person;
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD);
use strict;
use Reference;

# Poling code calved off from displayLoginPage by JA 13.4.04
sub makeAuthEntJavascript {
    my $dbt = shift;

	####
	## We need to build a list of the enterers and authorizers for 
	## the java script to use for autocompletion.
	####
	my $authListRef = listOfAuthorizers($dbt);
	my $entListRef = listOfEnterers($dbt);
	
	my $authList;
	my $entList;
	foreach my $p (@$authListRef) {
		$authList .= "\"" . reverseName($p->{'name'}) . "\", ";  # reversed name
	}
	$authList =~ s/,\s*$//; # remove last comma and space if present.


	foreach my $p (@$entListRef) {
		$entList .= "\"" . reverseName($p->{'name'}) . "\", ";  # reversed name
	}
	$entList =~ s/,\s*$//; # remove last comma and space if present.

	my $javaScript = '<SCRIPT language="JavaScript" type="text/javascript">
	// returns an array of enterer names
	function entererNames() {
		var names = new Array(' . $entList . ');
		return names;
	}
		
	// returns an array of enterer names
	function authorizerNames() {
		var names = new Array(' . $authList . ');
		return names;
	} 
	</SCRIPT>
	';

	return $javaScript;
}

# can pass it an optional argument, activeOnly
# if true, then only return active authorizers.
sub listOfAuthorizers {
    my ($dbt,$active_only,$fossil_record_only) = @_;
	
	my $sql = "SELECT name, person_no FROM person WHERE is_authorizer=1";
	if ($active_only) { 
        $sql .= " AND active=1 "; 
    }
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
	$sql .= " ORDER BY last_name,first_name";

    return $dbt->getData($sql);
}


# can pass it an optional argument, activeOnly
# if true, then only return active enterers. all authorizers
# are counted as enterers as well
sub listOfEnterers {
    my ($dbt,$active_only,$fossil_record_only) = @_;
	
	my $sql = "SELECT name, person_no FROM person WHERE is_authorizer=1";
	if ($active_only) { 
        $sql .= " AND active=1 "; 
    }
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
	$sql .= " ORDER BY last_name,first_name";

    return $dbt->getData($sql);
	
}

# Pass this an enterer or authorizer name (reversed or normal - doesn't matter)
# Returns a true value if the name exists in our database of people.
sub checkName {
    my $dbt = shift;
	my $name = shift;

    my $sql = "SELECT COUNT(*) as c FROM person WHERE name=".$dbt->dbh->quote($name);
	my $count = ${$dbt->getData($sql)}[0]->{'c'};	

	if ($count) { return 1; }
	else { return 0; }
}

# pass this a person number and it 
# will return the person's name
sub getPersonName {
    my $dbt = shift;
    my $num = shift;

    if (! $num) {
        return '';
    }
    my $result = ${$dbt->getData("SELECT name FROM person WHERE person_no=$num")}[0]->{'name'};

    return $result;
}

# a trivial function - reverse the order of the last name and initial
# If it was Sepkoski, J. before, now its J. Sepkoski
# Likewise J. Sepkoski will be reversed into Sepkoski, J.
sub reverseName {
    my $name = shift;
    if ($name =~ /,/) {
        $name =~ s/^\s*(.*)\s*,\s*(.*)\s*$/$2 $1/;
    } else {
        $name =~ s/^\s*(\w\.)\s*(.*)\s*$/$2, $1/;
    }
    return $name;
}

sub displayEnterers {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "<div align=\"center\"><div class=\"pageTitle\">Data enterers</div></div>";
    $html .= "<p \"align=left\">The following students and research assistants have entered data into the Database.
    Institution names are placed in parentheses to indicate students who have since moved on.
    <i>IMPORTANT: if your e-mail address is not on this list and should be, please notify <a href=\"mailto:alroy\@nceas.ucsb.edu\">John Alroy.</a></i></p><br>";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=0";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    $html .= '<table cellpadding="3" cellspacing="0" border="0" width="100%">';
    $html .= "<tr><th align=\"left\">Name</th><th align=\"left\">Institution</th><th align=\"left\">Email</th></tr>";
    for(my $i=0;$i<@results;$i++) {
        my $row = $results[$i];
        my $name = "$row->{first_name} $row->{last_name}";
        if ($i % 2 == 0) {
            $html .= "<tr class=\"darkList\">";
        } else {
            $html .= "<tr>";
        }
        my $email;
        if ($row->{'email'}) {
            $email = scramble("$row->{'email'}");
        } else {
            $email = "<i>address unknown</i>";
        }
        $html .= "<td>$name</td><td>$row->{institution}</td><td>$email</td></tr>";
    }
    $html .= "</table>";
    return $html;
}

sub displayAuthorizers {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=1 AND last_entry IS NOT NULL";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    my @firsthalf;
    my @secondhalf;
    for my $r ( 0..int($#results/2) )	{
        push @firsthalf , $results[$r];
    }
    for my $r ( int($#results/2)+1..$#results )	{
        push @secondhalf , $results[$r];
    }

    $html .= '<div align="center"><div class="pageTitle">Contributing researchers</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 1em; padding-right: 1em;"><p class="small">The following Database members have entered data and/or supervised data entry by their students. See also our list of <a href="$READ_URL?action=displayInstitutions">contributing institutions</a>.</p></div>|;
    $html .= "\n<table><tr><td valign=\"top\" width=\"50%\">\n\n";
    $html .= formatAuthorizerTable(\@firsthalf);
    $html .= "\n</td><td valign=\"top\" width=\"50%\">\n";
    $html .= formatAuthorizerTable(\@secondhalf);
    $html .= "\n</td></tr></table>\n\n";

}

sub formatAuthorizerTable	{
    my $peopleref = shift;
    my @people = @{$peopleref};

    my $html .= '<table cellpadding="3" cellspacing="0" border="0">';
    for(my $i=0;$i<@people;$i++) {
        my $row = $people[$i];
        my $name = "$row->{first_name} $row->{last_name}";
        $html .= "<tr>";
        my $email;
        if ($row->{'email'}) {
            $email = scramble("$row->{'email'}");
        } else {
            $email = "<i>address unknown</i>";
        }
        $html .= "<td class=\"tiny\" style=\"text-indent: -1em; padding-left: 1.5em; padding-right: 0.5em;\">$name, $row->{institution}, $email</td></tr>\n";
    }
    $html .= "</table>";
    return $html;

}

# JA 22.12.07
sub displayFeaturedAuthorizers	{
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT p.first_name,p.last_name,p.institution,p.country,p.homepage,p.photo,max(reference_no) AS max_ref FROM person p,person p2,refs r WHERE p.is_authorizer=1 AND p2.last_entry IS NOT NULL AND r.created>DATE_SUB( now(), INTERVAL 1 YEAR) AND p.person_no=authorizer_no AND p2.person_no=enterer_no GROUP BY enterer_no";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY p2.last_entry DESC LIMIT 20";
    my @results = @{$dbt->getData($sql)};
    @results = sort { $a->{'last_name'} cmp $b->{'last_name'} || $a->{'first_name'} cmp $b->{'first_name'} } @results;
    my @refnos;
    my %seen = ();
    my @newresults;
    for my $r ( @results )	{
        if ( ! $seen{$r->{'first_name'}." ".$r->{'last_name'}} && $#newresults < 11 )	{
            push @refnos , $r->{'max_ref'};
            $seen{$r->{'first_name'}." ".$r->{'last_name'}}++;
            push @newresults , $r;
        }
    }
    @results = @newresults;

    $sql = "SELECT * FROM refs WHERE reference_no IN (" . join(',',@refnos) . ")";
    my @refrefs = @{$dbt->getData($sql)};
    my %refdata;
    for my $r ( @refrefs )	{
        $refdata{$r->{'reference_no'}} = $r;
    }

    $html .= '<div align="center"><div class="pageTitle">Featured contributors</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 2.1em; padding-right: 2em; padding-bottom: 0em;"><p class="small">Here are some Database members who have entered data and/or supervised data entry recently. See also our full list of <a href="$READ_URL?action=displayAuthorizers">contributors</a> and our list of <a href="$READ_URL?action=displayInstitutions">contributing institutions</a>.</p></div>|;
    $html .= "<div class=\"small\" style=\"padding-left: 1em;\">\n";
    $html .= "<table><tr><td width=\"50%\" valign=\"top\">\n";
    for(my $i=0;$i<@results;$i++) {
        my $row = $results[$i];
        $html .= "<div class=\"displayPanel\" style=\"padding: 0.75em; border-right: 1px solid #909090; border-bottom: 1px solid #808080;\">";
        if ( $row->{'photo'} )	{
            $html .= '<div style="float: right; position: relative; top: 0px; right: 0px; padding-left: 1em; padding-bottom: 0.5em;"><img src="/public/mugshots/' . $row->{'photo'} . '"></div>';
        }
        my $name = "$row->{first_name} $row->{last_name}";
        if ( $row->{'homepage'} )	{
            $name = '<a href="http://' . $row->{'homepage'} . '">' . $name . '</a>';
        }
        $html .= "$name<br>\n";
        $html .= "$row->{'institution'}, $row->{'country'}<br>\n";
        my $longref = Reference::formatLongRef($refdata{$row->{'max_ref'}});
        # authorizer/enterer not needed
        $longref =~ s/ \[.*//;
        $html .= "<div class=\"verysmall\" style=\"padding-top: 0.3em;\"><i><a href=\"$READ_URL?action=displayReference&reference_no=$row->{'max_ref'}\">Latest reference:</a></i> " . $longref. "</div>";
        $html .= "<div style=\"clear: both;\"></div></div>\n";
        if ( $i == int( $#results / 2 ) )	{
            $html .= "</td><td width=\"50%\" valign=\"top\">\n";
        }
    }
    $html .= "</td></tr></table></div><br>\n";
    return $html;
}

sub displayInstitutions {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=1 AND last_entry IS NOT NULL";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    my %institutions;
    foreach my $row (@results) {
        if ( $row->{'institution'} =~ /[A-Za-z0-9]/ )	{
            push @{$institutions{$row->{'institution'}}}, $row;
        } 
    }
    my @inst_names= keys %institutions;
    @inst_names= sort {$a cmp $b} @inst_names;

    my @firsthalf;
    my @secondhalf;
    for my $i ( 0..int($#inst_names/2) )	{
        push @firsthalf , $inst_names[$i];
    }
    for my $i ( int($#inst_names/2)+1..$#inst_names)	{
        push @secondhalf , $inst_names[$i];
    }

    $html .= '<div align="center"><div class="pageTitle">Contributing institutions</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 1em; padding-right: 1em;"><p class="small"><a href="$READ_URL?action=displayAuthorizers">Database members</a> who have contributed data or supervised data entry by students are affiliated with the following institutions.</p></div>|;

    $html .= "\n<table><tr><td valign=\"top\" width=\"50%\">\n\n";
    $html .= formatInstitutionTable(\@firsthalf,\%institutions);
    $html .= "\n</td><td valign=\"top\" width=\"50%\">\n\n";
    $html .= formatInstitutionTable(\@secondhalf,\%institutions);
    $html .= "\n</td></tr></table>\n\n";

}

sub formatInstitutionTable	{
    my $instnameref = shift;
    my @inst_names = @{$instnameref};
    my $institutionref = shift;
    my %institutions = %{$institutionref};

    my $html .= qq|<table cellpadding="3" cellspacing="0" border="0">\n\n|;
    for(my $i=0;$i<@inst_names;$i++) {
        my @all_people = @{$institutions{$inst_names[$i]}};
        my @names = map {$_->{'first_name'}." ".$_->{'last_name'}} @all_people;

        $html .= "<tr>";
        $html .= "<td class=\"tiny\" style=\"text-indent: -1em; padding-left: 1.5em;\">\n<b>$inst_names[$i]</b><br>\n".join(", ",@names)."</td></tr>\n";
    }
    $html .= "</table>\n\n";
    return $html;
}

sub scramble {
    my $email = shift;
    if ($email =~ /@/) {
        my ($part1,$part2) = split(/@/,$email);
        $part2 = reverse($part2);
        $part1 = reverse($part1);
        $part1 =~ s/'/\\'/g;
        $part2 =~ s/'/\\'/g;
        $email = "<script type=\"text/javascript\">descram('$part2','$part1')</script>";
    }  
    return $email;
}



# end of Person.pm

1;
