package Person;
use strict;

# can pass it an optional argument, activeOnly
# if true, then only return active authorizers.
sub listOfAuthorizers {
    my $dbt = shift;
	my $activeOnly = shift;
	
	my $sql = "SELECT name, person_no FROM person WHERE is_authorizer=1";
	if ($activeOnly) { $sql .= " AND active = 1 "; }
	$sql .= " ORDER BY last_name,first_name";

    return $dbt->getData($sql);
}


# can pass it an optional argument, activeOnly
# if true, then only return active enterers. all authorizers
# are counted as enterers as well
sub listOfEnterers {
    my $dbt = shift;
	my $activeOnly = shift;
	
	my $sql = "SELECT name, person_no FROM person ";
	if ($activeOnly) { $sql .= " WHERE active = 1 "; }
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
    my $dbt = shift;
    my $html = "<div align=\"center\"><h3>List of data enterers</h3></div>";
    $html .= "The following students and research assistants have entered data into the Database.
    Institution names are placed in parentheses to indicate students who have since moved on.
    <i>IMPORTANT: if your e-mail address is not on this list and should be, please notify <a href=\"mailto:alroy\@nceas.ucsb.edu\">John Alroy.</a></i><br><br>";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=0 ORDER BY last_name,first_name";
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
    my $dbt = shift;
    my $html = "";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=1 ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    my @firsthalf;
    my @secondhalf;
    for my $r ( 0..int($#results/2) )	{
        push @firsthalf , $results[$r];
    }
    for my $r ( int($#results/2)+1..$#results )	{
        push @secondhalf , $results[$r];
    }

    $html .= '<div align="center"><h4>List of participants</h4></div>';
    $html .= '<div align="center"><p class="medium">See also our list of <a href="/cgi-bin/bridge.pl?action=displayInstitutions">participating institutions</a></p></div>';
    $html .= "\n<table><tr><td valign=\"top\">\n\n";
    $html .= formatAuthorizerTable(\@firsthalf);
    $html .= "\n</td><td valign=\"top\">\n";
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

sub displayInstitutions {
    my $dbt = shift;
    my $html = ""; 

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE is_authorizer=1 ORDER BY last_name,first_name";
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

    $html .= '<div align="center"><h4>List of participating institutions</h4></div>';
    $html .= '<div align="center"><p class="medium">See also our list of <a href="/cgi-bin/bridge.pl?action=displayAuthorizers">participants</a></p></div>';

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
