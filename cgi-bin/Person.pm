#!/usr/bin/perl

# 3/2004 by rjp
# represents information about the Person table in the database.

package Person;

use strict;
use DBI;
use DBTransactionManager;


# can pass it an optional argument, activeOnly
# if true, then only return active authorizers.
#
# Returns a matrix of authorizers with the 0th column
# being the normal name and the 1st column the reversed name.
sub listOfAuthorizers {
    my $dbt = shift;
	my $activeOnly = shift;
	
	my $sql = "SELECT name, reversed_name, person_no FROM person WHERE is_authorizer=1";
	if ($activeOnly) { $sql .= " AND active = 1 "; }
	$sql .= " ORDER BY reversed_name";

    return $dbt->getData($sql);
}


# can pass it an optional argument, activeOnly
# if true, then only return active enterers.
#
# Returns a matrix of enterers with the 0th column
# being the normal name and the 1st column the reversed name.
sub listOfEnterers {
    my $dbt = shift;
	my $activeOnly = shift;
	
	my $sql = "SELECT name, reversed_name, person_no FROM person ";
	if ($activeOnly) { $sql .= " WHERE active = 1 "; }
	$sql .= " ORDER BY reversed_name";

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
sub reverseName {
    my $name = shift;
    $name =~ s/^\s*(.*)\s*,\s*(.*)\s*$/$2 $1/;
    return $name;
}


# end of Person.pm

1;
