#!/usr/bin/perl

# 3/2004 by rjp
# represents information about the Person table in the database.

package Person;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use URLMaker;
use CGI::Carp qw(fatalsToBrowser);
use Constants;

use fields qw(	
				GLOBALVARS
				
				SQLBuilder
							);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Person $self = fields::new($class);

	$self->{GLOBALVARS} = shift;
	
	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Person $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
	    $SQLBuilder = SQLBuilder->new($self->{GLOBALVARS});
	}
	
	return $SQLBuilder;
}


# can pass it an optional argument, activeOnly
# if true, then only return active authorizers.
#
# Returns a matrix of authorizers with the 0th column
# being the normal name and the 1st column the reversed name.
sub listOfAuthorizers {
	my Person $self = shift;
	
	my $activeOnly = shift;
	
	my $sql = $self->getSQLBuilder();
	
	$sql->setSelectExpr("name, reversed_name FROM person");
	$sql->setWhereSeparator("AND");
	$sql->addWhereItem("is_authorizer = 1");
	
	if ($activeOnly) { 
		$sql->addWhereItem("active = 1");
	}
	
	$sql->setOrderByExpr("reversed_name");
	
	
	my $res = $sql->allResultsArrayRef();
	
	return $res;	
}


# can pass it an optional argument, activeOnly
# if true, then only return active enterers.
#
# Returns a matrix of enterers with the 0th column
# being the normal name and the 1st column the reversed name.
sub listOfEnterers {
	my Person $self = shift;
	
	my $activeOnly = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $temp = "SELECT name, reversed_name FROM person ";
	if ($activeOnly) { $temp .= " WHERE active = 1 "; }
	$temp .= " ORDER BY reversed_name";
	
	$sql->setSQLExpr($temp);
	
	my $res = $sql->allResultsArrayRef();
	
	return $res;
	
}

# Pass this an enterer or authorizer name (reversed or normal - doesn't matter)
# Returns a true value if the name exists in our database of people.
sub isValidName {
	my Person $self = shift;
	my $name = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $count = $sql->getSingleSQLResult("SELECT COUNT(*) FROM person WHERE
		name = '$name' OR reversed_name = '$name'");

	if ($count) { return TRUE; }
	else { return FALSE; }
}




# end of Person.pm

1;
