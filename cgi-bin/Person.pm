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


use fields qw(	
				
				SQLBuilder
							);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Person $self = fields::new($class);

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Person $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
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




# end of Person.pm

1;
