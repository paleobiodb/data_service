#!/usr/bin/perl

# created by rjp, 1/2004.

package Reference;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;

use fields qw(	reference_no
			
				SQLBuilder
							);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Reference $self = fields::new($class);
	
	# set up some default values
	#$self->clear();	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Reference $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
	}
	
	return $SQLBuilder;
}


# sets the occurrence
sub setWithReferenceNumber {
	my Reference $self = shift;
	
	if (my $input = shift) {
		$self->{reference_no} = $input;
	}
}


# return the referenceNumber
sub referenceNumber {
	my Reference $self = shift;

	return ($self->{reference_no});	
}


# get all authors and year for reference
sub authors {
	my Reference $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $ref_no = $self->{reference_no};
	$sql->setSQLExpr("SELECT author1last, author2last, otherauthors, pubyr FROM refs WHERE reference_no = $ref_no");
	$sql->executeSQL();
	
	my @result = $sql->nextResultRow();
	$sql->finishSQL();
	
	my $auth = $result[0];
	if ($result[1]) {	 # more than one author
		$auth .= " and $result[1]";
		
		if ($result[2]) {  # other authors
			$auth .= " et all";
		}
	}
	
	$auth .= " $result[3]";  # pubyr
	
	return $auth;
}




1;