#!/usr/bin/perl

# created by rjp, 1/2004.
# Represents information about a particular reference


package Reference;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use URLMaker;

use fields qw(	reference_no
				pubyr
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
		
		# get the pubyr and save it
		my $sql = $self->getSQLBuilder();
		my $pubyr = $sql->getSingleSQLResult("SELECT pubyr FROM refs WHERE reference_no = $input");
		$self->{pubyr} = $pubyr;
	}
}


# return the referenceNumber
sub referenceNumber {
	my Reference $self = shift;

	return ($self->{reference_no});	
}

# return the publication year for this reference
sub pubyr {
	my Reference $self = shift;
	return ($self->{pubyr});
}


# get all authors and year for reference
sub authors {
	my Reference $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $ref_no = $self->{reference_no};
	$sql->setSQLExpr("SELECT author1last, author2last, otherauthors FROM refs WHERE reference_no = $ref_no");
	$sql->executeSQL();
	
	my @result = $sql->nextResultArray();
	$sql->finishSQL();
	
	my $auth = $result[0];	# first author
	if ($result[2]) {	# we have other authors (implying more than two)
		$auth .= " et al."; 
	} elsif ($result[1]) {	# exactly two authors
		$auth .= " and $result[1]";
	}
	
	$auth .= " $self->{pubyr}";  # pubyr
		
	return $auth;
}


# returns a reference URL
sub referenceURL {	
	my Reference $self = shift;

	my $url = URLMaker::URLForReferenceNumber($self->{reference_no});
	my $authors = $self->authors();
	
	return ("<A HREF=\"$url\">$authors</A>");
	
}




# end of Reference.pm


1;