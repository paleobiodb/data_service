#!/usr/bin/perl

# currently for testing purposes only
# created by rjp, 1/2004.

package TaxonHierarchy;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;

use fields qw(	taxonName
				taxonNumber
				taxaHash
				
				SQLBuilder
							);  # list of allowable data fields.

# taxonName is the name of the original taxon the user set
# taxonNumber is the number for the original taxon
# taxaHash is a hash of taxa numbers and ranks.
							

sub new {
	my $class = shift;
	my TaxonHierarchy $self = fields::new($class);
	
	# set up some default values
	#$self->clear();	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my TaxonHierarchy $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
	}
	
	return $SQLBuilder;
}


# for internal use only - get the name for a taxon number
# return empty string if it can't find the name.
sub getTaxonNameFromNumber {
	my TaxonHierarchy $self = shift;
	
	if (my $input = shift) {

		my $sql = $self->getSQLBuilder();
		$sql->setSQLExpr("SELECT taxon_name FROM authorities 
				WHERE taxon_no = $input");

		my @result = $sql->resultArrayForExecutingSQL();
		my $tn = $result[0];
		
		if ($tn) {
			return $tn;
		}
	}
	
	return "";
}


# for internal use only - get the number of a taxon from the name
# returns -1 if it can't find the number.
sub getTaxonNumberFromName {
	my TaxonHierarchy $self = shift;

	if (my $input = shift) {
		my $sql = $self->getSQLBuilder();
		$sql->setSQLExpr("SELECT taxon_no FROM authorities 
				WHERE taxon_name = '$input'");

		my @result = $sql->resultArrayForExecutingSQL();
		my $tn = $result[0];
		
		if ($tn) {
			return $tn;
		}
	}
	
	return -1;
}



# sets the inital taxon with the taxon_no from the database.
sub setWithTaxonNumber {
	my TaxonHierarchy $self = shift;
	
	if (my $input = shift) {
		# now we need to get the taxonName from the database if it exists.
		my $tn = $self->getTaxonNameFromNumber($input);
		
		if ($tn) {
			# if we found a taxon_name for this taxon_no, then 
			# set the appropriate fields
			$self->{taxonName} = $tn;
			$self->{taxonNumber} = $input;
		}
	}
}


# Sets the initial taxon with the taxon_name from the database.
# If the taxon is not in the database, then it does nothing.
sub setWithTaxonName {
	my TaxonHierarchy $self = shift;
	if (my $input = shift) {
		# now we need to get the taxonNo from the database if it exists.
		my $tn = $self->getTaxonNumberFromName($input);
		
		if ($tn) {
			# if we found a taxon_no for this taxon_name, then 
			# set the appropriate fields
			$self->{taxonNumber} = $tn;
			$self->{taxonName} = $input;
		}
	}
}	


# return the taxonNumber for the originally specified taxon.
sub taxonNumber {
	my TaxonHierarchy $self = shift;

	return $self->{taxonNumber};	
}


# return the taxonName for the initially specifed taxon.
sub taxonName {
	my TaxonHierarchy $self = shift;

	return $self->{taxonName};	
}


# for internal use only
# creates a hash of all taxa ranks and numbers
# for the original taxa the user passed in.
sub createTaxaHash {
	my TaxonHierarchy $self = shift;

	my $sql = $self->getSQLBuilder();
	
	# first go up the hierarchy from the passed in taxon
	# ie, go to the parent of the passed in taxon
	
	# get the initial taxon the user set
	my $tn = $self->taxonNumber();
	my %hash;
	
	# go up the hierarchy to the top (kingdom)
	while ($tn) {
		$sql->setSQLExpr("SELECT parent_no FROM opinions WHERE child_no = $tn");
		my @result = $sql->resultArrayForExecutingSQL();
		my $parent = $result[0];

		# get the rank of the parent
		$sql->clear();
		$sql->setSQLExpr("SELECT taxon_rank FROM
		authorities WHERE taxon_no = '$parent'");
						 	
		@result = $sql->resultArrayForExecutingSQL();
		my $pRank = $result[0];
		
		# insert it into the hash, so we have the parent rank as the key
		# and the parent number as the value.
		$hash{$pRank} = $parent;
				
		$tn = $parent;
	}
	
	# now go down the hierarchy to the bottom
	# starting from the taxon the user originally passed in
	$tn = $self->taxonNumber();
	while ($tn) {
		$sql->setSQLExpr("SELECT child_no FROM opinions WHERE parent_no = $tn");
		my @result = $sql->resultArrayForExecutingSQL();
		my $child = $result[0];

		# get the rank of the parent
		$sql->clear();
		$sql->setSQLExpr("SELECT taxon_rank FROM
		authorities WHERE taxon_no = '$child'");
						 	
		@result = $sql->resultArrayForExecutingSQL();
		my $cRank = $result[0];
		
		# insert it into the hash, so we have the parent rank as the key
		# and the parent number as the value.
		$hash{$cRank} = $child;
				
		$tn = $child;			
	}
	
	my @keys = keys(%hash);
	print "keys = @keys";
		
	
	# store the hash in the object data field
	$self->{taxaHash} = %hash;
	

}


1;