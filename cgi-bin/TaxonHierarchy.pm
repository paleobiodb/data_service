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


# sets the inital taxon with the taxon_no from the database.
sub setWithTaxonNumber {
	my TaxonHierarchy $self = shift;
	
	if (my $input = shift) {
		# now we need to get the taxonName from the database if it exists.
		my $tn = $self->getTaxonNameFromNumber($input);
		
		$self->{taxonNumber} = $input;

		if ($tn) {
			# if we found a taxon_name for this taxon_no, then 
			# set the appropriate fields
			$self->{taxonName} = $tn;
		}
	}
}


# Sets the initial taxon with the taxon_name from the database.
# If the taxon is not in the database, then it does nothing.
sub setWithTaxonName {
	my TaxonHierarchy $self = shift;
	my $newname;
	
	if (my $input = shift) {
		# now we need to get the taxonNo from the database if it exists.
		my ($tn, $newname) = $self->getTaxonNumberFromName($input);
		
		if ($tn) {
			# if we found a taxon_no for this taxon_name, then 
			# set the appropriate fields
			$self->{taxonNumber} = $tn;
			$self->{taxonName} = $newname;
		
		}	
	}
}	


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
# NOTE *** Be very careful not to call this whithin a method
# which is *already* using SQLBuilder.. For example if you call
# a method from a loop which itself calls  
# getSQLBuilder, then you have a problem.
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
		my $tn = $sql->getSingleSQLResult("SELECT taxon_name FROM authorities 
				WHERE taxon_no = $input");

		if ($tn) {
			return $tn;
		}
	}
	
	return "";
}


# **For internal use only - get the number of a taxon from the name
# returns an ARRAY with two elements: the taxon_no, and the new taxon_name.
# This is done because the taxon_name may be shortened, for example, if
# it's a genus species pair, but we only have an entry for the genus, not the pair.
# returns -1 if it can't find the number. 
# Note, not all taxa are in this table, so it won't work for something that dosen't exist.
sub getTaxonNumberFromName {
	my TaxonHierarchy $self = shift;

	if (my $input = shift) {
		my $sql = $self->getSQLBuilder();
		
		my $tn = $sql->getSingleSQLResult("SELECT taxon_no FROM authorities WHERE taxon_name = '$input'");
		
		if ($tn) {
			return ($tn, $input);
		}
		
		# if we make it to here, then that means that we didn't find the
		# taxon in the authorities table, so try it with just the first part
		# (ie, we'll assume that it was a genus species, and cut off the species)
		$input =~ s/^(.*)\s.*$/$1/;
		
		my $tn = $sql->getSingleSQLResult("SELECT taxon_no FROM authorities WHERE taxon_name = '$input'");
		
		if ($tn) {
			return ($tn, $input);
		}
	}
	
	return (-1, "");
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


# pass this a rank such as "family" or "genus" and it will
# return the name of the taxon at that rank as determined by the taxaHash.
sub nameForRank {
	my TaxonHierarchy $self = shift;
	my $key = shift; 
	
	if (! ($self->{taxaHash}) ) {
		# if the hash doesn't exist, then create it
		$self->createTaxaHash();
	}

	my $hash = $self->{taxaHash};
	my %hash = %$hash;
	
	my $id = $hash{$key};
	
	if ($id) {
		# now we need to get the name for it
		my $sql = $self->getSQLBuilder();
		return $sql->getSingleSQLResult("SELECT taxon_name FROM authorities WHERE taxon_no = $id");
	}
	
	return "";
	
}


# for the taxon the user set with setTaxonNumber/Name(), 
# finds the original combination of this taxon (ie, genus and species).
# Note, if the current taxon doesn't have an entry in the opinions table,
# it will just return the taxon number we started with (the one the user originally set).
#
# returns a taxon_no (integer).
# if it can't find one, returns 0 (false).
sub originalCombination {
	my TaxonHierarchy $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $tn = $self->taxonNumber();  # number we're starting with
	
	my $cn = $sql->getSingleSQLResult("SELECT child_no FROM opinions WHERE parent_no = $tn 
		AND status = 'recombined as' OR status = 'corrected as'");
	
	if (! $cn) {
		return $tn;		# return the number we started with if there are no recombinations	
	}
	
	return $cn;
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
	
	if (! $tn) { return };
	
	my %hash;  # hash of the results
	
	my $ref_has_opinion;  # boolean
	my ($pubyr, $idNum);
	my $resultRef;  # sql query results
	
	# another sql object for executing subqueries.
	my $subSQL = SQLBuilder->new();
	
	# first, insert the current taxon into the hash
	my $ownTaxonRank = $subSQL->getSingleSQLResult("SELECT taxon_rank FROM 
								authorities WHERE taxon_no = $tn");
	
	$hash{$ownTaxonRank} = $tn;

	# go up the hierarchy to the top (kingdom)
	# from the rank the user started with.
	while ($tn) {

		# note, the "ORDER BY o.parent_no DESC" is important - this means that if we have two
		# rows with the same pubyr, then it will always fetch the last one added since 
		# the numbers increment on each addition.
		$sql->setSQLExpr("SELECT o.parent_no, o.pubyr, o.ref_has_opinion, 
		o.reference_no, r.pubyr FROM opinions o, refs r 
		WHERE o.child_no = $tn AND o.reference_no = r.reference_no ORDER BY o.parent_no DESC");
		$sql->executeSQL();

		# loop through all result rows, and find the one with the most
		# recent pubyr.  Note, we'll have to look at the reference if the ref_has_opinion field is true.
		$pubyr = 0;
		$idNum = 0;
		
		my $tempYR;
		while ($resultRef = $sql->nextResultArrayRef()) {			
			if ($ref_has_opinion = $resultRef->[2]) {
				# if ref_has_opinion is YES, then we need to look to the reference
				# to find the pubyr
				$tempYR = $resultRef->[4]; # pubyr from reference
			} else {
				$tempYR = $resultRef->[1];  # pubyr from opinion
			}
				
			if ($tempYR > $pubyr) {
				$pubyr = $tempYR;
				$idNum = $resultRef->[0];
			}
			
		} # end while $resultRef.

		$sql->finishSQL();
		my $parent = $idNum;  # this is the parent with the most recent pubyr.

		# get the rank of the parent
		my $pRank = $sql->getSingleSQLResult("SELECT taxon_rank FROM authorities WHERE taxon_no = $parent");
		
		# insert it into the hash, so we have the parent rank as the key
		# and the parent number as the value.
		$hash{$pRank} = $parent;
		
		# also insert the pubyr for this parent, keyed to the id number.
		$hash{$parent} = $pubyr;		
					
		$tn = $parent;
		
		#print "here, tn = $tn\n";	

		#print "tn = $tn, id = $idNum, pubyr = $pubyr\n";
	}
	
	#print "\t\tout of loop\n";
	
	# now go down the hierarchy to the bottom
	# starting from the taxon the user originally passed in
	$tn = $self->taxonNumber();
	while ($tn) {
		$sql->setSQLExpr("SELECT o.child_no, o.pubyr, o.ref_has_opinion,
		o.reference_no, r.pubyr FROM opinions o, refs r 
		WHERE o.parent_no = $tn AND o.reference_no = r.reference_no ORDER BY o.child_no DESC");
		$sql->executeSQL();

		# loop through all result rows, and find the one with the most
		# recent pubyr.  Note, we'll have to look at the reference if the ref_has_opinion field is true.
		$pubyr = 0;
		$idNum = 0;
		
		my $tempYR;
		while ($resultRef = $sql->nextResultArrayRef()) {
			if ($ref_has_opinion = $resultRef->[2]) {
				# if ref_has_opinion is YES, then we need to look to the reference
				# to find the pubyr
				$tempYR = $resultRef->[4]; # pubyr from reference
			} else {
				$tempYR = $resultRef->[1];  # pubyr from opinion
			}
				
			if ($tempYR > $pubyr) {
				$pubyr = $tempYR;
				$idNum = $resultRef->[0];
			}
			
		} # end while $resultRef.
	
	
		$sql->finishSQL();
		my $child = $idNum;  # this is the child with the most recent pubyr.

		# get the rank of the parent
		my $cRank = $sql->getSingleSQLResult("SELECT taxon_rank FROM authorities WHERE taxon_no = $child");
		
		# insert it into the hash, so we have the parent rank as the key
		# and the parent taxon_no as the value.
		$hash{$cRank} = $child;
		
		# also insert the pubyr for this child, keyed to the id number.
		$hash{$child} = $pubyr;
				
		$tn = $child;
		
		print "tn = $tn, id = $idNum, pubyr = $pubyr\n";
	}
	print "\t\tout of loop\n";
	
	# print out for debugging purposes.
#	my @keys = keys(%hash);
#	foreach my $key (@keys) {
#		$sql->clear();
#		
#		if (!($key =~ /\d/)) {
#			my $taxon_no = $hash{$key};
#		
#			$sql->setSQLExpr("SELECT taxon_name FROM authorities WHERE taxon_no = '$taxon_no'");
#			$sql->executeSQL();
#			my @result = $sql->nextResultArray();
#			my $taxon_name = $result[0];
#			print "$key = $taxon_name\n";
#		}
#	}
	# end of printing section for debugging
	

	#store the hash in the object data field
	$self->{taxaHash} = \%hash;
}


# debugging
sub printTaxaHash {
	my TaxonHierarchy $self = shift;
	my $hash = $self->{taxaHash};
	my %hash = %$hash;
	print "Printing Taxa Hash\n";
	print "hash = '$hash'";
	
	# print out for debugging purposes.
	my @keys = keys(%hash);
	foreach my $key (@keys) {
		print "key = $key\n";
	}

}


# end of TaxonHierarchy.pm

1;