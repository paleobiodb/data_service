#!/usr/bin/perl

# created by rjp, 1/2004.
# represents information about an occurrence
# has methods to set the occurrence number,
# and then to retrieve information about it
# including an HTML formated entry in an occurrence table.

package Occurrence;

use strict;

use TaxonHierarchy;
use SQLBuilder;
use Reference;
use URLMaker;

# these are the data fields for the object
use fields qw(	
				session
				
				occurrence_no
				reference_no
				genus_name
				species_name
				
				reidList
				latestClassNum
				latestOrderNum
				latestFamilyNum
				
				SQLBuilder
							);  # list of allowable data fields.

#	session				:	The current Session object, needed for permissions
#	occurrence_no		:	The occurrence_no for this occurrence, set by the user
#	reference_no		:	For this occurrence_no, figured out from database
#	genus_name			:	
#	species_name		:	 
#
#	reidList			:	List created by buildReidList() which will be used by 
#							formatAsHTML(), latestClassNum(), etc.
#	latestClassNum		:	Only filled if the user has called buildReidList() first.
#							Contains taxon_no for most recent reidentified class of this occurrence.
#	latestOrderNum		:	Same as latestClassNum, but for order.
#	latestFamilyNum		:	Same as latestClassNum, but for family.
						

# pass the current Session object.
# Note, some things will still work without it, but
# permission checking will not.
sub new {
	my $class = shift;
	my Occurrence $self = fields::new($class);
	
	my $session = shift;
	$self->{session} = $session;
	
	# set up some default values
	#$self->clear();	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Occurrence $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new($self->{session});
	}
	
	return $SQLBuilder;
}


# sets the occurrence
sub setWithOccurrenceNumber {
	my Occurrence $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	if (my $input = shift) {
		$self->{occurrence_no} = $input;

		# set the result_no parameter
		$sql->setSQLExpr("SELECT reference_no, genus_name, species_name FROM occurrences WHERE occurrence_no = $input");
		$sql->executeSQL();
		my @result = $sql->nextResultArray();
		
		$self->{reference_no} = $result[0];
		
		$self->{genus_name} = $result[1];
		$self->{species_name} = $result[2];
		
		$sql->finishSQL();

	}
}


# return the occurrenceNumber
sub occurrenceNumber {
	my Occurrence $self = shift;

	return $self->{occurrence_no};	
}

# return the referenceNumber for this occurrence
sub referenceNumber {
	my Occurrence $self = shift;
	
	return ($self->{reference_no});
}


# returns the taxon_no for the class of the most
# recent reid.
#
# ** Note ** only works if the user has already
# called buildReidList() first. 
sub mostRecentReidClassNumber {
	my Occurrence $self = shift;

	return 0;
}


# same as mostRecentReidClassNumber, but for order.
sub mostRecentReidOrderNumber {
	my Occurrence $self = shift;

	return 0;
}

# same as mostRecentReidClassNumber, but for family.
sub mostRecentReidFamilyNumber {
	my Occurrence $self = shift;

	return 0;
}




# returns a URL for the details for this occurrence
sub occurrenceDetailsLink {
	my Occurrence $self = shift;

	return (URLMaker::URLForTaxonName($self->{genus_name}, $self->{species_name}));
}


# Builds a listing of reidentifications for this occurrence.
# Basically, an array with each element being a reference to an array in
# the format that we'll eventually return as HTML.
#
# IE, each row is: Class, Order, Family, Taxon, Reference, Abundance, Comments.
sub buildReidList {
	my Occurrence $self = shift;
	
	my @reidList = ();	# this is the list we'll save...
	
	my (@result, $year);	
	my $sql = $self->getSQLBuilder();
	
	my $occ_no = $self->{occurrence_no};
	my $taxon = TaxonHierarchy->new();
	
	# initialize these to be empty..
	my $taxClass = "";
	my $taxOrder = "";
	my $taxFamily = "";
	
	# grab the author names for the first reference.
	my $ref = Reference->new();
	$ref->setWithReferenceNumber($self->referenceNumber());
	my $authors = $ref->authors();
	
	
	# figure out how many (if any) reidentifications exist
	# and simultaneously grab all rows as well.
	$sql->setSQLExpr("SELECT reid.collection_no, reid.genus_reso, 
		reid.genus_name, reid.species_reso,
		reid.species_name, r.pubyr, reid.comments, reid.reference_no
		FROM reidentifications reid, refs r
		WHERE reid.reference_no = r.reference_no AND reid.occurrence_no = $occ_no
		ORDER BY r.pubyr ASC");
	my $reids = $sql->allResultsArrayRefUsingPermissions();
		
	# the number of reids.
	my $numReids;
	
	if ($reids) {
		$numReids = scalar(@{$reids});
	} else { $numReids = 0; }
	
	
	# We have to treat the originally identified taxon differently from the reids
	# because we want to display it differently in the table (without an = sign, 
	# without the class,order,family, etc.).
	
	# get the information for the original ID.
	$sql->setSQLExpr("SELECT o.collection_no, o.genus_reso, o.genus_name, o.species_reso,
	o.species_name, o.abund_value, o.abund_unit, o.comments, o.reference_no
	FROM occurrences o, refs r
	WHERE o.reference_no = r.reference_no AND o.occurrence_no = $occ_no");
		
	$sql->executeSQL();

	@result = $sql->nextResultArrayUsingPermissions();
	
	if ($numReids <= 0) {
		# get the taxa information for the original id
		$taxon->setWithTaxonName("$result[2] $result[4]");
		
		$taxClass = $taxon->nameForRank("class");		
		$taxOrder = $taxon->nameForRank("order");
		$taxFamily = $taxon->nameForRank("family");
	}

	my @reidRow = ($taxClass, $taxOrder, $taxFamily,
					"<A HREF=\"" . URLMaker::URLForTaxonName($result[2], $result[4]) .
					"\">$result[1] $result[2] $result[3] $result[4]</A>",
					"<A HREF=\"" . URLMaker::URLForReferenceNumber($result[8]) . "\">$authors</A>",
					"$result[5] $result[6]",
					$result[7] );
	
	push (@reidList, \@reidRow);	# add this row to the list of reids for this taxon.
	
	$sql->finishSQL();

	
	# now we'll do the reids if they need to be done
	# Again - this is done separately because we display reids slightly differently
	# than the original id.
	
	# If no reids, then RETURN
	if ($numReids <= 0) {
		# save the list in a data field.
		$self->{reidList} = \@reidList;
		
		return;	
	}
	
	# if we make it to here, then we have 1 or more reids, stored
	# in the $allReids reference we got earlier...
	
	my $index = 0;
	foreach my $result (@{$reids}) {
		$ref->setWithReferenceNumber($result->[7]);
	 	$authors = $ref->authors();
		
		if ($index == $numReids - 1) {
			# then we're on the last one, so 
			# we should figure out the class, order, and family.
			
			$taxon->setWithTaxonName("$result->[2] $result->[4]");
			$taxClass = $taxon->nameForRank("class");
			$taxOrder = $taxon->nameForRank("order");
			$taxFamily = $taxon->nameForRank("family");
		}
	
		my @reidRow = ($taxClass, $taxOrder, $taxFamily,
					"= <A HREF=\"" . URLMaker::URLForTaxonName($result->[2], $result->[4]) .
					 "\">$result->[1] $result->[2] $result->[3] $result->[4]</A>",
					"<A HREF=\"" . URLMaker::URLForReferenceNumber($result->[7]) . "\">$authors</A>",
					"",
					$result->[6]);
	
		push (@reidList, \@reidRow);	# add this row to the list of reids for this taxon.	
		
		$index++;
	}
	
	$sql->finishSQL();
	
	# save the list in a data field.
	$self->{reidList} = \@reidList;
}


# get HTML formatted occurrence entry
# for entry in a table, ie, on the collections page under the listing of occurrences.
sub formatAsHTML {
	my Occurrence $self = shift;
	
	$self->buildReidList();
	my $reidListRef = $self->{reidList};
	
	# Class	Order	Family	Taxon	Reference	Abundance	Comments
	
	# some HTML tags
	my $TD = "TD nowrap";
	my $style;
	my $html;
	
	# loop through all rows in the reidList
	# each of these will become a row of HTML code.
	foreach my $row (@{$reidListRef}) {
	
		# if a cell has a style (from a style sheet)
		# this is used to make the entries which are indet. non-italic, and the others italic.
		$style = "class=\"nonindet\"";
		if ($row->[3] =~ m/indet[.]/) {
			$style = "class=\"indet\""; 
		}
		
		
		$html .= "<TR>
				<$TD>$row->[0]</TD>
				<$TD>$row->[1]</TD>
				<$TD>$row->[2]</TD>
				<$TD $style>$row->[3]</A></TD>
				<$TD>$row->[4]</TD>
				<$TD>$row->[5]</TD>
				<$TD>$row->[6]</TD>
			</TR>";		
	}
	

	return $html;	
}


# end of Occurrence.pm

1;