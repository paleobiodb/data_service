#!/usr/bin/perl

# created by rjp, 1/2004.
# represents information about an occurrence
# has methods to set the occurrence number,
# and then to retrieve information about it
# including an HTML formated entry in an occurrence table.

package Occurrence;

use strict;

use Taxon;
use SQLBuilder;
use Reference;
use URLMaker;
use CGI::Carp qw(fatalsToBrowser);


# these are the data fields for the object
use fields qw(	
				GLOBALVARS
				
				occurrence_no
				reference_no
				genus_name
				species_name
				abund_value
				comments
				
				reidList
				latestClassNum
				latestOrderNum
				latestFamilyNum
				
				html
				
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
#	html				:	html formatted entry made by buildHTML() and retrieved by formatAsHTML()
						

# pass the current Session object.
# Note, some things will still work without it, but
# permission checking will not.
sub new {
	my $class = shift;
	my Occurrence $self = fields::new($class);
	
	$self->{GLOBALVARS} = shift;

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
	    $SQLBuilder = SQLBuilder->new($self->{GLOBALVARS});
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
		$sql->setSQLExpr("SELECT reference_no, genus_name, species_name, abund_value, comments
						FROM occurrences WHERE occurrence_no = $input");
		$sql->executeSQL();
		my @result = $sql->nextResultArray();
		
		$self->{reference_no} = $result[0];
		
		$self->{genus_name} = $result[1];
		$self->{species_name} = $result[2];
		$self->{abund_value} = $result[3];
		$self->{comments} = $result[4];
		
		$sql->finishSQL();

	}
}


# returns comments for this occurrence
sub comments {
	my Occurrence $self = shift;	
	return $self->{comments};		
}

# return the abundvalue for this occurrence
sub abundValue {
	my Occurrence $self = shift;	
	return $self->{abund_value};	
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

# return reference_no for the collection this occurrence belongs to
sub collectionReferenceNumber {
	my Occurrence $self = shift;

	my $sql = SQLBuilder->new($self->{GLOBALVARS});

	return $sql->getSingleSQLResult("SELECT c.reference_no FROM 
								collections c, occurrences o
								WHERE o.occurrence_no = $self->{occurrence_no}
								AND o.collection_no = c.collection_no");
}

# returns the taxon_no for the class of the most
# recent reid.
#
# ** Note ** only works if the user has already
# called buildReidList() first. 
sub mostRecentReidClassNumber {
	my Occurrence $self = shift;

	return ($self->{latestClassNum} || 0);
}


# same as mostRecentReidClassNumber, but for order.
sub mostRecentReidOrderNumber {
	my Occurrence $self = shift;

	return ($self->{latestOrderNum} || 0);
}

# same as mostRecentReidClassNumber, but for family.
sub mostRecentReidFamilyNumber {
	my Occurrence $self = shift;

	return ($self->{latestFamilyNum} || 0);
}




# returns a URL for the details for this occurrence
sub occurrenceDetailsLink {
	my Occurrence $self = shift;

	return (URLMaker::URLForTaxonName($self->{genus_name}, $self->{species_name}));
}


# Meant for internal use only.
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
	my $taxon = Taxon->new($self->{GLOBALVARS});
	
	my $reference_no = $self->{reference_no};
	
	# initialize these to be empty..
	my $taxClass = "";		my $classNum;
	my $taxOrder = "";		my $orderNum;
	my $taxFamily = "";		my $familyNum;
	
	# grab the author names for the first reference.
	my $ref = Reference->new();
	$ref->setWithReferenceNumber($reference_no);
	my $authors = $ref->authors();
	
	my $referenceString;
	my $colRefNo = $self->collectionReferenceNumber();
	
	
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
	
		if (! ($taxon->setWithTaxonName("$result[2] $result[4]"))) {
			$taxon->setWithTaxonName("$result[2]");
		}
		
		$taxClass = $taxon->nameForRank("class");
		$classNum = $taxon->numberForRank("class");
		$taxOrder = $taxon->nameForRank("order");
		$orderNum = $taxon->numberForRank("order");
		$taxFamily = $taxon->nameForRank("family");
		$familyNum = $taxon->numberForRank("family");
	}


	$referenceString = "";
	if ($colRefNo != $result[8]) {
		# only list reference if it's not the same as the collection's reference number
		$referenceString = "<A HREF=\"" . URLMaker::URLForReferenceNumber($result[8]) . "\">$authors</A>";
	}
	my @reidRow = ($taxClass, $taxOrder, $taxFamily,
					"<A HREF=\"" . URLMaker::URLForTaxonName($result[2], $result[4]) .
					"\">$result[1] $result[2] $result[3] $result[4]</A>",
					$referenceString,
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
		$self->{latestClassNum} = $classNum;
		$self->{latestOrderNum} = $orderNum;
		$self->{latestFamilyNum} = $familyNum;
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
			
			if (! ($taxon->setWithTaxonName("$result->[2] $result->[4]"))) {
				$taxon->setWithTaxonName("$result->[2]");
			}

			$taxClass = $taxon->nameForRank("class");
			$classNum = $taxon->numberForRank("class");
			$taxOrder = $taxon->nameForRank("order");
			$orderNum = $taxon->numberForRank("order");
			$taxFamily = $taxon->nameForRank("family");
			$familyNum = $taxon->numberForRank("family");
		}
	
		$referenceString = "";
		if ($colRefNo != $result->[7]) {
			# only list reference if it's not the same as the collection's reference number
			$referenceString = "<A HREF=\"" . URLMaker::URLForReferenceNumber($result->[7]) . "\">$authors</A>";
		}
		my @reidRow = ($taxClass, $taxOrder, $taxFamily,
					"= <A HREF=\"" . URLMaker::URLForTaxonName($result->[2], $result->[4]) .
					 "\">$result->[1] $result->[2] $result->[3] $result->[4]</A>",
					$referenceString,
					"",
					$result->[6]);
	
		push (@reidList, \@reidRow);	# add this row to the list of reids for this taxon.	
		
		$index++;
	}
	
	$sql->finishSQL();
	

	# save the list in a data field.
	$self->{reidList} = \@reidList;
	
	$self->{latestClassNum} = $classNum;
	$self->{latestOrderNum} = $orderNum;
	$self->{latestFamilyNum} = $familyNum;
	

}


# returns the html formatted entry for this occurrence
# meant for public calling
sub formatAsHTML {
	my Occurrence $self = shift;

	my $html = $self->{html};
	if (! $html) {
		$self->buildHTML();		# build the html if it wasn't already built
		$html = $self->{html};
	}

	return $html;	
}


# builds HTML formatted occurrence entry
# for entry in a table, ie, on the collections page under the listing of occurrences.
#
# Stores in parameter called html
sub buildHTML {
	my Occurrence $self = shift;
	
	$self->buildReidList();
	my $reidListRef = $self->{reidList};
	
	
	# some HTML tags
	my $TD = "TD nowrap";
	my $fontStart = "";
	my $fontEnd = "";
	my $style;
	my $html;
	
	# loop through all rows in the reidList
	# each of these will become a row of HTML code.
	#
	# note, if more than one row, then everything after the
	# first row is a reid.  Make the reids show up in smaller font size.
	my $count = 0;
	foreach my $row (@{$reidListRef}) {
	
		# if a cell has a style (from a style sheet)
		# this is used to make the entries which are indet. non-italic, and the others italic.
		$style = "class=\"nonindet\"";
		if ($row->[3] =~ m/indet[.]/) {
			$style = "class=\"indet\""; 
		}
		
		# since we're going through in order, if we get to a count
		# of 1, that means that we have reids, so for this one and
		# all future reids, make the font small.
		if ($count == 1) {
			$fontStart = "<SPAN class=\"smaller\">";
			$fontEnd = "</SPAN>";
		}
		
		# Class	Order	Family	Taxon	Reference	Abundance	Comments
		my $specimens = $row->[5];
		$specimens =~ s/^1 specimens$/1 specimen/;  # remove s from specimens if only one.
		
		$html .= "<TR>
				<$TD>$fontStart$row->[0]$fontEnd</TD>
				<$TD>$fontStart$row->[1]$fontEnd</TD>
				<$TD>$fontStart$row->[2]$fontEnd</TD>
				<$TD $style>$fontStart$row->[3]</A>$fontEnd</TD>
				<$TD>$fontStart$row->[4]$fontEnd</TD>
				<$TD>$fontStart$specimens$fontEnd</TD>
				<TD>$fontStart$row->[6]$fontEnd</TD>
			</TR>";
		
		$count++;
	}
	
	$self->{html} = $html;
}


# end of Occurrence.pm

1;