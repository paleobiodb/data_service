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
				SQLBuilder
							);  # list of allowable data fields.

#	session				:	The current Session object, needed for permissions
#	occurrence_no		:	The occurrence_no for this occurrence, set by the user
#	reference_no		:	For this occurrence_no, figured out from database
#	genus_name			:	
#	species_name		:	 
						
						

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
		my @result = $sql->nextResultRow();
		
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





# returns a URL for the details for this occurrence
sub occurrenceDetailsLink {
	my Occurrence $self = shift;

	return (URLMaker::URLForTaxonName($self->{genus_name}, $self->{species_name}));
}


# get HTML formatted occurrence entry
# for entry in a table, ie, on the collections page under the listing of occurrences.
sub formatAsHTML {
	my Occurrence $self = shift;
	
	my (@result, $year);
	my $sql = $self->getSQLBuilder();
	my $html;
	
	my $occ_no = $self->{occurrence_no};
	
	my $taxon = TaxonHierarchy->new();
	
	# initialize these to be empty..
	my $class = "";
	my $order = "";
	my $family = "";

	# grab the author names for the first reference.
	my $ref = Reference->new();
	$ref->setWithReferenceNumber($self->referenceNumber());
	my $authors = $ref->authors();
	
	
	# figure out how many (if any) reidentifications exist
	my $numReids = $sql->getSingleSQLResult("SELECT count(*) FROM reidentifications WHERE occurrence_no = $occ_no");
	
	# We have to treat the originally identified taxon differently from the reids
	# because we want to display it differently in the table (without an = sign, 
	# without the class,order,family, etc.).
	
	# get the information for the original ID.
	$sql->setSQLExpr("SELECT o.collection_no, o.genus_reso, o.genus_name, o.species_reso,
	o.species_name, o.abund_value, o.abund_unit, o.comments, o.reference_no
	FROM occurrences o, refs r
	WHERE o.reference_no = r.reference_no AND o.occurrence_no = $occ_no");
		
	$sql->executeSQL();
	
	@result = $sql->nextResultRowUsingPermissions();
	
	if ($numReids <= 0) {
		# get the taxa information for the original id
		$taxon->setWithTaxonName("$result[2] $result[4]");
		$class = $taxon->nameForRank("class");
		$order = $taxon->nameForRank("order");
		$family = $taxon->nameForRank("family");
	}
	
	# Class	Order	Family	Taxon	Reference	Abundance	Comments
	
	# some HTML tags
	my $TD = "TD nowrap";
	my $style;
	
	# if a cell has a style (from a style sheet)
	# this is used to make the entries which are indet non-italic, and the others italic.
	if ($result[4] eq 'indet.') {
		$style = "class=\"indet\""; 
	} else {
		$style = "class=\"nonindet\"";
	}
	
	$html = "<TR>
				<$TD>$class</TD>
				<$TD>$order</TD>
				<$TD>$family</TD>
				<$TD $style><A HREF=\"" . URLMaker::URLForTaxonName($result[2], $result[4]) .
				"\">$result[1] $result[2] $result[3] $result[4]</A></TD>
				<$TD><A HREF=\"" . URLMaker::URLForReferenceNumber($result[8]) . "\">$authors</A></TD>
				<$TD>$result[5] $result[6]</TD>
				<$TD>$result[7]</TD>
			</TR>";
	
	
	$sql->finishSQL();

	
	# now we'll do the reids if they need to be done
	# Again - this is done separately because we display reids slightly differently
	# than the original id.
	
	# If no reids, then RETURN
	if ($numReids <= 0) {
		return $html;	
	}
	
	$sql->setSQLExpr("SELECT reid.collection_no, reid.genus_reso, reid.genus_name, reid.species_reso,
		reid.species_name, r.pubyr, reid.comments, reid.reference_no
		FROM reidentifications reid, refs r
		WHERE reid.reference_no = r.reference_no AND reid.occurrence_no = $occ_no
		ORDER BY r.pubyr ASC");
	
	$sql->executeSQL();
	my $index = 0;	
	while (@result = $sql->nextResultRowUsingPermissions()) {
		$ref->setWithReferenceNumber($result[7]);
	 	$authors = $ref->authors();
		
		if ($index == $numReids - 1) {
			# then we're on the last one, so 
			# we should figure out the class, order, and family.
			
			$taxon->setWithTaxonName("$result[2] $result[4]");
			$class = $taxon->nameForRank("class");
			$order = $taxon->nameForRank("order");
			$family = $taxon->nameForRank("family");
		}
		
		# if a cell has a style (from a style sheet)
		# this is used to make the entries which are indet non-italic, and the others italic.
		if ($result[4] eq 'indet.') {
			$style = "class=\"indet\""; 
		} else {
			$style = "class=\"nonindet\"";
		}
		
		$html .= "\n\n<TR>
				<$TD>$class</TD>
				<$TD>$order</TD>
				<$TD>$family</TD>
				<$TD $style>= <A HREF=\"" . URLMaker::URLForTaxonName($result[2], $result[4]) .
					 "\">$result[1] $result[2] $result[3] $result[4]</A></TD>
				<$TD><A HREF=\"" . URLMaker::URLForReferenceNumber($result[7]) . "\">$authors</A></TD>
				<$TD></TD>
				<$TD>$result[6]</TD>
				</TR>";
	
		
		$index++;
	}
	
	$sql->finishSQL();
	
	
	return $html;	
}


# end of Occurrence.pm

1;