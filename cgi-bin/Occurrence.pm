#!/usr/bin/perl

# created by rjp, 1/2004.
# represents information about an occurrence
# has methods to set the occurrence number,
# and then to retrieve information about it
# including an HTML formated entry in an occurrence table.

package Occurrence;

use strict;
use SQLBuilder;
use TaxonHierarchy;
use Reference;

# these are the data fields for the object
use fields qw(	occurrence_no
				reference_no
				genus_name
				species_name
				SQLBuilder
							);  # list of allowable data fields.


#	occurrence_no		:	The occurrence_no for this occurrence, set by the user
#	reference_no		:	For this occurrence_no, figured out from database
#	genus_name			:	
#	species_name		:	 
						
						


sub new {
	my $class = shift;
	my Occurrence $self = fields::new($class);
	
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
		$SQLBuilder = SQLBuilder->new();
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


# internal class method
# pass it a genus and species (or just a genus)
# and it will return a URL pointing to more 
# information about that genus (and possibly species).
# For internal use only!
sub detailsForGenusAndSpecies {
	my $genus = shift;
	my $species = shift;
	
	if ((! $genus) and (! $species)) {
		return "";  # nothing!	
	}
	
	my $name = $genus; 
	
	my $rank = "Genus";
	if ($species) {
		$name .= "+$species";
		$rank .= "+and+Species";
	}
	
	return "/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_name=$name&taxon_rank=$rank";
}



# returns a URL for the details for this occurrence
sub occurrenceDetailsLink {
	my Occurrence $self = shift;

	return (detailsForGenusAndSpecies($self->{genus_name}, $self->{species_name}));
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
	
	my $class = "";
	my $order = "";
	my $family = "";

	# grab the author names for the first reference.
	my $ref = Reference->new();
	$ref->setWithReferenceNumber($self->referenceNumber());
	my $authors = $ref->authors();
	
	
	# figure out how many (if any) reidentifications exist
	$sql->setSQLExpr("SELECT count(*) FROM reidentifications WHERE occurrence_no = $occ_no");
	$sql->executeSQL();
	my $numReids = ($sql->nextResultRow())[0];
	
	
	# get the information for the original ID.
	$sql->setSQLExpr("SELECT o.genus_name, o.species_reso, o.species_name, o.abund_value, o.abund_unit, o.comments
	FROM occurrences o, refs r
	WHERE o.reference_no = r.reference_no AND o.occurrence_no = $occ_no");
		
	$sql->executeSQL();
	
	@result = $sql->nextResultRow();
	
	if ($numReids <= 0) {
		# get the taxa information for the original id
		$taxon->setWithTaxonName("$result[0] $result[1]");
		$class = $taxon->nameForRank("class");
		$order = $taxon->nameForRank("order");
		$family = $taxon->nameForRank("family");
	}
	
	# Class	Order	Family	Genus and Species	Reference	Abundance	Comments
	
	$html = "<TR>
				<TD>$class</TD>
				<TD>$order</TD>
				<TD>$family</TD>
				<TD><A HREF=\"" . detailsForGenusAndSpecies($result[0], $result[2]) .
				"\">$result[0] $result[1] $result[2]</A></TD>
				<TD>$authors</TD>
				<TD>$result[3] $result[4]</TD>
				<TD>$result[5]</TD>
			</TR>";
	
	
	$sql->finishSQL();

	# now we'll do the reids if they need to be done
	# otherwise, RETURN.
	if ($numReids <= 0) {
		return $html;	
	}
	
	$sql->setSQLExpr("SELECT reid.genus_name, reid.species_reso, reid.species_name, r.pubyr, reid.comments, reid.reference_no
		FROM reidentifications reid, refs r
		WHERE reid.reference_no = r.reference_no AND reid.occurrence_no = $occ_no
		ORDER BY r.pubyr ASC");
	
	$sql->executeSQL();
	my $index = 0;	
	while (@result = $sql->nextResultRow()) {
		$ref->setWithReferenceNumber($result[5]);
	 	$authors = $ref->authors();
		
		if ($index == $numReids - 1) {
			# then we're on the last one, so 
			# we should figure out the class, order, and family.
			
			$taxon->setWithTaxonName("$result[0] $result[1]");
			$class = $taxon->nameForRank("class");
			$order = $taxon->nameForRank("order");
			$family = $taxon->nameForRank("family");
		}
		
		$html .= "\n\n<TR>
				<TD>$class</TD>
				<TD>$order</TD>
				<TD>$family</TD>
				<TD>= <A HREF=\"" . detailsForGenusAndSpecies($result[0], $result[2]) .
					 "\">$result[0] $result[1] $result[2]</A></TD>
				<TD>$authors</TD>
				<TD></TD>
				<TD>$result[4]</TD>
				</TR>";
	
		
		$index++;
	}
	
	$sql->finishSQL();
	
	
	return $html;
	
}

#188176


1;