#!/usr/bin/perl

# written by rjp, 1/2004
# module to create URL's for various common cases

package URLMaker;

use strict;


# Includes the following functions:
#----------------------------------
# URLForGenusAndSpecies
# URLForReferenceNumber


my $BADLINK = "";		# redirect them to this link if the info passed in was not valid
						# just an empty string for now.


# pass it a genus and species (or just a genus)
# and it will return a URL pointing to more 
# information about that genus (and possibly species).
# For internal use only!
sub URLForGenusAndSpecies {
	my $genus = shift;
	my $species = shift;
	
	if ((! $genus) and (! $species)) {
		return $BADLINK;  # nothing!	
	}
	
	my $name = $genus; 
	
	my $rank = "Genus";
	if ($species) {
		$name .= "+$species";
		$rank .= "+and+Species";
	}
	
	return "/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_name=$name&taxon_rank=$rank";
}


sub URLForReferenceNumber {
	my $ref = shift;

	if (!$ref) {
		return $BADLINK;
	}
	
	return "/cgi-bin/bridge.pl?action=displayRefResults&type=view&reference_no=$ref";
}



1;
