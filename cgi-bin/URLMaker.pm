#!/usr/bin/perl

# written by rjp, 1/2004
# module to create URL's for various common cases

package URLMaker;

use strict;


# Includes the following functions:
#----------------------------------
# URLForGenusAndSpecies
# URLForReferenceNumber


# *********
# Note, these should be escaped ...  do this soon?



my $BADLINK = "";		# redirect them to this link if the info passed in was not valid
						# just an empty string for now.


# pass it two names such as a genus and species
# and it will return a URL pointing to more 
# information about that genus (and possibly species).
# The second argument is optional.
# Note, if the second argument is "indet.", then the names
# represent a higher taxon, not a genus and species.
sub URLForTaxonName {
	my $first = shift;
	my $second = shift;
	
	if (! $first) {
		return $BADLINK;
	}
		
	my $name = $first; 
	my $rank;
	
	$rank = "Genus";
	
	if ($second) {
		$rank = "Genus+and+Species";
		
		if ($second eq "indet.") {
			$rank = "Higher+taxon";	
		}
	}
	
	if ($second ne "indet.") {
		$name .= "+$second";
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
