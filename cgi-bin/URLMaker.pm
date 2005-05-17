#!/usr/bin/perl

# DEPRECATED 05/15/2005 PS ... bleh

# written by rjp, 1/2004
# module to create URL's for various common cases

package URLMaker;

use strict;
use CGI;


# Includes the following functions:
#----------------------------------
# escapeURL
# URLForTaxonName
# URLForReferenceNumber



my $BADLINK = "";		# redirect them to this link if the info passed in was not valid
						# just an empty string for now.


# pass it a URL string, and it will return a properly 
# escaped URL string						
sub escapeURL {
	my $url = shift;
	$url =~ s/\s/%20/;
	return $url;
	#return (CGI::escape($url));
}

# pass it a url segment
# such as Homo+sapiens
# and it will replace the + signs with spaces and return it.
sub urlSegmentToSpaces {
	my $seg = shift;
	
	$seg =~ s/[+]/ /g;
	return $seg;
}


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
		$rank = "Genus+and+species";
		
		if ($second eq "indet.") {
			$rank = "Higher+taxon";	
		} else {
			# if it's not indet., but it contains a period, then
			# we'll assume that it's a Genus.  For example, second = sp. would fit this case
			if ($second =~ m/[.]/) {
				$rank = "Genus";
			}
		}
	}
	
	# if the second name contains a period, then remove it..
	$second =~ s/^.*([.]).*$//g;
	
	if ($second ne '') {
		$name .= "+$second";
	}
	
	return escapeURL("bridge.pl?action=checkTaxonInfo&taxon_name=$name&taxon_rank=$rank");
}





sub URLForReferenceNumber {
	my $ref = shift;

	if (!$ref) {
		return $BADLINK;
	}

	return escapeURL("bridge.pl?action=displayRefResults&type=view&reference_no=$ref");
}



1;
