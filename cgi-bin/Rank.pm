#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a taxon rank

# ************************
# not even nearly done yet
# ************************

package Rank;

use strict;

use CGI::Carp qw(fatalsToBrowser);
use Validation;
use Taxon;

use fields qw(	
				rankString
				rankNum
				
				isValid
				isHigher
				
				);  # list of allowable data fields.

			
			
# List of all allowable taxonomic ranks in order
# of hierarchy.  
my %rankToNum = (  "subspecies" => 1, "species" => 2, "subgenus" => 3,
                   "genus" => 4, "subtribe" => 5, "tribe" => 6,
                   "subfamily" => 7, "family" => 8, "superfamily" => 9,
                   "infraorder" => 10, "suborder" => 11,
                   "order" => 12, "superorder" => 13, "infraclass" => 14,
			       "subclass" => 15, "class" => 16, "superclass" => 17,
			       "subphylum" => 18, "phylum" => 19, "superphylum" => 20,
				   "subkingdom" => 21, "kingdom" => 22, "superkingdom" => 23,
				   "unranked clade" => 24, "informal" => 25);

			

# You can optionally pass a rank string such as "species" to this constructor
# and it will call setWithRankString() for you.
sub new {
	my $class = shift;
	my Rank $self = fields::new($class);
	
	my $rankString = shift;  #optional rank string.
	
	$self->{isValid} = 0;  # not valid by default.
	
	if ($rankString) {
		$self->setWithRankString($rankString);	
	}

	return $self;
}



# pass this a rank string such as 'species'
# and it will set the rank.
sub setWithRankString {
	my Rank $self = shift;
	
	my $string = shift;
	
	if (! $string) {
		$self->{isValid} = 0;
		return; 
	}
	
	$self->{rankString} = $string;
	
	# figure out the rank number for this rank..
	my $rankNum = $rankToNum{$string};
	$self->{rankNum} = $rankNum;
	
	# see if the rank string is in the enum list of valid ranks.
	if (!$rankNum) {
		$self->{isValid} = 0;
	}
	
	# is it a higher taxon or not?
	$self->{isHigher} = ($rankNum > 2);
	
	$self->{isValid} = 1;
}


# pass this a taxon name and it will try to 
# figure out the rank based on the spacing...
sub setWithTaxonNameSpacingOnly {
	my Rank $self = shift;
	
	my $taxonName = shift;
	
	if (! $taxonName) {
		$self->{isValid} = 0;
		return; 
	}
	
	my $spacingRank = Validation::taxonRank($taxonName);
	
	$self->{isValid} = ($spacingRank ne 'invalid');
	
	if ($spacingRank eq 'higher') {
		$self->{isHigher} = 1;
		
	} else {
		$self->{rankString} = $spacingRank;
		$self->{rankNum} = $rankToNum{$spacingRank};
		$self->{isHigher} = 0;
	}
}


# pass this a taxon name and it will try to 
# figure out the rank based on the spacing
# and if that fails, based on actually querying the database
# to figure out the rank.. Clearly, this will be the slowest of the
# setting methods.
sub setWithTaxonNameFullLookup {
	my Rank $self = shift;
	
	my $taxonName = shift;
	
	if (! $taxonName) {
		$self->{isValid} = 0;
		return; 
	}
	
	# attempt to set the rank based only on spacing..
	$self->setWithTaxonNameSpacingOnly($taxonName);
	
	# if we get a higher taxon, then actually look it up.
	if ($self->{isHigher}) {
		my $taxon = Taxon->new();
		$taxon->setWithTaxonName($taxonName);
		
		$self->setWithRankString($taxon->rank());
	}
}



# return the rank of this rank..  ;-)
sub rank {
	my Rank $self = shift;

	my $string = $self->{rankString};
	
	if ((!$string) && ($self->{isHigher})) {
		$string = 'higher';
	}
	
	return $string;
}


# is this object a valid rank?
sub isValid {
	my Rank $self = shift;
	
	return $self->{isValid};
}


# is the rank a higher rank?  IE: anything higher than species...
sub isHigher {
	my Rank $self = shift;
	
	return $self->{isHigher};
}


# Pass it another rank object and it will tell you
# if it is higher than the passed object.
# Returns boolean.
sub isHigherThan {
	my Rank $self = shift;
	
}

# Pass it another rank object and it will tell you
# if it is lower than the passed object.
# Returns boolean.
sub isLowerThan {
	my Rank $self = shift;
	
}








# end of Rank.pm


1;