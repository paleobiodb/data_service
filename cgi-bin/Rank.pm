#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a taxon rank
# 
# You can set the rank with either a string such as "species", or with a taxon name.
# If you set it with a taxon name, you have the choice of having it guess the rank based
# on spacing (can only figure out subspecies, species, higher, or invalid), or actually
# having it do a full database lookup to assign the rank.
#
# Includes methods for comparing ranks, etc.


package Rank;

use strict;

use Constants;

use Validation;
use Taxon;

use fields qw(	
				rankString
				rankNum
				
				isValid
				isHigher
				
				onlyKnowHigher
				
				);  # list of allowable data fields.

			
# rankString	:		ie, species, subspecies, etc.
# rankNum		:		number from rank hash below.
# isValid		:		0 = not valid rank, 1 = valid rank.
# isHigher		: 		0 = not higher, 1 = higher (subgenus or higher)			
# onlyKnowHigher:		0 = no, 1 = we only know that it's a higher rank, 
#						but we don't know what the exact rank is.
			
			
# List of all allowable taxonomic ranks in order
# of hierarchy.  
my %rankToNum = (  'subspecies' => 1, 'species' => 2, 'subgenus' => 3,
                   'genus' => 4, 'subtribe' => 5, 'tribe' => 6,
                   'subfamily' => 7, 'family' => 8, 'superfamily' => 9,
                   'infraorder' => 10, 'suborder' => 11,
                   'order' => 12, 'superorder' => 13, 'infraclass' => 14,
			       'subclass' => 15, 'class' => 16, 'superclass' => 17,
			       'subphylum' => 18, 'phylum' => 19, 'superphylum' => 20,
				   'subkingdom' => 21, 'kingdom' => 22, 'superkingdom' => 23,
				   'unranked clade' => 24, 'informal' => 25);

			

# You can optionally pass a rank string such as "species" to this constructor
# and it will call setWithRankString() for you.
sub new {
	my $class = shift;
	my Rank $self = fields::new($class);
	
	Debug::dbPrint("ranktest, SPECIES = " . SPECIES);
	my $rankString = shift;  #optional rank string.
	
	$self->{isValid} = 0;  # not valid by default.
	$self->{isHigher} = 0;
	$self->{onlyKnowHigher} = 0;
	$self->{rankNum} = 0;
	$self->{rankString} = '';
	
	
	if ($rankString) {
		$self->setWithRankString($rankString);	
	}

	return $self;
}


# pass this a Rank object and it sets itself to it..
# basically, a copy constructor
sub setWithRank {
	my Rank $self = shift;
	my Rank $other = shift;

	if ($other) {
		$self->{isValid} = $other->{isValid};
		$self->{isHigher} = $other->{isHigher};
		$self->{onlyKnowHigher} = $other->{onlyKnowHigher};
		$self->{rankNum} = $other->{rankNum};
		$self->{rankString} = $other->{rankString};
	}
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
	$self->{onlyKnowHigher} = 0;
	
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
		$self->{onlyKnowHigher} = 1;
	} else {
		$self->{rankString} = $spacingRank;
		$self->{rankNum} = $rankToNum{$spacingRank};
		$self->{isHigher} = 0;
		$self->{onlyKnowHigher} = 0;
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
	if ($self->{onlyKnowHigher}) {
		my $taxon = Taxon->new();
		$taxon->setWithTaxonName($taxonName);
		
		$self->setWithRankString($taxon->rankString());
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
#
# If it can't figure it out, it will return 0
sub isHigherThan {
	my Rank $self = shift;
	
	my Rank $other = shift;
	
	if (! ($self->{isValid} && $other->{isValid})) {
		return 0;	
	}
	
	
	# we'll have to change our action based on
	# whether we only know that it's a higher rank,
	# or if we know its exact rank.
	
	my $sokh = $self->{onlyKnowHigher};
	my $ookh = $other->{onlyKnowHigher};
		
	if ($sokh && $ookh) {
		return 0;	
	} elsif ($sokh) {
		# we know that we're a higher taxon, so we can 
		# only compare to the other rank if it's a species or subspecies.
		if ($other->isSpecies() || $other->isSubspecies()) {
			return 1;
		}
		
		return 0;
	} elsif ($ookh) {
		# we can never know for sure.
		return 0;
	} else {
		# we know the full rank of both objects.

		return (($self->{rankNum}) > ($other->{rankNum}));
	}
}

# same as isHigherThan(), but pass it
# a rank string such as "species" instead
# of a rank object.
sub isHigherThanString {
	my Rank $self = shift;
	my $string = shift;
	
	my $than = Rank->new($string);
	return ($self->isHigherThan($than));
}



# Pass it another rank object and it will tell you
# if it is lower than the passed object.
# Returns boolean.
#
# If it can't figure it out, it will return 0
sub isLowerThan {
	my Rank $self = shift;
	
	my Rank $other = shift;
	
	if (! ($self->{isValid} && $other->{isValid})) {
		return 0;	
	}
	
	# we'll have to change our action based on
	# whether we only know that it's a higher rank,
	# or if we know its exact rank.
	
	my $sokh = $self->{onlyKnowHigher};
	my $ookh = $other->{onlyKnowHigher};
	
	if ($sokh && $ookh) {
		return 0;	
	} elsif ($sokh) {
		# we can never know for sure.
		return 0;
	} elsif ($ookh) {
		
		# we know that the other rank is a higher taxon, so we can 
		# only compare to self if we're species or subspecies.
		if ($self->isSpecies() || $self->isSubspecies()) {
			return 1;
		}
		
		return 0;
	} else {
		# we know the full rank of both objects.
		
		return ($self->{rankNum} < $other->{rankNum});
	}
	
}



# same as isLowerThan(), but pass it
# a rank string such as "species" instead
# of a rank object.
sub isLowerThanString {
	my Rank $self = shift;
	my $string = shift;
	
	my $than = Rank->new($string);
	return ($self->isLowerThan($than));
}



# pass this another rank object and it will
# return a true value if they're equal ranks.
sub isEqualTo {
	my Rank $self = shift;
	my Rank $other = shift;

	if (!$other) {
		return 0;	
	}
	
	my $sokh = $self->{onlyKnowHigher};
	my $ookh = $other->{onlyKnowHigher};
	
	if ($sokh|| $ookh) {
		# we can't tell if they're equal if we only
		# know that either one (or both) is a higher taxon.
		return 0;
	} else {
		# we know the full rank of both objects.
		
		return ($self->{rankNum} == $other->{rankNum});
	}
}













###################
## simple accessors
###################

sub isSubspecies {
	my Rank $self = shift;
	return ($self->{rankNum} == 1);	
}
sub isSpecies {
	my Rank $self = shift;
	return ($self->{rankNum} == 2);	
}
sub isSubgenus {
	my Rank $self = shift;
	return ($self->{rankNum} == 3);	
}
sub isGenus {
	my Rank $self = shift;
	return ($self->{rankNum} == 4);	
}
sub isSubtribe {
	my Rank $self = shift;
	return ($self->{rankNum} == 5);	
}
sub isTribe {
	my Rank $self = shift;
	return ($self->{rankNum} == 6);	
}
sub isSubfamily {
	my Rank $self = shift;
	return ($self->{rankNum} == 7);	
}
sub isFamily {
	my Rank $self = shift;
	return ($self->{rankNum} == 8);	
}
sub isSuperfamily {
	my Rank $self = shift;
	return ($self->{rankNum} == 9);	
}
sub isInfraorder {
	my Rank $self = shift;
	return ($self->{rankNum} == 10);	
}
sub isSuborder {
	my Rank $self = shift;
	return ($self->{rankNum} == 11);	
}
sub isOrder {
	my Rank $self = shift;
	return ($self->{rankNum} == 12);	
}
sub isSuperorder {
	my Rank $self = shift;
	return ($self->{rankNum} == 13);	
}
sub isInfraclass {
	my Rank $self = shift;
	return ($self->{rankNum} == 14);	
}
sub isSubclass {
	my Rank $self = shift;
	return ($self->{rankNum} == 15);	
}
sub isClass {
	my Rank $self = shift;
	return ($self->{rankNum} == 16);	
}
sub isSuperclass {
	my Rank $self = shift;
	return ($self->{rankNum} == 17);	
}
sub isSubphylum {
	my Rank $self = shift;
	return ($self->{rankNum} == 18);	
}
sub isPhylum {
	my Rank $self = shift;
	return ($self->{rankNum} == 19);	
}
sub isSuperphylum {
	my Rank $self = shift;
	return ($self->{rankNum} == 20);	
}
sub isSubkingdom {
	my Rank $self = shift;
	return ($self->{rankNum} == 21);	
}
sub isKingdom {
	my Rank $self = shift;
	return ($self->{rankNum} == 22);	
}
sub isSuperkingdom {
	my Rank $self = shift;
	return ($self->{rankNum} == 23);	
}
sub isUnrankedClade {
	my Rank $self = shift;
	return ($self->{rankNum} == 24);	
}
sub isInformal {
	my Rank $self = shift;
	return ($self->{rankNum} == 25);	
}












# meant for testing only..
sub test {
	my $rank1 = Rank->new();
	my $rank2 = Rank->new();

	$rank2->setWithRankString(GENUS);
	#$rank1->setWithTaxonNameFullLookup('Aelurodon validus');
	#$rank->setWithTaxonNameSpacingOnly('Aelurodon validus');

	$rank1->setWithRankString(GENUS);
	#$rank2->setWithRank($rank1);
	#$rank2->setWithTaxonNameFullLookup('Equidae');

	print "rank1 rank = " . $rank1->rank() . "\n";
	print "rank2 rank = " . $rank2->rank() . "\n";
	
	my $isHigherThan = $rank1->isHigherThan($rank2);
	my $isLowerThan = $rank1->isLowerThan($rank2);
	
	
	print $rank1->rank() . " (isHigherThan = $isHigherThan) " . $rank2->rank() . "\n";
	print $rank1->rank() . " (isLowerThan = $isLowerThan) " . $rank2->rank() . "\n";
	
	
	my $family = Rank->new(FAMILY);
	my $genus = Rank->new(GENUS);
	
	if (! $family->isHigherThan($genus)) {
		print "family should be higher than genus\n";	
	}
}




# end of Rank.pm


1;
