#!/usr/bin/perl

# created by rjp, 1/2004.
# represents information about a collection

# note, this is ***DIFFERENT* from the Collections.pm file.

package Collection;

use strict;

use SQLBuilder;
use Occurrence;
use Debug;


# these are the data fields for the object
use fields qw(	
				session
				
				collection_no
				collection_name
				
				reference_no
				
				SQLBuilder
							);  # list of allowable data fields.

#	session				:	The current Session object, needed for permissions
						
						

# pass the current Session object.
# Note, some things will still work without it, but
# permission checking will not.
sub new {
	my $class = shift;
	my Collection $self = fields::new($class);
	
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
	my Collection $self = shift;
	
	my SQLBuilder $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new($self->{session});
	}
	
	return $SQLBuilder;
}


# sets the collection
sub setWithCollectionNumber {
	my Collection $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	if (my $input = shift) {
		$self->{collection_no} = $input;

		# set the result_no parameter
		$sql->setSQLExpr("SELECT reference_no, collection_name FROM collections WHERE collection_no = $input");
		$sql->executeSQL();
		my @result = $sql->nextResultArray();
		
		$self->{reference_no} = $result[0];
		$self->{collection_name} = $result[1];
		
		$sql->finishSQL();
	}
}


# return the collectionNumber
sub collectionNumber {
	my Collection $self = shift;

	return $self->{collection_no};	
}

# return the collectionName
sub collectionName {
	my Collection $self = shift;

	return $self->{collection_name};	
}


# return the referenceNumber for this collection
sub referenceNumber {
	my Collection $self = shift;
	
	return ($self->{reference_no});
}


# returns HTML formatted taxonomic list of
# all occurrences (that the user has permission to see)
# in this collection record.
sub HTMLFormattedTaxonomicList {
	my Collection $self = shift;

	my $collection_no = $self->{collection_no};
		
	my $html = "<CENTER><H3>Taxonomic list for " . $self->{collection_name} .
		" (PBDB collection $collection_no)</H3></CENTER>";

	my $occ = Occurrence->new($self->{session});
	
	my $sql = $self->getSQLBuilder();	
	$sql->setSQLExpr("SELECT collection_no, occurrence_no FROM occurrences 
			 		WHERE collection_no = $collection_no");

	my $result = $sql->allResultsArrayRefUsingPermissions();

	$html .= "<TABLE BORDER=0 cellpadding=4 cellspacing=0><TR><TH>Class</TH><TH>Order</TH>
				<TH>Family</TH><TH>Taxon</TH><TH>Reference</TH>
				<TH>Abundance</TH><TH>Comments</TH></TR>";
	
	$html =~ s/<th/<th class=style1/ig;
	

	# build up an array of occurrence objects.. Then we'll sort the array
	my @occArray = ();
	foreach my $row (@{$result}) {
		$occ = Occurrence->new($self->{session});
		$occ->setWithOccurrenceNumber($row->[1]);
		$occ->buildHTML();	# have to do this now so we can sort on values from this..
		push(@occArray, $occ);
	}
	
	# now we should have an array of occurrences to list.
	# so, sort it based on class, order, and family.
	my @occArray = sort {
			$a->mostRecentReidClassNumber() <=> $b->mostRecentReidClassNumber() ||
			$a->mostRecentReidOrderNumber() <=> $b->mostRecentReidOrderNumber() ||
			$a->mostRecentReidFamilyNumber() <=> $b->mostRecentReidFamilyNumber() ||
			$a->occurrenceNumber() <=> $b->occurrenceNumber()
	} @occArray;
	
	# note, it's a special case if the class, order, and family fields are all blank.
	# in this case, we want to *guess* and stick the occurrence *after* the occurrence
	# with the smallest difference in occurrence_no from it.  So basically, scan
	# all of the occurrence numbers, find the one with the least difference from the occurrence
	# we're trying to insert, and insert it after that one.
	#
	# Make an array of occurrences which have class, order, family = 0 and pull them out
	# of the main @occArray.  Then figure out where they should go and stick them back in it.
	my @occToReposition = ();
	for (my $count = 0; $count < scalar(@occArray); $count++) {
		$occ = $occArray[$count];
		
		if ($occ->mostRecentReidClassNumber() == 0 && $occ->mostRecentReidOrderNumber() == 0 &&
		$occ->mostRecentReidFamilyNumber() == 0) {
			# all three fields are 0, so pull it off the @occArray and stick it in the reposition array
			push (@occToReposition, splice(@occArray, $count, 1));
			$count--;	# have to decrement count here since we just removed an element.
		}
	}
	
	# so now, we have an array of occurrences to position correctly within the original array.
	foreach my $newOcc (@occToReposition) {
		my $occNum = $newOcc->occurrenceNumber();
		
		my $diff = 10000;	# some large number.
		my $pos = 0;
		my $count = 0;
		# look for a position to stick it in
		foreach my $oldOcc (@occArray) {
			my $temp = abs($oldOcc->occurrenceNumber() - $occNum);
			if ($temp < $diff) {
				$diff = $temp;	# record this difference
				$pos = $count;	# record where we are
			}
			$count++;
		}
		
		# now that we have figured out where to stick it, stick it there.
		splice(@occArray, $pos, 0, $newOcc);
	}
	## End of sorting section
	
	
	# now that we have sorted it, compose the HTML
	my $count = 0;
	my $color = "";
	foreach my $row (@occArray) {
		my $newRow = $row->formatAsHTML();
		
		# make every other row dark
		if ($count % 2) { 
			$newRow =~ s/<tr/<tr class=darkList/ig;
		}
			
		$html .= $newRow;
			
		$count++;
	}
	
	$html .= "</TABLE>";
	
	return $html;	
}









# end of Collection.pm

1;