#!/usr/bin/perl
# testing from flatpebble
# created by rjp, 1/2004.
# eventually, will draw a graph of how many collections/occurrences/etc. we've had per unit time.

#
# Note, we should create a new data table which will hold these statistics rather than generating
# them on the fly each time.  The queries seem to take three or four seconds, so we don't want
# to waste that much time.  Besides, the totals for past months aren't likely to change since they're
# already passed.
#
# So create a table which has the following fields to store this information:
# year		:		the month that this total is for
# month		:		the year that the total is for
# total		:		how many were added this month
# type		:		collection, occurrence, etc.. whatever
#
# primary_key (year, month)
#
# so, to figure out how many collections were added in December of 2002, one would query the new
# table like this:
# SELECT total FROM newtable WHERE year = 2002 AND month = 12 AND type = 'collection';

package Stats;

use strict;

use DBTransactionManager;
use Debug;


# these are the data fields for the object
use fields qw(	
				
				DBTransactionManager
		 	);  # list of allowable data fields.
						
						


sub new {
	my $class = shift;
	my Stats $self = fields::new($class);
		
	# set up some default values
	#$self->clear();	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getTransactionManager {
	my Stats $self = shift;
	
	my DBTransactionManager $DBTransactionManager = $self->{DBTransactionManager};
	if (! $DBTransactionManager) {
		$DBTransactionManager = DBTransactionManager->new();
	}
	
	return $DBTransactionManager;
}


sub collectionStats {
	my Stats $self = shift;

	my $sql = $self->getTransactionManager();
	$sql->setSQLExpr("SELECT count(*), YEAR(created), MONTH(created) 
			FROM collections GROUP BY YEAR(created), MONTH(created)");
	$sql->executeSQL();
	
	my $result = $sql->allResultsArrayRef();

	foreach my $row (@{$result}) {
		print "$row->[0]\t$row->[1]\t$row->[2]\n";	
	}

}


sub occurrenceStats {
	my Stats $self = shift;

	my $sql = $self->getTransactionManager();
	$sql->setSQLExpr("SELECT count(*), YEAR(created), MONTH(created) 
			FROM occurrences GROUP BY YEAR(created), MONTH(created)");
	$sql->executeSQL();
	
	my $result = $sql->allResultsArrayRef();

	foreach my $row (@{$result}) {
		print "$row->[0]\t$row->[1]\t$row->[2]\n";	
	}

}


# end of Stats.pm

1;
