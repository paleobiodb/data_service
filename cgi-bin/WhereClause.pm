# Represents the where clause of an SQL statement.  May be expanded in the future
# to represent the entire statement.
# Written by Ryan, 1/2004.

package WhereClause;
use strict;
use fields qw(separator items);  # list of allowable data fields.

# This class implements the following methods:
#----------------------------------------------
# new
# separator
# setSeparator
# addItem
# clear
# items
# whereClause


sub new {
	my $class = shift;
	my WhereClause $self = fields::new($class);
	
	# set up some default values
	$self->{separator} = 'and';
	$self->{items} = ();  # empty list for now

	return $self;
}

# returns the separator
sub separator {
	my WhereClause $self = shift;
	return $self->{separator};	
}

# set which separator to use
sub setSeparator {
	my WhereClause $self = shift;
	my $newSep = shift;
	
	if (($newSep =~ /and/i) || ($newSep =~ /or/i )) {
		#print "legal separator";
		
		$self->{separator} = $newSep;
		
	} else {  print "illegal separator"; }

}


# adds an item to the where clause.
sub addItem {
	my WhereClause $self = shift;
	my $item = shift;

	push(@{$self->{items}}, $item);
}


# returns a list of all the items.
sub items {
	my WhereClause $self = shift;
	return @{$self->{items}};
}


# removes all items
sub clear {
	my WhereClause $self = shift;
	$self->{items} = ();
}


# forms the where clause with the user defined separator (and, or, etc..) and
# returns it.
sub whereClause {
	my WhereClause $self = shift;

	my @itemsList = $self->items();
	
	#print "itemsList : @itemsList\n";
	
	my $clause = join(" " . $self->separator() . " ", @itemsList);	
	
	#print "clause : $clause\n";
	return $clause;
}




1;