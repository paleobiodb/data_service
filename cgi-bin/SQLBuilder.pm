# Represents an SQL select statement.  May be expanded in the future or replaced with a CPAN module...
# Note, although this *ONLY WORKS FOR SELECT STATEMENTS* for now, it may eventually be expanded
# to include other types of statements such as UPDATE, etc.
#
# Written by rjp, 1/2004.
#
# Each section of the SQL statement can either be built up one component at a time, or set all at once.
# (note, building up is only supported for the WHERE expression for now)
#
# To set it all at once, use the appropriate setXXXExpr() method.
# Setting an expression directly will override any expression which has been built up from components.
#
# To build the where expression from components, set the separator using setWhereSeparator(),
# and then use addWhereItem() to add each component.  
#
# You can also access any expression directly by using the method of its name, for example, whereExpr(),
# or you can access the entire SQL statement by using the SQLExpr() method.
#
# Note, expressions are *NOT* returned with the leading WHERE, SELECT, HAVING, etc. keywords *unless*
# you request the entire SQL statement at once.

package SQLBuilder;
use strict;
use fields qw(	selectExpr
				fromExpr
				whereExpr
				groupByExpr
				havingExpr
				orderByExpr
				limitExpr
				
				whereSeparator
				whereItems
							);  # list of allowable data fields.



sub new {
	my $class = shift;
	my SQLBuilder $self = fields::new($class);
	
	# set up some default values
	$self->clear();	

	return $self;
}


# clears everything
sub clear {
	my SQLBuilder $self = shift;
	$self->{selectExpr} = '';
	$self->{fromExpr} = '';
	$self->{groupByExpr} = '';
	$self->{havingExpr} = '';
	$self->{orderByExpr} = '';
	$self->{limitExpr} = '';
	$self->{whereSeparator} = 'and';
	$self->{whereItems} = ();
}

# set directly
sub setSelectExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{selectExpr} = $input;
	}
}

sub selectExpr {
	my SQLBuilder $self = shift;
	if ($self->{selectExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{selectExpr};	
	}
	
	return "";
}


# set directly
sub setFromExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{fromExpr} = $input;
	}
}

sub fromExpr {
	my SQLBuilder $self = shift;
	if ($self->{fromExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{fromExpr};	
	}
	
	return "";
}


## WHERE ##

# set directly
sub setWhereExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{whereExpr} = $input;
	}
}

# returns the separator
sub whereSeparator {
	my SQLBuilder $self = shift;
	return $self->{whereSeparator};	
}

# set which separator to use
sub setWhereSeparator {
	my SQLBuilder $self = shift;
	my $newSep = shift;
	
	if (($newSep =~ /and/i) || ($newSep =~ /or/i )) {	
		$self->{whereSeparator} = $newSep;
	} else {  print "illegal separator"; }
}


# adds an item to the where clause.
# does not allow addition of empty items
sub addWhereItem {
	my SQLBuilder $self = shift;
	my $item = shift;

	if (($item) && ($item ne " ")) {
		push(@{$self->{whereItems}}, $item);
	}
}


# returns a list of all the items.
sub whereItems {
	my SQLBuilder $self = shift;
	if ($self->{whereItems}) {
		return @{$self->{whereItems}};
	} else  {
		return ();
	}
}


# forms the where expression with the user defined 
# separator (and, or, etc..) and returns it.
# Note, if the user has explicitly set the whereExpr with the setWhereExpr() method,
# then this value will OVERRIDE anything set by using addWhereItem().
sub whereExpr {
	my SQLBuilder $self = shift;

	if ($self->{whereExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{whereExpr};	
	}
	
	# if we get to here, then they *didn't* set it explicitly, so we have to build it.
	my @itemsList = $self->whereItems();
		
	my $expr = join(" " . $self->whereSeparator() . " ", @itemsList);	
	
	return $expr;
}

# set directly
sub setGroupByExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{groupByExpr} = $input;
	}
}

sub groupByExpr {
	my SQLBuilder $self = shift;
	if ($self->{groupByExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{groupByExpr};
	}
	
	return "";
}

# set directly
sub setHavingExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{havingExpr} = $input;
	}
}

sub havingExpr {
	my SQLBuilder $self = shift;
	if ($self->{havingExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{havingExpr};	
	}
	
	return "";	
}

# set directly
sub setOrderByExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{orderByExpr} = $input;
	}
}

sub orderByExpr {
	my SQLBuilder $self = shift;
	if ($self->{orderByExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{orderByExpr};	
	}
	
	return "";
}

# set directly
sub setLimitExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{limitExpr} = $input;
	}
}

sub limitExpr {
	my SQLBuilder $self = shift;
	if ($self->{limitExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{limitExpr};	
	}
	
	return "";	
}


# return the *entire* SQL statement.  Note, this won't work if something crucial such as
# selectExpr is left blank.  Well, it will work, but the result won't be a well formed query.
sub SQLExpr {
	my SQLBuilder $self = shift;
	my $expr = "";

	$expr .= " SELECT " .	$self->selectExpr()		if $self->selectExpr();
	$expr .= " FROM "  .	$self->fromExpr()		if $self->fromExpr();
	$expr .= " WHERE " .	$self->whereExpr()		if $self->whereExpr();
	$expr .= " GROUP BY " .	$self->groupByExpr()	if $self->groupByExpr();
	$expr .= " HAVING " .	$self->havingExpr()		if $self->havingExpr();
	$expr .= " ORDER BY " .	$self->orderByExpr()	if $self->orderByExpr();
	$expr .= " LIMIT " .	$self->limitExpr()		if $self->limitExpr();
	
	return $expr;
}

1;