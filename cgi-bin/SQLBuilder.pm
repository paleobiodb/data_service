# Represents an SQL select statement.  May be expanded in the future or replaced with a CPAN module...
# Note, although this *ONLY WORKS FOR SELECT STATEMENTS* for now, it may eventually be expanded
# to include other types of statements such as UPDATE, etc.
#
# Written by rjp, 1/2004.
#
# Each section of the SQL statement can either be built up one component at a time, or set all at once.
# (**note, building up is only supported for the WHERE expression for now, but eventually for others)
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
#
# To execute an SQL expression, just call the executeSQL() method *after* you have set the SQL statement
# To retrieve the results, call nextResultArray() repeatedly, and call finishSQL() when finished.
#
# To execute an SQL expression using Permissions checking, make sure to pass the current
# Session object when creating the SQLBuilder object, ie, pass it in new().
# Then use the appropriate permission sensitive methods to fetch the results.

package SQLBuilder;

use strict;
use Permissions;
use DBConnection;
use Debug;
use Session;

use fields qw(	
				dbh
				sth
				perm
				session
								
				SQLExpr
				selectExpr
				fromExpr
				whereExpr
				groupByExpr
				havingExpr
				orderByExpr
				limitExpr
				
				whereSeparator
				whereItems
							);  # list of allowable data fields.

# dbh				:	handle to the database
# session			:	optional session object, for use with the Permissions object.
# perm				:	Permissions object
# SQLExpr			:	the entire SQL expression *if* the user set it explicitly

							
# If using permissions, then
# you must pass the current Session object
# when calling new().  If not using permissions,
# then this is optional. 
sub new {
	my $class = shift;
	my SQLBuilder $self = fields::new($class);
	
	my $session = shift;	# optional parameter

	# set up some default values
	$self->clear();	

	if ($session) {
		$self->{session} = $session;
	
		# create the permissions object as well
		# note, if the session object didn't really exist, then
		# the permissions object will catch the error, so don't have to check for it here.
		my $perm = Permissions->new($session);
		$self->{perm} = $perm;
	}
	
	# connect to the database
	# note, not sure if it's a good idea to do this every time.. might slow things down.
	$self->dbConnect();
	
	return $self;
}


# clears everything
sub clear {
	my SQLBuilder $self = shift;
	
	$self->{SQLExpr} = '';
	$self->{selectExpr} = '';
	$self->{fromExpr} = '';
	$self->{groupByExpr} = '';
	$self->{havingExpr} = '';
	$self->{orderByExpr} = '';
	$self->{limitExpr} = '';
	$self->{whereSeparator} = 'and';
	$self->{whereItems} = ();
	
	$self->finishSQL();  # finish the SQL query if there was one.
}



# directly set the entire SQL expression
# note, doing this will *override* any of the other
# set/get methods.. Only use in weird cases.
# not a recommended way of doing it..
sub setSQLExpr {
	my SQLBuilder $self = shift;
	if (my $input = shift) {
		$self->{SQLExpr} = $input;
	}	
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

# clears the list of where items
sub clearWhereItems {
	my SQLBuilder $self = shift;
	$self->{whereItems} = ();
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
	if ($self->{SQLExpr}) {
		# then the user set the SQL expression directly (not recommended)
		return ($self->{SQLExpr});
	}
	
	# otherwise, if the user didn't set the expresion explicitly,
	# then form it from the components.

	$expr .= " SELECT " .	$self->selectExpr()		if $self->selectExpr();
	$expr .= " FROM "  .	$self->fromExpr()		if $self->fromExpr();
	$expr .= " WHERE " .	$self->whereExpr()		if $self->whereExpr();
	$expr .= " GROUP BY " .	$self->groupByExpr()	if $self->groupByExpr();
	$expr .= " HAVING " .	$self->havingExpr()		if $self->havingExpr();
	$expr .= " ORDER BY " .	$self->orderByExpr()	if $self->orderByExpr();
	$expr .= " LIMIT " .	$self->limitExpr()		if $self->limitExpr();
	
	return $expr;
}


# for internal use only!
# connects to MySQL database if needed (ie, if we weren't already connected)
# returns handle to the database
sub dbConnect {
	my SQLBuilder $self = shift;
	
	my $dbh = $self->{dbh};
	
	if (! $dbh) {
		# connect if needed
		$dbh = DBConnection::connect();
		$self->{dbh} = $dbh;
	}
	
	return $dbh;	
}


# pass this an entire SQL query string
# and it will return a single result
# (ie, assumes that there aren't multiple rows)
#
# clearly - don't need to call execute() before using this one.
sub getSingleSQLResult {
	my SQLBuilder $self = shift;

	my $sql = shift;
	my $dbh = $self->{dbh};
	return $dbh->selectrow_array($sql);
}


# executes the SQL
# can use this with or without permissions
#
# note, not needed for all methods, for example, getSingleSQLResult() doesn't use this.
sub executeSQL {
	my SQLBuilder $self = shift;

	my $dbh = $self->{dbh};
	my ($sql, $sth);

	# if the user had already run a query, then $sth will
	# exist, so we should call finish() on it to clean up.
	$sth = $self->{sth};
	if ($sth) { $sth->finish(); }

	my $sql = $self->SQLExpr();
			
	$sth = $dbh->prepare( $sql );
	$sth->execute();
	
	# save the sth for later use in fetching rows.
	$self->{sth} = $sth;
}


# Pass this an SQL query.
# Returns the entire result set as a matrix using read permissions
# checking on all rows.
#
# SQL Query **must** include "collection_no" as the first column.
sub allResultsArrayRefUsingPermissions {
	my SQLBuilder $self = shift;
	
	my $sql = shift;
	
	if ( (! $self->{perm}) || (! $self->{session})) {
		Debug::logError("SQLBuilder must have valid permissions and sessions objects to execute this query.");
		return undef; 	
	}
	
	
	my $result; 	# that we'll return
	
	# fetch all array rows 	
	my $ref = ($self->{dbh})->selectall_arrayref($sql);
	
	
	# loop through result set
	if (defined $ref) {
		foreach my $row (@{$ref}) {
			# collection_no *must* be the first column for this to work
			my $collection_no = $row->[0];
			
			# check permissions
			if (($self->{perm})->userHasReadPermissionForCollectionNumber($collection_no)) {
				push(@$result, $row);	# add it on to the return result.
			} 
		}
	}
	
	
	return $result;

}


# returns an array of result rows using permissions
# to check if the user can read the rows.
#
# Also, **make sure** the SQL query includes the necessary
# column called "collection_no" AS THE FIRST column **********!!!
#
sub nextResultArrayUsingPermissions {
	my SQLBuilder $self = shift;
	
	my $sth = $self->{sth};
	
	if ((! $sth) || (! $self->{perm}) || (! $self->{session})) {
		Debug::logError("SQLBuilder must have valid permissions and sessions objects to execute this query.");
		return (); 	
	}
	
	my @result;
	
	# fetch the next array row and return it.	
	@result = $sth->fetchrow_array();
	$self->{sth} = $sth;  # important - save it back in the parameter.
	
	if (! @result) {
		$self->finishSQL();
		return ();		# return empty array
	}
	
	# collection_no *must* be the first column for this to work.
	my $collection_no = $result[0];
	
	if (($self->{perm})->userHasReadPermissionForCollectionNumber($collection_no)) {
		return @result;
	} else {
		# user is not allowed to read this row.
		# so return the next row.. if it exists
		return ($self->nextResultArrayUsingPermission($collection_no));
	}
			
}



# fetches the next result row from the $sth.
# ** doesn't use permissions checking **
# returns it as an array
#  
# must be called *after* first calling executeSQL(),
# otherwise will return empty arry.
sub nextResultArray {
	my SQLBuilder $self = shift;

	my $sth = $self->{sth};
	
	if (! $sth) {
		return ();  # return empty array if sth doesn't exist	
	}
	
	my @result;
	
	# fetch the next array row and return it.	
	@result = $sth->fetchrow_array();
	$self->{sth} = $sth;  # important - save it back in the parameter.
	
	if (! @result) {
		$self->finishSQL();
	}
		
	return @result;
}

# same as nextResultArray, but gets it
# as an array reference
sub nextResultArrayRef {
	my SQLBuilder $self = shift;

	my $sth = $self->{sth};
	
	if (! $sth) {
		return ();  # return empty array if sth doesn't exist	
	}
	
	# fetch the next array row and return it.	
	my $result = $sth->fetchrow_arrayref();
	$self->{sth} = $sth;  # important - save it back in the parameter.
	
	if (! $result) {
		$self->finishSQL();
	}
		
	return $result;
}



# clean up after finishing with a query.
# should be called when the user is done accessing result sets from a query.
sub finishSQL {
	my SQLBuilder $self = shift;

	my $sth = $self->{sth};
	if ($sth) {
		$sth->finish;
		$self->{sth} = undef;
	}
}


# end of SQLBuilder.pm

1;