# Represents an SQL select statement.  May be expanded in the future or replaced with a CPAN module...
# Note, although this only supports SELECT statements by setting direct SQL.  To insert, call
# the insertNewRecord() method.
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
# To execute an SQL expression:
# -----------------------------
# 
# 1.  If you'll be retrieving one row at a time, then you must call
#     the executeSQL() method before retrieving any rows, and you must
#	  call the finishSQL() method when you're done. Note, fetching one row
#	  at a time is slow.
#
# 2.  If you'll be retrieving all rows at once, then you can just directly
# 	  call the retrieval method without calling execute or finish.
#
# To execute an SQL expression using Permissions checking, make sure to pass the current
# Session object when creating the SQLBuilder object, ie, pass it in new().
# Then use the appropriate permission sensitive methods to fetch the results.  You must
# also include the collection_no as the *FIRST* item in the SELECT statement if you're
# fetching them as an array.  If it's as a hash, then it doesn't matter where it is.
#
#


package SQLBuilder;

use strict;
use Permissions;
use DBConnection;
use Debug;
use Session;
use Globals;
use CGI::Carp qw(fatalsToBrowser);


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
				
				tableNames
							);  # list of allowable data fields.

# dbh				:	handle to the database
# session			:	optional session object, for use with the Permissions object.
# perm				:	Permissions object
# SQLExpr			:	the entire SQL expression *if* the user set it explicitly

# tableNames		:	array ref of all table names in the database, not set by default.
							
							
# If using permissions, or performing updates or inserts, then
# you must pass the current Session object
# when calling new().  If not using permissions, update, or inserts
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

# pass it a session.
sub setSession {
	my SQLBuilder $self = shift;
	my $s = shift;
	
	$self->{session} = $s;	
}


# directly set the entire SQL expression
# note, doing this will *override* any of the other
# set/get methods.. 
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
sub whereSeparator() {
	my SQLBuilder $self = shift;
	return $self->{whereSeparator};	
}

# set which separator to use
sub setWhereSeparator($) {
	my SQLBuilder $self = shift;
	my $newSep = shift;
	
	if (($newSep =~ /and/i) || ($newSep =~ /or/i )) {	
		$self->{whereSeparator} = $newSep;
	} else {  print "illegal separator"; }
}


# adds an item to the where clause.
# does not allow addition of empty items
sub addWhereItem($) {
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



##################################################################################
# Portions which actually talk to the MySQL database
##################################################################################


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


# returns the DBI Statement Handle
# for those cases when it's simpler to directly use the statement handle
#
# note, this may be empty if you have not prepared anything yet.
sub sth {
	my SQLBuilder $self = shift;
	return $self->{sth};
}

# returns the DBI Database Handle
# for those cases when it's simpler to directly use the database handle
sub dbh {
	my SQLBuilder $self = shift;
	
	return $self->dbConnect();
}


# pass this an entire SQL query string
# and it will return a single result
# (ie, assumes that there aren't multiple rows)
# Note, this doesn't return a result row, it just
# returns the first element of the result.
#
# clearly - don't need to call execute() before using this one.
sub getSingleSQLResult {
	my SQLBuilder $self = shift;

	my $sql = shift;
	my $dbh = $self->{dbh};
	return ($dbh->selectrow_array($sql))[0];
}


# executes the SQL
# can use this with or without permissions
#
# only used for method which fetch one row at a time.
# methods which fetch all rows don't need this.
sub executeSQL {
	my SQLBuilder $self = shift;

	my $dbh = $self->{dbh};
	my ($sql, $sth);

	# if the user had already run a query, then $sth will
	# exist, so we should call finish() on it to clean up.
	$sth = $self->{sth};
	if ($sth) { $sth->finish(); }

	my $sql = $self->SQLExpr();
			
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	
	# save the sth for later use in fetching rows.
	$self->{sth} = $sth;
}


# Returns the entire result set as a hash with NO permissions checking
# optionally, pass it a key field to organize the results by
sub allResultsHashRef {
	my SQLBuilder $self = shift;
	my $key = shift;
	
	my $sql = $self->SQLExpr();
	
	# fetch all array rows 	
	my $ref = ($self->{dbh})->selectall_hashref($sql, $key);
	
	return $ref;
}


# Returns the entire result set as a matrix with NO permissions checking
#
# ****SPEED NOTE****
# This method is much faster than nextResultArray() if you
# have many result rows to fetch.
sub allResultsArrayRef {
	my SQLBuilder $self = shift;
	
	my $sql = $self->SQLExpr();
	
	# fetch all array rows 	
	my $ref = ($self->{dbh})->selectall_arrayref($sql);
	
	return $ref;
}




# Returns the entire result set as a matrix using read permissions
# checking on all rows.
#
# SQL Query **must** include "collection_no" as the first column.
#
# ****SPEED NOTE****
# This method is much faster than nextResultArrayUsingPermissions() if you
# have many result rows to fetch.
sub allResultsArrayRefUsingPermissions {
	my SQLBuilder $self = shift;
	
	my $sql = $self->SQLExpr();
	
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


# returns the next result row using read permissions as an array
#
# Also, **make sure** the SQL query includes the necessary
# column called "collection_no" AS THE FIRST column **********!!!
#
# must be called *after* first calling executeSQL(),
# otherwise will return empty array.
#
# ****SPEED NOTE****
# If you will be retrieving multiple rows, this is about twice as slow
# as using the allResultsArrayRefUsingPermissions() method which returns them
# all at once.  So, only use this one if there will only be a few rows.
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



# fetches the next result row WITHOUT using permissions of any kind.
# returns it as an array
#  
# must be called *after* first calling executeSQL(),
# otherwise will return empty array.
#
# ****SPEED NOTE****
# This is slow compared to getting all results at once.
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
#
# ****SPEED NOTE****
# slower than getting all results at once.
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


# as an hash reference
#
# ****SPEED NOTE****
# slower than getting all results at once.
sub nextResultHashRef {
	my SQLBuilder $self = shift;

	my $sth = $self->{sth};
	
	if (! $sth) {
		return ();  # return empty array if sth doesn't exist	
	}
	
	# fetch the next array row and return it.	
	my $result = $sth->fetchrow_hashref();
	$self->{sth} = $sth;  # important - save it back in the parameter.
	
	if (! $result) {
		$self->finishSQL();
	}
		
	return $result;
}




# Clean up after finishing with a query.
# should be called when the user is done accessing result sets from a query.
#
# Only necessary for methods which fetch one result row at a time.
# Method which fetch them all at once don't need to worry about this.
sub finishSQL {
	my SQLBuilder $self = shift;

	my $sth = $self->{sth};
	if ($sth) {
		$sth->finish;
		$self->{sth} = undef;
	}
}




#####################################################################
# Inserts, updates, etc.
#####################################################################


# for internal use only
# Pass it a table name, and it returns a reference to an array of array refs, one per col.
# Basically grabs the table description from the database.
#
sub getTableDesc {
	my SQLBuilder $self = shift;
	
	my $tableName = shift;
	
	if (! $self->isValidTableName($tableName)) {
		return 0;	
	}

	my $sql = "DESC $tableName";
	my $ref = ($self->{dbh})->selectall_arrayref($sql);

	return $ref;
}


# for internal use only, doesn't return anything
#
# simply grabs a list of all table names in the database
# and stores it in a member variable.
sub populateTableNames {
	my SQLBuilder $self = shift;

	my $sql = "SHOW TABLES";
	my $ref = ($self->{dbh})->selectcol_arrayref($sql);

	$self->{tableNames} = $ref;
}

# pass this a table name string and it will
# return a true or false value dependent on the existence of that
# table in the database.
sub isValidTableName {
	my SQLBuilder $self = shift;
	
	my $tableName = shift;
	
	if (! $self->{tableNames}) {
		# then call a method to figure out the names.
		$self->populateTableNames();
	}
	
	my $success = 0;
	my $tableNames = $self->{tableNames};
	foreach my $name (@$tableNames) {
		if ($tableName eq $name) {
			$success = 1;
		}
	}
	
	return $success;
}


# rjp, 2/2004.
#
# Pass it a table name such as "occurrences", and a
# hash ref - the hash should contain keys which correspond
# directly to column names in the database.  Note, not all columns need to 
# be listed; only the ones which you are inserting data for.
#
# Returns a true value for success, or false otherwise.
# Note, if it makes it to the insert statement, then it returns
# the result code from the dbh->do() method.
#
#
# Note, for now, this just blindly inserts whatever the user passes.
# However, in the future, since insert is an operation which won't occur very 
# often (and not in a loop), we should do some more checking.
# For example, we can make sure that NOT NULL fields are set before performing the
# insert.. We can check the column types to make sure they match (integers, varchars, etc.).
# We can make sure that the autoincrement fields aren't overwritten, etc..
# 
# Add this eventually..
#
#
sub insertNewRecord {
	my SQLBuilder $self = shift;
	my $tableName = shift;
	my $hashRef = shift;

	
	# make sure they're allowed to insert data!
	my $s = $self->{session};
	if (!$s || $s->get('enterer') eq 'Guest' || $s->get('enterer') eq '') {
		Debug::logError("invalid session or enterer in SQLBuilder::insertNewRecord");
		return;
	}
	
	
	# make sure the table name is valid
	if ((! $self->isValidTableName($tableName)) || (! $hashRef)) {
		return 0;	
	}
	
	# loop through each key in the passed hash and
	# build up the insert statement.
	
	# get the description of the table
	my @desc = @{$self->getTableDesc($tableName)};
	my @colName; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colName, $row->[0]);	
	}
	
	my $toInsert = "";
	
	my @keys = keys(%$hashRef);
	foreach my $key (@keys) {
		
		# if it's a valid column name, then add it to the insert
		if (Globals::isIn(\@colName, $key)) {
			$toInsert .= "$key = '" . $hashRef->{$key} . "', ";
		}
	}
	
	# remove the trailing comma
	$toInsert =~ s/, $/ /;
	
	$toInsert = "INSERT INTO $tableName SET " . $toInsert;
	
	# actually insert into the database
	my $insertResult = $self->{dbh}->do($toInsert);
		
	# return the result code from the do() method.
	return $insertResult;
}




# rjp, 3/2004.
#
# Pass it a table name, a hashref of key/value pairs to update, 
# a where clause so we know which records to update, and the primary
# key name for this table.  Note, not all columns need to 
# be listed; only the ones which you are inserting data for.
#
# Returns a true value for success, or false otherwise.
# Note, if it makes it to the update statement, then it returns
# the result code from the dbh->do() method.
#
# see also updateRecordEmptyFieldsOnly() below.
#
# ****NOTE**** this doesn't check write permissions yet...
# WARNING!!
#
sub updateRecord {
	my SQLBuilder $self = shift;
	
	my $tableName = shift;
	my $hashRef = shift;
	my $whereClause = shift;
	my $primaryKey = shift;
	
	$self->internalUpdateRecord(0, $tableName, $hashRef, $whereClause, $primaryKey);

}


# same as updateRecord(), but only 
# updates the fields which are empty or zero in the table row..
# ie, if you pass it a field which is already populated in this database
# row, then it won't update that field.
sub updateRecordEmptyFieldsOnly {
	my SQLBuilder $self = shift;
	
	my $tableName = shift;
	my $hashRef = shift;
	my $whereClause = shift;
	my $primaryKey = shift;
	
	$self->internalUpdateRecord(1, $tableName, $hashRef, $whereClause, $primaryKey);
}


# for internal use only!!
#
# Pass it a boolean which will determine whether we 
# should only update empty columns.  True (1) means that 
# we should only update empty columns, false (0) means update any column.
#
# Also pass it a table name, a hashref of key/value pairs to update, 
# a where clause so we know which records to update, and the primary
# key name for this table.
#
# Note: we could grab the primary key from the database, but I haven't figured
# out how to do that yet, so for now, we'll just pass it.
#
# rjp, 3/2004.
sub internalUpdateRecord {
	my SQLBuilder $self = shift;
	my $emptyOnly = shift;
	my $tableName = shift;
	my $hashRef = shift;
	my $whereClause = shift;
	my $primaryKey = shift;
	
	# make sure they're allowed to update data!
	my $s = $self->{session};
	if (!$s || $s->get('enterer') eq 'Guest' || $s->get('enterer') eq '')	{
		Debug::logError("invalid session or enterer in SQLBuilder::internalUpdateRecord");
		return;
	}
	
	# make sure the whereClause and tableName aren't empty!  That would be bad.
	if (($tableName eq '') || ($whereClause eq '')) {
		return 0;	
	}
	
	# make sure the table name is valid
	if ((! $self->isValidTableName($tableName)) || (! $hashRef)) {
		return 0;	
	}
	

	# loop through each key in the passed hash and
	# build up the update statement.
	
	# get the description of the table
	my @desc = @{$self->getTableDesc($tableName)};
	my @colName; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colName, $row->[0]);	
	}
	
	
	my $toUpdate;
	
	
	if ($emptyOnly) {
		# only allow updates of fields which are already empty.
		
		# fetch all of the rows we're going to try to update from the database
		my $select = SQLBuilder->new();
		$select->setSQLExpr("SELECT * FROM $tableName WHERE $whereClause");
		my $selResults = $select->allResultsHashRef($primaryKey);
	
		# loop through each row that we're going to try and update
		foreach my $row (keys(%$selResults)) {
		
			$toUpdate = '';
		
			# loop through each key (column) that the user want's to
			# update.
			my @keys = keys(%$hashRef);
			foreach my $key (@keys) {
		
				# if it's a valid column name, and the column is empty,
				# then add it to the update statement.
				if (Globals::isIn(\@colName, $key)) {
					
					# if the column in the database for this row is
					# already empty...
					my $dbcolval = ($selResults->{$row})->{$key};
					if (($dbcolval eq '') || ($dbcolval == 0)) {
						# then add the key to the update statement.
						$toUpdate .= "$key = '" . $hashRef->{$key} . "', ";
					}
				}	
			}
		
		
			if (! $toUpdate) {  # if there's nothing to update, skip to the next one.
				next;
			}
			
			# remove the trailing comma
			$toUpdate =~ s/, $/ /;
	
			$toUpdate = "UPDATE $tableName SET $toUpdate WHERE $whereClause";
	
			# actually update the row in the database
			my $updateResult = $self->{dbh}->do($toUpdate);	
		}
		
	} else {
		# update any field, doesn't matter if it's empty or not
	
		
		$toUpdate = '';
		
		my @keys = keys(%$hashRef);
		foreach my $key (@keys) {
			
			# if it's a valid column name, then add it to the update
			if (Globals::isIn(\@colName, $key)) {
					$toUpdate .= "$key = '" . $hashRef->{$key} . "', ";
			}
		}
		
		# remove the trailing comma
		$toUpdate =~ s/, $/ /;
		
		$toUpdate = "UPDATE $tableName SET $toUpdate WHERE $whereClause";
		
		# actually update the row in the database
		my $updateResult = $self->{dbh}->do($toUpdate);
			
		# return the result code from the do() method.
		return $updateResult;
	
	}
	
	
}



# end of SQLBuilder.pm

1;