## 3/30/2004 - merged functionality of SQLBuilder and DBTransactionManager
## Methods at the top of this class were written by rjp, methods at the bottom
## by the original author of DBTransactionManager.


# Represents an SQL select statement.  May be expanded in the future or replaced with a CPAN module...
# Note, although this only supports SELECT statements by setting direct SQL.  To insert, call
# the insertNewRecord() method.
#
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
# Session object when creating the DBTransactionManager object, ie, pass it in new().
# Then use the appropriate permission sensitive methods to fetch the results.  You must
# also include the collection_no as the *FIRST* item in the SELECT statement if you're
# fetching them as an array.  If it's as a hash, then it doesn't matter where it is.
#
#


package DBTransactionManager;

use strict;
use Permissions;
use DBConnection;
use Debug;
use Session;
use Globals;
use Class::Date qw(date localdate gmdate now);
use CGI::Carp;
use CGI;


use fields qw(	
				dbh
				sth
				perm
				GLOBALVARS
				session
				
				_id
				_err
								
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
				
				maxNumUpdatesAllowed
							);  # list of allowable data fields.

# dbh				:	handle to the database
# GLOBALVARS			:	optional GLOBAL hash, see top of bridge.pl for more info.
# perm				:	Permissions object
# SQLExpr			:	the entire SQL expression *if* the user set it explicitly

# tableNames		:	array ref of all table names in the database, not set by default.
#
# _id, _err 		: 	from the old DBTransactionManager
#
							
# maxNumUpdatesAllowed	: how many updates should we allow at the same time?
		

# If using permissions, or performing updates or inserts, then
# you must pass the GLOBALVARS hashref to it
# when calling new().  If not using permissions, update, or inserts
# then this is optional. 
sub new {
	my $class = shift;
	my DBTransactionManager $self = fields::new($class);
	
	my $GLOBALVARS = shift;  # optional parameter
	$self->{GLOBALVARS} = $GLOBALVARS;
    
	# set up some default values
	$self->clear();	

	if ($GLOBALVARS && $GLOBALVARS->{session}) {
	    $self->{session} = $GLOBALVARS->{session};
	    
		# create the permissions object as well
		# note, if the session object didn't really exist, then
		# the permissions object will catch the error, so don't have to check for it here.
		my $perm = Permissions->new($GLOBALVARS->{session});
		$self->{perm} = $perm;
	}
	
	# connect to the database
	# note, not sure if it's a good idea to do this every time.. might slow things down.
	$self->dbConnect();
	
	# don't allow more than 50 simultaneous updates
	$self->{maxNumUpdatesAllowed} = 10;
	
	
	return $self;
}


# clears everything
sub clear {
	my DBTransactionManager $self = shift;
	
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
# deprecated.
sub setSession {
	my DBTransactionManager $self = shift;
	my $s = shift;
	
	$self->{session} = $s;	
}


# directly set the entire SQL expression
# note, doing this will *override* any of the other
# set/get methods.. 
sub setSQLExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{SQLExpr} = $input;
	}	
}

# set directly
sub setSelectExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{selectExpr} = $input;
	}
}

sub selectExpr {
	my DBTransactionManager $self = shift;
	if ($self->{selectExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{selectExpr};	
	}
	
	return "";
}


# set directly
sub setFromExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{fromExpr} = $input;
	}
}

sub fromExpr {
	my DBTransactionManager $self = shift;
	if ($self->{fromExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{fromExpr};	
	}
	
	return "";
}


## WHERE ##

# set directly
sub setWhereExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{whereExpr} = $input;
	}
}

# returns the separator
sub whereSeparator() {
	my DBTransactionManager $self = shift;
	return $self->{whereSeparator};	
}

# set which separator to use
sub setWhereSeparator($) {
	my DBTransactionManager $self = shift;
	my $newSep = shift;
	
	if (($newSep =~ /and/i) || ($newSep =~ /or/i )) {	
		$self->{whereSeparator} = $newSep;
	} else {  print "illegal separator"; }
}


# adds an item to the where clause.
# does not allow addition of empty items
sub addWhereItem($) {
	my DBTransactionManager $self = shift;
	my $item = shift;

	if (($item) && ($item ne " ")) {
		push(@{$self->{whereItems}}, $item);
	}
}

# clears the list of where items
sub clearWhereItems {
	my DBTransactionManager $self = shift;
	$self->{whereItems} = ();
}

# returns a list of all the items.
sub whereItems {
	my DBTransactionManager $self = shift;
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
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{groupByExpr} = $input;
	}
}

sub groupByExpr {
	my DBTransactionManager $self = shift;
	if ($self->{groupByExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{groupByExpr};
	}
	
	return "";
}

# set directly
sub setHavingExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{havingExpr} = $input;
	}
}

sub havingExpr {
	my DBTransactionManager $self = shift;
	if ($self->{havingExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{havingExpr};	
	}
	
	return "";	
}

# set directly
sub setOrderByExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{orderByExpr} = $input;
	}
}

sub orderByExpr {
	my DBTransactionManager $self = shift;
	if ($self->{orderByExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{orderByExpr};	
	}
	
	return "";
}

# set directly
sub setLimitExpr {
	my DBTransactionManager $self = shift;
	if (my $input = shift) {
		$self->{limitExpr} = $input;
	}
}

sub limitExpr {
	my DBTransactionManager $self = shift;
	if ($self->{limitExpr}) {
		# if they set it explicitly, then just return it.
		return $self->{limitExpr};	
	}
	
	return "";	
}


# return the *entire* SQL statement.  Note, this won't work if something crucial such as
# selectExpr is left blank.  Well, it will work, but the result won't be a well formed query.
sub SQLExpr {
	my DBTransactionManager $self = shift;
	
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
	my DBTransactionManager $self = shift;
	
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
	my DBTransactionManager $self = shift;
	return $self->{sth};
}

# returns the DBI Database Handle
# for those cases when it's simpler to directly use the database handle
sub dbh {
	my DBTransactionManager $self = shift;
	
	return $self->dbConnect();
}


# Note - 3/30/2004 - alroy doesn't want us to use this
# method anymore.  He would like us to use getData instead.
#
# pass this an entire SQL query string
# and it will return a single result
# (ie, assumes that there aren't multiple rows)
# Note, this doesn't return a result row, it just
# returns the first element of the result.
#
# clearly - don't need to call execute() before using this one.
sub getSingleSQLResult {
	my DBTransactionManager $self = shift;

	my $sql = shift;
	Debug::dbPrint("getSingleSQLResult here 1, sql = $sql");
	
	my $dbh = $self->{dbh};
	
	return ($dbh->selectrow_array($sql))[0];
}


# executes the SQL
# can use this with or without permissions
#
# only used for method which fetch one row at a time.
# methods which fetch all rows don't need this.
sub executeSQL {
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;
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
	my DBTransactionManager $self = shift;
	
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
	my DBTransactionManager $self = shift;
	
	my $sql = $self->SQLExpr();
	
	if ( (! $self->{perm}) || (! $self->{session})) {
		Debug::logError("DBTransactionManager must have valid permissions and sessions objects to execute this query.");
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
	my DBTransactionManager $self = shift;
	
	my $sth = $self->{sth};
	
	if ((! $sth) || (! $self->{perm}) || (! $self->{session})) {
		Debug::logError("DBTransactionManager must have valid permissions and sessions objects to execute this query.");
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
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;

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
	my DBTransactionManager $self = shift;
	
	my $tableName = shift;
	
	if (! $self->isValidTableName($tableName)) {
		return 0;	
	}

	my $sql = "DESC $tableName";
	my $ref = ($self->{dbh})->selectall_arrayref($sql);

	return $ref;
}


# allowable for external use.
#
# Pass it a table name.
# Returns an arrayref of all column names in this table. 
sub allTableColumns {
	my DBTransactionManager $self = shift;
	
	my $tableName = shift;
	
	
	# it will always be the first row returned since
	# we're just using describe on this table.
	
	my @desc = @{$self->getTableDesc($tableName)};
	my @colNames; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colNames, $row->[0]);	
	}
	
	return \@colNames;
}


# for internal use only, doesn't return anything
#
# simply grabs a list of all table names in the database
# and stores it in a member variable.
sub populateTableNames {
	my DBTransactionManager $self = shift;

	my $sql = "SHOW TABLES";
	my $ref = ($self->{dbh})->selectcol_arrayref($sql);

	$self->{tableNames} = $ref;
}

# pass this a table name string and it will
# return a true or false value dependent on the existence of that
# table in the database.
sub isValidTableName {
	my DBTransactionManager $self = shift;
	
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
# Returns an array.  First element is esult code from the dbh->do() method,
# second element is primary key value of the last insert (very useful!).
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
	my DBTransactionManager $self = shift;
	my $tableName = shift;
	my $hashRef = shift;  # don't update this
	
	# make our own local copy of the hash so we can modify it.
	my $fields = Globals::copyHash($hashRef);

	my $dbh = $self->{dbh};
	
	# make sure they're allowed to insert data!
	my $s = $self->{session};
	if (!$s || $s->guest() || $s->get('enterer') eq '') {
		Debug::logError("invalid session or enterer in DBTransactionManager::insertNewRecord");
		return;
	}
	
	# make sure the table name is valid
	if ((! $self->isValidTableName($tableName)) || (! $fields)) {
		return 0;	
	}
	
	# when inserting records, we should always set the created date to now()
	# and make sure the authorizer/authorizer_no and enterer/enterer_no are set.
	$fields->{authorizer} = $s->get('authorizer');
	$fields->{authorizer_no} = $s->authorizerNumber();
	$fields->{created} = now();
	
	# loop through each key in the passed hash and
	# build up the insert statement.
	
	# get the description of the table
	my @desc = @{$self->getTableDesc($tableName)};
	my @colName; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colName, $row->[0]);	
	}
	
	my $toInsert = "";
	
	my @keys = keys(%$fields);
	foreach my $key (@keys) {
		
		# if it's a valid column name, then add it to the insert
		if (Globals::isIn(\@colName, $key)) {
			$toInsert .= "$key = " . $dbh->quote($fields->{$key}) . ", ";
		}
	}
	
	
	# remove the trailing comma
	$toInsert =~ s/, $/ /;
	
	$toInsert = "INSERT INTO $tableName SET " . $toInsert;
	
	
	Debug::printHash($fields);
	Debug::dbPrint("here3, toInsert = $toInsert");
	
	# actually insert into the database
	my $insertResult = $dbh->do($toInsert);
	
	# figure out the id of the last insert, ie, the primary key value.
#	my $idNum = $self->getSingleSQLResult("SELECT LAST_INSERT_ID() as l FROM $tableName");
	# bug fix here by JA 2.4.04
	my $idNum = ${$self->getData("SELECT LAST_INSERT_ID() AS l FROM $tableName")}[0]->{l};
		
	# return the result code from the do() method.
	return ($insertResult, $idNum);
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
	my DBTransactionManager $self = shift;
	
	my $tableName = shift;
	my $hashRef = shift;
	my $whereClause = shift;
	my $primaryKey = shift;
	
	$self->internalUpdateRecord(0, $tableName, $hashRef, $whereClause, $primaryKey);
    
}


# Same as updateRecord(), but only 
# updates the fields which are empty or zero in the table row..
# ie, if you pass it a field which is already populated in this database
# row, then it won't update that field.
#
# Note, it *WILL* allow updates of the modifier_no field since we need to modify that field 
# each time!
# ****NOTE**** this doesn't check write permissions yet...
# WARNING!!
sub updateRecordEmptyFieldsOnly {
	my DBTransactionManager $self = shift;
	
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
# ** Only allows update on one table at a time...
#
# Note, this figures out the modifier_no/modifier name from the session. 
# The modified date is set automatically by the database unless we
# try to override that.  It also prevents users from updating the created date
# and the authorizer/authorizer_no and enterer/entere_no fields since 
# those shouldn't ever be updated.
#
# rjp, 3/2004.
sub internalUpdateRecord {
	my DBTransactionManager $self = shift;
	my $emptyOnly = shift;
	my $tableName = shift;
	my $hashRef = shift;        # don't use this, use $fields instead.
	my $whereClause = shift;
	my $primaryKey = shift;
	
	my $dbh = $self->{dbh};
	
	# we want to make a copy of the hashRef so we can change values in it 
	# without worrying about affecting the original.

	my $fields = Globals::copyHash($hashRef);
	
	Debug::dbPrint("we're in internalUpdateRecord, emptyOnly = $emptyOnly");
    Debug::dbPrint("tableName = $tableName, where = $whereClause, key = $primaryKey");


	# make sure they're allowed to update data!
	my $s = $self->{session};
	if (!$s || $s->guest() || $s->get('enterer') eq '')	{
		Debug::logError("invalid session or enterer in DBTransactionManager::internalUpdateRecord");
		return 0;
	}
	
	# make sure the whereClause and tableName aren't empty!  That would be bad.
	if (($tableName eq '') || ($whereClause eq '')) {
		return 0;	
	}
	
	# make sure the table name is valid
	if ((! $self->isValidTableName($tableName)) || (! $fields)) {
		Debug::logError("invalid tablename or hashref in DBTransactionManager::internalUpdateRecord");
		return 0;	
	}

	
	# figure out the modifer and modifier_no
	# Note, some tables record the modifier_no, and some record the modifer name
	# so we have to set both.  If the field isn't in the table, then it shouldn't
	# hurt anything.
	$fields->{modifier} = $s->get('enterer');
	$fields->{modifier_no} = $s->entererNumber();
	# we never want to change the modified date - we should let the database
	# take care of setting this value correctly.
	delete($fields->{modified});
	
	# Since we're updating a record, we should never change the created
	# date, authorizer, or enterer if present.  So, delete both of those values
	# just to be safe.  These values are only set when we first create a record.
	delete($fields->{created});
	delete($fields->{authorizer});
	delete($fields->{authorizer_no});
	delete($fields->{enterer});
	delete($fields->{enterer_no});
	
	
	# **********
	# note, commented out the below check 3/22/04 because we're always going to be
	# setting the modifier or modifier_no, so we'll never have all empty fields.
	
	# make sure they're actually setting some non empty values
	# in the %fields, otherwise it would be equivalent to deleting the record!
	#{	
	#	my $atLeastOneNotEmpty = 0;
	#	
	#	foreach my $key (keys(%$fields)) {
	#		
	#		if ($fields->{$key}) {
	#			$atLeastOneNotEmpty = 1;
	#		}
	#	}
	#	
	#	if (! $atLeastOneNotEmpty) {
	#		Debug::logError("DBTransactionManager::internalUpdateRecord, tried to update a record with all empty values.");
	#		return 0;
	#	}
	#}
	
	
	Debug::dbPrint("WHERE = $whereClause");
	
	# figure out how many records this update statement will affect
	# return if they try to update too many at once, because it's probably an error.
#	my $count = $self->getSingleSQLResult("SELECT count(*) FROM $tableName WHERE $whereClause");
	my $count = ${$self->getData("SELECT COUNT(*) as c FROM $tableName WHERE
					$whereClause")}[0]->{c};
	
	if ($count > $self->{maxNumUpdatesAllowed}) {
		Debug::logError("DBTransactionManager::internalUpdateRecord, tried to update $count records at once which is more than the maximum allowed of " . $self->{maxNumUpdatesAllowed});
		return;
	}
	


	Debug::printHash($fields);
	
	# loop through each key in the passed hash and
	# build up the update statement.
	
	# get the description of the table
	my @colNames = @{$self->allTableColumns($tableName)};
	
	
	my $toUpdate;
	
	
	if ($emptyOnly) {
		# only allow updates of fields which are already empty.
		
		Debug::dbPrint("empty only update");
		
		# fetch all of the rows we're going to try to update from the database
		my $select = DBTransactionManager->new($self->{GLOBALVARS});
		$select->setSQLExpr("SELECT * FROM $tableName WHERE $whereClause");
		my $selResults = $select->allResultsHashRef($primaryKey);
	
		# loop through each row that we're going to try and update
		foreach my $row (keys(%$selResults)) {
		
			$toUpdate = '';
		
			# loop through each key (column) that the user want's to
			# update.
			my @keys = keys(%$fields);
			foreach my $key (@keys) {
				
				# if it's a valid column name, and the column is empty,
				# then add it to the update statement.
				if (Globals::isIn(\@colNames, $key)) {
				
					# if it's the modifier_no or modifier, then we'll update it regardless
					# of whether the current field in the database is empty or not.
					if (($key eq 'modifier_no') || ($key eq 'modifier')) {
						$toUpdate .= "$key = " . $dbh->quote($fields->{$key}) . ", ";
					}

					
					# if the column in the database for this row is
					# already empty...
					my $dbcolval = ($selResults->{$row})->{$key};
					Debug::dbPrint("dbcolval for $key = $dbcolval");
					if (! ($dbcolval)) {
						Debug::dbPrint("I think dbvolval is empty");
						# then add the key to the update statement.
						$toUpdate .= "$key = " . $dbh->quote($fields->{$key}) . ", ";
					}
				}	
			}
		
		
			if (! $toUpdate) {  # if there's nothing to update, skip to the next one.
				next;
			}
			
			# remove the trailing comma
			$toUpdate =~ s/, $/ /;
	
			if (!$toUpdate) {
				Debug::logError("DBTransactionManager::internalUpdateRecord, tried to update without a set clause in update blanks only.");
				return;
			}
			
			$toUpdate = "UPDATE $tableName SET $toUpdate WHERE $whereClause";
	
			Debug::dbPrint($toUpdate);
			
			# actually update the row in the database
			my $updateResult = $dbh->do($toUpdate);	
		}
		
	} else {
		# update any field, doesn't matter if it's empty or not
	
		Debug::dbPrint("DBTransactionManager:: update any record...");
		
		$toUpdate = '';
		
		Debug::printArray(\@colNames);
		
		my @keys = keys(%$fields);
		foreach my $key (@keys) {
			Debug::dbPrint("key = $key, value = " . $fields->{$key});
			# if it's a valid column name, then add it to the update
			if (Globals::isIn(\@colNames, $key)) {
					$toUpdate .= "$key = " . $dbh->quote($fields->{$key}) . ", ";
			}
		}
		
		# remove the trailing comma
		$toUpdate =~ s/, $/ /;
		
		if (!$toUpdate) {
			Debug::logError("DBTransactionManager::internalUpdateRecord, tried to update without a set clause in update any field.");
			return;
		}
		
		$toUpdate = "UPDATE $tableName SET $toUpdate WHERE $whereClause";
		
		Debug::dbPrint("update = $toUpdate");
		
		# actually update the row in the database
		my $updateResult = $dbh->do($toUpdate);
			
		# return the result code from the do() method.
		return $updateResult;
	
	}
	
	
}


###
## The following methods are from the old DBTransactionManager class which was
## merged with the SQLBuilder class on 3/30/2004.
###



## get_data
# from the old DBTransactionManager class...
#
#	description:	Handles basic SQL syntax checking, 
#					it executes the statement, and it
#					returns data that successfully makes it past the 
#					Permissions module.
#
#	parameters:		$sql			The SQL statement to be executed
#					$type			"read", "write", "both", "neither"
#					$attr_hash-ref	reference to a hash whose keys are set by
#					the user according to which attributes are desired for this
#					table, e.g. "NAME" (required!), "mysql_is_num", "NULLABLE",
#					etc. Upon calling this method, the values will be set as
#					references to arrays whose elements correspond to each 
#					column in the table, in the order of the NAME attribute,
#					which is why "NAME" is required (to correlate order).
#
#	returns:		For select statements, returns a reference to an array of 
#					hash references	to rows of all data returned.
#					For non-select statements, returns the number of rows
#					affected.
#					Returns empty anonymous array on failure.
##
sub getData{
	my DBTransactionManager $self = shift;
	
	my $sql = shift;
#	my $type = (shift or "neither");
	my $attr_hash_ref = shift;

	# First, check the sql for any obvious problems
	my $sql_result = $self->checkSQL($sql);
	if ($sql_result) {
		my $sth = $self->{dbh}->prepare($sql) or die $self->{dbh}->errstr;
		
		# execute returns the number of rows affected for non-select statements.
		# SELECT:
		if ($sql_result == 1) {
			eval { $sth->execute() };
			$self->{_err} = $sth->errstr;
            if ($sth->errstr) { 
                my $errstr = "SQL error: sql($sql)";
                my $q2 = new CGI;
                $errstr .= " sth err (".$sth->errstr.")" if ($sth->errstr);
                $errstr .= " IP ($ENV{REMOTE_ADDR})";
                $errstr .= " script (".$q2->url().")";
                my $getpoststr; my %params = $q2->Vars; my $k; my $v;
                while(($k,$v)=each(%params)) { $getpoststr .= "&$k=$v"; }
                $getpoststr =~ s/\n//;
                $errstr .= " GET,POST ($getpoststr)";
                croak $errstr;
            }    
			# Ok now attributes are accessible
			foreach my $key (keys(%{$attr_hash_ref})){
				$attr_hash_ref->{$key} = $sth->{$key};
			}	
			my @data = @{$sth->fetchall_arrayref({})};
			$sth->finish();
			
# ?? THIS MAY OR MAY NOT BE IMPLEMENTED AS NOTED BELOW (FUTURE RELEASE)
#*******	# Ok now check permissions... **********************************
# - session object.
# - either paste permissions methods in here (modified - without the executes,
#	etc., or make modified versions in Permissions.pm.  NOTE: modified versions
#	in Permissions.pm could be done as copied methods with new names, or adding
#	functionality (conditionals) to the existing methods so they behave 
#	differently depending on how they're called.

			return \@data;
		}
		# non-SELECT:
		else{
			my $num;
			eval { $num = $sth->execute() };
			$self->{_err} = $sth->errstr;
            if ($sth->errstr) { 
                my $errstr = "SQL error: sql($sql)";
                my $q2 = new CGI;
                $errstr .= " sth err (".$sth->errstr.")" if ($sth->errstr);
                $errstr .= " IP ($ENV{REMOTE_ADDR})";
                $errstr .= " script (".$q2->url().")";
                my $getpoststr; my %params = $q2->Vars; my $k; my $v;
                while(($k,$v)=each(%params)) { $getpoststr .= "&$k=$v"; }
                $getpoststr =~ s/\n//;
                $errstr .= " GET,POST ($getpoststr)";
                croak $errstr;
            }    
			# If we did an insert, make the record id available
			if($sql =~ /INSERT/i){
				$self->{_id} = $self->{dbh}->{'mysql_insertid'};
			}
			$sth->finish();
			return $num;
		}
	} # Don't execute anything that doesn't make it past checkSQL
	else{
		return [];
	}
}

# from the old DBTransactionManager class...
sub getID{
	my DBTransactionManager $self = shift;
	return $self->{_id};
}	

# from the old DBTransactionManager class...
sub getErr{
	my DBTransactionManager $self = shift;
	return $self->{_err};
}	


## checkSQL
# from the old DBTransactionManager class...
#
#	description:
#
#	parameters:		$sql	the sql statement to be checked
#
#	returns:	boolean:	0 means bad SQL and nothing was executed (ERROR)
#							1 means SELECT statement; nothing checked.
#							2 means valid INSERT statement
#							3 means valid UPDATE statement
#							4 means valid DELETE statement
##
sub checkSQL{
	my DBTransactionManager $self = shift;
	my $sql = shift;

	# NOTE: This messes with enumerated values
	# uppercase the whole thing for ease of use
	#$sql = uc($sql);

	# Is this a SELECT, INSERT, UPDATE or DELETE?
	$sql =~/^[\(]*(\w+)\s+/;
	my $type = uc($1);

	if($type ne "INSERT" && $type ne "REPLACE" && !$self->checkWhereClause($sql)){
		die "Bad WHERE clause in SQL: $sql";
	}
	
	if($type eq "SELECT"){
		return 1;
	}
	elsif($type eq "INSERT" || $type eq "REPLACE"){
		# Check that the columns and values lists are not empty
		# NOTE: down the road, we could check required fields
		# against table names.
		$sql =~ /(\(.*?\))\s+VALUES\s+(\(.*?\))/i;
		if($1 eq "()" or $1 eq "" or $2 eq "()" or $2 eq "" ){
			return 0;
		}
		else{
			return 2;
		}
	}
	elsif($type eq "UPDATE"){
		# NOTE (FUTURE): on a table by table basis, make sure required 
		# fields aren't blanked out.

		# Avoid full table updates.
		if($sql !~ /WHERE/i){
			return 0;
		}
		return 3;
	}
	elsif($type eq "DELETE"){
		# Try to avoid deleting all records from a table
		if($sql !~ /WHERE/i){
			return 0;
		}
		else{
			return 4;
		}
	}
}


# from the old DBTransactionManager class...
sub checkWhereClause {
	my DBTransactionManager $self = shift;
	# NOTE: disregard the following note...
	# Note: this has already been uppercase-d by the caller.
	my $sql = shift; 
	
	# This method is a 'pass-through' if there is no WHERE clause.
	if($sql !~ /WHERE/i){
		return 1;
	}

	# parenthetical constructions like "WHERE ( x OR y )" aren't going to
	#  be checked fully, so lop off leading open parentheses JA 28.9.03
	$sql =~ s/WHERE\s\(/WHERE /i;

	# This is only 'first-pass' safe. Could be more robust if we check
	# all AND clauses.
	# modified by JA 1.4.04 to accept IS NULL
	# modified by JA 2.28.05 to accept IS AGAINST
	$sql =~ /WHERE\s+([A-Z0-9_\.\(\)]+)\s*(=|AGAINST|LIKE|IN|IS NULL|!=|>|<|>=|<=)\s*(.+)?\s*/i;

	#print "\$1: $1, \$2: $2 \$3: $3<br>";
	if(!$1){
		return 0;
	}
	if($1 && !$3){
		# Zero is valid
		if($3 == 0){
			return 1;
		}
		else{
			return 0;
		}
	}
	if($1 && $3 && ($3 eq "AND")){
		return 0;
	}
	# passed so far
	else{
		return 1;
	}
}




# end of DBTransactionManager.pm

1;
