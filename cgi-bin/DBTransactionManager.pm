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
use DBConnection;
use Class::Date qw(date localdate gmdate now);
use Data::Dumper;
use CGI::Carp;
use CGI;


use fields qw(	
				dbh
				sth
				perm
				GLOBALVARS
				
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
    my $dbh = shift;
    $self->{'dbh'} = $dbh;
	
	# set up some default values
	$self->clear();	
	
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

	$sql = $self->SQLExpr();
			
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	
	# save the sth for later use in fetching rows.
	$self->{sth} = $sth;
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
# Returns an array.  First element is result code from the dbh->do() method,
# second element is primary key value of the last insert.
#
# Note, for now, this just blindly inserts whatever the user passes.
#
# Reworked PS 04/30/2005 to be more flexible - uses code from bridge.pl::insertRecord
#
#
sub insertRecord {
	my DBTransactionManager $self = shift;
    my $s = shift;
	my $tableName = shift;
	my $fields = shift;  # don't update this
	
	my $dbh = $self->{'dbh'};
	
	# make sure they're allowed to insert data!
	if (!$s || $s->guest() || $s->get('enterer') eq '') {
		croak("invalid session or enterer in DBTransactionManager::insertNewRecord");
		return;
	}
	
    # get the column info from the table
    my $sth = $dbh->column_info(undef,'pbdb',$tableName,'%');

	# loop through each key in the passed hash and
	# build up the insert statement.
    my @insertFields=();
    my @insertValues=();
    while (my $row = $sth->fetchrow_hashref()) {
        my $field = $row->{'COLUMN_NAME'};
        my $type = $row->{'TYPE_NAME'};
        my $is_nullable = ($row->{'IS_NULLABLE'} eq 'YES') ? 1 : 0;
        my $is_primary =  $row->{'mysql_is_pri_key'};

        # we never insert these fields ourselves
        next if ($field =~ /^(modifier|modifier_no)$/);
        next if ($is_primary);

        # handle these fields automatically
        if ($field =~ /^(authorizer_no|authorizer|enterer_no|enterer)$/) {
            # from the session object
            push @insertFields, $field;
            push @insertValues, $dbh->quote($s->get($field));
        } elsif ($field eq 'modified') {
            push @insertFields, $field;
            push @insertValues, 'NOW()';
        } elsif ($field eq 'created') {
            push @insertFields, $field;
            push @insertValues, 'NOW()';
        } else {
            # It exists in the passed in user hash
            if (defined($fields->{$field})) {
                # Multi-valued keys (i.e. checkboxes with the same name) passed from the CGI ($q) object have their values
                # separated by \0.  See CPAN CGI documentation about this
                my @vals = split(/\0/,$fields->{$field});
                my $value;
                if ($type eq 'SET') {
                    $value = join(",",map {$dbh->quote($_)} @vals);
                } else {
                    $value = $vals[0];
                    if ($value =~ /^\s*$/ && $is_nullable) {
                        $value = 'NULL';
                    } else {
                        if (!$value) { $value = ''; }
                        $value = $dbh->quote($value);
                    }
                }
                push @insertFields, $field;
                push @insertValues, $value;
            }
        }	
	}

    #print Dumper($fields);
    #for(my $i=0;$i<scalar(@insertFields);$i++) {
    #    main::dbg("$insertFields[$i] = $insertValues[$i]");
    #}

    if (@insertFields) {
        my $insertSQL = "INSERT INTO $tableName (".join(",",@insertFields).") VALUES (".join(",",@insertValues).")";
        main::dbg("insertRecord in DBTransaction manager called: sql: $insertSQL");
        # actually insert into the database
        my $insertResult = $dbh->do($insertSQL);
        
        # bug fix here by JA 2.4.04
        my $idNum = ${$self->getData("SELECT LAST_INSERT_ID() AS l FROM $tableName")}[0]->{l};
	
        # return the result code from the do() method.
	    return ($insertResult, $idNum);
    }
		
}

# Reworked PS 04/30/2005 
# Update a single record in a table with a simple primary key
#
# Args: 
#  s: session object
#  update_empty_only: 1 means update only blank or null columns, 0 mean update anything
#  data: hashref for all our key->value pairs we want to update
#    note you can just throw it a $q->Vars or something, it won't throw
#    stuff into there that isn't in the table definition, thats filtered out
#
sub updateRecord {
	my $self = shift;
	my $s = shift;
	my $tableName = shift;
	my $primary_key_field = shift;
	my $primary_key_value = shift;
	my $data = shift;
	
    my $dbh = $self->dbh;
	
	# make sure they're allowed to update data!
	if (!$s || $s->guest() || $s->get('enterer') eq '' || $s->get('enterer_no') !~ /^\d+$/)	{
		croak("Invalid session or enterer in updateRecord");
		return 0;
	}
	
	# make sure the whereClause and tableName aren't empty!  That would be bad.
	if (($tableName eq '') || ($primary_key_field eq '')) {
        croak ("No tablename or primary_key supplied to updateRecord"); 
		return 0;	
	}

	# get the record we're going to update from the table
    my $sql = "SELECT * FROM $tableName WHERE $primary_key_field=$primary_key_value";
    my @results = @{$self->getData($sql)};
    if (scalar(@results) != 1) {
        croak ("Error in updateRecord: $sql return ".scalar(@results)." values instead of 1");
        return 0;
    }
    my $table_row = $results[0];
    if (!$table_row) {
        croak("Could not pull row from table $tableName for $primary_key_field=$primary_key_value");
        return 0;
    }

    if ($primary_key_value !~ /^\d+$/) {
        croak ("Non numeric primary key value supplied: $primary_key_field --> $primary_key_value");
        return 0;
    }

    # People doing updates can only update previously empty fields, unless they own the record
    my $updateEmptyOnly = ($s->isSuperUser() || 
                           (exists $table_row->{'authorizer_no'} && $s->get('authorizer_no') == $table_row->{'authorizer_no'}) ||
                           (exists $table_row->{'authorizer'} && $s->get('authorizer') == $table_row->{'authorizer'})) ? 0 : 1;


    # get the column info from the table
    my $sth = $dbh->column_info(undef,'pbdb',$tableName,'%');

    my @updateTerms = ();
    while (my $row = $sth->fetchrow_hashref()) {
        my $field = $row->{COLUMN_NAME};
        my $type = $row->{TYPE_NAME};
        my $is_nullable = ($row->{IS_NULLABLE} eq 'YES') ? 1 : 0; 
        # we never update these fields
        next if ($field =~ /^(created|modified|authorizer|authorizer_no|enterer|enterer_no)$/);
        next if ($field eq $primary_key_field);
        
        # if it's the modifier_no or modifier, then we'll update it regardless
        if ($field eq 'modifier_no') {
            push @updateTerms, "modifier_no=".$dbh->quote($s->get('enterer_no'));
        } elsif ($field eq 'modifier') {
            push @updateTerms, "modifier=".$dbh->quote($s->get('enterer'));
        } else {
            my $fieldIsEmpty = ($table_row->{$field} eq '' || !defined $table_row->{$field});
            if (defined($data->{$field})) {
                if (($updateEmptyOnly && $fieldIsEmpty) || !$updateEmptyOnly) {
                    # Multi-valued keys (i.e. checkboxes with the same name) passed from the CGI ($q) object have their values
                    # separated by \0.  See CPAN CGI documentation about this
                    my @vals = split(/\0/,$data->{$field});
                    my $value;
                    if ($type eq 'SET') {
                        $value = join(",",map {$dbh->quote($_)} @vals);
                    } else {
                        $value = $vals[0];
                        if ($value =~ /^\s*$/ && $is_nullable) {
                            $value = 'NULL';
                        } else {
                            if (!$value) { $value = ''; }
                            $value = $dbh->quote($value);
                        }
                    }
                    push @updateTerms, "$field=$value";
                }
            } 
        }
    }

    if (@updateTerms) {
        my $updateSql = "UPDATE $tableName SET ".join(",",@updateTerms)." WHERE $primary_key_field=$primary_key_value";
        main::dbg("UPDATE SQL:".$updateSql);
    	my $updateResult = $dbh->do($updateSql);
                
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
    my $dbh = $self->{'dbh'};

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
        # This shit is fucking up so I'm commenting it out 
        # Its not necessary anyways PS 03/09/2004

		# Check that the columns and values lists are not empty
		# NOTE: down the road, we could check required fields
		# against table names.
		#$sql =~ /(\(.*?\))\s+VALUES\s*(\(.*?\))/i;
        #print "INSERT VALS for ==$sql== 1(($1)) 2:(($2))<BR>";
		#if($1 eq "()" or $1 eq "" or $2 eq "()" or $2 eq "" ){
		#	return 0;
		#}
		#else{
			return 2;
		#}
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
	# modified by PS 2.28.05 to accept MATCH (x) AGAINST (y)
    # modified by PS 3.07.05 to accept NOT LIKE/NOT IN
	$sql =~ /WHERE\s+([A-Z0-9_\.\(\)]+)\s*(NOT)*\s*(=|AGAINST|LIKE|IN|IS NULL|!=|>|<|>=|<=)\s*(.+)?\s*/i;

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
