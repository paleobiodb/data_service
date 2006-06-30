## 8/20/2005 - got rid of "SQLBuilder" functionality, its not necessary anymore
##  this module is just a straight utility module for DBI
## 3/30/2004 - merged functionality of SQLBuilder and DBTransactionManager rjp

package DBTransactionManager;

use strict;
use DBConnection;
use Class::Date qw(date localdate gmdate now);
use Permissions;
use Data::Dumper;
use CGI::Carp;
use CGI;


use fields qw(	
				dbh
				_id
				_err
                tableDefinitions								
				tableNames);  # list of allowable data fields.

# dbh				:	handle to the database
# _id, _err 		: 	from the old DBTransactionManager
#

# Just pass it a dbh object
sub new {
	my $class = shift;
	my DBTransactionManager $self = fields::new($class);
    my $dbh = shift;
    
	if (! $dbh) {
		# connect if needed
		$dbh = DBConnection::connect();
		$self->{'dbh'} = $dbh;
    } else {
        $self->{'dbh'} = $dbh;
    }
    $self->{'tableDefinitions'} = {};
    $self->{'tableNames'} = [];
	
	return $self;
}


# returns the DBI Database Handle
# for those cases when it's simpler to directly use the database handle
sub dbh {
	my DBTransactionManager $self = shift;
	return $self->{'dbh'};
}

# for internal use only
# Pass it a table name, and it returns a reference to an array of array refs, one per col.
# Basically grabs the table description from the database.
#
sub getTableDesc {
	my DBTransactionManager $self = shift;
	my $tableName = shift;
	my $sql = "DESC $tableName";
	my $ref = ($self->{dbh})->selectall_arrayref($sql);
	return $ref;
}

# allowable for external use.
#
# Pass it a table name.
# Returns an arrayref of all column names in this table. 
sub getTableColumns {
	my DBTransactionManager $self = shift;
	my $tableName = shift;
	
	# it will always be the first row returned since
	# we're just using describe on this table.
	my @desc = @{$self->getTableDesc($tableName)};
	my @colNames; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colNames, $row->[0]);	
	}
	
	return @colNames;
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


# Reword PS 2005.
#
# Pass it a table name such as "occurrences", and a
# hash ref - the hash should contain keys which correspond
# directly to column names in the database.  Note, not all columns need to 
# be listed; only the ones which you are inserting data for.
#
# Args:
#  s: session object
#  tableName, primary_key_field, primary_key_value: self explanatory, the record to update
#  data: hashref for all our key->value pairs we want to insert
#    note you can just throw it a $q->Vars or something, it won't throw
#    stuff into there that isn't in the table definition, thats filtered out
#
# Returns an array.  First element is result code from the dbh->do() method,
# second element is primary key value of the last insert.
#
sub insertRecord {
	my DBTransactionManager $self = shift;
    my $s = shift;
	my $tableName = shift;
	my $fields = shift;  # don't update this
	
	my $dbh = $self->{'dbh'};
	
	# make sure they're allowed to insert data!
	if (!$s || !$s->isDBMember()) { 
		croak("invalid session or enterer in DBTransactionManager::insertRecord");
		return;
	}
	
    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'tableDefinitions'}{$tableName}) {
        @table_definition = @{$self->{'tableDefinitions'}{$tableName}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$tableName,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;    
        }
        $self->{'tableDefinitions'}{$tableName} = \@table_definition;
    }

	# loop through each key in the passed hash and
	# build up the insert statement.
    my @insertFields=();
    my @insertValues=();
    foreach my $row (@table_definition) {
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
            if (exists ($fields->{$field})) {
                # Multi-valued keys (i.e. checkboxes with the same name) passed from the CGI ($q) object have their values
                # separated by \0.  See CPAN CGI documentation about this
                my @vals = split(/\0/,$fields->{$field});
                my $value;
                if ($type eq 'SET') {
                    $value = $dbh->quote(join(",",@vals));
                } else {
                    $value = $vals[0];
                    if ($value =~ /^\s*$/ && $is_nullable) {
                        $value = 'NULL';
                    } else {
                        if (! defined $value) { $value = ''; }
                        $value = $dbh->quote($value);
                    }
                }
                push @insertFields, $field;
                push @insertValues, $value;
            }
        }	
	}

    #print Dumper($fields);
    for(my $i=0;$i<scalar(@insertFields);$i++) {
        main::dbg("$insertFields[$i] = $insertValues[$i]");
    }

    if (@insertFields) {
        my $insertSQL = "INSERT INTO $tableName (".join(",",@insertFields).") VALUES (".join(",",@insertValues).")";
        main::dbg("insertRecord in DBTransaction manager called: sql: $insertSQL");
        # actually insert into the database
        my $insertResult = $dbh->do($insertSQL);
#       DON'T USE THIS VERY BUGGY!
#        my $idNum = $dbh->last_insert_id(undef, undef, undef, undef);
        my $idNum = 0;
        if ($insertResult) {
            my $sth = $dbh->prepare("SELECT LAST_INSERT_ID()");
            $sth->execute();
            my $row = $sth->fetchrow_arrayref;
            if ($row) {
                $idNum = $row->[0];
            }
        }
        main::dbg("INSERTED ID IS $idNum for TABLE $tableName");
	
        # return the result code from the do() method.
	    return ($insertResult, $idNum);
    }
}

# Reworked PS 04/30/2005 
# Update a single record in a table with a simple primary key
#
# Args: 
#  s: session object
#  tableName, primary_key_field, primary_key_value: self explanatory, the record to update
#  update_empty_only: 1 means update only blank or null columns, 0 mean update anything
#  data: hashref for all our key->value pairs we want to update
#    note you can just throw it a $q->Vars or something, it won't throw
#    stuff into there that isn't in the table definition, thats filtered out
#
# Returns the result returned my mysql, or -1 if there was nothing to update
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
	if (!$s || !$s->isDBMember()) {
		croak("Invalid session or enterer in updateRecord");
		return 0;
	}
	
	# make sure the whereClause and tableName aren't empty!  That would be bad.
	if (($tableName eq '') || ($primary_key_field eq '')) {
        croak ("No tablename or primary_key supplied to updateRecord"); 
		return 0;	
	}

    if ($primary_key_value !~ /^\d+$/) {
        croak ("Non numeric primary key value supplied: $primary_key_field --> $primary_key_value");
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

    # A list of people who have permitted the current authorizer to edit their records
    my $p = Permissions->new($s,$self);
    my %is_modifier_for = %{$p->getModifierList()};

    # People doing updates can only update previously empty fields, unless they own the record
    my $updateEmptyOnly = 1;
    # Following people may edit the record: super_user, authorizer, or someone whos listed authorizer as a buddy
    # Or anyone, if the table has no authorizer (i.e. measurements table);
    $updateEmptyOnly = 0 if ($s->isSuperUser());
    $updateEmptyOnly = 0 if (exists $table_row->{'authorizer_no'} && $s->get('authorizer_no') == $table_row->{'authorizer_no'});
    $updateEmptyOnly = 0 if (exists $table_row->{'authorizer_no'} && $is_modifier_for{$table_row->{'authorizer_no'}});
    $updateEmptyOnly = 0 if (!exists $table_row->{'authorizer_no'});
    $updateEmptyOnly = 0 if ($tableName =~ /authorities|opinions/);

    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'tableDefinitions'}{$tableName}) {
        @table_definition = @{$self->{'tableDefinitions'}{$tableName}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$tableName,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;
        }
        $self->{'tableDefinitions'}{$tableName} = \@table_definition;
    } 

    my @updateTerms = ();
    my $termCount = 0;
    foreach my $row (@table_definition) {
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
            my $fieldIsEmpty = ($table_row->{$field} == 0 || $table_row->{$field} eq '' || !defined $table_row->{$field}) ? 1 : 0;
            if (exists($data->{$field})) {
                if (($updateEmptyOnly && $fieldIsEmpty) || !$updateEmptyOnly) {
                    # Multi-valued keys (i.e. checkboxes with the same name) passed from the CGI ($q) object have their values
                    # separated by \0.  See CPAN CGI documentation about this
                    my @vals = split(/\0/,$data->{$field});
                    my $value;
                    my $raw_value;
                    if ($type eq 'SET') {
                        $raw_value = join(",",@vals);
                        $value = $dbh->quote($raw_value);
                    } else {
                        $raw_value = $vals[0];
                        if ($raw_value =~ /^\s*$/ && $is_nullable) {
                            $value = 'NULL';
                        } else {
                            if (! defined $raw_value) { 
                                $value = $dbh->quote('');
                            } else {
                                $value = $dbh->quote($raw_value);
                            }
                        }
                    }
                    my $old_value = $table_row->{$field};
                    if ($raw_value ne $old_value) {
                        push @updateTerms, "$field=$value";
                        $termCount++;
                    }
                }
            } 
        }
    }

    # updateTerms will always be at least 1 in size (for modifer_no/modifier). If it isn't, nothing to update
    if ($termCount) {
        my $updateSql = "UPDATE $tableName SET ".join(",",@updateTerms)." WHERE $primary_key_field=$primary_key_value";
        main::dbg("UPDATE SQL:".$updateSql);
    	my $updateResult = $dbh->do($updateSql);
                
        # return the result code from the do() method.
    	return $updateResult;
    } else {
        return -1;
    }
}

# PS 04/30/2005 
# This function deletes a database record and inserts a row into a table called delete_log.  This
# row contains an insert statement taht can exactly duplicate the deleted record, for easy undos
# should the case every arise, as well as keeps track of who/when the row was deleted
#
# Args: 
#  s: session object
#  tableName, primary_key, primary_key_value: self explanatory, delete this record from the table
#  comments: Why the record was deleted
sub deleteRecord {
	my $self = shift;
	my $s = shift;
	my $tableName = shift;
	my $primary_key_field = shift;
	my $primary_key_value = shift;
    my $comments = (shift || "");
	
    my $dbh = $self->dbh;
	
	# make sure they're allowed to update data!
	if (!$s || !$s->isDBMember()) {
		croak("Invalid session or enterer in updateRecord");
		return 0;
	}
	
	# make sure the whereClause and tableName aren't empty!  That would be bad.
	if (($tableName eq '') || ($primary_key_field eq '')) {
        croak ("No tablename or primary_key supplied to deleteRecord"); 
		return 0;	
	}

    if ($primary_key_value !~ /^\d+$/) {
        croak ("Non numeric primary key value supplied: $primary_key_field --> $primary_key_value");
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

    # A list of people who have permitted the current authorizer to edit their records
    my $p = Permissions->new($s,$self);
    my %is_modifier_for = %{$p->getModifierList()};

    # People doing updates can only update previously empty fields, unless they own the record
    my $deletePermission = 0;
    # Following people may edit the record: super_user, authorizer, or someone whos listed authorizer as a buddy
    # Or anyone, if the table has no authorizer (i.e. measurements table);
    $deletePermission = 1 if ($s->isSuperUser());
    $deletePermission = 1 if (exists $table_row->{'authorizer_no'} && $s->get('authorizer_no') == $table_row->{'authorizer_no'});
    $deletePermission = 1 if (exists $table_row->{'authorizer_no'} && $is_modifier_for{$table_row->{'authorizer_no'}});
    $deletePermission = 1 if (!exists $table_row->{'authorizer_no'});
    $deletePermission = 1 if ($tableName =~ /authorities|opinions/);

    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'tableDefinitions'}{$tableName}) {
        @table_definition = @{$self->{'tableDefinitions'}{$tableName}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$tableName,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;
        }
        $self->{'tableDefinitions'}{$tableName} = \@table_definition;
    } 

    if ($deletePermission) {
        # loop through each key in the passed hash and
        # build up the insert statement.
        my @insertFields=();
        my @insertValues=();
        foreach my $row (@table_definition) {
            my $field = $row->{'COLUMN_NAME'};
            my $type = $row->{'TYPE_NAME'};
            my $is_nullable = ($row->{'IS_NULLABLE'} eq 'YES') ? 1 : 0;
            my $is_primary =  $row->{'mysql_is_pri_key'};

            # It exists in the passed in user hash
            my $value = $table_row->{$field};
            if ($value =~ /^\s*$/ && $is_nullable) {
                $value = 'NULL';
            } else {
                if (! defined $value) { $value = ''; }
                $value = $dbh->quote($value);
            }
            push @insertFields, $field;
            push @insertValues, $value;
        }

        if (@insertFields) {
            my $insertSQL = "INSERT INTO $tableName (".join(",",@insertFields).") VALUES (".join(",",@insertValues).")";
            my $enterer_no = $s->get('enterer_no');
            my $authorizer_no = $s->get('authorizer_no');
            my $deleteLogSQL = "INSERT INTO delete_log (delete_time,authorizer_no,enterer_no,comments,delete_sql) VALUES (NOW(),$authorizer_no,$enterer_no,".$dbh->quote($comments).",".$dbh->quote($insertSQL).")";
            main::dbg("Delete log final SQL: $deleteLogSQL");
            $dbh->do($deleteLogSQL);
            my $deleteSQL = "DELETE FROM $tableName WHERE $primary_key_field=$primary_key_value LIMIT 1";
            main::dbg("Deletion SQL: $deleteSQL");
            $dbh->do($deleteSQL);
        }
    } else {
        main::dbg("User ".$s->get('authorizer_no')." does not have permission to delete $tableName $primary_key_value"); 
    }
}

###
## The following methods are from the old DBTransactionManager class 
###

## getData
#	description:	Handles basic SQL syntax checking, 
#					it executes the statement, and returns arrayref of hashrefs
#
#	parameters:		$sql			The SQL statement to be executed
#					$type			"read", "write", "both", "neither"
#					$attr_hash_ref	reference to a hash whose keys are set by
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
			my @data = @{$sth->fetchall_arrayref({})};
			$sth->finish();
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
	$sql =~ /WHERE\s+([A-Z0-9_\.\(\)]+)\s*(NOT)*\s*(=|AGAINST|LIKE|IN|IS NULL|IS NOT NULL|!=|>|<|>=|<=)\s*(.+)?\s*/i;

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
