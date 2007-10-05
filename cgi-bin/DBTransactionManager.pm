#  this module is just a straight utility module for DBI

package DBTransactionManager;

use strict;
use DBConnection;
use Class::Date qw(date localdate gmdate now);
use Permissions;
use Data::Dumper;
use CGI::Carp;
use CGI;
use Debug qw(dbg);

use fields qw(dbh _id _err table_definitions table_names);

# Just pass it a dbh object
sub new {
	my $class = shift;
    my $dbh = shift;

	my DBTransactionManager $self = fields::new($class);
	if (! $dbh) {
		# connect if needed
		$dbh = DBConnection::connect();
    } 
    $self->{'dbh'} = $dbh;
    $self->{'table_definitions'} = {};
    $self->{'table_names'} = [];
	
	return $self;
}


# returns the DBI Database Handle. This is a bit tricky
# since depending on flag dbh will return different handles
sub dbh {
	my $self = shift;
	return $self->{'dbh'};
}

# for internal use only
# Pass it a table name, and it returns a reference to an array of array refs, one per col.
# Basically grabs the table description from the database.
#
sub getTableDesc {
	my DBTransactionManager $self = shift;
    my $dbh = $self->dbh;
	my $table_name = shift;
	my $sql = "DESC $table_name";
	my $ref = $dbh->selectall_arrayref($sql);
	return $ref;
}

# allowable for external use.
#
# Pass it a table name.
# Returns an arrayref of all column names in this table. 
sub getTableColumns {
	my DBTransactionManager $self = shift;
	my $table_name = shift;
	
	# it will always be the first row returned since
	# we're just using describe on this table.
	my @desc = @{$self->getTableDesc($table_name)};
	my @colNames; 	# names of columns in table
	foreach my $row (@desc) {
		push(@colNames, $row->[0]);	
	}
	
	return @colNames;
}


# Reworked PS 2005.
#
# Pass it a table name such as "occurrences", and a
# hash ref - the hash should contain keys which correspond
# directly to column names in the database.  Note, not all columns need to 
# be listed; only the ones which you are inserting data for.
#
# Args:
#  s: session object
#  table_name, primary_key_field, primary_key_value: self explanatory, the record to update
#  data: hashref for all our key->value pairs we want to insert
#    note you can just throw it a $q->Vars or something, it won't throw
#    stuff into there that isn't in the table definition, thats filtered out
#
# Returns an array.  First element is result code from the dbh->do() method,
# second element is primary key value of the last insert.
#
sub insertRecord {
    my ($self,$s,$table_name,$fields,$options) = @_;
    my %options;
    if ($options) {
        %options = %$options;
    }

	my $dbh = $self->dbh;
	
	# make sure they're allowed to insert data!
	if (!$s || !$s->isDBMember()) { 
		croak("invalid session or enterer in DBTransactionManager::insertRecord");
		return;
	}
	
    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'table_definitions'}{$table_name}) {
        @table_definition = @{$self->{'table_definitions'}{$table_name}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$table_name,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;    
        }
        $self->{'table_definitions'}{$table_name} = \@table_definition;
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
        next if ($is_primary && !$options{'no_autoincrement'});

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
            # I'm stashing this for use in snooping below JA 26.4.07
            $fields->{'created'} = $insertValues[$#insertValues];
        } else {
            # It exists in the passed in user hash
            if (exists ($fields->{$field})) {
                # Multi-valued keys (i.e. checkboxes with the same name) passed from the CGI ($q) object have their values
                # separated by \0.  See CPAN CGI documentation about this
                my $value;
                if ($type eq 'SET') {
                    my @vals = split(/\0/,$fields->{$field});
                    $value = $dbh->quote(join(",",@vals));
                } else {
                    if ($type =~ /TEXT|BLOB/i) {
                        $value = $fields->{$field};
                    } else {
                        my @vals = split(/\0/,$fields->{$field});
                        $value = $vals[0];
                    }
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
        dbg("$insertFields[$i] = $insertValues[$i]");
    }

    if (@insertFields) {
        my $insertSQL = "INSERT INTO $table_name (".join(",",@insertFields).") VALUES (".join(",",@insertValues).")";
        dbg("insertRecord in DBTransaction manager called: sql: $insertSQL");
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
            dbg("INSERTED ID IS $idNum for TABLE $table_name");

            # track the last time each person entered data because we're snoops
            #  JA 26.4.07
            my $sql = "UPDATE person SET last_action=last_action,last_entry=NOW() WHERE person_no=".$s->get('enterer_no');
            $dbh->do($sql);
        }
        # return the result code from the do() method.
	    return ($insertResult, $idNum);
    }
}

# Reworked PS 04/30/2005 
# Update a single record in a table with a simple primary key
#
# Args: 
#  s: session object
#  table_name, primary_key_field, primary_key_value: self explanatory, the record to update
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
	my $table_name = shift;
	my $primary_key_field = shift;
	my $primary_key_value = shift;
	my $data = shift;
	
    my $dbh = $self->dbh;
	
	# make sure they're allowed to update data!
	if (!$s || !$s->isDBMember()) {
		croak("Invalid session or enterer in updateRecord");
		return 0;
	}
	
	# make sure the whereClause and table_name aren't empty!  That would be bad.
	if (($table_name eq '') || ($primary_key_field eq '')) {
        croak ("No table_name or primary_key supplied to updateRecord"); 
		return 0;	
	}

    if ($primary_key_value !~ /^\d+$/) {
        croak ("Non numeric primary key value supplied: $primary_key_field --> $primary_key_value");
        return 0;
    }

	# get the record we're going to update from the table
    my $sql = "SELECT * FROM $table_name WHERE $primary_key_field=$primary_key_value";
    my @results = @{$self->getData($sql)};
    if (scalar(@results) != 1) {
        croak ("Error in updateRecord: $sql return ".scalar(@results)." values instead of 1");
        return 0;
    }
    my $table_row = $results[0];
    if (!$table_row) {
        croak("Could not pull row from table $table_name for $primary_key_field=$primary_key_value");
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
    $updateEmptyOnly = 0 if ($table_name =~ /authorities|opinions|refs/);

    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'table_definitions'}{$table_name}) {
        @table_definition = @{$self->{'table_definitions'}{$table_name}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$table_name,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;
        }
        $self->{'table_definitions'}{$table_name} = \@table_definition;
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
        my $updateSql = "UPDATE $table_name SET ".join(",",@updateTerms)." WHERE $primary_key_field=$primary_key_value";
        dbg("UPDATE SQL:".$updateSql);
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
#  table_name, primary_key, primary_key_value: self explanatory, delete this record from the table
#  comments: Why the record was deleted
sub deleteRecord {
	my $self = shift;
	my $s = shift;
	my $table_name = shift;
	my $primary_key_field = shift;
	my $primary_key_value = shift;
    my $comments = (shift || "");
	
    my $dbh = $self->dbh;
	
	# make sure they're allowed to update data!
	if (!$s || !$s->isDBMember()) {
		croak("Invalid session or enterer in updateRecord");
		return 0;
	}
	
	# make sure the whereClause and table_name aren't empty!  That would be bad.
	if (($table_name eq '') || ($primary_key_field eq '')) {
        croak ("No table_name or primary_key supplied to deleteRecord"); 
		return 0;	
	}

    if ($primary_key_value !~ /^\d+$/) {
        croak ("Non numeric primary key value supplied: $primary_key_field --> $primary_key_value");
        return 0;
    }

	# get the record we're going to update from the table
    my $sql = "SELECT * FROM $table_name WHERE $primary_key_field=$primary_key_value";
    my @results = @{$self->getData($sql)};
    if (scalar(@results) != 1) {
        croak ("Error in updateRecord: $sql return ".scalar(@results)." values instead of 1");
        return 0;
    }
    my $table_row = $results[0];
    if (!$table_row) {
        croak("Could not pull row from table $table_name for $primary_key_field=$primary_key_value");
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
    $deletePermission = 1 if ($table_name =~ /authorities|opinions/);

    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'table_definitions'}{$table_name}) {
        @table_definition = @{$self->{'table_definitions'}{$table_name}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$table_name,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;
        }
        $self->{'table_definitions'}{$table_name} = \@table_definition;
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
            my $insertSQL = "INSERT INTO $table_name (".join(",",@insertFields).") VALUES (".join(",",@insertValues).")";
            my $enterer_no = $s->get('enterer_no');
            my $authorizer_no = $s->get('authorizer_no');
            my $deleteLogSQL = "INSERT INTO delete_log (delete_time,authorizer_no,enterer_no,comments,delete_sql) VALUES (NOW(),$authorizer_no,$enterer_no,".$dbh->quote($comments).",".$dbh->quote($insertSQL).")";
            dbg("Delete log final SQL: $deleteLogSQL");
            $dbh->do($deleteLogSQL);
            my $deleteSQL = "DELETE FROM $table_name WHERE $primary_key_field=$primary_key_value LIMIT 1";
            dbg("Deletion SQL: $deleteSQL");
            $dbh->do($deleteSQL);
        }
    } else {
        dbg("User ".$s->get('authorizer_no')." does not have permission to delete $table_name $primary_key_value"); 
    }
}

###
## The following methods are from the old DBTransactionManager class 
###

## getData
#	description:	it executes the statement, and returns arrayref of hashrefs
#                   now only works for selects, for writes use the dbh method to
#                   get a handle and check statements by hand
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
#					Returns empty anonymous array on failure.
##
sub getData {
	my DBTransactionManager $self = shift;
	my $sql = shift;
    my $dbh = $self->dbh;

	# First, check the sql for any obvious problems
	my $is_select = $self->checkIsSelect($sql);
	if ($is_select) {
		# execute returns the number of rows affected for non-select statements.
		# SELECT:
        my $sth = $dbh->prepare($sql);

        my $return = eval {$sth->execute();};

        if ($sth->errstr || $@ ne "") {
            $self->{_err} = $sth->errstr;
            my $stack = "";
            for(my $i = 0;$i < 10;$i++) {
                my ($package, $filename, $line, $subroutine, $hasargs) = caller($i);
                last unless $subroutine;
                $stack .= "$subroutine:$line ";
            }
            $stack =~ s/ $//;
            my $errstr = "SQL error: sql($sql)";
            my $q2 = new CGI;
            $errstr .= " sth err (".$sth->errstr.")" if ($sth->errstr);
            $errstr .= " IP ($ENV{REMOTE_ADDR})";
            $errstr .= " script (".$q2->url().")";
            $errstr .= " stack ($stack)";
            my $getpoststr; 
            my %params = $q2->Vars; 
            while(my ($k,$v)=each(%params)) { 
                $getpoststr .= "&$k=$v"; 
            }
            $getpoststr =~ s/\n//;
            $errstr .= " GET,POST ($getpoststr)";
            croak $errstr;
        } else {
            $self->{_err} = "";
        }
        my $data = $sth->fetchall_arrayref({});
        $sth->finish();
        return $data;
	} else {
        die("Can not execute non select statements with getData: $sql");
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
sub checkIsSelect {
	my DBTransactionManager $self = shift;
	my $sql = shift;

	# NOTE: This messes with enumerated values
	# uppercase the whole thing for ease of use
	#$sql = uc($sql);

	# Is this a SELECT, INSERT, UPDATE or DELETE?
	$sql =~/^[\(]*(\w+)\s+/;
	my $type = uc($1);

	if($type eq "SELECT"){
		return 1;
	} else {
        return 0;
    }
}


# from the old DBTransactionManager class... 
#Deprecated ... this function wasn't very useful anyways, PS
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
	$sql =~ /WHERE\s+([A-Z0-9_\.\(\)]+)\s*(NOT)*\s*(=|BETWEEN|AGAINST|LIKE|IN|IS NULL|IS NOT NULL|!=|>|<|>=|<=)\s*(.+)?\s*/i;

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

# Check for duplicates before inserting a record
sub checkDuplicates {
    my ($self,$table_name,$vars_ref,$excluded_fields) = @_;
    my %vars = %{$vars_ref}; # Make a copy
    my %excluded = ();
    if ($excluded_fields && ref($excluded_fields)) {
        my @excluded = @$excluded_fields;
        foreach my $f (@excluded) {
            $excluded{$f} = 1;
        }
    }
    
	my $dbh = $self->dbh;

    # Get the table definition, only get it once though. Careful about this if we move to mod_perl, cache
    # will exist for as long of the $dbt object exists
    my @table_definition = ();
    if ($self->{'table_definitions'}{$table_name}) {
        @table_definition = @{$self->{'table_definitions'}{$table_name}};
    } else {
        my $sth = $dbh->column_info(undef,'pbdb',$table_name,'%');
        while (my $row = $sth->fetchrow_hashref()) {
            push @table_definition, $row;    
        }
        $self->{'table_definitions'}{$table_name} = \@table_definition;
    }

    my @terms;
    my $primary_key_name;
    foreach my $row (@table_definition) {
        my $field = $row->{'COLUMN_NAME'};
        my $type = $row->{'TYPE_NAME'};
        my $is_nullable = ($row->{'IS_NULLABLE'} eq 'YES') ? 1 : 0;
        my $is_primary =  $row->{'mysql_is_pri_key'};
        if ($is_primary) {
            $primary_key_name = $field;
        }

        next unless exists $vars{$field};
        next if $excluded{$field};
        next if ($is_primary);
        # Ref specific
        next if ($field =~ /^(project_ref_no)$/i);
        # Generic skips
        next if ($field =~ /^(taxon_no|upload|most_recent|created|modified|release_date)$/i);
        next if ($field =~ /^(authorizer|enterer|modifier|authorizer_no|enterer_no|modifier_no)$/i);
        next if ($field =~ /comments/i);
        my @vals = split(/\0/,$vars{$field});
        my $v;
        if ($type eq 'SET') {
            $v = join(",",@vals);
            $v =~ s/^,//;
        } else {
            $v = $vals[0];
        }
        # Tack on the field and value; take care of NULLs.
        if ( $v eq "") {
            push @terms, "($field IS NULL OR $field='')";
        } else {
            push @terms, "$field=".$dbh->quote($v);
        }
	}
    if (@terms && $primary_key_name) {
        my $sql = "SELECT $primary_key_name FROM $table_name WHERE ".join(" AND ",@terms);
        dbg("checkDuplicates SQL:$sql<HR>");

        my @rows = @{$self->getData($sql)};
        if ( @rows) {
            return $rows[0]->{$primary_key_name};
        }
    }
    return 0;
}

# checkNearMatch
# This function shelved for now theres too many deficiencies in it PS
#
# 	Description:
#       Check for records that match at least some number of fields
#
#	Arguments:
#       $matchlimit		threshold of column matches to consider whole record a 'match.'
#       $primary_key_name			name of primary key of table
#       $table_name		db table in which to look for matches
#       $searchField	table column on which to search
#       $searchVal		column value to search against
#       $fields			names of fields from form (from 
#                       submission to insertRecord that called
#                       this method).	
#       $vals			values from form (as above)
#
##			
sub checkNearMatch {
    my ($self,$table_name,$primary_key_name,$hbo,$q,$matchlimit,$where_term);

    my $dbh = $self->dbh;

    my %vars = $q->Vars();
    my $what_to_do = $vars{'what_to_do'};

    my @fields = keys %vars;
    my @vals = values %vars;

    if ($what_to_do) {
        if($what_to_do eq 'Continue'){
            return 0;
        } else{
            print Debug::printWarnings(["Record addition canceled"]);
            return 1;
        }
    } else {
        my $sql = "SELECT * FROM $table_name WHERE $where_term";
        dbg("checkNearMatch SQL:$sql<br>");

        my @rows = @{$self->getData($sql)};

        # Look for matches in the returned rows
        my @complaints;
        foreach my $row (@rows) {
            my $fieldMatches;
            for ( my $i=0; $i<$#fields; $i++ ) {
                my $v = $vals[$i];
                if ( $fields[$i] !~ /^authorizer|^enterer|^modifier|^created$|^modified$|comments|^release_date$|^upload$/) {
                    if ( $v eq $row->{$fields[$i]} && $v ne "")	{
                        $fieldMatches++;
                    }
                }
            }
            if ($fieldMatches >= $matchlimit)	{
                push @complaints,$row;
            }
        }

        if (@complaints)	{
            # Print out the possible matches
            my $warning = "Your new record may duplicate one of the following old ones.";
            print "<CENTER><H3><FONT COLOR='red'>Warning:</FONT> $warning</H3></CENTER>\n";                                                                         
            print "<table><tr><td>\n";
            # Figure out what fields to show
            my @display = ();
            # Be more narrowminded if this is a coll
            if ($table_name eq "refs")	{
                @display = ("reference_no","author1last","author2last","otherauthors","pubyr","reftitle","pubtitle","pubvol","pubno");
            } elsif ($table_name eq "collections")	{
                @display = ("collection_no", "collection_name", "country", "state","formation", "period_max");
            } else {
                @display = $self->getTableColumns($table_name);
            }

            foreach my $row (@complaints) {
                # Do some cleanup if this is a ref
                if ($table_name eq "refs")	{
                    # If otherauthors is filled in, we do an et al.
                    if ($row->{'otherauthors'} )	{
                        $row->{'author1last'} .= " et al.";
                        $row->{'author2init'} = '';
                        $row->{'author2last'} = '';
                        $row->{'otherauthors'} = '';
                    }
                    # If there is a second author...
                    elsif ( $row->{'author2last'} )	{
                        $row->{'author1last'} .= " and ";
                    }
                }
                my @rowData;
                for my $d (@display)	{
                    push @rowData,$row->{$d};
                }
                if ($table_name eq "refs")	{
                    print $hbo->populateHTML('reference_display_row', \@rowData, \@display);
                }
                elsif($table_name eq "opinions"){
                    my $sql="SELECT taxon_name FROM authorities WHERE taxon_no=".
                            $row->{parent_no};
                    my @results = @{$self->getData($sql)};
                    print "<table><tr>";
                    print "<td>$row->{status} $results[0]->{taxon_name}: ".
                          "$row->{author1last} $row->{pubyr} ";

                    $sql="SELECT p1.name as name1, p2.name as name2, ".
                            "p3.name as name3 ".
                            "FROM person as p1, person as p2, person as p3 WHERE ".
                            "p1.person_no=$row->{authorizer_no} ".
                            "AND p2.person_no=$row->{enterer_no} ".
                            "AND p3.person_no=$row->{modifier_no}";
                    @results = @{$self->getData($sql)};

                    print "<font class=\"tiny\">[".
                          "$results[0]->{name1}/$results[0]->{name2}/".
                          "$results[0]->{name3}]".
                          "</font></td>";
                    print "</tr></table>";
                }
                elsif($table_name eq "authorities"){
                    print "<table><tr>";
                    print "<td> $row->{taxon_name}: $row->{author1last} $row->{pubyr} ";

                    my $sql="SELECT p1.name as name1, p2.name as name2, ".
                            "p3.name as name3 ".
                            "FROM person as p1, person as p2, person as p3 WHERE ".
                            "p1.person_no=$row->{authorizer_no} ".
                            "AND p2.person_no=$row->{enterer_no} ".
                            "AND p3.person_no=$row->{modifier_no}";
                    my @results = @{$self->getData($sql)};

                    print "<font class=\"tiny\">[".
                          "$results[0]->{name1}/$results[0]->{name2}/".
                          "$results[0]->{name3}]".
                          "</font></td>";
                    print "</tr></table>";
                }
            }
            print "</td></tr></table>\n";
        }

        if(@complaints){
            print "<center><p><b>What would you like to do?</b></p></center>";
            print "<form method=POST action=\"bridge.pl\">";
           
            
           
            print "<center><input type=submit name=\"what_to_do\" value=\"Cancel\"> ";
            print "<input type=submit name=\"what_to_do\" value=\"Continue\"></center>";
            print "</form>";
            if($table_name eq "refs"){
                print qq|<p><a href="brigde.pl?action=displaySearchRefs&type=add"><b>Add another reference</b></a></p></center><br>\n|;
            }
            # we don't want control to return to insertRecord() (which called this
            # method and will insert the record after control returns to it after
            # calling this method, thus potentially creating a duplicate record if
            # the user chooses to continue.
            # Terminate this server session, and wait for user's response.
            exit;
        }
    }
}

1;
