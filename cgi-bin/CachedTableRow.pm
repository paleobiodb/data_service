#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents a single row from any data table in the database
#
# When creating a new instance, pass it the table name and a where
# clause for the select, and it will automatically populate itself.
#
# This will only work well if the query returns one result, so it should
# only be used for queries based on the primary key.
#
# If using this to insert/update values in the database, you *must* 
# set the primary key name and value (value only for updates) before
# attempting to insert/update.


package CachedTableRow;


use strict;
use DBI;
use DBConnection;
use DBTransactionManager;
use Debug;
use CGI::Carp qw(fatalsToBrowser);

use Session;



use fields qw(	
				GLOBALVARS

				table
				where
				primaryKeyName
				primaryKeyValue
				
			    updateEmptyOnly
			
				row
				
				DBTransactionManager
							);  # list of allowable data fields.


#   table           :   name of the database table
#   where           :   where clause to query with
#   primaryKeyName  :   necessary for updates/inserts
#   primaryKeyValue :   necessary for updates
#   row             :   hashref of database row

						

# Must pass this the global variable and table name.
# If fetching a row which already
# exists in the database then you must also pass a where clause
#
# For example, my $row = CachedTableRow->new($session, 'authorities', 'taxon_no = 5');
#
sub new {
	my $class = shift;
    my CachedTableRow $self = fields::new($class);
    
	$self->{GLOBALVARS} = shift;
	my $table = shift;
	my $where = shift;

	
	$self->{table} = $table;
	$self->{primaryKeyName} = '';
	$self->{primaryKeyValue} = '';
	$self->{where} = $where;
	my %hash;
	$self->{row} = \%hash;
	
	$self->{updateEmptyOnly} = 1;
	
	if ($table && $where) {
		# only run the query if we know enough to run it.
		$self->fetchDatabaseRow();
	}
	
	return $self;
}
 

# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getTransactionManager {
	my CachedTableRow $self = shift;
	
	my $DBTransactionManager = $self->{DBTransactionManager};
	if (! $DBTransactionManager) {
	    $DBTransactionManager = DBTransactionManager->new($self->{GLOBALVARS});
	}
	
	return $DBTransactionManager;
}


sub setPrimaryKeyName {
    my CachedTableRow $self = shift;
    $self->{primaryKeyName} = shift;
}

sub setPrimaryKeyValue {
    my CachedTableRow $self = shift;
    $self->{primaryKeyValue} = shift;
}

# pass this a boolean.
# if set to true, then when updating its entry in the 
# database, it will only update empty fields in the record.
# if set to false, then it will update any field.
sub setUpdateEmptyOnly {
    my CachedTableRow $self = shift;
    $self->{updateEmptyOnly} = shift;    
}


# Internal use only
#
# fetches the row from the database based
# on the table name and where clause.
sub fetchDatabaseRow {
	my CachedTableRow $self = shift;
	
	my $sql = $self->getTransactionManager();
	
	# if it doesn't exist, make one up
	if (! $self->{where}) {
	    if ($self->{primaryKeyName} && $self->{primaryKeyValue}) {
    	    $self->{where} = $self->{primaryKeyName} . " = '" . $self->{primaryKeyValue} . "'";
        }
	}
	if (!$self->{where}) {
	    return;
	}
	
	my $sqlString = "FROM " . $self->{table} . " WHERE " . $self->{where};
	
	
	# figure out how many rows their where clause will return.
	my $count = $sql->getSingleSQLResult("SELECT count(*) $sqlString");

    # we don't want to fetch too many rows because it's probably an error
    # on the programmer's part - ie - maybe they forgot to include a valid
    # where clause.
    if ($count > 20) { 
        Debug::logError("CachedTableRow::fetchDatabaseRecord tried to fetch too many results.");
        return;
    }

	$sql->setSQLExpr("SELECT * $sqlString");
	$sql->executeSQL();
		
	my $row = $sql->nextResultHashRef();
	
	$sql->finishSQL();
	
	$self->{row} = $row;
    
    if ($self->{primaryKeyName}) {
        # set the primary key value in case we didn't know it before.
        $self->{primaryKeyValue} = $row->{$self->{primaryKeyName}};
    }
}


# Internal use only
#
# attempts to update or insert the database row.
# If the row already exists (based on the where clause), then it updates it.
# If it doesn't exist, it inserts it. 
#
sub setDatabaseRow {
	my CachedTableRow $self = shift;

    my $pkey = $self->{primaryKeyName};
    my $pkval = $self->{primaryKeyValue};
    my $where = $self->{where};
    my $row = $self->{row};
    
    if (!$pkey || !$row) { 
        Debug::logError("CachedTableRow::setDatabaseRow failed because primary key name or table row was empty.");
        return;
    }
    
	my $sql = $self->getTransactionManager();
    my $count;  # how many match in the db.
    
	if (!$where) {
        # if they didn't provide a where clause, then we can 
        # form our own from the fields in the row and make sure the record 
        # doesn't exist already.
        
        if ($pkey && $pkval) {
            # if we have primary key and value, then use that.
            $where = "$pkey = '" . $pkval . "'";
            $self->{where} = $where;
        } else {
            # otherwise, if we don't know primary key value, we'll 
            # have to figure it out based on other fields.
            
            $sql->setSelectExpr("COUNT(*)");
            $sql->setFromExpr($self->{table});
            $sql->setWhereSeparator("AND");

            foreach my $key (keys(%$row)) {
                Debug::dbPrint("key = $key");
                $sql->addWhereItem("$key = '" . $row->{$key} . "'");
            }
        
            $where = $sql->whereExpr();
            $self->{where} = $where;
        }
    }


    if ($where) {    
        $count = $sql->getSingleSQLResult("SELECT COUNT(*) FROM " . $self->{table} . " WHERE $where");
     } else {
        Debug::logError("No where clause in CachedTableRow::setDatabaseRow");
        return;
     }

    Debug::dbPrint("count = $count");


    # if it already exists in the database, then we'll need to update it.
    # otherwise, we need to insert.
    
    if ($count == 1) {
        # then it exists, so we have to do an update
        if ($self->{updateEmptyOnly}) {
            $sql->updateRecordEmptyFieldsOnly($self->{table}, $row, $where, $pkey);
        } else {
            $sql->updateRecord($self->{table}, $row, $where, $pkey);
        }

    } elsif ($count > 1) {
        Debug::logError("Tried to update a row in the " . $self->{table} . " table, but update would have affected $count rows.  Bailing out.");    
    } else {
        # doesn't exist, so do an insert
        Debug::dbPrint("we're trying to do an insert");
        my ($resultCode, $insertID) = $sql->insertNewRecord($self->{table}, $self->{row});
        
        $self->{primaryKeyValue} = $insertID;
    }
    
    
    # now that we have theoretically inserted or updated a record, we should update
    # our own hashref to be sure we're in sync with the database
    $self->fetchDatabaseRow();
}



# returns the value of the passed table column
# if it exists.
sub get {
	my CachedTableRow $self = shift;
	my $col = shift;
	
	my $row = $self->{row};
	
	if (!$row) {
		return '';	
	}
	
	if ( $row->{$col}) {
		return $row->{$col};	
	}
	
	return '';
}

# Pass this a key value pair and it will set the 
# key to value (but only in the internal representation, not
# in the database).
sub set {
	my CachedTableRow $self = shift;
	my $key = shift;
	my $val = shift;
	
	if (!$key) { return; }
	
	my $row = $self->{row};
	$row->{$key} = $val;
}


# Same as set, but actually *replaces* the entire 
# internal representation with the passed hash ref.
#
# Warning!  This will overwrite any values already in the hashref.
sub setWithHashRef {
	my CachedTableRow $self = shift;
    my $hashref = shift;
    
    if ($hashref) {
        $self->{row} = $hashref;
    }
}


# returns the hashref for this row.
sub row {
	my CachedTableRow $self = shift;

	return $self->{row};	
}


# tests functionality
# class function
sub testUpdate {
    my $ses = Session->new();
    $ses->put('authorizer', 'J. Sepkoski');
    $ses->put('enterer', 'J. Sepkoski');
    $ses->put('enterer_no', 48);
    $ses->put('authorizer_no', 48);
    my %GLOBALVARS;
    $GLOBALVARS{session} = $ses;
    
    my $row = CachedTableRow->new(\%GLOBALVARS, 'opinions', 'opinion_no = 1234');
    
    $row->setUpdateEmptyOnly(0);
    $row->setPrimaryKeyName('opinion_no');
    $row->setPrimaryKeyValue(1234);
   
    Debug::printHash($row->row());
    
    $row->set('pages', 'asdf');
    $row->setDatabaseRow();
    
    Debug::printHash($row->row());
}


# tests functionality
# class function
sub testInsert {
    
    my $ses = Session->new();
    $ses->put('authorizer', 'J. Sepkoski');
    $ses->put('enterer', 'J. Sepkoski');
    $ses->put('enterer_no', 48);
    $ses->put('authorizer_no', 48);

    my %GLOBALVARS;
    $GLOBALVARS{session} = $ses;

    
    my $row = CachedTableRow->new(\%GLOBALVARS, 'opinions');
        
    $row->setUpdateEmptyOnly(0);
    $row->setPrimaryKeyName('opinion_no');
    $row->setPrimaryKeyValue('80012');
   
    Debug::printHash($row->row());
    
    $row->set('pages', 'qwerty');
    $row->set('status', 'belongs to');
    $row->set('comments', 'who knows');
    
    $row->setDatabaseRow();
    
    Debug::printHash($row->row());
}


# end of CachedTableRow.pm


1;