#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents a single row from any data table


package CachedTableRow;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use CGI::Carp qw(fatalsToBrowser);


use fields qw(	
				table
				where
				
				row
				
				SQLBuilder
							);  # list of allowable data fields.

						

# Must pass this the table name
# and the where clause.
#
# For example, my $row = CachedTableRow->new('authorities', 'taxon_no = 5');
#
sub new {
	my $class = shift;
	my $table = shift;
	my $where = shift;

	my CachedTableRow $self = fields::new($class);
	
	$self->{table} = $table;
	$self->{where} = $where;
	
	$self->runQuery();
	
	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my CachedTableRow $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
	}
	
	return $SQLBuilder;
}


# internal use only
sub runQuery {
	my CachedTableRow $self = shift;
	
	my $sql = $self->getSQLBuilder();
	$sql->setSQLExpr("SELECT * FROM " . $self->{table} . " WHERE " . $self->{where});
		
	$sql->executeSQL();
		
	my $row = $sql->nextResultHashRef();
	
	$sql->finishSQL();
	
	$self->{row} = $row;
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

# returns the hashref for this row.
sub row {
	my CachedTableRow $self = shift;

	return $self->{row};	
}



# end of CachedTableRow.pm


1;