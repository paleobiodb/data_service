package DBTransactionManager;
 
# just what it says... this automagically replaces die() calls
use CGI::Carp qw(fatalsToBrowser);

## new
#
#	description:	constructor
#
#	parameters:		$dbh		(Already connected) database handle.
#					$session	session object (to know who we are - for
#								doing permissions work).
#
##
sub new{
	my $class = shift;
	my $self = {};
	bless $self, $class;

	$self->{_dbh} = shift;
	$self->{_session} = shift;

	#_initialize();

	return $self;
}

## _initialize
#
##
sub _initialize{
	my $self = shift;
	# This method doesn't do anything yet.
}

## get_data
#
#	description:	this method is the meat of this class. It handles basic
#					SQL syntax checking, it executes the statment, and it
#					returns data that successfully makes it past the 
#					Permissions module.
#
#	parameters:		$sql			The SQL statement to be executed
#					$type			"read", "write", "both", "neither"
#					$attr_hash-ref	reference to a hash whose keys are set by
#					the user according to which attributes are desired for this
#					table, e.g. "NAMES" (required!), "mysql_is_num", "NULLABLE",
#					etc. Upon calling this method, the values will be set as
#					references to arrays whose elements correspond to each 
#					column in the table, in the order of the NAMES attribute,
#					which is why "NAMES" is required (to correlate order).
#
#	returns:		For select statements, returns a reference to an array of 
#					hash references	to rows of all data returned.
#					For non-select statements, returns the number of rows
#					affected.
#					Returns empty anonymous array on failure.
##
sub getData{
	my $self = shift;
	my $sql = shift;
	my $type = (shift or "neither");
	my $attr_hash_ref = shift;

	# First, check the sql for any obvious problems
	my $sql_result = $self->checkSQL($sql);
	if($sql_result){
		my $sth = $self->{_dbh}->prepare($sql) or die $self->{_dbh}->errstr;
		# execute returns the number of rows affected for non-select statements.
		# SELECT:
		if($sql_result == 1){
			$sth->execute();
			# Ok now attributes are accessible
			if(scalar(keys(%{$attr_hash_ref})) > 0){
				foreach my $key (keys(%{$attr_hash_ref})){
					$attr_hash_ref->{$key} = $sth->{$key};
				}	
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
			my $num = $sth->execute();
			$sth->finish();
			return $num;
		}
	} # Don't execute anything that doesn't make it past checkSQL
	else{
		return [];
	}
}

## checkSQL
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
	my $self = shift;
	my $sql = shift;

	# uppercase the whole thing for ease of use
	$sql = uc($sql);

	# Is this a SELECT, INSERT, UPDATE or DELETE?
	$sql =~/^(\w+)\s+/;
	my $type = $1;

	if(!$self->checkWhereClause($sql)){
		die "Bad WHERE clause in SQL: $sql";
	}
	
	if($type eq "SELECT"){
		return 1;
	}
	elsif($type eq "INSERT"){
		# Check that the columns and values lists are not empty
		# NOTE: down the road, we could check required fields
		# against table names.
		$sql =~ /(\(.*?\))\s+VALUES\s+(\(.*?\))/;
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
		if($sql !~ /WHERE/){
			return 0;
		}
		return 3;
	}
	elsif($type eq "DELETE"){
		# Try to avoid deleting all records from a table
		if($sql !~ /WHERE/){
			return 0;
		}
		else{
			return 4;
		}
	}
}

##
#
##
sub checkWhereClause{
	my $self = shift;
	my $sql = shift; # Note: this has already been uppercase-d by the caller.
	
	# This method is a 'pass-through' if there is no WHERE clause.
	if($sql !~ /WHERE/){
		return 1;
	}

	# This is only 'first-pass' safe. Could be more robust if we check
	# all AND clauses.
	$sql =~ /WHERE\s+([A-Z0-9_\.\(]+)\s*(=|LIKE|IN)\s*(.+)?\s*/;

	#print "\$1: $1, \$2: $2<br>";
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

1;
