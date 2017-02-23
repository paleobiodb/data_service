#
# ContributorTables.pm
#
# Create and manage tables for recording information about database contributors.
#


package ContributorTables;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(buildContributorTables);

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($CONTRIB_STATS $CONTRIB_COUNTS $USERS_TABLE);

use CoreFunction qw(activateTables);
use ConsoleLog qw(logMessage);

our $CONTRIB_COUNTS_WORK = "cocw";
our $CONTRIB_STATS_WORK = "cosw";


# buildContributorStats ( dbh )
#
# Rebuild the contributor statistics table.

sub buildContributorTables {

    my ($dbh, $options) = @_;

    $options ||= { };
    
    my ($result, $sql);
    
    logMessage(2, "Computing contributor counts...");
    
    # First create working tables.
    
    my $table_name;
    my $users_table;
    
    try {

	$table_name = $CONTRIB_COUNTS_WORK;
	
	$dbh->do("DROP TABLE IF EXISTS $CONTRIB_COUNTS_WORK");
	$dbh->do("CREATE TABLE $CONTRIB_COUNTS_WORK (
			person_no int unsigned not null,
			role enum ('authorizer', 'enterer', 'student') not null,
			recordtype enum ('occurrences', 'collections', 'authorities', 'opinions') not null,
			records int unsigned not null,
			first datetime not null,
			latest datetime not null,
			UNIQUE KEY (person_no, role, recordtype))");
	
	$table_name = $CONTRIB_STATS_WORK;
	
	$dbh->do("DROP TABLE IF EXISTS $CONTRIB_STATS_WORK");
	$dbh->do("CREATE TABLE $CONTRIB_STATS_WORK (
			timespan smallint unsigned,
			role enum ('authorizer', 'enterer', 'student') not null,
			count int unsigned not null,
			UNIQUE KEY (timespan, role))");
	
    } catch {
	
	logMessage(1, "ABORTING: could not create table '$table_name'");
	return;
    };

    # Check for existence of user table.

    try {

	$users_table = $dbh->do("SELECT COUNT(*) FROM $USERS_TABLE");
    };
    
    # Populate the working counts table.
    
    try {
	
	foreach my $role ('authorizer', 'enterer')
	{
	    foreach my $recordtype ('occurrences', 'collections', 'authorities', 'opinions')
	    {
		$sql = "
		INSERT INTO $CONTRIB_COUNTS_WORK (person_no, role, recordtype, records, first, latest)
		SELECT ${role}_no, '$role', '$recordtype', count(*), min(created), max(created)
		FROM $recordtype
		GROUP BY ${role}_no";
		
		$result = $dbh->do($sql);
		
		logMessage(2, "    Found $result for $role - $recordtype");
	    }
	}
	
    } catch {
	
	logMessage(1, "ABORTING");
	return;
    };
    
    # Populate the working stats table.
    
    logMessage(2, "    Computing contributor stats...");
    
    try {
	
	# Iterate over roles and timespans

    ROLE:
	foreach my $role ('authorizer', 'enterer', 'student')
	{
	    my @clauses;
	    my $users_join = $users_table ? " join $USERS_TABLE as u using (person_no)" : "";
	    
	    push @clauses, "c.person_no > 0";
	    
	    if ( $role eq 'authorizer' )
	    {
		push @clauses, "c.role = 'authorizer'";
	    }
	    
	    elsif ( $role eq 'enterer' )
	    {
		push @clauses, "(c.role = 'authorizer' or c.role = 'enterer')";
		push @clauses, "u.role <> 'student'" if $users_table;
	    }
	    
	    elsif ( $users_table )
	    {
		push @clauses, "c.role = 'enterer'";
		push @clauses, "u.role = 'student'";
	    }

	    else
	    {
		next ROLE;
	    }
	    
	    foreach my $timespan (0, 5, 3, 1)
	    {
		my $timespan_clause;

		if ( $timespan )
		{
		    $timespan_clause = "c.latest >= DATE_SUB(NOW(), INTERVAL $timespan YEAR)";
		}

		else
		{
		    $timespan_clause = "1 = 1";
		}

		my $filter = join(' and ', @clauses, $timespan_clause);
		
		$sql = "
			INSERT INTO $CONTRIB_STATS_WORK (timespan, role, count)
			SELECT $timespan, '$role', count(distinct person_no)
			FROM $CONTRIB_COUNTS_WORK as c $users_join
			WHERE $filter";

		# print STDERR "$sql\n\n";
		
		$result = $dbh->do($sql);

		logMessage(2, "        found $result for $role - $timespan");
	    }
	}

    } catch {
	
	logMessage(1, "ABORTING");
	return;
    };

    # Then check to see if we need to update the tables themselves. If so, delete them and rename
    # the working tables.
    
    if ( $options->{new_tables} )
    {
	local $dbh->{AutoCommit} = 0;

	try {
	    
	    activateTables($dbh, $CONTRIB_COUNTS_WORK => $CONTRIB_COUNTS,
			   $CONTRIB_STATS_WORK => $CONTRIB_STATS);
	    
	} catch {

	    logMessage(1, "ABORTING");
	    return;
	};
    }
    
    # Otherwise, copy data from the working tables to the existing ones. We do this as a single transaction.
    
    else
    {
	local $dbh->{AutoCommit} = 0;
	
	try {
	    
	    $dbh->do("DELETE FROM $CONTRIB_STATS");
	    $dbh->do("INSERT INTO $CONTRIB_STATS SELECT * FROM $CONTRIB_STATS_WORK");

	    $dbh->do("DELETE FROM $CONTRIB_COUNTS");
	    $dbh->do("INSERT INTO $CONTRIB_COUNTS SELECT * FROM $CONTRIB_COUNTS_WORK");
	    
	    $dbh->commit;

	    $dbh->do("DROP TABLE IF EXISTS $CONTRIB_STATS_WORK");
	    $dbh->do("DROP TABLE IF EXISTS $CONTRIB_COUNTS_WORK");

	    logMessage(2, "    successfully updated contributor tables");
	    
	} catch {

	    logMessage(1, "ABORTING");
	    return;
	};
    }
}

1;
