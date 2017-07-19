#!/opt/local/bin/perl
# 
# vm_check.pl
# 
# Check the PBDB database to see if any new resources have been submitted or updated, and send an
# e-mail message if so.

use strict;

use lib '../lib', 'lib';
use feature 'say';
use Getopt::Std;
use YAML qw(Load);

use CoreFunction qw(connectDB);
use TableDefs qw($RESOURCE_QUEUE);

no warnings 'uninitialized';


# Connect to the database.

my $dbh = connectDB() || exit;


# See if we have a 'last_id' value.

my $sql;
my ($last_checked);

eval {
    ($last_checked) = $dbh->selectrow_array("
	SELECT last_checked FROM resource_check");
};


# If not, try creating and initializing the table, and then exit. We will check again later.

unless ( $last_checked )
{
    $dbh->do("	CREATE TABLE IF NOT EXISTS `resource_check` (
		last_checked timestamp not null )");
    
    $dbh->do("	DELETE FROM `resource_check`");
    
    $dbh->do("	INSERT INTO `resource_check` (last_checked) VALUES (NOW())");

    say "INITIALIZED TABLE";
    exit;
}


# Otherwise, see if there are any resources that have been posted or updated since the last
# check. If so, output some information about them which will be sent as an e-mail.

else
{
    # Select just the first resource that we find. Subsequent ones will be reported by subsequent
    # invocations of this script.

    my $quoted_mod = $dbh->quote($last_checked);
    my $results;
    
    eval {
	$results = $dbh->selectall_arrayref("
	SELECT eduresource_no, status, title, author, modified FROM $RESOURCE_QUEUE
	WHERE status in ('pending', 'changes') and modified > $quoted_mod", { Slice => { } });
    };
    
    # If we find any, update and report them. In any case, update the last_checked time.
    
    $dbh->do("DELETE FROM `resource_check`");
    $dbh->do("INSERT INTO `resource_check` (last_checked) VALUES(NOW())");
    
    if ( ref $results eq 'ARRAY' && @$results )
    {
	foreach my $r ( @$results )
	{
	    my $heading;

	    if ( $r->{status} eq 'pending' )
	    {
		$heading = 'New resource submitted';
	    }

	    else
	    {
		$heading = 'Resource record updated';
	    }
	    
	    print "$heading:\n\n";
	    print "Status: $r->{status}\n";
	    print "Title: $r->{title}\n";
	    print "Author: $r->{author}\n";
	    print "Last modified: $r->{modified}\n\n";
	}
    }    
}
