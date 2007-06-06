#!/usr/local/bin/perl
package taxa_cached;


use FindBin qw($Bin);
use lib ($Bin."/../cgi-bin");
use strict;	

# CPAN modules
use Class::Date qw(date localdate gmdate now);
use POSIX qw(setsid);
use DBTransactionManager;
use TaxaCache;

BEGIN {
    $SIG{'HUP'}  = \&doUpdate;
    $SIG{'USR1'} = \&doUpdate;
    $SIG{'USR2'} = \&doUpdate;
    $SIG{'INT'}  = sub { $taxa_cached::time_to_die = 1;};
    $SIG{'KILL'} = sub { $taxa_cached::time_to_die = 1;};
    $SIG{'QUIT'} = sub { $taxa_cached::time_to_die = 1;};
    $SIG{'TERM'} = sub { $taxa_cached::time_to_die = 1;};
}

daemonize();

my $DEBUG = 1;
my $POLL_TIME = 2;
my $dbt = new DBTransactionManager();
my $dbh = $dbt->dbh;

$taxa_cached::sync_time = TaxaCache::getSyncTime($dbt);
$taxa_cached::in_update = 0;
$taxa_cached::time_to_die = 0;


while(1) {
    doUpdate();
    if ($taxa_cached::time_to_die) {
        print "got termination signal, dying\n" if ($DEBUG);
        exit 0;
    }
    sleep($POLL_TIME);
}

sub doUpdate {
    if ($taxa_cached::in_update) {
        print "already being updated\n" if ($DEBUG);
        return;
    } else {
        $taxa_cached::in_update = 1;
    }
    my %to_update = ();
    my $sql = "SELECT DISTINCT o.child_no,r.modified FROM refs r, opinions o WHERE r.reference_no=o.reference_no AND r.modified > '$taxa_cached::sync_time'";
    print $sql."\n" if ($DEBUG > 1);
    my $rows = $dbt->getData($sql);
    foreach my $row (@$rows) {
        $to_update{$row->{'child_no'}} = $row->{'modified'};
    }
    my $sql = "SELECT DISTINCT o.child_no,o.modified FROM opinions o WHERE o.modified > '$taxa_cached::sync_time'";
    print $sql."\n" if ($DEBUG > 1);
    $rows = $dbt->getData($sql);
    foreach my $row (@$rows) {
        if ($to_update{$row->{'child_no'}}) {
            if ($row->{'modified'} ge $to_update{$row->{'child_no'}}) {
                $to_update{$row->{'child_no'}} = $row->{'modified'};
            }
        } else {
            $to_update{$row->{'child_no'}} = $row->{'modified'};
        }
    }
    my @taxon_nos = sort {$to_update{$a} cmp $to_update{$b}} keys %to_update;
    print "running: found ".scalar(@taxon_nos)." to update\n" if ($DEBUG);
    for(my $i = 0;$i< @taxon_nos;$i++) {
        my $taxon_no = $taxon_nos[$i];
        my $ts = $to_update{$taxon_no};
        print "updating $taxon_no:$ts\n" if ($DEBUG);
        TaxaCache::updateCache($dbt,$taxon_no);
        my $ts = $to_update{$taxon_no};

        my $next_ts = undef;
        if (($i+1) < @taxon_nos) {
            $next_ts = $to_update{$taxon_nos[$i+1]};
        }

        # If the next record was updated in the same second as current, wait on
        # updating the tc_sync table
        unless ($next_ts && $next_ts eq $ts) {
            TaxaCache::setSyncTime($dbt,$ts);
            $taxa_cached::sync_time = $ts;
            print "new sync time $taxa_cached::sync_time\n" if ($DEBUG);
        }
    }
    $taxa_cached::in_update = 0;
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDOUT, '>>/home/peters/testd.log' or die "Can't write to log: $!";
    open STDERR, '>>/home/peters/testd_err.log' or die "Can't write to errlog: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
#    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
#    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid()                    or die "Can't start a new session: $!";
    umask 0;
}
