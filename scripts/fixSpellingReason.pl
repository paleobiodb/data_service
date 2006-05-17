#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBConnection;
use DBTransactionManager;
use Data::Dumper;
use Opinion;

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);


$doUpdate = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdate = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}  
#
# This scripts  find (and optionally fixes) problems with chained reocmbinations
#


$sql = "SELECT o.opinion_no,o.child_no,o.child_spelling_no,o.status,a1.taxon_name child_name,a1.taxon_rank child_rank, a2.taxon_name child_spelling_name, a2.taxon_rank child_spelling_rank FROM opinions o LEFT JOIN authorities a1 ON o.child_no=a1.taxon_no LEFT JOIN authorities a2 on o.child_spelling_no=a2.taxon_no";
$sth = $dbh->prepare($sql);
$sth->execute();

while($row=$sth->fetchrow_hashref()) {
    $spelling_reason = ""; 
    $status_new = "";

    if ($row->{'status'} =~ /belongs to|rank|recombined|revalidated|corrected/) {
        $status_new = 'belongs to';
    } elsif ($row->{'status'} =~ /nomen|synonym|homonym|replaced/) {
        $status_new = $row->{'status'};
    }

    my $child = {
        'taxon_no'=>$row->{'child_no'},
        'taxon_name'=>$row->{'child_name'},
        'taxon_rank'=>$row->{'child_rank'}
    };
    my $spelling = {
        'taxon_no'=>$row->{'child_spelling_no'},
        'taxon_name'=>$row->{'child_spelling_name'},
        'taxon_rank'=>$row->{'child_spelling_rank'}
    };
    $spelling_reason = Opinion::guessSpellingReason($child,$spelling);

    if (!$status_new || !$spelling_reason) { 
        print "ERROR: could not set status_new($status_new) or speling_reason($spelling_reason), unknown $row->{status} for $row->{opinion_no}\n";
        next;
    }
   
    $sql = "UPDATE opinions SET modified=modified,status_new='$status_new',spelling_reason='$spelling_reason' WHERE opinion_no=$row->{opinion_no}";
    print "#$row->{opinion_no}: $row->{child_no} ($row->{child_spelling_no}) $row->{status}\n";
    print "  $sql\n ";
    if ($doUpdate) {
        $dbh->do($sql);
    }
}

sub dbg {
}
