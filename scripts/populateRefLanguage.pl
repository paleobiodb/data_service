#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;


$doUpdates = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}


$driver =       "mysql";
$host =         "localhost";
$user =         "pbdbuser";
$db =           "pbdb";

open PASSWD,"</home/paleodbpasswd/passwd";
$password = <PASSWD>;
$password =~ s/\n//;
close PASSWD;

my $dbh = DBI->connect("DBI:$driver:database=$db;host=$host", $user, $password, {RaiseError => 1});

# Make a Global Transaction Manager object
my $s = Session->new();
my $dbt = DBTransactionManager->new($dbh, $s);

@languages = ('Chinese','English','French','German','Italian','Japanese','Portugese','Russian','Spanish','other');
@lang{@languages} = ();
%refset = ();
$pubtitles = {};

%tables = ('marinepct'=>'1%','fivepct'=>'5%');

#$sql = "SELECT ref_no, pubtitle, $table.language FROM $table, refs WHERE $table.ref_no=refs.reference_no";
$sql1 = "SELECT * FROM refs WHERE project_ref_no IS NOT NULL AND project_name LIKE '5\\%'";
$sql2 = "SELECT * FROM refs WHERE project_ref_no IS NOT NULL AND project_name LIKE '1\\%'";
$sql = "($sql1) UNION ($sql2)";


$sth = $dbh->prepare($sql);
$sth->execute();

while ($row = $sth->fetchrow_hashref()) {
    $ref_no = $row->{project_ref_no}/20;
    if ($row->{project_name} eq '1%') {
        $table = "marinepct";
    } elsif ($row->{project_name} eq '5%') {
        $table = "fivepct";
    } else {
        die "Unknown project_name? $row->{project_name}";
    }
    $sql = "SELECT * FROM $table WHERE ref_no=$ref_no";
    $lrow = ${$dbt->getData($sql)}[0];

    if (!$lrow) {
        die ("Could not find $ref_no in $table?");
    }
   
    if ($lrow->{'language'}) {
        if (exists $lang{$lrow->{'language'}}) {
            $language = $lrow->{'language'};
        } else {
            $language = 'other';
        }
        print "updating #$row->{reference_no} set by $ref_no in table $table.";
        if ($row->{reftitle} ne $lrow->{title}) {
            print "Titles not equal? title ref: $row->{reftitle} -- title in table: $lrow->{title}\n";
        }
        
        $sql = "UPDATE refs SET modified=modified, language=".$dbh->quote($language)." WHERE reference_no=$row->{reference_no}";
        $refset{$row->{ref_no}}++;
        print $sql."\n";
        $dbh->do($sql) if ($doUpdates);
        #if ($row->{'pubtitle'}) {
        #    $sql = "SELECT count(*) c FROM refs WHERE pubtitle LIKE BINARY ".$dbh->quote($row->{'pubtitle'});
        #    @rs2 = @{$dbt->getData($sql)};
        #    if ($rs2[0]->{c}) {
        #        $pubtitles{$row->{pubtitle}}{$lrow->{language}} = $rs2[0]->{c};
        #    }
        #}
    }
}


#while(($pubtitle,$langref)=each %pubtitles) {
#    if (scalar(keys(%{$langref})) > 1) {
#        print "ERROR: multiple languages for $pubtitle: ";
#        print join (" ",keys(%{$langref}));
#        print "\n";
#    } else {
#        while (($lang,$total) = each %$langref) {
#            if ($total > 1) {
#                print "$total with same language $lang, pubtitle: $pubtitle\n";
#            }
#        }
#    }
#}
