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

foreach $table (('marinepct','fivepct')) {
    $sql = "SELECT ref_no, pubtitle, $table.language FROM $table, refs WHERE $table.ref_no=refs.reference_no";
    print "\nExecuting sql for table $table: $sql\n";
    @rs = @{$dbt->getData($sql)};
    foreach $row (@rs) {
        if ($row->{'language'}) {
            if (exists $lang{$row->{'language'}}) {
                $language = $row->{'language'};
            } else {
                $language = 'other';
            }
            if (!$refset{$row->{ref_no}}) {
                $sql = "UPDATE refs SET modified=modified, language=".$dbh->quote($language)." WHERE reference_no=$row->{ref_no}";
                $refset{$row->{ref_no}}++;
                print $sql."\n";
                $dbh->do($sql) if ($doUpdates);
                #if ($row->{'pubtitle'}) {
                #    $sql = "SELECT count(*) c FROM refs WHERE pubtitle LIKE BINARY ".$dbh->quote($row->{'pubtitle'});
                #    @rs2 = @{$dbt->getData($sql)};
                #    if ($rs2[0]->{c}) {
                #        $pubtitles{$row->{pubtitle}}{$row->{language}} = $rs2[0]->{c};
                #    }
                #}
            }
        }
    }
}


#while(($pubtitle,$langref)=each %pubtitles) {
#    if (scalar(keys(%{$langref})) > 1) {
#        print "ERROR: multiple languages for $pubtitle: ";
#        print join (" ",keys(%{$langref}));
#        print "\n";
#    } else {
#        while (($lang,$total) = each %$langref) {
#            print "$total with same language $lang, pubtitle: $pubtitle\n";
#        }
#    }
#}
