package Constants;
require Exporter;
use FindBin;

@ISA = qw(Exporter);
@EXPORT_OK = qw($READ_URL $WRITE_URL $HOST_URL $HTML_DIR $DATA_DIR $SQL_DB $DB_TYPE $DB_USER $DB_SOCKET $DB_PASSWD $IS_FOSSIL_RECORD $TAXA_TREE_CACHE $TAXA_LIST_CACHE $IP_MAIN $IP_BACKUP $DB $PAGE_TOP $PAGE_BOTTOM $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO $ALLOW_LOGIN $CGI_DEBUG $ADMIN_EMAIL);  # symbols to export on request
use strict;

# general constants
$Constants::conf = read_conf();
my $conf = $Constants::conf;

$Constants::HOST_URL        = $conf->{'HOST_URL'};
$Constants::HTML_DIR        = $conf->{'HTML_DIR'};
$Constants::DATA_DIR        = $conf->{'DATA_DIR'};
$Constants::DB_SOCKET       = $conf->{'DB_SOCKET'};
$Constants::DB_PASSWD       = $conf->{'DB_PASSWD'};
$Constants::DB_USER	    = $conf->{'DB_USER'} || 'pbdbuser';
$Constants::ALLOW_LOGIN	    = $conf->{'ALLOW_LOGIN'};
$Constants::CGI_DEBUG	    = $conf->{'CGI_DEBUG'};
$Constants::ADMIN_EMAIL	    = $conf->{'ADMIN_EMAIL'};
$Constants::IP_MAIN         = '137.111.92.50';
$Constants::IP_BACKUP       = '137.111.92.50';

$Constants::IS_FOSSIL_RECORD = $conf->{'IS_FOSSIL_RECORD'};

if ($Constants::IS_FOSSIL_RECORD) {
    $Constants::TAXA_TREE_CACHE = 'taxa_tree_cache_fr';
    $Constants::TAXA_LIST_CACHE = 'taxa_list_cache_fr';
    $Constants::READ_URL = 'bridge.pl';
    $Constants::WRITE_URL = 'bridge.pl';
} else {
    $Constants::TAXA_TREE_CACHE = 'taxa_tree_cache';
    $Constants::TAXA_LIST_CACHE = 'taxa_list_cache';
    $Constants::READ_URL = 'bridge.pl';
    $Constants::WRITE_URL = 'bridge.pl';
}

$Constants::DB = 'pbdb';
$Constants::SQL_DB = 'pbdb';
$Constants::DB_TYPE = '';
$Constants::PAGE_TOP = 'std_page_top';
$Constants::PAGE_BOTTOM = 'std_page_bottom';
$Constants::COLLECTIONS = 'collections';
$Constants::COLLECTION_NO = 'collection_no';
$Constants::OCCURRENCES = 'occurrences';
$Constants::OCCURRENCE_NO = 'occurrence_no';
if ( $ENV{'HTTP_USER_AGENT'} =~ /Mobile/i && $ENV{'HTTP_USER_AGENT'} !~ /iPad/i )	{
    $Constants::PAGE_TOP = 'mobile_top';
    $Constants::PAGE_BOTTOM = 'mobile_bottom';
}


sub read_conf {
    my $base_dir = $FindBin::RealBin;
    $base_dir =~ s/\/(upload|cgi-bin|scripts|html)(\/.*)*$/\/config/;
    my $filename = "$base_dir/pbdb.conf";
    my $cf;
    open $cf, "<$filename" or die "Can not open $filename\n";
    my %conf = ();
    while(my $line = readline($cf)) {
        chomp($line);
        if ($line =~ /^\s*(\w+)\s*=\s*(.*)$/) {
            $conf{uc($1)} = $2; 
        }
    }
    return \%conf;
}

1;
