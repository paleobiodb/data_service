package Constants;
require Exporter;
use FindBin;

@ISA = qw(Exporter);
@EXPORT_OK = qw($conf $READ_URL $WRITE_URL $HOST_URL $HTML_DIR $DATA_DIR $IS_FOSSIL_RECORD $TAXA_TREE_CACHE $TAXA_LIST_CACHE $IP_MAIN $IP_BACKUP);  # symbols to export on request
use strict;

# general constants
$Constants::conf = read_conf();
my $conf = $Constants::conf;

$Constants::HOST_URL        = $conf->{'HOST_URL'};
$Constants::HTML_DIR        = $conf->{'HTML_DIR'};
$Constants::DATA_DIR        = $conf->{'DATA_DIR'};
$Constants::IP_MAIN         = '128.111.220.135';
$Constants::IP_BACKUP       = '128.111.220.138';

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


sub read_conf {
    FindBin::again();
    my $base_dir = $FindBin::Bin;
    my $filename = "$base_dir/../config/pbdb.conf";
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
