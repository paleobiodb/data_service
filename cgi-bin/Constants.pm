package Constants;
require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw($READ_URL $WRITE_URL $HOST_URL $HTML_DIR $OUTPUT_DIR $DATAFILE_DIR);  # symbols to export on request
use strict;


$Constants::READ_URL = 'bridge.pl';
$Constants::WRITE_URL = 'bridge.pl';

$Constants::HOST_URL = $ENV{'BRIDGE_HOST_URL'};
$Constants::HTML_DIR = $ENV{'BRIDGE_HTML_DIR'};
$Constants::OUTPUT_DIR = "public/data";
$Constants::DATAFILE_DIR = $ENV{'DOWNLOAD_DATAFILE_DIR'};

1;
