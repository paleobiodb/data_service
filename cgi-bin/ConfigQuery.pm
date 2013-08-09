#
# ConfigQuery
# 
# A class that returns information from the PaleoDB database about the
# parameters necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package ConfigQuery;

use strict;
use base 'DataQuery';

use Carp qw(carp croak);


our (%CONFIG);
read_config();


our (%SELECT, %TABLES, %PROC, %OUTPUT);

$OUTPUT{single} = 
   [
    { rec => 'bin_size', com => 'bns',
	doc => "A list of bin sizes, in degrees.  All bins are aligned on 0-0 latitude and longitude.  The length of the list is the number of available summary levels." }
   ];

our (%DOC_ORDER);

$DOC_ORDER{'single'} = ['single'];


# fetchSingle ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub fetchSingle {

    my ($self) = @_;
    
    $self->{main_record} = { bin_size => [ $CONFIG{COARSE_BIN_SIZE}, $CONFIG{FINE_BIN_SIZE} ] };
    return 1;
}



sub read_config {
    
    my $filename = "config/pbdb.conf";
    my $cf;
    open $cf, "<$filename" or die "Can not open $filename\n";
    while(my $line = readline($cf)) {
        chomp($line);
        if ($line =~ /^\s*(\w+)\s*=\s*(.*)$/) {
            $CONFIG{uc($1)} = $2; 
        }
    }
}

1;
