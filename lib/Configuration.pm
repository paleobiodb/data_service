#
# ConfigQuery
# 
# A class that returns information from the PaleoDB database about the
# parameters necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package Configuration;

use strict;
use base 'DataService::Base';

use Carp qw(carp croak);


our (%CONFIG);
read_config();


our (%SELECT, %TABLES, %PROC, %OUTPUT);

$OUTPUT{basic} = 
   [
    { rec => 'bin_size', com => 'bns',
	doc => "A list of bin sizes, in degrees.  All bins are aligned on 0-0 latitude and longitude.  The length of the list is the number of available summary levels." }
   ];


# get ( )
# 
# Return configuration information.

sub get {

    my ($self) = @_;
    
    $self->{main_record} = { bin_size => [ $CONFIG{COARSE_BIN_SIZE}, $CONFIG{FINE_BIN_SIZE} ] };
    return 1;
}



sub read_config {
    
    my $filename = "config/pbdb.conf";
    my $cf;
    unless ( open $cf, "<$filename" )
    {
	carp "Can not open $filename\n";
	return;
    }
    while(my $line = readline($cf)) {
        chomp($line);
        if ($line =~ /^\s*(\w+)\s*=\s*(.*)$/) {
            $CONFIG{uc($1)} = $2; 
        }
    }
}

1;
