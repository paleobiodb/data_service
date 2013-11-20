#
# ConfigQuery
# 
# A class that returns information from the PaleoDB database about the
# parameters necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package ConfigData;

use strict;
use base 'DataService::Base';

use Carp qw(carp croak);


our (%SELECT, %TABLES, %PROC, %OUTPUT);

$OUTPUT{basic} = 
   [
    { rec => 'bin_size', com => 'bns',
	doc => "A list of bin sizes, in degrees.  All bins are aligned on 0-0 latitude and longitude.  The length of the list is the number of available summary levels." }
   ];


our (@BIN_RESO);

# configure ( )
# 
# This routine is called by the DataService module, and is passed the
# configuration data as a hash ref.

sub configure {
    
    my ($self, $dbh, $config) = @_;
    
    if ( ref $config->{bins} eq 'ARRAY' )
    {
	my $bin_level = 0;
	
	foreach my $bin (@{$config->{bins}})
	{
	    $bin_level++;
	    
	    next unless $bin->{resolution} > 0;
	    push @BIN_RESO, $bin->{resolution};
	}
    }
}


# get ( )
# 
# Return configuration information.

sub get {

    my ($self) = @_;
    
    $self->{main_record} = { bin_size => \@BIN_RESO };
    return 1;
}


1;
