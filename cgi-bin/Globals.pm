# some global functions which can be used by any other module.

package Globals;

use strict;


sub monthNameToNumber {
	my $name = shift;
	
	my %month2num = (  "January" => "01", "February" => "02", "March" => "03",
                         "April" => "04", "May" => "05", "June" => "06",
                         "July" => "07", "August" => "08", "September" => "09",
                         "October" => "10", "November" => "11",
                         "December" => "12");
	
	my $month = $month2num{$name};  # needs semicolon because it's a hash.. weird.
	
	return $month;
}

		


1;