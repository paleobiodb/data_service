# For debugging and error logging to log files
# originally written by rjp, 12/2004
#

package Debug;

use strict;

# prints the passed string to a debug file
# called "debug_log"
# added by rjp on 12/18/2003
sub dbPrint {

	open LOG, ">>debug_log";
	my $date = `date`;
	chomp($date);
 
	my $string = shift;
	chomp($string);
 
 	# make the file "hot" to ensure that the buffer is flushed properly.
	# see http://perl.plover.com/FAQs/Buffering.html for more info on this.
 	my $ofh = select LOG;
	$| = 1;
	select $ofh;
	
	print LOG "$date: $string \n";
}


# same as dbPrint, but without the timestamp
sub quickPrint {

	open LOG, ">>debug_log";
	
	my $string = shift;
	chomp($string);
 
 	# make the file "hot" to ensure that the buffer is flushed properly.
	# see http://perl.plover.com/FAQs/Buffering.html for more info on this.
 	my $ofh = select LOG;
	$| = 1;
	select $ofh;
	
	print LOG "$string \n";
}


# pass this an alternating series of variable names and variables
# and it will print them out in a human readable format.
sub printVars {
	if (! @_) { return; }
	
	my $string;
	
	my $i;
	
	for ($i = 0; $i < scalar(@_); $i+= 2) {
		if (($i + 1) < scalar(@_)) {
			$string .= "$_[$i] = '" . $_[$i+1] . "', ";
		}
	}
	
	$string =~ s/, $//;
	
	dbPrint("\nVariable List: " . $string);
}


# logs an error message to the error_log
sub logError {
	$| = 1;	# flushes buffer immediately

	open LOG, ">>error_log";
	my $date = `date`;
	chomp($date);
 
	my $string = shift;
	chomp($string);
	
	# make the file "hot" to ensure that the buffer is flushed properly.
	# see http://perl.plover.com/FAQs/Buffering.html for more info on this.
 	my $ofh = select LOG;
	$| = 1;
	select $ofh;
 
	print LOG "Error, $date: $string \n";	
}


# prints the passed hashref in
# a nice user readable form
sub printHash {
	my $hr = shift;
	
	if (!$hr) {
		return;
	}
	
	my %hash = %$hr;
	
	my $toprint;
	
	foreach my $key (keys(%hash)) {
		$toprint .= "$key = '" . $hash{$key} . "'\n";	
	}
	
	dbPrint($toprint);
}


# prints the passed arrayref in
# a nice user readable form
sub printArray {
	my $ar = shift;
	
	if (!$ar) {
		return;
	}
	
	my @array = @$ar;
	
	my $toprint;
	
	foreach my $key (@array) {
		$toprint .= "$key\n";	
	}
	
	dbPrint($toprint);
}

# prints each parameter in the passed CGI object
# note, this is normally called $q in our programs.
sub printAllCGIParams {
	my $q = shift;

	if (!$q) {
		dbPrint("CGI object doesn't exist in printAllCGIParams()");
		return;
	}
	
	quickPrint("_____________________________________________________________");
	dbPrint("Printing list of all CGI parameters:");
	my @params = $q->param();
	my @list;
	my $result;
	foreach my $p (@params) {
		# have to do this carefully because each param can either be a 
		# scalar value, or a list value.
		@list = $q->param($p); 
		$result .= "$p = " . "'" . join(", ", @list) . "'\n";	
	}
	quickPrint("\n$result");
	quickPrint("End of CGI parameter list\n_____________________________________________________________\n");	
}


# prints out all session parameters
# for debugging purposes
#
# pass it a session object
sub printAllSessionParams {
	my $s = shift;
	
	quickPrint("_____________________________________________________________");
	dbPrint("Printing list of all Session parameters:");
	quickPrint("\n" . $s->allKeysAndValues);
	quickPrint("End of Session parameter list\n_____________________________________________________________\n");	

}

1

