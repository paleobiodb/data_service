# For debugging and error logging to log files
# by rjp, 12/2004
#

package Debug;

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


# pass this an alternating series of variable names and variables
# and it will print them out in a human readable format.
sub printVars {
	if (! @_) { return; }
	
	my $string;
	
	my $i;
	
	for ($i = 0; $i < scalar(@_); $i+= 2) {
		if (($i + 1) < scalar(@_)) {
			$string .= "@_[$i] = '" . @_[$i+1] . "', ";
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


# prints each parameter in the passed CGI object
# note, this is normally called $q in our programs.
sub printAllParams {
	my $q = shift;
	dbPrint("Printing list of all parameters:\n");
	my @params = $q->param();
	my @list;
	foreach my $p (@params) {
		# have to do this carefully because each param can either be a 
		# scalar value, or a list value.
		@list = $q->param($p); 
		dbPrint("$p = " . "'" . join(", ", @list) . "'");	
	}
	dbPrint("End of parameter list\n\n");	
}

1

