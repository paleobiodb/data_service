package Debug;


# prints the passed string to a debug file
# called "debug_log"
# added by rjp on 12/18/2003
sub dbPrint {
	$| = 1;	# flushes buffer immediately

	open LOG, ">>debug_log";
	my $date = `date`;
	chomp($date);
 
	my $string = shift;
	chomp($string);
 
	print LOG "$date: $string \n";
}

1

