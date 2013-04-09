# For debugging and error logging to log files
# originally written by rjp, 12/2004
#

package Debug;
require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(dbg);  # symbols to export on request

use Constants qw($CGI_DEBUG);

use strict;

# Debug level, 1 is minimal debugging, higher is more verbose
$Debug::DEBUG = 0;

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

# Utilitiy, no other place to put it PS 01/26/2004
sub printWarnings {
    my @msgs;
    if (ref $_[0]) {
        @msgs = @{$_[0]};
    } else {
        @msgs = @_;
    }
    my $return = "";
    if (scalar(@msgs)) {
        my $plural = (scalar(@msgs) > 1) ? "s" : "";
        $return .= "<br><div class=\"warningBox\">" .
              "<div class=\"warningTitle\">Warning$plural</div>";
        $return .= "<ul>";
        $return .= "<li class='boxBullet'>$_</li>" for (@msgs);
        $return .= "</ul>";
        $return .= "</div>";
    }
    return $return;
}

sub printErrors{
    my @msgs;
    if (ref $_[0]) {
        @msgs = @{$_[0]};
    } else {
        @msgs = @_;
    }
    my $return = "";
    if (scalar(@msgs)) {
        my $plural = (scalar(@msgs) > 1) ? "s" : "";
        $return .= "<br><div class=\"errorBox\">" .
              "<div class=\"errorTitle\">Error$plural</div>";
        $return .= "<ul>";
        $return .= "<li class='boxBullet'>$_</li>" for (@msgs);
        $return .= "</ul>";
        $return .= "</div>";
    }
    return $return;
}  

sub dbg {
    my $message = shift;
    my $level = shift || 1;
    if ( $Debug::DEBUG && $Debug::DEBUG >= $level && $message ) {
        print "<font color='green'>$message</font><br>\n"; 
    }
    return $Debug::DEBUG;
}


# The following two routines are for saving and loading the CGI state.  This
# allows us to make a request using a web brower and then replay it under the
# Perl debugger.  To activate this system, set CGI_DEBUG = 1 in pbdb.conf.
# This will cause the last 5 requests to be saved.  Then run bridge.pl with
# the number of the desired saved request: 1 = most recent, 2 = next-most,
# etc.  Note: this only works if the directory 'saves' exists inside the main
# pbdb directory.

# save_cgi ( q )
# 
# Save the CGI state for later use in debugging.

sub save_cgi {
    
    my ($q) = @_;
    
    my $script_path = $ENV{PWD} . '/' . $0;
    my $savedir_path = $script_path;
    $savedir_path =~ s{/cgi-bin/.*}{/saves};
    
    my ($save_fh);
    
    # Rename each of the save files q<n>.txt for n=1,2...$CGI_DEBUG-1 to
    # q<n+1>.txt.
    
    for ( my $index = $CGI_DEBUG-1; $index > 0; $index-- )
    {
	my $next = $index + 1;
	rename("$savedir_path/q$index.txt", "$savedir_path/q$next.txt");
    }
    
    # Now save the CGI state, including the session cookie and path info, to
    # /saves/q1.txt
    
    my $save_path = ">$savedir_path/q1.txt";
    open($save_fh, $save_path) || die "can't open $save_path: $!";
    
    $q->save($save_fh);
    
    print $save_fh "session: " . $q->cookie('session_id') . "\n";
    print $save_fh "path_info: " . $q->path_info() . "\n";
    
    close $save_fh || die "can't close $save_path: $!";
}


# load_cgi ( num )
# 
# Load the CGI state from a saved file (numbered 1-5; these hold the state for
# the last 5 invocations of bridge.pl.)  Return a list of two objects: a CGI
# object, and a Session object.

sub load_cgi {
    
    my ($dbt, $arg) = @_;
    
    my $script_path = $ENV{PWD} . '/' . $0;
    my $savedir_path = $script_path;
    $savedir_path =~ s{/cgi-bin/.*}{/saves};
    
    my ($q, $s, $save_fh, $line, $print_args);
    
    if ( $arg =~ /^p([0-9]+)$/ )
    {
	$arg = $1;
	$print_args = 1;
    }
    
    die "Bad debug argument: $arg\n" unless $arg >= 1 and $arg <= $CGI_DEBUG;
    
    my $save_path = "$savedir_path/q$arg.txt";
    open ($save_fh, $save_path) || die "can't open $save_path: $!\n";
    
    unless ( $print_args )
    {
	$q = CGI->new($save_fh);
    }
    
    while ( defined($line = <$save_fh>) )
    {
	if ( $print_args )
	{
	    print $line;
	    next;
	}
	
	chomp $line;
	
	if ( $line =~ /^session: (.*)/ )
	{
	    $s = Session->new($dbt, $1);
	}
	elsif ( $line =~ /^path_info: (.*)/ )
	{
	    $q->{'.path_info'} = $1;
	}
    }
    
    if ( $print_args )
    {
	exit;
    }
    
    else
    {
	return $q, $s;
    }
}


1;
