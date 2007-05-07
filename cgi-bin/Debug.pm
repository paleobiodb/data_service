# For debugging and error logging to log files
# originally written by rjp, 12/2004
#

package Debug;
require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(dbg);  # symbols to export on request

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

1;
