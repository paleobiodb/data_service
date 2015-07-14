# 
# ConsoleLog.pm
# 
# Basic logging routines.


package ConsoleLog;

use strict;

use base qw(Exporter);

our (@EXPORT_OK) = qw(initMessages logMessage logTimestamp);


# Controlling variables for debug messages

our $MSG_TAG = 'unknown';
our $MSG_LEVEL = 1;



# initMessages ( level, tag )
# 
# Initialize a timer, so that we can tell how long each step takes.  Also set
# the $MSG_LEVEL and $MSG_TAG parameter.  

my ($START_TIME);

sub initMessages {
    
    my ($level, $tag) = @_;
    
    $MSG_LEVEL = $level if defined $level;    
    $MSG_TAG = $tag if defined $tag;
    $START_TIME = time;
}


# logMessage ( level, message )
# 
# If $level is greater than or equal to the package variable $MSG_LEVEL, then
# print $message to standard error.

sub logMessage {

    my ($level, $message) = @_;
    
    return if $level > $MSG_LEVEL;
    
    my $elapsed = time - $START_TIME;    
    my $elapsed_str = sprintf("%2dm %2ds", $elapsed / 60, $elapsed % 60);
    
    print STDOUT "$MSG_TAG: [ $elapsed_str ]  $message\n";
}


# logTimestamp ( gmt )
# 
# Print a timestamp to standard error.  If the first parameter is the string
# 'gmt', then use GMT instead of localtime.

sub logTimestamp {
    
    my $param = lc $_[0];
    
    my $now = $param eq 'gmt' ? gmtime : localtime;
    
    my $elapsed = time - $START_TIME;    
    my $elapsed_str = sprintf("%2dm %2ds", $elapsed / 60, $elapsed % 60);
    
    print STDOUT "$MSG_TAG: [ $elapsed_str ]  $now\n";
}

1;
