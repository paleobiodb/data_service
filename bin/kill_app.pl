#!/usr/bin/env perl
# 
# Kill any instance of 'perl bin/web_app.pl'.

my ($ps_cmd);


open $ps_cmd, "ps -ef |";

while ( <$ps_cmd> )
{
    if ( m{ ^ \s+ \d+ \s+ (\d+) .* perl \s bin/web_app.pl }x )
    {
	print "killing process $1\n";
	system("kill $1");
    }
}
