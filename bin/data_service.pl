#!/usr/bin/env perl
# 
# Paleobiology Database Data Service
# 
# This program reads configuration information from the file 'config.yml' and
# then launches the 'starman' web server to provide a data service for the
# Paleobiology Database.
# 
# The relevant configuration parameters are:
# 
# port - port on which to listen
# workers - how many active data service processes to maintain
# 



use strict;

use Dancer ':script';


my $PORT = config->{port}|| 3000;
my $WORKERS = config->{workers} || 5;
my $ACCESS_LOG = config->{access_log} || 'pbapi_log';

unless ( $ACCESS_LOG =~ qr{/} )
{
    $ACCESS_LOG = "logs/$ACCESS_LOG";
}

my $pid_file;

open($pid_file, ">", "starman.pid");
print $pid_file $$;
close $pid_file;

sleep(5);

exec('/usr/local/bin/starman', 
     '--listen', ":$PORT", '--workers', $WORKERS, '--access-log', $ACCESS_LOG, 
     'bin/web_app.pl')
    
    or die "Could not run program /opt/local/bin/starman: $!";







