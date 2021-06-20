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

use YAML::Any qw(LoadFile);

# Read the main configuration file, and supply defaults for any missing entries. Throw an
# exception if the configuration file is not found, which will prevent the container from starting
# up.

my $config = LoadFile("config.yml") || die "Could not read config.yml: $!\n";

my $PORT = $config->{port}|| 3000;
my $WORKERS = $config->{workers} || 3;
my $ACCESS_LOG = $config->{access_log} || 'pbapi_access.log';

# Do a final check before starting: try to run the main web app, and make sure it actually
# produces useful output. If this fails, we would like to know it immediately rather than
# continually spawning failed processes.

print STDOUT "Checking that the web application runs properly...\n";

my $precheck = `perl bin/web_app.pl GET /data1.2/`;

unless ( $precheck && $precheck =~ qr{<html><head>}m )
{
    print STDOUT "The application web_app.pl was not able to run successfully, terminating container.\n";
    exit;
}

print STDOUT "Passed.\n";

# Establish signal handlers to kill the child processes if we receive a QUIT, INT, or TERM signal.

$SIG{INT} = sub { &kill_dataservice('INT') };
$SIG{QUIT} = sub { &kill_dataservice('QUIT') };
$SIG{TERM} = sub { &kill_dataservice('TERM') };

# If we get here, there is a good chance that everything is fine. So start the data service.

system("start_server --port $PORT --pid-file=dataservice.pid -- starman --workers $WORKERS --access-log=logs/$ACCESS_LOG --preload-app bin/web_app.pl &");

print STDOUT "Started data service.\n";

while ( 1 )
{
    sleep(3600);
}


# my $pid_file;

# open($pid_file, ">", "starman.pid");
# print $pid_file $$;
# close $pid_file;

# sleep(5);

# exec('/usr/local/bin/starman', 
#      '--listen', ":$PORT", '--workers', $WORKERS, '--access-log', $ACCESS_LOG, 
#      'bin/web_app.pl')
    
#     or die "Could not run program /opt/local/bin/starman: $!";


# The following subroutine will kill the three separate services started by this script.

sub kill_dataservice {

    my ($signame) = @_;
    
    my $pid = `cat dataservice.pid`;
    chomp $pid;
    
    print STDERR "Shutting down on receipt of signal $signame...\n";
    
    print STDERR "Killing process $pid\n";
    kill('TERM', $pid) if $pid;
    
    exit;
}








