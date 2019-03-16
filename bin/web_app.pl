#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use lib './lib';

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;
use Template;
use TableDefs qw(init_table_names);
use Web::DataService;

# If we were called from the command line with one or more arguments, then
# assume that we have been called for debugging purposes.  This does not count
# the standard options accepted by Dancer, such as "--confdir" and "--port",
# which are handled before we ever get to this point.

BEGIN {

    Web::DataService->VERSION(0.3);

    my $test_mode;
    
    my $test_mode;
    
    # If we were given a command-line argument, figure out what to do with it.
    
    if ( defined $ARGV[0] )
    {
	my $cmd = lc $ARGV[0];
	
	# If the first command-line argument specifies an HTTP method (i.e. 'get') then set
	# Dancer's apphandler to 'Debug'.  This will cause Dancer to process a single request
	# using the command-line arguments and then exit.
	
	# In this case, the second argument must be the route path.  The third argument if given
	# should be a query string 'param=value&param=value...'.  Any subsequent arguments should
	# be of the form 'var=value' and are used to set environment variables that would
	# otherwise be set by Plack from HTTP request headers.
	
	if ( $cmd eq 'get' || $cmd eq 'head' || $cmd eq 'put' || $cmd eq 'post' || $cmd eq 'delete' )
	{
	    set apphandler => 'Debug';
	    set logger => 'console';
	    set show_errors => 0;
	    
	    Web::DataService->set_mode('debug', 'one_request');
	    $Web::DataService::ONE_PROCESS = 1;
	}
	
	# If the command-line argument is 'diag' then set a flag to indicate that Web::DataService
	# should print out information about the configuration of this data service application
	# and then exit.  This function can be used to debug the configuration.
	
	# This option is deliberately made available only via the command-line for security
	# reasons.
	
	elsif ( $cmd eq 'diag' )
	{
	    set apphandler => 'Debug';
	    set logger => 'console';
	    set show_errors => 0;
	    set startup_info => 0;
	    
	    Web::DataService->set_mode('diagnostic');
	    
	    # We need to alter the first argument to 'get' so that the Dancer
	    # routing algorithm will recognize it.
	    
	    $ARGV[0] = 'GET';
	}

	# If the command-line argument is 'debug' then we run in the regular mode (accepting
	# requests from a network port) but put Web::DataService into debug mode.  This will cause
	# debugging output to be printed to STDERR for eqch requests.  If the additional argument
	# 'oneproc' is given, then set the 'ONE_PROCESS' flag.  This tells the data operation
	# modules that it is safe to use permanent rather than temporary tables for some
	# operations, so that we can debug what is going on.
	
	elsif ( $cmd eq 'debug' )
	{
	    Web::DataService->set_mode('debug');
	    $Web::DataService::ONE_PROCESS = 1 if defined $ARGV[1] and lc $ARGV[1] eq 'oneproc';
	}
	
	# If the command is 'test' then we run in the regular mode (accepting requests from a
	# network port) but with two differences. First, the port to be listened on is the 'test
	# port' instead of the regular one. Second, we set a variable in the TableDefs module to
	# indicate that test table names should be used instead of the real ones. This will enable
	# us to test the data entry and editing functions of this data service without affecting
	# real data.

	elsif ( $cmd eq 'test' )
	{
	    my $test_port = setting('test_port');
	    set port => $test_port if $test_port;

	    $test_mode = 1;
	    
	    # Also check for the 'debug' command following this one.

	    Web::DataService->set_mode('debug') if defined $ARGV[1] and lc $ARGV[1] eq 'debug';
	}

	# If we do not recognize the command, complain and exit.

	else
	{
	    die "Unrecognized command '$ARGV[0]'";
	}
    }
    
    init_table_names(Dancer::config, $test_mode);
}

use PB0::Main;
use PB1::Main;
use PB2::Main;

use PBMain;
    
dance;

