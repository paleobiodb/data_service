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
use Web::DataService;

# If we were called from the command line with 'GET' or 'SHOW' as the first
# argument, then assume that we have been called for debugging purposes.

if ( defined $ARGV[0] and ( lc $ARGV[0] eq 'get' or lc $ARGV[0] eq 'show' ) )
{
    set apphandler => 'Debug';
    set logger => 'console';
    set show_errors => 0;
    
    Web::DataService->set_mode('debug', 'one_request');
}

#use PB0::Main;
use PB1::Main;
#use PB2::Main;

use PBMain;

dance;
