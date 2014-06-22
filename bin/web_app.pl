#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::Streamdata;

use Web::DataService;

require Data_1_0::Main;
require Data_1_1::Main;
require Data_1_2::Main;


# If we were called from the command line with 'GET' or 'SHOW' as the first
# argument, then assume that we have been called for debugging purposes.

if ( defined $ARGV[0] and ( lc $ARGV[0] eq 'get' or lc $ARGV[0] eq 'show' ) )
{
    set apphandler => 'Debug';
    set logger => 'console';
    set show_errors => 0;
    
    Web::DataService->set_mode('debug', 'one_request');
}


# We begin by instantiating a data service object, and then specify the
# subservices (i.e. data service versions) that we will be providing.  The
# subservices themselves are defined in the following files:
# 
#     Data_1_1/Main.pm
#     Data_1_2/Main.pm
#     etc.

my $ds_root = Web::DataService->new(
    { name => 'data',
      title => 'PaleobioDB Data',
      path_prefix => 'data',
      doc_templates => 'doc/root' });

Data_1_0::setup($ds_root);
Data_1_1::setup($ds_root);
Data_1_2::setup($ds_root);


# Then, we set things up so that a request on any path starting with "/data/" will
# return a documentation page listing the available versions.

$ds_root->define_path({ path => '/', 
			public_access => 1,
			collapse_path => 'version',
			doc_title => 'Documentation' });

# Any URL starting with /data/css indicates a common stylesheet, and the same for
# /data1.1/css and so on.

$ds_root->define_path({ path => 'css',
			send_files => 1,
			file_dir => 'css' });


# Next we configure a set of Dancer routes to recognize URL paths.  These should be
# rolled into DataService.pm at some point, but I do not want to do that until I get a
# better idea of how flexible they will need to be.  For now, people should be able to
# customize them.

# ============

get qr{ ^ (.*) $ }xs => sub {
    
    my ($path) = splat;
    
    #$DB::single = 1;
    
    my $request = $ds_root->new_request(undef, $path);
    return $request->execute;
};

dance;


