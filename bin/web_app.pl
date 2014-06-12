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

use Template;
use Try::Tiny;
use Scalar::Util qw(blessed);

use Web::DataService::Dancer;

require Data_1_0::Main;
require Data_1_1::Main;
require Data_1_2::Main;


# We begin by instantiating a data service object, and then specify the
# subservices (i.e. data service versions) that we will be providing.  The
# subservices themselves are defined in the following files:
# 
#     Data_1_1/Main.pm
#     Data_1_2/Main.pm
#     etc.

my $ds_root = Web::DataService::Dancer->new(
    { name => 'data',
      title => 'PaleobioDB Data',
      path_prefix => 'data',
      doc_dir => 'doc/root' });

Data_1_0::setup($ds_root);
Data_1_1::setup($ds_root);
Data_1_2::setup($ds_root);


# If we were called from the command line with 'GET' or 'SHOW' as the first
# argument, then assume that we have been called for debugging purposes.

if ( defined $ARGV[0] and ( lc $ARGV[0] eq 'get' or lc $ARGV[0] eq 'show' ) )
{
    set apphandler => 'Debug';
    set logger => 'console';
    set show_errors => 0;
    
    $ds_root->{DEBUG} = 1;
    $ds_root->{ONE_REQUEST} = 1;
}


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


# Any URL ending in 'index.html', 'index.pod', '_doc.html', '_doc.pod' is interpreted as
# a request for documentation.

get qr{
	^ / ?				# ignore initial '/'
        ( (?> (?: [^/]+ / )* ) )	# capture the path 
	( index | [^/]+ _doc )		# followed by either 'index' or '*_doc'
	[.]				# followed by a .
	( html | pod ) $		# and ending with either 'html' or 'pod'
  }xs => 

sub {
	
    my ($path, $last, $suffix) = splat;
    
    $DB::single = 1;
    
    # If the last component ends in _doc, append its initial string to the path.
    
    $path //= '';
    $path .= $1 if $last =~ qr{ (.*) _doc $ }xs;
    
    # Generate a request object.
    
    my $request = $ds_root->new_request_old(undef, $path, $suffix);
    
    # Return the documentation corresponding to this path if any is available,
    # otherwise indicate a bad request.
    
    if ( $request->can_document )
    {
	return $request->document;
    }
    
    else
    {
	$request->error_result("", "html", "404 The resource you requested was not found.");
    }
};


# Any other URL that ends in a filetype suffix (including .html or .pod) is interpreted
# as a request to execute an operation, with the suffix specifying the result format.

get qr{ 
	^ / ?				# ignore initial '/'
        ( (?> (?: [^/]+ / )* ) [^/.]+ )	# capture the path, including the last component
	[.] ( [^/.]+ ) $		# capture the suffix
  }xs =>

sub {
    
    my ($path, $suffix) = splat;
    
    $DB::single = 1;
    
    # If the path ends in a number, replace it by 'single' and add the parameter
    # as 'id'.
    
    if ( $path =~ qr{ (\d+) $ }xs )
    {
	params->{id} = $1;
	$path =~ s{\d+$}{single};
    }
    
    # Generate a new request object, which also involves figuring out which
    # subservice (if any) should be handling this request.
    
    my $request = $ds_root->new_request_old(undef, $path, $suffix);
    
    # Execute the path if allowed, otherwise indicate a bad request.
    
    if ( $request->can_execute )
    {
	return $request->execute;
    }
    
    else
    {
	$request->error_result("", "html", "404 The resource you requested was not found.");
    }
};


# Any URL which does not end in a filetype suffix is interpreted as a request for
# documentation.

get qr{ 
	^ / ?				    # ignore initial '/'
        ( (?> (?: [^/]+ / )* [^/.]* ) )	$   # capture the path with optional last component
  }xs =>

sub {
    
    my ($path) = splat;
    
    $DB::single = 1;
    
    # Generate a new request object, which also involves figuring out which
    # subservice (if any) should be handling this request.
    
    my $request = $ds_root->new_request_old(undef, $path, 'html');
    
    if ( $request->can_document )
    {
	return $request->document;
    }
    
    else
    {
	$request->error_result("", "html", "404 The resource you requested was not found.");
    }
};


# Any other URL is an error.

get qr{(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    
    my ($ds, $path_remainder) = $ds_root->select_service($path);
    $ds->error_result("", "html", "404 The resource you requested was not found.");
};


dance;


