# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

use Dancer qw(:syntax);
use PBLogger;
use TableDefs qw(init_table_names enable_test_mode disable_test_mode is_test_mode);
use ResourceDefs;

my $logger = PBLogger->new;


# We need a couple of routes to handle interaction with the server when it is in test mode.

get '/:prefix/testmode/:tablename/:op' => sub {

    my $tablename = params->{tablename};
    my $operation = params->{op};
    my $ds = Web::DataService->select(request);
    my $result;
    
    unless ( $ds )
    {
	pass;
    }
    
    unless ( is_test_mode )
    {
	die "500 Server is not in test mode\n";
    }
    
    if ( $operation eq 'enable' )
    {
	($result) = enable_test_mode($tablename, $ds);
	
	if ( $result && $result eq '1' )
	{
	    return "$tablename enabled";
	}

	else
	{
	    die "400 Unknown table group '$tablename'";
	}
    }
    
    elsif ( $operation eq 'disable' )
    {
	($result) = disable_test_mode($tablename, $ds);
	
	# if ( $tablename =~ /eduresources/i )
	# {
	#     ($result) = ResourceDefs->disable_test_mode($tablename, $ds);
	# }

	# else
	# {
	#     ($result) = select_test_tables($tablename, 0, $ds);
	# }
	
	if ( $result && $result eq '2' )
	{
	    return "$tablename disabled";
	}
	
	else
	{
	    die "400 Unknown table group '$tablename'";
	}
    }
    
    else
    {
	die "400 Operation $operation $tablename failed\n";
    }
};
    
get '/:prefix/startsession/:id' => sub {

    my $id = params->{id};
    
    unless ( Web::DataService->select(request) )
    {
	pass;
    }
    
    unless ( $TableDefs::TEST_MODE )
    {
	die "500 Server is not in test mode\n";
    }
    
    if ( $id eq 'none' )
    {
	cookie session_id => '';
	return "Session cleared.";
    }
    
    elsif ( $id =~ /^[\w-]+$/ )
    {
	cookie session_id => $id;
	return "Session set to '$id'";
    }
    
    else
    {
	die "400 Bad session id value '$id'\n";
    }
};


# Otherwise, a single route is all we need in order to handle all requests.

any qr{.*} => sub {
    
    my $r = request;
    
    # If we have successfully created a logger object, pass this request to it before we do
    # anything else. This will make sure that we have a record of it even in case this process
    # hangs while responding. But suppress this if we are running in test mode.
    
    $logger->log_request($r) if $logger && !$PBData::TEST_MODE;
    
    # Handle some special parameters.
    
    if ( exists params->{noheader} )
    {
	params->{header} = "no";
    }
    
    if ( exists params->{textresult} )
    {
	params->{save} = "no";
    }
    
    # A parameter named _ sometimes shows up. It appears to be added by certain javascript
    # libraries when they send AJAX requests. We need to delete this so that it doesn't mess up
    # the parameter validation.
    
    delete params->{_};
    
    # If the path ends in a string of digits with a format suffix, we treat this as if it were a
    # request for the object whose identifier corresponds to the digit string. To do this, we
    # rewrite the request as if it had been .../single.<format>?id=<digits>
    
    if ( $r->path =~ qr{^([\S]+)/([\d]+)[.](\w+)$}xs )
    {
	my $newpath = "$1/single.$3";
	my $id = $2;
	
	params->{id} = $id;
	forward($newpath);
    }
    
    # Now pass the request off to Web::DataService to handle according to the configuration
    # it has been given.
    
    return Web::DataService->handle_request($r);
};


# If an error occurs, we want to generate a Web::DataService response rather
# than the default Dancer response.  In order for this to happen, we need the
# following two hooks:

hook on_handler_exception => sub {
    
    var(error => $_[0]);
};

hook after_error_render => sub {
    
    Web::DataService->error_result(var('error'), var('wds_request'));
};


# Define a base page that will respond to the route '/data/' with a
# documentation page describing the various data service versions.

package PBBase;

unless ( Dancer::config->{no_old_versions} )
{
    my ($dsb) = Web::DataService->new(
	{ name => 'base',
	  title => 'PBDB Data Service',
	  version => '',
	  features => 'standard',
	  special_params => 'standard',
	  path_prefix => 'data/',
	  doc_template_dir => 'doc/1.0' });

    $dsb->define_node({ path => '/', 
			public_access => 1,
			doc_template => 'base.tt',
			title => 'Documentation' });
    
    $dsb->define_node({ path => 'css',
			file_dir => 'css' });
    
    $dsb->define_node({ path => 'images',
			file_dir => 'images' });
}

