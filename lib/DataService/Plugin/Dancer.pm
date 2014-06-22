#
# Web::DataService::Plugin::Dancer.pm
# 
# This plugin provides Web::DataService with the ability to use Dancer.pm as
# its "foundation framework".  Web::DataService uses the foundation framework
# to parse HTTP requests, to marshall and send back HTTP responses, and to
# provide configuration information for the data service.
# 
# Other plugins will eventually be developed to fill this same role using
# Mojolicious and other frameworks.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


use strict;

package Web::DataService::Plugin::Dancer;

use Carp qw( carp croak );



# The following methods are called with the parameters specified: $ds is a
# reference to a data service instance, $request is a reference to the request
# instance defined by Web::DataService.  If a request object was defined by
# the foundation framework, it will be available as $request->{outer}.  The
# data service instance is also available as $request->{ds}.

# ============================================================================

# get_config ( ds, name, param )
# 
# This method returns configuration information from the application
# configuration file used by the foundation framework.  If $param is given,
# then return the value of that configuration parameter (if any).  This value
# is looked up first under the configuration group $name (if given), and if not
# found is then looked up directly.
# 
# If $param is not given, then return the configuration group $name if that
# was given, or else a hash of the entire set of configuration parameters.

sub get_config {
    
    my ($class, $ds, $name, $param) = @_;
    
    my $config = Dancer::config;
    
    if ( defined $param )
    {
	return $config->{$name}{$param} if defined $name;
	return $config->{$param};
    }
    
    elsif ( defined $name )
    {
	return $config->{$name};
    }
    
    else
    {
	return $config;
    }
}


# get_connection ( request )
# 
# This method returns a database connection.  If you wish to use it, make sure
# that you "use Dancer::Plugin::Database" in your main program.

sub get_connection {
    
    return Dancer::Plugin::Database::database();
}


# get_request_url ( request )
# 
# Return the full URL that generated the current request

sub get_request_url {
    
    return Dancer::request->uri;
}


# get_base_url ( request )
# 
# Return the base URL for the data service.

sub get_base_url {
    
    my ($class, $request) = @_;
    
    return Dancer::request->uri_base . $request->{ds}->get_base_path;
}


# get_params ( request )
# 
# Return the parameters for the current request.

sub get_params {

    return Dancer::params;
}


# set_cors_header ( request, arg )
# 
# Set the CORS access control header according to the argument.

sub set_cors_header {

    my ($plugin, $request, $arg) = @_;
    
    if ( defined $arg && $arg eq '*' )
    {
	Dancer::header "Access-Control-Allow-Origin" => "*";
    }
}


# set_content_type ( request, type )
# 
# Set the response content type.

sub set_content_type {
    
    my ($plugin, $request, $type) = @_;
    
    Dancer::content_type($type);
}


# set_header ( request, header, value )
# 
# Set an arbitrary header in the response.

sub set_header {
    
    my ($plugin, $request, $header, $value) = @_;
    
    Dancer::response->header($header => $value);    
}


# set_status ( ds, outer, status )
# 
# Set the response status code.

sub set_status {
    
    my ($class, $request, $code) = @_;
    
    Dancer::status($code);
}

	
# send_file ( request, filename )
# 
# Send as the response the contents of the specified file.  For Dancer, the path
# is always evaluated relative to the 'public' directory.

sub send_file {
    
    my ($class, $request, $filename) = @_;
    
    return Dancer::send_file($filename);
}


1;
