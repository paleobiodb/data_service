#
# Dancer.pm
# 
# This module provides a subclass of Web::DataService that works with Dancer.
# 
# This is the first such module to be written; others will eventually be
# developed for Mojolicious, etc.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


use strict;

require 5.012;

package Web::DataService::Dancer;

use parent qw(Web::DataService);

use Carp qw( carp croak );
use Scalar::Util qw( reftype blessed weaken );
use Try::Tiny;

#use Dancer qw( :syntax );
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;



# This class inherits most of its methods from Web::DataService.  The
# Dancer-specific methods are defined below:
# 
# ===================================================================


# initialize ( )
# 
# This method is called automatically by Web::DataService::new.

sub initialize {
    
    my ($self) = @_;
    
    $self->{template_suffix} //= ".tt";
}


# get_config ( )
# 
# Return the a hashref containing the configuration directives for this data
# service.  This can be called either as a class method or as an instance
# method.

sub get_config {
    
    my ($self) = @_;
    
    my $config = Dancer::config();
    return $config;
}


# get_dbh ( )
# 
# Return a database handle, assuming that the proper configuration directives
# were included to enable a connection to be made.

sub get_dbh {
    
    my $dbh = database();
    return $dbh;
}


# get_request_url ( )
# 
# Return the full URL that generated the current request

sub get_request_url {
    
    my $request_url = Dancer::request->uri();
    return $request_url;
}



# get_base_url ( )
# 
# Return the base URL for the current request.

sub get_base_url {

    return Dancer::request->uri_base(),
}


# get_params ( )
# 
# Return the parameters for the current request.


sub get_params {

    my ($self) = @_;
    
    return Dancer::params;
}


# set_access_control ( outer, arg )
# 
# Set the CORS access control header according to the argument.  The argument
# $outer is ignored for the Dancer version of Web::DataService.

sub set_access_control {

    my ($self, $outer, $arg) = @_;
    
    if ( $arg eq '*' )
    {
	Dancer::header "Access-Control-Allow-Origin" => "*";
    }
}


# set_content_type ( outer, content_type )
# 
# Set the response content type.  The argument $outer is ignored for the
# Dancer version of Web::DataService.

sub set_content_type {
    
    my ($self, $outer, $type) = @_;
    
    Dancer::content_type($type);
}


# template_exists ( template_path )
# 
# Return true if the specified template exists, false otherwise.  Throw an
# exception if the file exists but is not readable.

sub template_exists {
    
    my ($self, $template_path) = @_;
    
    return unless defined $template_path && $template_path ne '';
    
    my $template_file = Dancer::config->{views} . "/$template_path";
    
    return 1 if -r $template_file;
    croak "cannot read template '$template_file': $!" if -e $template_file;
    return;
}


# render_template ( outer, template_path, layout_path, variables )
# 
# Render the specified template, using the specified layout and variables.
# The argument $outer is ignored for the Dancer version of Web::DataService.

sub render_template {
    
    my ($self, $outer, $template_path, $layout_path, $vars) = @_;
    
    Dancer::set layout => $layout_path;
    
    return Dancer::template( $template_path, $vars );
}


	# # We start by determining the filename for the documentation template.  If
	# # the filename was not explicitly specified, try the path with '_doc.tt'
	# # appended.  If that does not exist, try appending '/index.tt'.
	
	# my $viewdir = Dancer::config->{views};
	# my $doc_file = $path_attrs->{doc_file};
	
	# unless ( $doc_file )
	# {
	#     if ( -e "$viewdir/doc/${path}_doc.tt" )
	#     {
	# 	$doc_file = "${path}_doc.tt";
	#     }
	    
	#     elsif ( -e "$viewdir/doc/${path}/index.tt" )
	#     {
	# 	$doc_file = "${path}/index.tt";
	#     }
	# }
	
	# unless ( $doc_file && -r "$viewdir/doc/$doc_file" )
	# {
	#     $doc_file = $path_attrs->{doc_error_file} || $self->{doc_error_file} || 'doc_error.tt';
	# }
	
	# # Now select the appropriate layout and execute the template.
	
	
	
	
# send_file ( outer, filename )
# 
# Send the specified file.

sub send_file {
    
    my ($self, $outer, $filename) = @_;
    
    return Dancer::send_file($filename);
}
