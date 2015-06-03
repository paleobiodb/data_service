# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

use Dancer qw(:syntax);


# A single route is all we need in order to handle all requests.

any qr{.*} => sub {
    
    if ( exists params->{noheader} )
    {
	params->{header} = "no";
    }
    elsif ( exists params->{textresult} )
    {
	params->{save} = "no";
    }
    
    return Web::DataService->handle_request(request);
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
			collapse_tree => 1,
			doc_template => 'base.tt',
			title => 'Documentation' });
    
    $dsb->define_node({ path => 'css',
			file_dir => 'css' });
    
    $dsb->define_node({ path => 'images',
			file_dir => 'images' });
}

