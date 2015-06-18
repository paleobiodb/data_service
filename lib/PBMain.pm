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
    
    if ( request->path =~ qr{^([\S]+)/([\d]+)[.](\w+)$}xs )
    {
	my $newpath = "$1/single.$3";
	my $id = $2;
	
	params->{id} = $id;
	forward($newpath);
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



