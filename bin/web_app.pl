#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;


use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;


set environment => 'development';
set apphandler => 'Debug';
set log => 'error';


get '/taxa/list.:ct' => sub {
    
    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = TaxonQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchMultiple() or return $query->reportError();
    
    $query->generateCompoundResult( can_stream => server_supports_streaming );
};


get '/taxa/all.:ct' => sub {

    forward '/taxa/list.' . params->{ct};
};


get '/taxa/details.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = TaxonQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchSingle(params->{id}) or return $query->reportError();
    
    return $query->generateSingleResult();
};


get '/taxa/hierarchy.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = TreeQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchMultiple() or return $query->reportError();
    
    return $query->generateCompoundResult( can_stream => server_supports_streaming );
};


get '/taxa/:id.:ct' => sub {
    
    $DB::single = 1;
    return forward '/taxa/details.' . params->{ct}, { id => params->{id} };
};


get '/collections/list.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = CollectionQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchMultiple() or return $query->reportError();
    
    return $query->generateCompoundResult( can_stream => server_supports_streaming );
};


get '/collections/details.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});

    my $query = CollectionQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchSingle(params->{id}) or return $query->reportError();
    
    return $query->generateSingleResult();
};


sub setContentType {
    
    my ($format) = @_;
    
    if ( $format eq 'xml' )
    {
	content_type 'text/xml';
    }
    
    elsif ( $format eq 'json' )
    {
	content_type 'application/json';
    }
    
    else
    {
	send_error("Unknown Media Type", 415);
    }
}


sub dataError {

    my ($options, $errMessage) = @_;
    
    if ( defined $options->{xml} )
    {
	send_error("Not Found", 404);
	return $errMessage;
    }
    
    elsif ( defined $options->{json} )
    {
	return '{"error":"' . $errMessage . '"}';
    }
}

1;


dance;
