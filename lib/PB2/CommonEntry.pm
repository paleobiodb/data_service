# 
# CommonEntry.pm
# 
# A class that contains common routines for supporting PBDB data entry.
# 
# Author: Michael McClennen

package PB2::CommonEntry;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(croak);
use TableDefs qw(%IDP);
use ExternalIdent qw(extract_identifier generate_identifier);

use Moo::Role;



sub initialize {
    
    my ($class, $ds) = @_;
    
}


sub unpack_input_records {
    
    my ($request, $main_param_ref) = @_;
    
    # First decode the body, and extract parameters from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my ($body, $error) = $request->decode_body;
    
    if ( $error )
    {
	die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
    }
    
    # If there was no request body at all, just return the hashref of main parameters. These will
    # constitute the entire input record.
    
    elsif ( ! $body )
    {
	return $main_param_ref;
    }
    
    # Otherwise, if the request body is an object with the key 'records' and an array value,
    # then we assume that the array is a list of inptut records. If there is also a key 'all' with
    # an object value, then we assume that it gives common parameters to be applied to all records.
    
    my $records_ref;
    
    if ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
    {
	if ( ref $body->{all} eq 'HASH' )
	{
	    foreach my $k ( keys %{$body->{all}} )
	    {
		$main_params{$k} = $body->{all}{$k};
	    }
	}
	
	$records_ref = $body->{records};
    }
    
    # If we don't find a 'records' key with an array value, then assume that the body is a single
    # record.
    
    elsif ( ref $body eq 'HASH' )
    {
	$records_ref = [ $body ];
    }
    
    # If the body is in fact an array, and that array is either empty or contains at least one
    # object, then assume its elements are records.
    
    elsif ( ref $body eq 'ARRAY' && ( @$body == 0 || ref $body[0] eq 'HASH'  ) )
    {
	$records_ref = $body;
    }
    
    # Otherwise, we must return a 400 error.
    
    else
    {
	$request->add_error("E_BODY: Badly formatted request body");
	die $request->exception(400, "Invalid request");
    }
    
    # Now, return the list of records, which might be empty.
    
    return @$records_ref;
}
