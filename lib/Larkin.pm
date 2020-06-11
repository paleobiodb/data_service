#
# Web::DataService::Larkin
# 
# This module is responsible for putting data responses into the legacy format returned by the
# old Larkin data service that was written in Javascript. The output of this format is JSON, but
# without the header material included by the regular JSON output module. Instead, the data
# records are simply wrapped into an array and returned without any other header or footer.
# 
# Author: Michael McClennen

use strict;

package Larkin;

use Web::DataService::Plugin::JSON qw(json_clean);
use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);

use parent 'Exporter';


# emit_header ( request, field_list )
# 
# Return the initial text of a Larkin JSON result.

sub emit_header {

    my ($class, $request) = @_;
    
    return $request->is_single_result ? "" : "[\n";
}


# emit_separator ( )
# 
# Return the record separator string.  This will be output between each
# record, but not before the first one.

sub emit_separator {
    
    return ",\n";
}


# emit_empty ( )
# 
# Return the string (if any) to output in lieu of an empty result set.

sub emit_empty {
    
    my ($class, $request) = @_;
    
    return '';
}


# emit_footer ( )
# 
# Return the final text for a JSON result.

sub emit_footer {
    
    my ($class, $request) = @_;
    
    return $request->is_single_result ? "\n" : "\n]\n";
}


# emit_error ( code, errors, warnings )
# 
# Return the formatted output for an error message body in JSON.

sub emit_error {
    
    my ($class, $code, $errors, $warnings, $cautions) = @_;

    my $message;
    my $wmessage;
    
    if ( ref $errors eq 'ARRAY' )
    {
	$message = join '; ', @$errors;
    }
    
    else
    {
	$message = $errors;
    }

    if ( defined $warnings && ref $warnings eq 'ARRAY' )
    {
	$wmessage = join '; ', @$warnings;
    }

    elsif ( defined $warnings )
    {
	$wmessage = $warnings;
    }

    my $output = '{"error": {"v":1, "license": "CC-BY 4.0", "message": ';
    $output .= json_clean($message);

    if ( $wmessage )
    {
	$output .= ', "warning": ';
	$output .= json_clean($wmessage);
    }
    
    $output .= ', "about": ""}}' . "\n";
    
    return $output;
}


# emit_record ( request, record, field_list )
# 
# Return the formatted output for a single record in JSON according to the
# specified field list.

sub emit_record {
    
    my ($class, $request, $record, $field_list) = @_;
    
    return Web::DataService::Plugin::JSON->emit_object($request, $record, $field_list);
}


1;
