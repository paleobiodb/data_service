#
# Web::DataService::Plugin::HTML
# 
# This module is associated with the 'html' format. Its methods are not actually
# called, since the modules that generate HTML output use the 'main_data' key
# for that output which results in it being written to the client stream directly.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Plugin::HTML;



# emit_header ( request, field_list )
# 
# Generate any initial text that is necessary for a text format response.  This
# will be formatted according to the format suffix specified in the request
# (comma-separated or tab-separated).

sub emit_header {

    my ($class, $request, $field_list) = @_;

    return '';
}


# emit_empty ( )
# 
# Return the string (if any) to output in lieu of an empty result set.

sub emit_empty {
    
    my ($class, $request) = @_;

    return '';
}


# emit_footer ( request )
# 
# None of the formats handled by this module involve any text after the last record
# is output, so we just return the empty string.

sub emit_footer {

    return '';
}


# emit_record (request, record, field_list)
# 
# Return a text line expressing a single record, according to the format
# specified in the request (comma-separated or tab-separated) and the
# given list of output field specifications.

sub emit_record {

    my ($class, $request, $record, $field_list) = @_;

    return '';
}

1;
