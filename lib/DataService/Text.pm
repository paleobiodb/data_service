#
# Web::DataService::Text
# 
# This module is responsible for generating data responses in any of three
# formats: 
# 
# csv	comma-separated text
# tsv	tab-separated text
# txt	tab-separated text, to be shown directly in a browser tab
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Text;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);



# emit_header ( request, field_list )
# 
# Display any initial text that is necessary for a text format response.  This
# will be output according to the format specified in the request
# (comma-separated or tab-separated).

sub emit_header {

    my ($class, $request, $field_list) = @_;
    
    my $output = '';
    
    # If the user has directed that the header be suppressed, just return
    # the empty string.
    
    return $output unless $request->display_header;
    
    # If the user has directed that result counts are to be shown, and if any
    # are available to show, then add those at the very top.
    
    if ( $request->display_counts )
    {
	my $counts = $request->result_counts;
	
	$output .= $class->generate_line($request, "Records found", "Records returned", "Starting index");
	$output .= $class->generate_line($request, $counts->{found}, $counts->{returned}, $counts->{offset});
    }
    
    # If any warnings were generated on this request, add them in next.
    
    if ( my @msgs = $request->warnings )
    {
	$output .= $class->generate_line($request, "WARNINGS");
	$output .= $class->generate_line($request, $_) foreach @msgs;
	$output .= $class->generate_line($request, "END OF WARNINGS");
    }
    
    # Now, if any output fields were specified for this request, list them in
    # a header line.
    
    if ( ref $field_list eq 'ARRAY' )
    {
	my @fields = map { $_->{name} } @$field_list;
	
	$output .= $class->generate_line($request, @fields);
    }
    
    # Otherwise, note that no fields are available.
    
    else
    {
	$output .= $class->generate_line($request, "THIS REQUEST DID NOT GENERATE ANY OUTPUT FIELDS");
    }
    
    # Return the text that we have generated.
    
    return $output;
}


# emit_footer ( request )
# 
# None of the formats handled by this module involve any text after the last record
# is output, so we just return the empty string.

sub emit_footer {

    return '';
}


# emit_record (request, record )
# 
# Return a text line expressing a single record, according to the format
# specified in the request (comma-separated or tab-separated) and the
# given list of output field specifications.

sub emit_record {

    my ($class, $request, $record, $field_list) = @_;
    
    # If no output fields were specified, we return the empty string.
    
    return '' unless ref $field_list eq 'ARRAY';
    
    # Otherwise, generate the list of values for the current line.  For each output
    # field, we take either the explicitly specified value or the value of the
    # specified field from the record.
    
    my @values = map { $_->{value} // $record->{$_->{field}} } @$field_list;
    
    return $class->generate_line($request, @values);
}


# generate_line ( request, values... )
# 
# Generate an output line containing the given values.

sub generate_line {

    my $class = shift;
    my $request = shift;
    
    my $term = $request->linebreak_cr ? "\n" : "\r\n";
    
    if ( $request->output_format eq 'csv' )
    {
	return join(',', map { csv_clean($_) } @_) . $term;
    }
    
    else
    {
	return join("\t", map { txt_clean($_) } @_) . $term;
    }
}


my (%TXTESCAPE) = ( '"' => '""', "'" => "''", "\t" => '\t', "\n" => '\n',
		 "\r" => '\r' );	#'

# csv_clean ( string, quoted )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a csv-format result.  If 'quoted' is true, then all fields will
# be quoted.  Otherwise, only those which contain commas or quotes will be.

sub csv_clean {

    my ($string) = @_;
    
    # Return an empty string unless the value is defined.
    
    return '""' unless defined $string;
    
    # Do a quick check for okay characters.  If there's nothing exotic, just
    # return the quoted value.
    
    return '"' . $string . '"' unless $string =~ /[^a-zA-Z0-9 _.;:<>-]/;
    
    # Otherwise, we need to do some longer processing.
    
    # Turn any numeric character references into actual Unicode characters.
    # The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Next, double all quotes and textify whitespace control characters
    
    $string =~ s/("|'|\n|\r)/$TXTESCAPE{$1}/ge;
    
    # Finally, delete all other control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them).
    
    $string =~ s/[\0-\037\177]//g;
    
    return '"' . $string . '"';
}


# txt_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a csv-format result.  If 'quoted' is true, then all fields will
# be quoted.  Otherwise, only those which contain commas or quotes will be.

sub txt_clean {

    my ($string, $quoted) = @_;
    
    # Return an empty string unless the value is defined.
    
    return '' unless defined $string;
    
    # Do a quick check for okay characters.  If there's nothing exotic, just
    # return the value as-is.
    
    return $string unless $string =~ /^[a-zA-Z0-9 _.,;:<>-]/;
    
    # Otherwise, we need to do some longer processing.
    
    # Turn any numeric character references into actual Unicode characters.
    # The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Next, textify whitespace control characters
    
    $string =~ s/(\n|\t|\r)/$TXTESCAPE{$1}/ge;
    
    # Finally, delete all other control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them).
    
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
}


1;
