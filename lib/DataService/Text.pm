#
# Web::DataService::Text
# 
# This module is responsible for putting data responses into either
# tab-separated text or comma-separated text format.  It is used when
# the user selects any of the following three format strings:
# 
# csv	comma-separated text
# tsv	tab-separated text
# txt	comma-separated text, to be shown directly in a browser tab
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Text;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);



# emit_header ( request, field_list )
# 
# Generate any initial text that is necessary for a text format response.  This
# will be formatted according to the format suffix specified in the request
# (comma-separated or tab-separated).

sub emit_header {

    my ($class, $request, $field_list) = @_;
    
    my $output = '';
    
    # If the user has directed that the header be suppressed, just return
    # the empty string.
    
    return $output unless $request->display_header;
    
    # If the user has specified that the source of this data be shown, add
    # some header lines to convey this.
    
    if ( $request->display_source )
    {
	my $source = $request->get_data_source;
	my $base = $source->{base_url};
	my $url_rest = $request->get_request_url;
	
	my $data_url = $base . $url_rest;
	my $doc_url = $base . $request->get_request_path . "_doc.html";
	
	$output .= $class->emit_line($request, "Data Source:", $source->{name});
	$output .= $class->emit_line($request, "Data Source URL:", $base . '/');
	$output .= $class->emit_line($request, "Data License:", $source->{license});
	$output .= $class->emit_line($request, "Data License URL:", $source->{license_url});
	$output .= $class->emit_line($request, "Documentation URL:", $doc_url);
	$output .= $class->emit_line($request, "Data URL:", $data_url);
	$output .= $class->emit_line($request, "Access Time:", $source->{access_time});
	$output .= $class->emit_line($request, "Parameters:");
	
	my @display = $request->params_for_display;
	
	while ( @display )
	{
	    my $param = shift @display;
	    my $value = shift @display;
	    
	    next unless defined $param && $param ne '';
	    $value //= '';
	    
	    if ( ref $value eq 'ARRAY' )
	    {
		$output .= $class->emit_line($request, '', $param, @$value);
	    }
	    
	    else
	    {
		$output .= $class->emit_line($request, '', $param, $value);
	    }
	}
    }
    
    # If the user has directed that result counts are to be shown, and if any
    # are available to show, then add those at the very top.
    
    if ( $request->display_counts )
    {
	my $counts = $request->result_counts;
	
	$output .= $class->emit_line($request, "Records Found:", $counts->{found});
	$output .= $class->emit_line($request, "Records Returned:", $counts->{returned});
	$output .= $class->generate-line($request, "Starting Index:", $counts->{offset})
	    if defined $counts->{offset} && $counts->{offset} > 0;
    }
    
    # If any warnings were generated on this request, add them in next.
    
    if ( my @msgs = $request->warnings )
    {
	$output .= $class->emit_line($request, "Warning:", $_) foreach @msgs;
    }
    
    # If any header material was generated, add a line to introduce the start
    # of the actual data.
    
    if ( $output ne '' )
    {
	$output .= $class->emit_line($request, "Records:");
    }
    
    # Now, if any output fields were specified for this request, list them in
    # a header line.
    
    if ( ref $field_list eq 'ARRAY' )
    {
	my @fields = map { $_->{name} } @$field_list;
	
	$output .= $class->emit_line($request, @fields);
    }
    
    # Otherwise, note that no fields are available.
    
    else
    {
	$output .= $class->emit_line($request, "THIS REQUEST DID NOT GENERATE ANY OUTPUT RECORDS");
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


# emit_record (request, record, field_list)
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
    
    my @values;
    
    foreach my $f ( @$field_list )
    {
	my $v = '';
	
	# First figure out what each value should be
	
	if ( defined $f->{value} )
	{
	    $v = $f->{value};
	}
	
	elsif ( defined $f->{field} && defined $record->{$f->{field}} )
	{
	    $v = $record->{$f->{field}};
	}
	
	# If the value is an array, join it into a string.  If no joining
	# string was specified, use a comma.
	
	if ( ref $v eq 'ARRAY' )
	{
	    my $join = $f->{text_join} // q{, };
	    $v = join($join, @$v);
	}
	
	# Now add the value to the list.
	
	push @values, $v;
    }
    
    return $class->emit_line($request, @values);
}


# emit_line ( request, values... )
# 
# Generate an output line containing the given values.

sub emit_line {

    my $class = shift;
    my $request = shift;
    
    my $term = $request->linebreak_cr ? "\n" : "\r\n";
    
    if ( $request->output_format eq 'tsv' )
    {
	return join("\t", map { tsv_clean($_) } @_) . $term;
    }
    
    else
    {
	return join(',', map { csv_clean($_) } @_) . $term;
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


# tsv_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a tsv-format result.  If 'quoted' is true, then all fields will
# be quoted.  Otherwise, only those which contain commas or quotes will be.

sub tsv_clean {

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
