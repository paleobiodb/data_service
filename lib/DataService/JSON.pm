#
# Web::DataService::JSON
# 
# 
# 
# Author: Michael McClennen

use strict;

package Web::DataService::JSON;

use JSON;
use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);


# emit_header ( request )
# 
# Return the proper initial string for a JSON result.

sub emit_header {

    my ($class, $request) = @_;
    
    my $output = '{' . "\n";
    
    # Check if we have any warning messages to convey
    
    if ( my $w = $request->warnings )
    {
	my $json = JSON->new->allow_nonref;
	$output .= '"warnings":' . $json->encode($w) . ",\n";
    }
    
    # Check if we have been asked to report the result count, and if it is
    # available.
    
    if ( $request->{show_count} and defined $request->{result_count} )
    {
	my $returned = $request->{return_count};
	my $offset = $request->{result_offset};
	
	$output .= '"records_found":' . ($request->{result_count} + 0) . ",\n";
	$output .= '"records_returned":' . ($returned + 0) . ",\n";
	$output .= '"starting_index":' . ($offset + 0) . ",\n" if $offset > 0;
    }
    
    # The actual data will go into an array, in a field called "records".
    
    $output .= '"records": [';
    return $output;
}


# emit_footer ( )
# 
# Return a proper final string for a JSON result.

sub emit_footer {
    
    my ($class, $request) = @_;
    
    return qq<\n]\n}\n>;
}


# emit_separator ( )
# 
# Return the record separator string, if any.

sub emit_separator {
    
    return ",\n";
}


# emit_record ( )
# 
# Return the formatted output for a single record in JSON.

sub emit_record {
    
    my ($class, $request, $record, $record_opts) = @_;
    
    # Start the output.
    
    my $output = $record_opts->{first_row} ? "\n" : ",\n";
    
    # Write out the object data in JSON.
    
    $output .= $class->construct_object($request, $record, $request->{output_list});
    
    return $output;
}


# construct_object ( request, record, rule )
# 
# Generate a hash based on the given record and the specified rule (or rules,
# as an array ref).

sub construct_object {

    my ($class, $request, $record, $rulespec) = @_;
    
    # Start with an empty string.
    
    my $outrec = '{';
    my $sep = '';
    
    # Go through the rule list, generating the fields one by one.  $rulespec
    # may be either an array of rule records or a single one.
    
    foreach my $f (reftype $rulespec && reftype $rulespec eq 'ARRAY' ? @$rulespec : $rulespec)
    {
	# Skip any field that is empty, unless 'always' or 'value' is set.
	
	my $field = $f->{field};
	
	next unless $f->{always} or defined $f->{value} or 
	    defined $record->{$field} and $record->{$field} ne '';
	
	# Skip any field with a 'dedup' attribute if its value is the same as
	# the value of the field indicated by the attribute.
	
	next if $f->{dedup} and defined $record->{$field} and defined $record->{$f->{dedup}}
	    and $record->{$field} eq $record->{$f->{dedup}};
	
	# Start with the initial value for this field.  If it contains a
	# 'value' attribute, use that.  Otherwise, use the indicated field
	# value from the current record.  If that is not defined, use the
	# empty string.
	
	my $value = defined $f->{value}       ? $f->{value} 
	          : defined $record->{$field} ? $record->{$field}
		  :                             '';
	
	# If the field has a 'rule' attribute and the value is a hashref then
	# generate output to represent a sub-object by applying the named
	# output section to the value.  If the value is a scalar then this
	# field is silently ignored.
	
	if ( defined $f->{rule} )
	{
	    $request->configure_section($f->{rule});
	    
	    my $output_list = $request->{section_output}{$f->{rule}};
	    my $proc_list = $request->{section_proc}{$f->{rule}};
	    
	    if ( ref $value eq 'HASH' )
	    {
		$request->process_record($value, $proc_list) if $proc_list && @$proc_list;
		$value = $class->construct_object($request, $value, $output_list) if $output_list && @$output_list;
	    }
	    
	    # If instead the value is an arrayref then apply the rule to each item
	    # in the list.
	    
	    elsif ( ref $value eq 'ARRAY' )
	    {
		if ( $proc_list && @$proc_list )
		{
		    foreach my $v ( @$value )
		    {
			$request->process_record($v, $proc_list);
		    }
		}
		
		$value = $class->construct_array($request, $value, $output_list) if $output_list && @$output_list;
	    }
	}
	
	# Otherwise, if the value is an arrayref then we generate output for
	# an array.
	
	elsif ( ref $value eq 'ARRAY' )
	{
	    $value = $class->construct_array($request, $value);
	}
	
	# Otherwise just use the value.
	
	else
	{
	    $value = json_clean($value);
	}
	
	# Now, add the value to the growing output.  Add a comma before each
	# record except the first.
	
	my $outkey = $f->{name};
	
	$outrec .= qq{$sep"$outkey":$value"};
	$sep = ',';
    }
    
    # If this record has hierarchical children, process them now.  (Do we
    # still need this?)
    
    if ( exists $record->{hier_child} )
    {
	my $children = $class->construct_array($record->{hier_child}, $rulespec);
	$outrec .= ',"children":' . $children;
    }
    
    # Now finish the output string and return it.
    
    $outrec .= "}";
    
    return $outrec;
}


sub construct_array {

    my ($class, $request, $arrayref, $rulespec) = @_;
    
    my $f = $rulespec if reftype $rulespec && reftype $rulespec ne 'ARRAY';
    
    # Start with an empty string.
    
    my $outrec = '[';
    my $sep = '';
    
    # Go through the elements of the specified arrayref, applying the
    # specified rule to each one.
    
    my $value = '';
    
    foreach my $elt ( @$arrayref )
    {
	if ( reftype $elt && reftype $elt eq 'ARRAY' )
	{
	    $value = $class->construct_array($request, $elt, $rulespec);
	}
	
	elsif ( reftype $elt && reftype $elt eq 'HASH' )
	{
	    next unless $rulespec;
	    $value = $class->construct_object($request, $elt, $rulespec);
	}
	
	elsif ( ref $elt )
	{
	    next;
	}
	
	else
	{
	    $value = json_clean($elt);
	}
	
	if ( defined $value and $value ne '' )
	{
	    $outrec .= "$sep$value";
	    $sep = ',';
	}
    }
    
    $outrec .= ']';
    
    return $outrec;
}


# json_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a JSON result.

my (%ESCAPE) = ( '\\' => '\\\\', '"' => '\\"', "\t" => '\\t', "\n" => '\\n',
		 "\r" => '\\r' );	#'

sub json_clean {
    
    my ($string) = @_;
    
    # Return an empty string unless the value is defined.
    
    return '""' unless defined $string and $string ne '';
    
    # Do a quick check for numbers.  If it matches, return the value as-is.
    
    return $string if $string =~
    /^-?(?:[0-9]+|[0-9]+\.[0-9]*|[0-9]*\.[0-9]+)(?:[Ee]-?\d+)?$/;
    
    # Do another quick check for okay characters.  If there's nothing exotic,
    # just return the quoted value.
    
    return '"' . $string . '"' unless $string =~ /[^a-zA-Z0-9 _.,;:<>-]/;
    
    # Otherwise, we need to do some longer processing.
    
    # Turn any numeric character references into actual Unicode characters.
    # The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Next, escape all backslashes, double-quotes and whitespace control characters
    
    $string =~ s/(\\|\"|\n|\t|\r)/$ESCAPE{$1}/ge;
    
    # Translate ((b)) into <b> and ((i)) into <i>
    
    #$string =~ s/\(\(([bi])\)\)/<$1>/ge;
    
    # Finally, delete all other control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them).
    
    $string =~ s/[\0-\037\177]//g;
    
    return '"' . $string . '"';
}


sub json_clean_simple {
    
    my ($string) = @_;
    
    # Return an empty string unless the value is defined.
    
    return '' unless defined $string;
    
    # Otherwise, we need to do some longer processing.
    
    # Turn any numeric character references into actual Unicode characters.
    # The database does contain some of these.  Also take out all control
    # characters, which shouldn't be in there either.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
}


1;
