#
# DataService::Output
# 
# 
# 
# Author: Michael McClennen

use strict;

package DataService;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);

use Dancer::Plugin::StreamData;


# define_output_section ( package, name, specification... )
# 
# Define an output section with the specified name for the specified class,
# using the given specification records.

sub define_output_section {
    
    my $self = shift;
    my $class = shift;
    my $name = shift;
    
    # Check to make sure that we were given a valid name.
    
    if ( ref $name )
    {
	croak "the first argument to 'define_output' must be an output section name";
    }
    
    elsif ( $name !~ /^\w+$/ )
    {
	croak "invalid output section name '$name'";
    }
    
    # Initialize the output list for the specified class and section name.
    
    $self->{output_list}{$class}{$name} = [];
    
    # Then process the records one by one.  Make sure to throw an error if we
    # find a record whose type is ambiguous or that is otherwise invalid.  Each
    # record gets put in a list that is stored under the section name.
    
    my ($last_node);
    
    foreach my $item (@_)
    {
	# A scalar is interpreted as a documentation string.
	
	unless ( ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	    next;
	}
	
	# Any item that is not a hashref is an error.
	
	unless ( ref $item eq 'HASH' )
	{
	    croak "the arguments to 'output_section' must be hashrefs or scalars";
	}
	
	# Check the output record to make sure it was specified correctly.
	
	my ($type) = $self->check_output_record($item);
	
	# If the type is 'field', then any subsequent documentation strings
	# will be added to that record.
	
	$last_node = $item if $type eq 'field';
	
	# Add the record to the output list.
	
	push @{$self->{output_list}{$class}{$name}}, $item;
    }
}


our %OUTPUT_KEY = ('output' => 2, 'set' => 2, 'select' => 2, 'filter' => 2,
		   split => 1, add => 1, dedup => 1, use_each => 1, use_main => 1, tables => 1, 
		   if_section => 1, not_section => 1, if_format => 1, not_format => 1,
		   name => 1, value => 1, list => 1, subsection => 1);

our %FIELD_KEY = (always => 1, value => 1, dedup => 1, use_each => 1, use_main => 1, subsection => 1,
		  list => 1);

sub check_output_record {
    
    my ($self, $record) = @_;
    
    my $type;
    
    foreach my $k (keys %$record)
    {
	croak "unrecognized key '$k' in define_output record"
	    unless exists $OUTPUT_KEY{$k};
	
	if ( $OUTPUT_KEY{$k} == 2 )
	{
	    croak "you cannot have both keys '$type' and '$k' in a define_output record"
		if $type;
	    
	    $type = $k;
	}
    }
    
    croak "each record passed to define_output must include one key from the following list: 'output', 'set', 'select', 'filter'"
	unless $type;
    
    return $type;
}


# get_output_list ( section )
# 
# Return the output list for the specified section.

sub get_output_list {

    my ($self, $class, $section) = @_;
    
    die "get_output_list: no output section was specified" unless $section;
    die "get_output_list: invalid class '$class'" unless $class->isa('Web::DataService::Request');
    
    # If this class or any of its superclasses has defined this section,
    # then we use that definition.  Otherwise, put out a warning and go on
    # to the next section.
    
    my $loop_bound = 0;
    
    until ( ref $self->{output_list}{$class}{$section} )
    {
	$class = $self->{super_class}{$class} or return;
	die "problem with class configuration" if ++$loop_bound > 10;
    }
    
    return $self->{output_list}{$class}{$section};
}


# configure_query_output ( request )
# 
# Determine the list of selection, processing and output rules for the
# specified query, based on the query's attributes.  These attributes include: 
# 
# - the class
# - the operation
# - the output format
# - the output vocabulary
# - the selected output sections
# 
# Depending upon the attributes of the various output records, all, some or
# none of them may be relevant to a particular query.

sub configure_output {

    my ($self, $request) = @_;
    
    # Extract the relevant attributes of the request
    
    my $class = ref $request;
    my $format = $request->{format};
    my $vocab = $request->{vocab};
    my $require_vocab = 1 if $vocab and not $self->{vocab}{$vocab}{use_field_names};
    
    my @sections = @{$request->{base_output}} if ref $request->{base_output} eq 'ARRAY';
    push @sections, @{$request->{extra_output}} if ref $request->{extra_output} eq 'ARRAY';
    
    my %section = map { $_ => 1 } @sections;
    
    my %uniq_section;
    
    $request->{select_list} = [];
    $request->{select_hash} = {};
    $request->{tables_hash} = {};
    $request->{filter_hash} = {};
    $request->{proc_list} = [];
    $request->{field_list} = [];
    $request->{section_proc} = {};
    $request->{section_output} = {};
    
    # Then go through the list of output sections to be used for this query.
    
 SECTION:
    foreach my $section (@sections)
    {
	# Make sure that each section is only processed once, even if it is
	# listed more than once.
	
	next if $request->{section_set}{$section};
	$request->{section_set}{$section} = 1;
	
	# Generate a warning if the specified section does not exist, but do
	# not abort the request.
	
	my $output_list = $self->get_output_list($class, $section);
	
	unless ( $output_list )
	{
	    warn "undefined output section '$section' for path '$request->{path}'\n";
	    $request->add_warning("undefined output section '$section'");
	    next SECTION;
	}
	
	# Now go through the output list for this section and collect up
	# all records that are selected for this query.
	
    RECORD:
	foreach my $r ( @$output_list )
	{
	    # Evaluate dependency on the output section list
	    
	    next RECORD if $r->{if_section} 
		and not check_set($r->{if_section}, \%section);
	    
	    next RECORD if $r->{not_section}
		and check_set($r->{not_section}, \%section);
	    
	    # Evaluate dependency on the output format
	    
	    next RECORD if $r->{if_format}
		and not check_value($r->{if_format}, $format);
	    
	    next RECORD if $r->{not_format}
		and check_value($r->{not_format}, $format);
	    
	    # Evaluate dependency on the vocabulary
	    
	    next RECORD if $r->{if_vocab}
		and not check_value($r->{if_vocab}, $vocab);
	    
	    next RECORD if $r->{not_vocab}
		and check_value($r->{not_vocab}, $vocab);
	    
	    next RECORD if $require_vocab and not exists $r->{"${vocab}_name"};
	    
	    # If the record type is 'select', add to the selection list, the
	    # selection hash, and the tables hash.
	    
	    if ( defined $r->{select} )
	    {
		if ( ref $r->{select} )
		{
		    foreach my $s ( @{$r->{select}} )
		    {
			next if exists $request->{select_hash}{$s};
			$request->{select_hash}{$s} = 1;
			push @{$request->{select_list}}, $s;
		    }
		}
		
		elsif ( ! exists $request->{select_hash}{$r->{select}} )
		{
		    $request->{select_hash}{$r->{select}} = 1;
		    push @{$request->{select_list}}, $r->{select};
		}
		
		if ( ref $r->{tables} )
		{
		    foreach my $t ( @{$r->{tables}} )
		    {
			$request->{tables_hash}{$t} = 1;
		    }
		}
		
		elsif ( defined $r->{tables} )
		{
		    $request->{tables_hash}{$r->{tables}} = 1;
		}
	    }
	    
	    # If the record type is 'filter', add to the filter hash.
	    
	    elsif ( defined $r->{filter} )
	    {
		$request->{filter_hash}{$r->{filter}} = $r->{value};
	    }
	    
	    # If the record type is 'set', add a record to the process list.
	    
	    elsif ( defined $r->{set} )
	    {
		my $proc = { set => $r->{set} };
		
		foreach my $key ( qw(code set add split subfield) )
		{
		    $proc->{$key} = $r->{$key} if exists $r->{$key};
		}
		
		push @{$request->{proc_list}}, $proc;
	    }
	    
	    # If the record type is 'output', add a record to the field list.
	    # The attributes 'name' (the output name) and 'field' (the raw
	    # field name) are both set to the indicated name by default.
	    
	    elsif ( defined $r->{output} )
	    {
		my $field = { output => $r->{field}, name => $r->{field} };
		
		foreach my $key ( keys %$r )
		{
		    if ( $FIELD_KEY{$key} )
		    {
			$field->{$key} = $r->{$key};
		    }
		    
		    elsif ( $key =~ qr{ ^ (\w+) _ (name|value) $ }x )
		    {
			$field->{$2} = $r->{$key} if $1 eq $vocab || $1 eq $format;
		    }
		    
		    else
		    {
			warn "Warning: unknown key '$key' in output record\n";
		    }
		}
		
		push @{$request->{field_list}}, $field;
	    }
	}
    }
}    


# configure_section ( request, section_name )
# 
# Given a section name, determine the list of output fields and proc fields
# (if any) that are defined for it.  This is used primarily to configure
# sections referred to via 'rule' attributes.
# 
# These lists are stored under the keys 'section_proc' and 'section_output' in
# the request record.  If the appropriate keys are already present, do
# nothing.

sub configure_section {

    my ($self, $request, $section_name) = @_;
    
    # Return immediately if the relevant lists have already been computed
    # and cached (even if they are empty).
    
    return 1 if exists $request->{section_output}{$section_name};
    
    # Otherwise, we need to compute them.  Start by determining the relevant
    # attributes of the request and looking up the master output list for this
    # section.
    
    my $class = ref $request;
    my $vocab = $request->{vocab};
    my $require_vocab = 1 if $vocab and not $self->{vocab}{$vocab}{use_field_names};
    
    my $output_list = $self->get_output_list($class, $section_name);
    
    # If no list is available, indicate this in the routine record and return
    # false.  Whichever routine called us will be responsible for generating an
    # error or warning if appropriate.
    
    unless ( ref $output_list eq 'ARRAY' )
    {
	$request->{section_output} = undef;
	$request->{section_proc} = undef;
	return;
    }
    
    # Go through each record in the list, throwing out the ones that don't
    # apply and assorting the ones that do.
    
    my (@output_list, @proc_list);
    
 RECORD:
    foreach my $r ( @$output_list )
    {
	# Evaluate dependency on the output section list
	
	next RECORD if $r->{if_section} 
	    and not check_set($r->{if_section}, $request->{section_set});
	
	next RECORD if $r->{not_section}
	    and check_set($r->{not_section}, $request->{section_set});
	
	# Evaluate dependency on the output format
	
	next RECORD if $r->{if_format}
	    and not check_value($r->{if_format}, $request->{format});
	
	next RECORD if $r->{not_format}
	    and check_value($r->{not_format}, $request->{format});
	
	# Evaluate dependency on the vocabulary
	
	next RECORD if $require_vocab and not exists $r->{"${vocab}_name"};
	
	# If the record type is 'output', add a record to the output list.
	# The attributes 'name' (the output name) and 'field' (the raw
	# field name) are both set to the indicated name by default.
	    
	if ( defined $r->{output} )
	{
	    my $output = { output => $r->{output}, name => $r->{output} };
	    
	    foreach my $key ( keys %$r )
	    {
		if ( $FIELD_KEY{$key} )
		{
		    $output->{$key} = $r->{$key};
		}
		
		elsif ( $key =~ qr{ ^ (\w+) _ (name|value|rule) $ }x )
		{
		    $output->{$2} = $r->{$key} if $vocab eq $1;
		}
		
		else
		{
		    warn "Warning: unknown key '$key' in output record\n";
		}
	    }
	    
	    push @output_list, $output;
	}
	
	# If the record type is 'proc', add a record to the proc list.
	
	elsif ( defined $r->{set} )
	{
	    my $proc = { set => $r->{set} };
	    
	    foreach my $key ( qw(code set add split subfield) )
	    {
		$proc->{$key} = $r->{$key} if exists $r->{$key};
	    }
	    
	    push @proc_list, $proc;
	}
	
	# All other record types are ignored.
    }
    
    # Now cache the results.
    
    $request->{section_output}{$section_name} = \@output_list;
    $request->{section_proc}{$section_name} = \@proc_list;
    
    return 1;
}


# document_response ( path, section_list )
# 
# Generate documentation in POD format describing the available output fields.
# The parameter $section_list can be either a reference to a list of section
# names, or a comma-separated list in a scalar.

sub document_response {
    
    my ($self, $path) = @_;
    
    # First determine the relevant attributes of the path to be documented.
    
    my $class = $self->{path_attrs}{$path}{class};
    my @sections = @{$self->{path_attrs}{$path}{output_doc}};
    
    my $allow_vocab = $self->{path_attrs}{$path}{allow_vocab};
    my @vocab_list = grep { $allow_vocab->{$_} } @{$self->{vocab_list}};
    
    # Now generate the documentation.
    
    my $doc_string;
    
    my $field_count = scalar(@vocab_list);
    my $field_string = join ' / ', @vocab_list;
    $field_string =~ s/rec/pbdb/;
    
    if ( $field_count > 1 )
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pp_table_header Field name*/$field_count | Section | Description\n\n";
	$doc_string .= "=item $field_string\n\n";
    }
    
    else
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pp_table_header Field name / Section / Description\n\n";
    }
    
    foreach my $section (@sections)
    {
	next unless $section;
	
	my $output_list = $self->get_output_list($class, $section);
	next unless ref $output_list eq 'ARRAY';
	
	foreach my $r (@$output_list)
	{
	    $doc_string .= $self->document_field($section, \@vocab_list, $r);
	}
    }
    
    $doc_string .= "=back\n\n";
    
    return $doc_string;
}


sub document_field {
    
    my ($self, $section_name, $vocab_list, $r) = @_;
    
    my @names = map { $r->{$_} || '' } @$vocab_list;
    my $names = join ' / ', @names;
    
    $section_name = $r->{show} if $r->{show};
    
    my $descrip = $r->{doc} || "";
    
    my $line = "\n=item $names ( $section_name )\n\n$descrip\n";
    
    return $line;
}


# process_record ( request, record, steps )
# 
# Execute any per-record processing steps that have been defined for this
# record. 

sub process_record {
    
    my ($self, $request, $record, $steps) = @_;
    
    # If there are no processing steps to do, return immediately.
    
    return unless ref $steps eq 'ARRAY' and @$steps;
    
    # Otherwise go through the steps one by one.
    
    foreach my $p ( @$steps )
    {
	# Figure out which field (if any) we are affecting.
	
	my $set_field = $p->{set};
	
	# Figure out which field (if any) we are looking at.  Skip this
	# processing step if the source field is empty, unless the attribute
	# 'always' is set.
	
	my $source_field = $p->{from} || $p->{from_each};
	
	# Skip any processing step if the record does not have a non-empty
	# value in the corresponding field (unless the 'always' attribute is
	# set).
	
	unless ( $p->{always} )
	{
	    next unless defined $record->{$source_field} && $record->{source_field} ne '';
	    next if ref $record->{$source_field} eq 'ARRAY' && ! @{$record->{$source_field}};
	}
	
	if ( $p->{if_field} )
	{
	    my $cond_field = $p->{if_field};
	    next unless defined $record->{$cond_field} && $record->{cond_field} ne '';
	    next if ref $record->{$cond_field} eq 'ARRAY' && ! @{$record->{$cond_field}};
	}
	
	# Now generate a list of result values, according to the attributes of this
	# processing step.
	
	my @result;
	
	# If we have a 'code' attribute, then call it.
	
	if ( ref $p->{code} eq 'CODE' )
	{
	    if ( $p->{from_each} )
	    {
		@result = map { $p->{code}($self, $_, $p) } @{$record->{$source_field}};
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{code}($self, $record->{$source_field}, $p);
	    }
	    
	    else
	    {
		@result = $p->{code}($self, $record, $p);
	    }
	}
	
	# If we have a 'lookup' attribute, then use it.
	
	elsif ( ref $p->{lookup} eq 'HASH' )
	{
	    if ( $p->{from_each} )
	    {
		@result = map { $p->{lookup}{$_} } @{$record->{$source_field}};
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{code}{$record->{$source_field}};
	    }
	    
	    else
	    {
		@result = $p->{code}{$record->{$set_field}};
	    }
	}
	
	# If we have a 'split' attribute, then use it.
	
	elsif ( defined $p->{split} )
	{
	    if ( $p->{from_each} && ref $record->{$source_field} eq 'ARRAY' )
	    {
		@result = map { split($p->{split}, $_) } @{$record->{$source_field}};
	    }
	    
	    elsif ( $p->{from} && ! ref $record->{$source_field} )
	    {
		@result = split $p->{split}, $record->{$source_field};
	    }
	}
	
	# If we have a 'join' attribute, then use it.
	
	elsif ( defined $p->{join} )
	{
	    if ( ref $record->{$source_field} eq 'ARRAY' )
	    {
		@result = join($p->{join}, @{$record->{$source_field}});
	    }
	}
	
	# If the value of 'set' is '*', then we're done.  This is generally
	# only used to call a procedure with side effects.
	
	next if $set_field eq '*';
	
	# Otherwise, use the value to modify the specified field of the record.
	
	# If the attribute 'append' is set, then append to the specified field.
	# Convert the value to an array if it isn't already.
	
        if ( $p->{append} )
	{
	    $record->{$set_field} = [ $record->{$set_field} ] if defined $record->{$set_field}
		and ref $record->{$set_field} ne 'ARRAY';
	    
	    push @{$record->{$set_field}}, @result;
	}
	
	else
	{
	    if ( @result == 1 )
	    {
		($record->{$set_field}) = @result;
	    }
	    
	    elsif ( @result > 1 )
	    {
		$record->{$set_field} = \@result;
	    }
	    
	    elsif ( not $p->{always} )
	    {
		delete $record->{$set_field};
	    }
	    
	    else
	    {
		$record->{$set_field} = '';
	    }
	}
    }    
}


# generate_single_result ( request )
# 
# This function is called after an operation is executed and returns a single
# record.  Return this record formatted as a single string according to the
# specified output format.

sub generate_single_result {

    my ($self, $request) = @_;
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->{format};
    my $format_class = $self->{format}{$format}{class};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $format_class;
    
    # Generate the initial part of the output, before the first record.
    
    my $output = $format_class->emit_header($request);
    
    # If there are any processing steps to do, then do them.
    
    $self->process_record($request, $self->{main_record}, $self->{proc_list});
    
    # Generate the output corresponding to our single record.
    
    $output .= $format_class->emit_record($request, $request->{main_record});
    
    # Generate the final part of the output, after the last record.
    
    $output .= $format_class->emit_footer($request);
    
    return $output;
}


# generate_compound_result ( request )
# 
# This function is called after an operation is executed and returns a
# statement handle or list of records.  Return each record in turn formatted
# according to the specified output format.  If the option "can_stream" is
# given, and if the size of the output exceeds the threshold for streaming,
# set up to stream the rest of the output.

sub generate_compound_result {

    my ($self, $request) = @_;
    
    my $stream_threshold = $self->{stream_threshold};
    
    # $$$ init output...
    
    
    # $$$ process result set?
    

    # Determine the output format and figure out which class implements it.
    
    my $format = $request->{format};
    my $format_class = $self->{format}{$format}{class};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $format_class;
    
    # Generate the initial part of the output, before the first record.
    
    my $output = $format_class->emit_header($request);
    
    # A record separator is emitted before every record but the first.
    
    my $emit_rs = 0;
    
    # Fetch and process each output record in turn.  If output streaming is
    # available and our total output size exceeds the threshold, switch over
    # to streaming.
    
    while ( my $row = $self->next_row($request) )
    {
	# If there are any processing steps to do, then process this record.
	
	$self->process_record($request, $row, $request->{proc_list});
	
	# Generate the output for this record, preceded by a record separator if
	# it is not the first record.
	
	$output .= $format_class->emit_separator($request) if $emit_rs;	$emit_rs = 1;
	
	$output .= $format_class->emit_record($request, $row);
	
	# If streaming is a possibility, check whether we have passed the
	# threshold for result size.  If so, then we need to immediately
	# stash the output generated so far and call stream_data.  That
	# will cause the current function to be aborted, followed by an
	# automatic call to &stream_result (defined below).
	
	if ( $self->{streaming_available} and not $request->{do_not_stream} and
	     length($output) > $stream_threshold )
	{
	    $request->{stashed_output} = $output;
	    Dancer::Plugin::StreamData::stream_data($request, &stream_compound_result);
	}
    }
    
    # If we get here, then we did not initiate streaming.  So add the
    # footer and return the output data.
    
    # Generate the final part of the output, after the last record.
    
    $output .= $format_class->emit_footer($request);
    
    return $output;
}

    # If the flag 'process_resultset' is set, then we need to fetch and
    # process the entire result set before generating output.  Obviously,
    # streaming is not a possibility in this case.
    
    # if ( $self->{process_resultset} )
    # {
    # 	my @rows;
	
    # 	if ( $self->{main_sth} )
    # 	{
    # 	    while ( $row = $self->{main_sth}->fetchrow_hashref )
    # 	    {
    # 		push @rows, $row;
    # 	    }
    # 	}
	
    # 	else
    # 	{
    # 	    @rows = @{$self->{main_result}}
    # 	}
	
    # 	my $newrows = $self->{process_resultset}(\@rows);
	
    # 	if ( ref $newrows eq 'ARRAY' )
    # 	{
    # 	    foreach my $row (@$newrows)
    # 	    {
    # 		$self->processRecord($row, $self->{proc_list});
    # 		my $row_output = $self->emitRecord($row, is_first => $first_row);
    # 		$output .= $row_output;
		
    # 		$first_row = 0;
    # 		$self->{row_count}++;
    # 	    }
    # 	}
    # }


# stream_compound_result ( )
# 
# Continue to generate a compound query result from where
# generate_compound_result() left off, and stream it to the client
# record-by-record.
# 
# This routine must be passed a Plack 'writer' object, to which will be
# written in turn the stashed output from generate_compound_result(), each
# subsequent record, and then the footer.  Each of these chunks of data will
# be immediately sent off to the client, instead of being marshalled together
# in memory.  This allows the server to send results up to hundreds of
# megabytes in length without bogging down.

sub stream_compound_result {
    
    my ($request, $writer) = @_;
    
    my $self = $request->{ds};
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->{format};
    my $format_class = $self->{format}{$format}{class};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $format_class;
    
    # First send out the partial output previously stashed by
    # generate_compound_result().
    
    $writer->write( encode_utf8($self->{stashed_output}) );
    
    # Then process the remaining rows.
    
    while ( my $row = $self->next_row($request) )
    {
	# If there are any processing steps to do, then process this record.
	
	$self->process_record($request, $row, $request->{proc_list});
	
	# Generate the output for this record, preceded by a record separator
	# since we are always past the first record once we have switched over
	# to streaming.
	
	my $output = $format_class->emit_separator($request);
	
	$output .= $format_class->emit_record($request, $row);
	
	$writer->write( encode_utf8($output) ) if defined $output and $output ne '';
    }
    
    # finish output...
    
    # my $final = $self->finishOutput();
    # $writer->write( encode_utf8($final) ) if defined $final and $final ne '';
    
    # Finally, send out the footer and then close the writer object.
    
    my $footer = $format_class->emit_footer($request);
    
    $writer->write( encode_utf8($footer) ) if defined $footer and $footer ne '';
    $writer->close();
}


# next_row ( request )
# 
# Return the next row to be output for the given request.

sub next_row {
    
    my ($self, $request) = @_;
    
    if ( ref $request->{main_sth} )
    {
	return $request->{main_sth}->fetchrow_hashref
    }
    
    elsif ( ref $request->{main_result} eq 'ARRAY' )
    {
	return shift @{$request->{main_result}};
    }
    
    else
    {
	return;
    }
}


# generate_empty_result ( request )
# 
# This function is called after an operation is executed and returns no results
# at all.  Return the header and footer only.

sub generate_empty_result {
    
    my ($self, $request) = @_;
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->{format};
    my $class = $self->{format}{$format}{module};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $class;
    
    # Call the appropriate methods from this class to generate the header,
    # and footer.
    
    my $output = $class->emit_header($request);
    
    $output .= emit_footer($request);
    
    return $output;
}


1;
