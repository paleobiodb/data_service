#
# DataQuery
# 
# A base class that implements a data service for the PaleoDB.  This can be
# subclassed to produce any necessary data service.  For examples, see
# TaxonQuery.pm and CollectionQuery.pm. 
# 
# Author: Michael McClennen

use strict;

package DataService::Base;

use JSON;
use Encode;
use Scalar::Util qw(reftype);


our (@VOCABULARIES) = ('rec', 'com', 'dwc');

# new ( dbh, attrs )
# 
# Generate a new query object, using the given database handle and any other
# attributes that are specified.

sub new {
    
    my ($class, $attrs) = @_;
    
    # Now create a query record.
    
    my $self = { };
    
    if ( ref $attrs eq 'HASH' )
    {
	foreach my $key ( %$attrs )
	{
	    $self->{$key} = $attrs->{$key};
	}
    }
    
    # Bless it into the proper class and return it.
    
    bless $self, $class;
    return $self;
}


# warn ( message )
# 
# Add a warning message to this query object, which will be returned as part
# of the output.

sub warn {

    my ($self, $message) = @_;
    
    return unless defined $message and $message ne '';
    
    $self->{warnings} = [] unless defined $self->{warnings};
    push @{$self->{warnings}}, $message;
}


# setOutputList ( )
# 
# Determine the list of selection, processing and output rules for this query,
# based on the 'show' parameter, the operation and the output format.  The
# parameter 'valid' must be an object of type HTTP::Validate::Response.

sub set_response {

    my ($self, $format, $vocab, $op, @output_args) = @_;
    
    my $valid = $self->{valid};
    
    # Set 'show' and 'show_order' based on the value of the 'show' parameter.
    # Make sure that 'show_order' contains no duplicates.

    my (@show, %show);
    
    foreach my $o ( @output_args )
    {
	next unless $o;
	my @sections;
	
	if ( ref $o eq 'ARRAY' )
	{
	    @sections = @$o;
	}
	
	elsif ( not ref $o )
	{
	    @sections = split /\s*,\s*/, $o;
	}
	
	foreach my $s (@sections)
	{
	    next if $show{$s};
	    $show{$s} = 1;
	    push @show, $s;
	}
    }
    
    $self->{show} = \%show;
    $self->{show_order} = \@show;
    $self->{output_format} = $format;
    
    # Set the vocabulary according to the 'vocab' parameter, or defaulting to
    # the best vocabulary for the content type.
    
    $vocab ||=
	
	($self->{output_format} eq 'json' ? 'com' :
	 $self->{output_format} eq 'xml' ? 'dwc' :
	 'pbdb');
    
    $self->{vocab} = $vocab;
    
    # Figure out which operation we are executing
    
    my $class = ref $self;
    
    no strict 'refs';
    
    # Quote all output fields if we are directed to.  A level of 2 means to
    # quote everything, 1 means only quote fields that contain commas or newlines
    
    if ( $self->{output_format} eq 'csv' )
    {
	$self->{quoted} = 2;
    }
    
    # Now set the actual list of output fields for the basic query operation
    # and each of the requested sections.
    
    my (@select_list, @proc_list, @output_list, %tables);   
    
    foreach my $section (@show)
    {
	next unless $section;
	
	my $select_conf = ${"${class}::SELECT"}{"${op}_${section}"} || ${"${class}::SELECT"}{$section};
	my $tables_conf = ${"${class}::TABLES"}{"${op}_${section}"} || ${"${class}::TABLES"}{$section};
	my $proc_conf = ${"${class}::PROC"}{"${op}_${section}"} || ${"${class}::PROC"}{$section};
	my $output_conf = ${"${class}::OUTPUT"}{"${op}_${section}"} || ${"${class}::OUTPUT"}{$section};
	
	push @select_list, $select_conf if defined $select_conf and not ref $select_conf;
	
	if ( ref $tables_conf eq 'ARRAY' ) {
	    map { $tables{$_} = 1 } @$tables_conf;
	} elsif ( $tables_conf ) {
	    $tables{$tables_conf} = 1;
	}
	
	foreach my $f ( ref $proc_conf eq 'ARRAY' ? @$proc_conf : () )
	{
	    die "Error: bad proc specification in '$section', must be a hash" unless reftype $f && reftype $f eq 'HASH';
	    next if $self->{output_format} eq 'json' and $f->{no_json};
	    next if $self->{output_format} eq 'xml' and $f->{no_xml};
	    next if $self->{output_format} eq 'txt' and $f->{no_txt};
	    next if $self->{output_format} eq 'csv' and $f->{no_txt};
	    
	    my $out_f = { %$f };
	    	    
	    if ( defined $out_f->{"${vocab}_code"} )
	    {
		$out_f->{code} = $out_f->{"${vocab}_code"};
		delete $out_f->{"${vocab}_code"};
	    }

	    if ( defined $out_f->{"${vocab}_rec"} )
	    {
		$out_f->{code} = $out_f->{"${vocab}_rec"};
		delete $out_f->{"${vocab}_rec"};
	    }
	    
	    push @proc_list, $out_f;	    
	}
	
	foreach my $f ( ref $output_conf eq 'ARRAY' ? @$output_conf : () )
	{
	    die "Error: bad output specification in '$section', must be a hash" unless reftype $f && reftype $f eq 'HASH';
	    next if $f->{show} and not $self->{show}{$f->{show}};
	    next if $f->{no_show} and $self->{show}{$f->{show}};
	    next if $vocab eq 'dwc' and not exists $f->{dwc};
	    next if $vocab eq 'com' and not exists $f->{com};
	    next if $self->{output_format} eq 'json' and $f->{no_json};
	    next if $self->{output_format} eq 'xml' and $f->{no_xml};
	    next if $self->{output_format} eq 'txt' and $f->{no_txt};
	    next if $self->{output_format} eq 'csv' and $f->{no_txt};
	    
	    my $out_f = { %$f };
	    
	    if ( defined $out_f->{"${vocab}_value"} )
	    {
		$out_f->{value} = $out_f->{"${vocab}_value"};
		delete $out_f->{"${vocab}_value"};
	    }
	    
	    if ( defined $out_f->{"${vocab}_code"} )
	    {
		$out_f->{code} = $out_f->{"${vocab}_code"};
		delete $out_f->{"${vocab}_code"};
	    }

	    if ( defined $out_f->{"${vocab}_rec"} )
	    {
		$out_f->{code} = $out_f->{"${vocab}_rec"};
		delete $out_f->{"${vocab}_rec"};
	    }
	    
	    push @output_list, $out_f;
	}
    }
    
    $self->{select_list} = \@select_list;
    $self->{select_tables} = \%tables;
    $self->{output_list} = \@output_list;
    $self->{proc_list} = \@proc_list if @proc_list;
}


# document_response ( section_list )
# 
# Generate documentation in POD format describing the available output
# options.  This can be returned in one of two ways: as an HTML table, or as
# native POD.

sub document_response {
    
    my ($self, $section_list) = @_;
    
    my @sections = ref $section_list eq 'ARRAY' ? @$section_list : split(/\s*,\s*/, $section_list);
    my %vocab_check = map { $_ => 1 } @VOCABULARIES;
    my %include_section;
    my $class = ref $self || $self;
    my $format = 'pod';
    
    no strict 'refs';
    
    # First run through the sections and determine which vocabularies are
    # active. 
    
 SECTION:
    foreach my $section (@sections)
    {
	next unless $section;
	
	$include_section{$section} = 1;
	
	my $output_list = ${"${class}::OUTPUT"}{$section};
	next unless ref $output_list eq 'ARRAY';
	
	foreach my $r ( @$output_list )
	{
	    foreach my $v (keys %vocab_check)
	    {
		delete $vocab_check{$v} if $r->{$v};
	    }
	    
	    last SECTION unless keys %vocab_check;
	}
    }
    
    my @vocab_list = grep { ! $vocab_check{$_} } @VOCABULARIES;
    
    # Now generate the documentation.
    
    my $doc_string;
    
    my $field_count = scalar(@vocab_list);
    my $field_string = join ' / ', @vocab_list;
    
    if ( $field_count > 1 )
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pod_extra table_header Field name*/$field_count | Section | Description\n\n";
	$doc_string .= "=item $field_string\n\n";
    }
    
    else
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pod_extra table_header Field name / Section / Description\n\n";
    }
    
    foreach my $section (@sections)
    {
	next unless $section;
	
	my $output_list = ${"${class}::OUTPUT"}{$section};
	next unless ref $output_list eq 'ARRAY';
	
	my $name = $section eq $sections[0] ? 'basic' : $section;
	
	foreach my $r (@$output_list)
	{
	    $doc_string .= $self->document_field($name, \@vocab_list, $r);
	}
    }
    
    $doc_string .= $format eq 'pod' ? "=back\n\n" : "</table>\n\n=end html\n\n";
    
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


# countRecords ( )
# 
# Return the result count of the main query, regardless of any limit on the
# number of records returned.  This routine should only be called after all of
# the results have been fetched, so that we have an accurate count.

sub countRecords {

    my ($self) = @_;
    
    # If there is a limit on the number of rows fetched, and the number of
    # records fetched is equal to that limit, then we have to execute the
    # count statement previously generated by fetchMultiple() in order to get
    # the true count.
    
    if ( defined $self->{limit_results} and 
	    $self->{row_count} == $self->{limit_results} )
    {
	my ($dbh) = $self->{dbh};
	
	my ($result_count) = $dbh->selectrow_array($self->{count_sql});
	return $result_count;
    }
    
    # Otherwise we can just report the number of records actually fetched.
    
    else
    {
	return $self->{row_count};
    }
}


# generate_query_fields ( mt )
# 
# Return a query string based on the variables computed by set_output above.
# The parameter $mt should be the name or alias of the main query table.

sub generate_query_fields {
    
    my ($self, $mt) = @_;
    
    my $fields = join(', ', grep { defined $_ and $_ ne '' } @{$self->{select_list}});
    
    $fields =~ s/\$mt/$mt/g;
    
    return $fields;
}

# generateSingleResult ( )
# 
# Return the query result formatted as a single string according to the output
# format previously set by setOutputFormat().

sub generate_single_result {

    my ($self) = @_;
    
    # If we have a result row already, use that.
    
    if ( defined $self->{main_record} )
    {
	$self->processRecord($self->{main_record}, $self->{proc_list});
	$self->{row_count} = 1;
	
	return $self->emitHeader() . $self->emitRecord($self->{main_record}, is_first => 1) .
	    $self->emitFooter();
    }
    
    # If we have a result set, fetch the first record and generate the result.
    
    elsif ( defined $self->{main_sth} )
    {
	my $row = $self->{main_sth}->fetchrow_hashref();
	
	$self->processRecord($row, $self->{proc_list});
	$self->{row_count} = 1;
	
	return $self->emitHeader() . $self->emitRecord($row, is_first => 1) .
	    $self->emitFooter();
    }
    
    # Otherwise, we have an empty result set.
    
    else
    {
	$self->{row_count} = 0;
	
	return $self->emitHeader() . $self->emitFooter();
    }
}


# generateCompoundResult ( options )
# 
# Return the query result formatted as a single string according to the output
# format previously set by setOutputFormat().  The only option available at
# this time is:
# 
#   can_stream => 1
# 
# This option can be used to inform this function that it should stream the
# result if the result is large.  This option should only be used if the
# server does in fact support streaming.  If the size of the result will
# exceed $STREAM_THRESHOLD characters, the result computed so far is stashed
# in the query object and the function returns false.  The calling application
# can then setup a call to streamResult to send the stashed data followed by
# the rest of the data as it is retrieved from the database.

sub generate_compound_result {

    my ($self, %options) = @_;
    
    my $sth = $self->{main_sth};
    my $output = '';
    my $first_row = 1;
    my $row;
    
    $self->{should_stream} ||= '';
    $self->{row_count} = 0;
    
    my $stream_threshold = $self->{service}{stream_threshold} || 104000;
    
    # The first thing to do is to check if we have a statement handle.  If
    # not, the result set is empty and we need to return the relevant header
    # and footer.
    
    unless ( ref $self->{main_sth} or ref $self->{main_result} )
    {
	return $self->emitHeader() . $self->emitFooter();
    }
    
    # Otherwise, we have some results.  We call the initOutput method first,
    # in case the query class has overridden it.  Anything it returns will get
    # prepended to the output.
    
    my $initial = $self->initOutput();
    $output .= $initial if defined $initial;
    
    # If the flag 'process_resultset' is set, then we need to fetch and
    # process the entire result set before generating output.  Obviously,
    # streaming is not a possibility in this case.
    
    if ( $self->{process_resultset} )
    {
	my @rows;
	
	if ( $self->{main_sth} )
	{
	    while ( $row = $self->{main_sth}->fetchrow_hashref )
	    {
		push @rows, $row;
	    }
	}
	
	else
	{
	    @rows = @{$self->{main_result}}
	}
	
	my $newrows = $self->{process_resultset}(\@rows);
	
	if ( ref $newrows eq 'ARRAY' )
	{
	    foreach my $row (@$newrows)
	    {
		$self->processRecord($row, $self->{proc_list});
		my $row_output = $self->emitRecord($row, is_first => $first_row);
		$output .= $row_output;
		
		$first_row = 0;
		$self->{row_count}++;
	    }
	}
    }
    
    # Otherwise, we fetch and process the rows one at a time.  If streaming is
    # a possibility, we also test whether the output size is larger than our
    # threshold for streaming.
    
    elsif ( $self->{main_sth} )
    {
	while ( $row = $self->{main_sth}->fetchrow_hashref )
	{
	    # For each row, we start by calling the processRecord method (in case
	    # the query class has overridden it) and then call generateRecord to
	    # generate the actual output.
	    
	    $self->processRecord($row, $self->{proc_list});
	    my $row_output = $self->emitRecord($row, $first_row);
	    $output .= $row_output;
	    
	    $first_row = 0;
	    $self->{row_count}++;
	    
	    # If streaming is a possibility, check whether we have passed the
	    # threshold for result size.  If so, then we need to immediately
	    # generate the header and stash it along with the output so far.  We
	    # then return false, which should lead to a subsequent call to
	    # streamResult().
	    
	    if ( defined $options{can_stream} and $self->{should_stream} ne 'no' and
		 (length($output) > $stream_threshold or $self->{should_stream} eq 'yes') )
	    {
		$self->{stashed_output} = $self->emitHeader(streamed => 1) . $output;
		return;
	    }
	}
    }
    
    # If our results were already fetched, then we just process and emit them
    # one at a time.
    
    elsif ( ref $self->{main_result} eq 'ARRAY' )
    {
	while ( $row = shift @{$self->{main_result}} )
	{
	    # For each row, we start by calling the processRecord method (in case
	    # the query class has overridden it) and then call generateRecord to
	    # generate the actual output.
	    
	    $self->processRecord($row, $self->{proc_list});
	    my $row_output = $self->emitRecord($row, $first_row);
	    $output .= $row_output;
	    
	    $first_row = 0;
	    $self->{row_count}++;
	    
	    # If streaming is a possibility, check whether we have passed the
	    # threshold for result size.  If so, then we need to immediately
	    # generate the header and stash it along with the output so far.  We
	    # then return false, which should lead to a subsequent call to
	    # streamResult().
	    
	    if ( defined $options{can_stream} and $self->{should_stream} ne 'no' and
		 (length($output) > $stream_threshold or $self->{should_stream} eq 'yes') )
	    {
		$self->{stashed_output} = $self->emitHeader(streamed => 1) . $output;
		return;
	    }
	}
    }
    
    my $final = $self->finishOutput();
    $output .= $final if defined $final;
    
    return $self->emitHeader() . $output . $self->emitFooter();
}


# stream_result ( )
# 
# Continue to generate a compound query result from where
# generateCompoundResult() left off, and stream it to the client record by
# record.  This routine should only be called if generateCompoundResult()
# returned false.
# 
# This routine must be passed a Plack 'writer' object, to which will be
# written in turn the stashed output from generateCompoundResult(), each
# subsequent record, and then the footer.  Each of these chunks of data will
# be immediately sent off to the client, instead of being marshalled together
# in memory.  This allows the server to send results up to hundreds of
# megabytes in length without bogging down.

sub stream_result {
    
    my ($self, $writer) = @_;
    
    my $sth = $self->{main_sth};
    my $row;
    
    # First send out the partial output previously stashed by
    # generateCompoundResult().
    
    $writer->write( encode_utf8($self->{stashed_output}) );
    
    # Then generate the remaining output.  We don't have to worry about
    # 'is_first', because we know that we're past the first row already.
    
    if ( $sth )
    {
	while ( $row = $sth->fetchrow_hashref )
	{
	    $self->processRecord($row, $self->{proc_list});
	    $self->{row_count}++;
	    my $output = $self->emitRecord($row);
	    $writer->write( encode_utf8($output) ) if defined $output and $output ne '';
	}
    }
    
    elsif ( $self->{main_result} )
    {
	while ( $row = shift @{$self->{main_result}} )
	{
	    $self->processRecord($row, $self->{proc_list});
	    $self->{row_count}++;
	    my $output = $self->emitRecord($row);
	    $writer->write( encode_utf8($output) ) if defined $output and $output ne '';
	}
    }
    
    # Call the finishOutput() method, and send whatever if returns (if
    # anything).
    
    my $final = $self->finishOutput();
    $writer->write( encode_utf8($final) ) if defined $final and $final ne '';
    
    # Finally, send out the footer and then close the writer object.
    
    my $footer = $self->emitFooter(streamed => 1);
    $writer->write( encode_utf8($footer) ) if defined $footer and $footer ne '';
    $writer->close();
}


# processRecord ( record, proc_list )
# 
# Carry out each of the specified processing steps on the given record.

sub processRecord {

    my ($self, $record, $proc_list) = @_;
    
    return unless ref $proc_list eq 'ARRAY';
    
    foreach my $p ( @$proc_list )
    {
	next unless ref $p eq 'HASH';
	
	my $field = $p->{rec};
	next unless $record->{$field};
	
	my @result;
	
	# First figure out the result of the processing step
	
	if ( reftype $p->{code} and reftype $p->{code} eq 'CODE' )
	{
	    if ( $p->{use_main} )
	    {
		@result = $p->{code}($self, $record, $p);
	    }
	    
	    elsif ( $p->{use_each} and ref $record->{$field} eq 'ARRAY' )
	    {
		@result = map { $p->{code}($self, $_, $p) } @{$record->{$field}};
	    }
	    
	    else
	    {
		@result = $p->{code}($self, $record->{$field}, $p);
	    }
	}
	
	elsif ( defined $p->{split} and $p->{split} ne '' )
	{
	    @result = split $p->{split}, $record->{$field};
	}
	
	elsif ( $p->{subfield} and reftype $record->{$field} )
	{
	    if ( reftype $record->{$field} eq 'ARRAY' )
	    {
		@result = map { $_->{$p->{subfield}} if ref $_ eq 'HASH'; } @{$record->{$field}};
	    }
	    
	    elsif  ( reftype $record->{$field} eq 'HASH' )
	    {
		@result = $record->{$field}{$p->{subfield}};
	    }
	}
	
	else
	{
	    @result = $record->{$field};
	}
	
	# Then add it or set it to the specified field.
	
        if ( $p->{add} )
	{
	    my $res = $p->{add};
	    $record->{$res} = [ $record->{$res} ] if defined $record->{$res}
		and ref $record->{$res} ne 'ARRAY';
	    
	    push @{$record->{$res}}, @result;
	}
	
	elsif ( $p->{set} )
	{
	    my $res = $p->{set};
	    if ( @result == 1 )
	    {
		($record->{$res}) = @result;
	    }
	    
	    elsif ( @result > 1 )
	    {
		$record->{$res} = \@result;
	    }
	    
	    elsif ( not $p->{always} )
	    {
		delete $record->{$res};
	    }
	    
	    else
	    {
		$record->{$res} = '';
	    }
	}
    }
}


# emitHeader ( )
# 
# Generate the proper header for the requested output format.

sub emitHeader {
    
    my ($self, @options) = @_;
    
    if ( $self->{output_format} eq 'json' )
    {
	return $self->emitHeaderJSON(@options);
    }
    
    elsif ( $self->{output_format} eq 'xml' )
    {
	return $self->emitHeaderXML(@options);
    }
    
    else
    {
	return $self->emitHeaderText(@options);
    }
}


# emitHeaderJSON ( options )
# 
# Return the proper header for a JSON result.  The accepted options are:
# 
#   streamed - if true, ignore report_count.  We don't yet know how many
#		records there will be, so we have to wait and let
#		emitFooterJSON() report this information.

sub emitHeaderJSON {

    my ($self, %options) = @_;
    
    my $valid = $self->{valid};
    my $json = JSON->new->allow_nonref;
    
    my $output = '{' . "\n";
    
    # Check if we have any warning messages to convey
    
    if ( defined $self->{warnings} and $self->{warnings} > 0 )
    {
	$output .= '"warnings":' . $json->encode($self->{warnings}) . ",\n";
    }
    
    # Check if we have an object class to report 
    
    if ( defined $self->{object_class} )
    {
	$output .= '"class":' . $json->encode($self->{object_class}) . ",\n";
    }
    
    # Check if we have been asked to report the result count, and if it is
    # available.
    
    if ( defined $valid->value('count') and defined $self->{result_count} )
    {
	my $limit = $valid->value('limit');
	my $returned = $limit eq 'all' ? $self->{result_count} : 
	    $limit < $self->{result_count} ? $limit : $self->{result_count};
	
	$output .= '"records_found":' . $json->encode($self->{result_count} + 0) . ",\n";
	$output .= '"records_returned":' . $json->encode($returned + 0) . ",\n";
    }
    
    # The actual data will go into an array, in a field called "records".
    
    $output .= '"records": [';
    return $output;
}


# emitHeaderXML ( )
# 
# Return the proper header for an XML document in Darwin Core format.  We
# ignore any options, because XML/Darwin Core is such a rigid format that we don't
# have much ability to customize it.

sub emitHeaderXML {

    my ($self) = @_;
    
    # First check to see if we have any warning messages to convey
    
    my $warnings = '';
    
    if ( ref $self->{warnings} eq 'ARRAY' and @{$self->{warnings}} > 0 )
    {
	foreach my $w ( @{$self->{warnings}} )
	{
	    $warnings .= "<!-- warning: $w -->\n";
	}
    }
    
    # Then generate the header
    
    return <<END_XML;
<?xml version="1.0" standalone="yes"?>
$warnings<dwr:DarwinRecordSet xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dwr="http://rs.tdwg.org/dwc/dwcrecord/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://rs.tdwg.org/dwc/dwcrecord/ http://rs.tdwg.org/dwc/xsd/tdwg_dwc_classes.xsd">
END_XML

}


# emitHeaderText ( )
# 
# Return the proper header for comma-separated text output or tab-separated
# text output.  We ignore any options, because there is no room for extra
# information when using this format.

sub emitHeaderText {

    my ($self) = @_;
    
    my $valid = $self->{valid};
    
    # Skip the header if we are directed to do so.
    
    return '' if $valid->value('no_header');
    
    # Otherwise, we go through the output list and collect up the field names. 
    
    my $vocab = $self->{vocab};
    
    my (@fields) = map { $_->{$vocab} || $_->{rec} } @{$self->{output_list}};
    
    # Now put them all together into one line.
    
    return $self->generateTextLine(@fields);
}


# emitRecord ( )
# 
# Generate the output for a single record

sub emitRecord {

    my ($self, $record, $is_first) = @_;
    
    if ( $self->{output_format} eq 'json' )
    {
	return $self->emitRecordJSON($record, $is_first);
    }
    
    elsif ( $self->{output_format} eq 'xml' )
    {
	return $self->emitRecordXML($record);
    }
    
    else
    {
	return $self->emitRecordText($record);
    }
}


# emitRecordJSON ( )
# 
# Generate the proper output for a single record in JSON.

sub emitRecordJSON {
    
    my ($self, $record, $is_first) = @_;
    
    # Start the output.
    
    my $init = $is_first ? "\n" : ",\n";
    
    # Construct a hash that we can pass to the to_json function.
    
    my $outrec = $self->constructObjectJSON($record, $self->{output_list});
    
    return $init . $outrec;
}


# constructObjectJSON ( record, rule )
# 
# Generate a hash based on the given record and the specified rule (or rules,
# as an array ref).

sub constructObjectJSON {

    my ($self, $record, $rulespec) = @_;
    
    my $vocab = $self->{vocab};
    
    # Start with an empty string.
    
    my $outrec = '{';
    my $sep = '';
    
    # Go through the rule list, generating the fields one by one.
    
    foreach my $f (reftype $rulespec && reftype $rulespec eq 'ARRAY' ? @$rulespec : $rulespec)
    {
	# Skip rules which are not valid in this vocabulary.
	
	my $outkey = $vocab eq 'pbdb' ? $f->{pbdb} || $f->{rec} : $f->{$vocab};
	next unless $outkey;
	
	# Skip any field that is empty, unless 'always' or 'value' is set.
	
	my $field = $f->{rec};
	next unless $f->{always} or $f->{value} or 
	    defined $record->{$field} and $record->{$field} ne '';
	
	# Skip any field indicated by a 'dedup' rule.
	
	next if $f->{dedup} and defined $record->{$field} and defined $record->{$f->{dedup}}
	    and $record->{$field} eq $record->{$f->{dedup}};
	
	# Process the rule to generate a key/value pair.  If a code ref was
	# supplied, call that routine.  Otherwise, generate either an array,
	# sub-object or scalar value as indicated.
	
	my $value = defined $record->{$field} ? $record->{$field} : '';
	
	# If a specific value was defined, use that instead of the field value.
	
	$value = $f->{value} if defined $f->{value};
	
	# Process the value according to the rule
	
	if ( $f->{use_each} and reftype $value && reftype $value eq 'ARRAY' )
	{
	    my $rule = $f->{rule} || $f;
	    $value = $self->constructArrayJSON($value, $rule);
	}
	
	elsif ( reftype $f->{code} && reftype $f->{code} eq 'CODE' )
	{
	    if ( $f->{use_main} ) {
		$value = json_clean($f->{code}($self, $record, $f));
	    } else  {
		$value = json_clean($f->{code}($self, $value, $f));
	    }
	}
	
	elsif ( reftype $f->{code} && reftype $f->{code} eq 'HASH' )
	{
	    $value = json_clean($f->{code}{$value});
	}
	
	elsif ( reftype $value && reftype $value eq 'ARRAY' )
	{
	    my $rule = $f->{rule} || $f;
	    $value = $self->constructArrayJSON($value, $rule);
	}
	
	elsif ( $f->{json_list} )
	{
	    $value = $self->constructArrayJSON([$value], $f);
	}
	
	elsif ( $f->{json_list_literal} )
	{
	    if ( $value =~ /^(?:\d+,)*\d+$/ )
	    {
		$value = '[' . $value . ']';
	    }
	    else
	    {
		$value = '[]';
	    }
	}
	
	elsif ( reftype $record->{$field} && reftype $record->{$field} eq 'HASH' )
	{
	    my $rule = $f->{rule} || $f;
	    $value = $self->constructObjectJSON($value, $rule);
	}
	
	else
	{
	    $value = json_clean($value);
	}
	
	if ( defined $value and $value ne '' )
	{
	    $outrec .= "$sep\"$outkey\":$value";
	    $sep = ',';
	}
    }
    
    # If this record has hierarchical children, process them now.
    
    if ( exists $record->{hier_child} )
    {
	my $children = $self->constructArrayJSON($record->{hier_child}, $rulespec);
	$outrec .= ',"children":' . $children;
    }
    
    # Now return the record.

    return "$outrec}";
}


sub constructArrayJSON {

    my ($self, $arrayref, $rulespec) = @_;
    
    my $f = $rulespec if reftype $rulespec && reftype $rulespec ne 'ARRAY';
    
    # Start with an empty string.
    
    my $outrec = '[';
    my $sep = '';
    
    # Go through the elements of the specified arrayref, applying the
    # specified rule to each one.  If a code ref was supplied, call that
    # routine.  Otherwise, generate either an array, sub-object or scalar
    # value as indicated.
    
    my $value = '';
    
    foreach my $elt ( @$arrayref )
    {
	if ( reftype $rulespec && reftype $rulespec eq 'ARRAY' )
	{
	    $value = $self->constructObjectJSON($elt, $rulespec);
	}
	
	elsif ( reftype $f->{code} && reftype $f->{code} eq 'CODE' )
	{
	    $value = json_clean($f->{code}($elt, $rulespec));
	}
	
	elsif ( reftype $f->{code} && reftype $f->{code} eq 'HASH' )
	{
	    $value = json_clean($f->{code}{$elt});
	}
	
	elsif ( ref $elt eq 'ARRAY' )
	{
	    my $subrule = $f->{rule} || $rulespec;
	    $value = $self->constructArrayJSON($elt, $subrule);
	}
	
	elsif ( reftype $elt && reftype $elt eq 'HASH' )
	{
	    my $subrule = $f->{rule} || $rulespec;
	    $value = $self->constructObjectJSON($elt, $subrule);
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
    
    return "$outrec]";
}


# emitRecordText ( )
# 
# Generate the proper output for a single record in txt or csv format.

sub emitRecordText {
    
    my ($self, $record) = @_;
    
    # Start the output.
    
    my @values;
    my $vocab = $self->{vocab};
    
    foreach my $f ( @{$self->{output_list}} )
    {
	my $field = $f->{rec};
	my $sep = $f->{txt_list} || ',';
	
	# Process the rule to generate a value for each output rule, which may
	# be empty.  If a code ref was supplied, call that routine or apply
	# that hash.  If a list of values is found, join them using the
	# indicated separator.
	
	my $value = defined $record->{$field} ? $record->{$field} : '';
	
	# If a specific value was defined, use that instead of the field value.
	
	$value = $f->{value} if defined $f->{value};
	
	if ( reftype $f->{code} and reftype $f->{code} eq 'CODE' )
	{
	    if ( $f->{use_main} ) {
		$value = $f->{code}($self, $record, $f);
	    } elsif ( $f->{use_each} && reftype $value && reftype $value eq 'ARRAY' ) {
		my @list = map { $f->{code}($self, $_, $f) } @$value;
		$value = join($sep, @list);
	    } else {
		$value = $f->{code}($self, $value, $f);
	    }
	}
	
	elsif ( ref $f->{code} eq 'HASH' )
	{
	    if ( $f->{use_each} && reftype $value && reftype $value eq 'ARRAY' ) {
		my @list = map { $f->{code}{$_} } @$value;
		$value = join($sep, @list);
	    } else {
		$value = $f->{code}{$value};
	    }
	}
	
	elsif ( reftype $value && reftype $value eq 'ARRAY' )
	{
	    $value = join($sep, @$value);
	}
	
	push @values, $value;
    }
    
    return $self->generateTextLine(@values);
}


# emitFooter ( )
# 
# Generate the proper footer for the requested output format.

sub emitFooter {
    
    my ($self, @options) = @_;
    
    if ( $self->{output_format} eq 'json' )
    {
	return $self->emitFooterJSON(@options);
    }
    
    elsif ( $self->{output_format} eq 'txt' or $self->{output_format} eq 'csv' )
    {
	return $self->emitFooterText(@options);
    }
    
    else
    {
	return $self->emitFooterXML(@options);
    }
}


# emitFooterXML ( )
# 
# Return the proper footer for an XML document in Darwin Core format.

sub emitFooterXML {
    
    my ($self) = @_;
    
    my $output = <<END_XML;
</dwr:DarwinRecordSet>
END_XML

    if ( defined $self->{report_count} )
    {
	$output .= "<!-- records_found: " . $self->countRecords . " -->\n";
	$output .= "<!-- records_returned: " . $self->{row_count} . " -->\n";
    }
    
    return $output;
}


# emitFooterText ( )
# 
# Return nothing, because text files (whether comma-separated values or
# tab-separated values) don't have footers.

sub emitFooterText {

    return '';
}


# emitFooterJSON ( )
# 
# Return a string that will be valid as the end of a JSON result, after the
# header and zero or more records.  If the option "streamed" is given, then
# add in the record count.

sub emitFooterJSON {
    
    my ($self, %options) = @_;
    
    my $json = JSON->new->allow_nonref;
    my $output = "\n]";
    
    # If we were asked to report the number of records returned, and we are
    # streaming the results, report those numbers now.
    
    if ( defined $self->{report_count} and $options{streamed} )
    {
	$output .= ",\n" . '"records_found":' . $json->encode($self->countRecords + 0);
	$output .= ",\n" . '"records_returned":' . $json->encode($self->{row_count} + 0);
    }
    
    # Finish up the JSON object.
    
    $output .= "\n}\n";
    
    return $output;
}


# initOutput ( )
# 
# This method is intended to be overridden.  It is called before the first
# record is generated, and if it returns a string that string is appended to
# the output just after the header and before the first record.

sub initOutput {

}


# finishOutput ( )
# 
# This method is intended to be overridden.  It is called after the last
# record is generated, and if it returns a string that string is appended to
# the output just after the last record and before the header.

sub finishOutput {

}


# Following are some utility routines
# ===================================

# generateTextLine ( )
# 
# Generate a text line according to the output format and the 'quoted' and
# 'linebreak' parameters.

sub generateTextLine {

    my $self = shift;
    
    my $valid = $self->{valid};
    my $quoted = $self->{quoted};
    my $term = $valid->value('linebreak') eq 'cr' ? "\n" : "\r\n";
    
    if ( $self->{output_format} eq 'csv' )
    {
	return join(',', map { csv_clean($_, $quoted) } @_) . $term;
    }
    
    else
    {
	return join("\t", map { txt_clean($_) } @_) . $term;
    }
}


# xml_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of an XML document.

my (%ENTITY) = ( '&' => '&amp;', '<' => '&lt;', '>', '&gt;' );

sub xml_clean {
    
    my ($string, $preserve_tags) = @_;
    
    # Return an empty string unless the value is defined.
    
    return "" unless defined $string;
    
    # Do a quick check for okay characters.  If there's nothing exotic, just
    # return the value as-is.
    
    return $string unless $string =~ /[^a-zA-Z0-9 _.,;:-]/;
    
    # Otherwise, we have more work to do.
    
    # First, turn any numeric character references into actual Unicode
    # characters.  The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Then, take out all <b> and <i>.  We can't leave these in, because they
    # would look like XML tags instead of the HTML markup that they are.
    
    $string =~ s/<\/?[bi]>//g;
    
    # Next, rename any instance of & that doesn't start a valid character
    # entity reference to &amp; and rename all < and > to &lt; and &gt;
    
    $string =~ s/(&(?!\w+;)|<|>)/$ENTITY{$1}/ge;
    
    # Finally, delete all control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them) as well as invalid utf-8 characters.
    
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
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


my (%TXTESCAPE) = ( '"' => '""', "'" => "''", "\t" => '\t', "\n" => '\n',
		 "\r" => '\r' );	#'

# csv_clean ( string, quoted )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a csv-format result.  If 'quoted' is true, then all fields will
# be quoted.  Otherwise, only those which contain commas or quotes will be.

sub csv_clean {

    my ($string, $quoted) = @_;
    
    # Return an empty string unless the value is defined.
    
    return $quoted ? '""' : '' unless defined $string;
    
    # Do a quick check for okay characters.  If there's nothing exotic, just
    # return the quoted value.
    
    return $quoted ? '"' . $string . '"' : $string
	unless $string =~ /[^a-zA-Z0-9 _.;:<>-]/;
    
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
    
    return $quoted ? '"' . $string . '"' : $string;
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


sub generateLimitClause {

    my ($self) = @_;
    
    my $valid = $self->{valid};
    my $limit = $valid->value('limit');
    my $offset = $valid->value('offset');
    
    if ( defined $offset and $offset > 0 )
    {
	$offset += 0;
	$limit = $limit eq 'all' ? 10000000 : $limit + 0;
	return "LIMIT $offset,$limit";
    }
    
    elsif ( defined $limit and $limit ne 'all' )
    {
	return "LIMIT " . ($limit + 0);
    }
    
    return '';
}


our ($UTF8_DECODER) = Encode::find_encoding("utf8");

# decodeFields ( )
# 
# Decode the various fields from a given record from utf-8.

sub decodeFields {
    
    my ($self, $row) = @_;
    
    my @fields = qw(a_al1 a_al2 a_ai1 a_ai2 a_ao r_al1 r_al2 r_ai1
		    r_ai2 r_ao r_reftitle r_pubtitle r_editors);
    
    foreach my $f (@fields)
    {
	if ( defined $row->{$f} )
	{
	    eval {
		$row->{$f} = decode("utf8", $row->{$f}, Encode::FB_CROAK);
	    };
	}
    }
}


# generateURN ( record_no, record_type )
# 
# Given a record number and record type, generate a URN.  The format is:
# "urn:paleodb.org:<record_type>:<record_no>".

sub generateURN {
    
    my ($record_no, $record_type) = @_;
    
    my $type_label = 'x';
    $type_label = 'tn' if $record_type eq 'taxon_no';
    
    return "urn:paleodb:$type_label$record_no";
}


# reportError ( )
# 
# Return an error message, in the proper format.

sub reportError {

    my ($self) = @_;
    
    my $message = $self->{error} || "An error occurred.";
    
    if ( $self->{output_format} eq 'json' )
    {
	return '{"error":"' . $message . '"}';
    }
    
    else
    {
	return $message;
    }   
}

1;
