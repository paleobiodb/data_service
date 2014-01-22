
# DataService::Output
# 
# 
# 
# Author: Michael McClennen

use strict;

package Web::DataService;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);

use Dancer::Plugin::StreamData;


our (%MAP_DEF) = (name => 'type',
		  block => 'single',
		  disabled => 'single');

our ($BLOCK_REGEXP) = qr{ ^ [\w:/]+ $ }x;

our ($DEFAULT_INSTANCE);

# define_output_map ( package, specification... )
# 
# Define a map a map that specifies which output blocks will be used by the
# specified class, associating with each one a value for the 'show' parameter.

sub define_output_map {

    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    my $class = shift;
    
    # Make sure that we were given a valid class
    
    croak "define_output_map: the first argument must be the name of a subclass of Web::DataService::Request"
	unless $class->isa("Web::DataService::Request");
    
    # Initialize the output map for the specified class.
    
    $self->{output_map}{$class} = {};
    $self->{output_map_list}{$class} = [];
    
    # Then process the records one by one.  Make sure to throw am error if we
    # find an invalid record.
    
    my $last_node;
    
    foreach my $item (@_)
    {
	# A scalar is interpreted as a documentation string.
	
	unless ( ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	    next;
	}
	
	# Any item that is not a hashref or a scalar is an error.
	
	unless ( ref $item eq 'HASH' )
	{
	    croak "define_output_map: arguments must be hashrefs or scalars";
	}
	
	# Check for invalid attributes.
	
	foreach my $k ( keys %$item )
	{
	    croak "define_output_map: unknown attribute '$k'"
		unless defined $MAP_DEF{$k};
	}
	
	# Check that we have a name and a block.
	
	my $name = $item->{name} // '';
	my $block = $item->{block} // '';
	
	unless ( defined $name && defined $block )
	{
	    croak "define_output_map: you must include the attributes 'name' and 'block' in each hashref";
	}
	
	unless ( $name =~ qr{ ^ [a-z_]+ $ }x )
	{
	    croak "define_output_map: invalid value '$name' for 'name'";
	}
	
	unless ( $name =~ $BLOCK_REGEXP )
	{
	    croak "define_output_map: invalid value '$block' for 'block'";
	}
	
	push @{$self->{output_map_list}{$class}}, $item unless $item->{disabled};
	$self->{output_map}{$class}{$name} = $item;
    }
}


# define_block ( name, specification... )
# 
# Define an output block with the specified name, using the given
# specification records.

sub define_block {
    
    my $self = shift;
    my $name = shift;
    
    # Check to make sure that we were given a valid name.
    
    if ( ref $name )
    {
	croak "define_block: the first argument must be an output block name";
    }
    
    elsif ( $name !~ $BLOCK_REGEXP )
    {
	croak "define_block: invalid block name '$name'";
    }
    
    # Initialize the block list for the specified block.
    
    $self->{block_list}{$name} = [];
    
    # Then process the records one by one.  Make sure to throw an error if we
    # find a record whose type is ambiguous or that is otherwise invalid.  Each
    # record gets put in a list that is stored under the section name.
    
    my $last_node;
    
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
	
	$last_node = $item if $type eq 'output';
	
	# Add the record to the appropriate list.
	
	if ( $type eq 'include' )
	{
	    push @{$self->{include_list}{$name}}, $item;
	}
	
	else
	{
	    push @{$self->{block_list}{$name}}, $item;
	}
    }
}


our %OUTPUT_DEF = (output => 'type',
		   set => 'type',
		   select => 'type',
		   filter => 'type',
		   include => 'type',
		   require => 'type',
		   if_section => 'set',
		   not_section => 'set',
		   if_vocab => 'set',
		   not_vocab => 'set',
		   if_format => 'set',
		   not_format => 'set',
		   if_field => 'single',
		   not_field => 'single',
		   if_code => 'code',
		   dedup => 'single',
		   value => 'single',
		   always => 'single',
		   text_join => 'single',
		   xml_join => 'single',
		   rule => 'single',
		   from => 'single',
		   from_each => 'single',
		   from_record => 'single',
		   append => 'single',
		   code => 'code',
		   lookup => 'hash',
		   split => 'regexp',
		   join => 'single',
		   tables => 'set',
		   doc => 'single');

our %OUTPUT_KEY = (output => 2, set => 2, select => 2, filter => 2, include => 2,
		   dedup => 1, name => 1, value => 1, always => 1, rule => 1,
		   if_section => 1, not_section => 1, if_format => 1, not_format => 1,
		   if_field => 1, not_field => 1, if_code => 1, from_record => 1,
		   from => 1, from_each => 1, append => 1, code => 1, lookup => 1,
		   split => 1, join => 1, tables => 1, doc => 1, insert => 1);

our %FIELD_KEY = (dedup => 1, value => 1, always => 1, rule => 1, if_field => 1, text_join => 1, xml_join => 1, doc => 1);

our %PROC_KEY = (set => 1, append => 1, from => 1, from_each => 1, from_record => 1,
		 code => 1, lookup => 1, split => 1, join => 1, subfield => 1);

sub check_output_record {
    
    my ($self, $record) = @_;
    
    my $type = '';
    
    foreach my $k (keys %$record)
    {
	my $v = $record->{$k};
	
	if ( $k =~ qr{ ^ (\w+) _ (name|value) $ }x )
	{
	    croak "define_output: unknown format or vocab '$1' in '$k'"
		unless defined $self->{vocab}{$1} || defined $self->{format}{$1};
	}
	
	elsif ( ! defined $OUTPUT_DEF{$k} )
	{
	    croak "define_output: unrecognized attribute '$k'";
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'type' )
	{
	    croak "define_output: you cannot have both attributes '$type' and '$k' in one record"
		if $type;
	    
	    $type = $k;
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'single' )
	{
	    croak "define_output: the value of '$k' must be a scalar" if ref $v;
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'set' )
	{
	    croak "define_output: the value of '$k' must be an array ref or string"
		if ref $v && reftype $v ne 'ARRAY';
	    
	    unless ( ref $v )
	    {
		$v = [ split(qr{\s*,\s*}, $v) ];
	    }
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'code' )
	{
	    croak "define_output: the value of '$k' must be a code ref"
		unless ref $v && reftype $v eq 'CODE';
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'hash' )
	{
	    croak "define_output: the value of '$k' must be a hash ref"
		unless ref $v && reftype $v eq 'HASH';
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'regexp' )
	{
	    croak "define_output: the value of '$k' must be a regexp or string"
		if ref $v && reftype $v ne 'REGEXP';
	}
    }
    
    # Now make sure that each record has a 'type' attribute.
    
    croak "each record passed to define_output must include one attribute from the \
following list: 'include', 'output', 'set', 'select', 'filter'"
	unless $type;
    
    return $type;
}


# # get_output_list ( list, section )
# # 
# # Return the output list for the specified section.

# sub get_block {

#     my ($self, $class, $list, $section) = @_;
    
#     die "get_output_list: invalid list '$list'" unless $list eq 'output_list' or $list eq 'include_list';
    
#     die "get_output_list: no output section was specified" unless $section;
#     die "get_output_list: invalid class '$class'" unless $class->isa('Web::DataService::Request');
    
#     # If this class or any of its superclasses has defined this section,
#     # then we use that definition.  Otherwise, put out a warning and go on
#     # to the next section.
    
#     my $loop_bound = 0;
    
#     until ( ref $self->{$list}{$class}{$section} )
#     {
# 	$class = $self->{super_class}{$class} or return;
# 	die "problem with class configuration" if ++$loop_bound > 10;
#     }
    
#     return $self->{$list}{$class}{$section};
# }


# configure_output ( request )
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
    
    # Use the appropriate output map to determine which output blocks we will
    # be using to express the result of this request.
    
    # Start with the base block list defined for the request path.
    
    my @blocks = $request->base_block_list;
    push @blocks, $request->extra_block_list;
    
    unless ( @blocks )
    {
	$request->add_warning("no output blocks were specified for this request");
	return;
    }
    
    $request->{select_list} = [];
    $request->{select_hash} = {};
    $request->{tables_hash} = {};
    $request->{filter_hash} = {};
    $request->{proc_list} = [];
    $request->{field_list} = [];
    $request->{block_set} = { map { $_ => 1 } @blocks };
    
    # First scan through the list of output blocks and check for include_list
    # entries.  This allows us to know before the rest of the processing
    # exactly which blocks are included.
    
    my %uniq_block;
    
 INCLUDE_BLOCK:
    foreach my $block (@blocks)
    {
	# Make sure that each block is checked only once.
	
	next if $uniq_block{$block}; $uniq_block{$block} = 1;
	
	my $include_list = $self->{include_list}{$block};
	next unless ref $include_list eq 'ARRAY';
	
      INCLUDE_RECORD:
	foreach my $r ( @$include_list )
	{
	    # Evaluate dependency on the output section list
	    
	    next INCLUDE_RECORD if $r->{if_section} 
		and not check_set($r->{if_section}, $request->{section_set});
	    
	    next INCLUDE_RECORD if $r->{not_section}
		and check_set($r->{not_section}, $request->{section_set});
	    
	    # Evaluate dependency on the output format
	    
	    next INCLUDE_RECORD if $r->{if_format}
		and not check_value($r->{if_format}, $format);
	    
	    next INCLUDE_RECORD if $r->{not_format}
		and check_value($r->{not_format}, $format);
	    
	    # Evaluate dependency on the vocabulary
	    
	    next INCLUDE_RECORD if $r->{if_vocab}
		and not check_value($r->{if_vocab}, $vocab);
	    
	    next INCLUDE_RECORD if $r->{not_vocab}
		and check_value($r->{not_vocab}, $vocab);
	    
	    # Now add the specified block.
	    
	    my $new_block = $r->{include};
	    
	    push @blocks, $new_block;
	    $request->{block_set}{$new_block} = 1;
	}
    }
    
    # Now run through all of the blocks we have identified and collect up the
    # various kinds of records they contain.
    
    %uniq_block = ();
    
 BLOCK:
    foreach my $block (@blocks)
    {
	# Make sure that each block is only processed once, even if it is
	# listed more than once.
	
	next if $uniq_block{$block}; $uniq_block{$block} = 1;
	
	# Generate a warning if the specified block does not exist, but do
	# not abort the request.
	
	my $block_list = $self->{block_list}{$block};
	
	unless ( ref $block_list eq 'ARRAY' )
	{
	    warn "undefined output block '$block' for path '$request->{path}'\n";
	    $request->add_warning("undefined output block '$block'");
	    next BLOCK;
	}
	
	# Now go through the output list for this block and collect up
	# all records that are selected for this query.
	
    RECORD:
	foreach my $r ( @$block_list )
	{
	    # Evaluate dependency on the output block list
	    
	    next RECORD if $r->{if_block} 
		and not check_set($r->{if_block}, $request->{block_set});
	    
	    next RECORD if $r->{not_block}
		and check_set($r->{not_block}, $request->{block_set});
	    
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
		
		foreach my $key ( keys %$r )
		{
		    if ( $PROC_KEY{$key} )
		    {
			$proc->{$key} = $r->{$key};
		    }
		    
		    else
		    {
			warn "Warning: unknown key '$key' in proc record\n";
		    }
		}
		
		push @{$request->{proc_list}}, $proc;
	    }
	    
	    # If the record type is 'output', add a record to the field list.
	    # The attributes 'name' (the output name) and 'field' (the raw
	    # field name) are both set to the indicated name by default.
	    
	    elsif ( defined $r->{output} )
	    {
		next RECORD if $require_vocab and not exists $r->{"${vocab}_name"};
		
		my $field = { field => $r->{output}, name => $r->{output} };
		
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
		    
		    elsif ( $key ne 'output' )
		    {
			warn "Warning: unknown key '$key' in output record\n";
		    }
		}
		
		push @{$request->{field_list}}, $field;
	    }
	}
    }
}    


# configure_block ( request, block_name )
# 
# Given a block name, determine the list of output fields and proc fields
# (if any) that are defined for it.  This is used primarily to configure
# blocks referred to via 'rule' attributes.
# 
# These lists are stored under the keys 'block_proc_list' and
# 'block_field_list' in the request record.  If these have already been filled
# in for this block, do nothing.

sub configure_block {

    my ($self, $request, $block_name) = @_;
    
    # Return immediately if the relevant lists have already been computed
    # and cached (even if they are empty).
    
    return 1 if exists $request->{block_field_list}{$block_name};
    
    # Otherwise, we need to compute both lists.  Start by determining the
    # relevant attributes of the request and looking up the output list
    # for this block.
    
    my $vocab = $request->{vocab};
    my $require_vocab = 1 if $vocab and not $self->{vocab}{$vocab}{use_field_names};
    
    my $block_list = $self->{block_list}{$block_name};
    
    # If no list is available, indicate this to the request object and return
    # false.  Whichever routine called us will be responsible for generating an
    # error or warning if appropriate.
    
    unless ( ref $block_list eq 'ARRAY' )
    {
	$request->{block_field_list} = undef;
	$request->{block_proc_list} = undef;
	return;
    }
    
    # Go through each record in the list, throwing out the ones that don't
    # apply and assigning the ones that do.
    
    my (@field_list, @proc_list);
    
 RECORD:
    foreach my $r ( @$block_list )
    {
	# Evaluate dependency on the output block list
	
	next RECORD if $r->{if_block} 
	    and not check_set($r->{if_block}, $request->{block_set});
	
	next RECORD if $r->{not_block}
	    and check_set($r->{not_block}, $request->{block_set});
	
	# Evaluate dependency on the output format
	
	next RECORD if $r->{if_format}
	    and not check_value($r->{if_format}, $request->{format});
	
	next RECORD if $r->{not_format}
	    and check_value($r->{not_format}, $request->{format});
	
	# Evaluate dependency on the vocabulary
	
	next RECORD if $r->{if_vocab}
	    and not check_value($r->{if_vocab}, $vocab);
	
	next RECORD if $r->{not_vocab}
	    and check_value($r->{not_vocab}, $vocab);
	
	# If the record type is 'output', add a record to the field list.
	# The attributes 'name' (the output name) and 'field' (the raw
	# field name) are both set to the indicated name by default.
	    
	if ( defined $r->{output} )
	{
	    next RECORD if $require_vocab and not exists $r->{"${vocab}_name"};
	
	    my $output = { field => $r->{output}, name => $r->{output} };
	    
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
		
		elsif ( $key ne 'output' )
		{
		    warn "Warning: unknown key '$key' in output record\n";
		}
	    }
	    
	    push @field_list, $output;
	}
	
	# If the record type is 'set', add a record to the proc list.
	
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
    
    $request->{block_field_list}{$block_name} = \@field_list;
    $request->{block_proc_list}{$block_name} = \@proc_list;
    
    return 1;
}


# check_value ( list, value )
# 
# Return true if $list is equal to $value, or if it is a list and one if its
# items is equal to $value.

sub check_value {
    
    my ($list, $value) = @_;
    
    return 1 if $list eq $value;
    
    if ( ref $list eq 'ARRAY' )
    {
	foreach my $item (@$list)
	{
	    return 1 if $item eq $value;
	}
    }
    
    return;
}


# check_set ( list, set )
# 
# The parameter $set must be a hashref.  Return true if $list is one of the
# keys of $set, or if it $list is a list and one of its items is a key in
# $set.  A key only counts if it has a true value.

sub check_set {
    
    my ($list, $set) = @_;
    
    return unless ref $set eq 'HASH';
    
    return 1 if $set->{$list};
    
    if ( ref $list eq 'ARRAY' )
    {
	foreach my $item (@$list)
	{
	    return 1 if $set->{$item};
	}
    }
    
    return;
}

# document_response ( path, block_list )
# 
# Generate documentation in POD format describing the available output fields.
# The parameter $block_list must be a reference to a list of block names,
# while $allowed_vocab must be a reference to a hash whose keys are vocabulary
# names.

sub document_response {
    
    my ($self, $class, $allowed_vocab, $block_list) = @_;
    
    # First determine the set of output blocks to document, or return
    # immediately if there are none.
    
    return unless ref $block_list eq 'ARRAY' && @$block_list;
    
    # Next determine the set of vocabularies to document, or return
    # immediately if there are none.
    
    return "I<No output vocabularies were selected for this path>"
	unless ref $allowed_vocab eq 'HASH' && keys %$allowed_vocab;
    
    my @vocab_list = grep { $allowed_vocab->{$_} && 
			    ref $self->{vocab}{$_} &&
			    ! $self->{vocab}{$_}{disabled} } @{$self->{vocab_list}};
    
    return "I<No output vocabularies were selected for this path>"
	unless @vocab_list;
    
    # Now generate the header for the documentation, in POD format.  We
    # include the special "=for pp_table_header" line to give PodParser.pm the
    # information it needs to generate an HTML table.
    
    my $doc_string = '';
    my $field_count = scalar(@vocab_list);
    my $field_string = join ' / ', @vocab_list;
    
    if ( $field_count > 1 )
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pp_table_header Field name*/$field_count | Block | Description\n\n";
	$doc_string .= "=item $field_string\n\n";
    }
    
    else
    {
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=for pp_table_header Field name / Block / Description\n\n";
    }
    
    # Run through each block one at a time, documenting all of the fields in
    # the corresponding field list.
    
    my %uniq_block;
    
    foreach my $b (@$block_list)
    {
	# Look up the block name in the output map for this class to get the
	# global block name.
	
	next unless ref $self->{output_map}{$class}{$b} eq 'HASH';
	
	my $block = $self->{output_map}{$class}{$b}{block};
	
	# Make sure to only process each block once, even if it is listed more
	# than once.
	
	next if $uniq_block{$block}; $uniq_block{$block} = 1;
	
	my $output_list = $self->{block_list}{$block};
	next unless ref $output_list eq 'ARRAY';
	
	foreach my $r (@$output_list)
	{
	    $doc_string .= $self->document_field($block, \@vocab_list, $r);
	}
    }
    
    $doc_string .= "=back\n\n";
    
    return $doc_string;
}


sub document_field {
    
    my ($self, $block_name, $vocab_list, $r) = @_;
    
    my @names;
    
    foreach my $v ( @$vocab_list )
    {
	my $n = defined $r->{"${v}_name"}	    ? $r->{"${v}_name"}
	      : $self->{vocab}{$v}{use_field_names} ? $r->{output}
	      :					      '';
	
	push @names, $n
    }
    
    my $names = join ' / ', @names;
    
    $block_name = $r->{show} if $r->{show};
    
    my $descrip = $r->{doc} || "";
    
    my $line = "\n=item $names ( $block_name )\n\n$descrip\n";
    
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
	# Figure out which field (if any) we are affecting.  A value of '*'
	# means to use the entire record (only relevant with 'code').
	
	my $set_field = $p->{set};
	
	# Figure out which field (if any) we are looking at.  Skip this
	# processing step if the source field is empty, unless the attribute
	# 'always' is set.
	
	my $source_field = $p->{from} || $p->{from_each};
	
	# Skip any processing step if the record does not have a non-empty
	# value in the corresponding field (unless the 'always' attribute is
	# set).
	
	if ( $source_field && ! $p->{always} )
	{
	    next unless defined $record->{$source_field} && $record->{$source_field} ne '';
	    next if ref $record->{$source_field} eq 'ARRAY' && ! @{$record->{$source_field}};
	}
	
	# Skip this processing step based on a conditional field value, if one
	# is defined.
	
	if ( my $cond_field = $p->{if_field} )
	{
	    next unless defined $record->{$cond_field} && $record->{$cond_field} ne '';
	    next if ref $record->{$cond_field} eq 'ARRAY' && ! @{$record->{$cond_field}};
	}
	
	elsif ( $cond_field = $p->{not_field} )
	{
	    next if defined $record->{$cond_field} && $record->{$cond_field} ne '';
	}
	
	# Now generate a list of result values, according to the attributes of this
	# processing step.
	
	my @result;
	
	# If we have a 'code' attribute, then call it.
	
	if ( ref $p->{code} eq 'CODE' )
	{
	    if ( $p->{from_record} || $set_field eq '*' )
	    {
		@result = $p->{code}($self, $record, $p);
	    }
	    
	    elsif ( $p->{from_each} )
	    {
		@result = map { $p->{code}($self, $_, $p) } 
		    (ref $record->{$source_field} eq 'ARRAY' ? 
		     @{$record->{$source_field}} : $record->{$source_field});
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{code}($self, $record->{$source_field}, $p);
	    }
	    
	    else
	    {
		@result = $p->{code}($self, $record->{$set_field}, $p);
	    }
	}
	
	# If we have a 'lookup' attribute, then use it.
	
	elsif ( ref $p->{lookup} eq 'HASH' )
	{
	    if ( $p->{from_each} )
	    {
		if ( ref $record->{$source_field} eq 'ARRAY' )
		{
		    @result = map { $p->{lookup}{$_} } @{$record->{$source_field}};
		}
		elsif ( ! ref $record->{$source_field} )
		{
		    @result = $p->{lookup}{$record->{$source_field}};
		}
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{lookup}{$record->{$source_field}}
		    unless ref $record->{$source_field};
	    }
	    
	    elsif ( $set_field ne '*' && ! ref $record->{$set_field} )
	    {
		@result = $p->{lookup}{$record->{$set_field}};
	    }
	}
	
	# If we have a 'split' attribute, then use it.
	
	elsif ( defined $p->{split} )
	{
	    if ( $p->{from_each} )
	    {
		if ( ref $record->{$source_field} eq 'ARRAY' )
		{
		    @result = map { split($p->{split}, $_) } @{$record->{$source_field}};
		}
		elsif ( ! ref $record->{$source_field} )
		{
		    @result = split($p->{split}, $record->{$source_field});
		}
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = split $p->{split}, $record->{$source_field}
		    unless ref $record->{$source_field};
	    }
	    
	    elsif ( $set_field ne '*' )
	    {
		@result = split $p->{split}, $record->{$set_field}
		    unless ref $record->{$set_field} eq 'ARRAY';
	    }
	}
	
	# If we have a 'join' attribute, then use it.
	
	elsif ( defined $p->{join} )
	{
	    if ( $source_field )
	    {
		@result = join($p->{join}, @{$record->{$source_field}})
		    if ref $record->{$source_field} eq 'ARRAY';
	    }
	    
	    elsif ( $set_field ne '*' )
	    {
		@result = join($p->{join}, @{$record->{$set_field}})
		    if ref $record->{$set_field} eq 'ARRAY';
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
    my $format_class = $self->{format}{$format}{module};
    
    die "could not generate a result in format '$format': no implementing module was found"
	unless $format_class;
    
    # Get the lists that specify how to process each record and which fields
    # to output.
    
    my $proc_list = $request->{proc_list};
    my $field_list = $request->{field_list};
    
    # Generate the initial part of the output, before the first record.
    
    my $output = $format_class->emit_header($request, $field_list);
    
    # If there are any processing steps to do, then do them.
    
    $self->process_record($request, $self->{main_record}, $proc_list);
    
    # Generate the output corresponding to our single record.
    
    $output .= $format_class->emit_record($request, $request->{main_record}, $field_list);
    
    # Generate the final part of the output, after the last record.
    
    $output .= $format_class->emit_footer($request, $field_list);
    
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

    my ($self, $request, $streaming_threshold) = @_;
    
    # $$$ init output...
    
    
    # $$$ process result set?
    

    # Determine the output format and figure out which class implements it.
    
    my $format = $request->{format};
    my $format_class = $self->{format}{$format}{module};
    
    die "could not generate a result in format '$format': no implementing module was found"
	unless $format_class;
    
    # Get the lists that specify how to process each record and which fields
    # to output.
    
    my $proc_list = $request->{proc_list};
    my $field_list = $request->{field_list};
    
    # If we have an explicit result list, then we know the count.
    
    $request->{result_count} = scalar(@{$request->{main_result}})
	if ref $request->{main_result};
    
    # Generate the initial part of the output, before the first record.
    
    my $output = $format_class->emit_header($request, $field_list);
    
    # A record separator is emitted before every record except the first.  If
    # this format class does not define a record separator, use the empty
    # string.
    
    $request->{rs} = $format_class->can('emit_separator') ?
	$format_class->emit_separator($request) : '';
    
    my $emit_rs = 0;
    
    $request->{actual_count} = 0;
    
    # If an offset was specified and the result method didn't handle this
    # itself, then skip the specified number of records.
    
    if ( defined $request->{result_offset} && $request->{result_offset} > 0
	 && ! $request->{offset_handled} )
    {
	$self->next_record($request) foreach 1..$request->{result_offset};
    }
    
    # Now fetch and process each output record in turn.  If output streaming is
    # available and our total output size exceeds the threshold, switch over
    # to streaming.
    
    while ( my $record = $self->next_record($request) )
    {
	# If there are any processing steps to do, then process this record.
	
	$self->process_record($request, $record, $proc_list);
	
	# Generate the output for this record, preceded by a record separator if
	# it is not the first record.
	
	$output .= $request->{rs} if $emit_rs; $emit_rs = 1;
	
	$output .= $format_class->emit_record($request, $record, $field_list);
	
	# Keep count of the output records, and stop if we have exceeded the
	# limit. 
	
	last if $request->{result_limit} ne 'all' && 
	    ++$request->{actual_count} >= $request->{result_limit};
	
	# If streaming is a possibility, check whether we have passed the
	# threshold for result size.  If so, then we need to immediately
	# stash the output generated so far and call stream_data.  Doing that
	# will cause the current function to be aborted, followed by an
	# automatic call to &stream_result (defined below).
	
	if ( defined $streaming_threshold && length($output) > $streaming_threshold )
	{
	    $request->{stashed_output} = $output;
	    Dancer::Plugin::StreamData::stream_data($request, &stream_compound_result);
	}
    }
    
    # If we get here, then we did not initiate streaming.  So add the
    # footer and return the output data.
    
    # Generate the final part of the output, after the last record.
    
    $output .= $format_class->emit_footer($request, $field_list);
    
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
    # 	    while ( $record = $self->{main_sth}->fetchrow_hashref )
    # 	    {
    # 		push @rows, $record;
    # 	    }
    # 	}
	
    # 	else
    # 	{
    # 	    @rows = @{$self->{main_result}}
    # 	}
	
    # 	my $newrows = $self->{process_resultset}(\@rows);
	
    # 	if ( ref $newrows eq 'ARRAY' )
    # 	{
    # 	    foreach my $record (@$newrows)
    # 	    {
    # 		$self->processRecord($record, $self->{proc_list});
    # 		my $record_output = $self->emitRecord($record, is_first => $first_row);
    # 		$output .= $record_output;
		
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
    
    while ( my $record = $self->next_record($request) )
    {
	# If there are any processing steps to do, then process this record.
	
	$self->process_record($request, $record, $request->{proc_list});
	
	# Generate the output for this record, preceded by a record separator
	# since we are always past the first record once we have switched over
	# to streaming.
	
	my $output = $request->{rs};
	
	$output .= $format_class->emit_record($request, $record);
	
	$writer->write( encode_utf8($output) ) if defined $output and $output ne '';
	
	# Keep count of the output records, and stop if we have exceeded the
	# limit. 
	
	last if $request->{result_limit} ne 'all' && 
	    ++$request->{actual_count} >= $request->{result_limit};
    }
    
    # finish output...
    
    # my $final = $self->finishOutput();
    # $writer->write( encode_utf8($final) ) if defined $final and $final ne '';
    
    # Finally, send out the footer and then close the writer object.
    
    my $footer = $format_class->emit_footer($request);
    
    $writer->write( encode_utf8($footer) ) if defined $footer and $footer ne '';
    $writer->close();
}


# next_record ( request )
# 
# Return the next record to be output for the given request.  If
# $self->{main_result} is set, use that first.  Once that is exhausted (or if
# it was never set) then if $result->{main_sth} is set then read records from
# it until exhausted.

sub next_record {
    
    my ($self, $request) = @_;
    
    # If the result limit is 0, return nothing.  This prevents any records
    # from being returned.
    
    return if $request->{result_limit} eq '0';
    
    # If we have a 'main_result' array with something in it, return the next
    # item in it.
    
    if ( ref $request->{main_result} eq 'ARRAY' and @{$request->{main_result}} )
    {
	return shift @{$request->{main_result}};
    }
    
    # Otherwise, if we have a 'main_sth' statement handle, read the next item
    # from it.
    
    elsif ( ref $request->{main_sth} )
    {
	return $request->{main_sth}->fetchrow_hashref
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
