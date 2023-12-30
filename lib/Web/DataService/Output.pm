# 
# Web::DataService::Output
# 
# This module provides a role that is used by 'Web::DataService'.  It implements
# routines for configuring and generating data service output.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Output;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(carp croak);

use Moo::Role;


sub define_output_map {
    
    goto \&Web::DataService::Set::define_set;
}


# define_block ( name, specification... )
# 
# Define an output block with the specified name, using the given
# specification records.

sub define_block {
    
    my $ds = shift;
    my $name = shift;
    
    # Check to make sure that we were given a valid name.
    
    if ( ref $name )
    {
	croak "define_block: the first argument must be an output block name";
    }
    
    elsif ( not $ds->valid_name($name) )
    {
	croak "define_block: invalid block name '$name'";
    }
    
    # Make sure the block name is unique.
    
    if ( $ds->{block}{$name} )
    {
	my $location = $ds->{block_loc}{$name};
	croak "define_block: '$name' was already defined at $location\n";
    }
    
    else
    {
	my ($package, $filename, $line) = caller;
	$ds->{block_loc}{$name} = "$filename at line $line";
    }
    
    # Create a new block object.
    
    my $block = { name => $name,
		  include_list => [],
		  output_list => [] };
    
    $ds->{block}{$name} = bless $block, 'Web::DataService::Block';
    
    # Then process the records one by one.  Make sure to throw an error if we
    # find a record whose type is ambiguous or that is otherwise invalid.  Each
    # record gets put in a list that is stored under the section name.

    my $i = 0;
    
    foreach my $item (@_)
    {
	$i++;
	
	# A scalar is interpreted as a documentation string.
	
	unless ( ref $item )
	{
	    $ds->add_doc($block, $item);
	    next;
	}
	
	# Any item that is not a hashref is an error.
	
	unless ( ref $item eq 'HASH' )
	{
	    croak "the arguments to 'output_section' must be hashrefs or scalars";
	}
	
	# Check the output record to make sure it was specified correctly.
	
	my ($type) = $ds->check_output_record($item, $i);
	
	# If the type is 'field', then any subsequent documentation strings
	# will be added to that record.
	
	$ds->add_doc($block, $item) if $type eq 'output';
	
	# Add the record to the appropriate list(s).
	
	if ( $type eq 'include' )
	{
	    push @{$ds->{block}{$name}{include_list}}, $item;
	}
	
	push @{$ds->{block}{$name}{output_list}}, $item;
    }
    
    $ds->process_doc($block);
}


our %OUTPUT_DEF = (output => 'type',
		   set => 'type',
		   select => 'type',
		   filter => 'type',
		   include => 'type',
		   if_block => 'set',
		   not_block => 'set',
		   if_vocab => 'set',
		   not_vocab => 'set',
		   if_format => 'set',
		   not_format => 'set',
		   if_field => 'single',
		   not_field => 'single',
		   if_code => 'code',
		   dedup => 'single',
		   name => 'single',
		   value => 'single',
		   always => 'single',
		   text_join => 'single',
		   xml_join => 'single',
		   show_as_list => 'single',
		   data_type => 'single',
		   bad_value => 'single',
		   sub_record => 'single',
		   from => 'single',
		   from_each => 'single',
		   append => 'single',
		   code => 'code',
		   lookup => 'hash',
		   default => 'single',
		   split => 'regexp',
		   join => 'single',
		   tables => 'set',
		   disabled => 'single',
		   doc_string => 'single');

our %SELECT_KEY = (select => 1, tables => 1, if_block => 1);

our %FIELD_KEY = (dedup => 1, name => 1, value => 1, always => 1, sub_record => 1, if_field => 1, 
		  not_field => 1, if_block => 1, not_block => 1, if_format => 1, not_format => 1,
		  if_vocab => 1, not_vocab => 1, data_type => 1,
		  text_join => 1, xml_join => 1, doc_string => 1, show_as_list => 1, disabled => 1, undocumented => 1);

our %PROC_KEY = (set => 1, check => 1, append => 1, from => 1, from_each => 1, data_type => 1,
		 if_vocab => 1, not_vocab => 1, if_block => 1, not_block => 1,
	         if_format => 1, not_format => 1, if_field => 1, not_field => 1,
		 code => 1, lookup => 1, split => 1, join => 1, default => 1, disabled => 1);

our %DATA_TYPE = (str => 1, int => 1, pos => 1, dec => 1, sci => 1, mix => 1, json => 1);

sub check_output_record {
    
    my ($ds, $record, $i) = @_;
    
    my $type = '';
    $i ||= '?';
    
    foreach my $k (keys %$record)
    {
	my $v = $record->{$k};
	
	if ( $k =~ qr{ ^ (\w+) _ (name|value) $ }x && $k ne 'bad_value' )
	{
	    croak "define_block: in record $i, unknown format or vocab '$1' in '$k'"
		unless defined $ds->{vocab}{$1} || defined $ds->{format}{$1};
	}
	
	elsif ( ! defined $OUTPUT_DEF{$k} )
	{
	    croak "define_block: in record $i, unrecognized attribute '$k'";
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'type' )
	{
	    croak "define_block: in record $i, you cannot have both attributes '$type' and '$k' in one record"
		if $type;
	    
	    croak "define_block: in record $i, value of '$k' must be non-empty" unless $v;
	    
	    $type = $k;
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'single' )
	{
	    croak "define_block: in record $i, the value of '$k' must be a scalar" if ref $v;
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'set' )
	{
	    croak "define_output: in record $i, the value of '$k' must be an array ref or string"
		if ref $v && reftype $v ne 'ARRAY';
	    
	    unless ( ref $v )
	    {
		$record->{$k} = [ split(qr{\s*,\s*}, $v) ];
	    }
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'code' )
	{
	    croak "define_output: in record $i, the value of '$k' must be a code ref"
		unless ref $v && reftype $v eq 'CODE';
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'hash' )
	{
	    croak "define_output: in record $i, the value of '$k' must be a hash ref"
		unless ref $v && reftype $v eq 'HASH';
	}
	
	elsif ( $OUTPUT_DEF{$k} eq 'regexp' )
	{
	    croak "define_output: in record $i, the value of '$k' must be a regexp or string"
		if ref $v && reftype $v ne 'REGEXP';
	}
    }
    
    # Now make sure that each record has a 'type' attribute.
    
    croak "define_block: in record $i, no record type attribute was found" unless $type;
    
    return $type;
}


# _setup_output ( request )
# 
# Determine the list of selection, processing and output rules for the
# specified query, based on the query's attributes.  These attributes include: 
# 
# - the output map
# - the output format
# - the output vocabulary
# - the selected output keys
# 
# Depending upon the attributes of the various output records, all, some or
# none of them may be relevant to a particular query.

sub _setup_output {

    my ($ds, $request) = @_;
    
    # Extract the relevant attributes of the request
    
    my $path = $request->node_path;
    
    # Create a default output configuration for this request.
    
    $ds->init_output($request, 'main');
    
    $request->{current_output} = 'output_main';
    $request->{block_hash} = $request->{output_main}{block_hash};
    $request->{extra_headers} = [ ];
    
    my @output_list;

    # If the the node has an 'output_override' attribute, and if the output format matches one of
    # the formats specified in it, then stop here and do not complete the setup. In such cases,
    # the output method must add output blocks or else the output will be empty.

    if ( my $override = $ds->node_attr($path, 'output_override') )
    {
	return if check_value($override, $request->output_format);
    }
    
    # The node attribute 'output' specifies a list of blocks that are always included in this
    # configuration.
    
    if ( my $output_list = $ds->node_attr($path, 'output') )
    {
	if ( ref $output_list eq 'ARRAY' && @$output_list )
	{
	    @output_list = @$output_list;
	}
	
	elsif ( ! ref $output_list && $output_list )
	{
	    @output_list = $output_list;
	}
    }
    
    # The attribute 'optional_output' specifies a map which is used to select additional output
    # blocks according to the value of the special parameter 'show'.
    
    if ( my $map_name = $ds->node_attr($path, 'optional_output') )
    {
	if ( $map_name && ref $ds->{set}{$map_name} eq 'Web::DataService::Set' )
	{
	    my @optional_keys = $request->special_value('show');
	    
	    if ( @optional_keys )
	    {
		push @output_list, { map_name => $map_name, keys => \@optional_keys };
	    }
	}
    }
    
    # If any output blocks were specified by either of those two mechanisms, add them to the
    # default output configuration 'main'.
    
    $ds->add_output_blocks($request, 'main', @output_list) if @output_list;
    
    # At this point, we have added all of the blocks that can be determined from the operation
    # node and the 'show' parameter. The operation method can add more blocks, or can redo the
    # output configuration completely if it chooses.
}


sub init_output {

    my ($ds, $request, $config_name) = @_;
    
    my $configuration = { select_list => [],
			  select_hash => {},
			  tables_hash => {},
			  filter_hash => {},
			  proc_list => [],
			  field_list => [],
			  block_keys => {},
			  block_hash => {},
			  block_included => {} };
    
    bless $configuration, 'Web::DataService::OutputConfig';
    
    $request->{"output_${config_name}"} = $configuration;
}


sub select_output {

    my ($ds, $request, $config_name) = @_;
    
    my $config_key = "output_${config_name}";
    
    croak "unknown output configuration '$config_name'" unless
	ref $request->{$config_key} eq 'Web::DataService::OutputConfig';
    
    $request->{current_output} = $config_key;
    $request->{block_hash} = $request->{$config_key}{block_hash};
    $request->{extra_headers} = [ ];
}


sub add_header {
    
    my ($ds, $request, $name) = @_;

    # If the parameter is a block name, add it directly.
    
    if ( $ds->{block}{$name} )
    {
	push @{$request->{extra_headers}}, $name;
    }

    elsif ( ref $request->{"output_${name}"} eq 'Web::DataService::OutputConfig' )
    {
	push @{$request->{extra_headers}}, "output_${name}";
    }

    else
    {
	$request->add_warning("Unknown output block or configuration '$name'");
    }	
}


sub header_lists {

    my ($ds, $request) = @_;
    
    my @field_lists = $request->{$request->{current_output}}{field_list};
    
    if ( ref $request->{extra_headers} eq 'ARRAY' )
    {
	foreach my $name ( @{$request->{extra_headers}} )
	{
	    if ( $request->{block_field_list}{$name} )
	    {
		push @field_lists, $request->{block_field_list}{$name};
	    }

	    elsif ( $request->{$name}{field_list} )
	    {
		push @field_lists, $request->{$name}{field_list};
	    }

	    else
	    {
		$request->add_warning("Unknown header field list '$name'");
	    }
	}
    }

    return @field_lists;
}


sub map_output_blocks {
    
    my ($ds, $request, $config_name, $map_name, @keys) = @_;
    
    unless ( $request->{"output_${config_name}"} )
    {
	$request->add_warning("Unknown output configuration '$config_name'");
	return;
    }
    
    my $oc = $request->{"output_${config_name}"};

    unless ( ref $ds->{set}{$map_name} eq 'Web::DataService::Set' )
    {
	$request->add_warning("Unknown output map '$map_name'");
	return;
    }
    
    my $block_map = $ds->{set}{$map_name};
    my @mapped_blocks;
    
    foreach my $key ( @keys )
    {
	next unless defined $key;
	
	my $block_name = $block_map->{value}{$key}{maps_to};
	$oc->{block_keys}{$key} = 1;
	$oc->{block_hash}{$key} = 1;
	
	if ( $block_name && ref $ds->{block}{$block_name} eq 'Web::DataService::Block' )
	{
	    $oc->{block_hash}{$block_name} = $key;
	    push @mapped_blocks, $block_name;
	}
	
	elsif ( $block_name )
	{
	    $request->add_warning("Unknown output block '$block_name'");
	}
    }
    
    return @mapped_blocks;
}


sub add_output_blocks {

    my ($ds, $request, $config_name, @block_list) = @_;
    
    croak "unknown output configuration '$config_name'" unless
	$request->{"output_${config_name}"};
    
    my $oc = $request->{"output_${config_name}"};
    
    my @blocks;
    
    foreach my $block_spec ( @block_list )
    {
	if ( ref $block_spec eq 'HASH' )
	{
	    croak "Invalid hash argument to 'add_output_blocks', needs both 'map_name' and 'keys'" unless
		$block_spec->{map_name} && $block_spec->{keys} && ref $block_spec->{keys} eq 'ARRAY';
	    
	    push @blocks, $ds->map_output_blocks($request, $config_name, $block_spec->{map_name},
						 @{$block_spec->{keys}});
	}
	
	elsif ( ref $ds->{block}{$block_spec} eq 'Web::DataService::Block' )
	{
	    push @blocks, $block_spec;
	}
	
	else
	{
	    $request->add_warning("Output block '$block_spec' not found");
	}
    }
    
    # Then scan through the list of blocks and check for include_list entries.  This allows us to
    # know before the rest of the processing exactly which blocks are included.
    
    my $format = $request->output_format;
    my $vocab = $request->output_vocab;
    
    my $bound = 0;
    my @include_scan = @blocks;
    my %uniq_block = ();
    
 INCLUDE_BLOCK:
    while ( my $block = shift @include_scan )
    {
	# Make sure that each block is checked only once, and add a bounds
	# check to prevent a runaway loop.
	
	next if $uniq_block{$block}; $uniq_block{$block} = 1;
	next if ++$bound > 999;
	
	my $include_list = $ds->{block}{$block}{include_list};
	next unless $include_list && ref $include_list eq 'ARRAY';
	
      INCLUDE_RECORD:
	foreach my $r ( @$include_list )
	{
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

	    # Add the included block to the block_hash, so that if_block and not_block attributes
	    # in other output records can be properly evaluated.
	    
	    my $include_block = $r->{include};
	    
	    # Now add the specified key and block to the output hash, if they are defined. Add the
	    # block to the end of the scan list, so that it will be scanned for additional inclusions.
	    
	    $oc->{block_hash}{$include_block} = 1;
	    push @include_scan, $include_block;
	}
    }
    
    # Now run through all of the blocks we have identified and add each one to the specified
    # output configuration.
    
 BLOCK:
    foreach my $block_name (@blocks)
    {
	$ds->add_output_block($request, $config_name, $block_name);
    }
    
    my $a = 1;	# We can stop here when debugging
}


# add_output_block ( request, block_name )
# 
# Add the specified block to the specified output configuration for the specified request.

sub add_output_block {

    my ($ds, $request, $config_name, $block_name) = @_;
    
    # Make sure we have a proper output configuration.
    
    my $oc = $request->{"output_${config_name}"};
    
    croak "unknown output configuration '$oc'" unless ref $oc eq 'Web::DataService::OutputConfig';
    
    # Each given block can only be added to a given output configuration once.
    
    return if $oc->{block_included}{$block_name};
    $oc->{block_included}{$block_name} = 1;
    
    # If the specified block exists, add it to the specified output configuration. Otherwise,
    # output a warning.
    
    if ( ref $ds->{block}{$block_name}{output_list} eq 'ARRAY' )
    {
	$ds->_configure_block($request, $block_name, $oc);
    }
    
    else
    {
	carp "undefined output block '$block_name' for path '$request->{path}'\n";
	$request->add_warning("undefined output block '$block_name'");
    }
    
    my $a = 1;	# we can stop here when debugging
}


# configure_block ( request, block_name )
# 
# Given a block name, determine the list of output fields and proc fields
# (if any) that are defined for it.  This is used primarily to configure
# blocks referred to via 'sub_record' attributes.
# 
# These lists are stored under the keys 'block_proc_list' and
# 'block_field_list' in the request record.  If these have already been filled
# in for this block, do nothing.

sub configure_block {

    my ($ds, $request, $block_name) = @_;
    
    # Return immediately if the relevant lists have already been computed
    # and cached (even if they are empty).
    
    return 1 if exists $request->{block_field_list}{$block_name};
    
    # If no list is available, indicate this to the request object and return
    # false.  Whichever routine called us will be responsible for generating an
    # error or warning if appropriate.
    
    if ( ref $ds->{block}{$block_name}{output_list} eq 'ARRAY' )
    {
	$ds->_configure_block($request, $block_name);
	return 1;
    }
    
    else
    {
	$request->{block_field_list}{$block_name} = undef;
	$request->{block_proc_list}{$block_name} = undef;
	return;
    }
}


# _configure_block ( request, block_name, oc )
# 
# Go through the output list of the specified block and collect up the processing actions and
# output records into two separate lists. If $oc is specified, then store these lists
# under the corresponding output configuration. Otherwise, store them directly in the request
# under the keys 'block_field_list' and 'block_proc_list'.

sub _configure_block {

    my ($ds, $request, $block_name, $oc) = @_;
    
    # Start by determining the relevant attributes of the request and looking up the output list
    # for this block.
    
    my $format = $request->output_format;
    my $vocab = $request->output_vocab;
    my $require_vocab; $require_vocab = 1 if $vocab and not $ds->{vocab}{$vocab}{use_field_names};
    
    my @output_list = @{$ds->{block}{$block_name}{output_list}};
    
    # Allocate lists to hold the set of field and process records for the case when no output
    # configuration is specified.

    my (@field_list, @proc_list);
    
    # Go through each record in the output_list, throwing out the ones that don't
    # apply and assigning the ones that do to the field_list and proc_list.

    my $record_count = 0;
    
 RECORD:
    while ( my $r = shift @output_list )
    {
	$record_count++;
	
	# If we are configuring this block in a specific output configuration, evaluate dependency
	# on the set of blocks included in the configuration. For blocks that are configured
	# outside of an output configuration, any such dependencies are ignored.
	
	if ( $oc )
	{
	    next RECORD if $r->{if_block} and not check_set($r->{if_block}, $oc->{block_hash});
	    next RECORD if $r->{not_block} and check_set($r->{not_block}, $oc->{block_hash});
	}
	
	# Evaluate dependency on the output format
	
	next RECORD if $r->{if_format} and not check_value($r->{if_format}, $format);
	next RECORD if $r->{not_format} and check_value($r->{not_format}, $format);
	
	# Evaluate dependency on the vocabulary
	
	next RECORD if $r->{if_vocab} and not check_value($r->{if_vocab}, $vocab);
	next RECORD if $r->{not_vocab} and check_value($r->{not_vocab}, $vocab);
	
	# Now process the record according to its type, which is indicated by one of the keys
	# 'output', 'select', 'filter', 'set', 'check', 'include'. The define_block method
	# enforces that each record must have exactly one of these keys, with a nonempty value.
	
	# If the record type is 'output', add a record to the output field list. This will almost
	# always be the most common kind of record in any output block.
	
	if ( $r->{output} )
	{
	    # If the vocabulary selected for this request is required, then skip fields that
	    # do not include a field name in the required vocabulary.
	    
	    next RECORD if $require_vocab and not exists $r->{"${vocab}_name"};
	    
	    # The attributes 'name' (the output name) and 'field' (the raw field name) are both
	    # set to the value of the record type key by default.
	    
	    my $output_record = { field => $r->{output}, name => $r->{output} };
	    
	    # Now iterate through the rest of the keys in the record.
	    
	    my ($override_name, $override_value);
	    
	    foreach my $key ( keys %$r )
	    {
		# Each standard output field attribute is simply copied to the output_field
		# record.
		
		if ( $FIELD_KEY{$key} )
		{
		    $output_record->{$key} = $r->{$key};
		}
		
		# Any attribute that looks like <vocab>_name or <vocab>_value and matches the
		# vocabulary or format selected for this requests overrides the field name or field
		# value.
		
		elsif ( $key =~ qr{ ^ (\w+) _ (name|value) $ }x )
		{
		    if ( $1 eq $vocab || $1 eq $format )
		    {
			if ( $2 eq 'name' )
			{
			    $override_name = $r->{$key};
			}

			else
			{
			    $override_value = $r->{$key};
			}
 		    } 
		}
		
		# Add a warning about any unrecognized key.
		
		elsif ( $key ne 'output' )
		{
		    carp "unknown key '$key' in output record in block '$block_name'";
		}
		
		# If this block has a sub-record definition, make sure the subsidiary
		# output block is configured too.
		
		if ( $r->{sub_record} )
		{
		    $ds->configure_block($request, $r->{sub_record});
		}
	    }
	    
	    # If the key and/or value were overridden, apply that now.
	    
	    $output_record->{name} = $override_name if $override_name;
	    $output_record->{value} = $override_value if defined $override_value;
	    
	    # If the record specifies a data type, add a process record to check the field value
	    # against that data type.
	    
	    if ( my $type_value = $r->{data_type} )
	    {
		carp "unknown value '$r->{data_type}' for data_type: must be one of 'int', 'pos', 'dec', 'str', 'mix'"
		    unless $DATA_TYPE{$type_value};

		unless (  $r->{data_type} eq 'str' || $r->{data_type} eq 'mix' )
		{
		    my $check = { check_field => $r->{output}, data_type => $r->{data_type} };
		    $check->{bad_value} = $r->{bad_value} if defined $r->{bad_value};
		    
		    if ( $oc )
		    {
			push @{$oc->{proc_list}}, $check;
		    }
		    
		    else
		    {
			push @proc_list, $check;
		    }
		}
	    }
	    
	    # Now add the record to the proper field list.
	    
	    if ( $oc )
	    {
		push @{$oc->{field_list}}, $output_record;
	    }
	    
	    else
	    {
		push @field_list, $output_record;
	    }
	}
	
	# If the record type is 'select' add this record's information to the selection list and
	# the tables hash. But ignore this record if no output configuration was specified.
	
	elsif ( $r->{select} )
	{
	    next RECORD unless $oc;
	    
	    croak "value of 'select' must be a string or array"
		if ref $r->{select} && ref $r->{select} ne 'ARRAY';
	    
	    my @select = ref $r->{select} ? @{$r->{select}}
		: split qr{\s*,\s*}, $r->{select};
	    
	    foreach my $s ( @select )
	    {
		next if exists $oc->{select_hash}{$s};
		$oc->{select_hash}{$s} = 1;
		push @{$oc->{select_list}}, $s;
	    }
	    
	    if ( $r->{tables} )
	    {
		croak "value of 'tables' must be a string or array"
		    if ref $r->{tables} && ref $r->{tables} ne 'ARRAY';
		
		my @tables = ref $r->{tables} ? @{$r->{tables}}
		    : split qr{\s*,\s*}, $r->{tables};
		
		foreach my $t ( @tables )
		{
		    $oc->{tables_hash}{$t} = 1;
		}
	    }
	    
	    foreach my $k ( keys %$r )
	    {
		warn "ignored invalid key '$k' in 'select' record"
		    unless $SELECT_KEY{$k};
	    }
	}
	
	# If the record type is 'filter', add to the filter hash. But ignore this record if no
	# output configuration was specified. 
	
	elsif ( defined $r->{filter} )
	{
	    next RECORD unless $oc;
	    
	    $oc->{filter_hash}{$r->{filter}} = $r->{value};
	}
	
	# If the record type is 'set', add a record to the proc list.
	
	elsif ( $r->{set} )
	{
	    my $set_record = { set => $r->{set} };
	    
	    foreach my $key ( keys %$r )
	    {
		if ( $PROC_KEY{$key} )
		{
		    $set_record->{$key} = $r->{$key};
		}
		
		else
		{
		    carp "Warning: unknown key '$key' in proc record\n";
		}
	    }

	    if ( $oc )
	    {
		push @{$oc->{proc_list}}, $set_record;
	    }
	    
	    else
	    {
		push @proc_list, $set_record;
	    }
	}
	
	# If the record type is 'include', then add the specified records to the list immediately.
	# If no 'include_block' was specified, that means that the specified key did not
	# correspond to any block.  So we can ignore it in that case.  For now, we will also
	# ignore 'include' records if no output context was specified.
	
	elsif ( $r->{include} )
	{
	    next RECORD unless $oc;
	    
	    # If we have already processed this block, then skip it.  A
	    # block can only be included once per request.  If we haven't
	    # processed it yet, mark it so that it will be skipped if it
	    # comes up again.
	    
	    my $include_block = $r->{include};
	    next RECORD if $oc->{has_block}{$include_block};
	    
	    # Get the list of block records, or add a warning if no block
	    # was defined under that name.
	    
	    my $add_list = $ds->{block}{$include_block}{output_list};
	    
	    unless ( ref $add_list eq 'ARRAY' )
	    {
		warn "undefined output block '$include_block' for path '$request->{path}'\n";
		$request->add_warning("undefined output block '$include_block'");
		next RECORD;
	    }
	    
	    # Now add the included block's records to the front of the
	    # record list.
	    
	    unshift @output_list, @$add_list;
	}
	
	# All other record types throw an error.

	else
	{
	    croak "bad record $record_count in block '$block_name'";
	}
    }
    
    # If no output configuration was specified, save the field_list and proc_list separately under
    # the request.

    unless ( $oc )
    {
	$request->{block_field_list}{$block_name} = \@field_list;
	$request->{block_proc_list}{$block_name} = \@proc_list;
    }
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

    elsif ( ref $list eq 'HASH' )
    {
	return 1 if $list->{$value};
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


# add_doc ( node, item )
# 
# Add the specified item to the documentation list for the specified node.
# The item can be either a string or a record (hashref).

sub add_doc {

    my ($ds, $node, $item) = @_;
    
    # If the item is a record, close any currently pending documentation and
    # start a new "pending" list.  We need to do this because subsequent items
    # may document the record we were just called with.
    
    if ( ref $item )
    {
	croak "cannot add non-hash object to documentation"
	    unless reftype $item eq 'HASH';
	
	$ds->process_doc($node);
	push @{$node->{doc_pending}}, $item;
    }
    
    # If this is a string starting with one of the special characters, then
    # handle it properly.
    
    elsif ( $item =~ qr{ ^ ([!^?] | >>?) (.*) }xs )
    {
	# If >>, then close the active documentation section (if any) and
	# start a new one that is not tied to any rule.  This will generate an
	# ordinary paragraph starting with the remainder of the line.
		
	if ( $1 eq '>>' )
	{
	    $ds->process_doc($node);
	    push @{$node->{doc_pending}}, $2 if $2 ne '';
	}
	
	# If >, then add to the current documentation a blank line
	# (which will cause a new paragraph) followed by the remainder
	# of this line.
	
	elsif ( $1 eq '>' )
	{
	    push @{$node->{doc_pending}}, "\n$2";
	}
	
	# If !, then discard all pending documentation and mark the node as
	# 'undocumented'.  This will cause it to be elided from the documentation.
	
	elsif ( $1 eq '!' )
	{
	    $ds->process_doc($node, 'undocumented');
	}
	
	# If ?, then add the remainder of the line to the documentation.
	# The ! prevents the next character from being interpreted specially.
	
	else
	{
	    push @{$node->{doc_pending}}, $2;
	}
    }
    
    # Otherwise, just add this string to the "pending" list.
    
    else
    {
	push @{$node->{doc_pending}}, $item;
    }
}


# process_doc ( node, disposition )
# 
# Process all pending documentation items.

sub process_doc {

    my ($ds, $node, $disposition) = @_;
    
    # Return immediately unless we have something pending.
    
    return unless ref $node->{doc_pending} eq 'ARRAY' && @{$node->{doc_pending}};
    
    # If the "pending" list starts with an item record, take that off first.
    # Everything else on the list should be a string.
    
    my $primary_item = shift @{$node->{doc_pending}};
    return unless ref $primary_item;
    
    # Discard all pending documentation if the primary item is disabled or
    # marked with a '!'.  In the latter case, note this in the item record.
    
    $disposition //= '';
    
    if ( $primary_item->{disabled} or $primary_item->{undocumented} or
	 $disposition eq 'undocumented' )
    {
	@{$node->{doc_pending}} = ();
	$primary_item->{undocumented} = 1 if $disposition eq 'undocumented';
	return;
    }
    
    # Put the rest of the documentation items together into a single
    # string, which may contain a series of Pod paragraphs.
    
    my $body = '';
    my $last_pod;
    my $this_pod;
    
    while (my $line = shift @{$node->{doc_pending}})
    {
	# If this line starts with =, then it needs extra spacing.
	
	my $this_pod = $line =~ qr{ ^ = }x;
	
	# If $body already has something in it, add a newline first.  Add
	# two if this line starts with =, or if the previously added line
	# did, so that we get a new paragraph.
	
	if ( $body ne '' )
	{
	    $body .= "\n" if $last_pod || $this_pod;
	    $body .= "\n";
	}
	
	$body .= $line;
	$last_pod = $this_pod;
    }
    
    # Then add the documentation to the node's documentation list.  If there
    # is no primary item, add the body as an ordinary paragraph.
    
    unless ( defined $primary_item )
    {
	push @{$node->{doc_list}}, clean_doc($body);
    }
    
    # Otherwise, attach the body to the primary item and add it to the list.
    
    else
    {
	$primary_item->{doc_string} = clean_doc($body, 1);
	push @{$node->{doc_list}}, $primary_item;
    }
}


# clean_doc ( )
# 
# Make sure that the indicated string is valid POD.  In particular, if there
# are any unclosed =over sections, close them at the end.  Throw an exception
# if we find an =item before the first =over or a =head inside an =over.

sub clean_doc {

    my ($docstring, $item_body) = @_;
    
    my $list_level = 0;
    
    while ( $docstring =~ / ^ (=[a-z]+) /gmx )
    {
	if ( $1 eq '=over' )
	{
	    $list_level++;
	}
	
	elsif ( $1 eq '=back' )
	{
	    $list_level--;
	    croak "invalid POD string: =back does not match any =over" if $list_level < 0;
	}
	
	elsif ( $1 eq '=item' )
	{
	    croak "invalid POD string: =item outside of =over" if $list_level == 0;
	}
	
	elsif ( $1 eq '=head' )
	{
	    croak "invalid POD string: =head inside =over" if $list_level > 0 || $item_body;
	}
    }
    
    $docstring .= "\n\n=back" x $list_level;
    
    return $docstring;
}


# document_node ( node, state )
# 
# Return a documentation string for the given node, in Pod format.  This will
# consist of a main item list that may start and stop, possibly with ordinary
# Pod paragraphs in between list chunks.  If this node contains any 'include'
# records, the lists for those nodes will be recursively interpolated into the
# main list.  Sublists can only occur if they are explicitly included in the
# documentation strings for individual node records.
# 
# If the $state parameter is given, it must be a hashref containing any of the
# following keys:
# 
# namespace	A hash ref in which included nodes may be looked up by name.
#		If this is not given, then 'include' records are ignored.
# 
# items_only	If true, then ordinary paragraphs will be ignored and a single
#		uninterrupted item list will be generated.
# 

sub document_node {
    
    my ($ds, $node, $state) = @_;
    
    # Return the empty string unless documentation has been added to this
    # node. 
    
    return '' unless ref $node && ref $node->{doc_list} eq 'ARRAY';
    
    # Make sure we have a state record, if we were not passed one.
    
    $state ||= {};
    
    # Make sure that we process each node only once, if it should happen
    # to be included multiple times.  Also keep track of our recursion level.
    
    return if $state->{processed}{$node->{name}};
    
    $state->{processed}{$node->{name}} = 1;
    $state->{level}++;
    
    # Go through the list of documentation items, treating each one as a Pod
    # paragraph.  That means that they will be separated from each other by a
    # blank line.  List control paragraphs "=over" and "=back" will be added
    # as necessary to start and stop the main item list.
    
    my $doc = '';
    
 ITEM:
    foreach my $item ( @{$node->{doc_list}} )
    {
	# A string is added as an ordinary paragraph.  The main list is closed
	# if it is open.  But the item is skipped if we were given the
	# 'items_only' flag.
	
	unless ( ref $item )
	{
	    next ITEM if $state->{items_only};
	    
	    if ( $state->{in_list} )
	    {
		$doc .= "\n\n" if $doc ne '';
		$doc .= "=back";
		$state->{in_list} = 0;
	    }
	    
	    $doc .= "\n\n" if $doc ne '' && $item ne '';
	    $doc .= $item;
	}
	
	# An 'include' record inserts the documentation for the specified
	# node.  This does not necessarily end the list, only if the include
	# record itself has a documentation string.  Skip the inclusion if no
	# hashref was provided for looking up item names.
	
	elsif ( defined $item->{include} )
	{
	    next ITEM unless ref $state->{namespace} && reftype $state->{namespace} eq 'HASH';
	    
	    if ( defined $item->{doc_string} and $item->{doc_string} ne '' and not $state->{items_only} )
	    {
		if ( $state->{in_list} )
		{
		    $doc .= "\n\n" if $doc ne '';
		    $doc .= "=back";
		    $state->{in_list} = 0;
		}
		
		$doc .= "\n\n" if $doc ne '';
		$doc .= $item->{doc_string};
	    }
	    
	    my $included_node = $state->{namespace}{$item->{include}};
	    
	    next unless ref $included_node && reftype $included_node eq 'HASH';
	    
	    my $subdoc = $ds->document_node($included_node, $state);
	    
	    $doc .= "\n\n" if $doc ne '' && $subdoc ne '';
	    $doc .= $subdoc;
	}
	
	# Any other record is added as a list item.  Try to figure out the
	# item name as best we can.
	
	else
	{
	    my $name = ref $node eq 'Web::DataService::Set' ? $item->{value}
		     : defined $item->{name}		    ? $item->{name}
							    : '';
	    
	    $name ||= '';
	    
	    unless ( $state->{in_list} )
	    {
		$doc .= "\n\n" if $doc ne '';
		$doc .= "=over";
		$state->{in_list} = 1;
	    }
	    
	    $doc .= "\n\n=item $name";
	    $doc .= "\n\n$item->{doc_string}" if defined $item->{doc_string} && $item->{doc_string} ne '';
	}
    }
    
    # If we get to the end of the top-level ruleset and we are still in a
    # list, close it.  Also make sure that our resulting documentation string
    # ends with a newline.
    
    if ( --$state->{level} == 0 )
    {
	$doc .= "\n\n=back" if $state->{in_list};
	$state->{in_list} = 0;
	$doc .= "\n";
    }
    
    return $doc;
}


# document_response ( )
# 
# Generate documentation in Pod format describing the available output fields
# for the specified URL path.

sub document_response {
    
    my ($ds, $path) = @_;
    
    my @blocks;
    my @labels;
    
    # First collect up a list of all of the fixed (non-optional) blocks.
    # Block names that do not correspond to any defined block are ignored,
    # with a warning.
    
    my $output_list = $ds->node_attr($path, 'output') // [ ];
    my $fixed_label = $ds->node_attr($path, 'output_label') // 'basic';
    
    foreach my $block_name ( @$output_list )
    {
	if ( ref $ds->{block}{$block_name} eq 'Web::DataService::Block' )
	{
	    push @blocks, $block_name;
	    push @labels, $fixed_label;
	}
	
	elsif ( $ds->debug )
	{
	    warn "WARNING: block '$block_name' not found"
		unless $Web::DataService::QUIET || $ENV{WDS_QUIET};
	}
    }
    
    # Then add all of the optional blocks, if an output_opt map was
    # specified.
    
    my $optional_output = $ds->node_attr($path, 'optional_output');
    my $reverse_map;
    
    if ( $optional_output && ref $ds->{set}{$optional_output} eq 'Web::DataService::Set' )
    {
	my $output_map = $ds->{set}{$optional_output};
	my @keys; @keys = @{$output_map->{value_list}} if ref $output_map->{value_list} eq 'ARRAY';
	
    VALUE:
	foreach my $label ( @keys )
	{
	    my $block_name = $output_map->{value}{$label}{maps_to};
	    next VALUE unless defined $block_name;
	    next VALUE if $output_map->{value}{$label}{disabled} || 
		$output_map->{value}{$label}{undocumented};
	    
	    $reverse_map->{$block_name} = $label;
	    
	    if ( ref $ds->{block}{$block_name} eq 'Web::DataService::Block' )
	    {
		push @blocks, $block_name;
		push @labels, $label;
	    }
	}
    }
    
    elsif ( $optional_output && $ds->debug )
    {
	warn "WARNING: output map '$optional_output' not found"
	    unless $Web::DataService::QUIET || $ENV{WDS_QUIET};
    }
    
    # If there are no output blocks specified for this path, return an empty
    # string.
    
    return '' unless @blocks;
    
    # Otherwise, determine the set of vocabularies that are allowed for this
    # path.  If none are specifically selected for this path, then all of the
    # vocabularies defined for this data service are allowed.
    
    my $vocabularies; $vocabularies = $ds->node_attr($path, 'allow_vocab') || $ds->{vocab};	
    
    unless ( ref $vocabularies eq 'HASH' && keys %$vocabularies )
    {
	warn "No output vocabularies were selected for path '$path'" if $ds->debug;
	return '';
    }
    
    my @vocab_list = grep { $vocabularies->{$_} && 
			    ref $ds->{vocab}{$_} &&
			    ! $ds->{vocab}{$_}{disabled} } @{$ds->{vocab_list}};
    
    unless ( @vocab_list )
    {
	warn "No output vocabularies were selected for path '$path'" if $ds->debug;
	return "";
    }
    
    # Now generate the header for the documentation, in Pod format.  We
    # include the special "=for wds_table_header" line to give PodParser.pm the
    # information it needs to generate an HTML table.
    
    my $doc_string = '';
    my $field_count = scalar(@vocab_list);
    my $field_string = join ' / ', @vocab_list;
    
    if ( $field_count > 1 )
    {
	$doc_string .= "=for wds_table_header Field name*/$field_count | Block | Description\n\n";
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=item $field_string\n\n";
    }
    
    else
    {
	$doc_string .= "=for wds_table_header Field name* | Block | Description\n\n";
	$doc_string .= "=over 4\n\n";
    }
    
    # Run through each block one at a time, documenting all of the fields in
    # the corresponding field list.
    
    my %uniq_block;
    
    foreach my $i (0..$#blocks)
    {
	my $block_name = $blocks[$i];
	my $block_label = $labels[$i];
	
	# Make sure to only process each block once, even if it is listed more
	# than once.
	
	next if $uniq_block{$block_name}; $uniq_block{$block_name} = 1;
	
	my $output_list = $ds->{block}{$block_name}{output_list};
	next unless ref $output_list eq 'ARRAY';
	
	foreach my $r (@$output_list)
	{
	    next unless defined $r->{output};
	    $doc_string .= $ds->document_field($block_label, \@vocab_list, $r, $reverse_map)
		unless $r->{undocumented};
	}
    }
    
    $doc_string .= "\n=back\n\n";
    
    return $doc_string;
}


sub document_summary {

    my ($ds, $path) = @_;
    
    # Return the empty string unless a summary block was defined for this path.
    
    my $summary_block = $ds->node_attr($path, 'summary');
    return '' unless $summary_block;
    
    # Otherwise, determine the set of vocabularies that are allowed for this
    # path.  If none are specifically selected for this path, then all of the
    # vocabularies defined for this data service are allowed.
    
    my $vocabularies; $vocabularies = $ds->node_attr($path, 'allow_vocab') || $ds->{vocab};	
    
    unless ( ref $vocabularies eq 'HASH' && keys %$vocabularies )
    {
	return '';
    }
    
    my @vocab_list = grep { $vocabularies->{$_} && 
			    ref $ds->{vocab}{$_} &&
			    ! $ds->{vocab}{$_}{disabled} } @{$ds->{vocab_list}};
    
    unless ( @vocab_list )
    {
	return "";
    }
    
    # Now generate the header for the documentation, in Pod format.  We
    # include the special "=for wds_table_header" line to give PodParser.pm the
    # information it needs to generate an HTML table.
    
    my $doc_string = '';
    my $field_count = scalar(@vocab_list);
    my $field_string = join ' / ', @vocab_list;
    
    if ( $field_count > 1 )
    {
	$doc_string .= "=for wds_table_header Field name*/$field_count | Block | Description\n\n";
	$doc_string .= "=over 4\n\n";
	$doc_string .= "=item $field_string\n\n";
    }
    
    else
    {
	$doc_string .= "=for wds_table_header Field name* | Block | Description\n\n";
	$doc_string .= "=over 4\n\n";
    }
    
    # Now determine the summary output list.
    
    my $output_list = $ds->{block}{$summary_block}{output_list};
    return '' unless ref $output_list eq 'ARRAY';
    
    foreach my $r (@$output_list)
    {
	next unless defined $r->{output};
	$doc_string .= $ds->document_field('summary', \@vocab_list, $r, {})
	    unless $r->{undocumented};
    }
    
    $doc_string .= "\n=back\n\n";
    
    return $doc_string;
}


sub document_field {
    
    my ($ds, $block_key, $vocab_list, $r, $rev_map) = @_;
    
    my @names;
    
    foreach my $v ( @$vocab_list )
    {
	my $n = defined $r->{"${v}_name"}	    ? $r->{"${v}_name"}
	      : defined $r->{name}		    ? $r->{name}
	      : $ds->{vocab}{$v}{use_field_names} ? $r->{output}
	      :					      '';
	
	$n ||= 'I<n/a>';
	
	push @names, $n
    }
    
    my $names = join ' / ', @names;
    
    my $descrip = $r->{doc_string} || "";
    
    if ( defined $r->{if_block} )
    {
	if ( ref $r->{if_block} eq 'ARRAY' )
	{
	    $block_key = join(', ', map { $rev_map->{$_} // $_ } @{$r->{if_block}});
	}
	else
	{
	    $block_key = $rev_map->{$r->{if_block}} // $r->{if_block};
	}
    }
    
    my $line = "\n=item $names ( $block_key )\n\n$descrip\n";
    
    return $line;
}


# process_record ( request, record, steps )
# 
# Execute any per-record processing steps that have been defined for this
# record. Return true if the record is to be included in the result, false
# otherwise. $$$ start here

sub process_record {
    
    my ($ds, $request, $record, $steps) = @_;
    
    # If there are no processing steps to do, return immediately.
    
    return 1 unless ref $steps eq 'ARRAY' and @$steps;
    
    # Otherwise go through the steps one by one.
    
    foreach my $p ( @$steps )
    {
	# Skip this processing step based on a conditional field value, if one
	# is defined.
	
	if ( my $cond_field = $p->{if_field} )
	{
	    next unless defined $record->{$cond_field};
	    next if ref $record->{$cond_field} eq 'ARRAY' && @{$record->{$cond_field}} == 0;
	}
	
	elsif ( $cond_field = $p->{not_field} )
	{
	    next if defined $record->{$cond_field} && ref $record->{$cond_field} ne 'ARRAY';
	    next if ref $record->{$cond_field} eq 'ARRAY' && @{$record->{$cond_field}} > 0;
	}
	
	# If this step is a 'check_field' step, then do the check.
	
	if ( defined $p->{check_field} )
	{
	    $ds->check_field_type($record, $p->{check_field}, $p->{data_type}, $p->{bad_value});
	    next;
	}
	
	# If we get here, the current rule must be a 'set'.  Figure out which
	# field (if any) we are affecting.  A value of '*' means to use the
	# entire record (only relevant with 'code').
	
	my $set_field = $p->{set};
	
	# Figure out which field (if any) we are looking at.  Skip this
	# processing step if the source field is empty, unless the attribute
	# 'always' is set.
	
	my $source_field = $p->{from} || $p->{from_each} || $p->{set};
	
	# Skip any processing step if the record does not have a non-empty
	# value in the corresponding field (unless the 'always' attribute is
	# set).
	
	if ( $source_field && $source_field ne '*' && ! $p->{always} )
	{
	    next unless defined $record->{$source_field};
	    next if ref $record->{$source_field} eq 'ARRAY' && @{$record->{$source_field}} == 0;
	}
	
	# Now generate a list of result values, according to the attributes of this
	# processing step.
	
	my @result;
	
	# If we have a 'code' attribute, then call it.
	
	if ( ref $p->{code} eq 'CODE' )
	{
	    if ( $source_field eq '*' )
	    {
		@result = $p->{code}($request, $record);
	    }
	    
	    elsif ( $p->{from_each} )
	    {
		@result = map { $p->{code}($request, $_) } 
		    (ref $record->{$source_field} eq 'ARRAY' ? 
		     @{$record->{$source_field}} : $record->{$source_field});
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{code}($request, $record->{$source_field});
	    }
	    
	    else
	    {
		@result = $p->{code}($request, $record->{$set_field});
	    }
	}
	
	# If we have a 'lookup' attribute, then use it.
	
	elsif ( ref $p->{lookup} eq 'HASH' )
	{
	    if ( $p->{from_each} )
	    {
		if ( ref $record->{$source_field} eq 'ARRAY' )
		{
		    @result = map { $p->{lookup}{$_} // $p->{default} } @{$record->{$source_field}};
		}
		elsif ( ! ref $record->{$source_field} )
		{
		    @result = $p->{lookup}{$record->{$source_field}} // $p->{default};
		}
	    }
	    
	    elsif ( $p->{from} )
	    {
		@result = $p->{lookup}{$record->{$source_field}} // $p->{default}
		    unless ref $record->{$source_field};
	    }
	    
	    elsif ( $set_field ne '*' && ! ref $record->{$set_field} )
	    {
		@result = $p->{lookup}{$record->{$set_field}} // $p->{default} if defined $record->{$set_field};
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
		    if defined $record->{$source_field} && ! ref $record->{$source_field};
	    }
	    
	    elsif ( $set_field ne '*' )
	    {
		@result = split $p->{split}, $record->{$set_field}
		    if defined $record->{$set_field} && ! ref $record->{$set_field};
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
	
	# Otherwise, we just use the vaoue of the source field.
	
	else
	{
	    @result = ref $record->{$source_field} eq 'ARRAY' ?
		@{$record->{$source_field}} : $record->{$source_field};
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
    
    return 1;
}


# check_field_type ( record, field, type, subst )
# 
# Make sure that the specified field matches the specified data type.  If not,
# substitute the specified value.

sub check_field_type {

    my ($ds, $record, $field, $type, $subst) = @_;
    
    return unless defined $record->{$field};
    
    # If the data type is 'int', make sure that the value looks like an integer. Remove any leading
    # zeros before checking the format.
    
    if ( $type eq 'int' )
    {
	if ( $record->{$field} =~ qr{ ^ -? 0 }xs )
	{
	    $record->{$field} =~ s{ ^ (-?) 0+ }{$1}xs;
	    $record->{$field} = '0' if $record->{$field} =~ qr{ ^ (?: -0 | - ) $ }xs;
	}
	
	return if $record->{$field} =~ qr{ ^ -? (?: [1-9] [0-9]* | 0 ) $ }xs;
    }
    
    # If the data type is 'pos', make sure that the value looks like a nonnegative integer. Zero is
    # allowed, but leading zeros are always removed.
    
    elsif ( $type eq 'pos' )
    {
	if ( $record->{$field} =~ qr{ ^ 0 }xs )
	{
	    $record->{$field} =~ s{ ^ 0+ }{}xs;
	    $record->{$field} ||= '0';
	}
	
	return if $record->{$field} =~ qr{ ^ (?: [1-9][0-9]* | 0 ) $ }xs;
    }
    
    # If the data type is 'dec', make sure that the value looks like a decimal number. As above,
    # leading zeros are always removed.
    
    elsif ( $type eq 'dec' )
    {
	if ( $record->{$field} =~ qr{ ^ -? 0 [0-9] }xs )
	{
	    $record->{$field} =~ s{ ^ (-?) 0+ ([.]?) }{$2 ? $1 . '0.' : $1}xse;
	    $record->{$field} = '0' if $record->{$field} =~ qr{ ^ (?: -0 | -0. | 0. ) $ }xs;
	}
	
	return if $record->{$field} =~ qr< ^ -? (?: [1-9][0-9]* (?: \. [0-9]* )? | [0]? \. [0-9]+ | [0] \.? ) $ >x;
    }

    # If the data type is 'sci', make sure the value looks like a number in scientific notation.
    
    elsif ( $type eq 'sci' )
    {
	if ( $record->{$field} =~ qr{ ^ -? 0+ [1-9] }xs )
	{
	    $record->{$field} =~ s{ ^ (-?) 0+ ([.]?) }{$2 ? $1 . '0.' : $1}xse;
	    $record->{$field} = '0' if $record->{$field} =~ qr{ ^ (?: -0 | -0. | 0. ) $ }xs;
	}
	
	return if $record->{$field} =~ qr{ ^ -? (?: [1-9][0-9]* \. [0-9]* | [0]? \. [0-9]+ | [0] \. ) (?: [eE] -? [1-9][0-9]* ) $ }x;
    }
    
    # If the data type is 'json', make sure the result is actually a valid JSON string. The
    # 'subst' attribute can be used to substitute an empty object or some other error indicator if
    # not.
    
    elsif ( $type eq 'json' )
    {
	my $data;
	eval { $data = JSON::decode_json($record->{$field}) };
	
	return if defined $data;
    }
    
    # If the data type is something we don't recognize, don't do any check.
    
    else
    {
	return;
    }
    
    # If we get here, then the value failed the test.  If we were given a
    # replacement value, substitute it.  Otherwise, just delete the field.
    
    if ( defined $subst )
    {
	$record->{$field} = $subst;
    }
    
    else
    {
	delete $record->{$field};
    }
}


# output_to_file ( filehandle, response_hook )
# 
# If the response_hook argument is given, it must be either a method name or a code ref. All
# output is directed to the specified filehandle, and the response_hook is called to generate the
# response content. If no response_hook, is given, then output is copied to the filehandle in
# addition to being returned to the client.
#
# In either case, the first argument must be a filehandle that is open for writing.

sub output_to_file {
    
    my ($ds, $request, $filehandle, $response_hook) = @_;
    
    # Store the filehandle as an attribute of the request.
    
    $request->{out_fh} = $filehandle;
    
    # If an output character set is defined, add a the proper layer to the filehandle.
    
    if ( my $charset = $ds->{_config}{charset} )
    {
	my $layer = $charset =~ /^utf-?8$/ ? ":utf8" : ":encoding($charset)";
	binmode($request->{out_fh}, $layer);
    }

    # If $response_hook is defined and is a code ref, store it as an attribute of the request. 
    
    if ( $response_hook )
    {
	croak "response_hook parameter is not a code ref\n" unless ref $response_hook eq 'CODE';
	$request->{response_hook} = $response_hook;
	$request->{file_only} = 1;
    }
    
    # Otherwise, indicate that we are duplicating the output.
    
    else
    {
	$request->{tee_output} = 1;
    }
}


# _check_output_config ( request )
#
# If the current output configuration has no output fields, add a warning.

sub _check_output_config {
    
    my ($ds, $request) = @_;
    
    my $config_key = $request->{current_output};
    
    unless ( $request->{$config_key}{field_list} && @{$request->{$config_key}{field_list}} )
    {
	$request->add_warning("No output blocks were specified for this operation.");
    }
}


# _generate_single_result ( request )
# 
# This function is called after an operation is executed and returns a single
# record.  Return this record formatted as a single string according to the
# specified output format.

sub _generate_single_result {

    my ($ds, $request) = @_;
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->output_format;
    my $format_class = $ds->{format}{$format}{package};
    
    die "could not generate a result in format '$format': no implementing module was found"
	unless $format_class;
    
    my $path = $request->node_path;
    
    # Set the result count to 1, in case the client asked for it.
    
    $request->{result_count} = 1;
    
    # Get the output configuration to be used for this record.

    my $oc = $request->{$request->{current_output}};
    
    # Get the lists that specify how to process each record and which fields
    # to output.
    
    my $proc_list = $oc->{proc_list};
    my $field_list = $oc->{field_list};
    
    # Make sure we have at least one field to output.
    
    unless ( ref $field_list && @$field_list )
    {
	$request->add_warning("No output fields were defined for this request.");
    }

    # Mark this request as a single-result request, so that the format class can produce the
    # proper headers and footers.
    
    $request->{is_single_result} = 1;
    
    # If there is a before_record_hook defined for this path, call it now. For a single result,
    # calls to 'skip_output_record' are not allowed.
    
    $ds->_call_hooks($request, 'before_record_hook', $request->{main_record})
	if $request->{hook_enabled}{before_record_hook};
    
    # If there are any processing steps to do, then do them.
    
    $ds->process_record($request, $request->{main_record}, $proc_list);
    
    # Generate the initial part of the output, before the first record.
    
    my $header = $format_class->emit_header($request, $field_list);
    
    # Generate the output corresponding to our single record.
    
    my $record = $format_class->emit_record($request, $request->{main_record}, $field_list);
    
    # Generate the final part of the output, after the last record.
    
    my $footer = $format_class->emit_footer($request, $field_list);

    # If an after_serialize_hook is defined for this path, call it.
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	my $rs = '';
	
	$ds->_call_hooks($request, 'after_serialize_hook', 'header', \$header);
	$ds->_call_hooks($request, 'after_serialize_hook', 'record', \$rs, \$record);
	$ds->_call_hooks($request, 'after_serialize_hook', 'footer', \$footer);
    }
    
    return $header . $record . $footer;
}


# _generate_compound_result ( request )
# 
# This function is called after an operation is executed and returns a result
# set, provided that the entire result set does not need to be processed
# before output.  It serializes each result record according to the specified output
# format and returns the resulting string.  If $streaming_threshold is
# specified, and if the size of the output exceeds this threshold, this
# routine then sets up to stream the rest of the output.

sub _generate_compound_result {

    my ($ds, $request, $streaming_threshold) = @_;
    
    # Dancer::error "Generating compound result\n";
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->output_format;
    my $format_class = $ds->{format}{$format}{package};
    
    die "could not generate a result in format '$format': no implementing module was found"
	unless $format_class;
    
    my $path = $request->node_path;
    my $serial_hook = $ds->{hook_enabled}{after_serialize_hook} && $ds->node_attr($path, 'after_serialize_hook');
    
    # Get the output configuration to use
    
    # If we have an explicit result list, then we know the count.
    
    $request->{result_count} = scalar(@{$request->{main_result}})
	if ref $request->{main_result};
    
    # Get the list of fields for the header. There may be more than one, if add_header has been
    # called.
    
    my @header_lists = $ds->header_lists($request);;
    
    # Generate the initial part of the output, before the first record.
    
    my $output = $format_class->emit_header($request, @header_lists);
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	$ds->_call_hooks($request, 'after_serialize_hook', 'header', \$output);	
    }
    
    # A record separator is emitted before every record except the first.  If
    # this format class does not define a record separator, use the empty
    # string.
    
    $request->{rs} = $format_class->can('emit_separator') ?
	$format_class->emit_separator($request) : '';
    
    my $emit_rs = 0;
    
    $request->{actual_count} = 0;
    
    # If we have a result limit of 0, just output the header and footer and
    # don't bother about the records.
    
    if ( defined $request->{result_limit} && $request->{result_limit} eq '0' )
    {
	$request->{limit_zero} = 1;
    }
    
    # Otherwise, if an offset was specified and the result method didn't
    # handle this itself, then skip the specified number of records.
    
    elsif ( defined $request->{result_offset} && $request->{result_offset} > 0
	 && ! $request->{offset_handled} )
    {
	foreach (1..$request->{result_offset})
	{
	    $ds->_next_record($request) or last;
	}
    }
    
    # Now fetch and process each output record in turn.  If output streaming is
    # available and our total output size exceeds the threshold, switch over
    # to streaming.
    
 RECORD:
    while ( my $record = $ds->_next_record($request) )
    {
	my $oc = $record->{_output_config} ? $request->{$record->{_output_config}}
	    : $request->{$request->{current_output}};
	
	my $proc_list = $oc->{proc_list};
	my $field_list = $oc->{field_list};
	
	# If there is a before_record_hook defined for this path, call it now.
	
	if ( $request->{hook_enabled}{before_record_hook} )
	{
	    $ds->_call_hooks($request, 'before_record_hook', $record);
	}
	
	# If 'skip_output_record' was called on this record, then skip it now. If
	# 'select_output_block' was called, then substitute the field list and proc list associated
	# with that block.

	next RECORD if $record->{_skip_record};
	
	if ( my $alt = $record->{_output_block} )
	{
	    $proc_list = $request->{block_proc_list}{$alt};
	    $field_list = $request->{block_field_list}{$alt};
	}
	
	# If there are any processing steps to do, then process this record.
	
	$ds->process_record($request, $record, $proc_list);
	
	# Generate the output for this record, preceded by a record separator if
	# it is not the first record.
	
	my $outrs = $emit_rs ? $request->{rs} : ''; $emit_rs = 1;
	my $outrec = $format_class->emit_record($request, $record, $field_list);
	
	if ( $request->{hook_enabled}{after_serialize_hook} )
	{
	    $ds->_call_hooks($request, 'after_serialize_hook', 'record', \$outrs, \$outrec);
	}
	
	$output .= $outrs . $outrec;
	
	# Keep count of the output records, and stop if we have exceeded the
	# limit.
	
	$request->{actual_count}++;
	
	if ( defined $request->{result_limit} && $request->{result_limit} ne 'all' )
	{
	    last if $request->{actual_count} >= $request->{result_limit};
	}
	
	# If streaming is a possibility, check whether we have passed the
	# threshold for result size.  If so, then we need to immediately
	# stash the output generated so far and call stream_data.  Doing that
	# will cause the current function to be aborted, followed by an
	# automatic call to &stream_result (defined below).
	
	if ( defined $streaming_threshold && length($output) > $streaming_threshold )
	{
	    $request->{stashed_output} = $output;
	    $request->{header_lists} = \@header_lists;
	    
	    # Dancer::error "Initiating streaming at threshold $streaming_threshold\n";
	    
	    if ( $request->{out_fh} && $request->{response_hook} )
	    {
		$request->_stream_compound_result();

		my $response_hook = $request->{response_hook};
		return $request->$response_hook();
	    }
	    
	    else
	    {
		Dancer::Plugin::StreamData::stream_data($request, &_stream_compound_result);
	    }
	}
    }
    
    # If we get here, then we did not initiate streaming.  So add the
    # footer and return the output data.
    
    # If we didn't output any records, give the formatter a chance to indicate
    # this. 
    
    unless ( $request->{actual_count} )
    {
	my $empty = $format_class->emit_empty($request);
	
	if ( $request->{hook_enabled}{after_serialize_hook} )
	{
	    $ds->_call_hooks($request, 'after_serialize_hook', 'empty', \$empty);
	}

	$output .= $empty;
    }
    
    # Generate the final part of the output, after the last record.
    
    my $footer = $format_class->emit_footer($request, @header_lists);
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	$ds->_call_hooks($request, 'after_serialize_hook', 'footer', \$footer);
    }
    
    $output .= $footer;
    
    # If we are directing output to a file, write it now and then close the file. The proper encoding
    # should already have been set up. If a response_hook has been established, call it and return
    # as output whatever it returns. Otherwise, continue and return the output to the client as
    # normal.
    
    if ( my $fh = $request->{out_fh} )
    {
	print $fh $output;
	close $fh;
	
	if ( my $response_hook = $request->{response_hook} )
	{
	    return $request->$response_hook();
	}
    }
    
    # Determine if we need to encode the output into the proper character set.  Usually Dancer
    # does this for us, but only if it recognizes the content type as text.  For these formats,
    # the definition should set the attribute 'encode_as_text' to true.
    
    my $output_charset = $ds->{_config}{charset};
    my $must_encode;
    
    if ( $output_charset 
	 && $ds->{format}{$format}{encode_as_text}
	 && ! $request->{content_type_is_text} )
    {
	$must_encode = 1;
    }
    
    return $must_encode ? encode($output_charset, $output) : $output;
}


# _generate_processed_result ( request )
# 
# This function is called if the result set needs to be processed in its
# entirety before being output.  It processes the entire result set and
# collects a list of processed records, and then serializes each result record
# according to the specified output format.  If $streaming_threshold is
# specified, and if the size of the output exceeds this threshold, this
# routine then sets up to stream the rest of the output.

sub _generate_processed_result {

    my ($ds, $request, $streaming_threshold) = @_;
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->output_format;
    my $format_class = $ds->{format}{$format}{package};
    
    die "could not generate a result in format '$format': no implementing module was found"
	unless $format_class;
    
    $ds->debug_line("Processing result set before output.");
    
    my $path = $request->node_path;
    my $serial_hook = $ds->{hook_enabled}{after_serialize_hook} && $ds->node_attr($path, 'after_serialize_hook');
    
    # Now fetch and process each output record in turn.  Collect up all of the
    # records that pass the processing phase in a list.
    
    my @results;
    
 RECORD:
    while ( my $record = $ds->_next_record($request) )
    {
	my $oc = $record->{_output_config} ? $request->{$record->{_output_config}}
	    : $request->{$request->{current_output}};
	
	my $proc_list = $oc->{proc_list};
	
	# If there is a before_record_hook defined for this path, call it now.
	
	if ( $request->{hook_enabled}{before_record_hook} )
	{
	    $ds->_call_hooks($request, 'before_record_hook', $record);
	}

	# If 'skip_output_record' was called on this record, skip it now. If 'select_output_block'
	# was called, then substitute the proc list associated with the selected output block.
	
	next RECORD if $record->{_skip_record};
	
	if ( $record->{_output_block} )
	{
	    $proc_list = $request->{block_proc_list}{$record->{_output_block}};
	}
	
	# If there are any processing steps to do, then process this record.
	
	$ds->process_record($request, $record, $proc_list);
	
	# Add the record to the list.
	
	push @results, $record;
    }
    
    # We now know the result count.
    
    $request->{result_count} = scalar(@results);
    
    # At this point, we can generate the output.  We start with the header.
    
    my @header_lists = $ds->header_lists($request);
    
    my $output = $format_class->emit_header($request, @header_lists);
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	$ds->_call_hooks($request, 'after_serialize_hook', 'header', \$output);	
    }
    
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
	splice(@results, 0, $request->{result_offset});
    }
    
    # If the result limit is zero, we can ignore all records.
    
    if ( defined $request->{result_limit} && $request->{result_limit} eq '0' )
    {
	@results = ();	
    }
    
    # Otherwise iterate over all of the remaining records.
    
 OUTPUT:
    while ( @results )
    {
	my $record = shift @results;

	# If an alternate block or output configuration was specified for this record, use it.
	
	my $field_list;

	if ( $record->{_output_block} )
	{
	    $field_list = $record->{block_field_list}{$record->{_output_block}};
	}

	else
	{
	    my $oc = $record->{_output_config} ? $request->{$record->{_output_config}}
		: $request->{$request->{current_output}};
	    
	    $field_list = $oc->{field_list};
	}
	
	# Generate the output for this record, preceded by a record separator if
	# it is not the first record.
	
	my $outrs = $emit_rs ? $request->{rs} : ''; $emit_rs = 1;
	my $outrec = $format_class->emit_record($request, $record, $field_list);
	
	if ( $request->{hook_enabled}{after_serialize_hook} )
	{
	    $ds->_call_hooks($request, 'after_serialize_hook', 'record', \$outrs, \$outrec);
	}
	
	$output .= $outrs . $outrec;
	
	# Keep count of the output records, and stop if we have exceeded the
	# limit.
	
	$request->{actual_count}++;
	
	if ( defined $request->{result_limit} && $request->{result_limit} ne 'all' )
	{
	    last if $request->{actual_count} >= $request->{result_limit};
	}
	
	# If streaming is a possibility, check whether we have passed the
	# threshold for result size.  If so, then we need to immediately
	# stash the output generated so far and call stream_data.  Doing that
	# will cause the current function to be aborted, followed by an
	# automatic call to &stream_result (defined below).
	
	if ( defined $streaming_threshold && length($output) > $streaming_threshold )
	{
	    $request->{stashed_output} = $output;
	    $request->{stashed_results} = \@results;
	    $request->{processing_complete} = 1;
	    
	    if ( $request->{out_fh} && $request->{response_hook} )
	    {
		$request->_stream_compound_result();
		
		my $response_hook = $request->{response_hook};
		return $request->$response_hook();
	    }
	    
	    else
	    {
		Dancer::Plugin::StreamData::stream_data($request, &_stream_compound_result);
	    }
	}
    }
    
    # If we get here, then we did not initiate streaming.  So add the
    # footer and return the output data.
    
    # If we didn't output any records, give the formatter a chance to indicate
    # this. 
    
    unless ( $request->{actual_count} )
    {
	my $empty = $format_class->emit_empty($request);
	
	if ( $request->{hook_enabled}{after_serialize_hook} )
	{
	    $ds->_call_hooks($request, 'after_serialize_hook', 'empty', \$empty);
	}
	
	$output .= $empty;
    }
    
    # Generate the final part of the output, after the last record.
    
    my $footer = $format_class->emit_footer($request, @header_lists);
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	$ds->_call_hooks($request, 'after_serialize_hook', 'footer', \$footer);
    }
    
    $output .= $footer;
    
    # If we are directing output to a file, write it now and then close the file. The proper encoding
    # should already have been set up. If a response_hook has been established, call it and return
    # as output whatever it returns. Otherwise, continue and return the output to the client as
    # normal.
    
    if ( my $out_fh = $request->{out_fh} )
    {
	print $out_fh $output;
	close $out_fh;
	
	if ( my $response_hook = $request->{response_hook} )
	{
	    return $request->$response_hook();
	}
    }
    
    # Determine if we need to encode the output into the proper character set.
    # Usually Dancer does this for us, but only if it recognizes the content
    # type as text.  For these formats, the definition should set the
    # attribute 'encode_as_text' to true.
    
    my $output_charset = $ds->{_config}{charset};
    my $must_encode;
    
    if ( $output_charset 
	 && $ds->{format}{$format}{encode_as_text}
	 && ! $request->{content_type_is_text} )
    {
	$must_encode = 1;
    }
    
    return $must_encode ? encode($output_charset, $output) : $output;
}


# _stream_compound_result ( )
# 
# Continue to generate a compound query result from where generate_compound_result() left off, and
# stream it to the client or write it to an output file record-by-record.
# 
# If the second argument is defined, it must be a Plack 'writer' object. This object's 'write'
# method will be called to send in turn the previously stashed output, each subsequent record, and
# then the footer.  Each of these chunks of data will be immediately sent off to the client,
# instead of being marshalled together in memory.  This allows the server to send results up to
# hundreds of megabytes in length without bogging down.

sub _stream_compound_result {
    
    my ($request, $writer) = @_;
    
    my $ds = $request->{ds};
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->output_format;
    my $format_class = $ds->{format}{$format}{package};
    my $format_is_text = $ds->{format}{$format}{is_text};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $format_class;
    
    my $path = $request->node_path;
    my $serial_hook = $ds->{hook_enabled}{after_serialize_hook} && $ds->node_attr($path, 'after_serialize_hook');
    
    # Determine the output character set, because we will need to encode text
    # responses in it.
    
    my $output_charset = $ds->{_config}{charset};
    
    #return $must_encode ? encode($output_charset, $output) : $output;
    
    # If we have a writer object, send out the partial output previously stashed by
    # generate_compound_result().

    if ( $writer )
    {
	if ( $output_charset && $format_is_text )
	{
	    # Dancer::error "Writing stashed output encoded as $output_charset\n";
	    $writer->write( encode($output_charset, $request->{stashed_output}) );
	}
	
	else
	{
	    # Dancer::error "Writing stashed output\n";
	    $writer->write( $request->{stashed_output} );
	}
    }
    
    # If we have an output filehandle, write the stashed output to it. The proper encoding layer
    # should have been established already.
    
    if ( $request->{out_fh} )
    {
	# Dancer::error "Writing stashed output to filehandle\n";
	print $request->{stashed_output};
    }
    
    # Then process the remaining rows.
    
  RECORD:
    while ( my $record = $ds->_next_record($request) )
    {
	my $oc = $record->{_output_config} ? $request->{$record->{_output_config}}
	    : $request->{$request->{current_output}};
	
	my $proc_list = $oc->{proc_list};
	my $field_list = $oc->{field_list};
	
	# If there are any processing steps to do, then process this record. But skip this if this
	# subroutine was called from '_generate_processed_result'.

	if ( $request->{processing_complete} )
	{
	    $field_list = $record->{block_field_list}{$record->{_output_block}}
		if $record->{_output_block};
	}
	
	else
	{
	    # If there is a before_record_hook defined for this path, call it now.
	    
	    if ( $request->{hook_enabled}{before_record_hook} )
	    {
		$ds->_call_hooks($request, 'before_record_hook', $record);
	    }

	    # If 'skip_output_record' was called on this record, skip it now. If
	    # 'select_output_block' was called, then substitute the proc list and field list for
	    # the selected block.
	    
	    next RECORD if $record->{_skip_record};
	    
	    if ( my $alt = $record->{_output_block} )
	    {
		$proc_list = $request->{block_proc_list}{$alt};
		$field_list = $request->{block_field_list}{$alt};
	    }
	    
	    # Do any processing steps that were defined for this record.
	    
	    $ds->process_record($request, $record, $proc_list);
	}
	
	# Generate the output for this record, preceded by a record separator if
	# it is not the first record.
	
	my $outrs = $request->{rs};
	my $outrec = $format_class->emit_record($request, $record, $field_list);
	
	if ( $request->{hook_enabled}{after_serialize_hook} )
	{
	    $ds->_call_hooks($request, 'after_serialize_hook', 'record', \$outrs, \$outrec);
	}
	
	my $output .= $outrs . $outrec;
	
	if ( ! defined $output or $output eq '' )
	{
	    # do nothing
	}

	if ( $writer )
	{
	    if ( $output_charset && $format_is_text )
	    {
		$writer->write( encode($output_charset, $output) );
	    }
	    
	    else
	    {
		$writer->write( $output );
	    }
	}

	if ( my $out_fh = $request->{out_fh} )
	{
	    print $out_fh $output;
	}
	
	# Keep count of the output records, and stop if we have exceeded the
	# limit. 
	
	last if $request->{result_limit} ne 'all' && 
	    ++$request->{actual_count} >= $request->{result_limit};
    }
    
    # Finally, send out the footer and then close the writer object and/or filehandle.
    
    my @header_lists; @header_lists = @{$request->{header_lists}}
	if ref $request->{header_lists} eq 'ARRAY';
    
    my $footer = $format_class->emit_footer($request, @header_lists);
    
    if ( $request->{hook_enabled}{after_serialize_hook} )
    {
	$ds->_call_hooks($request, 'after_serialize_hook', 'footer', \$footer);
    }
    
    if ( $writer )
    {
	if ( ! defined $footer or $footer eq '' )
	{
	    # do nothing
	}
	
	elsif ( $output_charset && $format_is_text )
	{
	    $writer->write( encode($output_charset, $footer) );
	}
	
	else
	{
	    $writer->write( $footer );
	}
	
	$writer->close();
    }

    if ( my $out_fh = $request->{out_fh} )
    {
	if ( defined $footer && $footer ne '' )
	{
	    print $out_fh $footer;
	}

	close $out_fh;
    }
}


# _next_record ( request )
# 
# Return the next record to be output for the given request.  If
# $ds->{main_result} is set, use that first.  Once that is exhausted (or if
# it was never set) then if $result->{main_sth} is set then read records from
# it until exhausted.

sub _next_record {
    
    my ($ds, $request) = @_;
    
    # If the request has a zero limit, and no processing needs to be done on
    # the result set, then no records need to be returned.
    
    return if $request->{limit_zero};
    
    # If we have a stashed result list, return the next item in it.
    
    if ( ref $request->{stashed_results} eq 'ARRAY' )
    {
	return shift @{$request->{stashed_results}};
    }
    
    # If we have a 'main_result' array with something in it, return the next
    # item in it.
    
    elsif ( ref $request->{main_result} eq 'ARRAY' and @{$request->{main_result}} )
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


# _generate_empty_result ( request )
# 
# This function is called after an operation is executed and returns no results
# at all.  Return the header and footer only.

sub _generate_empty_result {
    
    my ($ds, $request) = @_;
    
    # Determine the output format and figure out which class implements it.
    
    my $format = $request->output_format;
    my $format_class = $ds->{format}{$format}{package};
    
    croak "could not generate a result in format '$format': no implementing class"
	unless $format_class;
    
    # Call the appropriate methods from this class to generate the header,
    # and footer.
    
    my $output = $format_class->emit_header($request);
    
    $output .= $format_class->emit_empty($request);
    $output .= $format_class->emit_footer($request);
    
    return $output;
}


1;
