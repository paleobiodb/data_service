#
# DataService.pm
# 
# This is a first cut at a data service application framework, built on top of
# Dancer.pm.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


use strict;

require 5.012_004;

package Web::DataService;

use Web::DataService::Request;
use Web::DataService::Output;
use Web::DataService::PodParser;

#use Dancer qw( :syntax );
use Dancer::Plugin;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;

use Carp qw( croak );
use Scalar::Util qw( reftype blessed weaken );

our ($DEFAULT_COUNT) = 1;

# Create a default instance, for when this module is used in a
# non-object-oriented fashion.

my ($DEFAULT_INSTANCE) = __PACKAGE__->new();

my (%MEDIA_TYPE) = 
    ('html' => 'text/html',
     'xml' => 'text/xml',
     'txt' => 'text/plain',
     'tsv' => 'text/tab-separated-values',
     'csv' => 'text/csv',
     'json' => 'application/json',
    );


# Methods
# =======

# new ( class, attrs )
# 
# Create a new data service instance.  If the second argument is provided, it
# must be a hash ref specifying options for the service as a whole.

sub new {
    
    my ($class, $options) = @_;
    
    # Determine option values.  Start from the Dancer configuration file.
    
    my $config = Dancer::config;
    
    my $output_param = $options->{output_param} || 'show';
    my $vocab_param = $options->{vocab_param} || 'vocab';
    my $needs_dbh = $options->{needs_dbh};
    my $path_prefix = $options->{path_prefix} || '/';
    my $public_access = $options->{public_access} || $config->{public_access};
    my $default_limit = $options->{default_limit} || $config->{default_limit} || 500;
    my $stream_threshold = $options->{stream_threshold} || $config->{stream_threshold} || 20480;
    my $name = $options->{name} || $config->{name};
    
    unless ( $name )
    {
	$name = 'Data Service' . ( $DEFAULT_COUNT > 1 ? $DEFAULT_COUNT : '' );
	$DEFAULT_COUNT++;
    }
    
    # Create a new HTTP::Validate object so that we can do parameter
    # validations.
    
    my $validator = HTTP::Validate->new();
    
    $validator->validation_settings(allow_unrecognized => 1) if $options->{allow_unrecognized);
    
    # Create a new DataService object, and return it:
    
    my $instance = {
		    name => $name,
		    path_prefix => $path_prefix,
		    validator => $validator,
		    response_selector => $selector,
		    public_access => $public_access,
		    output_param => $output_param,
		    vocab_param => $vocab_param,
		    needs_dbh => $needs_dbh,
		    default_limit => $default_limit,
		    stream_threshold => $stream_threshold,
		    path_attrs => {},
		    vocab => { 'default' => 
			       { name => 'default', use_field_names => 1, _default => 1,
				 doc => "The default vocabulary consists of the underlying field names" } },
		    vocab_list => [ 'default' ],
		    format => {},
		    format_list => [];
		   };
    
    # Return the new instance
    
    bless $instance, $class;
    return $instance;
}


# accessor methods for the various attributes:

sub get_path_prefix {
    
    return $_[0]->{path_prefix};
}

sub get_attr {
    
    return $_[0]->{$_[1]};
}


# define_path ( path, attrs... )
# 
# Set up a "path" entry, representing a complete or partial URL path.  This
# path should have a documentation page, but if one is not defined a template
# page will be used along with any documentation strings given in this call.
# Any path which represents an operation must be given an 'op' attribute.
# 
# An error will be signalled unless the "parent" path is already defined.  In
# other words, you cannot define 'a/b/c' unless 'a/b' is defined first.

sub define_path {
    
    my ($self);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( blessed $_[0] && $_[0]->isa('Web::DataService') )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    my ($package, $filename, $line) = caller;
    my ($last_node);
    
    # Now we go through the rest of the arguments.  Hashrefs define new
    # directories, while strings add to the documentation of the directory
    # whose definition they follow.
    
    foreach my $item (@_)
    {
	# A hashref defines a new directory.
	
	if ( ref $item eq 'HASH' )
	{
	    croak "a path definition must include the attribute 'path'"
		unless defined $item->{path} and $item->{path} ne '';
	    
	    $last_node = $self->create_path_node($path, $item, $filename, $line);
	}
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_path' must be hashrefs and strings";
	}
    }
    
    croak "the arguments to 'define_path' must include a hashref of attributes"
	unless $last_node;
}

register 'define_path' => \&define_path;


our (%NODE_DEF) = ( path => 'ignore',
		    class => 'single',
		    op => 'single',
		    ruleset => 'single'
		    output => 'single',
		    output_doc => 'list',
		    allow_format => 'set',
		    allow_vocab => 'set',
		    doc_file => 'single',
		    doc_title => 'single' );

# create_path_node ( attrs, filename, line )
# 
# Create a new node representing the specified path.  Attributes are inherited,
# as follows: 'a/b/c' inherits from 'a/b', while 'a' inherits from nothing.

sub create_path_node {

    my ($self, $path, $new_attrs, $filename, $line) = @_;
    
    # Make sure this path was not already defined by a previous call.
    
    if ( defined $self->{path_attrs}{$path} )
    {
	my $filename = $self->{path_attrs}{$path}{_filename};
	my $line = $self->{path_attrs}{$path}{_line};
	croak "path '$path' was already defined at line $line of $filename";
    }
    
    # Create a new node to hold the path attributes.
    
    my $path_attrs = { _filename => $filename, _line => $line };
    
    # If the path has a valid prefix, start with the prefix path's attributes
    # as a base.  Assume the attribute values are all valid, since they were
    # checked when the prefix path was defined (not sure if this is always
    # going to be a correct assumption).  Throw an error if the prefix path is
    # not already defined.
    
    if ( $path =~ qr{ ^ (.+) / [^/]+ } )
    {
	my $prefix_attrs = $self->{path_attrs}{$1};
	
	croak "path '$path' is invalid because '$1' was never defined"
	    unless defined $prefix_attrs;
	
	foreach my $key ( keys %$prefix_attrs )
	{
	    if ( $NODE_DEF{$key} eq 'single' )
	    {
		$path_attrs->{$key} = $prefix_attrs->{$key};
	    }
	    
	    elsif ( $NODE_DEF{$key} eq 'set' and ref $prefix_attrs->{$key} eq 'HASH' )
	    {
		$path_attrs->{$key} = { %{$prefix_attrs->{$key}} };
	    }
	    
	    elsif ( $NODE_DEF{$key} eq 'list' and ref $prefix_attrs->{$key} eq 'ARRAY' )
	    {
		$path_attrs->{$key} = [ @{$prefix_attrs->{$key}} ];
	    }
	}
    }
    
    # Now apply the specified attributes, overriding or modifying any
    # equivalent attributes from the prefix path.
    
    foreach my $key ( keys %$new_attrs )
    {
	croak "unknown key '$key' in path definition"
	    unless $NODE_DEF{$key};
	
	my $value = $new_attrs->{$key};
	
	# If the attribute takes a single value, then set the value as
	# specified.
	
	elsif ( $NODE_DEF{$key} eq 'single' )
	{
	    $path_attrs->{$key} = $value;
	}
	
	# If the attribute takes a set value, then turn a string value into a
	# hash whose keys are the individual values.  If the value begins with + or
	# -, then add or delete values as indicated.  Otherwise, substitute
	# the given set.
	
	elsif ( $NODE_DEF{$key} eq 'set' )
	{
	    my @values = split( qr{\s*,\s*}, $value );
	    
	    if ( $value =~ qr{ ^ [+-] }x )
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "invalid value '$v', must start with + or -"
			unless $v =~ qr{ ^ ([+-]) (.*) }x;
		    
		    $1 eq '-' ? delete $path_attrs->{$key}{$2}
			      : $path_attrs->{$key}{$2} = 1;
		}
	    }
	    
	    else
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "invalid value '$v', cannot start with + or -"
			if $v =~ qr{ ^+ | ^- }x;
		    
		    $path_attrs->{$key}{$v} = 1;
		}
	    }
	}
	
	# If the attribute takes a list value, then turn a string value into a
	# list.  If the value begins with + or -, then add or delete values as
	# indicated.  Otherwise, substitute the given list.
	
	elsif ( $NODE_DEF{$key} eq 'list' )
	{
	    my @values = split( qr{\s*,\s*}, $value );
	    
	    if ( $value =~ qr{ ^ [+-] }x )
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "invalid value '$v', must start with + or -"
			unless $v =~ qr{ ^ ([+-]) (.*) }x;
		    
		    if ( $1 eq '-' )
		    {
			$path_attrs->{$key} = [ grep { $_ ne $2 } @{$path_attrs->{$key}} ];
		    }
		    
		    else
		    {
			push @{$path_attrs->{$key}}, $2
			    unless grep { $_ eq $2 } @{$path_attrs->{$key}};
		    }
		}
	    }
	    
	    else
	    {
		$path_attrs->{$key} = [];
		
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "invalid value '$v', cannot start with + or -"
			if $v =~ qr{ ^+ | ^- }x;
		    
		    push @{$path_attrs->{$key}}, $v;
		}
	    }
	}
    }
    
    # Now check the attributes to make sure they are consistent:
    
    # Throw an error if 'class' doesn't specify an existing subclass of
    # Web::DataService::Request.
    
    my $class = $path_attrs->{class};
    
    croak "invalid class '$class': must be a subclass of 'Web::DataService::Request'"
	if defined $class and not $class->isa('Web::DataService::Request');
    
    # Throw an error if 'op' doesn't specify an existing method of this class.
    
    my $op = $path_attrs->{op};
    
    croak "invalid op '$op': must be a method of class '$class'"
	if defined $op and not $class->can($op);
    
    # Throw an error if any of the specified formats fails to match an
    # existing format.  If any of the formats has a default vocabulary, add it
    # to the vocabulary list.
    
    if ( ref $path_attrs->{allow_format} )
    {
	foreach my $f ( keys %{$path_attrs->{allow_format}} )
	{
	    croak "invalid value '$f' for format: no such format has been defined for this data service"
		unless ref $self->{format}{$f};
	}
	
	my $dv = $self->{format}{$f}{default_vocab};
	$path_attrs->{allow_vocab}{$dv} = 1 if $dv;
    }
    
    # Throw an error if any of the specified vocabularies fails to match an
    # existing vocabulary.
    
    if ( ref $path_attrs->{allow_vocab} )
    {
	foreach my $v ( keys %{$path_attrs->{vocab}} )
	{
	    croak "invalid value '$v' for vocab: no such vocabulary has been defined for this data service"
		unless ref $self->{vocab}{$v};
	}
    }
    
    # If no ruleset was specified, default to the path name.
    
    unless ( $path_attrs->{ruleset} )
    {
	$path_attrs->{ruleset} = $path;
    }
    
    # Install the node.
    
    $self->{path_attrs}{$path} = $path_attrs;
    
    # If one of the attributes is 'class', make sure that the class is
    # configured. 
    
    if ( $path_attrs->{class} )
    {
	$self->configure_class($path_attrs->{class})
    }
}


# path_defined ( path )
# 
# Return true if the specified path has been defined, false otherwise.

sub path_defined {

    my ($self, $path);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( ref $_[0] && blessed $_[0] )
    {
	($self, $path) = @_;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
	($path) = @_;
    }
    
    return $self->{path_attrs}{$path};
}

register 'path_defined' => \&path_defined;


# get_path_attr ( path, key )
# 
# Return the specified attribute for the given path.

sub get_path_attr {
    
    my ($self, $path, $key) = @_;
    
    return unless defined $key;
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( ref $_[0] && blessed $_[0] )
    {
	($self, $path, $key) = @_;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
	($path, $key) = @_;
    }
    
    # If the path is defined and has the specified key, return the
    # corresponding value.
    
    return $self->{path_attrs}{$path}{$key};
}


# define_vocab ( attrs... )
# 
# Define one or more vocabularies of field names for data service responses.

sub define_vocab {

    my ($self);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( blessed $_[0] && $_[0]->isa('Web::DataService') )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    #my ($package, $filename, $line) = caller;
    my ($last_node);
    
    # Now we go through the rest of the arguments.  Hashrefs define new
    # vocabularies, while strings add to the documentation of the vocabulary
    # whose definition they follow.
    
    foreach my $item (@_)
    {
	# A hashref defines a new vocabulary.
	
	if ( ref $item eq 'HASH' )
	{
	    # Make sure the attributes include 'name'.
	    
	    my $name = $item->{name}; 
	    
	    croak "could not define vocabulary: no name specified" unless $name;
	    
	    # Make sure this vocabulary was not already defined by a previous call,
	    # and set the attributes as specified.
	    
	    croak "vocabulary '$name' was already defined" if defined $self->{vocab}{$name}
		and not $self->{vocab}{$name}{_default};
	    
	    # Remove the default vocabulary, because it is only used if no
	    # other vocabularies are defined.
	    
	    if ( $self->{vocab}{default}{_default} )
	    {
		delete $self->{vocab}{default};
		unshift @{$self->{vocab_list}};
	    }
	    
	    # Now install the new vocabulary.
	    
	    $self->{vocab}{$name} = $item;
	    push @{$self->{vocab_list}}, $name;
	    $last_node = $item;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_vocab' must be hashrefs and strings";
	}
    }
    
    croak "the arguments must include a hashref of attributes"
	unless $last_node;
}


# document_vocab ( name )
# 
# Return a string containing POD documentation of the vocabulary
# possibilities.  If a name is specified, return the documentation string for
# that vocabulary only.

sub document_vocab {
    
    my ($self, $name) = @_;
    
    # Otherwise, if a single vocabulary name was given, return its
    # documentation string if any.
    
    if ( $name )
    {
	return $self->{vocab}{$name}{doc};
    }
    
    # Otherwise, document the entire list of vocabularies in POD format.
    
    my $doc = "=over 4\n\n";
    
    $doc .= "=for pp_table_no_header Name* | Documentation\n\n";
    
    foreach my $v (@{$self->{vocab_list}})
    {
	my $vrec = $self->{vocab}{$v};
	
	$doc .= "=item $vrec->{name}\n\n";
	$doc .= "$vrec->{doc}\n\n" if $vrec->{doc};
    }
    
    $doc .= "=back\n\n";
    
    return $doc;
}


# define_format ( attrs... )
# 
# Define one or more formats for data service responses.

sub define_format {

    my ($self);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( blessed $_[0] && $_[0]->isa('Web::DataService') )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    my ($last_node);
    
    # Now we go through the rest of the arguments.  Hashrefs define new
    # vocabularies, while strings add to the documentation of the vocabulary
    # whose definition they follow.
    
    foreach my $item (@_)
    {
	# A hashref defines a new vocabulary.
	
	if ( ref $item eq 'HASH' )
	{
	    # Make sure the attributes include 'name'.
	    
	    my $name = $item->{name}; 
	    
	    croak "the attributes must include 'name'" unless defined $name;
	    
	    # Make sure this format was not already defined by a previous call,
	    # and set the attributes as specified.
	    
	    croak "format '$name' was already defined" if defined $self->{format}{$name};
	    
	    # Now store it as a response format for this data service.
	    
	    $self->{format}{$name} = $item;
	    push @{$self->{format_list}}, $name;
	    $last_node = $item;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_format' must be hashrefs and strings";
	}
    }    
    
    croak "the arguments must include a hashref of attributes"
	unless $last_node;
}


# document_format ( name )
# 
# Return a string containing POD documentation of the response formats that
# have been defined for this data service.  If a format name is given, return
# just the documentation for that format.

sub document_format {
    
    # If no formats have been defined, return undef.
    
    return unless ref $self->{format_list} eq 'ARRAY';
    
    # Otherwise, if a single format name was given, return its
    # documentation string if any.
    
    if ( $name )
    {
	return $self->{format}{$name}{doc};
    }
    
    # Otherwise, document the entire list of formats in POD format.
    
    my $doc = "=over 4\n\n";
    
    $doc .= "=for pp_table_no_header Name* | Documentation\n\n";
    
    foreach my $f (@{$self->{format_list}})
    {
	my $frec = $self->{format}{$f};
	
	$doc .= "=item $frec->{name}\n\n";
	$doc .= "fvrec->{doc}\n\n" if $frec->{doc};
    }
    
    $doc .= "=back\n\n";
    
    return $doc;
}


# define_ruleset ( name, rule... )
# 
# Define a ruleset under the given name.  This is just a wrapper around the
# subroutine HTTP::Validate::ruleset.

sub define_ruleset {
    
    my ($self);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( blessed $_[0] && $_[0]->isa('Web::DataService') )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    unshift @_, $self->{validator};
    
    goto &HTTP::Validate::ruleset;
}


# define_output ( name, rule... )
# 
# Define an "output section" under the given name.  This comprises a set of
# field specifications which are used to generate output records.  This
# section is defined in relation to the class from which this routine has been
# called, for later use in query operations using that class.

sub define_output {

    my ($self);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( blessed $_[0] && $_[0]->isa('Web::DataService') )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    # Now figure out which package we are being called from.  The output
    # section being defined will be stored under this package name.  If we
    # were not called from a subclass of Web::DataService::Request, then store
    # this information under Web::DataService::Request so that it will be
    # available to all packages.
    
    my ($package) = caller;
    
    unless ( $package->isa('Web::DataService::Request') )
    {
	$package = 'Web::DataService::Request';
    }
    
    # Now adjust the argument list and call define_output_section
    
    unshift @_, $package;
    unshift @_, $self;
    
    goto &define_output_section;
}


# add_node_doc ( node, doc_string )
# 
# Add the specified documentation string to the specified node.

sub add_node_doc {
    
    my ($self, $node, $doc) = @_;
    
    return unless defined $doc and $doc ne '';
    
    croak "only strings may be added to documentation: '$doc' is not valid"
	if ref $doc;
    
    $node->{doc} = '' unless defined $node->{doc};
    $node->{doc} .= "\n" if $node->{doc} ne '';
    $node->{doc} .= $doc;
}


# configure_class ( class )
# 
# If the specified class has a 'configure' method, call it.  Pass it a
# database handle and the Dancer configuration hash.

sub configure_class {
    
    my ($self, $class) = @_;
    
    # If we have already configured this class, return.
    
    return if $self->{configured}{$class};
    
    # Record that we have configured this class.
    
    $self->{configured}{$class} = 1;
    
    # If this class has an immediate parent which is a subclass of
    # Web::DataService::Request, record that so that we can search for inherited
    # output sections.
    
    no strict 'refs';
    
    foreach my $super ( @{"$class::ISA"} )
    {
	if ( $super->isa('Web::DataService::Request') )
	{
	    $self->{super_class}{$class} = $super;
	    last;
	}
    }
    
    # If the class has a configuration method, call it.
    
    if ( $class->can('configure') )
    {
	print STDERR "Configuring $class for data service $self->{name}\n" if $PBDB_Data::DEBUG;
	$class->configure($self, Dancer::config, database());
    }
}


# can_execute_path ( path, format )
# 
# Return true if the path can be used for a request, i.e. if it has a class
# and operation defined.  Return false otherwise.

sub can_execute_path {
    
    my $self;
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( ref $_[0] && blessed $_[0] )
    {
	$self = shift;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
    }
    
    my ($path) = @_;
    
    # Now check whether we have the necessary attributes.
    
    return defined $self->{path_attrs}{$path}{class} &&
	   defined $self->{path_attrs}{$path}{op};
}


# execute_path ( path, format )
# 
# Execute the operation corresponding to the attributes of the given path, and
# return the resulting data in the specified format.

sub execute_path {
    
    my ($self, $path, $format);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( ref $_[0] && blessed $_[0] )
    {
	($self, $path, $format) = @_;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
	($path, $format) = @_;
    }
    
    # Do all of the processing in a try block, so that if an error occurs we
    # can respond with an appropriate error page.
    
    try {
	
	$DB::single = 1;
	
	# Return an error result if the specified format is not valid.
	
	return $self->error_result($path, $format, "415")
	    unless defined $format 
		&& ref $self->{format}{$format}
		    && $self->{path_attrs}{$path}{allow_format}{$format};
	
	my $class = $self->{path_attrs}{$path}{class};
	my $op = $self->{path_attrs}{$path}{op};
	
	# Do a basic sanity check to make sure that the operation is valid.
	
	croak "cannot execute path '$path': invalid class '$class' and op '$op'"
	    unless $class->isa('Web::DataService::Request') && $class->can($op);
	
	# Create a new object to represent this request.
	
	my $request = { ds => $self,
		      path => $path,
		      op => $op };
	
	$request->{dbh} = database() if $self->{needs_dbh} || $self->{path_attrs}{$path}{needs_dbh};
	
	weaken $request->{ds};	# Don't block garbage collection
	
	bless $request, $class;
	
	# If a ruleset was specified, then validate and clean the parameters.
	# Otherwise check if there is a ruleset corresponding to the path.  If
	# so, use that.
	
	
	my $ruleset = $attrs->{ruleset} || $self->path_attr($path, 'ruleset');
	if ( defined $ruleset )
	{
	    my $validator = $self->{validator};
	    my $result = $validator->validate_params($ruleset, Dancer::params);
	    
	    if ( $result->errors )
	    {
		return $self->error_result($path, $format, $result);
	    }
	    
	    $request->{params} = $result->values;
	    $request->{valid} = $result;
	}
	
	# Determine additional attributes relevant to this operation type.
	
	my $output = $attrs->{output} // $self->path_attr($path, 'output');
	my $arg = $attrs->{arg} // $self->path_attr($path, 'arg');
	
	$ruleset = $path if !defined $ruleset && $validator->ruleset_defined($path);
	
	# Set the response content type and access control header
	# appropriately for this request.
	
	$self->set_response_content_type($path, $format) if $MEDIA_TYPE{$format};
	$self->set_access_control_header($path, $attrs);
	
	# Determine the output fields and vocabulary, based on the parameters
	# already specified.
	
	$request->set_response($format, $request->{params}{vocab}, $op, $output, $request->{params}{show});
	
	# Execute the query or other operation.
	
	$request->$op();
	
	# If we have a single main record or data, then we return it.
	
	if ( exists $request->{main_record} or exists $request->{main_data} )
	{
	    return $request->generate_single_result();
	}
	
	# If we have a main statement handle, then read records from it and
	# generate a compound response.
	
	if ( exists $request->{main_sth} or exists $request->{main_result} )
	{
	    # If the server supports streaming, call generate_compound_result with
	    # can_stream => 1 and see if it returns any data or not.  If it does
	    # return data, send it back to the client as the response content.  An
	    # undefined result is a signal that we should stream the data by
	    # calling stream_data (imported from Dancer::Plugin::StreamData).
	    
	    if ( server_supports_streaming )
	    {
		$result = $request->generate_compound_result( can_stream => 1 ) or
		    stream_data( $request, 'stream_result' );
		
		return $result;
	    }
	    
	    # If the server does not support streaming, we just generate the result
	    # and return it.
	    
	    else
	    {
		return $request->generate_compound_result();
	    }
	}
	
	# Otherwise, return a "not found" error
	
	else
	{
	    return $self->error_result($path, $format, "404 The requested resource was not found.");
	}
    }
    
    # If an error occurs, return an appropriate error response to the client.
    
    catch {

	return $self->error_result($path, $format, $_);
    };
};

register 'execute_path' => \&execute_path;


# document_path ( path, format )
# 
# Generate and return a documentation page corresponding to the specified
# path, in the specified format.  The accepted formats are 'html' and 'pod'.
# 
# If a documentation template corresponding to the specified path is found, it
# will be used.  Otherwise, a default template will be used.

sub document_path {
    
    my ($self, $path, $format);
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    if ( ref $_[0] && blessed $_[0] )
    {
	($self, $path, $attrs) = @_;
    }
    
    else
    {
	$self = $DEFAULT_INSTANCE;
	($path, $attrs) = @_;
    }
    
    $DB::single = 1;
    
    # We start by determining the filename for the documentation template.  If
    # the filename was not explicitly specified, try the path with '_doc.tt'
    # appended.  If that does not exist, try appending '/index.tt'.
    
    my $viewdir = Dancer::config->{views};
    my $doc_file = $self->{path_attrs}{$path}{doc_file};
    
    unless ( $doc_file )
    {
	if ( -e "$viewdir/doc/${path}_doc.tt" )
	{
	    $doc_file = "${path}_doc.tt";
	}
	
	elsif ( -e "$viewdir/doc/${path}/index.tt" )
	{
	    $doc_file = "${path}/index.tt";
	}
    }
    
    unless ( -r "$viewdir/doc/$docfile" )
    {
	$doc_file = $self->{path_attrs}{$path}{doc_error_file} || $self->{doc_error_file} || 'doc_error.tt';
    }
    
    # Then assemble the variables used to fill in the template:
    
    my $vars = { doc_title => $self->{path_attrs}{$path}{doc_title},
		 ds_version => $self->{path_attrs}{$path}{version} || $self->{version},
	         param_doc => '',
	         response_doc => '' };
    
    # Add the documentation for the parameters.  If no corresponding ruleset
    # is found, then state that no parameters are accepted.
    
    my $validator = $self->{validator};
    my $ruleset = $self->path_attr{path_attrs}{$path}{ruleset};
    
    if ( $validator->ruleset_defined($ruleset) )
    {
	$vars->{param_doc} = $validator->document_params($ruleset)
    }
    
    else
    {
	$vars->{param_doc} ||= "I<This path does not take any parameters>";
    }
    
    # Add the documentation for the response.
    
    $vars->{response_doc} = $class->document_response($path);
    
    # Now select the appropriate layout and execute the template.
    
    my $doc_layout = $self->{path_attrs}{$path}{doc_layout} || $self->{doc_layout} || 'doc_main.tt';
    
    Dancer::set layout => $doclayout;
    
    my $doc_string = Dancer::template( "doc/$doc_file", $vars );
    
    # All documentation is public, so set the maximally permissive CORS header.
    
    Dancer::header "Access-Control-Allow-Origin" => "*";
    
    # If POD format was requested, return the documentation as is.
    
    if ( $format eq 'pod' )
    {
	Dancer::content_type 'text/plain';
	return $doc_string;
    }
    
    # Otherwise, convert the POD to HTML using the PodParser and return the result.
    
    else
    {
	my $parser = DataService::PodParser->new();
	
	$parser->parse_pod($doc_string);
	
	my $doc_html = $parser->generate_html({ css => '/data/css/dsdoc.css', tables => 1 });
	
	Dancer::content_type 'text/html';
	return $doc_html;
    }
}

register 'document_path' => \&document_path;


sub set_access_control_header {
    
    my ($self, $path, $attrs) = @_;
    
    if ( $self->{public_access} )
    {
	Dancer::header "Access-Control-Allow-Origin" => "*";
    }
    
    # At some point we need to add provision for authenticated access.
    
    return;
}


sub set_response_content_type {
    
    my ($self, $path, $format) = @_;
    
    unless ( $MEDIA_TYPE{$format} )
    {
	$self->error_result($path, 'html', "415");
    }
    
    Dancer::content_type $MEDIA_TYPE{$format};
}


my %CODE_STRING = ( 400 => "Bad Request", 
		    404 => "Not Found", 
		    415 => "Invalid Media Type",
		    500 => "Server Error" );

# error_result ( path, format, error )
# 
# Send an error response back to the client.

sub error_result {

    my ($self, $path, $format, $error) = @_;
    
    my ($code);
    my (@errors, @warnings);
    
    # If the error is actually a response object from HTTP::Validate, then
    # extract the error and warning messages.  In this case, the error code
    # should be "400 bad request".
    
    if ( ref $error eq 'HTTP::Validate::Response' )
    {
	@errors = $error->errors;
	@warnings = $error->warnings;
	$code = "400";
    }
    
    # If the error message begins with a 3-digit number, then that should be
    # used as the code and the rest of the message as the error text.
    
    elsif ( $error =~ qr{ ^ (\d\d\d) \s+ (.*) }xs )
    {
	$code = $1;
	@errors = $2;
    }
    
    # Otherwise, this is an internal error and all that we should report to
    # the user (for security reasons) is that an error occurred.  The actual
    # message is written to the server error log.
    
    else
    {
	$code = 500;
	#error("Error on path $path: $error");
	@errors = "A server error occurred.  Please contact the server administrator.";
    }
    
    # If the format is 'json', render the response as a JSON object.
    
    if ( $format eq 'json' )
    {
	my $error = 'error: [' . "\n\"" . join("\",\n\"", @errors) . "\"\n]\n";
	$error .= ", warn: [\"\n" . join("\",\n\"", @warnings) . "\"\n]\n" if @warnings;
	
	Dancer::status($code);
	Dancer::halt( "{ $error }" );
    }
    
    # Otherwise, generate a generic HTML response (we'll add template
    # capability later...)
    
    else
    {
	my $text = $CODE_STRING{$code};
	my $error = '';
	my $warning = '';
	
	$error .= "<h3>$_</h3>\n" foreach @errors;
	
	$warning .= "<h2>Warnings:</h2>\n" if @warnings;
	$warning .= "<h3>$_</h3>\n" foreach @warnings;
	
	my $body = <<END_BODY;
<html><head><title>$code $text</title></head>
<body><h1>$code $text</h1>
$error
$warning
</body></html>
END_BODY
    
	Dancer::status($code);
	Dancer::halt($body);
    }
}


register_plugin;

1;
