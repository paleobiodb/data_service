#
# DataService.pm
# 
# This is a first cut at a data service application framework, built on top of
# Dancer.pm.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


use strict;

require 5.012;

package Web::DataService;

use parent qw(Exporter);

use Carp qw( croak );
use Scalar::Util qw( reftype blessed weaken );
use POSIX qw( strftime );
use Try::Tiny;
use Time::HiRes;

#use if $INC{'Dancer'}, 'Web::DataService::FF::Dancer';

use Web::DataService::Format;
use Web::DataService::Vocabulary;
use Web::DataService::Cache;
use Web::DataService::Request;
use Web::DataService::Documentation;
use Web::DataService::Output;
use Web::DataService::Render;
use Web::DataService::PodParser;
use Web::DataService::JSON qw(json_list_value);

use HTTP::Validate qw( :validators );


HTTP::Validate->VERSION(0.35);


BEGIN {
    our (@EXPORT_OK) = @HTTP::Validate::VALIDATORS;
    
    our (%EXPORT_TAGS) = (
        validators => \@HTTP::Validate::VALIDATORS
    );
}


our @HTTP_METHOD_LIST = ('GET', 'HEAD', 'POST', 'PUT', 'DELETE');


# Methods
# =======

# new ( class, attrs )
# 
# Create a new data service instance.  The second argument must be a hash of
# attribute values.  It may contain any of the attributes specified in
# %DS_DEF, and must always contain 'name'.  Any values specified in the Dancer
# configuration file are applied as well, unless they are overridden by the
# attributes given here.  Configuration values can appear either as
# sub-entries under the data service name, or as top-level entries, with
# precedence given to the former.
# 
# If the attribute 'parent' is given, its value must be an existing data
# service object.  The new object then represents a subservice of the parent.


my (%DS_DEF) = ( 'name' => 'single',
		 'version' => 'single',
		 'label' => 'single',
		 'title' => 'single',
		 'parent' => 'single',
		 'foundation_plugin' => 'single',
		 'templating_plugin' => 'single',
		 'backend_plugin' => 'single',
		 'path_prefix' => 'single',
		 'ruleset_prefix' => 'single',
		 'public_access' => 'single',
		 'template' => 'single',
		 'doc_templates' => 'single',
		 'output_templates' => 'single',
		 'doc_defs' => 'single',
		 'doc_header' => 'single',
		 'doc_footer' => 'single',
		 'doc_layout' => 'single',
		 'default_limit' => 'single',
		 'streaming_threshold' => 'single',
		 'allow_unrecognized' => 'single',
		 'doc_dir' => 'single' );

our ($DEBUG);
our ($ONE_REQUEST);

my (%DS_DEFAULT) = ( 'default_limit' => 500, 
		     'streaming_threshold' => 20480 );

sub new {
    
    my ($class_or_parent, $attrs) = @_;
    
    croak "Each data service must be given a set of attributes"
	unless ref $attrs && reftype $attrs eq 'HASH';
    
    my $instance = {};
    
    # First, make sure we have a valid foundation plugin.  This is necessary
    # in order to get configuration information.
    
    $instance->{foundation_plugin} = $attrs->{foundation_plugin};
    
    if ( $instance->{foundation_plugin} )
    {
	croak "class '$attrs->{foundation_plugin}' is not a valid foundation plugin: cannot find method 'get_config'"
	    unless $instance->{foundation_plugin}->can('get_config');
    }
    
    # Otherwise, if 'Dancer.pm' has already been required then install the
    # corresponding plugin.
    
    elsif ( $INC{'Dancer.pm'} )
    {
	require Web::DataService::Plugin::Dancer;
	$instance->{foundation_plugin} = 'Web::DataService::Plugin::Dancer';
    }
    
    else
    {
	croak "could not find a foundation framework: try using 'Dancer'";
    }
    
    # Ensure that we have a non-empty name, and retrieve any configuration
    # directives for this named data service.
    
    croak "Each data service must be given a 'name' attribute"
	unless defined $attrs->{name} && $attrs->{name} ne '';
    
    my $name = $attrs->{name};
    my $config = $instance->{foundation_plugin}->get_config($name);
    
    # If $class_or_parent is actually a Web::DataService instance, use it as the parent.
    
    my $parent_attrs = {};
    my $parent_name = '';
    
    if ( ref $class_or_parent && $class_or_parent->isa('Web::DataService') )
    {
	$parent_attrs = $class_or_parent;
	$parent_name = $class_or_parent->{name};
	$instance->{parent} = $class_or_parent;
    }
    
    # Determine attribute values.  Any values found in the configuration hash
    # serve as defaults, and %DS_DEFAULT values are used if nothing else is
    # specified.
    
    foreach my $key ( keys %DS_DEF )
    {
	my $value = $attrs->{$key} // $config->{$key} // $DS_DEFAULT{$key};
	$instance->{$key} = $value if defined $value;
    }
    
    # The label and title default to the name, if not otherwise specified.
    
    $instance->{label} //= $instance->{name};
    $instance->{title} //= $instance->{name};
    
    # Create a pattern for recognizing paths, unless one has been specifically provided.
    
    if ( $attrs->{path_prefix} && ! $instance->{path_re} )
    {
	$instance->{path_re} = qr{ ^ [/]? $attrs->{path_prefix} (?: [/] (.*) | $ ) }xs;
    }
    
    # Create a new HTTP::Validate object so that we can do parameter
    # validations.
    
    $instance->{validator} = HTTP::Validate->new();
    
    $instance->{validator}->validation_settings(allow_unrecognized => 1)
	if $instance->{allow_unrecognized};
    
    # Create a default vocabulary, to be used in case no others are defined.
    
    $instance->{vocab} = { 'default' => 
			   { name => 'default', use_field_names => 1, _default => 1,
			     doc => "The default vocabulary consists of the underlying field names" } };
    
    $instance->{vocab_list} = [ 'default' ];
    
    # Add a few other necessary fields.
    
    $instance->{path_attrs} = {};
    $instance->{format} = {};
    $instance->{format_list} = [];
    $instance->{subservice} = {};
    $instance->{subservice_list} = [];
    
    $instance->{DEBUG} = 1 if $config->{ds_debug};
    
    # Now check to make sure we have rest of the necessary plugins.
    
    # If a templating plugin was explicitly specified, check that it is valid. 
    
    if ( $instance->{templating_plugin} )
    {
	croak "class '$instance->{templating_plugin}' is not a valid templating plugin: cannot find method 'render_template'"
	    unless $instance->{templating_plugin}->can('render_template');
    }
    
    # Otherwise, if 'Template.pm' has already been required then install the
    # corresponding plugin.
    
    elsif ( $INC{'Template.pm'} )
    {
	require Web::DataService::Plugin::TemplateToolkit;
	$instance->{templating_plugin} = 'Web::DataService::Plugin::TemplateToolkit';
    }
    
    else
    {
	warn "WARNING: no templating engine was specified, so documentation pages\n";
	warn "    and templated output will not be available.\n";
    }
    
    # If we have a templating plugin, instantiate it for documentation and
    # output.
    
    if ( $instance->{templating_plugin} )
    {
	my $plugin = $instance->{templating_plugin};
	my $doc_templates = $instance->{doc_templates} // 'doc';
	my $output_templates = $instance->{output_templates};
	
	# If we were given a directory for documentation templates, initialize
	# an engine for evaluating them.
	
	if ( $doc_templates )
	{
	    $doc_templates = $ENV{PWD} . '/' . $doc_templates
		unless $doc_templates =~ qr{ ^ / }xs;
	    
	    croak "$doc_templates: $!" unless -r $doc_templates;
	    
	    $instance->{doc_templates} = $doc_templates;
	    
	    $instance->{doc_engine} = 
		$plugin->initialize_engine($instance, $config,
				       { template_dir => $doc_templates });
	}
	
	# we were given a directory for output templates, initialize an
	# engine for evaluating them as well.
    
	if ( $output_templates )
	{
	    $output_templates = $ENV{PWD} . '/' . $output_templates
		unless $output_templates =~ qr{ ^ / }xs;
	    
	    croak "$output_templates: $!" unless -r $output_templates;
	    
	    $instance->{output_templates} = $output_templates;
	    
	    $instance->{output_engine} =
		$plugin->initialize_engine($instance, $config,
				       { template_dir => $output_templates });
	}
    }
    
    # If a backend plugin was explicitly specified, check that it is valid.
    
    if ( $instance->{backend_plugin} )
    {
	croak "class 'instance->{backend_plugin}' is not a valid backend plugin: cannot find method 'get_connection'"
	    unless $instance->{backend_plugin}->can('get_connection');
    }
    
    # Otherwise, if 'Dancer::Plugin::Database' is available then select the
    # corresponding plugin.
    
    elsif ( $INC{'Dancer.pm'} && $INC{'Dancer/Plugin/Database.pm'} )
    {
	$instance->{backend_plugin} = 'Web::DataService::Plugin::Dancer';
    }
    
    else
    {
	# If no backend plugin is available, then leave this field undefined.
	# The application must then either add code to the various operation
	# methods or rely on an 'init_request_hook' to provide access to
	# backend data.
    }
    
    # Bless the new instance, and link it in to its parent if necessary.
    
    bless $instance, $class_or_parent;
    
    if ( $instance->{parent} )
    {
	my $parent = $instance->{parent};
	
	$parent->{subservice}{$name} = $instance;
	push @{$parent->{subservice_list}}, $instance;
    }
    
    # Give the various plugins a chance to check and/or modify this instance.
    
    $instance->plugin_init('foundation_plugin');
    $instance->plugin_init('templating_plugin');
    $instance->plugin_init('backend_plugin');
    
    # Return the new instance.
    
    return $instance;
}


sub plugin_init {

    my ($self, $plugin) = @_;
    
    return unless defined $self->{$plugin};
    return unless $self->{$plugin}->can('initialize_service');
    
    $self->{$plugin}->initialize_service($self);
}


sub get_connection {
    
    my ($self) = @_;
    
    croak "cannot execute get_connection: no backend plugin was defined"
	unless defined $self->{backend_plugin};
    return $self->{backend_plugin}->get_connection($self);
}


sub get_config {
    
    my ($self, @args) = @_;
    
    return $self->{foundation_plugin}->get_config(@args);
}


sub set_mode {
    
    my ($self, @modes) = @_;
    
    foreach my $mode (@modes)
    {
	if ( $mode eq 'debug' )
	{
	    $DEBUG = 1;
	}
	
	elsif ( $mode eq 'one_request' )
	{
	    $ONE_REQUEST = 1;
	}
    }
}


# accessor methods for the various attributes:

sub get_attr {
    
    return $_[0]->{$_[1]};
}


# set_version ( v )
# 
# Set the 'version' attribute of this data service

sub set_version {
    
    my ($self, $v) = @_;
    
    $self->{version} = $v;
    return $self;
}


# define_subservice ( attrs... )
# 
# Define one or more subservices of this data service.  This routine cannot be
# used except as an object method.

sub define_subservice { 

    my ($self) = shift;
    
    my ($last_node);
    
    # Start by determining the class of the parent instance.  This will be
    # used for the subservice as well.
    
    my $class = ref $self;
    
    # We go through the arguments one by one.  Hashrefs define new
    # subservices, while strings add to the documentation of the subservice
    # whose definition they follow.
    
    foreach my $item (@_)
    {
	# A hashref defines a new subservice.
	
	if ( ref $item eq 'HASH' )
	{
	    $item->{parent} = $self;
	    
	    $last_node = $class->new($item)
		unless defined $item->{disabled};
	}
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "define_subservice: arguments must be hashrefs and strings";
	}
    }
    
    croak "define_subservice: arguments must include at least one hashref of attributes"
	unless $last_node;
    
    return $last_node;
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
    
    my $self = shift;
    
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
	    croak "define_path: a path definition must include a non-empty value for 'path'"
		unless defined $item->{path} && $item->{path} ne '';
	    
	    $last_node = $self->create_path_node($item, $filename, $line)
		unless defined $item->{disabled};
	}
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "define_path: arguments must be hashrefs and strings";
	}
    }
    
    croak "define_path: arguments must include at least one hashref of attributes"
	unless $last_node;
}


our (%NODE_DEF) = ( path => 'ignore',
		    collapse_path => 'single',
		    send_files => 'single',
		    file_dir => 'single',
		    class => 'single',
		    method => 'single',
		    arg => 'single',
		    ruleset => 'single',
		    output => 'list',
		    output_label => 'single',
		    output_opt => 'single',
		    uses_dbh => 'single',
		    version => 'single',
		    subvers => 'single',
		    public_access => 'single',
		    also_initialize => 'set',
		    output_param => 'single',
		    vocab_param => 'single',
		    limit_param => 'single',
		    offset_param => 'single',
		    count_param => 'single',
		    nohead_param => 'single',
		    linebreak_param => 'single',
		    showsource_param => 'single',
		    textresult_param => 'single',
		    default_limit => 'single',
		    streaming_theshold => 'single',
		    init_operation_hook => 'hook',
		    post_params_hook => 'hook',
		    post_configure_hook => 'hook',
		    post_process_hook => 'hook',
		    output_record_hook => 'hook',
		    use_cache => 'single',
		    allow_method => 'set',
		    allow_format => 'set',
		    allow_vocab => 'set',
		    doc_template => 'single',
		    doc_header => 'list',
		    doc_footer => 'list',
		    doc_layout => 'single',
		    doc_title => 'single' );

# create_path_node ( attrs, filename, line )
# 
# Create a new node representing the specified path.  Attributes are
# inherited, as follows: 'a/b/c' inherits from 'a/b', while 'a' inherits the
# defaults set for the data service as a whole.

sub create_path_node {

    my ($self, $new_attrs, $filename, $line) = @_;
    
    my $path = $new_attrs->{path};
    
    # Make sure this path was not already defined by a previous call.
    
    if ( defined $self->{path_attrs}{$path} )
    {
	my $filename = $self->{path_attrs}{$path}{_filename};
	my $line = $self->{path_attrs}{$path}{_line};
	croak "define_path: '$path' was already defined at line $line of $filename";
    }
    
    # Create a new node to hold the path attributes.
    
    my $path_attrs = { _filename => $filename, _line => $line };
    
    # If the path has a valid prefix, start with the prefix path's attributes
    # as a base.  Assume the attribute values are all valid, since they were
    # checked when the prefix path was defined (not sure if this is always
    # going to be a correct assumption).  Throw an error if the prefix path is
    # not already defined, except that we do not require the root '/' to be
    # explicitly defined.
    
    my $parent_attrs;
    
    if ( $path =~ qr{ ^ (.+) / [^/]+ }x )
    {
	$parent_attrs = $self->{path_attrs}{$1};
	croak "define_path: '$path' is not a valid path because '$1' must be defined first"
	    unless reftype $parent_attrs && reftype $parent_attrs eq 'HASH';
    }
    
    elsif ( $path =~ qr{ ^ [^/]+ $ }x )
    {
	$parent_attrs = $self->{path_attrs}{'/'};
    }
    
    elsif ( $path ne '/' )
    {
	croak "invalid path '$path'";
    }
    
    # If no parent attributes are found we start with some defaults.
    
    $parent_attrs ||= { vocab_param => 'vocab', 
			output_param => 'show',
			limit_param => 'limit',
			offset_param => 'offset',
			count_param => 'count',
			nohead_param => 'noheader',
			linebreak_param => 'linebreak',
			textresult_param => 'textresult',
			showsource_param => 'showsource',
		        allow_method => { GET => 1 } };
    
    # Now go through the parent attributes and copy into the new node.  We
    # only need to copy one level down, since the attributes are not going to
    # be any deeper than that (this may need to be revisited if the attribute
    # system gets more complicated).
    
    foreach my $key ( keys %$parent_attrs )
    {
	next unless defined $NODE_DEF{$key};
	
	if ( $NODE_DEF{$key} eq 'single' or $NODE_DEF{$key} eq 'hook' )
	{
	    $path_attrs->{$key} = $parent_attrs->{$key};
	}
	
	elsif ( $NODE_DEF{$key} eq 'set' and ref $parent_attrs->{$key} eq 'HASH' )
	{
	    $path_attrs->{$key} = { %{$parent_attrs->{$key}} };
	}
	
	elsif ( $NODE_DEF{$key} eq 'list' and ref $parent_attrs->{$key} eq 'ARRAY' )
	{
	    $path_attrs->{$key} = [ @{$parent_attrs->{$key}} ];
	}
    }
    
    # Then apply the newly specified attributes, overriding or modifying any
    # equivalent attributes inherited from the parent.
    
    foreach my $key ( keys %$new_attrs )
    {
	croak "define_path: unknown attribute '$key'"
	    unless $NODE_DEF{$key};
	
	my $value = $new_attrs->{$key};
	
	next unless defined $value;
	
	# If the attribute takes a single value, then set the value as
	# specified.
	
	if ( $NODE_DEF{$key} eq 'single' )
	{
	    $path_attrs->{$key} = $value;
	}
	
	# If it takes a hook value, then throw an error unless the value is a
	# code reference.
	
	elsif ( $NODE_DEF{$key} eq 'hook' )
	{
	    croak "define_path: ($key) invalid value '$value', must be a code ref or string"
		unless ref $value eq 'CODE' || ! ref $value;
	    $path_attrs->{$key} = $value;
	}
	
	# If the attribute takes a set value, then turn a string value
	# into a hash whose keys are the individual values.  If the value
	# begins with + or -, then add or delete values as indicated.
	# Otherwise, substitute the given set.
	
	elsif ( $NODE_DEF{$key} eq 'set' )
	{
	    my @values = ref $value eq 'ARRAY' ? @$value : split( qr{\s*,\s*}, $value );
	    
	    if ( $value =~ qr{ ^ [+-] }x )
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "define_path: ($key) invalid value '$v', must start with + or -"
			unless $v =~ qr{ ^ ([+-]) (.*) }x;
		    
		    if ( $1 eq '-' )
		    {
			delete $path_attrs->{$key}{$2};
		    }
		    
		    else
		    {
			$path_attrs->{$key}{$2} = 1;
		    }
		}
	    }
	    
	    else
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "define_path: ($key) invalid value '$v', cannot start with + or -"
			if $v =~ qr{ ^\+ | ^\- }x;
		    
		    $path_attrs->{$key}{$v} = 1;
		}
	    }
	}
	
	# If the attribute takes a list value, then turn a string value into a
	# list.  If the value begins with + or -, then add or delete values as
	# indicated.  Otherwise, substitute the given list.
	
	elsif ( $NODE_DEF{$key} eq 'list' )
	{
	    my @values = ref $value eq 'ARRAY' ? @$value : split( qr{\s*,\s*}, $value );
	    
	    if ( $value =~ qr{ ^ [+-] }x )
	    {
		foreach my $v (@values)
		{
		    next unless defined $v && $v ne '';
		    
		    croak "define_path: ($key) invalid value '$v', must start with + or -"
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
		    
		    croak "define_path: ($key) invalid value '$v', cannot start with + or -"
			if $v =~ qr{ ^\+ | ^\- }x;
		    
		    push @{$path_attrs->{$key}}, $v;
		}
	    }
	}
    }
    
    # Now check the attributes to make sure they are consistent:
    
    # Throw an error if 'class' doesn't specify an existing subclass of
    # Web::DataService::Request.
    
    my $class = $path_attrs->{class};
    
    croak "define_path: invalid class '$class', must be a subclass of 'Web::DataService::Request'"
	if defined $class and not $class->isa('Web::DataService::Request');
    
    # Throw an error if 'method' doesn't specify an existing method of this class.
    
    my $method = $path_attrs->{method};
    
    croak "define_path: '$method' must be a method of class '$class'"
	if defined $method and not $class->can($method);
    
    # Throw an error if any of the specified formats fails to match an
    # existing format.  If any of the formats has a default vocabulary, add it
    # to the vocabulary list.
    
    if ( ref $path_attrs->{allow_format} )
    {
	foreach my $f ( keys %{$path_attrs->{allow_format}} )
	{
	    croak "define_path: invalid value '$f' for format, no such format has been defined for this data service"
		unless ref $self->{format}{$f};

	    my $dv = $self->{format}{$f}{default_vocab};
	    $path_attrs->{allow_vocab}{$dv} = 1 if $dv;
	}
    }
    
    # Throw an error if any of the specified vocabularies fails to match an
    # existing vocabulary.
    
    if ( ref $path_attrs->{allow_vocab} )
    {
	foreach my $v ( keys %{$path_attrs->{vocab}} )
	{
	    croak "define_path: invalid value '$v' for vocab, no such vocabulary has been defined for this data service"
		unless ref $self->{vocab}{$v};
	}
    }
    
    # Install the node.
    
    $self->{path_attrs}{$path} = $path_attrs;
    
    # If one of the attributes is 'class', make sure that the class is
    # initialized unless we are in "one request" mode.
    
    if ( $path_attrs->{class} and not $ONE_REQUEST )
    {
	$self->initialize_class($path_attrs->{class})
    }
    
    # Now return the new node.
    
    return $path_attrs;
}


# path_defined ( path )
# 
# Return true if the specified path has been defined, false otherwise.

sub path_defined {

    my ($self, $path) = @_;
    
    return unless defined $path;
    $path = '/' if $path eq '';
    
    return $self->{path_attrs}{$path};
}



# get_path_attr ( path, key )
# 
# Return the specified attribute for the given path.

sub get_path_attr {
    
    my ($self, $path, $key) = @_;
    
    return unless defined $key;
    
    # If the path is defined and has the specified key, return the
    # corresponding value.
    
    return $self->{path_attrs}{$path}{$key};
}


# define_ruleset ( name, rule... )
# 
# Define a ruleset under the given name.  This is just a wrapper around the
# subroutine HTTP::Validate::ruleset.

sub define_ruleset {
    
    my $self = shift;
    
    $self->{validator}->define_ruleset(@_);
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


# initialize_class ( class )
# 
# If the specified class has an 'initialize' method, call it.  Recursively
# initialize its parent class as well.  But make sure that the initialization
# method is called only once for any particular class.

sub initialize_class {
    
    my ($self, $class) = @_;
    
    no strict 'refs';
    
    # If we have already initialized this class, there is nothing else we need
    # to do.
    
    return if ${"${class}::_INITIALIZED"};
    ${"${class}::_INITIALIZED"} = 1;
    
    # If this class has an immediate parent which is a subclass of
    # Web::DataService::Request, initialize it first (unless, of course, it
    # has already been initialized).
    
    foreach my $super ( @{"${class}::ISA"} )
    {
	if ( $super->isa('Web::DataService::Request') && $super ne 'Web::DataService::Request' )
	{
	    $self->initialize_class($super);
	}
    }
    
    # If this class requires that one or more other classes be initialized
    # first, then do so (unless they have already been initialized).
    
    if ( defined @{"${class}::REQUIRES_CLASS"} )
    {
	foreach my $required ( @{"${class}::REQUIRES_CLASS"} )
	{
	    $self->initialize_class($required);
	}
    }
    
    # If the class has an initialization routine, call it.
    
    if ( $class->can('initialize') )
    {
	print STDERR "Initializing $class for data service $self->{name}\n" if $DEBUG || $self->{DEBUG};
	eval { &{"${class}::initialize"}($class, $self) };
	die $@ if $@ && $@ !~ /^Can't locate object method "initialize_class"/;
    }
    
    my $a = 1; # we can stop here when debugging
}


# get_data_source ( )
# 
# Return the following pieces of information:
# - The name of the data source
# - The license under which the data is made available

sub get_data_source {
    
    my ($self) = @_;
    
    my $root_config = $self->get_config;
    my $access_time = strftime("%a %F %T GMT", gmtime);
    
    my $ds_name = $self->{name};
    my $ds_config = undef;#$config->{$name};
    $ds_config = {} unless ref $ds_config && reftype $ds_config eq 'HASH';
    
    my $result = { 
	data_provider => $ds_config->{data_provider} // $root_config->{data_provider},
	data_source => $ds_config->{data_source} // $root_config->{data_source},
	base_url => $self->get_base_url,
	access_time => $access_time };
    
    if ( defined $ds_config->{data_license} )
    {
	$result->{data_license} = $ds_config->{data_license};
	$result->{data_license_url} = $ds_config->{data_license_url};
    }
    
    elsif ( defined $root_config->{data_license} )
    {
	$result->{data_license} = $root_config->{data_license};
	$result->{data_license_url} = $root_config->{data_license_url};
    }
    
    $result->{data_provider} //= $result->{data_source};
    $result->{data_source} //= $result->{data_provider};
    
    $result->{data_provider} //= '';
    $result->{data_source} //= '';
    
    return $result;
}


# call_hook ( hook, request, arg... )
# 
# Call the specified hook.  If it is specified as a code reference, call it
# with the request as the first parameter followed by any subsequent
# arguments.  If it is a string, call it as a method of the request object. 

sub call_hook {
    
    my ($self, $hook, $request, @args) = @_;
    
    if ( ref $hook eq 'CODE' )
    {
	return &$hook($request, $self, @args);
    }
    
    else
    {
	return $request->$hook($self, @args);
    }
}



# new_request ( outer, path )
# 
# Generate a new request object, using the given parameters.  $outer should be
# a reference to an "outer" request object that was generated by the
# underlying framework (i.e. Dancer or Mojolicious) or undef if there is
# none.  $path should be the path which is being requested.

sub new_request {

    my ($self, $outer, $path) = @_;
    
    # A valid path must be given.
    
    croak "a valid path must be provided" unless defined $path && !ref $path;
    
    # First, check to see whether this path should be handled by one of the
    # subservices or by the main data service.  At the same time, extract the
    # sub-path (typically by removing the prefix corresponding to the selected
    # sub-service) so that we will be able to properly process the request.
    
    my ($ds, $sub_path) = $self->select_service($path);
    
    # Then generate a new request using this path.
    
    my $request = Web::DataService::Request->new($ds, $outer, $sub_path);
    
    # Return the new request object.
    
    return $request;
}


# get_base_path ( )
# 
# Return the base path for the current data service, derived from the path
# prefix.  For example, if the path prefix is 'data', the base path is
# '/data/'. 

sub get_base_path {
    
    my ($self) = @_;
    
    my $base = '/';
    $base .= $self->{path_prefix} . '/'
	if defined $self->{path_prefix} && $self->{path_prefix} ne '';
    
    return $base;
}


# select_service ( path )
# 
# Returns the data service instance for this path, followed by the processed
# path (i.e. with the prefix removed).

sub select_service {
    
    my ($self, $path) = @_;
    
    # If there are any subservices, check them first.
    
    foreach my $ss ( @{$self->{subservice_list}} )
    {
	if ( defined $ss->{path_re} && $path =~ $ss->{path_re} )
	{
	    return ($ss, $1);
	}
    }
    
    # Otherwise, try the main service.
    
    if ( $path =~ $self->{path_re} )
    {
	return ($self, $1);
    }
    
    # Otherwise, return the path unchanged.
    
    return ($self, $path);
}


my %CODE_STRING = ( 400 => "Bad Request", 
		    404 => "Not Found", 
		    415 => "Invalid Media Type",
		    500 => "Server Error" );

# error_result ( request, error )
# 
# Send an error response back to the client.

sub error_result {

    my ($ds, $request, $error) = @_;
    
    my $format = $request->{format};
    
    my ($code);
    my (@errors, @warnings);
    
    # If the error is actually a response object from HTTP::Validate, then
    # extract the error and warning messages.  In this case, the error code
    # should be "400 bad request".
    
    if ( ref $error eq 'HTTP::Validate::Result' )
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
	warn $error;
	@errors = "A server error occurred.  Please contact the server administrator.";
    }
    
    # If the format is 'json', render the response as a JSON object.
    
    if ( defined $format && $format eq 'json' )
    {
	$error = '"status_code": ' . $code;
	$error .= ",\n" . json_list_value("errors", @errors);
	$error .= ",\n" . json_list_value("warnings", @warnings) if @warnings;
	
	$ds->{foundation_plugin}->set_content_type($request, 'application/json');
	$ds->{foundation_plugin}->set_cors_header($request, "*");
	$ds->{foundation_plugin}->set_status($request, $code);
	return "{ $error }";
    }
    
    # Otherwise, generate a generic HTML response (we'll add template
    # capability later...)
    
    else
    {
	my $text = $CODE_STRING{$code};
	my $error = "<ul>\n";
	my $warning = '';
	
	$error .= "<li>$_</li>\n" foreach @errors;
	$error .= "</ul>\n";
	
	if ( @warnings )
	{
	    $warning .= "<h2>Warnings:</h2>\n<ul>\n";
	    $warning .= "<li>$_</li>\n" foreach @warnings;
	    $warning .= "</ul>\n";
	}
	
	my $body = <<END_BODY;
<html><head><title>$code $text</title></head>
<body><h1>$code $text</h1>
$error
$warning
</body></html>
END_BODY
    
	$ds->{foundation_plugin}->set_content_type($request, 'text/html');
	$ds->{foundation_plugin}->set_cors_header($request, "*");
	$ds->{foundation_plugin}->set_status($request, $code);
	return $body;
    }
}


# generate_path_link ( path, title )
# 
# Generate a link in Pod format to the documentation for the given path.  If
# $title is defined, use that as the link title.  Otherwise, if the path has a
# 'doc_title' attribute, use that.
# 
# If something goes wrong, generate a warning and return the empty string.

sub generate_path_link {
    
    my ($self, $path, $title) = @_;
    
    return '' unless defined $path && $path ne '';
    
    # Make sure this path is defined.
    
    my $path_attrs = $self->{path_attrs}{$path};
    
    unless ( $path_attrs )
    {
	warn "cannot generate link to unknown path '$path'";
	return '';
    }
    
    # Get the correct title for the link.
    
    $title //= $path_attrs->{doc_title};
    
    # Transform the path into a valid URL.
    
    $path = $self->get_base_path . $path;
    
    unless ( $path =~ qr{/$}x )
    {
	$path .= "_doc.html";
    }
    
    if ( defined $title && $title ne '' )
    {
	return "L<$title|$path>";
    }
    
    else
    {
	return "L<$path>";
    }
}


sub debug {

    my ($self) = @_;
    
    return $DEBUG || $self->{DEBUG};
}

1;
