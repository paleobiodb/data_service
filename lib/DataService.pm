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

use Carp qw( croak );
use Scalar::Util qw( reftype blessed weaken );
use POSIX qw( strftime );
use Try::Tiny;
use Time::HiRes;

use Web::DataService::Format;
use Web::DataService::Vocabulary;
use Web::DataService::Cache;
use Web::DataService::Request;
use Web::DataService::Output;
use Web::DataService::PodParser;
use Web::DataService::JSON qw(json_list_value);

use HTTP::Validate qw( :validators );


HTTP::Validate->VERSION(0.35);

#use Dancer qw( :syntax );
use Dancer::Plugin;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;

BEGIN {
    our (@KEYWORDS) = qw(define_vocab valid_vocab document_vocab
			 define_format document_format
			 define_cache
			 define_set valid_set document_set
			 define_path document_path path_defined
			 define_ruleset define_output_map define_block
			 get_config get_dbh
			 initialize_class can_execute_path execute_path error_result);
    
    our (@EXPORT_OK) = (@KEYWORDS, @HTTP::Validate::VALIDATORS);
    
    our (%EXPORT_TAGS) = (
	keywords => \@KEYWORDS,
        validators => \@HTTP::Validate::VALIDATORS
    );
}


# Create a default instance, for when this module is used in a
# non-object-oriented fashion.

my $DEFAULT_INSTANCE = __PACKAGE__->new({ name => 'Default' });

my $DEFAULT_COUNT = 0;

our @HTTP_METHOD_LIST = ('GET', 'HEAD', 'POST', 'PUT', 'DELETE');


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
    
    my $path_prefix = $options->{path_prefix} || '/';
    my $public_access = $options->{public_access} || $config->{public_access};
    my $default_limit = $options->{default_limit} || $config->{default_limit} || 500;
    my $streaming_threshold = $options->{streaming_threshold} || $config->{streaming_threshold} || 20480;
    my $name = $options->{name} || $config->{name};
    
    unless ( $name )
    {
	$DEFAULT_COUNT++;
	$name = 'Data Service' . ( $DEFAULT_COUNT > 1 ? " $DEFAULT_COUNT" : "" );
    }
    
    # Create a new HTTP::Validate object so that we can do parameter
    # validations.
    
    my $validator = HTTP::Validate->new();
    
    $validator->validation_settings(allow_unrecognized => 1) if $options->{allow_unrecognized};
    
    # Create a new DataService object, and return it:
    
    my $instance = {
		    name => $name,
		    path_prefix => $path_prefix,
		    validator => $validator,
		    public_access => $public_access,
		    default_limit => $default_limit,
		    # streaming_available => server_supports_streaming,
		    streaming_threshold => $streaming_threshold,
		    path_attrs => {},
		    vocab => { 'default' => 
			       { name => 'default', use_field_names => 1, _default => 1,
				 doc => "The default vocabulary consists of the underlying field names" } },
		    vocab_list => [ 'default' ],
		    format => {},
		    format_list => [],
		   };
    
    $instance->{DEBUG} = 1 if $config->{ds_debug};
    
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
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
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
	    croak "define_path: a path definition must include the attribute 'path'"
		unless defined $item->{path} and $item->{path} ne '';
	    
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

register 'define_path' => \&define_path;


our (%NODE_DEF) = ( path => 'ignore',
		    class => 'single',
		    method => 'single',
		    arg => 'single',
		    ruleset => 'single',
		    base_output => 'list',
		    doc_output => 'list',
		    output => 'single',
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
		    init_request_hook => 'hook',
		    post_params_hook => 'hook',
		    post_configure_hook => 'hook',
		    post_operation_hook => 'hook',
		    pre_output_hook => 'hook',
		    use_cache => 'single',
		    allow_method => 'set',
		    allow_format => 'set',
		    allow_vocab => 'set',
		    doc_file => 'single',
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
    
    if ( $path_attrs->{class} and not $self->{ONE_REQUEST} )
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


# define_ruleset ( name, rule... )
# 
# Define a ruleset under the given name.  This is just a wrapper around the
# subroutine HTTP::Validate::ruleset.

sub define_ruleset {
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
    unshift @_, $self->{validator};
    
    goto &HTTP::Validate::define_ruleset;
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
# method is called only once for any particular class.  It is passed a
# reference to the data service, a database handle, and the Dancer
# configuration hash.

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
	print STDERR "Initializing $class for data service $self->{name}\n" if $self->{DEBUG};
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
    
    my $config = Dancer::config();
    my $access_time = strftime("%a %F %T GMT", gmtime);
    
    return { name => $config->{data_source}, 
	     data_provider => $config->{data_provider} || $config->{data_source},
	     data_source => $config->{data_source} || '',
	     base_url => Dancer::request->uri_base(), 
	     license => $config->{data_license}, 
	     license_url => $config->{data_license_url},
	     access_time => $access_time };
}


# get_config ( )
# 
# Return the Dancer configuration object, which provides access to the
# configuration directives for this data service.

sub get_config {
    
    my $config = Dancer::config();
    return $config;
}


# get_dbh ( )
# 
# Return a database handle, assuming that the proper configuration directives
# were included to enable a connection to be made.

sub get_dbh {
    
    my $dbh = database();
    return $dbh;
}


# get_request_url ( )
# 
# Return the URL that generated the current request

sub get_request_url {
    
    my $request_url = Dancer::request->uri();
    return $request_url;
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
	&$hook($request, $self, @args);
    }
    
    else
    {
	$request->$hook($self, @args);
    }
}


# can_execute_path ( path, format )
# 
# Return true if the path can be used for a request, i.e. if it has a class
# and operation defined.  Return false otherwise.

sub can_execute_path {
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
    my ($path) = @_;
    
    # Now check whether we have the necessary attributes.
    
    return defined $self->{path_attrs}{$path}{class} &&
	   defined $self->{path_attrs}{$path}{method};
}


# execute_path ( path, format )
# 
# Execute the operation corresponding to the attributes of the given path, and
# return the resulting data in the specified format.

sub execute_path {
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
    my ($path, $format) = @_;
    
    my ($request, $req_output);
    
    # Do all of the processing in a try block, so that if an error occurs we
    # can respond with an appropriate error page.
    
    try {
	
	$DB::single = 1;
	
	my $path_attrs = $self->{path_attrs}{$path};
	my $params = Dancer::params;
	
	# Create a new object to represent this request, and bless it into the
	# correct class.
	
	$request = { ds => $self,
		     path => $path,
		     format => $format,
		     class => $path_attrs->{class},
		     method => $path_attrs->{method},
		     arg => $path_attrs->{arg},
		     public_access => $path_attrs->{public_access}
		   };
	
	bless $request, $path_attrs->{class};
	
	# If an init_request hook was specified for this path, call it now.
	
	$self->call_hook($path_attrs->{init_request_hook}, $request, $params)
	    if $path_attrs->{init_request_hook};
	
	# Then check to see that the specified format is valid for the
	# specified path.
	
	unless ( defined $format && ref $self->{format}{$format} &&
		 ! $self->{format}{$format}{disabled} &&
		 $path_attrs->{allow_format}{$format} )
	{
	    return $self->error_result($path, $format, "415")
	}
	
	# If we are in 'one request' mode, initialize the class plus all of
	# the classes it requires.  If we are not in this mode, then all of
	# the classes will have been previously initialized.
	
	if ( $self->{ONE_REQUEST} )
	{
	    $self->initialize_class($request->{class});
	}
	
	# Check to see if there is a ruleset corresponding to this path.  If
	# not, then the request is rejected.
	
	$request->{rs_name} //= $self->determine_ruleset($path, $path_attrs->{ruleset})
	    or die "No ruleset could be found for path $path";
	
	if ( $request->{rs_name} )
	{
	    my $context = { ds => $self, request => $request };
	    
	    my $result = $self->{validator}->check_params($request->{rs_name}, $context, $params);
	    
	    if ( $result->errors )
	    {
		return $self->error_result($path, $format, $result);
	    }
	    
	    elsif ( $result->warnings )
	    {
		$request->add_warning($result->warnings);
	    }
	    
	    $request->{valid} = $result;
	    $request->{params} = $result->values;
	}
	
	else
	{
	    $request->{valid} = undef;
	    $request->{params} = $params;
	}
	
	# If a post_params_hook is defined for this path, call it.
	
	$self->call_hook($path_attrs->{post_params_hook}, $request)
	    if $path_attrs->{post_params_hook};
	
	# Determine the result limit and offset, if any.
	
	$request->{result_limit} = 
	    defined $path_attrs->{limit_param} &&
	    defined $request->{params}{$path_attrs->{limit_param}}
		? $request->{params}{$path_attrs->{limit_param}}
		    : $path_attrs->{default_limit} || $self->{default_limit} || 'all';
	
	$request->{result_offset} = 
	    defined $path_attrs->{offset_param} &&
	    defined $request->{params}{$path_attrs->{offset_param}}
		? $request->{params}{$path_attrs->{offset_param}} : 0;
	
	# Set the vocabulary and output section list using the validated
	# parameters, so that we can properly configure the output.
	
	$request->{vocab} = $request->{params}{$path_attrs->{vocab_param}} || 
	    $self->{format}{$format}{default_vocab} || $self->{vocab_list}[0];
	
	$request->{output_name} = $self->determine_output_name($path, $path_attrs->{output});
	
	# Determine whether we should show the optional header information in
	# the result.
	
	$request->{display_header} = $request->{params}{$path_attrs->{nohead_param}} ? 0 : 1;
	$request->{display_source} = $request->{params}{$path_attrs->{showsource_param}} ? 1 : 0;
	$request->{display_counts} = $request->{params}{$path_attrs->{count_param}} ? 1 : 0;
	$request->{linebreak_cr} = 
	    $request->{params}{$path_attrs->{linebreak_param}} &&
		$request->{params}{$path_attrs->{linebreak_param}} eq 'cr' ? 1 : 0;
	
	# Set the HTTP response headers appropriately for this request.
	
	$self->set_response_headers($request);
	
	# Now that the parameters have been processed, we can configure the
	# output.  This tells us what information we have been requested
	# to display, and how to query for it.
	
	$self->configure_output($request);
	
	# If a post_configure_hook is defined for this path, call it.
	
	$self->call_hook($path_attrs->{post_configure_hook}, $request)
	    if $path_attrs->{post_configure_hook};
	
	# Prepare to time the query operation.
	
	my (@starttime) = Time::HiRes::gettimeofday();
	
	# Now execute the query operation.  This is the central step of this
	# entire routine; everything before and after is in support of this
	# call.
	
	my $method = $request->{method};
	my $arg = $request->{arg};
	
	$request->$method($arg);
	
	# Determine how long the query took.
	
	my (@endtime) = Time::HiRes::gettimeofday();
	$request->{elapsed} = Time::HiRes::tv_interval(\@starttime, \@endtime);
	
	# If a post_operation_hook is defined for this path, call it.
	
	$self->call_hook($path_attrs->{post_operation_hook}, $request)
	    if $path_attrs->{post_operation_hook};
	
	# If a pre_output_hook is defined for this path, save it in the
	# request object so it can be called at the appropriate time.
	
	$request->{pre_output_hook} = $path_attrs->{pre_output_hook}
	    if $path_attrs->{pre_output_hook};
	
	# Then we use the output configuration and the result of the query
	# operation to generate the actual output.  How we do this depends
	# upon how the query operation chooses to return its data.  It must
	# set one of the following fields in the request object, as described:
	# 
	# main_data		A scalar, containing data which is to be 
	#			returned as-is without further processing.
	# 
	# main_record		A hashref, representing a single record to be
	#			returned according to the output format.
	# 
	# main_result		A list of hashrefs, representing multiple
	#			records to be returned according to the output
	# 			format.
	# 
	# main_sth		A DBI statement handle, from which all 
	#			records that can be read should be returned
	#			according to the output format.
	# 
	# It is okay for main_result and main_sth to both be set, in which
	# case the records in the former will be sent first and then the
	# latter will be read.
	
	if ( ref $request->{main_record} )
	{
	    return $self->generate_single_result($request);
	}
	
	elsif ( ref $request->{main_sth} or ref $request->{main_result} )
	{
	    my $threshold = $self->{path_attrs}{$path}{streaming_threshold} || $self->{streaming_threshold}
		if $self->{streaming_available} and not $request->{do_not_stream};
	    
	    return $self->generate_compound_result($request, $threshold);
	}
	
	elsif ( defined $request->{main_data} )
	{
	    return $request->{main_data};
	}
	
	# If none of these fields are set, then the result set is empty.
	
	else
	{
	    return $self->generate_empty_result($request);
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
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
    my ($path, $format) = @_;
    
    # Do all of the processing in a 'try' block so that if an error occurs we
    # can return an appropriate message.
    
    try {
	
	my $path_attrs = $self->{path_attrs}{$path};
	my $class = $self->{path_attrs}{$path}{class};
	
	$DB::single = 1;
	
	# If we are in 'one request' mode, initialize the class plus all of
	# the classes it requires.
	
	if ( $self->{ONE_REQUEST} )
	{
	    if ( ref $path_attrs->{also_initialize} eq 'ARRAY' )
	    {
		foreach my $c ( @{$path_attrs->{also_initialize}} )
		{
		    $self->initialize_class($c);
		}
	    }
	    
	    $self->initialize_class($class);
	}
	
	# We start by determining the filename for the documentation template.  If
	# the filename was not explicitly specified, try the path with '_doc.tt'
	# appended.  If that does not exist, try appending '/index.tt'.
	
	my $viewdir = Dancer::config->{views};
	my $doc_file = $path_attrs->{doc_file};
	
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
	
	unless ( $doc_file && -r "$viewdir/doc/$doc_file" )
	{
	    $doc_file = $path_attrs->{doc_error_file} || $self->{doc_error_file} || 'doc_error.tt';
	}
	
	my $doc_title = $path_attrs->{doc_title};
	my $ds_version = $path_attrs->{version} || $self->{version} || '';
	my $ds_subvers = $path_attrs->{subvers} || $self->{subvers} || '';
	
	unless ( $doc_title )
	{
	    $doc_title = $path;
	    $doc_title =~ s/^$ds_version//;
	}
	
	# Then assemble the variables used to fill in the template:
	
	my $vars = { path => $path,
		     doc_title => $doc_title,
		     ds_version => $ds_version,
		     ds_subvers => $ds_subvers,
		     vocab_param => $path_attrs->{vocab_param},
		     output_param => $path_attrs->{output_param} };
	
	# Add the documentation for the parameters.  If no corresponding ruleset
	# is found, then state that no parameters are accepted.
	
	my $ruleset = $self->determine_ruleset($path, $path_attrs->{ruleset});
	
	$vars->{param_doc} = 
	    $ruleset ? $self->{validator}->document_params($ruleset)
		: "I<This path does not take any parameters>";
	
	# Document which methods are valid for this path.
	
	my @http_methods = $self->list_http_methods($path);
	$vars->{http_method_list} = join(', ', map { "C<$_>" } @http_methods);
	
	# Add the documentation for the response.  If no 'allow_vocab' attribute
	# was given for this path, then all vocabularies are allowed.
	
	my $output_name = $self->determine_output_name($path, $path_attrs->{output});
	
	$vars->{response_doc} = $self->document_response($path, $output_name);
	
	
	my @fixed_blocks = $self->list_fixed_blocks($path, $output_name);
	my @optional_blocks = $self->list_optional_blocks($path, $output_name);
	
	$vars->{fixed_blocks} = scalar(@fixed_blocks);
	$vars->{fixed_list} = join(', ', map { "I<$_>" } @fixed_blocks);
	$vars->{optional_blocks} = scalar(@optional_blocks);
	$vars->{optional_list} = join(', ', map { "I<$_>" } @optional_blocks);
	
	# Add the documentation for the allowed formats and vocabularies.
	
	$vars->{format_doc} = $self->document_allowed_formats($path);
	$vars->{vocab_doc} = $self->document_allowed_vocab($path);
	
	# Add the labels and links for the navigation trail.
	
	my @path = split qr{/}, $path;
	my $link = $self->{path_prefix} . shift @path;
	my @trail = ($link, $link);
	
	foreach my $comp (@path)
	{
	    $link .= "/$comp";
	    push @trail, ($comp, $link);
	}
	
	$vars->{trail_list} = \@trail;
	
	# Now select the appropriate layout and execute the template.
	
	my $doc_layout = $path_attrs->{doc_layout} || $self->{doc_layout} || 'doc_main.tt';
	
	Dancer::set layout => $doc_layout;
	
	my $doc_string = Dancer::template( "doc/$doc_file", $vars );
	
	# All documentation is public, so set the maximally permissive CORS header.
	
	Dancer::header "Access-Control-Allow-Origin" => "*";
	
	# If POD format was requested, return the documentation as is.
	
	if ( defined $format && $format eq 'pod' )
	{
	    Dancer::content_type 'text/plain';
	    return $doc_string;
	}
	
	# Otherwise, convert the POD to HTML using the PodParser and return the result.
	
	else
	{
	    my $parser = Web::DataService::PodParser->new();
	    
	    $parser->parse_pod($doc_string);
	    
	    my $doc_html = $parser->generate_html({ css => '/data/css/dsdoc.css', tables => 1 });
	    
	    Dancer::content_type 'text/html';
	    return $doc_html;
	}
    }
    
    catch {
	return $self->error_result($path, $format, $_);
    };
}
	
register 'document_path' => \&document_path;


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
    
    if ( defined $self->{path_prefix} && $self->{path_prefix} ne '' )
    {
	$path = "$self->{path_prefix}$path";
    }
    
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


# determine_ruleset ( path, ruleset )
# 
# Determine the ruleset that should apply to the given path.  If $ruleset is
# given, then use that if it is defined or throw an exception if not.
# Otherwise, try the path with slashes turned into commas.

sub determine_ruleset {
    
    my ($self, $path, $ruleset) = @_;
    
    my $validator = $self->{validator};
    
    # If a ruleset name was explicitly given, then use that or throw an
    # exception if not defined.
    
    if ( defined $ruleset and $ruleset ne '' )
    {
	croak "unknown ruleset '$ruleset' for path $path"
	    unless $validator->ruleset_defined($ruleset);
	
	return $ruleset;
    }
    
    # If the ruleset was explicitly specified as '', do not process the
    # parameters for this path.
    
    elsif ( defined $ruleset )
    {
	return;
    }
    
    # Otherwise, try the path with / replaced by :.  If that is not defined,
    # then return empty.  The parameters for this path will not be processed.
    
    else
    {
	$path =~ s{/}{:}g;
	
	return $path if $validator->ruleset_defined($path);
	return; # empty if not defined.
    }
}


# determine_output_name {
# 
# If an explicit output name is given, then use that if it corresponds to a
# defined map or block or throw an error otherwise.  If not, try the path with
# slashes turned into colons and ':output_map' appended.

sub determine_output_name {

    my ($self, $path, $output_name) = @_;
    
    # If an output name was explicitly given, then see if it corresponds to a
    # known output map or output block.  Otherwise, throw an exception.
    
    if ( defined $output_name and $output_name ne '' )
    {
	if ( ref $self->{set}{$output_name} eq 'Web::DataService::Set' )
	{
	    return $output_name;
	}
	
	elsif ( ref $self->{block}{$output_name} eq 'Web::DataService::Block' )
	{
	    return $output_name;
	}
	
	else
	{
	    croak "no block or set is defined as '$output_name' for path $path";
	}
    }
    
    # If the outputset was explicitly specified as '', then this path does not
    # use one.
    
    elsif ( defined $output_name )
    {
	return;
    }
    
    # Otherwise, try the path with / changed to : but return empty if this is
    # not found.  Not all paths need an output set.
    
    else
    {
	$path =~ s{/}{:}g;
	$path .= ':output_map';
	
	return "$path:output_map" if ref $self->{set}{"$path:output_map"} eq 'Web::DataService::Set';
	return "$path:basic" if ref $self->{block}{"$path:basic"} eq 'Web::DataService::Block';
	return; # empty if not defined.
    }   
}


sub set_response_headers {
    
    my ($self, $request) = @_;
    
    # If this is a public-access data service, we add a universal CORS header.
    # At some point we need to add provision for authenticated access.
    
    my $path = $request->{path};
    
    if ( $self->{path_attrs}{$path}{public_access} )
    {
	Dancer::header "Access-Control-Allow-Origin" => "*";
    }
    
    # If the parameter 'textresult' was given, set the content type to
    # 'text/plain' which will cause the response to be displayed in a browser
    # tab. 
    
    if ( $request->{params}{textresult} )
    {
	Dancer::content_type 'text/plain';
    }
    
    # Otherwise, set the content type based on the format.
    
    else
    {
	my $format = $request->{format};
	my $ct = $self->{format}{$format}{content_type} || 'text/plain';
	my $disp = $self->{format}{$format}{disposition};
	
	Dancer::content_type $ct;
	
	if ( defined $disp && $disp eq 'attachment' )
	{
	    Dancer::header 'Content-Disposition' => qq{attachment; filename="paleobiodb.$format"};
	}
    }
    
    return;
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
	#error("Error on path $path: $error");
	warn $error;
	@errors = "A server error occurred.  Please contact the server administrator.";
    }
    
    # If the format is 'json', render the response as a JSON object.
    
    if ( $format eq 'json' )
    {
	$error = '"status_code": ' . $code;
	$error .= ",\n" . json_list_value("errors", @errors);
	$error .= ",\n" . json_list_value("warnings", @warnings) if @warnings;
	
	Dancer::content_type('application/json');
	Dancer::header "Access-Control-Allow-Origin" => "*";
	Dancer::status($code);
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
    
	Dancer::content_type('text/html');
	Dancer::header "Access-Control-Allow-Origin" => "*";
	Dancer::status($code);
	return $body;
    }
}


register_plugin;

1;
