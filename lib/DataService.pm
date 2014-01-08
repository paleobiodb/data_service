#
# DataService.pm
# 
# This is a first cut at a data service application framework, built on top of
# Dancer.pm.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


use strict;

package Web::DataService;

use Web::DataService::Query;
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
    
    my $selector = $options->{response_selector} || 'fields';
    my $needs_dbh = $options->{needs_dbh};
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
		    validator => $validator,
		    response_selector => $selector,
		    public_access => $public_access,
		    needs_dbh => $needs_dbh,
		    default_limit => $default_limit,
		    stream_threshold => $stream_threshold,
		    path_attrs => {},
		    vocabulary => {},
		   };
    
    # Return the new instance
    
    bless $instance, $class;
    return $instance;
}


# define_directory ( path, attrs... )
# 
# Set up a "directory" entry, representing a partial URL path.  All paths
# that extend this one will inherit any attributes defined here.  This partial
# path may (or may not) correspond to a documentation page.

sub define_directory {
    
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
	    $last_node = $self->define_node($item, $filename, $line, 1);
	}
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_directory' must be hashrefs and strings";
	}
    }
    
    croak "the arguments to 'define_directory' must include a hashref of attributes"
	unless $last_node;
}

register 'define_directory' => \&define_directory;

# define_path ( path, attrs... )
# 
# Set up a "path" entry, representing a complete path.  This path should have
# a documentation page, but if one is not defined a template page will be used
# along with any documentation strings given in this call.

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
	    $last_node = $self->define_node($item, $filename, $line);
	}
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_directory' must be hashrefs and strings";
	}
    }
    
    croak "the arguments to 'define_directory' must include a hashref of attributes"
	unless $last_node;
}

register 'define_path' => \&define_path;


# define_node ( attrs, filename, line, is_directory )
# 
# Define a node according to the given parameters.

sub define_node {

    my ($self, $attrs, $filename, $line, $is_directory) = @_;
    
    # Make sure the attributes include 'path'.
    
    my $path = $attrs->{path};
    
    croak "the attributes must include 'path'" unless defined $path;
    
    # Make sure this path was not already defined by a previous call.
    
    if ( defined $self->{path_attrs}{$path} )
    {
	my $filename = $self->{path_attrs}{$path}{filename};
	my $line = $self->{path_attrs}{$path}{line};
	croak "path '$path' was already defined at line $line of $filename";
    }
    
    # Now set the attributes.
    
    $self->{path_attrs}{$path} = $attrs;
    $self->{path_attrs}{$path}{filename} = $filename;
    $self->{path_attrs}{$path}{line} = $line;
    $self->{is_directory}{$path} = 1 if $is_directory;
    
    # See if a partial path for this route was itself defined as a directory.
    # If so, link it up so that attributes will inherit.
    
    if ( my $parent = $self->find_parent($path) )
    {
	$self->{parent}{$path} = $parent;
    }
    
    # If one of the attributes is 'class', make sure that the class is
    # configured. 
    
    if ( $attrs->{class} )
    {
	$self->configure_class($attrs->{class})
    }
    
    croak "the arguments must include a hashref of attributes"
	unless $last_node;
}


# find_parent ( path )
# 
# This utility routine finds the longest partial path of the given route that
# was defined as a directory.  If one is found, it is returned.  Otherwise,
# returns false.

sub find_parent {

    my ($self, $path) = @_;
    
    # Iteratively remove the last path component and see if it matches a
    # defined directory.
    
    while ( $path =~ qr{ ^ (.*) / [^/]+ $ }xs )
    {
	return $1 if $self->{is_directory}{$1};
	$path = $1;
    }
    
    # Check '/' as a last resort, otherwise give up.
    
    return "/" if $path ne "/" && $self->{is_directory}{"/"};
    return;
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


# path_attr ( path, key )
# 
# Return the specified attribute for the given path.  If not found in the
# record for the path, look in the records corresponding to its parents if
# any. 

sub path_attr {
    
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
    
    if ( exists $self->{path_attrs}{$path}{$key} )
    {
	return $self->{path_attrs}{$path}{$key};
    }
    
    # Otherwise, we try to find a parent.
    
    my $parent = $self->{parent}{$path} || $self->find_parent($path);
    
    # Recursively check the parent and its parents.  If any of them has the
    # specified key, return the corresponding value.
    
    while ( $parent )
    {
	if ( exists $self->{path_attrs}{$parent}{$key} )
	{
	    return $self->{path_attrs}{$parent}{$key};
	}
	
	else
	{
	    $parent = $self->{parent}{$parent};
	}
    }
    
    # If no value can be found, give up.
    
    return;
}


# define_vocabulary ( attrs... )
# 
# Define one or more vocabularies of field names for data service responses.

sub define_vocabulary {

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
	    
	    croak "the attributes must include 'name'" unless defined $name;
	    
	    # Make sure this vocabulary was not already defined by a previous call,
	    # and set the attributes as specified.
	    
	    croak "vocabulary '$name' was already defined" if defined $self->{vocabulary}{$name};
	    
	    # Now set the attributes.
	    
	    $self->{vocabulary}{$name} = $item;
	    $last_node = $item;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "the arguments to 'define_vocabulary' must be hashrefs and strings";
	}
    }    
    
    croak "the arguments must include a hashref of attributes"
	unless $last_node;
}


# document_vocabulary ( )
# 
# Return a string containing POD documentation of the vocabulary
# possibilities. 

sub document_vocabulary {
    
    my ($self) = @_;
    
    return '' unless ref $self->{vocabulary} eq 'ARRAY';
    
    my $doc = "=over 4\n\n";
    
    foreach my $v (@{$self->{vocabulary}})
    {
	$doc .= "=item $v->{name}\n\n";
	$doc .= "$v->{doc}\n\n";
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
	    
	    # Now set the attributes.
	    
	    $self->{format}{$name} = $item;
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


# document_formats ( )
# 
# Return a string containing POD documentation of the vocabulary
# possibilities. 

sub document_formats {
    
    my ($self) = @_;
    
    return '' unless ref $self->{format} eq 'ARRAY';
    
    my $doc = "=over 4\n\n";
    
    foreach my $v (@{$self->{format}})
    {
	$doc .= "=item $v->{name}\n\n";
	$doc .= "$v->{doc}\n\n";
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
    # were not called from a subclass of Web::DataService::Query, then store
    # this information under Web::DataService::Query so that it will be
    # available to all packages.
    
    my ($package) = caller;
    
    unless ( $package->isa('Web::DataService::Query') )
    {
	$package = 'Web::DataService::Query';
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
    
    # If the class has a configuration method, call it.
    
    if ( $class->can('configure') )
    {
	print STDERR "Configuring $class for data service $self->{name}\n" if $PBDB_Data::DEBUG;
	$class->configure($self, Dancer::config, database());
    }
}


# send_documentation ( path, attrs )
# 
# Respond with a documentation message corresponding to the specified path.
# If attrbutes have been previously registered for this path using either of
# the functions setup_route or setup_directory, then those attributes are used
# to select and format the documentation.  Otherwise, if a corresponding
# documentation template is found, it will be used.  The attributes can be
# overridden using the second parameter, which must be a hash ref.

sub generate_documentation {
    
    my ($self, $path, $attrs);
    
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
    
    # Now extract the proper attributes.
    
    my $format = $attrs->{format} || $self->path_attr($path, 'format') || 'html';
    
    my $class = $attrs->{class} || $self->path_attr($path, 'class');
    my $op = $attrs->{op} || $self->path_attr($path, 'op');
    my $ruleset = $attrs->{ruleset} || $self->path_attr($path, 'ruleset') || $path;
    my $docresp = $attrs->{docresp} || $self->path_attr($path, 'docresp');
    my $doctitle = $attrs->{doctitle} || $self->path_attr($path, 'doctitle');
    my $docfile = $attrs->{docfile} || $self->path_attr($path, 'docfile');
    my $doclayout = $attrs->{doclayout} || $self->path_attr($path, 'doclayout') || 'doc_main.tt';
    my $docerror = $attrs->{docerror} || $self->path_attr($path, 'docerror') || 'doc_error.tt';
    
    my ($version) = $path =~ qr{ ^ ( \d+ \. \d+ ) }xs;
    
    my $validator = $self->{validator};
    
    # Set the title and documentation filename, if they were not already
    # defined.
    
    if ( $self->{is_directory}{$path} )
    {
	$doctitle //= "/data$path/";
	$docfile //= "${path}/index.tt";
    }
    
    else
    {
	$doctitle //= "/data$path";
	$docfile //= "${path}_doc.tt";
	$docresp //= $op;
    }
    
    # All documentation is public, so set the maximally permissive CORS header.
    
    Dancer::header "Access-Control-Allow-Origin" => "*";
    
    # Now pull up the documentation file for the given path and assemble it
    # together with elements describing the parameters and output fields.
    
    Dancer::set layout => $doclayout;
    
    my $viewdir = Dancer::config->{views};
    
    unless ( -e "$viewdir/doc/$docfile" )
    {
	$docfile = $docerror;
    }
    
    my $param_doc = $validator->document_params($ruleset) if $ruleset;
    my $response_doc = $class->document_response($docresp) if $class && $docresp;
    
    my $doc_string = Dancer::template( "doc/$docfile", { version => $version, 
						param_doc => $param_doc,
						response_doc => $response_doc,
						title => $doctitle });
    
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

register 'send_documentation' => \&send_documentation;


# execute_operation ( path, attrs )
# 
# Execute the operation corresponding to the attributes of the given path.
# These attributes can be overridden by means of the second argument, which
# must be a hash ref.

register 'execute_operation' => sub {
    
    my ($self, $path, $attrs);
    
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
    
    my ($format, $class, $op);
    my ($request, $result);
    
    try {
	
	$DB::single = 1;
	
	# Determine the attributes that will define this operation.
	
	$format = $attrs->{format} || $self->path_attr($path, 'format');
	$class = $attrs->{class} || $self->path_attr($path, 'class');
	$op = $attrs->{op} || $self->path_attr($path, 'op');
	
	# Return an error result if we are missing any of the necessary
	# attributes. 
	
	croak "execute_operation: format is undefined" unless $format;
	croak "execute_operation: class is undefined" unless $class;
	croak "execute_operation: op is undefined" unless $op;
	
	# Determine additional attributes relevant to this operation type.
	
	my $validator = $self->{validator};
	my $ruleset = $attrs->{ruleset} || $self->path_attr($path, 'ruleset');
	my $output = $attrs->{output} // $self->path_attr($path, 'output');
	my $arg = $attrs->{arg} // $self->path_attr($path, 'arg');
	my $needs_dbh = $attrs->{needs_dbh} // $self->path_attr($path, 'needs_dbh') // $self->{needs_dbh};
	
	$ruleset = $path if !defined $ruleset && $validator->ruleset_defined($path);
	
	# Marshall that attributes needed for the operation, including a
	# database handle if one is needed.
	
	$attrs->{op} = $op;
	$attrs->{class} = $class;
	$attrs->{dbh} = database() if $needs_dbh;
	$attrs->{service} = $self;
	
	weaken $attrs->{service};	# Don't block garbage collection
	
	# Create a new query object of the specified class, using the
	# specified attributes.
	
	$request = $class->new($attrs);
	
	# If a ruleset was specified, then validate and clean the parameters.
	# If an error occurs, an error response will be generated
	# automatically.
	
	if ( defined $ruleset )
	{
	    my $result = $validator->validate_params($ruleset, Dancer::params, { ct => $format });
	    
	    if ( $result->errors )
	    {
		return $self->error_result($path, $format, $result);
	    }
	    
	    $request->{params} = $result->values;
	    $request->{valid} = $result;
	}
	
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
