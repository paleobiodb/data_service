#
# Web::DataService::Doc
# 
# This module adds routines to the class Web::DataService::Request for
# generating responses that serve as documentation.  This allows for
# auto-generated documentation pages for the various URL paths provided by a
# data service.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Request;

use Scalar::Util qw(reftype);



# can_document ( )
# 
# Return true if the request has any attributes defined for it at all.
# In this case, we can at least generate a template documentation page even if
# nothing else is available.  Return false otherwise.

sub can_document {
    
    my ($self, $path) = @_;
    
    return defined $self->{attrs};
}


# document ( )
# 
# Generate and return a documentation page for this request.  The accepted
# formats, one of which was selected when the request was created, are 'html'
# and 'pod'.
# 
# If a documentation template corresponding to the specified path is found, it
# will be used.  Otherwise, a default template will be used.

sub document {
    
    my ($self) = @_;
    
    # Do all of the processing in a 'try' block so that if an error occurs we
    # can return an appropriate message.
    
    try {
	
	my $ds = $self->{ds};
	my $path = $self->{path};
	my $attrs = $self->{attrs};
	my $class = $attrs->{class};
	my $format = $attrs->{format};
	
	$DB::single = 1;
	
	# If we are in 'one request' mode, initialize the class plus all of
	# the classes it requires.
	
	if ( $ds->{ONE_REQUEST} )
	{
	    if ( ref $attrs->{also_initialize} eq 'ARRAY' )
	    {
		foreach my $c ( @{$attrs->{also_initialize}} )
		{
		    $ds->initialize_class($c);
		}
	    }
	    
	    $ds->initialize_class($class);
	}
	
	# We start by determining the values necessary to fill in the documentation
	# template.  This may include one or more of: a title, parameters,
	# response fields, etc.
	
	my $vars = { request => $self,
		     path => $path,
		     doc_title => $attrs->{doc_title} // $path,
		     ds_label => $ds->{label} // '',
		     ds_version => $ds->{version} // '',
		     vocab_param => $attrs->{vocab_param},
		     output_param => $attrs->{output_param},
		     output_key => $attrs->{output_key} // 'basic' };
	
	# All documentation is public, so set the maximally permissive CORS header.
	
	$self->set_access_control("*");
	
	# Now determine the location of the template for generating this
	# documentation page.  If one has not been specified, we try the path
	# appended with "/index.tt", and if that does not exist we try the
	# path appended with "_doc.tt".  Or with whatever suffix has been
	# specified for template files.  If none of these template files are
	# present, we try the documentation error template as a backup.
	
	my $doc_dir = $ds->{doc_dir} // "doc";
	my $doc_suffix = $ds->{template_suffix} // "";
	
	my $layout_path = $attrs->{doc_layout} || $ds->{doc_layout} || "doc_main${doc_suffix}";
	my $error_path = $attrs->{doc_error_template} || $ds->{doc_error_template} || "doc_error${doc_suffix}";
	
	my @template_list;
	
	push @template_list, "$doc_dir/$attrs->{doc_template}" if $attrs->{doc_template};
	push @template_list, "$doc_dir/index${doc_suffix}" if $path eq '';
	push @template_list, "$doc_dir/${path}/index${doc_suffix}" if $path ne '';
	push @template_list, "$doc_dir/${path}_doc${doc_suffix}" if $path ne '';
	
	# Try each possible template path in turn.  If a valid template is
	# found, render it and return the result in the appropriate format.
	
	foreach my $template_path ( @template_list )
	{
	    next unless defined $template_path;
	    next unless $ds->template_exists($template_path);
	    
	    my $doc_string = $ds->render_template($self->{outer}, $template_path, $layout_path, $vars);
	    
	    # If POD format was requested, return the documentation as is.
	    
	    if ( defined $format && $format eq 'pod' )
	    {
		$self->set_content_type('text/plain');
		return $doc_string;
	    }
	    
	    # Otherwise, convert the POD to HTML using the PodParser and return the result.
	    
	    else
	    {
		my $parser = Web::DataService::PodParser->new();
		
		$parser->parse_pod($doc_string);
		
		my $doc_html = $parser->generate_html({ css => '/data/css/dsdoc.css', tables => 1 });
		
		$self->set_content_type('text/html');
		return $doc_html;
	    }
	}
	
	# If no valid template file was found, we return an error result.
	
	return $ds->error_result("", $format, "404 The documentation you requested was not found");
    }
    
    catch {

	my $ds = $self->{ds};
	my $path = $self->{path};
	my $format = $self->{format};
	
	return $ds->error_result($path, $format, $_);
    };
}


# The following methods are designed to be called from a templating engine
# ========================================================================

# list_navtrail ( )
# 
# Return a list of navigation trail components for the current request, in POD
# format.  This is derived component-by-component from the request path.

sub list_navtrail {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    my @path = split qr{/}, $self->{path};
    
    # If there are no path components, 
    
    return if $self->{path} eq '' || $self->{path} eq '/';
    
    # Otherwise, construct a list.
    
    my @trail;
    
    my $link = "/" . $ds->{path_prefix} if $ds->{path_prefix};
    
    foreach my $component (@path)
    {
	$link .= "/$component";
	push @trail, "L<$component|$link>";
    }
    
    return @trail;
}


# get_base_url ( )
# 
# Return the base URL for this data service.

sub get_base_url {

    my ($self) = @_;
    
    return $self->{ds}->get_base_url;
}


# list_http_methods ( )
# 
# Return a list of the HTTP methods that are allowed for this request path.

sub list_http_methods {

    my ($self) = @_;
    
    my $method = $self->{attrs}{allow_method};
    return grep { $method->{$_} } @Web::DataService::HTTP_METHOD_LIST;
}


# document_params ( )
# 
# Return a documentation string in POD format describing the parameters
# available for this request.

sub document_params {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    my $validator = $ds->{validator};
    my $ruleset_name = $self->determine_ruleset;
    
    # Generate documentation about the parameters, using the appropriate
    # method from the validator class (HTTP::Validate).  If no ruleset
    # is selected for this request, then state that no parameters are accepted.
    
    return $ruleset_name ? 
	$validator->document_params($ruleset_name) :
	    "I<This path does not take any parameters>";
}


# document_response ( )
# 
# Return a documentation string in POD format documenting the fields that can
# be included in the result.

sub document_response {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    
    return $ds->document_response($self->{attr_path});
}


# document_allowed_formats ( extended )
# 
# Return a string in POD format documenting the formats allowed for the path
# associated with this request.  If $extended is true, then include a text
# description of each format.

sub document_allowed_formats {

    my ($self, $extended) = @_;
    
    my $ds = $self->{ds};
    
    return $ds->document_allowed_formats($self->{attr_path}, $extended);
}


# document_allowed_vocab ( extended )
# 
# Return a string in POD format documenting the vocabularies allowed for the
# path associated with this request.  If $extended is true, then include a
# text description of each vocabulary.

sub document_allowed_vocab {
    
    my ($self, $extended) = @_;
    
    my $ds = $self->{ds};
    
    return $ds->document_allowed_vocab($self->{attr_path}, $extended);
}


# template_exists ( template_path )
# 
# Return true if the given template exists, false otherwise.

sub template_exists {
    
    my ($self, $template_path) = @_;
    
    return $self->{ds}->template_exists($template_path);
}


# render_template ( template_path, layout_path, vars )
# 
# Render the specified template usign the specified layout and variables.

sub render_template {
    
    my ($self, $template_path, $layout_path, $vars) = @_;
    
    return $self->{ds}->render_template($template_path, $layout_path, $vars);
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


1;
