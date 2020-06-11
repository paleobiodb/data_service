#
# Web::DataService::Document
# 
# This module provides a role that is used by 'Web::DataService'.  It implements
# routines for executing documentation requests.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Document;

use Carp 'croak';
use Scalar::Util qw(reftype weaken);

use Moo::Role;


# generate_doc ( request )
# 
# Generate and return a documentation page for this request.  The accepted
# formats, one of which was selected when the request was created, are 'html'
# and 'pod'.
# 
# If a documentation template corresponding to the specified path is found, it
# will be used.  Otherwise, a default template will be used.

sub generate_doc {
    
    my ($ds, $request) = @_;
    
    my $path = $request->node_path;
    my $format = $request->output_format;
    
    # If this is not a valid request, then return a 404 error.
    
    die "404\n" if $request->{is_invalid_request} || 
	$ds->node_attr($path, 'undocumented') ||
	    $ds->node_attr($path, 'disabled');
    
    # If we are in 'one request' mode, initialize this request's primary
    # role.  If we are not in this mode, then all of the roles will have
    # been previously initialized.
    
    if ( $Web::DataService::ONE_REQUEST )
    {
	my $role = $ds->node_attr($path, 'role');
	$ds->initialize_role($role) if $role;
    }
    
    # If the output format is not already set, then try to determine what
    # it should be.
    
    unless ( $format )
    {
	# If the special parameter 'format' is enabled, check to see if a
	# value for that parameter was given.

	$request->{raw_params} //= $Web::DataService::FOUNDATION->get_params($request);
	
	$format ||= $request->special_value('format');
	
	# Default to HTML.
	
	$format ||= 'html';
	
	$request->output_format($format);
    }
    
    # We start by determining the values necessary to fill in the documentation
    # template.  This may include one or more of: a title, parameters,
    # response fields, etc.
    
    my $doc_title = $ds->node_attr($path, 'title') // $path;
    
    my $vars = { ds => $ds,
		 request => $request,
		 doc_title => $doc_title };
    
    # All documentation is public, so set the maximally permissive CORS header.
    
    $ds->_set_cors_header($request, "*");
    
    # Now determine the class that corresponds to this request's primary role
    # and bless the request into that class.
    
    my $role = $ds->node_attr($request, 'role');
    bless $request, $ds->documentation_class($role);
    
    # Now determine the location of the template for generating this
    # documentation page.  If one has not been specified, we try the path
    # appended with "/index.tt", and if that does not exist we try the
    # path appended with "_doc.tt".  Or with whatever suffix has been
    # specified for template files.  If none of these template files are
    # present, we try the documentation error template as a backup.
    
    my $doc_suffix = $ds->{template_suffix} // "";
    
    my $doc_defs = $ds->node_attr($path, 'doc_defs') // $ds->check_doc("doc_defs${doc_suffix}");
    my $doc_header = $ds->node_attr($path, 'doc_header') // $ds->check_doc("doc_header${doc_suffix}");
    my $doc_footer = $ds->node_attr($path, 'doc_footer') // $ds->check_doc("doc_footer${doc_suffix}");
    
    # Now see if we can find a template for this documentation page.  If one
    # was explicitly specified, we try that first.  Otherwise, try the node
    # path suffixed by '_doc' with the template suffix added, and then
    # '/index' with the template suffix.
    
    my $doc_template = $ds->node_attr($path, 'doc_template');
    
    if ( defined $doc_template )
    {
	die "404\n" if $doc_template eq '';
	croak "template $doc_template: not found\n" unless $ds->check_doc($doc_template);
    }
    
    else
    {
	my @try_template;
	
	if ( $path eq '/' )
	{
	    push @try_template, 'index' . $doc_suffix;
	}
	
	else
	{
	    push @try_template, $path . '_doc' . $doc_suffix;
	    push @try_template, $path . '/index' . $doc_suffix;
	    push @try_template, $ds->node_attr($path, 'doc_default_op_template')
		if $ds->node_has_operation($path);
	    push @try_template, $ds->node_attr($path, 'doc_default_template');
	}
	
 	foreach my $t ( @try_template )
	{
	    next unless defined $t;
	    
	    $doc_template = $t, last if $ds->check_doc($t);
	}
    } 
    
    if ( $ds->debug )
    {
	print STDERR "---------------\nDocumentation '$path'\n";
    }
    
    # Record this request's URL base so that we have it in order to generate
    # documentation if necessary.
    
    $ds->{base_url} = $request->base_url;
    
    # Now, if we have found a template that works then render it.
    
    if ( $doc_template )
    {
	my $doc_string = $ds->render_doc($doc_template, $doc_defs, $doc_header, $doc_footer, $vars);
	
	my $url_formatter = sub {
	    if ( $_[0] =~ qr{ ^ (node|op|path) (abs|rel|site)? [:] ( [^#?]* ) (?: [?] ( [^#]* ) )? (?: [#] (.*) )? }xs )
	    {
		my $arg = $1;
		my $type = $2 || 'site';
		my $path = $3 || '/';
		my $params = $4;
		my $frag = $5;
		my $format;
		
		if ( $arg ne 'path' && $path =~ qr{ (.*) [.] ([^.]+) $ }x )
		{
		    $path = $1; $format = $2;
		}
		
		return $request->generate_url({ $arg => $path, type => $type, format => $format,
						params => $params, fragment => $frag });
	    }
	    else
	    {
		return $_[0];
	    }
	};
	
	# If Pod format was requested, return the documentation as is.  The
	# only change we need to make is to convert our special link syntax to
	# standard Pod syntax.
	
	if ( defined $format && $format eq 'pod' )
	{
	    $ds->_set_content_type($request, 'text/plain');
	    return $ds->convert_pod_links($doc_string, $url_formatter);
	}
	
	# Otherwise, convert the POD to HTML using the PodParser and return the result.
	
	else
	{
	    my $stylesheet = $ds->node_attr($path, 'doc_stylesheet') || 
		$ds->generate_site_url({ path => 'css/dsdoc.css' });
	    
	    my $parser = Web::DataService::PodParser->new({ target => 'html', css => $stylesheet, 
							    url_formatter => $url_formatter,
							    page_title => $doc_title });
	    
	    $parser->parse_string_document($doc_string);
	    
	    $ds->_set_content_type($request, 'text/html');
	    return $parser->output;
	}
    }
    
    # If no valid template file was found, we return an error result.
    
    else
    {
	die "404\n";
    }
}


# check_for_template ( path )
# 
# Return true if a documentation template exists for the specified node path.
# Return false if not.  Throw an exception if the file exists but is not
# readable. 

sub check_for_template {

    my ($ds, $path) = @_;
    
    my $doc_suffix = $ds->{template_suffix} // "";
    
    my $check1 = $path . '_doc' . $doc_suffix;
    
    return $check1 if $ds->check_doc( $check1 );
    
    my $check2 = $path . '/index' . $doc_suffix;
    
    return $check2 if $ds->check_doc( $check2 );
    
    return; # otherwise
}


# make_doc_node ( path, doc_path )
# 
# Create a documentation node for the specified path, reading the title from
# the template file.  The second method parameter is the actual (relative)
# path of the file on disk.

sub make_doc_node {
    
    my ($ds, $path, $doc_path) = @_;
    
    my $new_attrs = { path => $path, title => 'NULL' };
    
    my $partial_contents = $ds->read_doc_partial($doc_path);
    
    while ( $partial_contents =~ m{ ^ =for \s+ wds_node \s* (.*) $ }gxmi )
    {
	my $expr = $1;
	
	while ( $expr )
	{
	    if ( $expr =~ qr{ ^ (\w+) \s* = \s* " ( (?: [^"] | \\{2} | \\" )+ ) " \s* (.*) }xs )
	    {
		$expr = $3;
		my $attr = $1;
		my $value = $2;
		$value =~ s{\\{2}}{\\}g;
		
		unless ( $Web::DataService::Node::NODE_DEF{$attr} )
		{
		    die "500 Invalid attribute '$attr' for wds_node\n";
		}
		
		$new_attrs->{$attr} = $value;
	    }
	    
	    elsif ( $expr =~ qr{ ^ (\w+) \s* = \s* ( (?: [^;] | \\{2} | \\; )+ ) \s* (.*) }xs )
	    {
		$expr = $3;
		my $attr = $1;
		my $value = $2;
		$value =~ s{\\{2}}{\\}g;
		
		unless ( $Web::DataService::Node::NODE_DEF{$attr} )
		{
		    die "500 Invalid attribute '$attr' for wds_node\n";
		}
		
		$new_attrs->{$attr} = $value;
	    }
	    
	    elsif ( $expr =~ qr{ ^ ; \s* (.*) }xs )
	    {
		$expr = $1;
	    }
	    
	    else
	    {
		die "500 Invalid syntax for wds_node: '$expr'\n";
	    }
	}
    }
    
    $ds->_create_path_node($new_attrs, '', '');
}


# get_nodelist ( )
# 
# Return a list of sub-nodes of the current one.  This will include all
# sub-nodes with a value for the node attribute 'place', in order by the value
# of that attribute.

sub get_nodelist {

    my ($ds, $path) = @_;
    
    my $node_hash = $ds->{node_list}{$path};
    
    return unless ref $node_hash eq 'HASH';
    
    return map { @{$node_hash->{$_}} } sort { $a <=> $b } keys %$node_hash;
}


# document_nodelist ( )
# 
# Return a documentation string in Pod format listing the subnodes (if any)
# given for this node.  See &list_subnodes above.

sub document_nodelist {
    
    my ($ds, $path, $options) = @_;
    
    $options ||= {};
    
    my @list = $ds->get_nodelist($path);
    
    return '' unless @list;
    
    my $documentation = "=over\n\n";
    
    foreach my $n ( @list )
    {
	my $path = $n->{path};
	my $title = $n->{title} // $ds->node_attr($path, 'title') // $path;
	my $body = $n->{doc_string} // $ds->node_attr($path, 'doc_string');
	
	$documentation .= "=item L<$title|node:$path>\n\n";
	
	if ( defined $body && $body ne '' )
	{
	    $documentation .= $body;
	}
	
	if ( $options->{usage} )
	{
	    my $usage = $n->{usage} // $ds->node_attr($path,'usage');
	    my @usage_list = ref $usage eq 'ARRAY' ? @$usage : $usage;
	    
	    my $usage_doc = $ds->_make_usage_doc($path, @usage_list);
	    
	    if ( $usage_doc )
	    {
		$documentation .= "\n" . $options->{usage};
		$documentation .= "\n\n";
		$documentation .= $usage_doc;
	    }
	}
	
	$documentation .= "\n\n";
    }
    
    $documentation .= "=back\n\n";
    
    return $documentation;
}


# document_usage ( )
# 
# Return a documentation string in Pod format describing the usage examples
# (if any) given for this node.

sub document_usage {
    
    my ($ds, $path, $options) = @_;
    
    $options ||= {};
    
    my $usage = $ds->node_attr($path, 'usage');
    
    my @usage_list = ref $usage eq 'ARRAY' ? @$usage : $usage;
    
    return $ds->_make_usage_doc($path, @usage_list);
}


sub _make_usage_doc {
    
    my ($ds, $path, @usage_list) = @_;
    
    my @urls;
    
    foreach my $example ( @usage_list )
    {
	next unless defined $example;
	
	if ( $example =~ qr{ ^ html: | ^ text: }xs )
	{
	    push @urls, $example;
	}
	
	elsif ( $example =~ qr{ ( / | http:/+ )? ( [^?.#]+ ) (?: [.] ([^?.#]+) ) (?: [?] ( [^#]+ ) )? (?: [#] (.*) )? }xs )
	{
	    my $args = { op => $2 };
	    $args->{format} = $3 if $3;
	    $args->{params} = $4 if $4;
	    $args->{fragment} = $5 if $5;
	    $args->{type} = 'abs' if defined $1 && $1 =~ qr{ ^h }x;
	    
	    my $url = $ds->generate_site_url($args);
	    push @urls, $url if $url;
	}
	
	elsif ( ref $example eq 'HASH' )
	{
	    my $args = { op => $path };
	    $args->{format} = $example->{format} if $example->{format};
	    $args->{params} = $example->{params} if $example->{params};
	    $args->{fragment} = $example->{fragment} if $example->{fragment};
	    $args->{type} = $example->{type} if $example->{type};
	    
	    my $url = $ds->generate_site_url($args);
	    push @urls, $url if $url;
	}
    }
    
    return '' unless @urls;
    
    my $doc_string = "=over\n\n";
    
    foreach my $url ( @urls )
    {
	if ( $url =~ qr{ ^ (\w+): (.+) }xs )
	{
	    if ( $1 eq 'html' )
	    {
		$doc_string .= "=for html $2\n\n";
	    }
	    
	    elsif ( $1 eq 'text' )
	    {
		my $rest = $2;
		$doc_string =~ s/\n$//;
		$doc_string .= "$rest\n\n";
	    }
	}
	
	else
	{
	    $doc_string .= "=item *\n\nL<$url>\n\n";
	}
    }
    
    $doc_string .= "=back\n";
    
    return $doc_string;
}


# convert_pod_links ( doc_string )
# 
# Convert the contents of all L<...>, L<<...>>, etc. elements to proper links.

sub convert_pod_links {
    
    my ($ds, $doc_string, $urlgen) = @_;
    
    $doc_string =~ s{L<<<(.*?)>>>}{'L<<<' . $ds->convert_pod_link($1, $urlgen) . '>>>'}ge;
    $doc_string =~ s{L<<(.*?)>>}{'L<<' . $ds->convert_pod_link($1, $urlgen) . '>>'}ge;
    $doc_string =~ s{L<(.*?)>}{'L<' . $ds->convert_pod_link($1, $urlgen). '>'}ge;
    
    return $doc_string;
}


sub convert_pod_link {
    
    my ($ds, $target, $urlgen) = @_;
    
    if ( $target =~ qr{ ^ (.*?) \| (.*) }xs )
    {
	return "$1|" . $urlgen->($2);
    }
    
    else
    {
	return $urlgen->($target);
    }
}

1;
