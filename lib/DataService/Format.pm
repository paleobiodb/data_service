#
# Web::DataService::Format
# 
# This module is responsible for the definition of output formats.
# 
# Author: Michael McClennen

use strict;

package Web::DataService;

use Carp qw(carp croak);

our (%FORMAT_DEF) = (name => 'ignore',
		     default_vocab => 'single',
		     content_type => 'single',
		     disposition => 'single',
		     title => 'single',
		     doc_path => 'single',
		     class => 'single',
		     module => 'single',
		     no_module => 'single',
		     doc => 'single',
		     disabled => 'single');

our (%FORMAT_CT) = (json => 'application/json',
		    txt => 'text/plain',
		    tsv => 'text/tab-separated-values',
		    csv => 'text/csv',
		    xml => 'text/xml');

our (%FORMAT_CLASS) = (json => 'Web::DataService::JSON',
		       txt => 'Web::DataService::Text',
		       tsv => 'Web::DataService::Text',
		       csv => 'Web::DataService::Text',
		       xml => 'Web::DataService::XML');

our ($DEFAULT_INSTANCE);

# define_format ( attrs... )
# 
# Define one or more formats for data service responses.

sub define_format {

    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
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
	    
	    croak "define_format: the attributes must include 'name'" unless defined $name;
	    
	    # Make sure this format was not already defined by a previous call.
	    
	    croak "define_format: '$name' was already defined" if defined $self->{format}{$name};
	    
	    # Create a new record to represent this format and check the attributes.
	    
	    my $record = bless { name => $name }, 'Web::DataService::Format';
	    
	    foreach my $k ( keys %$item )
	    {
		croak "define_format: invalid attribute '$k'" unless $FORMAT_DEF{$k};
		
		my $v = $item->{$k};
		
		if ( $k eq 'default_vocab' )
		{
		    croak "define_format: unknown vocabulary '$v'"
			unless ref $self->{vocab}{$v};
		    
		    croak "define_format: cannot default to disabled vocabulary '$v'"
			if $self->{vocab}{$v}{disabled} and not $item->{disabled};
		}
		
		$record->{$k} = $item->{$k};
	    }
	    
	    $record->{content_type} ||= $FORMAT_CT{$name};
	    
	    croak "define_format: you must specify an HTTP content type for format '$name' using the attribute 'content_type'"
		unless $record->{content_type};
	    
	    $record->{class} //= $FORMAT_CLASS{$name};
	    
	    croak "define_format: you must specify a class to implement format '$name' using the attribute 'class'"
		unless defined $record->{class};
	    
	    $record->{module} ||= $record->{class} . ".pm" if $record->{class} ne '';
	    
	    # Make sure that the module is loaded, unless the format is disabled.
	    
	    if ( $record->{module} && ! $record->{disabled} )
	    {
		my $filename = $record->{module};
		$filename =~ s{::}{/}g;
		$filename .= '.pm' unless $filename =~ /\.pm$/;
		
		require $filename;
	    }
	    
	    # Now store the record as a response format for this data service.
	    
	    $self->{format}{$name} = $record;
	    push @{$self->{format_list}}, $name unless $record->{disabled};
	    $last_node = $record;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "define_format: the arguments to this routine must be hashrefs and strings";
	}
    }    
    
    croak "define_format: you must include at least one hashref of attributes"
	unless $last_node;
}


# document_allowed_formats ( path, extended )
# 
# Return a string containing POD documentation of the response formats that
# are allowed for the specified path.  If the path is '/', then document all
# of the formats enabled for this data service regardless of whether they are
# actually allowed for that path.
# 
# If $extended is true, then include the text description of each format.

sub document_allowed_formats {

    my ($self, $path, $extended) = @_;
    
    # Go through the list of defined formats in order, filtering out those
    # which are not allowed for this path.  The reason for doing it this way
    # is so that the formats will always be listed in the order defined,
    # instead of the arbitrary hash order.
    
    my $list = $self->{format_list};
    my $allowed = $path eq '/' ? $self->{format}
			       : $self->{path_attrs}{$path}{allow_format};
    
    return '' unless ref $allowed eq 'HASH' && ref $list eq 'ARRAY';
    
    my @names = grep { $allowed->{$_} && ! $self->{format}{$_}{disabled} } @$list;
    
    return '' unless @names;
    
    my $doc = "=over 4\n\n";
    my $ext_header = $extended ? " | Description" : '';
    
    $doc .= "=for pp_table_header Format | Suffix | Documentation$ext_header\n\n";
    
    foreach my $name (@names)
    {
	my $frec = $self->{format}{$name};
	my $doc_link = $self->generate_path_link($frec->{doc_path});
	
	if ( $extended )
	{
	    $doc .= "=item $frec->{title} | C<.$frec->{name}> | $doc_link\n\n";
	    $doc .= "$frec->{doc}\n\n" if $frec->{doc};
	}
	
	else
	{
	    $doc .= "=item $frec->{title} | C<.$frec->{name}>\n\n";
	    $doc .= "$doc_link\n\n";
	}
    }
    
    $doc .= "=back";
    
    return $doc;
}


# document_format ( name, formats )
# 
# Return a string containing POD documentation of the response formats that
# have been defined for this data service.  If a format name is given, return
# just the documentation for that format.

sub document_format {
    
    my ($self, $name) = @_;
    
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


1;
