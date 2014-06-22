#
# Web::DataService::Vocabulary.pm
# 
# This module is responsible for the definition of output vocablaries.
# 
# Author: Michael McClennen

use strict;

package Web::DataService;

use Carp qw(carp croak);

our (%VOCAB_DEF) = (name => 'ignore',
		    title => 'single',
		    doc_path => 'single',
		    use_field_names => 'single',
		    disabled => 'single');

our ($DEFAULT_INSTANCE);


# define_vocab ( attrs... )
# 
# Define one or more vocabularies for data service responses.  These
# vocabularies provide field names for the responses.

sub define_vocab {

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
	    
	    croak "define_vocab: you must include the attribute 'name'" unless $name;
	    
	    # Make sure this vocabulary was not already defined by a previous call,
	    # and set the attributes as specified.
	    
	    croak "define_vocab: '$name' was already defined" if defined $self->{vocab}{$name}
		and not $self->{vocab}{$name}{_default};
	    
	    # Create a new record to represent this vocabulary.
	    
	    my $record = bless { name => $name }, 'Web::DataService::Vocab';
	    
	    foreach my $k ( keys %$item )
	    {
		croak "define_vocab: invalid attribute '$k'" unless $VOCAB_DEF{$k};
		
		$record->{$k} = $item->{$k};
	    }
	    
	    # Remove the default vocabulary, because it is only used if no
	    # other vocabularies are defined.
	    
	    if ( $self->{vocab}{default}{_default} and not $item->{disabled} )
	    {
		delete $self->{vocab}{default};
		shift @{$self->{vocab_list}};
	    }
	    
	    # Now install the new vocabulary.  But don't add it to the list if
	    # the 'disabled' attribute is set.
	    
	    $self->{vocab}{$name} = $record;
	    push @{$self->{vocab_list}}, $name unless $record->{disabled};
	    $last_node = $record;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "define_vocab: arguments must be hashrefs and strings";
	}
    }
    
    croak "define_vocab: the arguments must include a hashref of attributes"
	unless $last_node;
}


# validate_vocab ( )
# 
# Return a code reference (actually a reference to a closure) that can be used
# in a parameter rule to validate a vocaubulary-selecting parameter.  All
# non-disabled vocabularies are included.

sub valid_vocab {
    
    my ($self) = @_;
    
    # The ENUM_VALUE subroutine is defined by HTTP::Validate.pm.
    
    return ENUM_VALUE(@{$self->{vocab_list}});
}


# document_vocab ( path, options )
# 
# Return a string containing POD documentation of the response vocabularies
# that are allowed for the specified path.  If the option 'all' is true, then
# document all of the vocabularies enabled for this data service regardless of
# whether they are actually allowed for that path.
# 
# If the option 'extended' is true, then include the text description of each
# vocabulary.

sub document_vocab {

    my ($self, $path, $options) = @_;
    
    $options ||= {};
    $path //= '/';
    
    # Go through the list of defined vocabularies in order, filtering out
    # those which are not allowed for this path.  The reason for doing it this
    # way is so that the vocabularies will always be listed in the order
    # defined, instead of the arbitrary hash order.
    
    my $list = $self->{vocab_list};
    my $allowed = $options->{all} ? $self->{vocab}
	: $self->{path_attrs}{$path}{allow_vocab};
    
    return '' unless ref $allowed eq 'HASH' && ref $list eq 'ARRAY';
    
    my @names = grep { $allowed->{$_} && ! $self->{vocab}{$_}{disabled} } @$list;
    
    return '' unless @names;
    
    my @paths = grep { $self->{vocab}{$_}{doc_path} } @names;
    
    my $doc = "=over 4\n\n";
    my $ext_header = $options->{extended} || ! @paths ? " | Description" : '';
    my $doc_header = @paths ? " | Documentation" : '';
    
    $doc .= "=for pp_table_header Vocabulary* | Name$doc_header$ext_header\n\n";
    
    foreach my $name (@names)
    {
	my $frec = $self->{vocab}{$name};
	my $title = $frec->{title} || $frec->{name};
	my $doc_link = $self->generate_path_link($frec->{doc_path}) if $frec->{doc_path};
	
	$doc .= "=item $frec->{title} | C<$frec->{name}>";
	$doc .= " | $doc_link" if $doc_link && @paths && $options->{extended};
	$doc .= "\n\n";

	if ( $options->{extended} || ! @paths )
	{
	    $doc .= "$frec->{doc}\n\n" if $frec->{doc};
	}
	
	elsif ( $doc_link )
	{
	    $doc .= "$doc_link\n\n";
	}
    }
    
    $doc .= "=back";
    
    return $doc;
}


# document_vocab ( name )
# 
# Return a string containing POD documentation of the vocabulary
# possibilities.  If a name is specified, return the documentation string for
# that vocabulary only.

sub document_one_vocab {
    
    my ($self, $name) = @_;
    
    # Otherwise, if a single vocabulary name was given, return its
    # documentation string if any.
    
    if ( $name )
    {
	return $self->{vocab}{$name}{doc};
    }
    
    # Otherwise, document the entire list of enabled vocabularies in POD
    # format.
    
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

1;
