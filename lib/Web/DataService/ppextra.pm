sub generate_pod {
    
    my ($self, $attrs) = @_;
    
    my $output = '';
    
    # Make sure that the output starts with an =encoding directive
    
    $output .= $self->generate_pod_directive('encoding', $self->{encoding});
    
    # Then output the parsed nodes one by one.
    
    foreach my $node ( @{$self->{body}} )
    {
	$output .= $self->generate_pod_node($node);
    }
    
    # If any error messages occurred, note this now.
    
    if ( ref $self->{errors} eq 'ARRAY' && @{$self->{errors}} )
    {
	$output .= "\n# Errors occurred when generating this document.\n";
    }
    
    return $output;
}


sub generate_pod_node {
    
    my ($self, $node) = @_;
    
    my $output = '';
    
    if ( $node->{type} eq 'head' )
    {
	my $dir = "head$node->{level}";
	my $content = $self->generate_pod_content($node->{content});
	
	$output .= $self->generate_pod_directive($dir, $content);
    }
    
    elsif ( $node->{type} eq 'para' )
    {
	$output .= $self->generate_pod_para($node);
    }
    
    elsif ( $node->{type} eq 'verbatim' )
    {
	$output .= $self->generate_pod_verbatim($node);
    }
    
    elsif ( $node->{type} eq 'list' )
    {
	$output .= $self->generate_pod_list($node);
    }
    
    elsif ( $node->{type} eq 'format' )
    {
	$output .= $self->generate_pod_format($node);
    }
    
    elsif ( $node->{type} eq 'error' )
    {
	$output .= $self->generate_pod_error($node);
    }
}


sub generate_pod_directive {
    
    my ($dir, $content) = @_;
    
    my $output = "\n=$dir";
    
    if ( defined $content && $content =~ qr{\S} )
    {
	return "$output $content";
    }
    
    else
    {
	return $output;
    }
}


sub generate_pod_para {
    
    my ($node) = @_;
    
    my $output = "\n" . $self->generate_pod_content($node->{content});
    
    return $output;
}


sub generate_pod_verbatim {
    
    my ($node) = @_;
    
    my $output = $self->generate_pod_content($node->{content});
    
    $output =~ s{^}{    }xm;
    
    return "\n" . $output;
}


sub generate_pod_list {
    
    my ($self, $node) = @_;
    
    my $output = '';
    my $in_item = 0;
    
    $output .= $self->generate_pod_directive('over');
    
    foreach my $subnode ( @{$node->{body}} )
    {
	if ( $subnode->{type} eq 'item' )
	{
	    my $content = $self->generate_pod_content($subnode->{content});
	    $output .= $self->generate_pod_directive('item', $content);
	    $in_item = 1;
	}
	
	elsif ( $subnode->{type} eq 'para' )
	{
	    $content .= $self->generate_pod_para($subnode);
	}
	
	elsif ( $subnode->{type} eq 'format' )
	{
	    $content .= $self->generate_pod_format($subnode);
	}
	
	elsif ( $subnode->{type} eq 'verbatim' )
	{
	    $content .= $self->generate_pod_verbatim($subnode);
	}
	
	elsif ( $subnode->{type} eq 'list' )
	{
	    $content .= $self->generate_pod_list($subnode);
	}
	
	elsif ( $subnode->{type} eq 'error' )
	{
	    $output .= $self->generate_pod_error($subnode);
	}
    }
    
    $output .= $self->generate_pod_directive('back');
    
    return $output;
}


