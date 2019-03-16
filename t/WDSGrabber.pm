# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to define a class to act as a surrogate dataservice object, in order
# to gather information about output blocks and rulesets. This information can then be used for
# testing purposes.
# 

use strict;

package WDSGrabber;

use Test::More ();

use Dancer;
use Carp 'croak';

use lib 'lib';

BEGIN {
    push @WDSGrabber::ISA, 'Web::DataService';
};

use CoreFunction qw(connectDB loadConfig configData);


# Create a new WDSGrabber object, which can be used as a surrogate for a data service object. This
# surrogate can be passed to the 'initialize' method of a data service operation module in order
# to collect up output blocks and rulesets.

sub new {
    
    my ($class, $name, $options) = @_;
    
    $options ||= { };
    
    my $config_filename = $options->{config};
    my $db_name = $options->{db_name};
    my $foundation = $options->{foundation};
    
    my $dbh = connectDB($config_filename, $db_name);
    
    my $grabber = { name => $name,
		    dbh => $dbh,
		    maps => { },
		    map_hash => { },
		    blocks => { },
		    fields => { },
		    rulesets => { },
		  };
    
    bless $grabber, $class;
    
    $foundation ||= 'Web::DataService::Plugin::Dancer';
    
    my $require_name = $foundation; $require_name =~ s{::}{/}g;
    
    require "$require_name.pm";
    
    $foundation->read_config($grabber);

    return $grabber;
}


# Process an operation module by calling its initialization routine and collecting up the output
# blocks, maps, and rulesets that it defines.

sub process_modules {
    
    my ($grabber, @module_names) = @_;

    foreach my $m ( @module_names )
    {
	my $filename = $m; $filename =~ s{::}{/}g;
	
	require "$filename.pm";
	
	if ( $m->can('initialize') )
	{
	    $m->initialize($grabber);
	}
	
	else
	{
	    warn "WARNING: $m does not define an 'initialize' method";
	}
    }
}


# Return the list of values from a named output map or set.

sub list_values {
    
    my ($grabber, $map_name, $options) = @_;

    $options ||= { };

    my $map_list = $grabber->{maps}{$map_name};
    
    unless ( $map_list )
    {
	Test::More::fail "unknown map '$map_name'";
	return;
    }
    
    my %skip;

    if ( $options->{skip} )
    {
	%skip = map { $_ => 1 } extract_list($options->{skip});
    }
    
    my @list;

    foreach my $r ( @$map_list )
    {
	my $key = $r->{value};
	
	next unless $key && ! $skip{$key};
	
	push @list, $key;
	push @list, 1 if $options->{hash};
    }
    
    return @list;
}


# Lookup key values from a map or set.

sub map_lookup {
    
    my ($grabber, $map_name, $value) = @_;
    
    my $target = $grabber->{map_hash}{$map_name}{$value};
    
    return $target eq '1' ? undef : $target;
}


# Return the list of field names from the specified block, in the specified vocabulary.
    
sub list_fields {
    
    my ($grabber, $block_name, $vocab, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };

    my $block_list = $grabber->{blocks}{$block_name};
    
    unless ( $block_list )
    {
	Test::More::fail "unknown block '$block_name'";
	return;
    }

    my %skip;

    if ( $options->{skip} )
    {
	%skip = map { $_ => 1 } extract_list($options->{skip});
    }
        
    my @list;
    
    foreach my $r ( @$block_list )
    {
	next unless $r->{output};

	# next if $r->{if_block} && $r->{if_block} !~ /$block_key/;
	
	next if $vocab && $r->{if_vocab} && $r->{if_vocab} !~ /$vocab/;
	
	my $key = $vocab ? $r->{"${vocab}_name"} || $r->{name} || $r->{output}
			 : $r->{name} || $r->{output};
	
	next unless $key && ! $skip{$key};
	
	push @list, $key;
	push @list, 1 if $options->{hash};
    }
    
    return @list;    
}


# Return the list of field names from the specified block, in the specified vocabulary.
    
sub list_if_blocks {
    
    my ($grabber, $block_name, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    my $block_list = $grabber->{blocks}{$block_name};
    
    unless ( $block_list )
    {
	Test::More::fail "unknown block '$block_name'";
	return;
    }
    
    my %if_block;
    
    foreach my $r ( @$block_list )
    {
	next unless $r->{output};
	
	if ( $r->{if_block} )
	{
	    my @list = grep { $_ !~ /:/  && $_ ne 'full' } split /\s*,\s*/, $r->{if_block};
	    $if_block{$_} = 1 foreach @list;
	}
    }
    
    return keys %if_block;    
}


# Utility methods:

# Turn an argument into a list of values, regardless of whether it is a list ref, a hash ref, or
# a scalar.

sub extract_list {
    
    my ($arg) = @_;

    if ( ref $arg eq 'ARRAY' )
    {
	return @$arg;
    }

    elsif ( ref $arg eq 'HASH' )
    {
	return keys %$arg;
    }

    elsif ( ref $arg )
    {
	croak "invalid argument '$arg'";
    }

    else
    {
	return split /\s*,\s*/, $arg;
    }    
}


# Now define overrides for the methods used in the PBDB data service operation module
# initialization routines. These will be called when an object from this package is passed to an
# initialization routine from a test module.

sub get_connection {

    return $_[0]->{dbh};
}


sub config_value {

    my ($grabber, $param) = @_;

    my $name = $grabber->{name};
    
    return $grabber->{_config}{$name}{$param} // $grabber->{_config}{$param};
}


sub define_output_map {
    
    goto &define_set;
}


sub define_block {
    
    my ($grabber, $name, @args) = @_;

    my @list = grep { ref $_ eq 'HASH' } @args;

    $grabber->{blocks}{$name} = \@list;

    foreach my $r ( @list )
    {
	if ( $r->{if_block} )
	{
	    my @blocks = split /\s*,\s*/, $r->{if_block};

	    foreach my $b ( @blocks )
	    {
		push @{$grabber->{blocks}{$b}}, $r;
	    }
	}
    }
}


sub define_set {
    
    my ($grabber, $name, @args) = @_;

    my @list = grep { ref $_ eq 'HASH' } @args;

    $grabber->{maps}{$name} = \@list;
    
    foreach my $r ( @list )
    {
	if ( my $value = $r->{value} )
	{
	    my $target = defined $r->{maps_to} ? $r->{maps_to} : 1;
	    $grabber->{map_hash}{$name}{$value} = $target;
	}
    }
}


sub define_ruleset {
    
    my ($grabber, $name, @args) = @_;
    
    my @list = grep { ref $_ eq 'HASH' } @args;
    
    $grabber->{rulesets}{$name} = \@list;
}


sub document_set {

    # no-op
}


# We may not have actually loaded the Web::DataService module. Make sure that the package exists,
# so that the assignment to @ISA at the top of this file won't cause problems. If the actual
# Web::DataService module has in fact been loaded, then the following code will do nothing except
# define a useless variable.

package Web::DataService;

our ($A) = 1;


1;
