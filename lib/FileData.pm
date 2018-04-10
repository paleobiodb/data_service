# 
# FileData.pm - read record data from files
# 
# This module is intended for use with command-line tools for updating records in the database.
# 


package FileData;

use strict;

use base 'Exporter';

our @EXPORT_OK = qw(decode_input_lines);


# decode_input_lines ( fh )
# 
# 
# Given a file handle or the string '-' to represent filenames from the command line, read and decode
# the contents. For now, they must be in JSON. At some point, we will add the ability to read in
# text files.

sub decode_input_lines {
    
    my ($fh) = @_;
    
    my @records;
    my @lines;
    
    # If the argument is '-', read from the double-diamond magic file handle.
    
    if ( $fh && $fh eq '-' )
    {
	while ( <<>> )

	{
	    push @lines, $_;
	}
    }
    
    # Otherwise, assume it is an already-open file handle and read from it.
    
    elsif ( $fh )
    {
	while ( <$fh> )
	{
	    push @lines, $_;
	}
    }
    
    else
    {
	die "You must specify an input file.\n";
    }
    
    # If the first line starts with [ or {, assume it is in JSON format and decode it.
    
    if ( $lines[0] =~ /^[[{]/ )
    {
	my $raw = join('', @lines);
	my $body = JSON->new->utf8->relaxed->decode($raw);
	
	if ( ref $body eq 'ARRAY' && ( @$body == 0 || ref $body->[0] eq 'HASH' ) )
	{
	    push @records, @$body;
	}
	
	elsif ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
	{
	    push @records, @{$body->{records}};
	}
	
	elsif ( ref $body eq 'HASH' )
	{
	    push @records, $body;
	}
	
	else
	{
	    die "Could not interpret JSON content.\n";
	}
    }
    
    # Eventually we will allow other input types.
    
    else
    {
	die "Content must be a JSON hash or array.\n";
    }
    
    return \@records;
}

1;
