#!/usr/bin/env perl
# 
# Diff the named file between versions, eliminating any difference between
# version numbers


use strict;

my (@files);
my (@libdir);
my (%prefix);

while ( @ARGV )
{
    my $arg = shift @ARGV;
    
    if ( $arg =~ qr{ (\S+) = (\S+) }xs )
    {
	my $lib = $1;
	my $pref = $2;
	
	push @libdir, $lib;
	$pref =~ s/[.]/\\./g;
	$prefix{$lib} = $pref;
    }
    
    else
    {
	push @files, $arg;
    }
}

# Make sure we have two arguments.

unless ( @libdir == 2 )
{
    die "You must specify two library directories to diff.\n";
}


# Now, go through the files and process them one by one.  Create temp files by
# taking out the package prefix and version prefix, then diff them.

FILE:
foreach my $f (@files)
{
    foreach my $lib (@libdir)
    {
	my $source = "lib/$lib/$f";
	
	unless ( -r $source )
	{
	    warn "Ignoring $source: not found\n";
	    next FILE;
	}
	
	system(qq{perl -pe "s/$lib/<L>/g; s/$prefix{$lib}/<V>/g" $source > /tmp/$lib.pm});
    }
    
    system("diff /tmp/$libdir[0].pm /tmp/$libdir[1].pm");    
}
