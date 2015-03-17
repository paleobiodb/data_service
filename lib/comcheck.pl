#!/opt/local/bin/perl
# 
# comcheck.pl - check the specified files for 'com_name' fields, and flag any
# that are repeated.

use strict;

use Getopt::Std;

my %COUNT;
my %FILE;
my %VALUES;
my %LOCATIONS;
my %BLOCKS;
my %COND;

my %options;

getopts('adlbvt:', \%options);

# Start by iterating over the files specified on the command line, collecting
# statistics about the com_name values.

my $current_block = '';
my $com_name = '';
my $location = '';
my $output;
my $value;
my $com_value;
my $cond = '';

while (<>)
{
    
    if ( / define_block \s* \( \s* ['"] ([^'"]+) ['"] /x )
    {
	$current_block = $1;
    }
    
    if ( / ^ \s* \{ /x )
    {
	$com_name = '';
	$location = '';
	$cond = '';
	$output = undef;
	$value = undef;
	$com_value = undef;
    }
    
    if ( / output \s* => \s* ['"] ([^"']+) ['"] /x )
    {
	$output = $1;
    }
    
    if ( / value \s* => \s* ['"] ([^"']+) ['"] /x )
    {
	$value = "'$1'";
    }
    
    if ( / com_value \s* => \s* ['"] ([^"']+) ['"] /x )
    {
	$com_value = "'$1'";
    }
    
    if ( / (if|not) _block \s* => \s* ( \[ [^]]+ \] | ['"] [^'"]+ ['"] ) /x )
    {
	$cond = "$1 $2";
    }
    
    if ( / ['"]?com_name['"]? \s* => \s* ['"] ([^'"]+) ['"] /x )
    {
	$com_name = $1;
	$location = "$ARGV line $.";
    }	
    
    if ( / \} \s* (?: , | \) \s* ; ) /x )
    {
	if ( $com_name ne '' )
	{
	    my $value = $com_value // $value // $output;
	    
	    $COUNT{$com_name}++;
	    $FILE{$ARGV}{$com_name} = 1;
	    push @{$VALUES{$com_name}}, $value || $location;
	    push @{$LOCATIONS{$com_name}}, $location;
	    push @{$BLOCKS{$com_name}}, $current_block || $location;
	    push @{$COND{$com_name}}, $cond;
	}
	
	$com_name = '';
	$location = '';
	$output = undef;
	$value = undef;
	$com_value = undef;
	$cond = '';
    }
    
    if ( / \); /x )
    {
	$current_block = '';
    }
}

continue {
    close ARGV if eof;  # Not eof()!
}

# Print the total number of names found.

my $name_count = scalar(keys %COUNT);
my $dup_count = sum( map { 1 if $COUNT{$_} > 1 } keys %COUNT );

print "Found $name_count com_name values.\n";
print "Found $dup_count duplicate values.\n";

# If the option -a was specified, then print all defined names.

print_all_by_name() if $options{a} || $options{d};
print_entry($options{t}) if $options{t};


# print_all_by_name ( )
# 
# Print all com_name values found, sorted by tag name.

sub print_all_by_name {
    
    print "\n";
    
    foreach my $k ( sort keys %COUNT )
    {
	next if $options{d} && $COUNT{$k} < 2;
	print_entry($k);
    }
    
}


sub print_entry {
    
    my ($k) = @_;
    
    print "unknown name '$k'" unless $COUNT{$k};
    
    if ( $options{l} )
    {
	my $locs = join(', ', @{$LOCATIONS{$k}});
	print "'$k': $locs\n" 
    }
    elsif ( $options{b} )
    {
	my $blocks = join(', ', @{$BLOCKS{$k}});
	print "'$k': $blocks\n";
    }
    elsif ( $options{v} )
    {
	my $fields = join(', ', @{$VALUES{$k}});
	print "'$k': $fields\n";
    }
    else
    {
	print "'$k':\n";
	foreach my $i ( 0..$COUNT{$k}-1 )
	{
	    print sprintf("    %-20s %-20s %-20s %-40s\n", $VALUES{$k}[$i], $BLOCKS{$k}[$i], $COND{$k}[$i], $LOCATIONS{$k}[$i]);
	}
    }
}


sub sum {
    
    my $sum = 0;
    $sum += $_ foreach @_;
    return $sum;
}
