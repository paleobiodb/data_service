#!/opt/local/bin/perl
# 
# comcheck.pl - check the specified files for 'com_name' fields, and flag any
# that are repeated.

use strict;

use Getopt::Long;
use Pod::Usage;

our ($VERSION) = '1.0';

my %COUNT;
my %FILE;
my %VALUES;
my %LOCATIONS;
my %BLOCKS;
my %COND;

my ($field_name, $vocab);
my ($show_version, $show_help, $show_man);


my $options = GetOptions (
	# Application-specific options
	'field=s' => \$field_name,
	
	# Standard meta-options
	'version' => $show_version,
	'help' => $show_help,
	'man' => $show_man,
	);

printversion() if $show_version;
pod2usage(-exitval => 2, -verbose => 1) unless $options;
pod2usage(-exitval => 1, -verbose => 1) if $show_help;
pod2usage(-exitval => 0, -verbose => 2) if $man;


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



sub print_version {
    
    print "$VERSION\n";
    exit(1);
}


__END__

=head1 NAME

wdsfieldcheck - check for duplicate field names in server code that uses Web::DataService

=head1 SYNOPSIS

wdsfieldcheck [options] [file...]

  Options:
    -help		brief help message
    -man		full documentation

    -field		analyze one field name

  If no field name is given, then all fields will be analyzed

=head1 DESCRIPTION

This command analyzes the specified input files, which should contain Perl
code that includes calls to the Web::DataService method C<define_output_block>.

The primary purpose of this command is to look for instances where the same
field name is used for different purposes in different output blocks.  This is
not always a problem per-se, but might cause confusion to users of the data
service.  It will definitely be a problem if the conflicting output blocks are
able to be included together in the output of a single data service call, or
if they might ever be so included under some future version of the data service.

In any case, all such instances should be checked before each release of the
data service.

=head1 OPTIONS

=over 4

=item -help

Print a brief help message and exit

=item -man

Print this manual page and exit

=item -field

Analyze all occurrences of a single field name.  If you wish to restrict the
search to a particular vocabulary, you can prefix the name with the vocabulary
name followed by a colon.  This should hardly ever be necessary, as it is
highly recommended that multiple vocabularies use non-overlapping sets of
field names.

=OUTPUT

A description of the command output should go here.

=AUTHOR

This command is installed as part of the Web::Dataservice module.

Please report bugs using http://rt.cpan.org/.

Michael McClennen <mmcclenn@cpan.org>
