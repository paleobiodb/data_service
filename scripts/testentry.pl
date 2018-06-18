#!/usr/bin/env perl
# 
# timescale_tables.pl
# 
# Establish or reload the timescale tables.

use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;
use Term::ReadLine;
use Data::Dump qw(dd);
use Test::More;

use Tester;


my ($opt_real_tables, $opt_cookie, $opt_debug, $opt_help, $opt_man, $opt_verbose);

GetOptions("real-tables" => \$opt_real_tables,
	   "cookie=s" => \$opt_cookie,
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);

# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;


# Initialize a Tester object which will allow us to make requests.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

if ( $opt_real_tables )
{
    print "\n  !!!! WARNING: YOU WILL BE MODIFYING THE REAL DATABASE TABLES !!!!\n\n";
}

else
{
    $T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
    # $T->test_mode('specimen_data', 'enable') || BAIL_OUT("could not select test specimen data");
}

# We start as the test superuser.

$T->set_cookie("session_id", $opt_cookie || "SESSION-SUPERUSER");

# Then loop, asking for an operation and then data.

my $state = 'OP';
my $prompt = "Operation: ";
my $operation = '';
my $last_operation = '';
my @records;
my @last_records;
my $record;
my $term = Term::ReadLine->new('testentry');

my (%operation) = ( 'specs/addupdate' => 1,
		    'specs/addupdate_measurements' => 1,
		    'timescales/addupdate' => 1,
		    'timescales/update' => 1,
		    'timescales/delete' => 1,
		    'bounds/addupdate' => 1,
		    'bounds/update' => 1,
		    'bounds/delete' => 1 );

while ( $state ne 'DONE' )
{
    my $input = $term->readline($prompt);
    
    if ( ! defined $input )
    {
	last;
    }
    
    if ( $state eq 'OP' )
    {
	if ( $input =~ qr{ ^ cookie (.*) $ }xs )
	{
	    $T->set_cookie("session_id", $input);
	}

	elsif ( $input =~ qr { ^ redo $ }xs )
	{
	    unless ( $last_operation )
	    {
		print "NOTHING TO REDO\n";
		next;
	    }

	    $operation = $last_operation;
	    @records = @last_records;
	    $state = 'DATA';

	    print "REDOING $operation";
	    dd(\@records);
	}
	
	elsif ( $operation{$input} )
	{
	    $operation = $input;
	    $state = 'DATA';
	    $prompt = "Data: ";
	    $record = { };
	    next;
	}
	
	else
	{
	    print "UNKNOWN OPERATION '$input'\n";
	    next;
	}
    }

    elsif ( $state eq 'DATA' )
    {
	if ( $input eq 'print' )
	{
	    if ( $record->{record_label} )
	    {
		print "record_label => '$record->{record_label}'\n";
	    }
	    
	    foreach my $k ( sort keys %$record )
	    {
		next if $k eq 'record_label';
		print "$k => '$record->{$k}'\n";
	    }

	    next;
	}
	
	elsif ( $input =~ qr{ ^ [#] (.*) | ^ [.] $ | ^ done $ }xs )
	{
	    my $new_label = $1;
	    
	    if ( ! $record->{record_label} && defined $new_label && $new_label ne '' )
	    {
		if ( keys %$record )
		{
		    print "ADDED LABEL: $new_label\n";
		}
		
		$record->{record_label} = $new_label;
		next;
	    }
	    
	    if ( keys %$record )
	    {
		save_record($record);
		$record = { };
	    }
	    
	    if ( defined $new_label && $new_label ne '' )
	    {
		$record = load_record($new_label);
	    }

	    next;
	}

	elsif ( $input eq 'cancel' )
	{
	    $operation = '';
	    @records = ();
	    $record = { };
	    $state = 'OP';
	    next;
	}

	elsif ( $input eq 'clear' )
	{
	    remove_record($record->{record_label});
	    $record = { };
	}
	
	elsif ( $input eq 'send' || $input eq 'done' )
	{
	    if ( keys %$record )
	    {
		save_record($record);
		$record = { };
	    }
	    
	    send_operation($operation, \@records);

	    @last_records = @records;
	    @records = ();
	    $last_operation = $operation;
	    
	    if ( $operation eq 'done' )
	    {
		$state = 'DONE';
	    }
	    
	    else
	    {
		$state = 'OP';
		$prompt = 'Operation: ';
	    }
	    
	    next;
	}
	
	elsif ( $input =~ qr{ ^ \s* (\w+) \s* (?: : | => | = ) \s* ( ["']? ) (.*) $ }xs )
	{
	    my $key = $1;
	    my $quote = $2;
	    my $value = $3;

	    $value =~ s/[\s,]+$//;

	    if ( $quote )
	    {
		$value =~ s/$quote$//;
	    }
	    
	    $value =~ s/\\./$1/g;
	    
	    $record->{$key} = $value;
	    next;
	}
	
	# elsif ( $input =~ qr{ ^ \s* (\w+) \s* (?: : | => | = )  \s* ['] ( [^']* ) ['] [\s,]* $ }xs )
	# {
	#     $record->{$1} = $2;
	#     next;
	# }
	
	# elsif ( $input =~ qr{ ^ \s* (\w+) \s* (?: : | => | = )  \s* (.*) }xs )
	# {
	#     my $key = $1;
	#     my $val = $2;
	#     $val =~ s/[\s,]+$//;
	#     $record->{$key} = $val;
	#     next;
	# }

	else
	{
	    print "UNRECOGNIZED INPUT: $input\n";
	    next;
	}
    }
    
    else
    {
	die "bad state '$state'";
    }
}

done_testing();
exit;


sub BAIL_OUT {
    exit(2);
}


sub send_operation {

    my ($operation, $records) = @_;

    dd($records);
    
    my (@r) = $T->send_records("$operation.json", "test add", json => $records);
    
    # my @warnings = $T->get_warnings();

    # foreach my $w ( @warnings )
    # {
    # 	print "WARNING: $w\n";
    # }
    
    # my @errors = $T->get_errors();

    # foreach my $w ( @errors )
    # {
    # 	print "ERROR: $w\n";
    # }

    dd(\@r);
}


sub list_records {
    
    foreach my $i ( 0..$#records )
    {
	if ( $records[$i]{record_label} )
	{
	    print "RECORD $i: $records[$i]{record_label}\n";
	}
	
	else
	{
	    print "RECORD $i: #" . $i+1 . "\n";
	}
    }
    
}


sub print_record {

    my ($label) = @_;
    
    foreach my $i ( 0..$#records )
    {
	if ( $records[$i]{record_label} eq $label )
	{
	    dd($records[$i]);
	}
    }
    
}

    
sub remove_record {

    my ($label) = @_;
    
    foreach my $i ( 0..$#records )
    {
	if ( $records[$i]{record_label} eq $label )
	{
	    splice(@records, $i, 1);
	    last;
	}
    }
}


sub load_record {

    my ($label) = @_;

    unless ( $label )
    {
	return { };
    }
    
    if ( $label )
    {
	foreach my $i ( 0..$#records )
	{
	    if ( $records[$i]{record_label} eq $label )
	    {
		my %new = %{$records[$i]};
		return \%new;
	    }
	}
    }
    
    return { record_label => $label };
}


sub save_record {

    my ($record) = @_;
    
    my $label = $record->{record_label};

    if ( $label )
    {
	foreach my $i ( 0..$#records )
	{
	    if ( $records[$i]{record_label} eq $label )
	    {
		$records[$i] = $record;
		return;
	    }
	}
    }
    
    push @records, $record;
}
