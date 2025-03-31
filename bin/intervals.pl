#!/usr/bin/env perl
# 
# intervals.pl
# 
# Manage the definitions of geologic time intervals in the Paleobiology Database,
# in conjunction with a spreadsheet which holds the master interval definitions.

use strict;

use lib 'lib';
use utf8;

use CoreFunction qw(connectDB);
use TableDefs qw(%TABLE);
use CoreTableDefs;

use Getopt::Long qw(:config bundling no_auto_abbrev permute);

use JSON;
use Encode;
use LWP::UserAgent;
use Carp qw(croak);
use List::Util qw(any max min);

use feature 'say';
use feature 'fc';

our ($MACROSTRAT_INTERVALS) = "https://macrostrat.org/api/defs/intervals";
our ($MACROSTRAT_TIMESCALES) = "https://macrostrat.org/api/defs/timescales";

our ($AUTHORIZER_NO);

our ($DBNAME);
our ($CREATE_INTERVALS) = 0;
our ($REMOVE_INTERVALS) = 0;
our ($UPDATE_INTERVALS) = 0;
our ($UPDATE_MAX) = 0;
our ($UPDATE_MIN) = 0;
our ($UPDATE_MA) = 0;
our ($CREATE_SCALES) = 0;
our ($REMOVE_SCALES) = 0;
our ($UPDATE_SCALES) = 0;
our ($UPDATE_SEQUENCES) = 0;

# The input data and its parsed content are stored in the following globals.
# Yes, I know that's not consistent with best coding practice.

our ($FORMAT, $PARSER, $LINEEND);
our (@FIELD_LIST, %FIELD_MAP);
our (%INTERVAL_NAME, %INTERVAL_NUM, @ALL_INTERVALS);
our (%SCALE_NAME, %SCALE_NUM, %SCALE_INTS, @SCALE_NUMS, %SCALE_CHECKED, %SCALE_SELECT);
our (%DIFF_NAME, %DIFF_INT, @DIFF_MISSING, @DIFF_EXTRA);
our (%DIFF_SCALE, @DIFF_MISSING_SCALES);
our ($HAS_UNC, $T_AGE, $B_AGE, $T_UNC, $B_UNC);
our (@ERRORS);

# The following regexes validate ages and colors respectively.

our ($AGE_RE) = qr{ ^ \d+ (?: [.]\d* )? $ }x;
our ($UNC_RE) = qr{ ^ ~ $ | ^ \d+ (?: [.]\d* )? $ }x;
our ($AGE_UNC_RE) = qr{ ^ (?: (~) \s*)? (\d [.\d]*) (?: \s* ± \s* (\d [.\d]*))? $}x;
our ($AGE_RANGE_RE) = qr{ ^ (\d [.\d]*) \s* - \s* (\d [.\d]*) $ }x; 
our ($COLOR_RE) = qr{ ^ \# [0-9A-F]{6} $ }x;

# Allowed interval types:

our (%INTERVAL_TYPE) = (eon => 1, era => 1, period => 1, epoch => 1,
			subepoch => 1, age => 1, subage => 1,
			zone => 1, chron => 1, bin => 1);

our (%TYPE_LABEL) = (eon => 'Eons', era => 'Eras', period => 'Periods',
		     epoch => 'Epochs', subepoch => 'Subepochs', 
		     age => 'Ages', subage => 'Subages', zone => 'Zones');

# Allowed boundary types:

our (%BOUND_TYPE) = (top => 1, base => 1, 'use' => 1, 
		     anchor => 1, gssp => 1, def => 1, interpolate => 1);

our (%DEFN_BOUND) = (anchor => 1, gssp => 1, def => 1, interpolate => 1);

# Allowed actions:

our (%ACTION_TYPE) = (REMOVE => 1, RENAME => 1, COALESCE => 1);

# The following scales are used in generating values for stage_no, epoch_no, etc.:

our ($INTL_SCALE) = '1';
our ($CENO_SCALE) = '2';
our ($BIN_SCALE) = '10';

# The following value is used for specifying extra boundaries.

our ($EMPTY_INTERVAL) = '9999';

# Do not buffer STDOUT.

$| = 1;

# Start by parsing command-line options.

my ($opt_quiet, $opt_verbose, $opt_url, $opt_format, $opt_file, $opt_output, $opt_interval,
    $opt_dbname, $opt_force, $opt_debug, $opt_help);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "url|u=s" => \$opt_url,
	   "format=s" => \$opt_format,
	   "file|f=s" => \$opt_file,
	   "out|o=s" => \$opt_output,
	   "interval|i=s" => \$opt_interval,
	   "db=s" => \$opt_dbname,
	   "force" => \$opt_force,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

# The first remaining argument specifies a subcommand.

my ($CMD, @REST) = @ARGV;

my $DBNAME;

# If --help was specified, display the help message.

if ( $opt_help )
{
    &ShowHelp;
    exit;
}

# If the 'url' and 'out' options were given, the command defaults to 'fetch'. Otherwise,
# it defaults to 'help'.

if ( $opt_url && $opt_output )
{
    $CMD ||= 'fetch';
}

else
{
    $CMD ||= 'help';
}

# The subcommand 'help' prints out a help message.

if ( $CMD eq 'help' )
{
    &ShowHelp;
    exit;
}

# The subcommand 'fetch' fetches the contents of the specified spreadsheet and
# prints it to STDOUT or to a file.

elsif ( $CMD eq 'fetch' )
{
    &FetchSheet(@REST);
}

# The subcommand 'check' reads the specified spreadsheet data and checks one or more
# timescales (or all of them) for consistency.

elsif ( $CMD eq 'check' )
{
    &ReadSheet;
    &CheckScales(@REST);
    &ReportErrors;
}

# The subcommand 'print' reads the specified spreadsheet data, interpolates ages, and
# prints out the result. This enables confirmation that the data is being read properly.

elsif ( $CMD eq 'print' )
{
    &ReadSheet;
    &PrintScales(@REST);
    &ReportErrors;
}

# The subcommand 'diagram' reads the specified spreadsheet data and prints out one or
# more timescales as a sequence of boxes. This enables visual confirmation that the
# timescale boundaries have been input correctly.

elsif ( $CMD eq 'diagram' )
{
    &ReadSheet;
    
    if ( $REST[0] eq 'pbdb' || $REST[0] eq 'test' )
    {
	&DiagramPBDBScales(@REST);
    }
    
    else
    {
	&DiagramScales(@REST);
    }
    
    &ReportErrors;
}

# The subcommand 'diff' prints out a table of differences between the specified
# spreadsheet contents and Macrostrat, or between the spreadsheet contents and
# the PBDB.

elsif ( $CMD eq 'diff' )
{
    my $SUBCMD = shift @REST;
    &ReadSheet;
    
    if ( $SUBCMD eq 'macrostrat' )
    {
	&DiffMacrostrat(@REST);
	&ReportErrors;
    }
    
    elsif ( $SUBCMD eq 'pbdb' )
    {
	$DBNAME = $opt_dbname || 'pbdb';
	&DiffPBDB('diff', $DBNAME, @REST);
	&ReportErrors;
    }
    
    elsif ( $SUBCMD eq 'test' )
    {
	$DBNAME = $opt_dbname || 'test';
	&DiffPBDB('diff', $DBNAME, @REST);
	&ReportErrors;
    }
    
    elsif ( ! $SUBCMD )
    {
	die "You must specify either 'macrostrat', 'pbdb', or 'test'\n";
    }
    
    else
    {
	die "Invalid subcommand '$SUBCMD'\n";
    }
}

# The subcommand 'update' updates the PBDB interval tables to match the contents of the
# spreadsheet. The subcommand 'debug' prints out the SQL statements that would be used
# to perform this update.

elsif ( $CMD eq 'update' || $CMD eq 'debug' )
{
    my $SUBCMD = shift @REST;
    &ReadSheet;
    
    $opt_debug = 1 if $CMD eq 'debug';
    
    if ( $SUBCMD eq 'pbdb' )
    {
	$DBNAME = $opt_dbname || 'pbdb';
	&DiffPBDB('update', $DBNAME, @REST) if @ERRORS == 0 || $opt_force;
	&ReportErrors;
    }
    
    elsif ( $SUBCMD eq 'test' )
    {
	$DBNAME = $opt_dbname || 'PBDBtest';
	&DiffPBDB('update', $DBNAME, @REST) if @ERRORS == 0 || $opt_force;
	&ReportErrors;
    }
    
    elsif ( ! $SUBCMD )
    {
	die "You must specify either 'pbdb' or 'test'\n";
    }
    
    else
    {
	die "Invalid subcommand '$SUBCMD'\n";
    }
}

# The subcommand 'backup' creates backup tables for the PBDB interval tables. The
# subcommand 'restore' restores the tables to match the contents of the backup tables.
# The subcommand 'ints' synchronizes the 'intervals' table with the 'interval_data' table.

elsif ( $CMD eq 'backup' || $CMD eq 'restore' || $CMD eq 'ints' || $CMD eq 'validate' )
{
    my $SUBCMD = shift @REST;
    
    if ( $SUBCMD eq 'pbdb' )
    {
	$DBNAME = $opt_dbname || 'pbdb';
	&PBDBCommand($CMD, $DBNAME, @REST);
	&ReportErrors;
    }
    
    elsif ( $SUBCMD eq 'test' )
    {
	$DBNAME = $opt_dbname || 'test';
	&PBDBCommand($CMD, $DBNAME, @REST);
	&ReportErrors;
    }
    
    elsif ( ! $SUBCMD )
    {
	die "You must specify either 'pbdb' or 'test'\n";
    }
    
    else
    {
	die "Invalid subcommand '$SUBCMD'\n";
    }
}

else
{
    die "Invalid subcommand '$CMD'\n";
}


# FetchSheet ( url )
# 
# Fetch the interval spreadsheet data from the specified URL, in the specified
# format defaulting to CSV. If the --url option was given, use that value. Write
# the fetched data to STDOUT, so that it can be written to local storage or
# piped to some other process.

sub FetchSheet {
    
    my ($cmd_url) = @_;
    
    my $content;
    
    if ( $opt_url )
    {
	$content = FetchData($opt_url, 1);
    }
    
    elsif ( $cmd_url )
    {
	$content = FetchData($cmd_url, 1);
    }
    
    else
    {
	die "You must specify a URL to fetch\n";
    }
    
    if ( $content )
    {
	&SetupOutput;
	print $content;
    }
}


# ReadSheet ( )
# 
# Read an interval spreadsheet in TSV or CSV format from the specified URL or
# filename, or from STDIN. Put the data into a structure that can be used for
# checking and updating. Check the data for self consistency, as follows:
# 
# 1. Every scale name must be associated with a single scale number.
# 2. Every interval name must be associated with a unique interval number, and vice versa.
# 3. Every interval must have exactly one definition, and may be used zero or
#    more times in other scales.
# 

sub ReadSheet {
    
    my ($header, @data);
    
    # Read data from a source specified by the command-line options.
    
    if ( $opt_url )
    {
	my $content = FetchData($opt_url, 1);
	
	($header, @data) = map { decode_utf8($_) } split /[\n\r]+/, $content;
    }
    
    elsif ( $opt_file && $opt_file eq '-' )
    {
	($header, @data) = map { decode_utf8($_) } <STDIN>;
    }
    
    elsif ( $opt_file )
    {
	open(my $fh, '<', $opt_file) || die "Could not read $opt_file: $!";
	
	($header, @data) = map { decode_utf8($_) } <$fh>;
	
	close $fh;
    }
    
    else
    {
	die "You must specify a filename or url\n";
    }
    
    # The first line must contain column headings.
    
    my $line_no = 1;
    
    @FIELD_LIST = ParseLine($header);
    
    unless ( any { $_ eq 'interval_name' } @FIELD_LIST )
    {
	push @ERRORS, "The column headings are missing from the first row of this data";
	return;
    }
    
    # Run through the rest of the input data, processing each line as a possible scale
    # definition or interval definition. Keep track of line numbers, so that error
    # messages may be referred back to the row number in the input data. Lines that have
    # content only in the scale_name column are ignored, so these can be used as comments
    # or to separate different portions of the input data.
    
  LINE:
    foreach my $line ( @data )
    {
	my $record = LineToRecord($line, \@FIELD_LIST);
	
	$record->{line_no} = ++$line_no;
	
	my $scale_no = $record->{scale_no};
	my $scale_name = $record->{scale_name};
	
	my $interval_no = $record->{interval_no};
	my $interval_name = $record->{interval_name};
	
	my $t_type = $record->{t_type};
	my $b_type = $record->{b_type};
	
	# If the 'scale_no' column contains the value STOP, then stop here.
	
	last LINE if $record->{scale_no} eq 'STOP';
	
	# If the 'action' column contains the value SKIP, then ignore this line completely.
	
	next LINE if $record->{action} eq 'SKIP';
	
	# Otherwise, if this row contains both a scale_name and scale_no, it is either an
	# interval definition or a scale definition. Either way, create a record for this
	# scale if there isn't one already.
	
	if ( defined $scale_no && $scale_no ne '' && defined $scale_name && $scale_name ne '' )
	{
	    # The scale_no must be a positive integer, and scale_name must be non-numeric.
	    
	    unless ( $scale_no && $scale_no =~ /^\d+$/ )
	    {
		push @ERRORS, "at line $line_no, bad scale_no '$scale_no'";
		next LINE;
	    }
	    
	    unless ( $scale_name !~ /^\d/ )
	    {
		push @ERRORS, "at line $line_no, bad scale_name '$scale_name'";
		next LINE;
	    }
	    
	    unless ( $SCALE_NUM{$scale_no} )
	    {
		$SCALE_NUM{$scale_no} = { scale_no => $scale_no, 
					  scale_name => $scale_name,
					  line_no => $line_no };
		
		# Keep a list of the scale numbers in order as they are defined.
		
		push @SCALE_NUMS, $scale_no;
		
		# If there is no interval name on this line, and this is the first mention
		# of this scale_no, it represents a scale definition. The column 'type'
		# should contain the region to which the scale applies, and 'reference_no'
		# should contain the reference_no for the scale as a whole.
		
		unless ( $interval_name )
		{
		    $SCALE_NUM{$scale_no}{type} = $record->{type} 
			if $record->{type};
		    $SCALE_NUM{$scale_no}{color} = $record->{color}
			if $record->{color};
		    $SCALE_NUM{$scale_no}{reference_no} = $record->{reference_no} 
			if $record->{reference_no};
		    $SCALE_NUM{$scale_no}{authorizer_no} = $record->{authorizer_no}
		        if $record->{authorizer_no};
		    $SCALE_NUM{$scale_no}{action} = $record->{action}
		    if $record->{action};
		    
		    push @ERRORS, "at line $line_no, scale $scale_no has no authorizer_no"
			unless $record->{authorizer_no};
		}
	    }
	    
	    # If we have encountered this scale name before, it is an error if it now has
	    # a different scale number. It is okay for the same number to be used with
	    # different names, but each name must have a single number.
	    
	    if ( $SCALE_NAME{$scale_name} )
	    {
		if ( $scale_no ne $SCALE_NAME{$scale_name}{scale_no} )
		{
		    my $prevline = $SCALE_NAME{$scale_name}{line_no};
		    push @ERRORS, "at line $line_no, scale $scale_no inconsistent with line $prevline";
		}
	    }
	    
	    # Otherwise, store a reference to the scale definition under the scale name.
	    
	    else
	    {
		$SCALE_NAME{$scale_name} = $SCALE_NUM{$scale_no};
	    }
	}
	
	# All other lines are ignored. It is an error for a line to contain an interval
	# name without both a scale name and a scale number.
	
	elsif ( defined $interval_name && $interval_name ne '' )
	{
	    push @ERRORS, "at line $line_no, missing scale number or name";
	    next LINE;
	}
	
	# If the interval name is not empty, this row represents an interval definition.
	# Otherwise it is either a scale definition, in which case we are done, or else
	# the line is ignored.
	
	if ( defined $interval_name && $interval_name ne '' )
	{
	    push @ALL_INTERVALS, $record;
	}
	
	else
	{
	    next LINE;
	}
	
	# The value of interval_no must be a positive integer, and the value of
	# interval_name must be non-numeric.
	
	unless ( $interval_no && $interval_no =~ /^\d+$/ )
	{
	    if ( $interval_no )
	    {
		push @ERRORS, "at line $line_no, bad interval_no '$interval_no'";
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, missing interval_no";
	    }
	    
	    next LINE;
	}
	
	if ( $interval_name =~ /^\d/ )
	{
	    push @ERRORS, "at line $line_no, bad interval_name '$interval_name'";
	    next LINE;
	}
	
	# If we have encountered this interval name before, check that its use is
	# consistent. There must be a 1-1 correspondence between interval names and
	# numbers, and each interval must be defined exactly once. An interval name can
	# also be used an arbitrary number of times with 'use' in the t_type column.
	
	if ( $INTERVAL_NAME{$interval_name} )
	{
	    my $prevline = $INTERVAL_NAME{$interval_name}{line_no};
	    my $prev_no = $INTERVAL_NAME{$interval_name}{interval_no};
	    my $prev_type = $INTERVAL_NAME{$interval_name}{t_type};
	    
	    # It is an error for an interval name to be used with two different interval
	    # numbers.
	    
	    if ( $interval_no ne '' && $prev_no ne $interval_no )
	    {
		push @ERRORS, "at line $line_no, interval '$interval_name' inconsistent " . 
		    "with line $prevline";
		next LINE;
	    }
	    
	    # If t_type is 'use', then this record represents the use of an interval
	    # defined elsewhere. Check that b_type is either empty or is also 'use'.
	    
	    if ( $t_type eq 'use' )
	    {
		push @ERRORS, "at line $line_no, interval '$interval_name', bad b_type '$b_type'"
		    if $b_type && $b_type ne 'use';
	    }
	    
	    # Otherwise, if this interval was previously used but not defined, replace the
	    # placeholder record with the current one. This will be its definition.
	    
	    elsif ( $prev_type eq 'use' )
	    {
		$INTERVAL_NAME{$interval_name} = $record;
		$INTERVAL_NUM{$interval_no} = $record;
	    }
	    
	    # Otherwise, we have encountered a second definition for the same interval.
	    # This is an error for anything other than the empty interval.
	    
	    elsif ( $interval_no ne $EMPTY_INTERVAL )
	    {
		push @ERRORS, "at line $line_no, interval '$interval_name' already " .
		    "defined at line $prevline";
		next LINE;
	    }
	}
	
	# It is an error if we have a previously defined interval with the same number.
	
	elsif ( $INTERVAL_NUM{$interval_no} )
	{
	    my $prevline = $INTERVAL_NUM{$interval_no}{line_no};
	    push @ERRORS, "at line $line_no, interval $interval_no inconsistent " .
		"with line $prevline";
	    next LINE;
	}
	
	# If this is the first time we have encountered this interval name, and its number
	# is unique, store this record in the INTERVAL_NAME hash under the name and in the
	# INTERVAL_NUM hash under the number.
	
	else
	{
	    $INTERVAL_NAME{$interval_name} = $record;
	    $INTERVAL_NUM{$interval_no} = $record;
	}
	
	# Keep a list of the interval records corresponding to each scale number.
	
	push $SCALE_INTS{$scale_no}->@*, $record;
	
	# If the action for this interval is 'REMOVE' or 'COALESCE', then we do not check
	# it any further. Otherwise, check various columns to make sure they have correct
	# values.
	
	if ( $record->{action} =~ /^REMOVE|^COALESCE/ )
	{
	    next LINE;
	}
	
	# Check the action.
	
	if ( $record->{action} && ! $ACTION_TYPE{$record->{action}} )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad action '$record->{action}'";
	}
	
	# Check the interval type and color.
	
	unless ( $interval_no eq $EMPTY_INTERVAL || $INTERVAL_TYPE{$record->{type}} )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad type '$record->{type}'";
	}
	
	if ( $record->{color} && $record->{color} !~ $COLOR_RE )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad color '$record->{color}'";
	}
	
	# If reference_no is not empty, it must be a non-negative integer.
	
	if ( $record->{reference_no} && $record->{reference_no} !~ /^\d+$/ )
	{
	    push @ERRORS, "at line $line, bad reference_no '$record->{reference_no}'";
	}
	
	# Check 't_type' and 'b_type'.
	
	my $t_type = $record->{t_type};
	my $b_type = $record->{b_type};
	
	unless ( $t_type && $BOUND_TYPE{$t_type} )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad t_type '$t_type'";
	}
	
	if ( $t_type eq 'use' && $b_type && $b_type ne 'use' )
	{
	    push @ERRORS, "at line $line_no, interval_no $interval_no bad b_type '$b_type'";
	}
	
	unless ( $t_type eq 'use' || $interval_no eq $EMPTY_INTERVAL || 
		 $b_type && $BOUND_TYPE{$b_type} )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad b_type '$b_type'";
	}
	
	# If t_type is 'use', then the value of top (and base if specified) must
	# be the same as interval_name.
	
	if ( $t_type eq 'use' )
	{
	    if ( $record->{top} ne $record->{interval_name} )
	    {
		push @ERRORS, "at line $line_no, interval $interval_no bad top '$record->{top}'";
	    }
	    
	    if ( $record->{base} && $record->{base} ne $record->{interval_name} )
	    {
		push @ERRORS, "at line $line_no, interval $interval_no bad base '$record->{base}'";
	    }		
	}
	
	# If t_type is anything other than 'use', then top and base cannot be the same as
	# interval_name.
	
	else
	{
	    if ( $record->{top} && $record->{top} eq $record->{interval_name} )
	    {
		push @ERRORS, "at line $line, interval $interval_no top cannot be self-referential";
	    }
	
	    if ( $record->{base} && $record->{base} eq $record->{interval_name} )
	    {
		push @ERRORS, "at line $line, interval $interval_no base cannot be self-referential";
	    }
	}
	
	# If the value of 'top' matches the pattern for an age with possible uncertainty
	# or else matches the pattern for an age range, unpack it into an age and
	# uncertainty. If the value is empty, or if it contains a digit but fails to match
	# either pattern, record an error.
	
	if ( $DEFN_BOUND{$t_type} )
	{
	    $record->{t_bound} = $t_type;
	    
	    if ( defined $record->{top} && $record->{top} =~ $AGE_UNC_RE )
	    {
		$record->{t_age} = $2;
	    
		if ( $1 ) { $record->{t_unc} = 0; }
		elsif ( $3 ) { $record->{t_unc} = $3; }
		else { $record->{t_unc} = undef; }
	    }

	    elsif ( defined $record->{top} && $record->{top} =~ $AGE_RANGE_RE )
	    {
		$record->{t_age} = ($1 + $2) / 2;
		$record->{t_unc} = &ComputeUncertainty($2, $1);
	    }
	
	    elsif ( $record->{top} )
	    {
		push @ERRORS, "at line $line_no, interval $interval_no bad top age '$record->{top}'";
	    }
	
	    else
	    {
		push @ERRORS, "at line $line_no, interval $interval_no missing top age";
	    }
	}
	
	elsif ( defined $record->{top} && $record->{top} =~ /^\d/ )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad top reference '$record->{top}'";
	}
	
	# If the value of 'base' matches the pattern for an age with possible uncertainty
	# or else matches the pattern for an age range, unpack it into an age and
	# uncertainty. If the value is empty but t_type is not empty, or if the value
	# contains a digit but does not match either pattern, record an error.
	
	if ( $DEFN_BOUND{$b_type} )
	{
	    $record->{b_bound} = $b_type;
	    
	    if ( defined $record->{base} && $record->{base} =~ $AGE_UNC_RE )
	    {
		$record->{b_age} = $2;
		
		if ( $1 ) { $record->{b_unc} = 0; }
		elsif ( $3 ) { $record->{b_unc} = $3; }
		else { $record->{b_unc} = undef; }
	    }
	    
	    elsif ( defined $record->{base} && $record->{base} =~ $AGE_RANGE_RE )
	    {
		$record->{b_age} = ($1 + $2) / 2;
		$record->{b_unc} = &ComputeUncertainty($2, $1);
	    }
	    
	    elsif ( $record->{base} )
	    {
		push @ERRORS, "at line $line_no, interval $interval_no bad base age '$record->{base}'";
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, interval_no $interval_no missing base age";
	    }
	}
	
	elsif ( defined $record->{base} && $record->{base} =~ /^\d/ )
	{
	    push @ERRORS, "at line $line_no, interval $interval_no bad base reference '$record->{base}'";
	}
	
	# Check the authorizer_no.
	
	if ( $record->{authorizer_no} ne '' && $record->{authorizer_no} !~ /^\d+$/ )
	{
	    push @ERRORS, "at line $line_no, interval_no $interval_no bad authorizer_no " .
		"'$record->{authorizer_no}'";
	}
    }
    
    my $a = 1;	# we can pause here when debugging
}


# ParseLine ( line )
# 
# Parse a line in CSV or TSV format and return a list of column values. If the format has
# not already been determined, guess based on whether the first line contains more commas
# or tabs. If the line ends in some combination of carriage returns and newlines, remember
# that for later generation of output.

sub ParseLine {
    
    my ($line) = @_;
    
    unless ( $FORMAT )
    {
	my $commas = $line =~ tr/,//;
	my $tabs = $line =~ tr/\t//;
	
	# If there are more commas than tabs, assume that the format is CSV. The Text::CSV
	# module is required at runtime so that this program can be used with TSV files if
	# that module is not available.
	
	if ( $commas > $tabs )
	{
	    $FORMAT = 'csv';
	    require "Text/CSV.pm";
	    $PARSER = Text::CSV->new
	}
	
	else
	{
	    $FORMAT = 'tsv';
	}
	
	# If the input lines end with a line end sequence, remember that for subsequent
	# generation of output. Otherwise, guess a single newline.
	
	if ( $line =~ /([\n\r]+)/ )
	{
	    $LINEEND = $1;
	}
	
	else
	{
	    $LINEEND = "\n";
	}
    }
    
    # For the first and subsequent lines, if the format is CSV then use the previously
    # instantiated parser object.
    
    if ( $FORMAT eq 'csv' )
    {
	if ( $PARSER->parse($line) )
	{
	    return $PARSER->fields;
	}
	
	else
	{
	    $PARSER->error_diag;
	    exit 2;
	}
    }
    
    # Otherwise, remove any line end sequence and split the remainder by tabs.
    
    else
    {
	$line =~ s/[\n\r]+$//s;
	return split /\t/, $line;
    }
}


# GenerateLine ( field... )
# 
# Combine all of the arguments into a single output line. Use the same format and line end
# sequence as the input data.

sub GenerateLine {
    
    my (@fields) = @_;
    
    if ( $FORMAT eq 'csv' )
    {
	if ( $PARSER->combine(@fields) )
	{
	    return $PARSER->string . $LINEEND;
	}
	
	else
	{
	    $PARSER->error_diag;
	    exit 2;
	}
    }
    
    else
    {
	return join("\t", @fields) . $LINEEND;
    }
}


# LineToRecord ( line, fields )
# 
# Parse a line of input data and return a hashref. The second parameter must be an
# arrayref whose values are the column names.

sub LineToRecord {
    
    my ($line, $fields) = @_;
    
    my @columns = ParseLine($line);
    
    my $record = { };
    
    foreach my $i ( 0..$#columns )
    {
	my $field = $fields->[$i] || next;
	my $value = $columns[$i];
	
	if ( $value =~ /^"(.*)"$/ )
	{
	    $value = $1;
	    $value =~ s/\\"|""/"/g;
	}
	
	$record->{$field} = $value if defined $value && $value ne '';
    }
    
    return $record;
}


# RecordToLine ( record, fields )
# 
# Generate an output line from the specified record. The second parameter must be an
# arrayref whose values are the column names. Each output column will contain the value of
# the corresponding field in the record, or else the empty string.

sub RecordToLine {
    
    my ($record, $fields) = @_;
    
    my @fields = $fields->@*;
    my @values;
    
    foreach my $i ( 0..$#fields )
    {
	my $field = $fields[$i] || next;
	$values[$i] = $record->{$field} // '';
	
	if ( $values[$i] =~ /"/ )
	{
	    $values[$i] =~ s/"/\\"/g;
	    $values[$i] = '"' . $values[$i] . '"';
	}
    }
    
    return GenerateLine(@values);
}

# GenerateHeader ( )
# 
# Return an output line consisting of the column names read from the first line of the
# input data.

sub GenerateHeader {
    
    return GenerateLine(@FIELD_LIST);
}


# ReportErrors ( )
# 
# If @ERRORS is not empty, print each of its elements to STDERR.

sub ReportErrors {
    
    if ( @ERRORS )
    {
	my $count = scalar(@ERRORS);
	
	say STDERR "\nFound $count errors in this spreadsheet:";
	
	foreach my $e ( @ERRORS )
	{
	    say STDERR "  $e";
	}
    }
}


# CheckScales ( scale... )
# 
# Check the specified scale(s) for consistency. If the argument is 'all' then check all
# scales. A numeric argument will select the correspondingly numbered scale, and a
# non-numeric argument will select all scales which match it as a regexp.

sub CheckScales {
    
    SelectScales(@_);
    
    return unless %SCALE_SELECT;
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no} )
	{
	    my $errors = CheckOneScale($scale_no);
	    	    
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    my $count = grep { $_->{action} !~ /^REMOVE|^COALESCE/ } $SCALE_INTS{$scale_no}->@*;
	    
	    if ( $errors )
	    {
		say STDERR "Timescale $scale_no '$name' had *** $errors errors ***";
	    }
	    
	    elsif ( $count )
	    {
		say STDERR "Timescale $scale_no '$name' passes all checks";
	    }
	    
	    else
	    {
		say STDERR "Timescale $scale_no '$name' is empty";
	    }
	}
    }
}


# SelectScales ( arg... )
# 
# Add entries to %SCALE_SELECT based on the arguments. An argument of 'all' selects all
# scales. A numeric argument or range selects any scales whose scale_no matches. A
# non-numeric argument selects any scales whose scale_name matches.

sub SelectScales {
    
    my @scale_list;
    
    my $bad_argument;
    
    unless ( @_ )
    {
	$SCALE_SELECT{all} = 1;
    }
    
    foreach my $t ( @_ )
    {
	if ( $t eq 'all' )
	{
	    $SCALE_SELECT{all} = 1;
	}
	
	elsif ( $SCALE_INTS{$t} )
	{
	    push @scale_list, $t unless $SCALE_SELECT{$t};
	    $SCALE_SELECT{$t} = 1;
	}
	
	elsif ( $t =~ /^(\d+)-(\d+)$/ )
	{
	    my $min = $1;
	    my $max = $2;
	    my $found;
	    
	    foreach my $s ( @SCALE_NUMS )
	    {
		if ( $s >= $min && $s <= $max )
		{
		    push @scale_list, $s unless $SCALE_SELECT{$s};
		    $SCALE_SELECT{$s} = 1;
		    $found++;
		}
	    }
	    
	    unless ( $found )
	    {
		warn "No timescales in range $min-$max\n";
		$bad_argument = 1;
	    }
	}
	
	elsif ( $t =~ /[a-z]/ )
	{
	    my $re = qr{(?i)$t};
	    my $found;
	    
	    foreach my $s ( @SCALE_NUMS )
	    {
		if ( $SCALE_NUM{$s}{scale_name} =~ $re )
		{
		    push @scale_list, $s unless $SCALE_SELECT{$s};
		    $SCALE_SELECT{$s} = 1;
		    $found++;
		}
	    }
	    
	    unless ( $found )
	    {
		warn "Unrecognized timescale '$t'\n";
		$bad_argument = 1;
	    }
	}
	
	else
	{
	    warn "Unrecognized timescale '$t'\n";
	    $bad_argument = 1;
	}
    }
    
    if ( @scale_list == 1 && $scale_list[0] eq $INTL_SCALE && $bad_argument )
    {
	@scale_list = ();
    }
    
    return @scale_list;
}


# CheckOneScale ( scale_no )
# 
# Perform a series of consistency checks on a timescale, specified by scale_no. If any
# errors are found, they are appended to @ERRORS and the number of errors is returned.
# Otherwise, this subroutine returns an empty result.

sub CheckOneScale {
    
    my ($scale_no) = @_;
    
    # Return immediately if this scale has already been checked. Otherwise, mark it as
    # checked. This ensures that each scale is checked (and the ages set and interpolated)
    # only once.
    
    return if $SCALE_CHECKED{$scale_no};
    
    $SCALE_CHECKED{$scale_no} = 1;
    
    my $scale_name = $SCALE_NUM{$scale_no}{scale_name};
    
    my ($has_interpolation, $has_reference, @errors);
    
    # Iterate over all of the interval records associated with the specified timescale,
    # checking for value errors and consistency errors.
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	my $line = $i->{line_no};
	my $name = $i->{interval_name};
	my $action = $i->{action};
	
	# If the value of 't_type' is 'use', it is an error for the action to be
	# non-empty. 
	
	if ( $i->{t_type} eq 'use' && $i->{action} ) # && $i->{action} ne 'RENAME' )
	{
	    push @errors, "at line $line, cannot remove or rename an interval with 'use'";
	    next;
	}
	
	# If the action is 'REMOVE', no other checks are necessary.
	
	if ( $action =~ /^REMOVE/ )
	{
	    next;
	}
	
	# If the action is 'COALESCE', check that the interval name) to be coalesced
	# with is defined. No other checks are necessary.
	
	elsif ( $action =~ /^COALESCE (\w.*)/ )
	{
	    my $coalesce = $1;
	    
	    # Generate an error if the coalesce value is not the name of a known
	    # interval, or if it corresponds to an interval that itself will be
	    # coalesced or removed.
	    
	    if ( $coalesce =~ /^(.*?)-(.*)$/ )
	    {
		my $interval1 = $1;
		my $interval2 = $2;
		
		push @errors, "at line $line, unrecognized interval '$1'"
		    unless $INTERVAL_NAME{$interval1};
		
		push @errors, "at line $line, first coalesce interval has removal action"
		    if $INTERVAL_NAME{$interval1} && $INTERVAL_NAME{$interval1}{action} =~
		    /^REMOVE|^COALESCE/;
		
		push @errors, "at line $line, unrecognized interval '$2'"
		    unless $INTERVAL_NAME{$interval2};
	    
		push @errors, "at line $line, second coalesce interval has removal action"
		    if $INTERVAL_NAME{$interval2} && $INTERVAL_NAME{$interval2}{action} =~
		    /^REMOVE|^COALESCE/;
	    }
	    
	    elsif ( $INTERVAL_NAME{$coalesce} )
	    {
		push @errors, "at line $line, cannot coalesce with interval to be removed"
		    if $INTERVAL_NAME{$coalesce}{action} =~ /^REMOVE|^COALESCE/;
	    }
	    
	    else
	    {
		push @errors, "at line $line, unrecognized interval '$coalesce'";
	    }
	    
	    next;
	}
	
	# If the action is neither of the above nor RENAME, generate an error.
	
	elsif ( $action && $action !~ /^RENAME/ )
	{
	    push @errors, "at line $line, invalid action '$action'";
	}
	
	# If the value of t_type is 'top', 'base', or 'use', evaluate the reference if we
	# can. If the reference is to a different scale, make sure that one is checked first.
	
	if ( $i->{t_type} =~ /^top|^base|^use/ )
	{
	    my ($top_interval, $which) = TopAgeRef($i);
	    
	    if ( ref $top_interval )
	    {
		# If we have not yet checked the referenced scale, check it now.
		
		my $top_scale_no = $top_interval->{scale_no};
		
		unless ( $SCALE_CHECKED{$top_scale_no} )
		{
		    CheckOneScale($top_scale_no);
		}
		
		# Now set the t_age, t_unc, and t_bound fields from the referenced
		# interval.
		
		if ( $which eq 'top' )
		{
		    $i->{t_age} = $top_interval->{t_age};
		    $i->{t_unc} = $top_interval->{t_unc} if defined $top_interval->{t_unc};
		    $i->{t_intp} = $top_interval->{t_intp} if defined $top_interval->{t_intp};
		    $i->{t_bound} = $top_interval->{t_bound};
		}
		
		else
		{
		    $i->{t_age} = $top_interval->{b_age};
		    $i->{t_unc} = $top_interval->{b_unc} if defined $top_interval->{b_unc};
		    $i->{t_intp} = $top_interval->{b_intp} if defined $top_interval->{b_intp};
		    $i->{t_bound} = $top_interval->{b_bound};
		}
		
		# If this boundary is a reference to a different scale and the t_ref field
		# is not empty, then we have a reference to use below for interpolation.
		
		if ( $top_scale_no ne $scale_no && defined $i->{t_ref} && $i->{t_ref} ne '' )
		{
		    $i->{t_is_reference} = 1;
		    $has_reference = 1;
		}
	    }
	    
	    # If a scalar value is returned for $top_interval, it represents an error
	    # message.
	    
	    else
	    {
		push @errors, "at line $line, interval '$name': $top_interval";
	    }
	}
	
	# If the value of t_type is 'interpolate', we have at least one boundary to
	# interpolate. Bounds with other types are ignored for this procedure.
	
	elsif ( $i->{t_type} eq 'interpolate' )
	{
	    $has_interpolation = 1;
	}
	
	# If the value of b_type is 'top' or 'base', or t_type is 'use', evaluate the
	# reference if we can. But skip this for empty intervals. If the reference is to a
	# different scale, make sure that scale is checked first.
	
	if ( ($i->{b_type} =~ /^top|^base/ || $i->{t_type} eq 'use') &&
	     $i->{interval_no} ne $EMPTY_INTERVAL )
	{
	    my ($base_interval, $which) = BaseAgeRef($i);
	    
	    if ( ref $base_interval )
	    {
		# If we have not yet checked the referenced scale, check it now.
		
		my $base_scale_no = $base_interval->{scale_no};
		
		unless ( $SCALE_CHECKED{$base_scale_no} )
		{
		    CheckOneScale($base_scale_no);
		}
		
		# Set the base boundary fields from the referenced interval.
		
		if ( $which eq 'top' )
		{
		    $i->{b_age} = $base_interval->{t_age};
		    $i->{b_unc} = $base_interval->{t_unc} if defined $base_interval->{t_unc};
		    $i->{b_intp} = $base_interval->{t_intp} if defined $base_interval->{t_intp};
		    $i->{b_bound} = $base_interval->{t_bound};
		}
		
		else
		{
		    $i->{b_age} = $base_interval->{b_age};
		    $i->{b_unc} = $base_interval->{b_unc} if defined $base_interval->{b_unc};
		    $i->{b_intp} = $base_interval->{b_intp} if defined $base_interval->{b_intp};
		    $i->{b_bound} = $base_interval->{b_bound};
		}
		
		# If this boundary is a reference to a different scale and the t_ref field
		# of this interval is not empty, then we have a reference to use below for
		# interpolation.
		
		if ( $base_scale_no ne $scale_no && defined $i->{b_ref} && $i->{b_ref} ne '' )
		{
		    $i->{b_is_reference} = 1;
		    $has_reference = 1;
		}
	    }
	    
	    # If a scalar value is returned for $base_interval, it represents an error
	    # message.
	    
	    else
	    {
		push @errors, "at line $line, interval '$name': $base_interval";
	    }
	}
	
	# If the value of t_type is 'interpolate', we have at least one boundary to
	# interpolate. Bounds with other types are ignored for this procedure.
			
	elsif ( $i->{b_type} eq 'interpolate' )
	{
	    $has_interpolation = 1;
	}
    }
    
    # If no errors have been found, and this scale has at least one 'interpolate' boundary
    # and at least one 'reference' boundary, check to see if interpolation is possible.
    
    my (%bound_type, %actual_value, %intp_value);
    
    if ( ! @errors && $has_interpolation && $has_reference )
    {
	foreach my $int ( $SCALE_INTS{$scale_no}->@* )
	{
	    next if $int->{action} =~ /^REMOVE|^COALESCE/;
	    
	    my $line = $int->{line_no};
	    my $int_no = $int->{interval_no};
	    my $t_ref = $int->{t_ref};
	    
	    if ( $int->{t_is_reference} )
	    {
		push @errors, "at line $line, interval $int_no top boundary type conflict"
		    if $bound_type{$t_ref} && $bound_type{$t_ref} ne 'reference';
		
		push @errors, "at line $line, interval $int_no top boundary value conflict"
		    if defined $actual_value{$t_ref} && 
		    $actual_value{$t_ref} != ($int->{t_intp} // $int->{t_age});
		
		$bound_type{$t_ref} = 'reference';
		$actual_value{$t_ref} = $int->{t_intp} // $int->{t_age};
	    }
	    
	    elsif ( $int->{t_type} eq 'interpolate' )
	    {
		push @errors, "at line $line, interval $int_no top boundary type conflict"
		    if $bound_type{$t_ref} && $bound_type{$t_ref} ne 'interpolate';
		
		$bound_type{$int->{t_age}} = 'interpolate';
	    }
	    
	    my $b_ref = $int->{b_ref};
	    
	    if ( $int->{b_is_reference} )
	    {
		push @errors, "at line $line, interval $int_no top boundary type conflict"
		    if $bound_type{$b_ref} && $bound_type{$b_ref} ne 'reference';
		
		push @errors, "at line $line, interval $int_no top boundary value conflict"
		    if defined $actual_value{$b_ref} && 
		    $actual_value{$b_ref} != ($int->{b_intp} // $int->{b_age});
		
		$bound_type{$b_ref} = 'reference';
		$actual_value{$b_ref} = $int->{b_intp} // $int->{b_age};
	    }
	    
	    elsif ( $int->{b_type} eq 'interpolate' )
	    {
		push @errors, "at line $line, interval $int_no top boundary type conflict"
		    if $bound_type{$b_ref} && $bound_type{$b_ref} ne 'interpolate';
		
		$bound_type{$int->{b_age}} = 'interpolate';
	    }
	}
    }
    
    # If we have not found any errors, iterate through the distinct bound ages and
    # interpolate those of type 'interpolate'.
    
    if ( ! @errors && $has_reference && $has_interpolation )
    {
	my @bound_list = sort { $a <=> $b } keys %bound_type;
	
	foreach my $i ( 0..$#bound_list )
	{
	    my $bound = $bound_list[$i];
	    
	    # The interpolated value is rounded to the same number of places as the
	    # original, or 1 place if the original has no decimal.
	    
	    my $places = 1;
	    
	    if ( $bound =~ /[.](\d+)/ )
	    {
		$places = length($1);
	    }
	    
	    if ( $bound_type{$bound} eq 'interpolate' )
	    {
		# Search for a reference boundary both above and below the boundary to be
		# interpolated.
		
		my ($above, $below);
		
		for ( my $j = $i; $j >= 0 ; $j-- )
		{
		    $above = $bound_list[$j], last if $bound_type{$bound_list[$j]} eq 'reference';
		}
		
		for ( my $j = $i; $j <= $#bound_list; $j++ )
		{
		    $below = $bound_list[$j], last if $bound_type{$bound_list[$j]} eq 'reference';
		}
		
		# If we find an anchor both above and below, generate a corrected value for
		# the boundary using linear interpolation.
		
		if ( defined $above && defined $below &&
		     defined $actual_value{$above} && defined $actual_value{$below} )
		{
		    if ( $actual_value{$above} != $above || $actual_value{$below} != $below )
		    {
			my $fraction = ($bound - $above) / ($below - $above);
			
			my $new = $actual_value{$above} + 
			    $fraction * ($actual_value{$below} - $actual_value{$above});
			
			$intp_value{$bound} = int($new * (10**$places)) / (10**$places);
		    }
		}
		
		# If we only have an anchor boundary above, generate a corrected value
		# using the difference between the uncorrected and corrected anchor age.
		
		elsif ( defined $above && defined $actual_value{$above} )
		{
		    if ( $actual_value{above} != $above )
		    {
			my $new = $bound + ($actual_value{$above} - $above);

			$intp_value{$bound} = int($new * (10**$places)) / (10**$places);
		    }
		}
		
		# Similarly if we only have an anchor boundary below.
		
		elsif ( defined $below && defined $actual_value{$below} )
		{
		    if ( $actual_value{$below} != $below )
		    {
			my $new = $bound + ($actual_value{$below} - $below);
			
			$intp_value{$bound} = int($new * (10**$places)) / (10**$places);
		    }
		}
		
		# The following case should never occur, but is included in case a
		# subsequent coding error allows it.
		
		else
		{
		    push @errors, "error interpolating '$scale_name': bad bracket for '$bound'";
		}
	    }
	}
	
	# Finally, run through the intervals again and assign the interpolated values
	# for both top and base bounds.
	
	foreach my $int ( $SCALE_INTS{$scale_no}->@* )
	{
	    next if $int->{interval_no} eq $EMPTY_INTERVAL;
	    next if $int->{action} =~ /^REMOVE|^COALESCE/;
	    
	    if ( $int->{t_bound} eq 'interpolate' )
	    {
		if ( defined $intp_value{$int->{t_age}} )
		{
		    $int->{t_intp} = $intp_value{$int->{t_age}};
		    $int->{t_bound} = $int->{t_intp} != $int->{t_age} ? 'interpolated' : 'defined';
		}
		
		else
		{
		    $int->{t_bound} = 'defined';
		}
	    }
	    
	    if ( $int->{b_bound} eq 'interpolate' )
	    {
		if ( defined $intp_value{$int->{b_age}} )
		{
		    $int->{b_intp} = $intp_value{$int->{b_age}};
		    $int->{b_bound} = $int->{b_intp} != $int->{b_age} ? 'interpolated' : 'defined';
		}
		
		else
		{
		    $int->{b_bound} = 'defined';
		}
	    }
	    
	    my $t_age = $int->{t_intp} // $int->{t_age};
	    my $b_age = $int->{b_intp} // $int->{b_age};
	    my $line = $int->{line_no};
	    my $name = $int->{interval_name};
	    
	    if ( $t_age eq $b_age )
	    {
		push @errors, "at line $line, interval '$name': top age and base age are the same";
	    }
	    
	    elsif ( $t_age > $b_age )
	    {
		push @errors, "at line $line, interval '$name': top age is greater than base age";
	    }
	    
	    unless ( $int->{t_bound} )
	    {
		push @errors, "at line $line, interval '$name': missing t_bound";
	    }
	    
	    unless ( $int->{b_bound} )
	    {
		push @errors, "at line $line, interval '$name': missing b_bound";
	    }
	}
    }	
    
    # If any errors were generated, append them to @ERRORS and return the error count
    # which will be a true value.
    
    if ( @errors )
    {
	push @ERRORS, @errors;
	return scalar(@errors);
    }
    
    # Otherwise, return false.
    
    else
    {
	return;
    }
}


# ComputeUncertainty ( b, a )
# 
# Compute the quantity (b-a)/2, correcting for floating point errors.

sub ComputeUncertainty {
    
    my ($b, $a) = @_;
    
    # Compute the maximum number of decimal places in b and a.
    
    my $places = 0;
    
    if ( $b =~ /[.](\d+)$/ )
    {
	$places = length($1);
    }
    
    if ( $a =~ /[.](\d+)$/ )
    {
	$places = length($1) if length($1) > $places;
    }
    
    # Compute the raw result, which may look like "0.530000000001" or "0.539999999998".
    
    my $raw = ($b - $a) / 2;
    
    # Round the result to precision computed above.
    
    return int($raw * (10**$places) + 0.5) / (10**$places);
}


# PrintScales ( scale... )
# 
# Print the specified timescale(s) as a spreadsheet, with interpolated ages.

sub PrintScales {
    
    my @scale_list = SelectScales(@_);
        
    return unless @scale_list;
    
    # Check each of the scales to be printed, and interpolate the boundary ages if
    # necessary.
    
    foreach my $scale_no ( @scale_list )
    {
	if ( my $errors = CheckOneScale($scale_no) )
	{
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    say STDERR "Timescale '$name' had *** $errors errors ***";
	}
    }
    
    my $output = GenerateHeader();
    
    $output .= "\n";
    
    foreach my $scale_no ( @scale_list )
    {
	my $scale_name = $SCALE_NUM{$scale_no}{scale_name};
	
	foreach my $int ( $SCALE_INTS{$scale_no}->@* )
	{
	    my $interval_no = $int->{interval_no};
	    my $line_no = $int->{line_no};
	    my $name = $int->{interval_name};
	    my $abbrev = $int->{abbrev};
	    my $t_type = $int->{t_bound};
	    my $t_age = $int->{t_intp} // $int->{t_age};
	    my $t_unc = $int->{t_unc};
	    my $t_ref = $int->{t_ref};
	    my $b_type = $int->{b_bound};
	    my $b_age = $int->{b_intp} // $int->{b_age};
	    my $b_unc = $int->{b_unc};
	    my $b_ref = $int->{b_ref};
	    my $type = $int->{type};
	    my $color = $int->{color};
	    my $reference_no = $int->{reference_no};
	    
	    $t_ref = $int->{t_age} if $t_type eq 'interpolated';
	    $b_ref = $int->{b_age} if $b_type eq 'interpolated';
	    
	    ComputeContainers($int, $scale_no);
	    
	    my $out = { scale_no => $scale_no,
			scale_name => $scale_name,
			interval_no => $int->{interval_no},
			interval_name => $int->{interval_name},
			abbrev => $int->{abbrev},
			renamed => $int->{renamed},
			action => $int->{action},
			n_colls => $int->{n_colls},
			type => $int->{type},
			color => $int->{color},
			reference_no => $int->{reference_no},
			t_type => $t_type,
			top => &AgeUnc($t_age, $t_unc),
			t_ref => $t_ref,
			b_type => $b_type,
			base => &AgeUnc($b_age, $b_unc),
			b_ref => $b_ref,
			stage => $int->{stage},
			subepoch => $int->{subepoch},
			epoch => $int->{epoch},
			period => $int->{period},
			parent => $int->{parent} };
	    
	    $output .= RecordToLine($out, \@FIELD_LIST);
	}
	
	$output .= "\n";
    }
    
    &SetupOutput;
    
    print $output;    
}


# DiagramScales ( scale... )
# 
# Diagram the specified timescale(s) as a sequence of boxes using ASCII characters. The
# scales will be drawn next to each other in the order specified, with boundaries lined
# up.  This enables visual confirmation that the timescale bounds have been entered
# correctly.

sub DiagramScales {
    
    my @scale_list = SelectScales(@_);
    
    return unless @scale_list;
    
    # Check each of the scales to be printed, and interpolate the boundary ages if
    # necessary.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( my $errors = CheckOneScale($scale_no) )
	{
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    say STDERR "Timescale '$name' had *** $errors errors ***";
	}
    }
    
    # Abort if any errors are found, unless the 'force' option was given.
    
    return if @ERRORS && ! $opt_force;
    
    # Generate a 2-dimensional array of records describing the boxes to be printed.
    
    my $options = { margin_left => 2, margin_top => 1, margin_bottom => 1 };
    
    if ( $opt_interval )
    {
	my ($top, $base) = IntervalBounds($opt_interval);
	
	$options->{t_limit} = $top;
	$options->{b_limit} = $base;
    }
    
    my $d = GenerateDiagram($options, \%SCALE_NUM, \%SCALE_INTS, @scale_list);
    
    # Turn that array into character output and print it.
    
    my $output;
    
    if ( $opt_debug )
    {
	$output = DebugDiagram($options, $d);
    }
    
    else
    {
	$output = DrawDiagram($options, $d);
    }
    
    if ( $d->{unplaced} && $d->{unplaced}->@* )
    {
	$output .= "\n";
	
	foreach my $int ( $d->{unplaced}->@* )
	{
	    $output .= "Could not place '$int->{interval_name}' ($int->{interval_no})\n";
	}
	
	$output .= "\n";
    }
    
    &SetupOutput;
    
    print $output;
}


sub DiagramPBDBScales {
    
    my ($dbname, @args) = @_;
    
    my $dbh = connectDB("config.yml", $dbname);
    
    CheckScaleTables($dbh);
    
    my @scale_list = SelectScales(@args);
    
    return unless @scale_list;
    
    # Fetch the interval records from the PBDB corresponding to the selected
    # scale(s). 
    
    my (%scale_hash, %ints_hash) = @_;
    
    foreach my $scale_no ( @scale_list )
    {
	$scale_hash{$scale_no} = FetchPBDBScale($dbh, $scale_no);
	$ints_hash{$scale_no} = FetchPBDBScaleIntervals($dbh, $scale_no);
    }
    
    # Generate a 2-dimensional array of records describing the boxes to be printed.
    
    my $options = { margin_left => 2, margin_top => 1, margin_bottom => 1 };
    
    if ( $opt_interval )
    {
	my ($top, $base) = IntervalBounds($opt_interval);
	
	$options->{t_limit} = $top;
	$options->{b_limit} = $base;
    }
    
    my $d = GenerateDiagram($options, \%scale_hash, \%ints_hash, @scale_list);
    
    # Turn that array into character output and print it.
    
    my $output;
    
    if ( $opt_debug )
    {
	$output = DebugDiagram($options, $d);
    }
    
    else
    {
	$output = DrawDiagram($options, $d);
    }
    
    if ( $d->{unplaced} && $d->{unplaced}->@* )
    {
	$output .= "\n";
	
	foreach my $int ( $d->{unplaced}->@* )
	{
	    $output .= "Could not place '$int->{interval_name}' ($int->{interval_no})\n";
	}
	
	$output .= "\n";
    }
    
    &SetupOutput;
    
    print $output;
}


# TopAgeRef ( interval, uniq )
# 
# Return a reference to the interval definition record whose boundary defines the top
# boundary of the argument interval.  The second argument is a hashref which is used to
# detect loops, such as A depending on B which depends on A again.

sub TopAgeRef {
    
    my ($interval, $uniq) = @_;
    
    my $type = $interval->{t_type};
    my $line = $interval->{line_no};
    
    # If the value of t_type is 'top' or 'base', the value of top will be the name of
    # some other interval.  Look this name up in the INTERVAL_NAME hash.
    
    if ( $type eq 'top' || $type eq 'base' )
    {
	my $name = $interval->{top};
	my $lookup = $INTERVAL_NAME{$name};
	
	# If we find some other interval, make sure it hasn't already been entered into
	# the $uniq hash. If it has, then we have a dependency loop. Otherwise, call either
	# this routine or BaseAgeRef recursively.
	
	if ( $lookup && $lookup ne $interval )
	{
	    if ( $uniq->{$name} )
	    {
		return "loop on '$name' at line $line";
	    }
	    
	    elsif ( $type eq 'top' )
	    {
		$uniq->{$name} = 1;
		return TopAgeRef($lookup, $uniq);
	    }
	    
	    else
	    {
		$uniq->{$name} = 1;
		return BaseAgeRef($lookup, $uniq);
	    }
	}
	
	# If a nonexistent interval name is given, or else the name refers to the same
	# interval, then the boundary age cannot be evaluated.
	
	else
	{
	    return "could not find '$name' at line $line";
	}
    }
    
    # If the value of t_type is 'use', then the interval name ought to refer to an
    # interval definition record which is different from the argument record. If it
    # doesn't, that means the interval was not defined anywhere else in the input data.
    
    elsif ( $type eq 'use' )
    {
	my $name = $interval->{interval_name};
	my $lookup = $INTERVAL_NAME{$name};
	
	if ( $lookup && $lookup ne $interval )
	{
	    return TopAgeRef($lookup, $uniq);
	}
	
	else
	{
	    return "could not find '$name' at line $line";
	}
    }
    
    # If the value of t_type for the argument interval is one of the following types, then
    # this interval record defines its own boundary.
    
    if ( $type eq 'def' or $type eq 'interpolate' or $type eq 'anchor' or $type eq 'gssp')
    {
	return $interval, 'top';
    }
    
    # If the value of t_type is anything else, return an error message.
    
    else
    {
	return "bad t_type '$type' at line $line";
    }
}


# BaseAgeRef ( interval, uniq )
# 
# Return a reference to the interval definition record whose boundary defines the base
# boundary of the argument interval.  The second argument is a hashref which is used to
# detect loops, such as A depending on B which depends on A again.

sub BaseAgeRef {
    
    my ($interval, $uniq) = @_;
    
    my $type = $interval->{b_type};
    my $line = $interval->{line_no};
    
    # If the value of b_type is 'top' or 'base', the value of base will be the name of
    # some other interval.  Look this name up in the INTERVAL_NAME hash.
    
    if ( $type eq 'top' || $type eq 'base' )
    {
	my $name = $interval->{base};
	my $lookup = $INTERVAL_NAME{$name};
	
	# If we find some other interval, make sure it hasn't already been entered into
	# the $uniq hash. If it has, then we have a dependency loop. Otherwise, call either
	# this routine or TopAgeRef recursively.
	
	if ( $lookup && $lookup ne $interval )
	{
	    if ( $uniq->{$name} )
	    {
		return "loop on '$name' at line $line";
	    }
	    
	    elsif ( $type eq 'top' )
	    {
		$uniq->{$name} = 1;
		return TopAgeRef($lookup, $uniq);
	    }
	    
	    else
	    {
		$uniq->{$name} = 1;
		return BaseAgeRef($lookup, $uniq);
	    }
	}
	
	# If a nonexistent interval name is given, or else the name refers to the same
	# interval, then the boundary age cannot be evaluated.
	
	else
	{
	    return "could not find '$name' at line $line";
	}
    }
    
    # If the value of t_type is 'use', then the interval name ought to refer to an
    # interval definition record which is different from the argument record. If it
    # doesn't, that means the interval was not defined anywhere else in the input data.
    
    elsif ( $interval->{t_type} eq 'use' )
    {
	my $name = $interval->{interval_name};
	my $lookup = $INTERVAL_NAME{$name};
	
	if ( $lookup && $lookup ne $interval )
	{
	    return BaseAgeRef($lookup, $uniq);
	}
	
	else
	{
	    return "could not fine '$name' at line $line";
	}
    }
    
    # If the value of t_type for the argument interval is one of the following types, then
    # this interval record defines its own boundary.
    
    if ( $type eq 'def' or $type eq 'interpolate' or $type eq 'anchor' or $type eq 'gssp')
    {
	return $interval, 'base';
    }
    
    # If the value of b_type is anything else, return an error message.
    
    else
    {
	return "bad b_type '$type' at line $line";
    }
}


# IntervalName ( interval_no )
# 
# Return the interval name corresponding to the specified interval number. If there isn't
# one, return undefined.

sub IntervalName {
    
    my ($interval_no) = @_;
    
    return $INTERVAL_NUM{$interval_no} && $INTERVAL_NUM{$interval_no}{interval_name};
}


# IntervalBounds ( interval_name )
# 
# Return the top age and base age corresponding to the specified interval or interval range.

sub IntervalBounds {
    
    my ($interval_name) = @_;
    
    if ( my $i = $INTERVAL_NAME{$interval_name} )
    {
	# Check the scale in which the specified interval is defined, and make sure that
	# its ages are set properly.
	
	my $scale_no = $i->{scale_no};
	
	unless ( $SCALE_CHECKED{$scale_no} )
	{
	    CheckOneScale($scale_no);
	}
	
	die "Bad top age for '$interval_name': $i->{t_age}\n" unless $i->{t_age} =~ /^\d/;
	
	die "Bad base age for '$interval_name': $i->{b_age}\n" unless $i->{b_age} =~ /^\d/;
	
	return ($i->{t_age}, $i->{b_age});
    }
    
    elsif ( $interval_name =~ /^(\w.*)-(\w.*)$/ )
    {
	my $int1 = $1;
	my $int2 = $2;
	
	my ($top1, $base1) = IntervalBounds($int1);
	my ($top2, $base2) = IntervalBounds($int2);
	
	my $top = min($top1, $top2);
	my $base = max($base1, $base2);
	
	return ($top, $base);
    }
    
    else
    {
	die "Unknown interval '$interval_name'\n";
    }
}


# DiffElt ( new, old )
# 
# Generate a diff element for the specified new and old values.

sub DiffElt {
    
    my ($for_update, $new, $old) = @_;
    
    if ( $for_update )
    {
	return $new;
    }
    
    else
    {
	my $delt = defined $new && $new ne '' ? $new : '';
	
	if ( defined $old && $old ne '' )
	{
	    $old =~ s/([.]\d*?)0+$/$1/;
	    $old =~ s/[.]$//;
	    $delt .= ' ' if defined $new && $new ne '';
	    $delt .= "($old)";
	}
	
	return $delt;
    }
}


# CompareUnc ( new, old )
# 
# Return true if the two uncertainty values are different, or one is defined and
# the other not. Return false otherwise.

sub CompareUnc {
    
    my ($new, $old) = @_;
    
    if ( !defined $new && !defined $old )
    {
	return 0;
    }
    
    if ( defined $new && defined $old )
    {
	return 0 if $new + 0 eq $old + 0;
    }
    
    return 1;
}


# AgeUnc ( age, unc )
# 
# Generate a string displaying the specified age and uncertainty.

sub AgeUnc {
    
    my ($age, $unc) = @_;
    
    if ( defined $unc && $unc == 0 )
    {
	return "~ $age";
    }
    
    elsif ( defined $unc )
    {
	$unc += 0;
	return "$age ±$unc";
    }
    
    else
    {
	return $age;
    }
}


# DiffMacrostrat ( timescale )
# 
# Compute a table of differences between the specified macrostrat timescale and the
# corresponding intervals in the input data. List all intervals that occur in Macrostrat
# but not the input data, and all intervals whose age, abbreviation, or color is different
# in Macrostrat than in the input data. For any timescale in the input data from which
# more than 1/4 of its intervals correspond to Macrostrat intervals, list all intervals
# that did not correspond to any Macrostrat interval. This will cause the display any
# extra intervals that do not show up in Macrostrat, without bringing in the entirety of
# any timescale from which a handful of intervals are incorporated.
# 
# The Macrostrat timescale may be specified either by name or number.

sub DiffMacrostrat {
    
    my ($timescale, @rest) = @_;
    
    # Check and interpolate all timescales, so that we have the right ages for comparison.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( my $errors = CheckOneScale($scale_no) )
	{
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    say STDERR "Timescale '$name' had *** $errors errors ***";
	}
    }
    
    # Exactly one timescale must be specified.
    
    if ( @rest )
    {
	die "You can only diff one timescale at a time\n";
    }
    
    unless ( $timescale )
    {
	die "You must specify a timescale\n";
    }
    
    # If the argument is 'international', use the Macrostrat timescale 'international
    # intervals'.
    
    if ( lc $timescale eq 'international' )
    {
	$timescale = 'international intervals';
    }
    
    # Fetch a list of interval records from Macrostrat corresponding to the specified
    # timescale.
    
    my (%matched_interval, %matched_scale);
    
    my @macro_intervals = FetchMacrostratIntervals($timescale);
    
    # Iterate through these interval records in order.
    
    foreach my $m ( @macro_intervals )
    {
	my $name = $m->{name};
	
	# If the Macrostrat interval is defined in the input data, check for differences.
	# Any differences that are found are entered into the %DIFF_NAME hash under the
	# interval name.
	
	if ( my $interval = $INTERVAL_NAME{$name} )
	{
	    my $interval_no = $interval->{interval_no};
	    my $scale_no = $interval->{scale_no};
	    my $line_no = $interval->{line_no};
	    my ($t_interval, $t_which) = TopAgeRef($interval);
	    my ($b_interval, $b_which) = BaseAgeRef($interval);
	    my $type = $interval->{type};
	    my $color = $interval->{color};
	    my $abbrev = $interval->{abbrev};
	    
	    # Keep track of which intervals and scales are matched by Macrostrat
	    # intervals.
	    
	    $matched_interval{$interval_no}++;
	    $matched_scale{$scale_no}++;
	    
	    if ( $m->{type} && $type ne $m->{type} )
	    {
		$DIFF_NAME{$name}{type} = DiffElt('', $m->{type}, $type);
	    }
	    
	    if ( $m->{color} && $color ne $m->{color} )
	    {
		$DIFF_NAME{$name}{color} = DiffElt('', $m->{color}, $color);
	    }
	    
	    if ( $m->{abbrev} && $abbrev ne $m->{abbrev} )
	    {
		$DIFF_NAME{$name}{abbrev} = DiffElt('', $m->{abbrev}, $abbrev);
	    }
	    
	    # Any interval which is to be removed or coalesced represents a difference.
	    # But an interval whose action is 'RENAME' has no difference if its new name
	    # matches the Macrostrat interval name.
	    
	    if ( $interval->{action} =~ /^REMOVE|^COALESCE/ )
	    {
		$DIFF_NAME{$name}{action} = 'REMOVE';
	    }
	    
	    if ( ref $t_interval )
	    {
		my $t_name = $t_interval->{interval_name};
		my $t_age = $t_interval->{$t_which};
		
		if ( $m->{t_age} ne $t_age )
		{
		    # $$$ need to guard against inconsistent updates
		    $DIFF_NAME{$name}{top} = DiffElt('', $m->{t_age}, $t_age);
		}
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, $t_interval";
	    }
	    
	    if ( ref $b_interval )
	    {
		my $b_name = $b_interval->{interval_name};
		my $b_age = $b_interval->{$b_which};
		
		if ( $m->{b_age} ne $b_age )
		{
		    # $$$ need to guard against inconsistent updates
		    $DIFF_NAME{$name}{base} = DiffElt('', $m->{b_age}, $b_age);
		}
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, $b_interval";
	    }
	}
	
	# If the Macrostrat interval has no corresponding definition in the input data,
	# add a difference record to the @DIFF_MISSING list.
	
	else
	{
	    my $diff = { interval_no => 'MISSING',
			 interval_name => $name,
			 type => $m->{type},
			 color => $m->{color},
			 t_type => 'def',
			 top => $m->{t_age},
			 b_type => 'def',
			 base => $m->{b_age} };
	    
	    push @DIFF_MISSING, $diff;
	}
    }
    
    # Cross off any matched scales in which the number of intervals matched is
    # less than 25% of the total number in that scale. Under most circumstances,
    # that means a few intervals from the scale in question were used in
    # the scale actually being checked.
    
    foreach my $n ( keys %matched_scale )
    {
	delete $matched_scale{$n} if ($matched_scale{$n} / $SCALE_INTS{$n}->@*) < 0.25;
    }
    
    # Now go through all of the intervals in any of the matched scales and
    # report any that didn't appear in the Macrostrat list and aren't flagged
    # for removal. Skip eons as well, because the Macrostrat international
    # interval list doesn't include those.
    
    foreach my $i ( @ALL_INTERVALS )
    {
	my $scale_no = $i->{scale_no};
	my $interval_no = $i->{interval_no};
	
	next unless $matched_scale{$scale_no};
	next if $matched_interval{$interval_no};
	
	next if $i->{type} eq 'eon';
	next if $i->{action} =~ /^REMOVE|^COALESCE/;
	
	push @DIFF_EXTRA, { $i->%* };
    }
    
    # If any errors were found, abort unless the 'force' option was given.
    
    return if @ERRORS && ! $opt_force;
    
    # If we have found any differences, print out a table. 
    
    &SetupOutput;
    
    # Start by generating the same header line that was read from the input data.
    
    if ( %DIFF_NAME || @DIFF_MISSING || @DIFF_EXTRA )
    {
	print GenerateHeader();
    }
    
    # Otherwise, let the user know that no differences were found.
    
    else
    {
	say STDERR "No differences in timescale '$timescale' between this spreadsheet and Macrostrat";
    }
    
    # Print out differences in corresponding intervals first.
    
    if ( %DIFF_NAME )
    {
	say "\nDifferences from Macrostrat:\n";
	
	# Iterate through all of the interval definitions in the input data, so that the
	# differences will be printed in the same order.
	
	foreach my $i ( @ALL_INTERVALS )
	{
	    my $name = $i->{interval_name};
	    
	    # If the interval has any differences from Macrostrat, generate an output
	    # line. Make sure that the output line includes the scale number and name
	    # and the interval number and name.
	    
	    if ( $DIFF_NAME{$name} )
	    {
		$DIFF_NAME{$name}{interval_name} = $name;
		$DIFF_NAME{$name}{interval_no} = $i->{interval_no};
		$DIFF_NAME{$name}{scale_name} = $i->{scale_name};
		$DIFF_NAME{$name}{scale_no} = $i->{scale_no};
		
		print RecordToLine($DIFF_NAME{$name}, \@FIELD_LIST);
	    }
	}
    }
    
    # Print out intervals missing from this spreadsheet next.
    
    if ( @DIFF_MISSING )
    {
	say "\nMissing from this spreadsheet:\n";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
    
    # Finally, print out intervals that appear in this spreadsheet in one of the
    # corresponding timescales that didn't appear in the Macrostrat list.
    
    if ( @DIFF_EXTRA )
    {
	say "\nExtra in this spreadsheet:\n";
	
	foreach my $i ( @DIFF_EXTRA )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
}


# SetupOutput ( )
# 
# Prepare to write output data. If the 'out' option was given, write to the specified
# file. Otherwise, write to STDOUT.

sub SetupOutput {
    
    if ( $opt_output )
    {
	open(STDOUT, '>', $opt_output) or die "Could not write to $opt_output: $!\n";
    }
    
    else
    {
	$| = 1;
    }
    
    binmode(STDOUT, ':utf8');
}


# FetchData ( url, decode )
# 
# Perform an HTTP GET request on the specified URL. If $decode is true, return the decoded
# content. Otherwise, return the raw content.
# 
# If the URL includes either '/edit?' or '/edit#', change that to '/export?format=$format'
# where the format defaults to 'tsv' unless overridden by the 'format' command-line
# option. This allows us to specify a Google Sheets editing URL and get the contents of
# the currently visible sheet in the specified format.

sub FetchData {
    
    my ($url, $decode) = @_;
    
    # Edit the URL if necessary.
    
    my $format = 'tsv';
    
    if ( $opt_format && $opt_format =~ /^(csv|tsv)$/ )
    {
	$format = $1;
    }
    
    elsif ( $opt_format )
    {
	die "Invalid format '$format'\n";
    }
    
    if ( $url =~ qr{/edit[?#]} )
    {
	$url =~ s{/edit[?#]}{/export?format=$format&};
    }
    
    # Make the request.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $req = HTTP::Request->new(GET => $url);
    
    my $response = $ua->request($req);
    
    if ( $response->is_success )
    {
	if ( $decode )
	{
	    return $response->decoded_content;
	}
	
	else
	{
	    return $response->content;
	}
    }
    
    else
    {
	my $status = $response->status_line;
	die "Could not fetch spreadsheet: $status\n";
    }
}


# FetchMacrostratIntervals ( timescale )
# 
# Make an HTTP GET request to Macrostrat for the specified timescale, and return the
# decoded content as a Perl data structure. The argument may be either a Macrostrat
# timescale number or name.

sub FetchMacrostratIntervals {
    
    my ($timescale) = @_;
    
    my ($url, $data);
    
    # If the timescale argument is numeric, use the 'timescale_id' parameter.
    
    if ( $timescale =~ /^\d+$/ )
    {
	$url = "$MACROSTRAT_INTERVALS?timescale_id=$timescale&true_colors=true";
    }
    
    # Otherwise, use the 'timescale' parameter.
    
    else
    {
	$url="$MACROSTRAT_INTERVALS?timescale=$timescale&true_colors=true";
    }
    
    # Make the request.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $req = HTTP::Request->new(GET => $url);
    
    my $response = $ua->request($req);
    my $content_ref = $response->content_ref;
    
    unless ( $response->is_success )
    {
	return;
    }
    
    eval {
	$data = decode_json($$content_ref);
    };
    
    my $intervals = $data->{success}{data};
    
    if ( ref $intervals eq 'ARRAY' )
    {
	return $intervals->@*;
    }
    
    else
    {
	return;
    }
}


# DiffPBDB ( subcommand, timescale... )
# 
# Compare the data in the interval spreadsheet with the corresponding interval definitions
# stored in the Paleobiology Database, and print out a table of differences. Compare the
# specified timescales, or all of them if 'all' is given. This function is only available
# if this program is run in the 'pbapi' container on one of the PBDB servers.
# 
# Numeric arguments select a timescale by scale_no. Non-numeric arguments select all
# timescales whose name matches the argument as a regexp.
# 
# If $subcommand is 'update', then the PBDB interval tables will be
# updated.  If it is 'diff', just print out the table of differences.

sub DiffPBDB {
    
    my ($cmd, $dbname, @args) = @_;
    
    my $dbh = connectDB("config.yml", $dbname);
    
    # Select timescales matching the arguments given.
    
    &SelectScales(@args);
    
    return unless %SCALE_SELECT;
    
    # Check all selected scales, and evaluate the ages of the interval boundaries.
    # Check the International scale and the tertiary subepochs first, because those
    # two are used to compute containing intervals for other scales.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( my $errors = CheckOneScale($scale_no) )
	{
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    say STDERR "Timescale '$name' had *** $errors errors ***";
	}
    }
    
    # If any errors were found, abort unless the 'force' option was given.
    
    return if @ERRORS && ! $opt_force;
    
    # Check to see if certain columns have been added to the interval tables in the
    # database.
    
    CheckScaleTables($dbh);
    
    $AUTHORIZER_NO = AuthenticateSession($dbh);
    
    # If we are updating, generate an "update" diff rather than a "display"
    # diff. 
    
    my $u = $cmd eq 'update' ? 1 : '';
    
    # If we are diffing all scales, fetch a hash of all the intervals known to the PBDB
    # by interval_no, and all scales known to the PBDB by scale_no. This allows us to
    # make sure that no interval and no scale is being left out.
    
    my $leftover_ints = { };
    my $leftover_scales = { };
    
    if ( $SCALE_SELECT{all} )
    {
	($leftover_ints, $leftover_scales) = FetchPBDBNums($dbh);
    }
    
    # If no errors were found in the selected scales, go through the entire set of
    # intervals. Compare those which are contained in one of the selected scales.
    
    my %processed_scale;
    
    foreach my $i ( @ALL_INTERVALS )
    {
	my $scale_no = $i->{scale_no};
	
	next unless $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no};
	
	my $interval_no = $i->{interval_no};
	my $line_no = $i->{line_no};
	my $name = $i->{interval_name};
	my $abbrev = $i->{abbrev};
	my $t_type = $i->{t_bound};
	my $t_age = $i->{t_intp} // $i->{t_age};
	my $t_unc = $i->{t_unc};
	my $t_ref = $i->{t_ref};
	my $b_type = $i->{b_bound};
	my $b_age = $i->{b_intp} // $i->{b_age};
	my $b_unc = $i->{b_unc};
	my $b_ref = $i->{b_ref};
	my $type = $i->{type};
	my $color = $i->{color};
	my $obsolete = $i->{obsolete} ? 1 : 0;
	my $reference_no = $i->{reference_no};
	my $authorizer_no = $i->{authorizer_no} || $AUTHORIZER_NO;
	
	$t_ref = $t_age if $t_type eq 'interpolated';
	$b_ref = $b_age if $b_type eq 'interpolated';
	
	$t_type = 'defined' if $t_type eq 'def' || $t_type eq 'interpolate';
	$b_type = 'defined' if $b_type eq 'def' || $b_type eq 'interpolate';
	
	# Skip empty intervals.
	
	next if $interval_no eq $EMPTY_INTERVAL;
	
	$processed_scale{$scale_no}++;
	
	# Compute containing intervals (age, subepoch, epoch, period) from the
	# international timescale and the tertiary/cretaceous subepochs.
	
	ComputeContainers($i, $scale_no);
	
	# For the international scale, verify that each interval other than eons has a
	# containing 'parent' interval. This is required by the Navigator app.
	
	if ( $scale_no == 1 )
	{
	    my $parent_no = $INTERVAL_NAME{$i->{parent}}{interval_no};
	    
	    unless ( $parent_no > 0 || $type eq 'eon' )
	    {
		push @ERRORS, "at line $line_no, no parent interval was found for '$name'";
	    }
	}
	
	# If there is a record for this interval in the PBDB, compare its attributes to
	# the attributes in the spreadsheet.
	
	if ( my $p = FetchPBDBInterval($dbh, $interval_no, $scale_no) )
	{
	    # For a 'use' row, we ignore the attributes that are stored in the
	    # interval_data table.
	    
	    if ( $i->{t_type} ne 'use' )
	    {
		if ( $name ne $p->{interval_name} )
		{
		    if ( $i->{action} eq 'RENAME' )
		    {
			$DIFF_INT{$scale_no}{$interval_no}{action} = 'RENAME';
		    }
		    
		    else
		    {
			push @ERRORS, "at line $line_no, '$name' differs from PBDB name " .
			    "'$p->{interval_name}'";
		    }
		}
		
		if ( $i->{action} =~ /^REMOVE|^COALESCE/ )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{action} = $i->{action};
		    
		    delete $leftover_ints->{$interval_no};
		    next;
		}
		
		if ( $abbrev ne $p->{abbrev} )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{abbrev} = 
			DiffElt($u, $abbrev, $p->{abbrev});
		}
		
		if ( $p->{main_scale_no} && $p->{main_scale_no} ne $scale_no )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{main_scale_no} = 
			DiffElt($u, $scale_no, $p->{main_scale_no});
		}
		
		if ( $t_age + 0 ne $p->{t_age} + 0 || CompareUnc($t_unc, $p->{t_unc}) )
		{
		    if ( $u )
		    {
			$DIFF_INT{$scale_no}{$interval_no}{t_age} = $t_age;
			$DIFF_INT{$scale_no}{$interval_no}{t_unc} = $t_unc;
		    }
		    
		    else
		    {
			$DIFF_INT{$scale_no}{$interval_no}{top} = 
			    DiffElt($u, AgeUnc($t_age, $t_unc), AgeUnc($p->{t_age} + 0, $p->{t_unc}));
		    }
		}
		
		if ( $t_ref + 0 ne $p->{t_ref} + 0 || defined $t_ref && ! defined $p->{t_ref} ||
		     defined $p->{t_ref} && ! defined $t_ref )
		{
		    if ( $u )
		    {
			$DIFF_INT{$scale_no}{$interval_no}{t_ref} = $t_ref;
		    }
		    
		    else
		    {
			$DIFF_INT{$scale_no}{$interval_no}{t_ref} = 
			    DiffElt($u, AgeUnc($t_ref, undef), AgeUnc($p->{t_ref}, undef));
		    }
		}
		
		if ( $t_type && $t_type ne $p->{t_type} )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{t_type} = 
			DiffElt($u, $t_type, $p->{t_type});
		}
		
		elsif ( ! $t_type )
		{
		    push @ERRORS, "at line $line_no, interval_no $interval_no has no t_type";
		}
		
		if ( $b_age + 0 ne $p->{b_age} + 0 || CompareUnc($b_unc, $p->{b_unc}) )
		{
		    if ( $u )
		    {
			$DIFF_INT{$scale_no}{$interval_no}{b_age} = $b_age;
			$DIFF_INT{$scale_no}{$interval_no}{b_unc} = $b_unc;
		    }
		    
		    else
		    {
			$DIFF_INT{$scale_no}{$interval_no}{base} = 
			    DiffElt($u, AgeUnc($b_age, $b_unc), AgeUnc($p->{b_age}, $p->{b_unc}));
		    }
		}
		
		if ( $b_ref + 0 ne $p->{b_ref} + 0 || defined $b_ref && ! defined $p->{b_ref} ||
		     defined $p->{b_ref} && ! defined $b_ref )
		{
		    if ( $u ) 
		    {
			$DIFF_INT{$scale_no}{$interval_no}{b_ref} = $b_ref;
		    }
		    
		    else
		    {
			my $p_value = defined $p->{b_ref} ? $p->{b_ref} + 0 : undef;
			$DIFF_INT{$scale_no}{$interval_no}{b_ref} = 
			    DiffElt($u, AgeUnc($t_ref, undef), AgeUnc($p_value, undef));
		    }
		}
		
		if ( $b_type && $b_type ne $p->{b_type} )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{b_type} = 
			DiffElt($u, $b_type, $p->{b_type});
		}
		
		elsif ( ! $b_type )
		{
		    push @ERRORS, "at line $line_no, interval_no $interval_no has no b_type";
		}
		
		if ( $reference_no ne $p->{int_ref_no} )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{int_ref_no} =
			DiffElt($u, $reference_no, $p->{int_ref_no});
		}
		
		if ( ! $p->{authorizer_no} )
		{
		    $DIFF_INT{$scale_no}{$interval_no}{authorizer_no} = 
			DiffElt($u, $authorizer_no, $p->{authorizer_no});
		}
	    }
	    
	    # The remaining attributes are stored in the scale_map table, and should be
	    # updated if different even with a 'use' row.
	    
	    if ( $obsolete ne ($p->{obsolete} // '0') )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{obsolete} = DiffElt($u, $i->{obsolete} || '0',
								       $p->{obsolete} // '0');
	    }
	    
	    if ( $type ne $p->{type} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{type} = DiffElt($u, $type, $p->{type});
	    }
	    
	    if ( $color ne $p->{color} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{color} = DiffElt($u, $color, $p->{color});
	    }
	    
	    if ( $reference_no ne $p->{reference_no} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{reference_no} = 
		    DiffElt($u, $reference_no, $p->{reference_no});
	    }
	    
	    if ( $i->{stage} ne IntervalName($p->{stage_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{stage} =
		    DiffElt($u, $i->{stage}, IntervalName($p->{stage_no}));
	    }
	    
	    if ( $i->{subepoch} ne IntervalName($p->{subepoch_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{subepoch} =
		    DiffElt($u, $i->{subepoch}, IntervalName($p->{subepoch_no}));
	    }
	    
	    if ( $i->{epoch} ne IntervalName($p->{epoch_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{epoch} =
		    DiffElt($u, $i->{epoch}, IntervalName($p->{epoch_no}));
	    }
	    
	    if ( $i->{period} ne IntervalName($p->{period_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{period} =
		    DiffElt($u, $i->{period}, IntervalName($p->{period_no}));
	    }
	    
	    if ( $i->{ten_my_bin} ne $p->{ten_my_bin} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{ten_my_bin} = 
		    DiffElt($u, $i->{ten_my_bin}, $p->{ten_my_bin});
	    }
	    
	    if ( $scale_no eq $INTL_SCALE && $i->{parent} ne IntervalName($p->{parent_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{parent} =
		    DiffElt($u, $i->{parent}, IntervalName($p->{parent_no}));
	    }
	    
	    # Remove this interval from the $leftover_ints hash, because it is accounted for.
	    
	    delete $leftover_ints->{$interval_no};
	}
	
	# If there is no record for this interval in the PBDB and the action is
	# not 'REMOVE' or 'COALESCE', then a record should be created.
	
	elsif ( $i->{action} !~ /^REMOVE|^COALESCE/ )
	{
	    my $action = $i->{t_type} eq 'use' ? '' : 'CREATE';
	    
	    my $diff = { action => $action,
			 type => $type,
			 color => $color,
			 obsolete => $obsolete,
			 reference_no => $reference_no,
			 t_type => $t_type,
			 t_age => $t_age,
			 t_unc => $t_unc,
			 t_ref => $t_ref,
			 top => AgeUnc($t_age, $t_unc),
			 b_type => $b_type,
			 b_age => $b_age,
			 b_ref => $b_ref,
			 base => AgeUnc($b_age, $b_unc),
			 stage => $i->{stage},
			 subepoch => $i->{subepoch},
			 epoch => $i->{epoch},
			 period => $i->{period},
			 parent => $i->{parent} };
	    	    
	    $DIFF_INT{$scale_no}{$interval_no} = $diff;
	}
    }
    
    # If we are checking all scales, report anything remaining in $leftover_ints.
    
    if ( $leftover_ints->%* )
    {
	foreach my $interval_no ( keys $leftover_ints->%* )
	{
	    push @DIFF_MISSING, { interval_no => $interval_no,
				  interval_name => $leftover_ints->{$interval_no} };
	}
    }
    
    # Then go through all of the scales in order of appearance, and compare the selected
    # ones.
    
    my %scale_seq = FetchPBDBSequences($dbh);
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	next unless $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no};
	
	ComputeScaleAttrs($scale_no);
	
	my $s = $SCALE_NUM{$scale_no};
	my $line_no = $s->{line_no};
	my $name = $s->{scale_name};
	my $locality = $s->{type};
	my $reference_no = $s->{reference_no};
	my $color = $s->{color};
	my $t_age = $s->{t_age};
	my $b_age = $s->{b_age};
	my $authorizer_no = $s->{authorizer_no} || $AUTHORIZER_NO;
	
	if ( my $p = FetchPBDBScale($dbh, $scale_no) )
	{
	    if ( $name ne $p->{scale_name} )
	    {
		$DIFF_SCALE{$scale_no}{action} = 'RENAME';
	    }
	    
	    if ( $locality ne $p->{locality} )
	    {
		$DIFF_SCALE{$scale_no}{type} = DiffElt($u, $locality, $p->{locality});
	    }
	    
	    if ( $color ne $p->{color} )
	    {
		$DIFF_SCALE{$scale_no}{color} = DiffElt($u, $color, $p->{color});
	    }
	    
	    if ( $t_age + 0 ne $p->{t_age} + 0 )
	    {
		$DIFF_SCALE{$scale_no}{top} = DiffElt($u, $t_age + 0, $p->{t_age} + 0);
	    }
	    
	    if ( $b_age + 0 ne $p->{b_age} + 0 )
	    {
		$DIFF_SCALE{$scale_no}{base} = DiffElt($u, $b_age + 0, $p->{b_age} + 0);
	    }
	    
	    if ( ($reference_no + 0) ne ($p->{reference_no} + 0) )
	    {
		$DIFF_SCALE{$scale_no}{reference_no} =
		    DiffElt($u, $reference_no + 0, $p->{reference_no} + 0);
	    }
	    
	    if ( ! $p->{authorizer_no} || $authorizer_no && $authorizer_no =~ /[*]$/ )
	    {
		$authorizer_no =~ s/[*]$// if $authorizer_no;
		
		if ( $authorizer_no )
		{
		    if ( ! $p->{authorizer_no} || $authorizer_no ne $p->{authorizer_no} )
		    {
			$DIFF_SCALE{$scale_no}{authorizer_no} = 
			    DiffElt($u, $authorizer_no, $p->{authorizer_no});
		    }
		}
		
		else
		{
		    push @ERRORS, "at line $line_no, scale_no $scale_no has no authorizer_no";
		}
	    }
	    
	    if ( $s->{action} eq 'REMOVE' )
	    {
		$DIFF_SCALE{$scale_no}{action} = 'REMOVE';
	    }
	    
	    elsif ( $s->{action} )
	    {
		push @ERRORS, "at line $line_no, invalid action '$s->{action}'";
	    }
	    
	    # Remove this interval from the $leftover_scales hash, because it is accounted for.
	    
	    delete $leftover_scales->{$scale_no};
	}
	
	elsif ( $s->{action} ne 'REMOVE' )
	{
	    $DIFF_SCALE{$scale_no} = { action => 'CREATE',
				       scale_name => $name,
				       top => $t_age,
				       base => $b_age,
				       type => $locality,
				       color => $color,
				       reference_no => $reference_no
				     };
	}
	
	# Then compare the sequence of interval numbers for this scale to the
	# sequence stored in the scale_map table. If the two differ, then the
	# scale_map for this scale must be updated.
	
	my (@sheet_ints, @pbdb_ints );
	
	# We must skip over anchors and intervals to be removed from the PBDB.
	
	if ( ref $SCALE_INTS{$scale_no} eq 'ARRAY' )
	{
	    @sheet_ints = 
		map { $_->{interval_no} }
	        grep { $_->{interval_no} ne $EMPTY_INTERVAL } 
	        grep { $_->{action} !~ /^REMOVE|^COALESCE/ } $SCALE_INTS{$scale_no}->@*;
	}
	
	if ( ref $scale_seq{$scale_no} eq 'ARRAY' )
	{
	    @pbdb_ints = $scale_seq{$scale_no}->@*;
	}
	
	# If the two sequences differ in length, the PBDB sequence must be updated.
	
	if ( @sheet_ints != @pbdb_ints )
	{
	    $DIFF_SCALE{$scale_no}{sequence} = 1;
	}
	
	# If the interval counts are equal and there are some intervals in the
	# spreadsheet, make sure that the sequence of interval numbers is the same.
	
	elsif ( @sheet_ints )
	{
	    foreach my $i ( 0..$#sheet_ints )
	    {
		if ( $pbdb_ints[$i] ne $sheet_ints[$i] )
		{
		    $DIFF_SCALE{$scale_no}{sequence} = 1;
		}
	    }
	}
    }
    
    # If we are checking all scales, report anything remaining in $leftover_scales.
    
    if ( $leftover_scales->%* )
    {
	foreach my $scale_no ( keys $leftover_scales->%* )
	{
	    push @DIFF_MISSING_SCALES, { scale_no => $scale_no,
					 scale_name => $leftover_scales->{$scale_no} };
	}
    }
    
    # Abort if any errors were found. They must be fixed before a valid diff can
    # be produced.
    
    return if @ERRORS && ! $opt_force;
    
    # If the command is 'diff', print out a table of differences.
    
    if ( $cmd eq 'diff' )
    {
	&PrintDifferences;
    }
    
    # If the command is 'update', actually update the PBDB.
    
    elsif ( $cmd eq 'update' )
    {
	&ApplyDifferences($dbh);
    }
    
    else
    {
	say STDERR "Unknown command '$CMD'";
    }
}


# ComputeContainers ( interval, scale_no )
# 
# Compute the containing intervals for the given interval, from the International scale
# and the Cenozoic and Late Cretaceous subepochs. For intervals in the international
# scale, also compute the immediate parent. This is required so that the API result
# requested by the Navigator application reports the parent of each interval in the
# international timescale. Navigator depends on that information.

sub ComputeContainers {
    
    my ($i, $scale_no) = @_;
    
    # Run through all the intervals from the international scale, looking for
    # those that contain interval $i.
    
    foreach my $c ( $SCALE_INTS{$INTL_SCALE}->@* )
    {
	last if $c->{t_age} > $i->{b_age};
	
	if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
	{
	    if ( $c->{type} eq 'eon' )
	    {
		if ( $i->{scale_no} eq $INTL_SCALE && $i->{type} eq 'era' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }
	    
	    elsif ( $c->{type} eq 'era' )
	    {
		if ( $i->{scale_no} eq $INTL_SCALE && $i->{type} eq 'period' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }	    
	    
	    elsif ( $c->{type} eq 'period' )
	    {
		$i->{period} = $c->{interval_name};
		
		if ( $i->{scale_no} eq $INTL_SCALE && $i->{type} eq 'epoch' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }
	    
	    elsif ( $c->{type} eq 'epoch' )
	    {
		$i->{epoch} = $c->{interval_name};
		
		if ( $i->{scale_no} eq $INTL_SCALE && $i->{type} eq 'age' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }
	    
	    elsif ( $c->{type} eq 'age' )
	    {
		if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
		{
		    $i->{stage} = $c->{interval_name};
		}
	    }
	}
    }
    
    # Then run through the Cenozoic subepochs.
    
    foreach my $c ( $SCALE_INTS{$CENO_SCALE}->@* )
    {
	last if $c->{t_age} > $i->{b_age};
	
	if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
	{
	    $i->{subepoch} = $c->{interval_name};
	}
    }
    
    # Finally, run through the 10 million year bins.
    
    my @intersection;
    
    foreach my $c ( $SCALE_INTS{$BIN_SCALE}->@* )
    {
	last if $c->{t_age} > $i->{b_age};
	
	if ( $c->{t_age} < $i->{b_age} && $c->{b_age} > $i->{t_age} )
	{
	    push @intersection, $c->{interval_name};
	}
    }
    
    # If this interval falls across bin boundaries, indicate the range.
    
    if ( @intersection == 1 )
    {
	$i->{ten_my_bin} = $intersection[0];
    }
    
    elsif ( @intersection )
    {
	my ($period1, $bin1) = split /\s+/, $intersection[0];
	my ($period2, $bin2) = split /\s+/, $intersection[-1];
	
	if ( $period1 eq $period2 )
	{
	    $i->{ten_my_bin} = "$period1 $bin2-$bin1";
	}
	
	else
	{
	    $i->{ten_my_bin} = "$period2-$period1";
	}
    }
}


# ComputeScaleAttrs ( scale_no )
# 
# Scan through the intervals and compute the top and base ages for the specified scale.
# If the scale does not have a reference_no, fill that in from the first non-empty
# reference_no value among its intervals.

sub ComputeScaleAttrs {
    
    my ($scale_no) = @_;
    
    my ($t_age, $b_age, $reference_no, $remove, $has_intervals);
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	# Compute the minimum and maximum age bounds.
	
	if ( !defined $t_age || defined($i->{t_age}) && $i->{t_age} < $t_age )
	{
	    $t_age = $i->{t_age};
	}
	
	if ( !defined $b_age || defined($i->{b_age}) && $i->{b_age} > $b_age )
	{
	    $b_age = $i->{b_age};
	}
	
	# Determine the first non-empty reference_no value.
	
	if ( $i->{reference_no} )
	{
	    $reference_no ||= $i->{reference_no};
	}
	
	# If all of the intervals are to be removed, flag the scale for removal. If even
	# one of them is not, clear the flag.
	
	if ( $i->{action} =~ /^REMOVE|^COALESCE/ )
	{
	    $remove //= 1;
	}
	
	else
	{
	    $remove = 0;
	}
    }
    
    $SCALE_NUM{$scale_no}{t_age} = $t_age;
    $SCALE_NUM{$scale_no}{b_age} = $b_age;
    $SCALE_NUM{$scale_no}{reference_no} ||= $reference_no;
    $SCALE_NUM{$scale_no}{action} = 'REMOVE' if $remove;
}


# PrintDifferences ( )
# 
# Print out the differences that were computed by &DiffPBDB between the
# spreadsheet and the PBDB interval tables. Print a table of differences to
# STDOUT, in the same format (CSV or TSV) as the input data.

sub PrintDifferences {
    
    SetupOutput;
    
    # If we have generated any differences in containing intervals, start by generating
    # the same header line that was read from the input data.
    
    if ( %DIFF_INT || %DIFF_SCALE || @DIFF_MISSING || @DIFF_MISSING_SCALES )
    {
	print GenerateHeader();
    }
    
    if ( %DIFF_INT )
    {
	say "\nInterval differences from PBDB:\n";
	
	foreach my $i ( @ALL_INTERVALS )
	{
	    my $scale_no = $i->{scale_no};
	    my $interval_no = $i->{interval_no};
	    
	    if ( $DIFF_INT{$scale_no}{$interval_no} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{interval_no} = $interval_no;
		$DIFF_INT{$scale_no}{$interval_no}{interval_name} = $i->{interval_name};
		$DIFF_INT{$scale_no}{$interval_no}{scale_no} = $i->{scale_no};
		$DIFF_INT{$scale_no}{$interval_no}{scale_name} = $i->{scale_name};
		
		print RecordToLine($DIFF_INT{$scale_no}{$interval_no}, \@FIELD_LIST);
	    }
	}
    }
    
    if ( @DIFF_MISSING )
    {
	say "\nIntervals missing from this spreadsheet:\n";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
    
    unless ( %DIFF_INT || @DIFF_MISSING )
    {
	say "\nNo differences among intervals.\n";
    }
    
    if ( %DIFF_SCALE )
    {
	say "\nScale differences from PBDB:\n";
	
	foreach my $scale_no ( @SCALE_NUMS )
	{
	    next unless $SCALE_SELECT{$scale_no} || $SCALE_SELECT{all};
	    
	    if ( $DIFF_SCALE{$scale_no} )
	    {
		$DIFF_SCALE{$scale_no}{scale_no} = $scale_no;
		$DIFF_SCALE{$scale_no}{scale_name} = $SCALE_NUM{$scale_no}{scale_name};
		$DIFF_SCALE{$scale_no}{interval_no} = 'sequence' 
		    if $DIFF_SCALE{$scale_no}{sequence};
		
		print RecordToLine($DIFF_SCALE{$scale_no}, \@FIELD_LIST);
	    }
	}
    }
    
    if ( @DIFF_MISSING_SCALES )
    {
	say "\nScales missing from this spreadsheet:\n";
	
	foreach my $s ( @DIFF_MISSING_SCALES )
	{
	    print RecordToLine($s, \@FIELD_LIST);
	}
    }
    
    unless ( %DIFF_SCALE || @DIFF_MISSING_SCALES )
    {
	say "\nNo differences among scales.\n";
    }
}


# ApplyDifferences ( )
# 
# Apply the differences that were computed by &DiffPBDB to the PBDB interval tables. The
# tables 'interval_data', 'scale_map', 'scale_data', 'intervals', and 'interval_lookup'
# will be updated to match the spreadsheet. The scale_map table is the most difficult. For
# each updated scale, a new set of scale_map records is computed. If the sequence has
# changed, all the map records for that scale will be deleted and new ones added.

sub ApplyDifferences {
    
    my ($dbh) = @_;
    
    # If there are intervals in the database that are not mentioned in the spreadsheet,
    # abort unless the 'force' option was specified. Any attempt to update the entire
    # spreadsheet should cover all of the intervals in the database.
    
    if ( @DIFF_MISSING && ! $opt_force )
    {
	say STDERR "The update cannot be carried out, because some intervals are missing:";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    say "  $i->{interval_name} ($i->{interval_no})";
	}	
	
	exit;
    }
    
    # Make sure that the scale_data table has the proper fields.
    
    ConditionScaleTables($dbh);
    
    # Fetch the list of PBDB intervals from each PBDB scale from the scale_map table.
    
    # my %scale_seq = FetchPBDBSequences($dbh);
    
    # Run through the list of selected scales, and update each one that has changed.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	next unless $SCALE_SELECT{$scale_no} || $SCALE_SELECT{all};
	
	# # First check the interval sequence of this scale against the PBDB scale map. In
	# # order to do this, we must skip over anchors and intervals to be removed from the
	# # PBDB.
	
	# my (@sheet_ints, @pbdb_ints, $sequence_diff );
	
	# if ( ref $SCALE_INTS{$scale_no} eq 'ARRAY' )
	# {
	#     @sheet_ints = 
	# 	map { $_->{interval_no} }
	#         grep { $_->{interval_no} ne $EMPTY_INTERVAL } 
	#         grep { $_->{action} !~ /^REMOVE|^COALESCE/ } $SCALE_INTS{$scale_no}->@*;
	# }
	
	# if ( ref $scale_seq{$scale_no} eq 'ARRAY' )
	# {
	#     @pbdb_ints = $scale_seq{$scale_no}->@*;
	# }
	
	# # If the two sequences differ in length, the PBDB sequence must be updated.
	
	# if ( @sheet_ints != @pbdb_ints )
	# {
	#     $sequence_diff = 1;
	# }
	
	# # If the interval counts are equal and there are some intervals in the
	# # spreadsheet, make sure that the sequence of interval numbers is the same.
	
	# elsif ( @sheet_ints )
	# {
	#     foreach my $i ( 0..$#sheet_ints )
	#     {
	# 	if ( $pbdb_ints[$i] ne $sheet_ints[$i] )
	# 	{
	# 	    $sequence_diff = 1;
	# 	}
	#     }
	# }
	
	# If there is a difference in the interval sequence for this scale,
	# change the scale_map to match. We do this first because many of the
	# interval attributes are stored here.
	
	if ( defined $DIFF_SCALE{$scale_no} && $DIFF_SCALE{$scale_no}{sequence} )
	{
	    UpdatePBDBSequence($dbh, $scale_no, $SCALE_INTS{$scale_no});
	    $SCALE_NUM{$scale_no}{scale_map_updated} = 1;
	    $UPDATE_SEQUENCES++;
	}
	
	# Now compare the intervals one by one and update any that are different.
	
	foreach my $i ( $SCALE_INTS{$scale_no}->@* )
	{
	    my $interval_no = $i->{interval_no};
	    
	    if ( $DIFF_INT{$scale_no}{$interval_no} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{interval_name} = $i->{interval_name};
		$DIFF_INT{$scale_no}{$interval_no}{line_no} = $i->{line_no};
		
		my $result = UpdatePBDBInterval($dbh, $scale_no, $interval_no,
						$DIFF_INT{$scale_no}{$interval_no});
	    }
	}
	
	# If there is a difference in the scale attributes, update the scale.
	
	if ( $DIFF_SCALE{$scale_no} )
	{
	    &UpdatePBDBScale($dbh, $scale_no, $DIFF_SCALE{$scale_no});
	}
    }
    
    # Finally, synchronize the classic intervals table with the interval_data table.
    
    SyncIntervalsTable($dbh);
    
    # Print a summary of actions taken.
    
    if ( $UPDATE_INTERVALS || $CREATE_INTERVALS || $REMOVE_INTERVALS ||
	 $UPDATE_SCALES || $CREATE_SCALES || $REMOVE_SCALES || $UPDATE_SEQUENCES )
    {
	if ( $opt_debug )
	{
	    say STDERR "\nSummary of database changes that would have been made:\n";
	}
	
	else
	{
	    say STDERR "\nSummary of database changes made:\n";
	}
	
	say STDERR "  Created $CREATE_INTERVALS intervals" if $CREATE_INTERVALS > 0;
	say STDERR "  Removed $REMOVE_INTERVALS intervals" if $REMOVE_INTERVALS > 0;
	say STDERR "    Updated $UPDATE_MAX max_interval_no values" if $UPDATE_MAX > 0;
	say STDERR "    Updated $UPDATE_MIN min_interval_no values" if $UPDATE_MIN > 0;
	say STDERR "    Updated $UPDATE_MA ma_interval_no values" if $UPDATE_MA > 0;
	say STDERR "  Updated $UPDATE_INTERVALS intervals" if $UPDATE_INTERVALS > 0;
	say STDERR "  Created $CREATE_SCALES timescales" if $CREATE_SCALES > 0;
	say STDERR "  Removed $REMOVE_SCALES timescales" if $REMOVE_SCALES > 0;
	say STDERR "  Updated $UPDATE_SCALES timescales" if $UPDATE_SCALES > 0;
	say STDERR "  Updated $UPDATE_SEQUENCES timescale sequences" if $UPDATE_SEQUENCES > 0;
	say STDERR "";
    }
    
    elsif ( $opt_debug )
    {
	say STDERR "No changes would have been made to the database\n";
    }
    
    else
    {
	say STDERR "No changes were made to the database\n";
    }
}


# FetchPBDBInterval ( dbh, interval_no, scale_no )
# 
# Given an interval_no and scale_no, fetch the corresponding PBDB interval
# record. Include information from the specified scale, if that interval
# is included in that scale.

sub FetchPBDBInterval {
    
    my ($dbh, $interval_no, $scale_no) = @_;
    
    my $qi = $dbh->quote($interval_no);
    my $qs = $dbh->quote($scale_no);
    
    my $unc = '';
    $unc .= "$T_UNC as t_unc, $B_UNC as b_unc, " if $HAS_UNC;
    
    my $sql = "SELECT i.interval_no, interval_name, abbrev, type, color, parent_no,
		    $T_AGE as t_age, t_type, t_ref, $B_AGE as b_age, b_type, b_ref, 
		    ${unc}i.scale_no as main_scale_no,
		    i.reference_no as int_ref_no, sm.obsolete, sm.reference_no,
		    stage_no, subepoch_no, epoch_no, period_no, ten_my_bin, 
		    authorizer_no, enterer_no, modifier_no
		FROM $TableDefs::TABLE{INTERVAL_DATA} as i
		    left join interval_lookup using (interval_no)
		    left join $TableDefs::TABLE{SCALE_MAP} as sm
			on sm.interval_no = i.interval_no and sm.scale_no = $qs
		WHERE i.interval_no = $qi";
    
    my $result = $dbh->selectrow_hashref($sql, { Slice => { } });
    
    return $result;
}


# FetchPBDBScale ( dbh, scale_no )
# 
# Given a scale_no, fetch the corresponding PBDB scale record.

sub FetchPBDBScale {
    
    my ($dbh, $scale_no) = @_;
    
    my $qs = $dbh->quote($scale_no);
    
    my $sql = "SELECT scale_no, scale_name, $B_AGE as b_age, $T_AGE as t_age, color,
		   locality, reference_no, authorizer_no, enterer_no, modifier_no
		FROM $TableDefs::TABLE{SCALE_DATA} as s
		WHERE s.scale_no = $qs";
    
    my $result = $dbh->selectrow_hashref($sql, { Slice => { } });
    
    return $result;    
}


# FetchPBDBScaleIntervals ( dbh, scale_no )
# 
# Given a scale identifier, fetch all of the intervals corresponding to that
# scale in the order in which they were originally defined in this spreadsheet.

sub FetchPBDBScaleIntervals {
    
    my ($dbh, $scale_no) = @_;
    
    my $qs = $dbh->quote($scale_no);
    my $SCALE_MAP = $TableDefs::TABLE{SCALE_MAP};
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    
    my $unc = '';
    $unc .= '$T_UNC as t_unc, $B_UNC as b_unc, ' if $HAS_UNC;
    
    my $sql = "SELECT i.interval_no, interval_name, abbrev, type, color, parent_no,
		    $T_AGE as t_age, t_type, t_ref, $B_AGE as b_age, b_type, b_ref, 
		    ${unc}i.scale_no as main_scale_no, sm.obsolete, sm.reference_no,
		    stage_no, subepoch_no, epoch_no, period_no
		FROM $SCALE_MAP as sm
		    left join $INTERVAL_DATA as i using (interval_no)
		    left join interval_lookup using (interval_no)
		WHERE sm.scale_no = $qs
		ORDER BY sm.sequence";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    return $result;
}


# FetchPBDBNums ( dbh )
# 
# Return a hashref of PBDB interval numbers and names, and a hashref of PBDB scale
# numbers and names.

sub FetchPBDBNums {
    
    my ($dbh) = @_;
    
    my $sql = "SELECT interval_no, interval_name FROM $TableDefs::TABLE{INTERVAL_DATA}";
    
    my @ints = $dbh->selectall_array($sql, { Slice => { } });
    
    my %intervals;
    
    foreach my $i ( @ints )
    {
	$intervals{$i->{interval_no}} = $i->{interval_name};
    }
    
    $sql = "SELECT scale_no, scale_name FROM $TableDefs::TABLE{SCALE_DATA}";
    
    my @scales = $dbh->selectall_array($sql, { Slice => { } });
    
    my %scales;
    
    foreach my $s ( @scales )
    {
	$scales{$s->{scale_no}} = $s->{scale_name};
    }
    
    return (\%intervals, \%scales);
}


# FetchPBDBSequences ( dbh )
# 
# Return a hashref of PBDB scale numbers and interval sequences. Each hash value
# will be a list of interval numbers, in the order they appear in the scale_map table.

sub FetchPBDBSequences {
    
    my ($dbh) = @_;
    
    my $sql;
    
    $sql = "SELECT scale_no, interval_no FROM $TABLE{SCALE_MAP} order by sequence";
    
    my @ints = $dbh->selectall_array($sql, { Slice => { } });
    
    my %sequence;
    
    foreach my $i ( @ints )
    {
	push $sequence{$i->{scale_no}}->@*, $i->{interval_no};
    }
    
    return %sequence;
}


# UpdatePBDBSequence ( dbh, scale_no, interval_list )
# 
# Update the PBDB scale_map table to reflect the specified scale and interval list. If the
# interval list is empty, remove all entries corresponding to $scale_no from the scale
# map. Otherwise, remove all entries and add the new list.

sub UpdatePBDBSequence {
    
    my ($dbh, $scale_no, $interval_list) = @_;
    
    my ($sql, $result);
    
    my $SCALE_MAP = $TableDefs::TABLE{SCALE_MAP};
    
    my $qs = $dbh->quote($scale_no);
    my $name = $SCALE_NUM{$scale_no}{scale_name};
    my $line_no = $SCALE_NUM{$scale_no}{line_no};
    
    unless ( $scale_no && $scale_no =~ /^\d+$/ )
    {
	push @ERRORS, "at line $line_no, could not update scale: bad scale_no '$scale_no'";
	return;
    }
    
    # Start by removing all existing entries for this scale from scale_map.
    
    $sql = "DELETE FROM $SCALE_MAP WHERE scale_no = $qs";
    
    $result = DoStatement($dbh, $sql);
    
    return unless ref $interval_list eq 'ARRAY' && $interval_list->@*;
    
    my $value_string = '';
    my $seq = 0;
    
    foreach my $i ( $interval_list->@* )
    {
	if ( $i->{action} !~ /^REMOVE|^COALESCE/ && $i->{interval_no} ne $EMPTY_INTERVAL )
	{
	    my $interval_no = $i->{interval_no};
	    my $qtype = $dbh->quote($i->{type});
	    my $qcolor = $dbh->quote($i->{color});
	    my $qobs = $i->{obsolete} ? '1' : '0';
	    my $qrefno = $dbh->quote($i->{reference_no});
	    
	    $seq++;
	    
	    $value_string .= ', ' if $value_string;
	    
	    if ( $scale_no == 1 )
	    {
		my $parent_no = $INTERVAL_NAME{$i->{parent}}{interval_no} || 'NULL';
		die "Bad parent '$i->{parent}' for interval $i->{interval_no}"
		    unless $parent_no > 0 || $i->{type} eq 'eon';
		
		$value_string .= "($scale_no,$interval_no,$seq,$parent_no,$qtype,$qcolor,$qobs,$qrefno)";
	    }
	    
	    else
	    {
		$value_string .= "($scale_no,$interval_no,$seq,$qtype,$qcolor,$qobs,$qrefno)";
	    }
	}
    }
    
    if ( $scale_no == 1 )
    {
	$sql = "REPLACE INTO $SCALE_MAP (scale_no, interval_no, sequence, parent_no,
		    type, color, obsolete, reference_no)
		VALUES $value_string";
    }
    
    else
    {
	$sql = "REPLACE INTO $SCALE_MAP (scale_no, interval_no, sequence,
		    type, color, obsolete, reference_no)
		VALUES $value_string";
    }
    
    $result = DoStatement($dbh, $sql);
    
    unless ( $opt_debug )
    {
	say STDOUT "Updated the sequence for '$name' ($scale_no)";
    }
}


# UpdatePBDBInterval ( dbh, scale_no, interval_no, diff )
# 
# Update the PBDB definition of the specified interval, according to the information in
# the hashref $diff.

sub UpdatePBDBInterval {
    
    my ($dbh, $scale_no, $interval_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    my $CLASSIC_LOOKUP = $TableDefs::TABLE{CLASSIC_INTERVAL_LOOKUP};
    my $SCALE_MAP = $TableDefs::TABLE{SCALE_MAP};
    
    my $name = $diff->{interval_name};
    my $line = $diff->{line_no};
    
    my $qino = $dbh->quote($interval_no);
    my $qsno = $dbh->quote($scale_no);
    
    my $scale_map_updated = $SCALE_NUM{$scale_no}{scale_map_updated};
    
    unless ( $scale_no && $scale_no =~ /^\d+$/ )
    {
	push @ERRORS, "at line $line, could not update '$name': bad scale_no '$scale_no'";
	return;
    }
    
    unless ( $interval_no && $interval_no =~ /^\d+$/ )
    {
	push @ERRORS, "at $line, could not update '$name': bad interval_no '$interval_no'";
    }
    
    # If this is a new interval, create the necessary records.
    
    if ( $diff->{action} eq 'CREATE' )
    {
	CreatePBDBInterval($dbh, $scale_no, $interval_no, $diff);
	return;
    }
    
    # If this interval is to be removed or coalesced, do the corresponding deletions and
    # updates.
    
    elsif ( $diff->{action} =~ /^REMOVE|^COALESCE/ )
    {
	RemovePBDBInterval($dbh, $scale_no, $interval_no, $diff);
	return;
    }
    
    # Otherwise, update the existing records. Start with the interval_data table, but only
    # if this is not a 'use' row.
    
    my @id_updates;
    
    if ( $diff->{action} eq 'RENAME' )
    {
	my $qname = $dbh->quote($name);
	
	push @id_updates, "interval_name = $qname";
    }
    
    if ( exists $diff->{abbrev} )
    {
	my $qabbr = $diff->{abbrev} eq '' ? 'NULL' : $dbh->quote($diff->{abbrev});
	
	push @id_updates, "abbrev = $qabbr";
    }
    
    if ( defined $diff->{t_type} && $diff->{t_type} ne '' )
    {
    	my $qtype = $dbh->quote($diff->{t_type});
	
	push @id_updates, "t_type = $qtype";
    }
    
    if ( defined $diff->{t_age} )
    {
	my $qtop = $dbh->quote($diff->{t_age});
	
	if ( $diff->{t_age} =~ $AGE_RE )
	{
	    push @id_updates, "$T_AGE = $qtop";
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad t_age $qtop";
	}
    }
    
    if ( exists $diff->{t_unc} )
    {
	my $qunc = $dbh->quote($diff->{t_unc});
	
	if ( ! defined $diff->{t_unc} || $diff->{t_unc} =~ $AGE_RE )
	{
	    push @id_updates, "$T_UNC = $qunc" if $HAS_UNC;
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad t_unc $qunc";
	}
    }
    
    if ( exists $diff->{t_ref} )
    {
	my $qref = $dbh->quote($diff->{t_ref});
	
	if ( ! defined $diff->{t_ref} || $diff->{t_ref} =~ $AGE_RE )
	{
	    push @id_updates, "t_ref = $qref";
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad t_ref $qref";
	}
    }
    
    if ( defined $diff->{b_type} && $diff->{b_type} ne '' )
    {
    	my $qtype = $dbh->quote($diff->{b_type});
	
	push @id_updates, "b_type = $qtype";
    }
    
    if ( defined $diff->{b_age} && $diff->{b_age} ne '' )
    {
	my $qbase = $dbh->quote($diff->{b_age});
	
	if ( $diff->{b_age} =~ $AGE_RE )
	{
	    push @id_updates, "$B_AGE = $qbase";
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad b_age $qbase";
	}
    }
    
    if ( exists $diff->{b_unc} )
    {
	my $qunc = $dbh->quote($diff->{b_unc});
	
	if ( ! defined $diff->{b_unc} || $diff->{b_unc} =~ $AGE_RE )
	{
	    push @id_updates, "$B_UNC = $qunc" if $HAS_UNC;
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad b_unc $qunc";
	}
    }
    
    if ( exists $diff->{b_ref} )
    {
	my $qref = $dbh->quote($diff->{b_ref});
	
	if ( ! defined $diff->{b_ref} || $diff->{b_ref} =~ $AGE_RE )
	{
	    push @id_updates, "b_ref = $qref";
	}
	
	else
	{
	    push @ERRORS, "at line $line, problem updating '$name': bad b_ref $qref";
	}
    }
    
    if ( defined $diff->{main_scale_no} && $diff->{main_scale_no} ne '' )
    {
	my $qmain = $dbh->quote($diff->{main_scale_no});
	
	push @id_updates, "scale_no = $qmain";
    }
    
    if ( defined $diff->{int_ref_no} && $diff->{int_ref_no} ne '' )
    {
	my $qref = $dbh->quote($diff->{int_ref_no});
	
	push @id_updates, "reference_no = $qref";
    }
    
    if ( defined $diff->{authorizer_no} && $diff->{authorizer_no} > 0 )
    {
	my $qauth = $dbh->quote($diff->{authorizer_no});
	
	push @id_updates, "authorizer_no = $qauth", "enterer_no = $qauth";
    }
    
    if ( @id_updates )
    {
	my $qmod = $dbh->quote($AUTHORIZER_NO);
	
	push @id_updates, "modifier_no = $qmod";
	
	my $update_string = join(', ', @id_updates);
	
	$sql = "UPDATE $INTERVAL_DATA
		SET $update_string
		WHERE interval_no = $qino";	
	
	my $result = DoStatement($dbh, $sql);
	
	unless ( $result || $opt_debug )
	{
	    push @ERRORS, "at line $line, failed to update '$name'";
	}
    }
    
    # Next update the scale_map table.
    
    my @sm_updates;
    
    if ( $diff->{type} && ! $scale_map_updated )
    {
	my $qtype = $diff->{type} eq 'none' ? 'NULL' : $dbh->quote($diff->{type});
	
	push @sm_updates, "type = $qtype";
    }
    
    if ( $diff->{color} && ! $scale_map_updated )
    {
	my $qcolor = $diff->{color} eq 'none' ? 'NULL' : $dbh->quote($diff->{color});
	
	push @sm_updates, "color = $qcolor";
    }
    
    if ( defined $diff->{obsolete} && ! $scale_map_updated )
    {
	my $qobs = $dbh->quote($diff->{obsolete});
	
	push @sm_updates, "obsolete = $qobs";
    }
    
    if ( $diff->{reference_no} && ! $scale_map_updated )
    {
	my $qrefno = $diff->{reference_no} eq 'none' ? '0' : $dbh->quote($diff->{reference_no} // '0');
	
	push @sm_updates, "reference_no = $qrefno";
    }
    
    if ( $diff->{parent} && ! $scale_map_updated )
    {
	my $parent_no = $INTERVAL_NAME{$diff->{parent}}{interval_no};
	my $qparent = $dbh->quote($parent_no);
	
	push @sm_updates, "parent_no = $qparent";
    }
    
    if ( @sm_updates )
    {
	my $update_string = join(', ', @sm_updates);
	
	$sql = "UPDATE $SCALE_MAP
		SET $update_string
		WHERE scale_no = $qsno and interval_no = $qino";
	
	$result = DoStatement($dbh, $sql);
    }
    
    # Finally, update the interval_lookup table.
    
    if ( $diff->{period} || $diff->{epoch} || $diff->{subepoch} || $diff->{stage} )
    {
	UpdateIntervalLookup($dbh, $scale_no, $interval_no, $diff);
    }
    
    # Report what we have done.
    
    $UPDATE_INTERVALS++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Updated interval '$name' ($interval_no)";
    }
}


sub CreatePBDBInterval {
    
    my ($dbh, $scale_no, $interval_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    my $CLASSIC_INTS = $TableDefs::TABLE{CLASSIC_INTERVALS};
    my $CLASSIC_LOOKUP = $TableDefs::TABLE{CLASSIC_INTERVAL_LOOKUP};
    
    my $name = $diff->{interval_name};
    my $line_no = $diff->{line_no};
    
    unless ( $name )
    {
	push @ERRORS, "at line $line_no, could not create interval: missing interval_name";
	return;
    }
    
    unless ( $diff->{t_type} )
    {
	push @ERRORS, "at line $line_no, could not create '$name': missing t_type";
	return;
    }
    
    unless ( $diff->{b_type} )
    {
	push @ERRORS, "at line $line_no, could not create '$name': missing b_type";
	return;
    }
    
    unless ( $diff->{t_age} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create '$name': bad t_age '$diff->{t_age}'";
	return;
    }
    
    unless ( ! defined $diff->{t_unc} || $diff->{t_unc} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create '$name': bad t_unc '$diff->{t_unc}'";
	return;
    }
    
    unless ( ! defined $diff->{t_ref} || $diff->{t_ref} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create '$name': bad t_ref '$diff->{t_ref}'";
	return;
    }
    
    unless ( $diff->{b_age} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no,  could not create interval: bad b_age '$diff->{b_age}'";
	return;
    }
    
    unless ( ! defined $diff->{b_unc} || $diff->{b_unc} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create '$name': bad b_unc '$diff->{b_unc}'";
	return;
    }
    
    unless ( ! defined $diff->{b_ref} || $diff->{b_ref} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create '$name': bad b_ref '$diff->{b_ref}'";
	return;
    }
    
    my $qino = $dbh->quote($interval_no);
    my $qname = $dbh->quote($name);
    my $qabbr = $dbh->quote($diff->{abbrev});
    my $qtop = $dbh->quote($diff->{t_age});
    my $qtunc = $dbh->quote($diff->{t_unc});
    my $qttype = $dbh->quote($diff->{t_type});
    my $qtref = $dbh->quote($diff->{t_ref});
    my $qbase = $dbh->quote($diff->{b_age});
    my $qbunc = $dbh->quote($diff->{b_unc});
    my $qbtype = $dbh->quote($diff->{b_type});
    my $qbref = $dbh->quote($diff->{b_ref});
    my $qauth = $dbh->quote($INTERVAL_NUM{$interval_no}{authorizer_no} || $AUTHORIZER_NO);
    my $qobs = $diff->{obsolete} ? '1' : '0';
    my $qrefno = $dbh->quote($INTERVAL_NUM{$interval_no}{reference_no} || '0');
    my $qmain = $dbh->quote($INTERVAL_NUM{$interval_no}{scale_no});
    
    # Create a record in the interval_data table.
    
    if ( $HAS_UNC )
    {
	$sql = "INSERT INTO $INTERVAL_DATA (interval_no, scale_no, interval_name, abbrev,
		$T_AGE, $T_UNC, t_ref, t_type, $B_AGE, $B_UNC, b_ref, b_type, 
		authorizer_no, enterer_no, reference_no)
	    VALUES ($qino, $qmain, $qname, $qabbr, $qtop, $qtunc, $qtref, $qttype,
		$qbase, $qbunc, $qbref, $qbtype, $qauth, $qauth, $qrefno)";
    }
    
    else
    {
	$sql = "INSERT INTO $INTERVAL_DATA (interval_no, scale_no, interval_name, abbrev,
		$T_AGE, t_ref, t_type, $B_AGE, b_ref, b_type, 
		authorizer_no, enterer_no, reference_no)
	    VALUES ($qino, $qmain, $qname, $qabbr, $qtop, $qtref, $qttype,
		$qbase, $qbref, $qbtype, $qauth, $qauth, $qrefno)";
    }
    
    $result = DoStatement($dbh, $sql);
    
    # Create the corresponding record in the intervals table.
    
    my ($eml, $classic_name) = EmlName($name);
    
    my $qeml = $dbh->quote($eml);
    my $qclassic = $dbh->quote($classic_name);
    
    $sql = "INSERT INTO $CLASSIC_INTS (authorizer_no, enterer_no, interval_no,
		eml_interval, interval_name, reference_no)
	    VALUES ($qauth, $qauth, $qino, $qeml, $qclassic, $qrefno)";
    
    $result = DoStatement($dbh, $sql);
    
    # Then create the corresponding record in the interval_lookup table.
    
    UpdateIntervalLookup($dbh, $scale_no, $interval_no, $diff);
    
    # Report what we have done.
    
    $CREATE_INTERVALS++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Created interval '$name' ($interval_no)";
    }
}


sub UpdateIntervalLookup {
    
    my ($dbh, $scale_no, $interval_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $CLASSIC_LOOKUP = $TableDefs::TABLE{CLASSIC_INTERVAL_LOOKUP};
    
    # Create a record in the interval_lookup table. We use 'replace' instead of 'insert'
    # because there may already be a record there for this interval_no, and the only
    # fields that are significant are the ones that are explicitly set here.
    
    my $stage_no = $INTERVAL_NAME{$diff->{stage}}{interval_no};
    my $subepoch_no = $INTERVAL_NAME{$diff->{subepoch}}{interval_no};
    my $epoch_no = $INTERVAL_NAME{$diff->{epoch}}{interval_no};
    my $period_no = $INTERVAL_NAME{$diff->{period}}{interval_no};
    my $ten_my_bin = $diff->{ten_my_bin} eq 'none' ? undef : $diff->{ten_my_bin};
    
    my $qstage = $dbh->quote($stage_no);
    my $qsubep = $dbh->quote($subepoch_no);
    my $qepoch = $dbh->quote($epoch_no);
    my $qperiod = $dbh->quote($period_no);
    my $qbin = $dbh->quote($ten_my_bin);
    
    my $qtop = $dbh->quote($diff->{t_age});
    my $qbase = $dbh->quote($diff->{b_age});
    
    $sql = "REPLACE INTO $CLASSIC_LOOKUP (interval_no, ten_my_bin, stage_no, subepoch_no,
		epoch_no, period_no, top_age, base_age)
	    VALUES ($interval_no, $qbin, $qstage, $qsubep, $qepoch, $qperiod, $qtop, $qbase)";
    
    $result = DoStatement($dbh, $sql);
}


sub RemovePBDBInterval {
    
    my ($dbh, $scale_no, $interval_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $COLLECTIONS = $TableDefs::TABLE{COLLECTION_DATA};
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    my $CLASSIC_INTS = $TableDefs::TABLE{CLASSIC_INTERVALS};
    my $CLASSIC_LOOKUP = $TableDefs::TABLE{CLASSIC_INTERVAL_LOOKUP};
    
    my $name = $diff->{interval_name};
    my $line_no = $diff->{line_no};
    
    # First check to see how many collections refer to this interval number.
    
    $sql = "SELECT count(*) FROM $COLLECTIONS as c
	    WHERE c.max_interval_no = $interval_no or 
		  c.min_interval_no = $interval_no or
		  c.ma_interval_no = $interval_no";
    
    my ($count) = $dbh->selectrow_array($sql);
    
    if ( $count && $diff->{action} eq 'REMOVE' )
    {
	my $name = $INTERVAL_NUM{$interval_no}{interval_name};
	
	push @ERRORS, "at line $line_no, could not remove '$name': $count collections";
	return;
    }
    
    # If the action is 'COALESCE', then these collections must be updated to point to
    # the new interval.
    
    if ( $count && $diff->{action} =~ /^COALESCE (.*)/ )
    {
	my $arg = $1;
	my ($new_max, $new_min);
	
	if ( $arg =~ /(.*?)-(.*)/ )
	{
	    my $base1 = $INTERVAL_NAME{$1}{b_age};
	    my $base2 = $INTERVAL_NAME{$2}{b_age};
	    
	    unless ( defined $base1 )
	    {
		push @ERRORS, "at line $line_no, could not coalesce '$name': bad argument '$1'";
		return;
	    }
	    
	    unless ( defined $base2 )
	    {
		push @ERRORS, "at line $line_no, could not coalesce '$name': bad argument '$2'";
		return;
	    }
	    
	    if ( $base1 > $base2 )
	    {
		$new_max = $INTERVAL_NAME{$1}{interval_no};
		$new_min = $INTERVAL_NAME{$2}{interval_no};
	    }
	    
	    else
	    {
		$new_max = $INTERVAL_NAME{$2}{interval_no};
		$new_min = $INTERVAL_NAME{$1}{interval_no};
	    }
	}
	
	else
	{
	    $new_min = $new_max = $INTERVAL_NAME{$arg}{interval_no};
	    
	    unless ( defined $new_max )
	    {
		push @ERRORS, "at line $line_no, could not coalesce '$name': bad argument '$arg'";
		return;
	    }
	}
	
	$sql = "UPDATE $COLLECTIONS set max_interval_no = $new_max
		WHERE max_interval_no = $interval_no";
	
	my $update_max = DoStatement($dbh, $sql);
	
	$sql = "UPDATE $COLLECTIONS set min_interval_no = $new_min
		WHERE min_interval_no = $interval_no";
	
	my $update_min =  DoStatement($dbh, $sql);
	
	$sql = "UPDATE $COLLECTIONS set ma_interval_no = $new_max
		WHERE ma_interval_no = $interval_no";
	
	my $update_ma = DoStatement($dbh, $sql);
	
	unless ( $opt_debug )
	{
	    say STDERR "Updated $update_max max_interval_nos '$name' => '$arg'" if $update_max;
	    say STDERR "Updated $update_min min_interval_nos '$name' => '$arg'" if $update_min;
	    say STDERR "Updated $update_ma ma_interval_nos '$name' => '$arg'" if $update_ma;
	    
	    $UPDATE_MAX += $update_max;
	    $UPDATE_MIN += $update_min;
	    $UPDATE_MA += $update_ma;
	}
    }
    
    # If we get here, then we can safely remove the interval.
    
    $sql = "DELETE FROM $INTERVAL_DATA WHERE interval_no = $interval_no";
    
    $result = DoStatement($dbh, $sql);
    
    $sql = "DELETE FROM $CLASSIC_INTS WHERE interval_no = $interval_no";
    
    $result = DoStatement($dbh, $sql);
    
    $sql = "DELETE FROM $CLASSIC_LOOKUP WHERE interval_no = $interval_no";
    
    $result = DoStatement($dbh, $sql);
    
    # Report what we have done.
    
    $REMOVE_INTERVALS++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Removed interval '$name' ($interval_no)";
    }
}


# UpdatePBDBScale ( dbh, scale_no, diff )
# 
# Update the PBDB definition of the specified timescale, according to the information in
# the hashref $diff.

sub UpdatePBDBScale {
    
    my ($dbh, $scale_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $SCALE_DATA = $TableDefs::TABLE{SCALE_DATA};
    
    my $name = $SCALE_NUM{$scale_no}{scale_name};
    my $line_no = $SCALE_NUM{$scale_no}{line_no};
    
    unless ( $scale_no && $scale_no =~ /^\d+$/ )
    {
	push @ERRORS, "at line $line_no, bad scale_no '$scale_no'";
	return;
    }
    
    # If this is a new scale, create the necessary record.
    
    if ( $diff->{action} eq 'CREATE' )
    {
	&CreatePBDBScale($dbh, $scale_no, $diff);
	return;
    }
    
    # If this scale is to be removed, do that.
    
    elsif ( $diff->{action} eq 'REMOVE' )
    {
	&RemovePBDBScale($dbh, $scale_no, $diff);
	return;
    }
    
    # Otherwise, update the existing record in the scale_data table.
    
    my @sd_updates;
    
    if ( $diff->{action} eq 'RENAME' )
    {
	my $qname = $dbh->quote($name);
	
	push @sd_updates, "scale_name = $qname";
    }
    
    if ( $diff->{color} )
    {
	my $color = $diff->{color} eq 'none' ? undef : $diff->{color};
	my $qcolor = $dbh->quote($color);
	
	push @sd_updates, "color = $qcolor";
    }
    
    if ( $diff->{type} )
    {
	my $locality = $diff->{type} eq 'none' ? undef : $diff->{type};
	my $qloc = $dbh->quote($locality);
	
	push @sd_updates, "locality = $qloc";
    }
    
    if ( $diff->{reference_no} )
    {
	my $refno = $diff->{reference_no} eq 'none' ? undef : $diff->{reference_no};
	my $qrefno = $dbh->quote($refno);
	
	push @sd_updates, "reference_no = $qrefno";
    }
    
    if ( defined $diff->{top} )
    {
	my $qtop = $dbh->quote($diff->{top});
	
	push @sd_updates, "$T_AGE = $qtop";
    }
    
    if ( defined $diff->{base} )
    {
	my $qbase = $dbh->quote($diff->{base});
	
	push @sd_updates, "$B_AGE = $qbase";
    }
    
    if ( defined $diff->{authorizer_no} )
    {
	my $qauth = $dbh->quote($diff->{authorizer_no});
	
	push @sd_updates, "authorizer_no = $qauth", "enterer_no = $qauth";
    }
    
    if ( @sd_updates )
    {
	my $qmod = $dbh->quote($AUTHORIZER_NO);
	
	push @sd_updates, "modifier_no = $qmod";
	
	my $update_string = join(', ', @sd_updates);
	
	$sql = "UPDATE $SCALE_DATA SET $update_string
		       WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
	
	unless ( $result || $ opt_debug )
	{
	    push @ERRORS, "at line $line_no, failed to update scale $scale_no"; 
	}
	
	# Report what we have done.
	
	$UPDATE_SCALES++;
	
	unless ( $opt_debug )
	{
	    say STDOUT "Updated scale '$name' ($scale_no)";
	}
    }
}


sub CreatePBDBScale {
    
    my ($dbh, $scale_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $SCALE_DATA = $TableDefs::TABLE{SCALE_DATA};
    
    my $name = $SCALE_NUM{$scale_no}{scale_name};
    my $line_no = $SCALE_NUM{$scale_no}{line_no};
    
    my $qname = $dbh->quote($name);
    my $qcolor = $dbh->quote($diff->{color});
    my $qloc = $dbh->quote($diff->{type});
    my $qrefno = $dbh->quote($diff->{reference_no});
    my $qtop = $dbh->quote($diff->{top});
    my $qbase = $dbh->quote($diff->{base});
    my $qauth = $dbh->quote($SCALE_NUM{$scale_no}{authorizer_no} || $AUTHORIZER_NO);
    
    # Create a record in the scale_data table.
    
    $sql = "INSERT INTO $SCALE_DATA (scale_no, scale_name, color, locality,
		reference_no, $T_AGE, $B_AGE, authorizer_no, enterer_no)
	    VALUES ($scale_no, $qname, $qcolor, $qloc, $qrefno, $qtop, $qbase, $qauth, $qauth)";
    
    $result = DoStatement($dbh, $sql);
    
    $CREATE_SCALES++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Created scale '$name' ($scale_no)";
    }
}


sub RemovePBDBScale {
    
    my ($dbh, $scale_no, $diff) = @_;
    
    my ($sql, $result);
    
    my $SCALE_DATA = $TableDefs::TABLE{SCALE_DATA};
    
    my $name = $SCALE_NUM{$scale_no}{scale_name};
    my $line_no = $SCALE_NUM{$scale_no}{line_no};
    
    $sql = "DELETE FROM $SCALE_DATA WHERE scale_no = $scale_no";
    
    $result = DoStatement($dbh, $sql);
    
    # Report what we have done.
    
    $REMOVE_SCALES++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Removed scale '$name' ($scale_no)";
    }
}


# PBDBCommand ( command, dbname, @args )

sub PBDBCommand {
    
    my ($cmd, $dbname, @args) = @_;
    
    my $dbh = connectDB("config.yml", $dbname);
    
    my ($sql, $result, $count);
    
    my @table_list = ($TABLE{INTERVAL_DATA}, $TABLE{CLASSIC_INTERVALS},
		      $TABLE{CLASSIC_INTERVAL_LOOKUP}, $TABLE{SCALE_DATA},
		      $TABLE{SCALE_MAP});
	
    # If the command is 'ints', just synchronize the intervals table with interval_data.
    
    if ( $cmd eq 'ints' )
    {
	return SyncIntervalsTable($dbh);
    }
    
    # If the command is 'backup', then backup each of the five tables updated by this
    # program. First, add the necessary fields if they aren't already in place.
    
    elsif ( $cmd eq 'backup' )
    {
	CheckScaleTables($dbh);
	ConditionScaleTables($dbh);
	
	foreach my $table ( @table_list )
	{
	    my $backup_name = $table . "_backup";
	    
	    $sql = "DROP TABLE IF EXISTS $backup_name";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    $sql = "CREATE TABLE $backup_name like $table";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    $sql = "INSERT INTO $backup_name SELECT * FROM $table";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say "Backed up table $table => $backup_name";
	}
	
	return;
    }
    
    # If the command is 'restore', then restore the tables from the backup tables.
    
    elsif ( $cmd eq 'restore' )
    {
	foreach my $table ( @table_list )
	{
	    my $backup_name = $table . "_backup";
	    
	    $sql = "DELETE FROM $table";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    $sql = "INSERT INTO $table SELECT * FROM $backup_name";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say "Restored table $table from $backup_name";
	}
	
	return;
    }
    
    # If the command is 'validate' then check the consistency of the tables.
    
    elsif ( $cmd eq 'validate' )
    {
	my $errors = 0;
	my $warnings = 0;
	
	# Check table INTERVAL_DATA.
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE interval_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have interval_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE scale_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have scale_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE interval_name = ''";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have interval_name = ''";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE early_age is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have early_age = NULL";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE late_age is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have late_age = NULL";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE authorizer_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have authorizer_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE enterer_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` have enterer_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} WHERE reference_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "WARNING: $count rows in `$TABLE{INTERVAL_DATA}` have reference_no = 0";
	    $warnings++;
	}
	
	# Check table SCALE_DATA.
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE scale_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have scale_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE scale_name = ''";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have scale_name = ''";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE early_age is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have early_age = NULL";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE late_age is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have late_age = NULL";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE authorizer_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have authorizer_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE enterer_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_DATA}` have enterer_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_DATA} WHERE reference_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "WARNING: $count rows in `$TABLE{SCALE_DATA}` have reference_no = 0";
	    $warnings++;
	}
	
	# Check table SCALE_MAP.
	
	$sql = "SELECT count(*) FROM `$TABLE{SCALE_MAP}` WHERE interval_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_MAP}` have interval_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM `$TABLE{SCALE_MAP}` WHERE scale_no = 0";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_MAP}` have scale_no = 0";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM `$TABLE{SCALE_MAP}` WHERE type is null or type = ''";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_MAP}` have type = NULL or ''";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{INTERVAL_DATA} as i 
		  LEFT JOIN $TABLE{SCALE_MAP} as sm using (interval_no)
		WHERE sm.interval_no is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{INTERVAL_DATA}` are missing from `$TABLE{SCALE_MAP}`";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_MAP} as sm
		  LEFT JOIN $TABLE{INTERVAL_DATA} as i using (interval_no)
		WHERE i.interval_no is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_MAP}` are missing from `$TABLE{INTERVAL_DATA}`";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM $TABLE{SCALE_MAP} as sm
		  LEFT JOIN $TABLE{SCALE_DATA} as s using (scale_no)
		WHERE s.scale_no is null";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( $count )
	{
	    say "ERROR: $count rows in `$TABLE{SCALE_MAP}` are missing from `$TABLE{SCALE_DATA}`";
	    $errors++;
	}
	
	$sql = "SELECT count(*) FROM interval_data";
	
	my ($id_count) = $dbh->selectrow_array($sql);
	
	$sql = "SELECT count(*) FROM scale_data";
	
	my ($sd_count) = $dbh->selectrow_array($sql);
	
	say "There are $id_count intervals in $sd_count scales.";
	
	unless ( $errors )
	{
	    say "All checks passed.";
	}
    }
    
    else
    {
	die "Unknown command 'cmd'\n";
    }
}


# SyncIntervalsTable ( dbh )
# 
# Update the classic intervals table to match the interval_data table. This is made more
# difficult by the field 'eml_interval' which needs to be computed.

sub SyncIntervalsTable {
    
    my ($dbh) = @_;
    
    my ($sql, $result);
    
    my $INTERVAL_DATA = $TABLE{INTERVAL_DATA};
    my $CLASSIC_INTS = $TABLE{CLASSIC_INTERVALS};
    
    # Start by deleting anything in the intervals table that isn't in the intervals_data
    # table. 
    
    $sql = "DELETE $CLASSIC_INTS
	    FROM $CLASSIC_INTS left join $INTERVAL_DATA using (interval_no)
	    WHERE $INTERVAL_DATA.interval_no is null";
    
    $result = DoStatement($dbh, $sql);
    
    if ( $result > 0 )
    {
	say STDERR "  Deleted $result intervals from the classic intervals table";
    }
    
    # Now compare the names between the two tables, and update eml_interval and
    # interval_name in the intervals table as necessary.
    
    $sql = "SELECT id.interval_no, id.interval_name, id.authorizer_no, id.modifier_no, 
		id.created, id.modified, i.interval_no as classic_no,
		i.eml_interval, i.interval_name as classic_name, i.reference_no as classic_ref
	    FROM $INTERVAL_DATA as id left join $CLASSIC_INTS as i using (interval_no)";
    
    my @result = $dbh->selectall_array($sql, { Slice => { } });
    
    foreach my $r ( @result )
    {
	my $interval_no = $r->{interval_no};
	my $interval_name = $r->{interval_name};
	my $reference_no = $INTERVAL_NUM{$interval_no}{reference_no} // '0';
	
	my ($eml, $classic_name) = EmlName($interval_name);
	
	my $qino = $dbh->quote($interval_no);
	my $qref = $dbh->quote($reference_no);
	my $qeml = $dbh->quote($eml);
	my $qname = $dbh->quote($classic_name);
	my $qauth = $dbh->quote($r->{authorizer_no});
	my $qmod = $dbh->quote($r->{modifier_no});
	my $qupdate = $dbh->quote($r->{modified});
	
	# If the interval table record is missing, create it.
	
	if ( ! $r->{classic_no} )
	{
	    my $qcreate = $dbh->quote($r->{created});
	    
	    $sql = "INSERT INTO $CLASSIC_INTS (authorizer_no, enterer_no, modifier_no,
			interval_no, eml_interval, interval_name, reference_no, created, modified)
		    VALUES ($qauth, $qauth, $qmod, $qino, $qeml, $qname, $qref, $qcreate, $qupdate)";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say STDERR  "  Created $qname ($interval_no) in the classic intervals table"
		unless $opt_debug;
	}
	
	# If the names don't match, do an update.
	
	elsif ( $eml ne $r->{eml_interval} || $classic_name ne $r->{classic_name} )
	{
	    $sql = "UPDATE $CLASSIC_INTS SET eml_interval = $qeml, interval_name = $qname
		    WHERE interval_no = $qino";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say STDERR "  Renamed $qname ($interval_no) in the classic intervals table"
		unless $opt_debug;
	}
    }
    
    # Update modifier number and date.
    
    $sql = "UPDATE $INTERVAL_DATA as id join $CLASSIC_INTS as i using (interval_no)
	    SET i.modifier_no = id.modifier_no,
	        i.modified = id.modified,
		i.reference_no = id.reference_no
	    WHERE i.modifier_no <> id.modifier_no or i.modified <> id.modified
		or i.reference_no <> id.reference_no";
    
    $result = DoStatement($dbh, $sql);
    
    if ( $result > 0 )
    {
	say STDERR "  Updated $result rows in the classic intervals table";
    }
}


sub EmlName {
    
    my ($name) = @_;
    
    if ( $name =~ qr{ ^ (early|middle|late) (\s (?:early|middle|late))? \s (.*) }xi )
    {
	my $eml = $1 . $2;
	my $rest = $3;
	
	if ( lc $eml eq 'early' )
	{
	    $eml = 'Early/Lower';
	}
	
	elsif ( lc $eml eq 'late' )
	{
	    $eml = 'Late/Upper';
	}
	
	elsif ( lc $eml eq 'middle' )
	{
	    $eml = 'Middle';
	}
	
	return $eml, $rest;
    }
    
    else
    {
	return '', $name;
    }
}


# DoStatement ( dbh, sql )
# 
# If we are running in debug mode, print the specified SQL statement to STDOUT.
# Otherwise, execute it and return the result.

sub DoStatement {
    
    my ($dbh, $sql, $print_stmt) = @_;
    
    if ( $opt_debug )
    {
	print STDOUT "$sql\n\n";
	return;
    }
    
    else
    {
	print STDOUT "$sql\n\n" if $print_stmt;
	
	my $result = eval { $dbh->do($sql) };
	
	if ( $@ )
	{
	    my ($package, $filename, $line) = caller;
	    print STDERR "SQL error at line $line of $filename:\n$sql\n";
	    die "$@\n";
	}
	
	else
	{
	    return $result;
	}
    }
}


# CheckScaleTables ( dbh )
# 
# Check to see whether the PBDB scale_data table has the fields 'color' and 'locality',
# whether the scale_map table has the fields 'reference_no' and 'sequence'.

sub CheckScaleTables {
    
    my ($dbh) = @_;
    
    my $sql = "SHOW CREATE TABLE $TABLE{INTERVAL_DATA}";
    
    my ($table, $def) = $dbh->selectrow_array($sql);
    
    if ( $def =~ /`early_unc`|`t_unc`/ )
    {
	$HAS_UNC = 1;
    }
    
    $T_AGE = ( $def =~ /`t_age`/ ) ? 't_age' : 'late_age';
    $B_AGE = ( $def =~ /`b_age`/ ) ? 'b_age' : 'early_age';
    
    $T_UNC = ( $def =~ /`t_unc`/ ) ? 't_unc' : 'late_unc';
    $B_UNC = ( $def =~ /`b_unc`/ ) ? 'b_unc' : 'early_unc';
}


# ConditionScaleTables ( dbh )
# 
# Add any necessary fields to the interval and scale tables if they don't already exist.
# If $opt_debug is true, print out the statements but don't execute them.

sub ConditionScaleTables {
    
    my ($dbh) = @_;
    
    my ($sql, $result);
    
    my $SCALE_DATA = $TableDefs::TABLE{SCALE_DATA};
    my $SCALE_MAP = $TableDefs::TABLE{SCALE_MAP};
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    my $CLASSIC_INTS = $TableDefs::TABLE{CLASSIC_INTERVALS};
    
    my ($table, $def) = $dbh->selectrow_array("SHOW CREATE TABLE `$INTERVAL_DATA`");
    
    unless ( $def =~ /`early_unc`|`b_unc`/ )
    {
	DoStatement($dbh, "ALTER TABLE `$INTERVAL_DATA` ADD COLUMN IF NOT EXISTS
				`$B_UNC` decimal(9,5) null after `$B_AGE`", 1);
	
	DoStatement($dbh, "ALTER TABLE `$INTERVAL_DATA` ADD COLUMN IF NOT EXISTS
				`$T_UNC` decimal(9,5) null after `$T_AGE`", 1);
	
	$HAS_UNC = 1;
    }
    
    my ($row, $def) = $dbh->selectrow_array("SHOW COLUMNS FROM `$INTERVAL_DATA` like 'b_type'");
    
    unless ( $def =~ /gssp/i )
    {
	DoStatement($dbh, "ALTER TABLE `$INTERVAL_DATA` MODIFY `b_type`
				enum('anchor','gssp','reference','defined','interpolated') NOT NULL");
	
	DoStatement($dbh, "ALTER TABLE `$INTERVAL_DATA` MODIFY `t_type`
				enum('anchor','gssp','reference','defined','interpolated') NOT NULL");
    }
}


# GenerateDiagram ( options, scale_hash, ints_hash, timescale... )
# 
# Generate a 2-d array encoding columns of boxes, in order to display the specified
# timescale(s) visually. Each row in the @bounds2d array represents a distinct age
# boundary. Column 0 specifies the age of each boundary, and the remaining columns specify
# the content to be displayed below that boundary for each timescale in turn.
# 
# The first argument must be a hashref, and subsequent arguments must all be scale_no
# values.

sub GenerateDiagram {
    
    my ($options, $scale_hash, $ints_hash, @scale_list) = @_;
    
    # If no age limits are given, use 0 and 5000.
    
    my $t_limit = $options->{t_limit} || 0;
    my $b_limit = $options->{b_limit} || 5000;
    
    # Unless we are displaying the Precambrian, there is no point in showing eons and
    # eras.
    
    my $remove_eras = $b_limit < 550;
    
    # Phase I: collect interval boundaries
    
    # Start by computing the age of each boundary in the scale, and the minimum and
    # maximum. If top and base limits were given, restrict the set of age boundaries to
    # that range.
    
    my (%ints_list);	# List of intervals to be displayed for each timescale
    
    my (%bound);	# Boundary ages from scales other than the international timescale
    
    my ($t_range, $b_range);	# The range of ages from those scales
    
    my (%ibound);	# Boundary ages from the international timescale
    
    my ($t_intl, $b_intl);	# The range of ages from the international timescale
    
    my %intp_bound;	# True for each age value that was interpolated
    
    my %open_top;	# True for each interval that continues past the top limit
    
    my %open_base;	# True for each interval that continues past the bottom limit
    
    my @unplaced;	# If any intervals cannot be placed, they are listed here.
    
    # For each displayed scale in turn, run through its list of intervals.
    
    foreach my $snum ( @scale_list )
    {
	foreach my $int ( $ints_hash->{$snum}->@* )
	{
	    # Ignore any interval that is flagged for removal, and ignore eras and eons if
	    # $remove_eras is true.
	    
	    next if $int->{action} =~ /^REMOVE|^COALESCE/;
	    next if $remove_eras && $int->{type} =~ /^era|^eon/;
	    
	    # Ignore empty intervals.
	    
	    next if $int->{interval_no} eq $EMPTY_INTERVAL;
	    
	    # We cannot display any interval that doesn't have a good top and bottom
	    # age. This shouldn't happen except in rare cases where the 'force' option
	    # is given.
	    
	    my $top = $int->{t_intp} // $int->{t_age};
	    my $base = $int->{b_intp} // $int->{b_age};
	    
	    unless ( defined $top && $top =~ $AGE_RE )
	    {
		my $line = $int->{line_no};
		push @ERRORS, "at line $line, bad top age '$top'";
		push @unplaced, $int;
		next;
	    }
	    
	    unless ( defined $base && $base =~ $AGE_RE )
	    {
		my $line = $int->{line_no};
		push @ERRORS, "at line $line, bad base age '$top'";
		push @unplaced, $int;
		next;
	    }
	    
	    # If either bound has been evaluated (interpolated) then mark it as such.
	    
	    $intp_bound{$top} = 1 if $int->{t_bound} eq 'interpolated';
	    $intp_bound{$base} = 1 if $int->{b_bound} eq 'interpolated'; 
	    
	    # Skip this interval if it falls outside of the age limits.
	    
	    next if $base <= $t_limit;
	    next if $top >= $b_limit;
	    
	    # The interval key is generated from the scale_no and interval_no values.
	    
	    my $inum = $int->{interval_no};
	    my $iref = "$snum-$inum";
	    
	    # If this interval overlaps the age limits, only display the part that lies
	    # within them.
	    
	    if ( $top < $t_limit )
	    {
		$top = $t_limit;
		$open_top{$iref} = 1;
	    }
	    
	    if ( $base > $b_limit )
	    {
		$base = $b_limit;
		$open_base{$iref} = 1;
	    }
	    
	    # Add this interval to the list for display
	    
	    push $ints_list{$snum}->@*, $int;
	    
	    # Keep track of the age boundaries separately for the international scale and
	    # all other scales. Keep track of the minimum and maximum boundary ages
	    # separately as well.
	    
	    if ( $snum eq $INTL_SCALE )
	    {
		$ibound{$top} = 1;
		
		if ( !defined $t_intl || $top < $t_intl )
		{
		    $t_intl = $top;
		}
	    }
	    
	    else
	    {
		$bound{$top} = 1;
		
		if ( !defined $t_range || $top < $t_range )
		{
		    $t_range = $top;
		}
	    }
		
	    if ( $snum eq $INTL_SCALE )
	    {
		$ibound{$base} = 1;
		
		if ( !defined $b_intl || $base > $b_intl )
		{
		    $b_intl = $base;
		}
	    }
	    
	    else
	    {
		$bound{$base} = 1;
		
		if ( !defined $b_range || $base > $b_range )
		{
		    $b_range = $base;
		}
	    }
	}
    }
    
    # If we are displaying one or more scales other than the international one, use only
    # the international boundaries which lie in their range. Do not display the whole
    # international scale unless it is the only one being shown.
    
    if ( defined $b_range )
    {
	foreach my $b ( keys %ibound )
	{
	    $bound{$b} = 1 if $b >= $t_range && $b <= $b_range;
	}
    }
    
    # If we are displaying only the international scale, use all of its boundaries
    # between $t_limit and $b_limit.
    
    else
    {
	foreach my $b ( keys %ibound )
	{
	    $bound{$b} = 1;
	}
	
	$t_range = $t_intl;
	$b_range = $b_intl;
    }
    
    # Don't show eras and eons unless the bottom of the displayed range reaches
    # into the Precambrian.
    
    $remove_eras = 1 if $b_range < 550;
    
    # Phase II: Generate the diagram
    
    # The following arrays and hashes store rest of the information necessary to draw
    # the diagram.
    
    my @bound2d;	# Each element (cell) represents one interval boundary plus the
			# content below it. The first column holds the age.
    
    my @col_type;	# Stores which interval type belongs in which column
    
    my @col_width;	# Stores the width of each column
    
    my @header;		# Stores the header label for each column
    
    my %label;		# Stores the label for each cell.
    
    my %height;		# Stores the height of each cell in rows.
    
    my %label_split;	# Indicates labels which are split across lines.
    
    # Store age boundaries in the first column of the bounds2d array, in order from newest
    # to oldest.
    
    my @bound_list = sort { $a <=> $b } keys %bound;
    
    foreach my $b ( @bound_list )
    {
	push @bound2d, [ $b ];
    }
    
    # Now go through the scales and their intervals one by one. Place each interval in
    # turn in the 2-d array in such a way that it does not overlap any of the intervals
    # already there.
    
    my $max_row = $#bound_list;
    
    # The following two variables bracket the columns that correspond to a given scale.
    # Because they are initialized to zero, the first scale starts with both of them given
    # the value of 1. Each new scale starts with the next empty column.
    
    my $min_col = 0;
    my $max_col = 0;
    
    foreach my $snum ( @scale_list )
    {
	$min_col = $max_col + 1;
	$max_col = $min_col;
	
	my ($last_top, $last_base, $last_type, $last_ref);
	
	# Run through the intervals a second time in the order they appear in the
	# timescale.  This order can affect how they are displayed if some of them
	# overlap others.
	
	foreach my $int ( $ints_list{$snum}->@* )
	{
	    # Ignore any interval that is flagged for removal, and ignore eras and eons if
	    # $remove_eras is true.
	    
	    # next if $int->{action} =~ /^REMOVE|^COALESCE/;
	    next if $remove_eras && $int->{type} =~ /^era|^eon/;
	    
	    # # The displayed intervals are identified by their interval_no value. It is
	    # # possible that the same interval may appear in more than one timescale
	    # # displayed at the same time, in which case it must have the same name and
	    # # top/bottom ages in each case. It is okay to use the same identifier for
	    # # both, since name and top/bottom ages are the only attributes stored.
	    
	    my $inum = $int->{interval_no};
	    my $iname = $int->{interval_name};
	    my $itype = $int->{type};
	    my $iref = "$snum-$inum";
	    
	    my $top = $int->{t_intp} // $int->{t_age};
	    my $base = $int->{b_intp} // $int->{b_age};
	    
	    # Ignore any interval that falls outside of the age range to be displayed.
	    
	    next if $base <= $t_range;
	    next if $top >= $b_range;
	    
	    # If this interval overlaps the top or bottom age boundary, display only the
	    # part that falls within these boundaries. The horizontal boundary line will
	    # be suppressed in these cases (see below) so that the overlap is clear.
	    
	    if ( $top < $t_range )
	    {
		$top = $t_range;
		$open_top{$iref} = 1;
	    }
	    
	    if ( $base > $b_range )
	    {
		$base = $b_range;
		$open_base{$iref} = 1;
	    }
	    
	    # If the top and bottom ages for this interval are identical to the previous
	    # interval, display this interval name in the same box as the previous one
	    # rather than generating a separate box in a separate column.  This situation
	    # happens quite a bit in some of our timescales.
	    
	    if ( $top eq $last_top && $base eq $last_base && $itype eq $last_type )
	    {
		$label{$last_ref} .= '/' . $iname;
		next;
	    }
	    
	    # In all other cases, store the interval name and top/bottom ages using the
	    # interval_no as key.
	    
	    else
	    {
		$label{$iref} = $iname;		
		$last_top = $top;
		$last_base = $base;
		$last_type = $itype;
		$last_ref = $iref;
	    }
	    
	    # # The header of the first column for each displayed scale contains the scale
	    # # number and as much of the scale name as will fit. For the international
	    # # scale, use the label corresponding to the interval type.
	    
	    # unless ( $header[$min_col] )
	    # {
	    # 	if ( $snum eq $INTL_SCALE )
	    # 	{
	    # 	    my $label = $TYPE_LABEL{$int->{type}};
	    # 	    $header[$min_col] = "$snum $label";
	    # 	}
		
	    # 	else
	    # 	{
	    # 	    $header[$min_col] = "$snum $scale_hash->{$snum}{scale_name}";
	    # 	}
	    # }
	    
	    # Determine which column this interval should be place into. The value of $c
	    # will be that column number.
	    
	    my $c = $min_col;
	    
	    # Find the top and bottom rows corresponding to the top and bottom
	    # boundaries of this interval.
	    
	    my ($rtop, $rbase);
	    
	    for my $r ( 0..$max_row )
	    {
		$rtop = $r if $bound2d[$r][0] eq $top;
		$rbase = $r, last if $bound2d[$r][0] eq $base;
	    }
	    
	    # If either the top or the bottom age cannot be matched to a row, this
	    # interval cannot be placed.
	    
	    unless ( defined $rtop && defined $rbase )
	    {
		push @unplaced, $int;
		next;
	    }
	    
	    # Place this interval either in the minimum column for this scale, or up to 5
	    # columns further to the right if necessary to avoid overlapping any interval
	    # that has already been placed.
	    
	  COLUMN:
	    while ( $c < $min_col+5 )
	    {
		# If this interval has a type that is different from the interval type for
		# the current column, move one column to the right and try again.
		
		if ( $col_type[$c] && $int->{type} )
		{
		    $c++, next COLUMN unless $col_type[$c] eq $int->{type};
		}
		
		# Otherwise, set the interval type for this column to the type for this
		# interval if it is not already set.
		
		elsif ( $int->{type} )
		{
		    $col_type[$c] ||= $int->{type};
		}
		
		# If any of the cells where this interval would be placed are occupied by
		# anything other than a bottom boundary, move one column to the right and
		# try again. If there are no conflicts other than a bottom boundary, that
		# would have to be at the very top of the placement area. In that case,
		# that bottom boundary should be properly overwritten with the new
		# interval to indicate that the new interval immediately follows the one
		# above it.
		
		for my $r ( $rtop..$rbase-1 )
		{
		    $c++, next COLUMN if $bound2d[$r][$c] && $bound2d[$r][$c] ne 'b';
		}
		
		# If this column doesn't yet have a header, that means it is an additional
		# column for this time scale. Give it a header label consisting of the
		# scale number plus an indication of the interval type being displayed in
		# this column.
		
		unless ( $header[$c] )
		{
		    if ( $c == $min_col && $snum ne $INTL_SCALE )
		    {
			$header[$c] = "$snum $scale_hash->{$snum}{scale_name}";
		    }
		    
		    elsif ( $int->{type} )
		    {
			my $label = $TYPE_LABEL{$int->{type}};
			$header[$c] = "$snum $label";
		    }
		    
		    else
		    {
			$header[$c] = $snum;
		    }
		}
		
		# If we get to this point, there is nothing to prevent us placing the
		# interval in the current column. So stop here.
		
		last COLUMN;
	    }
	    
	    # Keep track of the maximum column number used by this timescale.
	    
	    $max_col = $c if $c > $max_col;
	    
	    # Place the interval by storing its interval number in all of the cells
	    # from the top boundary row to just above the bottom boundary row.
	    
	    for my $r ( $rtop..$rbase-1 )
	    {
		$bound2d[$r][$c] = $iref;
	    }
	    
	    # If the interval continues past the display area, store the interval number
	    # to indicate that situation.
	    
	    if ( $open_base{$iref} )
	    {
		$bound2d[$rbase][$c] = $iref;
	    }
	    
	    # Otherwise, store a 'b' to indicate a bottom boundary.
	    
	    else
	    {
		$bound2d[$rbase][$c] = 'b';
	    }
	    
	    # Store the height of this cell.
	    
	    $height{$iref} = $rbase - $rtop;
	}
    }
    
    # Once all of the intervals have been placed, compute the column widths. Each column
    # must be wide enough to display its entire label plus a space on either side, with a
    # minimum width of 10.
    
    for my $c ( 1..$max_col )
    {
	my $c_width = 10;
	
	for my $r ( 0..$max_row )
	{
	    if ( $bound2d[$r][$c] > 0 )
	    {
		my $iref = $bound2d[$r][$c];
		my $w = length($label{$iref}) + 2;
		
		# If the label contains a '/' character and is at least 20 characters
		# wide, and the cell is more than 1 row in height, split the label.
		
		if ( $label{$iref} =~ qr{/} && length($label{$iref}) >= 20 && 
		     $height{$iref} > 1 )
		{
		    if ( $label{$iref} =~ qr{(.*?/.*?/.*?/.*?/)(.*?/.*?/.*?/.*)} )
		    {
			$label_split{$iref} = length($1);
		    }
		    
		    elsif ( $label{$iref} =~ qr{(.*?/.*?/.*?/)(.*?/.*?/.*)} )
		    {
			$label_split{$iref} = length($1);
		    }
		    
		    elsif ( $label{$iref} =~ qr{(.*?/.*?/)(.*)} )
		    {
			$label_split{$iref} = length($1);
		    }
		    
		    else
		    {
			$label{$iref} =~ qr{(.*?/)(.*)};
			$label_split{$iref} = length($1);
		    }
		}
		
		if ( my $w1 = $label_split{$iref} )
		{
		    $w = max($w1, $w - $w1) + 2;
		}
		
		if ( $w > $c_width )
		{
		    $c_width = $w;
		}
	    }
	}
	
	$col_width[$c] = $c_width;
    }
    
    # Return a data structure containing all of this information, which will be passed to
    # DrawDiagram.
    
    my $result = { bound2d => \@bound2d,
		   col_type => \@col_type,
		   col_width => \@col_width,
		   header => \@header,
		   label => \%label,
		   label_split => \%label_split,
		   intp_bound => \%intp_bound,
		   open_top => \%open_top,
		   unplaced => \@unplaced,
		   max_row => $max_row,
		   max_col => $max_col };
    
    return $result;
}


sub DebugDiagram {
    
    my ($options, $diagram) = @_;
    
    my @bound2d = $diagram->{bound2d}->@*;
    my @col_width = $diagram->{col_width}->@*;
    my @col_intnum;
    my $max_row = $diagram->{max_row};
    my $max_col = $diagram->{max_col};
    
    my $output = "";
    
    $output .= "Cells:\n\n";
    
    foreach my $r ( 0..$max_row )
    {
	my $line = "";
	
	foreach my $c ( 0..$max_col )
	{
	    $line .= sprintf("%-9s ", $bound2d[$r][$c]);
	}
	
	$output .= "$line\n";
    }
    
    return $output;
}


# DrawDiagram ( options, diagram )
# 
# Draw a set of boxes using ASCII characters, according to the rows and columns laid out
# in $diagram, modified by the options in $options.

sub DrawDiagram {
    
    my ($options, $diagram) = @_;
    
    # Create the output string in $output. Create the top and left margins.
    
    my $output = "";
    my $margin = "";
    
    if ( $options->{margin_top} > 0 )
    {
	$output .= "\n" x $options->{margin_top};
    }
    
    if ( $options->{margin_left} > 0 )
    {
	$margin .= " " x $options->{margin_left};
    }
    
    # Unpack the data structure.
    
    my @bound2d = $diagram->{bound2d}->@*;
    my @col_width = $diagram->{col_width}->@*;
    my $max_row = $diagram->{max_row};
    my $max_col = $diagram->{max_col};
    
    # If we have nothing to diagram, say so and return.
    
    unless ( $max_row > 0 )
    {
	say STDERR "Timescale is empty";
	return;
    }
    
    # The following array keeps track of which interval identifier was last seen in each
    # column as we scan down the rows, so we will know when it changes.
    
    my @col_intnum;
    
    # The following hash allows us to split labels between lines
    
    my %line2;
    
    # Generate the column header row. The foreach loops here and in the footer are only
    # there so that the $c loop is equally indented in all three sections.
    
    foreach my $r ( 0..0 )
    {
	my $line1 = $margin . '*';
	
	foreach my $c ( 1..$max_col )
	{
	    $line1 .= ColumnHeader($diagram, $c);
	}
	
	$output .= "$line1\n";
    }
    
    # Generate all rows of the table except the bottom boundary. These rows are generated
    # two at a time. The first row in each pair is a horizontal boundary row, and the
    # second is available to display interval names.
    
    foreach my $r ( 0..$max_row-1 )
    {
	my $age = $bound2d[$r][0];
	
	my $line1 = $margin;
	my $line2 = $margin;
	
	# For each row, iterate through the columns and add the appropriate strings to the
	# two lines together.
	
	foreach my $c ( 1..$max_col )
	{
	    my $iref = $bound2d[$r][$c];
	    
	    # If we have an interval identifier that is different from the one in the row
	    # above, that indicates a new boundary and a new interval below it.
	    
	    if ( $iref > 0 && $iref ne $col_intnum[$c] )
	    {
		$col_intnum[$c] = $iref;
		
		# my $t_age = $diagram->{top_bound}{$iref};
		my $label = $diagram->{label}{$iref};
		
		if ( my $split = $diagram->{label_split}{$iref} )
		{
		    $line2{$iref} = substr($label, $split);
		    $label = substr($label, 0, $split);
		}
		
		# If the interval doesn't extent up past this boundary, add a horizontal
		# border for the first line and the interval name to the second.
		
		unless ( $diagram->{open_top}{$iref} )
		{
		    # Uncomment the following line to generate a + at these junctions.
		    
		    # $line1 =~ s/[|]$/+/;
		    
		    $line1 .= '+' if $c == 1;
		    $line2 .= '|' if $c == 1;
		    
		    $line1 .= CellBorder($diagram, $c);
		    $line2 .= CellInterior($diagram, $c, $label);
		}
		
		# If the interval does extend up past this boundary, no horizontal border
		# is generated.
		
		else
		{
		    $line1 .= '|' if $c == 1;
		    $line2 .= '|' if $c == 1;
		    
		    $line1 .= CellInterior($diagram, $c);
		    $line2 .= CellInterior($diagram, $c, $label);
		}
	    }
	    
	    # If we have an interval identifier that is the same as the one in the row
	    # above, add empty cell interiors to both rows.
	    
	    elsif ( $iref > 0 && $line2{$iref} )
	    {
		$line1 .= '|' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellInterior($diagram, $c, $line2{$iref});
		$line2 .= CellInterior($diagram, $c);
		
		delete $line2{$iref};
	    }
	    
	    elsif ( $iref > 0 )
	    {
		$line1 .= '|' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellInterior($diagram, $c);
		$line2 .= CellInterior($diagram, $c);
	    }
	    
	    # If we have a 'b', that indicates a bottom boundary where no interval has
	    # been placed immediately afterward. Add a horizontal border to the first row,
	    # and below it a cell interior with the label '(empty)' to indicate a gap in
	    # the timescale.
	    
	    elsif ( $iref eq 'b' )
	    {
		$line1 .= '+' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellBorder($diagram, $c);
		$line2 .= CellInterior($diagram, $c, "(empty)");
	    }
	    
	    # If we have nothing at all, that indicates a continuing gap in the
	    # timescale.  Add empty cell interiors to both rows. If the row number is
	    # zero, put in an empty cell label.
	    
	    else
	    {
		$line1 .= '|' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellInterior($diagram, $c);
		$line2 .= CellInterior($diagram, $c, $r == 0 ? '(empty)' : undef);
	    }
	}
	
	# Add the age at the end of each horizontal boundary. If the age was evaluated
	# (interpolated) then add an asterisk.
	
	$line1 .= ' ' . sprintf("%-9s", $bound2d[$r][0]);
	$line2 .= ' ' x 10;
	
	if ( $diagram->{intp_bound}{$age} )
	{
	    $line1 .= ' *';
	}
	
	# Add both of the lines to the output.
	
	$output .= "$line1\n$line2\n";
    }
    
    # Add the bottom border of the table. Once again, the foreach is included here only so
    # that the $c loop is equally indented in all three sections.
    
    foreach my $r ( $max_row..$max_row )
    {
	my $age = $bound2d[$r][0];
	
	my $line1 = $margin;
	
	# Iterate through the columns.
	
	foreach my $c ( 1..$max_col )
	{
	    my $iref = $bound2d[$r][$c];
	    
	    # If we have an interval identifier in this row that is different from the
	    # interval above it, treat it as a bottom boundary.
	    
	    if ( $iref > 0 && $iref ne $col_intnum[$c] )
	    {
		$iref = 'b';
	    }
	    
	    # For a bottom boundary, add a horizontal border to the line.
	    
	    if ( $iref eq 'b' )
	    {
		$line1 .= '+' if $c == 1;		
		$line1 .= CellBorder($diagram, $c);
	    }
	    
	    # Otherwise, add an empty cell interior to the line. This will indicate that
	    # the interval continues past the bottom boundary of the display.
	    
	    else
	    {
		$line1 .= '|' if $c == 1;
		$line1 .= CellInterior($diagram, $c);
	    }
	}
	
	# Add the age at the end of the last horizontal boundary. If the age was
	# interpolated, add an asterisk.
	
	my $age = $bound2d[$r][0];
	
	$line1 .= ' ' . sprintf("%-9s", $age);
	
	if ( $diagram->{intp_bound}{$age} )
	{
	    $line1 .= ' *';
	}
	
	# Add the last line to the output.
	
	$output .= "$line1\n";
    }
    
    # Add the bottom margin, if one was specified.
    
    if ( $options->{margin_bottom} > 0 )
    {
	$output .= "\n" x $options->{margin_bottom};
    }
    
    return $output;
}


# CellBorder ( plot, c )
# 
# Return a string indicating a horizontal border for column $c of the diagram.

sub CellBorder {
    
    my ($diagram, $c) = @_;
    
    my $width = $diagram->{col_width}[$c] || 10;
    
    my $border = '-' x $width;
    
    return $border . '+';
}

# CellInterior ( plot, c, [label] )
# 
# Return a string indicating a cell interior, with or without a label, for column $c of
# the diagram.

sub CellInterior {
    
    my ($diagram, $c, $label) = @_;
    
    my $width = $diagram->{col_width}[$c] || 10;
    my $content;
    
    if ( $label )
    {
	$content = sprintf("%-${width}s", " $label");
    }
    
    else
    {
	$content = ' ' x $width;
    }
    
    return $content . '|';
}


# ColumnHeader ( plot, c )
# 
# Return a string indicating the header for column $c of the diagram.

sub ColumnHeader {
    
    my ($diagram, $c) = @_;
    
    my $width = $diagram->{col_width}[$c] || 10;
    
    my $label = $diagram->{header}[$c];
    
    if ( length($label) > $width - 2)
    {
	$label = substr($label, 0, $width - 2);
    }
    
    my $content = sprintf("%-${width}s", " $label");
    
    return $content . '*';
}
    
    
# AuthenticateSession ( )
# 
# 

sub AuthenticateSession {
    
    my ($dbh) = @_;
    
    if ( $ENV{AUTHORIZER_NO} )
    {
	return $ENV{AUTHORIZER_NO};
    }
    
    else
    {
	print "Who is authorizing these changes? ";
	my $auth = <STDIN>;
	chomp $auth;
	
	if ( $auth =~ /^\d+$/ )
	{
	    return $auth;
	}
	
	elsif ( $auth )
	{
	    my $qauth = $dbh->quote($auth);
	    
	    my ($auth_no, $name) = $dbh->selectrow_array("
		SELECT person_no, real_name FROM pbdb_wing.users
		WHERE username=$qauth or email=$qauth");
	    
	    if ( $auth_no =~ /^\d+$/ )
	    {
		say "Using $auth_no ($name)";
		return $auth_no;
	    }
	    
	    else
	    {
		die "Unknown authorizer '$auth'\n";
	    }
	}
	
	else
	{
	    die "You must specify an authorizer\n";
	}
    }
}

# ShowHelp ( )
# 
# Print out a help message about this command.

sub ShowHelp {
    
    print <<USAGE;
Usage: $0 [options] check [timescale] ( [pbdb timescale] )

       $0 [options] update [timescale] ( [pbdb timescale] )

The first form checks the interval definitions in The Paleobiology Database against
the definitions available through the Macrostrat API. The second form updates
the Paleobiology Database definitions to match the Macrostrat ones.

To update the standard international timescale, use 'international'. If a second
timescale name is given, it is used to select the corresponding timescale in the
Paleobiology Database.

Available options are:

  --log=filename      Write lines to the specified log file indicating what this
                      invocation of the command is doing. Otherwise, those lines
                      will be written to standard output.

  --db=dbname         Select a database to use. The default is 'pbdb'.

  --debug | -D        Write debugging output to standard error.

  --help | -h         Display this message and exit.

USAGE
}
