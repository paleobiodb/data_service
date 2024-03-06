#!/usr/bin/env perl
# 
# intervals.pl
# 
# Manage the definitions of geologic time intervals in the Paleobiology Database,
# in conjunction with a spreadsheet which holds the master interval definitions.

use strict;

use lib 'lib';

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

our ($AUTHORIZER_NO) = 185;	# All created and modified records are credited to
                                # Michael McClennen, unless this is changed.

our ($DBNAME);
our ($CREATE_INTERVALS) = 0;
our ($REMOVE_INTERVALS) = 0;
our ($UPDATE_INTERVALS) = 0;
our ($CREATE_SCALES) = 0;
our ($REMOVE_SCALES) = 0;
our ($UPDATE_SCALES) = 0;
our ($UPDATE_SEQUENCES) = 0;

# The input data and its parsed content are stored in the following globals.
# Yes, I know that's not consistent with best coding practice.

our ($FORMAT, $PARSER, $LINEEND);
our (@FIELD_LIST, %FIELD_MAP);
our (%INTERVAL_NAME, %INTERVAL_NUM, @ALL_INTERVALS);
our (%SCALE_NAME, %SCALE_NUM, %SCALE_INTS, @SCALE_NUMS, %SCALE_SELECT);
our (%DIFF_NAME, %DIFF_INT, @DIFF_MISSING, @DIFF_EXTRA);
our (%DIFF_SCALE, @DIFF_MISSING_SCALES);
our ($HAS_COLOR, $HAS_LOCALITY, $HAS_REFNO, $HAS_SEQUENCE);
our ($UPDATE_COUNT, $CREATE_COUNT, $REMOVE_COUNT);
our (@ERRORS);

# The following regexes validate ages and colors respectively.

our ($AGE_RE) = qr{^\d[.\d]*$};
our ($COLOR_RE) = qr{^#[0-9A-F]{6}$};

# Allowed interval types.

our (%INTERVAL_TYPE) = (eon => 1, era => 1, period => 1, epoch => 1,
			subepoch => 1, age => 1, subage => 1, zone => 1);

our (%TYPE_LABEL) = (eon => 'Eons', era => 'Eras', period => 'Periods',
		     epoch => 'Epochs', age => 'Ages', subage => 'Subages', zone => 'Zones');

# The following scales are used in generating values for stage_no, epoch_no, etc.

our ($INTL_SCALE) = '1';
our ($TERT_SUBEPOCHS) = '2';


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

# The subcommand 'print' reads the specified spreadsheet data and prints out one or
# more timescales as a sequence of boxes. This enables visual confirmation that the
# timescale boundaries have been input correctly.

elsif ( $CMD eq 'diagram' )
{
    &ReadSheet;
    &PrintScales(@REST);
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
	$DBNAME = $opt_dbname || 'test';
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
# Read an interval spreadsheet in CSV format from the specified URL or filename,
# or from STDIN. Put the data into a structure that can be used for checking and
# updating. Check the data for self consistency, as follows:
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
	
	($header, @data) = split /[\n\r]+/, $content;
    }
    
    elsif ( $opt_file && $opt_file eq '-' )
    {
	($header, @data) = <STDIN>;
    }	
    
    elsif ( $opt_file )
    {
	open(my $fh, '<', $opt_file) || die "Could not read $opt_file: $!";
	
	($header, @data) = <$fh>;
	
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
		    $SCALE_NUM{$scale_no}{action} = $record->{action}
			if $record->{action};
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
		    push @ERRORS, "at line $line_no, scale_no $scale_no inconsistent with line $prevline";
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
		push @ERRORS, "at line $line_no, interval '$interval_name' inconsistent with line $prevline";
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
	    # This is an error.
	    
	    else
	    {
		push @ERRORS, "at line $line_no, interval '$interval_name' already defined at line $prevline";
		next LINE;
	    }
	}
	
	# It is an error if we have a previously defined interval with the same number.
	
	elsif ( $INTERVAL_NUM{$interval_no} )
	{
	    my $prevline = $INTERVAL_NUM{$interval_no}{line_no};
	    push @ERRORS, "at line $line_no, interval_no $interval_no inconsistent with line $prevline";
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
    
    my (@args) = @_;
    
    foreach my $t ( @args )
    {
	if ( $t eq 'all' )
	{
	    $SCALE_SELECT{all} = 1;
	}
	
	elsif ( $SCALE_NUM{$t} )
	{
	    $SCALE_SELECT{$t} = 1;
	}
	
	elsif ( $t =~ /^(\d+)-(\d+)$/ )
	{
	    my $min = $1;
	    my $max = $2;
	    
	    foreach my $s ( @SCALE_NUMS )
	    {
		$SCALE_SELECT{$s} = 1 if $s >= $min && $s <= $max;
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
		    $SCALE_SELECT{$s} = 1;
		    $found++;
		}
	    }
	    
	    warn "Unrecognized timescale '$t'\n" unless $found;
	}
	
	else
	{
	    warn "Unrecognized timescale '$t'\n";
	}
    }
    
    return unless %SCALE_SELECT;
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no} )
	{
	    my $errors = &CheckOneScale($scale_no);
	    
	    my $name = $SCALE_NUM{$scale_no}{scale_name};
	    my $count = grep { $_->{action} !~ /^RENAME|^COALESCE/ } $SCALE_INTS{$scale_no}->@*;
	    
	    if ( $errors )
	    {
		say STDERR "Timescale $scale_no '$name' had $errors ERRORS";
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


# CheckOneScale ( scale_no )
# 
# Perform a series of consistency checks on a timescale, specified by scale_no. If any
# errors are found, they are appended to @ERRORS and the number of errors is returned.
# Otherwise, this subroutine returns an empty result.

sub CheckOneScale {
    
    my ($scale_no) = @_;
    
    my @errors;
    
    # Iterate over all of the interval records associated with the specified timescale.
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	my $line = $i->{line_no};
	my $name = $i->{interval_name};
	my $action = $i->{action};
	
	# If the action is 'REMOVE', no other checks are necessary.
	
	if ( $action =~ /^REMOVE/ )
	{
	    next;
	}
	
	# If the action is 'COALESCE', check that the interval name(s) to be coalesced
	# with are defined.
	
	elsif ( $action =~ /^COALESCE (\w.*)/ )
	{
	    my $coalesce = $1;
	    
	    # If the value is two interval names separated by a dash, generate errors if
	    # either or both are not found.
	    
	    if ( $coalesce =~ /^(.*?)-(.*)/ )
	    {
		push @errors, "at line $line, unrecognized interval '$1'" unless $INTERVAL_NAME{$1};
		push @errors, "at line $line, unrecognized interval '$2'" unless $INTERVAL_NAME{$2};
	    }
	    
	    # Otherwise, generate an error if the coalesce value is not found, or if it
	    # corresponds to an interval that itself will be coalesced or removed.
	    
	    elsif ( $INTERVAL_NAME{$coalesce} )
	    {
		push @errors, "at line $line, cannot coalesce with an interval to be removed"
		    if $INTERVAL_NAME{$coalesce}{action} =~ /^REMOVE|^COALESCE/;
	    }
	    
	    else
	    {
		push @errors, "at line $line, unrecognized interval '$coalesce'";
	    }
	}
	
	# If the action is neither of the above nor RENAME, generate an error.
	
	elsif ( $action && $action !~ /^RENAME/ )
	{
	    push @errors, "at line $line, invalid action '$action'";
	}
	
	# If the age fields are not empty, they must be checked.
	
	if ( $i->{t_type} )
	{
	    # If the value of t_type is 'use', then top must be the same as interval_name.
	    
	    if ( $i->{t_type} eq 'use' && $i->{top} ne $name )
	    {
		push @errors, "at line $line, top must be the same as interval_name";
	    }
	    
	    # Otherwise, the top age must evaluate to a number. If TopAge returns a value that
	    # is not a number, it is an error message.
	    
	    else
	    {
		my $top = $i->{t_age} = TopAge($i);
		
		unless ( $top =~ $AGE_RE )
		{
		    push @errors, "at line $line, interval '$name': $top";
		}
	    }
	    
	    # If the value of b_type is 'use', then base must be empty or the same as
	    # interval_name.
	    
	    if ( $i->{b_type} eq 'use' && $i->{base} && $i->{base} ne $name )
	    {
		push @errors, "at line $line, base must be the same as interval_name";
	    }
	    
	    else
	    {
		my $base = $i->{b_age} = BaseAge($i);
		
		unless ( $base =~ $AGE_RE )
		{
		    push @errors, "at line $line, interval '$name': $base";
		}
	    }
	}
	
	# An empty t_type field generates an error.
	
	else
	{
	    push @errors, "at line $line, missing age boundaries";
	}
	
	# If color is not empty, it must match the regexp defined above.
	
	if ( my $color = $i->{color} )
	{
	    unless ( $color =~ $COLOR_RE )
	    {
		push @errors, "at line $line, bad color '$color'";
	    }
	}
	
	# If type is not empty, it must be one of the values in %INTERVAL_TYPE.
	
	if ( my $type = $i->{type} )
	{
	    unless ( $INTERVAL_TYPE{$type} )
	    {
		push @errors, "at line $line, bad type '$type'";
	    }
	}
	
	# If reference_no is not empty, it must be a non-negative integer.
	
	if ( my $reference_no = $i->{reference_no} )
	{
	    unless ( $reference_no =~ /^\d+$/ )
	    {
		push @errors, "at line $line, bad reference_no '$reference_no'";
	    }
	}
	
	# If t_type is anything other than 'use', then top cannot be the same as
	# interval_name. 
	
	if ( $i->{top} && $i->{top} eq $name && $i->{t_type} ne 'use' )
	{
	    push @errors, "at line $line, top cannot be self-referential";
	}
	
	# If b_type is anything other than 'use', then base cannot be the same as
	# interval_name.
	
	if ( $i->{base} && $i->{base} eq $name && $i->{t_type} ne 'use' )
	{
	    push @errors, "at line $line, base cannot be self-referential";
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


# PrintScales ( scale... )
# 
# Diagram the specified timescale(s) as a sequence of boxes using ASCII characters. The
# scales will be drawn next to each other in the order specified, with boundaries lined
# up.  This enables visual confirmation that the timescale bounds have been entered
# correctly.

sub PrintScales {
    
    my (@args) = @_;
    
    my @print_list;
    
    foreach my $t ( @args )
    {
	if ( $SCALE_NUM{$t} )
	{
	    push @print_list, $t unless $SCALE_SELECT{$t};
	    $SCALE_SELECT{$t} = 1;
	}
	
	elsif ( $t =~ /[a-z]/ )
	{
	    my $re = qr{(?i)$t};
	    my $found;
	    
	    foreach my $s ( @SCALE_NUMS )
	    {
		if ( $SCALE_NUM{$s}{scale_name} =~ $re )
		{
		    push @print_list, $s unless $SCALE_SELECT{$s};
		    $SCALE_SELECT{$s} = 1;
		    $found++;
		}
	    }
	    
	    warn "Unrecognized timescale '$t'\n" unless $found;
	}
	
	else
	{
	    warn "Unrecognized timescale '$t'\n";
	}
    }
    
    return unless @print_list;
    
    # Check each of the scales to be printed.
    
    foreach my $scale_no ( @print_list )
    {
	my $errors = &CheckOneScale($scale_no);
	
	my $name = $SCALE_NUM{$scale_no}{scale_name};
	
	if ( $errors )
	{
	    say STDERR "Timescale '$name' had $errors ERRORS";
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
    
    my $plot = DiagramScales($options, \%SCALE_NUM, \%SCALE_INTS, @print_list);
    
    # Turn that array into character output and print it.
    
    my $output;
    
    if ( $opt_debug )
    {
	$output = DebugDiagram($options, $plot);
    }
    
    else
    {
	$output = DrawDiagram($options, $plot);
    }
    
    &SetupOutput;
    
    print $output;
}


# TopAge ( interval )
# 
# Evaluate the top age for the specified interval, and return it. If an error occurs, such
# as a reference to a nonexistent interval, return an error message instead.

sub TopAge {
    
    my ($interval) = @_;
    
    # Determine the interval whose bound is being used, and which bound it is. This could
    # be the same interval, or a different one. The empty hashref is used to detect
    # dependency loops.
    
    my ($lookup, $which) = TopAgeRef($interval, { });
    
    if ( ref $lookup )
    {
	my $age = $lookup->{$which};
	my $line = $lookup->{line_no};
	
	# If the age matches the regexp defined above, return it. Otherwise, return an
	# error message.
	
	if ( $age =~ $AGE_RE )
	{
	    return $age;
	}
	
	else
	{
	    return "bad top age '$age' at line $line";
	}
    }
    
    # If the result is not a reference, it is an error message.
    
    else
    {
	return $lookup;
    }
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
    
    # If the value of t_type for the argument interval is 'def', then this interval record
    # defines its own boundary.
    
    if ( $type eq 'def' )
    {
	return $interval, 'top';
    }
    
    # Otherwise, the value of top will be the name of some other interval. Look this name
    # up in the INTERVAL_NAME hash.
    
    elsif ( $type eq 'top' || $type eq 'base' )
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
    
    # If the value of t_type is anything else, return an error message.
    
    else
    {
	return "bad t_type '$type' at line $line";
    }
}


# BaseAge ( interval )
# 
# Evaluate the base age for the specified interval, and return it. If an error occurs, such
# as a reference to a nonexistent interval, return an error message instead.

sub BaseAge {
    
    my ($interval) = @_;
    
    # Determine the interval whose bound is being used, and which bound it is. This could
    # be the same interval, or a different one. The empty hashref is used to detect
    # dependency loops.
    
    my ($lookup, $which) = BaseAgeRef($interval, { });
    
    if ( ref $lookup )
    {
	my $age = $lookup->{$which};
	my $line = $lookup->{line_no};
	
	# If the age matches the regexp defined above, return it. Otherwise, return an
	# error message.
	
	if ( $age =~ $AGE_RE )
	{
	    return $age;
	}
	
	else
	{
	    return "bad base age '$age' at line $line";
	}
    }
    
    # If the result is not a reference, it is an error message.
    
    else
    {
	return $lookup;
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
    
    # If the value of b_type for the argument interval is 'def', then this interval record
    # defines its own boundary.
    
    if ( $type eq 'def' )
    {
	return $interval, 'base';
    }
    
    # Otherwise, the value of base will be the name of some other interval. Look this name
    # up in the INTERVAL_NAME hash.
    
    elsif ( $type eq 'top' || $type eq 'base' )
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
	$i->{t_age} //= TopAge($i);
	
	die "Bad top age for '$interval_name': $i->{t_age}\n" unless $i->{t_age} =~ /^\d/;
	
	$i->{b_age} //= BaseAge($i);
	
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
		$DIFF_NAME{$name}{type} = $m->{type};
	    }
	    
	    if ( $m->{color} && $color ne $m->{color} )
	    {
		$DIFF_NAME{$name}{color} = $m->{color};
	    }
	    
	    if ( $m->{abbrev} && $abbrev ne $m->{abbrev} )
	    {
		$DIFF_NAME{$name}{abbrev} = $m->{abbrev};
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
		    $DIFF_NAME{$name}{top} = $m->{t_age};
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
		    $DIFF_NAME{$name}{base} = $m->{b_age};
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
	$url = "$MACROSTRAT_INTERVALS?timescale_id=$timescale";
    }
    
    # Otherwise, use the 'timescale' parameter.
    
    else
    {
	$url="$MACROSTRAT_INTERVALS?timescale=$timescale";
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
    
    # use lib './lib';
    # require "CoreFunction.pm";
    # CoreFunction->import('connectDB');
    # require "TableDefs.pm";
    # TableDefs->import('%TABLE');
    # require "CoreTableDefs.pm";
    
    my $dbh = connectDB("config.yml", $dbname);
    
    # Select intervals contained in any of the specified timescales.
    
    foreach my $t ( @args )
    {
	if ( $t eq 'all' )
	{
	    $SCALE_SELECT{all} = 1;
	}
	
	elsif ( $SCALE_INTS{$t} )
	{
	    $SCALE_SELECT{$t} = 1;
	}
	
	elsif ( $t =~ /[a-z]/ )
	{
	    my $re = qr{(?i)$t};
	    my $found;
	    
	    foreach my $s ( @SCALE_NUMS )
	    {
		if ( $SCALE_NUM{$s}{scale_name} =~ $re )
		{
		    $SCALE_SELECT{$s} = 1;
		    $found++;
		}
	    }
	    
	    warn "Unrecognized timescale '$t'\n" unless $found;
	}
	
	else
	{
	    warn "Unrecognized timescale '$t'\n";
	}
    }
    
    return unless %SCALE_SELECT;
    
    # Check all selected scales, and compute the top and bottom ages. Do this for the
    # International scale and the tertiary subepochs as well, because those two are used
    # to compute containing intervals for other scales.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no} || 
	     $scale_no eq $INTL_SCALE || $scale_no eq $TERT_SUBEPOCHS )
	{
	    CheckOneScale($scale_no);
	}
    }
    
    # If any errors were found, abort unless the 'force' option was given.
    
    return if @ERRORS && ! $opt_force;
    
    # Check for certain database fields.
    
    CheckScaleTables($dbh);
    
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
    
    foreach my $i ( @ALL_INTERVALS )
    {
	my $scale_no = $i->{scale_no};
	
	next unless $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no};
	
	my $interval_no = $i->{interval_no};
	my $line_no = $i->{line_no};
	my $name = $i->{interval_name};
	my $abbrev = $i->{abbrev};
	my $t_age = $i->{t_age};
	my $b_age = $i->{b_age};
	my $type = $i->{type};
	my $color = $i->{color};
	my $reference_no = $i->{reference_no};
	
	ComputeContainers($i, $scale_no);
	
	if ( $scale_no == 1 )
	{
	    my $parent_no = $INTERVAL_NAME{$i->{parent}}{interval_no};
	    
	    unless ( $parent_no > 0 || $type eq 'eon' )
	    {
		push @ERRORS, "at line $line_no, no parent interval was found for '$name'";
	    }
	}
	
	if ( my $p = FetchPBDBInterval($dbh, $interval_no, $scale_no) )
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
		$DIFF_INT{$scale_no}{$interval_no}{abbrev} = $abbrev;
	    }
	    
	    if ( $type ne $p->{type} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{type} = $type;
	    }
	    
	    if ( $color ne $p->{color} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{color} = $color;
	    }
	    
	    if ( $reference_no ne $p->{reference_no} )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{reference_no} = $reference_no;
	    }
	    
	    if ( $t_age + 0 != $p->{t_age} + 0 )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{t_type} = $i->{t_type};
		$DIFF_INT{$scale_no}{$interval_no}{top} = $t_age;
	    }
	    
	    if ( $b_age + 0 != $p->{b_age} + 0 )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{b_type} = $i->{b_type};
		$DIFF_INT{$scale_no}{$interval_no}{base} = $b_age;
	    }
	    
	    if ( $i->{stage} ne IntervalName($p->{stage_no}) ||
		 $i->{subepoch} ne IntervalName($p->{subepoch_no}) ||
		 $i->{epoch} ne IntervalName($p->{epoch_no}) ||
		 $i->{period} ne IntervalName($p->{period_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{stage} = $i->{stage} || 'none';
		$DIFF_INT{$scale_no}{$interval_no}{subepoch} = $i->{subepoch} || 'none';
		$DIFF_INT{$scale_no}{$interval_no}{epoch} = $i->{epoch} || 'none';
		$DIFF_INT{$scale_no}{$interval_no}{period} = $i->{period} || 'none';
	    }
	    
	    if ( $scale_no eq $INTL_SCALE &&
		 $i->{parent} ne IntervalName($p->{parent_no}) )
	    {
		$DIFF_INT{$scale_no}{$interval_no}{parent} = $i->{parent} || 'none';
	    }
	    
	    # Remove this interval from the $remaining hash.
	    
	    delete $leftover_ints->{$interval_no};
	}
	
	elsif ( $i->{action} !~ /^REMOVE|^COALESCE/ )
	{
	    my $diff = { action => 'CREATE',
			 type => $type,
			 color => $color,
			 reference_no => $reference_no,
			 top => $t_age,
			 base => $b_age,
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
	
	if ( my $p = FetchPBDBScale($dbh, $scale_no) )
	{
	    if ( $name ne $p->{scale_name} )
	    {
		$DIFF_SCALE{$scale_no}{action} = 'RENAME';
	    }
	    
	    if ( $locality ne $p->{locality} )
	    {
		$DIFF_SCALE{$scale_no}{type} = $locality || 'none';
	    }
	    
	    if ( $color ne $p->{color} )
	    {
		$DIFF_SCALE{$scale_no}{color} = $color || 'none';
	    }
	    
	    if ( defined $t_age && $t_age ne '' && $t_age != $p->{t_age} )
	    {
		$DIFF_SCALE{$scale_no}{top} = $t_age;
	    }
	    
	    if ( defined $b_age && $b_age ne '' && $b_age != $p->{b_age} )
	    {
		$DIFF_SCALE{$scale_no}{base} = $b_age;
	    }
	    
	    if ( $reference_no ne $p->{reference_no} )
	    {
		$DIFF_SCALE{$scale_no}{reference_no} = $reference_no || 'none';
	    }
	    
	    if ( $s->{action} eq 'REMOVE' )
	    {
		$DIFF_SCALE{$scale_no}{action} = 'REMOVE';
	    }
	    
	    elsif ( $s->{action} )
	    {
		push @ERRORS, "at line $line_no, invalid action '$s->{action}'";
	    }
	    
	    delete $leftover_scales->{$scale_no};
	}
	
	else
	{
	    $DIFF_SCALE{$scale_no} = { action => 'CREATE',
				       scale_name => $name,
				       top => $t_age,
				       base => $b_age,
				       locality => $locality,
				       color => $color,
				       reference_no => $reference_no
				     };
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
    
    else
    {
	say STDERR "No differences between this spreadsheet and the PBDB";
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
	say "\Intervals missing from this spreadsheet:\n";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
    
    if ( %DIFF_SCALE )
    {
	say "\nScale differences from PBDB:\n";
	
	foreach my $scale_no ( @SCALE_NUMS )
	{
	    next unless $SCALE_SELECT{$scale_no} || $SCALE_SELECT{all};
	    
	    $DIFF_SCALE{$scale_no}{scale_no} = $scale_no;
	    $DIFF_SCALE{$scale_no}{scale_name} = $SCALE_NUM{$scale_no}{scale_name};
	    
	    print RecordToLine($DIFF_SCALE{$scale_no}, \@FIELD_LIST);
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
	say STDERR "The update cannot be carried out, because some intervals are missing";
	exit;
    }
    
    # Make sure that the scale_data table has the proper fields.
    
    ConditionScaleTables($dbh);
    
    # Fetch the list of PBDB intervals from each PBDB scale from the scale_map table.
    
    my %scale_seq = FetchPBDBSequences($dbh);
    
    # Run through the list of selected scales, and update each one that has changed.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	next unless $SCALE_SELECT{$scale_no} || $SCALE_SELECT{all};
	
	my $scale_name = $SCALE_NUM{$scale_no}{scale_name};
	
	# First check the interval sequence of this scale against the PBDB scale map.
	# Start by comparing the number of intervals in each sequence.
	
	my $sheet_count = $SCALE_INTS{$scale_no} ? 
	    grep { $_->{action} !~ /^REMOVE|^COALESCE/ } $SCALE_INTS{$scale_no}->@* : 0;
	my $pbdb_count = $scale_seq{$scale_no} ? $scale_seq{$scale_no}->@* : 0;
	
	my ($sequence_diff, $create_count, $remove_count, $update_count, $error_count);
	
	if ( $sheet_count != $pbdb_count )
	{
	    $sequence_diff = 1;
	}
	
	# If the interval counts are equal and there are some intervals in the
	# spreadsheet, make sure that the same set of interval numbers occurs in both
	# sequences.
	
	elsif ( $sheet_count )
	{
	    foreach my $i ( 0..$sheet_count-1 )
	    {
		if ( $scale_seq{$scale_no}[$i] ne $SCALE_INTS{$scale_no}[$i]{interval_no} )
		{
		    $sequence_diff = 1;
		}
	    }
	    
	    # my (%check) = 1;
	    
	    # # Add all of the interval_no values from the pbdb sequence to %check.
	    
	    # foreach my $i ( 0..$sheet_count-1 )
	    # {
	    # 	$check{ $scale_seq{$scale_no}[$i] } = 1;
	    # }
	    
	    # # Check that each of the interval_no values from the spreadsheet is in
	    # # %check, and remove it. If any are missing, set sequence_diff to true.
	    
	    # foreach my $i ( 0..$sheet_count-1 )
	    # {
	    # 	my $ino = $SCALE_INTS{$scale_no}[$i]{interval_no};
		
	    # 	if ( $check{$ino} )
	    # 	{
	    # 	    delete $check{$ino};
	    # 	}
		
	    # 	else
	    # 	{
	    # 	    $sequence_diff = 1;
	    # 	    last;
	    # 	}
	    # }
	    
	    # # If anything remains in %check, set sequence_diff to true.
	    
	    # $sequence_diff = 1 if %check;
	}
	
	# If there is a difference, change the scale_map to match.
	
	if ( $sequence_diff )
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
		
		$DIFF_INT{$scale_no}{$interval_no}{t_age} = $i->{t_age} // TopAge($i);
		$DIFF_INT{$scale_no}{$interval_no}{b_age} = $i->{b_age} // BaseAge($i); 
		
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
	    say STDERR "Summary of database changes that would have been made:\n";
	}
	
	else
	{
	    say STDERR "Summary of database changes made:\n";
	}
	
	say STDERR "  Created $CREATE_INTERVALS intervals" if $CREATE_INTERVALS;
	say STDERR "  Removed $REMOVE_INTERVALS intervals" if $REMOVE_INTERVALS;
	say STDERR "  Updated $UPDATE_INTERVALS intervals" if $UPDATE_INTERVALS;
	say STDERR "  Created $CREATE_SCALES timescales" if $CREATE_SCALES;
	say STDERR "  Removed $REMOVE_SCALES timescales" if $REMOVE_SCALES;
	say STDERR "  Updated $UPDATE_SCALES timescales" if $UPDATE_SCALES;
	say STDERR "  Updated $UPDATE_SEQUENCES timescale sequences" if $UPDATE_SEQUENCES;
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
    
    my $sql = "SELECT i.interval_no, interval_name, abbrev, type, early_age as b_age,
		    late_age as t_age, scale_no, parent_no, color, sm.reference_no,
		    stage_no, subepoch_no, epoch_no, period_no
		FROM $TableDefs::TABLE{INTERVAL_DATA} as i
		    left join interval_lookup using (interval_no)
		    left join $TableDefs::TABLE{SCALE_MAP} as sm 
		        on sm.interval_no = i.interval_no and sm.scale_no = $qs
		WHERE i.interval_no = $qi";
    
    unless ( $HAS_REFNO )
    {
	$sql =~ s/sm.reference_no,//;
    }
    
    my $result = $dbh->selectrow_hashref($sql, { Slice => { } });
    
    return $result;
}


# FetchPBDBScale ( dbh, scale_no )
# 
# Given a scale_no, fetch the corresponding PBDB scale record.

sub FetchPBDBScale {
    
    my ($dbh, $scale_no) = @_;
    
    my $qs = $dbh->quote($scale_no);
    
    my $sql = "SELECT scale_no, scale_name, early_age as b_age, late_age as t_age,
		      color, locality, reference_no
		FROM $TableDefs::TABLE{SCALE_DATA} as s
		WHERE s.scale_no = $qs";
    
    my $result = $dbh->selectrow_hashref($sql, { Slice => { } });
    
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
    
    if ( $HAS_SEQUENCE )
    {
	$sql = "SELECT scale_no, interval_no FROM $TABLE{SCALE_MAP} order by sequence";
    }
    
    else
    {
	$sql = "SELECT scale_no, interval_no FROM $TABLE{SCALE_MAP}";
    }
    
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
	my $interval_no = $i->{interval_no};
	my $qtype = $dbh->quote($i->{type});
	my $qcolor = $dbh->quote($i->{color});
	my $qrefno = $dbh->quote($i->{reference_no});
	$seq++;
	
	$value_string .= ', ' if $value_string;
	
	if ( $scale_no == 1 )
	{
	    my $parent_no = $INTERVAL_NAME{$i->{parent}}{interval_no} || 'NULL';
	    die "Bad parent '$i->{parent}' for interval $i->{interval_no}"
		unless $parent_no > 0 || $i->{type} eq 'eon';
	    
	    $value_string .= "($scale_no,$interval_no,$seq,$parent_no,$qtype,$qcolor,$qrefno)";
	}
	
	else
	{
	    $value_string .= "($scale_no,$interval_no,$qtype,$qcolor,$qrefno)";
	}
    }
    
    if ( $scale_no == 1 )
    {
	$sql = "REPLACE INTO $SCALE_MAP (scale_no, interval_no, sequence, parent_no,
		    type, color, reference_no)
		VALUES $value_string";
    }
    
    else
    {
	$sql = "REPLACE INTO $SCALE_MAP (scale_no, interval_no, sequence,
		    type, color, reference_no)
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
    my $line_no = $diff->{line_no};
    
    my $scale_map_updated = $SCALE_NUM{$scale_no}{scale_map_updated};
    
    unless ( $scale_no && $scale_no =~ /^\d+$/ )
    {
	push @ERRORS, "at line $line_no, could not update interval: bad scale_no '$scale_no'";
	return;
    }
    
    unless ( $interval_no && $interval_no =~ /^\d+$/ )
    {
	push @ERRORS, "at $line_no, could not update interval: bad interval_no '$interval_no'";
    }
    
    # If this is a new interval, create the necessary records.
    
    if ( $diff->{action} eq 'CREATE' )
    {
	CreatePBDBInterval($dbh, $scale_no, $interval_no, $diff);
	UpdateIntervalLookup($dbh, $scale_no, $interval_no, $diff);
	return;
    }
    
    # If this interval is to be removed or coalesced, do the corresponding deletions and
    # updates.
    
    elsif ( $diff->{action} =~ /^REMOVE|^COALESCE/ )
    {
	RemovePBDBInterval($dbh, $scale_no, $interval_no, $diff);
	return;
    }
    
    # Otherwise, update the existing records in the interval_data table, scale_map
    # table, and interval_lookup table.
    
    if ( $diff->{action} eq 'RENAME' )
    {
	my $qname = $dbh->quote($name);
	
	$sql = "UPDATE $INTERVAL_DATA set interval_name = $qname
		WHERE interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{abbrev} )
    {
	my $qabbr = $dbh->quote($diff->{abbrev});
	
	$sql = "UPDATE $INTERVAL_DATA SET abbrev = $qabbr
		WHERE interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{top} )
    {
	my $qtop = $dbh->quote($diff->{top});
	    
	if ( $diff->{top} =~ $AGE_RE )
	{
	    $sql = "UPDATE $INTERVAL_DATA set late_age = $qtop
		    WHERE interval_no = $interval_no";
	    
	    $result = DoStatement($dbh, $sql);
	}
	
	else
	{
	    push @ERRORS, "at line $line_no, problem updating interval: bad top age $qtop";
	}
    }
    
    if ( $diff->{base} )
    {
	my $qbase = $dbh->quote($diff->{base});
	
	if ( $diff->{base} =~ $AGE_RE )
	{
	    $sql = "UPDATE $INTERVAL_DATA set early_age = $qbase
		    WHERE interval_no = $interval_no";
	    
	    $result = DoStatement($dbh, $sql);
	}
	
	else
	{
	    push @ERRORS, "at line $line_no, problem updating interval: bad base age $qbase";
	}
    }
    
    if ( $diff->{type} && ! $scale_map_updated )
    {
	my $qtype = $dbh->quote($diff->{type});
	
	$sql = "UPDATE $SCALE_MAP set type = $qtype
		WHERE scale_no = $scale_no and interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{color} && ! $scale_map_updated )
    {
	my $qcolor = $dbh->quote($diff->{color});
	
	$sql = "UPDATE $SCALE_MAP set color = $qcolor
		WHERE scale_no = $scale_no and interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
	
    if ( $diff->{reference_no} && ! $scale_map_updated )
    {
	my $qrefno = $dbh->quote($diff->{reference_no});
	
	$sql = "UPDATE $SCALE_MAP set reference_no = $qrefno
		WHERE scale_no = $scale_no and interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
	
    if ( $diff->{parent} && ! $scale_map_updated )
    {
	my $parent_no = $INTERVAL_NAME{$diff->{parent}}{interval_no};
	my $qparent = $dbh->quote($parent_no);
	
	$sql = "UPDATE $SCALE_MAP set parent_no = $qparent
		WHERE scale_no = $scale_no and interval_no = $interval_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{stage} )
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
    
    unless ( $diff->{top} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no, could not create interval: bad top age '$diff->{top}'";
	return;
    }
    
    unless ( $diff->{base} =~ $AGE_RE )
    {
	push @ERRORS, "at line $line_no,  could not create interval: bad base age '$diff->{base}'";
	return;
    }
    
    my $qino = $dbh->quote($interval_no);
    my $qname = $dbh->quote($name);
    my $qabbr = $dbh->quote($diff->{abbrev});
    my $qtop = $dbh->quote($diff->{top});
    my $qbase = $dbh->quote($diff->{base});
    my $qrefno = $dbh->quote($diff->{reference_no});
    
    # Create a record in the interval_data table.
    
    $sql = "INSERT INTO $INTERVAL_DATA (interval_no, interval_name, abbrev,
		late_age, early_age, reference_no)
	    VALUES ($qino, $qname, $qabbr, $qtop, $qbase, $qrefno)";
    
    $result = DoStatement($dbh, $sql);
    
    # Create the corresponding record in the intervals table.
    
    my ($eml, $classic_name) = EmlName($name);
    
    my $qeml = $dbh->quote($eml);
    my $qclassic = $dbh->quote($classic_name);
    
    $sql = "INSERT INTO $CLASSIC_INTS (authorizer_no, enterer_no, interval_no,
		eml_interval, interval_name, reference_no)
	    VALUES ($AUTHORIZER_NO, $AUTHORIZER_NO, $qino, $qeml, $qclassic, $qrefno)";
    
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
    
    my $qstage = $dbh->quote($stage_no);
    my $qsubep = $dbh->quote($subepoch_no);
    my $qepoch = $dbh->quote($epoch_no);
    my $qperiod = $dbh->quote($period_no);
    
    my $qtop = $dbh->quote($diff->{t_age});
    my $qbase = $dbh->quote($diff->{b_age});
    
    $sql = "REPLACE INTO $CLASSIC_LOOKUP (interval_no, stage_no, subepoch_no,
		epoch_no, period_no, top_age, base_age)
	    VALUES ($interval_no, $qstage, $qsubep, $qepoch, $qperiod, $qtop, $qbase)";
    
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
	push @ERRORS, "at line $line_no, could not remove interval: $count collections";
	return;
    }
    
    # If the action is 'COALESCE', then these collections must be updated to point to
    # the new interval.
    
    if ( $count && $diff->{action} =~ /^COALESCE (.*)/ )
    {
	my $arg = $1;
	my $new_no = $INTERVAL_NAME{$arg}{interval_no};
	my $update_count = 0;
	
	unless ( $new_no > 0 )
	{
	    push @ERRORS, "at line $line_no, could not coalesce interval: bad argument '$arg'";
	    return;
	}
	
	$sql = "UPDATE $COLLECTIONS set max_interval_no = $new_no
		WHERE max_interval_no = $interval_no";
	
	$update_count += DoStatement($dbh, $sql);
	
	$sql = "UPDATE $COLLECTIONS set min_interval_no = $new_no
		WHERE min_interval_no = $interval_no";
	
	$update_count += DoStatement($dbh, $sql);
	
	$sql = "UPDATE $COLLECTIONS set ma_interval_no = $new_no
		WHERE ma_interval_no = $interval_no";
	
	$update_count += DoStatement($dbh, $sql);
	
	unless ( $opt_debug )
	{
	    say STDERR "Performed $update_count collection interval updates '$name' => '$arg'";
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
	push @ERRORS, "at line $line_no, could not update scale: bad scale_no '$scale_no'";
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
    
    if ( $diff->{action} eq 'RENAME' )
    {
	my $qname = $dbh->quote($name);
	
	$sql = "UPDATE $SCALE_DATA set scale_name = $qname
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{color} )
    {
	my $color = $diff->{color} eq 'none' ? undef : $diff->{color};
	my $qcolor = $dbh->quote($color);
	
	$sql = "UPDATE $SCALE_DATA set color = $qcolor
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{type} )
    {
	my $locality = $diff->{type} eq 'none' ? undef : $diff->{type};
	my $qloc = $dbh->quote($locality);
	
	$sql = "UPDATE $SCALE_DATA set locality = $qloc
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{reference_no} )
    {
	my $refno = $diff->{reference_no} eq 'none' ? undef : $diff->{reference_no};
	my $qrefno = $dbh->quote($refno);
	
	$sql = "UPDATE $SCALE_DATA set reference_no = $qrefno
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{top} )
    {
	my $qtop = $dbh->quote($diff->{top});
	
	$sql = "UPDATE $SCALE_DATA set late_age = $qtop
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    if ( $diff->{base} )
    {
	my $qbase = $dbh->quote($diff->{base});
	
	$sql = "UPDATE $SCALE_DATA set early_age = $qbase
		WHERE scale_no = $scale_no";
	
	$result = DoStatement($dbh, $sql);
    }
    
    # Report what we have done.
    
    $UPDATE_SCALES++;
    
    unless ( $opt_debug )
    {
	say STDOUT "Updated scale '$name' ($scale_no)";
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
    
    # Create a record in the scale_data table.
    
    $sql = "INSERT INTO $SCALE_DATA (scale_no, scale_name, color, locality,
		reference_no, late_age, early_age)
	    VALUES ($scale_no, $qname, $qcolor, $qloc, $qrefno, $qtop, $qbase)";
    
    $result = DoStatement($dbh, $sql);
    
    unless ( $opt_debug )
    {
	say STDOUT "Created scale '$name' ($scale_no)";
	$CREATE_SCALES++;
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
    
    my ($sql, $result);
    
    my @table_list = qw(interval_data interval_lookup intervals scale_data scale_map);
    
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
	$sql = "SELECT count(*) FROM interval_data";
	
	my ($id_count) = $dbh->selectrow_array($sql);
	
	$sql = "SELECT count(*) FROM scale_data";
	
	my ($sd_count) = $dbh->selectrow_array($sql);
	
	say "There are $id_count intervals in $sd_count scales.";
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
    
    my $INTERVAL_DATA = $TableDefs::TABLE{INTERVAL_DATA};
    my $CLASSIC_INTS = $TableDefs::TABLE{CLASSIC_INTERVALS};
    
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
    
    $sql = "SELECT id.interval_no, id.interval_name, id.reference_no, 
		i.interval_no as classic_no, i.eml_interval, i.interval_name as classic_name
	    FROM $INTERVAL_DATA as id left join $CLASSIC_INTS as i using (interval_no)";
    
    my @result = $dbh->selectall_array($sql, { Slice => { } });
        
    foreach my $r ( @result )
    {
	my $interval_no = $r->{interval_no};
	my $interval_name = $r->{interval_name};
	
	my ($eml, $classic_name) = EmlName($interval_name);
	
	# If the interval table record is missing, create it.
	
	if ( ! $r->{classic_no} )
	{
	    my $qino = $dbh->quote($interval_no);
	    my $qeml = $dbh->quote($eml);
	    my $qname = $dbh->quote($classic_name);
	    my $qref = $dbh->quote($r->{reference_no});
	    
	    $sql = "INSERT INTO $CLASSIC_INTS (authorizer_no, enterer_no, interval_no,
			eml_interval, interval_name, reference_no)
		    VALUES ($AUTHORIZER_NO, $AUTHORIZER_NO, $qino, $qeml, $qname, $qref)";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say STDERR  "  Created $qname ($interval_no) in the classic intervals table"
		unless $opt_debug;
	}
	
	# If the names don't match, do an update.
	
	elsif ( $eml ne $r->{eml_interval} || $classic_name ne $r->{classic_name} )
	{
	    my $qino = $dbh->quote($interval_no);
	    my $qeml = $dbh->quote($eml);
	    my $qname = $dbh->quote($classic_name);
	    
	    $sql = "UPDATE $CLASSIC_INTS
		    SET eml_interval=$qeml, interval_name=$qname
		    WHERE interval_no = $qino";
	    
	    $result = DoStatement($dbh, $sql);
	    
	    say STDERR "  Renamed $qname ($interval_no) in the classic intervals table"
		unless $opt_debug;
	}
    }
    
    # Update reference numbers.
    
    $sql = "UPDATE $INTERVAL_DATA as id join $CLASSIC_INTS as i using (interval_no)
	    SET i.reference_no = id.reference_no, i.modifier_no = $AUTHORIZER_NO
	    WHERE i.reference_no <> id.reference_no";
    
    $result = DoStatement($dbh, $sql);
    
    if ( $result > 0 )
    {
	say STDERR "  Updated $result reference numbers in the classic intervals table";
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
    
    my ($dbh, $sql) = @_;
    
    if ( $opt_debug )
    {
	print STDOUT "$sql\n\n";
	return;
    }
    
    else
    {
	return $dbh->do($sql);
    }
}


# CheckScaleTables ( dbh )
# 
# Check to see whether the PBDB scale_data table has the fields 'color' and 'locality'.

sub CheckScaleTables {
    
    my ($dbh) = @_;
    
    my $sql = "SHOW CREATE TABLE $TableDefs::TABLE{SCALE_DATA}";
    
    my ($table, $def) = $dbh->selectrow_array($sql);
    
    if ( $def =~ /`color`/ )
    {
	$HAS_COLOR = 1;
    }
    
    if ( $def =~ /`locality`/ )
    {
	$HAS_LOCALITY = 1;
    }
    
    $sql = "SHOW CREATE TABLE $TableDefs::TABLE{SCALE_MAP}";
    
    my ($table, $def) = $dbh->selectrow_array($sql);
    
    if ( $def =~ /`reference_no`/ )
    {
	$HAS_REFNO = 1;
    }
    
    if ( $def =~ /`sequence`/ )
    {
	$HAS_SEQUENCE = 1;
    }
}


# ConditionScaleTables ( dbh )
# 
# Add the fields 'color' and 'locality' to the PBDB scale_data table, and 'reference_no'
# and 'sequence' to the scale_map table.

sub ConditionScaleTables {
    
    my ($dbh) = @_;
    
    my ($sql, $result);
    
    my $SCALE_DATA = $TableDefs::TABLE{SCALE_DATA};
    my $SCALE_MAP = $TableDefs::TABLE{SCALE_MAP};
    
    unless ( $HAS_COLOR )
    {
	$sql = "ALTER TABLE $SCALE_DATA change default_color color varchar(10) null";
	$result = DoStatement($dbh, $sql);
	$HAS_COLOR = 1;
    }
    
    unless ( $HAS_LOCALITY )
    {
	$sql = "ALTER TABLE $SCALE_DATA add locality varchar(80) null after color";
	$result = DoStatement($dbh, $sql);
	$HAS_LOCALITY = 1;
    }
    
    unless ( $HAS_SEQUENCE )
    {
	$sql = "ALTER TABLE $SCALE_MAP add sequence int unsigned not null after interval_no";
	$result = DoStatement($dbh, $sql);
	$HAS_SEQUENCE = 1;
    }
    
    unless ( $HAS_REFNO )
    {
	$sql = "ALTER TABLE $SCALE_MAP add reference_no int unsigned null after color";
	$result = DoStatement($dbh, $sql);
	$HAS_REFNO = 1;
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
    
    # Then run through the Cenozoic and Late Cretaceous subepochs.
    
    foreach my $c ( $SCALE_INTS{$TERT_SUBEPOCHS}->@* )
    {
	if ( $c->{type} eq 'subepoch' )
	{
	    last if $c->{t_age} > $i->{b_age};
	
	    if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
	    {
		$i->{subepoch} = $c->{interval_name};
	    }
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
    
    my ($t_age, $b_age, $reference_no, $remove);
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	# Compute the minimum and maximum age bounds.
	
	if ( !defined $t_age || $i->{t_age} < $t_age )
	{
	    $t_age = $i->{t_age};
	}
	
	if ( !defined $b_age || $i->{b_age} > $b_age )
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


# DiagramScales ( options, scale_hash, ints_hash, timescale... )
# 
# Generate a 2-d array encoding columns of boxes, in order to display the specified
# timescale(s) visually. Each row in the @bounds2d array represents a distinct age
# boundary. Column 0 specifies the age of each boundary, and the remaining columns specify
# the content to be displayed below that boundary for each timescale in turn.
# 
# The first argument must be a hashref, and subsequent arguments must all be scale_no
# values.

sub DiagramScales {
    
    my ($options, $scale_hash, $ints_hash, @scale_list) = @_;
    
    # If no age limits are given, use 0 and 5000.
    
    my $t_limit = $options->{t_limit} || 0;
    my $b_limit = $options->{b_limit} || 5000;
    
    # Unless we are displaying to the end of the Proterozoic, there is no point in showing
    # eons and eras.
    
    my $remove_eras = $b_limit < 2500;
    
    # Phase I: determine interval boundaries
    
    # Start by computing the age of each boundary in the scale, and the minimum and
    # maximum. If top and base limits were given, restrict the set of age boundaries to
    # that range.
    
    my (%bound, $t_range, $b_range, %ibound, $t_intl, $b_intl);
    
    # For each displayed scale in turn, run through its list of intervals.
    
    foreach my $s ( @scale_list )
    {
	foreach my $i ( $ints_hash->{$s}->@* )
	{
	    # Ignore any interval that is flagged for removal, and ignore eras and eons if
	    # $remove_eras is true.
	    
	    next if $i->{action} =~ /^REMOVE|^COALESCE/;
	    next if $remove_eras && $i->{type} =~ /^era|^eon/;
	    
	    # Throw an exception if we find a bad age. This shouldn't happen, except in
	    # rare cases when the 'force' option is used.
	    
	    my $top = $i->{t_age} = TopAge($i);
	    my $base = $i->{b_age} = BaseAge($i);
	    
	    unless ( $top =~ $AGE_RE and $base =~ $AGE_RE )
	    {
		die "Bad age boundary for '$i->{interval_name}'\n";
	    }
	    
	    # Skip this interval if it falls outside of the age limits.
	    
	    next if $base <= $t_limit;
	    next if $top >= $b_limit;
	    
	    # If this interval overlaps the age limits, only display the part that lies
	    # within them. The Phase II code will leave out the boundary to show that the
	    # interval overlaps at top or bottom.
	    
	    if ( $top < $t_limit )
	    {
		$top = $t_limit;
	    }
	    
	    if ( $base > $b_limit )
	    {
		$base = $b_limit;
	    }
	    
	    # Keep track of the age boundaries separately for the international scale and
	    # all other scales. Keep track of the minimum and maximum boundary ages
	    # separately as well.
	    
	    if ( $s eq $INTL_SCALE )
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
		
	    if ( $s eq $INTL_SCALE )
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
    
    if ( $b_range )
    {
	foreach my $b ( keys %ibound )
	{
	    $bound{$b} = 1 if $b >= $t_range && $b <= $b_range;
	}
    }
    
    # If we are displaying only the international scale, use all of its boundaries.
    
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
    # the end of the Proterozoic.
    
    $remove_eras = 1 if $b_range < 2500;
    
    # Phase II: Generate the diagram
    
    # The following arrays and hashes store the information necessary to draw the
    # diagram.
    
    my @bound2d;	# Each element (cell) represents one interval boundary plus the
			# content below it. The first column holds the age.
    
    my @col_type;	# Stores which interval type belongs in which column
    
    my @col_width;	# Stores the width of each column
    
    my @header;		# Stores the header label for each column
    
    my %label;		# Stores the label for each cell.
    
    my %t_age;		# Stores the top age for each cell.
    
    my %b_age;		# Stores the bottom age for each cell.
    
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
    
    foreach my $s ( @scale_list )
    {
	$min_col = $max_col + 1;
	$max_col = $min_col;
	
	my ($last_top, $last_base, $last_ref);
	
	# Run through the intervals in the order they appear in the timescale. This
	# order can affect how they are displayed if some of them overlap others.
	
	foreach my $i ( $ints_hash->{$s}->@* )
	{
	    # Ignore any interval that is flagged for removal, and ignore eras and eons if
	    # $remove_eras is true.
	    
	    next if $i->{action} =~ /^REMOVE|^COALESCE/;
	    next if $remove_eras && $i->{type} =~ /^era|^eon/;
	    
	    # The displayed intervals are identified by their interval_no value. It is
	    # possible that the same interval may appear in more than one timescale
	    # displayed at the same time, in which case it must have the same name and
	    # top/bottom ages in each case. It is okay to use the same identifier for
	    # both, since name and top/bottom ages are the only attributes stored.
	    
	    my $iref = $i->{interval_no};
	    my $iname = $i->{interval_name};
	    my $top = $i->{t_age};
	    my $base = $i->{b_age};
	    
	    # Ignore any interval that falls outside of the age bounds.
	    
	    next if $base <= $t_limit;
	    next if $top >= $b_limit;
	    
	    # If this interval overlaps the top or bottom age boundary, display only the
	    # part that falls within these boundaries. The horizontal boundary line will
	    # be suppressed in these cases (see below) so that the overlap is clear.
	    
	    if ( $top < $t_limit )
	    {
		$top = $t_limit;
	    }
	    
	    if ( $base > $b_limit )
	    {
		$base = $b_limit;
	    }
	    
	    # If the top and bottom ages for this interval are identical to the previous
	    # interval, display this interval name in the same box as the previous one
	    # rather than generating a separate box in a separate column.  This situation
	    # happens quite a bit in some of our timescales.
	    
	    if ( $top eq $last_top && $base eq $last_base )
	    {
		$label{$last_ref} .= '/' . $iname;
		next;
	    }
	    
	    # In all other cases, store the interval name and top/bottom ages using the
	    # interval_no as key.
	    
	    else
	    {
		$label{$iref} = $iname;
		$t_age{$iref} = $top;
		$b_age{$iref} = $base;
		
		$last_top = $top;
		$last_base = $base;
		$last_ref = $iref;
	    }
	    
	    # The header of the first column for each displayed scale contains the scale
	    # number and as much of the scale name as will fit.
	    
	    $header[$min_col] ||= "$s $scale_hash->{$s}{scale_name}";
	    
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
	    
	    # Place this interval either in the minimum column for this scale, or up to 5
	    # columns further to the right if necessary to avoid overlapping any interval
	    # that has already been placed.
	    
	  COLUMN:
	    while ( $c < $min_col+5 )
	    {
		# If this interval has a type that is different from the interval type for
		# the current column, move one column to the right and try again.
		
		if ( $col_type[$c] && $i->{type} )
		{
		    $c++, next COLUMN unless $col_type[$c] eq $i->{type};
		}
		
		# Otherwise, set the interval type for this column to the type for this
		# interval if it is not already set.
		
		elsif ( $i->{type} )
		{
		    $col_type[$c] ||= $i->{type};
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
		    if ( $i->{type} )
		    {
			my $label = $TYPE_LABEL{$i->{type}};
			$header[$c] = "$s $label";
		    }
		    
		    else
		    {
			$header[$c] = $s;
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
	    
	    # If the bottom age of the interval is not the same as the age of the bottom
	    # boundary row, that means the interval continues past the display area. So
	    # store the interval number to indicate that situation.
	    
	    if ( $base ne $i->{b_age} )
	    {
		$bound2d[$rbase][$c] = $iref;
	    }
	    
	    # Otherwise, store a 'b' to indicate a bottom boundary.
	    
	    else
	    {
		$bound2d[$rbase][$c] = 'b';
	    }
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
		   t_age => \%t_age,
		   b_age => \%b_age,
		   max_row => $max_row,
		   max_col => $max_col };
    
    return $result;
}


sub DebugDiagram {
    
    my ($options, $plot) = @_;
    
    my @bound2d = $plot->{bound2d}->@*;
    my @col_width = $plot->{col_width}->@*;
    my @col_intnum;
    my $max_row = $plot->{max_row};
    my $max_col = $plot->{max_col};
    
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


# DrawDiagram ( options, plot )
# 
# Draw a set of boxes using ASCII characters, according to the rows and columns laid out
# in $plot, modified by the options in $options.

sub DrawDiagram {
    
    my ($options, $plot) = @_;
    
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
    
    my @bound2d = $plot->{bound2d}->@*;
    my @col_width = $plot->{col_width}->@*;
    my $max_row = $plot->{max_row};
    my $max_col = $plot->{max_col};
    
    # The following array keeps track of which interval identifier was last seen in each
    # column as we scan down the rows, so we will know when it changes.
    
    my @col_intnum;
    
    # Generate the column header row. The foreach loops here and in the footer are only
    # there so that the $c loop is equally indented in all three sections.
    
    foreach my $r ( 0..0 )
    {
	my $line1 = $margin . '*';
	
	foreach my $c ( 1..$max_col )
	{
	    $line1 .= ColumnHeader($plot, $c);
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
		
		my $t_age = $plot->{t_age}{$iref};
		my $label = $plot->{label}{$iref};
		
		# If the interval doesn't extent up past this boundary, add a horizontal
		# border for the first line and the interval name to the second.
		
		if ( $t_age eq $age )
		{
		    # Uncomment the following line to generate a + at these junctions.
		    
		    # $line1 =~ s/[|]$/+/;
		    
		    $line1 .= '+' if $c == 1;
		    $line2 .= '|' if $c == 1;
		    
		    $line1 .= CellBorder($plot, $c);
		    $line2 .= CellInterior($plot, $c, $label);
		}
		
		# If the interval does extend up past this boundary, no horizontal border
		# is generated.
		
		else
		{
		    $line1 .= '|' if $c == 1;
		    $line2 .= '|' if $c == 1;
		    
		    $line1 .= CellInterior($plot, $c);
		    $line2 .= CellInterior($plot, $c, $label);
		}
	    }
	    
	    # If we have an interval identifier that is the same as the one in the row
	    # above, add empty cell interiors to both rows.
	    
	    elsif ( $iref > 0 )
	    {
		$line1 .= '|' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellInterior($plot, $c);
		$line2 .= CellInterior($plot, $c);
	    }
	    
	    # If we have a 'b', that indicates a bottom boundary where no interval has
	    # been placed immediately afterward. Add a horizontal border to the first row,
	    # and below it a cell interior with the label '(empty)' to indicate a gap in
	    # the timescale.
	    
	    elsif ( $iref eq 'b' )
	    {
		$line1 .= '+' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellBorder($plot, $c);
		$line2 .= CellInterior($plot, $c, "(empty)");
	    }
	    
	    # If we have nothing at all, that indicates a continuing gap in the timescale.
	    # Add empty cell interiors to both rows.
	    
	    else
	    {
		$line1 .= '|' if $c == 1;
		$line2 .= '|' if $c == 1;
		
		$line1 .= CellInterior($plot, $c);
		$line2 .= CellInterior($plot, $c);
	    }
	}
	
	# Add the age at the end of each horizontal boundary.
	
	$line1 .= ' ' . sprintf("%-9s", $bound2d[$r][0]);
	$line2 .= ' ' x 10;
	
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
		$line1 .= CellBorder($plot, $c);
	    }
	    
	    # Otherwise, add an empty cell interior to the line. This will indicate that
	    # the interval continues past the bottom boundary of the display.
	    
	    else
	    {
		$line1 .= '|' if $c == 1;
		$line1 .= CellInterior($plot, $c);
	    }
	}
	
	# Add the age at the end of the last horizontal boundary.
	
	$line1 .= ' ' . sprintf("%-9s", $bound2d[$r][0]);
	
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
    
    my ($plot, $c) = @_;
    
    my $width = $plot->{col_width}[$c] || 10;
    
    my $border = '-' x $width;
    
    return $border . '+';
}

# CellInterior ( plot, c, [label] )
# 
# Return a string indicating a cell interior, with or without a label, for column $c of
# the diagram.

sub CellInterior {
    
    my ($plot, $c, $label) = @_;
    
    my $width = $plot->{col_width}[$c] || 10;
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
    
    my ($plot, $c) = @_;
    
    my $width = $plot->{col_width}[$c] || 10;
    
    my $label = $plot->{header}[$c];
    
    if ( length($label) > $width - 2)
    {
	$label = substr($label, 0, $width - 2);
    }
    
    my $content = sprintf("%-${width}s", " $label");
    
    return $content . '*';
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
