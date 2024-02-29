#!/usr/bin/env perl
# 
# intervals.pl
# 
# Manage the definitions of geologic time intervals in the Paleobiology Database,
# in conjunction with a spreadsheet which holds the master interval definitions.

use strict;

use Getopt::Long qw(:config bundling no_auto_abbrev);

use JSON;
use Encode;
use LWP::UserAgent;
use Carp qw(croak);
use List::Util qw(any);

use feature 'say';
use feature 'fc';

our ($MACROSTRAT_INTERVALS) = "https://macrostrat.org/api/defs/intervals";
our ($MACROSTRAT_TIMESCALES) = "https://macrostrat.org/api/defs/timescales";

our ($UPDATE_INTERVALS) = 0;
our ($UPDATE_INTERVAL_LOOKUP) = 0;
our ($UPDATE_INTERVAL_DATA) = 0;
our ($UPDATE_SCALE_MAP) = 0;
our ($UPDATE_CORRELATIONS) = 0;


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_quiet, $opt_verbose, $opt_url, $opt_format, $opt_file,
    $opt_dbname, $opt_force, $opt_debug, $opt_help);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "url|u=s" => \$opt_url,
	   "format=s" => \$opt_format,
	   "file|f=s" => \$opt_file,
	   "db=s" => \$opt_dbname,
	   "force" => \$opt_force,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

# If --help was specified, display the help message.

if ( $opt_help )
{
    &ShowHelp;
    exit;
}


# Do not buffer when printing to STDOUT.

$|=1;

# The input data and its parsed content are stored in the following globals.
# Yes, I know that's not consistent with best coding practice.

our ($FORMAT, $PARSER, $LINEEND);
our (@FIELD_LIST, %FIELD_MAP);
our (%INTERVAL_NAME, %INTERVAL_NUM, @ALL_INTERVALS);
our (%SCALE_NAME, %SCALE_NUM, %SCALE_INTS, @SCALE_NUMS);
our (%SCALE_SELECT, $DIFF_CONTAINERS);
our (%DIFF_NAME, %DIFF_NUM, @DIFF_MISSING, @DIFF_EXTRA, @ERRORS);

# The following regexes validate ages and colors respectively.

our ($AGE_RE) = qr{^\d[.\d]*$};
our ($COLOR_RE) = qr{^#[0-9A-F]{6}$};

# Allowed interval types.

our (%INTERVAL_TYPE) = (eon => 1, era => 1, period => 1, epoch => 1,
			subepoch => 1, age => 1, subage => 1, zone => 1);

# The following scales are used in generating values for stage_no, epoch_no, etc.

our ($INTERNATIONAL_SCALE) = '1';
our ($TERTIARY_SUBEPOCHS) = '2';

# The next remaining argument specifies a subcommand. If none was specified,
# print out the help message.

my ($CMD, @REST) = @ARGV;

if ( ! $CMD || $CMD eq 'help' )
{
    &ShowHelp;
    exit;
}

# The subcommand 'fetch' fetches the contents of the specified spreadsheet and
# prints it to STDOUT.

elsif ( $CMD eq 'fetch' )
{
    &FetchSheet(@REST);
}

# The subcommand 'check' reads the specified contents and checks one or more
# timescales (or all of them) for consistency.

elsif ( $CMD eq 'check' )
{
    &ReadSheet;
    &CheckScales(@REST);
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
	&DiffPBDB('diff', @REST);
	&ReportErrors;
    }
    
    elsif ( ! $SUBCMD )
    {
	die "You must specify either 'macrostrat' or 'pbdb'\n";
    }
    
    else
    {
	die "Invalid subcommand '$SUBCMD'\n";
    }
}

# The subcommand 'update' updates the PBDB interval tables to match the contents
# of the spreadsheet.

elsif ( $CMD eq 'update' )
{
    shift @REST if $REST[0] eq 'pbdb';
    
    &ReadSheet;
    &DiffPBDB('update', @REST) if @ERRORS == 0 || $opt_force;
    &ReportErrors;
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
	$content = FetchData($opt_url);
    }
    
    elsif ( $cmd_url )
    {
	$content = FetchData($cmd_url);
    }
    
    else
    {
	die "You must specify a URL to fetch\n";
    }
    
    if ( $content )
    {
	binmode(STDOUT, ":utf8");
	print $content;
    }
}


# ReadSheet ( )
# 
# Read an interval spreadsheet in CSV format from the specified URL or filename,
# or from STDIN. Put the data into a structure that can be used for checking and
# updating. Check the data for self consistency, as follows:
# 
# 1. Every interval name must be associated with a unique number, and vice versa.
# 2. Every interval must have exactly one definition, and may be used zero or
#    more times in other scales.
# 

sub ReadSheet {
    
    my ($header, @data);
    
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
    
    @FIELD_LIST = ParseLine($header);
    
    unless ( any { $_ eq 'interval_name' } @FIELD_LIST )
    {
	push @ERRORS, "The column headings are missing from the first row of this data";
	return;
    }
    
    my $line_no = 1;
    
    foreach my $line ( @data )
    {
	my $record = LineToRecord($line, \@FIELD_LIST);
	
	$record->{line_no} = ++$line_no;
	
	my $name = $record->{interval_name};
	my $interval_no = $record->{interval_no};
	my $scale_no = $record->{scale_no};
	my $scale_name = $record->{scale_name};
	my $t_type = $record->{t_type};
	my $b_type = $record->{b_type};
	
	# A row represents a valid interval if the 'interval_name' is not
	# empty and the 'action' is not 'SKIP'. All other rows are ignored.
	
	if ( $name && $record->{action} ne 'SKIP' )
	{
	    push @ALL_INTERVALS, $record;
	}
	
	else
	{
	    next;
	}
	
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
	    
	    next;
	}
	
	unless ( $scale_no && $scale_no =~ /^\d+$/ )
	{
	    if ( $scale_no )
	    {
		push @ERRORS, "at line $line_no, bad scale_no '$scale_no'";
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, missing scale_no";
	    }
	    
	    next;
	}
	
	unless ( $scale_name && $scale_name !~ /^\d+$/ )
	{
	    if ( $scale_name )
	    {
		push @ERRORS, "at line $line_no, bad scale_name '$scale_name'";
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, missing scale_name";
	    }
	    
	    next;
	}
	
	if ( $INTERVAL_NAME{$name} )
	{
	    my $prevline = $INTERVAL_NAME{$name}{line_no};
	    my $prev_no = $INTERVAL_NAME{$name}{interval_no};
	    my $prev_type = $INTERVAL_NAME{$name}{t_type};
	    
	    if ( $interval_no ne '' && $prev_no ne $interval_no )
	    {
		push @ERRORS, "at line $line_no, interval '$name' inconsistent with line $prevline";
		next;
	    }
	    
	    if ( $t_type eq 'use' )
	    {
		push @ERRORS, "at line $line_no, interval '$name', bad b_type '$b_type'"
		    if $b_type && $b_type ne 'use';
	    }
	    
	    elsif ( $prev_type eq 'use' )
	    {
		$INTERVAL_NAME{$name} = $record;
		$INTERVAL_NUM{$interval_no} = $record;
	    }
	    
	    else
	    {
		push @ERRORS, "at line $line_no, interval '$name' already defined at line $prevline";
		next;
	    }
	}
	
	else
	{
	    $INTERVAL_NAME{$name} = $record;
	    
	    if ( $interval_no )
	    {
		if ( $INTERVAL_NUM{$interval_no} )
		{
		    my $prevline = $INTERVAL_NAME{$interval_no}{line_no};
		    push @ERRORS, "at line $line_no, #$interval_no inconsistent with line $prevline";
		    next;
		}
		
		$INTERVAL_NUM{$interval_no} = $record;
	    }
	}
	
	unless ( $SCALE_NUM{$scale_no} )
	{
	    $SCALE_NUM{$scale_no} = { scale_no => $scale_no, 
				      scale_name => $scale_name,
				      line_no => $line_no };
	    
	    push @SCALE_NUMS, $scale_no;
	}
	
	unless ( $SCALE_NAME{$scale_name} )
	{
	    $SCALE_NAME{$scale_name} = { scale_no => $scale_no,
					   scale_name => $scale_name,
					   line_no => $line_no };
	}
	
	if ( $scale_no ne $SCALE_NAME{$scale_name}{scale_no} )
	{
	    my $prevline = $SCALE_NAME{$scale_name}{line_no};
	    push @ERRORS, "at line $line_no, scale_no $scale_no inconsistent with line $prevline";
	}
	
	$SCALE_INTS{$scale_no} ||= [ ];
	
	push $SCALE_INTS{$scale_no}->@*, $record;
    }
}


# ParseLine ( line )
# 
# Parse a line in CSV or TSV format and return a list of column values.

sub ParseLine {
    
    my ($line) = @_;
    
    unless ( $FORMAT )
    {
	my $commas = $line =~ tr/,//;
	my $tabs = $line =~ tr/\t//;
	
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
	
	if ( $line =~ /([\n\r]+)/ )
	{
	    $LINEEND = $1;
	}
	
	else
	{
	    $LINEEND = "\n";
	}
    }
    
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
    
    else
    {
	$line =~ s/[\n\r]+$//s;
	return split /\t/, $line;
    }
}


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
# Parse a line in CSV format and return a hashref. The keys are field names from the
# $fields parameter, and the values are the corresponding column values. The $fields
# parameter must be an arrayref.

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
# Generate an output line from the specified record and list of fields. The
# $fields parameter must be an arrayref.

sub RecordToLine {
    
    my ($record, $fields) = @_;
    
    my @fields = $fields->@*;
    my @values;
    
    foreach my $i ( 0..$#fields )
    {
	my $field = $fields[$i] || next;
	$values[$i] = $record->{$field} || '';
	
	if ( $values[$i] =~ /"/ )
	{
	    $values[$i] =~ s/"/\\"/g;
	    $values[$i] = '"' . $values[$i] . '"';
	}
    }
    
    return GenerateLine(@values);
}


sub GenerateHeader {
    
    return GenerateLine(@FIELD_LIST);
}


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
# Check the specified scale(s) for consistency. If the argument is 'all' then check
# all scales.

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
	    
	    if ( $errors )
	    {
		say STDERR "Timescale '$name' had $errors ERRORS";
	    }
	    
	    else
	    {
		say STDERR "Timescale '$name' passes all checks";
	    }
	}
    }
}


sub CheckOneScale {
    
    my ($scale_no) = @_;
    
    my @errors;
    my $scale_name = $SCALE_NUM{$scale_no}{scale_name} // '';
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	my $line = $i->{line_no};
	my $name = $i->{interval_name};
	my $action = $i->{action};
	
	if ( $action =~ /^REMOVE/ )
	{
	    next;
	}
	
	elsif ( $action =~ /^COALESCE (\w.*)/ )
	{
	    my $coalesce = $1;
	    
	    if ( $coalesce =~ /^(.*?)-(.*)/ )
	    {
		push @errors, "at line $line, unrecognized interval '$1'" unless $INTERVAL_NAME{$1};
		push @errors, "at line $line, unrecognized interval '$2'" unless $INTERVAL_NAME{$2};
	    }
	    
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
	
	elsif ( $action && $action !~ /^RENAME/ )
	{
	    push @errors, "at line $line, invalid action '$action'";
	}
	
	my $top = $i->{t_age} = TopAge($i);
	
	unless ( $top =~ $AGE_RE )
	{
	    push @errors, "at line $line, interval '$name': $top";
	}
	
	my $base = $i->{b_age} = BaseAge($i);
	
	unless ( $base =~ $AGE_RE )
	{
	    push @errors, "at line $line, interval '$name': $base";
	}
	
	if ( my $color = $i->{color} )
	{
	    unless ( $color =~ $COLOR_RE )
	    {
		push @errors, "at line $line, interval '$name': bad color '$color'";
	    }
	}
	
	if ( my $type = $i->{type} )
	{
	    unless ( $INTERVAL_TYPE{$type} )
	    {
		push @errors, "at line $line, interval '$name': bad type '$type'";
	    }
	}
	
	if ( $i->{top} && $i->{top} eq $name && $i->{t_type} ne 'use' )
	{
	    push @ERRORS, "at line $line, top cannot be self-referential";
	}
	
	if ( $i->{base} && $i->{base} eq $name && $i->{t_type} ne 'use' )
	{
	    push @ERRORS, "at line $line, base cannot be self-referential";
	}
    }
    
    my $name = $scale_name || $scale_no;
    
    if ( @errors )
    {
	push @ERRORS, @errors;
	return scalar(@errors);
    }
    
    else
    {
	return;
    }
}


sub TopAge {
    
    my ($interval) = @_;
    
    my ($lookup, $which) = TopAgeRef($interval, { });
    
    if ( ref $lookup )
    {
	my $age = $lookup->{$which};
	my $line = $lookup->{line_no};
	
	if ( $age =~ $AGE_RE )
	{
	    return $age;
	}
	
	else
	{
	    return "bad top age '$age' at line $line";
	}
    }
    
    else
    {
	return $lookup;
    }
}


sub TopAgeRef {
    
    my ($interval, $uniq) = @_;
    
    my $type = $interval->{t_type};
    my $line = $interval->{line_no};
    
    if ( $type eq 'def' )
    {
	return $interval, 'top';
    }
    
    elsif ( $type eq 'top' || $type eq 'base' )
    {
	my $name = $interval->{top};
	my $lookup = $INTERVAL_NAME{$name};
	
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
	
	else
	{
	    return "could not find '$name' at line $line";
	}
    }
    
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
    
    else
    {
	return "bad t_type '$type' at line $line";
    }
}


sub BaseAge {
    
    my ($interval) = @_;
    
    my ($lookup, $which) = BaseAgeRef($interval, { });
    
    if ( ref $lookup )
    {
	my $age = $lookup->{$which};
	my $line = $lookup->{line_no};
	
	if ( $age =~ $AGE_RE )
	{
	    return $age;
	}
	
	else
	{
	    return "bad base age '$age' at line $line";
	}
    }
    
    else
    {
	return $lookup;
    }
}


sub BaseAgeRef {
    
    my ($interval, $uniq) = @_;
    
    my $type = $interval->{b_type};
    my $line = $interval->{line_no};
    
    if ( $type eq 'def' )
    {
	return $interval, 'base';
    }
    
    elsif ( $type eq 'top' || $type eq 'base' )
    {
	my $name = $interval->{base};
	my $lookup = $INTERVAL_NAME{$name};
	
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
	
	else
	{
	    return "could not find '$name' at line $line";
	}
    }	
    
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
    
    else
    {
	return "bad b_type '$type' at line $line";
    }
}


sub IntervalName {
    
    my ($interval_no) = @_;
    
    return $INTERVAL_NUM{$interval_no} && $INTERVAL_NUM{$interval_no}{interval_name};
}


sub DiffMacrostrat {
    
    my ($timescale, @rest) = @_;
    
    if ( @rest )
    {
	die "You can only diff one timescale at a time\n";
    }
    
    unless ( $timescale )
    {
	die "You must specify a timescale\n";
    }
    
    if ( ! $timescale || $timescale eq 'international' )
    {
	$timescale = 'international intervals';
    }
    
    my (%matched_interval, %matched_scale);
    
    my @macro_intervals = FetchMacrostratIntervals($timescale);
    
    foreach my $m ( @macro_intervals )
    {
	my $name = $m->{name};
	
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
	    
	    if ( $interval->{action} eq 'REMOVE' )
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
	next if $i->{action} eq 'REMOVE';
	
	push @DIFF_EXTRA, { $i->%* };
    }
    
    return if @ERRORS;
    
    if ( %DIFF_NAME || @DIFF_MISSING || @DIFF_EXTRA )
    {
	print GenerateHeader();
    }
    
    else
    {
	say STDERR "No differences in timescale '$timescale' between this spreadsheet and Macrostrat";
    }
    
    if ( %DIFF_NAME )
    {
	say "\nDifferences from Macrostrat:\n";
	
	foreach my $i ( @ALL_INTERVALS )
	{
	    my $name = $i->{interval_name};
	    
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
    
    if ( @DIFF_MISSING )
    {
	say "\nMissing from this spreadsheet:\n";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
    
    if ( @DIFF_EXTRA )
    {
	say "\nExtra in this spreadsheet:\n";
	
	foreach my $i ( @DIFF_EXTRA )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
}


sub FetchData {
    
    my ($url, $decode) = @_;
    
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


sub FetchMacrostratIntervals {
    
    my ($timescale) = @_;
    
    my ($url, $data);
    
    if ( $timescale =~ /^\d+$/ )
    {
	$url = "$MACROSTRAT_INTERVALS?timescale_id=$timescale";
    }
    
    else
    {
	$url="$MACROSTRAT_INTERVALS?timescale=$timescale";
    }
    
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


# DiffPBDB ( timescale... )
# 
# Compare the data in the interval spreadsheet with the corresponding interval
# definitions stored in the Paleobiology Database. Print out a list of all
# differences. Compare the specified timescales, or all of them if 'all' is
# given. This function is only available if this program is run in the directory
# which contains the source code for the Paleobiology Database API.

sub DiffPBDB {
    
    my ($CMD, @args) = @_;
    
    # First make sure we have the necessary PBDB API libraries available. We do
    # this at runtime rather than at compile time so that the other functions of
    # this program can be run in environments where they are not available.
    
    use lib './lib';
    require "CoreFunction.pm";
    CoreFunction->import('connectDB');
    require "TableDefs.pm";
    TableDefs->import('%TABLE');
    require "CoreTableDefs.pm";
    
    my $dbh = connectDB("config.yml", $opt_dbname || 'pbdb');
    
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
    
    # Check all selected scales, and compute the top and bottom ages. Do this
    # for the International scale as well, because it is used in the definitions
    # of so many other scales.
    
    foreach my $scale_no ( @SCALE_NUMS )
    {
	if ( $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no} || $scale_no eq $INTERNATIONAL_SCALE )
	{
	    &CheckOneScale($scale_no);
	}
    }
    
    # If any errors were found, abort unless the 'force' option was given.
    
    return if @ERRORS && ! $opt_force;
    
    # If we are diffing all scales, fetch a hash of all the intervals known to
    # the PBDB by interval_no. This allows us to make sure that no interval is
    # being left out.
    
    my $remaining = { };
    
    if ( $SCALE_SELECT{all} )
    {
	$remaining = FetchPBDBNums($dbh);
    }
    
    # If no errors were found in the selected scales, go through the entire set
    # of intervals. Process those which are contained in one of the selected
    # scales.
    
    foreach my $i ( @ALL_INTERVALS )
    {
	my $scale_no = $i->{scale_no};
	
	next unless $SCALE_SELECT{all} || $SCALE_SELECT{$scale_no};
	
	my $interval_no = $i->{interval_no};
	my $line_no = $i->{line_no};
	my $name = $i->{interval_name};
	my $t_age = $i->{t_age};
	my $b_age = $i->{b_age};
	my $type = $i->{type};
	my $color = $i->{color};
	my $reference_no = $i->{reference_no};
	
	&ComputeContainers($i, $scale_no);
	
	if ( my $p = FetchPBDBInterval($dbh, $interval_no, $scale_no) )
	{
	    if ( $type ne $p->{type} )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{type} = $type;
	    }
	    
	    if ( $color ne $p->{color} )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{color} = $color;
	    }
	    
	    if ( $reference_no ne $p->{reference_no} )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{reference_no} = $reference_no;
	    }
	    
	    if ( $name ne $p->{interval_name} )
	    {
		if ( $i->{action} eq 'RENAME' )
		{
		    $DIFF_NUM{$scale_no}{$interval_no}{action} = 'RENAME';
		}
		
		else
		{
		    push @ERRORS, "at line $line_no, '$name' differs from PBDB name " .
			"'$p->{interval_name}'";
		}
	    }
	    
	    if ( $i->{action} =~ /^REMOVE|^COALESCE/ )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{action} = $i->{action};
		
		delete $remaining->{$interval_no};
		next;
	    }
	    
	    if ( $t_age + 0 != $p->{t_age} + 0 )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{t_type} = $i->{t_type};
		$DIFF_NUM{$scale_no}{$interval_no}{top} = $t_age;
	    }
	    
	    if ( $b_age + 0 != $p->{b_age} + 0 )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{b_type} = $i->{b_type};
		$DIFF_NUM{$scale_no}{$interval_no}{base} = $b_age;
	    }
	    
	    if ( $i->{stage} ne IntervalName($p->{stage_no}) )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{stage} = $i->{stage} || 'none';
		$DIFF_CONTAINERS = 1;
	    }
	    
	    if ( $i->{subepoch} ne IntervalName($p->{subepoch_no}) )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{subepoch} = $i->{subepoch} || 'none';
		$DIFF_CONTAINERS = 1;
	    }
	    
	    if ( $i->{epoch} ne IntervalName($p->{epoch_no}) )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{epoch} = $i->{epoch} || 'none';
		$DIFF_CONTAINERS = 1;
	    }
	    
	    if ( $i->{period} ne IntervalName($p->{period_no}) )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{period} = $i->{period} || 'none';
		$DIFF_CONTAINERS = 1;
	    }
	    
	    if ( $scale_no eq $INTERNATIONAL_SCALE &&
		 $i->{parent} ne IntervalName($p->{parent_no}) )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{parent} = $i->{parent} || 'none';
	    }
	    
	    # Remove this interval from the $remaining hash.
	    
	    delete $remaining->{$interval_no};
	}
	
	elsif ( $i->{action} =~ /^REMOVE|^COALESCE/ )
	{
	    my $diff = { interval_no => $interval_no,
			 interval_name => $name,
			 type => $type,
			 color => $color,
			 reference_no => $reference_no,
			 top => $t_age,
			 base => $b_age };
	    
	    $diff->{stage} = $i->{stage} if $i->{stage};
	    $diff->{subepoch} = $i->{subepoch} if $i->{subepoch};
	    $diff->{epoch} = $i->{epoch} if $i->{epoch};
	    $diff->{period} = $i->{period} if $i->{period};
	    $diff->{parent} = $i->{parent} if $i->{parent};
	    
	    push @DIFF_EXTRA, $diff;
	}
    }
    
    # If we are checking all scales, report anything remaining in $remaining.
    
    if ( $remaining->%* )
    {
	foreach my $interval_no ( keys $remaining->%* )
	{
	    push @DIFF_MISSING, { interval_no => $interval_no,
				  interval_name => $remaining->{$interval_no} };
	}
    }
    
    # Abort if any errors were found. They must be fixed before a valid diff can
    # be produced.
    
    return if @ERRORS && ! $opt_force;
    
    # If the command is 'diff', print out a table of differences.
    
    if ( $CMD eq 'diff' )
    {
	&PrintDifferences;
    }
    
    # If the command is 'update', actually update the PBDB.
    
    elsif ( $CMD eq 'update' )
    {
	&ApplyDifferences;
    }
}


# ComputeContainers ( interval, scale_no )
# 
# Compute the containing intervals for the given interval, from the
# International scale and the Tertiary subepochs. For intervals in the
# international scale, compute the immediate parent.

sub ComputeContainers {
    
    my ($i, $scale_no) = @_;
    
    # Run through all the intervals from the international scale, looking for
    # those that contain interval $i.
    
    foreach my $c ( $SCALE_INTS{$INTERNATIONAL_SCALE}->@* )
    {
	last if $c->{t_age} > $i->{b_age};
	
	if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
	{
	    if ( $c->{type} eq 'eon' )
	    {
		if ( $i->{scale_no} eq $INTERNATIONAL_SCALE && $i->{type} eq 'era' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }
	    
	    elsif ( $c->{type} eq 'era' )
	    {
		if ( $i->{scale_no} eq $INTERNATIONAL_SCALE && $i->{type} eq 'period' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }	    
	    
	    elsif ( $c->{type} eq 'period' )
	    {
		$i->{period} = $c->{interval_name};
		
		if ( $i->{scale_no} eq $INTERNATIONAL_SCALE && $i->{type} eq 'epoch' )
		{
		    $i->{parent} = $c->{interval_name};
		}
	    }
	    
	    elsif ( $c->{type} eq 'epoch' )
	    {
		$i->{epoch} = $c->{interval_name};
		
		if ( $i->{scale_no} eq $INTERNATIONAL_SCALE && $i->{type} eq 'age' )
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
    
    # Then run through the Tertiary subepochs.
    
    foreach my $c ( $SCALE_INTS{$TERTIARY_SUBEPOCHS}->@* )
    {
	last if $c->{t_age} > $i->{b_age};
	
	if ( $c->{t_age} <= $i->{t_age} && $c->{b_age} >= $i->{b_age} )
	{
	    if ( $c->{type} eq 'subepoch' )
	    {
		$i->{subepoch} = $c->{interval_name};
	    }
	}
    }
}


# PrintDifferences ( )
# 
# Print out the differences that were computed by &DiffPBDB between the
# spreadsheet and the PBDB interval tables. Print a table of differences to
# STDOUT, in the same format (CSV or TSV) as the input data.

sub PrintDifferences {
    
    # If we have generated any differences in containing intervals, 
    
    if ( %DIFF_NUM || @DIFF_MISSING || @DIFF_EXTRA )
    {
	print GenerateHeader();
    }
    
    else
    {
	say STDERR "No differences between this spreadsheet and the PBDB";
    }
    
    if ( %DIFF_NUM )
    {
	say "\nDifferences from PBDB:\n";
	
	foreach my $i ( @ALL_INTERVALS )
	{
	    my $scale_no = $i->{scale_no};
	    my $interval_no = $i->{interval_no};
	    
	    if ( $DIFF_NUM{$scale_no}{$interval_no} )
	    {
		$DIFF_NUM{$scale_no}{$interval_no}{interval_no} = $interval_no;
		$DIFF_NUM{$scale_no}{$interval_no}{interval_name} = $i->{interval_name};
		$DIFF_NUM{$scale_no}{$interval_no}{scale_no} = $i->{scale_no};
		$DIFF_NUM{$scale_no}{$interval_no}{scale_name} = $i->{scale_name};
		
		print RecordToLine($DIFF_NUM{$scale_no}{$interval_no}, \@FIELD_LIST);
	    }
	}
    }
    
    if ( @DIFF_MISSING )
    {
	say "\nMissing from this spreadsheet:\n";
	
	foreach my $i ( @DIFF_MISSING )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
    
    if ( @DIFF_EXTRA )
    {
	say "\nExtra in this spreadsheet:\n";
	
	foreach my $i ( @DIFF_EXTRA )
	{
	    print RecordToLine($i, \@FIELD_LIST);
	}
    }
}


# ApplyDifferences ( )
# 
# Apply the differences that were computed by &DiffPBDB to the PBDB interval
# tables. The tables 'interval_data', 'scale_map', 'intervals', and
# 'interval_lookup' will be updated to match the spreadsheet. The scale_map
# table is the most difficult. For each updated scale, a new set of scale_map
# records is computed. If the sequence has changed, 

sub ApplyDifferences {
    
    
    
    
    
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
		    late_age as t_age, reference_no, scale_no, parent_no, color,
		    stage_no, subepoch_no, epoch_no, period_no
		FROM $TableDefs::TABLE{INTERVAL_DATA} as i
		    left join interval_lookup using (interval_no)
		    left join $TableDefs::TABLE{SCALE_MAP} as sm 
		        on sm.interval_no = i.interval_no and sm.scale_no = $qs
		WHERE i.interval_no = $qi";
    
    my $result = $dbh->selectrow_hashref($sql, { Slice => { } });
    
    return $result;
}


# FetchPBDBNums ( dbh )
# 
# Return a hashref of PBDB interval numbers and names.

sub FetchPBDBNums {
    
    my ($dbh) = @_;
    
    my $sql = "SELECT interval_no, interval_name FROM $TableDefs::TABLE{INTERVAL_DATA}";
    
    my @ints = $dbh->selectall_array($sql, { Slice => { } });
    
    my %result;
    
    foreach my $i ( @ints )
    {
	$result{$i->{interval_no}} = $i->{interval_name};
    }
    
    return \%result;
}


# FetchPBDBSequences ( dbh )
# 
# Return a hashref of PBDB scale numbers and interval sequences. Each hash value
# will be a list of interval numbers, in the order they appear in the database.

sub FetchPBDBSequences {
    
    my ($dbh) = @_;
    
    my $sql = "SELECT scale_no, interval_no FROM $TableDefs::TABLE{SCALE_MAP}";
    
    my @ints = $dbh->selectall_array($sql, { Slice => { } });
    
    my %sequence;
    
    foreach my $i ( @ints )
    {
	$sequence{$i->{scale_no}} ||= [ ];
	push $sequence{$i->{scale_no}}->@*, $i->{interval_no};
    }
    
    return \%sequence;
}


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
