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
    $opt_debug, $opt_help);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "url|u=s" => \$opt_url,
	   "format=s" => \$opt_format,
	   "file|f=s" => \$opt_file,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

# If --help was specified, display the help message.

if ( $opt_help )
{
    &ShowHelp;
    exit;
}


# The remaining arguments specify the command.

$|=1;

our ($FORMAT, $PARSER, $LINEEND);
our (@FIELD_LIST, %FIELD_MAP, @CONTENT);
our (%INTERVAL_NAME, %INTERVAL_NUM, %SCALE, %SCALE_INTS);
our (%DIFF_NAME, @DIFF_MISSING, @ERRORS);

our ($AGE_RE) = qr{^\d[.\d]*$};
our ($COLOR_RE) = qr{^#[0-9A-F]{6}$};


if ( ! $ARGV[0] || $ARGV[0] eq 'help' )
{
    &ShowHelp;
    exit;
}

elsif ( $ARGV[0] eq 'fetch' )
{
    shift @ARGV;
    &FetchSheet(@ARGV);
}

elsif ( $ARGV[0] eq 'check' )
{
    shift @ARGV;
    &ReadSheet;
    &CheckScales(@ARGV);
    &ReportErrors;
}

elsif ( $ARGV[0] eq 'diff' )
{
    shift @ARGV;
    &ReadSheet;
    
    if ( $ARGV[0] eq 'macrostrat' )
    {
	&DiffMacrostrat(@ARGV);
	&ReportErrors;
    }
    
    elsif ( $ARGV[0] eq 'pbdb' )
    {
	&DiffPaleoBioDB(@ARGV);
	&ReportErrors;
    }
    
    elsif ( ! $ARGV[0] )
    {
	die "You must specify either 'macrostrat' or 'pbdb'\n";
    }
    
    else
    {
	die "Invalid subcommand '$ARGV[0]'\n";
    }
}

elsif ( $ARGV[0] && $ARGV[0] ne 'help' )
{
    die "Invalid command '$ARGV[0]'\n";
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
	($header, @data) = FetchData($opt_url, 1);
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
    
    my $line_no = 1;
    
    foreach my $line ( @data )
    {
	my $record = LineToRecord($line, \@FIELD_LIST);
	
	$record->{line_no} = ++$line_no;
	
	my $name = $record->{interval_name};
	my $interval_no = $record->{interval_no};
	my $scale_no = $record->{scale_no};
	my $t_type = $record->{t_type};
	my $b_type = $record->{b_type};
	
	if ( $name )
	{
	    push @CONTENT, $record;
	}
	
	else
	{
	    push @CONTENT, "";
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
	
	if ( my $other = $record->{other_names} )
	{
	    my @names = split /\s*,\s*/, $other;
	    
	    foreach my $n ( @names )
	    {
		if ( $INTERVAL_NAME{$n} )
		{
		    my $prevline = $INTERVAL_NAME{$n}{line_no};
		    push @ERRORS, "at line $line_no, '$n' inconsistent with line $prevline";
		    next;
		}
		
		else
		{
		    $INTERVAL_NAME{$n} = $record;
		}
	    }
	}
	
	$SCALE{$scale_no} ||= { scale_no => $scale_no, 
				scale_name => $record->{scale_name} };
	
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
    
    my (@scales) = @_;
    
    my $checked = 0;
    
    foreach my $arg ( @scales )
    {
	if ( $SCALE_INTS{$arg} )
	{
	    &CheckOneScale($arg);
	    $checked++;
	}
	
	else
	{
	    my $name = lc $arg;
	    
	    foreach my $s ( values %SCALE )
	    {
		if ( lc $s->{scale_name} eq $name )
		{
		    &CheckOneScale($s->{scale_no});
		    $checked++;
		}
	    }
	}
    }
    
    unless ( $checked )
    {
	die "You must provide a valid scale name\n";
    }
}


sub CheckOneScale {
    
    my ($scale_no) = @_;
    
    my @errors;
    my $scale_name = $SCALE{$scale_no}{scale_name} // '';
    
    foreach my $i ( $SCALE_INTS{$scale_no}->@* )
    {
	my $line = $i->{line_no};
	my $name = $i->{interval_name};
	
	my $top = TopAge($i);
	
	unless ( $top =~ $AGE_RE )
	{
	    push @errors, "at line $line, interval '$name': $top";
	}
	
	my $bottom = BottomAge($i);
	
	unless ( $bottom =~ $AGE_RE )
	{
	    push @errors, "at line $line, interval '$name': $bottom";
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
	    unless ( $type eq 'eon' || $type eq 'era' || $type eq 'period' ||
		     $type eq 'subperiod' || $type eq 'epoch' || 
		     $type eq 'subepoch' || $type eq 'age' || $type eq 'zone' )
	    {
		push @errors, "at line $line, interval '$name': bad type '$type'";
	    }
	}
	
	if ( $i->{top} && $i->{top} eq $name )
	{
	    push @ERRORS, "at line $line, top cannot be self-referential";
	}
	
	if ( $i->{bottom} && $i->{bottom} eq $name )
	{
	    push @ERRORS, "at line $line, bottom cannot be self-referential";
	}
    }
    
    if ( @errors )
    {
	push @ERRORS, @errors;
    }
    
    else
    {
	my $name = $scale_name || $scale_no;
	say STDERR "Timescale '$name' passes the check";
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
    
    elsif ( $type eq 'top' || $type eq 'bottom' )
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
		return BottomAgeRef($lookup, $uniq);
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


sub BottomAge {
    
    my ($interval) = @_;
    
    my ($lookup, $which) = BottomAgeRef($interval, { });
    
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
	    return "bad bottom age '$age' at line $line";
	}
    }
    
    else
    {
	return $lookup;
    }
}


sub BottomAgeRef {
    
    my ($interval, $uniq) = @_;
    
    my $type = $interval->{b_type};
    my $line = $interval->{line_no};
    
    if ( $type eq 'def' )
    {
	return $interval, 'bottom';
    }
    
    elsif ( $type eq 'top' || $type eq 'bottom' )
    {
	my $name = $interval->{bottom};
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
		return BottomAgeRef($lookup, $uniq);
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
	    return BottomAgeRef($lookup, $uniq);
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


sub UpdateFromMacrostrat {
    
    my @macro_intervals;
    
    push @macro_intervals, FetchMacrostratIntervals('international intervals');
    
    my $got_eons = any { $_->{int_type} eq 'eon' } @macro_intervals;
    
    unless ( $got_eons )
    {
	push @macro_intervals, FetchMacrostratIntervals('international eons');
    }
    
    foreach my $m ( @macro_intervals )
    {
	my $name = $m->{name};
	
	if ( my $interval = $INTERVAL_NAME{$name} )
	{
	    my $interval_no = $interval->{interval_no};
	    my $line_no = $interval->{line_no};
	    my ($t_interval, $t_which) = TopAgeRef($interval);
	    my ($b_interval, $b_which) = BottomAgeRef($interval);
	    my $type = $interval->{type};
	    my $color = $interval->{color};
	    
	    my $diff = { interval_no => $interval_no,
			 interval_name => $name };
	    
	    if ( $m->{type} && $type ne $m->{type} )
	    {
		$DIFF_NAME{$name}{type} = $m->{type};
	    }
	    
	    if ( $m->{color} && $color ne $m->{color} )
	    {
		$DIFF_NAME{$name}{color} = $m->{color};
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
		    $DIFF_NAME{$name}{bottom} = $m->{b_age};
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
			 bottom => $m->{b_age} };
	    
	    push @DIFF_MISSING, $diff;
	}
    }
    
    return if @ERRORS;
    
    if ( %DIFF_NAME || @DIFF_MISSING )
    {
	print GenerateHeader();
    }
    
    else
    {
	say STDERR "No differences in the international intervals between this spreadsheet and Macrostrat";
    }
    
    if ( %DIFF_NAME )
    {
	say "\nDifferences from Macrostrat:\n";
	
	foreach my $i ( @CONTENT )
	{
	    next unless ref $i;
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
}


sub FetchData {
    
    my ($url, $decode) = @_;
    
    my $format = 'csv';
    
    if ( $opt_format && $opt_format =~ /^(csv|tsv|xlsx)$/ )
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
