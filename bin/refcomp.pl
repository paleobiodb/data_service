#!/usr/bin/env perl
# 
# refcomp.pl
# 
# Compare different expressions for differentiating good reference matches from bad.

use strict;

use feature 'unicode_strings';

use open ':std', ':encoding(UTF-8)';

use lib 'lib', '../lib';

use Term::ReadLine;
use Try::Tiny;
use Carp qw(carp croak);
use Storable;
use Getopt::Long;
use Encode;
use JSON;
use Data::Dumper;
use Cwd;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use ReferenceSources;
use ReferenceMatch qw(ref_similarity split_authorlist parse_authorname get_authorname);



# Initial declarations
# --------------------

sub print_msg ($);
sub print_msgs (@);
sub print_line (@);
sub print_table (@);

my $CMD_COUNT = 1;
my ($DONE, $INTERRUPT, $STOP, $WAITING);

our (%VARMAP, %MODMAP, %OPMAP);

# Handle an interrupt according to how we are running. If the debugger is active, set the $STOP
# flag to drop us into the debugger at an appropriate part of the code. Otherwise, set the $DONE
# flag which will cause the program to terminate gracefully.

our ($SIG_ROUTINE) = $SIG{INT};

$SIG{INT} = \&do_interrupt;

# Configure the terminal I/O. If the debugger is running, add the UTF8 encoding layer to its
# output. Why doesn't "use open :std" do this???

if ( $DB::OUT )
{
    binmode($DB::OUT, ':encoding(UTF8)');
}

STDOUT->autoflush(1);

# Look for the configuration file in the current directory, and higher in the directory tree. If
# we can't find it, die because the configuration file contains the necessary information for
# establishing a database connection. The state file will be stored in the same directory as the
# configuration file.

my $base_dir = getcwd();
my $config_file = "config.yml";
my $state_file = ".refcomp-state";

while ( ! -e "$base_dir/$config_file" && $base_dir =~ qr{ (.+) / [^/]+ $ }xs )
{
    $base_dir = $1;
}

unless ( -e "$base_dir/$config_file" )
{
    print_msg "ERROR: could not find $config_file";
    exit(2);
}


# Initialize Term::ReadLine, and then read in the saved state if any. The load_state routine
# restores the values of the following variables to the state when they were last saved, which
# will usually be on the termination of the previous execution of this program. This
# includes restoring the command history.

our (%DEBUG, %VARS, @ACTIVE, @INACTIVE, @MAINHISTORY, @DBHISTORY);
our $HSEL = 'main';
our %SETTINGS = ( source => 'all', color => 'off', history => 50 );
our %SELECTION = ( SLOT => undef, EXPR => undef, SQL => undef,
		   COUNT => undef, COUNT_POS => undef, COUNT_NEG => undef,
		   MATCHES => [ ], MATCH_TYPE => undef, MATCH_INDEX => undef,
		   RESTRICTION => undef );

load_state("$base_dir/$state_file");

our ($TERM) = Term::ReadLine->new('Refcomp');

$TERM->enableUTF8 if $TERM->isa('Term::ReadLine::Gnu');

$TERM->SetHistory(@MAINHISTORY);


# Database connection
# -------------------

# Get a database handle, using the connection information stored in the configuration file.

my $dbh = connectDB("$base_dir/$config_file", "pbdb");

# Verify to the user the database that we are checking.

if ( $dbh->{Name} =~ /database=([^;]+)/ )
{
    print_msg "Connected to database: $1";
}

else
{
    print_msg "Using connect string: $dbh->{Name}";
}


# Main loop
# ---------

# Create a new ReferenceSources object with which we can make queries and interact with the
# REFERENCE_SOURCES table. The reference source defaults to crossref, unless overridden by an option.

my $source = $SETTINGS{source};
$source = 'all' unless $source eq 'crossref' || $source eq 'xdd';

my $rs = ReferenceSources->new($dbh, $source, { debug => $DEBUG{sql} });

my %COMMAND = ( debug => \&do_debug, set => \&do_set, show => \&do_show, test => \&do_test,
		select => \&do_select, clear => \&do_clear, help => \&do_help, h => \&do_help,
		list => \&do_list, l => \&do_list,
		add => \&do_add, delete => \&do_delete, test => \&do_test, score => \&do_score,
		stop => \&do_stop, quit => \&do_quit, q => \&do_quit,
		a => \&do_match, r => \&do_match, u => \&do_match,
		n => \&do_match, p => \&do_match, s => \&do_match,
		'^' => \&do_match, '$' => \&do_match, '.' => \&do_match);

# If we have a selection, print out the current match.

if ( $SELECTION{MATCHES}->@* )
{
    my $i = $SELECTION{MATCH_INDEX};

    if ( $i < $SELECTION{MATCHES}->@* )
    {
	display_match($SELECTION{MATCHES}[$i]);
    }

    else
    {
	print_msg "End of current match list.";
    }
}

# Main loop starts here

eval {
    while ( !$DONE )
    {
	my $prompt = "Refcomp";

	if ( my $count = $SELECTION{MATCHES}->@* )
	{
	    my $i = $SELECTION{MATCH_INDEX};
	    
	    if ( $i < $SELECTION{MATCHES}->@* )
	    {
		$prompt .= " [match $i of $count]";
	    }
	    
	    else
	    {
		$prompt .= " [end of $count matches]";
	    }
	}

	$prompt .= ' > ';
	
	&mainhist;
	
	$WAITING = 1;
	$INTERRUPT = undef;
	$STOP = undef;
	
	my $input = $TERM->readline($prompt);
	
	last unless $INTERRUPT || defined $input;
	
	$WAITING = undef;
	$INTERRUPT = undef;
	
	try {
	    handle_command($input) if $input =~ /\S/;
	}
	    
	catch {
	    print_msg $_;
	};
    }
};

save_state("$base_dir/$state_file");
print_msg "Done.";
exit;


# handle_command ( input_line )
#
# Handle a command entered on the terminal.

sub handle_command {

    my ($input) = @_;
    
    unless ( $input =~ qr{ ^ [!]? ([\w\^\$\.]+) (?: \s+ (.*) )? }xs )
    {
	print_msg "ERROR: cannot interpret '$input'";
	return;
    }
    
    my $command = $1;
    my $rest = $2;
    
    $STOP = 1 if $input =~ /^!/;
    
    # If the command is one we recognize, execute it.
    
    if ( ref $COMMAND{$command} eq 'CODE' )
    {
	&{$COMMAND{$command}}($command, $rest);
    }

    # If it is a decimal number, execute &do_match.
    
    elsif ( $command =~ /^\d+$/ )
    {
	do_match($command, $rest);
    }
    
    else
    {
	print_msg "ERROR: unknown command '$command'";
	return;
    }
}


# do_debug ( argstring )
#
# Execute the 'debug' command, which sets or clears debugging flags as specified by
# $argstring. Currently, the valid arguments are 'sql' and 'nosql', which set and clear the
# 'sql' debug flag respectively.

sub do_debug {

    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    if ( $argstring =~ qr{ ^ (no)? (sql|scoring|display|test) \s* (off)? \s* $ }xsi )
    {
	my $flag = lc $2;
	
	if ( $1 || $3 )
	{
	    $DEBUG{$flag} = undef;
	    $rs->set_debug(undef) if $flag eq 'sql' && $rs;
	    print_msg "  debug $flag is off";
	}
	
	else
	{
	    $DEBUG{$flag} = 1;
	    $rs->set_debug(1) if $flag eq 'sql' && $rs;
	    print_msg "  debug $flag is on";
	}
    }
    
    else
    {
	print_msg "Unknown debug mode '$argstring'";
    }
}


# do_set ( argstring )
#
# Execute the 'set' command, which changes the value of a setting as specified by
# $argstring. Currently, the valid settings are:
# 
# source:	xdd, crossref, all
# color:        on, off
# history:      maximum number of history lines to save

sub do_set {
    
    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    if ( $argstring =~ qr{ ^ (\w+) \s+ (.+) }xs )
    {
	my $setting = $1;
	my $value = $2;
	
	if ( $setting eq 'source' && $value =~ qr{ ^ (xdd|crossref|all) \s* $ }xsi )
	{
	    $SETTINGS{source} = lc $1;
	    $rs->set_source($SETTINGS{source});
	}
	
	elsif ( $setting eq 'source' )
	{
	    print_msg "Unknown source '$value'";
	}

	elsif ( $setting eq 'color' && $value =~ qr{ ^ (1|yes|on) $ }xsi )
	{
	    $SETTINGS{color} = 'on';
	    print_msg "color is on";
	}
	
	elsif ( $setting eq 'color' && $value =~ qr{ ^ (0|no|off) $ }xsi )
	{
	    $SETTINGS{color} = 'off';
	    print_msg "color is off";
	}
	
	elsif ( $setting eq 'history' && $value =~ qr{ ^ \d+ $ }xs )
	{
	    $SETTINGS{history} = $value;
	    print_msg "history set to $value";
	}
	
	elsif ( $setting eq 'color' || $setting eq 'history' )
	{
	    print_msg "Bad value '$value'";
	}
	
	else
	{
	    print_msg "Unknown setting '$setting'";
	}
    }

    else
    {
	print_msg "Bad arguments '$argstring'";
    }
}


# do_show ( argstring )
#
# Execute the 'show' command, showing whatever information is specified by $argstring.

sub do_show {

    my ($command, $argstring) = @_;
    
    $argstring =~ s/\s+$//;
    
    $DB::single = 1 if $STOP;
    
    # 'show debug' shows the values of all debugging flags.
    
    if ( $argstring =~ qr{ ^ debug $ }xsi )
    {
	my @lines = map { sprintf("debug \%-10s\%s", "$_:", $DEBUG{$_} ? 'ON' : 'off') }
	    qw(sql scoring display test);

	print_msgs @lines;
    }
    
    # 'show source' and 'show color' show the values of those settings.
    
    elsif ( $argstring =~ qr{ ^ (source|color|history) $ }xsi )
    {
	my $value = $SETTINGS{$1};
	print_msg "  $1: $value";
    }
    
    # 'show selection' shows the currently selected expression and counts.

    elsif ( $argstring =~ qr{ ^ (?: sel | selection ) }xsi )
    {
	do_show_selection();
    }
    
    # 'show expr' shows the table of active expressions. If 'sql' is appended, the expanded SQL
    # for each expression is included.
    
    elsif ( $argstring =~ qr{ ^ (?: expr | exprs | expressions ) ( \s+ sql )? $ }xsi )
    {
	do_show_expressions($1);
    }
    
    # 'show vars' shows the table of active variable values.
    
    elsif ( $argstring =~ qr{ ^ vars $ }xsi )
    {
	do_show_vars();
    }
    
    # 'show' with no argument is ignored.

    elsif ( $argstring =~ /\S/ )
    {
	print_msg "Unknown argument: '$argstring'";
    }
}


sub do_show_selection {
    
    my @lines;
    
    if ( $SELECTION{SLOT} && $SELECTION{EXPR} )
    {
	push @lines, "Active expression: E$SELECTION{SLOT} = $SELECTION{EXPR}";
    }
    
    elsif ( $SELECTION{EXPR} )
    {
	push @lines, "Active expression: $SELECTION{EXPR}";
    }
    
    else
    {
	print_msg "No expression is selected.";
	return;
    }
    
    push @lines, "Compiled SQL:      $SELECTION{SQL}" if defined $SELECTION{SQL};
    push @lines, "Matching scores:   $SELECTION{COUNT}" if defined $SELECTION{COUNT};
    push @lines, "Positive matches:  $SELECTION{COUNT_POS}" if defined $SELECTION{COUNT_POS};
    push @lines, "Negative matches:  $SELECTION{COUNT_NEG}" if defined $SELECTION{COUNT_NEG};
    
    my $match_count = ref $SELECTION{MATCHES} eq 'ARRAY' ? scalar(@{$SELECTION{MATCHES}}) : 0;
    
    if ( $match_count && $SELECTION{MATCH_TYPE} eq 'p' )
    {
	push @lines, "Cached records:    $match_count positive";
    }

    elsif ( $match_count && $SELECTION{MATCH_TYPE} eq 'n' )
    {
	push @lines, "Cached records:    $match_count negative";
    }

    elsif ( $match_count && $SELECTION{MATCH_TYPE} eq 'u' )
    {
	push @lines, "Cached records:    $match_count unassigned";
    }

    elsif ( $match_count && $SELECTION{MATCH_TYPE} eq 'a' )
    {
	push @lines, "Cached records:    $match_count all";
    }

    elsif ( $SELECTION{MATCH_TYPE} eq 'r' )
    {
	push @lines, "Match restriction: $SELECTION{RESTRICTION}";
	push @lines, "Cached records:    $match_count";
    }
    
    print_msgs @lines;
}


sub do_show_expressions {

    my ($show_sql) = @_;
    
    my $expr_width = $show_sql ? 40 : 60;
    my $sql_width = 60;
    
    unless ( @ACTIVE )
    {
	print_msg "Expression table is empty.";
	return;
    }
    
    my @header = ('Slot', 'Name', 'Expression');
    push @header, 'SQL' if $show_sql;
    
    my ($has_name, @rows);
    
    # Run through the active expression table, adding each entry in turn to @rows.
    
    foreach my $i ( 0..$#ACTIVE )
    {
	my $slot = $i + 1;
	my $name = $ACTIVE[$i]{name} || '';
	$has_name = 1 if $name;
	my $expr = $ACTIVE[$i]{expr} || '<empty>';
	my $sql = $ACTIVE[$i]{sql} || '<empty>';
	
	# If either $expr or $sql exceeds the width limit, break at spaces and add additional
	# rows.
	
	while ( length($expr) > $expr_width || ($show_sql && length($sql) > $sql_width) )
	{
	    my $expr_1 = '';
	    my $sql_1 = '';

	    if ( length($expr) > $expr_width )
	    {
		for ( my $i = $expr_width - 1; $i >= $expr_width - 20; $i-- )
		{
		    if ( substr($expr, $i, 1) eq ' ' )
		    {
			$expr_1 = substr($expr, 0, $i);
			$expr = substr($expr, $i+1);
		    }
		}
	    }

	    else
	    {
		$expr_1 = $expr;
		$expr = '';
	    }
	    
	    if ( $show_sql && length($sql) > $sql_width )
	    {
		for ( my $i = $sql_width - 1; $i >= $sql_width - 20; $i-- )
		{
		    if ( substr($sql, $i, 1) eq ' ' )
		    {
			$sql_1 = substr($sql, 0, $i);
			$sql = substr($sql, $i+1);
		    }
		}
	    }

	    else
	    {
		$sql_1 = $sql;
		$sql = '';
	    }
	    
	    my @row_1 = ($slot, $name, $expr_1);
	    push @row_1, $sql_1 if $show_sql;

	    push @rows, \@row_1;
	    
	    $slot = '';
	    $name = '';
	}
	
	my @row = ($slot, $name, $expr);
	push @row, $sql if $show_sql;
	
	push @rows, \@row;
    }
    
    unless ( $has_name )
    {
	splice(@header, 1, 1);
	
	foreach my $r ( @rows )
	{
	    splice(@$r, 1, 1);
	}
    }
    
    print_line "";
    print_table \@header, @rows;
    print_line "";
}


sub do_show_vars {
    
    my @rows;
    
    foreach my $v ( sort keys %VARS )
    {
	if ( defined $VARS{$v} && $VARS{$v} ne '' )
	{
	    push @rows, [$v, $VARS{$v}];
	}
    }
    
    if ( @rows )
    {
	print_line "";
	print_table ['Variable', 'Value'], @rows;
	print_line "";
    }
    
    else
    {
	print_msg "Variables table is empty.";
    }
}
    

# do_select ( argstring )
#
# Carry out the 'select' command: evaluate an abbreviated SQL expression and print out the number
# of references that match it. Also sets the %SELECTION variables, to provide a basis for
# subsequent commands such as 'list', 'add', etc.

sub do_select {

    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    # If the expression is empty, there is nothing to add.

    unless ( defined $argstring && ($argstring =~ /[A-Za-z]/ || $argstring eq '.' ) )
    {
	print_msg "You must specify an expression.";
	return;
    }
    
    # If the expression is a decimal number optionally preceded by the letter 'E' for 'expression',
    # use the expression in the corresponding slot of the active expressions table.

    my ($slot, $label, $expr, $sql, $error);
    
    if ( $argstring =~ /^E?(\d+)$/ )
    {
	my $index = $1;
	
	if ( $index && $ACTIVE[$index - 1]{expr} )
	{
	    $slot = $index;
	    $expr = $ACTIVE[$slot - 1]{expr};
	    $sql = $ACTIVE[$slot - 1]{sql};
	    set_selection($slot, $expr, $sql);
	}
	
	else
	{
	    set_selection();
	    print_msg "Invalid entry E$index.";
	    return;
	}
    }
    
    # If the expression starts with a single ' optionally preceded by the letter 'E', look for a
    # slot whose name matches the rest of the argstring. The pattern may contain the wildcard *.
    
    elsif ( $argstring =~ /^E?['](.*)/ )
    {
	if ( my $found = search_active($1) )
	{
	    $slot = $found;
	    $expr = $ACTIVE[$found - 1]{expr};
	    $sql = $ACTIVE[$found - 1]{sql};
	    set_selection($slot, $expr, $sql);
	}
	
	else
	{
	    set_selection();
	    print_msg "Nothing matched.";
	    return;
	}
    }
    
    # If the expression is '.', use the current slot if any.
    
    elsif ( $argstring eq '.' )
    {
	if ( $SELECTION{SLOT} && $ACTIVE[$SELECTION{SLOT} - 1]{expr} )
	{
	    set_selection($SELECTION{SLOT}, $ACTIVE[$SELECTION{SLOT} - 1]{expr},
			  $ACTIVE[$SELECTION{SLOT} - 1]{sql});
	}
	
	elsif ( $SELECTION{SLOT} )
	{
	    set_selection();
	    print_msg "Current slot has an empty expression.";
	    return;
	}

	else
	{
	    print_msg "Nothing selected.";
	    return;
	}
    }

    # Otherwise, we use the argstring as the expression. In this case, we clear the selection.
    
    else
    {
	# Remove leading and trailing spaces.
	
	$expr = $argstring;
	$expr =~ s/^\s+//;
	$expr =~ s/\s+$//;
	
	set_selection();
    }
    
    # Generate a label (if this expression is stored in an active slot) and interpret the
    # expression to generate an SQL clause. If the result is undefined, that means an error
    # occurred. Clear the selection except for the slot number if any (which may be needed for a
    # delete operation) and return.
    
    ($sql, $error) = generate_sql($expr);
    
    # If there is an error, print it and return.
    
    if ( $error )
    {
	print_msg "$label$error";
	set_selection($SELECTION{SLOT});
	return;
    }
    
    # Otherwise, print the SQL clause and go on. If the debug flag 'sql' is set, don't print
    # anything because the statement that queries for the counts will be printed below and it will
    # contain the same information.
    
    else
    {
	print_msg "$label$sql" unless $DEBUG{sql};
    }
    
    # If the expression is valid, check to see if there are any uninterpreted variables in the
    # generated SQL. If so, ask the user to specify values. Each value will be filled in to the
    # %VARS table. If the user declines to enter a value for some variable, return without doing
    # anything further.
    
    if ( $sql =~ qr{ \$ \w+ }xs )
    {
	$sql = fill_variables($sql);

	unless ( defined $sql )
	{
	    $SELECTION{SQL} = undef;
	    return;
	}
    }
    
    # If we get here, then provided expand_sql produced a correct result we have a valid SQL
    # expression. So use the expression to query the database. This verifies that it is in fact a
    # valid SQL expression, and also returns the number of matches.
    
    my ($match_total, $match_pos, $match_neg);
    
    eval {
	($match_total, $match_pos, $match_neg) = $rs->count_matching_scores($sql);
    };
    
    # If an error was thrown, the most likely reason is that the expression was not in fact
    # valid. Print the error message, clear $SELECTION{EXPR} and $SELECTION{SQL}, and return. We do not
    # clear $SELECTION{SLOT}, since it may be needed for a subsequent 'delete' operation. But if the
    # error indicates that the server has gone away, reconnect to the database and retry the
    # operation.
    
    if ( $@ =~ /server has gone away/i )
    {
	print_msg "Server has gone away, reconnecting...";
	
	my $dbh = connectDB("$base_dir/$config_file", "pbdb");
	
	$rs->set_dbh($dbh);
	
	eval {
	    ($match_total, $match_pos, $match_neg) = $rs->count_matching_scores($sql);
	};
    }
    
    if ( $@ )
    {
	print_msg "$label$@";
	set_selection($SELECTION{SLOT});
	return;
    }
    
    # Otherwise, store the selection and counts, and print out the counts.
    
    set_selection($SELECTION{SLOT}, $expr, $sql);
    set_selection_counts($match_total, $match_pos, $match_neg);
    
    my @msgs = "  $label$match_total matching scores";
    push @msgs, "  $match_pos known positives" if $match_pos > 0;
    push @msgs, "  $match_neg known negatives" if $match_neg > 0;
    
    print_msgs @msgs;
}


sub generate_label {

    my ($slot) = @_;
    
    if ( $slot )
    {
	my $label = "E$slot";
	
	if ( my $name = $ACTIVE[$slot - 1]{name} )
	{
	    $label .= " [$name]";
	}
	
	$label .= ': ';

	return $label;
    }
    
    else
    {
	return "";
    }
}


sub generate_sql {
    
    my ($expr) = @_;
    
    # Interpret the given expression. If there is a syntax error, return it as the second return
    # element.
    
    my $sql;
    
    eval {
	$sql = expand_sql($expr);
    };
    
    if ( $@ )
    {
	return undef, $@;
    }

    # If the generated SQL clause is empty, return an error message in this case as well.

    elsif ( $sql eq '' || $sql eq '()' )
    {
	return undef, "Empty expression.";
    }
    
    # Otherwise, return the generated SQL clause.
    
    else
    {
	return $sql;
    }
}


sub fill_variables {
    
    my ($sql) = @_;
    
    my (@varexpr) = ( $sql =~ qr{ \$ (\w+) }xs );
    
    foreach my $name ( @varexpr )
    {
	unless ( $VARS{$name} )
	{
	    my $val = ask_value($name, 'posint');
	    
	    if ( defined $val && $val ne '' )
	    {
		$VARS{$name} = $val;
	    }
	    
	    else
	    {
		return;
	    }
	}
    }
    
    $sql =~ s/\$(\w+)/$VARS{$1}/ge;
    
    return $sql;
}


# do_clear ( )
#
# Carry out the 'clear' command. This clears the selection completely.

sub do_clear {

    $DB::single = 1 if $STOP;
    
    set_selection();
    print_msg "Selection cleared.";
}


sub set_selection {

    ($SELECTION{SLOT}, $SELECTION{EXPR}, $SELECTION{SQL}) = @_;
    
    $SELECTION{PREVIOUS_MATCHES} = $SELECTION{MATCHES}
	if ref $SELECTION{MATCHES} eq 'ARRAY' && $SELECTION{MATCHES}->@*;
    
    $SELECTION{COUNT} = undef;
    $SELECTION{COUNT_POS} = undef;
    $SELECTION{COUNT_NEG} = undef;
    $SELECTION{MATCHES} = [ ];
    $SELECTION{MATCH_TYPE} = undef;
    $SELECTION{MATCH_INDEX} = undef;
    $SELECTION{RESTRICTION} = undef;
    $SELECTION{OFFSET_TYPE} = undef;
    $SELECTION{OFFSET} = undef;
}


sub set_selection_counts {
    
    ($SELECTION{COUNT}, $SELECTION{COUNT_POS}, $SELECTION{COUNT_NEG}) = @_;
    
    $SELECTION{PREVIOUS_MATCHES} = $SELECTION{MATCHES}
	if ref $SELECTION{MATCHES} eq 'ARRAY' && $SELECTION{MATCHES}->@*;
    
    $SELECTION{MATCHES} = [ ];
    $SELECTION{MATCH_TYPE} = undef;
    $SELECTION{MATCH_INDEX} = undef;
    $SELECTION{RESTRICTION} = undef;
    $SELECTION{OFFSET_TYPE} = undef;
    $SELECTION{OFFSET} = undef;
}


sub set_selection_matches {

    my @new_matches;
    
    ($SELECTION{MATCH_TYPE}, $SELECTION{MATCH_INDEX}, @new_matches) = @_;
    
    $SELECTION{PREVIOUS_MATCHES} = $SELECTION{MATCHES}
	if ref $SELECTION{MATCHES} eq 'ARRAY' && $SELECTION{MATCHES}->@*;
    
    $SELECTION{MATCHES} = \@new_matches;
    
    $SELECTION{RESTRICTION} = undef;
}


sub set_selection_restriction {

    ($SELECTION{RESTRICTION}) = @_;
}


sub set_selection_offset {

    ($SELECTION{OFFSET_TYPE}, $SELECTION{OFFSET}) = @_;
}


sub swap_selection_matches {
    
    if ( ref $SELECTION{PREVIOUS_MATCHES} eq 'ARRAY' &&
	 $SELECTION{PREVIOUS_MATCHES}->@* )
    {
	if ( ref $SELECTION{MATCHES} eq 'ARRAY' && $SELECTION{MATCHES}->@* )
	{
	    my $temp = $SELECTION{PREVIOUS_MATCHES};
	    $SELECTION{PREVIOUS_MATCHES} = $SELECTION{MATCHES};
	    $SELECTION{MATCHES} = $temp;
	}

	else
	{
	    $SELECTION{MATCHES}->@* = $SELECTION{PREVIOUS_MATCHES}->@*;
	}

	$SELECTION{MATCH_INDEX} = 0;
    }
    
    else
    {
	print_msg "No previous matches found.";
    }
}


sub search_active {
    
    my ($pattern) = @_;
    
    $pattern =~ s/[\s_]+/\s+/g;
    $pattern =~ s/\*+/.*/g;
    
    my $re = qr{$pattern};
    
    foreach my $i ( 0..$#ACTIVE )
    {
	if ( $ACTIVE[$i]{name} =~ $re )
	{
	    return $i + 1;
	}
    }

    return;
}


# do_add ( name )
#
# Add the current expression to the ACTIVE table with the given name. If we already have a
# selected slot, change the name.

sub do_add {
    
    my ($command, $name) = @_;
    
    $DB::single = 1 if $STOP;
    
    # If a name was given and we have a current slot, rename the current slot and/or replace the expression.
    
    if ( $name && $SELECTION{SLOT} )
    {
	# Remove leading and trailing spaces.
	
	$name =~ s/^\s+//;
	$name =~ s/\s+$//;
	
	# If the current slot already has a name, ask the user.
	
	if ( $ACTIVE[$SELECTION{SLOT} - 1]{name} )
	{
	    if ( answer_yorn("Rename E$SELECTION{SLOT} to '$name'?") )
	    {
		$ACTIVE[$SELECTION{SLOT} - 1]{name} = $name;
	    }
	}
	
	# Otherwise, if the current slot has an expression, just add the name.
	
	elsif ( $ACTIVE[$SELECTION{SLOT} - 1]{expr} )
	{
	    $ACTIVE[$SELECTION{SLOT} - 1]{name} = $name;
	}
	
	# If the current slot has an empty expression, it is not valid.
	
	else
	{
	    print_msg "The selected slot has an empty expression.";
	    return;
	}
	
	# If $SELECTION{EXPR} is non-empty and is different from the expression associated with the
	# active slot, ask the user if they wish to update it.
	
	if ( $SELECTION{EXPR} && $SELECTION{EXPR} ne $ACTIVE[$SELECTION{SLOT} - 1]{expr} )
	{
	    if ( answer_yorn("Change E$SELECTION{SLOT} to \"$SELECTION{EXPR}\"?") )
	    {
		$ACTIVE[$SELECTION{SLOT} - 1]{expr} = $SELECTION{EXPR};
	    }
	}

	return;
    }
    
    # If no name was given and we have a current slot, check the expression and possibly change it.
    
    elsif ( $SELECTION{SLOT} )
    {
	if ( $SELECTION{EXPR} && $SELECTION{EXPR} ne $ACTIVE[$SELECTION{SLOT} - 1]{expr} )
	{
	    if ( answer_yorn("Change E$SELECTION{SLOT} to \"$SELECTION{EXPR}\"?") )
	    {
		$ACTIVE[$SELECTION{SLOT} - 1]{expr} = $SELECTION{EXPR};
	    }
	}
	
	return;
    }
    
    # If we don't have a current slot, add the current expression (if there is one) to @ACTIVE in
    # a new slot.
    
    elsif ( $SELECTION{EXPR} )
    {
	my $new = { expr => $SELECTION{EXPR}, sql => $SELECTION{SQL} };
	
	# If a name was given, remove leading and trailing spaces and include it.
	
	if ( $name )
	{
	    $name =~ s/^\s+//;
	    $name =~ s/\s+$//;
	    
	    $new->{name} = $name;
	}
	
	push @ACTIVE, $new;
	
	my $index = scalar(@ACTIVE);
	my $label = $new->{name} ? " [$new->{name}]" : '';
	
	print_msg "Added E$index$label: $SELECTION{EXPR}";
	return;
    }
    
    else
    {
	print_msg "Nothing to add.";
	return;
    }
}


sub do_delete {
    
    my ($command, $argstring) = @_;
    
    return unless defined $argstring && $argstring ne '';
    
    $argstring =~ s/\s+//;

    my $to_delete = undef;
    
    $DB::single = 1 if $STOP;
    
    if ( $argstring eq '.' )
    {
	if ( $SELECTION{SLOT} && $SELECTION{SLOT} > 0 && $SELECTION{SLOT} <= scalar(@ACTIVE) )
	{
	    $to_delete = $SELECTION{SLOT};
	}
	
	else
	{
	    print_msg "Nothing to delete.";
	    $SELECTION{SLOT} = undef;
	    $SELECTION{EXPR} = undef;
	    $SELECTION{SQL} = undef;
	    return;
	}
    }
    
    elsif ( $argstring =~ /^E?(\d+)$/ )
    {
	if ( $1 && $1 > 0 && $1 <= scalar(@ACTIVE) )
	{
	    $to_delete = $1;
	}

	else
	{
	    print_msg "Nothing to delete.";
	    return;
	}
    }
    
    # If the expression starts with a single ', look for a slot whose name matches the rest of the
    # argstring. The pattern may contain the wildcard *.
    
    elsif ( $argstring =~ /^['](.*)/ )
    {
	if ( my $found = search_active($1) )
	{
	    $to_delete = $found;
	}
	
	else
	{
	    print_msg "Nothing matched.";
	    return;
	}
    }
    
    else
    {
	print_msg "Invalid selection.";
	return;
    }
    
    # If we identified a slot to delete, delete it now.
    
    if ( $to_delete )
    {
	my $name = $ACTIVE[$to_delete-1]{name};
	splice @ACTIVE, $to_delete-1, 1;
	my $label = '';
	$label = " [$name]" if $name;
	print_msg "Deleted E$to_delete$label";
    }

    else
    {
	print_msg "Nothing to delete.";
	return;
    }
    
    return;
}


# do_list ( argstring )
#
# Carry out the 'list' operation, listing score records that match the current selection
# expression. Depending on the arguments and the current position in the list, either continue
# where the previous 'list' operation left off, or fetch another batch of records and start
# listing them, or start from the beginning of the list of positive matches, negative matches,
# unassigned matches, or all matches.

sub do_list {
    
    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    # Unless we have a current selection, let the user know they have to select something first
    # unless we were given an argument that starts with @ or %.
    
    unless ( $SELECTION{EXPR} || $argstring =~ /^[@%]/ )
    {
	print_msg "Nothing selected.";
	return;
    }
    
    # Now parse the argument and determine what to list.
    
    my $record_expr;
    
    # If no argument was given and we have reached the end of the current batch of records, empty
    # it so that another batch of matching records will be fetched. They will be fetched using the
    # current match type unless that is overridden below.
    
    if ( ! defined $argstring || $argstring eq '' )
    {
	if ( ref $SELECTION{MATCHES} eq 'ARRAY' &&
	     $SELECTION{MATCH_INDEX} == $SELECTION{MATCHES}->@* )
	{
	    set_selection_matches($SELECTION{MATCH_TYPE}, 0);
	}

	elsif ( ref $SELECTION{MATCHES} ne 'ARRAY' || ! $SELECTION{MATCHES}->@* )
	{
	    set_selection_matches('a', 0);
	}
    }
    
    # If the argument starts with 'r', reset our position to the beginning of the current batch.
    
    elsif ( $argstring =~ qr{ ^ r }xsi )
    {
	$SELECTION{MATCH_INDEX} = 0;
	print_msg "Starting from the beginning of the current batch";
    }
    
    # If the argument starts with 'm', empty the current batch of records so that a new random
    # batch will be fetched. If the current batch is already empty, there is no need to print a
    # message to the user.
    
    elsif ( $argstring =~ qr{ ^ m }xsi )
    {
	set_selection_matches($SELECTION{MATCH_TYPE}, 0);
    }

    # If the argument starts with 'pr' or 'prev', put the previous list of matches back into the
    # current list. If the current list is not empty, swap the two.
    
    elsif ( $argstring =~ qr{ ^ pr (ev|$) }xsi )
    {
	swap_selection_matches();
    }
    
    # If the argument starts with 'n', 'p', 'u', or 'a', then fetch records using the
    # corresponding match type. If the specified match type is different from the current one,
    # empty the current batch of records. Otherwise, just keep going where we left off.
    
    elsif ( $argstring =~ qr{ ^ n (eg|$) }xsi )
    {
	set_selection_matches('n', 0) unless $SELECTION{MATCH_TYPE} eq 'n';
    }

    elsif ( $argstring =~ qr{ ^ p (os|$) }xsi )
    {
	set_selection_matches('p', 0) unless $SELECTION{MATCH_TYPE} eq 'p';
    }
    
    elsif ( $argstring =~ qr{ ^ u (n|a|$) }xsi )
    {
	set_selection_matches('u', 0) unless $SELECTION{MATCH_TYPE} eq 'u';
    }
    
    elsif ( $argstring =~ qr{ ^ a (ll)? $ }xsi )
    {
	set_selection_matches('a', 0) unless $SELECTION{MATCH_TYPE} eq 'a';
    }
    
    # If the argument starts with @, then fetch records using the specified refsource_no value(s).
    
    elsif ( $argstring =~ qr{ ^ ([?])? [@] ( \d [\d\s,@]* ) }xs )
    {
	my $type = $1 ? 'r' : 'l';
	my $valstring = $2;
	my @values = split /[\s,@]+/, $valstring;
	$valstring = join "','", @values;
	set_selection_matches($type, 0);
	set_selection_restriction("sc.refsource_no in ('$valstring')");
    }
    
    # If the argument starts with %, then fetch records using the specified reference_no value(s).
    
    elsif ( $argstring =~ qr{ ^ ([?])? [%] ( \d [\d\s,%]* ) }xs )
    {
	my $type = $1 ? 'r' : 'l';
	my $valstring = $2;
	my @values = split /[\s,%]+/, $valstring;
	$valstring = join "','", @values;
	set_selection_matches($type, 0);
	set_selection_restriction("sc.reference_no in ('$valstring')");
    }
    
    # Anything else is an error.
    
    else
    {
	print_msg "Unknown argument '$argstring'";
	return;
    }
    
    # If the current selection is generated from an active expression slot, generate a label for
    # it. Otherwise, the label will be the empty string.
    
    my $label = generate_label($SELECTION{SLOT});
        
    # Unless we have match counts for the current selection expression, compute them now. To be on
    # the safe side we regenerate the SQL, and then run count_matching_scores.
    
    unless ( defined $SELECTION{COUNT} )
    {
	count_selection($label) || return;
    }
    
    # If we don't have a cached batch of score records, fetch some. 
    
    unless ( @{$SELECTION{MATCHES}} )
    {
	my ($count, $posneg);
	
	my $limit = $SETTINGS{listsize} || 20;
	my $typelabel = $SELECTION{MATCH_TYPE} eq 'n' ? 'rejected'
	    : $SELECTION{MATCH_TYPE} eq 'p' ? 'accepted'
	    : $SELECTION{MATCH_TYPE} eq 'u' ? 'unassigned'
	    : $SELECTION{MATCH_TYPE} eq 'l' ? 'listed'
	    : $SELECTION{MATCH_TYPE} eq 'r' ? 'selected'
	    : 'unfiltered';
	
	if ( $SELECTION{MATCH_TYPE} eq 'n' )
	{
	    $count = $SELECTION{COUNT_NEG};
	    $posneg = "sc.manual = 0";
	}
	
	elsif ( $SELECTION{MATCH_TYPE} eq 'p' )
	{
	    $count = $SELECTION{COUNT_POS};
	    $posneg = "sc.manual = 1";
	}

	elsif ( $SELECTION{MATCH_TYPE} eq 'u' )
	{
	    $count = $SELECTION{COUNT} - $SELECTION{COUNT_POS} - $SELECTION{COUNT_NEG};
	    $posneg = "sc.manual is null";
	}
	
	elsif ( $SELECTION{MATCH_TYPE} eq 'a' || $SELECTION{MATCH_TYPE} eq 'r' || $SELECTION{MATCH_TYPE} eq 'l' )
	{
	    $count = $SELECTION{COUNT};
	    $posneg = "";
	}
	
	else
	{
	    print_msg "Assuming 'all'.";
	    
	    $count = $SELECTION{COUNT};
all	    $posneg = "";
	}
	
	unless ( $count )
	{
	    if ( $SELECTION{COUNT} == 0 )
	    {
		print_msg "Selection is empty.";
	    }

	    else
	    {
		print_msg "No $typelabel matches in the selection.";
	    }
	    
	    return;
	}
	
	# List either the first chunk or a random selection of matching score entries.
	
	my $mode = $SETTINGS{listmode} || 'random';
	my $sql = $SELECTION{SQL};
	
	if ( $posneg )
	{
	    $sql .= " and $posneg";
	}

	if ( $SELECTION{RESTRICTION} )
	{
	    if ( $SELECTION{MATCH_TYPE} eq 'l' )
	    {
		$sql = $SELECTION{RESTRICTION};
	    }
	    else
	    {
		$sql .= " and $SELECTION{RESTRICTION}";
	    }
	    $mode = 'sequential';
	}
	
	print_msg "Fetching $limit $typelabel records";
	
	# Now fetch the matching score records.

	my @matches;
	
	eval {
	    @matches = $rs->list_matching_scores($sql, $mode, $count, $limit);
	};
	
	# If an error is thrown indicating that the server has gone away, reconnect to the database and
	# retry the operation.
	
	if ( $@ =~ /server has gone away/i )
	{
	    print_msg "Server has gone away, reconnecting...";
	    
	    my $dbh = connectDB("$base_dir/$config_file", "pbdb");
	    
	    $rs->set_dbh($dbh);
	    
	    eval {
		@matches = $rs->list_matching_scores($sql, $mode, $count, $limit);
	    };
	}
	
	# If any other error occurs, the constructed SQL expression is not valid. Since
	# count_matching_scores already succeeded, it is most likely one of the modifications done
	# by list_matching_scores that is the problem. So leave the selection as-is and just
	# return.
	
	if ( $@ )
	{
	    print_msg "$label$@";
	    return;
	}

	set_selection_matches($SELECTION{MATCH_TYPE}, 0, @matches);
	
	if ( $SELECTION{RESTRICTION} && ! @matches )
	{
	    print_msg "The specified record(s) are not in the selection";
	}
    }
    
    # Display the first match, before returning to the command loop.

    my $i = $SELECTION{MATCH_INDEX};
    my $m = $SELECTION{MATCHES}[$i];
    
    display_match($m);
}


# count_selection ( label )
#
# This routine is called if a selection expression is given but we don't have counts for
# it. Return true on success, false if an error occurred.

sub count_selection {

    my ($label) = @_;
    
    my ($sql, $error) = generate_sql($SELECTION{EXPR});
    
    # If there is an error, print it and return. Leave $SELECTION{SLOT}, so that the user can
    # subsequently delete this slot if they wish.
    
    if ( $error )
    {
	print_msg "$label$error";
	set_selection($SELECTION{SLOT});
	return;
    }
    
    # Otherwise, fill in any uninterpreted variables in the generated SQL. If the user
    # declines to give a value for any variable that doesn't already have one, return without
    # doing anything else.
    
    if ( $sql =~ qr{ \$ \w+ }xs )
    {
	$sql = fill_variables($sql);
	
	unless ( defined $sql )
	{
	    $SELECTION{SQL} = undef;
	    return;
	}
    }
    
    # Now count the matching scores.
    
    my ($match_total, $match_pos, $match_neg);
    
    eval {
	($match_total, $match_pos, $match_neg) = $rs->count_matching_scores($sql);
    };
    
    # If an error is thrown indicating that the server has gone away, reconnect to the database and
    # retry the operation.
    
    if ( $@ =~ /server has gone away/i )
    {
	print_msg "Server has gone away, reconnecting...";
	
	my $dbh = connectDB("$base_dir/$config_file", "pbdb");
	
	$rs->set_dbh($dbh);
	
	eval {
	    ($match_total, $match_pos, $match_neg) = $rs->count_matching_scores($sql);
	};
    }
    
    # If any other error is thrown, the most likely reason is that the expression was not in
    # fact valid. Print the error message, clear $SELECTION{EXPR} and $SELECTION{SQL}, and
    # return. We do not clear $SELECTION{SLOT}, since it may be needed for a subsequent 'delete'
    # operation.
    
    if ( $@ )
    {
	print_msg "$label$@";
	set_selection($SELECTION{SLOT});
	return;
    }
    
    # Otherwise, store the SQL expression and the counts and return true.
    
    set_selection($SELECTION{SLOT}, $SELECTION{EXPR}, $sql);
    set_selection_counts($match_total, $match_pos, $match_neg);

    return 1;
}


# do_match ( command )
#
# Handle the commands for manual marking and scoring of match records.

sub do_match {
    
    my ($command, $argstring) = @_;
    
    # None of these commands take arguments. If the command is empty, do nothing.
    
    if ( $argstring )
    {
	print_msg "Invalid argument '$argstring'";
	return;
    }

    if ( ! defined $command || $command eq '' )
    {
	return;
    }
    
    $DB::single = 1 if $STOP;
    
    # These commands are only valid if we have listed some matches.
    
    unless ( ref $SELECTION{MATCHES} eq 'ARRAY' && $SELECTION{MATCHES}->@* )
    {
	print_msg "There are no listed matches.";
	return;
    }
    
    # Now handle the individual commands:

    my $i = $SELECTION{MATCH_INDEX};
    my $m = $SELECTION{MATCHES}[$i];
    
    my ($new_status, $display);
    
    eval {
	
	# 'p' moves to the previous record in the list. If we are already at the beginning, do
	# nothing.
	
	if ( $command eq 'p' )
	{
	    if ( $i > 0 )
	    {
		$SELECTION{MATCH_INDEX}--;
		$display = 1;
	    }
	}
	
	# 'n' moves to the next record in the list. If we are already at the end of the current list,
	# do nothing.
	
	elsif ( $command eq 'n' )
	{
	    if ( $i < $SELECTION{MATCHES}->@* )
	    {
		$SELECTION{MATCH_INDEX}++;
		$display = 1;
	    }
	}
	
	# '^' moves to the first record in the list.
	
	elsif ( $command eq '^' )
	{
	    $SELECTION{MATCH_INDEX} = 0;
	    $display = 1;
	}
	
	# '$" moves to the last record in the list.

	elsif ( $command eq '$' )
	{
	    $SELECTION{MATCH_INDEX} = $SELECTION{MATCHES}->$#*;
	    $display = 1;
	}

	# '.' redisplays the current record.

	elsif ( $command eq '.' )
	{
	    $display = 1;
	}
	
	# '0'-'9', etc. move to the specified index in the list, or the end of the list if the
	# list doesn't have that many items.

	elsif ( $command =~ /^(\d+)$/ )
	{
	    if ( $1 >= $SELECTION{MATCHES}->@* )
	    {
		$SELECTION{MATCH_INDEX} = $SELECTION{MATCHES}->@*;
		$display = 1;
	    }

	    else
	    {
		$SELECTION{MATCH_INDEX} = $1 + 0;
		$display = 1;
	    }
	}
	
	# 'a' accepts the current match and displays the next one.
	
	elsif ( $command eq 'a' )
	{
	    $DB::single = 1 if $STOP;
	    $new_status = $rs->set_manual($m, 1);
	    
	    print_msg "Above match is now " . match_label($new_status);
	    $SELECTION{MATCH_INDEX}++;
	    $display = 1;
	}
	
	# 'r' rejects the current match and displays the next one.
	
	elsif ( $command eq 'r' )
	{
	    $DB::single = 1 if $STOP;
	    $new_status = $rs->set_manual($m, 0);
	    
	    print_msg "Above match is now " . match_label($new_status);
	    $SELECTION{MATCH_INDEX}++;
	    $display = 1;
	}

	# 'u' unassigns the current match and displays the next one.
	
	elsif ( $command eq 'u' )
	{
	    $DB::single = 1 if $STOP;
	    $new_status = $rs->set_manual($m, undef);
	    
	    print_msg "Above match is now " . match_label($new_status);
	    $SELECTION{MATCH_INDEX}++;
	    $display = 1;
	}
	
	# 's' rescores the current match and displays any changes.
	
	elsif ( $command eq 's' )
	{
	    rescore_match($m);
	}
	
	else
	{
	    print_msg "Unknown command '$command'";
	}
    };
    
    if ( $@ )
    {
	print_msg $@;
    }
    
    # If the next match is supposed to be displayed, do so and then return to the command loop.
    
    if ( $display )
    {
	my $next = $SELECTION{MATCH_INDEX};

	if ( $next < $SELECTION{MATCHES}->@* )
	{
	    display_match($SELECTION{MATCHES}[$next]);
	}

	else
	{
	    print_msg "End of current match list.";
	}
    }
}


# list_current_matches ( )
#
# Interactively display the current match list one by one, and ask the user what to do with each
# one. Valid responses include:
#
#   a	Mark the match as accepted.
#   r   Mark the match as rejected.
#   u   Mark the match as unassigned.
#   s   Re-score the match.
#   n   Go on to the next match.
#   p   Return to the previous match.
#   q   Return to the main command line.
#   x   Quit this program entirely.

sub list_current_matches {

  MATCH:
    while ( $SELECTION{MATCH_INDEX} < @{$SELECTION{MATCHES}} )
    {
	display_match($SELECTION{MATCHES}[$SELECTION{MATCH_INDEX}]);
	
	my $response;
	
	# Prompt for user input until a valid response is received.
	
	while ( 1 )
	{
	    $response = $TERM->readline("Match $SELECTION{MATCH_INDEX} [arus|np|qx]: ");
	    
	    next if $response =~ /^p/ && ! ($SELECTION{MATCH_INDEX} > 0);
	    last if ! defined $response || $response =~ /^(a|r|u|s|n|p|q|x)/i;
	}
	
	if ( ! defined $response || $response =~ /^(q|x)/i )
	{
	    $DONE = 1 unless $response =~ /^q/;
	    return;
	}
	
	elsif ( $response =~ /^p/i )
	{
	    $SELECTION{MATCH_INDEX}-- if $SELECTION{MATCH_INDEX} > 0;
	    next MATCH;
	}
	
	elsif ( $response =~ /^n/i )
	{
	    $SELECTION{MATCH_INDEX}++;
	    next MATCH;
	}
	
	my ($new, $set, $stay);
	
	eval {
	    if ( $response =~ /^a/i )
	    {
		$new = $rs->set_manual($SELECTION{MATCHES}[$SELECTION{MATCH_INDEX}], 1);
		$set = 1;
	    }
	    
	    elsif ( $response =~ /^r/i )
	    {
		$new = $rs->set_manual($SELECTION{MATCHES}[$SELECTION{MATCH_INDEX}], 0);
		$set = 1;
	    }
	    
	    elsif ( $response =~ /^u/i )
	    {
		$new = $rs->set_manual($SELECTION{MATCHES}[$SELECTION{MATCH_INDEX}], undef);
		$set = 1;
	    }

	    elsif ( $response =~ /^s/i )
	    {
		$stay = 1;
		rescore_match($SELECTION{MATCHES}[$SELECTION{MATCH_INDEX}]);
	    }
	};
	
	if ( $@ )
	{
	    print STDERR "$@\n";
	}
	
	elsif ( $set )
	{
	    my $match_label = match_label($new);
	    
	    print_msg "Above match is $match_label";
	}
	
	$SELECTION{MATCH_INDEX}++ unless $stay;
    }

    print_msg "Done with the current list of matches.";
}


# display_match ( match_record )
#
# Display the specified match. Display formatted text representing both the original paleobiodb
# reference and the matching reference data. Below them, display the matrix of match scores. Also
# display a label indicating whether this record has been manually accepted or rejected or is
# unassigned.

sub display_match {
    
    my ($m) = @_;
    
    my $color_output = $SETTINGS{color} eq 'on' ? 1 : undef;
    
    # Ensure that we have formatted text for both sets of reference data.

    eval {
	$DB::single = 1 if $DEBUG{display};
	$rs->format_match($m);
    };
    
    # If an exception was thrown, stuff the error message into $m->{match_formatted} so that it
    # will be displayed below. In almost all circumstances, $m->{ref_formatted} will already have
    # been filled in.
    
    if ( $@ )
    {
	$m->{match_formatted} = $@;
    }
    
    my $match_label = match_label($m->{manual});
    
    print_line "REF $m->{reference_no}    \@$m->{refsource_no}:    $match_label\n";
    print_line $m->{ref_formatted}, "\n";
    print_line $m->{match_formatted}, "\n";
    print_line $rs->format_scores_horizontal($m, $color_output), "\n";
    
    return;
}


# match_label ( match_value )
#
# Return a string that can be displayed to indicate whether a given score record has a match value
# of true (accepted), false (rejected), or undefined (unknown or unassigned). Depending on the
# 'color' setting, this string may or may not include ANSI terminal color codes.

sub match_label {

    my ($match_value) = @_;

    if ( $match_value )
    {
	return $SETTINGS{color} ? "\033[0;33mACCEPTED\033[0m" : "ACCEPTED";
    }

    elsif ( defined $match_value && ! $match_value )
    {
	return $SETTINGS{color} ? "\033[0;31mREJECTED\033[0m" : "REJECTED";
    }

    else
    {
	return "UNASSIGNED";
    }
}


# rescore_match ( match_record )
#
# Re-run the scoring procedure for this match record. If any of the scores have changed, store the
# new set of scores.

sub rescore_match {
    
    my ($m) = @_;
    
    my $color_output = $SETTINGS{color} eq 'on' ? 1 : undef;
    
    # Decode and extract the data structure containing the reference data to be matched. If an
    # exception is thrown, print it out and return.
    
    my $item;
    
    eval {
	$item = $rs->get_match_data($m);
    };

    if ( $@ )
    {
	print_msg $@;
	return;
    }
    
    # Compute a new set of scores using the ref_similarity subroutine from ReferenceMatch.pm. If
    # the debug flag 'scoring' is set, stop here so the person running this can single-step through
    # the scoring process.
    
    $DB::single = 1 if $STOP || $DEBUG{scoring};
    
    my $scores = ref_similarity($m, $item);
    
    # If any of the elements in the score matrix have changed, store the changed numbers back to
    # the corresponding record in the REFERENCE_SCORES table. Inform the user how many scores have
    # been updated.
    
    if ( my $count = $rs->update_match_scores($m, $scores) )
    {
	print_msg "$count score variables updated";
	print_line $rs->format_scores_horizontal($m, $color_output), "\n";
    }
    
    # Otherwise, inform the user that the re-scoring operation produced the same set of scores
    # that we had before.
    
    else
    {
	print_msg "No change to match scores";
    }

    # Also recreate the formatted reference text and compare it to what we already have. If it
    # differs, store it.

    my $new_formatted = $rs->format_ref($item);

    if ( $m->{match_formatted} ne $new_formatted && $rs->update_match_formatted($m, $new_formatted) )
    {
	$m->{match_formatted} = $new_formatted;
	print_line $m->{match_formatted}, "\n";
    }
    
    else
    {
	print_line "No change to formatted reference text\n";
    }
}


# do_score ( argstring )
# 
# Rescore reference match records from the current selection. Accepted arguments are 'positive',
# 'negative', 'unassigned' or 'all'. All of the corresponding records from the current selection
# are rescored, not just the list that is currently cached.

sub do_score {
    
    my ($command, $argstring) = @_;

    $DB::single = 1 if $STOP;
    
    unless ( $argstring )
    {
	print_msg "You must specify either 'negative', 'positive', 'unassigned' or 'all'";
	return;
    }

    my $commandlabel;

    if ( $command eq 'score' )
    {
	$commandlabel = 'Scored';
    }

    elsif ( $command eq 'format' )
    {
	$commandlabel = 'Formatted';
    }

    else
    {
	croak "bad command '$command'";
    }
    
    # If the argument starts with 'n', 'p', 'u', or 'a', then fetch records using the
    # corresponding match type.
    
    my $match_type;
    
    if ( $argstring =~ qr{ ^ n (eg|$) }xsi )
    {
	$match_type = 'n';
    }
    
    elsif ( $argstring =~ qr{ ^ p (os|$) }xsi )
    {
	$match_type = 'p';
    }
    
    elsif ( $argstring =~ qr{ ^ u (n|a|$) }xsi )
    {
	$match_type = 'u';
    }
    
    elsif ( $argstring =~ qr{ ^ a (ll)? $ }xsi )
    {
	$match_type = 'a';
    }
    
    else
    {
	print_msg "Invalid argument '$argstring'";
	return;
    }
    
    # Generate an SQL statement handle that will fetch records of the corresponding type.
    
    my ($count, $posneg, $match_label);
    
    if ( $match_type eq 'n' )
    {
	$count = $SELECTION{COUNT_NEG};
	$posneg = "sc.manual = 0";
	$match_label = "rejected";
    }
    
    elsif ( $match_type eq 'p' )
    {
	$count = $SELECTION{COUNT_POS};
	$posneg = "sc.manual = 1";
	$match_label = "accepted";
    }

    elsif ( $match_type eq 'u' )
    {
	$count = $SELECTION{COUNT} - $SELECTION{COUNT_POS} - $SELECTION{COUNT_NEG};
	$posneg = "sc.manual is null";
	$match_label = "unassigned";
    }

    elsif ( $match_type eq 'a' )
    {
	$count = $SELECTION{COUNT};
	$posneg = "";
	$match_label = "all";
    }

    else
    {
	return;
    }
    
    my $sql = $SELECTION{SQL};

    if ( $posneg )
    {
	$sql .= " and $posneg";
    }
    
    unless ( $count )
    {
	if ( $SELECTION{COUNT} == 0 )
	{
	    print_msg "Selection is empty.";
	}
	
	else
	{
	    print_msg "No $match_label matches in the selection.";
	}
	
	return;
    }
    
    print_msg "Scoring $count records matching $SELECTION{EXPR}:";
    
    my $offset;
    
    if ( $SELECTION{OFFSET} && $SELECTION{OFFSET_TYPE} eq $match_type )
    {
	$offset = $SELECTION{OFFSET};
	print_line "    Resuming at record $offset\n";
    }

    else
    {
	set_selection_offset($match_type, 0);
    }
    
    # Now create a statement handle for fetching these records.
    
    eval {
	$rs->select_matching_scores($sql, $offset, 1);
    };

    if ( $@ )
    {
	print_msg $@;
	return;
    }

    # Then loop, fetching a record and scoring it, until we have done all of them. Set $WAITING to
    # true so that an interrupt will just set $INTERRUPT to true.
    
    my $record_count = $offset || 0;
    my $update_count = 0;
    my $label_length = 0;
    my $LAST = 0;
    
    $WAITING = 1;
    
    while ( my $m = $rs->get_next() )
    {
	if ( ++$record_count % 10 == 0 )
	{
	    if ( $label_length ) { print "\x{8}" x $label_length; }
	    my $label = "$commandlabel $record_count of $count records";
	    $label_length = length($label);
	    print $label;
	}
	
	set_selection_offset($match_type, $record_count);
	
	eval {
	    my $item = $rs->get_match_data($m);
	    
	    # Compute a new set of scores using the ref_similarity subroutine from
	    # ReferenceMatch.pm. If an interrupt is received and the debugger is active, stop here.
	    
	    if ( $command eq 'score' )
	    {
		if ( $INTERRUPT && defined $DB::level )
		{
		    $INTERRUPT = undef;
		    print "\n";
		    $label_length = 0;
		    $DB::single = 1;
		}
		
		my $scores = ref_similarity($m, $item);
		
		# If any of the elements in the score matrix have changed, store the changed
		# numbers back to the corresponding record in the REFERENCE_SCORES table. Inform
		# the user how many scores have been updated.
		
		my $count = $rs->update_match_scores($m, $scores);
		
		$update_count++ if $count;
	    }

	    elsif ( $command eq 'format' )
	    {
		if ( $INTERRUPT && defined $DB::level )
		{
		    $INTERRUPT = undef;
		    print "\n";
		    $label_length = 0;
		    $DB::single = 1;
		}
		
		my $new_formatted = $rs->format_ref($item);
		
		if ( ! $m->{formatted} || $m->{formatted} ne $new_formatted )
		{
		    if ( $rs->update_match_formatted($m, $new_formatted) )
		    {
			$update_count++;
		    }
		}
	    }
 	};
	
	if ( $@ )
	{
	    if ( $label_length ) { print "\x{8}" x $label_length; }
	    print_line '@', $m->{refsource_no}, ': ', $@;
	    next;
	}
	
	# If an interrupt is received and the debugger is not active, end the loop early.
	
	last if $INTERRUPT && ! defined $DB::level;
    }
    
    if ( $label_length ) { print "\x{8}" x $label_length; }
    
    # If an interrupt was received, let the user know.
    
    if ( $INTERRUPT )
    {
	print_line "Loop interrupted.\n";
    }

    # Otherwise, reset the offset back to zero.

    else
    {
	set_selection_offset($match_type, 0);
    }
    
    # Notify the user how many records were processed, and how many were updated.
    
    print_line "$commandlabel $record_count of $count records.";
    print_line "$update_count records were updated.\n";
}


sub do_test {
    
    my ($command, $argstring) = @_;

    $DB::single = 1 if $STOP || $DEBUG{test};
    
    if ( $argstring =~ qr{ ^ authorlist \s+ (.*) }xs )
    {
	my $authorlist = $1;

	my @data = map { [ parse_authorname($_) ] } split_authorlist($authorlist);
	
	print Dumper(@data);
    }
    
    elsif ( $argstring =~ qr{ ^ authorname \s+ (.*) }xs )
    {
	my $authorname = $1;

	print Dumper([parse_authorname($authorname)]);
    }
    
    else
    {
	print_msg "Unknown command '$argstring'";
    }
}


sub do_quit {

    $DONE = 1;
}


sub do_stop {

    if ( defined $DB::level )
    {
	$DB::single = 1;

	my $a = 1;
	my $b = 1;
	my $c = 1;
    }
    
    else
    {
	save_state("$base_dir/$state_file");
	print_msg "Application state saved.";
	exec("perl", "-d", $0);
    }
}


sub do_interrupt {

    if ( $WAITING )
    {
	$INTERRUPT = 1;
    }
    
    elsif ( $SIG_ROUTINE )
    {
	$INTERRUPT = 1;
	&$SIG_ROUTINE;
    }
    
    else
    {
	die "Interrupt\n";
    }
}


# do_help ( topic )
#
# Display documentation about this program or any of its subcommands, plus some other topics.

sub do_help {

    my ($command, $argstring) = @_;
    
    if ( $argstring !~ /\S/ )
    {
	print "\nThe purpose of this program is to compare stored bibliographic references\n" .
	    "with data fetched from CrossRef and other sources.\n\n";
    }
}


# expand_sql ( expr )
#
# Parse and translate the given expression into a fully compliant SQL expression that can be used
# to make queries on the REFERENCE_SCORES table. The language accepted by this subroutine is
# defined by the following token maps and by the regular expressions in the while() loop below.
# 
# If the argument is empty, the empty string will be returned. If a syntax error is found or if
# there is an error in the parsing code, an exception will be raised starting with either 'Syntax
# error' or 'Parse error'. Otherwise, a valid SQL expression will be returned.

BEGIN {
    (%VARMAP) = ( ti => 'sc.title', pu => 'sc.pub', a1 => 'sc.auth1', a2 => 'sc.auth2',
		  yr => 'sc.pubyr', vo => 'sc.volume', pg => 'sc.pages', pr => 'sc.pblshr',
		  mc => 'sc.count', mp => 'sc.complete', ms => 'sc.sum',
		  xc => 'sc.count', xp => 'sc.complete', xs => 'sc.sum');
    (%MODMAP) = ( mc => 's', mp => 's', ms => 's', xc => 'c', xp => 'c', xs => 'c',
		  "'" => 's', "!" => 'c' );
    (%OPMAP) = ( '' => '>=', '.' => '>=', '+' => '>=', '=' => '=', '-' => '<',
		 '>=' => '>=', '<=' => '<=', '>' => '>', '<' => '<' );
}

sub accumulate (\@$);

sub expand_sql {
    
    my ($expr) = @_;
    
    # An empty expression results in the empty string.
    
    return '' unless defined $expr && $expr ne '';
    
    # Initialize the parse stack to '('. We will use shift and unshift to add and remove items, so
    # the top of the stack will always be position 0. If the input expression is a valid string in
    # the language defined by this subroutine, the process below will result in the stack
    # being left with a single item which is a valid SQL expression surrounded by parentheses.
    
    my @stack = '(';
    
    # Loop until the input expression has been completely parsed. Each iteration of this loop should
    # remove one token from the expression. The variable $n_tokens counts the number of loop
    # iterations, and is used primarily as a backstop to prevent an infinite loop if there is an
    # error in the code below.
    
    my $n_tokens = 0;
    my $sc;
    
    while ( $expr ne '' )
    {
	# At each step of the parsing process, record the next 10 characters to provide a context
	# for error messages.
	
	$sc = substr($expr, 0, 10);
	
	# To prevent an infinite loop if there is an error in the code below, stop after 200
	# iterations and throw an exception.
	
	if ( $n_tokens++ > 200 )
	{
	    die "Parse error: runaway loop at '$sc'";
	}
	
	# Recognize a token that represents a simple expression on one of the score
	# variables. Examples: 'ti80' => 'title_s >= 80'; 'xc<3' => 'complete_c < 3'
	
	if ( $expr =~ qr{ ^ (ti|pu|a1|a2|yr|vo|pg|pr|mc|mp|ms|xc|xp|xs) ([!'])?
			    ([.=<>+-]|<=|>=)? (\d+ | [@] | [a-zA-Z]\w*) \s* (.*) }xs )
	{
	    my $raw_var = $1;
	    my $raw_mod = $2 || '';
	    my $raw_op = $3 || '';
	    my $raw_val = $4;
	    $expr = $5;
	    
	    my $variable = score_variable($raw_var, $raw_mod);
	    my $op = score_operator($raw_op);
	    my $value = score_value($raw_val);
	    
	    die "Syntax error: unknown variable '$raw_var' at '$sc'" unless $variable;
	    die "Syntax error: unknown operator '$raw_op' at '$sc'" unless $op;
	    die "Syntax error: bad value '$raw_val' at '$sc'" unless defined $value && $value ne '';
	    
	    accumulate( @stack, "$variable $op $value" );
	}

	# Recognize a token that represents an insertion of a piece of SQL from the active
	# expression table.

	elsif ( $expr =~ qr{ ^ E (?: (\d+) | ['] (\w+) ) \s* (.*) }xs )
	{
	    my $index = $1;
	    my $name = $2;
	    $expr = $3;
	    
	    if ( defined $index && $index ne '' )
	    {
		if ( $index && $ACTIVE[$index - 1]{sql} )
		{
		    accumulate( @stack, $ACTIVE[$index - 1]{sql} );
		}
		
		else
		{
		    die "Substitution error: no sql found for E$index";
		}
	    }
	    
	    elsif ( $name )
	    {
		my $slot = search_active($name);

		if ( $slot && $ACTIVE[$slot - 1]{sql} )
		{
		    accumulate( @stack, $ACTIVE[$slot - 1]{sql} );
		}

		elsif ( $slot )
		{
		    die "Substitution error: no sql found for E$slot";
		}

		else
		{
		    die "Substitution error: no entry found for '$name'";
		}
	    }

	    else
	    {
		die "Parse error at '$sc'";
	    }
	}

	# Recognize a token that represents a simple variable.

	elsif ( $expr =~ qr{ ^ (ti|pu|a1|a2|yr|vo|pg|pr|mc|mp|ms|xc|xp|xs) ([!'] | (?!\w)) \s* (.*) }xs )
	{
	    my $raw_var = $1;
	    my $raw_mod = $2 || '';
	    my $expr = $3;
	    
	    my $variable = score_variable($raw_var, $raw_mod) ||
		die "Syntax error: unknown variable '$raw_var' at '$sc'";
	    
	    accumulate( @stack, $variable );
	}
	
	# Recognize the '|' token, which represents a disjunction. We are using loose validation, so
	# just ignore | when it occurs at the beginning or end of a subexpression and coalesce two
	# or more | in a row into one.
	
	elsif ( $expr =~ qr{ ^ [|] \s* (.*) }xs )
	{
	    $expr = $1;
	    
	    # Push 'OR' onto the parse stack, to signal a disjunction. But skip this if the top of
	    # the stack is already 'OR', which means we found two '|' in a row. Also skip this if
	    # the top of the stack ends with '(', which means that '|' was the first token in the
	    # current subexpression.
	    
	    unshift @stack, 'OR' unless $stack[0] eq 'OR' || $stack[0] =~ /[(]$/;
	}
	
	# Recognize the '(' token, which starts a subexpression. Push '(' onto the parse stack,
	# which will subsequently accumulate the contents of the subexpression.
	
	elsif ( $expr =~ qr{ ^ [(] \s* (.*) }xs )
	{
	    unshift @stack, '(';
	    $expr = $1;
	}
	
	# Recognize the ')' token, which ends a subexpression. The subexpression will be popped off
	# the stack and added to the expression that is then at the top.
	
	elsif ( $expr =~ qr{ ^ [)] \s* (.*) }xs )
	{
	    $expr = $1;
	    
	    # If the top of the parse stack is 'OR', the subexpression must have ended with
	    # '|'. Since we are validating loosely, we simply discard it.
	    
	    shift @stack if $stack[0] eq 'OR';
	    
	    # If the stack contains only one item, that means we have encountered a ')' without a
	    # matching '(' preceding it.
	    
	    die "Syntax error: unbalanced parentheses at '$sc'" unless @stack > 1;
	    
	    # Pop the subexpression off the stack, add the ')' to close it, and then add it to the
	    # end of the expression that is now at the top of the stack.
	    
	    accumulate( @stack, shift(@stack) . ')' );
	}
	
	# Recognize the '/' token, which initiates an "and not..." or "or not..."
	# subexpression. So, for example, '(abc / def | ghi)' is interpreted as "(abc) and not
	# (def or ghi)". But '(abc |/ def | ghi)' is interpreted as "(abc) or not (def or ghi)".
	
	elsif ( $expr =~ qr{ ^ / \s* (.*) }xs )
	{
	    $expr = $1;
	    
	    # Terminate the top expression on the stack with ')'. If the top of the stack is 'OR',
	    # then the next entry is the expression which needs to be terminated and the 'OR' is
	    # preserved. That will cause the new subexpression to be interpreted as "or not..."
	    # instead of the default "and not...".
	    
	    my $i = $stack[0] eq 'OR' ? 1 : 0;
	    $stack[$i] .= ')';
	    
	    # Push 'NOT (' onto the stack. This new entry will accumulate the subexpression
	    # introduced by the current token.
	    
	    unshift @stack, 'NOT (';
	}
	
	# Recognize the ';' token, representing a disjunction that takes precedence over all other
	# operators. This token terminates a subexpression started with '/'. But any subexpression
	# that starts with an explicit '(' must be closed with an explicit ')' or else a syntax
	# error will be thrown.
	
	elsif ( $expr =~ qr{ ^ ; \s* (.*) }xs )
	{
	    $expr = $1;
	    
	    # If the top of the parse stack is 'OR', the previous subexpression must have ended
	    # with '|'. Since we are validating loosely, we simply discard it.
	    
	    shift @stack if $stack[0] eq 'OR';

	    # If the top of the stack starts with 'NOT (', it represents a subexpression that was
	    # started with '/'. Pop it off the stack, close it with a ')', and add it to the end
	    # of the expression that is now at the top of the stack. Since the subexpression
	    # started with '/', the closing ')' is implicit.
	    
	    if ( $stack[0] =~ qr{ ^ NOT \s* [(] }xs )
	    {
		accumulate( @stack, shift(@stack) . ')' );
	    }
	    
	    # Push an 'OR' onto the top of the stack, to signal the disjunction.
	    
	    unshift @stack, 'OR';
	}

	# Recognize the constants 'true' and 'false', without case sensitivity. These are replaced
	# by '1' and '0' respectively.
	
	elsif ( $expr =~ qr{ ^ (?: (true)|(false)) \s* (.*) }xs )
	{
	    $expr = $3;

	    accumulate( @stack, $1 ? '1' : '0' );
	}
	
	# If we get here, then the input string is not valid. If it starts with an identifier,
	# it probably represents a misspelled or otherwise unknown variable.
	
	elsif ( $expr =~ qr{ ^ (\w+) }xs )
	{
	    die "Syntax error: unknown variable '$1' at '$sc'";
	}
	
	# As a catch-all, throw a generic syntax error.
	
	else
	{
	    die "Syntax error: bad syntax at '$sc'";
	}
    }
    
    # Now we clean up and return the parsed expression. If the top of the stack is 'OR', it
    # represents a trailing disjunction and we simply discard it.
    
    shift @stack if $stack[0] eq 'OR';
    
    # If the top of the stack starts with 'NOT (', that means the final subexpression was
    # initiated by '/'. Pop it off the stack, close it with an implicit ')', and add it to the end
    # of the next expression down (which should be the only one remaining on the stack).
    
    if ( $stack[0] =~ qr{ ^ NOT \s* [(] }xs )
    {
	accumulate( @stack, shift(@stack) . ')' );
    }
    
    # Otherwise, close the expression at the top of the stack with ')'.
    
    else
    {
	$stack[0] .= ')';
    }
    
    # If the stack contains more than one item, that means the number of '(' exceeded the number
    # of ')'.
    
    die "Syntax error: unbalanced parentheses at '$sc'" unless @stack == 1;

    # Otherwise, we return the expression at the top of the stack.
    
    return $stack[0];
    
    # Examples: ti+90 (a1@ | a2+th1 yr95) / yr* ; ti+80 (a1@ a2@) /yr*  // pu.th2 ti!-50
}


# accumulate ( stack_ref, subexpr )
# 
# Add the specified subexpression to the end of the top expression on the stack.

sub accumulate (\@$) {

    my ($stack_ref, $subexpr) = @_;

    # Repeated parentheses are coalesced into one. So "((abc))" becomes '(abc)' and "not
    # ((abc))" becomes 'not (abc)'.

    if ( $subexpr =~ qr{ ^ (NOT \s*)? [(][(] (.*) [)][)] }xs )
    {
	$subexpr = "$1($2)";
    }

    # After parentheses are coalesced, empty subexpressions such as '()' and 'not ()' are
    # discarded.

    return if $subexpr eq '()' || $subexpr =~ qr{ ^ NOT \s* [(][)] $ }xs;

    # If the top expression on the stack ends with '(', that means the subexpression currently
    # being added is the first one. So just add it.

    if ( $stack_ref->[0] =~ /[(]$/ )
    {
	$stack_ref->[0] = "$stack_ref->[0]$subexpr";
    }

    # If the top of the stack is 'OR', that means the subexpression currently being added
    # should be disjoined instead of conjoined to the growing expression. Pop the 'OR' off the
    # stack, and add the subexpression with an 'OR' operator.

    elsif ( $stack_ref->[0] eq 'OR' )
    {
	shift @$stack_ref;
	$stack_ref->[0] = "$stack_ref->[0] OR $subexpr";
    }

    # Otherwise, add the subexpression with an 'AND' operator.

    else
    {
	$stack_ref->[0] = "$stack_ref->[0] AND $subexpr";
    }
}


# score_variable ( raw_var, raw_mod, negative )
#
# Given an abbreviation, return the full name of a score variable with a suffix of either '_s'
# (similarity) or '_c' (conflict). If an explicit modifier is given, use that modifier to
# determine which suffix to use. If the $negative argument is true, that means the name is being
# interpreted in a context that defaults to '_c'. Otherwise, the suffix defaults to '_s'.
#
# If either the abbreviation or the modifier are unrecognized, return undefined which will trigger
# a syntax error to be thrown.

sub score_variable {
    
    my ($raw_var, $raw_mod, $negative) = @_;
    
    my $variable = $VARMAP{$raw_var};
    my $suffix = $MODMAP{$raw_var} || $MODMAP{$raw_mod} || ($negative && 'c') || 's';
    
    if ( $variable && $suffix )
    {
	return $variable . '_' . $suffix;
    }
    
    else
    {
	return undef;
    }
}


# score_operator ( raw_op )
#
# Return the appropriate SQL comparison operator corresponding to the given abbreviation. If the
# given argument is not a recognized abbreviation, return undefined which will trigger a syntax
# error to be thrown.

sub score_operator {
    
    my ($raw_op) = @_;
    
    return $raw_op ? $OPMAP{$raw_op} : '>=';
}


# score_value ( raw_val )
#
# Interpret the given value. If it is a decimal number, return that. The special value '@' means
# 100. If it is an identifier which is listed in the %VARS table with a decimal number value,
# return the value. Otherwise, return the identifier preceded by '$'. If the value doesn't match
# any of these patterns, return undefined which will trigger a syntax error to be thrown.

sub score_value {
    
    my ($raw_val) = @_;
    
    return $raw_val if $raw_val =~ /^\d+$/;
    return 100 if $raw_val eq '@';
    
    if ( $raw_val =~ /^[a-zA-Z]\w*$/ )
    {
	if ( defined $VARS{$raw_val} && $VARS{$raw_val} =~ /^\d+$/ )
	{
	    return $VARS{$raw_val};
	}

	else
	{
	    return '$' . $raw_val;
	}
    }
    
    else
    {
	return undef;
    }
}


# # If the action is 'compare', do a score comparison. First query for differential counts and
# # display that info to the user. Ask the user if they want to see the actual lists of reference matches.

# if ( $action eq 'compare' )
# {
#     # First get the counts.
    
#     my ($count_a, $count_b) = $rs->compare_scores($expr_a, $expr_b);

#     # Print them out.

#     if ( $expr_b )
#     {
# 	print "Matched A: $count_a\n\n";

# 	print "Matched B: $count_b\n\n";
#     }

#     else
#     {
# 	print "Matched: $count_a\n\n";
#     }
    
#     # Now loop, asking for a response.
    
#     my $answer;
#     my $matchlist_a;
#     my $matchlist_b;
    
#     while (1)
#     {
# 	print "See match list? ";
# 	my $answer = <STDIN>;
# 	chomp $answer;
	
# 	last if $answer =~ /^[nq]/i;
# 	next unless $answer =~ /^[aby]/i;
	
# 	my @matches;
	
# 	if ( $answer =~ /^[ay]/i )
# 	{
# 	    if ( ref $matchlist_a eq 'ARRAY' )
# 	    {
# 		@matches = @$matchlist_a;
# 	    }

# 	    else
# 	    {
# 		@matches = $rs->compare_list_scores($expr_a, $expr_b, 50);
# 		$matchlist_a = \@matches;
# 	    }
# 	}
	
# 	else
# 	{
# 	    if ( ref $matchlist_b eq 'ARRAY' )
# 	    {
# 		@matches = @$matchlist_b;
# 	    }

# 	    else
# 	    {
# 		@matches = $rs->compare_list_scores($expr_b, $expr_a, 50);
# 		$matchlist_b = \@matches;
# 	    }
# 	}
	
# 	open(my $outfile, '>', "/var/tmp/refcheck$$.output");
	
# 	my $sep = '';
	
# 	foreach my $m ( @matches )
# 	{
# 	    print $outfile $sep;
# 	    print $outfile "REF $m->{reference_no}    \@$m->{refsource_no}:\n\n";
# 	    print $outfile $m->{ref_formatted}, "\n\n";
# 	    print $outfile $m->{formatted}, "\n\n";
# 	    print $outfile format_scores_horizontal($m), "\n";
	    
# 	    $sep = "===================================================\n";
# 	}

# 	close $outfile;

# 	system("less", "/var/tmp/refcheck$$.output");
#     }
    
#     unlink "/var/tmp/refcheck$$.output";
    
#     exit;
# }


# elsif ( $action eq 'history' )
#     {
# 	my @events = $rs->select_events($r, 'history');
# 	printout_history($r, \@events);
#     }
    
#     elsif ( $action eq 'dump' )
#     {
# 	my ($event) = $rs->select_events($r, 'latest');
# 	my @items = get_event_content($r, $event, $opt_which);
# 	printout_event_attrs($event);
	
# 	foreach my $i (@items)
# 	{
# 	    printout_item_source($i);
# 	}
#     }
    
#     elsif ( $action eq 'show' )
#     {
# 	my ($event) = $rs->select_events($r, 'latest');
# 	my @items = get_event_content($r, $event, $opt_which);
# 	printout_event_attrs($event);
	
# 	foreach my $i (@items)
# 	{
# 	    printout_item_formatted($i);
# 	}
#     }
    
#     elsif ( $action eq 'score' )
#     {
# 	my ($event) = $rs->select_events($r, 'latest');
# 	my @items = get_event_content($r, $event, $opt_which);
# 	printout_event_attrs($event) unless $NO_PRINT;
# 	printout_item_formatted($r) unless $NO_PRINT;
# 	my $index = $opt_which eq 'all' ? 0 : $opt_which > 0 ? $opt_which-1 : 0;
	
# 	foreach my $item (@items)
# 	{
# 	    my $scores = ref_similarity($r, $item);
# 	    printout_item_formatted($item) unless $NO_PRINT;
# 	    printout_item_scores($scores) unless $NO_PRINT;
# 	    my $result = $rs->store_scores($event, $scores, $index);
# 	    $index++;
# 	    if ( $result ) { $score_count++; }
# 	    else { $error_count++; }
# 	}
#     }

#     elsif ( $action eq 'recount' )
#     {
# 	$rs->recount_scores($r->{reference_no});
#     }
    
#     elsif ( $action eq 'match' )
#     {
# 	my ($event) = $rs->select_events($r, 'latest');
# 	# my @items = get_event_content($r, $event, $opt_which);
# 	# if ( my $f = get_eventdata($r, $event, $opt_which) )
# 	# {
# 	#     printout_data($r, $f, 'match') unless $NO_PRINT;
# 	# }
#     }
    

# sub stop_loop {

#     $END_LOOP = 'interrupt';
# }

# # =============================================
# #
# # reference actions
# #
# # =============================================

# sub printout_ref {
    
#     my ($r) = @_;
    
#     my $string = encode_utf8(format_ref($r));
    
#     my $score = defined $r->{score} ? " [$r->{score}]" : "";
    
#     print STDOUT "$r->{reference_no} :$score $string\n";
# }


# sub fetch_check {

#     if ( rand > 0.45 ) { print STDERR "200 OK\n"; return "200 OK"; }
#     else { print STDERR "400 Bad request\n"; return "400 Bad Request"; }
# }


# sub fetch_ref {

#     my ($rs, $r, $action) = @_;
    
#     my $string = format_ref($r);
    
#     print STDERR "Fetching refno $r->{reference_no} from $source:\n$string\n\n";
    
#     my ($status, $query_text, $query_url, $response_data) = $rs->metadata_query($r, 2);
    
#     if ( $action eq 'fetch' && $status && $r->{reference_no} )
#     {
# 	my $result = $rs->store_result($r->{reference_no}, $status,
# 				       $query_text, $query_url, $response_data);
	
# 	if ( $result )
# 	{
# 	    print STDERR "Result: $status; refsource_no = $result\n\n";
# 	}
	
# 	else
# 	{
# 	    print STDERR "Result $status; DATABASE ERROR, no record inserted.\n\n";
# 	    return "500 No record inserted";
# 	}
#     }
    
#     elsif ( $status && $r->{reference_no} )
#     {
# 	print STDERR "Result: $status\n\n";
#     }
    
#     elsif ( $status )
#     {
# 	print STDERR "Query text: $query_text\n\n";
# 	print STDERR "Result: $status\n\n";
#     }
    
#     else
#     {
# 	print STDERR "FETCH ERROR, no status returned.\n\n";
#     }
    
#     if ( $opt_print && $response_data )
#     {
# 	print STDOUT $response_data;
	
# 	unless ( $response_data =~ /\n$/ )
# 	{
# 	    print STDOUT "\n";
# 	}
#     }

#     return $status || "500 No status returned";
# }


# sub printout_event_history {

#     my ($r, $eventlist) = @_;
    
#     # If there aren't any, print an error message and exit.
    
#     unless ( ref $eventlist eq 'ARRAY' && @$eventlist )
#     {
# 	if ( ref $r && $r->{reference_no} )
# 	{
# 	    print STDERR "No events found for refno $r->{reference_no}";
# 	    exit;
# 	}

# 	else
# 	{
# 	    print STDERR "No refno found";
# 	    exit;
# 	}
#     }
    
#     # Otherwise, print out the results.
    
#     my @rows = ['id', 'refno', 'source', 'eventtype', 'eventtime', 'status', 'data'];
    
#     foreach my $e ( @$eventlist )
#     {
# 	push @rows, [$e->{refsource_no}, $e->{reference_no}, $e->{source},
# 		     $e->{eventtype}, $e->{eventtime}, $e->{status}, $e->{data} ? 'yes' : 'no'];
#     }
    
#     print_table(@rows);
# }
    

# sub get_event_content {
    
#     my ($r, $e, $which) = @_;
    
#     # If there isn't any data, print an error message and exit.
    
#     unless ( $e && ref $e eq 'HASH' )
#     {
# 	if ( ref $r && $r->{reference_no} )
# 	{
# 	    print STDERR "No fetched data found for refno $r->{reference_no}";
# 	    return;
# 	}

# 	else
# 	{
# 	    print STDERR "No refno found";
# 	    return;
# 	}
#     }
    
#     my ($data, @items);
    
#     unless ( $e->{response_data} )
#     {
# 	print STDERR "ERROR: no response data found in event $e->{refsource_no} ($e->{reference_no})\n";
# 	return;
#     }
    
#     eval {
# 	$data = decode_json($e->{response_data});
#     };
    
#     if ( $@ )
#     {
# 	print STDERR "An error occurred while decoding \@$e->{refsource_no}: $@\n";
# 	return;
#     }
    
#     if ( ref $data eq 'HASH' && ref $data->{message}{items} eq 'ARRAY' )
#     {
# 	@items = @{$data->{message}{items}};
#     }

#     elsif ( ref $data eq 'ARRAY' && ( $data->[0]{deposited} || $data->[0]{title} ) )
#     {
# 	@items = @$data;
#     }

#     if ( $which eq 'all' )
#     {
# 	return @items;
#     }

#     elsif ( $which > 0 )
#     {
# 	return $items[$which-1];
#     }

#     else
#     {
# 	return $items[0];
#     }
# }


# sub printout_event_attrs {
    
#     my ($e) = @_;
    
#     my @rows = ['id', 'refno', 'source', 'eventtype', 'eventtime', 'status'];
    
#     push @rows, [$e->{refsource_no}, $e->{reference_no}, $e->{source},
# 		 $e->{eventtype}, $e->{eventtime}, $e->{status}];
    
#     print_table(@rows);
    
#     print "\n";
# }


# sub printout_item_formatted {
    
#     my ($r) = @_;
    
#     print encode_utf8($rs->format_ref($r));
#     print "\n\n";
# }


# sub printout_item_source {
    
#     my ($i) = @_;
    
#     print JSON->new->pretty->utf8->encode($i);
#     print "\n";
# }


# sub printout_item_scores {
    
#     my ($scores) = @_;
    
#     foreach my $key ( qw(title pub auth1 auth2 pubyr volume pages pblshr) )
#     {
# 	my $key1 = $key . '_s';
# 	my $key2 = $key . '_c';
# 	my $line = sprintf("%-15s %5d %5d\n", $key, $scores->{$key1}, $scores->{$key2});
	
# 	print $line;
#     }
    
#     print "\n";
# }


# sub format_scores_horizontal {

#     my ($scores) = @_;
    
#     my $line1 = "stat      ";
#     my $line2 = "similar   ";
#     my $line3 = "conflict  ";

#     foreach my $key ( qw(complete count sun title pub auth1 auth2 pubyr volume pages pblshr) )
#     {
# 	my $key1 = $key . '_s';
# 	my $key2 = $key . '_c';
# 	$line1 .= fixed_width($key, 10);
# 	$line2 .= fixed_width($scores->{$key1}, 10);
# 	$line3 .= fixed_width($scores->{$key2}, 10);
#     }

#     return "$line1\n\n$line2\n$line3\n";
# }


# sub fixed_width {
    
#     return $_[0] . (' ' x ($_[1] - length($_[0])));
# }


sub print_table (@) {
    
    my $options = ref $_[0] eq 'HASH' ? shift @_ : { };
    
    my ($header, @body) = @_;
    
    my $columnpad = $options->{pad} // 5;
    
    my $outfh = $options->{outfh} || $TERM->OUT;
    
    # If the 'noformat' option was given, just print out lines of tab-separated fields.
    
    # if ( $options->{noformat} )
    # {
    # 	foreach my $i ( 0..$linelimit )
    # 	{
    # 	    print join "\t", map { $_->[$i] // '' } @columns;
    # 	    print "\n";
    # 	}

    # 	return;
    # }
    
    # Otherwise, print formatted output.
    
    my (@width, @entrywidth, @separator, $format);
    
    # Start by computing column widths.

    foreach my $c ( 0..$#$header )
    {
	$width[$c] = string_width($header->[$c]);
	$separator[$c] = '-' x $width[$c];
    }
    
    foreach my $row ( $header, @body )
    {
	foreach my $c ( 0..$#$row )
	{
	    my $this_width = string_width($row->[$c]);
	    if ( ! $width[$c] || $this_width > $width[$c] )
	    {
		$width[$c] = $this_width;
	    }
	}
    }
    
    # # Create a format string.
    
    # $format = '%s';
    
    # foreach my $c ( 0..$#columns )
    # {
    #     my $mod = $options->{format}[$c] && $options->{format}[$c] =~ /R/ ? '' : '-';
    #     $format .= "%$mod$width[$c]s  ";
    # }
    
    # $format =~ s/\s*$/\n/;
    
    # print "format: $format\n" if $DEBUG;
    
    # If we were given a header list, print out the header followed by a separator line.
    
    if ( ref $header eq 'ARRAY' && @$header )
    {
	# print sprintf($format, '', @$header);
	# print sprintf($format, '', @separator);
	
	PrintLine(\@width, $columnpad, @$header);
	PrintLine(\@width, $columnpad, @separator);
    }
    
    # Print out the data lines.
    
    foreach my $row ( @body )
    {
	# print sprintf($format, '', map { $_->[$i] // '' } @columns);
	PrintLine(\@width, $columnpad, @$row);
    }
    
    sub PrintLine {

	my ($widths, $columnpad, @fields) = @_;
	
	foreach my $j ( 0..$#fields )
	{
	    my $data = $fields[$j];
	    my $fieldwidth = $widths->[$j];
	    my $datawidth = string_width($data);
	    my $pad = $datawidth < $fieldwidth ? $fieldwidth - $datawidth : 0;
	    $pad += $columnpad if $j < $#fields;
	    
	    print $outfh $data . (" " x $pad);
	}
	
	print $outfh "\n";
    }
}


sub string_width {
    
    my ($string) = @_;
    
    return 0 unless defined $string && $string ne '';
    $string =~ s/\033\[[\d;]+m//g;
    return length($string);
}


# # # If --fulltitle was specified, do a full text search on each nonempty reftitle.

# # elsif ( $opt_fulltitle )
# # {
# #     read_input();
# #     fulltitle_proc();
# # }

# # # If --fullpub was specified, the same on each nonempty pubtitle.

# # elsif ( $opt_fullpub )
# # {
# #     read_input();
# #     fullpub_proc();
# # }

# # exit;


# sub ref_from_args {
    
#     my ($arg) = @_;
    
#     my $ref = { };
    
#     while ( $arg =~ qr{ ^ (\w+) [:] \s* (.*?) (?= \w+ [:] | $) (.*) }xs )
#     {
# 	my $field = $1;
# 	my $value = $2;
# 	$arg = $3;
	
# 	$value =~ s/\s+$//;
	
# 	if ( $field eq 'author' || $field eq 'au' )
# 	{
# 	    my $key = $ref->{author1last} ? 'author2last' : 'author1last';
# 	    $ref->{$key} = $value;
# 	}
	
# 	elsif ( $field eq 'author1' || $field eq 'a1' || $field eq 'author1last' )
# 	{
# 	    $ref->{author1last} = $value;
# 	}
	
# 	elsif ( $field eq 'author2' || $field eq 'a2' || $field eq 'author2last' )
# 	{
# 	    $ref->{author2last} = $value;
# 	}
	
# 	elsif ( $field eq 'title' || $field eq 'ti' || $field eq 'reftitle' )
# 	{
# 	    $ref->{reftitle} = $value;
# 	}
	
# 	elsif ( $field eq 'pub' || $field eq 'pu' || $field eq 'pubtitle' || $field eq 'publication' )
# 	{
# 	    $ref->{pubtitle} = $value;
# 	}
	
# 	elsif ( $field eq 'pubyr' || $field eq 'py' || $field eq 'year' )
# 	{
# 	    $ref->{pubyr} = $value;
# 	}
	
# 	elsif ( $field eq 'pubtype' || $field eq 'ty' || $field eq 'type' || $field eq 'publication_type' )
# 	{
# 	    $ref->{pubtype} = $value;
# 	}

# 	elsif ( $field eq 'label' || $field eq 'lb' )
# 	{
# 	    $ref->{label} = $value;
# 	}
#     }
    
#     if ( $arg )
#     {
# 	print "WARNING: unparsed remainder '$arg'\n\n";
#     }
    
#     return $ref;
# }


# sub ref_from_refno {
    
#     my ($dbh, $reference_no) = @_;
    
#     return unless $reference_no && $reference_no =~ /^\d+$/;
    
#     my $sql = "SELECT * FROM $TABLE{REFERENCE_DATA} WHERE reference_no = $reference_no";

#     print STDERR "$sql\n\n" if $opt_debug;
    
#     my $result = $dbh->selectrow_hashref($sql);
    
#     return $result && $result->{reference_no} ? $result : ();
# }


# sub ref_from_sourceno {

#     my ($dbh, $refsource_no) = @_;

#     return unless $refsource_no && $refsource_no =~ /^\d+$/;
    
#     my $sql = "SELECT r.*, s.refsource_no
# 	FROM $TABLE{REFERENCE_DATA} as r join $TABLE{REFERENCE_SOURCES} as s using (reference_no)
# 	WHERE s.refsource_no = $refsource_no";
    
#     print STDERR "$sql\n\n" if $opt_debug;
    
#     my $result = $dbh->selectrow_hashref($sql);
    
#     return $result && $result->{reference_no} ? $result : ();
# }
    

# # find_matches ( reference_attrs )
# # 
# # Return a list of matches for the specified reference attributes in the REFERENCE_DATA (refs)
# # table. The attributes must be given as a hashref.

# sub ref_match {
    
#     my ($dbh, $r) = @_;
    
#     my @matches;
    
#     # If a doi was given, find all references with that doi. Compare them all to the given
#     # attributes; if no other attributes were given, each one gets a score of 90 plus the number
#     # of important attributes with a non-empty value. The idea is to select the matching reference
#     # record that has the greatest amount of information filled in.
    
#     if ( $r->{doi} )
#     {
# 	my $quoted = $dbh->quote($r->{doi});
	
# 	my $sql = "SELECT * FROM refs WHERE doi = $quoted";
	
# 	print STDERR "$sql\n\n" if $opt_debug;
	
# 	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
# 	@matches = @$result if $result && ref $result eq 'ARRAY';
	
# 	# Assign match scores.
	
# 	foreach my $m ( @matches )
# 	{
# 	    my $score = match_score($r, $m);

# 	    $m->{score} = $score;
# 	}	
#     }
    
#     # If no doi was given or if no references with that doi were found, look for references that
#     # match some combination of reftitle, pubtitle, pubyr, author1last, author2last.
    
#     unless ( @matches )
#     {
# 	my $base;
# 	my $having;

# 	# If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
	
# 	if ( $r->{reftitle} )
# 	{
# 	    my $quoted = $dbh->quote($r->{reftitle});
	    
# 	    $base = "SELECT refs.*, match(refsearch.reftitle) against($quoted) as score
# 		FROM refs join refsearch using (reference_no)";
	    
# 	    $having = "score > 5";
# 	}
	
# 	elsif ( $r->{pubtitle} )
# 	{
# 	    my $quoted = $dbh->quote($r->{pubtitle});
	    
# 	    $base = "SELECT refs.*, match(refsearch.pubtitle) against($quoted) as score
# 		FROM refs join refsearch using (reference_no)";
	    
# 	    $having = "score > 0";
# 	}
	
# 	else
# 	{
# 	    $base = "SELECT * FROM refs";
# 	}
	
# 	# Then add clauses to restrict the selection based on pubyr and author names.
	
# 	my @clauses;
	
# 	if ( $r->{pubyr} )
# 	{
# 	    my $quoted = $dbh->quote($r->{pubyr});
# 	    push @clauses, "refs.pubyr = $quoted";
# 	}
	
# 	if ( $r->{author1last} && $r->{author2last} )
# 	{
# 	    my $quoted1 = $dbh->quote($r->{author1last});
# 	    my $quoted2 = $dbh->quote($r->{author2last});
	    
# 	    push @clauses, "(refs.author1last sounds like $quoted1 and refs.author2last sounds like $quoted2)";
# 	}
	
# 	elsif ( $r->{author1last} )
# 	{
# 	    my $quoted1 = $dbh->quote($r->{author1last});
	    
# 	    push @clauses, "refs.author1last sounds like $quoted1";
# 	}

# 	if ( $r->{anyauthor} )
# 	{
# 	    my $quoted1 = $dbh->quote($r->{anyauthor});
# 	    my $quoted2 = $dbh->quote('%' . $r->{anyauthor} . '%');
	    
# 	    push @clauses, "(refs.author1last sounds like $quoted1 or refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
# 	}
	
# 	# Now put the pieces together into a single SQL statement and execute it.
	
# 	my $sql = $base;
	
# 	if ( @clauses )
# 	{
# 	    $sql .= "\n\t\tWHERE " . join(' and ', @clauses);
# 	}
	
# 	if ( $having )
# 	{
# 	    $sql .= "\n\t\tHAVING $having";
# 	}
	
# 	print STDERR "$sql\n\n" if $opt_debug;
	
# 	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
# 	# If we get results, look through them and keep any that have even a slight chance of
# 	# matching.
	
# 	if ( $result && ref $result eq 'ARRAY' )
# 	{
# 	    foreach my $m ( @$result )
# 	    {
# 		my $score = match_score($r, $m);
		
# 		if ( $score > 20 )
# 		{
# 		    $m->{score} = $score;
# 		    push @matches, $m;
# 		}
# 	    }
# 	}
#     }
    
#     # Now sort the matches in descending order by score.
    
#     my @sorted = sort { $b->{score} <=> $a->{score} } @matches;
    
#     return @sorted;
# }


# our ($PCHARS);

# sub Progress {

#     my ($message) = @_;
    
#     if ( $PCHARS )
#     {
# 	print STDOUT chr(8) x $PCHARS;
#     }
    
#     print STDOUT $message;
#     $PCHARS = length($message);
# }




	# my $refcount;
	
	# while (<>)
	# {
	#     chomp;
	#     next unless $_ =~ /[[:alnum:]]{3}/;
	    
	#     $refcount++;
	    
	#     my $r = ref_from_args($_);
	    
	#     my @matches = ref_match($dbh, $r);

	#     my $matchcount = scalar(@matches);
	#     my $matchphrase = $matchcount == 1 ? "1 match" : "$matchcount matches";
	    
	#     print "Reference $r: $matchphrase\n";
	    
	#     foreach my $i ( 0..$#matches )
	#     {
	# 	my $m = $matches[$i];
	# 	my $r = $m->{label} || $refcount;
	# 	my $n = $i + 1;
	# 	my $s = $m->{score} || 'no score';
		
	# 	print "  Match $n: [$s]\n\n";
		
	# 	print format_ref($m, '    ') . "\n\n";
	#     }
	# }

	# unless ( $refcount )
	# {
	#     print "You must specify at least one reference either on the command line\nor through standard input.\n\n";
	#     exit(2);
	# }



# sub read_input {
    
#     while (<>)
#     {
# 	$_ =~ s/[\n\r]+$//;
# 	my @cols = split /\t/;
	
# 	unless ( $LINE_NO++ )
# 	{
# 	    /reference_no/ || die "The first line must be a list of field names.\n";
# 	    @FIELD_LIST = @cols;
# 	    next;
# 	}
	
# 	my $r = { };
	
# 	foreach my $i ( 0..$#cols )
# 	{
# 	    $r->{$FIELD_LIST[$i]} = $cols[$i];
# 	}
	
# 	my $reference_no = $r->{reference_no};
	
# 	$REF{$reference_no} = $r;
# 	push @REF_LIST, $reference_no;
	
# 	if ( ! ( $LINE_NO % 100 ) )
# 	{
# 	    Progress($LINE_NO);
# 	}
#     }
    
#     Progress('');
#     print STDOUT "Read $LINE_NO lines.\n";
# }


# sub read_table {

#     my $sql = "SELECT * FROM refsearch";
    
#     my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
#     my $lines;
    
#     if ( $result && @$result )
#     {
# 	foreach my $r ( @$result )
# 	{
# 	    my $reference_no = $r->{reference_no};
# 	    $REF{$reference_no} = $r;
# 	    push @REF_LIST, $reference_no;
	    
# 	    if ( ! ( ++$lines % 100 ) )
# 	    {
# 		Progress($lines);
# 	    }
# 	}
	
# 	Progress('');
# 	print STDOUT "Read $lines lines.\n";
#     }

#     else
#     {
# 	print "No data read.\n";
#     }
# }


# sub fulltitle_proc {
    
#     my $reftitle_sth = $dbh->prepare("
# 	SELECT reference_no, reftitle, match(reftitle) against (?) as score
# 	FROM refsearch HAVING score > 0 ORDER BY score desc LIMIT 3");
    
#     my $refupdate_sth = $dbh->prepare("
# 	UPDATE refsearch SET selfmatch = ?, maxmatch = ?, match_no = ?
# 	WHERE reference_no = ? LIMIT 1");
    
#     my $count;
    
#     foreach my $ref_no ( @REF_LIST )
#     {
# 	$count++;
# 	my $reftitle = $REF{$ref_no}{reftitle};

# 	next if $REF{$ref_no}{maxmatch};
	
# 	if ( $reftitle )
# 	{
# 	    $reftitle_sth->execute($reftitle);
	    
# 	    my $result = $reftitle_sth->fetchall_arrayref({ });
	    
# 	    if ( $result && @$result )
# 	    {
# 		$MATCH{$ref_no} = $result;
		
# 		foreach my $r ( @$result )
# 		{
# 		    if ( $r->{reference_no} eq $ref_no )
# 		    {
# 			$REF{$ref_no}{selfmatch} = $r->{score};
# 		    }

# 		    elsif ( $r->{score} && ( ! $REF{$ref_no}{maxmatch} ||
# 					     $r->{score} > $REF{$ref_no}{maxmatch} ) )
# 		    {
# 			$REF{$ref_no}{maxmatch} = $r->{score};
# 			$REF{$ref_no}{match_no} = $r->{reference_no};
# 		    }

# 		    else
# 		    {
# 			last;
# 		    }
# 		}

# 		my $result = $refupdate_sth->execute($REF{$ref_no}{selfmatch}, $REF{$ref_no}{maxmatch},
# 						     $REF{$ref_no}{match_no}, $ref_no);

# 		my $a = 1; # we can stop here when debugging
# 	    }
# 	}
	
# 	Progress($count) unless $count % 100;
#     }

#     Progress('');
#     print STDOUT "Queried for $count entries.\n";
# }


# sub fullpub_proc {
    
#     my $pubtitle_sth = $dbh->prepare("
# 	SELECT match(pubtitle) against (?) as score
# 	FROM refsearch WHERE reference_no = ?");
    
#     my $refupdate_sth = $dbh->prepare("
# 	UPDATE refsearch SET pselfmatch = ? WHERE reference_no = ? LIMIT 1");
    
#     my $count;
    
#     foreach my $ref_no ( @REF_LIST )
#     {
# 	$count++;
# 	my $pubtitle = $REF{$ref_no}{pubtitle};
	
# 	next if $REF{$ref_no}{pselfmatch};
	
# 	if ( $pubtitle )
# 	{
# 	    $pubtitle_sth->execute($pubtitle, $ref_no);
	    
# 	    my ($score) = $pubtitle_sth->fetchrow_array();
	    
# 	    if ( $score )
# 	    {
# 		$REF{$ref_no}{pselfmatch} = $score;
# 	    }
	    
# 	    my $result = $refupdate_sth->execute($score, $ref_no);
	    
# 	    my $a = 1; # we can stop here when debugging
# 	}
	
# 	Progress($count) unless $count % 100;
#     }
    
#     Progress('');
#     print STDOUT "Queried for $count entries.\n";
# }


# sub match_doi {

#     my ($dbh, $r) = @_;
    
#     if ( $r->{doi} )
#     {
# 	my $quoted = $dbh->quote($r->{doi});
	
# 	my $sql = "SELECT * FROM refs WHERE doi = $quoted";
	
# 	print STDERR "$sql\n\n" if $opt_debug;
	
# 	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
# 	return @$result if $result && ref $result eq 'ARRAY';
#     }
    
#     return;	# otherwise
# }


sub ask_value {

    my ($name, $type) = @_;

    while (1)
    {
	print "Enter a value for '$name': ";
	my $val = <STDIN>;
	chomp $val;
	
	return unless defined $val && $val ne '';
	
	if ( $type eq 'posint' )
	{
	    unless ( $val =~ /^\d+$/ )
	    {
		print "  The value must be a positive integer.\n";
		next;
	    }
	}

	return $val;
    }
}


sub print_msg ($) {
    
    my ($msg) = @_;
    
    print "\n$msg\n\n";
}


sub print_msgs (@) {
    
    print "\n";
    print "$_\n" foreach @_;
    print "\n";
}


sub print_line (@) {
    
    my (@args) = @_;
    
    print @args, "\n";
}


# load_state ( filename )
#
# Load the application state from the specified filename.

sub load_state {

    my ($filename) = @_;

    unless ( -e $filename )
    {
	print_msg "Application state is empty, $filename not found\n";
	return;
    }
    
    unless ( -r $filename )
    {
	print_msg "Application state is empty, cannot read $filename: $!";
	return;
    }
    
    # Retrieve the data from $filename using Storable.
    
    my $SAVED = retrieve($filename);
    
    # Load the following variables directly from the retrieved data. If they contain obsolete
    # entries, just leave 'em around in case I decide to re-enable those settings.
    
    %SETTINGS = %{$SAVED->{SETTINGS}} if ref $SAVED->{SETTINGS} eq 'HASH';
    %DEBUG = %{$SAVED->{DEBUG}} if ref $SAVED->{DEBUG} eq 'HASH';
    %VARS = %{$SAVED->{VARS}} if ref $SAVED->{VARS} eq 'HASH';
    @ACTIVE = @{$SAVED->{ACTIVE}} if ref $SAVED->{ACTIVE} eq 'ARRAY';
    @INACTIVE = @{$SAVED->{INACTIVE}} if ref $SAVED->{INACTIVE} eq 'ARRAY';
    @DBHISTORY = @{$SAVED->{DBHISTORY}} if ref $SAVED->{DBHISTORY} eq 'ARRAY';
    @MAINHISTORY = @{$SAVED->{MAINHISTORY}} if ref $SAVED->{MAINHISTORY} eq 'ARRAY';
    
    # The selection, on the other hand, is a dynamic data structure that needs to be emptyable. It
    # shouldn't contain anything not understood by the current version of the code. So we
    # specifically load those sub-variables we know about, and ignore everything else.
    
    foreach my $var ( qw(SLOT EXPR SQL COUNT COUNT_POS COUNT_NEG
			 MATCHES MATCH_TYPE MATCH_INDEX RESTRICTION OFFSET_TYPE OFFSET) )
    {
	$SELECTION{$var} = $SAVED->{SELECTION}{$var} if defined $SAVED->{SELECTION}{$var};
    }
    
    # # The terminal history is saved in reverse because we only want the last 50 lines (or the
    # # number specified by the 'history' setting). So we need to reverse the list before
    # # loading it into $TERM.
    
    # if ( ref $SAVED->{HISTORY} eq 'ARRAY' )
    # {
    # 	foreach my $h ( reverse @{$SAVED->{HISTORY}} )
    # 	{
    # 	    $TERM->add_history($h);
    # 	}
    # }
}


# save_state ( filename )
# 
# Save the current application state to the specified filename. This subroutine should be called
# just before a graceful exit.

sub save_state {
    
    my ($filename) = @_;
    
    # Construct a single record to hold the state.
    
    my $SAVED = { SETTINGS => \%SETTINGS,
		  DEBUG => \%DEBUG,
		  VARS => \%VARS,
		  ACTIVE => \@ACTIVE,
		  INACTIVE => \@INACTIVE,
		  SELECTION => \%SELECTION,
		  MAINHISTORY => [ ],
		  DBHISTORY => [ ] };
    
    # Add the most recent items from the Term::ReadLine history, but skip all single-character
    # commands. If not set, the limit defaults to 50.
    
    my $history_limit = $SETTINGS{history} > 0 ? $SETTINGS{HISTORY} : 50;
    
    if ( $HSEL eq 'main' )
    {
	@MAINHISTORY = $TERM->GetHistory;
	$SAVED->{DBHISTORY}->@* = @DBHISTORY;
    }
    
    elsif ( $HSEL eq 'db' )
    {
	$SAVED->{DBHISTORY}->@* = $TERM->GetHistory;
    }
    
    foreach my $line ( reverse @MAINHISTORY )
    {
	if ( defined $line && length($line) > 1 )
	{
	    unshift $SAVED->{MAINHISTORY}->@*, $line;
	    last if $SAVED->{MAINHISTORY}->@* >= $history_limit;
	}
    }
    
    store($SAVED, $filename);
}


sub dbhist {

    if ( $HSEL ne 'db' && $TERM )
    {
	@MAINHISTORY = $TERM->GetHistory;
	$TERM->SetHistory(@DBHISTORY);
	$HSEL = 'db';
    }
}


sub mainhist {

    if ( $HSEL ne 'main' )
    {
	@DBHISTORY = $TERM->GetHistory;
	$TERM->SetHistory(@MAINHISTORY);
	$HSEL = 'main';
    }
}


