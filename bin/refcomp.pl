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

our (%HELPMSG, %HELPTEXT, @HELPLIST, @TOPICLIST);

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
print_msg "Application state saved.";
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

BEGIN { push @HELPLIST, 'debug';
$HELPMSG{debug} = "Set or clear a debugging flag.";
$HELPTEXT{debug} = <<HelpDebug;

Usage: debug <flag> | debug no<flag>

To set a debugging flag, execute 'debug <flag>'. To clear it, execute 
'debug no<flag>'.

Available flags include:

  sql       Display the SQL statement(s) that are generated in the process
            of selecting records.

  scoring   Stop at a breakpoint before any reference is scored.

  display   Stop at a breakpoint before any formatted reference is displayed.

  test      Stop at a breakpoint before each execution of a 'test' command.

HelpDebug
};

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

BEGIN { push @HELPLIST, 'set';
$HELPMSG{set} = "Change the value of a setting.";
$HELPTEXT{set} = <<HelpSet;

Usage: set <setting> <value> | set <setting>=<value>

The following settings are available:

  source    Change the default source. Values are: xdd, crossref, all.

  color     Turn color output on or off. Values are: on, off.

  history   Set the maximum number of history lines to save.

HelpSet
};

sub do_set {
    
    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    if ( $argstring =~ qr{ ^ (\w+) (?: \s+ | \s* = \s* ) (.+) }xs )
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

BEGIN { push @HELPLIST, 'show';
$HELPMSG{show} = "Display setting values and other information.";
$HELPTEXT{show} = <<HelpShow;

Usage: show <argument>

Accepted arguments are:

  selection   Displays the current selection expression and number of records
              selected.

  expr        Displays the table of active selections. If 'sql' is appended,
              displays the expanded SQL statement corresponding to each one.

  vars        Displays the table of active variable values.

  debug       Displays the values of the debugging flags.

  source      Displays the default source for scoring information.

  color       Displays whether color output is on or off.

  history     Displays the maximum number of history lines to save.

HelpShow
};

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

BEGIN { push @HELPLIST, 'select';
$HELPMSG{select} = "Select records according to a specified expression.";
$HELPTEXT{select} = <<HelpSelect;

Usage: select <n> | select E<n> | select '<name> | select . | select <expr>

The first two forms select records using the nth entry in the active expression table. You
can list these using the 'show expr' command. The next form uses the first entry whose
name matches the argument. You can use * as a wildcard. The third re-computes the current
selection.

Otherwise, you may specify an expression directly. The syntax is given under the help
topic 'selecting'.

HelpSelect
};

sub do_select {

    my ($command, $argstring) = @_;
    
    $DB::single = 1 if $STOP;
    
    # If the expression is empty, there is nothing to add.

    unless ( defined $argstring && ($argstring =~ /[A-Za-z0-9]/ || $argstring eq '.' ) )
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

BEGIN { push @HELPLIST, 'clear';
$HELPMSG{clear} = "Clear the current selection.";
$HELPTEXT{clear} = <<HelpClear;

Usage: clear

After this command is executed, the selection will be empty.

HelpClear
};

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


# do_list ( argstring )
#
# Carry out the 'list' operation, listing score records that match the current selection
# expression. Depending on the arguments and the current position in the list, either continue
# where the previous 'list' operation left off, or fetch another batch of records and start
# listing them, or start from the beginning of the list of positive matches, negative matches,
# unassigned matches, or all matches.

BEGIN { push @HELPLIST, 'list';
$HELPMSG{list} = "List records from the current selection.";
$HELPTEXT{list} = <<HelpList;

Usage: list | list <match_type> | list @<n>[,<n>...] | list %<n>[,<n>...]

If no arguments are given, list another batch of records from the current selection using
the current match type. If a match type is given and it is different from the current one,
list a new batch of records using the specified match type. The match type can be
abbreviated to the first letter. The match types are:

  pos     List records from the selection where the match has been accepted.
  neg     List records from the selection where the match has been rejected.
  un      List records from the selection that are still unevaluated.
  all     List a random batch of records from the selection.

If the argument starts with @, select the records with the specified refsource numbers.

If the argument starts with %, select the records with the specified reference numbers.

HelpList
};

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
    
    # If the argument is 'm' or 'more', empty the current batch of records so that a new random
    # batch will be fetched. If the current batch is already empty, there is no need to print a
    # message to the user.
    
    elsif ( $argstring =~ qr{ ^ m (ore|$) }xsi )
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
    
    elsif ( $argstring =~ qr{ ^ n (eg|egative|) $ }xsi )
    {
	set_selection_matches('n', 0) unless $SELECTION{MATCH_TYPE} eq 'n';
    }

    elsif ( $argstring =~ qr{ ^ p (os|ositive|) $ }xsi )
    {
	set_selection_matches('p', 0) unless $SELECTION{MATCH_TYPE} eq 'p';
    }
    
    elsif ( $argstring =~ qr{ ^ u (n|na|nassigned|) $ }xsi )
    {
	set_selection_matches('u', 0) unless $SELECTION{MATCH_TYPE} eq 'u';
    }
    
    elsif ( $argstring =~ qr{ ^ a (ll|) $ }xsi )
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
	    $posneg = "";
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


# do_add ( name )
#
# Add the current expression to the ACTIVE table with the given name. If we already have a
# selected slot, change the name.

BEGIN { push @HELPLIST, 'add';
$HELPMSG{add} = "Add the current selection expression to the active expression table.";
$HELPTEXT{add} = <<HelpAdd;

Usage: add | add <name>

If a name is specified and the current selection is associated with an active expression
entry, you will be asked if you wish to rename that entry. If the current selection
expression is different from that entry's expression, you will be asked if you wish to
substitute it.

If the current selection is not associated with an active expression entry, a new entry
will be created. If a name is specified it will be given that name.

HelpAdd
};

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


# do_delete ( argument )
#
# Delete the specified entry from the active expression table.

BEGIN { push @HELPLIST, 'delete';
$HELPMSG{delete} = "Delete the specified entry from the active expression table.";
$HELPTEXT{delete} = <<HelpDelete;

Usage: delete <n> | delete E<n> | delete '<name> | delete .

The first two forms delete the nth entry from the active expression table. The third form
deletes the first entry whose name matches the argument. You can use * as a wildcard. If
the argument is '.' and the current selection is associated with an active expression
entry, that entry is deleted.

If the entry associated with the current selection is deleted, the selection is cleared.

HelpDelete
};

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
	print_msg "Deleted E$to_delete$label.";

	if ( $to_delete == $SELECTION{SLOT} )
	{
	    set_selection();
	    print_msg "Selection cleared.";
	}
    }
    
    else
    {
	print_msg "Nothing to delete.";
	return;
    }
    
    return;
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

BEGIN { push @TOPICLIST, 'navigation';
$HELPMSG{navigation} = "Commands for navigating the current list.";
$HELPTEXT{navigation} = <<HelpNav;

You can use the following commands to navigate the current list and accept
or reject matches:

  a       Accept the current match and display the next one.
  r       Reject the current match and display the next one.
  s       Re-score and redisplay the current match.
  n       Display the next match.
  p       Display the previous match.
  ^       Display the first match in the current list.
  \$       Display the last match in the current list.
  .       Redisplay the current match.
  0-9     Display the numbered match from the current list.

HelpNav
};

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
    print_line $rs->format_scores_horizontal($m, $color_output);
    print_line "debug:    ", $m->{debugstr}, "\n" if $m->{debugstr};
    print_line "";
    
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
	print_line $rs->format_scores_horizontal($m, $color_output);
	print_line "debug:    ", $m->{debugstr}, "\n" if $m->{debugstr};
	
	print_line "";
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

BEGIN { $HELPTEXT{main} = <<HelpMain;

The purpose of this program is to compare stored bibliographic references with
with data fetched from CrossRef and other sources. Available commands are:

HelpMain
};

sub do_help {

    my ($command, $argstring) = @_;
    
    if ( $HELPTEXT{$argstring} )
    {
	print $HELPTEXT{$argstring};
    }

    elsif ( $argstring )
    {
	print "\nUnrecognized command or topic: $argstring\n\n";
    }
    
    else
    {
	print $HELPTEXT{main};

	foreach my $cmd ( @HELPLIST )
	{
	    print sprintf("  %-10s \%s\n", $cmd, $HELPMSG{cmd});
	}

	if ( @TOPICLIST )
	{
	    print "\n";
	    
	    foreach my $topic ( @TOPICLIST )
	    {
		print sprintf("  %-10s \%s\n", $topic, $HELPMSG{topic});
	    }
	}

	print "\n";
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


