#
# This Perl module provides high level functions for querying a database through
# DBI.
#
# Written by: Michael McClennen
# Created: 2025-10-30


package DBQuery;

use strict;
use parent 'Exporter';
use Carp qw(croak);
use feature 'say';

our (@EXPORT_OK) = qw(DebugMode CheckMode
		      DBHashQuery DBArrayQuery DBRowQuery DBColumnQuery
		      DBSingleHashQuery DBTextQuery
		      DBCommand DBInsert);

our ($DEBUG_MODE, $CHECK_MODE);


# DebugMode (arg)
#
# If an argument is given, set the module variable $DEBUG_MODE true or false. If
# true, each query or command will be printed to STDERR before it is executed,
# with two newlines appended. If no argument is given, return the value of
# $DEBUG_MODE.

sub DebugMode {

    if ( @_ ) { $DEBUG_MODE = ($_[0] ? 1 : ''); }
    else { return $DEBUG_MODE; }
}


# CheckMode (arg)
#
# If an argument is given, set the module variable $CHECK_MODE to true or false.
# If true, calls to DBCommand will not be executed. Each command will be printed
# out if the arguments call for that, but they will not be passed on to DBI.
# This is intended for use by calling scripts to inform the user of a series of
# commands that would be executed if check mode were not on. If no argument is
# given, return the value of $CHECK_MODE.

sub CheckMode {

    if ( @_ ) { $CHECK_MODE = ($_[0] ? 1 : ''); }
    else { return $CHECK_MODE; }
}


# DBHashQuery (dbh, query)
#
# Send the specified query to the database using the DBI selectall_arrayref
# method, with the argument necessary to return each row as a hash slice. If an
# error occurs, throw an exception using Carp. Otherwise, this subroutine is
# guaranteed to return an arrayref.

sub DBHashQuery {
    
    my ($dbh, $query) = @_;

    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my $dbresult = eval { $dbh->selectall_arrayref($query, { Slice => { } }) };
    
    if ( $@ )
    {
	say STDERR $query unless $DEBUG_MODE;
	croak "$@\n";
    }
    
    elsif ( ref $dbresult eq 'ARRAY' )
    {
	return $dbresult;
    }
    
    else
    {
	return [ ];
    }
}


# DBArrayQuery (dbh, query)
#
# Send the specified query to the database using the DBI selectall_arrayref
# method. If an error occurs, throw an exception using Carp. Otherwise, this
# subroutine is guaranteed to return an arrayref.

sub DBArrayQuery {
    
    my ($dbh, $query) = @_;

    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my $dbresult = eval { $dbh->selectall_arrayref($query) };

    if ( $@ )
    {
	croak $@;
    }
    
    if ( ref $dbresult eq 'ARRAY' )
    {
	return $dbresult;
    }
    
    else
    {
	return [ ];
    }
}


# DBRowQuery (dbh, query)
#
# Send the specified query to the database using the DBI selectrow_array method.
# If an error occurs, throw an exception using Carp. Otherwise, return the
# result as a list.

sub DBRowQuery {
    
    my ($dbh, $query) = @_;
    
    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my @dbresult = eval { $dbh->selectrow_array($query) };
    
    if ( $@ )
    {
	croak $@;
    }
    
    else
    {
	return @dbresult;
    }
}


# DBColumnQuery (dbh, query)
#
# Send the specified query to the database using the DBI selectcol_arrayref
# method. If an error occurs, throw an exception using Carp. Otherwise, return
# the result as a list. Note: the result is NOT a listref unless the third
# argument is 'ref'.

sub DBColumnQuery {
    
    my ($dbh, $query, $modifier) = @_;
    
    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my $dbresult = eval { $dbh->selectcol_arrayref($query) };
    
    if ( $@ )
    {
	croak $@;
    }
    
    elsif ( $modifier && $modifier eq 'ref' )
    {
	return ref $dbresult ? $dbresult : [];
    }
    
    elsif ( ref $dbresult )
    {
	return @$dbresult;
    }

    else
    {
	return ();
    }
}


# DBSingleHashQuery (dbh, query, key_column)
#
# Send the specified query to the database using the DBI selectall_hashref
# method. If an error occurs, throw an exception using Carp. Otherwise, return
# the result as a hash. Note: the result is NOT a hashref unless the fourth
# argument is 'ref'.

sub DBSingleHashQuery {

    my ($dbh, $query, $key_field, $modifier) = @_;

    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my $dbresult = eval { $dbh->selectall_hashref($query, $key_field) };
    
    if ( $@ )
    {
	croak $@;
    }
    
    my $test = (values $dbresult->%*)[0];
    my @columns = grep { $_ ne $key_field } keys $test->%*;
    
    if ( @columns == 1 )
    {
	foreach my $key ( keys $dbresult->%* )
	{
	    $dbresult->{$key} = $dbresult->{$key}{$columns[0]}
	}
    }
    
    if ( $modifier && $modifier eq 'ref' )
    {
	return ref $dbresult ? $dbresult : {};
    }
    
    elsif ( ref $dbresult )
    {
	return %$dbresult;
    }

    else
    {
	return ();
    }
}


# DBTextQuery (dbh, query)
#
# Send the specified query to the database using the DBI selectall_arrayref
# method. If an error occurs, throw an exception using Carp. Otherwise, return
# the result as a text string with the rows terminated by newlines and the
# columns separated by tabs.

sub DBTextQuery {
    
    my ($dbh, $query) = @_;
    
    if ( $DEBUG_MODE )
    {
	$query =~ s/^\s+//;
	say STDERR "$query\n";
    }
    
    my $textresult = '';
    
    my $dbresult = eval { $dbh->selectall_arrayref($query) };
    
    if ( $@ )
    {
	croak $@;
    }
    
    elsif ( ref $dbresult eq 'ARRAY' )
    {
	foreach my $row ( @$dbresult )
	{
	    $textresult .= join("\t", @$row) . "\n";
	}
    }
    
    return $textresult;
}


# DBCommand (dbh, command, say_it)
#
# Execute the specified database command using the DBI do method. Any
# backslashes will be converted to double backslashes, which preserves their
# function. Any newlines at the end of the command will be removed. If the third
# argument is true and $DEBUG_MODE is false, the command will be printed to
# STDOUT, followed by a newline.

sub DBCommand {
    
    my ($dbh, $command, $say_it) = @_;
    
    $command =~ s/[\n\r]*$//;

    if ( $say_it )
    {
	say $command;
    }
    
    unless ( $CHECK_MODE )
    {
	if ( $DEBUG_MODE && !$say_it )
	{
	    say STDERR "$command\n";
	}
	
	my $dbresult = eval { $dbh->do($command) };
	
	if ( $@ )
	{
	    say STDERR $command unless $DEBUG_MODE;
	    croak "$@\n";
	}
	
	elsif ( defined $dbresult && $dbresult ne '' && $say_it )
	{
	    say "Changed $dbresult rows.\n";
	}
	
	return $dbresult;
    }
}


sub DBInsert {
    
    my ($dbh, $command, $say_it) = @_;
    
    $command =~ s/[\n\r]*$//;

    if ( $say_it )
    {
	say $command;
    }
    
    unless ( $CHECK_MODE )
    {
	if ( $DEBUG_MODE && !$say_it )
	{
	    say STDERR "$command\n";
	}
	
	my $dbresult = eval { $dbh->do($command) };
	
	if ( $@ )
	{
	    say STDERR $command unless $DEBUG_MODE;
	    croak "$@\n";
	}
	
	elsif ( defined $dbresult && $dbresult ne '' && $say_it )
	{
	    say "Changed $dbresult rows.\n";
	}
	
	return $dbh->last_insert_id();
    }
}


1;
