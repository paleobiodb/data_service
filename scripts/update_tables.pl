#!/usr/bin/env perl
# 
# timescale_tables.pl
# 
# Establish or reload the timescale tables.

use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;
use Try::Tiny;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use Permissions;
use TableDefs qw(init_table_names $TEST_DB);
use TableData qw(get_table_schema);


# First parse option switches. We also look for one or two arguments, specifing the new and old
# tables.

my ($opt_copy);
my ($opt_session_id, $opt_actual, $opt_debug, $opt_man, $opt_help, $opt_verbose);

GetOptions("copy-all" => \$opt_copy,
	   "actual" => \$opt_actual,
	   "debug" => \$opt_debug,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);

pod2usage(1) if $opt_help;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;

# Look for an argument indicating a table name. If there is a second argument, it indicates the
# old table name.

my $new_table = shift(@ARGV);
my $old_table = shift(@ARGV);

# If no argument was given at all, 

unless ( $new_table )
{
    pod2usage(2);
}

# If no old table name was given, append _bak to the new one.

unless ( $old_table )
{
    $old_table = "${new_table}_bak";
}


# If we get here, then we have stuff to do. So get a database handle.

my $dbh = connectDB("config.yml");

my $message = $opt_actual ? 'ACTUAL' : 'TEST';

initMessages(2, $message);
logTimestamp();


my $options = { };
$options->{debug} = 1 if $opt_debug;

# Unless we were directed to use the real tables, use the test ones.

my $prefix = '';

unless ( $opt_actual )
{
    init_table_names(configData, 1);
    $prefix = "$TEST_DB.";
}


# Now fetch both table schemas.

my $new_schema = get_table_schema($dbh, $new_table, $opt_debug);
my $old_schema = get_table_schema($dbh, $old_table, $opt_debug);

my @new_columns = @{$new_schema->{_column_list}};
my @old_columns = @{$old_schema->{_column_list}};


# Then go through the list of columns in the new table, and pick out any that need special treatment.

my (@messages, %new_field, %process, %expr);

foreach my $field ( @new_columns )
{
    my $cr = $new_schema->{$field};
    my $type = $new_schema->{$field}{Type};
    
    $new_field{$field} = 1;
    
    unless ( $old_schema->{$field} )
    {
	push @messages, "$field: <missing> => $type";
	$process{$field} = 'IGNORE';
	next;
    }
    
    my $old_type = $old_schema->{$field}{Type};
    
    unless ( $old_type eq $type )
    {
	push @messages, "$field: $old_type => $type";
    }
}


foreach my $field ( @old_columns )
{
    unless ( $new_field{$field} )
    {
	my $old_type = $old_schema->{$field}{Type};
	push @messages, "$field: $old_type => <missing>";
    }
}


unless ( @messages )
{
    push @messages, "All fields copy OK";
}


logMessage(1, "FIELD LIST:");
print "$_\n" foreach @messages;

if ( $opt_copy )
{
    my (@insert_list, @select_list);

    foreach my $field ( @new_columns )
    {
	next if $process{$field} && $process{$field} eq 'IGNORE';

	push @insert_list, $field;
	push @select_list, $field;
    }
    
    my $insert_string = join(', ', @insert_list);
    my $select_string = join(', ', @select_list);

    my $sql = "INSERT INTO $new_table ($insert_string)
	SELECT $select_string FROM $old_table";

    print STDERR "$sql\n\n" if $opt_debug;

    my $count = $dbh->do($sql);

    if ( $count && $count > 0 )
    {
	logMessage(1, "Copied $count records" );
    }

    else
    {
	logMessage(1, "No records were copied." );
    }
}



1;


__END__

=head1 NAME

update_tables.pl - copy data from the old version of a database table to the current one.

=head1 SYNOPSIS

  update_tables [options] <table_name> [<old_name>]
  
  Options:
    
    --help              Display a brief help message
    
    --man               Display the full documentation
    
    --debug             Produce debugging output

    --actual		Update the tables in the real database, not the test one
    
=head1 OPTIONS

To be written later...

=cut


    
