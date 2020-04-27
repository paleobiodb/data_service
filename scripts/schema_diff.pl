#!/usr/bin/env perl
#
# Diff two schema dumps, each produced by "SELECT * from mysql.COLUMNS WHERE ..."
#


use strict;

use lib '../lib', 'lib';
use Getopt::Long;
use Pod::Usage;

use Algorithm::Diff;

my ($opt_help, $opt_man, $opt_verbose, $opt_mt);

GetOptions("missing-tables|mt" => \$opt_mt,
	   "help|h" => \$opt_help,
	   "man" => \$opt_man,
	   "verbose|v" => \$opt_verbose)
    or pod2usage(2);


# Check for documentation requests

pod2usage(1) if $opt_help;
pod2usage(-exitval => 0, -verbose => 2) if $opt_man;


# The following database schema attributes are significant.

our (%USE_FIELD) = ( TABLE_SCHEMA => 1,
		     TABLE_NAME => 1,
		     COLUMN_NAME => 1,
		     ORDINAL_POSITION => 1,
		     COLUMN_DEFAULT => 1,
		     IS_NULLABLE => 1,
		     DATA_TYPE => 1,
		     CHARACTER_SET_NAME => 1,
		     COLLATION_NAME => 1,
		     COLUMN_TYPE => 1,
		     COLUMN_KEY => 1,
		     EXTRA => 1,
		     NON_UNIQUE => 1,
		     INDEX_NAME => 1,
		     INDEX_TYPE => 1,
		     SEQ_IN_INDEX => 1);

# We expect two arguments, each one being the name of an existing file.

my ($fn1, $fn2) = @ARGV;


open(my $infile1, '<', $fn1) || die "Could not open $fn1: $!\n";

open(my $infile2, '<', $fn2) || die "Could not open $fn2: $!\n";


# Now read and process each file.

my $schema1 = { };
my $schema2 = { };

ReadFile($infile1, $schema1);

ReadFile($infile2, $schema2);

Compare($schema1, $schema2);


# Read through an input file, and parse the contents. Multiple header lines are allowed, each
# recognized because it begins with the string TABLE_CATALOG. Each header line defines the field
# map for subsequent data lines until the next header is recognized. Each recognized record is
# added to the specified schema hash, as either a column record or an index record.

sub ReadFile {
    
    my ($infile, $schema) = @_;
    
    my @field_map;
    
    my $last_tn;
    my $last_cn;

    # Read lines from $infile one by one, and remove the linebreak at the end.
    
    while ( my $line = <$infile> )
    {
	chomp $line;

	# If this is a header line, then clear the field map and generate a new one.
	
	if ( $line =~ /^TABLE_CATALOG/ )
	{
	    my @fields = split /\t/, $line;
	    @field_map = ();
	    
	    foreach my $i ( 0..$#fields )
	    {
		$field_map[$i] = $fields[$i] if $USE_FIELD{$fields[$i]};
	    }
	    
	    next;
	}

	# Otherwise, we split the line into fields and process them according to the field map
	# currently in effect. Each line generates one record.
	
	my @values = split /\t/, $line;
	
	my $record = { };
	
	foreach my $i ( 0..$#values )
	{
	    if ( $field_map[$i] )
	    {
		$record->{$field_map[$i]} = $values[$i];
	    }
	}
	
	my $db = $record->{TABLE_SCHEMA};
	my $tn = $record->{TABLE_NAME};
	my $cn = $record->{COLUMN_NAME};
	my $in = $record->{INDEX_NAME};
	
	# If this is an index record, it gets added to the _index part of the schema. The list of
	# indices has no inherent order, so we don't need to keep track of the order in which they
	# appear. We do need to keep track of the columns in each index, though.
	
	if ( $in )
	{
	    $schema->{$db}{$tn}{_index}{$in} ||= $record;
	    push @{$schema->{$db}{$tn}{_index}{$in}{_columns}}, $cn;
	}
	
	# Otherwise, it is a column record. In this case, we need to keep track of the previous
	# column, if any.
	
	else
	{
	    # If this is a new table, clear $last_cn which stores the previous column
	    # name. Otherwise, use it to set the PREVIOUS_COLUMN field.
	    
	    if ( $tn && $last_tn && $tn ne $last_tn ) { $last_cn = undef; }
	    elsif ( $last_cn ) { $record->{PREVIOUS_COLUMN} = $last_cn; };
	    $last_cn = $cn;
	}

	# Store the column record under its table record, and also add its name to the column
	# sequence.
	
	$schema->{$db}{$tn}{$cn} = $record;
	push @{$schema->{$db}{$tn}{_columns}}, $cn;
    }
    
    close $infile;
}


# Compare the two schemas together.

sub Compare {
    
    my ($schema1, $schema2) = @_;

    # Generate a list of database names that appear in either or both input files.
    
    my @dblist = Uniq(keys %$schema1, keys %$schema2);
    
    # Then determine the names of databases that appear only in one file or the other. Only print
    # these out if --missing-tables was specified. All databases that appear in both files are passed on
    # to the next step.
    
    my @dbcompare;
    my $header_line;
    my $different_tables;
    my $added_tables;
    my $removed_tables;
    
    foreach my $db ( @dblist )
    {
	if ( $schema1->{$db} && $schema2->{$db} )
	{
	    push @dbcompare, $db;
	}
	
	elsif ( $schema1->{$db} && $opt_mt )
	{
	    print "===================================================\n" unless $header_line;
	    print "--- Database '$db'\n";
	    $header_line = 1;
	}
	
	elsif ( $opt_mt )
	{
	    print "===================================================\n" unless $header_line;
	    print "+++ Database '$db'\n";
	    $header_line = 1;
	}
    }
    
    print "===================================================\n";
    
    # Now compare all databases that are in common between the two input files, table by table.
    
    my $separator = "---------------------------------------------------\n";
    my $show_separator;
    
    foreach my $db ( @dbcompare )
    {
	# Generate a list of tables that appear in either or both files. This list will be in
	# sorted order by table name.
	
	my @tblist = Uniq(keys %{$schema1->{$db}}, keys %{$schema2->{$db}});
	
	# Go through the list and compare each table name between the two input files.
	
	foreach my $tb ( @tblist )
	{
	    my @output;
	    
	    # Tables that appear in both schemas are compared element by element.
	    
	    if ( $schema1->{$db}{$tb} && $schema2->{$db}{$tb} )
	    {
		@output = CompareTable($schema1, $schema2, $db, $tb);
		
		if ( @output )
		{
		    unshift @output, " *  Table '$tb'\n";
		    $different_tables++;
		}
	    }
	    
	    elsif ( $schema1->{$db}{$tb} )
	    {
		push @output, "--- Table '$tb'\n" if $opt_mt;
		$removed_tables++;
	    }
	    
	    else
	    {
		push @output, "+++ Table '$tb'\n" if $opt_mt;
		$added_tables++;
	    }
	    
	    if ( @output )
	    {
		print $separator if $show_separator; $show_separator = 1;
		print @output;
	    }
	}
    }
    
    # If there were no differences between the two schemas, print a message to that effect.

    unless ( $different_tables || $added_tables || $removed_tables )
    {
	print "    No differences were found between the two schemas.\n";
    }

    # Otherwise, if output was suppressed for added and removed tables, summarize them now.

    elsif ( $added_tables || $removed_tables )
    {
	print $separator if $show_separator; $show_separator = 1;
	
	if ( $added_tables )
	{
	    print "+++ $added_tables tables were added.\n";
	}
	
	if ( $removed_tables )
	{
	    print "--- $removed_tables tables were removed.\n";
	}
    }
    
    print "===================================================\n";    
}


# Compare the specified table between the two versions of the database schema.

sub CompareTable {
    
    my ($schema1, $schema2, $db, $tn) = @_;
    
    my $table1 = $schema1->{$db}{$tn};
    my $table2 = $schema2->{$db}{$tn};
    
    # First compare the two column lists. We use Algorithm::Diff to generate a reasonable
    # add-and-remove list.
    
    my $diff = Algorithm::Diff->new( $table1->{_columns}, $table2->{_columns} );
    
    $diff->Base(1);
    
    my @output;
    my @inplace;
    
    while ( $diff->Next() )
    {
	my @items1 = $diff->Items(1);
	my @items2 = $diff->Items(2);
	
	if ( my @list = $diff->Same() )
	{
	    push @inplace, @list;
	    next;
	}
	
	if ( @items1 )
	{
	    foreach my $cn ( @items1 )
	    {
		if ( $table2->{$cn} )
		{
		    push @inplace, $cn;
		}
		
		else
		{
		    push @output, "    ALTER TABLE $db.$tn DROP COLUMN IF EXISTS $cn\n";
		}
	    }
	}
	
	if ( @items2 )
	{
	    foreach my $cn ( @items2 )
	    {
		if ( $table1->{$cn} )
		{
		    push @inplace, $cn;
		}
		
		else
		{
		    my $def = ColumnDefinition($table2->{$cn});
		    push @output, "    ALTER TABLE $db.$tn ADD COLUMN IF NOT EXISTS $cn $def\n";
		}
	    }
	}
    }
    
    foreach my $cn ( @inplace )
    {
	my ($new_def, $old_def) = CompareColumn($table1->{$cn}, $table2->{$cn});

	next unless $new_def;
	
	push @output, "    ALTER TABLE $db.$tn MODIFY COLUMN $cn $new_def\n";
	push @output, "      was: $old_def\n";
    }
    
    # Then compare the index lists, if any.
    
    if ( $table1->{_index} || $table2->{_index} )
    {
	my @indexes;
	my $output;
	
	push @indexes, keys %{$table1->{_index}} if $table1->{_index};
	push @indexes, keys %{$table2->{_index}} if $table2->{_index};
	
	@indexes = Uniq(@indexes);

	if ( @output )
	{
	    $output = 1;
	}
	
	foreach my $in ( @indexes )
	{
	    if ( $table1->{_index}{$in} && ! $table2->{_index}{$in} )
	    {
		push @output, "    ALTER TABLE $db.$tn DROP KEY IF EXISTS $in\n";
	    }

	    elsif ( $table2->{_index}{$in} && ! $table1->{_index}{$in} )
	    {
		my $keys2 = join ', ', @{$table2->{_index}{$in}{_columns}};
		my $uniq = $table2->{_index}{$in}{NON_UNIQUE} ? '' : 'UNIQUE ';
		my $type = $table2->{_index}{$in}{INDEX_TYPE} eq 'SPATIAL' ? 'SPATIAL '
		         : $table2->{_index}{$in}{INDEX_TYPE} eq 'FULLTEXT' ? 'FULLTEXT ' : '';
		push @output, "    ALTER TABLE $db.tn ADD ${uniq}${type}KEY IF NOT EXISTS $in ($keys2)\n";
	    }
	    
	    else
	    {
		my $keys1 = join ', ', @{$table1->{_index}{$in}{_columns}};
		my $keys2 = join ', ', @{$table2->{_index}{$in}{_columns}};

		if ( $keys1 ne $keys2 )
		{
		    push @output, "    KEY $in FROM $keys1 => $keys2\n";
		}
	    }
	}
    }
    
    # Return the collected output.
    
    return @output;
}


sub CompareColumn {
    
    my ($col1, $col2) = @_;

    $col1->{COLUMN_DEFAULT} =~ s/'//g;
    $col2->{COLUMN_DEFAULT} =~ s/'//g;
    
    $col1->{COLUMN_DEFAULT} =~ s/current_timestamp\(\)/CURRENT_TIMESTAMP/i;
    $col2->{COLUMN_DEFAULT} =~ s/current_timestamp\(\)/CURRENT_TIMESTAMP/i;
    
    $col1->{EXTRA} =~ s/current_timestamp\(\)/CURRENT_TIMESTAMP/i;
    $col2->{EXTRA} =~ s/current_timestamp\(\)/CURRENT_TIMESTAMP/i;
    
    return if
	$col1->{COLUMN_TYPE} eq $col2->{COLUMN_TYPE} &&
	$col1->{COLUMN_KEY} eq $col2->{COLUMN_KEY} &&
	$col1->{EXTRA} eq $col2->{EXTRA} &&
	$col1->{COLUMN_DEFAULT} eq $col2->{COLUMN_DEFAULT} &&
	$col1->{IS_NULLABLE} eq $col2->{IS_NULLABLE} &&
	$col1->{CHARACTER_SET_NAME} eq $col2->{CHARACTER_SET_NAME};
    
    my $def = ColumnDefinition($col1, $col2->{COLLATION_NAME}, $col2->{PREVIOUS_COLUMN});
    
    my $old = ColumnDefinition($col2, $col1->{COLLATION_NAME}, $col1->{PREVIOUS_COLUMN});
    
    return ($def, $old);
}


sub ColumnDefinition {

    my ($col, $charset_compare, $prev_compare) = @_;
    
    my $def = $col->{COLUMN_TYPE};
    
    $def .= ' NOT NULL'
	if $col->{IS_NULLABLE} eq 'NO';
    
    $def .= ' DEFAULT \'' . $col->{COLUMN_DEFAULT} . '\''
	if defined $col->{COLUMN_DEFAULT} && $col->{COLUMN_DEFAULT} ne 'NULL';
    
    $def .= ' ' . $col->{EXTRA} if $col->{EXTRA};

    if ( $col->{COLLATION_NAME} && $col->{COLLATION_NAME} ne 'NULL' )
    {
	$def .= ' CHARACTER SET = ' . $col->{COLLATION_NAME}
	    if ! $charset_compare || $col->{COLLATION_NAME} && $col->{COLLATION_NAME} ne $charset_compare;
    }
    
    if ( $col->{PREVIOUS_COLUMN} )
    {
	$def .= ' AFTER ' . $col->{PREVIOUS_COLUMN} if ! $prev_compare || $prev_compare ne $col->{PREVIOUS_COLUMN};
    }
    
    return $def;
}


sub Uniq {
    
    my %seen;
    my @result;
    
    foreach ( @_ )
    {
	next if $seen{$_};
	push @result, $_;
	$seen{$_} = 1;
    }

    return sort @result;
}

