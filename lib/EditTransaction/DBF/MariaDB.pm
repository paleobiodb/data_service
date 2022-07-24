# 
# EditTransaction::Interface::MariaDB
# 
# This module provides an interface for EditTransaction module to the MariaDB database.
# 
# Author: Michael McClennen

package EditTransaction::DBF::MariaDB;

use strict;

use EditTransaction qw($REGISTERED_APP);

use TableDefs qw(%TABLE get_table_defn get_table_column_defn set_table_name list_column_property_names);

use Carp qw(croak);

our (@CARP_NOT) = qw(EditTransaction);


# The following variables cache retrieved table and column information.
# ---------------------------------------------------------------------

our (%TABLE_SCHEMA_CACHE, %COLUMN_SCHEMA_CACHE, %COLUMN_DIRECTIVE_CACHE);


# After this module is compiled, register it with EditTransaction.pm.
# -------------------------------------------------------------------

UNITCHECK {
    EditTransaction->register_dbf('MariaDB');
    
    EditTransaction->register_hook('get_table_description', \&get_table_description);
    EditTransaction->register_hook('get_column_description', \&get_column_description);
    EditTransaction->register_hook('fetch_table_schema', \&fetch_table_schema);
    EditTransaction->register_hook('get_column_property', \&get_column_property);
    EditTransaction->register_hook('alter_column_property', \&alter_column_property);
    
    EditTransaction->register_hook('validate_data_value', \&validate_data_value);
    
    EditTransaction->register_hook('execute_insert', \&execute_insert);
    EditTransaction->register_hook('execute_replace', \&execute_replace);
    EditTransaction->register_hook('execute_update', \&execute_update);
    EditTransaction->register_hook('execute_delete', \&execute_delete);
};


# Public hooks and auxiliary routines
# -----------------------------------

# get_table_description ( table_specifier )
#
# Fetch and cache the schema of the specified table, if it does not already appear in the
# cache. Return a list of two references. The first provides the table properties, and the second
# is a hash with one key per column. The values are secondary hashes that provide the column
# properties.

sub get_table_description {
    
    my ($edt, $table_arg) = @_;
    
    my $app = $edt->app;
    
    unless ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_arg} )
    {
	&fetch_table_schema($edt, $table_arg);
    }
    
    return ($TABLE_SCHEMA_CACHE{$app}{$table_arg}, $COLUMN_SCHEMA_CACHE{$app}{$table_arg});
}


# get_column_description ( table_specifier, column_name )
#
# If a column name is specified, return a reference to the requested column record. Otherwise,
# return a list of all column records for the specified table. If this information does not appear
# in our cache, then fetch it and cache it first.

sub get_column_description {

    my ($edt, $table_arg, $column_arg) = @_;
    
    my $app = $edt->app;
    
    unless ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_arg} )
    {
	&fetch_table_schema($edt, $table_arg);
    }

    if ( defined $column_arg )
    {
	return $COLUMN_SCHEMA_CACHE{$app}{$table_arg}{$column_arg};
    }

    else
    {
	return $COLUMN_SCHEMA_CACHE{$app}{$table_arg};
    }
}


# table_schema_present ( table_specifier, column_name )
# 
# Return true if the specified table schema has been fetched and cached. If the second argument is
# non-empty, return true if the specified column name exists within it.

sub table_schema_present {
    
    my ($edt, $table_specifier, $column_name) = @_;
    
    croak "required argument: table specifier" unless $table_specifier;
    
    my $app = $edt->app;
    
    if ( $column_name )
    {
	return defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name};
    }
    
    else
    {
	return defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier};
    }
}


# fetch_table_schema ( table_specifier )
# 
# Fetch the schema for the specified table, and cache it. The argument can be either a name that
# was defined using the table definition module, or else a name of the form <table> or
# <database>.<table> known to the database.

sub fetch_table_schema {
    
    my ($edt, $table_arg) = @_;
    
    my $app = $edt->app;
    
    my $dbh = $edt->dbh || die "missing database handle";
    
    my $debug_mode = $edt->debug_mode;
    
    my ($table_key, $table_name);
    
    # If the specified name is defined in the table definition module, try the name provided
    # by that module. Otherwise, try the specified name directly.
    
    if ( defined $TABLE{$table_arg} && $TABLE{$table_arg} ne '' )
    {
	$table_key = $table_arg;
	$table_name = $TABLE{$table_arg};
    }
    
    # Otherwise, check to see if the given table name exists in the database. If so, add a table
    # definition under its own name.
    
    else
    {
	$table_key = $table_arg;
	$table_name = $table_arg;
    }
    
    # Confirm that this table exists. The fetch statement is slightly different for a database
    # qualified name than for a simple table name.
    
    my ($sql, $check_table, $quoted_table);
    
    if ( $table_name =~ /(\w+)[.](.+)/ )
    {
	$sql = "SHOW TABLES FROM `$1` LIKE " . $dbh->quote($2);
	$quoted_table = "`$1`.". $dbh->quote_identifier($2);
    }
    
    else
    {
	$sql = "SHOW TABLES LIKE " . $dbh->quote($table_name);
	$quoted_table = $dbh->quote_identifier($table_name);
    }
    
    $edt->debug_line("$sql\n") if $debug_mode;
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    # If the table doesn't exist in the database, throw an exception.
    
    if ( not $check_table )
    {
	my $extra = $table_key ? " ($table_key)" : "";
	croak $table_key "The table '$table_name'$extra does not exist in the database";
    }
    
    # Otherwise, fetch all of the available information about this table.
    
    my (%table_definition, %column_definition, %column_properties);
    
    # If properties have been defined for it, fetch them now from the table definition system.
    
    if ( $table_specifier )
    {
	%table_definition = get_table_defn($table_specifier);
	%column_properties = get_table_column_defn($table_specifier);
    }
    
    # Then fetch the table column descriptions from the database.
    
    $sql = "SHOW FULL COLUMNS FROM $quoted_table";
    
    $edt->debug_line("$sql\n") if $debug_mode;
    
    my $column_info = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    # Now go through the column description records one by one. Collect a list of column
    # names. Find the primary key if there is one, and add all relevant column properties.
    
    my @column_list;
    my @primary_keys;

  COLUMN:
    foreach my $cr ( @$column_info )
    {
	# Each column definition comes to us as a hash. The column name is under 'Field'.
	
	my $colname = $cr->{Field};

	# If the column property IGNORE has a true value, ignore this column.
	
	next COLUMN if $column_properties{$colname}{IGNORE};
	
	# Otherwise, add it to the column list.
	
	$column_definition{$colname} = $cr;
	push @column_list, $colname;
	
	# If this is a primary key field, add it to the primary key list.
	
	if ( $cr->{Key} =~ 'PRI' )
	{
	    push @primary_keys, $colname;
	}
	
	# If the column is not nullable and has neither a default value nor an auto_increment,
	# mark it as NOT_NULL. Otherwise, a database error will be generated when we try to insert
	# or update a record with a null value for this column.
	
	if ( $cr->{Null} && $cr->{Null} eq 'NO' &&
	     not ( defined $cr->{Default} ) &&
	     not ( $cr->{Extra} && $cr->{Extra} =~ /auto_increment/i ) )
	{
	    $cr->{NOT_NULL} = 1;
	}
	
	# If any properties have been set for this column using the table definition system, add
	# them now.
	
	if ( $column_properties{$colname} && $column_properties{$colname}->%* )
	{
	    set_column_properties($cr, $column_properties{$colname});
	}
	
	# If the FOREIGN_KEY property was specified, and it contains a /, separate it into a table
	# specifier and a column specifier.
	
	if ( my $fk = $cr->{FOREIGN_KEY} )
	{
	    if ( $fk =~ qr{ (.*?) / (.*) }xs )
	    {
		$cr->{FOREIGN_KEY} = $1;
		$cr->{FOREIGN_COL} = $2;
	    }
	}
	
	# If we have information about the column type, unpack it now.
	
	if ( $cr->{Type} )
	{
	    unpack_column_type($cr, $cr->{Type});
	}

	# The two directives handled by default are 'ts_created' and 'ts_modified'. If this column
	# has one of them, add an INSERT_FILL or UPDATE_FILL property.
	
	if ( $cr->{DIRECTIVE} && $cr->{DIRECTIVE} =~ /^ts_created$|^ts_modified$/ )
	{
	    unless ( $cr->{Default} =~ /current_timestamp/i )
	    {
		$cr->{INSERT_FILL} = 'NOW()';
	    }

	    unless ( $cr->{Extra} =~ /on update/i )
	    {
		$cr->{UPDATE_FILL} = 'NOW()';
	    }
	}
    }
    
    # If we found any primary key columns, set the table's PRIMARY_KEY attribute.
    
    if ( @primary_keys == 1 )
    {
	$table_definition{PRIMARY_KEY} = $primary_keys[0];
    }
    
    elsif ( @primary_keys > 2 )
    {
	$table_definition{PRIMARY_KEY} = \@primary_keys;
    }
    
    # Store the list of column names in the table definition.
    
    $table_definition{COLUMN_LIST} = \@column_list;
    
    # If the current application module includes a hook for post-processing table definitions,
    # call it now.
    
    $edt->call_hook('finish_table_definition', \%table_definition, \%column_definition, \@column_list);
    
    # Then collect up all of the column directives into a single hash. Ignore any 'validate'
    # directives, since that is the default.
    
    my %directives;
    
    foreach my $colname ( @column_list )
    {
	if ( my $directive = $column_definition{$colname}{DIRECTIVE} )
	{
	    $directives{$colname} = $directive unless $directive eq 'validate';
	}
    }
    
    # Cache all the information we have collected, overwriting any previous cache entries.
    
    $TABLE_SCHEMA_CACHE{$app}{$table_specifier} = \%table_definition;
    
    $COLUMN_SCHEMA_CACHE{$app}{table_specifier} = \%column_definition;
    
    $COLUMN_DIRECTIVE_CACHE{$app}{$table_specifier} = \%directives;
    
    return 1;
}


# set_column_properties ( column_record, properties )
# 
# Add the specified properties to the specified column record. Those which are listrefs or
# hashrefs have their immediate contents copied, but no deeper.

sub set_column_properties {

    my ($cr, $props) = @_;
    
    foreach my $name ( keys $props->%* )
    {
	cr->{$name} = copy_property_value($props->{$name});
    }
}


# unpack_column_type ( column_record, type_info )
# 
# Unpack the type information string into a list of parameters that can be used by the validation
# routines.

our (%PREFIX_SIZE) = ( tiny => 255,
		       regular => 65535,
		       medium => 16777215,
		       large => 4294967295 );

our (%INT_SIZE) = ( tiny => 'one byte integer',
		    small => 'two byte integer',
		    medium => 'three byte integer',
		    regular => 'four byte integer',
		    big => 'eight byte integer');

sub unpack_column_type {

    my ($cr, $typeinfo) = @_;

    if ( $typeinfo =~ qr{ ^ ( var )? ( char | binary ) [(] ( \d+ ) }xs )
    {
	my $type = $2 eq 'char' ? 'text' : 'data';
	my $mode = $1 ? 'variable' : 'fixed';
	my $size = $3;
	my ($charset) = $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeParams} = [ $type, $size, $mode, $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( tiny | medium | long )? ( text | blob ) (?: [(] ( \d+ ) )? }xs )
    {
	my $type = $2 eq 'text' ? 'text' : 'data';
	my $size = $3 || $PREFIX_SIZE{$1 || 'regular'};
	my ($charset) = $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeParams} = [ $type, $size, 'variable', $charset ];
    }
    
    elsif ( $type =~ qr{ ^ tinyint [(] 1 [)] }xs )
    {
	$cr->{TypeParams} = [ 'boolean' ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ (tiny|small|medium|big)? int [(] (\d+) [)] \s* (unsigned)? }xs )
    {
	my $bound = $1 || 'regular';
	my $unsigned = $3 ? 'unsigned' : '';
	$cr->{TypeParams} = [ 'integer', $unsigned, $bound, $2 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ decimal [(] (\d+) , (\d+) [)] \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $before = $1 - $2;
	my $after = $2;
	
	$after = 10 if $after > 10;	# If people want fields with more than 10 decimals, they should
					# use floating point.
	
	$cr->{TypeParams} = [ 'fixed', $unsigned, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( float | double ) (?: [(] ( \d+ ) , ( \d+ ) [)] )? \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $precision = $1;
	my $before = defined $2 ? $2 - $3 : undef;
	my $after = $3;
	$cr->{TypeParams} = [ 'floating', $unsigned, $precision, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ bit [(] ( \d+ ) }xs )
    {
	$cr->{TypeParams} = [ 'bits', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( enum | set ) [(] (.+) [)] $ }xs )
    {
	my $type = $1;
	my $list = $2;
	my $value_hash = unpack_enum($2);
	$cr->{TypeParams} = [ $type, $value_hash, $list ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( date | time | datetime | timestamp ) \b }xs )
    {
	$cr->{TypeParams} = [ 'datetime', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( (?: multi )? (?: point | linestring | polygon ) ) \b }xs )
    {
	$cr->{TypeParams} = [ 'geometry', $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( geometry (?: collection )? ) \b }xs )
    {
	$cr->{TypeParams} = [ 'geometry', $1 ];
    }
    
    else
    {
	$cr->{TypeParams} = [ 'unknown' ];
    }
}


# unpack_enum ( value_string )
# 
# Given a string of values, unpack them and construct a hash ref with each value as a key.

sub unpack_enum {

    use feature 'fc';
    
    my ($string) = @_;
    
    my $value_hash = { };
    
    while ( $string =~ qr{ ^ ['] ( (?: [^'] | '' )* ) ['] ,? (.*) }xs )
    {
	my $value = $1;
	$string = $2;
	
	$value =~ s/''/'/g;
	$value_hash->{fc $value} = 1;
    }
    
    if ( $string )
    {
	return 'ERROR';
    }

    else
    {
	return $value_hash;
    }
}


# get_column_property ( table_specifier, column_name, property )
# 
# Return the specified information from the schema cache, if it is available.

sub get_column_property {

    my ($edt, $table_specifier, $column_name, $property) = @_;
    
    croak "required arguments: table specifier, column name"
	unless $table_specifier && $column_name;

    my $app = $edt->app;
    
    if ( my $pcache = $COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name} )
    {
	if ( $property )
	{
	    return defined $pcache->{$property} ? copy_property_value($pcache->{$property}) : undef;
	}
	
	else
	{
	    local $_;
	    return map { $_ => copy_property_value($pcache->{$_}) } keys $pcache->%*;
	}
    }
}


sub copy_property_value {

    if ( ref $_[0] && ( ref $_[0] eq 'ARRAY' || ref $_[0] eq 'HASH' ) )
    {
	if ( ref $_[0] eq 'HASH' )
	{
	    return { $_[0]->%* };
	}
	
	else
	{
	    return  = [ $_[0]->@* ];
	}
    }
    
    else
    {
	return $_[0];
    }
}


# alter_column_property ( table_specifier, column_name, property, value )
# 
# This routine is intended primarily for testing purposes. It alters a specific entry in the
# $COLUMN_SCHEMA_CACHE.

sub alter_column_property {
    
    my ($edt, $table_specifier, $column_name, $property, $value) = @_;
    
    croak "required arguments: table specifier, column name, property name"
	unless $table_specifier && $column_name && $property;
    
    my $app = $edt->app;
    
    if ( defined $COLUMN_SCHEMA_CACHE{$app}{$table_specifier} )
    {
	$COLUMN_SCHEMA_CACHE{$app}{$table_specifier}{$column_name}{$property} = $value;
	return 1;
    }
    
    else
    {
	return undef;
    }
}


1;
