# 
# EditTransaction::Interface::MariaDB
# 
# This module provides an interface for EditTransaction module to the MariaDB database.
# 
# Author: Michael McClennen


package EditTransaction::Mod::MariaDB;

use strict;

use EditTransaction::TableInfo qw(%DB_CONSTANTS);

use TableDefs qw(%TABLE);
use Carp qw(croak);
use Scalar::Util qw(blessed);

our (@CARP_NOT) = qw(EditTransaction);

use feature 'unicode_strings', 'postderef';

use Role::Tiny;

no warnings 'uninitialized';



# Methods for connecting this module to the specified class
# ---------------------------------------------------------

sub db_register_module {
    
    my ($class, $DB_CONSTANTS) = @_;
    
    $DB_CONSTANTS->{colname_field} = 'Field';
}


# Methods for database handles
# ----------------------------

sub db_validate_dbh {
    
    my ($edt, $dbh) = @_;
    
    my $message;
    
    if ( $dbh && blessed $dbh && $dbh->can('quote') && $dbh->can('selectrow_array') )
    {
	if ( $dbh->{mariadb_clientinfo} )
	{
	    return 'ok';
	}
	
	else
	{
	    return "'$dbh' is not a MariaDB database handle";
	}
    }
    
    else
    {
	return "'$dbh' is not a valid database handle";
    }
}


# Fetching table information
# --------------------------

sub db_check_table_exists {
    
    my ($edt, $dbh, $table_name) = @_;
    
    my ($sql, $check_table, $quoted_name);
    
    if ( $table_name =~ /(\w+)[.](.+)/ )
    {
	$sql = "SHOW TABLES FROM `$1` LIKE " . $dbh->quote($2);
	$quoted_name = $dbh->quote_identifier($1, $2);
    }
    
    else
    {
	$sql = "SHOW TABLES LIKE " . $dbh->quote($table_name);
	$quoted_name = $dbh->quote_identifier($table_name);
    }
    
    $edt->debug_line("$sql\n") if ref $edt && $edt->debug_mode;
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    return ($check_table, $quoted_name);
}


sub db_fetch_column_info {
    
    my ($edt, $dbh, $table_name, $quoted_name) = @_;
    
    my $sql = "SHOW FULL COLUMNS FROM $quoted_name";
    
    $edt->debug_line("$sql\n") if ref $edt && $edt->debug_mode;
    
    return $dbh->selectall_arrayref($sql, { Slice => { } });
}


# db_unpack_column_type ( column_record, type_info )
# 
# Unpack the type information string into a list of parameters that can be used by the validation
# routines.

our (%PREFIX_SIZE) = ( tiny => 255,
		       regular => 65535,
		       medium => 16777215,
		       large => 4294967295 );


sub db_unpack_column_type {

    my ($edt, $cr, $typeinfo) = @_;

    if ( $typeinfo =~ qr{ ^ ( var )? ( char | binary ) [(] ( \d+ ) }xs )
    {
	my $type = $2 eq 'char' ? 'text' : 'data';
	my $mode = $1 ? 'variable' : 'fixed';
	my $size = $3;
	my ($charset) = $cr->{Collation} && $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeMain} = 'char';
	$cr->{TypeParams} = [ $type, $size, $mode, $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( tiny | medium | long )? ( text | blob ) (?: [(] ( \d+ ) )? }xs )
    {
	my $type = $2 eq 'text' ? 'text' : 'data';
	my $size = $3 || $PREFIX_SIZE{$1 || 'regular'};
	my ($charset) = $cr->{Collation} && $cr->{Collation} =~ qr{ ^ ([^_]+) }xs;
	$cr->{TypeMain} = 'char';
	$cr->{TypeParams} = [ $type, $size, 'variable', $charset ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ tinyint [(] 1 [)] }xs )
    {
	$cr->{TypeMain} = 'boolean';
    }
    
    elsif ( $typeinfo =~ qr{ ^ (tiny|small|medium|big)? int [(] (\d+) [)] \s* (unsigned)? }xs )
    {
	my $bound = $1 || 'regular';
	my $type = $3 ? 'unsigned' : 'signed';
	$cr->{TypeMain} = 'integer';
	$cr->{TypeParams} = [ $type, $bound, $2 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ decimal [(] (\d+) , (\d+) [)] \s* (unsigned)? }xs )
    {
	my $unsigned = $3 ? 'unsigned' : '';
	my $before = $1 - $2;
	my $after = $2;
	
	$cr->{TypeMain} = 'fixed';
	$cr->{TypeParams} = [ $unsigned, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( float | double ) (?: [(] ( \d+ ) , ( \d+ ) [)] )? \s* (unsigned)? }xs )
    {
	my $unsigned = $4 ? 'unsigned' : '';
	my $precision = $1;
	my $before = defined $2 ? $2 - $3 : undef;
	my $after = $3;
	$cr->{TypeMain} = 'floating';
	$cr->{TypeParams} = [ $unsigned, $precision, $before, $after ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ bit [(] ( \d+ ) }xs )
    {
	$cr->{TypeMain} = 'bits';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( enum | set ) [(] (.+) [)] $ }xs )
    {
	my $type = $1;
	my $list = $2;
	my $value_hash = unpack_enum($2);
	$cr->{TypeMain} = 'enum';
	$cr->{TypeParams} = [ $type, $value_hash, $list ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( date | time | datetime | timestamp ) \b }xs )
    {
	$cr->{TypeMain} = 'datetime';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( (?: multi )? (?: point | linestring | polygon ) ) \b }xs )
    {
	$cr->{TypeMain} = 'geometry';
	$cr->{TypeParams} = [ $1 ];
    }
    
    elsif ( $typeinfo =~ qr{ ^ ( geometry (?: collection )? ) \b }xs )
    {
	$cr->{TypeMain} = 'geometry';
	$cr->{TypeParams} = [ $1 ];
    }
    
    else
    {
	$cr->{TypeMain} = 'unknown';
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


# sub copy_property_value {

#     if ( ref $_[0] eq 'HASH' )
#     {
# 	return { $_[0]->%* };
#     }
    
#     elsif ( ref $_[0] eq 'ARRAY' )
#     {
# 	return [ $_[0]->@* ];
#     }
    
#     else
#     {
# 	return $_[0];
#     }
# }


# SQL expressions for use elsewhere in the codebase
# -------------------------------------------------

sub sql_current_timestamp {
    
    return 'NOW()';
}


# Database query routines
# -----------------------

# count_matching_rows ( table_specifier, expression )
#
# Return a count of rows from the specified table that match the specified SQL expression.

sub db_count_matching_rows {
    
    my ($edt, $table_specifier, $expression) = @_;
    
    my $sql = "SELECT count(*) FROM $TABLE{$table_specifier} WHERE $expression";
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my ($count) = eval { $edt->dbh->selectrow_array($sql) };
    
    if ( $@ )
    {
	if ( $@ =~ /selectrow_array failed: (.*) at .* MariaDB.pm line/i )
	{
	    return ['E_EXECUTE', $1];
	}
	
	else
	{
	    $@ =~ s/.*selectrow_array failed: //;
	    return ['E_EXECUTE', $@];
	}
    }
    
    else
    {
	return $count;
    }
}


# fetch_record ( table_arg, keyexpr )
#
# Return the record from the specified table with the specified key expression. If the table does
# not exist or the key expression does not select any records, return undefined.

sub fetch_record {

    my ($edt, $table_specifier, $keyexpr) = @_;
    
    if ( my $tableinfo = $edt->table_info_ref($table_specifier) )
    {
	my $quoted_name = $tableinfo->{QUOTED_NAME};
	my $sql = "SELECT * FROM $quoted_name WHERE $keyexpr LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	return $edt->dbh->selectrow_hashref($sql);
    }

    else
    {
	return;
    }
}    


# get_old_values ( action, table, fields )
# 
# Fetch the specified columns from the table record whose key is given by the specified action.

sub get_old_values {

    my ($edt, $tablename, $keyexpr, $fields) = @_;

    return unless $keyexpr;
    
    if ( my $tableinfo = $edt->table_info_ref($tablename) )
    {
	my $quoted_name = $tableinfo->{QUOTED_NAME};
	
	my $sql = "SELECT $fields FROM $quoted_name WHERE $keyexpr LIMIT 1";
	
	$edt->debug_line("$sql\n") if $edt->debug_mode;
	
	return $edt->dbh->selectrow_array($sql);
    }

    else
    {
	return;
    }
}


1;
