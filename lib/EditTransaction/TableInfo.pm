#
# EditTransaction::TableInfo - role for providing information about the database.
#


package EditTransaction::TableInfo;

use TableDefs qw(is_table_property is_column_property);
use Carp qw(croak);

use Moo::Role;

no warnings 'uninitialized';


# Methods for querying information about tables.
# ----------------------------------------------

# These methods are intended for use by client code, including test scripts. They throw exceptions
# when the specified table or property does not exist.


# get_table_description ( table_name )
# 
# Fetch and cache the schema of the specified table, if it does not already appear in the
# cache. Then return a copy of the cached table information.

sub get_table_description {
    
    my ($edt, $tablename) = @_;
    
    croak "you must specify a table name" unless $tablename;
    
    if ( my $tableinfo = $edt->table_info_ref($tablename) )
    {
	my %copy = map { $_ => &copy_property_value($tableinfo->{$_}) }
	    keys $tableinfo->%*;
	
	return \%copy;
    }
    
    else
    {
	croak "unknown table name '$tablename'";
    }
}


# table_exists ( table_name )
#
# Return a true value (the underlying table name) if the specified table exists, false otherwise.

sub table_exists {

    my ($edt, $tablename) = @_;
    
    croak "you must specify a table name" unless $tablename;
    
    my $tableinfo = $edt->table_info_ref($tablename);
    
    return $tableinfo ? $tableinfo->{TABLE_NAME} : '';
}


# get_table_property ( table_name, property_name )
#
# Fetch and cache the schema of the specified table, if it does not already appear in the
# cache. Then return the value of the specified property.

sub get_table_property {

    my ($edt, $tablename, $propname) = @_;
    
    croak "you must specify a table name and a property name" unless $tablename && $propname;
    croak "invalid property name '$propname'" unless is_table_property($propname);
    
    if ( my $tableinfo = $edt->table_info_ref($tablename) )
    {
	return copy_property_value($tableinfo->{$propname});
    }
    
    else
    {
	croak "unknown table name '$tablename'";
    }
}
	    

# table_has_column ( table_name, column_name )
#
# Return true if the specified table has the specified column, false otherwise. 

sub table_has_column {

    my ($edt, $tablename, $colname) = @_;
    
    croak "you must specify a table name and a column name" unless $tablename && $colname;
    
    if ( $edt->column_info_ref($tablename, $colname) )
    {
	return $colname;
    }
    
    else
    {
	return '';
    }
}


# get_column_property ( table_name, column_name, property_name )
# 
# Fetch and cache the schema of the specified table, if it does not already appear in the
# cache. Then return the value of the specified property.

sub get_column_property {

    my ($edt, $tablename, $colname, $propname) = @_;
    
    croak "you must specify a table name, a column name, and a property name"
	unless $tablename && $colname && $propname;
    croak "invalid property name '$propname'" unless is_column_property($propname);
    
    if ( my $columninfo = $edt->column_info_ref($tablename, $colname) )
    {
	return copy_property_value($columninfo->{$propname});
    }
    
    elsif ( ! $edt->table_info_ref($tablename) )
    {
	croak "unknown table name '$tablename'";
    }
    
    else
    {
	croak "column '$colname' does not exist in table '$tablename'";
    }
}


sub get_column_type {
    
    my ($edt, $tablename, $colname) = @_;
    
    croak "you must specify a table name and a column name" unless $tablename && $colname;
    
    if ( my $columninfo = $edt->column_info_ref($tablename, $colname) )
    {
	return $columninfo->{TypeMain};
    }
    
    elsif ( ! $edt->table_info_ref($tablename) )
    {
	croak "unknown table name '$tablename'";
    }
    
    else
    {
	croak "column '$colname' does not exist in table '$tablename'";
    }
}


sub copy_property_value {

    if ( ref $_[0] eq 'HASH' )
    {
	return { $_[0]->%* };
    }
    
    elsif ( ref $_[0] eq 'ARRAY' )
    {
	return [ $_[0]->@* ];
    }
    
    else
    {
	return $_[0];
    }
}


1;
