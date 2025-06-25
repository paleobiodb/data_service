#
# EditTransaction::TableInfo
# 
# This role provides methods for fetching and cacheing information about
# database tables.
#


package EditTransaction::TableInfo;

use TableDefs qw(%TABLE get_table_properties get_column_properties set_table_name
		 is_table_property is_column_property);

use Carp qw(croak);

use Role::Tiny;

no warnings 'uninitialized';


# Module variables
# ----------------

# The following variables hold cached table and column information.

our (%TABLE_INFO_CACHE, %COLUMN_INFO_CACHE, %COLUMN_DIRECTIVE_CACHE);


# The following hash is filled in by whichever database module is included with
# each class. It holds constants that are used to interpret data returned by the
# database module. The 'register_database_module' method asks the database
# module to fill in a section of this hash for the specified class.

our (%DB_CONSTANTS);


sub register_database_module {
    
    my ($class) = @_;
    
    my $constants = $DB_CONSTANTS{$class} = { };
    
    $class->db_register_module($constants);
}


# Schemas and properties for tables and columns
# ---------------------------------------------

# The methods in this section are designed to be called only from the
# EditTransaction modules. They return references to the contents of cached
# table schemas, which means they are not safe for use by client code. Client
# code should instead use the methods from the section "Methods for use by
# client code" below. 


# table_info_ref ( table_specifier )
# 
# Return a reference to a hash containing cached information about the specified
# table. If that table has not yet been added to the cache, do so. If this is
# called as a class method, a database handle must be provided as the second
# argument.

sub table_info_ref {
    
    my ($edt, $table_specifier, $dbh_arg, $flag) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier, $dbh_arg, $flag);
    }
}


# table_column_ref ( table_specifier, [column_name] )
# 
# Return a reference to a hash containing cached information about the columns
# from the specified table. If that table has not yet been added to the cache,
# do so. If a column name is provided, return a reference to that column's
# record.

sub table_column_ref {

    my ($edt, $table_specifier, $column_arg) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier) || return;
    }
    
    return $column_arg ? $COLUMN_INFO_CACHE{$class}{$table_specifier}{$column_arg} :
	$COLUMN_INFO_CACHE{$class}{$table_specifier};
}


# table_column_list ( table_specifier )
#
# Return a list of the columns in the specified table.

sub table_column_list {
    
    my ($edt, $table_specifier) = @_;
    
    if ( my $tableinfo = $edt->table_info_ref($table_specifier) )
    {
	return $tableinfo->{COLUMN_LIST}->@*;
    }
    
    else
    {
	return;
    }
}


# table_directives_list ( table_specifier )
#
# Return a list of cached column names and directives associated with the specified table. If that
# table has not yet been added to the cache, do so. This method is designed to be called only from
# the EditTransaction modules. Client code should use one of the methods from
# EditTransaction::Info instead.

sub table_directives_list {

    my ($edt, $table_specifier) = @_;
    
    # First check if %TABLE_INFO_CACHE has an entry for this class and table. If it does not, then
    # fetch the relevant information from the database and modify it according to the defined
    # table and column properties.
    
    my $class = ref $edt || $edt;
    
    unless ( defined $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier);
    }
    
    return ref $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} eq 'HASH' ?
	$COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}->%* : ();
}


# get_table_handling ( table_specifier, colname )
# 
# Both arguments must be specified. If $colname is '*', return the result of
# table_directives_list. Otherwise, return the table directive (if any) for the
# specified column. If there is none, return '' if the column exists and undef
# otherwise. 

sub get_table_handling {
    
    my ($edt, $table_specifier, $colname) = @_;
    
    croak "you must provide a table specifier and a column name" 
	unless $table_specifier && $colname;
    
    my $class = ref $edt || $edt;
    
    if ( $colname eq '*' )
    {
	return $edt->table_directives_list($table_specifier);;
    }
    
    elsif ( ! defined $TABLE_INFO_CACHE{$class}{$table_specifier} )
    {
	fetch_table_schema($edt, $table_specifier);
    }
    
    if ( $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname} )
    {
	return $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname};
    }
    
    elsif ( $COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname} )
    {
	return '';
    }
    
    else
    {
	return undef;
    }
}


# table_info_present ( table_specifier, [column_name] )
# 
# Return true if the information for the specified table is cached by this module, false
# otherwise. This method will NOT cause the information to be fetched if it is not in the cache.
# 
# If a column name is specified, return true if the specified column is known to
# be part of the table and false otherwise.

sub table_info_present {

    my ($edt, $table_specifier, $column_name) = @_;
    
    my $class = ref $edt || $edt;
    
    if ( @_ == 3 )
    {
	if ( $table_specifier && $column_name )
	{
	    return $COLUMN_INFO_CACHE{$class}{$table_specifier}{$column_name} ? 1 : '';
	}
    }
    
    elsif ( $table_specifier )
    {
	return $TABLE_INFO_CACHE{$class}{$table_specifier} ? 1 : '';
    }
    
    return undef;
}


# fetch_table_schema ( table_specifier, [dbh] )
# 
# Fetch the schema for the specified table, and cache it. The argument can be either a name that
# was defined using the table definition module, or else a name of the form <table> or
# <database>.<table> known to the database. The optional $dbh argument can be provided if this is
# called as a class method. If it is called as an instance method, the EditTransaction instance
# will already have a stored database handle.

sub fetch_table_schema {
    
    my ($edt, $table_specifier, $dbh_arg, $debug_flag) = @_;
    
    # If this is called as an instance method, retrieve the necessary information from the
    # EditTransaction instance.
    
    my ($dbh, $debug_mode);
    
    if ( ref $edt )
    {
	$dbh = $edt->dbh || die "missing database handle";
	$debug_mode = $edt->debug_mode;
    }
    
    # If called as a class method, we require a database handle as the second argument. Debug mode
    # is always off in this situation.
    
    elsif ( $dbh_arg && ref($dbh_arg) =~ /DB||Database/ )
    {
	$dbh = $dbh_arg;
	$debug_mode = $debug_flag;
    }
    
    else
    {
	croak "you must specify a database handle";
    }
    
    my $class = ref $edt || $edt;
    
    my ($table_key, $table_name);
    
    $DB::single = 1 if ref $edt && $edt->{breakpoint}{schema}{$table_specifier};
    
    # If the specified name is defined in the table definition module, try the name provided
    # by that module. Otherwise, try the specified name directly.
    
    if ( exists $TABLE{$table_specifier} && $TABLE{$table_specifier} )
    {
	$table_key = $table_specifier;
	$table_name = $TABLE{$table_specifier};
    }
    
    else
    {
	$table_name = $table_specifier;
    }
    
    # Confirm that this table exists. The fetch statement is slightly different for a database
    # qualified name than for a simple table name.
    
    my ($check_table, $quoted_name) = $edt->db_check_table_exists($dbh, $table_name);
    
    # If the table exists in the database, add it to the table definition system under its own
    # name if it is not already there.
    
    if ( $check_table )
    {
	unless ( $table_key )
	{
	    set_table_name($table_name => $table_name);
	}
    }
    
    # If the table doesn't exist in the database, return undef.
    
    else
    {
	return undef;
    }
    
    # If this table was known to the table definition system, fetch the table and column
    # properties.
    
    my (%table_definition, %column_definition, %column_properties);
    
    if ( $table_key )
    {
	%table_definition = get_table_properties($table_key);
	%column_properties = get_column_properties($table_key);
    }
    
    # Add the table name to its definition.
    
    $table_definition{TABLE_NAME} = $table_name;
    $table_definition{QUOTED_NAME} = $quoted_name;
    
    # Then fetch the table column descriptions from the database.
    
    my $column_info = $edt->db_fetch_column_info($dbh, $table_name, $quoted_name);
    
    # Now go through the column description records one by one. Collect a list of column
    # names. Find the primary key if there is one, and add all relevant column properties.
    
    my @column_list;
    my @primary_keys;
    
  COLUMN:
    foreach my $cr ( @$column_info )
    {
	# Each column definition comes to us as a hash. The column name is under 'Field'.
	
	my $colname = $cr->{Field};
	
	$DB::single = 1 if ref $edt && $edt->{breakpoint}{colname}{$colname};
	
	# If any properties have been set for this column using the table definition system, add
	# them now. But if the property IGNORE has a true value, skip this column entirely.
	
	if ( ref $column_properties{$colname} eq 'HASH' )
	{
	    next COLUMN if $column_properties{$colname}{IGNORE};
	    
	    foreach my $p ( keys $column_properties{$colname}->%* )
	    {
		$cr->{$p} = $column_properties{$colname}{$p};
	    }
	}
	
	# Since we are not ignoring this column, add it to the column list.
	
	$column_definition{$colname} = $cr;
	push @column_list, $colname;
	
	# If this is a primary key field, add it to the primary key list. If it
	# is not an auto-increment key, add the PRIMARY_REQUIRED attribute to
	# the table.
	
	if ( $cr->{Key} =~ 'PRI' )
	{
	    push @primary_keys, $colname;
	}
	
	# If the column is not nullable and has neither a default value nor the auto_increment
	# property, mark it as NOT_NULL. This will allow us to catch null values and prevent the
	# database from throwing an exception.
	
	if ( $cr->{Null} && $cr->{Null} eq 'NO' &&
	     not ( defined $cr->{Default} ) &&
	     not ( $cr->{Extra} && $cr->{Extra} =~ /auto_increment/i ) )
	{
	    $cr->{NOT_NULL} = 1;
	}
	
	# If the FOREIGN_KEY property was specified and it contains '/', separate it into a table
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
	    $edt->db_unpack_column_type($cr, $cr->{Type});
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
    
    # If any required columns were specified, set the REQUIRED attribute on each
    # one.
    
    if ( $table_definition{REQUIRED_COLS} )
    {
	my @col_list;

	if ( ref $table_definition{REQUIRED_COLS} eq 'ARRAY' )
	{
	    @col_list = $table_definition{REQUIRED_COLS}->@*;
	}

	else
	{
	    @col_list = split /\s*,\s*/, $table_definition{REQUIRED_COLS};
	}
	
	foreach my $colname ( @col_list )
	{
	    if ( $colname && $column_definition{$colname} )
	    {
		$column_definition{$colname}{REQUIRED} = 1;
	    }
	    
	    else
	    {
		print STDERR "ERROR: unknown column '$colname' in REQUIRED_COLS\n";
	    }
	}
    }
    
    # If any special columns were specified, set the DIRECTIVE attribute on each
    # one to the column name. Directives other than the column name must be set
    # separately.
    
    # if ( $table_definition{SPECIAL_COLS} )
    # {
    # 	foreach my $colname ( split /\s*,\s*/, $table_definition{REQUIRED_COLS} )
    # 	{
    # 	    if ( $colname && $column_definition{$colname} )
    # 	    {
    # 		$column_definition{$colname}{DIRECTIVE} = $colname;
    # 	    }
	    	    
    # 	    else
    # 	    {
    # 		print STDERR "ERROR: unknown column '$colname' in REQUIRED_COLS\n";
    # 	    }
    # 	}
    # }
    
    # Cache all the information we have collected, overwriting any previous cache entries.
    
    $TABLE_INFO_CACHE{$class}{$table_specifier} = \%table_definition;
    
    $COLUMN_INFO_CACHE{$class}{$table_specifier} = \%column_definition;
    
    # If this table is specified as using another table for authorization, fetch
    # that table's schema as well. This is done after the $TABLE_INFO_CACHE
    # entry for this table is set, to make sure the recursion will always stop
    # even if a superior-table loop is incorrectly present.
    
    if ( my $auth_table_specifier = $table_definition{AUTH_TABLE} )
    {
	my $auth_table_info = $edt->fetch_table_schema($auth_table_specifier, $dbh_arg);
	
	if ( $auth_table_info )
	{
	    $table_definition{AUTH_KEY} ||= $auth_table_info->{PRIMARY_KEY};
	}
    }
    
    # If the current class includes a method for post-processing table
    # definitions, call it now. Because we are passing the addresses of the
    # cache entries, any updates will be stored in the cache.
    
    if ( $edt->can('finish_table_definition') )
    {
	$edt->finish_table_definition(\%table_definition, \%column_definition, \@column_list);
    }
    
    # Store the list of column names in the table definition.
    
    $table_definition{COLUMN_LIST} = \@column_list;
    
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
    
    $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} = \%directives;
    
    # Now return a reference to the newly cached table information record.
    
    return $TABLE_INFO_CACHE{$class}{$table_specifier};
}


# clear_table_cache ( table_specifier )
# 
# This method is useful primarily for testing purposes. It allows a unit test to
# alter table and column properties and then cause the cached table information
# to be recomputed. If a table specifier is given, clear all cached information
# associated with that table. If '*' is specified instead, clear all cached
# table information for this class.

sub clear_table_cache {
    
    my ($edt, $table_specifier) = @_;
    
    my $class = ref $edt || $edt;
    
    if ( $table_specifier eq '*' )
    {
	$TABLE_INFO_CACHE{$class} = undef;
	$COLUMN_INFO_CACHE{$class} = undef;
	$COLUMN_DIRECTIVE_CACHE{$class} = undef;
    }
    
    elsif ( $table_specifier )
    {
	$TABLE_INFO_CACHE{$class}{$table_specifier} = undef;
	$COLUMN_INFO_CACHE{$class}{$table_specifier} = undef;
	$COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier} = undef;
    }
    
    else
    {
	croak "you must provide a table specifier or '*'";
    }
}


# alter_column_property ( table_specifier, column_name, property, value )
# 
# This routine is intended primarily for testing purposes. It alters a specific
# entry in the %COLUMN_INFO_CACHE. If the property is 'DIRECTIVE', the specified
# value is also stored in %COLUMN_DIRECTIVE_CACHE.

sub alter_column_property {
    
    my ($edt, $table_specifier, $colname, $propname, $value) = @_;
    
    croak "required arguments: table specifier, column name, property name"
	unless $table_specifier && $colname && $propname;
    
    my $class = ref $edt || $edt;
    
    if ( ref $COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname} eq 'HASH' )
    {
	$COLUMN_INFO_CACHE{$class}{$table_specifier}{$colname}{$propname} = $value;
	
	if ( $propname eq 'DIRECTIVE' )
	{
	    $COLUMN_DIRECTIVE_CACHE{$class}{$table_specifier}{$colname} = $value;
	}
	
	return 1;
    }
    
    else
    {
	return undef;
    }
}


# Methods for use by client code
# ------------------------------

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
    
    if ( $edt->table_column_ref($tablename, $colname) )
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
    
    if ( my $columninfo = $edt->table_column_ref($tablename, $colname) )
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
    
    if ( my $columninfo = $edt->table_column_ref($tablename, $colname) )
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
