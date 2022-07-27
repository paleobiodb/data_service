# 
# EditTransaction::APP::PBDB
# 
# This module gives EditTransaction the extra semantics required by the Paleobiology Database.
# 
# Author: Michael McClennen

package EditTransaction::APP::PBDB;

use strict;

use EditTransaction;

use Carp qw(croak);

our (@CARP_NOT) = qw(EditTransaction);

our (%COMMON_FIELD_SPECIAL) = ( authorizer_no => 'auth_authorizer',
				authorizer_id => 'auth_authorizer',
				enterer_no => 'auth_creater',
				enterer_id => 'auth_creater',
				modifier_no => 'auth_modifier',
				modifier_id => 'auth_modifier',
				created => 'ts_created',
				modified => 'ts_modified',
				admin_lock => 'adm_lock',
			        owner_lock => 'own_lock' );


# After this module is compiled, register it with EditTransaction.pm.
# -------------------------------------------------------------------

UNITCHECK {
    EditTransaction->register_app('PBDB');
    EditTransaction->register_hook('finish_table_definition', \&finish_table_definition);
    
    EditTransaction->register_hook('check_data_column', \&check_data_column);
    
    EditTransaction->register_value_hook('validate_special_column',
		['auth_authorizer', 'auth_creater', 'auth_modifier', 'adm_lock', 'own_lock'], 
		\&validate_special_column);
    
    set_table_property_name('BY_AUTHORIZER', 1);
    set_column_property_name('EXTID_TYPE', 1);
};


# Hooks and auxiliary routines
# ----------------------------

# finish_table_definition ( $table_defn, $column_defn, $column_list )
#
# This hook is called whenever the database module loads a table schema. It gives the application
# the chance to modify the definition and add extra properties.

sub finish_table_definition {

    my ($edt, $table_defn, $column_defn, $column_list) = @_;
    
    # Go through each of the columns, and modify certain column properties.
    
    foreach my $colname ( @$column_list )
    {
	my $cr = $column_defn{$colname};
	
	# Adjust the FOREIGN_KEY property if necessary by checking %FOREIGN_KEY_TABLE.
	
	if ( not $cr->{FOREIGN_KEY} and $FOREIGN_KEY_TABLE{$colname} )
	{
	    $cr->{FOREIGN_KEY} = $FOREIGN_KEY_TABLE{$colname};
	    $cr->{FOREIGN_COL} = $FOREIGN_KEY_COL{$colname} if $FOREIGN_KEY_COL{$colname};
	}
	
	# If the column does not have a DIRECTIVE property, check for a special handler based on
	# the column name.
	
	if ( not defined $cr->{DIRECTIVE} and $COMMON_FIELD_SPECIAL{$colname} )
	{
	    $cr->{DIRECTIVE} = $COMMON_FIELD_SPECIAL{$colname};
	}
	
	# If the column does not have an EXTID_TYPE property, check for an external identifier type
	# based on the column name.
	
	if ( not defined $cr->{EXTID_TYPE} and $COMMON_FIELD_IDTYPE{$colname} )
	{
	    $cr->{EXTID_TYPE} = $COMMON_FIELD_IDTYPE{$colname};
	}
	
	# If the name of the field ends in _no, then record its alternate as the same name with
	# _id substituted unless there is already a field with that name.
	
	if ( not $cr->{ALTERNATE_NAME} and $colname =~ qr{ ^ (.*) _no }xs )
	{
	    my $alt = $1 . '_id';
	    
	    unless ( $column_defn->{$alt} )
	    {
		$cr->{ALTERNATE_NAME} = $alt;
	    }
	}
    }
}


# validate_special_column ( directive, cr, permission, fieldname, value )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ts_created      Records the date and time at which this record was created.
# ts_modified     Records the date and time at which this record was last modified.
# au_creater      Records the person_no or user_id of the person who created this record.
# au_authorizer   Records the person_no or user_id of the person who authorized its creation.
# au_modifier     Records the person_no or user_id of the person who last modified this record.
# 
# Values for these columns cannot be specified explicitly except by a user with administrative
# permission, and then only if this EditTransaction allows the condition 'ALTER_TRAIL'.
# 
# If this transaction is in FIXUP_MODE, both field values will be left unchanged if the user has
# administrative privilege. Otherwise, a permission error will be returned.
#
# The parameter $cr must contain the column description record.

# $$$ start here

sub validate_special_column {

    my ($edt, $directive, $cr, $permission, $fieldname, $value) = @_;
    
    # If the value is empty, return undef. The only exception is for the modifier/modified fields
    # if the transaction is in FIXUP_MODE.
    
    unless ( defined $value && $value eq '' )
    {
	if ( $edt->{allows}{FIXUP_MODE} && ( $directive eq 'ts_modified' ||
					     $directive eq 'au_modifier' ) )
	{
	    if ( $permission =~ /admin/ )
	    {
		return 'UNCHANGED';
	    }
	    
	    else
	    {
		return [ 'main', 'E_PERM', 'fixup_mode' ];
	    }
	}
	
	else
	{
	    return undef;
	}
    }
    
    # Otherwise, a non-empty value has been specified for this field. Check that the value matches
    # the required format.
    
    my ($additional, $no_quote);
    
    # The ts fields take datetime values, which are straightforward to validate.
    
    if ( $directive =~ /^ts/ )
    {
	($value, $additional, $no_quote) = $edt->validate_datetime_value('datetime', $fieldname, $value);
    }
    
    # The au fields take key values as specified in the column description. The ones that have
    # integer values accept external identifiers of type PRS.
    
    else
    {
	# If we don't have any type parameters for some reason, default to integer.
	
	my $type = ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams} : 'integer';
	my $maintype = ref $type eq 'ARRAY' ? $type->[0] : $type;
	
	# If the column type is 'integer', check to see if the value is an external identifier of
	# the specified type (defaulting to 'PRS').
	
	if ( $maintype eq 'integer' && looks_like_extid($value) )
	{
	    ($value) = $edt->validate_extid_value($cr->{EXTID_TYPE} || 'PRS', $fieldname, $value);
	}
	
	# If we don't already have an error condition, check if the key value is present in the
	# proper table. If not, set an error condition.
	
	unless ( ref $value eq 'ARRAY' || $edt->check_foreign_key($cr, $value) )
	{
	    $value = [ 'E_KEY_NOT_FOUND', $fieldname, $value ];
	}
    }
    
    # If the user has administrative permission on this table, check to see if the ALTER_TRAIL
    # allowance is present and add a caution if it is not. If we already have an error condition
    # related to the value, bump it into second place.
    
    # $$$ check for ENABLE_ALTER_TRAIL or require superuser.

    if ( $permission =~ /admin/ )
    {
	unless ( $edt->{allows}{ALTER_TRAIL} )
	{
	    $additional = $value if ref $value eq 'ARRAY';
	    $value = [ 'C_ALTER_TRAIL', $fieldname ];
	}
    }
    
    # Otherwise, add a permission error. If we already have an error condition related to the
    # value, bump it into second place.
    
    else
    {
	$additional = $value if ref $value eq 'ARRAY';
	$value = [ 'E_PERM_COL', $fieldname ];
    }
    
    return ($value, $additional, $no_quote);
}


# check_foreign_key ( column_record, value )
# 
# If the specified key value exists for the foreign table associated with this column, return
# it. Otherwise return undef.

sub check_foreign_key {
    
    my ($edt, $cr, $value) = @_;
    
    # Return undef unless we know what column we are working with.
    
    return undef unless $cr && $cr->{Field};
    
    my $colname = $cr->{Field};
    my $check_table = $cr->{FOREIGN_KEY} || $FOREIGN_KEY_TABLE{$colname} || return;
    my $check_col = $cr->{FOREIGN_COL} || $FOREIGN_KEY_COL{$colname} || $colname;
    
    my $quoted = $edt->dbh->quote($value);
    
    my $sql = "SELECT $check_col FROM $TABLE{$check_table} WHERE $check_col=$quoted LIMIT 1";
    
    $edt->debug_line( "$sql\n" );
    
    my ($found) = $edt->dbh->selectrow_array($sql);
    
    return $found;
}


# validate_special_admin ( directive, cr, permission, fieldname, value )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ad_lock         If set, this record is locked and can only be unlocked by an administrator 
# 
# The parameter $cr must contain the column description record.

sub validate_special_admin {

    my ($edt, $directive, $cr, $permission, $fieldname, $value) = @_;
    
    if ( defined $value && $value ne '' )
    {
	unless ( $permission =~ /admin/ )
	{
	    return [ 'E_PERM_LOCK' ];
	}
	
	($value) = $edt->validate_boolean_value('boolean', $fieldname, $value);
    }
    
    return $value;
}


# validate_special_owner ( directive, cr, permission, fieldname, value )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ow_lock         If set, this record is locked and can only be unlocked by its owner
#		  or authorizer
# 
# The parameter $cr holds the column description record.

sub validate_special_owner {

    my ($edt, $directive, $cr, $permission, $fieldname, $value) = @_;
    
    if ( defined $value && $value ne '' )
    {
	if ( $permission =~ /,unowned/ )
	{
	    return [ 'E_PERM_LOCK' ];
	}

	($value) = $edt->validate_boolean_value('boolean', $fieldname, $value);
    }

    return $value;
}


# special_default_value ( directive, column_record, operation )
#
# Generate the default value (if any) for special columns whose value isn't specifically
# given. This varies depending on which operation is being executed.

sub special_default_value {

    my ($edt, $directive, $cr, $operation) = @_;
    
    $operation = 'update' if $operation eq 'update_many';
    
    # Fill in the default value according to the special handling directive.
    
    my $value;
    my $no_quote;
    
    if ( $directive eq 'au_authorizer' )
    {
	$value = $operation eq 'replace' ? 'UNCHANGED'
	       : $operation eq 'update'  ? undef
	                                 : $edt->{perms}->authorizer_no;
    }
    
    elsif ( $directive eq 'au_creater' )
    {
	$value = $operation eq 'replace' ? 'UNCHANGED' :
	         $operation eq 'update'  ? undef :
	         $cr->{Field} =~ /_no$/  ? $edt->{perms}->enterer_no
	                                 : $edt->{perms}->enterer_id;
    }
    
    elsif ( $directive eq 'au_modifier' )
    {
	$value = $operation eq 'insert' ? 0
	       : $cr->{Field} =~ /_no$/ ? $edt->{perms}->enterer_no
					: $edt->{perms}->user_id;
    }
    
    elsif ( $directive eq 'ts_modified' )
    {
	if ( $operation eq 'update' && not ( $cr->{Extra} && $cr->{Extra} =~ /^on update/i ) )
	{
	    $value = 'NOW()';
	    $no_quote = 1;
	}
    }
    
    elsif ( $directive eq 'ad_lock' || $directive eq 'ow_lock' )
    {
	$value = $operation eq 'replace' ? 'UNCHANGED' : undef;
    }
    
    return ($value, undef, $no_quote);
}


# Define the properties of certain fields that are common to many tables in the PBDB.

our (%FOREIGN_KEY_TABLE) = ( taxon_no => 'AUTHORITY_DATA',
			     resource_no => 'REFERENCE_DATA',
			     collection_no => 'COLLECTION_DATA',
			     occurrence_no => 'OCCURRENCE_DATA',
			     specimen_no => 'SPECIMEN_DATA',
			     measurement_no => 'MEASUREMENT_DATA',
			     specelt_no => 'SPECELT_DATA',
			     reid_no => 'REID_DATA',
			     opinion_no => 'OPINION_DATA',
			     interval_no => 'INTERVAL_DATA',
			     timescale_no => 'TIMESCALE_DATA',
			     bound_no => 'TIMESCALE_BOUNDS',
			     eduresource_no => 'RESOURCE_QUEUE',
			     person_no => 'WING_USERS',
			     authorizer_no => 'WING_USERS',
			     enterer_no => 'WING_USERS',
			     modifier_no => 'WING_USERS');

our (%FOREIGN_KEY_COL) = ( authorizer_no => 'person_no',
			   enterer_no => 'person_no',
			   modifier_no => 'person_no' );

our (%COMMON_FIELD_IDTYPE) = ( taxon_no => 'TID',
			       orig_no => 'TXN',
			       reference_no => 'REF',
			       collection_no => 'COL',
			       occurrence_no => 'OCC',
			       specimen_no => 'SPM',
			       measurement_no => 'MEA',
			       specelt_no => 'ELS',
			       reid_no => 'REI',
			       opinion_no => 'OPN',
			       interval_no => 'INT',
			       timescale_no => 'TSC',
			       bound_no => 'BND',
			       eduresource_no => 'EDR',
			       person_no => 'PRS',
			       authorizer_no => 'PRS',
			       enterer_no => 'PRS',
			       modifier_no => 'PRS' );



sub check_data_column {

	    # If the column allows external identifiers, check to see if the value is one. If so,
	    # the raw value will be unpacked and the clean value substituted.
	    
	    if ( my $expected = $cr->{EXTID_TYPE} || $COMMON_FIELD_IDTYPE{$colname} )
	    {
		if ( looks_like_extid($value) )
		{
		    ($value, $additional, $no_quote) = 
			$edt->validate_extid_value($expected, $fieldname, $value);
		}
	    }
	    
	    # Add an error or warning condition if we are given an external identifier for a
	    # column that doesn't accept them.
	    
	    elsif ( ref $value eq 'PBDB::ExtIdent' )
	    {
		$value = [ 'E_EXTID', $fieldname,
			   "this field does not accept external identifiers" ];
	    }
	    

	}
}

# looks_like_extid ( value )
#

sub looks_like_extid {

    my ($value) = @_;

    return ref $value eq 'PBDB::ExtIdent' || $value =~ $IDRE{LOOSE};
}


# validate_extid_value ( type, fieldname, value )
#
# 

sub validate_extid_value {

    my ($edt, $type, $fieldname, $value) = @_;
    
    # If the external identifier has already been parsed and turned into an object, make sure it
    # has the proper type and return the stringified value.
    
    if ( ref $value eq 'PBDB::ExtIdent' )
    {
	my $value_type = $value->type;
	
	# If the value matches the proper type, stringify it and return it.
	
	$EXTID_CHECK{$type} ||= qr/$IDP{$type}/;
	
	if ( $value_type =~ $EXTID_CHECK{$type} )
	{
	    return '' . $value;
	}
	
	# Otherwise, return an error condition.
	
	else
	{
	    return [ 'E_EXTID', $fieldname,
		     "external identifier must be of type '$IDP{$type}', was '$value_type'" ];
	}
    }
    
    # If the value is a string that looks like an unparsed external identifier of the proper type,
    # unpack it and return the extracted value.
    
    elsif ( $value =~ $IDRE{$type} )
    {
	return $2;
    }
    
    # If it looks like an external identifier but is not of the right type, return an error
    # condition.
    
    elsif ( $value =~ $IDRE{LOOSE} )
    {
	$value = [ 'E_EXTID', $fieldname,
		   "external identifier must be of type '$IDP{$type}', was '$1'" ];
    }
    
    # Otherise, return undef to indicate that the value isn't an external identifier.
    
    else
    {
	return undef;
    }
}


sub check_key {
    
    my ($edt, $check_table, $check_col, $value) = @_;
    
    return unless $check_table && $check_col && $value;

    my $quoted = $edt->dbh->quote($value);
    
    my $sql = "SELECT $check_col FROM $TABLE{$check_table} WHERE $check_col=$quoted LIMIT 1";
    
    $edt->debug_line( "$sql\n" );
    
    my ($found) = $edt->dbh->selectrow_array($sql);
    
    return $found;    
}



    1;
