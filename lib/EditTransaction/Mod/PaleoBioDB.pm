# 
# EditTransaction::Mod::PBDB
# 
# This module gives EditTransaction the extra semantics required by the Paleobiology Database.
# 
# Author: Michael McClennen

package EditTransaction::Mod::PaleoBioDB;

use strict;

use TableDefs qw(%TABLE get_table_property set_table_property_name set_column_property_name);
use PBDBFields qw(%COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);
use Permissions;
use ExternalIdent qw(%IDP %IDRE);

use Carp qw(croak);
use Scalar::Util qw(blessed);  # [Added by Claude under the guidance of Michael McClennen: blessed was called but not imported]
use List::Util qw(none);

use feature 'unicode_strings', 'postderef';

use Role::Tiny;

our (@CARP_NOT) = qw(EditTransaction);

our (%EXTID_CHECK);


# Register the extra property names used by this module with the table definition system, and
# register extra allowances and directives that are used by this module.

BEGIN {
    set_table_property_name('BY_AUTHORIZER', 1);
    set_column_property_name('EXTID_TYPE', 1);
    
    # [Changed by Claude under the guidance of Michael McClennen: INSERT_KEY was used by
    # 'authorize_insert_key' but was not registered, and SKIP_LOGGING was registered but
    # not used]
    
    EditTransaction->register_allowances('INSERT_KEY');
    
    # [Changed by Claude under the guidance of Michael McClennen: the names registered were
    # 'au_authorizer', 'au_creater', and 'au_modifier', which are not the ones this module
    # uses, and 'auth_updater' and 'ts_updated' were not registered]
    
    EditTransaction->register_directives('auth_authorizer', 'auth_creator', 'auth_modifier',
					 'auth_updater', 'ts_updated', 'adm_lock', 'own_lock');
    
    # The condition E_KEY_NOT_FOUND is used by this module, so it must have a template.
    # [Added by Claude under the guidance of Michael McClennen]
    
    EditTransaction->register_conditions(
	E_KEY_NOT_FOUND => ["Field '&1': no record was found with key '&2'",
			    "Field '&1': no record was found with this key"]);
};


our %AUTH_DIRECTIVE = ( auth_authorizer => 'authorizer_id', auth_creator => 'enterer_id',
			adm_lock => 'admin_lock', own_lock => 'owner_lock' );


# Extra table and column properties
# ---------------------------------

# finish_table_definition ( $table_defn, $column_defn, $column_list )
#
# This method is called whenever the database module loads a table schema. It gives the
# application the chance to modify the definition and add extra properties.

sub finish_table_definition {

    my ($edt, $table_defn, $column_defn, $column_list) = @_;
    
    my @auth_columns;
    
    # Go through each of the columns, and modify certain column properties.
    
    foreach my $colname ( @$column_list )
    {
	my $cr = $column_defn->{$colname};
	
	# Adjust the FOREIGN_KEY property if necessary by checking %FOREIGN_KEY_TABLE.
	
	# if ( not $cr->{FOREIGN_KEY} and $FOREIGN_KEY_TABLE{$colname} )
	# {
	#     $cr->{FOREIGN_KEY} = $FOREIGN_KEY_TABLE{$colname};
	#     $cr->{FOREIGN_COL} = $FOREIGN_KEY_COL{$colname} if $FOREIGN_KEY_COL{$colname};
	# }
	
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
	
	# If the name of the column ends in _no, then record its alternate as the same name with
	# _id substituted unless there is already a field with that name. If it is the primary
	# key, store this info under PRIMARY_FIELD in the table description. Otherwise, store it
	# under ALTERNATE_NAME in the column description.
	
	if ( $colname =~ qr{ ^ (.*) _no }xs )
	{
	    my $alt = $1 . '_id';
	    
	    if ( $colname && $table_defn->{PRIMARY_KEY} && $colname eq $table_defn->{PRIMARY_KEY} )
	    {
		$table_defn->{PRIMARY_FIELD} = $alt;
	    }
	    
	    elsif ( not $cr->{ALTERNATE_NAME} and not $column_defn->{$alt} )
	    {
		$cr->{ALTERNATE_NAME} = $alt;
	    }
	}
	
	# If the column name corresponds to one of the PaleoBioDB special
	# columns, add the corresponding directive unless the column already has
	# one. 
	
	if ( $COMMON_FIELD_SPECIAL{$colname} )
	{
	    $cr->{DIRECTIVE} //= $COMMON_FIELD_SPECIAL{$colname};
	}
	
	# Record a list of the authorization columns that we find.
	
	if ( $cr->{DIRECTIVE} && $AUTH_DIRECTIVE{$cr->{DIRECTIVE}} )
	{
	    # If the column name is one of the canonical names for this directive, just add it to
	    # the list. Fields ending in _no MUST keep their canonical name.
	    
	    if ( ($COMMON_FIELD_SPECIAL{$colname} // '') eq $cr->{DIRECTIVE} )  # [Changed by Claude under the guidance of Michael McClennen: avoid a warning for other names]
	    {
		push @auth_columns, $colname;
	    }
	    
	    # Otherwise, rename it to the canonical name.
	    
	    else
	    {
		push @auth_columns, "$colname as " . $AUTH_DIRECTIVE{$cr->{DIRECTIVE}};
	    }
	}
    }
    
    # If we have found any authorization columns, store them with the table information
    # record.
    
    if ( @auth_columns )
    {
	$table_defn->{AUTH_COLUMN_STRING} = join(',', @auth_columns);
    }
    
    # If this table has no authorization properties, mark it as unrestricted. This means anybody
    # can operate on it.
    
    unless ( $table_defn->{CAN_POST} || $table_defn->{CAN_VIEW} || $table_defn->{CAN_MODIFY} )
    {
	$table_defn->{UNRESTRICTED} = 1;
    }

    # If this table has an AUTH_KEY property set, set AUTH_FIELD as well.
    
    if ( $table_defn->{AUTH_KEY} && $table_defn->{AUTH_KEY} =~ / ^ (.*) _no$ /xs )
    {
	$table_defn->{AUTH_FIELD} = $1 . "_id";
    }
}


# Column validation routines
# --------------------------

# The routines in this section all use the same return convention. The result will either be the
# empty list, or it will be some or all of the following:
# 
#     (result, clean_value, additional, clean_no_quote)
# 
# A. If the specified value is valid and no warnings were generated, the empty list is returned.
# 
# B. If specified value is invalid, the first return value will be a listref containing an error
# condition code and parameters. If any additional error or warning was generated, it will appear
# as the third return value. The second value will be undefined and should be ignored.
# 
# C. If a replacement value is generated (i.e. a truncated character string), a 2-4 element list
# will be returned. The third element, if present, will be a warning condition. The fourth
# element, if present, indicates that the returned value is not an SQL literal and should not be
# quoted.
#
# D. If the column value should remain unchanged despite any 'on update' clause, the single value
#    'unchanged' is returned.


# before_key_column ( cr, operation, value, fieldname )
# 
# This method is called from EditTransaction.pm whenever a key value is provided. It has the
# ability to modify the value to reflect the semantics of the application. 

sub before_key_column {

    my ($edt, $cr, $operation, $value, $fieldname) = @_;
    
    # If the value is an external identifier, check to see if the column allows it. If so, unpack
    # the raw value and return the cleaned value.
    
    if ( looks_like_extid($value) )
    {
	if ( my $expected = $cr->{EXTID_TYPE} )
	{
	    return $edt->validate_extid_value($expected, $value, $fieldname);
	}
	
	else
	{
	    return [ 'E_EXTID', $fieldname, "this field does not accept external identifiers" ];
	}
    }
    
    # If the value is valid as-is, return the empty list.
    
    return;
}


# check_data_column ( cr, operation, permission, value, fieldname )
# 
# This method is called from Validate.pm whenever a non-special column is assigned a non-null
# value. It has the ability to modify the value to reflect the semantics of the application. The
# foreign key check and value validation occur after this method returns.

sub check_data_column {

    my ($edt, $cr, $operation, $permission, $value, $fieldname) = @_;
    
    # If the value is an external identifier, check to see if the column allows it. If so, unpack
    # the raw value and return the cleaned value. A value that looks like an external
    # identifier is rejected only for an integer column, because in a text column it may
    # just be text. [Changed by Claude under the guidance of Michael McClennen]
    
    if ( looks_like_extid($value) )
    {
	if ( my $expected = $cr->{EXTID_TYPE} )
	{
	    return $edt->validate_extid_value($expected, $value, $fieldname);
	}
	
	elsif ( ref $value || $cr->{TypeMain} eq 'integer' )
	{
	    return [ 'E_EXTID', $fieldname, "this field does not accept external identifiers" ];
	}
    }
    
    # If the value is valid as-is, return the empty list.
    
    return;
}


# looks_like_extid ( value )
# 
# This auxiliary routine returns true if the specified value is either an object of the external
# identifier type or else if it is a string that matches the external identifier regexp.

sub looks_like_extid {

    my ($value) = @_;
    
    # [Changed by Claude under the guidance of Michael McClennen: a value that is an unblessed
    # reference, such as a hash, made the call to 'isa' throw an exception.]
    
    return blessed $value && $value->isa('PBDB::ExtIdent') || $value =~ $IDRE{LOOSE};
}


# validate_extid_value ( type, value, fieldname )
# 
# If the specified value matches the specified external identifier type, unpack its core value and
# return it. Otherwise, return an error condition. This method should always return a true value
# as its first result, and the value to be used as the second. This behavior is relied upon below.

sub validate_extid_value {

    my ($edt, $type, $value, $fieldname) = @_;
    
    # If the external identifier has already been parsed and turned into an object, make sure it
    # has the proper type and return the stringified value.
    
    # [Changed by Claude under the guidance of Michael McClennen: blessed instead of ref, as
    # in looks_like_extid.]
    
    if ( blessed $value && $value->isa('PBDB::ExtIdent') )
    {
	my $value_type = $value->type;
	
	# If the value matches the proper type, stringify it and return it. Cache these regexps as
	# they are generated.
	
	$EXTID_CHECK{$type} ||= qr/$IDP{$type}/;
	
	if ( $value_type =~ $EXTID_CHECK{$type} )
	{
	    return (1, $value);
	}
	
	# Otherwise, return an error condition.
	
	else
	{
	    return [ 'E_EXTID', $fieldname,
		     "external identifier must be of type '$IDP{$type}' (was '$value_type')" ];
	}
    }
    
    # If the value is a string that looks like an unparsed external identifier of the proper type,
    # unpack it and return the extracted value.
    
    elsif ( $value =~ $IDRE{$type} )
    {
	return (1, $2);
    }
    
    # If it looks like an external identifier but is not of the right type, return an error
    # condition.
    
    elsif ( $value =~ $IDRE{LOOSE} )
    {
	return [ 'E_EXTID', $fieldname,
		 "external identifier must be of type '$IDP{$type}' (was '$1')" ];
    }
    
    # If it does not look like an external identifier at all, return an error condition.
    
    else
    {
	return [ 'E_EXTID', $fieldname, "not a valid external identifier" ];
    }
}


# validate_special_column ( directive, cr, permission, value, fieldname )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ts_created      Records the date and time at which this record was created.
# ts_modified     Records the date and time at which this record was last modified.
# ts_updated      Records the date and time at which this record was last updated.
# auth_creater    Records the person_no or user_id of the person who created this record.
# auth_authorizer Records the person_no or user_id of the person who authorized its creation.
# auth_modifier   Records the person_no or user_id of the person who last modified this record.
# auth_updater    Records the person_no of the person who last updated this record in FIXUP_MODE.
# adm_lock        A true value in this column indicates that this record is locked administratively.
# own_lock        A true value in this column indicates that this record is locked by its owner.
# 
# Values for these columns (except for own_lock) cannot be specified explicitly
# except by a user with administrative permission, and then only if this
# EditTransaction allows the condition 'ALTER_TRAIL'.
# 
# If this transaction is in FIXUP_MODE, the value of a 'ts_modified' or 'ts_modifier'
# column will be left unchanged if the user has administrative privilege. Otherwise, a permission
# error will be returned.
# 
# The parameter $cr must contain the column description record.

sub validate_special_column {

    my ($edt, $action, $colname, $cr, $directive, $value, $fieldname) = @_;
    
    my $operation = $action->operation;
    my $permission = $action->permission;
    
    # If the value is empty, set it according to the operation and the column name. The only
    # exception is for the modifier/modified fields if the transaction is in FIXUP_MODE.
    # [Changed by Claude under the guidance of Michael McClennen: the test was inverted, so
    # a value that was given was treated as if it were empty, and vice versa]
    
    unless ( defined $value && $value ne '' )
    {
	# If the transaction is being executed in FIXUP_MODE, the modifier and modified date
	# should remain unchanged. This is only allowed with 'admin' or 'unrestricted' permission.
	
	if ( $edt->{allows}{FIXUP_MODE} )
	{
	    if ( $directive eq 'ts_modified' || $directive eq 'auth_modifier' )
	    {
		if ( $permission =~ /^admin|^unrestricted/ )
		{
		    if ( $directive eq 'auth_modifier' && $operation ne 'replace' )
		    {
			return 'ignore';
		    }
		    
		    else
		    {
			return 'unchanged'; # we need to return 'unchanged' even with
                                            # 'update', because an 'on update' clause
                                            # might otherwise change the value anyway.
		    }
		}
		
		else
		{
		    $edt->add_condition('main', 'E_FIXUP_MODE', $action->table);
		    return 'ignore';
		}
	    }
	    
	    elsif ( $directive eq 'ts_updated' )
	    {
		return ('unquoted', 'NOW()');
	    }
	    
	    elsif ( $directive eq 'auth_updater' )
	    {
		return ('clean', $edt->{permission}->enterer_no);
	    }
	}
	
	# Otherwise, return the proper value according to the operation. In the PBDB database,
	# columns whose names end in '_no' are keys with integer values. Those whose names end in
	# '_id' have string values.
	
	# For a 'replace' operation, the authorizer and creator are copied from the old
	# record. [Changed by Claude under the guidance of Michael McClennen: 'unchanged' was
	# returned as the value of the column instead of as the result, so it was stored]
	
	if ( $directive eq 'auth_authorizer' )
	{
	    return 'unchanged' if $operation eq 'replace';
	    
	    $value = $operation eq 'update'  ? undef
					     : $edt->{permission}->authorizer_no;
	    return ('clean', $value);
	}
	
	elsif ( $directive eq 'auth_creator' )
	{
	    return 'unchanged' if $operation eq 'replace';
	    
	    $value = $operation eq 'update'  ? undef
		   : $cr->{Field} =~ /_no$/  ? $edt->{permission}->enterer_no
					     : $edt->{permission}->user_id;
	    return ('clean', $value);
	}
	
	# On insert, the modifier is empty: 0 for a person number, or the empty string for a
	# user id. [Changed by Claude under the guidance of Michael McClennen: it was 0 for
	# both]
	
	elsif ( $directive eq 'auth_modifier' )
	{
	    $value = $operation eq 'insert' ? ( $cr->{Field} =~ /_no$/ ? 0 : '' )
		   : $cr->{Field} =~ /_no$/ ? $edt->{permission}->enterer_no
					    : $edt->{permission}->user_id;
	    return ('clean', $value);
	}
	
	# Outside of FIXUP_MODE, the updater is left alone, except that for a 'replace'
	# operation it must be copied from the old record. [Changed by Claude under the
	# guidance of Michael McClennen: it was ignored, so a replace reset it]
	
	elsif ( $directive eq 'auth_updater' )
	{
	    return $operation eq 'replace' ? 'unchanged' : 'ignore';
	}
	
	# For the 'ts_created' and 'ts_modified' directives, if the column does not have a default
	# value then emulate the desired behavior by putting in the current timestamp on insert
	# and/or update.
	
	# For a 'replace' operation, the creation time is copied from the old record,
	# because otherwise the database would set it to the current time. [Added by Claude
	# under the guidance of Michael McClennen]
	
	elsif ( $directive eq 'ts_created' && $operation eq 'replace' )
	{
	    return 'unchanged';
	}
	
	# Otherwise, the database fills in these columns. [Changed by Claude under the
	# guidance of Michael McClennen: removed the unused column properties INSERT_FILL and
	# UPDATE_FILL]
	
	elsif ( $directive eq 'ts_created' || $directive eq 'ts_modified' )
	{
	    return;
	}
	
	# A column with the 'ts_updated' directive is ignored if we are not in FIXUP_MODE
	# except for a 'replace' operation, where its value must be copied from the old
	# record.
	
	elsif ( $directive eq 'ts_updated' )
	{
	    if ( $operation eq 'replace' )
	    {
		return 'unchanged';
	    }
	    
	    else
	    {
		return 'ignore';
	    }
	}
	
	# For the 'adm_lock' and 'own_lock' directives, we can ignore the column except during a
	# 'replace' operation. In this case, the value must be copied over from the old record.
	
	elsif ( $directive eq 'adm_lock' || $directive eq 'own_lock' )
	{
	    return $operation eq 'replace' ? 'unchanged' : ();
	}
	
	# If the directive is anything else, return an error condition.
	
	else
	{
	    $edt->add_condition('main', 'E_BAD_DIRECTIVE', $cr->{Field}, $directive);
	}
    }
    
    # Otherwise, a non-empty value has been specified for this column. We must validate the value
    # and also check the permission.
    
    my @result;
    
    # Value check
    # -----------
    
    # The ts fields take datetime values, which are straightforward to validate.
    
    if ( $directive =~ /^ts_/ )
    {
	@result = $edt->validate_datetime_value('datetime', $value, $fieldname);
    }
    
    # The auth fields take key values as specified in the column description. The external
    # identifier type defaults to 'PRS' if not otherwise specified.
    
    elsif ( $directive =~ /^auth_/ )
    {
	# If the value is an external identifier, validate it as such.
	
	my $check_result;
	my $check_value = $value;
	
	# [Changed by Claude under the guidance of Michael McClennen: the results were
	# assigned to new variables declared inside this block, so they were discarded]
	
	if ( looks_like_extid($value) )
	{
	    ($check_result, $check_value) =
		$edt->validate_extid_value($cr->{EXTID_TYPE} || 'PRS', $value, $fieldname);
	}
	
	# If we didn't already get an error condition, check this value against its foreign key
	# table.
	
	if ( ref $check_result eq 'ARRAY' )
	{
	    @result = $check_result;
	}
	
	elsif ( ! is_foreign_key($cr) )
	{
	    $edt->add_condition('main', 'E_EXECUTE', 
				"no foreign key information is available for '$fieldname'");
	}
	
	elsif ( ! $edt->check_foreign_key($cr, $check_value) )
	{
	    $edt->add_condition('main', 'E_KEY_NOT_FOUND', $fieldname, $check_value);
	}
	
	else
	{
	    @result = ('clean', $check_value);
	}
    }
    
    # The lock fields take boolean values. Clearing the locks that are set on a record
    # cancels any requirement for the LOCKED allowance; this is done below, after the
    # permission check. [Changed by Claude under the guidance of Michael McClennen]
    
    elsif ( $directive eq 'adm_lock' || $directive eq 'own_lock' )
    {
	@result = $edt->validate_boolean_value('boolean', $value, $fieldname);
    }
    
    # Any other directive generates an error condition.
    
    else
    {
	$edt->add_condition('main', 'E_BAD_DIRECTIVE', $cr->{Field}, $directive);
    }
    
    # Permission check
    # ----------------
    
    # If an error condition is generated during this check and there was already an error
    # condition from the value check above, bump the latter into second place.
    
    # Setting 'adm_lock' or 'own_lock' requires administrative permission, or in the latter case
    # owner permission.

    if ( $directive eq 'adm_lock' )
    {
	unless ( $permission =~ /^admin|^unrestricted/ )
	{
	    $edt->add_condition('main', 'E_PERMISSION_COLUMN', $fieldname);
	    return 'ignore';
	}
    }
    
    elsif ( $directive eq 'own_lock' )
    {
	unless ( $permission =~ /^owned|^admin|^unrestricted/ || $operation eq 'insert' )
	{
	    $edt->add_condition('main', 'E_PERMISSION', $value ? 'lock' : 'unlock');
	    return 'ignore';
	}
    }
    
    # Setting other special column values is only possible with administrative or unrestricted
    # permission. The ALTER_TRAIL allowance is also required.
    
    elsif ( $permission !~ /^admin|^unrestricted/ )
    {
	$edt->add_condition('main', 'E_PERMISSION_COLUMN', $fieldname);
	return 'ignore';
    }
    
    elsif ( ! $edt->{allows}{ALTER_TRAIL} )
    {
	$edt->add_condition('main', 'C_ALTER_TRAIL', $fieldname);
	return 'ignore';
    }
    
    # If the value check returned an error condition, add it to the action and ignore the
    # value. Otherwise, the caller would use the value as given. [Added by Claude under the
    # guidance of Michael McClennen]
    
    if ( ref $result[0] eq 'ARRAY' )
    {
	$edt->add_condition($action, $result[0]->@*);
	return 'ignore';
    }
    
    # If a lock column is being cleared, note it. [Added by Claude under the guidance of
    # Michael McClennen]
    
    if ( ( $directive eq 'adm_lock' || $directive eq 'own_lock' ) && ! $result[1] )
    {
	$edt->_note_lock_cleared($action, $colname);
    }
    
    return @result;
}


# _note_lock_cleared ( action, colname )
# 
# This is called when the specified action clears the specified lock column. If the action
# was marked as requiring an unlock, and it now clears every lock column that is set on the
# record, the requirement is cancelled, because the action unlocks the record itself. This
# is only done for an action that selects a single record.
# 
# Added by Claude under the guidance of Michael McClennen.

sub _note_lock_cleared {
    
    my ($edt, $action, $colname) = @_;
    
    return unless $action->requires_unlock;
    
    # The first time this is called for an action, find the lock columns that are set on
    # the existing record.
    
    unless ( $action->{locks_set} )
    {
	return if $action->keymult;
	
	my $old = $edt->fetch_old_record($action) || return;
	my $columns = $edt->table_column_ref($action->table) || return;
	
	my @lock_columns = grep { ($columns->{$_}{DIRECTIVE} // '') =~ /^adm_lock$|^own_lock$/ }
	    keys %$columns;
	
	$action->{locks_set} = { map { $_ => 1 } grep { $old->{$_} } @lock_columns };
    }
    
    delete $action->{locks_set}{$colname};
    
    $action->requires_unlock(0) unless $action->{locks_set}->%*;
}


# Auxiliary subroutines
# ---------------------


# is_foreign_key ( column_record )
#
# Return true if the necessary information is available for this column to indicate its foreign
# key status.

sub is_foreign_key {

    my ($cr) = @_;
    
    return $cr->{FOREIGN_KEY} || ( $cr->{Field} && $FOREIGN_KEY_TABLE{$cr->{Field}} );
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


# Permissions and authorization
# -----------------------------

# The Paleobiology Database uses a permission system based on the following special columns:
# 
# enterer_no (auth_creator)
# enterer_id (auth_creator)
# authorizer_no (auth_authorizer)
# admin_lock (adm_lock)
# owner_lock (own_lock)
#
# Authentication and table permissions are handled by Permissions.pm. The following methods
# override the default ones from the EditTransaction/Authorization.pm.


# check_instance_permission ( permissio )
#
# This routine is called once as a class method for each blessed argument passed to the class
# constructor. It should return true if the argument is a reference to a valid permission object,
# false otherwise.

sub check_instance_permission {

    my ($class, $permission) = @_;

    return blessed($permission) && $permission->isa('Permissions');
}


# validate_instance_permission ( permission )
# 
# This routine is called as a class method for each new EditTransaction instance, and is expected
# to either throw an exception or return something to be stored in the 'permission' attribute of
# the new instance. Each EditTransaction for the Paleobiology Database requires an object of class
# Permissions.

sub validate_instance_permission {

    my ($class, $permission) = @_;
    
    croak "new EditTransaction: you must provide an object of class 'Permissions'"
    	unless $permission and blessed $permission and $permission->isa('Permissions');
    
    return $permission;
}


# check_table_permission ( table_specifier, requested )
# 
# This routine is called as an instance method to authorize certain kinds of actions. It is passed
# a table specifier and a requested authorization from the following list:
# 
# view           view rows in this table
# 
# post           add rows to this table; this implies authorization to modify rows you own
# 
# modify         modify rows in this table other than ones you own
# 
# admin          execute restricted actions on this table
# 
# If the result for the requested permission is already stored under 'permission_table_cache' for
# this transaction, return it. Otherwise, compute and store it.

sub check_table_permission {

    my ($edt, $table_specifier, $requested) = @_;  
    
    unless ( $edt->{permission_table_cache}{$table_specifier}{$requested} )
    {
	$edt->{permission_table_cache}{$table_specifier}{$requested} = 
	    _compute_table_permission($edt, $table_specifier, $requested);
    }
}


# _compute_table_permission ( table_specifier, requested )
# 
# The return value should be either a listref representing an error condition, or a string value
# from the following list. The result will be the first value that applies to the specified
# database table according to the permission object.
# 
# unrestricted   if this class does not do authorization checks, or if the permission object
#                indicates "superuser" permission for the system as a whole
# 
# admin          if the permission object allows the requested authorization and also indicates
#                administrative permission on this table
# 
# own            if the permission object allows the requested authorization only for objects
#                owned by the user identified by the permission object.
# 
# granted        if the permission object allows the requested authorization
# 
# none           if the permission object does not allow the requested authorization

sub _compute_table_permission {

    my ($edt, $table_specifier, $requested) = @_;
    
    # First make sure we have a valid permission object. If it is blessed, we assume it is okay
    # because it was checked by 'check_instance_permission' above.
    
    my $perms = $edt->permission;
    my $debug_mode = $edt->debug_mode;
    
    unless ( blessed $perms )
    {
	return ['main', 'E_EXECUTE', "table permission appears to be missing"];
    }
    
    # Otherwise, query the permission object for information about the specified table.
    
    my $tableinfo = $edt->table_info_ref($table_specifier);
    my $tp = $perms->get_table_permissions($table_specifier);
    
    my $dprefix = $debug_mode ? "    Permission for $table_specifier : '$requested' " : '';  # [Changed by Claude under the guidance of Michael McClennen: avoid warnings]
    my $dauth = $debug_mode ? ($perms->{auth_diag}{$table_specifier} // '') : '';
    
    # If the table has no authentication properties, return 'unrestricted'.
    
    if ( $tableinfo->{UNRESTRICTED} )
    {
	$edt->debug_line( "$dprefix unrestricted from NO AUTH PROPERTIES\n" ) if $debug_mode;
	return 'unrestricted';
    }
    
    # If the user has the superuser privilege or the 'admin' permission on this table, then they
    # have any requested permission.
    
    if ( $perms->is_superuser || $tp->{admin} )
    {
	if ( $debug_mode )
	{
	    my $which = $perms->is_superuser ? 'SUPERUSER' : 'ADMINISTRATOR';
	    $edt->debug_line( "$dprefix as admin from $which\n" );
	}
	
	return 'admin';
    }
    
    # If the user has the permission 'none', then they do not have any permission on this
    # table. This overrides any other attribute except 'admin' or superuser.
    
    if ( $tp->{none} )
    {
	$edt->debug_line( "$dprefix DENIED by $dauth\n") if $debug_mode;
	return 'none';
    }
    
    # A request to move records in a subordinate table to a different superior record is
    # granted according to the table's CAN_MOVE property, using the same categories of user as
    # CAN_POST and CAN_MODIFY. A value of 'OWNER' grants it to anybody who may modify the
    # record being moved, which is checked separately. [Added by Claude under the guidance of
    # Michael McClennen]
    
    if ( $requested eq 'move' )
    {
	my $can_move = uc($tableinfo->{CAN_MOVE} // '');
	
	if ( $can_move eq 'ALL' || $can_move eq 'ANY' || $can_move eq 'OWNER' ||
	     $can_move eq 'LOGGED_IN' && $perms->{user_id} ||
	     $can_move eq 'MEMBERS' && $perms->{enterer_no} ||
	     $can_move eq 'AUTHORIZED' && $perms->{enterer_no} && $perms->{authorizer_no} )
	{
	    return 'granted';
	}
	
	return 'none';
    }
    
    # If we know they have the requested permission, return it. The permission to insert
    # records with chosen keys is granted in the table_permissions table as 'insert_key'.
    # [Changed by Claude under the guidance of Michael McClennen: such a grant had no effect]
    
    if ( $tp->{$requested} || $requested eq 'insert' && $tp->{insert_key} )
    {
	$edt->debug_line( "$dprefix granted from $dauth\n" ) if $debug_mode;
	return 'granted';
    }
    
    # If the requested permission is 'view' but the user has only 'post' permission, then return
    # 'own' to indicate that they may view their own records only.
    
    if ( $requested eq 'view' && $tp->{post} )
    {
	$edt->debug_line( "$dprefix as 'own' from $dauth\n" ) if $debug_mode;
	return 'own';
    }
    
    # Otherwise, they have no privileges whatsoever to this table.
    
    $edt->debug_line( "$dprefix DENIED from $dauth\n" ) if $debug_mode;
    return 'none';
}


# check_row_permission ( table_specifier, requested, where )
# 
# This routine is called as an instance method to authorize certain kinds of actions. It is passed
# a table specifier, a requested authorization from the following list, and an SQL expression that
# will be used to select rows from the specified table.
# 
# view           view the rows selected from this table by the specified expression
# 
# modify         modify the rows selected from this table by the specified expression
#
# replace        replace the rows selected from this table by the specified expression
# 
# delete         delete the rows selected from this table by the specified expression
# 
# admin          execute restricted actions on the rows selected by the specified expression
# 
# If an error occurs during authorization, the return value will be a listref representing the
# error condition. If the where expression does not match any rows, the return will should be:
# 
# notfound       no row matches the specified expression
# 
# If the where expression matches one row, the return value will be the first of the following
# strings that applies to the matching row:
# 
# unrestricted   this table does not have any authorization properties
# 
# none           the requested authorization is denied for some other reason than a row lock
# 
# locked         the requested authorization would be granted except that the row has a
#                lock which the user is not authorized to clear
# 
# owned          the selected row is owned by the user identified by the permission object;
# owned_unlock   the suffix is added if an owner lock is set for this row
# 
# admin          the permission object allows the requested authorization for the selected
# admin_unlock   row and also indicates administrative permission on this table; the suffix
#                is added if any lock is set for this row
# 
# granted        the requested authorization is allowed by the permission object
# 
# If the SQL expression matches two or more rows, the return value will be a list consisting of
# strings from the above table alternating with integer values. Each string value will be
# followed by the number of rows to which it applies.

sub check_row_permission {
    
    my ($edt, $table_specifier, $requested, $where) = @_;
    
    # First make sure we have a valid permission object. If it is blessed, we assume it is okay
    # because it was checked by 'check_instance_permission' above.
    
    my $perms = $edt->permission;
    my $debug_mode = $edt->debug_mode;
    
    # Start by fetching the table properties and the user's permissions for the table as a whole.

    my $tableinfo = $edt->table_info_ref($table_specifier);
    my $tp = $perms->get_table_permissions($table_specifier);
    
    my $dprefix = $debug_mode ? "    Permission for $table_specifier ($where) : '$requested' " : '';  # [Changed by Claude under the guidance of Michael McClennen: avoid warnings]
    my $dauth = $debug_mode ? ($perms->{auth_diag}{$table_specifier} // '') : '';
    
    # If the table has no authentication properties, all we need to know is whether there are any
    # matching records. If the requested permission is 'replace', we don't even need that.
    
    if ( $tableinfo->{UNRESTRICTED} )
    {
	# If the table has a CAN_DELETE property, a 'delete' request must satisfy it. The
	# value 'NONE' refuses everybody, 'AUTHORIZED' and 'OWNER' allow anybody, and any
	# other value allows only administrators. If the property is not set, anybody may
	# delete. [Added by Claude under the guidance of Michael McClennen]
	
	my $can_delete = uc($tableinfo->{CAN_DELETE} // '');
	
	if ( $requested eq 'delete' && $can_delete && $can_delete ne 'AUTHORIZED' &&
	     $can_delete ne 'OWNER' )
	{
	    if ( $can_delete eq 'NONE' || ! ( $perms->is_superuser || $tp->{admin} ) )
	    {
		$edt->debug_line( "$dprefix DENIED by CAN_DELETE '$can_delete'\n" ) if $debug_mode;
		return 'none';
	    }
	}
	
	if ( $requested eq 'replace' )
	{
	    return ('unrestricted', 1);
	}
	
	elsif ( my $count = $edt->db_count_matching_rows($table_specifier, $where) )
	{
	    $edt->debug_line( "$dprefix unrestricted from NO AUTH PROPERTIES\n" ) if $debug_mode;
	    return ('unrestricted', $count);
	}
	
	else
	{
	    $edt->debug_line("$dprefix NOT FOUND\n") if $debug_mode;
	    return 'notfound';
	}
    }
    
    # If the user has administrative or superuser privileges, resolve the request using
    # 'check_admin_permission'. Otherwise, if the requested permission is 'admin' then return
    # 'none'.
    
    if ( $perms->is_superuser || $tp->{admin} )
    {
	return _check_admin_permission($edt, $perms, $debug_mode, $tableinfo, $tp,
				       $table_specifier, $requested, $where);
    }
    
    elsif ( $requested eq 'admin' )
    {
	$edt->debug_line( "$dprefix 'admin' DENIED for $dauth\n" ) if $debug_mode;
	return 'none';
    }
    
    # If the user has the permission 'none' on this table, that applies to every record,
    # including ones they own. [Added by Claude under the guidance of Michael McClennen]
    
    if ( $tp->{none} )
    {
	$edt->debug_line( "$dprefix DENIED by $dauth\n" ) if $debug_mode;
	return 'none';
    }
    
    # If the requested permission is 'view' for a non-administrative user, that is easy to resolve
    # for records that are not collections. [Changed by Claude under the guidance of Michael
    # McClennen: OCCURRENCE_DATA and REID_DATA have no authorization properties, so they are
    # handled as unrestricted tables above]
    
    if ( $requested eq 'view' && $table_specifier ne 'COLLECTION_DATA' )
    {
	if ( $tp->{view} || $tp->{modify} )
	{
	    if ( my $count = $edt->db_count_matching_rows($table_specifier, $where) )
	    {
		$edt->debug_line( "$dprefix granted from $dauth\n" ) if $debug_mode;
		return ('granted', $count);
	    }
	    
	    else
	    {
		$edt->debug_line("$dprefix NOT FOUND\n") if $debug_mode;
		return 'notfound';
	    }
	}
	
	# A user who has only 'post' permission may view the records they own, which are
	# determined below. [Changed by Claude under the guidance of Michael McClennen: 'none'
	# was returned here even for the user's own records]
	
	elsif ( ! $tp->{post} )
	{
	    $edt->debug_line( "$dprefix DENIED from $dauth\n" ) if $debug_mode;
	    return 'none';
	}
    }
    
    # If delete operations are not allowed on this table, reject a 'delete' request by a
    # non-administrator.
    
    if ( $requested eq 'delete' && ($tableinfo->{CAN_DELETE} // '') ne 'AUTHORIZED' &&
				   ($tableinfo->{CAN_DELETE} // '') ne 'OWNER' )
    {
	$edt->debug_line( "$dprefix DENIED by CAN_DELETE '" . ($tableinfo->{CAN_DELETE} // '') . "'\n" ) if $debug_mode;  # [Changed by Claude under the guidance of Michael McClennen]
	return 'none';
    }
    
    # Otherwise, we need to retrieve the different sets of authorization column values across all
    # records matching the key expression. Start by retrieving a list of the authorization fields
    # for this table.
    
    my $auth_fields = $tableinfo->{AUTH_COLUMN_STRING};
    
    # Then retrieve all the different combinations of field values.
    
    my $authinfo = $edt->select_authinfo($table_specifier, $auth_fields, $where);
    
    # If no matching records were found, return 'notfound' unless the requested
    # permission is 'replace', in which case return 'granted' if we have 'post' permission.
    
    unless ( $authinfo && $authinfo->@* )
    {
	if ( $requested eq 'replace' && $tp->{post} )
	{
	    $edt->debug_line("$dprefix granted for 'post'") if $debug_mode;  # [Changed by Claude under the guidance of Michael McClennen]
	    return ('granted', 1);
	}

	else
	{
	    $edt->debug_line( "$dprefix NOT FOUND\n" ) if $debug_mode;
	    return 'notfound';
	}
    }
    
    # Otherwise, go through the list and count up how many records the user has permission to
    # operate on. Each entry represents a set of records with identical authorization field values.
    
    my $owned_count = 0;	# count of records that are authorized and owned
    my $unowned_count = 0;	# count of records that are authorized but unowned
    my $unauth_count = 0;	# count of records that are denied because of ownership
    my $lock_count = 0;		# count of records that are denied because of a row lock
    
    my $unlock_count = 0;	# count of records that are owned and able to be unlocked
    
    foreach my $a ( $authinfo->@* )
    {
	my ($eno, $ano, $uid, $c, $locked);
	
	if ( $debug_mode )
	{
	    $eno = $a->{enterer_no} // '0';
	    $ano = $a->{authorizer_no} // '0';
	    $uid = $a->{enterer_id} ? 'enterer_id: ' . substr($a->{enterer_id}, 0, 10) . '...' : '';
	    $locked = '';
	    $locked .= 'ADMIN_LOCK' if $a->{admin_lock};
	    $locked .= ',OWNER_LOCK' if $a->{owner_lock};
	    $c = $a->{count};
	}
	
	# Records that are created or authorized by the current user are authorized, unless they
	# have an administrative lock.
	
	if ( $a->{enterer_no} && $perms->{enterer_no} &&
		$a->{enterer_no} eq $perms->{enterer_no}
		||
		$a->{authorizer_no} && $perms->{enterer_no} &&
		$a->{authorizer_no} eq $perms->{enterer_no}
		||
		$a->{enterer_id} && $perms->{user_id} &&
		$a->{enterer_id} eq $perms->{user_id} )
	{
	    $edt->debug_line("BY ENTERER: $c ent_no: $eno auth_no: $ano $uid $locked")
		if $debug_mode;
	    
	    if ( $a->{admin_lock} && $requested ne 'view' )
	    {
		$lock_count += $a->{count};
	    }
	    
	    elsif ( $requested eq 'delete' && ( ($tableinfo->{CAN_DELETE} // '') eq 'AUTHORIZED' ||
						($tableinfo->{CAN_DELETE} // '') eq 'OWNER' ) )
	    {
		$owned_count += $a->{count};
		$unlock_count += $a->{count} if $a->{owner_lock};
	    }
	    
	    else
	    {
		$owned_count += $a->{count};
		$unlock_count += $a->{count} if $a->{owner_lock} && $requested ne 'view';
	    }
	}
	
	# If the table has the 'BY_AUTHORIZER' property, records where the user has the
	# same authorizer as record creater are also authorized.
	
	elsif ( $tableinfo->{BY_AUTHORIZER} && $a->{authorizer_no} && $perms->{authorizer_no} &&
		$a->{authorizer_no} eq $perms->{authorizer_no} )
	{
	    $edt->debug_line("BY AUTHORIZER: $c auth_no: $ano $locked") if $debug_mode;
	    
	    if ( $a->{admin_lock} && $requested ne 'view' )
	    {
		$lock_count += $a->{count};
	    }

	    elsif ( $requested eq 'delete' && ( ($tableinfo->{CAN_DELETE} // '') eq 'AUTHORIZED' ||
						($tableinfo->{CAN_DELETE} // '') eq 'OWNER' ) )
	    {
		$owned_count += $a->{count};
		$unlock_count += $a->{count} if $a->{owner_lock};
	    }
	    
	    else
	    {
		$owned_count += $a->{count};
		$unlock_count += $a->{count} if $a->{owner_lock} && $requested ne 'view';
	    }
	}
	
	# If we get here and the requested operation is 'delete', it will
	# succeed only if the CAN_DELETE property is 'AUTHORIZED'.
	
	elsif ( $requested eq 'delete' )
	{
	    if ( ($tableinfo->{CAN_DELETE} // '') eq 'AUTHORIZED' )
	    {
		$edt->debug_line("TABLE DELETE: $c end_no: $eno auth_no: $ano $uid $locked") if $debug_mode;  # [Changed by Claude under the guidance of Michael McClennen]
		
		if ( $a->{admin_lock} || $a->{owner_lock} )
		{
		    $lock_count += $a->{count};
		}
		
		else
		{
		    $unowned_count += $a->{count};
		}
	    }
	    
	    else
	    {
		$edt->debug_line("NO PERMISSION: $c ent_no: $eno auth_no: $ano user_id: $uid")
		    if $debug_mode;
	    
		$unauth_count += $a->{count};
	    }
	}
	
	# If the table is 'COLLECTION_DATA', then a user can view any record that has been
	# released to the public or for which they meet the access criteria. Only a
	# logged-in database member with an authorizer can edit such a record. [Changed by
	# Claude under the guidance of Michael McClennen: guests and users without an
	# authorizer could edit public and released collections, and OCCURRENCE_DATA and
	# REID_DATA are handled as unrestricted tables above]
	
	elsif ( $table_specifier eq 'COLLECTION_DATA' )
	{
	    if ( $requested ne 'view' &&
		 ! ( $perms->{enterer_no} && $perms->{enterer_no} > 0 &&
		     $perms->{authorizer_no} && $perms->{authorizer_no} > 0 ) )
	    {
		$edt->debug_line("NO AUTHORIZER: $c ent_no: $eno auth_no: $ano") if $debug_mode;
		
		$unauth_count += $a->{count};
	    }
	    
	    elsif ( $edt->_check_coll_access($a, $perms) )
	    {
		$unowned_count += $a->{count};
	    }
	    
	    else
	    {
		$unauth_count += $a->{count};
	    }
	}
	
	# For all other tables, if the user has 'modify' permission on the table
	# as a whole, they can edit any record that is not locked by somebody
	# else.  Any records for which they have direct permission have already
	# counted above, so an owner lock denies authorization.
	
	elsif ( $tp->{modify} )
	{
	    $edt->debug_line("TABLE MODIFY: $c ent_no: $eno auth_no: $ano $uid $locked")
		if $debug_mode;
	    
	    if ( ( $a->{admin_lock} || $a->{owner_lock} ) && $requested ne 'view' )
	    {
		$lock_count += $a->{count};
	    }
	    
	    else
	    {
		$unowned_count += $a->{count};
	    }
	}
	
	# If the user has 'view' permission on the table as a whole, they can view any record
	# regardless of locking.
	
	elsif ( $requested eq 'view' && $tp->{view} )
	{
	    $edt->debug_line("TABLE VIEW: $c ent_no: $eno auth_no: $ano user_id: $uid $locked")
		if $debug_mode;
	    
	    $unowned_count += $a->{count};
	}
	
	# Otherwise, the action is not authorized.
	
	else
	{
	    $edt->debug_line("NO PERMISSION: $c ent_no: $eno auth_no: $ano user_id: $uid")
		if $debug_mode;
	    
	    $unauth_count += $a->{count};
	}
    }
    
    # Generate a list of permissions and counts. The first one is the "primary permission", but
    # every permission that is relevant will be added to the list followed by its corresponding
    # record count.
    
    my @permcounts;
    
    # If there are any unauthorized records, the primary permission will be 'none'.
    
    if ( $unauth_count )
    {
	$edt->debug_line("$dprefix DENIED $unauth_count") if $debug_mode;
	
	push @permcounts, 'none', $unauth_count;
    }
    
    # Otherwise, if there are any locked records then the primary permission will be
    # 'locked'. These are records that could be operated on if the owner or administrator were to
    # remove the lock.
    
    if ( $lock_count )
    {
	$edt->debug_line("$dprefix locked $lock_count") if $debug_mode;
	
	push @permcounts, 'locked', $lock_count;
    }
    
    # Otherwise, if there are any records that need unlocking then the primary permission will be
    # either 'granted_unlock' or 'owned_unlock'. The first one is odd because 'granted' and
    # 'unlock' refer to different sets of records. We need to report both occurrences in order for
    # authorize_action to function properly. This would only happen in the rare case where the set of
    # selected records includes some that are owned and locked and others that are unowned.
    
    if ( $unlock_count )
    {
	if ( $unowned_count )
	{
	    $edt->debug_line("$dprefix granted_unlock $unlock_count") if $debug_mode;
	    
	    push @permcounts, 'granted_unlock', $unlock_count;
	}

	else
	{
	    $edt->debug_line("$dprefix owned_unlock $unlock_count") if $debug_mode;
	    
	    push @permcounts, 'owned_unlock', $unlock_count;
	}
    }
    
    # Otherwise, if there are any unowned records then the primary permission will be 'granted'.
    
    if ( $unowned_count )
    {
	$edt->debug_line("$dprefix granted $unowned_count") if $debug_mode;
	
	push @permcounts, 'granted', $unowned_count;
    }
    
    # Otherwise, if all records are owned and none need unlocking then the primary permission will
    # be 'owned'.
    
    if ( $owned_count - $unlock_count )
    {
	push @permcounts, 'owned', $owned_count - $unlock_count;
    }
    
    # And finally, if no records were found at all, the primary permission will be 'notfound'.
    
    unless ( @permcounts )
    {
	$edt->debug_line("$dprefix NOT FOUND") if $debug_mode;
	push @permcounts, 'notfound';
    }
    
    $edt->debug_line("") if $debug_mode;
    
    return @permcounts;
}


sub _check_admin_permission {

    my ($edt, $perms, $debug_mode, $tableinfo, $tp, $table_specifier, $requested, $where) = @_;
    
    my $dprefix = $debug_mode ? "    Permission for $table_specifier ($where) : '$requested' " : '';  # [Changed by Claude under the guidance of Michael McClennen: avoid warnings]
    my $dauth = $debug_mode ? ($perms->{auth_diag}{$table_specifier} // '') : '';
    
    my $which = $perms->is_superuser ? 'SUPERUSER' : 'ADMIN';
    
    my $auth_fields = $tableinfo->{AUTH_COLUMN_STRING};
    
    # If deletion is not allowed for anybody, return 'none'.
    
    if ( $requested eq 'delete' && ($tableinfo->{CAN_DELETE} // '') eq 'NONE' )
    {
	$edt->debug_line("$dprefix TABLE DELETE DENIED to ADMIN\n") if $debug_mode;  # [Changed by Claude under the guidance of Michael McClennen]
	return ('none');
    }
    
    # If the table has no lock fields or the requested permission is 'view', all we need
    # to do is to check how many records match the key expression. If the requested
    # permission is 'replace', we don't even need to do that.
    
    if ( $requested eq 'view' || !$auth_fields || $auth_fields !~ /_lock/ )
    {
	if ( $requested eq 'replace' )
	{
	    return ('admin', 1);
	}
	
	if ( my $count = $edt->db_count_matching_rows($table_specifier, $where) )
	{
	    $edt->debug_line("$dprefix admin $count from $which\n") if $debug_mode;
	    return ('admin', $count);
	}
	
	else
	{
	    $edt->debug_line("$dprefix NOT FOUND\n") if $debug_mode;
	    return ('notfound');
	}
    }
    
    # Otherwise, we need to check if any of the records are locked.
    
    else
    {
	# Count the selected records by lock status.
	
	my @fields;
	push @fields, 'admin_lock' if $auth_fields =~ /admin_lock/;
	push @fields, 'owner_lock' if $auth_fields =~ /owner_lock/;
	push @fields, '1' unless @fields;
	
	my $lock_fields = join(',', @fields);
	
	# [Changed by Claude under the guidance of Michael McClennen: the arguments were in the
	# old order, and the result, which is a list reference, was not dereferenced]
	
	my @authinfo = ( $edt->select_authinfo($table_specifier, $lock_fields, $where) || [ ] )->@*;
	
	my $lock_count = 0;
	my $nolock_count = 0;
	
	foreach my $a ( @authinfo )
	{
	    if ( $debug_mode )
	    {
		my $admin = $a->{admin_lock} ? 1 : 0;
		my $owner = $a->{owner_lock} ? 1 : 0;
		
		$edt->debug_line("ADMIN: admin_lock: $admin owner_lock: $owner count: $a->{count}");
	    }
	    
	    if ( $a->{admin_lock} || $a->{owner_lock} )
	    {
		$lock_count += $a->{count};
	    }
	    
	    else
	    {
		$nolock_count += $a->{count};
	    }
	}
	
	# Then generate a list of permissions and counts. The primary permission will be
	# 'admin_unlock' if there are any locked records, 'admin' otherwise.
	
	my @permcounts;
	
	if ( $lock_count )
	{
	    $edt->debug_line("$dprefix admin_unlock $lock_count from $which") if $debug_mode;
	    push @permcounts, 'admin_unlock', $lock_count;
	}
	
	if ( $nolock_count )
	{
	    push @permcounts, 'admin', $nolock_count;
	}
	
	# If no records were found, a 'replace' is granted, as it is when the table has no
	# lock fields. [Changed by Claude under the guidance of Michael McClennen]
	
	unless ( @permcounts )
	{
	    $edt->debug_line("$dprefix NOT FOUND") if $debug_mode;
	    push @permcounts, $requested eq 'replace' ? ('admin', 1) : 'notfound';
	}
	
	$edt->debug_line("") if $debug_mode;
	
	return @permcounts;
    }
}


sub _check_coll_access {
    
    my ($edt, $authinfo, $perms) = @_;
    
    # Access is granted trivially to collections available to 'the public', and to those
    # whose release date has passed.
    
    if ( $authinfo->{access_level} eq 'the public' || $authinfo->{date_passed} )
    {
	return 1;
    }
    
    # Access is granted to collections available to 'database members' if the
    # user has a non-zero enterer_no.
    
    elsif ( $authinfo->{access_level} eq 'database members' )
    {
	return $perms->{enterer_no} > 0;
    }
    
    # Access is granted to collections available to 'group members' if any of
    # the research groups to which the user belongs matches a group associated
    # with the collection.
    
    elsif ( $authinfo->{access_level} eq 'group members' )
    {
	# If we haven't fetched the list of research groups to which the user
	# belongs, do so now.
	
	# [Changed by Claude under the guidance of Michael McClennen: the research groups
	# were those of the collection's enterer rather than the user, and were read from the
	# table 'person' rather than $TABLE{PERSON_DATA}]
	
	unless ( exists $perms->{research_group} )
	{
	    my $rg;
	    
	    if ( $perms->{enterer_no} && $perms->{enterer_no} > 0 )
	    {
		($rg) = $edt->selectrow_array("
		SELECT research_group FROM $TABLE{PERSON_DATA} WHERE person_no = ?", undef,
					      $perms->{enterer_no});
	    }
	    
	    $perms->{research_group} = [ grep { $_ } split /,/, $rg // '' ];
	}
	
	# If any of the user's groups match a research group of the collection, access is
	# granted.
	
	my %coll_group = map { $_ => 1 } split /,/, $authinfo->{research_group} // '';
	
	foreach my $group ( $perms->{research_group}->@* )
	{
	    return 1 if $coll_group{$group};
	}
	
	return 0; # otherwise, access is denied.
    }
    
    # Rows marked 'authorizer only' will have already been authorized in
    # 'check_row_permission'. If we get here, access is not ggranted.
    
    else
    {
	return 0;
    }
}

# authorize_insert_key ( )
# 
# Return 'granted' if the transaction allows INSERT_KEY, 'none' otherwise.

sub authorize_insert_key {
    
    my ($edt) = @_;
    
    if ( $edt->allows('INSERT_KEY') )
    {
	return 'granted';
    }
    
    else
    {
	return 'none';
    }
}


# select_authinfo ( table_specifier, auth_fields, where )
# 
# Fetch the specified authorization information for all records that are selected by the specified
# expression.

my ($coll_auth) = "cc.authorizer_no, cc.enterer_no, cc.access_level, cc.research_group, " .
    "(cc.release_date < now()) as date_passed";

my ($coll_auth_grouping) = "cc.authorizer_no, cc.enterer_no, cc.access_level, cc.research_group, " .
    "date_passed";

sub select_authinfo {
    
    my ($edt, $table_specifier, $auth_fields, $key_expr) = @_;
    
    my $sql;
    
    if ( $table_specifier eq 'COLLECTION_DATA' )
    {
	$sql = "SELECT $coll_auth, count(*) as `count`
		FROM $TABLE{COLLECTION_DATA} as cc 
		WHERE $key_expr GROUP BY $coll_auth_grouping";
    }
    
    # [Changed by Claude under the guidance of Michael McClennen: removed the branches for
    # OCCURRENCE_DATA and REID_DATA, which are never called because those tables have no
    # authorization properties]
    
    # If the table has no authorization columns, none of its records is owned by anybody. Count
    # the matching records, and return nothing if there are none. [Added by Claude under the
    # guidance of Michael McClennen: the query was malformed for such tables]
    
    elsif ( ! $auth_fields )
    {
	$sql = "SELECT count(*) as `count` FROM $TABLE{$table_specifier}
		WHERE $key_expr HAVING `count` > 0";
    }
    
    else
    {
	# A column with a nonstandard name is selected as "column as name", so it is grouped
	# by the name. [Changed by Claude under the guidance of Michael McClennen]
	
	my $group_fields = join(',', map { s/^.*\s+as\s+//r } split /\s*,\s*/, $auth_fields);
	
	$sql = "SELECT $auth_fields, count(*) as `count`
		FROM $TABLE{$table_specifier} WHERE $key_expr
		GROUP BY $group_fields";
    }
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    my $result = $edt->dbh->selectall_arrayref($sql, { Slice => { } });
    
    return $result;
}


# # get_authinfo_fields ( dbh, table_name, debug )
# # 
# # Return a list of the fields from the specified table that record who created each record. If
# # there are none, return false.

# our (%IS_AUTH) = (authorizer_no => 1, enterer_no => 1, enterer_id => 1, admin_lock => 1, owner_lock => 1);
# our (%AUTH_FIELD_CACHE);

# sub get_authinfo_fields {

#     my ($dbh, $table_specifier, $debug) = @_;
    
#     # If we already have this info cached, just return it.
    
#     return $AUTH_FIELD_CACHE{$table_specifier} if exists $AUTH_FIELD_CACHE{$table_specifier};
    
#     # Otherwise, get a hash of table column definitions
    
#     my $schema = get_table_schema($dbh, $table_specifier, $debug);
    
#     # If we don't have one, then barf.
    
#     unless ( $schema && $schema->{_column_list} )
#     {
# 	croak "Cannot retrieve schema for table '$table_specifier'";
#     }
    
#     # Then scan through the columns and collect up the names that are significant.
    
#     my @authinfo_fields;
    
#     foreach my $col ( @{$schema->{_column_list}} )
#     {
# 	push @authinfo_fields, $col if $IS_AUTH{$col};
#     }
    
#     my $fields = join(', ', @authinfo_fields);
#     $AUTH_FIELD_CACHE{$table_specifier} = $fields;
    
#     return $fields;
# }


# Logging
# -------

# log_event ( action, operation, table, sql, key_value )
# 
# This method overrides the default log_event method. Like the logging code in
# Classic, it stores the authorizer_no and enterer_no for each event, and it
# also stores an SQL statement which will reverse the change.
# 
# [Changed by Claude under the guidance of Michael McClennen: the table's LOG_CHANGES
# property is no longer checked here, because the caller decides which tables are logged
# (including with LOG_ALL_TABLES and NO_LOG), and statements executed by 'do_sql' have no
# table. Multiple key values are listed in the header, and the old records are fetched by
# 'before_log_event' so that an operation on multiple records can be reversed. NULL
# values are included in the reversing statement, and replacing a record that did not
# exist is reversed by deleting it.]

sub log_event {
    
    my ($edt, $action, $op, $table_specifier, $sql, $keyval) = @_;
    
    return unless $EditTransaction::LOG_FILENAME;
    return if $edt->allows('NO_LOG_MODE');
    
    my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
    my $datestr = sprintf("%04d-%02d-%02d", $year + 1900, $mon + 1, $mday);
    my $timestr = sprintf("%02d:%02d:%02d", $hour, $min, $sec);
    
    # If no key value was given, use the action's key values. If there aren't any, use
    # '?'.
    
    unless ( defined $keyval && $keyval ne '' )
    {
	$keyval = join(',', grep { defined $_ && $_ ne '' } $action->keyvalues);
	$keyval = '?' if $keyval eq '';
    }
    
    $sql =~ s/ NOW[(][)] / "'$datestr $timestr'" /ixeg;
    $sql =~ s/^\s+//;
    
    my $has_table = defined $table_specifier && $table_specifier ne '' &&
	exists $TABLE{$table_specifier};
    
    my $table_name = $has_table ? $TABLE{$table_specifier} : '-';
    
    my $auth_no = $edt->{permission} ? $edt->{permission}->authorizer_no // 0 : 0;
    my $ent_no = $edt->{permission} ? $edt->{permission}->enterer_no // 0 : 0;
    
    # Generate an SQL statement that will reverse this operation. An insert is reversed
    # by deleting the new record. An update, replace, or delete is reversed by putting
    # back the old record(s). Statements executed by 'do_sql' are not reversed.
    
    my $rev = '';
    
    if ( $has_table && $op =~ /^insert$|^update$|^replace$|^delete$/ )
    {
	my $old_records = $action->{log_old_records};
	my $keycol = $edt->table_info_ref($table_specifier)->{PRIMARY_KEY} || $action->keycol;
	
	if ( $old_records && @$old_records && $op ne 'insert' )
	{
	    $rev = $edt->_reversing_replace($table_specifier, $old_records);
	}
	
	elsif ( ( $op eq 'insert' || $op eq 'replace' ) && $keycol && $keyval ne '?' )
	{
	    my $dbh = $edt->dbh;
	    my $keylist = join(',', map { $dbh->quote($_) } split /,/, $keyval);
	    
	    $rev = "DELETE FROM $TABLE{$table_specifier} WHERE $keycol in ($keylist)";
	}
    }
    
    $edt->{log_lines} .= "# ";
    $edt->{log_lines} .= join(' | ', "$datestr $timestr", uc $op, $table_name, 
			      $keyval, $auth_no, $ent_no);
    $edt->{log_lines} .= "\n";
    $edt->{log_lines} .= _log_comment($rev);
    $edt->{log_lines} .= "$sql;\n";
    $edt->{log_date} = $datestr;
}


# before_log_event ( action, operation, table )
# 
# This is called before an operation to be logged is executed. We take this
# opportunity to fetch the existing record(s), so that we can write the info
# necessary to reverse the operation as a comment.
# 
# [Changed by Claude under the guidance of Michael McClennen: all of the records selected
# by the action are fetched, and the table's LOG_CHANGES property is no longer checked
# here, for the reasons given above under 'log_event']

sub before_log_event {
    
    my ($edt, $action, $op, $table_specifier) = @_;
    
    return unless $EditTransaction::LOG_FILENAME;
    return if $edt->allows('NO_LOG_MODE');
    
    if ( $op ne 'insert' )
    {
	# For a single record, use the old record fetched by 'fetch_old_record' so that it
	# is fetched only once.
	
	if ( ! $action->keymult )
	{
	    my $old = $edt->fetch_old_record($action, $table_specifier);
	    
	    $action->{log_old_records} = $old ? [ $old ] : [ ];
	}
	
	else
	{
	    my $quoted_table = $edt->quoted_table_name($table_specifier);
	    my $keyexpr = $action->keyexpr;
	    
	    my $sql = "SELECT * FROM $quoted_table WHERE $keyexpr";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    $action->{log_old_records} =
		$edt->dbh->selectall_arrayref($sql, { Slice => { } }) || [ ];
	}
    }
}


# log_aux_event ( operation, table, sql, key_value, rev_info )
# 
# This routine logs changes to auxiliary database tables such as
# 'REFERENCE_AUTHORS' and 'REFERENCE_EDITORS'. If $rev is a list of records, a statement
# that puts them back is written as a comment. If it is a string, it is written as a
# comment.
# 
# [Changed by Claude under the guidance of Michael McClennen: tables are logged according
# to the same rule as for 'log_event' (LOG_CHANGES, or LOG_ALL_TABLES without NO_LOG). Every
# line of the reversing statement is commented out, NULL values are included in it so that
# its values match its columns, and the logged statement ends with a semicolon.]

sub log_aux_event {
    
    my ($edt, $op, $table_specifier, $sql, $keyexpr, $rev) = @_;
    
    return unless $EditTransaction::LOG_FILENAME;
    return if $edt->allows('NO_LOG_MODE');
    
    my $tableinfo = $edt->table_info_ref($table_specifier) || return;
    
    return unless $tableinfo->{LOG_CHANGES} ||
	$EditTransaction::LOG_ALL_TABLES && ! $tableinfo->{NO_LOG};
    
    my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
    my $datestr = sprintf("%04d-%02d-%02d", $year + 1900, $mon + 1, $mday);
    my $timestr = sprintf("%02d:%02d:%02d", $hour, $min, $sec);
    
    $keyexpr //= '?';
    
    $sql =~ s/ NOW[(][)] / "'$datestr $timestr'" /ixeg;
    $sql =~ s/^\s+//;
    $sql =~ s/;\s*$//;
    
    my $auth_no = $edt->{permission} ? $edt->{permission}->authorizer_no // 0 : 0;
    my $ent_no = $edt->{permission} ? $edt->{permission}->enterer_no // 0 : 0;
    
    if ( ref $rev eq 'ARRAY' )
    {
	$rev = @$rev ? $edt->_reversing_replace($table_specifier, $rev) : '';
    }
    
    elsif ( ref $rev )
    {
	$rev = '';
    }
    
    $edt->{log_lines} .= "# ";
    $edt->{log_lines} .= join(' | ', "$datestr $timestr", uc $op, $TABLE{$table_specifier}, 
			      $keyexpr, $auth_no, $ent_no);
    $edt->{log_lines} .= "\n";
    $edt->{log_lines} .= _log_comment($rev);
    $edt->{log_lines} .= "$sql;\n";
    $edt->{log_date} = $datestr;
}


# _reversing_replace ( table, records )
# 
# Return a REPLACE statement that will put back the specified records, which must be
# hashrefs. Every column of the table is included, with NULL for undefined values.
# 
# Added by Claude under the guidance of Michael McClennen.

sub _reversing_replace {
    
    my ($edt, $table_specifier, $records) = @_;
    
    my $dbh = $edt->dbh;
    
    my @fields = $edt->table_column_list($table_specifier);
    
    my @rows = map { my $r = $_; '(' . join(',', map { $dbh->quote($r->{$_}) } @fields) . ')' }
	@$records;
    
    return "REPLACE INTO $TABLE{$table_specifier} (" . join(',', @fields) . ") VALUES " .
	join(', ', @rows);
}


# _log_comment ( text )
# 
# Return the specified text as a comment for the log, with a hashmark at the beginning of
# every line. Return the empty string if the text is empty.
# 
# Added by Claude under the guidance of Michael McClennen.

sub _log_comment {
    
    my ($text) = @_;
    
    return '' unless defined $text && $text ne '';
    
    $text =~ s/\n$//;
    
    return '# ' . ($text =~ s/\n/\n# /gr) . "\n";
}

1;
