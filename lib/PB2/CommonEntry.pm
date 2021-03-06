# 
# CommonEntry.pm
# 
# A class that contains common routines for supporting PBDB data entry.
# 
# Author: Michael McClennen

package PB2::CommonEntry;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(croak);

use TableDefs qw(get_table_property);

use ExternalIdent qw(extract_identifier generate_identifier %IDP %IDRE);
use TableData qw(get_table_schema);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::TableData PB2::Authentication);


# The following hash will cache the list of ruleset parameters corresponding to various
# rulesets. The hash keys are ruleset names.

our (%RULESET_HAS_PARAM);



# get_main_params ( conditions_ref, entry_ruleset )
# 
# Go through the main parameters to this request, looking for the following. If the parameter
# 'allow' is found, then enter all of the specified conditions as hash keys into
# $conditions_ref. These will already have been checked by the initial ruleset validation of the
# request. Otherwise, any parameters we find that are specified in the ruleset whose name is given
# in $entry_ruleset should be copied into a hash which is then returned as the result. These
# parameters will be used as defaults for all records speified in the request body.

sub get_main_params {
    
    my ($request, $allowances_ref, $entry_ruleset) = @_;
    
    # First grab a list of the parameters that were specified in the request URL. Also allocate a
    # new hash which will end up being the return value of this routine.
    
    my @request_keys = $request->param_keys;
    my $main_params = { };
    
    # If we have not already extracted the list of parameters corresponding to the named ruleset,
    # do that now.
    
    if ( $entry_ruleset && ! defined $RULESET_HAS_PARAM{$entry_ruleset} )
    {
	my @ruleset_params = $request->ds->list_ruleset_params($entry_ruleset);
	
	foreach my $p ( @ruleset_params )
	{
	    $RULESET_HAS_PARAM{$entry_ruleset}{$p} = 1 if $p;
	}
    }
    
    foreach my $k ( @request_keys )
    {
	if ( $k eq 'allow' )
	{
	    my @list = $request->clean_param_list($k);
	    $allowances_ref->{$_} = 1 foreach @list;
	    next;
	}
	
	if ( $entry_ruleset && $RULESET_HAS_PARAM{$entry_ruleset}{$k} )
	{
	    $main_params->{$k} = $request->clean_param($k);
	}
    }
    
    if ( $request->debug )
    {
	$request->debug_out($main_params);
    }
    
    return $main_params;
}


sub unpack_input_records {
    
    my ($request, $main_params_ref, $entry_ruleset, $main_key) = @_;
    
    my (@raw_records, @records, @rulesets);
    my ($body, $error);

    # Mark this request as a "data entry request" so that the external identifier validator knows
    # to accept label references.

    $request->{is_data_entry} = 1;
    
    # If we were given one or more listrefs indicating multiple rulesets, unpack them.
    
    if ( ref $entry_ruleset eq 'ARRAY' )
    {
	my ($foo, $bar, @args) = @_;

	foreach my $list (@args)
	{
	    croak "argument must be a listref, was $list" unless ref $list eq 'ARRAY';

	    push @rulesets, $list;
	}
    }
    
    elsif ( $entry_ruleset )
    {
	push @rulesets, [$entry_ruleset, $main_key, 'DEFAULT'];
    }
    
    # If the method is GET, then we don't expect any body. Assume that the main parameters
    # constitute the only input record.
    
    if ( Dancer::request->method eq 'GET' )
    {
	push @raw_records, $main_params_ref;
    }
    
    # Otherwise decode the body and extract input records from it. If an error occured, return an
    # HTTP 400 result.
    
    else
    {
	($body, $error) = $request->decode_body;
	
	if ( $error )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
	}
	
	# If there was no request body at all, throw an exception.
	
	elsif ( ! defined $body || $body eq '' )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Request body must not be empty with PUT or POST");
	}
	
	# Otherwise, if the request body is an object with the key 'records' and an array value,
	# then we assume that the array is a list of input records. If there is also a key 'all'
	# with an object value, then we assume that it gives common parameters to be applied to
	# all records.
	
	if ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
	{
	    push @raw_records, @{$body->{records}};
	    
	    if ( ref $body->{all} eq 'HASH' )
	    {
		foreach my $k ( keys %{$body->{all}} )
		{
		    $main_params_ref->{$k} = $body->{all}{$k};
		}
	    }
	}
	
	# If we don't find a 'records' key with an array value, then assume that the body is a single
	# record.
	
	elsif ( ref $body eq 'HASH' )
	{
	    push @raw_records, $body;
	}
	
	# If the body is in fact an array, and that array is either empty or contains at least one
	# object, then assume its elements are records.
	
	elsif ( ref $body eq 'ARRAY' && ( @$body == 0 || ref $body->[0] eq 'HASH'  ) )
	{
	    push @raw_records, @$body;
	}
	
	# Otherwise, we must return a 400 error.
	
	else
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: no record found");
	}
    }
    
    # Now, for each record, substitute any missing parameters with the values
    # given by $main_params_ref. Then validate it according to the specified ruleset.
    
    foreach my $r ( @raw_records )
    {
	unless ( ref $r eq 'HASH' )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Invalid body element '$r'");
	}
	
	unless ( keys %$r )
	{
	    next;
	}
	
	if ( $r == $main_params_ref )
	{
	    push @records, $r;
	    next;
	}
	
	foreach my $k ( keys %$main_params_ref )
	{
	    unless ( exists $r->{$k} )
	    {
		$r->{$k} = $main_params_ref->{$k};
	    }
	}
	
	# If a ruleset was specified for validating the parameters, then apply
	# it. Unfortunately, HTTP::Validate (at the time this code was
	# written) does not properly handle parameters with empty values. So
	# a quick hack fills these in.
	
	# If there is more than one ruleset, we try them one at a time.
	
	my ($ruleset, $main_key, $check_key);
	
      RULESET:
	foreach my $list ( @rulesets )
	{
	    ($ruleset, $main_key, $check_key) = @$list;
	    
	    # Check to see if this ruleset is the proper one to apply.

	    my $ok;
	    
	    if ( $check_key && ($check_key eq 'DEFAULT' || exists $r->{$check_key}) )
	    {
		$ok = 1;
	    }
	    
	    elsif ( $main_key && exists $r->{$main_key} )
	    {
		$ok = 1;
	    }
	    
	    next RULESET unless $ok;
	    
	    # Validate the input record against the specified ruleset.
	    
	    my $result = $request->validate_params($ruleset, $r);
	    
	    # Fill in any parameters that were included with empty values.
	    
	    my $raw = $result->raw;
	    
	    foreach my $k ( keys %$raw )
	    {
		if ( defined $raw->{$k} && $raw->{$k} eq '' )
		{
		    $result->{clean}{$k} = '';
		    push @{$result->{clean_list}}, $k;
		}
	    }
	    
	    # If any errors or warnings were generated, add them to the
	    # current request.
	    
	    my $label = $r->{_label} || ($main_key && $r->{$main_key}) || '';
	    my $lstr = $label ? " ($label)" : "";
	    
	    foreach my $e ( $result->errors )
	    {
		if ( $e =~ /identifier must have type/ )
		{
		    $request->add_error("E_EXTTYPE$lstr: $e");
		}

		elsif ( $e =~ /may not specify/ )
		{
		    $request->add_error("E_PARAM$lstr: $e");
		}
		
		else
		{
		    $request->add_error("E_FORMAT$lstr: $e");
		}
	    }
	    
	    foreach my $w ( $result->warnings )
	    {
		$request->add_warning("W_PARAM$lstr: $w");
	    }
	    
	    # Then add the hash of cleaned parameter values to the list of
	    # records to add or update.
	    
	    push @records, $result->values;
	    last;
	}
	
	croak "no ruleset found for record" unless $ruleset;
    }
    
    if ( $request->debug )
    {
	foreach my $r ( @records )
	{
	    $request->debug_out($r, '    ');
	}
    }
    
    # Now, return the list of records, which might be empty.
    
    return @records;
}


sub debug_line {
    
    my ($request, $line) = @_;
    
    print STDERR "$line\n";
}

# sub validate_against_table {
    
#     my ($request, $dbh, $table_name, $op, $record, $primary_key, $ignore_keys) = @_;
    
#     croak "bad record" unless ref $record eq 'HASH';
#     croak "bad value '$op' for op" unless $op eq 'add' || $op eq 'update' || $op eq 'mirror';
    
#     my (@fields, @values);
    
#     $dbh ||= $request->get_connection;
    
#     # Get the schema for the requested table.
    
#     my $schema = get_table_schema($request, $dbh, $table_name);
    
#     croak "no schema found for table '$table_name'" unless $schema;
    
#     # Determine the name of the table without any database prefix.
    
#     my $lookup_name = $table_name;
#     $lookup_name =~ s/^\w+[.]//;
    
#     # First go through all of the fields in the record and validate their values.
    
#     foreach my $k ( keys %$record )
#     {
# 	# Skip any field called 'record_label', because that is only used by the data service and
# 	# has no relevance to the backend database.
	
# 	next if $k eq 'record_label';
	
# 	# If we were given a hash of keys to ignore, then ignore them.
	
# 	next if ref $ignore_keys && $ignore_keys->{$k};
	
# 	# For each field in the record, fetch the correspondingly named field from the schema.
	
# 	my $value = $record->{$k};
# 	my $field_record = $schema->{$k};
# 	my $field = $field_record->{Field};
# 	my $type = $field_record->{Type};
# 	my $key = $field_record->{Key};
	
# 	# If no such field is found, skip this one. Add a warning to the request unless the
# 	# operation is 'mirror', in which case the mirror table may be missing some of the fields
# 	# of the original one.
	
# 	unless ( $field_record && $field)
# 	{
# 	    $request->add_record_warning('W_PARAM', $record->{record_label} || $record->{$primary_key}, 
# 					 "field '$k' does not match any column in the table for this data type")
# 		unless $op eq 'mirror';
	    
# 	    next;
# 	}
	
# 	# The following set of fields is handled automatically, and are skipped if they appear in
# 	# the record.
	
# 	if ( $field eq 'modified_no' || $field eq 'authorizer_no' || $field eq 'enterer_no' ||
# 	     $field eq 'modified' || $field eq 'modified_on' || $field eq 'enterer_id' )
# 	{
# 	    next;
# 	}
	
# 	# The following set of fields are skipped automatically unless the operation is 'mirror'
# 	# and the field has a value in the source record. This includes the primary key for the
# 	# table, if one was specified. For add operations there is no primary key value yet, and
# 	# for updates it is handled separately.
	
# 	if ( $field eq 'created' || $field eq 'created_on' || ($primary_key && $field eq $primary_key) )
# 	{
# 	    next unless $op eq 'mirror' && defined $value && $value ne '';
# 	}
	
# 	# Now we add the field and its value to their respective lists. These can be used later to
# 	# construct an SQL insert or update statement.
	
# 	# If the field value is undefined, set it to "NULL". We need to do this because the user
# 	# may specifically want to null out that field in the record.
	
# 	if ( ! defined $value )
# 	{
# 	    push @fields, $field;
# 	    push @values, "NULL";
# 	}
	
# 	# Otherwise, if the field has an integer type then make sure the corresponding value is an
# 	# integer. Record an error otherwise.
	
# 	elsif ( $type =~ /int\(/ )
# 	{
# 	    unless ( defined $value && $value =~ /^\d+$/ )
# 	    {
# 		$request->add_record_error('E_PARAM', $record->{record_label} || $record->{$primary_key}, 
# 					   "field '$k' must have an integer value (was '$value')");
# 		next;
# 	    }
	    
# 	    push @fields, $field;
# 	    push @values, $value;
# 	}
	
# 	# elsif other types ...
	
# 	# Otherwise, treat this as a string value. If the value we get is an array, just
# 	# concatenate the items with commas. (This will probably have to be revisited later, and
# 	# made into a more general mechanism).
	
# 	else
# 	{
# 	    if ( ref $value eq 'ARRAY' )
# 	    {
# 		$value = join(',', @$value);
# 	    }
	    
# 	    my $quoted = $dbh->quote($value);
	    
# 	    push @fields, $field;
# 	    push @values, $quoted;
# 	}
#     }
    
#     # Now add the appropriate values for any of the built-in fields.
    
#     foreach my $k ( qw(authorizer_no enterer_no modifier_no modified modified_on enterer_id) )
#     {
# 	next unless $schema->{$k};
	
# 	my $new_value;
	
# 	# We don't set 'authorizer_no' and 'enterer_no' on update, the values given when the
# 	# record was created are retained. If we are mirroring from another record, then we copy
# 	# the value from that record if there is one. Otherwise, we use the authorizer_no and
# 	# enterer_no associated with the requestor.
	
# 	if ( $k eq 'authorizer_no' || $k eq 'enterer_no' )
# 	{
# 	    next if $op eq 'update';
	    
# 	    $new_value = $op eq 'mirror' && defined $record->{$k} && $record->{$k} ne '' ? 
# 		$dbh->quote($record->{$k}) : 
# 		$request->{my_auth_info}{$k};
	    
# 	    # If the table has an authorizer_no or enterer_no field, then we need to be providing
# 	    # a value for them. If no value can be found, then bomb.
	    
# 	    unless ( $new_value || ( $TABLE_PROPERTIES{$lookup_name}{ALLOW_POST} &&
# 				     $TABLE_PROPERTIES{$lookup_name}{ALLOW_POST} eq 'LOGGED_IN' ) )
# 	    {
# 		croak "no value was found for '$k'";
# 	    }
	    
# 	    # unless ( $new_value )
# 	    # {
# 	    # 	if ( $request->{my_auth_info}{guest_no} && $TABLE_PROPERTIES{$table_name}{ALLOW_POST} &&
# 	    # 	     $TABLE_PROPERTIES{$table_name}{ALLOW_POST} eq 'LOGGED_IN' )
# 	    # 	{
# 	    # 	    $new_value = $k eq 'enterer_no' ? $request->{my_auth_info}{guest_no} : 0;
# 	    # 	}

# 	    # 	else
# 	    # 	{
# 	    # 	    croak "no value was found for '$k'";
# 	    # 	}
# 	    # }
# 	}
	
# 	# We only set 'enterer_id' on record creation, not on update.
	
# 	elsif ( $k eq 'enterer_id' )
# 	{
# 	    next if $op eq 'update';
	    
# 	    $new_value = $dbh->quote($request->{my_auth_info}{user_id} || '');
# 	}
	
# 	# The modifier_no field is in some sense the opposite of authorizer_no and enterer_no,
# 	# since it is set on update but not when we are adding a new record. If we are mirroring
# 	# from another record, use the value from that record if there is one. Otherwise, set this
# 	# field to the enterer_no associated with the requestor.
	
# 	elsif ( $k eq 'modifier_no' )
# 	{
# 	    next if $op eq 'add';
	    
# 	    $new_value = $op eq 'mirror' && defined $record->{$k} && $record->{$k} ne '' ? 
# 		$dbh->quote($record->{$k}) : 
# 		$request->{my_auth_info}{enterer_no};
# 	}
	
# 	# The modification timestamp is copied if the record is being mirrored, and otherwise set
# 	# to the current timestamp unless the operation is 'add'.
	
# 	elsif ( $k eq 'modified' || $k eq 'modified_on' )
# 	{
# 	    next if $op eq 'add';
	    
# 	    $new_value = $op eq 'mirror' && defined $record->{$k} && $record->{$k} ne '' ?
# 		$dbh->quote($record->{$k}) :
# 		'NOW()';
# 	}
	
# 	push @fields, $k;
# 	push @values, $new_value;
#     }
    
#     # We stuff the fields and values into special record keys, so they will be available for use
#     # in constructing subsequent SQL statements.
    
#     $record->{_fields} = \@fields;
#     $record->{_values} = \@values;
# }


# sub add_edt_warnings {
    
#     my ($request, $edt) = @_;
    
#     my (@warnings) = $edt->warnings;
    
#     return unless @warnings;
#     my %added;
    
#     foreach my $w ( @warnings )
#     {
#     	my $code = $w->code;
# 	my $label = $w->label;
# 	my $table = $w->table;
# 	my $explanation = $edt->generate_msg($w);
# 	# my $explanation = join('; ', $w->data);
	
# 	my $str = $code;
# 	$str .= " ($label)" if defined $label && $label ne '';
# 	$str .= ": $explanation" if defined $explanation && $explanation ne '';
	
# 	unless ( $added{$str} )
# 	{
# 	    $request->add_warning($str);
# 	    $added{$str} = 1;
# 	}
#     }
# }


sub collect_edt_warnings {
    
    my ($request, $edt) = @_;
    
    my @strings = $edt->warning_strings;

    foreach my $m ( @strings )
    {
	$request->add_warning($m);
    }
}


sub collect_edt_errors {
    
    my ($request, $edt) = @_;
    
    my @strings = $edt->error_strings;

    foreach my $m ( @strings )
    {
	$request->add_error($m);
    }
}


sub add_edt_errors {
    
    my ($request, $edt) = @_;
    
    my (@errors) = $edt->errors;
    
    return unless @errors;
    my %added;
    
    foreach my $e ( @errors )
    {
    	my $code = $e->code;
	my $label = $e->label;
	my $table = $e->table;
	my $explanation = $edt->generate_msg($e);
	# my $explanation = join('; ', $e->data);
	
	my $str = $code;
	$str .= " ($label)" if defined $label && $label ne '';
	$str .= ": $explanation" if defined $explanation && $explanation ne '';
	
	unless ( $added{$str} )
	{
	    $request->add_error($str);
	    $added{$str} = 1;
	}
    }
}


sub validate_extident {

    my ($request, $type, $value, $param) = @_;
    
    $param ||= '';
    
    if ( defined $value && $value =~ $IDRE{$type} )
    {
	return $2;
    }
    
    elsif ( $1 && $1 ne $IDP{$type} )
    {
	$request->add_error("E_PARAM: the value of '$param' must be an identifier of type '$IDP{$type}' (type '$1' is not allowed for this operation)");
    }
    
    else
    {
	$request->add_error("E_PARAM: the value of '$param' must be a valid external identifier of type '$IDP{$type}'");
    }
}


# sub do_add {
    
#     my ($request, $dbh, $table_name, $r) = @_;
    
#     $dbh ||= $request->get_connection;
    
#     croak "bad record" unless ref $r eq 'HASH' && ref $r->{_fields} eq 'ARRAY';
#     croak "empty record" unless @{$r->{_fields}};
    
#     my $field_string = join(',', @{$r->{_fields}});
#     my $value_string = join(',', @{$r->{_values}});
    
#     my $quoted_table = $table_name; # $dbh->quote_identifier($table_name);
    
#     my $sql = "INSERT INTO $quoted_table ($field_string) VALUES ($value_string)";
    
#     print STDERR "$sql\n\n" if $request->debug;
    
#     my $result = $dbh->do($sql);
    
#     my $insert_id = $dbh->last_insert_id(undef, undef, undef, undef);
#     print STDERR "RESULT: 0\n" unless $insert_id;
    
#     return $insert_id;
# }


# sub do_replace {
    
#     my ($request, $dbh, $table_name, $r) = @_;
    
#     $dbh ||= $request->get_connection;
    
#     croak "bad record" unless ref $r eq 'HASH' && ref $r->{_fields} eq 'ARRAY';
#     croak "empty record" unless @{$r->{_fields}};
    
#     my $field_string = join(',', @{$r->{_fields}});
#     my $value_string = join(',', @{$r->{_values}});
    
#     my $quoted_table = $table_name; # $dbh->quote_identifier($table_name);
    
#     my $sql = "REPLACE INTO $quoted_table ($field_string) VALUES ($value_string)";
    
#     print STDERR "$sql\n\n" if $request->debug;
    
#     my $result = $dbh->do($sql);
    
#     return $result;
# }


# sub do_update {

#     my ($request, $dbh, $table_name, $key_expr, $r) = @_;

#     $dbh ||= $request->get_connection;
    
#     croak "bad record" unless ref $r eq 'HASH' && ref $r->{_fields} eq 'ARRAY';
#     croak "empty record" unless @{$r->{_fields}};
#     croak "empty key expr" unless $key_expr;
    
#     my @exprs;
    
#     foreach my $i ( 0..$#{$r->{_fields}} )
#     {
# 	push @exprs, "$r->{_fields}[$i] = $r->{_values}[$i]";
#     }
    
#     my $set_string = join(",\n", @exprs);
#     my $quoted_table = $table_name; # $dbh->quote_identifier($table_name);
    
#     my $sql = " UPDATE $quoted_table
# 		SET $set_string
# 		WHERE $key_expr LIMIT 1";
    
#     print STDERR "$sql\n\n" if $request->debug;
    
#     my $result = $dbh->do($sql);
    
#     return $result;
# }


# sub fetch_record {
    
#     my ($request, $dbh, $table_name, $key_expr, $field_expr) = @_;
    
#     $dbh ||= $request->get_connection;
    
#     croak "empty key expr" unless $key_expr;
#     croak "empty field expr" unless $field_expr;
    
#     my $quoted_table = $table_name; # $dbh->quote_identifier($table_name);
    
#     my $sql = "	SELECT $field_expr FROM $quoted_table
# 		WHERE $key_expr LIMIT 1";
    
#     print STDERR "$sql\n\n" if $request->debug;
    
#     return $dbh->selectrow_hashref($sql, { Slice => { } });
# }


# sub fetch_record_permission {
    
#     my ($request, $dbh, $table_name, $permission, $id_field, $record_no, $auth_fields) = @_;
    
#     # If we need to determine what permissions the requestor has to this
#     # particular record. If no record was given, return 'notfound'.
    
#     unless ( $record_no && $record_no =~ /^\d+$/ )
#     {
# 	print STDERR "ERROR: no record number was given\n\n" if $request->debug;
# 	return (undef, 'notfound');
#     }
    
#     # The first thing to do is to fetch the specified fields from the record.
    
#     my $sql = "	SELECT $auth_fields FROM $table_name
# 		WHERE $id_field = $record_no LIMIT 1";
    
#     print STDERR "$sql\n\n" if $request->debug;
    
#     my ($record) = $dbh->selectrow_hashref($sql);
    
#     # If no record is found, then we return 'notfound'. Otherwise, set the status if there is
#     # one. 
    
#     my $status;
    
#     if ( $record )
#     {
# 	my $permission = $request->check_fetched_permission($table_name, $permission, $record, $id_field)
# 	return ($permission, $record->{status} || '');
#     }
    
#     else
#     {
# 	return (undef, 'notfound');
#     }
# }


sub add_record_error {
    
    my ($request, $code, $label, $string) = @_;
    
    my $err = $code;
    $err .= " ($label)" if defined $label && $label ne '';
    $err .= ": $string" if defined $string && $string ne '';
    
    unless ( $request->{my_err_hash}{$err} )
    {
	$request->{my_err_hash}{$err} = 1;
	$request->add_error($err);
    }
    
    return;
}


sub add_record_warning {
    
    my ($request, $code, $label, $string) = @_;
    
    my $err = $code;
    $err .= " ($label)" if defined $label && $label ne '';
    $err .= ": $string" if defined $string && $string ne '';
    
    unless ( $request->{my_warning_hash}{$err} )
    {
	$request->{my_warning_hash}{$err} = 1;
	$request->add_warning($err);
    }
    
    return;
}


sub debug_out {

    my ($request, $record, $prefix) = @_;
    
    return unless ref $record eq 'HASH';
    
    $prefix ||= '';
    
    foreach my $k ( keys %$record )
    {
	my $value = ref $record->{$k} eq 'ARRAY' ? '(' . join(', ', @{$record->{$k}}) . ')'
	    : $record->{$k};
	
	if ( defined $value )
	{
	    $request->{ds}->debug_line("$prefix$k = $value");
	}
    }
    
    $request->{ds}->debug_line("");
}

1;
