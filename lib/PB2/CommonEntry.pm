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

use ExternalIdent qw(extract_identifier generate_identifier %IDP %IDRE);
use PB2::TableData qw(get_table_schema);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::TableData);



sub get_main_params {
    
    my ($request, $conditions_ref, $ignore_ref) = @_;
    
    my @request_keys = $request->param_keys;
    my $main_params = { };
    
    $ignore_ref ||= { };
    
    foreach my $k ( @request_keys )
    {
	my @list = $request->clean_param_list($k);
	
	if ( $k eq 'show' || $k eq 'return' || $k eq 'record_label' || $ignore_ref->{$k} )
	{
	    next;
	}
	
	elsif ( $k eq 'allow' )
	{
	    $conditions_ref->{$_} = 1 foreach @list;
	}
	
	elsif ( @list == 1 )
	{
	    $main_params->{$k} = $list[0];
	}
	
	else {
	    $main_params->{$k} = \@list;
	}
    }
    
    return $main_params;
}


sub get_auth_info {
    
    my ($request, $dbh, $message) = @_;
    
    if ( $request->{my_auth_info} )
    {
	return $request->{my_auth_info};
    }
    
    $dbh ||= $request->get_connection;
    
    if ( my $cookie_id = Dancer::cookie('session_id') )
    {
	my $session_id = $dbh->quote($cookie_id);
	
	my $sql = "
		SELECT authorizer_no, enterer_no, superuser FROM session_data
		WHERE session_id = $session_id";
	
	my $auth_info = $dbh->selectrow_hashref($sql);
	
	unless ( ref $auth_info eq 'HASH' && $auth_info->{authorizer_no} && $auth_info->{enterer_no} )
	{
	    die $request->exception(401, $message || "You must be logged in to perform this operation");
	}
	
	return $request->{my_auth_info} = $auth_info;
    }
    
    else
    {
	die $request->exception(401, $message || "You must be logged in to perform this operation");
    }
}


sub unpack_input_records {
    
    my ($request, $main_params_ref) = @_;
    
    # First decode the body, and extract parameters from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my ($body, $error) = $request->decode_body;
    
    if ( $error )
    {
	die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
    }
    
    # If there was no request body at all, just return the hashref of main parameters minus
    # those specified to be ignored. These will constitute the entire input record.
    
    elsif ( ! $body )
    {
	return $main_params_ref;
    }
    
    # Otherwise, if the request body is an object with the key 'records' and an array value,
    # then we assume that the array is a list of inptut records. If there is also a key 'all' with
    # an object value, then we assume that it gives common parameters to be applied to all records.
    
    my (@body_records, @records);
    
    if ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
    {
	push @body_records, @{$body->{records}};
	
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
	push @body_records, $body;
    }
    
    # If the body is in fact an array, and that array is either empty or contains at least one
    # object, then assume its elements are records.
    
    elsif ( ref $body eq 'ARRAY' && ( @$body == 0 || ref $body->[0] eq 'HASH'  ) )
    {
	push @body_records, @$body;
    }
    
    # Otherwise, we must return a 400 error.
    
    else
    {
	$request->add_error("E_BODY: Badly formatted request body: no record found");
	die $request->exception(400, "Invalid request");
    }
    
    # Now, for each record, substitute any missing parameters with the values
    # given by $main_params_ref.
    
    foreach my $r ( @body_records )
    {
	unless ( ref $r eq 'HASH' )
	{
	    $request->add_error("E_BODY: Invalid body element '$r'");
	    die $request->exception(400, "Invalid request");
	}
	
	unless ( keys %$r )
	{
	    next;
	}
	
	foreach my $k ( keys %$main_params_ref )
	{
	    unless ( exists $r->{$k} )
	    {
		$r->{$k} = $main_params_ref->{$k};
	    }
	}
	
	push @records, $r;
    }
    
    # Now, return the list of records, which might be empty.
    
    return @records;
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


sub validate_against_table {
    
    my ($request, $dbh, $table_name, $op, $record, $label) = @_;
    
    croak "bad record" unless ref $record eq 'HASH';
    
    my $lstr = defined $label && $label ne '' ? " ($label)" : "";
    
    # $$$ op = add or update
    
    my (@fields, @values);
    
    my $schema = get_table_schema($request, $dbh, $table_name);
    
    foreach my $k ( keys %$record )
    {
	next if $k eq 'record_label';
	
	my $value = $record->{$k};
	my $field_record = $schema->{$k};
	my $field = $field_record->{Field};
	my $type = $field_record->{Type};
	my $key = $field_record->{Key};
	
	unless ( $field_record && $field )
	{
	    $request->add_warning("W_PARAM$lstr: unknown field '$k' in the table for this data type");
	    next;
	}
	
	if ( $field eq 'created' || $field eq 'modified' || $field eq 'authorizer_no' || $field eq 'enterer_no' || $key eq 'PRI' )
	{
	    next;
	}
	
	if ( ! defined $value )
	{
	    push @fields, $field;
	    push @values, "NULL";
	}
	
	elsif ( $type =~ /int\(/ )
	{
	    unless ( defined $value && $value =~ /^\d+$/ )
	    {
		$request->add_error("E_PARAM$lstr: field '$k' must have an integer value (was '$value')");
		next;
	    }
	    
	    push @fields, $field;
	    push @values, $value;
	}
	
	# elsif other types ...
	
	# otherwise, treat this as a string value
	
	else
	{
	    my $quoted = $dbh->quote($value);
	    
	    push @fields, $field;
	    push @values, $quoted;
	}
    }
    
    # Now add the appropriate values for any of the built-in fields.
    
    foreach my $k ( qw(authorizer_no enterer_no modifier_no modified) )
    {
	next unless $schema->{$k};
	
	next if $k eq 'modifier_no' && $op eq 'add';
	
	push @fields, $k;
	
	if ( $k eq 'authorizer_no' )
	{
	    croak "unauthorized access" unless $request->{my_auth_info}{authorizer_no};
	    push @values, $request->{my_auth_info}{authorizer_no};
	}
	
	elsif ( $k eq 'enterer_no' || $k eq 'modifier_no' )
	{
	    croak "unauthorized access" unless $request->{my_auth_info}{enterer_no};
	    push @values, $request->{my_auth_info}{enterer_no};
	}
	
	elsif ( $k eq 'modified' )
	{
	    push @values, "NOW()";
	}
	
	else
	{
	    croak "bad built-in field name";
	}
    }
    
    $record->{_fields} = \@fields;
    $record->{_values} = \@values;
    
    # if ( $op eq 'add' )
    # {
    # 	my $field_string = join(',', @fields);
    # 	my $value_string = join(',', @values);
	
    # 	return $field_string, $value_string;
    # }
    
    # elsif ( $op eq 'update' )
    # {
    # 	my @exprs;
	
    # 	foreach my $i ( 0..$#$fields )
    # 	{
    # 	    push @exprs, "$fields[$i] = $values[$i]";
    # 	}
	
    # 	my $expr_string = join(",\n", @exprs);
	
    # 	return $expr_string;
    # }
    
    # else
    # {
    # 	croak "bad value for 'op'";
    # }
}


sub do_add {
    
    my ($request, $dbh, $table_name, $r, $conditions_ref) = @_;
    
    $dbh ||= $request->get_connection;
    $conditions_ref ||= { };
    
    croak "bad record" unless ref $r eq 'HASH' && ref $r->{_fields} eq 'ARRAY';
    croak "empty record" unless @{$r->{_fields}};
    
    my $field_string = join(',', @{$r->{_fields}});
    my $value_string = join(',', @{$r->{_values}});
    
    my $quoted_table = $dbh->quote_identifier($table_name);
    
    my $sql = "INSERT INTO $quoted_table ($field_string) VALUES ($value_string)";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result = $dbh->do($sql);
    
    my $insert_id = $dbh->last_insert_id(undef, undef, undef, undef);
    print STDERR "RESULT: 0\n" unless $insert_id;
    
    return $insert_id;
}


sub do_update {

    my ($request, $dbh, $table_name, $key_expr, $r, $conditions_ref) = @_;

    $dbh ||= $request->get_connection;
    $conditions_ref ||= { };
    
    croak "bad record" unless ref $r eq 'HASH' && ref $r->{_fields} eq 'ARRAY';
    croak "empty record" unless @{$r->{_fields}};
    croak "empty key expr" unless $key_expr;
    
    my @exprs;
    
    foreach my $i ( 0..$#{$r->{_fields}} )
    {
	push @exprs, "$r->{_fields}[$i] = $r->{_values}[$i]";
    }
    
    my $set_string = join(",\n", @exprs);
    my $quoted_table = $dbh->quote_identifier($table_name);
    
    my $sql = " UPDATE $quoted_table
		SET $set_string
		WHERE $key_expr LIMIT 1";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result = $dbh->do($sql);
    
    return $result;
}


1;
