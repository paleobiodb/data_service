# 
# CommonEntry.pm
# 
# A class that contains common routines for supporting PBDB data entry.
# 
# Author: Michael McClennen

package PB2::CommonEntry;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);
use Scalar::Util qw(reftype);
use List::Util qw(any);

use TableDefs qw(get_table_property %TABLE);
use ExternalIdent qw(extract_identifier generate_identifier %IDP %IDRE);

use Moo::Role;

use namespace::clean;

our (@REQUIRES_ROLE) = qw(PB2::TableData PB2::Authentication);

use EditTransaction;

EditTransaction->log_filename('./datalogs/datalog-DATE.sql');

# The following hash will cache the list of ruleset parameters corresponding to various
# rulesets. The hash keys are ruleset names.

our (%RULESET_HAS_PARAM);

# Methods
# -------

# initialize ( )
#
# This routine is called once by Web::DataService, to initialize this role.

sub initialize {
    
    my ($class, $ds) = @_;

    $ds->define_valueset('1.2:common:entry_ops',
	{ value => 'insert' },
	    "Insert this record into the database, and return the newly",
	    "generated primary key value. This is the default operation for any",
	    "record that does not include an identifier/primary key value.",
	{ value => 'update' },
	    "If a record is found in the database whose identifier matches",
	    "the one included in this record, use the attributes of this record",
	    "to update that one. This is the default operation for any record",
	    "that includes an identifier/primary key value.",
	{ value => 'replace' },
	    "If a record is found in the database whose identifier matches",
	    "the one included in this record, replace that record with this one.",
	{ value => 'delete' },
	    "If a record is found in the database whose primary key value matches",
	    "the one included in this record, delete that record. All other",
	    "attributes of this record are ignored.");

    $ds->define_ruleset('1.2:common:entry_fields',
	{ optional => '_label', valid => ANY_VALUE },
	    "If you provide a non-empty value for this parameter, that value",
	    "can be used in association with auxiliary references, i.e. occurrences",
	    "to collections. Otherwise, a label will be generated according to the following",
	    "pattern: '#1' for the first submitted record, '#2' for the second,",
	    "etc.");
    
    # { optional => '_operation', valid => '1.2:common:entry_ops' },
    #     "You may include this parameter in any submitted record. It specifies",
    #     "the database operation to be performed, overriding the default operation.",
    # 		"Accepted values are:");
    
    $ds->define_valueset('1.2:common:std_allowances',
	{ value => 'CREATE', undocumented => 1 },
	{ value => 'PROCEED' },
	    "If some of the submitted records generate cautions or,",
	    "errors, this allowance will enable any database operations that do",
	    "B<not> generate cautions or errors to complete. Without it, the",
	    "entire operation will be rolled back if any cautions or errors are generated.",
	{ value => 'NOT_PERMITTED' },
	    "This is a more limited allowance than 'PROCEED'. If the user has permission",
	    "to modify some records but not others, this allowance will enable the records",
	    "which can be modified to be modified, while generating warnings about the",
	    "rest. Without it, the entire operation will be rolled back.",
	{ value => 'NOT_FOUND' },
	    "This is a more limited allowance than 'PROCEED'. If some of the records to",
	    "be modified are not found, this allowance will enable the records which do",
	    "exist to be modified, while generating warnings about the rest. Without it,",
	    "the entire operation will be rolled back.",
	{ value => 'FIXUP_MODE' },
	    "This allowance can only be used by a user with administrative privilege.",
	    "It causes any update or replacement of records to be recorded as an 'update'",
	    "rather than a 'modification'.");
}


# parse_main_params ( url_ruleset, default_params )
# 
# Go through the main parameters to this request, and return a list of two
# hashrefs. The first will contain as keys any "allowances", which are values of
# the parameter 'allow'. If one or more default parameter names are provided,
# and if any of those are specified in this request, include them in the second
# hash. These parameter values will be used as defaults for all records
# specified in the request body.

sub parse_main_params {
    
    my ($request, $url_ruleset, $default_params) = @_;
    
    # First grab a list of the parameters that were specified in the request URL. Also allocate
    # two hashes which will end up being the return value of this routine.
    
    my $allowances = { };
    my $main_params = { };
    
    # If we have not already extracted the list of parameters corresponding to the named ruleset,
    # do that now.
    
    if ( $url_ruleset && ! defined $RULESET_HAS_PARAM{$url_ruleset} )
    {
	foreach my $p ( $request->{ds}->list_ruleset_params($url_ruleset) )
	{
	    $RULESET_HAS_PARAM{$url_ruleset}{$p} = 1 if $p;
	}
    }

    # Determine which parameters (if any) should be used as defaults for the
    # body records.

    my %is_default;
    
    if ( ref $default_params eq 'ARRAY' )
    {
	$is_default{$_} = 1 foreach @$default_params;
    }

    elsif ( $default_params )
    {
	$is_default{$default_params} = 1;
    }
    
    # Now process the request parameters one by one.
    
    foreach my $k ( $request->param_keys )
    {
	if ( $k eq 'allow' )
	{
	    my @list = $request->clean_param_list($k);
	    $allowances->{$_} = 1 foreach @list;
	}
	
	elsif ( $url_ruleset && $RULESET_HAS_PARAM{$url_ruleset}{$k} && $is_default{$k} )
	{
	    $main_params->{$k} = $request->clean_param($k);
	}
    }
    
    # Then add any extra flags that were specified.
    
    # $allowances->{$_} = 1 foreach @extra_flags;
    
    # Print out the main parameters and allowances if we are in debug mode, then return the two
    # hashes.
    
    $request->debug_out($main_params);
    $request->debug_out($allowances);
    
    return ($allowances, $main_params);
}


# get_main_params ( allowances_ref, url_ruleset )
# 
# This is a wrapper around 'parse_main_params' that can be used by older code
# which has not yet been updated.

sub get_main_params {
    
    my ($request, $allowances_ref, $url_ruleset) = @_;
    
    my ($allowances, $main_params) = $request->parse_main_params($url_ruleset);
    
    foreach my $k ( keys %$allowances )
    {
	$allowances_ref->{$k} = $allowances->{$k};
    }
    
    return $main_params;
}


sub parse_body_records {
    
    my ($request, $main_params, @ruleset_patterns) = @_;
    
    my (@raw_records, @records);
    
    # Mark this request as a "data entry request" so that the external identifier validator knows
    # to accept label references.
    
    $request->{is_data_entry} = 1;

    # If the method is GET, then we don't expect any body. Assume that the main parameters
    # constitute the only input record. They have already been validated during request
    # processing, and have already been cleaned by parse_main_params. So there is no need to
    # process them further. However, we do need to throw an error if they are empty.
    
    if ( Dancer::request->method eq 'GET' )
    {
	if ( $main_params->%* )
	{
	    push @records, $main_params;
	}

	else
	{
	    die $request->exception(400, "E_PARAM: Method is 'GET' and no data parameters were recognized");
	}
    }
    
    # Otherwise decode the body and extract input records from it. If an error occurs, return an
    # HTTP 400 result.
    
    else
    {
	my ($body, $error) = $request->decode_body;
	
	# If an error occurred while decoding the request body, or if the request body was empty,
	# return a 400 response immediately.
	
	if ( $error )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
	}
	
	elsif ( ! defined $body || $body eq '' )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Request body must not be empty");
	}
	
	# Otherwise, if the request body is an object with the key 'records' and an array value,
	# then we assume that the array is a list of input records. If there is also a key 'all'
	# with an object value, then we assume that it gives common parameters to be applied to
	# all records.
	
	if ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
	{
	    push @raw_records, $body->{records}->@*;
	    
	    if ( ref $body->{all} eq 'HASH' )
	    {
		foreach my $k ( keys $body->{all}->%* )
		{
		    $main_params->{$k} = $body->{all}{$k};
		}
	    }
	}
	
	# If we don't find a 'records' key with an array value, then assume that the body is a single
	# record.
	
	elsif ( ref $body eq 'HASH' )
	{
	    push @raw_records, $body;
	}
	
	# If the body is an array, and that array contains at least one object, then assume its
	# elements are records.
	
	elsif ( ref $body eq 'ARRAY' && $body->@* && ref $body->[0] eq 'HASH' )
	{
	    push @raw_records, @$body;
	}
	
	# Otherwise, we must return a 400 error.
	
	else
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: no records found");
	}
    }
    
    # Then validate the records one by one.
    
    my $INDEX = 0;

  RECORD:
    foreach my $r ( @raw_records )
    {
	$INDEX++;
	
	# Any item that is not an object will generate a dummy record. So will an empty object,
	# but those get warnings rather than errors.
	
	unless ( ref $r eq 'HASH' )
	{
	    my $val = ref $r ? uc reftype $r : "'$r'";
	    
	    push @records, { _errors => 1, _errwarn => ['E_BAD_RECORD', "record content cannot be decoded ($val)"] };
	}
	
	unless ( keys %$r )
	{
	    push @records, { _skip => 1, _errwarn => ['W_EMPTY_RECORD'] };
	}
	
	# If $main_params is not empty, its attributes provide defaults for every record.
	
	foreach my $k ( keys $main_params->%* )
	{
	    unless ( exists $r->{$k} )
	    {
		$r->{$k} = $main_params->{$k};
	    }
	}
	
	# Now choose a ruleset for this record. If more than one pattern was given, iterate
	# through them until a matching one is found. The value 'NO_MATCH' will cause the record
	# to be rejected. Otherwise, the ruleset from the last pattern will be selected whether or
	# not it matches.
	
	my $rs_name = 'NO_MATCH';
	
      RULESET:
	foreach my $rs_arg ( @ruleset_patterns )
	{
	    # If the ruleset includes a list of keys to check, choose this ruleset if the record
	    # contains any of them with a non-empty, non-zero value. Also choose it if it is the
	    # last one in the list.
	    
	    if ( ref $rs_arg eq 'ARRAY' )
	    {
		my ($table_name, $ruleset_name, $key_name, @check_keys) = $rs_arg->@*;
		
		foreach my $k ( $key_name, @check_keys )
		{
		    if ( exists $r->{$k} )
		    {
			$rs_name = $ruleset_name;
			# $r->{_table} = $table_name; # $$$ need to check for main_table
			last RULESET;
		    }
		}
	    }
	    
	    # Any other reference value is a configuration error.
	    
	    elsif ( ref $rs_arg )
	    {
		my $type = reftype $rs_arg;
		croak "Invalid ruleset pattern ($type)";
	    }
	    
	    # A bare ruleset name causes that ruleset to be chosen by default. The value
	    # 'NO_MATCH' rejects the record.
	    
	    elsif ( $rs_arg )
	    {
		$rs_name = $rs_arg;
		last RULESET;
	    }
	}
	
	# If a ruleset was selected, use it to validate the record.
	
	if ( $rs_name && $rs_name ne 'NO_MATCH' )
	{
	    my $result = $request->validate_params($rs_name, $r);
	    
	    # HTTP::Validate (at the time this code was written) does not properly handle
	    # parameters with empty values. So fill in any parameters that were given with
	    # empty values.
	    
	    my $raw = $result->raw;
	    
	    foreach my $k ( keys %$raw )
	    {
		if ( defined $raw->{$k} && $raw->{$k} eq '' && ! defined $result->{clean}{$k} )
		{
		    $result->{clean}{$k} = '';
		    push @{$result->{clean_list}}, $k;
		}
		
		elsif ( ref $raw->{$k} eq 'ARRAY' && scalar($raw->{$k}->@*) == 0 &&
			! defined $result->{clean}{$k} )
		{
		    $result->{clean}{$k} = [];
		    push @{$result->{clean_list}}, $k;
		}
	    }
	    
	    # Generate a record with the cleaned parameter values. If any errors or warnings were
	    # generated, add them to the record.
	    
	    my $cleaned = $result->values;
	    
	    # my (@record_errors, @record_warnings);
	    
	    # my $label = $r->{_label} || ($key_name && $r->{$key_name}) || "#$INDEX";
	    
	    foreach my $e ( $result->errors )
	    {
		if ( $e =~ /^Field '?(.*?)'?: (.*)/ )
		{
		    my $field = $1;
		    my $msg = $2;

		    if ( $msg =~ /identifier must have type/ )
		    {
			push $cleaned->{_errwarn}->@*, ['E_EXTID', $field, $msg];
			$cleaned->{_errors} = 1;
		    }

		    else
		    {
			push $cleaned->{_errwarn}->@*, ['E_PARAM', $field, $msg];
			$cleaned->{_errors} = 1;
		    }
		}
		
		else
		{
		    push $cleaned->{_errwarn}->@*, ['E_PARAM', $e];
		    $cleaned->{_errors} = 1;
		}
	    }
	    
	    foreach my $w ( $result->warnings )
	    {
		push $cleaned->{_errwarn}->@*, ['W_PARAM', $w];
	    }
	    
	    # Add the cleaned record to the list of records to process.
	    
	    push @records, $cleaned;
	}
	
	# If no ruleset was selected, add a validation error to the record.
	
	else
	{
	    $r->{_errwarn} = ['E_UNRECOGNIZED'];
	    $r->{_errors} = 1;
	    
	    push @records, $r;
	}
    }
    
    # Return the list of cleaned records, which may be empty.

    return @records;
    
    # # If validation errors occurred and the PROCEED allowance was not specified, add an error
    # # condition to the request.
    
    # if ( $validation_errors && ! $allows->{PROCEED} )
    # {
    # 	$request->add_error('E_REQUEST_BODY: errors were detected in one or more records');
    # }
    
    # if ( $request->debug )
    # {
    # 	foreach my $r ( @records )
    # 	{
    # 	    $request->debug_out($r, '    ');
    # 	}
    # }
}


# sub validation_error {
    
#     my ($request, $proceed, $errmsg) = @_;

#     if ( $proceed )
#     {
# 	$errmsg =~ s/^E/F/;
# 	$request->add_warning($errmsg);
#     }

#     else
#     {
# 	$request->add_error($errmsg);
#     }
# }


sub addupdate_common {

    my ($request, $config, @extra_flags) = @_;
    
    croak "addupdate_simple: first parameter must be a configuration hash"
	unless ref $config eq 'HASH';
    
    my $ETclass = $config->{class} || 'EditTransaction';
    my $main_table = $config->{main_table};
    my $main_ruleset = $config->{main_ruleset};
    my $url_ruleset = $config->{url_ruleset};
    my $table_selector = $config->{table_selector};
    my $record_cleaner = $config->{record_cleaner};
    
    croak "addupdate_common: configuration must include a valid internal table name under 'main_table'"
	unless $main_table && $TABLE{$main_table};
    
    croak "addupdate_common: the value of 'class' must be either 'EditTransaction' or a subclass"
	if $config->{class} && ! $config->{class}->isa('EditTransaction');
    
    croak "the value of 'record_cleaner' must be a code ref or a method name"
	if $record_cleaner && ref $record_cleaner && ref $record_cleaner ne 'CODE';
    
    # If 'table_selector' is given, its value must match one of the accepted patterns.

    my @table_list;
    
    if ( $table_selector )
    {
	if ( ref $table_selector eq 'HASH' && $table_selector->%* )
	{
	    push @table_list, $request->validate_tabsel($table_selector);
	}
	
	elsif ( ref $table_selector eq 'ARRAY' && $table_selector->[0] )
	{
	    if ( ref $table_selector->[0] )
	    {
		push @table_list, $request->validate_tabsel($_) foreach $table_selector->@*;
	    }

	    else
	    {
		push @table_list, $request->validate_tabsel($table_selector);
	    }
	}
	
	elsif ( ref $table_selector eq 'CODE' )
	{
	    push @table_list, $table_selector;
	}
	
	elsif ( $table_selector eq 'NO_MATCH' )
	{
	    push @table_list, ['NO_MATCH', 'NO_MATCH'];
	}
	
	else
	{
	    my $val = ref $table_selector || $table_selector;
	    croak "addupdate_common: the value of 'table_selector' is not valid ($val)";
	}
    }
    
    unless ( @table_list && $table_list[-1] eq 'NO_MATCH' )
    {
	croak "addupdate_common: you must specify a value for 'main_ruleset'" unless $main_ruleset;
	push @table_list, $request->validate_tabsel([ $main_table, $main_ruleset ]);
    }
    
    # If extra flags or allowances are given, make sure they are all valid.
    
    foreach my $flag ( @extra_flags )
    {
	croak "addupdate_common: unrecognized flag '$flag'"
	    unless $ETclass->has_allowance($flag);
    }
    
    # my @ruleset_patterns;
    
    # if ( ref $config->{entry_ruleset} eq 'ARRAY' && $config->{entry_ruleset}->@* )
    # {
    # 	my $rs_name = $config->{entry_ruleset}[0];
    
    # 	croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
    
    # 	push @ruleset_patterns, $config->{entry_ruleset};
    # }
    
    # elsif ( $config->{entry_ruleset} )
    # {
    # 	my $rs_name = $config->{entry_ruleset};
	
    # 	croak "entry_ruleset must be a list or a scalar" if ref $config->{entry_ruleset};
    # 	croak "you cannot specify 'entry_ruleset' and 'entry_list' together" if $config->{entry_list};
    # 	croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
	
    # 	if ( $config->{entry_key} )
    # 	{
    # 	    push @ruleset_patterns, [ $rs_name, $config->{entry_key}, 'DEFAULT' ];
    # 	}
	
    # 	else
    # 	{
    # 	    push @ruleset_patterns, $rs_name;
    # 	}
    # }
    
    # elsif ( $config->{entry_list} )
    # {
    # 	croak "entry_list must be a list of lists" unless ref $config->{entry_list} eq 'ARRAY' &&
    # 	    $config->{entry_list}->@* && ref $config->{entry_list}[0] eq 'ARRAY' && $config->{entry_list}[0]->@*;
	
    # 	foreach my $list ( $config->{entry_list}->@* )
    # 	{
    # 	    my $rs_name = ref $list eq 'ARRAY' ? $list->[0] : $list;
	    
    # 	    croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
	    
    # 	    push @ruleset_patterns, $list;
    # 	}
    # }
    
    # else
    # {
    # 	croak "no ruleset specified";
    # }
    
    # Authenticate the user who made this request, and check that they have write permission on
    # the main table. 
    
    my $perms = $request->require_authentication($main_table);
    
    # Parse the URL parameters, and return a hash of allowances and another one with the rest of
    # the parameter values. If $url_ruleset is empty, the second return value will be an empty
    # hash. The url parameters will already have been validated against the ruleset before this
    # method is called, and if they are invalid an error response will already have been returned.
    
    my ($allowances, $main_params) = $request->parse_main_params($url_ruleset, @extra_flags);
    
    # Then decode the body, and extract input records from it. The variable @table_list specifies
    # which ruleset to use for different kinds of records, if there is more than one kind
    # accepted. Any attribute values specified in $main_params override those in the individual
    # records. If any validation errors are found, they are attached to the individual records
    # under the key _errors.
    
    my (@records) = $request->parse_body_records($main_params, @table_list);
    
    # The next step is to create an object of class EditTransaction or one of its subclasses, to
    # carry out this request.
    
    # If any records have validation errors, we cannot complete the request unless the allowance
    # PROCEED was given. In that case, create the EditTransaction object in validation mode. This
    # will validate the rest of the records against the database and do nothing else. This allows
    # us to return a comprehensive result showing which of the submitted records were valid and
    # which were not. Otherwise, create a regular EditTransaction object which will complete the
    # request if possible.
    
    my $invalid_records = any { $_->{_errors} } @records;
    
    if ( $invalid_records && ! $allowances->{PROCEED} )
    {
	$allowances->{VALIDATION_ONLY} = 1;
    }
    
    my $edt = $ETclass->new($request, $perms, $main_table, $allowances);
    
    # Now iterate through the records. The @AUX_TABLES list will collect the names of all tables
    # used other than the main one. We use three different loops, depending on which of the table
    # selection options was specified. If a record cleaning routine was specified, it is called
    # for each record. The purpose of such a routine is to modify record values prior to insertion
    # or update. For example, the interface developer may wish to allow certain attributes to be
    # specified in ways that don't match the database structure and must be converted first. It
    # can also select the table to be used by altering _table.
    
    my (%TABLE_USED, @AUX_TABLES);
    
    foreach my $r ( @records )
    {
	# If a record cleaner routine was defined, call it.
	
	$request->$record_cleaner($edt, $r, $main_table, $perms) if $record_cleaner;
	
	# If the key _table has a nonempty value, process this record with this table. Otherwise,
	# use the main table.
	
	my $selected_table = $r->{_table} || $main_table;
	delete $r->{_table};
	
	if ( $selected_table ne $main_table && ! $TABLE_USED{$selected_table} )
	{
	    croak "addupdate_common: bad table '$selected_table'" unless $TABLE{$selected_table};
	    push @AUX_TABLES, $selected_table;
	    $TABLE_USED{$selected_table} = 1;
	}
	
	$edt->process_record($selected_table, $r);
    }
    
    # if ( $record_handler )
    # {
    # 	foreach my $r ( @records )
    # 	{
    # 	    $request->$record_cleaner($edt, $r, $main_table, $perms) if $record_cleaner;
	    
    # 	    my $table_name = $request->$record_handler($edt, $r, $main_table, $perms);

    # 	    if ( $table_name )
    # 	    {
    # 		push @AUX_TABLES, $table_name unless $TABLE_USED{$table_name} || $table_name eq $main_table;
    # 		$TABLE_USED{$table_name}++;
    # 	    }
    # 	}
    # }
	# # Otherwise, if the record includes either _skip or _errors then call $edt->skip_record.
	
	# elsif ( $r->{_skip} || $r->{_errors} )
	# {
	#     $edt->skip_record($r);
	# }
    
    # 
    
    # Attempt to commit the database transaction. If any errors have occurred and PROCEED was not
    # specified, it is automatically rolled back instead.
    
    $edt->commit;
    
    # Handle any errors or warnings that may have been generated.
    
    $request->collect_edt_errors($edt);
    $request->collect_edt_warnings($edt);
    
    if ( $edt->fatals )
    {
    	die $request->exception(400, "Bad request");
    }
    
    # The final step is to generate a list of results for this operation to indicate which
    # database records have been affected and how. We start with the list of records from the main
    # table. Deleted records are listed first, in order by primary key, followed by all inserted
    # or altered records in order by primary key. Auxiliary records that contain the main table's
    # primary key as a foreign key are listed in the aux_records hash, under the primary key
    # value. That allows the auxiliary records to be interleaved with the main table records, and
    # for auxiliary records associated with the same main table record to be grouped together. Any
    # records that don't contain any known key go on the other_records list.
    
    my %table_records;
    
    # The main key defaults to the primary key of the main table. This can be overridden in the
    # configuration.
    
    my $main_key = $config->{main_key} || get_table_property($main_table, 'PRIMARY_KEY');
    
    # One or more additional keys can also be specified, which allows record chains of more than
    # one link.
    
    my @aux_keys;
    
    if ( $config->{aux_keys} && ref $config->{aux_keys} eq 'ARRAY' )
    {
	@aux_keys = $config->{aux_keys}->@*;
    }

    elsif ( $config->{aux_keys} )
    {
	carp "addupdate_common: the value of 'aux_keys' must be an array of key names";
    }
    
    # Now go through the tables that have been touched, starting with the main table, and grab the
    # keys for each one.
    
    foreach my $tn ( $main_table, @AUX_TABLES )
    {
	my $key_label = $edt->key_labels($tn);
	my $display_key = get_table_property($tn, 'PRIMARY_KEY') || 'oid';
	
	$table_records{$tn} = [ ];
	
	# If any records have been deleted, those are listed first by means of placeholder records.
	
	if ( my @deleted_keys = $edt->deleted_keys($tn) )
	{
	    foreach my $key_value ( sort { $a <=> $b} @deleted_keys )
	    {
		my $record = { $display_key => $key_value, status => 'deleted',
			       _operation => 'delete', _label => $key_label->{$key_value} };

		push $table_records{$tn}->@*, $record;
	    }
	}
	
	# $$$ EditTranasction needs to generate records corresponding to the input ones, conveying
	# all the information necessary to produce a result: key, label, operation, action,
	# result, etc. 
    
    # my @existing_keys = ($edt->inserted_keys, $edt->updated_keys, $edt->replaced_keys);
    
    # $request->list_updated_refs($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;
    }

}

# $$$ add primary key if it can be determined from the table name

sub validate_tabsel {

    my ($request, $selector) = @_;

    my ($table_name, $ruleset_name, $primary_key, @keylist);
    
    if ( ref $selector eq 'HASH' )
    {
	$table_name = $selector->{table} || '';
	$ruleset_name = $selector->{ruleset} || '';
	
	if ( ref $selector->{keys} eq 'ARRAY' )
	{
	    @keylist = grep $selector->{keys}->@*;
	}
	
	elsif ( ! ref $selector->{keys} )
	{
	    @keylist = grep split /,\s*/, $selector->{keys};
	}

	unshift @keylist, $selector->{primary_key} if $selector->{primary_key};
    }
    
    elsif ( ref $selector eq 'ARRAY' )
    {
	($table_name, $ruleset_name, @keylist) = $selector->@*;
    }
    
    croak "addupdate_common: table name '$table_name' invalid in 'table_selector'"
	unless $table_name && $TABLE{$table_name};
    
    croak "addupdate_common: ruleset name '$ruleset_name' invalid in 'table_selector'"
	unless $ruleset_name && $request->{ds}->has_ruleset($ruleset_name);

    return [$table_name, $ruleset_name, @keylist];
}


# sub debug_line {
    
#     my ($request, $line) = @_;
    
#     print STDERR "$line\n";
# }


sub collect_edt_warnings {
    
    my ($request, $edt) = @_;
    
    my @strings = $edt->nonfatals;

    foreach my $m ( @strings )
    {
	$request->add_warning($m);
    }
}


sub collect_edt_errors {
    
    my ($request, $edt) = @_;
    
    my @strings = $edt->fatals;

    foreach my $m ( @strings )
    {
	$request->add_error($m);
    }
}


sub debug_out {

    my ($request, $record, $prefix) = @_;
    
    return unless ref $record eq 'HASH' && $request->debug;
    
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


sub generate_sandbox {

    my ($request, $config) = @_;
    
    my ($ds_operation, $ruleset_name, $extra_params, $allowances, @allow_list);
    
    if ( ref $config eq 'HASH' )
    {
	$ds_operation = $config->{operation} || '???';
	$ruleset_name = $config->{ruleset} || '';
	$extra_params = $config->{extra_params} || '';
	$allowances = $config->{allowances};
    }
    
    else
    {
	croak "You must provide a hashref to configure the sandbox.";
    }
    
    @allow_list = $request->ds->list_set_values($allowances) if $allowances;
    
    my $allow_string = join ', ', @allow_list;

    my $allow_stmt = '';

    $allow_stmt = "If you wish to specify allowances, those available are: $allow_string.";
    
    my $ds_params = 'rowcount';
    
    if ( $extra_params )
    {
	$ds_params .= "&$extra_params";
    }

    if ( $ds_operation eq 'unknown' )
    {
	my $output = "<html><head><title>No sandbox is available</title>\n";
	$output .= "    <link rel=\"stylesheet\" href=\"/data1.2/css/sandbox.css\">\n";
	$output .= "</head>\n";
	$output .= "<body>\n";
	
	$output .= "<div id=\"main_body\" class=\"sbmain\">\n";
	$output .= "<h1>No sandbox is available</h1>\n";
	$output .= "</div>\n";
	$output .= "</body></html>\n";

	$request->data_result($output);
	return;
    }
    
    my $output = "<html><head><title>Sandbox for $ds_operation</title>\n";
    $output .= "    <link rel=\"stylesheet\" href=\"/data1.2/css/sandbox.css\">\n";
    $output .= "</head>\n";
    $output .= "<body>\n";
    
    $output .= "<div id=\"main_body\" class=\"sbmain\">\n";
    $output .= "<h1>Sandbox for '$ds_operation'</h1>\n";
    $output .= "<form id=\"sandbox_form\" onsubmit=\"return false\">\n";
    
    $output .= "<p>You can use this form to make calls to the data service operation '$ds_operation'.\n";
    $output .= "You can find the documentation for this\n";
    $output .= "operation at <a href=\"/data1.2/${ds_operation}_doc.html\" target=\"_blank\">\n";
    $output .= "/data1.2/${ds_operation}_doc.html</a>.</p>\n";
    
    $output .= "<button id=\"b_clear\" onclick=\"sandbox_clear()\">Clear</button>\n";
    $output .= "<button id=\"b_submit\" class=\"submit\" onclick=\"sandbox_request()\">Submit</button>\n\n";
    
    $output .= "<span id=\"testcontrol\" style=\"margin-left: 20px\"></span>\n";
    
    $output .= "<table class=\"sbtable\">\n";
    $output .= "<tr><td>call parameters<br><input type=\"text\" id=\"ds_params\" size=\"50\" " .
	"value=\"$ds_params\"/></td>\n";
    $output .= "<td class=\"sbdoc\">The parameters in this box will be added to the data service\n";
    $output .= "request. $allow_stmt</td></tr>\n";
    $output .= "</table>\n";
    
    $output .= "<hr>\n";

    $output .= "<p>Parameters which are <b>required</b> must be given a non-empty value for new records, and must not be given an empty value in an update. If you wish to update a field to have a null value, enter <i>NULL</i> below. To update a field to the empty string, enter <i>EMPTY</i> below. If you wish to enter JSON content into a field that accepts it, start the value with either '[' or '{'.</p>\n";
    
    $output .= "<table class=\"sbtable\">\n";
    
    my @doc_list = $request->ds->list_rules($ruleset_name);
    
    my (@field_list, @json_list);
    
    # while ( @doc_list && ! ref $doc_list[0] )
    # {
    # 	shift @doc_list;
    # }
    
    while ( @doc_list )
    {
	my $rule = shift @doc_list;
	
	my $field_name = $rule->{param} || $rule->{optional} || $rule->{required} || '';
	my $field_doc = ref $rule->{doc_ref} ? $rule->{doc_ref}->$* : '';
	
	# $field_doc = shift @doc_list if @doc_list && !ref $doc_list[0];
	
	# shift @doc_list while @doc_list && !ref $doc_list[0];
	
	if ( $field_doc )
	{
	    $field_doc =~ s/C<(.*?)>/<span class="dbfield">$1<\/span>/g;
	    $field_doc =~ s/B<(.*?)>/<b>$1<\/b>/g;
	}
	
	if ( $field_name )
	{
	    if ( $rule->{note} && $rule->{note} =~ /textarea/ )
	    {
		$output .= "<tr><td class=\"sbfield\">$field_name<br>\n";
		$output .= "<textarea class=\"sbtext\" name=\"f_$field_name\" " .
		    "rows=\"2\" cols=\"40\"></textarea></td>\n";
	    }
	    else
	    {
		$output .= "<tr><td>$field_name<br><input type=\"text\" name=\"f_$field_name\" " .
		    "size=\"40\"></td>\n";
	    }
	    $output .= "<td class=\"sbdoc\">$field_doc</td></tr>\n";
	    
	    push @field_list, $field_name;

	    if ( $rule->{note} && $rule->{note} =~ /json/ )
	    {
		push @json_list, $field_name;
	    }
	}
    }
    
    $output .= "</table>\n";
    $output .= "</form>\n";
    
    $output .= "<hr>\n";
    
    $output .= "<p><button id=\"b_submit\" onclick=\"sandbox_request()\">Submit</button></p>\n\n";
    $output .= "</div>\n";
    
    my $field_string = join "','", @field_list;
    my $json_string = join ', ', map { "\"$_\": 1" } @json_list;
    
    $output .= "<script src=\"//ajax.googleapis.com/ajax/libs/jquery/2.0.3/jquery.min.js\"></script>\n";
    $output .= "<script src=\"/data1.2/js/sandbox.js\" type=\"text/javascript\"></script>\n";
    $output .= "<script type=\"text/javascript\">\n";
    $output .= "    sandbox_fields = ['$field_string'];\n";
    $output .= "    sandbox_json = { $json_string };\n";
    $output .= "    sandbox_operation = '$ds_operation';\n";
    $output .= "    sandbox_extra = '$ds_params';\n";
    $output .= "    var testreq = new XMLHttpRequest();\n";
    $output .= "    testreq.open('GET', '/dtest1.2/formats/png', true);\n";
    $output .= "    testreq.onload = function () {\n";
    $output .= "        if ( testreq.status == \"200\" ) sandbox_addtest(); }\n";
    $output .= "    testreq.send();\n";
    $output .= "</script>\n\n";
    
    $output .= "</body></html>\n";
    
    $request->data_result($output);
}


1;
