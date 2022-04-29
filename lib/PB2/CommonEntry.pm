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
use Scalar::Util qw(reftype);

use TableDefs qw(%TABLE);

use ExternalIdent qw(extract_identifier generate_identifier %IDP %IDRE);
use TableData qw(get_table_schema get_table_property);

use Moo::Role;

use namespace::clean;

our (@REQUIRES_ROLE) = qw(PB2::TableData PB2::Authentication);


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
	    "will be included in any response associated with this record.",
	    "Otherwise, a label will be generated according to the following",
	    "pattern: '#1' for the first submitted record, '#2' for the second,",
	    "etc.",
	{ optional => '_operation', valid => '1.2:common:entry_ops' },
	    "You may include this parameter in any submitted record. It specifies",
	    "the database operation to be performed, overriding the default operation.",
			"Accepted values are:");
    
    $ds->define_valueset('1.2:common:std_allowances',
	{ value => 'CREATE' },
	    "This allowance enables new records to be inserted. Without it, a",
	    "caution will be thrown for any record that does not include an",
	    "identifier. This feature is present as a failsafe to make sure",
	    "that new records are not added accidentally due to bad client code",
	    "that omits a record identifier where one should appear. The argument",
	    "C<B<allow=CREATE>> should be included with every request that is",
	    "expected to insert new records.",
	{ value => 'PROCEED' },
	    "If some of the submitted records generate cautions or,",
	    "errors, this allowance will enable any database operations that do",
	    "B<not> generate cautions or errors to complete. Without it, the",
	    "entire operation will be rolled back if any cautions or errors are generated.");
}


# get_main_params ( allowances_ref, url_ruleset )
# 
# Go through the main parameters to this request, looking for the following. If the parameter
# 'allow' is found, then enter all of the specified conditions as hash keys into
# $conditions_ref. These will already have been checked by the initial ruleset validation of the
# request. Otherwise, any parameters we find that are specified in the ruleset whose name is given
# in $entry_ruleset should be copied into a hash which is then returned as the result. These
# parameters will be used as defaults for all records speified in the request body.

sub parse_main_params {
    
    my ($request, $url_ruleset) = @_;
    
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
    
    # Now process the request parameters one by one.
    
    foreach my $k ( $request->param_keys )
    {
	if ( $k eq 'allow' )
	{
	    my @list = $request->clean_param_list($k);
	    $allowances->{$_} = 1 foreach @list;
	}
	
	elsif ( $url_ruleset && $RULESET_HAS_PARAM{$url_ruleset}{$k} )
	{
	    $main_params->{$k} = $request->clean_param($k);
	}
    }
    
    $request->debug_out($main_params);
    $request->debug_out($allowances);
    
    return ($allowances, $main_params);
}


sub parse_body_records {
    
    my ($request, $allows, $main_params, @ruleset_patterns) = @_;
    
    my (@raw_records, @records);
    
    # Mark this request as a "data entry request" so that the external identifier validator knows
    # to accept label references.
    
    $request->{is_data_entry} = 1;

    my $PROCEED  = $allows->{PROCEED};
    
    # If the method is GET, then we don't expect any body. Assume that the main parameters
    # constitute the only input record. They have already been validated during request
    # processing, and have already been cleaned by parse_main_params. So there is no need to
    # process them further. However, we do need to throw an error if they are empty.
    
    if ( Dancer::request->method eq 'GET' )
    {
	if ( $main_params_ref->%* )
	{
	    push @records, $main_params_ref;
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
	
	if ( $error )
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
	}
	
	# If there was no request body at all, throw an exception.
	
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
	
	# If the body is an array, and that array contains at least one object, then assume its
	# elements are records.
	
	elsif ( ref $body eq 'ARRAY' && $body->@* && ref $body->[0] eq 'HASH' )
	{
	    push @raw_records, @$body;
	}
	
	# Otherwise, we must return a 400 error.
	
	else
	{
	    die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: no record found");
	}
    }
    
    # Then validate the records one by one.
    
    my $INDEX = 0;

  RECORD:
    foreach my $r ( @raw_records )
    {
	$INDEX++;
	
	# Any item that is not an object will generate an error, or else a warning if PROCEED is
	# allowed. In the latter case, a dummy record will be added to the list to keep the record
	# index consistent.
	
	unless ( ref $r eq 'HASH' )
	{
	    my $val = ref $r ? uc reftype $r : "'$r'";
	    
	    push @records, { _skip => 1 };
	    $request->errwarn($PROCEED, "E_REQUEST_BODY (#$INDEX): element is invalid ($val)");
	}
	
	# An empty item generates a warning, and a dummy record.
	
	unless ( keys %$r )
	{
	    push @records, { _skip => 1 };
	    $request->add_warning("W_REQUEST_BODY: Body item #$INDEX is empty");
	}
	
	# If $main_params_ref is not empty, its attributes provide defaults for every record.
	
	foreach my $k ( keys $main_params_ref->%* )
	{
	    unless ( exists $r->{$k} )
	    {
		$r->{$k} = $main_params_ref->{$k};
	    }
	}
	
	# Now choose a ruleset for this record. If more than one pattern was given, iterate
	# through them until a matching one is found. The value 'NO_MATCH' will cause the record
	# to be rejected. Otherwise, the ruleset from the last pattern will be selected whether or
	# not it matches.
	
	my $rs_name = 'NO_MATCH';
	my $key_name;
	
      RULESET:
	foreach my $rs_arg ( @ruleset_patterns )
	{
	    # If the ruleset includes a list of keys to check, choose this ruleset if the record
	    # contains any of them with a non-empty, non-zero value. Also choose it if it is the
	    # last one in the list.
	    
	    if ( ref $rs_arg eq 'ARRAY' )
	    {
		($rs_name, $key_name, @check_keys) = $rs->@*;
		
		foreach my $k ( $key_name, @check_keys )
		{
		    last RULESET if $r->{$k};
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
	    }
	    
	    # If any errors or warnings were generated, add them to the current request.
	    
	    my $label = $r->{_label} || ($key_name && $r->{$key_name}) || "#$INDEX";
	    
	    foreach my $e ( $result->errors )
	    {
		if ( $e =~ /identifier must have type/ )
		{
		    $request->errwarn($PROCEED, "E_EXTTYPE ($label): $e");
		    $r->{_skip} = 1;
		}
		
		elsif ( $e =~ /may not specify/ )
		{
		    $request->errwarn($PROCEED, "E_PARAM ($label): $e");
		    $r->{_skip} = 1;
		}
		
		else
		{
		    $request->errwarn($PROCEED, "E_FORMAT ($label): $e");
		    $r->{_skip} = 1;
		}
	    }
	    
	    foreach my $w ( $result->warnings )
	    {
		$request->add_warning("W_PARAM ($label): $w");
	    }
	    
	    # Then add the hash of cleaned parameter values to the list of
	    # records to add or update.
	    
	    push @records, $result->values;
	}
	
	# If no ruleset was selected, throw an error (or a warning with PROCEED).
	
	else
	{
	    my $label = $r->{_label} || "#$INDEX";
	    
	    $request->errwarn($PROCEED, "E_UNRECOGNIZED ($label): Could not validate record, no matching ruleset");
	    $r->{_skip} = 1;
	}
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


sub errwarn {
    
    my ($request, $proceed, $errmsg) = @_;

    if ( $proceed )
    {
	$errmsg =~ s/^E/F/;
	$request->add_warning($errmsg);
    }

    else
    {
	$request->add_error($errmsg);
    }
}


sub addupdate_common {

    my ($request, $config, @extra) = @_;
    
    croak "addupdate_simple: first parameter must be a configuration hash"
	unless ref $config eq 'HASH';
    
    my $class = $config->{class} || 'EditTransaction';
    my $main_table = $config->{main_table};
    my $url_ruleset = $config->{url_ruleset};
    
    croak "addupdate_common: configuration must include a valid internal table name under 'main_table'"
	unless $main_table && $TABLE{$main_table};
    
    croak "addupdate_common: the value of 'class' must be either 'EditTransaction' or a subclass"
	if $config->{class} && ! $config->{class}->isa('EditTransaction');
    
    croak "addupdate_common: you cannot specify 'record_handler' and 'table_selector' together"
	if $config->{record_handler} && $config->{table_selector};
    
    croak "addupdate_common: the value of 'table_sequence' must be a list of table names"
	if $config->{table_sequence} && ref $config->{table_sequence} ne 'ARRAY';
    
    my @ruleset_patterns;
    
    if ( ref $config->{entry_ruleset} eq 'ARRAY' && $config->{entry_ruleset}->@* )
    {
	my $rs_name = $config->{entry_ruleset}[0];

	croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
	
	push @ruleset_patterns, $config->{entry_ruleset};
    }
    
    elsif ( $config->{entry_ruleset} )
    {
	my $rs_name = $config->{entry_ruleset};
	
	croak "entry_ruleset must be a list or a scalar" if ref $config->{entry_ruleset};
	croak "you cannot specify 'entry_ruleset' and 'entry_list' together" if $config->{entry_list};
	croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
	
	if ( $config->{entry_key} )
	{
	    push @ruleset_patterns, [ $rs_name, $config->{entry_key}, 'DEFAULT' ];
	}
	
	else
	{
	    push @ruleset_patterns, $rs_name;
	}
    }
    
    elsif ( $config->{entry_list} )
    {
	croak "entry_list must be a list of lists" unless ref $config->{entry_list} eq 'ARRAY' &&
	    $config->{entry_list}->@* && ref $config->{entry_list}[0] eq 'ARRAY' && $config->{entry_list}[0]->@*;
	
	foreach my $list ( $config->{entry_list}->@* )
	{
	    my $rs_name = ref $list eq 'ARRAY' ? $list->[0] : $list;
	    
	    croak "unknown ruleset '$rs_name'" unless $request->{ds}->has_ruleset($rs_name);
	    
	    push @ruleset_patterns, $list;
	}
    }
    
    else
    {
	croak "no ruleset specified";
    }
    
    # Authenticate the user who made this request, and check that they have write permission on
    # the main table. 
    
    my $perms = $request->require_authentication($main_table);
    
    # Parse the URL parameters, and return a hash of allowances and another one with the rest of
    # the parameter values. If $url_ruleset is empty, the second return value will be an empty
    # hash. The url parameters will already have been validated against the ruleset before this
    # method is called, and if they are invalid an error response will already have been returned.
    
    my ($allowances, $main_params) = $request->parse_main_params($url_ruleset);
    
    # If extra allowances were specified, add them now. They can be specified either through the
    # configuration or as additional arguments.

    foreach my $value ( $config->{allowances}->@*, @extra )
    {
	croak "addupdate_common: invalid allowance '$value'" unless $class->has_allowance($value);
	$allowances->{$value} = 1;
    }
    
    # Then decode the body, and extract input records from it. Any attribute values specified in
    # $main_params override those in the individual records. If a parameter error is encountered,
    # throw a code 400 exception unless the PROCEED allowance was given. In that case, the record
    # will be skipped.
    
    my (@records) = $request->parse_body_records($allowances, $main_params, @ruleset_patterns);
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request body");
    }
    
    # If we get here without any errors being detected so far, create a new object to handle this
    # operation. This will be of class EditTransaction unless a subclass was specified in the
    # configuration.
    
    my $edt = $class->new($request, $perms, $main_table, $allowances);
    
    # The next step is to iterate through the records extracted from the request body and process
    # each one in turn. Every record will be validated against the corresponding database table,
    # and if no errors are detected it will be queued up for insertion, updating, replacement,
    # deletion, or some auxiliary operation.
    
    my $record_cleaner = $config->{record_cleaner};
    my $record_handler = $config->{record_handler};
    my $table_list = $config->{table_list};
    
    croak "the value of 'record_cleaner' must be a code ref or a method name"
	if $record_cleaner && ref $record_cleaner && ref $record_cleaner ne 'CODE';
    
    croak "the value of 'record_handler' must be a code ref or a method name"
        if $record_handler && ref $record_handler && ref $record_handler ne 'CODE';
    
    croak "the value of 'table_list' must be a list of lists" if $table_list &&
	! ( ref $table_list eq 'ARRAY' && $table_list->@* &&
	    ref $table_list->[0] eq 'ARRAY' && $table_list->[0]->@* );
    
    my (%TABLE_USED, @AUX_TABLES);
    
    foreach my $r ( @records )
    {
	# If a record cleaning routine was specified, call it first. The purpose of such routines
	# is to modify record values prior to insertion or update. For example, the interface
	# developer may wish to allow certain attributes to be specified in ways that don't match
	# the database structure and must be converted first.
	
	if ( $record_cleaner )
	{
	    $request->$record_cleaner($edt, $r, $main_table, $perms);
	}
	
	# For maximal control, a record handler may be specified. If so, it is expected to call
	# either $edt->process_record or $edt->skip_record for every record it is given. It should
	# return the name of the table in which this record was processed.
	
	if ( $record_handler )
	{
	    my $table_name = $request->$record_handler($edt, $r, $main_table, $perms);
	    
	    push @AUX_TABLES, $table_name unless $TABLE_USED{$table_name} || $table_name eq $main_table;
	    $TABLE_USED{$table_name}++;
	}
	
	# Otherwise, if the record includes the key _skip then call $edt->skip_record.
	
	elsif ( $r->{_skip} )
	{
	    $edt->skip_record($r);
	}
	
	# Otherwise, if a table selector list is specified then use it to select the table that
	# best matches this record. Each item in the list must be a list, in a format similar to
	# the table-selection arguments to &parse_body_records. The first item must be a table
	# name, and the remaining items the names of keys. If the record has a non-empty, non-zero
	# value for any of these keys, that table will be selected. In most circumstances, the two
	# lists will match. The ruleset selected for validating a record should correspond to the
	# table in which that record will be stored. The last item can be a simple table name,
	# which will select that table regardless, or 'NO_MATCH', which will reject the record.
	
	elsif ( $table_list )
	{
	    my ($table_name, @check_keys);
	    
	  PATTERN:
	    foreach my $item ( $table_list->@* )
	    {
		if ( ref $item eq 'ARRAY' )
		{
		    ($table_name, @check_keys) = $item->@*;
		    
		    foreach $k ( @check_keys )
		    {
			last PATTERN if $r->{$k};
		    }
		}

		elsif ( ref $item )
		{
		    my $type = reftype $item;
		    croak "Invalid table_list pattern ($type)";
		}

		elsif ( $item )
		{
		    $table_name = $item;
		    last PATTERN;
		}
	    }
	    
	    if ( $table_name && $table_name ne 'NO_MATCH' )
	    {
		$edt->process_record($table_name, $r);
		push @AUX_TABLES, $table_name unless $TABLE_USED{$table_name} || $table_name eq $main_table;
		$TABLE_USED{$table_name}++;
	    }
	    
	    else
	    {
		my $label = $r->{_label} || "#$INDEX";
		$request->errwarn($PROCEED, "E_UNRECOGNIZED ($label): Could not process record, no matching table");
		$edt->skip_record($r);
	    }
	}
	
	# If none of these conditions hold, then call $edt->process_record with $main_table.
	
	else
	{
	    $edt->process_record($main_table, $r);
	    $TABLE_USED{$main_table}++;
	}
    }
        
    # Now attempt to execute the queued actions inside a database transaction. If no database
    # errors occur, and none occurred above, the transaction will be automatically committed. If
    # any errors occurred, the transaction will be rolled back unless the PROCEED allowance was
    # given. In that case, it will be committed after skipping any records that generated errors.
    
    $edt->commit;
    
    # Handle any errors or warnings that may have been generated.
    
    $request->collect_edt_errors($edt);
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
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
    
    my $main_key = $config->{main_key} || get_table_property($tn, 'PRIMARY_KEY');
    
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
    
    foreach $tn ( $main_table, @AUX_TABLES )
    {
	my $key_labels = $edt->key_labels($tn);
	my $display_key = get_table_property($tn, 'PRIMARY_KEY') || 'oid';
	
	$table_records{$tn} = [ ];
	
	# If any records have been deleted, those are listed first by means of placeholder records.
	
	if ( my @deleted_keys = $edt->deleted_keys($tn) )
	{
	    foreach my $key_value ( sort { $a <=> $b} @deleted_keys )
	    {
		my $record = { $primary_key => $key_value, status => 'deleted',
			       _operation => 'delete', _label => $key_label->{$key_value} };

		push $table_records{$tn}->@*, $record;
	    }
	}
	
	# $$$ EditTranasction needs to generate records corresponding to the input ones, conveying
	# all the information necessary to produce a result: key, label, operation, action,
	# result, etc. 
    
    my @existing_keys = ($edt->inserted_keys, $edt->updated_keys, $edt->replaced_keys);
    
    $request->list_updated_refs($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;


}


# sub debug_line {
    
#     my ($request, $line) = @_;
    
#     print STDERR "$line\n";
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



1;
