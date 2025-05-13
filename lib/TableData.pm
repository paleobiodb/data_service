# 
# TableData.pm
# 
# This module manages table schemas. It fetches them when necessary and checks records against
# them to make sure that data inserts and updates will complete properly.
# 
# Author: Michael McClennen

package TableData;

use strict;

use TableDefs qw(%TABLE get_column_properties);
use PBDBFields qw(%COMMON_FIELD_IDTYPE %COMMON_FIELD_SPECIAL %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);

use Carp qw(croak);
use HTTP::Validate qw(:validators);
use ExternalIdent qw(extract_identifier generate_identifier VALID_IDENTIFIER %IDP);
use Scalar::Util qw(reftype);

use base 'Exporter';

our (@EXPORT_OK) = qw(complete_output_block complete_ruleset add_to_ruleset complete_valueset);

our (@CARP_NOT) = qw(EditTransaction Try::Tiny);

our (%COMMON_FIELD_COM) = ( taxon_no => 'tid',
			    resource_no => 'rid',
			    collection_no => 'cid',
			    interval_no => 'iid',
			    authorizer_no => 'ati',
			    enterer_no => 'eni',
			    modifier_no => 'mdi',
			    created => 'dcr',
			    modified => 'dmd',
			  );

our (%COMMON_FIELD_IDSUB);

our (%INT_SIZE) = ( tiny => 'one byte integer',
		    small => 'two byte integer',
		    medium => 'three byte integer',
		    regular => 'four byte integer',
		    big => 'eight byte integer');

our (%INT_SIZE2) = ( tiny => 'one byte',
		     small => 'two bytes',
		     medium => 'three bytes',
		     regular => 'four bytes',
		     big => 'eight bytes' );

sub complete_output_block {
    
    my ($ds, $dbh, $block_name, $table_specifier, $override) = @_;
    
    $override = { } unless ref $override eq 'HASH';
    
    # First get a hash of table column definitions
    
    my $tableinfo = TableSchema->table_info_ref($table_specifier, $dbh, $ds->debug);
    my $columninfo = TableSchema->table_column_ref($table_specifier);
    
    # Then get the existing contents of the block and create a hash of the field names that are
    # already defined. If no block by this name is yet defined, create an empty one.
    
    unless ( $ds->{block}{$block_name} )
    {
	my $new_block = { name => $block_name,
			  include_list => [],
			  output_list => [] };
	
	$ds->{block}{$block_name} = bless $new_block, 'Web::DataService::Block';
    }
    
    my $block = $ds->{block}{$block_name};
    my $output_list = $block->{output_list};
    my %block_has_field;
    my $block_needs_oid = 1;
    
    foreach my $b ( @$output_list )
    {
	$block_has_field{$b->{output}} = 1 if $b->{output};
	$block_needs_oid = 0 if $b->{com_name} && $b->{com_name} eq 'oid';
    }
    
    # Then go through the field list from the schema and add any fields that aren't already in the
    # output list. We need to translate names that end in '_no' to '_id', and we can substitute
    # compact vocabulary names where known. If an override hash was defined, look up the field
    # name in that hash and apply any specified attributes. If the _ignore key is present with a
    # true value, skip that field entirely.
    
    my $field_list = $tableinfo->{COLUMN_LIST};
    
    foreach my $field_name ( @$field_list )
    {
	# If this field is one of the standard ones for authorizer/enterer or created/modified,
	# then skip it.
	
	if ( $COMMON_FIELD_SPECIAL{$field_name} )
	{
	    next;
	}
	
	# If this field is already in the output block, skip it as well. This allows us to
	# explicitly include some of the fields in the block definition, with documentation
	# strings and other attributes, and prevents duplicate output fields.
	
	next if $block_has_field{$field_name};
	
	# If this field has the 'IGNORE' attribute set, skip it as well. Check for an override
	# entry as well.

	next if $columninfo->{$field_name}{IGNORE} ||
	    ref $override->{$field_name} && $override->{$field_name}{IGNORE};
	    $override->{$field_name} && $override->{$field_name} eq 'IGNORE';
	
	# Now create a record to represent this field, along with a documentation string and
	# whatever other attributes we can glean from the table definition.
	
	my $field_record = $columninfo->{$field_name};
	my $type = $field_record->{Type};
	
	my $r = { output => $field_name };
	
	if ( $COMMON_FIELD_COM{$field_name} )
	{
	    $r->{com_name} = $COMMON_FIELD_COM{$field_name};
	}
	
	elsif ( $field_name =~ /(.*)_no/ )	# $$$ need to replace this with a hash mapping _no
                                                # => _id
	{
	    if ( $block_needs_oid )
	    {
		$r->{com_name} = 'oid';
		$block_needs_oid = 0;
	    }
	    
	    else
	    {
		$r->{com_name} = $1 . '_id';
	    }
	}
	
	else
	{
	    $r->{com_name} = $field_name;
	}
	
	my $doc = "The contents of field C<$field_name> from the table C<$TABLE{$table_specifier}>.";
	
	if ( $type =~ /int\(/ )
	{
	    $doc .= " The value will be an integer.";
	}
	
	# If there are any attribute overrides specified for this field, apply them now.

	if ( ref $override->{$field_name} )
	{
	    my $ov = $override->{$field_name};
	    
	    foreach my $attr ( keys %$ov )
	    {
		if ( $attr eq 'doc' && $ov->{$attr} )
		{
		    $doc = $ov->{$attr};
		}
		
		if ( defined $ov->{$attr} )
		{
		    $r->{$attr} = $ov->{$attr};
		}

		else
		{
		    delete $r->{$attr};
		}
	    }
	}
	
	# Add the record for this field to the output list for the block we are completing, and
	# add it to the documentation list as well along with a default documentation string.
	
	push @$output_list, $r;
	$ds->add_doc($block, $r);
	$ds->add_doc($block, $doc);
	
	# If the field is one that we know contains a value that should be expressed as an
	# external identifier, create a subroutine to do that.
	
	if ( my $type = $COMMON_FIELD_IDTYPE{$field_name} )
	{
	    unless ( $COMMON_FIELD_IDSUB{$type} )
	    {
		$COMMON_FIELD_IDSUB{$type} = sub {
		    my ($request, $value) = @_;
		    return $value unless $request->{block_hash}{extids};
		    return generate_identifier($type, $value);
		};
	    }
	    
	    push @$output_list, { set => $field_name, code => $COMMON_FIELD_IDSUB{$type} };
	}
    }
    
    $ds->process_doc($block);
}


# complete_ruleset ( dbh, ruleset_name, table_specifier )
#
# Complete the definition of the specified ruleset by reading the table schema corresponding to
# $table_identifier and iterating through the columns. For each table column, add a new parameter
# to the ruleset unless an existing ruleset parameter already corresponds to that column.

sub complete_ruleset {
    
    my ($ds, $dbh, $ruleset_name, $table_specifier, $override) = @_;

    $override = { } unless ref $override eq 'HASH';
    
    # First get a hash of table column definitions
    
    my $tableinfo = TableSchema->table_info_ref($table_specifier, $dbh, $ds->debug);
    my $columninfo = TableSchema->table_column_ref($table_specifier);
    
    # Then get the existing ruleset documentation and create a hash of the field names that are
    # already defined. If no ruleset by this name is yet defined, croak.
    
    my $rs = $ds->validator->{RULESETS}{$ruleset_name};
    
    croak "unknown ruleset '$ruleset_name'" unless defined $rs;
    
    # Record all of the parameters in the current ruleset. For each parameter
    # ending in '_id', also add the corresponding name ending in '_no'. For
    # example, the ruleset parameter 'collection_id' will correspond to the
    # database column 'collection_no'.
    
    my @param_list = $ds->validator->list_params($ruleset_name);
    
    my %ruleset_has_field;

    foreach my $param ( @param_list )
    {
	$ruleset_has_field{$param} = 1;

	if ( $param =~ /^(\w+)_id$/ )
	{
	    $ruleset_has_field{$1 . "_no"} = 1;
	}
    }
    
    # We need to keep a list of the parameter records generated below, because the references to
    # them inside the validator record are weakened.
    
    $ds->{my_param_records} ||= [ ];
    
    # Then go through the field list from the schema and add any fields that aren't already in the
    # ruleset.
    
    my $field_list = $tableinfo->{COLUMN_LIST};
    
    foreach my $column_name ( @$field_list )
    {
	# Skip all of the special columns, plus 'authorizer', 'enterer', and
	# 'modifier', unless we are told to include them.
	
	unless ( $override->{$column_name} && $override->{$column_name} ne 'IGNORE' )
	{
	    next if $COMMON_FIELD_SPECIAL{$column_name};
	    next if $column_name =~ /^authorizer$|^enterer$|^modifier$/;
	}
	
	my $field_record = $columninfo->{$column_name};
	
	my $field_name = $column_name;
	
	if ( $field_record->{ALTERNATE_NAME} )
	{
	    $field_name = $field_record->{ALTERNATE_NAME};
	}
	
	# If the ruleset already has a rule corresponding to this field name, skip it. This allows
	# you to specifically include certain fields in the ruleset with documentation strings and
	# attributes, without them being duplicated by this routine.
	
	next if $ruleset_has_field{$field_name};
	
	next if $ruleset_has_field{$column_name} && ! $field_record->{ALTERNATE_ONLY};
	
	# If there is an entry for this field in the $override hash with the IGNORE attribute,
	# ignore it. Also ignore any field for which the column property IGNORE was set.
	
	next if $field_record->{IGNORE} ||
	    ref $override->{$field_name} && $override->{$field_name}{IGNORE} ||
	    $override->{$field_name} && $override->{$field_name} eq 'IGNORE';
	
	# Now create a record to represent this field, along with a documentation string
	# and whatever other attributes we can glean from the table definition. We include
	# the attribute 'allow_empty' by default, so that clients can set the value of
	# this column to the empty string or null.
	
	my $rr = { optional => $field_name, allow_empty => 1 };
	
	my $attr = '';
	$attr = 'B<required> ' if $field_record->{REQUIRED};
	
	my $doc = "This ${attr}parameter sets the value of C<$field_name> in the C<$TABLE{$table_specifier}> table.";
	
	my $type = $field_record->{TypeMain} // '?';
	my @type_params = $field_record->{TypeParams} ? $field_record->{TypeParams}->@* : ();
	
	if ( $field_name ne $column_name && ! $field_record->{ALTERNATE_ONLY} )
	{
	    $rr->{alias} = $column_name;
	}
	
	# Add documentation depending on the data type of this field, unless
	# documentation was specified in the override hash.
	
	if ( ref $override->{$field_name} &&
	     ($override->{$field_name}{doc} || $override->{$field_name}{doc_full}) )
	{
	    my $newdoc = $override->{$field_name}{doc_full} // $override->{$field_name}{doc};
	    
	    if ( $newdoc =~ /%{type}/ )
	    {
		my $typestring;

		if ( $type eq 'char' )
		{
		    $typestring = $type_params[0] eq 'data' ? 'binary data' : 'a string';
		}
		
		elsif ( $type eq 'integer' )
		{
		    $typestring = $type_params[0] eq 'unsigned' ?
			'an unsigned integer' : 'an integer' ;
		}
		
		elsif ( $type eq 'fixed' || $type eq 'floating' )
		{
		    $typestring = $type_params[0] eq 'unsigned' ?
			'an unsigned decimal number' : 'a decimal number';
		}

		elsif ( $type eq 'enum' )
		{
		    $typestring = ($type_params[0] && $type_params[0] eq 'set') ?
			'one or more values' : 'a value';
		}

		elsif ( $type eq 'boolean' )
		{
		    $typestring = 'a boolean value';
		}

		elsif ( $type eq 'datetime' )
		{
		    $typestring = "a $type_params[0]";
		}
		
		else
		{
		    $typestring = '?';
		}

		$newdoc =~ s/%{type}/$typestring/eg;
	    }
	    
	    if ( $newdoc =~ /%{size}/ )
	    {
		my $size;
		
		if ( $type eq 'char' )
		{
		    $size = $type_params[1];
		}
		
		elsif ( $type eq 'integer' )
		{
		    $size = $INT_SIZE2{$type_params[1]} || '?';
		}

		elsif ( $type eq 'floating' )
		{
		    if ( $type_params[1] && $type_params[1] eq 'float' )
		    {
			$size = 'single precision floating point';
		    }

		    elsif ( $type_params[1] && $type_params[1] eq 'double' )
		    {
			$size = 'double precision floating point';
		    }

		    else
		    {
			$size = '?';
		    }
		}

		elsif ( $type eq 'fixed' )
		{
		    my $a = ($type_params[1] + $type_params[2]) // '?';
		    my $b = $type_params[2] // '?';
		    $size = "decimal($a,$b)";
		}

		else
		{
		    $size = '?';
		}

		$newdoc =~ s/%{size}/$size/eg;
	    }
	    
	    if ( $newdoc =~ /%{charset}/ )
	    {
		my $charset = '?';
		
		if ( $type_params[0] && $type_params[0] eq 'text' )
		{
		    if ( $type_params[3] =~ /latin1/ )
		    {
			$charset = 'latin1';
		    }
		    
		    elsif ( $type_params[3] =~ /utf(\d+)/ )
		    {
			$charset = "utf-$1";
		    }
		    
		    else
		    {
			$charset = $type_params[3] || '?';
		    }
		}
		
		$newdoc =~ s/%{size}/$charset/eg;
	    }

	    if ( $newdoc =~ /%{values}/ )
	    {
		my $values = '?';
		
		if ( $type_params[0] && ($type_params[0] eq 'enum' || $type_params[0] eq 'set') )
		{
		    $values = $type_params[2] // '?';
		}
		
		elsif ( $type eq 'boolean' )
		{
		    $values = "'1','0','true','false','yes','no','y','n','on','off'";
		}

		$newdoc =~ s/%{values}/$values/eg;
	    }

	    if ( $newdoc =~ /%{extid}/ )
	    {
		my $extid = $columninfo->{$column_name}{EXTID_TYPE} // '?';

		$newdoc =~ s/%{extid}/$extid/eg;
	    }

	    if ( $override->{$field_name}{doc_full} )
	    {
		$doc = $newdoc;
	    }

	    else
	    {
		$doc .= " $newdoc";
	    }
	}
	
	elsif ( my $extid_type = $columninfo->{$column_name}{EXTID_TYPE} )
	{
	    $doc .= " It accepts an external identifier of type '$IDP{$extid_type}', or an integer value.";
	}
	
	elsif ( $type eq 'char' )
	{
	    my $which = ($type_params[0] && $type_params[0] eq 'data') ? 'binary data' : 'a string';
	    my $size = $type_params[1] // '?';
	    
	    $doc .= " It accepts $which of maximum length $size";

	    if ( $type_params[0] && $type_params[0] eq 'text' )
	    {
		if ( $type_params[2] && $type_params[2] =~ /latin1/ )
		{
		    $doc .= ", stored as latin1";
		}
		
		elsif ( $type_params[2] && $type_params[2] =~ /utf(\d+)/ )
		{
		    $doc .= ", stored as utf-$1";
		}
	    }
	    
	    $doc .= ".";
	}
	
	elsif ( $type eq 'boolean' )
	{
	    $doc .= " It accepts any of the following values: C<'1','0','true','false','yes','no','y','n','on','off'>.";
	}
	
	elsif ( $type eq 'integer' )
	{
	    my $prefix = $type_params[0] eq 'unsigned' ? 'an unsigned' : 'a';
	    my $size = $INT_SIZE{$type_params[1]} || 'integer';
	    $prefix = 'an' if $size eq 'integer';
	    
	    $doc .= " It accepts $prefix $size value.";
	}
	
	elsif ( $type eq 'fixed' )
	{
	    my $prefix = $type_params[0] eq 'unsigned' ? 'an unsigned' : 'a';
	    $doc .= " It accepts $prefix decimal value with at most $type_params[1] digits before the decimal and at most $type_params[2] digits after.";
	}
	
	elsif ( $type eq 'floating' )
	{
	    my $prefix = $type_params[0] eq 'unsigned' ? 'an unsigned' : 'a';
	    my $size = $type_params[2] eq 'double' ? 'double precision' : 'single precision';
	    $doc .= " It accepts $prefix decimal value that can be stored in $size floating point.";
	}
	
	elsif ( $type eq 'enum' )
	{
	    my $prefix = $type_params[0] eq 'enum' ? 'a value' : 'one or more values';
	    my $list = $type_params[2];
	    $list =~ s/','/', '/g;
	    $doc .= " It accepts $prefix from the following list: C<$list>.";
	}
	
	elsif ( $type eq 'datetime' )
	{
	    $doc .= " It accepts a $type_params[0] in any format acceptable to MariaDB.\n";
	}
	
	else
	{
	    $doc .= " The data type is not reported.";
	}

	# If this column accepts an external identifier, set the validator function
	# of the ruleset to a function that accepts the specified identifier type.
	
	if ( my $extid_type = $columninfo->{$column_name}{EXTID_TYPE} )
	{
	    $rr->{valid} = VALID_IDENTIFIER($extid_type);
	}

	# If the override hash includes any attributes other than 'doc', add
	# them to the rule record, overriding any existing attributes with those
	# keys. 
	
	if ( ref $override->{$field_name} eq 'HASH' )
	{
	    foreach my $k ( keys $override->{$field_name}->%* )
	    {
		if ( $k ne 'doc' )
		{
		    $rr->{$k} = $override->{$field_name}{$k};
		}
	    }
	}
	
	# Add this rule record and its corresponding documentation to the
	# specified ruleset.
	
	push @{$ds->{my_param_records}}, $rr;
	
	$ds->validator->add_rules($rs, $rr, $doc);
    }
}


# add_to_ruleset ( ds, ruleset_name, @rules_and_doc )

sub add_to_ruleset {
    
    my ($ds, $ruleset_name, @rules_and_doc) = @_;
    
    my $rs = $ds->validator->{RULESETS}{$ruleset_name};
    
    while ( @rules_and_doc )
    {
	my $rr = shift @rules_and_doc;
	my @doc;
	
	while ( @rules_and_doc && ! ref $rules_and_doc[0] )
	{
	    push @doc, shift @rules_and_doc;
	}
	
	croak "bad rule" unless $rr && reftype($rr) eq 'HASH';
	
	push @{$ds->{my_param_records}}, $rr;
	
	$ds->validator->add_rules($rs, $rr, @doc);
    }
}


# complete_valueset ( ds, dbh, set_name, table_specifier )
# 
# The argument $table_specifier should specify a table whose first two columns are a
# number and a string value. These are both added to the specified set, with the string
# values added to the value list and the numeric values collected under 'numeric_list'.
# 
# The specified set can then be used to map either numeric values into strings or vice
# versa. 

sub complete_valueset {
    
    my ($ds, $dbh, $set_name, $table_specifier) = @_;
    
    my $vs = $ds->{set}{$set_name} || croak "unknown set '$set_name'";
    
    $vs->{value_list} ||= [ ];
    $vs->{numeric_list} ||= [ ];
    
    croak "unknown table '$table_specifier'" unless exists $TABLE{$table_specifier};
    
    my $sql = "SELECT * FROM $TABLE{$table_specifier} LIMIT 50";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => [0, 1] });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	foreach my $row ( $result->@* )
	{
	    my ($numeric, $string) = $row->@*;
	    
	    $vs->{value}{$string} = $numeric;
	    $vs->{value}{$numeric} = $string;
	    
	    push $vs->{value_list}->@*, $string;
	    push $vs->{numeric_list}->@*, $numeric;
	}
    }
}


# Create a package for use in fetching table information

package TableSchema;

use Role::Tiny::With;

with 'EditTransaction::TableInfo';
with 'EditTransaction::Mod::MariaDB';


1;
