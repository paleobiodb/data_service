# 
# TableData.pm
# 
# A role that contains routines for formatting and processing PBDB data based on table
# definitions.
# 
# Author: Michael McClennen

package PB2::TableData;

use strict;

use Carp qw(croak);
use ExternalIdent qw(extract_identifier generate_identifier);

use base 'Exporter';

our (@EXPORT_OK) = qw(complete_output_block complete_ruleset get_table_schema);

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

our (%COMMON_FIELD_IDTYPE) = ( taxon_no => 'TXN',
			       resource_no => 'RES',
			       collection_no => 'COL',
			       interval_no => 'INT',
			       authorizer_no => 'PRS',
			       enterer_no => 'PRS',
			       modifier_no => 'PRS',
			     );

our (%COMMON_FIELD_IDSUB);

our (%COMMON_FIELD_OTHER) = ( authorizer_no => 'authent',
			      enterer_no => 'authent',
			      modifier_no => 'authent',
			      enterer_id => 'authent',
			      created => 'crmod',
			      modified => 'crmod',
			    );

our (%TABLE_HAS_FIELD);


sub complete_output_block {
    
    my ($ds, $dbh, $block_name, $table_name) = @_;
    
    # First get a hash of table column definitions
    
    my $schema = get_table_schema($ds, $dbh, $table_name);
    
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
    # compact vocabulary names where known.
    
    my $field_list = $schema->{_field_list};
    
    foreach my $field_name ( @$field_list )
    {
	# If this field is one of the standard ones for authorizer/enterer or created/modified,
	# then skip it. But also record that the table has this field, for use later.
	
	if ( $COMMON_FIELD_OTHER{$field_name} )
	{
	    $TABLE_HAS_FIELD{$table_name}{$field_name} = $COMMON_FIELD_OTHER{$field_name};
	    next;
	}
	
	# If this field is already in the output block, skip it as well. This allows us to
	# explicitly include some of the fields in the block definition, with documentation
	# strings and other attributes, and prevents duplicate output fields.
	
	next if $block_has_field{$field_name};
	
	# Now create a record to represent this field, along with a documentation string and
	# whatever other attributes we can glean from the table definition.
	
	my $field_record = $schema->{$field_name};
	my $type = $field_record->{Type};
	
	my $r = { output => $field_name };
	
	if ( $COMMON_FIELD_COM{$field_name} )
	{
	    $r->{com_name} = $COMMON_FIELD_COM{$field_name};
	}
	
	elsif ( $field_name =~ /(.*)_no/ )
	{
	    if ( $block_needs_oid )
	    {
		$r->{com_name} = 'oid';
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
	
	my $doc = "The contents of field C<$field_name> from the table.";
	
	if ( $type =~ /int\(/ )
	{
	    $doc .= " The value will be an integer.";
	}
	
	$block_needs_oid = 0;
	
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


sub complete_ruleset {
    
    my ($ds, $dbh, $ruleset_name, $table_name) = @_;
    
    # First get a hash of table column definitions
    
    my $schema = get_table_schema($ds, $dbh, $table_name);
    
    # Then get the existing ruleset documentation and create a hash of the field names that are
    # already defined. If no ruleset by this name is yet defined, croak.
    
    my $rs = $ds->validator->{RULESETS}{$ruleset_name};
    
    croak "unknown ruleset '$ruleset_name'" unless defined $rs;
    
    my @param_list = $ds->validator->list_params($ruleset_name);
    
    my %ruleset_has_field = map { $_ => 1 } @param_list;
    
    # We need to keep a list of the parameter records generated below, because the references to
    # them inside the validator record are weakened.
    
    $ds->{my_param_records} ||= [ ];
    
    # Then go through the field list from the schema and add any fields that aren't already in the
    # ruleset. We need to translate names that end in '_no' to '_id'.
    
    my $field_list = $schema->{_field_list};
    
    foreach my $field_name ( @$field_list )
    {
	next if $COMMON_FIELD_OTHER{$field_name};
	
	if ( $field_name =~ /(.*)_no/ )
	{
	    $field_name = $1 . '_id';
	}
	
	next if $ruleset_has_field{$field_name};
	
	my $field_record = $schema->{$field_name};
	my $type = $field_record->{Type};
	
	my $rr = { type => 'param', param => $field_name };
	my $doc = "This parameter sets the value of C<$field_name> in the table.";
	
	if ( $type =~ /int\(/ )
	{
	    $doc .= " The value must be an integer.";
	}
	
	push @{$ds->{my_param_records}}, $rr;
	
	$ds->validator->add_doc($rs, $rr, $doc);
    }
}


sub get_table_schema {
    
    my ($obj, $dbh, $table_name) = @_;
    
    my $ds = $obj->can('ds') ? $obj->ds : $obj;
    
    if ( ref $ds->{my_table_schema}{$table_name} eq 'HASH' )
    {
	return $ds->{my_table_schema}{$table_name};
    }
    
    my ($sql, $check_table, %schema, $quoted_table);
    
    if ( $table_name =~ /(\w+)[.](.+)/ )
    {
	$sql = "SHOW TABLES FROM `$1` LIKE " . $dbh->quote($2);
	$quoted_table = "`$1`.". $dbh->quote_identifier($2);
    }
    
    else
    {
	$sql = "SHOW TABLES LIKE " . $dbh->quote($table_name);
	$quoted_table = $dbh->quote_identifier($table_name);
    }
    
    print STDERR "$sql\n\n";
    
    eval {
	($check_table) = $dbh->selectrow_array($sql);
    };
    
    croak "unknown table '$table_name'" unless $check_table;
    
    print STDERR "	SHOW COLUMNS FROM $quoted_table\n\n" if $ds->debug;
    
    my $columns_ref = $dbh->selectall_arrayref("
	SHOW COLUMNS FROM $quoted_table", { Slice => { } });
    
    my @field_list;
    
    foreach my $c ( @$columns_ref )
    {
	my $field = $c->{Field};
	my $can_input = $c->{Key} eq 'PRI' ? 0 : 1;
	
	$can_input = 0 if $field eq 'created' || $field eq 'created_on' || $field eq 'modified' ||
	    $field eq 'authorizer_no' || $field eq 'enterer_no';
	
	$c->{can_input} = $can_input;
	
	$schema{$field} = $c;
	push @field_list, $field;
	
	if ( $c->{Key} eq 'PRI' && ! $schema{_primary} )
	{
	    $schema{_primary} = $field;
	}
    }
    
    $schema{_field_list} = \@field_list;
    
    $ds->{my_table_schema}{$table_name} = \%schema;
    
    return \%schema;
}


1;
