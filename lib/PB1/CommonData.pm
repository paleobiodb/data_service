# CollectionData
# 
# A class that contains common routines for formatting and processing PBDB data.
# 
# Author: Michael McClennen

package PB1::CommonData;

use strict;

use HTTP::Validate qw(:validators);

use parent 'Exporter';

our (@EXPORT_OK) = qw(generateAttribution generateReference generateRISReference);

use Moo::Role;


my $remember_ds;		# for use by datetime_value()

our (%PERSON_NAME);


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $remember_ds = $ds;
    
    $ds->define_ruleset('1.1:common_params' => 
	"The following parameters can be used with most requests:",
	{ optional => 'limit', valid => [POS_ZERO_VALUE, ENUM_VALUE('all')], 
	  error => "acceptable values for 'limit' are a positive integer, 0, or 'all'",
	  default => 500 },
	    "Limits the number of records returned.  The value may be a positive integer, zero, or C<all>.",
	    "It defaults to 500, in order to prevent people from accidentally sending requests that",
	    "might generate megabytes of data in response.  If you really want the entire result set,",
		"specify C<limit=all>.",
	{ optional => 'offset', valid => POS_ZERO_VALUE },
	    "Returned records start at this offset in the result set.  The value may be a positive integer or zero.",
	    "You can use this parameter along with C<limit> to return a large result set in many smaller chunks.",
	{ optional => 'count', valid => FLAG_VALUE },
	    "If this parameter is specified, then the response includes a header stating",
	    "the number of records that match the query and the number of records actually returned.",
	    "For more information about how this information is encoded, see the documentation pages",
	    "for the various L<response formats|/data1.1/formats>.  This parameter does not need any value.",
	{ optional => 'showsource', valid => FLAG_VALUE },
	    "If this parameter is specified, then the response will include a header containing",
	    "a variety of information including:", "=over",
	    "=item *", "The source of the data",
	    "=item *", "The license under which it is provided",
	    "=item *", "The date and time at which the data was accessed",
	    "=item *", "The URL and parameters used to generate this result set",
	    "=back",
	    "This is particularly useful for responses that will be saved to disk for later analysis",
	    "and use.  This extra information will serve to document the criteria by which data are included",
	    "in the result set, the time at which the result was generated, and will contain a URL",
	    "which can be used to re-run the query at a later time.",
	    "For more information about how this information is encoded, see the documentation pages",
	    "for the various L<response formats|/data1.1/formats>.  This parameter does not need any value.",
	{ optional => 'textresult', valid => FLAG_VALUE },
	    "If specified, then the result will be given a content type of 'text/plain'.",
	    "With most browsers, that will cause the result to be displayed directly",
	    "instead of saved to disk.  This parameter does not need any value.",
	{ optional => 'markrefs', valid => FLAG_VALUE },
	    "If specified, then formatted references will be marked up with E<lt>bE<gt> and E<lt>iE<gt> tags.",
	    "This parameter does not need a value.",
	{ optional => 'vocab', valid => $ds->valid_vocab },
	    "Selects the vocabulary used to name the fields in the response.  You only need to use this if",
	    "you want to override the default vocabulary for your selected format.",
	    "Possible values depend upon the particular URL path, and include:", $ds->document_vocab,
	">The following parameters are only relevant to the text formats (.csv, .tsv, .txt):",
	{ optional => 'noheader', valid => FLAG_VALUE },
	    "If specified, then the header line which gives the field names is omitted.",
	    "This parameter does not need any value.",
	{ optional => 'linebreak', valid => ENUM_VALUE('cr','crlf') },
	    "Specifies the character sequence used to terminate each line.",
	    "The value may be either 'cr' or 'crlf', and defaults to the latter.",
	{ ignore => 'splat' });
    
    $ds->define_ruleset('1.1:common:select_crmod' =>
	{ param => 'created_before', valid => \&datetime_value },
	    "Select only records that were created before the specified L<date or date/time|/data1.1/datetime>.",
	{ param => 'created_after', valid => \&datetime_value, alias => 'created_since' },
	    "Select only records that were created on or after the specified L<date or date/time|/data1.1/datetime>.",
	{ param => 'modified_before', valid => \&datetime_value },
	    "Select only records that were last modified before the specified L<date or date/time|/data1.1/datetime>.",
	{ param => 'modified_after', valid => \&datetime_value, alias => 'modified_since' },
	    "Select only records that were modified on or after the specified L<date or date/time|/data1.1/datetime>.");
    
    $ds->define_block('1.1:common:crmod' =>
	{ select => ['$bt.created', '$bt.modified'], tables => '$bt' },
	{ output => 'created', com_name => 'dcr' },
	  "The date and time at which this record was created.",
	{ output => 'modified', com_name => 'dmd' },
	  "The date and time at which this record was last modified.");
    
    $ds->define_ruleset('1.1:common:select_ent' =>
	{ param => 'authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_block('1.1:common:ent' =>
	{ select => ['$bt.authorizer_no', '$bt.enterer_no', '$bt.modifier_no'], tables => '$bt' },
	{ output => 'authorizer_no', com_name => 'ati', if_block => 'ent,entname' },
	    "The identifier of the person who authorized the entry of this record",
	{ output => 'enterer_no', com_name => 'eni', if_block => 'ent,entname' },
	    "The identifier of the person who actually entered this record.",
	{ output => 'modifier_no', com_name => 'mdi', if_block => 'ent,entname' },
	    "The identifier of the person who last modified this record, if it has been modified.");
    
    $ds->define_block('1.1:common:entname' =>
	{ select => ['$bt.authorizer_no', '$bt.enterer_no', '$bt.modifier_no'], tables => '$bt' },
	{ set => 'authorizer', from => 'authorizer_no', lookup => \%PERSON_NAME, default => 'unknown' },
	{ set => 'enterer', from => 'enterer_no', lookup => \%PERSON_NAME, default => 'unknown' },
	{ set => 'modifier', from => 'modifier_no', lookup => \%PERSON_NAME, default => 'unknown' },
	{ output => 'authorizer', com_name => 'ath' },
	    "The name of the person who authorized the entry of this record",
	{ output => 'enterer', com_name => 'ent' },
	    "The name of the person who actually entered this record",
	{ output => 'modifier', com_name => 'mdf' },
	    "The name of the person who last modified this record, if it has been modified.");
    
    # Now fill in the %PERSON_NAME hash.
    
    my $dbh = $ds->get_connection;
    
    my $values = $dbh->selectcol_arrayref("SELECT person_no, reversed_name FROM person",
					  { Columns => [1, 2] });
    
    %PERSON_NAME = @$values;
}


# datetime_value ( )
# 
# Validate a date or date/time value.

my (%UNIT_MAP) = ( d => 'DAY', m => 'MINUTE', h => 'HOUR', w =>'WEEK', M => 'MONTH', Y => 'YEAR' );

sub datetime_value {
    
    my ($value, $context) = @_;
    
    my $dbh = $remember_ds->get_connection;
    my $quoted = $dbh->quote($value);
    my $clean;
    
    # If we were given a number of days/hours, then treat that as "ago".
    
    if ( $value =~ /^(\d+)([mhdwMY])$/xs )
    {
	if ( $2 eq 'm' || $2 eq 'h' )
	{
	    ($clean) = $dbh->selectrow_array("SELECT DATE_SUB(NOW(), INTERVAL $1 $UNIT_MAP{$2})");
	}
	
	else
	{
	    ($clean) = $dbh->selectrow_array("SELECT DATE_SUB(CURDATE(), INTERVAL $1 $UNIT_MAP{$2})");
	}
    }
    
    else {
	($clean) = $dbh->selectrow_array("SELECT CONVERT($quoted, datetime)");
    }
    
    if ( $clean )
    {
	return { value => "'$clean'" };
    }
    
    else
    {
	return { error => "the value of {param} must be a valid date or date/time as defined by the MySQL database (was {value})" };
    }
}


# generate_crmod_filters ( table_name )
# 
# Generate the proper filters to select records by date created/modified.

sub generate_crmod_filters {

    my ($self, $table_name, $tables_hash) = @_;
    
    my @filters;
    
    if ( my $dt = $self->clean_param('created_after') )
    {
	push @filters, "$table_name.created >= $dt";
    }
    
    if ( my $dt = $self->clean_param('created_before') )
    {
	push @filters, "$table_name.created < $dt";
    }
    
    if ( my $dt = $self->clean_param('modified_after') )
    {
	push @filters, "$table_name.modified >= $dt";
    }
    
    if ( my $dt = $self->clean_param('modified_before') )
    {
	push @filters, "$table_name.modified >= $dt";
    }
    
    $tables_hash->{$table_name} = 1 if @filters && ref $tables_hash eq 'HASH';
    
    return @filters;
}


# generate_ent_filters ( table_name )
# 
# Generate the proper filters to select records by authorizer/enterer/modifier
# name or number.

sub generate_ent_filters {
    
    my ($self, $table_name, $tables_hash) = @_;
    
    my @filters;
    
    # First go through the parameters and figure out if we have names or
    # identifiers.  Convert all names into identifiers.
    
    if ( my $value = $self->clean_param('authorized_by') )
    {
	push @filters, ent_filter($self, $table_name, 'authorizer_no', $value);
    }
    
    if ( my $value = $self->clean_param('entered_by') )
    {
	push @filters, ent_filter($self, $table_name, 'enterer_no', $value);
    }
    
    if ( my $value = $self->clean_param('modified_by') )
    {
	push @filters, ent_filter($self, $table_name, 'modifier_no', $value);
    }
    
    if ( my $value = $self->clean_param('touched_by') )
    {
	push @filters, ent_filter($self, $table_name, 'touched', $value);
    }
    
    $tables_hash->{$table_name} = 1 if @filters && ref $tables_hash eq 'HASH';
        
    return @filters;
}


sub ent_filter {
    
    my ($self, $tn, $param, $person_value) = @_;

    my $dbh = $self->get_connection;
    my @values = ref $person_value eq 'ARRAY' ? @$person_value : $person_value;
    my @ids;
    
    # Go through each of the names in the list.  Any names we find get looked
    # up in the database.
    
    foreach my $p ( @values )
    {
	if ( $p =~ /^\d+$/ )
	{
	    push @ids, $p;
	}
	
	else
	{
	    my $quoted = $dbh->quote("$p%");
	    my $values = $dbh->selectcol_arrayref("
		SELECT person_no, name FROM person
		WHERE name like $quoted or reversed_name like $quoted", { Columns => [1, 2] });
	    
	    if ( defined $values && @$values < 3 )
	    {
		push @ids, $values->[0];
	    }
	    
	    elsif ( defined $values )
	    {
		my @ambiguous;
		
		while ( @$values )
		{
		    shift @$values;
		    push @ambiguous, "'" . shift(@$values) . "'";
		}
		
		my $list = join(', ', @ambiguous);
		$self->add_warning("Ambiguous name: '$p' could match any of the following names: $list");
	    }
	    
	    else
	    {
		$self->add_warning("Unknown name: '$p' is not a name known to this database");
	    }
	}
    }
    
    # Now generate a filter expression using the ids.  If we have no
    # identifiers, return a string which will select nothing.  This is the
    # proper response because the client clearly wanted to filter by identifier.
    
    my $id_list = join(',', @ids);
    return "$tn.authorizer_no = 0" unless $id_list;
    
    if ( $param eq 'touched' )
    {
	return "($tn.authorizer_no in ($id_list) or $tn.enterer_no in ($id_list) or $tn.modifier_no in ($id_list))";
    }
    
    else
    {
	return "$tn.$param in ($id_list)";
    }
}


# generateAttribution ( )
# 
# Generate an attribution string for the given record.  This relies on the
# fields "a_al1", "a_al2", "a_ao", and "a_pubyr".

sub generateAttribution {

    my ($self, $row) = @_;
    
    my $auth1 = $row->{a_al1} || '';
    my $auth2 = $row->{a_al2} || '';
    my $auth3 = $row->{a_ao} || '';
    my $pubyr = $row->{a_pubyr} || '';
    
    $auth1 =~ s/( Jr)|( III)|( II)//;
    $auth1 =~ s/\.$//;
    $auth1 =~ s/,$//;
    $auth2 =~ s/( Jr)|( III)|( II)//;
    $auth2 =~ s/\.$//;
    $auth2 =~ s/,$//;
    
    my $attr_string = $auth1;
    
    if ( $auth3 ne '' or $auth2 =~ /et al/ )
    {
	$attr_string .= " et al.";
    }
    elsif ( $auth2 ne '' )
    {
	$attr_string .= " and $auth2";
    }
    
    $attr_string .= " $pubyr" if $pubyr ne '';
    
    if ( $attr_string ne '' )
    {
	$attr_string = "($attr_string)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && defined $row->{taxon_no} && 
		$row->{orig_no} != $row->{taxon_no};
	
	return $attr_string;
    }
    
    return;
}


1;
