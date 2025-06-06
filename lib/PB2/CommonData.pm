# 
# CommonData.pm
# 
# A class that contains common routines for formatting and processing PBDB data.
# 
# Author: Michael McClennen

package PB2::CommonData;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(croak);
use TableDefs qw(%TABLE);
use ExternalIdent qw(extract_identifier generate_identifier);

use Moo::Role;

our (%PERSON_NAME);

our ($COMMON_OPT_RE) = qr{ ^ (?: ( taxa | ops | refs | occs | specs | colls ) _ )?
			     ( created_before | created_after | 
			       modified_before | modified_after |
			       authorized_by | entered_by | modified_by |
			       authent_by | touched_by ) $ }xs;


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $ds->define_ruleset('1.2:special_params' => 
	"The following parameters can be used with most requests:",
	{ optional => 'SPECIAL(limit)' },
	{ optional => 'SPECIAL(offset)' },
	{ optional => 'SPECIAL(count)' },
	{ optional => 'SPECIAL(datainfo)' },
	{ optional => 'private', valid => FLAG_VALUE },
	    "If this parameter is included in a request, and if you are logged in to",
	    "the Paleobiology Database using the same browser in which the request",
	    "is made, then the result will include any private (embargoed) data which",
	    "matches the request parameters and which you have permission to access.",
	    "This includes not only your own data but also data whose authorizer has",
	    "permitted you to edit their collections.",
	{ optional => 'strict', valid => FLAG_VALUE },
	    "If this parameter is specified, then any warnings will result in an error response.",
	    "You can use this parameter to make sure that all of your parameters",
	    "have proper values.  Otherwise, by default, the result will be",
	    "generated using good values and ignoring bad ones.",
	{ optional => 'textresult', valid => FLAG_VALUE },
	    "If specified, then the result will be given a content type of 'text/plain'.",
	    "With most browsers, that will cause the result to be displayed directly",
	    "instead of saved to disk.  This parameter does not need any value.",
	{ optional => 'markrefs', valid => FLAG_VALUE },
	    "If specified, then formatted references will be marked up with E<lt>bE<gt> and E<lt>iE<gt> tags.",
	    "This parameter does not need a value.",
	{ optional => 'extids', valid => FLAG_VALUE },
	    "If specified, then record identifiers will be output with a record type prefix rather than",
	    "as numbers.  This is done by default for the JSON format.",
	{ optional => 'SPECIAL(vocab)' },
	{ optional => 'SPECIAL(save)' },
	">>The following parameters are only relevant to the text formats (.csv, .tsv, .txt):",
	{ optional => 'noheader', valid => FLAG_VALUE },
	    "If specified, then the header line which gives the field names is omitted.",
	    "This parameter does not need any value.  It is equivalent to \"header=no\".",
	{ optional => 'SPECIAL(linebreak)', alias => 'lb' },
	{ optional => 'SPECIAL(header)' },
	{ ignore => 'splat' });
    
    $ds->define_ruleset('1.2:common:select_crmod' =>
	{ param => 'created_before', valid => \&datetime_value },
	    "Select only records that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'created_after', valid => \&datetime_value, alias => 'created_since' },
	    "Select only records that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'modified_before', valid => \&datetime_value },
	    "Select only records that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'modified_after', valid => \&datetime_value, alias => 'modified_since' },
	    "Select only records that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_taxa_crmod' =>
	{ param => 'taxa_created_before', valid => \&datetime_value },
	    "Select only records associated with taxa that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'taxa_created_after', valid => \&datetime_value, alias => 'taxa_created_since' },
	    "Select only records associated with taxa that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'taxa_modified_before', valid => \&datetime_value },
	    "Select only records associated with taxa that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'taxa_modified_after', valid => \&datetime_value, alias => 'taxa_modified_since' },
	    "Select only records associated with taxa that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_ops_crmod' =>
	{ param => 'ops_created_before', valid => \&datetime_value },
	    "Select only records associated with taxa that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'ops_created_after', valid => \&datetime_value, alias => 'ops_created_since' },
	    "Select only records associated with taxa that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'ops_modified_before', valid => \&datetime_value },
	    "Select only records associated with taxa that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'ops_modified_after', valid => \&datetime_value, alias => 'ops_modified_since' },
	    "Select only records associated with taxa that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_refs_crmod' =>
	{ param => 'refs_created_before', valid => \&datetime_value },
	    "Select only records associated with references that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'refs_created_after', valid => \&datetime_value, alias => 'refs_created_since' },
	    "Select only records associated with references that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'refs_modified_before', valid => \&datetime_value },
	    "Select only records associated with references that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'refs_modified_after', valid => \&datetime_value, alias => 'refs_modified_since' },
	    "Select only records associated with references that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_occs_crmod' =>
	{ param => 'occs_created_before', valid => \&datetime_value },
	    "Select only records associated with occurrences that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'occs_created_after', valid => \&datetime_value, alias => 'occs_created_since' },
	    "Select only records associated with occurrences that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'occs_modified_before', valid => \&datetime_value },
	    "Select only records associated with occurrences that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'occs_modified_after', valid => \&datetime_value, alias => 'occs_modified_since' },
	    "Select only records associated with occurrences that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_specs_crmod' =>
	{ param => 'specs_created_before', valid => \&datetime_value },
	    "Select only records associated with occurrences that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'specs_created_after', valid => \&datetime_value, alias => 'specs_created_since' },
	    "Select only records associated with occurrences that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'specs_modified_before', valid => \&datetime_value },
	    "Select only records associated with occurrences that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'specs_modified_after', valid => \&datetime_value, alias => 'specs_modified_since' },
	    "Select only records associated with occurrences that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_ruleset('1.2:common:select_colls_crmod' =>
	{ param => 'colls_created_before', valid => \&datetime_value },
	    "Select only records associated with collections that were created before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'colls_created_after', valid => \&datetime_value, alias => 'colls_created_since' },
	    "Select only records associated with collections that were created on or after the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'colls_modified_before', valid => \&datetime_value },
	    "Select only records associated with collections that were last modified before the specified L<date or date/time|/data1.2/datetime>.",
	{ param => 'colls_modified_after', valid => \&datetime_value, alias => 'colls_modified_since' },
	    "Select only records associated with collections that were modified on or after the specified L<date or date/time|/data1.2/datetime>.");
    
    $ds->define_block('1.2:common:crmod' =>
	{ select => ['$cd.created', '$cd.modified'], tables => '$cd' },
	{ output => 'created', com_name => 'dcr' },
	  "The date and time at which this record was created.",
	{ output => 'modified', com_name => 'dmd' },
	  "The date and time at which this record was last modified.",
	{ output => 'updated', com_name => 'dud' },
	  "The date and time at which this record was last updated, if it has been updated");
    
    $ds->define_ruleset('1.2:common:select_ent' =>
	{ param => 'authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were authorized by the specified person,",
	    "indicated by name or identifier<",
	{ param => 'entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_taxa_ent' =>
	{ param => 'taxa_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'taxa_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'taxa_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'taxa_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'taxa_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_ops_ent' =>
	{ param => 'ops_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'ops_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'ops_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'ops_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'ops_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with taxa that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_refs_ent' =>
	{ param => 'refs_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with references that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'refs_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with references that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'refs_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with references that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'refs_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with references that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'refs_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with references that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_occs_ent' =>
	{ param => 'occs_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'occs_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'occs_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'occs_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'occs_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_specs_ent' =>
	{ param => 'specs_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'specs_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'specs_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'specs_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'specs_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with occurrences that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
    
    $ds->define_ruleset('1.2:common:select_colls_ent' =>
	{ param => 'colls_authorized_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with collections that were authorized by the specified person,",
	    "indicated by name or identifier",
	{ param => 'colls_entered_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with collections that were entered by the specified person,",
	    "indicated by name or identifier",
	{ param => 'colls_modified_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with collections that were modified by the specified person,",
	    "indicated by name or identifier",
	{ param => 'colls_touched_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with collections that were either authorized, entered or modified by",
	    "the specified person, indicated by name or identifier",
	{ param => 'colls_authent_by', valid => ANY_VALUE, list => ',' },
	    "Select only records associated with collections that were authorized or entered by the specified",
	    "the specified person, indicated by name or identifier");
        
    $ds->define_block('1.2:common:ent' =>
	{ select => ['$cd.authorizer_no', '$cd.enterer_no', '$cd.modifier_no'], tables => '$cd' },
	{ set => 'authorizer_extid', from => 'authorizer_no', code => \&generate_person_id,
	  not_vocab => 'pbdb' },
	{ set => 'authorizer_extid', from => 'authorizer_no', if_vocab => 'pbdb' },
	{ set => 'enterer_extid', from => 'enterer_no', code => \&generate_person_id,
	  not_vocab => 'pbdb' },
	{ set => 'enterer_extid', from => 'enterer_no', if_vocab => 'pbdb' },
	{ set => 'modifier_extid', from => 'modifier_no', code => \&generate_person_id, 
	  not_vocab => 'pbdb' },
	{ set => 'modifier_extid', from => 'modifier_no', if_vocab => 'pbdb' },
	{ set => 'updater_extid', from => 'updater_no', code => \&generate_person_id,
	  not_vocab => 'pbdb' },
	{ set => 'updater_extid', from => 'updater_no', if_vocab => 'pbdb' },
	{ output => 'authorizer_extid', com_name => 'ati', pbdb_name => 'authorizer_no', 
	  if_block => 'ent,entname' },
	    "The identifier of the person who authorized the entry of this record",
	{ output => 'enterer_extid', com_name => 'eni', pbdb_name => 'enterer_no', 
	  if_block => 'ent,entname' },
	    "The identifier of the person who actually entered this record.",
	{ output => 'modifier_extid', com_name => 'mdi', pbdb_name => 'modifier_no', 
	  if_block => 'ent,entname' },
	    "The identifier of the person who last modified this record, if it has been modified.",
	{ output => 'updater_extid', com_name => 'udi', pbdb_name => 'updater_no',
	  if_block => 'ent,entname' },
	    "The identifier of the person or process who last updated this record, if it has been updated.");
    
    $ds->define_block('1.2:common:entname' =>
	{ select => ['$cd.authorizer_no', '$cd.enterer_no', '$cd.modifier_no'], tables => '$cd' },
	# { set => 'authorizer', from => 'authorizer_no', lookup => \%PERSON_NAME, default => 'unknown' },
	# { set => 'enterer', from => 'enterer_no', lookup => \%PERSON_NAME, default => 'unknown' },
	# { set => 'modifier', from => 'modifier_no', lookup => \%PERSON_NAME },
	{ set => '*', code => \&process_entnames },
	{ output => 'authorizer', com_name => 'ath' },
	    "The name of the person who authorized the entry of this record",
	{ output => 'enterer', com_name => 'ent' },
	    "The name of the person who actually entered this record",
	{ output => 'modifier', com_name => 'mdf' },
	    "The name of the person who last modified this record, if it has been modified.",
	{ output => 'updater', com_name => 'udf' },
	    "The name of the person or process who last updated this record, if it has been updated.");
    
    $ds->define_block('1.2:common:ent_guest' =>
    	{ select => ['$cd.enterer_id'] },
    	{ set => '*', code => \&process_enterer_id });
    
    $ds->define_ruleset('1.2:common:archive_params' =>
	{ param => 'archive_title', valid => ANY_VALUE },
	    "A value for this parameter directs that an archive be created",
	    "with the specified title unless one already exists.",
	{ param => 'archive_replace', valid => BOOLEAN_VALUE },
	    "A true value for this parameter directs that if there is an",
	    "existing archive with the specified title then its contents",
	    "should be replaced by the output of the current request.",
	    "Without this parameter, the error code E_EXISTING will be returned",
            "if the specified title matches an existing record. If the existing",
	    "record has a DOI, the error E_IMMUTABLE will be returned and this",
	    "cannot be overridden.",
	{ param => 'archive_authors', valid => ANY_VALUE },
	    "The value of this parameter, if any, is put into the 'authors'",
	    "field of the created or updated archive record.",
	{ param => 'archive_desc', valid => ANY_VALUE },
	    "The value of this parameter, if any, is put into the 'description'",
	    "field of the created or updated archive record.");
    
    # $ds->define_block('1.2:common:entname_guest' =>
    # 	{ select => ['$cd.enterer_id', 'wu.real_name'], tables => 'wu' },
    # 	{ set => '*', code => \&process_entnames },
    # 	{ output => 'enterer', com_name => 'ent' },
    # 	    "The name of the person who actually entered this record");
    
    # Now fill in the %PERSON_NAME hash.
    
    my $dbh = $ds->get_connection;
    
    my $values = $dbh->selectcol_arrayref("SELECT person_no, name FROM $TABLE{PERSON_DATA}",
					  { Columns => [1, 2] });
    
    %PERSON_NAME = @$values;
}


sub update_person_name_cache { 
    
    my ($class, $ds) = @_;
    
    my $dbh = $ds->get_connection;
    
    my $values = $dbh->selectcol_arrayref("SELECT person_no, name FROM $TABLE{PERSON_DATA}",
					  { Columns => [1, 2] });
    
    my %new_names = @$values;

    foreach my $k (keys %new_names)
    {
	$PERSON_NAME{$k} = $new_names{$k};
    }

    print STDERR "Updating PERSON_NAME cache\n\n" if $ds->debug;
}


# datetime_value ( )
# 
# Validate a date or date/time value.

my (%UNIT_MAP) = ( d => 'DAY', m => 'MINUTE', h => 'HOUR', w =>'WEEK', M => 'MONTH', Y => 'YEAR' );

sub datetime_value {
    
    my ($value, $context) = @_;
    
    my $dbh = $PBData::ds2->get_connection;
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
    
    else
    {
	$quoted = "\"$value-01-01\"" if $value =~ /^\d\d\d\d$/;
	
	($clean) = $dbh->selectrow_array("SELECT CONVERT($quoted, datetime)");
    }
    
    if ( $clean )
    {
	return { value => "$clean" };
    }
    
    else
    {
	return { error => "the value of {param} must be a valid date or date/time as defined by the MySQL database (was {value})" };
    }
}


# generate_common_filters ( table_short, select_table, tables_hash )
# 
# 

sub generate_common_filters {
    
    my ($request, $select_table, $tables_hash) = @_;
    
    my $dbh = $request->get_connection;
    
    my @params = $request->param_keys();
    my @filters;
    
    foreach my $key ( @params )
    {
	next unless $key =~ $COMMON_OPT_RE;
	
	my $prefix = $1 || 'bare';
	my $option = $2;
	
	next if defined $prefix && defined $select_table->{$prefix} && $select_table->{$prefix} eq 'ignore';
	
	my $value = $request->clean_param($key);
	next unless defined $value && $value ne '';    
	
	my $quoted; $quoted = $dbh->quote($value) unless ref $value;
	
	my $t = $select_table->{$prefix} || die "Error: bad common option prefix '$prefix'";
	
	$tables_hash->{$t} = 1 if ref $tables_hash;
	
	if ( $option eq 'created_after' )
	{
	    push @filters, "$t.created >= $quoted";
	}
	
	elsif ( $option eq 'created_before' )
	{
	    push @filters, "$t.created < $quoted";
	}
	
	elsif ( $option eq 'modified_after' )
	{
	    push @filters, "$t.modified >= $quoted";
	}
	
	elsif ( $option eq 'modified_before' )
	{
	    push @filters, "$t.modified < $quoted";
	}
	
	elsif ( $option eq 'authent_by' )
	{
	    push @filters, $request->ent_filter($t, 'authent', $value);
	}
	
	elsif ( $option eq 'authorized_by' )
	{
	    push @filters, $request->ent_filter($t, 'authorizer_no', $value);
	}
	
	elsif ( $option eq 'entered_by' )
	{
	    push @filters, $request->ent_filter($t, 'enterer_no', $value);
	}
	
	elsif ( $option eq 'modified_by' )
	{
	    push @filters, $request->ent_filter($t, 'modifier_no', $value);
	}
	
	elsif ( $option eq 'touched_by' )
	{
	    push @filters, $request->ent_filter($t, 'touched', $value);
	}
	
	else
	{
	    die "Error: bad common option '$option'";
	}
    }
    
    return @filters;
}


sub ent_filter {
    
    my ($request, $tn, $param, $person_value) = @_;
    
    my $dbh = $request->get_connection;
    my @values = ref $person_value eq 'ARRAY' ? @$person_value : $person_value;
    my @ids;
    my $exclude;
    my $exclude_all;
    my $all_except;
    my $select_all;
    my $select_different;
    
    # If the value is '@' or '!@', generate a difference filter.
    
    if ( $values[0] =~ qr{ ^ ([!])? \s* [@] \s* (.*) $ }xs )
    {
	die $request->exception(400, "You may only use '\@' alone or with '!'")
	    if (defined $2 && $2 ne '') || @values > 1;
	
	$select_different = 1;
	$exclude = 1 if defined $1 && $1 eq '!';
	@values = ();
    }
    
    # If the first value starts with '!', generate an exclusion filter.  If there were no other
    # values, set select_all to true.
    
    elsif ( $values[0] =~ qr{ ^ ! \s* (.*) }xs )
    {
	$values[0] = $1;
	$exclude = 1;
    }
    
    # If the first value starts with '%!', generate an "all except" filter.
    
    elsif ( $values[0] =~ qr{ ^ %! \s* (.*) }xs )
    {
	$values[0] = $1;
	$all_except = 1;
    }
    
    # Go through each of the names in the list.  Any names we find get looked
    # up in the database.
    
    foreach my $p ( @values )
    {
	next unless defined $p && $p ne '';
	
	if ( $p =~ /\d/ )
	{
	    if ( $p =~ /^\d+$/ )
	    {
		push @ids, $p;
	    }
	    
	    elsif ( my $id = extract_identifier('ANY', $p ) )
	    {
		if ( $id->{type} eq 'prs' )
		{
		    push @ids, $id->{num};
		}
		
		else
		{
		    $request->add_warning("Bad identifier '$p': you may only use identifiers of type 'prs' with parameter '$param'");
		}
	    }
	    
	    else
	    {
		$request->add_warning("Bad identifier '$p': must be an identifier of the form 'prs:nnnn' or a positive integer");
	    }
	}
	
	elsif ( $p eq '%' )
	{
	    $select_all = 1;
	}
	
	elsif ( $p =~ /@/ )
	{
	    die $request->exception(400, "You may only use '\@' alone or with '!'");
	}
	
	else
	{
	    my $quoted = $dbh->quote("$p%");
	    my $values = $dbh->selectcol_arrayref("
		SELECT person_no, name FROM $TABLE{PERSON_DATA}
		WHERE name like $quoted or reversed_name like $quoted", { Columns => [1, 2] });
	    
	    if ( defined $values && @$values < 3 && defined $values->[0] && $values->[0] ne '' )
	    {
		push @ids, $values->[0];
	    }
	    
	    elsif ( defined $values && defined $values->[0] && $values->[0] ne '' )
	    {
		my @ambiguous;
		
		while ( @$values )
		{
		    shift @$values;
		    push @ambiguous, "'" . shift(@$values) . "'";
		}
		
		my $list = join(', ', @ambiguous);
		$request->add_warning("Ambiguous name: '$p' could match any of the following names: $list");
	    }
	    
	    else
	    {
		$request->add_warning("Unknown name: '$p' is not a name known to this database");
	    }
	}
    }
    
    # If the paramter $tn is 'id_list', then return the list of identifiers directly.  If the
    # exclusion flag was found, prepend it to the first item in the list.  If no valid identifiers
    # were specified, then use the value '-1' which will select nothing.  This is the proper
    # response because the client clearly wanted to filter by identifier.  If any of the various
    # flags were set, return a special value.
    
    if ( $tn eq 'id_list' )
    {
	# If $select_all or $select_different is set, return a special value.
	
	if ( $select_all )
	{
	    return $exclude ? [ 0 ] : [ '_ANY_' ];
	}
	
	if ( $select_different )
	{
	    return $exclude ? [ '_SAME_' ] : [ '_DIFFERENT_' ];
	}
	
	# If we do not have any valid identifiers, return a special value.
	
	unless ( @ids )
	{
	    # If $exclude is set, then select only records with no value for this parameter.
	    
	    if ( $exclude )
	    {
		return [ 0 ];
	    }
	    
	    # If $all_except is set, then select only records with a value for this parameter,
	    # just as if $select_all were set.
	    
	    if ( $all_except )
	    {
		return [ '_ANY_' ];
	    }
	    
	    # Otherwise, return the value -1 which will select an empty result set.  Since it is
	    # clear that the user wanted to select only records with a specified value for this
	    # parameter, and no valid values were given, an empty result set is the only
	    # reasonable result.
	    
	    return [ -1 ];
	}
	
	# If the exclusion or all except flag was found, prepend it to the list.
	
	if ( $exclude )
	{
	    unshift @ids, '_EXCLUDE_';
	}
	
	elsif ( $all_except )
	{
	    unshift @ids, '_ANY_EXCEPT_';
	}
	
	# Now return the list.
	
	return \@ids;
    }
    
    # Otherwise, generate an SQL filter expression using the ids and the value of $tn as the table
    # name.  The first step is to join the ids together into a list.
    
    my $id_list = join(',', @ids);
    
    # If $select_all is true, then return an expression that will select any value or no value.
    
    if ( $select_all )
    {
	if ( $param eq 'modifier_no' or $param eq 'authorizer_no' or $param eq 'enterer_no' )
	{
	    my $op = $exclude ? '=' : '<>';
	    return "$tn.$param $op 0";
	}
	
	else
	{
	    return "1=1";
	}
    }
    
    # If $select_different is true, then return an expression that will select a difference.
    
    if ( $select_different )
    {
	my $op = $exclude ? '=' : '<>';
	
	if ( $param eq 'authorizer_no' or $param eq 'enterer_no' or $param eq 'authent' )
	{
	    return "$tn.authorizer_no $op $tn.enterer_no";
	}
	
	elsif ( $param eq 'modifier_no' )
	{
	    if ( $exclude )
	    {
		return "($tn.modifier_no = $tn.enterer_no or $tn.modifier_no = $tn.authorizer_no" .
		    " or $tn.modifier_no = 0)";
	    }
	    
	    else
	    {
		return "$tn.modifier_no <> $tn.enterer_no and $tn.modifier_no <> $tn.authorizer_no" .
		    " and $tn.modifier_no <> 0"
	    }
	}
	
	else
	{
	    return "1=1";
	}
    }
    
    # Otherwise, if no valid ids were found, return an expression that will select the appropriate
    # set given the other flags.
    
    unless ( @ids )
    {
	# If $exclude is set, then select only records with no value for this parameter.
	
	if ( $exclude )
	{
	    $param = 'authorizer_no' if $param eq 'authent' || $param eq 'touched';
	    return "$tn.$param = 0";
	}
	
	# If $all_except is set, then select only records with a value for this parameter,
	# just as if $select_all were set.
	
	if ( $all_except )
	{
	    $param = 'authorizer_no' if $param eq 'authent' || $param eq 'touched';
	    return "$tn.$param <> 0";
	}
	
	# Otherwise, return an expression which will select an empty result set.  Since it is
	# clear that the user wanted to select only records with a specified value for this
	# parameter, and no valid values were given, an empty result set is the only
	# reasonable result.
	    
	return "$tn.authorizer_no = -1";
    }
    
    # Otherwise, return the proper expression.
    
    my $op = $exclude || $all_except ? 'not ' : '';
    
    if ( $param eq 'touched' )
    {
	return "$op($tn.authorizer_no in ($id_list) or $tn.enterer_no in ($id_list) or $tn.modifier_no in ($id_list))";
    }
    
    elsif ( $param eq 'authent' )
    {
	return "$op($tn.authorizer_no in ($id_list) or $tn.enterer_no in ($id_list))";
    }
    
    else
    {
	my $ex = $all_except ? " and $tn.$param <> 0" : '';
	return "$tn.$param ${op}in ($id_list)$ex";
    }
}


# process_enterer_id ( )
# 
# If the record contains an enterer_id value and no enterer_no value, set the enterer_no from the
# enterer_id.

sub process_enterer_id {
    
    my ($request, $record) = @_;
    
    if ( $record->{enterer_id} && ! $record->{enterer_no} )
    {
	$record->{enterer_no} = $record->{enterer_extid} = $record->{enterer_id};
    }
}


# process_entnames ( )
#
# Generate enterer names from ids, when available.

sub process_entnames {

    my ($request, $record) = @_;

    if ( $record->{authorizer_no} )
    {
	$record->{authorizer} = $PERSON_NAME{$record->{authorizer_no}} || 'unknown';
    }

    if ( $record->{modifier_no} )
    {
	$record->{modifier} = $PERSON_NAME{$record->{modifier_no}} || 'unknown';
    }
    
    if ( $record->{updater_no} )
    {
	$record->{updater} = $PERSON_NAME{$record->{updater_no}} || 'unknown';
    }
    
    if ( $record->{enterer_no} && $record->{enterer_no} =~ /^\d+$/ )
    {
	$record->{enterer} = $PERSON_NAME{$record->{enterer_no}} || 'unknown';
    }

    elsif ( $record->{enterer_id} )
    {
	if ( $PERSON_NAME{$record->{enterer_id}} )
	{
	    $record->{enterer} = $PERSON_NAME{$record->{enterer_id}};
	}

	else
	{
	    my $dbh = $request->get_connection;
	    my $quoted_id = $dbh->quote($record->{enterer_id});
	    my ($name) = $dbh->selectrow_array("SELECT real_name FROM $TABLE{WING_USERS} WHERE id = $quoted_id");

	    $name ||= 'unknown';

	    $record->{enterer} = $PERSON_NAME{$record->{enterer_id}} = $name;
	}
    }
}


# generateAttribution ( )
# 
# Generate an attribution string for the given record.  This relies on the
# fields "a_al1", "a_al2", "a_ao", and "a_pubyr".

sub generateAttribution {

    my ($request, $row) = @_;
    
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


# safe_param_list ( param_name, bad_value )
# 
# If the specified parameter was not given, return undefined.  If it was given
# one or more good values, return a list of them.  If it was given only bad
# values, return $bad_value (or default to -1).

sub safe_param_list {
    
    my ($request, $param, $bad_value) = @_;
    
    unless ( $request->param_given($param) )
    {
	return;
    }
    
    my @values = grep { $_ && $_ ne '' } $request->clean_param_list($param);
    
    unless ( @values )
    {
	push @values, ($bad_value // -1);
    }
    
    return @values;
}


# generate_person_id ( person_no )
# 
# Return an external person identifier given a person_no value.

sub generate_person_id {
    
    my ($request, $person_no) = @_;
    
    if ( $request->{block_hash}{extids} && $person_no && $person_no =~ /^\d+$/ )
    {
	return generate_identifier('PRS', $person_no);
    }
    
    elsif ( $person_no )
    {
	return $person_no;
    }
    
    else
    {
	return;
    }
}


# check_values ( value_list, value_field, value_table, error_msg )
# 
# 

sub check_values {
    
    my ($request, $dbh, $value_list, $value_field, $value_table, $error_msg) = @_;
    
    # Start by checking for an empty value list.  If the
    # value list is already -1, then we know that all values given in the
    # request were already determined to be bad.
    
    my $id_list = join(q{,}, @$value_list);
    return '-1' unless $id_list && $id_list ne '-1';
    
    # Check for invalid identifiers.
    
    my %id_hash = map { $_ => 1 } @$value_list;
    
    my $check_result = $dbh->selectcol_arrayref("
	SELECT DISTINCT $value_field FROM $value_table WHERE $value_field in ($id_list)");
    
    foreach my $id ( @$check_result )
    {
	delete $id_hash{$id};
    }
    
    foreach my $id ( keys %id_hash )
    {
	my $msg = $error_msg;
	$msg =~ s/%/$id/;
	
	$request->add_warning($msg);
    }
    
    @$value_list = @$check_result;
    
    my $result = join(q{,}, @$check_result) || '-1';
    return $result;
}


# generate_match_like ( string_list )
# 

sub generate_match_like {

    my ($request, $dbh, $field, $match_list) = @_;
    
    my @exprs;
    
    foreach my $n ( @$match_list )
    {
	next unless defined $n && $n ne '';
	
	my $quoted = $dbh->quote($n);
	
	push @exprs, "$field like $quoted";
    }
    
    push @exprs, '_MATCH_NOTHING_' unless @exprs;
    
    if ( @exprs == 1 )
    {
	return @exprs;
    }
    
    else
    {
	return '(' . join(' or ', @exprs) . ')';
    }
}


# generate_match_regex ( string_list )
# 
# Convert the specified list of strings into a regex

sub generate_match_regex {
    
    my ($request, $dbh, $field, $match_list) = @_;
    
    my @exprs;
    
    foreach my $n ( @$match_list )
    {
	next unless defined $n && $n ne '';
	
	$n =~ s/([(){}])/\\$1/g;
	$n =~ s/\s+/\\s+/g;
	$n =~ s/%/.*/g;
	$n =~ s/_/./g;
	
	push @exprs, $n;
    }
    
    push @exprs, '_MATCH_NOTHING_' unless @exprs;
    
    my $regex = $dbh->quote(join('|', @exprs));
    
    return "$field rlike $regex";
}


sub generate_match_list {
    
    my ($request, $dbh, $field, $match_list) = @_;
    
    my @list;
    
    foreach my $n ( @$match_list )
    {
	next unless defined $n && $n ne '';
	
	push @list, $dbh->quote($n);
    }
    
    push @list, '_MATCH_NOTHING_' unless @list;
    
    return "$field in (" . join(q{,}, @list) . ")";
}


# stict_check ( )
# 
# If the special parameter 'strict' was specified, and if any warnings have
# been generated for this request, then return an error.

sub strict_check {
    
    my ($request) = @_;
    
    my @warnings = $request->warnings;
    
    if ( $request->clean_param('strict') && @warnings )
    {
	my $code = '404';
	my $message = @warnings > 1 ? 'Not found' : $warnings[0];
	
	foreach my $w (@warnings)
	{
	    if ( $w !~ qr{unknown taxon|did not match|not known to the database}i )
	    {
		$code = '400';
		$message = @warnings > 1 ? 'Bad parameter values' : $warnings[0];
	    }
	}
	
	if ( @warnings == 1 )
	{
	    $request->{warnings} = [ ];
	}
	
	die $request->exception( $code, $message );
    }
}


# extid_check ( )
# 
# If we are supposed to use external ids, add the block 'extid' to the request.

sub extid_check {
    
    my ($request) = @_;

    my $default_extids = $request->output_vocab =~ /^com$|^bibjson$/;
    
    if ( $request->clean_param('extids') || ( ! $request->param_given('extids') &&
					      $default_extids ) )
    {
	$request->{block_hash}{extids} = 1;
	$request->delete_output_field('record_type');
    }
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
}


1;
