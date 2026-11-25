# 
# The Paleobiology Database
# 
#   SpecimenEdit.pm - a class for handling updates to fossil specimen records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting fossil specimen records.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = SpecimenEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record(), etc.


package SpecimenEdit;

use strict;

use Carp qw(carp croak);

use TableDefs qw(%TABLE get_table_property);
use CoreTableDefs;
# [Removed by Claude under the guidance of Michael McClennen: 'use ExternalIdent qw(%IDRE)'. It was
# used to match external identifier strings such as 'col:100', but callers are expected to
# turn external identifiers into PBDB::ExtIdent objects before passing them to this module.]

# [Removed by Claude under the guidance of Michael McClennen: 'use Taxonomy'. Taxon names are
# now matched with matchIdentifiedName and matchExactName from OccurrenceBase.pm.]

# [Added by Claude under the guidance of Michael McClennen: used to create the occurrence of a
# specimen and to keep the matrices up to date.]

use OccurrenceBase qw(splitTaxonName parseIdentifiedName matchIdentifiedName matchExactName
		      defaultSpeciesName);
use MatrixBase qw(updateOccurrenceMatrix deleteFromOccurrenceMatrix updateOccurrenceCounts
		  updateSpecimenMatrix deleteFromSpecimenMatrix);

use parent 'EditTransaction';

# [Changed by Claude under the guidance of Michael McClennen: the MariaDB and PaleoBioDB
# roles were not included, so the default permission checks allowed everything and the
# special columns were not handled]

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

# The name fields of an occurrence. [Added by Claude under the guidance of Michael McClennen]

our (@NAME_FIELDS) = qw(genus_name genus_reso subgenus_name subgenus_reso
			species_name species_reso subspecies_name subspecies_reso);



{
    SpecimenEdit->register_conditions(
	C_UNKNOWN_TAXON => "The specified taxon name is not in the database. Add 'allow=UNKNOWN_TAXON' to continue.",
	E_MISSING_IDENTIFICATION => "A specimen record requires either 'taxon_name', 'taxon_id' or 'occurrence_id'.",
	E_UNREGISTERED_TAXON => "A specimen record requires either 'taxon_id' or a 'taxon_name' that is known to the database, unless it is tied to an existing collection or occurrence.",
	E_CANNOT_CHANGE => "The field '&1' cannot be modified once created. Delete the specimen and add another.",
	E_AMBIGUOUS => "Field 'taxon_name' matches more than one taxon in the database");	# [Added by Claude under the guidance of Michael McClennen]
    
    SpecimenEdit->register_allowances('UNKNOWN_TAXON');
}



# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( action, operation, table )

# This method is called from EditTransaction.pm to validate each insert and update action.
# We override it to do additional checks before calling validate_against_schema.
#
# [Changed by Claude under the guidance of Michael McClennen: the key expression is taken
# from the action. It was read from a fourth argument, which is never passed.]

sub validate_action {
    
    my ($edt, $action, $operation, $table) = @_;

    # call the appropriate routine to validate this action.

    if ( $table eq 'SPECIMEN_DATA' )
    {
	return $edt->validate_specimen($action, $operation);
    }

    elsif ( $table eq 'MEASUREMENT_DATA' )
    {
	return $edt->validate_measurement($action, $operation);
    }

    else
    {
	croak "invalid table '$table'";
    }
}


# Validate the specimen record that has been submitted for insertion or update.

sub validate_specimen {
    
    my ($edt, $action, $operation) = @_;
    
    my $record = $action->record;
    my $dbh = $edt->dbh;
    
    # [Changed by Claude under the guidance of Michael McClennen: the E_PARAM conditions below
    # pass the field name and the message separately. The message was passed as the field
    # name, so it was displayed as "Field '...'".]
    
    # If we have a taxon name rather than a taxon id, look it up and see if it is already in the database.
    # 
    # [Changed by Claude under the guidance of Michael McClennen: the name is parsed with
    # parseIdentifiedName and matched with match_taxon_name below, as CollectionEdit does for
    # occurrences. It was looked up with Taxonomy->resolve_names, which does not recognize
    # names with modifiers such as 'cf.'. The parsed name is stored in the action attribute
    # 'parsed_name', so that validate_collection can use it for the specimen's occurrence.
    # The taxon_id is the taxon_no of the matching name; it was the orig_no.]
    
    if ( $record->{taxon_name} )
    {
	$action->ignore_field('taxon_name');
	
	if ( $record->{taxon_id} )
	{
	    $edt->add_condition('E_PARAM', 'taxon_name',
				"you may not specify both 'taxon_name' and 'taxon_id' in the same record");
	}
	
	else
	{
	    OccurrenceBase::initializeModifiers($dbh);
	    
	    my @fields = parseIdentifiedName($record->{taxon_name});
	    
	    if ( ref $fields[0] || ! $fields[0] )
	    {
		my $message = ref $fields[0] && $fields[0]{error} ||
		    "not a valid taxon name: '$record->{taxon_name}'";
		$edt->add_condition($action, 'E_FORMAT', 'taxon_name', $message);
	}
	
	else
	{
		my %parsed;
		@parsed{@NAME_FIELDS} = map { $_ // '' } @fields;
	    
		$action->set_attr('parsed_name', \%parsed);
	    
		if ( my $taxon_no = $edt->match_taxon_name($action, \%parsed) )
	    {
		    $record->{taxon_id} = $taxon_no;
		$edt->debug_line("taxon_no: $record->{taxon_id}");
	    }
	}
    }
    }
    
    # Now we need to make sure that any inserted record has a taxonomic identification.
    
    if ( $operation eq 'insert' )
    {
	# If we have a taxon_id then we are okay. But not if occurrence_id was also specified.
	
	if ( $record->{taxon_id} )
	{
	    $edt->add_condition($action, 'E_PARAM', 'occurrence_id',
				"you may not specify both 'taxon_id' and 'occurrence_id' in the same record")
		if $record->{occurrence_id};
	}
	
	# If we have an occurrence_id then we are okay. But not if taxon_name was also specified.
	
	elsif ( $record->{occurrence_id} )
	{
	    $edt->add_condition($action, 'E_PARAM', 'occurrence_id',
				"you may not specify both 'taxon_name' and 'occurrence_id' in the same record")
		if $record->{taxon_name};
	    
	    # Add another error condition if both 'collection_id' and 'occurrence_id' are specified.
	    
	    $edt->add_condition($action, 'E_PARAM', 'occurrence_id',
				"you may not specify both 'collection_id' and 'occurrence_id' in the same record")
		if $record->{collection_id};
	}
	
	# If we have a collection_id and a taxon_name, then we are okay as long as 'UNKNOWN_TAXON' is
	# allowed.
	
	elsif ( $record->{collection_id} && $record->{taxon_name} )
	{
	    $edt->add_condition($action, 'C_UNKNOWN_TAXON', 'taxon_name') unless $edt->allows('UNKNOWN_TAXON');
	}
	
	# If we have just a taxon name but no collection_id, then we have an UNREGISTERED_TAXON error.
	
	elsif ( $record->{taxon_name} )
	{
	    $edt->add_condition($action, 'E_UNREGISTERED_TAXON');
	}

	# Otherwise, we don't have any taxonomic identification at all.

	else
	{
	    $edt->add_condition($action, 'E_MISSING_IDENTIFICATION');
	}
    }
    
    # If we are updating a specimen, we cannot change its taxonomic identification, its
    # collection_id, or its occurrence_id. To change any of these, the user must delete the
    # specimen and add another.
    # 
    # [Added by Claude under the guidance of Michael McClennen: the check on taxon_id and
    # taxon_name. A taxon_name was matched above to a known taxon, if there is one, and the
    # orig_no of that taxon is compared with the orig_no of the specimen's existing taxon. If they match, the
    # specimen keeps its existing taxon_no, which may be a different spelling of that taxon.]
    # 
    # [Added by Claude under the guidance of Michael McClennen: a replace must give the
    # specimen's identification, as an insert must, and repeat its taxon and occurrence. A
    # field that is left out of a replacement record is cleared, so leaving out the taxon or the
    # occurrence would change it. The occurrence may be repeated either with occurrence_id or
    # with the same collection_id, which keeps the existing occurrence.]
    #
    # [Changed by Claude under the guidance of Michael McClennen: the existing occurrence_no is
    # read using the action's key expression. This code called the nonexistent methods
    # 'get_old_values' and 'debug', and used a key expression that was never set. The field
    # name 'occurrence_id' was misspelled in the E_CANNOT_CHANGE condition.]
    
    elsif ( $operation eq 'update' || $operation eq 'replace' )
    {
	my $is_replace = $operation eq 'replace';
	
	if ( $is_replace && ! ( $record->{taxon_id} || $record->{taxon_name} ||
				$record->{occurrence_id} || $record->{collection_id} ) )
	{
	    $edt->add_condition($action, 'E_MISSING_IDENTIFICATION');
	}
	
	if ( $is_replace || exists $record->{taxon_id} || exists $record->{taxon_name} )
	{
	    my $keyexpr = $action->keyexpr;
	    
	    my ($old_taxon, $old_orig) = $dbh->selectrow_array("
		SELECT s.taxon_no, a.orig_no FROM $TABLE{SPECIMEN_DATA} as s
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE s.$keyexpr");
	    
	    if ( exists $record->{taxon_name} )
	    {
		my ($new_orig) = $record->{taxon_id} ? $dbh->selectrow_array("
		    SELECT orig_no FROM $TABLE{AUTHORITY_DATA}
		    WHERE taxon_no = $record->{taxon_id}") : ();
	    
		$new_orig ||= 0;
		
		if ( $new_orig && $old_orig && $new_orig eq $old_orig )
		{
		    $record->{taxon_id} = $old_taxon;
		}
		
		else
		{
		    $edt->add_condition($action, 'E_CANNOT_CHANGE', 'taxon_name');
		}
	    }
	    
	    else
	    {
		my $new = $record->{taxon_id} ? $record->{taxon_id} + 0 : 0;
		my $old = $old_taxon || 0;
	    
	    if ( $new ne $old )
	    {
		    $edt->add_condition($action, 'E_CANNOT_CHANGE', 'taxon_id');
		}
	    }
	    }
	    
	if ( $is_replace || exists $record->{occurrence_id} || exists $record->{collection_id} )
	{
	    my $keyexpr = $action->keyexpr;
	    
	    my ($occurrence_no) = $dbh->selectrow_array("
		SELECT occurrence_no FROM $TABLE{SPECIMEN_DATA} WHERE $keyexpr");
	    
	    $edt->debug_line("UPDATE occ = " . ($occurrence_no // '') . " / " .
			     ($record->{occurrence_id} // '') . "\n");
	    
	    my $same_collection;
	    
	    if ( exists $record->{collection_id} )
	    {
		my ($collection_no) = $occurrence_no ? $dbh->selectrow_array("
		SELECT collection_no FROM $TABLE{OCCURRENCE_DATA}
		    WHERE occurrence_no = $occurrence_no") : ();
		
		$edt->debug_line("UPDATE coll = " . ($collection_no // '') . " / " .
				 ($record->{collection_id} // '') . "\n");
		
		my $new = $record->{collection_id} ? $record->{collection_id} + 0 : 0;
		my $old = $collection_no || 0;
		
		if ( $new ne $old )
		{
		    $edt->add_condition($action, 'E_CANNOT_CHANGE', 'collection_id');
		}
		
		else
		{
		    $same_collection = 1;
		}
	    }
	    
	    # If a new occurrence_id is specified and there is an existing one, add an error
	    # condition. A replace that gives the same collection_id but no occurrence_id keeps
	    # the existing occurrence; one that gives neither would clear it.
	    
	    if ( exists $record->{occurrence_id} || $is_replace && ! $same_collection )
	    {
		my $new = $record->{occurrence_id} ? $record->{occurrence_id} + 0 : 0;
		my $old = $occurrence_no || 0;
		
		if ( $new ne $old )
		{
		    $edt->add_condition($action, 'E_CANNOT_CHANGE', 'occurrence_id');
		}
	    }
	    
	    elsif ( $is_replace && $occurrence_no )
	    {
		$record->{occurrence_no} = $occurrence_no;
	    }
	}
	
	# $$$ need to check old values and make sure no more than one of taxon, occ, coll are
	# specified.
    }
    
    # If we have a collection_id on an insert, check to make sure the collection exists, and
    # determine the occurrence that the new specimen will belong to. The occurrence is found
    # or created when the specimen is inserted.
    #
    # [Changed by Claude under the guidance of Michael McClennen: this is only done for
    # inserts. For an update, collection_id can only repeat the existing collection (see
    # above), and validate_collection would have counted the specimen a second time.]
    
    if ( $record->{collection_id} )
    {
	$edt->validate_collection($action, $record) if $operation eq 'insert';

	# We need to ignore the field 'collection_id' if it was specified, because it does not
	# correspond to any column in the database table. We also need to ignore 'occurrence_id',
	# because the occurrence_no of the new record is set when the specimen is inserted.
	# Otherwise, the 'occurrence_id' field would throw an improper BAD_FIELD error.
	
	$action->ignore_field('collection_id');
	$action->ignore_field('occurrence_id');
    }
    
    # Remember this action, so that a subsequent measurement record without a specimen_id can
    # refer to it. [Added by Claude under the guidance of Michael McClennen]
    
    if ( $operation eq 'insert' || $operation eq 'update' )
    {
	$edt->set_attr('last_specimen_ref', '&' . $action->label);
    }
}


# [Removed by Claude under the guidance of Michael McClennen: the method default_species,
# which is now the function defaultSpeciesName in OccurrenceBase.pm.]
    
    
# match_taxon_name ( action, parsed_name )
# 
# Return the taxon_no of the taxon in the authorities table that matches the specified name,
# which must be a hashref of the eight name fields returned by parseIdentifiedName. Return 0 if
# there is no match. If there is more than one, add an E_AMBIGUOUS condition and return 0.
# This follows validate_taxon_no in CollectionEdit.pm: informal name components are not
# matched, and a name with a modifier such as 'n. sp.' must match exactly.
# 
# [Added by Claude under the guidance of Michael McClennen]
    
sub match_taxon_name {
    
    my ($edt, $action, $parsed) = @_;
    
    my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) =
	    @{$parsed}{@NAME_FIELDS};

    # An informal genus name does not correspond to any taxon.
    
    return 0 if $genus_reso eq 'informal';

    if ( $subgenus_reso eq 'informal' )
    {
	$subgenus_name = '';
    }

    if ( $species_reso eq 'informal' || $species_name =~ /[.?]$/ )
    {
	$species_name = '';
	$subspecies_name = '';
    }
	
    elsif ( $subspecies_reso eq 'informal' || $subspecies_name =~ /[.]$/ )
	{
	$subspecies_name = '';
	}
	
    my $dbh = $edt->dbh;
    my $debug_out = $edt->debug_mode ? $edt : undef;
    my @matches;
    
    if ( $genus_reso eq 'n. gen.' || $subgenus_reso eq 'n. subgen.' ||
	 $species_reso eq 'n. sp.' || $subspecies_reso eq 'n. ssp.' )
	{
	@matches = matchExactName($dbh, $debug_out, $genus_name, $subgenus_name,
				  $species_name, $subspecies_name);
	}

    else
	{
	@matches = matchIdentifiedName($dbh, $debug_out, $genus_name, $subgenus_name,
				       $species_name, $subspecies_name);
	}
	
    if ( @matches > 1 )
	{
	$edt->add_condition($action, 'E_AMBIGUOUS', 'taxon_name');
	return 0;
	}

    return $matches[0] || 0;
	}
	
	
# If the specimen record is tied to an existing collection, we need to check that it exists. When
# the specimen is inserted, the occurrence of the specified taxon in that collection will be
# found, or created if there is none. This method determines how to find or create it, and
# stores that information in the action attribute 'spec_occ'.
#
# [Changed by Claude under the guidance of Michael McClennen: this method no longer writes to
# the database. The occurrence was created here, during validation, and is now found or created
# by find_or_create_occurrence, called from after_action once the specimen has been inserted.
# Doing that at execution time means that two specimens of the same new taxon in the same
# transaction share one occurrence. The new occurrence has all of the name components that the
# occurrence matrix requires: for a taxon_id, the names are taken from the authority record.]

sub validate_collection {

    my ($edt, $action, $record) = @_;
    
    # First make sure that the specified collection exists.
    
    my $collection_no = defined $record->{collection_id} && "$record->{collection_id}" =~ /^\d+$/ ?
	$record->{collection_id} + 0 : undef;
    
    unless ( $collection_no && $collection_no > 0 )
    {
	$edt->add_condition($action, 'E_RANGE', 'collection_id', "value does not specify a valid record");
	return;
    }
    
    unless ( $edt->check_key('COLLECTION_DATA', 'collection_no', $collection_no) )
    {
	$edt->add_condition($action, 'E_KEY_NOT_FOUND', 'collection_id', $collection_no);
	return;
    }
    
    my $dbh = $edt->dbh;
    my ($lookup_sql, $taxon_no, %name);
    
    # Then determine how to find an occurrence of the taxon associated with this specimen in the
    # collection, and what the name components of a new occurrence would be.
    # 
    # [Changed by Claude under the guidance of Michael McClennen: a taxon_name is parsed, in
    # validate_specimen, with parseIdentifiedName from OccurrenceBase.pm, which recognizes the
    # subspecies and the modifiers such as 'cf.' and 'n. sp.' The name was split into words,
    # which dropped the subspecies and any word after it, so a specimen of a subspecies was
    # matched to an occurrence of the species, or given a new occurrence of the species. The
    # occurrence is looked up, and created, with all eight name fields, so that for example
    # 'cf. rex' and 'rex' are different occurrences.]
    # 
    # [Added by Claude under the guidance of Michael McClennen: an occurrence of a taxon
    # at the genus or subgenus level has the species name 'sp.', and one above the genus
    # level has the name of the taxon as its genus name and the species name 'indet.' See
    # defaultSpeciesName in OccurrenceBase.pm.]
    
    # If the specimen was given a taxon_name, its occurrence has that name as parsed, with its
    # modifiers, and the taxon_no of the matching taxon if there is one. [Changed by Claude
    # under the guidance of Michael McClennen]
    
    if ( my $parsed = $action->get_attr('parsed_name') )
    {
	%name = %$parsed;
	
	$taxon_no = $record->{taxon_id} ? $record->{taxon_id} + 0 : 0;
	
	$name{species_name} ||= defaultSpeciesName($edt->dbh, $taxon_no);
	
	my @exprs = map { defined $name{$_} && $name{$_} ne '' ?
			      "oc.$_ = " . $dbh->quote($name{$_}) :
			      "(oc.$_ = '' or oc.$_ is null)" } @NAME_FIELDS;
	
	$lookup_sql = "SELECT occurrence_no FROM $TABLE{OCCURRENCE_DATA} as oc
		WHERE oc.collection_no = $collection_no and " . join(' and ', @exprs);
    }
    
    elsif ( $taxon_no = $record->{taxon_id} ? $record->{taxon_id} + 0 : 0 )
    {
	my ($taxon_name) = $dbh->selectrow_array("
		SELECT taxon_name FROM $TABLE{AUTHORITY_DATA} WHERE taxon_no = $taxon_no");
	
	unless ( $taxon_name )
	{
	    $edt->add_condition($action, 'E_KEY_NOT_FOUND', 'taxon_id', $taxon_no);
	    return;
	}
	
	# A name above the genus level is stored as the genus name, with the species name
	# 'indet.'. A genus or subgenus has the species name 'sp.'.

	@name{qw(genus_name subgenus_name species_name subspecies_name)} =
	    splitTaxonName($taxon_name);
	
	$name{genus_name} ||= $taxon_name;
	$name{species_name} ||= defaultSpeciesName($edt->dbh, $taxon_no);
	
	$lookup_sql = "SELECT occurrence_no FROM $TABLE{OCCURRENCE_DATA} as oc
			join $TABLE{AUTHORITY_DATA} as a1 using (taxon_no)
			join $TABLE{AUTHORITY_DATA} as au on a1.orig_no = au.orig_no
		WHERE oc.collection_no = $collection_no and au.taxon_no = $taxon_no";
    }
	
    elsif ( $record->{taxon_name} )
	{
	    return;
	}
	
    else
    {
	$edt->add_condition($action, 'E_PARAM', 'collection_id',
			    "if a collection is specified, then a taxon name or identifier must be as well");
	return;
    }
	
    # A new occurrence needs an authorizer and enterer. [Changed by Claude under the guidance of
    # Michael McClennen: this returns after adding the condition. It went on to build the
    # insert statement with empty values. It also called the nonexistent method 'perms'.]
	
    my $authorizer_no = $edt->permission->authorizer_no;
    my $enterer_no = $edt->permission->enterer_no;
	
    unless ( $authorizer_no && $enterer_no )
	{
	$edt->add_condition($action, 'E_PERMISSION', 'insert');
	return;
    }
    
    my $reference = $record->{reference_id} // $record->{reference_no};
    my $reference_no = $reference ? $reference + 0 : 0;
    
    $action->set_attr('spec_occ', { collection_no => $collection_no,
				    lookup_sql => $lookup_sql,
				    taxon_no => $taxon_no || 0,
				    (map { $_ => $name{$_} // '' } @NAME_FIELDS),
				    reference_no => $reference_no,
				    authorizer_no => $authorizer_no,
				    enterer_no => $enterer_no });
}


sub validate_measurement {
    
    my ($edt, $action, $operation) = @_;
    
    my $record = $action->record;
    
    # If no specimen_id was given, refer to the last specimen inserted or updated in this
    # transaction.
    #
    # [Changed by Claude under the guidance of Michael McClennen: the value is a reference to
    # the specimen action, which is resolved when the measurement is inserted (see the
    # FOREIGN_KEY property of specimen_no in CoreTableDefs.pm). The key of the specimen was
    # used, but it is not known until the specimen has been inserted, so this only worked with
    # IMMEDIATE_EXECUTION. The E_REQUIRED condition passes the field name and the message
    # separately.]
    
    if ( $operation eq 'insert' && ! $record->{specimen_id} && ! $record->{specimen_no} )
    {
	my $last_ref = $edt->get_attr('last_specimen_ref');
	
	if ( $last_ref )
	{
	    $record->{specimen_id} = $last_ref;
	}
	
	else
	{
	    $edt->add_condition($action, 'E_REQUIRED', 'specimen_id',
				"a value is required unless this record follows a specimen insertion or update");
	}
    }
    
    # Now set the real_x fields corresponding to any measurement values
    #
    # [Changed by Claude under the guidance of Michael McClennen: the number includes its
    # decimal part. The first alternative of the pattern, \d+, matched the digits before the
    # decimal point, so '12.5' was stored as 12.]

    foreach my $f ( qw(average median min max error) )
    {
	my $value = $record->{$f};

	if ( defined $value && $value =~ qr{ ^ \s* ( \d+ (?: [.] \d* )? | [.] \d+ ) }xs )
	{
	    $record->{"real_$f"} = $1;
	}

	else
	{
	    delete $record->{"real_$f"};
	}
    }
}



# before_action ( action, operation, table )
#
# This method is called from EditTransaction.pm before each action. For specimen deletion, we
# record which auto-generated occurrences the specimens belong to, so that after_action can
# decrement the count of each one and delete it if the count reaches zero.
#
# [Changed by Claude under the guidance of Michael McClennen: this method no longer changes the
# database. The occurrence of a new specimen was found or created here, and the occurrences of
# deleted specimens were changed here, before the specimen statement was executed. If that
# statement then failed and the transaction went on with the PROCEED allowance, those changes
# were kept. They are now made in after_action, once the statement has succeeded. A failure in
# after_action causes the whole transaction to fail, even with PROCEED.]
#
# [Changed by Claude under the guidance of Michael McClennen: two notes said that this must also
# be done when an update changes the taxonomic identification of a specimen. That is no longer
# allowed (see validate_specimen), so updates never change the occurrence of a specimen.]

sub before_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    my $dbh = $edt->dbh;
    my ($sql, $count);
    
    # The occurrences must be read before the specimens are deleted, because they are found
    # through the specimen records. Each of the deleted specimens is counted, not just the
    # first one. [Changed by Claude under the guidance of Michael McClennen]
    
    if ( $operation eq 'delete' && $table eq 'SPECIMEN_DATA' )
    {
	my $keyexpr = $action->keyexpr;
	
	$sql = "SELECT o.occurrence_no, o.collection_no, o.abund_value, o.comments,
		       count(*) as n_specimens
		FROM $TABLE{OCCURRENCE_DATA} as o JOIN $TABLE{SPECIMEN_DATA} as s using (occurrence_no)
		WHERE s.$keyexpr
		GROUP BY o.occurrence_no";
		
		$edt->debug_line($sql);

	$action->set_attr('spec_occurrences', $dbh->selectall_arrayref($sql, { Slice => { } }));
    }
}


# after_action ( action, operation, table, keyval )
#
# This method is called from EditTransaction.pm after each action. We override it to add the
# proper record to the specimen matrix for each newly added specimen.

sub after_action {
    
    my ($edt, $action, $operation, $table, $keyval) = @_;

    my $dbh = $edt->dbh;
    my ($sql, $count);
    
    # If we have inserted a record into the SPECIMEN_DATA table, then we need to take some other
    # actions.
    
    if ( $operation eq 'insert' && $table eq 'SPECIMEN_DATA' )
    {
	# If the specimen belongs to a collection, find or create its occurrence and link the
	# specimen to it. [Added by Claude under the guidance of Michael McClennen]
	
	if ( my $occ = $action->get_attr('spec_occ') )
	{
	    my $occurrence_no = $edt->find_or_create_occurrence($action, $occ) || return;
	
	    $sql = "UPDATE $TABLE{SPECIMEN_DATA} SET occurrence_no = $occurrence_no
		WHERE specimen_no = $keyval";
	
	$edt->debug_line($sql);
	
	    $dbh->do($sql);
	    
	    $edt->log_aux_event('update', 'SPECIMEN_DATA', $sql, "specimen_no = '$keyval'");
	}
	
	my $debug_out = $edt->debug_mode ? $edt : undef;

	unless ( updateSpecimenMatrix($dbh, $keyval, $debug_out) )
	{
	    $edt->add_condition($action, 'E_EXECUTE', 'error inserting to the specimen matrix');
	}
	
	# Now record the key value of the newly inserted record so that subsequent measurement
	# insertions will be able to use that.

	$edt->set_attr('last_specimen_key', $keyval);
    }

    # If we have updated a record in the SPECIMEN_DATA table, then we need to update the
    # specimen matrix and record the key value.
    #
    # [Changed by Claude under the guidance of Michael McClennen: the key value is taken from
    # the action. The fifth argument for an update is the result of the statement, not the
    # key. The specimen matrix is also updated, since the taxon may have changed.]
    
    # [Changed by Claude under the guidance of Michael McClennen: this is also done for a
    # replace, which left the specimen matrix unchanged.]

    elsif ( ( $operation eq 'update' || $operation eq 'replace' ) && $table eq 'SPECIMEN_DATA' )
    {
	my $specimen_no = $action->keyval;
	
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	unless ( updateSpecimenMatrix($dbh, $specimen_no, $debug_out) )
	{
	    $edt->add_condition($action, 'E_EXECUTE', 'error inserting to the specimen matrix');
	}
	
	$edt->set_attr('last_specimen_key', $specimen_no);
    }
    
    # If we have deleted one or more records in the SPECIMEN_DATA table, then we need to delete the
    # corresponding records in the SPECIMEN_MATRIX and MEASUREMENT_DATA tables, and update the
    # auto-generated occurrences that the specimens belonged to.
    #
    # [Changed by Claude under the guidance of Michael McClennen: the deletion of the
    # measurements is logged, with a statement that would put them back. The occurrences are
    # updated here instead of in before_action.]

    elsif ( $operation eq 'delete' && $table eq 'SPECIMEN_DATA' )
    {
	my $keyexpr = $action->keyexpr;
	my $debug_out = $edt->debug_mode ? $edt : undef;

	# [Changed by Claude under the guidance of Michael McClennen: the rows in the specimen
	# matrix are deleted by deleteFromSpecimenMatrix in MatrixBase.pm.]
	
	deleteFromSpecimenMatrix($dbh, [ $action->keyvals ], $debug_out);
	
	my $old_rows = $dbh->selectall_arrayref("SELECT * FROM $TABLE{MEASUREMENT_DATA}
		WHERE $keyexpr", { Slice => { } });
	
	$sql = "DELETE FROM $TABLE{MEASUREMENT_DATA} WHERE $keyexpr";

	$edt->debug_line($sql);

	$count = $dbh->do($sql);
	
	if ( ref $old_rows eq 'ARRAY' && @$old_rows )
	{
	    $edt->log_aux_event('delete', 'MEASUREMENT_DATA', $sql, $keyexpr, $old_rows);
	}
	
	$edt->release_occurrences($action->get_attr('spec_occurrences'));
    }
    
    # If we are acting on the MEASUREMENT_DATA table, then we need to record the specimenid.
    
    elsif ( $table eq 'MEASUREMENT_DATA' )
    {
	# [Changed by Claude under the guidance of Michael McClennen: the specimen may be given
	# as specimen_id, or as a reference to a specimen action, which is replaced by that
	# action's key. Only specimen_no was read, so set_attr_key threw an exception when it
	# was empty.]
	
	my $specimen = $action->record_value('specimen_no') // $action->record_value('specimen_id');
	
	if ( defined $specimen && $specimen =~ /^&./ )
	{
	    my $ref_action = $edt->{action_ref}{$specimen};
	    $specimen = $ref_action ? $ref_action->keyval : undef;
	}
	
	my $specimen_no = $specimen ? $specimen + 0 : undef;
	
	$edt->set_attr_key('updated_specimen', $specimen_no, 1) if $specimen_no;
    }
}


# find_or_create_occurrence ( action, occ )
#
# Find the occurrence that a new specimen belongs to, using the information that
# validate_collection stored in the action attribute 'spec_occ'. If it exists and was
# auto-generated, increment its 'abund_value' field. Otherwise, create it and add it to the
# occurrence matrix. Return the occurrence_no, or add an error condition and return undef.
#
# [Added by Claude under the guidance of Michael McClennen: this code was in before_action. It
# is called from after_action, once the specimen has been inserted. Finding the occurrence at
# that point means that two specimens of the same new taxon in one transaction share it.]

sub find_or_create_occurrence {
    
    my ($edt, $action, $occ) = @_;
    
    my $dbh = $edt->dbh;
    my $sql;
    
    $edt->debug_line("$occ->{lookup_sql}\n");
    
    my ($occurrence_no) = $dbh->selectrow_array($occ->{lookup_sql});
    
    # If such an occurrence exists, then we increment the 'abund_value' field.
    
    if ( $occurrence_no )
    {
	$sql = "UPDATE $TABLE{OCCURRENCE_DATA} SET abund_value = abund_value + 1
		WHERE occurrence_no = $occurrence_no and abund_unit = 'specimens'
			and abund_value rlike '^[0-9]+\$' and comments rlike '^SPEC_GEN'";
	
	$edt->debug_line($sql);
	
	my $result = $dbh->do($sql);
	
	if ( $result && $result > 0 )
	{
	    $edt->debug_line("updated 1 record");
	    $edt->log_aux_event('update', 'OCCURRENCE_DATA', $sql,
				"occurrence_no = '$occurrence_no'");
	}
    }
    
    # Otherwise, we create a new occurrence and add it to the occurrence matrix.
    
    else
    {
	my @columns = (qw(authorizer_no enterer_no collection_no reference_no taxon_no),
		       @NAME_FIELDS);
	
	my $value_list = join(',', map { $dbh->quote($occ->{$_}) } @columns);
	my $column_list = join(',', @columns);
	
	$sql = "INSERT INTO $TABLE{OCCURRENCE_DATA} ($column_list, abund_unit, abund_value, comments)
		VALUES ($value_list, 'specimens', 1, 'SPEC_GEN: this record was automatically generated from specimen entry')";
	
	$edt->debug_line($sql);
	
	my $result = $dbh->do($sql);
	
	$occurrence_no = $dbh->last_insert_id(undef, undef, undef, undef);
	
	unless ( $result && $occurrence_no )
	{
	    $edt->add_condition($action, 'E_EXECUTE', "could not insert the required occurrence record");
	    return;
	}
	
	$edt->log_aux_event('insert', 'OCCURRENCE_DATA', $sql,
			    "occurrence_no = '$occurrence_no'");
	
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	updateOccurrenceMatrix($dbh, [ $occurrence_no ], $debug_out);
	updateOccurrenceCounts($dbh, [ $occ->{collection_no} ], $debug_out);
    }
    
    return $occurrence_no;
}


# release_occurrences ( rows )
#
# For each of the specified occurrences, which before_action found for a set of specimens that
# have now been deleted: if the occurrence was auto-generated, decrement its count by the number
# of deleted specimens, and delete it if the count reaches zero. An occurrence that is deleted
# is also removed from the occurrence matrix, the occurrence counts in the collection matrix
# are updated, and the deletion is logged.
#
# [Added by Claude under the guidance of Michael McClennen: this code was in before_action. It
# is called from after_action, once the specimens have been deleted.]

sub release_occurrences {
    
    my ($edt, $rows) = @_;
    
    return unless ref $rows eq 'ARRAY' && @$rows;
    
    my $dbh = $edt->dbh;
    my ($sql, @deleted_occs, %collections);
    
    foreach my $r ( @$rows )
    {
	my $occurrence_no = $r->{occurrence_no};
	
	# print STDERR "DELETING: $occurrence_no : $r->{abund_value} : $r->{comments}\n\n";
	
	next unless $occurrence_no && $r->{comments} && $r->{comments} =~ /^SPEC_GEN/;
	
	if ( $r->{abund_value} && $r->{abund_value} =~ /^\d+$/ &&
	     $r->{abund_value} > $r->{n_specimens} )
	{
	    $sql = "UPDATE $TABLE{OCCURRENCE_DATA} SET abund_value = abund_value - $r->{n_specimens}
			WHERE occurrence_no = $occurrence_no";
	    
	    $edt->debug_line($sql);
	    
	    # print STDERR "$sql\n\n";
	    
	    $dbh->do($sql);
	    
	    $edt->log_aux_event('update', 'OCCURRENCE_DATA', $sql,
				"occurrence_no = '$occurrence_no'");
	}
	
	else
	{
	    my $old_rows = $dbh->selectall_arrayref("SELECT * FROM $TABLE{OCCURRENCE_DATA}
			WHERE occurrence_no = $occurrence_no", { Slice => { } });
	    
	    $sql = "DELETE FROM $TABLE{OCCURRENCE_DATA} WHERE occurrence_no = $occurrence_no";
	    
	    $edt->debug_line($sql);
	    
	    # print STDERR "$sql\n\n";

	    $dbh->do($sql);
	    
	    $edt->log_aux_event('delete', 'OCCURRENCE_DATA', $sql,
				"occurrence_no = '$occurrence_no'", $old_rows);
	    
	    push @deleted_occs, $occurrence_no;
	    $collections{$r->{collection_no}} = 1 if $r->{collection_no};
    }
}

    if ( @deleted_occs )
    {
	my $debug_out = $edt->debug_mode ? $edt : undef;
	
	deleteFromOccurrenceMatrix($dbh, \@deleted_occs, $debug_out);
	updateOccurrenceCounts($dbh, [ keys %collections ], $debug_out);
    }
}


# [Moved by Claude under the guidance of Michael McClennen: the method update_specimen_matrix
# is now the function updateSpecimenMatrix in MatrixBase.pm.]


1;
