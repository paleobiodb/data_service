#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a particular opinion


package Opinion;

use strict;
use Constants;

use DBI;
use DBConnection;
use SQLBuilder;
use URLMaker;
use CGI::Carp qw(fatalsToBrowser);
use Class::Date qw(date localdate gmdate now);
use CachedTableRow;
use Rank;
use Globals;
use Session;


# names of radio buttons, etc. used in the form.
use constant BELONGS_TO => 'belongs to';
use constant RECOMBINED_AS => 'recombined as';
use constant INVALID1 => 'invalid1';
use constant INVALID2 => 'invalid2';



use fields qw(	
                GLOBALVARS
				opinion_no
				
				parentName
				
				cachedDBRow
				SQLBuilder
							);  # list of allowable data fields.

						
# optionally pass it a reference to the GLOBALVARS hash.
sub new {
	my $class = shift;
		
	my Opinion $self = fields::new($class);
	
	$self->{GLOBALVARS} = shift;
	
	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Opinion $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new($self->{GLOBALVARS});
	}
	
	return $SQLBuilder;
}


# mainly meant for internal use
# returns a hashref with all data (select *) for the current opinion,
# *if* we have an opinion number for it.  If we don't, then it returns nothing.
sub databaseOpinionRecord {
	my Opinion $self = shift;

	if (! $self->{opinion_no}) {
		return;	
	}
	
	my $row = $self->{cachedDBRow};
	
	if (! $row ) {
		# if a cached version of this row query doesn't already exist,
		# then go ahead and fetch the data.
	
		$row = CachedTableRow->new($self->{GLOBALVARS}, 'opinions', "opinion_no = '" . $self->{opinion_no} . "'");

		$self->{cachedDBRow} = $row;  # save for future use.
	}
	
	#Debug::dbPrint($row->get('taxon_name'));
	return $row->row(); 	
}


# sets the occurrence
sub setWithOpinionNumber {
	my Opinion $self = shift;
	
	if (my $input = shift) {
		$self->{opinion_no} = $input;
	}
}


sub opinionNumber {
	my Opinion $self = shift;

	return $self->{opinion_no};	
}


# the parent_no is the parent in a relationship such as
# recombined as.  It's the *new* one.
sub parentNumber {
	my Opinion $self = shift;

	my $rec = $self->databaseOpinionRecord();
	return $rec->{parent_no};	
}

# returns the parent name for this opinion if it can figure it out.
# (this is the taxon name from the authorities table figured out from the 
# parent_no of this opinion record).
sub parentName {
	my Opinion $self = shift;
	
	my $parent_no = $self->parentNumber();
	if (($parent_no) && (! $self->{parentName})) {
		my $pt = Taxon->new();
		$pt->setWithTaxonNumber($parent_no);
		$self->{parentName} = $pt->taxonName();
	}
	
	return $self->{parentName};
}

# the child_no is the original taxon_no that this opinion is about.
sub childNumber {
	my Opinion $self = shift;
	
	my $rec = $self->databaseOpinionRecord();
	return $rec->{child_no};
}




# returns the authors of the opinion record
sub authors {
	my Opinion $self = shift;
	
	# get all info from the database about this record.
	my $hr = $self->databaseOpinionRecord();
	
	if (!$hr) {
		return '';	
	}
	
	my $auth;
	
	if ($hr->{ref_has_opinion} eq YES) {
		# then get the author info for that reference
		my $ref = Reference->new();
		$ref->setWithReferenceNumber($hr->{reference_no});
		
		$auth = $ref->authorsWithInitials();
	} else {
	
		$auth = Globals::formatAuthors(1, $hr->{author1init}, $hr->{author1last}, $hr->{author2init}, $hr->{author2last}, $hr->{otherauthors} );
		$auth .= " " . $self->pubyr();
	}
	
	return $auth;
}


sub pubyr {
	my Opinion $self = shift;

	# get all info from the database about this record.
	my $hr = $self->databaseOpinionRecord();
	
	if (!$hr) {
		return '';	
	}
	
	return $hr->{pubyr};
}



# Formats the opinion as HTML and
# returns it.
#
# For example, "belongs to Equidae according to J. D. Archibald 1998"
sub formatAsHTML {
	my Opinion $self = shift;
	
	my $ref = $self->databaseOpinionRecord();
	
	my $status = $ref->{status};
	my $statusPhrase = $status;
	
	if (($status eq 'subjective synonym of') || 
		($status eq 'objective synonym of') || 
		($status eq 'homonym of') || 
		($status =~ m/nomen/)) {
			$statusPhrase = "is a $status";
	}
	
	my $ref_has_opinion = $ref->{ref_has_opinion};
	
	my $parent = Taxon->new($self->{GLOBALVARS});
	$parent->setWithTaxonNumber($self->parentNumber());
	
	my $child = Taxon->new($self->{GLOBALVARS});
	$child->setWithTaxonNumber($self->childNumber());
	
	
	if ($status =~ m/nomen/) {
		# nomen anything...
		return "'" . $child->taxonNameHTML() . " $statusPhrase'". " according to " . $self->authors() ;
	} elsif ($status ne 'revalidated') {
		
		return "'" . $child->taxonNameHTML() . " $statusPhrase " .  $parent->taxonName() . "' according to " . $self->authors();
	} else {
		# revalidated
		return "'" . $child->taxonNameHTML() . " $statusPhrase' according to " . $self->authors();
	}
}






# display the form which allows users to enter/edit opinion
# table data.
#
# Pass this an HTMLBuilder object,
# a session object, and the CGI object.
#
# rjp, 3/2004
sub displayOpinionForm {
	my Opinion $self = shift;
	my $hbo = shift;
	my $s = shift;
	my $q = shift;
	
	
	my $sql = $self->getSQLBuilder();
	
	my %fields;  # a hash of fields and values that
				 # we'll pass to HTMLBuilder to pop. the form.
				 
	# Fields we'll pass to HTMLBuilder that the user can't edit.
	# (basically, any field which is not empty or 0 in the database,
	# if the authorizer is not the original authorizer of the record).
	my @nonEditables; 	
	
	if ((!$hbo) || (! $s) || (! $q)) {
		Debug::logError("Taxon::displayOpinionForm had invalid arguments passed to it.");
		return;
	}
	
	
	# Figure out if it's the first time displaying this form, or if it's already been displayed,
	# for example, if we're showing some errors about the last attempt at submission.
	my $secondTime = 0;  # is this the second time displaying this form?
	if ($q->param('second_submission') eq YES) {
		# this means that we're displaying the form for the second
		# time with some error messages.
		$secondTime = 1;
			
		# so grab all of the fields from the CGI object
		# and stick them in our fields hash.
		my $fieldRef = Globals::copyCGIToHash($q);
		%fields = %$fieldRef;
	}
	
	#Debug::dbPrint("second_submission = $secondTime");
	#Debug::dbPrint("fields, second sub = " . $fields{second_submission});

	# Is this supposed to be a new entry, or just an edit of an old record?
	# We'll look for a hidden field first, just in case we're re-displaying the form.
	my $isNewEntry = 1;
	if ($q->param('is_new_entry') eq 'NO') {
		$isNewEntry = 0;
	} elsif ($q->param('is_new_entry') eq YES) {
		$isNewEntry = 1;	
	} elsif ($self->{opinion_no}) {
		$isNewEntry = 0;  # it must be an edit if we have an opinion_no for it.
	}

	#Debug::dbPrint("isNewEntry = $isNewEntry");
	
	# if the opinion is already in the opinions table,
	# then grab the data from that table row.
	my $dbFieldsRef;
	if (! $isNewEntry) {	
		$dbFieldsRef = $self->databaseOpinionRecord();
		Debug::dbPrint("querying database");
		
		# we only want to stick the database data in the
		# fields to populate if it's the first time displaying the form,
		# otherwise we'll overwrite any changes the user has already made to the form.
		if (!$secondTime) {
			%fields = %$dbFieldsRef;		
			$fields{opinion_no} = $self->{opinion_no};
		}
	}

	
	#### 
	## At this point, it's safe to start assigning things to the %fields
	## hash.  However, it shouldn't be done before here, because the hash
	## might be overwritten in two places above.
	####


	# figure out the taxon name and number
	my $taxon = Taxon->new($self->{GLOBALVARS});
	$taxon->setWithTaxonNumber($q->param('taxon_no'));
	$fields{taxon_name} = $taxon->taxonName();
	$fields{taxon_no} = $taxon->taxonNumber();

	
	
	# Store whether or not this is a new entry in a hidden variable so 
	# we'll have access to this information even after a taxon_no has been assigned...	
	if ($isNewEntry) {
		$fields{is_new_entry} = YES;
	} else {
		$fields{is_new_entry} = 'NO';
		
		# record the opinion number for later use if it's not a new entry.. (ie, if it's an edit).
		$fields{opinion_no} = $self->{opinion_no};
	}
	
	
	# fill out the authorizer/enterer/modifier info at the bottom of the page
	if (!$isNewEntry) {
		if ($fields{authorizer_no}) { $fields{authorizer_name} = " <B>Authorizer:</B> " . $s->personNameForNumber($fields{authorizer_no}); }
		
		if ($fields{enterer_no}) { $fields{enterer_name} = " <B>Enterer:</B> " . $s->personNameForNumber($fields{enterer_no}); }
		
		if ($fields{modifier_no}) { $fields{modifier_name} = " <B>Modifier:</B> " . $s->personNameForNumber($fields{modifier_no}); }
	}
	
	
	# populate the correct pages/figures fields depending
	# on the ref_has_opinion value.
		
	if ($isNewEntry) {
		# for a new entry, use the current reference from the session.
		$fields{reference_no} = $s->currentReference();
	} 
	
	
	#print "ref_is_authority = " . $fields{'ref_is_authority'};
	
	if ($fields{'ref_has_opinion'} eq YES) {
		# reference_no is the authority
		$fields{'ref_has_opinion_checked'} = 'checked';
		$fields{'ref_has_opinion_notchecked'} = '';
	
	} else {
		# reference_no does not hold the opinion info..
		
		if ((!$isNewEntry) || $secondTime) {
			# we only want to check a reference radio button
			# if they're editing an old record.  This will force them to choose
			# one for a new record.  However, if it's the second time
			# through, then it's okay to check one since they already did.
			$fields{'ref_has_opinion_checked'} = '';
			$fields{'ref_has_opinion_notchecked'} = 'checked';
			
			$fields{'ref_has_opinion'} = 'NO';
		}
		
		if (!$secondTime) {
			$fields{'2nd_pages'} = $fields{'pages'};
			$fields{'2nd_figures'} = $fields{'figures'};
			$fields{'pages'} = '';
			$fields{'figures'} = '';
		}
	}
	
	
	my $childRank = Rank->new($taxon->rankString());
	$fields{taxon_rank} = $childRank->rank();
	
	
	# We display a couple of different things depending on whether
	# the rank is species or higher...

	$fields{'OPTIONAL_species_only'} = $childRank->isSpecies();
	$fields{'OPTIONAL_subspecies_only'} = $childRank->isSubspecies();	
	$fields{'OPTIONAL_not_species'} = $childRank->isHigherThanString(SPECIES);
	
	if ( $childRank->isSubspecies() || $childRank->isSpecies() ) {		
		
		# if it's a species or subspecies, then we need to figure out
		# the genus or species.  Eventually, this should be handled by the Rank class,
		# but for now, we'll just grab the first "word" of the taxon_name
		my $parentName;
		
		Debug::dbPrint("is new entry = $isNewEntry");
		
		if ($isNewEntry) {  # if new entry, get parent name from taxon_name entered.
			$parentName = $fields{taxon_name}; 
		} else {
			# else, if we're editing a form, then figure out the parent
			# name based on the parent_no field in the opinion record.
			$parentName = $self->parentName();
			Debug::dbPrint("parentName = $parentName");
		}
		
		my ($one, $two, $three) = split(/ /, $parentName);
		$fields{parent_genus} = $one;
		$fields{parent_species} = "$one $two";
	
		# don't do anything for now.
	}
	

	

	# figure out which radio button to select for the status:
	# status is one of these:
	#
	# Valid: 'belongs to','recombined as','revalidated'
	#
	# Invalid1: 'subjective synonym of', 'objective synonym of',
	# 'homonym of','replaced by','corrected as'
	#
	# Invalid2: 'nomen dubium','nomen nudum','nomen oblitem',
	# 'nomen vanum' 
	#

	my @synArray = ('subjective synonym of', 'objective synonym of', 'homonym of','replaced by','corrected as');
	my @nomArray = ('nomen dubium','nomen nudum','nomen oblitem', 'nomen vanum');
	
	my @valid = (BELONGS_TO, RECOMBINED_AS, 'revalidated');

	
	Debug::dbPrint("fields{status} = " . $fields{status});
 	Debug::dbPrint("fields{taxon_status} = " . $fields{taxon_status});
	
	
	# note, taxon_status is the name of the radio buttons in the form.
	# status is the name in the database.
	# 
	# So, if this is the second time through (after displaying errors), we
	# need to re-assign the status field based off of what the user entered.
	if ($secondTime) {
		$fields{status} = $fields{taxon_status};
		if ($fields{status} eq INVALID1) {
			$fields{status} = $fields{synonym};
		} elsif ($fields{status} eq INVALID2) {
			$fields{status} = $fields{nomen};
		}
	}
	
	
		
	# Now we should figure out the parent taxon name for this opinion.
	my $parent = Taxon->new($self->{GLOBALVARS});
	$parent->setWithTaxonNumber($fields{parent_no});
	
	
	if ($fields{status}) {
		if (! (Globals::isIn(\@valid, $fields{status}))) {
			# it's an invalid status
		
			if ($fields{status} =~ m/nomen/) {
				# it's a nomen status..
				# "Invalid and no other name can be applied"
				
				$fields{'taxon_status_invalid2'} = 'checked';
				$fields{taxon_status} = INVALID2;
				
			} else {
				# else, we should pick the invalid1 radio button.
				# "Invalid and another name should be used"
				
				$fields{'taxon_status_invalid1'} = 'checked';
				$fields{taxon_status} =  INVALID1;
				
				if (! $secondTime) {
					$fields{'parent_taxon_name2'} = $parent->taxonName();
				}
			}
		} else {
			# it must be a valid status
	
			if ($fields{status} eq RECOMBINED_AS) {
				$fields{'taxon_status_recombined_as'} = 'checked';
				$fields{taxon_status} = RECOMBINED_AS;
			} else {
				$fields{taxon_status_belongs_to} = 'checked';
				$fields{taxon_status} = BELONGS_TO;
			}
			
			if (! $secondTime) {
				$fields{'parent_taxon_name'} = $parent->taxonName();
			}
		}
	}
	
	
	# actually build the nomen popup menu.
	$fields{nomen_select} = $hbo->buildSelect(\@nomArray, 'nomen', $fields{status});
			
	# actually build the synonym popup menu.
	$fields{synonym_select} = $hbo->buildSelect(\@synArray, 'synonym', $fields{status});

	
	

	
	
	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.  However, if they're superuser
	# (alroy, alroy), then let them edit anything.
	#
	# otherwise, they can edit any field.
	my $sesAuth = $s->get('authorizer_no');
	
	if ($s->isSuperUser()) {
		$fields{'message'} = "<p align=center><i>You are the superuser, so you can edit any field in this record!</i></p>";
	} elsif ((! $isNewEntry) && ($sesAuth != $dbFieldsRef->{authorizer_no}) &&
	($dbFieldsRef->{authorizer_no} != 0)) {
	
		# grab the authorizer name for this record.
		my $authName = $s->personNameForNumber($fields{authorizer_no});
	
		$fields{'message'} = "<p align=center><i>This record was created by a different authorizer ($authName) so you can only edit empty fields.</i></p>";
		
	
		# set the nomen and synonyms correctly 
		if ($fields{taxon_status} eq INVALID1) {
			$fields{synonym} = $fields{status};
		} elsif ($fields{taxon_status} eq INVALID2) {
			$fields{nomen} = $fields{status};
		}
	
		# we should always make the ref_has_opinion radio buttons disabled
		# because only the original authorizer can edit these.
		
		push (@nonEditables, 'ref_has_opinion');
		
		# depending on the status of the ref_has_opinion radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_has_opinion'} eq YES) {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
		}
		
		
		# depending on the status, we should disable some fields.		
		if ($fields{'status'}) {
			push(@nonEditables, 'taxon_status');
			push(@nonEditables, 'nomen');
			push(@nonEditables, 'synonym');
			push(@nonEditables, 'parent_taxon_name');
			push(@nonEditables, 'parent_taxon_name2');
		}
		
		if (($fields{'status'} ne RECOMBINED_AS) && ($fields{status} ne BELONGS_TO)) {
			push(@nonEditables, 'diagnosis');
		}
				
				
		# find all fields in the database record which are not empty and add them to the list.
		foreach my $f (keys(%$dbFieldsRef)) {	
			if ($dbFieldsRef->{$f}) {
				push(@nonEditables, $f);
			}
		}
		
		# we'll also have to add a few fields separately since they don't exist in the database,
		# but only in our form.
		
		if ($fields{'2nd_pages'}) { push(@nonEditables, '2nd_pages'); }
		if ($fields{'2nd_figures'}) { push(@nonEditables, '2nd_figures'); }
		if ($fields{'taxon_status'}) { push(@nonEditables, 'taxon_status'); }
		
	}
	
	
	# print the form	
	print main::stdIncludes("std_page_top");
	print $hbo->newPopulateHTML("add_enter_opinion", \%fields, \@nonEditables);
	print main::stdIncludes("std_page_bottom");
}






# Call this when you want to submit an opinion form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
#
# rjp, 3/2004.
sub submitOpinionForm {
	my Opinion $self = shift;
	my $hbo = shift;
	my $s = shift;		# the cgi parameters
	my $q = shift;		# session

	if ((!$hbo) || (!$s) || (!$q)) {
		Debug::logError("Taxon::submitOpinionForm had invalid arguments passed to it.");
		return;	
	}
	
	my $sql = $self->getSQLBuilder();
	$sql->setSession($s);
	
	my $errors = Errors->new();
		
	# if this is the second time they submitted the form (or third, fourth, etc.),
	# then this variable will be true.  This would happen if they had errors
	# the first time, and then resubmitted it.
	my $isSecondSubmission;
	if ($q->param('second_submission') eq YES) {
		$isSecondSubmission = 1;
	} else {
		$isSecondSubmission = 0;
	}
	
	
	# is this a new entry, or an edit of an old record?
	my $isNewEntry;
	if ($q->param('is_new_entry') eq YES) {
		$isNewEntry = 1;	
	} else {
		$isNewEntry = 0;
	}

	
	# grab all the current data from the database about this record
	# if it's not a new entry (ie, if it already existed).	
	my %dbFields;
	if (! $isNewEntry) {
		my $results = $self->databaseOpinionRecord();

		if ($results) {
			%dbFields = %$results;
		}
	}
	
		
	# this $editAny variable is true if they can edit any field,
	# false if they can't.
	my $editAny = $s->editAnyFormField($isNewEntry, $dbFields{authorizer_no});
	
	
	# build up a hash of fields/values to enter into the database
	my %fieldsToEnter;
	
	if ($isNewEntry) {
		$fieldsToEnter{authorizer_no} = $s->authorizerNumber();
		$fieldsToEnter{enterer_no} = $s->entererNumber();
		$fieldsToEnter{reference_no} = $s->currentReference();
		
		if (! $fieldsToEnter{reference_no} ) {
			$errors->add("You must set your current reference before submitting a new opinion");	
		}
		
	} else {
		$fieldsToEnter{modifier_no} = $s->entererNumber();	
	}
	
	
	###
	## Deal with the reference section at the top of the form.  This
	## is almost identical to the way we deal with it in the authority form
	## so this functionality should probably be merged at some point.
	###
	
	if ( 	($q->param('ref_has_opinion') ne YES) && 
			($q->param('ref_has_opinion') ne 'NO')) {
		
		$errors->add("You must choose one of the reference radio buttons");
	}
	
	
	# merge the pages and 2nd_pages, figures and 2nd_figures fields together
	# since they are one field in the database.
	
	if ($q->param('ref_has_opinion') eq 'NO') {
		$fieldsToEnter{pages} = $q->param('2nd_pages');
		$fieldsToEnter{figures} = $q->param('2nd_figures');
		
		if (! $q->param('author1last')) {
			$errors->add('You must enter at least a first author last name');	
		}
		
		# make sure the pages/figures fields above this are empty.
		my @vals = ($q->param('pages'), $q->param('figures'));
		if (!(Globals::isEmpty(\@vals))) {
			$errors->add("Don't enter pages or figures for a primary reference if you chose the 'named in an earlier publication' radio button");	
		}
		
		# make sure the format of the author initials is proper
		if  (( $q->param('author1init') && 
			(! Validation::properInitial($q->param('author1init')))
			) ||
			( $q->param('author2init') && 
			(! Validation::properInitial($q->param('author2init')))
			)
			) {
			
			$errors->add("Improper author initial format");		
		}
		

		# make sure the format of the author names is proper
		if  ( $q->param('author1last')) {
			if (! (Validation::properLastName($q->param('author1last'))) ) {
				$errors->add("Improper first author last name");
			}
		}
			
			
		if  ( $q->param('author2last') && 
			(! Validation::properLastName( $q->param('author2last') ) )
			) {
		
			$errors->add("Improper second author last name");	
		}

			
		if ( ($q->param('pubyr') && 
			(! Validation::properYear($q->param('pubyr'))))) {
			$errors->add("Improper year format");
		}
		
		if (($q->param('otherauthors')) && (! $q->param('author2last') )) {
			# don't let them enter other authors if the second author field
			# isn't filled in.
		
			$errors->add("Don't enter other authors if you haven't entered a second author");
		}
		
		
	} else {
		# ref_has_opinion is YES
		# so make sure the other publication info is empty.
		my @vals = ($q->param('author1init'), $q->param('author1last'), $q->param('author2init'), $q->param('author2last'), $q->param('otherauthors'), $q->param('pubyr'), $q->param('2nd_pages'), $q->param('2nd_figures'));
		
		if (!(Globals::isEmpty(\@vals))) {
			$errors->add("Don't enter other publication information if you chose the 'first named in primary reference' radio button");	
		}
		
	}
	
	
	## End of dealing with the reference section
	####


	####
	## Now loop through all fields submitted from the form.
	## If a field is not empty, then see if we're allowed to edit it in the database.
	## If we can edit it, then make sure the name is correct (since a few fields like
	## 2nd_pages, etc. don't match the database field names) and add it to the 
	## %fieldsToEnter hash.
	####
	
	#Debug::dbPrint("dbFields = ");
	#Debug::printHash(\%dbFields);
	
	foreach my $formField ($q->param()) {
		
		my $okayToEdit = $editAny;
		if (! $okayToEdit) {
			# then we should do some more checks, because maybe they are allowed
			# to edit it aferall..  If the field they want to edit is empty in the
			# database, then they can edit it.
			
			if (! $dbFields{$formField}) {
				# If the field in the database is empty, then it's okay
				# to edit it.
				$okayToEdit = 1;
			}
		}
		
		if ($okayToEdit) {
			
			# if the value isn't already in our fields to enter
			# then add it to the fields to enter
			if (! $fieldsToEnter{$formField}) {
				$fieldsToEnter{$formField} = $q->param($formField);
			}
		}
		
		#Debug::dbPrint("okayToEdit = $okayToEdit, $formField = " . $q->param($formField));
		
	} # end foreach formField.
	
	
	
	###
	## Most of the rest of this function is just error checking.
	##
	###
	
	
	# Set up a few variables to represent the state of the form..
	
	# the taxon_status radio is in ('belongs to', 'recombined as', 'invalid1', 'invalid2).
	my $taxonStatusRadio = $q->param('taxon_status');
	
	# figure out the name of the child taxon.
	my $childTaxon = Taxon->new($self->{GLOBALVARS});
	$childTaxon->setWithTaxonNumber($q->param('taxon_no'));
	my $childTaxonName = $childTaxon->taxonName();
	my $childRank = Rank->new($q->param('taxon_rank'));
	
	###
	# Figure out the name of the parent taxon.
	# This is dependent on the taxonStatusRadio value
	###
	
	my $parentTaxonName;
	my $parentTaxon = Taxon->new($self->{GLOBALVARS});
	my $parentRank;
	
	if ($taxonStatusRadio eq BELONGS_TO) {
		# if this is the radio button, then the parent taxon depends
		# on whether the child is a higher taxon or not.
		if ($childRank->isHigher()) {
			$parentTaxonName = $q->param('parent_taxon_name');	
		} else {
			# it's a species or subspecies, so just grab the genus or species
			# name from the child taxon..
			
			if ($childRank->isSpecies()) {
				# then the parent is genus.
				$parentTaxonName = $childTaxon->firstWord();
			} elsif ($childRank->isSubspecies()) {
				# then the parent is a species
				$parentTaxonName = $childTaxon->firstWord() . " " . $childTaxon->secondWord();
			}

		}		
	} elsif ($taxonStatusRadio eq RECOMBINED_AS) {
		$parentTaxonName = $q->param('parent_taxon_name');
	} elsif ($taxonStatusRadio eq INVALID1) {
		$parentTaxonName = $q->param('parent_taxon_name2');
	}	
	
	Debug::dbPrint("parentTaxonName = $parentTaxonName");
	
	# figure out the parent rank.
	$parentRank = Rank->new();
	$parentRank->setWithTaxonNameFullLookup($parentTaxonName);
	
	
	# make sure it's valid, if it exists.
	
	if ($parentTaxonName) {
		$parentTaxon->setWithTaxonName($parentTaxonName);
		
		if (! ($parentRank->isValid())) {
			$errors->add("The parent taxon name '" . $parentTaxonName . "' is not valid");
		}
		
		if (! $parentTaxon->taxonNumber()) {
			$errors->add("The parent taxon '" . $parentTaxonName ."' doesn't exist in our database.  Please go back and enter an authority record for the parent taxon <i>before</i> entering this opinion.");	
		}
		
		if ($parentTaxonName eq $childTaxonName) {
			$errors->add("The parent and child taxon names should not be the same");	
		}
	}

	
	# We have to do different things depending on which status radio button
	# the user selected.
	
	# They must select a radio button, otherwise, give them an error message

	
	if (! $taxonStatusRadio) {
		$errors->add("You must select a status radio button before submitting this record");	
	}
	
	# Note: the actual field in the database is called 'status'	
	
	# This huge if statement basically figures out the parentTaxonName depending
	# on which radio button they chose and also sets the 'status' field to the
	# correct value for the database table.
	
	if ($taxonStatusRadio eq BELONGS_TO) {
		$fieldsToEnter{status} = BELONGS_TO;
		
		# for belongs to, the parent rank should always be higher than the child rank.
		if (! ($parentRank->isHigherThan($childRank)) ) {
			$errors->add("The parent taxon rank (" . $parentRank->rankString() . ") must be higher than the child taxon rank (" . $childRank->rankString() . ")");	
		}
		
		
		
	} elsif ($taxonStatusRadio eq RECOMBINED_AS) {
		#####
		## RECOMBINED AS
		#####
		
		## Tons of things to check for the Recombined As..  
	
		$fieldsToEnter{status} = RECOMBINED_AS;
		
		if (! ($parentRank->isSpecies())) {
			$errors->add("The parent rank for a recombined 
			relationship must be species");	
		}
		
		if ($parentTaxonName eq $childTaxonName) {
			$errors->add("The parent taxon and child taxon
			can't be the same for a recombined relationship");	
		}
		
		if (!$childRank->isSpecies()) {
			$errors->add("The child rank must be 
			species for a recombined as relationship");
		}
		
		if ($childTaxon->firstWord() eq $parentTaxon->firstWord()) {
			$errors->add("The genus of the child 
			taxon must be different from the genus of the parent taxon");
		}
		
		
		# The last big check we need to do is to make sure that the child numbers in
		# recombined as relationships point to the original combination.  So, if
		# the user is adding an opinion about taxon A and they say that it has been
		# recombined as B, we need to make sure that A is an original, ie, that there
		# are no opinion relationships where A is the parent and the status is recombined_as.
		#
		# If there are cases where A is not the original, then we need to go back down
		# the list and find the original before adding the opinion.
		
		if (!($childTaxon->isOriginalCombination())) {
			# figure out what the original combination was.
			my $originalCombination = $childTaxon->originalCombinationTaxon();
			
			$errors->add("The child taxon is not the original combination.  
					The original combination is " . $originalCombination->taxonName() . ".");
		}
		
		
		
		
	} elsif ($taxonStatusRadio eq INVALID1) {
		
		# in this case, the status is not "invalid1", it's whatever they
		# chose in the synonym popup.
		$fieldsToEnter{status} = $q->param('synonym');
		
		# the parent rank should be the same as the child rank
		if (!($parentRank->isEqualTo($childRank))) {
			$errors->add("For a synonym, the parent rank should match the child rank");
		}
		
	} elsif ($taxonStatusRadio eq INVALID2) {
		
		# in this case, the status is not "invalid2", it's whatever they
		# chose in the nomen popup.
		$fieldsToEnter{status} = $q->param('nomen');
		$fieldsToEnter{parent_no} = undef;
	} 
	
	
	# we're required to have a parent taxon for EVERY instance except 
	# for a nomen whatever...
	if ($taxonStatusRadio ne INVALID2) {
		if (! $parentTaxonName) {
			$errors->add("You must enter a parent taxon name");
		}
	}
	
	
	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( (! (($taxonStatusRadio eq RECOMBINED_AS) ||
			($taxonStatusRadio eq BELONGS_TO) )) && ($q->param('diagnosis'))) {
		$errors->add("Don't enter a diagnosis unless you choose the belongs to or recombined as  radio button");
	}
	
	
	# assign the parent_no and child_no fields if they don't already exist.
	if (!$fieldsToEnter{child_no} ) { $fieldsToEnter{child_no} = $q->param('taxon_no'); }
	
	# if we have a parent name, then we
	# want to use it.. Otherwise, it would be impossible to change the parent
	# of an opinion.
	if ($parentTaxon) {
		$fieldsToEnter{parent_no} = $parentTaxon->taxonNumber(); 
	}


	# Delete some fields that may be present since these don't correspond
	# to fields in the database table.. (ie, they're in the form, but not in the table)
	delete $fieldsToEnter{action};
	delete $fieldsToEnter{'2nd_authors'};
	delete $fieldsToEnter{'2nd_figures'};
	delete $fieldsToEnter{'parent_taxon_name'};
	delete $fieldsToEnter{'parent_taxon_name2'};
	delete $fieldsToEnter{'taxon_status'};
	delete $fieldsToEnter{'nomen'};
	delete $fieldsToEnter{'synonym'};

	
	# correct the ref_has_opinion field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($fieldsToEnter{ref_has_opinion} eq 'NO') {
		$fieldsToEnter{ref_has_opinion} = '';
	}
	
	
	#Debug::dbPrint("new entry = $isNewEntry");
	#Debug::dbPrint("fields to enter = ");
	#Debug::printHash(\%fieldsToEnter);
	#Debug::dbPrint($editAny);
	#return;

	
	# at this point, we should have a nice hash array (%fieldsToEnter) of
	# fields and values to enter into the authorities table.
	

	if ($errors->count() > 0) {
		# put a message in a hidden to let us know that we have already displayed
		# some errors to the user and this is at least the second time through (well,
		# next time will be the second time through - whatever).

		$q->param(-name=>'second_submission', -values=>[YES]);
			
		# stick the errors in the CGI object for display.
		my $message = $errors->errorMessage();
						
		$q->param(-name=>'error_message', -values=>[$message]);
			
			
		$self->displayOpinionForm($hbo, $s, $q);
			
		return;
	}
	
	
	# now we'll actually insert or update into the database.
	
	
	my $resultOpinionNumber;
	
	if ($isNewEntry) {
		my $code;	# result code from dbh->do.
	
		# grab the date for the created field.
		$fieldsToEnter{created} = now();
		
		# make sure we have a taxon_no for this entry...
		if (! $fieldsToEnter{child_no} ) {
			Debug::logError("Opinion::submitOpinionForm, tried to insert a record without knowing its child_no (original taxon).");
			return;	
		}
		
		
		# we'll have to remove the opinion_no from the fieldsToInsert if it 
		# exists, because this is the primary key
		
		delete $fieldsToEnter{opinion_no};
		
		($code, $resultOpinionNumber) = $sql->insertNewRecord('opinions', \%fieldsToEnter);
		
		###
		# At this point, *if* the status of the new opinion was 'recombined as',
		# then we need to make sure we're migrating non 'belongs to' opinions to
		# the original combination.
		#
		# For example, if we have the taxon 'Equus blah' which has some arbitrary
		# number of opinions about it, and the user is entering an opinion that
		# 'Homo blah' has been 'recombined as' 'Equus blah', then we should
		# move all the non 'belongs to' opinions from 'Equus blah' onto 'Homo blah'
		###
		
		if ($code && 
			($fieldsToEnter{status} eq 'recombined as') &&
			($childRank->isSpecies() || $childRank->isSubspecies())) {
			
			my $oldTaxon = Taxon->new();  # ie, 'Equus blah'
			$oldTaxon->setWithTaxonNumber($fieldsToEnter{parent_no});
			
			my $newTaxon = Taxon->new();  # ie, 'Homo blah'
			$newTaxon->setWithTaxonNumber($fieldsToEnter{child_no});
			
			$oldTaxon->moveNonBelongsToOpinionsToTaxon($newTaxon);
		}	
		
	} else {
		# if it's an old entry, then we'll update.
		
		# Delete some fields that should never be updated...
		delete $fieldsToEnter{authorizer_no};
		delete $fieldsToEnter{enterer_no};
		delete $fieldsToEnter{created};
		delete $fieldsToEnter{reference_no};
		
		
		if (!($self->{opinion_no})) {
			Debug::logError("Opinion::submitOpinionForm, tried to update a record without knowing its opinion_no..  Oops.");
			return;
		}
			
		$resultOpinionNumber = $self->{opinion_no};
		
		if ($editAny) {
			Debug::dbPrint("Opinion update any record");
			# allow updates of any fields in the database.
			$sql->updateRecord('opinions', \%fieldsToEnter, "opinion_no = '" . $self->{opinion_no} . "'", 'opinion_no');
		} else {
			
			
			#Debug::dbPrint("Opinion update empty records only");
			#Debug::printHash(\%fieldsToEnter);
			
			my $whereClause = "opinion_no = '" . $self->{opinion_no} . "'";
			
			# only allow updates of fields which are already blank in the database.	
			$sql->updateRecordEmptyFieldsOnly('opinions', \%fieldsToEnter, $whereClause, 'opinion_no');
		}
	}
	
	
	
	# now show them what they inserted/updated...
	my $o = Opinion->new();
	$o->setWithOpinionNumber($resultOpinionNumber);
	$o->displayOpinionSummary($isNewEntry);
	
}





# displays info about the opinion record the user just entered...
# pass it a boolean
# is it a new entry or not..
sub displayOpinionSummary {
	my Opinion $self = shift;
	my $newEntry = shift;

	my $enterupdate;
	if ($newEntry) {
		$enterupdate = 'entered into';
	} else {
		$enterupdate = 'updated in'	
	}
	
	print main::stdIncludes("std_page_top");
	
	
	print "<CENTER>";
	
	my $opinion = Opinion->new();
	$opinion->setWithOpinionNumber($self->opinionNumber());
	
	my $dbrec = $opinion->databaseOpinionRecord();
	
	if (!$dbrec) {
		print "<DIV class=\"warning\">Error inserting/updating opinion record.  Please start over and try again.</DIV>";	
	} else {
		
		my $opinionHTML = $opinion->formatAsHTML();
		$opinionHTML =~ s/according to/of/i;
		
		print "<H3> The opinion $opinionHTML has been $enterupdate the database</H3><p>To verify that the information was entered correctly, click on the add more data link below.</p>";
		
		print "<TABLE border=0><TR><TD align=left>";
		print "<p><A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionForm&opinion_no=" . $self->{opinion_no} ."\"><B>Add more data about this opinion</B></A></p>";
		print "<p><A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionList&taxon_no=" . $self->childNumber() . " \"><B>Add/edit a different opinion about this taxon</B></A></p>";
		print "<p><A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&amp;goal=opinion\"><B>Add/edit an opinion about another taxon</B></A></p>";
		print "</TD></TR></TABLE>";
	}
	
	print "<BR>";
	print "</CENTER>";
	
	print main::stdIncludes("std_page_bottom");
}





# end of Opinion.pm


1;