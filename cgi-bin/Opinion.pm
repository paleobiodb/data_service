#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a particular opinion


package Opinion;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use URLMaker;
use CGI::Carp qw(fatalsToBrowser);
use CachedTableRow;


use fields qw(	
				opinion_no
				cachedDBRow
				SQLBuilder
							);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Opinion $self = fields::new($class);
	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Opinion $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
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
	
		$row = CachedTableRow->new('opinions', "opinion_no = '" . $self->{opinion_no} . "'");

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



# returns the authors of the opinion record
sub authors {
	my Opinion $self = shift;
	
	# get all info from the database about this record.
	my $hr = $self->databaseOpinionRecord();
	
	if (!$hr) {
		return '';	
	}
	
	my $auth;
	
	if ($hr->{ref_has_opinion} eq 'YES') {
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
	my $ref_has_opinion = $ref->{ref_has_opinion};
	
	my $parent = Taxon->new();
	$parent->setWithTaxonNumber($ref->{parent_no});
	
	return $status . " " .  $parent->taxonName() . " according to " . $self->authors() ;
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
	if ($q->param('second_submission') eq 'YES') {
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
	} elsif ($q->param('is_new_entry') eq 'YES') {
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
	my $taxon = Taxon->new();
	$taxon->setWithTaxonNumber($q->param('taxon_no'));
	$fields{taxon_name} = $taxon->taxonName();
	$fields{taxon_no} = $taxon->taxonNumber();
	
	
	# Store whether or not this is a new entry in a hidden variable so 
	# we'll have access to this information even after a taxon_no has been assigned...	
	if ($isNewEntry) {
		$fields{is_new_entry} = 'YES';
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
	
	if ($fields{'ref_has_opinion'} eq 'YES') {
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
	
	my $rankToUse = $taxon->rank();
	$fields{taxon_rank} = $rankToUse;
	
	
	# We display a couple of different things depending on whether
	# the rank is species or higher...
	
	if (! ($rankToUse eq 'species' || $rankToUse eq 'subspecies')) {
		# remove the type_taxon_name field.
		$fields{'OPTIONAL_species_only'} = 0;
	} else {
		# must be a genus or higher taxon
		
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
	
	my @valid = ('belongs to', 'recombined as', 'revalidated');

	
	if (! (Globals::isIn(\@valid, $fields{status}))) {
		# it's an invalid status
		
		if ($fields{status} =~ m/nomen/) {
			# it's a nomen status..
			# "Invalid and no other name can be applied"
			
			$fields{'taxon_status_invalid2'} = 'checked';
			
		} else {
			# else, we should pick the invalid1 radio button.
			# "Invalid and another name should be used"
			
			$fields{'taxon_status_invalid1'} = 'checked';	
		}
	} else {
		# it must be a valid status

		if ($fields{status} eq 'recombined as') {
			$fields{taxon_status_recombined_as} = 'checked';
		} else {
			$fields{taxon_status_belongs_to} = 'checked';
		}
	}
	
	
	
	# actually build the nomen popup menu.
	$fields{nomen_select} = $hbo->buildSelect(\@nomArray, 'nomen', $fields{status});
			
	# actually build the synonym popup menu.
	$fields{synonym_select} = $hbo->buildSelect(\@synArray, 'synonym', $fields{status});
	
	
	# we show a different message depending on the rank...
	if (($rankToUse eq 'species') || ($rankToUse eq 'subspecies')) { 
		$fields{recombined_message} = 'recombined into a different genus as'
	} else {
		$fields{recombined_message} = 'classified as belonging to';	
	}
	
	
	
	# Now we should figure out the parent taxon name for this opinion.
	
	my $parent = Taxon->new();
	$parent->setWithTaxonNumber($fields{parent_no});
	$fields{parent_taxon_name} = $parent->taxonName();
	

	
	
	
	
	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.  However, if they're superuser
	# (alroy, alroy), then let them edit anything.
	#
	# otherwise, they can edit any field.
	my $sesAuth = $s->get('authorizer_no');
	
	if ($s->isSuperUser()) {
		$fields{'message'} = "<p align=center><i>You are the superuser, so you can edit any field in this record.</i></p>";
	} elsif ((! $isNewEntry) && ($sesAuth != $dbFieldsRef->{authorizer_no}) &&
	($dbFieldsRef->{authorizer_no} != 0)) {
	
		# grab the authorizer name for this record.
		my $authName = $s->personNameForNumber($fields{authorizer_no});
	
		$fields{'message'} = "<p align=center><i>This record was created by a different authorizer ($authName) so you can only edit empty fields.</i></p>";
		
		#  we should always make the ref_has_opinion radio buttons disabled
		# because only the original authorizer can edit these.
		
		push (@nonEditables, 'ref_has_opinion');
		
		# depending on the status of the ref_has_opinion radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_has_opinion'} eq 'YES') {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
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
		if ($fields{'status'}) { 
			push(@nonEditables, 'taxon_status');
			push(@nonEditables, 'nomen');
			push(@nonEditables, 'synonym');
			push(@nonEditables, 'parent_taxon_name');
			push(@nonEditables, 'parent_taxon_name2');
		}
		
	}
	
	
	# print the form	
	print main::stdIncludes("std_page_top");
	print $hbo->newPopulateHTML("add_enter_opinion", \%fields, \@nonEditables);
	print main::stdIncludes("std_page_bottom");
}



# end of Opinion.pm


1;