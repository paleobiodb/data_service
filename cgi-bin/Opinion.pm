#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a particular opinion


package Opinion;

use strict;
use Constants;

use DBI;
use DBConnection;
use DBTransactionManager;
use PBDBUtil;
use URLMaker;
use Class::Date qw(date localdate gmdate now);
use CachedTableRow;
use Rank;
use Globals;
use Session;
use Classification;


# names of radio buttons, etc. used in the form.
use constant BELONGS_TO => 'belongs to';
use constant RECOMBINED_AS => 'recombined as';
use constant INVALID1 => 'invalid1';
use constant INVALID2 => 'invalid2';



use fields qw(	
                GLOBALVARS
				opinion_no
                reference_no
				
				cachedDBRow
				DBTransactionManager
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
sub getTransactionManager {
	my Opinion $self = shift;
	
	my $DBTransactionManager = $self->{DBTransactionManager};
	if (! $DBTransactionManager) {
		$DBTransactionManager = DBTransactionManager->new($self->{GLOBALVARS});
	}
	
	return $DBTransactionManager;
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

	# JA: I have no idea why Poling even wrote this function because
	#  originally he didn't bother to correctly compute the pubyr when
	#  ref is authority, but here it is (same as pubyr function in
	#  Taxon.pm)

	if ( ! $hr->{ref_is_authority} )        {
		return $hr->{pubyr};
	}

	# okay, so because ref is authority we need to grab the pubyr off of
	#  that ref
	# I hate to do it, but I'm using Poling's ridiculously baroque
	#  Reference module to do so just for consistency
	my $ref = Reference->new();
	$ref->setWithReferenceNumber($hr->{reference_no});
	return $ref->{pubyr};

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
	my $rec = $self->databaseOpinionRecord();
	$parent->setWithTaxonNumber($rec->{parent_no});
	
	my $child = Taxon->new($self->{GLOBALVARS});
	$child->setWithTaxonNumber($self->childNumber());
	
	
	if ($status =~ m/nomen/) {
		# nomen anything...
		return "'" . $child->taxonNameHTML() . " $statusPhrase' according to " . $self->authors() ;
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
	my $parentnosref = shift;
	my @parentnos;
	if ( $parentnosref )	{
		@parentnos = @{$parentnosref};
	}
	
	
	my $sql = $self->getTransactionManager();
	
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
	

	# figure out which radio button to select for the status:
	# status is one of these:
	#
	# Valid: 'belongs to','recombined as','revalidated'
	#
	# Invalid1: 'subjective synonym of', 'objective synonym of',
	# 'homonym of','replaced by','corrected as','ranked changed as'
	#
	# Invalid2: 'nomen dubium','nomen nudum','nomen oblitum',
	# 'nomen vanum' 
	#

	my @synArray = ('subjective synonym of', 'objective synonym of', 'homonym of','replaced by','corrected as','rank changed as');
	my @nomArray = ('nomen dubium','nomen nudum','nomen oblitum', 'nomen vanum');
	
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
<<<<<<< Opinion.pm

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $pulldown;
	if ( @parentnos )	{
		$pulldown = "<select name=parent_taxon_no>\n";
		$pulldown .= "<option selected>\n";
	# grab the parent taxon name from the database - kind of ugly to do
	#  this so late, but really the information exists nowhere else
		my $pnamesql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $parentnos[0];
		my $pname = ${$sql->getData($pnamesql)}[0]->{taxon_name};
		for my $i (@parentnos)	{
			$pulldown .= "<option value=\"" . $i . "\">";
			$pulldown .= $pname . " ";
		# lucky us, Poling made $sql local to the whole subroutine
			my %auth_yr = %{PBDBUtil::authorAndPubyrFromTaxonNo($sql,$i)};
			$pulldown .= $auth_yr{author1last} . " " . $auth_yr{pubyr};
		# tack on the closest higher order name
		# a little clunky, but it works and doesn't require messing
		#  with get_classification_hash
			my %master_class=%{Classification::get_classification_hash($sql, [ "family,order,class" ] , [ $i ] )};
			my @parents = split ',',$master_class{$i};
			if ( $parents[2] )	{
				$pulldown .= " [" . $parents[2] . "]";
			} elsif ( $parents[1] )	{
				$pulldown .= " [" . $parents[1] . "]";
			} elsif ( $parents[0] )	{
				$pulldown .= " [" . $parents[0] . "]";
			}
			$pulldown .= "\n";
		}
		$pulldown .= "</select>\n";
	}

	# format the "belongs to" section of the form
	my $belongstotext;
	# species get special treatment
	if ( $fields{taxon_rank} =~ /species/ )	{
		$belongstotext = "<TD colspan=2><b>Valid ".$fields{taxon_rank}."</b> as originally combined; belongs to ";
		# if the user needs to choose from several possible parent
		#  genera or species, print the pulldown
		# note that we don't have to include the parent name in a
		#  hidden because it will be computed from the child name
		if ( $fields{'status'} eq BELONGS_TO && @parentnos )	{
			$pulldown =~ s/<select name=parent_taxon_no>/<select name=belongs_to_parent_taxon_no>/;
			$belongstotext .= $pulldown . "<br>";
		}
		# or print the species or genus name as plain text
		else	{
			my ($one,$two,$three) = split / /,$fields{taxon_name};
			if ( $three )	{
				$belongstotext .= "<i>" . $one . " " . $two . "</i>.";
			} else	{
				$belongstotext .= "<i>" . $one . "</i>.";
			}
		}
	}
	# standard approach for genera or higher taxa
	else	{
		$belongstotext = "<TD colspan=2><b>Valid ".$fields{taxon_rank}."</b>, classified as belonging to ";
		if ( $fields{'status'} eq BELONGS_TO && @parentnos )	{
			$pulldown =~ s/<select name=parent_taxon_no>/<select name=belongs_to_parent_taxon_no>/;
	# have to store the actual name in case the submission fails and the
	#  pulldown has to be recomputed
			$pulldown .= "<br>\n<input type=\"hidden\" name=\"parent_taxon_name\" value=\"%%parent_taxon_name%%\">\n";
			$belongstotext .= $pulldown . "<br>";
		}
		else	{
			$belongstotext .= "<input id=\"parent_taxon_name\" name=\"parent_taxon_name\" size=\"50\" value=\"%%parent_taxon_name%%\"><br>";
		}
	}
	$fields{belongs_to} = $belongstotext;

	# if the rank is species or subspecies, throw in a recombined option
	if ( $fields{taxon_rank} =~ /species/ )	{
		my $recombinedtext = "<TR><TD valign=top>
<input type=\"radio\" name=\"taxon_status\" value=\"recombined as\" %%taxon_status_recombined_as%%></TD>
<TD colspan=2><b>Valid %%taxon_rank%%</b>, but recombined into a different genus.<br>
New genus and species:<br>";
		if ( $fields{taxon_rank} eq "subspecies" )	{
			$recombinedtext =~ s/different genus/different species/;
			$recombinedtext =~ s/genus and species/genus, species, and subspecies/;
		}
		$fields{recombined_as} = $recombinedtext;
		if ( $fields{'status'} eq RECOMBINED_AS && @parentnos )	{
			$pulldown =~ s/<select name=parent_taxon_no>/<select name=recombined_parent_taxon_no>/;
			$fields{recombined_input} = $pulldown . "<br>\n<input type=\"hidden\" name=\"parent_taxon_name\" value=\"%%parent_taxon_name%%\">\n";
		}
		else	{
			my $inputfield = "<input id=\"parent_taxon_name\" name=\"parent_taxon_name\" size=\"50\" value=\"%%parent_taxon_name%%\"><br></td></tr>";
			$fields{recombined_input} = $inputfield;
		}
	}

	# format the synonym section
	# note: by now we already have a status pulldown ready to go; we're
	#  tacking on either another pulldown or an input to store the name
	# need a pulldown if this is a second pass and we have multiple
	#  alternative senior synonyms
	if ( $fields{taxon_status} eq INVALID1 && @parentnos )	{
		$pulldown =~ s/<select name=parent_taxon_no>/<select name=synonym_parent_taxon_no>/;
		$fields{synonym_select} .= $pulldown . "\n<input type=\"hidden\" name=\"parent_taxon_name2\" value=\"%%parent_taxon_name2%%\">\n";
	}
	# standard version
	else	{
		$fields{synonym_select} .= "<input name=\"parent_taxon_name2\" size=\"50\" value=\"%%parent_taxon_name2%%\">";
	}

=======

>>>>>>> 1.44
	# print the form	
	print main::stdIncludes("std_page_top");

	my $html = $hbo->newPopulateHTML("add_enter_opinion", \%fields, \@nonEditables);

	print $html;
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

	my %rankToNum = (  'subspecies' => 1, 'species' => 2, 'subgenus' => 3,
		'genus' => 4, 'subtribe' => 5, 'tribe' => 6,
		'subfamily' => 7, 'family' => 8, 'superfamily' => 9,
		'infraorder' => 10, 'suborder' => 11,
		'order' => 12, 'superorder' => 13, 'infraclass' => 14,
		'subclass' => 15, 'class' => 16, 'superclass' => 17,
		'subphylum' => 18, 'phylum' => 19, 'superphylum' => 20,
		'subkingdom' => 21, 'kingdom' => 22, 'superkingdom' => 23,
		'unranked clade' => 24, 'informal' => 25 );

	if ((!$hbo) || (!$s) || (!$q)) {
		Debug::logError("Taxon::submitOpinionForm had invalid arguments passed to it.");
		return;	
	}
	
	my $sql = $self->getTransactionManager();
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

	# Set up a few variables to represent the state of the form..
	
	# the taxon_status radio is in ('belongs to', 'recombined as', 'invalid1', 'invalid2).
	my $taxonStatusRadio = $q->param('taxon_status');
	
	# figure out the name of the child taxon.
	my $childTaxon = Taxon->new($self->{GLOBALVARS});
	$childTaxon->setWithTaxonNumber($q->param('taxon_no'));
	my $childTaxonName = $childTaxon->taxonName();
	my $childRank = Rank->new($q->param('taxon_rank'));
	
	
	###
	## Deal with the reference section at the top of the form.  This
	## is almost identical to the way we deal with it in the authority form
	## so this functionality should probably be merged at some point.
	###
	
	if ( 	($q->param('ref_has_opinion') ne YES) && 
			($q->param('ref_has_opinion') ne 'NO')) {
		
		$errors->add("You must choose one of the reference radio buttons");
	}

	# JA: now Poling does the bulk of the checks on the reference
	if ($q->param('ref_has_opinion') eq 'NO') {

		# JA: first order of business is making sure that the child
		#  plus author combination doesn't duplicate anything
		#  already in the database
		# I have no idea why Poling didn't do this himself; typical
		#  incompetence
		# note: the ref no and parent no don't need to match
		# WARNING: we don't check at all to see if the author/taxon
		#  matches when one opinion (old or new) is a ref_has_opinion
		#  record and the other is not
		my $sql = $self->getTransactionManager();
		my $osql = "SELECT parent_no FROM opinions WHERE ref_has_opinion!='YES' AND child_no=".$q->param('taxon_no')." AND author1last='".$q->param('author1last')."' AND author2last='".$q->param('author2last')."' AND pubyr='".$q->param('pubyr')."'";
		my $oref = ${$sql->getData($osql)}[0];
		if ( $oref ) {
			$errors->add("The author's opinion on ".$childTaxonName." already has been entered - an author can only have one opinion on a name");
		}

		# merge the pages and 2nd_pages, figures and 2nd_figures fields
		# together since they are one field in the database.
		$fieldsToEnter{pages} = $q->param('2nd_pages');
		$fieldsToEnter{figures} = $q->param('2nd_figures');
		
		if (! $q->param('author1last')) {
			$errors->add('You must enter at least the last name of the first author');	
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
			
			$errors->add("The first author's initials are improperly formatted");		
		}
		

		# make sure the format of the author names is proper
		if  ( $q->param('author1last')) {
			if (! (Validation::properLastName($q->param('author1last'))) ) {
				$errors->add("The first author's last name is improperly formatted");
			}
		}
			
			
		if  ( $q->param('author2last') && 
			(! Validation::properLastName( $q->param('author2last') ) )
			) {
		
			$errors->add("The second author's last name is improperly formatted");	
		}

			
		if ($q->param('pubyr')) {
			my $pubyr = $q->param('pubyr');
			
			if (! Validation::properYear( $pubyr ) ) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new();
			$ref->setWithReferenceNumber($q->param('reference_no'));
			if ($pubyr > $ref->pubyr()) {
				$errors->add("The publication year ($pubyr) can't be more recent than that of the primary reference (" . $ref->pubyr() . ")");
			}
		}
		

		
		if (($q->param('otherauthors')) && (! $q->param('author2last') )) {
			# don't let them enter other authors if the second author field
			# isn't filled in.
		
			$errors->add("Don't enter other author names if you haven't entered a second author");
		}
		
		
	} else {
		# ref_has_opinion is YES
		# so make sure the other publication info is empty.
		my @vals = ($q->param('author1init'), $q->param('author1last'), $q->param('author2init'), $q->param('author2last'), $q->param('otherauthors'), $q->param('pubyr'), $q->param('2nd_pages'), $q->param('2nd_figures'));
		
		if (!(Globals::isEmpty(\@vals))) {
			$errors->add("Don't enter any other publication information if you chose the 'first named in primary reference' radio button");	
		}
		
		
		# if they chose ref_has_opinion, then we also need to make sure that there
		# are no other opinions about the current taxon (child_no) which use 
		# this as the reference.  So, look for all opinions with ref_has_opinion = 'YES'
		# and child_no = our current child_no and matching reference_no's.
		my $child_no = $q->param('taxon_no');
		my $reference_no = $q->param('reference_no');
		
		# if we're editing an opinion, then we also need to make sure that
		# we don't include the current opinion in the count.
		my $own_opinion_no_clause;
		if (! $isNewEntry) {
			$own_opinion_no_clause = " AND opinion_no != " . $self->opinionNumber() . " ";
		}
		
		my $sql = $self->getTransactionManager();
#		my $count = $sql->getSingleSQLResult("SELECT COUNT(*) FROM opinions WHERE 
#		child_no = $child_no AND
#		ref_has_opinion = 'YES' AND reference_no = $reference_no
#		$own_opinion_no_clause");

		my $count = ${$sql->getData("SELECT COUNT(*) as c FROM opinions WHERE 
		child_no = $child_no AND ref_has_opinion = 'YES' 
		AND reference_no = $reference_no $own_opinion_no_clause")}[0]->{c};
		
		if ($count > 0) {
			$errors->add("The author's opinion on ".$childTaxonName." already has been entered - an author can only have one opinion on a name");
		}
	}
	
	
	
	{
		# also make sure that the pubyr of this opinion isn't older than
		# the pubyr of the authority record the opinion is about.
		my $taxon = Taxon->new();
		$taxon->setWithTaxonNumber($q->param('taxon_no'));
		my $ref = Reference->new();
		$ref->setWithReferenceNumber($q->param('reference_no'));
		
		my $pubyr = $q->param('pubyr');
		
		Debug::dbPrint("ref pub yr = " . $ref->pubyr() . ", taxon pub yr
		= " . $taxon->pubyr());
		if (( $taxon->pubyr() > $ref->pubyr() ) ||
			( $taxon->pubyr() > $pubyr && $pubyr > 1700 ) ) {
			$errors->add("The publication year for this opinion can't be earlier than the year the taxon was named");	
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
	
	
	###
	# Figure out the name of the parent taxon.
	# This is dependent on the taxonStatusRadio value
	###
	# JA: we're also going to set the parent no if a pulldown was used
	#  AND the status didn't change (happens only on second passes)
	
	my $parentTaxonName;

	# tracks whether we going to have to create a new parent on the fly
	my $createParent;

	if ($taxonStatusRadio eq BELONGS_TO) {
		# if this is the radio button, then the parent taxon depends
		# on whether the child is a higher taxon or not.
		if ($childRank->isHigher()) {
			$parentTaxonName = $q->param('parent_taxon_name');	
		} else {
			# it's a species or subspecies, so just grab the genus or species
			# name from the child taxon..
			my ($one,$two,$three) = split / /,$childTaxonName;
			if ($childRank->isSpecies()) {
				# then the parent is genus.
				$parentTaxonName = $one;
			} elsif ($childRank->isSubspecies()) {
				# then the parent is a species
				$parentTaxonName = $one . " " . $two;
			}

		}		
		if ( $q->param('belongs_to_parent_taxon_no') )	{
			$fieldsToEnter{parent_no} =  $q->param('belongs_to_parent_taxon_no');
		}
	} elsif ($taxonStatusRadio eq RECOMBINED_AS) {
		$parentTaxonName = $q->param('parent_taxon_name');
		if ( $q->param('recombined_parent_taxon_no') )	{
			$fieldsToEnter{parent_no} =  $q->param('recombined_parent_taxon_no');
		}
	} elsif ($taxonStatusRadio eq INVALID1) {
		$parentTaxonName = $q->param('parent_taxon_name2');
		if ( $q->param('synonym_parent_taxon_no') )	{
			$fieldsToEnter{parent_no} =  $q->param('synonym_parent_taxon_no');
		}
	}

#FOO
# does the insert actually work?

	Debug::dbPrint("parentTaxonName = $parentTaxonName");

	# make sure it's valid, if it exists.

	# do a bunch of things based on the parent taxon name:
	# (1) set the parent no if the name matches once
	# (2) if more than once, save a list of possible numbers
	# (3) do some error checking on the name per se
	# note that we'll do the search and save the list even if we already
	#  know the "right" parent number because the user selected it in
	#  an earlier pass, just in case other errors redirect the user to
	#  the input form

	my $parentRank;
	my @parentnos;

	# if the user submitted a parent number after choosing one from
	#  a select list populated during a second pass, then get the rank
	#  so the parent can be entered if error checks pass
	if ( $fieldsToEnter{parent_no} )	{
		my $sql = $self->getTransactionManager();
		my $ranksql = "SELECT taxon_rank FROM authorities WHERE taxon_no=" . $fieldsToEnter{parent_no};
		$parentRank = ${$sql->getData($ranksql)}[0]->{taxon_rank};
	}

	if ($parentTaxonName) {

	#  find taxa matching the name
		my $sql = $self->getTransactionManager();
		my $parentsql = "SELECT taxon_no,taxon_rank FROM authorities WHERE taxon_name='" . $parentTaxonName . "'";

		my @parentrefs = @{$sql->getData($parentsql)};

	# if there are multiple matches there's big trouble
	# basically, (1) we're going to stop the user with an error, and
	#  (2) we're going to save an array of the taxon numbers so
	#  displayOpinionForm can populate a pulldown menu
		if ( $#parentrefs > 0 )	{
			for my $i (0..$#parentrefs)	{
				push @parentnos , $parentrefs[$i]->{taxon_no};
			}
		# give the user a pass on the error if the parent number
		#   was preselected
			if ( ! $fieldsToEnter{parent_no} )	{
				$errors->add("You need to select the taxon that " . $childTaxonName . " belongs to");
			}
		}

	# there's a single match, so set the no and rank
	# this is redundant but harmless if the user preselected a parent no
		if ( $#parentrefs == 0 )	{
			$fieldsToEnter{parent_no} = $parentrefs[0]->{taxon_no};
			$parentRank = $parentrefs[0]->{taxon_rank};
		}

		if (Validation::looksLikeBadSubgenus($parentTaxonName)) {
			$errors->add("The taxon name '$parentTaxonName' is invalid; you can't use parentheses");
		}

	# if the parent taxon doesn't exist yet, we have big trouble
		if ( ! @parentrefs )	{
		# if the parent is a recombination, we're going to let them
		#  get away with creating a new taxon on the fly JA 14.4.04
			if ( $taxonStatusRadio eq RECOMBINED_AS )	{
				$createParent = 1;
			}
		# but if it's a proper parent we're just going to complain
			else	{
				$errors->add("The taxon '" . $parentTaxonName ."' doesn't exist in our database.  Please <A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_name=$parentTaxonName\">create a new authority record for '$parentTaxonName'</a> <i>before</i> entering this opinion.");	
			}
		}
		
		if ($parentTaxonName eq $childTaxonName) {
			$errors->add("The taxon you are entering and the one it belongs to can't have the same name");	
		}
	}

	
	# We have to do different things depending on which status radio button
	# the user selected.
	
	# They must select a radio button, otherwise, give them an error message

	
	if (! $taxonStatusRadio) {
		$errors->add("You must choose one of the status radio buttons");	
	}
	
	# Note: the actual field in the database is called 'status'	
	
	# This huge if statement basically figures out the parentTaxonName depending
	# on which radio button they chose and also sets the 'status' field to the
	# correct value for the database table.
	
	if ($taxonStatusRadio eq BELONGS_TO) {
		$fieldsToEnter{status} = BELONGS_TO;
		
		# for belongs to, the parent rank should always be higher than the child rank.
		# unless either taxon is an unranked clade (JA)
		# or there's no parent rank because there are multiple matching
		#  parents and the user hasn't selected one yet (JA)
		if ( $rankToNum{$parentRank} < $rankToNum{$childRank->rank()} && $parentRank ne "unranked clade" && $childRank->rank() ne "unranked clade" && $parentRank ) {
			$errors->add("The rank of the higher taxon (currently " . $parentRank . ") must be higher than the rank of $childTaxonName (" . $childRank->rank() . ")");	
		}
		
		
		
	} elsif ($taxonStatusRadio eq RECOMBINED_AS) {
		#####
		## RECOMBINED AS
		#####
		
		## Tons of things to check for the Recombined As..  
	
		$fieldsToEnter{status} = RECOMBINED_AS;
		
		if ( $parentTaxonName !~ / / )	{
			$errors->add("If a species is recombined its new rank must be 'species'");	
		}
		
		if ($parentTaxonName eq $childTaxonName) {
			$errors->add("Original and new combinations can't be exactly the same");	
		}
		
		if (!$childRank->isSpecies()) {
			$errors->add("If a species is recombined its old rank must be 'species'");
		}

		my ($cgen,$foo) = split / /,$childTaxonName;
		my ($pgen,$foo) = split / /,$parentTaxonName;
		if ( $cgen eq $pgen )	{
			$errors->add("The genus name in the new combination must be different from the genus name in the old combination");
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
			
			$errors->add("The taxon's name is not the original combination. The original combination is " . $originalCombination->taxonName() . ".");
		}
		
		
		
		
	} elsif ($taxonStatusRadio eq INVALID1) {
		
		# in this case, the status is not "invalid1", it's whatever they
		# chose in the synonym popup.
		$fieldsToEnter{status} = $q->param('synonym');
		
		# the parent rank should be the same as the child rank...
		if ( $parentRank ne $childRank->rank() && $fieldsToEnter{status} ne "rank changed as" ) {
			$errors->add("The rank of a taxon and the rank of its synonym must be the same");
		}
		# JA: ... except if the status is "rank changed as," which is
		#  actually the opposite case
		elsif ( $parentRank eq $childRank->rank() && $fieldsToEnter{status} eq "rank changed as" ) {
			$errors->add("If you change a taxon's rank, its old and ew ranks must be different");
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
			$errors->add("You must enter the name of the taxon this one belongs to");
		}
	}
	
	
	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( (! (($taxonStatusRadio eq RECOMBINED_AS) ||
			($taxonStatusRadio eq BELONGS_TO) )) && ($q->param('diagnosis'))) {
		$errors->add("Don't enter a diagnosis unless you choose the 'belongs to' or 'recombined as' radio button");
	}
	
	
	# assign the child_no field if it doesn't already exist.
	# JA: I'm pretty sure Poling only ever assigns this variable here,
	#  so the conditional is actually superfluous
	if (!$fieldsToEnter{child_no} ) { $fieldsToEnter{child_no} = $q->param('taxon_no'); }

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
			
			
		$self->displayOpinionForm($hbo, $s, $q, \@parentnos);
			
		return;
	}
	
	
	# now we'll actually insert or update into the database.

	# first step is to create the parent taxon if a species is being
	#  recombined and the new combination doesn't exist JA 14.4.04
	# WARNING: this is very dangerous; typos in parent names will
	# create bogus combinations, and likewise if the opinion create/update
	#  code below bombs
	if ( $createParent )	{
		my @authFields;
		my @authVals;

	# sine qua non: the new taxon's name is the parent name
		push @authFields , "taxon_name";
		push @authVals , $parentTaxonName;

	# next we need to steal data from the opinion
		my @opAuthFields = ( "authorizer_no", "enterer_no",
			"reference_no" );
		for my $af ( @opAuthFields )	{
			if ( $fieldsToEnter{$af} )	{
				push @authFields , $af;
				push @authVals , $fieldsToEnter{$af};
			}
		}

	# author information comes from the original combination,
	#  not the opinion
	# I'm doing this the "old" way instead of using some
	#  ridiculously complicated Poling-style objects
		my $sql = $self->getTransactionManager();
		my $asql = "SELECT * FROM authorities WHERE taxon_no=" . $fieldsToEnter{child_no};
		my $aref = ${$sql->getData($asql)}[0];
		my @origAuthFields = ( "taxon_rank", "pages",
			"figures", "comments" );
		for my $af ( @origAuthFields )	{
			if ( $aref->{$af} )	{
				push @authFields , $af;
				push @authVals , $aref->{$af};
			}
		}
		@origAuthFields = ( "author1init", "author1last",
			"author2init", "author2last",
			"otherauthors", "pubyr" );
		if ( $aref->{'author1last'} )	{
			for my $af ( @origAuthFields )	{
				if ( $aref->{$af} )	{
					push @authFields , $af;
					push @authVals , $aref->{$af};
				}
			}
		}

	# ref is authority is always false, and just to make sure that
	#  makes sense, we're going to steal the author info from the
	#  original combination's reference if we don't have it already
		else	{
			my $rsql = "SELECT * FROM refs WHERE reference_no=" . $aref->{'reference_no'};
			my $rref = ${$sql->getData($rsql)}[0];
			for my $af ( @origAuthFields )	{
				if ( $rref->{$af} )	{
					push @authFields , $af;
					push @authVals , $rref->{$af};
				}
			}
		}

	# have to store created date
		push @authFields, "created";
		push @authVals , now();

	# go for it
	# Poling screwed this up by getting the first authority record
	#  that matches the entered name, instead of the last
		my $insertsql = "INSERT INTO authorities (" . join(',', @authFields ) . ") VALUES ('" . join("', '", @authVals) . "')";
		my $sql = $self->getTransactionManager();
		$sql->getData($insertsql);
	# get the LAST authority record matching this name JA 25.4.04
		my $matchsql = "SELECT taxon_no FROM authorities WHERE taxon_name='" . $parentTaxonName . "'";
		my @parentrefs = @{$sql->getData($matchsql)};
		$fieldsToEnter{parent_no} = $parentrefs[$#parentrefs]->{taxon_no}; 
	}

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
    $o->{reference_no} = $q->param('reference_no'); # bad hack so link in summary display ok PS
	$o->displayOpinionSummary($isNewEntry);
	
}





# displays info about the opinion record the user just entered...
# pass it a boolean
# is it a new entry or not..
sub displayOpinionSummary {
	my Opinion $self = shift;
	my $newEntry = shift;

	my $sql = $self->getTransactionManager();

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

	# following computation could be done with Poling's objects, but
	#  he can go to hell - actually faster and easier this way
		my $asql = "SELECT taxon_name,taxon_rank FROM authorities WHERE taxon_no=" . $self->childNumber();
		my $ref = ${$sql->getData($asql)}[0];

		my $tempTaxon = $ref->{taxon_name};
		$tempTaxon =~ s/ /+/g;
		
		my $opinionHTML = $opinion->formatAsHTML();
		$opinionHTML =~ s/according to/of/i;
		
		print "<H3> The opinion $opinionHTML has been $enterupdate the database</H3>";
		
		print "<center>
		<p><A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonInfoResults&taxon_rank=" . $ref->{taxon_rank} . "&genus_name=" . $tempTaxon . "+(" . $self->childNumber() .")\"><B>Get&nbsp;general&nbsp;information&nbsp;about&nbsp;" . $ref->{taxon_name} . "</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionForm&opinion_no=" . $self->{opinion_no} ."\"><B>Edit&nbsp;this&nbsp;opinion</B></A>&nbsp;-
<<<<<<< Opinion.pm
		<A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionList&taxon_no=" . $self->childNumber() . " \"><B>Add/edit&nbsp;a&nbsp;different&nbsp;opinion&nbsp;about&nbsp;" . $ref->{taxon_name} . "</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&amp;goal=opinion\"><B>Add/edit&nbsp;an&nbsp;opinion&nbsp;about&nbsp;another&nbsp;taxon</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&goal=authority\"><B>Add/edit&nbsp;authority&nbsp;data&nbsp;about&nbsp;another&nbsp;taxon</B></A></p>
=======
		<A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=" . $self->{reference_no} . " \"><B>Edit&nbsp;a&nbsp;different&nbsp;opinion&nbsp;with&nbsp;same&nbsp;reference</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=startDisplayOpinionChoiceForm&taxon_no=" . $self->childNumber() . " \"><B>Add/edit&nbsp;a&nbsp;different&nbsp;opinion&nbsp;about&nbsp;" . $ref->{taxon_name} . "</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&amp;goal=opinion\"><B>Add/edit&nbsp;an&nbsp;opinion&nbsp;about&nbsp;another&nbsp;taxon</B></A>&nbsp;-
        <A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&goal=authority\"><B>Add/edit&nbsp;authority&nbsp;data&nbsp;about&nbsp;another&nbsp;taxon</B></A></p>

>>>>>>> 1.44
		</center>";
	}
	
	print "<BR>";
	print "</CENTER>";
	
	print main::stdIncludes("std_page_bottom");
}

# Displays a form which lists all opinions that currently exist
# for a reference no/taxon
# Moved/Adapted from Taxon::displayOpinionChoiceForm PS 01/24/2004
sub displayOpinionChoiceForm{
    my $dbt = shift;
    my $s = shift;
    my $q = shift;
    my $suppressAddNew = (shift || 0);
    my $dbh = $dbt->dbh;

    my $orig_taxon_no = TaxonInfo::getOriginalCombination($dbt,$q->param("taxon_no"));
    
    my $sql = "SELECT o.opinion_no FROM opinions o "; 
    if ($q->param("taxon_no")) {
        $sql .= " LEFT JOIN refs r ON r.reference_no=o.reference_no";
        $sql .= " WHERE o.child_no=$orig_taxon_no";
        $sql .= " ORDER BY IF((o.ref_has_opinion != 'YES' AND o.pubyr), o.pubyr, r.pubyr) ASC";
    } elsif ($q->param("reference_no")) {
        $sql .= " LEFT JOIN authorities a ON a.taxon_no=o.child_no";
        $sql .= " WHERE o.reference_no=".int($q->param("reference_no"));
        $sql .= " ORDER BY a.taxon_name";
    } else {
        print "No terms were entered.";
    }
    my @results = @{$dbt->getData($sql)};

    if (scalar(@results) == 0 && $suppressAddNew) {
        print "<center><h3>No opinions found</h3></center><br><br>";
        return;
    }
    print "<center>";
    if ($q->param("taxon_no")) {
        my $t = Taxon->new();
        $t->setWithTaxonNumber($orig_taxon_no);
        print "<h3>Which opinion about ".$t->taxonNameHTML()." do you want to edit?</h3>\n";

        if ($orig_taxon_no != $q->param('taxon_no')) {
            my $t2 = Taxon->new();
            $t2->setWithTaxonNumber($q->param('taxon_no'));
            print "<I>(Recombination of ".$t2->taxonNameHTML().")</I><BR>";
        }
        print "<BR>"; 
    } else {
        print "<br><h3>Select an opinion to edit:</h3><br>\n";
    }
                                                                                                                                                             
    print qq|<form method="POST" action="bridge.pl">
             <input type="hidden" name="action" value="displayOpinionForm">\n|;
    if ($q->param("taxon_no")) {
        print qq|<input type="hidden" name="taxon_no" value="$orig_taxon_no">\n|;
    }
    print "<table border=0>";
    foreach my $row (@results) {
        my $o = Opinion->new();
        $o->setWithOpinionNumber($row->{'opinion_no'});
        my $html = $o->formatAsHTML();
        print "<tr>".
              qq|<td><input type="radio" id="radio" name="opinion_no" value="$row->{opinion_no}"></td>|.
              "<td>$html</td>".
              "</tr>\n";
    }
    unless ($suppressAddNew) {
        print "<TR><TD><INPUT type=\"radio\" name=\"opinion_no\" id=\"opinion_no\" value=\"-1\" checked></td><td>Create a <b>new</b> opinion record</TD></TR>\n";
    }    
       
    if ($suppressAddNew) {
        print qq|<tr><td align="center" colspan=2><p><input type=submit value="Edit"></p><br></td></tr>|;
    } else {
        print qq|<tr><td align="center" colspan=2><p><input type=submit value="Submit"></p><br></td></tr>|;
    }
    print qq|<tr><td align="left" colspan=2><p><span class="tiny">An "opinion" is when an author classifies or synonymizes a taxon.<br>\nSelect an old opinion if it was entered incorrectly or incompletely.<br>\nCreate a new one if the author whose opinion you are looking at right now is not in the above list.<br>\n|;
    print qq|You may want to read the <a href="javascript:tipsPopup('/public/tips/taxonomy_tips.html')">tip sheet</a>.</span></p>\n|;
    print "</span></p></td></tr>\n";
    print "</table>\n";
    print "</form>\n";
    print "</center>\n";
}

1;


