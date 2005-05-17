#!/usr/bin/perl -w

# created by rjp, 1/2004.
#
# Represents a single taxon from the authorities database table. 
# Note: if the taxon doesn't exist in the authorities table, then not all methods will work,
# for example, asking for the taxon number for a taxon which isn't in the database will
# return an empty string.
#
# Includes various methods for setting the taxon such as by name/number, accessors for
# various authority table fields such as taxon_rank, and methods to fetch/submit information
# from the database.
#
# Reworked PS 04/30/2005 - reworked accessor methods to make sense.  Also, return undef
# if the taxon isn't in the authorities table, since a Taxon object with a taxon doesn't actually make sense

package Taxon;

use strict;

use Constants;

use DBI;
use DBConnection;
use DBTransactionManager;
use Errors;
use Data::Dumper;
use CGI::Carp;

use Reference;

use fields qw(dbt DBrow);
				
# includes the following public methods
# -------------------------------------
# $var = $o->get('classFieldname') - i.e. $o->get('taxon_rank')
# $var = $o->getRow() - gets the database row hash
# $var = $o->pubyr() - publication year
# $var = $o->authors() - formatted string of authors
# $var = $o->taxonNameHTML() - html formatted name

# Called by $o = Taxon->new($dbt,$taxon_no)
#  or $o = Taxon->new($dbt,$taxon_name).  If $taxon_name is ambiguous (a homonym), or it can't
# find it in the DB, returns undef.
sub new {
	my $class = shift;
    my $dbt = shift;
    my $name_or_no = shift;
	my Taxon $self = fields::new($class);
    $self->{'dbt'}=$dbt;

    my ($sql,@results);
    if ($name_or_no =~ /^\d+$/) {
        $sql = "SELECT * FROM authorities where taxon_no=$name_or_no";
        @results = @{$dbt->getData($sql)};
    } elsif ($name_or_no) {
        $sql = "SELECT * FROM authorities where taxon_name=".$dbt->dbh->quote($name_or_no);
        @results = @{$dbt->getData($sql)};
    } else {
        carp "Could not create taxon object with passed variable $name_or_no.";
        return;
    }
    if (@results) {
        $self->{'DBrow'} = $results[0];
    } 
	return $self;
}




####
## Some accessors for the Taxon.
##
####

# return the taxonName for the initially specifed taxon.
# but with proper italicization
sub taxonNameHTML {
	my Taxon $self = shift;
    if ($self->get('taxon_rank') =~ /(?:species|genus)$/) {
		return "<i>" . $self->get('taxon_name') . "</i>";
	} else {
		return $self->get('taxon_name');	
	}
}

# Universal accessor
sub get {
    my Taxon $self = shift;
    my $fieldName = shift;
    if ($fieldName) {
        return $self->{'DBrow'}{$fieldName};
    } else {
        return(keys(%{$self->{'DBrow'}}));
    }
}

# Get the raw underlying database hash;
sub getRow {
    my Taxon $self = shift;
    return $self->{'DBrow'};
}


# returns the authors of the authority record for this taxon (if any)
sub authors {
	my Taxon $self = shift;
	
	# get all info from the database about this record.
	my $hr = $self->getRow();
	
	if (!$hr) {
		return '';	
	}
	
	my $auth;
	
	if ($hr->{ref_is_authority}) {
		# then get the author info for that reference
		my $ref = Reference->new($self->{'dbt'},$hr->{'reference_no'});
		$auth = $ref->authorsWithInitials();
	} else {
	
		$auth = Globals::formatAuthors(1, $hr->{author1init}, $hr->{author1last}, $hr->{author2init}, $hr->{author2last}, $hr->{otherauthors} );
		$auth .= " " . $self->pubyr();
	}
	
	return $auth;
}

sub pubyr {
	my Taxon $self = shift;

	# get all info from the database about this record.
	my $hr = $self->getRow();
	
	if (!$hr) {
		return '';	
	}

	# JA: Poling originally just returned hr's pubyr, but that depends on
	#  whether the ref is authority
	if ( ! $hr->{ref_is_authority} )	{
		return $hr->{pubyr};
	}

	# okay, so because ref is authority we need to grab the pubyr off of
	#  that ref
	# I hate to do it, but I'm using Poling's ridiculously baroque
	#  Reference module to do so just for consistency
	my $ref = Reference->new($self->{'dbt'},$hr->{'reference_no'});
	return $ref->{pubyr};
}


###
## End of simple accessors
###


# Pass this an HTMLBuilder object,
# a session object, and the CGI object.
# 
# Displays the form which allows users to enter/edit authority
# table data.
#
# rjp, 3/2004
sub displayAuthorityForm {
    my $dbt = shift; 
	my $hbo = shift;
	my $s = shift;
	my $q = shift;
    my $error_message = shift;
	
	my %fields;  # a hash of fields and values that
				 # we'll pass to HTMLBuilder to pop. the form.
				 
	# Fields we'll pass to HTMLBuilder that the user can't edit.
	# (basically, any field which is not empty or 0 in the database,
	# if the authorizer is not the original authorizer of the record).
	my @nonEditables; 	
	
	if ((!$dbt) || (!$hbo) || (! $s) || (! $q)) {
        carp "displayAuthorityform had invalid arguments passed to it";
		return;
	}


    # Simple variable assignments
    my $isNewEntry = ($q->param('taxon_no') > 0) ? 0 : 1;
    my $reSubmission = ($error_message) ? 1 : 0;
    
	# if the taxon is already in the authorities table, grab it
    my $t;
    if (!$isNewEntry) {
        $t = Taxon->new($dbt,$q->param('taxon_no'));
        if (!$t) {
            carp "Could not create taxon object in displayAuthorityForm for taxon_no ".$q->param('taxon_no');
            return;
        }
    }

    # grab previous fields
	if ($reSubmission) {
        %fields = %{$q->Vars()};
	} elsif (!$isNewEntry) {
        %fields = %{$t->getRow()};
    } else { # brand new, first submission
	    $fields{'taxon_name'} = $q->param('taxon_name');
		$fields{'reference_no'} = $s->get('reference_no');
    }    

	# fill out the authorizer/enterer/modifier info at the bottom of the page
	if (!$isNewEntry) {
		if ($fields{'authorizer_no'}) { 
            $fields{'authorizer_name'} = " <B>Authorizer:</B> " . Person::getPersonName($dbt,$fields{'authorizer_no'}); 
        }
		if ($fields{'enterer_no'}) { 
            $fields{'enterer_name'} = " <B>Enterer:</B> " . Person::getPersonName($dbt,$fields{'enterer_no'}); 
        }
		if ($fields{'modifier_no'}) { 
            $fields{'modifier_name'} = " <B>Modifier:</B> ".Person::getPersonName($dbt,$fields{'modifier_no'}); 
        }
	}
	
	
	# if the type_taxon_no exists, then grab the name for that taxon.
	if ((!$reSubmission) && ($fields{'type_taxon_no'})) {
        my $type_taxon = Taxon->new($dbt,$fields{'type_taxon_no'});
		$fields{'type_taxon_name'} = $type_taxon->get('taxon_name');
	}
	
	
	# populate the correct pages/figures fields depending
	# on the ref_is_authority value.
	#print "ref_is_authority = " . $fields{'ref_is_authority'};
	if ($fields{'ref_is_authority'} eq 'YES') {
		# reference_no is the authority
		$fields{'ref_is_authority_checked'} = 'checked';
		$fields{'ref_is_authority_notchecked'} = '';
	
	} else {
		# reference_no is not the authority
		
		if ((!$isNewEntry) || $reSubmission) {
			# we only want to check a reference radio button
			# if they're editing an old record.  This will force them to choose
			# one for a new record.  However, if it's the second time
			# through, then it's okay to check one since they already did.
			$fields{'ref_is_authority_checked'} = '';
			$fields{'ref_is_authority_notchecked'} = 'checked';
			
			$fields{'ref_is_authority'} = 'NO';
		}
		
		if (!$reSubmission) {
			$fields{'2nd_pages'} = $fields{'pages'};
			$fields{'2nd_figures'} = $fields{'figures'};
			$fields{'pages'} = '';
			$fields{'figures'} = '';
		}
	}
	
		
	
	
	# Now we need to deal with the taxon rank popup menu.
	# If we've already displayed the form and the user is now making changes
	# from an error message, then we should use the rank they chose on the last form.
	# Else, if it's the first display of the form, then we use the rank from the database
	# if it's an edit of an old record, or we use the rank from the spacing of the name
	# they typed in if it's a new record.
	
	my $rankToUse;
	if ($reSubmission) {
		$rankToUse = $q->param('taxon_rank'); 
	} else { 
		# first time
		if ($isNewEntry) {
	        # Figure out the rank based on spacing of the name.
			$rankToUse = Validation::taxonRank($q->param('taxon_name'));
	        if ($rankToUse eq 'higher') {
		        # the popup menu doesn't have "higher", so use "genus" instead.
		        $rankToUse = 'genus';
	        }
		} else {
			# not a new entry
			$rankToUse = $t->get('taxon_rank');
		}
	}
	$fields{'taxon_rank'} = $rankToUse;
	
	# if the rank is species, then display the type_specimen input
	# field.  Otherwise display the type_taxon_name field.
	
	if ($rankToUse eq 'species' || $rankToUse eq 'subspecies') {
		# remove the type_taxon_name field.
		$fields{'OPTIONAL_type_taxon_name'} = 0;
	} else {
		# remove the type_specimen field.	
		$fields{'OPTIONAL_type_specimen'} = 0;
	}
	
	
	## If this is a new species or subspecies, then we will automatically
	# create an opinion record with a state of 'belongs to'.  However, we 
	# have to make sure that we use the correct parent taxon if we have multiple
	# ones in the database.  For example, if they enter a  new taxon named
	# 'Equus newtaxon' and we have three entries in authorities for 'Equus'
	# then we should present a menu and ask them which one to use.

	if ($isNewEntry && ($rankToUse eq 'species' || $rankToUse eq 'subspecies')) {
		my $tname = $fields{taxon_name};
		my ($one, $two, $three) = split(/ /, $tname);
		
		my $name;
		if ($rankToUse eq 'species') {
			$name = $one;
		} else {
			$name = "$one $two";
		}
		
		# figure out how many authoritiy records could be the possible parent.
		my $count = ${$dbt->getData("SELECT COUNT(*) as c FROM authorities WHERE taxon_name = '" . $name . "'")}[0]->{c};
		
		
		# if only one record, then we don't have to ask the user anything.
		# otherwise, we should ask them to pick which one.
		my $select;
		my $errors = Errors->new();
		my $parentRankToPrint;

		my $parentRankShouldBe;
		if ($rankToUse eq 'species') {
			$parentRankShouldBe = "(taxon_rank = 'genus' OR taxon_rank = 'subgenus')";
			$parentRankToPrint = "genus or subgenus";
		} elsif ($rankToUse eq 'subspecies') {
			$parentRankShouldBe = "taxon_rank = 'species'";
			$parentRankToPrint = "species";
		}

		
		if ($count >= 1) {
			# make sure that the parent we select is the correct parent,
			# for example, we don't want to grab a family or something higher
			# by accident.
			
			my $results = $dbt->getData("SELECT taxon_no, taxon_name FROM authorities WHERE taxon_name = '" . $name . "' AND $parentRankShouldBe");
		
			my $select;
			
			my $chosen;
			foreach my $row (@$results) {
				my $taxon = Taxon->new($dbt,$row->{'taxon_no'});
	
				# if they are redisplaying the form, we want to choose
				# the appropriate one.
				if ($q->param('parent_taxon_no') eq $row->{'taxon_no'}) {
					$chosen = 'selected';
				} else {
					$chosen = '';
				}
				
				$select .= "<option value=\"$row->{taxon_no}\" $chosen>";
				$select .= $taxon->get('taxon_name') . " " . $taxon->authors();
		        # tack on the closest higher order name
				my %master_class=%{Classification::get_classification_hash($dbt,"parent",[$row->{'taxon_no'}])};
				$select .= "[".$master_class{$row->{'taxon_no'}}."]";
				$select .= "</option>\n";

			}
			
			$fields{parent_taxon_popup} = "<b>Belongs to:</b>
			<SELECT name=\"parent_taxon_no\">
			$select
			</SELECT>";
		} else {
			# count = 0, so we need to warn them to enter the parent taxon first.
			$errors->add("The $parentRankToPrint '$name' for this $rankToUse doesn't exist in our database.  Please <A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_name=$name\">create a new authority record for '$name'</A> before trying to add this $rankToUse.");
			
			$errors->setDisplayEndingMessage(0); 
			
			print $errors->errorMessage();
			return;
		}
	}

	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.  However, if they're superuser
	# (alroy, alroy), then let them edit anything.
	#
	# otherwise, they can edit any field.
	my $sesAuth = $s->get('authorizer_no');
	
	if ($s->isSuperUser()) {
		$fields{'message'} = "<p align=center><i>You are the superuser, so you can edit any field in this record!</i></p>";
	} elsif (!$isNewEntry && $sesAuth != $t->get('authorizer_no') && $t->get('authorizer_no') != 0) {
		# grab the authorizer name for this record.
		my $authName = Person::getPersonName($dbt,$fields{authorizer_no});
	
		$fields{'message'} = "<p align=center><i>This record was created by a different authorizer ($authName) so you can only edit empty fields.</i></p>";
		
		# we should always make the ref_is_authority radio buttons disabled
		# because only the original authorizer can edit these.
		
		push (@nonEditables, 'ref_is_authority');
		
		# depending on the status of the ref_is_authority radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_is_authority'} eq 'YES') {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
		}
		
				
		# find all fields in the database record which are not empty and add them to the list.
		foreach my $f ($t->get()) {
			if ($t->get($f)) {
				push(@nonEditables, $f);
			}
		}
		
		# we'll also have to add a few fields separately since they don't exist in the database,
		# but only in our form.
		
		if ($fields{'2nd_pages'}) { push(@nonEditables, '2nd_pages'); }
		if ($fields{'2nd_figures'}) { push(@nonEditables, '2nd_figures'); }
		if ($fields{'type_taxon_name'}) { push(@nonEditables, 'type_taxon_name'); }
		if ($fields{'type_specimen'}) { push(@nonEditables, 'type_specimen'); }
		
		push(@nonEditables, 'taxon_name');
	}
	
	
	## Make the taxon_name non editable if this is a new entry to simplify things
	## New addition, 3/22/2004
	if ($isNewEntry) {
		push(@nonEditables, 'taxon_name');
	}

    # add in the error message
    if ($error_message) {
        $fields{'error_message'}=$error_message;
    }
	
	# print the form	
	print $hbo->newPopulateHTML("add_enter_authority", \%fields, \@nonEditables);
}




# Call this when you want to submit an authority form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
#
# Note: If the user submits a *new* authority which has a rank of species (or subspecies),
# we should *automatically* create an opinion record with status "belongs to" to
# show that this species belongs to the genus in its name.
#
# rjp, 3/2004.
sub submitAuthorityForm {
    my $dbt = shift;
	my $hbo = shift;
	my $s = shift;		# the cgi parameters
	my $q = shift;		# session

	if ((!$dbt) || (!$hbo) || (!$s) || (!$q)) {
		carp("Taxon::submitAuthorityForm had invalid arguments passed to it.");
		return;	
	}
	
	my $errors = Errors->new();
    my @warnings = ();

    # Simple variable assignments
    my $isNewEntry = ($q->param('taxon_no') > 0) ? 0 : 1;

    # if the taxon is already in the authorities table, grab it
    my $t;
    if (!$isNewEntry) {
        $t = Taxon->new($dbt,$q->param('taxon_no'));
        if (!$t) {
            carp "Could not create taxon object in submitAuthorityForm for taxon_no ".$q->param('taxon_no');
            return;
        }
    }

	
	# build up a hash of fields/values to enter into the database
	my %fieldsToEnter;
	
	if ($isNewEntry) {
		$fieldsToEnter{authorizer_no} = $s->get('authorizer_no');
		$fieldsToEnter{enterer_no} = $s->get('enterer_no');
		$fieldsToEnter{reference_no} = $s->get('reference_no');
		
		if (! $fieldsToEnter{reference_no} ) {
			$errors->add("You must set your current reference before submitting a new authority");	
		}
		
	} else {
		$fieldsToEnter{modifier_no} = $s->get('enterer_no');	
	}
	
	
	if (($q->param('ref_is_authority') ne 'YES') && 
		($q->param('ref_is_authority') ne 'NO')) {
		$errors->add("You must choose one of the reference radio buttons");
	}
	
	
	# merge the pages and 2nd_pages, figures and 2nd_figures fields together
	# since they are one field in the database.
	if ($q->param('ref_is_authority') eq 'NO') {
		$fieldsToEnter{'pages'} = $q->param('2nd_pages');
		$fieldsToEnter{'figures'} = $q->param('2nd_figures');

	# commented out 10.5.04 by JA because we often need to add (say) genera
	#  without any data when we create and classify species for which we
	#  do have data
#		if (! $q->param('author1last')) {
#			$errors->add('You must enter at least the last name of a first author');	
#		}
		
		# make sure the pages/figures fields above this are empty.
		if ($q->param('pages') || $q->param('figures')) {
			$errors->add("Don't enter pages or figures for a primary reference if you chose the 'named in an earlier publication' radio button");	
		}
	
        # make sure the format of the author names is proper
        if  ($q->param('author1init') && ! Validation::properInitial($q->param('author1init'))) {
            $errors->add("The first author's initials are improperly formatted");
        }
        if  ($q->param('author2init') && ! Validation::properInitial($q->param('author2init'))) {
            $errors->add("The second author's initials are improperly formatted");
        }
        if  ( $q->param('author1last') && !Validation::properLastName($q->param('author1last')) ) {
            $errors->add("The first author's last name is improperly formatted");
        }
        if  ( $q->param('author2last') && !Validation::properLastName($q->param('author2last')) ) {
            $errors->add("The second author's last name is improperly formatted");
        }
        if ($q->param('otherauthors') && !$q->param('author2last') ) {
            $errors->add("Don't enter other author names if you haven't entered a second author");
        }	
			
		if ($q->param('pubyr')) {
            my $pubyr = $q->param('pubyr');

			if (! Validation::properYear( $pubyr ) ) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new($dbt,$q->param('reference_no'));
			if ($pubyr > $ref->get('pubyr')) {
				$errors->add("The publication year ($pubyr) can't be more 
				recent than that of the primary reference (" . $ref->get('pubyr') . ")");
			}
		}
	} else {
		# ref_is_authority is YES
		# so make sure the other publication info is empty.
		if ($q->param('author1init') || $q->param('author1last') || 
            $q->param('author2init') || $q->param('author2last') || 
            $q->param('otherauthors')|| $q->param('pubyr') || 
            $q->param('2nd_pages')   || $q->param('2nd_figures')) {
			$errors->add("Don't enter other publication information if you chose the 'first named in primary reference' radio button");	
		}
	}
	
	# check and make sure the taxon_name field in the form makes sense
	if (!($q->param('taxon_name'))) {
		$errors->add("You can't submit the form with an empty taxon name!");	
	}
	
	# to prevent complications, we will prevent the user from changing 
	# a genus name on a species, or a species name on a subspecies if they
	# are editing an old record.
	if (!$isNewEntry && $t->get('taxon_rank') =~ /species/) {
        my @new_name = split(/ /,$q->param('taxon_name'));
        my @old_name = split(/ /,$t->get('taxon_name'));

        my $error = 0;
        if (scalar(@new_name) != scalar(@old_name)) {
            $error =1;
        }
		# make sure no higher order names are changes (i.e. genus or subgenus if its a species)
        for (my $i=0;$i<$#new_name;$i++) {
			if ($new_name[$i] ne $old_name[$i]) {
                $error=1;
            }
        }
       
        if ($error) {
		    if ($t->get('taxon_rank') eq 'species') {
				$errors->add("You can't change the genus or subgenus name of a species that already exists.  Contact the database manager if you need to do this.");
		    } elsif ($t->get('taxon_rank') eq 'subspecies') {
				$errors->add("You can't change the genus, subgenus, or species name of a subspecies that already exists.  Contact the database manager if you need to do this.");
            }    
        }
	}
	
	
	{ # so we have our own scope for these variables.
		my $taxon_name = $q->param('taxon_name');
		my $rankFromSpaces = Validation::taxonRank($taxon_name);
		my $trank = $q->param('taxon_rank');
		my $er = 0;
		
		
		if ($rankFromSpaces eq 'invalid') {
			$errors->add("The taxon's name is invalid; please check spacing and capitalization");	
		}
		
		if (Validation::looksLikeBadSubgenus($taxon_name)) {
			$errors->add("If you are attempting to enter a subgenus, only enter the subgenus name and don't use parentheses");
		}
		
		if (($rankFromSpaces eq 'subspecies' && $trank ne 'subspecies') ||
			($rankFromSpaces eq 'species'    && $trank ne 'species') ||
			($rankFromSpaces eq 'higher'     && $trank =~ /species/)) {
			$errors->add("The original rank '$trank' doesn't match the spacing of the taxon name '$taxon_name'");
		}
	}
	
	# Now loop through all fields submitted from the form.
	# If a field is not empty, then see if we're allowed to edit it in the database.
	# If we can edit it, then make sure the name is correct (since a few fields like
	# 2nd_pages, etc. don't match the database field names) and add it to the 
	# %fieldsToEnter hash.
	
	
	#Debug::dbPrint("dbFields = ");
	#Debug::printHash(\%dbFields);
	
	# this $editAny variable is true if they can edit any field,
	# false if they can't.
	my $editAny = ($isNewEntry || $s->isSuperUser() || $s->get('authorizer_no') == $t->get('authorizer_no')) ? 1 : 0;
	foreach my $formField ($q->param()) {
		#if (! $q->param($formField)) {
		#	next;  # don't worry about empty fields.	
		#}
		
		my $okayToEdit = $editAny;
		if (! $okayToEdit) {
			# then we should do some more checks, because maybe they are allowed
			# to edit it aferall..  If the field they want to edit is empty in the
			# database, then they can edit it.
			
			if (! $t->get($formField)) {
				# If the field in the database is empty, then it's okay
				# to edit it.
				$okayToEdit = 1;
			}
		}
		
		if ($okayToEdit) {
			
			# if the value isn't already in our fields to enter
			if (! $fieldsToEnter{$formField}) {
				$fieldsToEnter{$formField} = $q->param($formField);
			}
		}
		
		#Debug::dbPrint("okayToEdit = $okayToEdit, $formField = " . $q->param($formField));
		
	} # end foreach formField.


	my $taxonRank = $q->param('taxon_rank'); 	# rank in popup menu

	
	# If they entered a type_taxon_name, then we'll have to look it
	# up in the database because it's stored in the authorities table as a 
	# type_taxon_no, not a name.
	#
	# Also, make sure the rank of the type taxon they type in makes sense.
	# If the rank of the authority record is genus or subgenus, then the type taxon rank should
	# be species.  	If the authority record is not genus or subgenus (ie, anything else), then
	# the type taxon rank should *not* be species or subspecies.
	if ($q->param('type_taxon_name')) {
		
		# check the spacing/capitilization of the type taxon name
		my $ttRankFromSpaces = Validation::taxonRank($fieldsToEnter{type_taxon_name});
		if ($ttRankFromSpaces eq 'invalid') {
			$errors->add("The type taxon's name is invalid; please check spacing and capitalization");	
		}
		
		if (Validation::looksLikeBadSubgenus($fieldsToEnter{type_taxon_name})) {
			$errors->add("Invalid type taxon format; don't use parentheses");
		}


        my @type_taxa = TaxonInfo::getTaxon($dbt,'taxon_name'=>$fieldsToEnter{'type_taxon_name'},'get_reference'=>1);
		
		if (!@type_taxa) {
			# if it doesn't exist, tell them to go enter it first.
			$errors->add("The type taxon '" . $q->param('type_taxon_name') . "' doesn't exist in our database.  If you made a typo, correct it and try again.  Otherwise, please submit this form without the type taxon and then go back and add it later (after you have added an authority record for the type taxon).");
		} else {
			# the type taxon exists in the database, so do some checks on it.
			
			# check to make sure the rank of the type taxon makes sense.
			my $ttaxon = $type_taxa[0];
			
			if (($taxonRank eq 'genus') || ($taxonRank eq 'subgenus')) {
				# then the type taxon rank must be species
				if ($ttaxon->{'taxon_rank'} ne 'species') {
					$errors->add("The type taxon's rank should be a species");	
				}
			} else {
				# for any other rank, the type taxon rank must not be species.
				if ($ttaxon->{'taxon_rank'} eq 'species') {
					$errors->add("The type taxon's rank shouldn't be a species");	
				}
			}

			# make sure the publicaion date of the type taxon in the authorities
			# table is <= the publication date of the current authority record
			# which is either in the pubyr field or the reference.
			my $pubyrToCheck;
			
			if ($q->param('ref_is_authority') eq 'YES') {
				my $ref = Reference->new($dbt,$q->param('reference_no'));
				$pubyrToCheck = $ref->get('pubyr');
			} else {
				$pubyrToCheck = $q->param('pubyr');
			}
			
			if ($ttaxon->{'pubyr'} > $pubyrToCheck ) {
				push @warnings,"The type taxon was published after the current taxon.";
			}
			
					
			$fieldsToEnter{type_taxon_no} = $ttaxon->{'taxon_no'};
		}
	}
	
	# if they didn't enter a type taxon, then set the type_taxon number to zero.
	if (! $q->param('type_taxon_name')) {
		$fieldsToEnter{type_taxon_no} = 0;	
	}
	

	$fieldsToEnter{taxon_name} = $q->param('taxon_name');
	

	# Delete some fields that may be present since these don't correspond
	# to fields in the database table.. (ie, they're in the form, but not in the table)
	delete $fieldsToEnter{action};
	delete $fieldsToEnter{'2nd_authors'};
	delete $fieldsToEnter{'2nd_figures'};
	
	# correct the ref_is_authority field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($fieldsToEnter{ref_is_authority} eq 'NO') {
		$fieldsToEnter{ref_is_authority} = '';
	}
	
	# If the rank was species or subspecies, then we also need to insert
	# an opinion record automatically which has the state of "belongs to"
	# For example, if the child taxon is "Equus blah" then we need to 
	# make sure we have an opinion that it belongs to "Equus".
	#
	# 3/22/2004, this is bit of a  **HACK** for now.  Eventually,
	# the opinion object should do this for us (?)
	my $parentTaxon;

	if ( $isNewEntry && (($taxonRank eq 'species') || ($taxonRank eq 'subspecies')) ) {
		# we want to do this for new entries & for edits.
				
		$parentTaxon = Taxon->new($dbt, $q->param('parent_taxon_no'));
		
		if (! $parentTaxon->get('taxon_no')) {
			$errors->add("The parent taxon '" . $parentTaxon->get('taxon_name') . 
			"' that this $taxonRank belongs to doesn't exist in our 
			database.  Please add an authority record for this $taxonRank
			before continuing.");
		}
	}
	## end of hack
	####
	
	# at this point, we should have a nice hash array (%fieldsToEnter) of
	# fields and values to enter into the authorities table.
	
	
	# *** NOTE, if they try to enter a record which has the same name and
	# taxon_rank as an existing record, we should display a warning page stating
	# this fact..  However, if they *really* want to submit a duplicate, we should 
	# let them.  
	#
	# This only applies to new entries, and to edits where they changed the taxon_name
	# field to be the name of a different taxon which already exists.
	if ($q->param('confirmed_taxon_name') ne $q->param('taxon_name')) {
        my @taxon = TaxonInfo::getTaxon($dbt,'taxon_name'=>$fieldsToEnter{'taxon_name'},'get_reference'=>1);
        my $taxonExists = scalar(@taxon);
        
		if (($isNewEntry && $taxonExists) ||
		    (!$isNewEntry && $taxonExists && $q->param('taxon_name') ne $t->get('taxon_name'))) {
            my @pub_info = ();
            foreach my $row (@taxon) {
                push @pub_info, Reference::formatShortRef($row);
            }
            my $plural = ($taxonExists == 1) ? "" : "s";
            $q->param('confirmed_taxon_name'=>$q->param('taxon_name'));
			$errors->add("The taxon already appears $taxonExists time$plural in the database: ".join(", ",@pub_info).". If you really want to submit this record, hit submit again.");
		}
	}
	
	if ($errors->count() > 0) {
        # If theres an error message, then we know its the second time through
		my $message = $errors->errorMessage();
		displayAuthorityForm($dbt,$hbo, $s, $q, $message);
		return;
	}
	
	
	# now we'll actually insert or update into the database.
	my $resultTaxonNumber;
    my $status;
	
	if ($isNewEntry) {
		($status, $resultTaxonNumber) = $dbt->insertRecord($s,'authorities', \%fieldsToEnter);
		
		# if the $parentTaxon object exists, then that means that we
		# need to insert an opinion record which says that our taxon
		# belongs to the genus represented in $parentTaxon.
		if ($parentTaxon) {
            # Get original combination for parent no PS 04/22/2005
            my $orig_parent_no = $parentTaxon->get('taxon_no');
            if ($orig_parent_no) {
                $orig_parent_no = TaxonInfo::getOriginalCombination($dbt,$orig_parent_no);
            }    
			
			my %opinionHash = (
                'status'=>'belongs to',
                'child_no'=>$resultTaxonNumber,
                'child_spelling_no'=>$resultTaxonNumber,
                'parent_no'=>$orig_parent_no,
                'parent_spelling_no'=>$parentTaxon->get('taxon_no'),
                'ref_has_opinion'=>$fieldsToEnter{'ref_is_authority'}
            );
            my @fields = ('reference_no','author1init','author1last','author2init','author2last','otherauthors','pubyr','pages','figures');
            $opinionHash{$_} = $fieldsToEnter{$_} for @fields;
		
            $dbt->insertRecord($s,'opinions',\%opinionHash);
		}

		# JA 2.4.04
		# if the taxon name is unique, find matches to it in the
		#  occurrences table and set the taxon numbers appropriately

		# start with a test for uniqueness
		my $mysql = "SELECT count(*) AS c FROM authorities WHERE taxon_name='" . $fieldsToEnter{taxon_name} . "'";
		if ( ${$dbt->getData($mysql)}[0]->{'c'} == 1 )	{

			# start composing update sql
			# NOTE: in theory, taxon no for matching records always
			#  should be zero, unless the new name is a species and
			#  some matching records were set on the basis of their
			#  genus, in which case we assume users will want the
			#  new number for the species instead; that's why there
			#  is no test to make sure the taxon no is empty
			$mysql = "UPDATE occurrences SET modified=modified,taxon_no=" . $resultTaxonNumber . " WHERE ";

			# if the name has a space then match on both the
			#  genus and species name fields
			if ( $fieldsToEnter{taxon_name} =~ / / )	{
				my ($a,$b) = split / /,$fieldsToEnter{taxon_name};
				$mysql .= " genus_name='" . $a ."' AND species_name='" . $b . "'";
			}
			# otherwise match only on the genus name field
			else 	{
				$mysql .= " genus_name='" . $fieldsToEnter{taxon_name} . "'";
			}

			# update the occurrences table
			$dbt->getData($mysql);

			# and then the reidentifications table
			$mysql =~ s/UPDATE occurrences /UPDATE reidentifications /;
			$dbt->getData($mysql);
		}

	} else {
		# if it's an old entry, then we'll update.
		
		# Delete some fields that should never be updated...
		delete $fieldsToEnter{reference_no};
		$resultTaxonNumber = $t->get('taxon_no');
		
		$status = $dbt->updateRecord($s,'authorities', 'taxon_no',$resultTaxonNumber, \%fieldsToEnter);
	}
	
    # displays info about the authority record the user just entered...
	my $enterupdate;
	if ($isNewEntry) {
		$enterupdate = 'entered into';
	} else {
		$enterupdate = 'updated in'	
	}
	print "<CENTER>";
	if (!$status) {
		print "<DIV class=\"warning\">Error inserting/updating authority record.  Please start over and try again.</DIV>";	
	} else {
		
        if (@warnings) {
		    print "<DIV class=\"warning\">";
            print "Warnings inserting/updating authority record:<BR>"; 
            print "<LI>$_</LI>" for (@warnings);
            print "</DIV>";
        }
		print "<H3>" . $fieldsToEnter{'taxon_name'} . " " .Reference::formatShortRef(\%fieldsToEnter). " has been $enterupdate the database</H3>";

		my $tempTaxon = $fieldsToEnter{'taxon_name'};
		$tempTaxon =~ s/ /+/g;

		print "<center>
		<p><A HREF=\"bridge.pl?action=checkTaxonInfo&taxon_no=$resultTaxonNumber\"><B>Get&nbsp;general&nbsp;information&nbsp;about&nbsp;" . $fieldsToEnter{taxon_name} . "</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_no=" . $resultTaxonNumber ."\"><B>Edit&nbsp;authority&nbsp;data&nbsp;about&nbsp;" . $fieldsToEnter{taxon_name} . "</B></A>&nbsp;-
        <A HREF=\"/cgi-bin/bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=" . $fieldsToEnter{'reference_no'}. " \"><B>Edit&nbsp;authority&nbsp;data&nbsp;about&nbsp;a&nbsp;different&nbsp;taxon&nbsp;with&nbsp;same&nbsp;reference</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionChoiceForm&taxon_no=" . $resultTaxonNumber . "\"><B>Add/edit&nbsp;opinion&nbsp;about&nbsp;" . $fieldsToEnter{taxon_name} . "</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityTaxonSearchForm\"><B>Add/edit&nbsp;authority&nbsp;data&nbsp;about&nbsp;another&nbsp;taxon</B></A>&nbsp;-
		<A HREF=\"/cgi-bin/bridge.pl?action=displayOpinionTaxonSearchForm\"><B>Add/edit&nbsp;opinion&nbsp;about&nbsp;another&nbsp;taxon</B></A></p>
		</center>";	

	}
	
	print "<BR>";
	print "</CENTER>";
}


# JA 17,20.8.02
#
# This will print out the name of a taxon, its publication info, and its first parent
# for distinguishing between taxon of the same name
# Assumes correct publication info is conveniently in the record itself
#   I.E. data from getTaxon($dbt,'taxon_name'=>$taxon_name,'get_reference'=>1) -- see function for details
# 
# it returns some HTML to display the authority information.
sub formatAuthorityLine	{
    my $dbt = shift;
    my $taxon = shift;
	my $authLine;

	# Print the name
	# italicize if genus or species.
	if ( $taxon->{'taxon_rank'} =~ /species|genus/) {
		$authLine .= "<i>" . $taxon->{'taxon_name'} . "</i>";
	} else {
		$authLine .= $taxon->{'taxon_name'};
	}
	
	# If the authority is a PBDB ref, retrieve and print it
    my $pub_info = Reference::formatShortRef($taxon);
    if ($pub_info !~ /^\s*$/) {
        $authLine .= ', '.$pub_info;
    }

	# Print name of higher taxon JA 10.4.03
	# Get the status and parent of the most recent opinion
	# shortened by calling selectMostRecentParentOpinion 5.4.04
    # shortened by calling getMostRecentParentOpinion 4/20/2005 PSmore using classification function
    my %parents = %{Classification::get_classification_hash($dbt,'parent',[$taxon->{'taxon_no'}],'names')};

	if ( $parents{$taxon->{'taxon_no'}})	{
		$authLine .= " [".$parents{$taxon->{'taxon_no'}}."]";
	}

	return $authLine;
}

# end of Taxon.pm

1;
