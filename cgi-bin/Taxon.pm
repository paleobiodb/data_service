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

use DBI;
use DBTransactionManager;
use Errors;
use Data::Dumper;
use CGI::Carp;
use URI::Escape;

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
		$auth = $ref->authors() if ($ref);
	} else {
        $auth = Reference::formatShortRef($hr);	
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
	

	# If this taxon is a type taxon for something higher, mark the check box as checked
    if (!$isNewEntry && !$reSubmission && $fields{'taxon_rank'} =~ /species/) {
        my @taxa = getTypeTaxonList($dbt,$fields{'taxon_no'},$fields{'reference_no'});
        $fields{'type_taxon'} = 0;
        foreach my $row (@taxa) {
            if ($row->{'type_taxon_no'} == $fields{'taxon_no'}) {
                $fields{'type_taxon'} = 1;
            }
        }  
    }
    $fields{'type_taxon_checked'} = ($fields{'type_taxon'}) ? 'CHECKED' : '';
	
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
	
	# remove the type taxon stuff, it'll be assigned in opinions
	if (!($rankToUse eq 'species' || $rankToUse eq 'subspecies')) {
		$fields{'OPTIONAL_type_taxon'} = 0;
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
        $fields{'parent_name'} = $name;
		
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
				$select .= "[".$master_class{$row->{'taxon_no'}}."]" if ($master_class{$row->{'taxon_no'}});
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

    # Build extant popup
    my @extant_values = ('','YES','NO');
    $fields{'extant_popup'} = $hbo->buildSelect('extant',\@extant_values,\@extant_values,$fields{'extant'});
    
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
		$fieldsToEnter{'reference_no'} = $s->get('reference_no');
		if (! $fieldsToEnter{'reference_no'} ) {
			$errors->add("You must set your current reference before submitting a new authority");	
		}
        $fieldsToEnter{'type_taxon'} = ($q->param('type_taxon')) ? 1 : 0;
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
			if ($ref && $pubyr > $ref->get('pubyr')) {
				$errors->add("The publication year ($pubyr) can't be more 
				recent than that of the primary reference (" . $ref->get('pubyr') . ")");
			}
		}
        if ($q->param('taxon_rank') =~ /species/) {
            if (!$q->param('author1last')) {
                $errors->add("If entering a species or subspecies, enter at least the last name of the first author");
            }
            if (!$q->param('pubyr')) {
                $errors->add("If entering a species or subspecies, the publication year is required");
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

	$fieldsToEnter{taxon_name} = $q->param('taxon_name');
	
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
    my $resultReferenceNumber = $fieldsToEnter{'reference_no'};
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
		my $sql = "SELECT count(*) AS c FROM authorities WHERE taxon_name='" . $fieldsToEnter{taxon_name} . "'";
		if ( ${$dbt->getData($sql)}[0]->{'c'} == 1 )	{

			# start composing update sql
			# NOTE: in theory, taxon no for matching records always
			#  should be zero, unless the new name is a species and
			#  some matching records were set on the basis of their
			#  genus, in which case we assume users will want the
			#  new number for the species instead; that's why there
			#  is no test to make sure the taxon no is empty
			$sql = "UPDATE occurrences SET modified=modified,taxon_no=" . $resultTaxonNumber . " WHERE ";

			# if the name has a space then match on both the
			#  genus and species name fields
			if ( $fieldsToEnter{taxon_name} =~ / / )	{
				my ($a,$b) = split / /,$fieldsToEnter{taxon_name};
				$sql .= " genus_name='" . $a ."' AND species_name='" . $b . "'";
			}
			# otherwise match only on the genus name field
			else 	{
				$sql .= " genus_name='" . $fieldsToEnter{taxon_name} . "'";
			}

			# update the occurrences table
			$dbt->getData($sql);
            main::dbg("sql to update occs: $sql");

			# and then the reidentifications table
			$sql =~ s/UPDATE occurrences /UPDATE reidentifications /;
			$dbt->getData($sql);
            main::dbg("sql to update reids: $sql");
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
        my $end_message = "<H3>" . $fieldsToEnter{'taxon_name'} . " " .Reference::formatShortRef(\%fieldsToEnter). " has been $enterupdate the database</H3>";

        my $origResultTaxonNumber = TaxonInfo::getOriginalCombination($dbt,$resultTaxonNumber);
        $end_message .= qq|
    <div align="center">
    <table cellpadding=10><tr><td>
      <li><p><b><a href="bridge.pl?action=displayAuthorityForm&taxon_no=$resultTaxonNumber">Edit $fieldsToEnter{taxon_name}</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$resultTaxonNumber">Get general information about $fieldsToEnter{taxon_name}</a></b></p></li>   
      <li><p><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit a name from the same reference</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm&use_reference=current">Add/edit another taxon</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm">Add/edit another taxon from another reference</a></b></p></li>
    </td>
    <td valign=top>
      <li><p><b><a href="bridge.pl?action=displayOpinionForm&opinion_no=-1&skip_ref_check=1&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber">Add an opinion about $fieldsToEnter{taxon_name}</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayOpinionChoiceForm&taxon_no=$resultTaxonNumber">Edit an opinion about $fieldsToEnter{taxon_name}</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit an opinion from the same reference</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayOpinionTaxonSearchForm&use_reference=current">Add/edit opinion about another taxon</a></b></p></li>
      <li><p><b><a href="bridge.pl?action=displayOpinionTaxonSearchForm">Add/edit opinion about another taxon from another reference</a></b></p></li>
    </td></tr></table>
    </div>|;

        displayTypeTaxonSelectForm($dbt,$s,$fieldsToEnter{'type_taxon'},$resultTaxonNumber,$fieldsToEnter{'taxon_name'},$fieldsToEnter{'taxon_rank'},$resultReferenceNumber,$end_message);
	}
	
	print "<BR>";
	print "</CENTER>";
}

# This section handles updating of the type_taxon_no field in the authorities table and is used both
# when entering subspecies/species in the authorities form, and entering opinions in the opinions form
# Behavior is:
#  Find out how many possible higher taxa this taxon can be a type for:
#    if its 0: this is bad, it should always be 1 unless the entering of the opinion was botched
#    if its 1: do the insertion or deletion on the spot, if permissions allow it
#    if its >1: print out a new form displaying a list of all parents for the user to check
#  possible higher taxa must be linked by opinions from the same ref as this opinion
sub displayTypeTaxonSelectForm {
    my ($dbt,$s,$is_tt_form_value,$type_taxon_no,$type_taxon_name,$type_taxon_rank,$reference_no,$end_message) = @_;

    main::dbg("displayTypeTaxonSelectForm called with is_tt_form_value $is_tt_form_value tt_no $type_taxon_no tt_name $type_taxon_name tt_rank $type_taxon_rank ref_no $reference_no");

    my @warnings = ();
    my @parents = getTypeTaxonList($dbt,$type_taxon_no,$reference_no);

    # The end message is the normal links + "This record has been updated in the DB" message.  save that message
    # for later if we're going to display another form.  If we're not, then show it
    my $show_end_message = 1;

    # This section handles updating of the type_taxon_no field in the authorities table:
    # Behavior is:
    #  Find out how many possible higher taxa this taxon can be a type for:
    #    if its 0: this is bad, dump an error into the error log
    #    if its 1: do the insertion or deletion
    #    if its >1: display a list of all parents for the user to check
    #  possible higher taxa must be linked by opinions from the same ref as this opinion
    main::dbg("TYPE TAXON PARENTS:\n<PRE>".Dumper(\@parents)."</PRE>");
    if ($is_tt_form_value) {
        if (scalar(@parents) > 1) {
            print "<div align=\"center\">";
            print "<form method=\"POST\" action=\"bridge.pl\">\n";
            print "<input type=\"hidden\" name=\"action\" value=\"submitTypeTaxonSelect\">\n";
            print "<input type=\"hidden\" name=\"reference_no\" value=\"$reference_no\">\n";
            print "<input type=\"hidden\" name=\"type_taxon_no\" value=\"$type_taxon_no\">\n";
            print "<input type=\"hidden\" name=\"end_message\" value=\"".uri_escape($end_message)."\">\n";
            print "<table><tr><td>\n";
            print "<h2>For which taxa is $type_taxon_name a type $type_taxon_rank?</h2>";
            foreach my $row (reverse @parents) {
                my $checked = ($row->{'type_taxon_no'} == $type_taxon_no) ? 'CHECKED' : '';
                my $disabled = ($s->get('authorizer_no') != $row->{'authorizer_no'} && $row->{'type_taxon_no'}) ? 'DISABLED READONLY' : '';
                print "<input type=\"checkbox\" name=\"taxon_no\" value=\"$row->{taxon_no}\" $disabled $checked> ";
                print "$row->{taxon_name} ($row->{taxon_rank})";
                if ($row->{'type_taxon_no'} && $row->{'type_taxon_no'} != $type_taxon_no) {
                    print " - <small>type taxon currently $row->{type_taxon_name} ($row->{type_taxon_rank})</small>";
                }
                if ($disabled) {
                    print " - <small>authority record belongs to ".Person::getPersonName($dbt,$row->{'authorizer_no'})."</small>";
                    if ($checked) {
                        print "<input type=\"hidden\" name=\"taxon_no\" value=\"$row->{taxon_no}\">";
                    }
                }
                print '<br>';
            }
            print "</td></tr></table>\n";
            print "<input type=\"submit\" value=\"Submit\">";
            print "</form>";
            print "</div>";
            $show_end_message = 0;
        } elsif (scalar(@parents) == 1) {
            my $return;
            if ($parents[0]->{'type_taxon_no'} != $type_taxon_no) {
                $return = $dbt->updateRecord($s,'authorities','taxon_no',$parents[0]->{'taxon_no'},{'type_taxon_no'=>$type_taxon_no});
            }
            if ($return == -1) {
                push @warnings,"Can't set this as the type taxon for authority $parents[0]->{taxon_name}, its owned by a difference authorizer: ".Person::getPersonName($dbt,$parents[0]->{'authorizer_no'}).". Its type taxon is already set to: $parents[0]->{type_taxon_name} ($parents[0]->{type_taxon_rank})";
            }
        } else {
            carp "Something is wrong in the opinions script, got no parents for current taxon after adding an opinion.  (in section dealing with type taxon)\n";
        }
    } else {
        # This is not a type taxon.  Find all parents from the same reference, and set the
        # type_taxon_no to 0 if its set to this taxon, otherwise leave it alone
        main::dbg("Handling deletion of type taxon no $type_taxon_no");
        foreach my $parent (@parents) {
            if ($parent->{'type_taxon_no'} == $type_taxon_no) {
                my $return = $dbt->updateRecord($s,'authorities','taxon_no',$parent->{'taxon_no'},{'type_taxon_no'=>'0'});
                if ($return == -1) {
                    push @warnings,"Can't unset this as the type taxon for authority $parent->{taxon_name}, its owned by a difference authorizer: ".Person::getPersonName($dbt,$parent->{'authorizer_no'});
                }
            }
        }
    }


    if (@warnings) {
        my $plural = (scalar(@warnings) > 1) ? "s" : "";
        print "<br><div align=\"center\"><table width=600 border=0>" .
              "<tr><td class=darkList><font size='+1'><b>Warning$plural</b></font></td></tr>" .
              "<tr><td>";
        print "<li class='medium'>$_</li>" for (@warnings);
        print "</td></tr></table></div><br>";
    }

    if ($show_end_message) {
        print $end_message;
    }

}


sub submitTypeTaxonSelect {
    my ($dbt,$s,$q) = @_;

    my $type_taxon_no = $q->param('type_taxon_no');
    my $reference_no = $q->param('reference_no');
    my $end_message = uri_unescape($q->param('end_message'));
    my @taxon_nos = $q->param('taxon_no');
    my @warnings = ();

    my @parents = getTypeTaxonList($dbt,$type_taxon_no,$reference_no);

    foreach my $parent (@parents) {
        my $found = 0;
        foreach my $taxon_no (@taxon_nos) {
            if ($parent->{'taxon_no'} == $taxon_no) {
                $found = 1;
            }
        }

        my $return;
        if ($found) {
            if ($parent->{'type_taxon_no'} != $type_taxon_no) {
                $return = $dbt->updateRecord($s,'authorities','taxon_no',$parent->{'taxon_no'},{'type_taxon_no'=>$type_taxon_no});
            }
        } else {
            if ($parent->{'type_taxon_no'} == $type_taxon_no) {
                $return = $dbt->updateRecord($s,'authorities','taxon_no',$parent->{'taxon_no'},{'type_taxon_no'=>'0'});
            }
        }
        if ($return == -1) {
            push @warnings,"Can't change the type taxon for authority $parent->{taxon_name}, its owned by a difference authorizer: ".Person::getPersonName($dbt,$parent->{'authorizer_no'});
        }
    }

    if (@warnings) {
        my $plural = (scalar(@warnings) > 1) ? "s" : "";
        print "<br><div align=\"center\"><table width=600 border=0>" .
              "<tr><td class=darkList><font size='+1'><b>Warning$plural</b></font></td></tr>" .
              "<tr><td>";
        print "<li class='medium'>$_</li>" for (@warnings);
        print "</td></tr></table></div><br>";
    }

    print $end_message;
}
    
# This function returns an array of potential higher taxa for which the focal taxon can be a type.
# The array is an array of hash refs with the following keys: taxon_no, taxon_name, taxon_rank, type_taxon_no, type_taxon_name, type_taxon_rank
sub getTypeTaxonList {
    my $dbt = shift;
    my $type_taxon_no = shift;   
    my $reference_no = shift;
            
    my $focal_taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$type_taxon_no);
            
    my $parents = Classification::get_classification_hash($dbt,'all',[$type_taxon_no],'array',$reference_no);
    # This array holds possible higher taxa this taxon can be a type taxon for
    # Note the reference_no passed to get_classification_hash - parents must be linked by opinions from
    # the same reference as the reference_no of the opinion which is currently being inserted/edited
    my @parents = @{$parents->{$type_taxon_no}}; # is an array ref
        
    if ($focal_taxon->{'taxon_rank'} =~ /species/) {
        # A species may be a type for genus/subgenus only
        my $i = 0;
        for($i=0;$i<scalar(@parents);$i++) {
            last if ($parents[$i]->{'taxon_rank'} !~ /species|genus|subgenus/);
        }
        splice(@parents,$i);
    } else {
        # A higher order taxon may be a type for subtribe/tribe/family/subfamily/superfamily
        # Don't know about unranked clade, leave it for now
        my $i = 0;
        for($i=0;$i<scalar(@parents);$i++) {
            last if ($parents[$i]->{'taxon_rank'} !~ /tribe|family|unranked clade/);        }   
        splice(@parents,$i);
    }
    # This sets values in the hashes for the type_taxon_no, type_taxon_name, and type_taxon_rank
    # in addition to the taxon_no, taxon_name, taxon_rank of the parent
    foreach my  $parent (@parents) {
        my $parent_taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$parent->{'taxon_no'});
        $parent->{'authorizer_no'} = $parent_taxon->{'authorizer_no'};
        $parent->{'type_taxon_no'} = $parent_taxon->{'type_taxon_no'};
        if ($parent->{'type_taxon_no'}) {
            my $type_taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$parent->{'type_taxon_no'});
            $parent->{'type_taxon_name'} = $type_taxon->{'taxon_name'};
            $parent->{'type_taxon_rank'} = $type_taxon->{'taxon_rank'};
        }
    }

    return @parents;
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
