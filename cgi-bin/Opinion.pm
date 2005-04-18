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
use CGI::Carp;
use Data::Dumper;

# list of allowable data fields.
use fields qw(GLOBALVARS opinion_no reference_no cachedDBRow DBTransactionManager );  

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
    my $error_message = shift;
	
	
	my $dbt = $self->getTransactionManager();
    my $dbh = $dbt->dbh;
	
	my %fields;  # a hash of fields and values that
				 # we'll pass to HTMLBuilder to pop. the form.
				 
	# Fields we'll pass to HTMLBuilder that the user can't edit.
	# (basically, any field which is not empty or 0 in the database,
	# if the authorizer is not the original authorizer of the record).
	my @nonEditables; 	
	
	if ((!$hbo) || (! $s) || (! $q)) {
		croak("Opinion::displayOpinionForm had invalid arguments passed to it.");
		return;
	}

    # Simple variable assignments
    my $isNewEntry = ($q->param('opinion_no') > 0) ? 0 : 1;
    my $reSubmission = ($error_message) ? 1 : 0;
	my @valid = ('belongs to', 'recombined as', 'revalidated');
	my @synArray = ('subjective synonym of', 'objective synonym of', 'homonym of','replaced by','corrected as','rank changed as');
	my @nomArray = ('nomen dubium','nomen nudum','nomen oblitum', 'nomen vanum');


    # If a drop down is presented, do not use the parent_no from it if they switched radio buttons
    if (($q->param('orig_taxon_status') && $q->param('orig_taxon_status') ne $q->param('taxon_status'))) {
        $q->param('parent_no'=>''); 
    }
   
    # Grab the appropriate data to auto-fill the form
	my $dbFieldsRef;
	if ($reSubmission) {
		%fields = %{$q->Vars};
        if (!$isNewEntry) {
            $dbFieldsRef = $self->databaseOpinionRecord();
            $fields{'child_no'} = $dbFieldsRef->{'child_no'};
        }
	} else {
        if ($isNewEntry) {
            $fields{'child_no'} = $q->param('child_no');
		    $fields{'reference_no'} = $s->currentReference();
        } else {
            $dbFieldsRef = $self->databaseOpinionRecord();
            %fields = %$dbFieldsRef;		
            
            $fields{'child_no'} = $dbFieldsRef->{'child_no'};

            if ($fields{'ref_has_opinion'} !~ /YES/i) {
                $fields{'2nd_pages'} = $fields{'pages'};
                $fields{'2nd_figures'} = $fields{'figures'};
                $fields{'pages'} = '';
                $fields{'figures'} = '';
            }

            # if its from the DB, populate appropriate form vars
            for (@valid) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'taxon_status'} = $_; 
                }
            }    
            for (@synArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'taxon_status'} = 'invalid1'; 
                    $fields{'synonym'} = $_;
                    last;
                }
            }    
            for (@nomArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'taxon_status'} = 'invalid2'; 
                    $fields{'nomen'} = $_;
                    last;
                }
            }    
            #$fields{opinion_no} = $self->{opinion_no};
        }
    }

    # Assign error message
    $fields{'error_message'} = $error_message if ($error_message);

    # This block gets a list of potential homonyms, either from the database for an edit || resubmission
    # or from the values passed in for a new && resubmission
    my @parent_nos = ();
    #my @recombined_nos = ();
    my $parentName = "";
    #my $recombinedName = "";
    if ($fields{'parent_no'} > 0) {
        # This will happen on an edit (first submission) or resubmission w/homonyms
        # SQL trick: get not only the authoritiy data for parent_no, but all its homonyms as well
        my $sql = "SELECT a2.* FROM authorities a1, authorities a2 WHERE a1.taxon_name=a2.taxon_name AND a1.taxon_no=".$dbh->quote($fields{'parent_no'});
        my @results= @{$dbt->getData($sql)}; 
        foreach my $row (@results) {
            push @parent_nos, $row->{'taxon_no'};
            $parentName = $row->{'taxon_name'};
        }
    } else {
        # This block will happen on a resubmission w/o homonyms
        if ($fields{'taxon_status'} eq 'belongs to') { 
            $parentName = $q->param('belongs_to_parent');
        } elsif ($fields{'taxon_status'} eq 'recombined as') { 
            $parentName = $q->param('recombined_as_parent');
        } elsif ($fields{'taxon_status'} eq 'invalid1') { 
            $parentName = $q->param('synonym_parent');
        }
        if ($parentName) {
            my $sql = "SELECT * FROM authorities WHERE taxon_name=".$dbh->quote($parentName);
            my @results= @{$dbt->getData($sql)}; 
            foreach my $row (@results) {
                push @parent_nos, $row->{'taxon_no'};
            }
        }
    }

    main::dbg("parentName $parentName parent_nos ".Dumper(@parent_nos));

    if (!$isNewEntry) {
	    # fill out the authorizer/enterer/modifier info at the bottom of the page
		if ($fields{authorizer_no}) { $fields{authorizer_name} = " <B>Authorizer:</B> " . $s->personNameForNumber($fields{authorizer_no}); }
		if ($fields{enterer_no}) { $fields{enterer_name} = " <B>Enterer:</B> " . $s->personNameForNumber($fields{enterer_no}); }
		if ($fields{modifier_no}) { $fields{modifier_name} = " <B>Modifier:</B> " . $s->personNameForNumber($fields{modifier_no}); }
    }

    # Handle radio button
    if ($fields{'ref_has_opinion'} eq 'YES') {
		$fields{'ref_has_opinion_checked'} = 'checked';
		$fields{'ref_has_opinion_notchecked'} = '';
    } elsif (exists $fields{'ref_has_opinion'}) {
		$fields{'ref_has_opinion_checked'} = '';
		$fields{'ref_has_opinion_notchecked'} = 'checked';
	}

	
	my $child = Taxon->new($self->{GLOBALVARS}); $child->setWithTaxonNumber($fields{'child_no'});
	my $childName = $child->taxonName();
	my $childRank = $child->rankString();
    $fields{'child_name'} = $childName;
	
	
	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.  However, if they're superuser
	# (alroy, alroy), then let them edit anything.
	#
	# otherwise, they can edit any field.
	my $sesAuth = $s->get('authorizer_no');
	
	if ($s->isSuperUser()) {
		$fields{'message'} = "<p align=center><i>You are the superuser, so you can edit any field in this record!</i></p>";
	} elsif ((! $isNewEntry) && ($sesAuth != $dbFieldsRef->{authorizer_no}) && ($dbFieldsRef->{authorizer_no} != 0)) {
	
		# grab the authorizer name for this record.
		my $authName = $s->personNameForNumber($fields{authorizer_no});
	
		$fields{'message'} = "<p align=center><i>This record was created by a different authorizer ($authName) so you can only edit empty fields.</i></p>";
		
		# we should always make the ref_has_opinion radio buttons disabled
		# because only the original authorizer can edit these.
		
		push (@nonEditables, 'ref_has_opinion');
		
		# depending on the status of the ref_has_opinion radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_has_opinion'} eq 'YES') {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
		}
		
		
		# depending on the status, we should disable some fields.		
		if ($fields{'status'}) {
			push(@nonEditables, 'taxon_status');
			push(@nonEditables, 'nomen');
			push(@nonEditables, 'synonym');
			push(@nonEditables, 'belongs_to_parent');
			push(@nonEditables, 'recombined_as_parent');
			push(@nonEditables, 'synonym_parent');
		}
		
		if (($fields{'status'} ne 'recombined as') && ($fields{status} ne 'belongs to')) {
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

    # its important that we save this in case the user selected a parent_no from the pulldown
    # AND switches the taxon_status radio. In that case we have to throw out the parent_no
    $fields{'orig_taxon_status'} = $fields{'taxon_status'};

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $pulldown;
	if ( scalar(@parent_nos) > 1) {
        $pulldown .= qq|<input type="radio" name="parent_no" value=""> \nOther: <input type="text" name="belongs_to_parent" value=""><br>\n|;
	    my %classification=%{Classification::get_classification_hash($dbt,"parent",\@parent_nos)};
        foreach my $parent_no (@parent_nos) {
			my %auth = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$parent_no)};
            my $selected = ($fields{'parent_no'} == $parent_no) ? "CHECKED" : "";
            my $pub_info = "$auth{author1last} $auth{pubyr}";
            $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
			$pulldown .= qq|<input type="radio" name="parent_no" $selected value='$parent_no'> ${parentName}$pub_info [$classification{$parent_no}]<br>\n|;
        }
	}

    # Each of the 'taxon_status' row options
	my $belongs_to_row;
    my $recombined_as_row;
    my $synonym_row;
    my $nomen_row;

	# species get special treatment
	if ( $childRank =~ /species/ )	{
        my $selected = ($fields{'taxon_status'} eq 'recombined as') ? "CHECKED" : "";
        my $colspan = ($pulldown && $selected) ? "": "colspan=2";
        my $parentRank = ($childRank eq 'subspecies') ? 'species' : 'genus';
		$recombined_as_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" $selected value="recombined as"></td>|;
        if ($childRank eq 'subspecies') {
            $recombined_as_row .= "<td colspan=2><b>Valid $childRank</b>, but recombined into a different species.</td></tr>";
            $recombined_as_row .= "<tr><td></td><td $colspan nowrap valign='top'>New genus, species, and subspecies: ";
        } else {
            $recombined_as_row .= "<td colspan=2><b>Valid $childRank</b>, but recombined into a different genus.</td></tr>";
            $recombined_as_row .= "<tr><td></td><td $colspan nowrap valign='top'>New genus and species: ";
        }
        if ($pulldown && $selected) {
            $pulldown =~ s/belongs_to_parent/recombined_as_parent/;
            $recombined_as_row .= "<td width='100%'>$pulldown</td>";
        } else {
            my $parentTaxon = ($selected) ? $parentName : "";
			$recombined_as_row .= qq|<input name="recombined_as_parent" size="50" value="$parentTaxon">|;
        }   
            
        $recombined_as_row .= "</td></tr>";

        $selected = ($fields{'taxon_status'} eq 'belongs to') ? "CHECKED" : "";
        $colspan = ($pulldown && $selected) ? "": "colspan=2";
        $belongs_to_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="belongs to" $selected></td>\n|;
        $belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid $childRank</b> as originally combined; belongs to |;
        # or print the species or genus name as plain text
        if ($pulldown && $selected) {
            $belongs_to_row .= "<td width='100%'>$pulldown</td>";
		} else	{
            my @parentBits = split /\s+/,$childName;
            pop @parentBits;
            my $parentTaxon = join(" ",@parentBits);
            $belongs_to_row .= "<i>$parentTaxon</i>.";
#            $belongs_to_row .= qq|<input type="hidden" name="belongs_to_parent" value="$parentTaxon">|;
		}
        $belongs_to_row.= "</td></tr>";
	} else	{
	    # standard approach for genera or higher taxa
        my $selected = ($fields{'taxon_status'} eq 'belongs to') ? "CHECKED" : "";
        my $colspan = ($pulldown && $selected) ? "": "colspan=2";
        $belongs_to_row .= qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="belongs to" $selected></td>\n|;
		$belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid $childRank</b>, classified as belonging to |;

        if ($pulldown && $selected) {
            $belongs_to_row .= "<td width='100%'>$pulldown</td>";
		} else	{
            my $parentTaxon = ($selected) ? $parentName : "";
			$belongs_to_row .= qq|<input name="belongs_to_parent" size="50" value="$parentTaxon">|;
		}
	}

	# format the synonym section
	# note: by now we already have a status pulldown ready to go; we're
	#  tacking on either another pulldown or an input to store the name
	# need a pulldown if this is a second pass and we have multiple
	#  alternative senior synonyms
    my $selected = ($fields{'taxon_status'} eq 'invalid1') ? "CHECKED" : "";
    my $colspan = ($pulldown && $selected) ? "": "colspan=2";
    $synonym_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="invalid1" $selected></td>|;
	$synonym_row .= "<td colspan=2 valign='top'><b>Invalid</b>, and another name should be used.</td></tr>";
    $synonym_row .= "<tr><td></td><td $colspan valign='top' nowrap>Status: "; 
	# actually build the synonym popup menu.
	$synonym_row .= $hbo->buildSelect(\@synArray, 'synonym', $fields{synonym});
	if ($pulldown && $selected) {
        $pulldown =~ s/belongs_to_parent/synonym_parent/;
		$synonym_row .= "<td width='100%'>$pulldown</td>"; 
	} else	{
        my $parentTaxon = ($selected) ? $parentName : "";
		$synonym_row .= qq|<input name="synonym_parent" size="50" value="$parentTaxon">|;
	}
    $synonym_row .= "</td></tr>";

	# actually build the nomen popup menu.
    $selected = ($fields{'taxon_status'} eq 'invalid2') ? "CHECKED" : "";
    $nomen_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="invalid2" $selected></td>|;
	$nomen_row .= "<td colspan=2><b>Invalid</b>, and no other name can be used.</td></tr>";
    $nomen_row .= "<tr><td></td><td colspan=2>Status: "; 
    $nomen_row .= $hbo->buildSelect(\@nomArray, 'nomen', $fields{nomen});
    $nomen_row .= "</td></tr>";

    main::dbg("showOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");

	$fields{belongs_to_row} = $belongs_to_row;
	$fields{nomen_row} = $nomen_row;
	$fields{synonym_row} = $synonym_row;
	$fields{recombined_as_row} = $recombined_as_row;

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
		croak("Taxon::submitOpinionForm had invalid arguments passed to it.");
		return;	
	}
	
	my $dbt = $self->getTransactionManager();
	$dbt->setSession($s);
    my $dbh = $dbt->dbh;
	
	my $errors = Errors->new();
		
	# Simple checks
    my $isNewEntry = ($q->param('opinion_no') > 0) ? 0 : 1;
	
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
	
    # Get the original combination
    my $orig_child_no = TaxonInfo::getOriginalCombination($dbt,$q->param('child_no'));
	my $childTaxon = Taxon->new($self->{GLOBALVARS});
	$childTaxon->setWithTaxonNumber($orig_child_no);
	my $childName = $childTaxon->taxonName();
	my $childRank = $childTaxon->rankString();

    # If a drop down is presented, do not use the parent_no from it if they switched radio buttons
    if (($q->param('orig_taxon_status') && $q->param('orig_taxon_status') ne $q->param('taxon_status'))) {
        $q->param('parent_no'=>''); 
    }

	###
	## Deal with the reference section at the top of the form.  This
	## is almost identical to the way we deal with it in the authority form
	## so this functionality should probably be merged at some point.
	###
	
	if (($q->param('ref_has_opinion') ne 'YES') && 
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
        if ($isNewEntry) {
		    my $osql = "SELECT parent_no FROM opinions WHERE ref_has_opinion!='YES' AND child_no=".$q->param('child_no')." AND author1last='".$q->param('author1last')."' AND author2last='".$q->param('author2last')."' AND pubyr='".$q->param('pubyr')."'";
            my $oref = ${$dbt->getData($osql)}[0];
            if ( $oref ) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		# merge the pages and 2nd_pages, figures and 2nd_figures fields
		# together since they are one field in the database.
		$fieldsToEnter{'pages'} = $q->param('2nd_pages');
		$fieldsToEnter{'figures'} = $q->param('2nd_figures');
		
		if (! $q->param('author1last')) {
			$errors->add('You must enter at least the last name of the first author');	
		}
		
		# make sure the pages/figures fields above this are empty.
		my @vals = ($q->param('pages'), $q->param('figures'));
		if (!(Globals::isEmpty(\@vals))) {
			$errors->add("Don't enter pages or figures for a primary reference if you chose the 'named in an earlier publication' radio button");	
		}
		
		# make sure the format of the author names is proper
		if  (( $q->param('author1init') && (! Validation::properInitial($q->param('author1init')))) ||
             ( $q->param('author2init') && (! Validation::properInitial($q->param('author2init')))) ) {
			$errors->add("The first author's initials are improperly formatted");		
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
			
			if (! Validation::properYear($pubyr)) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new();
			$ref->setWithReferenceNumber($q->param('reference_no'));
			if ($pubyr > $ref->pubyr()) {
				$errors->add("The publication year ($pubyr) can't be more recent than that of the primary reference (" . $ref->pubyr() . ")");
			}
		} else {
            $errors->add("A publication year is required.");
        }
	} else {
		# if they chose ref_has_opinion, then we also need to make sure that there
		# are no other opinions about the current taxon (child_no) which use 
		# this as the reference.  
        if ($isNewEntry) {
		    my $sql = "SELECT parent_no FROM opinions ".
                      " WHERE ref_has_opinion='YES'".
                      " AND child_no=".$dbh->quote($q->param('child_no')).
                      " AND reference_no=".$dbh->quote($q->param('reference_no'));
            my $row = ${$dbt->getData($sql)}[0];
            if ( $row) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		# ref_has_opinion is YES
		# so make sure the other publication info is empty.
		my @vals = ($q->param('author1init'), $q->param('author1last'), $q->param('author2init'), $q->param('author2last'), $q->param('otherauthors'), $q->param('pubyr'), $q->param('2nd_pages'), $q->param('2nd_figures'));
		
		if (!(Globals::isEmpty(\@vals))) {
			$errors->add("Don't enter any other publication information if you chose the 'first named in primary reference' radio button");	
		}
		
		
	}
	
	{
		# also make sure that the pubyr of this opinion isn't older than
		# the pubyr of the authority record the opinion is about.
		my $taxon = Taxon->new();
		$taxon->setWithTaxonNumber($q->param('child_no'));
		my $ref = Reference->new();
		$ref->setWithReferenceNumber($q->param('reference_no'));
		
		my $pubyr = $q->param('pubyr');
		
		if (( $taxon->pubyr() > $ref->pubyr() ) ||
			( $taxon->pubyr() > $pubyr && $pubyr > 1700 ) ) {
			$errors->add("The publication year ($pubyr) for this opinion can't be earlier than the year the taxon was named (".$taxon->pubyr().")");	
		}
	}

    # Flatten the status
    my $status;
    if ($q->param('taxon_status') =~ /belongs to|recombined as/) {
        $status = $q->param('taxon_status');
    } elsif ($q->param('taxon_status') eq 'invalid1') {
        $status = $q->param('synonym');
    } elsif ($q->param('taxon_status') eq 'invalid2') {
        $status = $q->param('nomen');
    }
	$fieldsToEnter{'status'} = $status;

	if (!$status) {
		$errors->add("You must choose one of the status radio buttons");	
	}

    # Whether or not we'll create a new record in the authorities table for a 
    # 'corrected as', 'rank changed as', or 'recombined as'
    my $parentName = '';
    my $parentRank = '';
    my %createAuthority = ();

    if ($q->param('parent_no')) {
        # This is a second pass through, b/c there was a homonym issue, the user
        # was presented with a pulldown to distinguish between homonyms, and has submitted the form
        # To simplify, assumed the users choice is locked in at this point, so we only have 1 available option
        my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_no=".$dbh->quote($q->param('parent_no'));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            croak("Fatal error, parent_taxon_no ".$q->param('parent_no')." was set but not in the authorities table");
        }
        $parentName = $row->{'taxon_name'};
        $parentRank = $row->{'taxon_rank'};
        #$fields{'parent_no'} = $q->param('parent_no');
    } else {
        # This block of code deals with a first pass through, when no homonym problems have yet popped up
        # We want to:
        #  * Parse out the parent name, if appropriate 
        #  * Hit the DB to make sure theres exactly 1 copy of the parent name (error if > 1 or 0)
        if ($status eq 'belongs to') {
            if (defined($q->param('belongs_to_parent'))) {
                $parentName = $q->param('belongs_to_parent');	
            } else {
                # No box was selected bc it was species or subspecies, who is belongs to
                # is implicit in the name, so we just grab it from there
                my @parentParts = split(/\s+/,$childName);
                pop @parentParts;
                $parentName = join(' ',@parentParts);
            }
        } elsif ($status eq 'recombined as') {
            my @parentParts = split(/\s+/,$q->param('recombined_as_parent'));
            $parentName = $q->param('recombined_as_parent');
        } elsif ($status =~ /synonym|homonym|replaced by|rank changed as|corrected as/) {
            $parentName = $q->param('synonym_parent');
        } elsif ($status =~ /nomen/) {
            $parentName = undef;
        }

        # Get a single parent no or we have an error
        if ($status !~ /nomen/) {
            if (!$parentName) {
                $errors->add("You must enter the name of the taxon this one belongs to");
            } else {    
                my $sql  = "SELECT taxon_no, taxon_rank, taxon_name FROM authorities WHERE taxon_name=".$dbh->quote($parentName);
                my @parentArray = @{$dbt->getData($sql)};
                if (scalar(@parentArray) > 1) {
                    $errors->add("The taxon '" . $parentName ."' exists multiple times in the database. Please select the one you want.");	
                } elsif (scalar(@parentArray) == 0) {
                    if ($status =~ /recombined|corrected as/) {
                        %createAuthority = ('taxon_rank'=>$childRank, 'taxon_name'=>$parentName);
                        $parentRank = $childRank;
                    } else {
                        $errors->add("The taxon '" . $parentName ."' doesn't exist in our database.  Please <A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_name=$parentName\">create a new authority record for '$parentName'</a> <i>before</i> entering this opinion.");	
                    }
                } elsif (scalar(@parentArray) == 1) {
                    $q->param("parent_no"=>$parentArray[0]->{'taxon_no'});
                    #$fields{'parent_no'} = $parentArray[0]->{'taxon_no'};
                    $parentRank = $parentArray[0]->{'taxon_rank'};
                }
            }
        } else {
            $q->param("parent_no"=>0);
            #$fields{'parent_no'} = '0';
        }
    }
  

    # Additional error checking related to ranks mostly
    # Only bother if we're down to one parent
    main::dbg("childRank $childRank childName $childName parentRank $parentRank parentName $parentName");
    if ($q->param('parent_no')) {
	    if ($status eq 'belongs to') {
		    # for belongs to, the parent rank should always be higher than the child rank.
		    # unless either taxon is an unranked clade (JA)
		    if ($rankToNum{$parentRank} <= $rankToNum{$childRank} && 
                $parentRank ne "unranked clade" && 
                $childRank ne "unranked clade") {
		    	$errors->add("The rank of the higher taxon $parentName ($parentRank) must be higher than the rank of $childName ($childRank)");	
		    }
        }    
        if ($status eq 'recombined as') {
		    my ($childParentName) = split /\s+/,$childName;
		    my ($parentParentName) = split /\s+/,$parentName;
		    if ( $childParentName eq $parentParentName)	{
			    $errors->add("The genus name in the new combination must be different from the genus name in the old combination");
		    }
		    if ($parentRank ne 'species' )	{
			    $errors->add("If a species is recombined its new rank must be 'species'");	
		    }
		    if ($childRank ne 'species') {
			    $errors->add("If a species is recombined its old rank must be 'species'");
		    }
        }    
        if ($status =~ /synonym|homonym|replaced/) {
    		# the parent rank should be the same as the child rank...
	    	if ( $parentRank ne $childRank) {
		    	$errors->add("The rank of a taxon and the rank of its synonym, homonym, or replacement name must be the same");
            }    
		} 

        if ($status eq 'rank changed as') {
		    # JA: ... except if the status is "rank changed as," which is actually the opposite case
		    if ( $parentRank eq $childRank) {
			    $errors->add("If you change a taxon's rank, its old and new ranks must be different");
            }
	    } 

    	if ($parentName eq $childName) {
	    	$errors->add("The taxon you are entering and the one it belongs to can't have the same name");	
	    }
    }	

	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( (! (($status eq 'recombined as') || ($status eq 'belongs to') )) && ($q->param('diagnosis'))) {
		$errors->add("Don't enter a diagnosis unless you choose the 'belongs to' or 'recombined as' radio button");
	}

    # Add editable fields to fieldsToEnter hash
	foreach my $formField ($q->param()) {
		my $okayToEdit = $editAny;
		if (! $okayToEdit) {
			if (! $dbFields{$formField}) {
				$okayToEdit = 1;
			}
		}
		if ($okayToEdit) {
			if (! $fieldsToEnter{$formField}) {
				$fieldsToEnter{$formField} = $q->param($formField);
			}
		}
	}
	# assign the child_no field if it doesn't already exist.
	$fieldsToEnter{'child_no'} = $q->param('child_no'); 

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
	
	# at this point, we should have a nice hash array (%fieldsToEnter) of
	# fields and values to enter into the authorities table.
	if ($errors->count() > 0) {
		# put a message in a hidden to let us know that we have already displayed
		# some errors to the user and this is at least the second time through (well,
		# next time will be the second time through - whatever).

		# stick the errors in the CGI object for display.
		my $message = $errors->errorMessage();

		$self->displayOpinionForm($hbo, $s, $q, $message);
		return;
	}
	
	
	# now we'll actually insert or update into the database.

	# first step is to create the parent taxon if a species is being
	#  recombined and the new combination doesn't exist JA 14.4.04
	# WARNING: this is very dangerous; typos in parent names will
	# create bogus combinations, and likewise if the opinion create/update
	#  code below bombs
	if (%createAuthority) {
	    # next we need to steal data from the opinion
        $createAuthority{'authorizer_no'} = $s->get('authorizer_no');
        $createAuthority{'enterer_no'} = $s->get('enterer_no');
        $createAuthority{'reference_no'} = $fieldsToEnter{'reference_no'};

        # author information comes from the original combination,
        # I'm doing this the "old" way instead of using some
        #  ridiculously complicated Poling-style objects
		my $sql = "SELECT * FROM authorities WHERE taxon_no=" . $fieldsToEnter{child_no};
		my $aref = ${$dbt->getData($sql)}[0];
		my @origAuthFields = ( "taxon_rank", "pages","figures", "comments" );
		for my $af ( @origAuthFields )	{
			if ( $aref->{$af} )	{
                $createAuthority{$af}=$aref->{$af};
			}
		}
		@origAuthFields = ( "author1init", "author1last","author2init", "author2last","otherauthors", "pubyr" );
		if ( $aref->{'author1last'} )	{
			for my $af ( @origAuthFields )	{
				if ( $aref->{$af} )	{
                    $createAuthority{$af}=$aref->{$af};
				}
			}
		}
        # ref is authority is always false, and just to make sure that
        #  makes sense, we're going to steal the author info from the
        #  original combination's reference if we don't have it already
		else	{
			my $rsql = "SELECT * FROM refs WHERE reference_no=" . $aref->{'reference_no'};
			my $rref = ${$dbt->getData($rsql)}[0];
			for my $af ( @origAuthFields )	{
				if ( $rref->{$af} )	{
                    $createAuthority{$af}=$rref->{$af};
				}
			}
		}

    	# have to store created date
        $createAuthority{'created'} = now();
        my @authFields = keys %createAuthority;
        my @authValues = map { $dbh->quote($_) } (values %createAuthority);

		my $insertsql = "INSERT INTO authorities (" . join(',',@authFields).") VALUES (".join(',',@authValues).")";
		$dbt->getData($insertsql);
        $fieldsToEnter{'parent_no'} = $dbt->getID();
        main::dbg("INSERTSQL is $insertsql and got back id $fieldsToEnter{parent_no}");
	}

	my $resultOpinionNumber;
    my $resultReferenceNumber = $fieldsToEnter{'reference_no'};

    # We must point to the original parent_no
    my $first_parent_no = $fieldsToEnter{'parent_no'};
    if ($fieldsToEnter{'parent_no'}) {
        if ($fieldsToEnter{'status'} !~ /recombined|corrected|rank/) {
           $fieldsToEnter{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$fieldsToEnter{'parent_no'});
        }
    }
	
    main::dbg("submitOpinionForm, fields are: <pre>".Dumper(\%fieldsToEnter)."</pre>");
	if ($isNewEntry) {
		my $code;	# result code from dbh->do.
	
		# grab the date for the created field.
		$fieldsToEnter{created} = now();
		
		# make sure we have a taxon_no for this entry...
		if (! $fieldsToEnter{child_no} ) {
			croak("Opinion::submitOpinionForm, tried to insert a record without knowing its child_no (original taxon).");
			return;	
		}
		
		
		# we'll have to remove the opinion_no from the fieldsToInsert if it 
		# exists, because this is the primary key
		
		delete $fieldsToEnter{opinion_no};
		
		($code, $resultOpinionNumber) = $dbt->insertNewRecord('opinions', \%fieldsToEnter);
		
		if ($code && ($fieldsToEnter{'status'} =~ /recombined as|corrected as|rank changed as/)) { 
		    # At this point, *if* the status of the new opinion was 'recombined as',
		    # migrated opinions to the original combination.
            if ($fieldsToEnter{'child_no'} && $fieldsToEnter{'parent_no'}) {
                my $rsql = "UPDATE opinions SET child_no=$fieldsToEnter{child_no} WHERE child_no=$fieldsToEnter{parent_no}";
                main::dbg("Move opinions to point FROM: $rsql");;
                my $return = $dbt->getData($rsql);
                if (!$return) {
                    carp "Failed to move opinions off of recombined|corrected as|rank changed as taxon ($fieldsToEnter{parent_no} to original combination ($fieldsToEnter{child_no}) for opinion no $code";
                }
            }
            # Secondly, opinions must point to the original combination as well
            if ($fieldsToEnter{'child_no'} && $fieldsToEnter{'parent_no'}) {
                my $rsql = "UPDATE opinions SET parent_no=$fieldsToEnter{child_no} WHERE parent_no=$fieldsToEnter{parent_no} AND child_no != $fieldsToEnter{child_no}";
                main::dbg("Move opinions to point TO: $rsql");;
                my $return = $dbt->getData($rsql);
                if (!$return) {
                    carp "Failed to move opinions to point to original comb ($fieldsToEnter{child_no} that pointed to recomb name ($fieldsToEnter{parent_no}) for opinion no $code";
                }
            }
            
		}	
	} else {
		# if it's an old entry, then we'll update.
		
		# Delete some fields that should never be updated...
		delete $fieldsToEnter{authorizer_no};
		delete $fieldsToEnter{enterer_no};
		delete $fieldsToEnter{created};
		delete $fieldsToEnter{reference_no};
		
		
		if (!($self->{opinion_no})) {
			croak("Opinion::submitOpinionForm, tried to update a record without knowing its opinion_no..  Oops.");
			return;
		}
			
		$resultOpinionNumber = $self->{opinion_no};
		
		if ($editAny) {
			$dbt->updateRecord('opinions', \%fieldsToEnter, "opinion_no = '" . $self->{opinion_no} . "'", 'opinion_no');
		} else {
			$dbt->updateRecordEmptyFieldsOnly('opinions', \%fieldsToEnter, "opinion_no = '" . $self->{opinion_no} . "'", 'opinion_no');
		}
	}
	
    my $o = Opinion->new(); $o->setWithOpinionNumber($resultOpinionNumber);
    my $opinionHTML = $o->formatAsHTML();
    $opinionHTML =~ s/according to/of/i;

    # At this point, we've run getOriginalCombination on $fields{parent_no}, so it may
    # be an original combination of parent_name. if it is, display a message
    my $recomb_message;
    if ($first_parent_no != $fieldsToEnter{'parent_no'}) {
        my $origParentName = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$fieldsToEnter{parent_no}")}[0]->{'taxon_name'};
        $recomb_message = "<h4>($parentName is a recombination/correction of $origParentName)</h4>";
    }
    
	print main::stdIncludes("std_page_top");
	my $enterupdate = ($isNewEntry) ? 'entered into' : 'updated in';
    print <<EOF;
<div align=center>
  <h3> The opinion $opinionHTML has been $enterupdate the database</h3>
  $recomb_message
  <p>
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_no=$fieldsToEnter{child_no}"><B>Get general information about $childName</b></a></span> -
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=displayOpinionForm&opinion_no=$resultOpinionNumber"><B>Edit this opinion</b></a></span> -
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber"><B>Edit a different opinion with same reference</b></a></span> -
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=startDisplayOpinionChoiceForm&taxon_no=$fieldsToEnter{child_no}"><B>Add/edit a different opinion about $childName</b></a></span> -
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&goal=opinion"><B>Add/edit an opinion about another taxon</b></a></span> -
    <span style='white-space: nowrap;'><a href="/cgi-bin/bridge.pl?action=displayTaxonomySearchForm&goal=authority"><B>Add/edit authority data about another taxon</b></a></span>
  </p>
  <br>
</div>
EOF
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

    my $orig_child_no; 
    
    my $osql = "SELECT o.opinion_no FROM opinions o "; 
    if ($q->param("taxon_no")) {
        $orig_child_no = TaxonInfo::getOriginalCombination($dbt,$q->param("taxon_no"));
        $osql .= " LEFT JOIN refs r ON r.reference_no=o.reference_no";
        $osql .= " WHERE o.child_no=$orig_child_no";
        $osql .= " ORDER BY IF((o.ref_has_opinion != 'YES' AND o.pubyr), o.pubyr, r.pubyr) ASC";
    } elsif ($q->param("reference_no")) {
        $osql .= " LEFT JOIN authorities a ON a.taxon_no=o.child_no";
        $osql .= " WHERE o.reference_no=".int($q->param("reference_no"));
        $osql .= " ORDER BY a.taxon_name";
    } else {
        print "No terms were entered.";
    }
    my @results = @{$dbt->getData($osql)};

    if (scalar(@results) == 0 && $suppressAddNew) {
        print "<center><h3>No opinions found</h3></center><br><br>";
        return;
    }
    print "<center>";
    if ($q->param("taxon_no")) {
        my $t = Taxon->new();
        $t->setWithTaxonNumber($orig_child_no);
        print "<h3>Which opinion about ".$t->taxonNameHTML()." do you want to edit?</h3>\n";
        my $c_row = PBDBUtil::getCorrectedName($dbt,$orig_child_no);

        if ($orig_child_no!= $c_row->{'taxon_no'}) {
            my $t2 = Taxon->new();
            $t2->setWithTaxonNumber($c_row->{'taxon_no'});
            print "<I>(Currently known as ".$t2->taxonNameHTML().")</I><BR>";
        }
        print "<BR>"; 
    } else {
        print "<br><h3>Select an opinion to edit:</h3><br>\n";
    }
                                                                                                                                                             
    print qq|<form method="POST" action="bridge.pl">
             <input type="hidden" name="action" value="displayOpinionForm">\n|;
    if ($q->param("taxon_no")) {
        print qq|<input type="hidden" name="child_no" value="$orig_child_no">\n|;
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


