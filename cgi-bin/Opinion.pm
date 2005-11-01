#!/usr/bin/perl

# created by rjp, 3/2004.
# Represents information about a particular opinion

# Reworked PS 04/30/2005 - reworked accessor methods to make sense.  Also, return undef
# if the opinion isn't in the opinions table, since a Opinion object with a opinion_no is pointless 



package Opinion;

use strict;

use DBI;
use DBTransactionManager;
use PBDBUtil;
use Class::Date qw(date localdate gmdate now);
use Session;
use Classification;
use CGI::Carp;
use Data::Dumper;
use Permissions;
use TaxaCache;

# list of allowable data fields.
use fields qw(opinion_no reference_no dbt DBrow);  

# Optionally pass it an opinion_no
sub new {
	my $class = shift;
    my $dbt = shift;
    my $opinion_no = shift;
	my Opinion $self = fields::new($class);
    $self->{'dbt'}=$dbt;

    my ($sql,@results);
    if ($opinion_no =~ /^\d+$/) { 
        $sql = "SELECT * FROM opinions WHERE opinion_no=$opinion_no";
        @results = @{$dbt->getData($sql)};
    }
    if (@results) {
        $self->{'DBrow'} = $results[0];
        $self->{'opinion_no'} = $results[0]->{'opinion_no'};
        $self->{'reference_no'} = $results[0]->{'reference_no'};
    } else {
        carp "Could not create opinion object with passed in opinion $opinion_no";
        return;
    }
	return $self;
}


# Universal accessor
sub get {
    my Opinion $self = shift;
    my $fieldName = shift;
    if ($fieldName) {
        return $self->{'DBrow'}{$fieldName};
    } else {
        return(keys(%{$self->{'DBrow'}}));
    }
}

# Get the raw underlying database hash;
sub getRow {
    my Opinion $self = shift;
    return $self->{'DBrow'};
}


# Figure out the spelling status - tricky cause we may need to infer it
# Something can be a 'corrected as', but the status is 'synonym of', so that info is lost
sub getSpellingStatus {
    my ($dbt,%fields);
    if ($_[0]->isa('DBTransactionManager')) {
        # If called from a functional interface
        $dbt = $_[0];
        %fields = %{$_[1]};
    } else {
        # If called from an object oriented interface
        my $self = shift;
        $dbt = $self->{'dbt'};
        %fields = %{$self->getRow()};
    }
    
    my $spelling_status = "";
    
    if ($fields{'status'} =~ /belongs|recombined|rank|corrected/) {
        $spelling_status = $fields{'status'};   
    } elsif ($fields{'child_no'} eq $fields{'child_spelling_no'}) {
        $spelling_status ='belongs to';
    } else {
        my $child= Taxon->new($dbt,$fields{'child_no'}); 
        my $spelling = Taxon->new($dbt,$fields{'child_spelling_no'});
       
        # For a recombination, the upper names will always differ. If they're the same, its a correction
        if ($child->get('taxon_rank') =~ /species/) {
            my @childBits = split(/ /,$child->get('taxon_name'));
            my @spellingBits= split(/ /,$spelling->get('taxon_name'));
            pop @childBits;
            pop @spellingBits;
            my $childName = join(' ',@childBits);
            my $spellingName = join(' ',@spellingBits);
            if ($childName eq $spellingName) {
                # If the genus/subgenus/species names are the same, its a correction
                $spelling_status ='corrected as';
            } else {
                # If they differ, its a bad record or its a recombination
                $spelling_status ='recombined as';
            }
        } elsif ($child->get('taxon_rank') ne $spelling->get('taxon_rank')) {
            $spelling_status = 'rank changed as';
        } else {
            $spelling_status = 'corrected as';
        }
    }
    main::dbg("Get spelling status called, return $spelling_status");
    return $spelling_status;
}

# Small utility function, added 04/26/2005 PS
# Transparently enter in the correct publication information and return it as well
sub getOpinion {
    my $dbt = shift;
    my $opinion_no= shift;
    my @results = ();
    if ($dbt && $opinion_no)  {
        my $sql = "SELECT * FROM opinions WHERE opinion_no=$opinion_no";
        @results = @{$dbt->getData($sql)};
    }


    return @results;
}




# returns the authors of the opinion record
sub authors {
	my Opinion $self = shift;
	
	# get all info from the database about this record.
	my $hr = $self->{'DBrow'};
	
	if (!$hr) {
		return '';	
	}
	
	my $auth;
	
	if ($hr->{ref_has_opinion} eq 'YES') {
		# then get the author info for that reference
		my $ref = Reference->new($self->{'dbt'},$hr->{'reference_no'});
		$auth = $ref->authors() if ($ref);
	} else {
        $auth = Reference::formatShortRef($hr);	
	}
	
	return $auth;
}


sub pubyr {
	my Opinion $self = shift;

	# get all info from the database about this record.
	my $hr = $self->{'DBrow'};
	
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
	# I hate to do it, but I'm using Poling's ridiculously baroque
	#  Reference module to do so just for consistency
	my $ref = Reference->new($self->{'dbt'},$hr->{'reference_no'});
	return $ref->{pubyr};

}



# Formats the opinion as HTML and
# returns it.
#
# For example, "belongs to Equidae according to J. D. Archibald 1998"
sub formatAsHTML {
	my Opinion $self = shift;
	my $row = $self->{'DBrow'};
	
	my $status = $row->{'status'};
	my $statusPhrase = $row->{'status'};
	
	if (($status eq 'subjective synonym of') || 
		($status eq 'objective synonym of') || 
		($status eq 'homonym of') || 
		($status =~ m/nomen/)) {
			$statusPhrase = "is a $status";
	}

    my ($child,$parent,$spelling);
    if ($status =~ /recombined|corrected|rank/) {
	    $child = Taxon->new($self->{'dbt'},$row->{'child_no'});
    } else {
	    $child = Taxon->new($self->{'dbt'},$row->{'child_spelling_no'});
    }
    my $child_html = ($child) ? $child->taxonNameHTML() : "";
	if ($status =~ m/nomen/ || $status eq 'revalidated') {
		return "'$child_html $statusPhrase' according to " . $self->authors() ;
	} else {
        if ($status =~ /corrected|rank/) {
            $parent = Taxon->new($self->{'dbt'},$row->{'parent_spelling_no'});
            my $parent_html = ($parent) ? $parent->taxonNameHTML() : "";
            $spelling = Taxon->new($self->{'dbt'},$row->{'child_spelling_no'});
            my $spelling_html = ($spelling) ? $spelling->taxonNameHTML() : "";
		    return "'$child_html $statusPhrase $spelling_html and belongs to $parent_html' according to " . $self->authors();
        } elsif ($status =~ /recombined/) {
            $spelling = Taxon->new($self->{'dbt'},$row->{'child_spelling_no'});
            my $spelling_html = ($spelling) ? $spelling->taxonNameHTML() : "";
		    return "'$child_html $statusPhrase $spelling_html' according to " . $self->authors();
        } else {
            $parent = Taxon->new($self->{'dbt'},$row->{'parent_spelling_no'});
            my $parent_html = ($parent) ? $parent->taxonNameHTML() : "";
		    return "'$child_html $statusPhrase $parent_html' according to " . $self->authors();
        }
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
    my $dbt = shift;
	my $hbo = shift;
	my $s = shift;
	my $q = shift;
    my $error_message = shift;
	
    my $dbh = $dbt->dbh;
	
	my %fields;  # a hash of fields and values that
				 # we'll pass to HTMLBuilder to pop. the form.
				 
	# Fields we'll pass to HTMLBuilder that the user can't edit.
	# (basically, any field which is not empty or 0 in the database,
	# if the authorizer is not the original authorizer of the record).
	my @nonEditables; 	
	
	if ((!$dbt) || (!$hbo) || (! $s) || (! $q)) {
		croak("Opinion::displayOpinionForm had invalid arguments passed to it.");
		return;
	}

    # Simple variable assignments
    my $isNewEntry = ($q->param('opinion_no') > 0) ? 0 : 1;
    my $reSubmission = ($error_message) ? 1 : 0;
	my @belongsArray = ('belongs to', 'recombined as', 'revalidated', 'rank changed as','corrected as');
	my @synArray = ('','subjective synonym of', 'objective synonym of', 'homonym of','replaced by');
	my @nomArray = ('','nomen dubium','nomen nudum','nomen oblitum', 'nomen vanum');

    # if the opinion already exists, grab it
    my $o;
    if (!$isNewEntry) {
        $o = Opinion->new($dbt,$q->param('opinion_no'));
        if (!$o) {
            carp "Could not create opinion object in displayOpinionForm for opinion_no ".$q->param('opinion_no');
            return;
        }
    }

    # If a drop down is presented, do not use the parent_no from it if they switched radio buttons
    if (($q->param('orig_taxon_status') && $q->param('orig_taxon_status') ne $q->param('taxon_status'))) {
        $q->param('parent_spelling_no'=>''); 
    }
   
    # Grab the appropriate data to auto-fill the form
	if ($reSubmission) {
		%fields = %{$q->Vars};
	} else {
        if ($isNewEntry) {
            $fields{'child_no'} = $q->param('child_no');
            $fields{'child_spelling_no'} = $q->param('child_spelling_no') || $q->param('child_no');
		    $fields{'reference_no'} = $s->get('reference_no');
            $fields{'spelling_status'} = getSpellingStatus($dbt,\%fields);
        } else {
            %fields = %{$o->getRow()};

            if ($fields{'ref_has_opinion'} !~ /YES/i) {
                $fields{'2nd_pages'} = $fields{'pages'};
                $fields{'2nd_figures'} = $fields{'figures'};
                $fields{'pages'} = '';
                $fields{'figures'} = '';
            }

            # if its from the DB, populate appropriate form vars
            for (@belongsArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'taxon_status'} = 'belongs to'; 
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

            # This is in its own function cause it may/may not equal the actual status
            # and must be inferred
            $fields{'spelling_status'} = $o->getSpellingStatus();
        }
    }

    # Get the child name and rank
    my $childTaxon = Taxon->new($dbt,$fields{'child_no'});
    my $childName = $childTaxon->get('taxon_name');
    my $childRank = $childTaxon->get('taxon_rank');

    
    # its important that we save this in case the user selected a parent_spelling_no from the pulldown
    # AND switches the taxon_status radio. In that case we have to throw out the parent_spelling_no
    $fields{'orig_taxon_status'} = $fields{'taxon_status'};

    # This block gets a list of potential homonyms, either from the database for an edit || resubmission
    # or from the values passed in for a new && resubmission
    my @child_spelling_nos = ();
    my $childSpellingName = "";
    if ($fields{'child_spelling_no'} > 0) {
        # This will happen on an edit (first submission) OR resubmission w/homonyms
        # SQL trick: get not only the authoritiy data for child_spelling_no, but all its homonyms as well
        my $sql = "SELECT a2.* FROM authorities a1, authorities a2 WHERE a1.taxon_name=a2.taxon_name AND a1.taxon_no=".$dbh->quote($fields{'child_spelling_no'});
        my @results= @{$dbt->getData($sql)}; 
        foreach my $row (@results) {
            push @child_spelling_nos, $row->{'taxon_no'};
            $childSpellingName = $row->{'taxon_name'};
        }
    } else {
        $childSpellingName = $q->param('child_spelling_name') || $childName;
        @child_spelling_nos = TaxonInfo::getTaxonNos($dbt,$childSpellingName);
    }
    # If the childSpellingName and childName are the same (and possibly amiguous)
    # Use the child_no as the spelling_no so we unneccesarily don't get a radio select to select
    # among the different homonyms
    if ($childSpellingName eq $childName) {
        @child_spelling_nos = ($fields{'child_no'});
        $fields{'child_spelling_no'} = $fields{'child_no'};
    }
    
    $fields{'child_name'} = $childName;
    $fields{'child_spelling_name'} = $childSpellingName;

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $spelling_pulldown;
	if ( scalar(@child_spelling_nos) > 1) {
        $spelling_pulldown .= qq|<input type="radio" name="child_spelling_no" value=""> \nOther: <input type="text" name="child_spelling_name" value=""><br>\n|;
        foreach my $child_spelling_no (@child_spelling_nos) {
	        my $parent = TaxaCache::getParent($dbt,$child_spelling_no);
			my %auth = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$child_spelling_no)};
            my $selected = ($fields{'child_spelling_no'} == $child_spelling_no) ? "CHECKED" : "";
            my $pub_info = "$auth{author1last} $auth{pubyr}";
            $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
            my $higher_class;
            if ($parent) {
                $higher_class = $parent->{'taxon_name'};
            } else {
                my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$child_spelling_no);
                $higher_class = "unclassified $taxon->{taxon_rank}";
            } 
			$spelling_pulldown .= qq|<input type="radio" name="child_spelling_no" $selected value='$child_spelling_no'> ${childSpellingName}$pub_info [$higher_class]<br>\n|;
        }
	}

    # This does the same thing for the parent
    my @parent_nos = ();
    my $parentName = "";
    if ($fields{'parent_spelling_no'} > 0) {
        # This will happen on an edit (first submission) OR resubmission w/homonyms
        # SQL trick: get not only the authoritiy data for parent_no, but all its homonyms as well
        my $sql = "SELECT a2.* FROM authorities a1, authorities a2 WHERE a1.taxon_name=a2.taxon_name AND a1.taxon_no=".$dbh->quote($fields{'parent_spelling_no'});
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
        } else {
            if ($childSpellingName =~ / /) {
                my @bits = split(/\s+/,$childSpellingName);
                pop @bits;
                $parentName = join(" ",@bits);
            }
        }
        if ($parentName) {
            @parent_nos = TaxonInfo::getTaxonNos($dbt,$parentName);
        }
    }


	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $parent_pulldown;
	if ( scalar(@parent_nos) > 1) {
        $parent_pulldown .= qq|<input type="radio" name="parent_spelling_no" value=""> \nOther: <input type="text" name="belongs_to_parent" value=""><br>\n|;
        foreach my $parent_no (@parent_nos) {
	        my $parent = TaxaCache::getParent($dbt,$parent_no);
			my %auth = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$parent_no)};
            my $selected = ($fields{'parent_spelling_no'} == $parent_no) ? "CHECKED" : "";
            my $pub_info = "$auth{author1last} $auth{pubyr}";
            $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
            my $higher_class;
            if ($parent) {
                $higher_class = $parent->{'taxon_name'}
            } else {
                my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$parent_no);
                $higher_class = "unclassified $taxon->{taxon_rank}";
            }
			$parent_pulldown .= qq|<input type="radio" name="parent_spelling_no" $selected value='$parent_no'> ${parentName}$pub_info [$higher_class]<br>\n|;
        }
	}

    main::dbg("parentName $parentName parents: ".scalar(@parent_nos));
    main::dbg("childSpellingName $childSpellingName spellings: ".scalar(@child_spelling_nos));

    if (!$isNewEntry) {
        if ($o->get('authorizer_no')) {
            $fields{'authorizer_name'} = " <B>Authorizer:</B> " . Person::getPersonName($dbt,$o->get('authorizer_no')); 
        }   
        if ($o->get('enterer_no')) { 
            $fields{'enterer_name'} = " <B>Enterer:</B> " . Person::getPersonName($dbt,$o->get('enterer_no')); 
        }
        if ($o->get('modifier_no')) { 
            $fields{'modifier_name'} = " <B>Modifier:</B> ".Person::getPersonName($dbt,$o->get('modifier_no'));
        }
    }

    # Handle radio button
    if ($fields{'ref_has_opinion'} eq 'YES') {
		$fields{'ref_has_opinion_checked'} = 'checked';
		$fields{'ref_has_opinion_notchecked'} = '';
    } elsif (exists $fields{'ref_has_opinion'}) {
		$fields{'ref_has_opinion_checked'} = '';
		$fields{'ref_has_opinion_notchecked'} = 'checked';
	}

	
	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.  However, if they're superuser
	# (alroy, alroy), then let them edit anything.
	#
	# otherwise, they can edit any field.
	my $sesAuth = $s->get('authorizer_no');

    # A list of people who have permitted the current authorizer to edit their records
    my $p = Permissions->new($s,$dbt);
    my %is_modifier_for = %{$p->getModifierList()};  

	if ($s->isSuperUser()) {
		$fields{'message'} = "<p align=center><i>You are the superuser, so you can edit any field in this record!</i></p>";
	} elsif ((! $isNewEntry) && (!$is_modifier_for{$o->get('authorizer_no')}) && ($sesAuth != $o->get('authorizer_no')) && ($o->get('authorizer_no') != 0)) {
	
		# grab the authorizer name for this record.
		my $authName = $fields{'authorizer_name'};
	
		$fields{'message'} = "<p align=center><i>This record was created by a different authorizer ($authName) so you can only edit empty fields.</i></p>";
		
		# we should always make the ref_has_opinion radio buttons disabled
		# because only the original authorizer can edit these.
		
		
		# depending on the status of the ref_has_opinion radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_has_opinion'} eq 'YES') {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
		}
		
		
        # Required fields which will always be set 
		push(@nonEditables, 'taxon_status', 'nomen','synonym','belongs_to_parent','recombined_as_parent','synonym_parent');
		push(@nonEditables, 'ref_has_opinion','child_spelling_name','spelling_status');
		
		if ($fields{'status'} ne 'recombined as' && ($fields{status} ne 'belongs to')) {
			push(@nonEditables, 'diagnosis');
		}
				
		# find all fields in the database record which are not empty and add them to the list.
        while (my ($field,$value)=each %{$o->getRow()}) {
            push(@nonEditables,$field) if ($value);
		}
		
		# we'll also have to add a few fields separately since they don't exist in the database,
		# but only in our form.
		if ($fields{'2nd_pages'}) { push(@nonEditables, '2nd_pages'); }
		if ($fields{'2nd_figures'}) { push(@nonEditables, '2nd_figures'); }
		
	}


    # Each of the 'taxon_status' row options
	my $belongs_to_row;
    my $spelling_row;
    my $synonym_row;
    my $nomen_row;

    # standard approach to get belongs to
    # build the spelling pulldown, if necessary, else the spelling box
    my ($higher_rank);
    if ($childRank eq 'subspecies') {
        $higher_rank =  'species';
    } elsif ($childRank eq 'species') {
        $higher_rank =  'genus';
    }  else {
        $higher_rank = 'higher taxon';
    }
    my $selected = ($fields{'taxon_status'} eq 'belongs to') ? "CHECKED" : "";
    my $colspan = ($parent_pulldown && $selected) ? "": "colspan=2";
    $belongs_to_row .= qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="belongs to" $selected></td>\n|;
    if ($childRank =~ /species/) {
        $belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid $childRank</b>, classified as belonging to $higher_rank |;
    } else {
        $belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid or reranked $childRank</b>, classified as belonging to $higher_rank |;
    }

    if ($parent_pulldown && $selected) {
        $belongs_to_row .= "<td width='100%'>$parent_pulldown</td>";
    } else	{
        my $parentTaxon = ($selected || ($isNewEntry && $childRank =~ /species/)) ? $parentName : "";
        $belongs_to_row .= qq|<input name="belongs_to_parent" size="30" value="$parentTaxon">|;
    }
    $belongs_to_row .= "</td></tr>";
    if (!$reSubmission && !$isNewEntry) {

        my @taxa = Taxon::getTypeTaxonList($dbt,$fields{'child_no'},$fields{'reference_no'});
        $fields{'type_taxon'} = 0;
        foreach my $row (@taxa) {
            if ($row->{'type_taxon_no'} == $fields{'child_no'}) {
                $fields{'type_taxon'} = 1;
            }
        }
    }
    if ($childRank =~ /species|genus|tribe|family/) {
        my $checked = ($fields{'type_taxon'}) ? "CHECKED" : "";
        $belongs_to_row .= "<tr><td></td><td>".
                       "<input name=\"type_taxon\" type=\"checkbox\" $checked  value=\"1\">".
                       " This is the type $childRank".
                       "</td></tr>";
    }                       

	# format the synonym section
	# note: by now we already have a status pulldown ready to go; we're
	#  tacking on either another pulldown or an input to store the name
	# need a pulldown if this is a second pass and we have multiple
	#  alternative senior synonyms
    $selected = ($fields{'taxon_status'} eq 'invalid1') ? "CHECKED" : "";
    $colspan = ($parent_pulldown && $selected) ? "": "colspan=2";
    $synonym_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="invalid1" $selected></td>|;
	$synonym_row .= "<td colspan=2 valign='top'><b>Invalid</b>, and another name should be used.</td></tr>";
    $synonym_row .= "<tr><td></td><td $colspan valign='top' nowrap>Status: "; 
	# actually build the synonym popup menu.
	$synonym_row .= $hbo->buildSelect('synonym',\@synArray, \@synArray, $fields{'synonym'});
	if ($parent_pulldown && $selected) {
        $parent_pulldown =~ s/belongs_to_parent/synonym_parent/;
		$synonym_row .= "<td width='100%'>$parent_pulldown</td>"; 
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
    $nomen_row .= $hbo->buildSelect('nomen',\@nomArray, \@nomArray, $fields{'nomen'});
    $nomen_row .= "</td></tr>";

    # build the spelling pulldown, if necessary, else the spelling box
    my $all_ranks = "";
    if ($childRank eq 'subspecies') {
        $all_ranks = ' (genus, species, and subspecies)';
    } elsif ($childRank eq 'species') {
        $all_ranks = ' (genus and species)';
    } 
    my $spelling_note .= "<small>Note that the name may be different than listed above due to a correction, recombination, or rank change.</small>";
    $spelling_row .= "<tr><td colspan=2>Please enter the full name of the taxon as used in the reference${all_ranks}:</td></tr>";
    if ($spelling_pulldown) {
        $spelling_row .= "<tr><td nowrap width=\"100%\">$spelling_pulldown<br>$spelling_note</td></tr>";
    } else {
        $spelling_row .= qq|<tr><td nowrap width="100%"><input id="child_spelling_name" name="child_spelling_name" size=30 value="$childSpellingName"><br>$spelling_note</td></tr>|;
    }   

#    my @select_values = ('belongs to','corrected as','recombined as','rank changed as','lapsus calami for');
#    my @select_keys = ("was the original spelling $childName","was a correction of $childName","is a recombination of $childName","had its rank changed from it's original rank of $childRank","was a lapsus calami for an older spelling");
    my @select_values = ('belongs to','recombined as','corrected as','rank changed as');
    my @select_keys = ("is the original spelling of '$childName'","is a recombination of '$childName'","is a correction of '$childName'","has had its rank changed from its original rank of $childRank");
    $spelling_row .= "<tr><td>&nbsp;</td></tr>";
    $spelling_row .= "<tr><td>Enter the reason why this spelling was used:<br>This name ". $hbo->buildSelect('spelling_status',\@select_keys,\@select_values,$fields{'spelling_status'})."</td></tr>";

    main::dbg("showOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");

	$fields{belongs_to_row} = $belongs_to_row;
	$fields{nomen_row} = $nomen_row;
	$fields{synonym_row} = $synonym_row;
	$fields{spelling_row} = $spelling_row;

	# print the form	
    $fields{'error_message'} = $error_message;
	my $html = $hbo->newPopulateHTML("add_enter_opinion", \%fields, \@nonEditables);

	print $html;
}


# Call this when you want to submit an opinion form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
#
# rjp, 3/2004.
sub submitOpinionForm {
    my $dbt = shift;
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
  
    my @warnings = ();

	if ((!$dbt) || (!$hbo) || (!$s) || (!$q)) {
		croak("Opinion::submitOpinionForm had invalid arguments passed to it");
		return;	
	}
	
    my $dbh = $dbt->dbh;
	my $errors = Errors->new();

	# build up a hash of fields/values to enter into the database
	my %fields;
	
	# Simple checks
    my $isNewEntry = ($q->param('opinion_no') > 0) ? 0 : 1;

    # if the opinion already exists, grab it
    my $o;
    if (!$isNewEntry) {
        $o = Opinion->new($dbt,$q->param('opinion_no'));
        if (!$o) {
            carp "Could not create opinion object in displayOpinionForm for opinion_no ".$q->param('opinion_no');
            return;
        }
        $fields{'child_no'} = $o->get('child_no');
        $fields{'reference_no'} = $o->get('reference_no');
    } else {	
        $fields{'child_no'} = TaxonInfo::getOriginalCombination($dbt,$q->param('child_no')); 
		$fields{'reference_no'} = $s->get('reference_no');
		
		if (! $fields{'reference_no'} ) {
			$errors->add("You must set your current reference before submitting a new opinion");	
		}
	} 

    # Get the child name and rank
	my $childTaxon = Taxon->new($dbt,$fields{'child_no'});
	my $childName = $childTaxon->get('taxon_name');
	my $childRank = $childTaxon->get('taxon_rank');

    # If a drop down is presented, do not use the parent_spelling_no from it if they switched radio buttons
    if (($q->param('orig_taxon_status') && $q->param('orig_taxon_status') ne $q->param('taxon_status'))) {
        $q->param('parent_spelling_no'=>''); 
    }



    ############################
    # Validate the form, top section
    ############################
    
	## Deal with the reference section at the top of the form.  This
	## is almost identical to the way we deal with it in the authority form
	## so this functionality should probably be merged at some point.
	if (($q->param('ref_has_opinion') ne 'YES') && 
		($q->param('ref_has_opinion') ne 'NO')) {
		$errors->add("You must choose one of the reference radio buttons");
	}

	# JA: now Poling does the bulk of the checks on the reference
	elsif ($q->param('ref_has_opinion') eq 'NO') {

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
		    my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion !='YES' ".
                      " AND child_no=".$dbh->quote($fields{'child_no'}).
                      " AND author1last=".$dbh->quote($q->param('author1last')).
                      " AND author2last=".$dbh->quote($q->param('author2last')).
                      " AND pubyr=".$dbh->quote($q->param('pubyr'));
            my $row = ${$dbt->getData($sql)}[0];
            if ( $row->{'c'} > 0 ) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		# merge the pages and 2nd_pages, figures and 2nd_figures fields
		# together since they are one field in the database.
		$fields{'pages'} = $q->param('2nd_pages');
		$fields{'figures'} = $q->param('2nd_figures');
		
		if (! $q->param('author1last')) {
			$errors->add('You must enter at least the last name of the first author');	
		}
		
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
			
			if (! Validation::properYear($pubyr)) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new($dbt,$q->param('reference_no'));
			if ($ref && $pubyr > $ref->get('pubyr')) {
				$errors->add("The publication year ($pubyr) can't be more recent than that of the primary reference (" . $ref->get('pubyr') . ")");
			}
		} else {
            $errors->add("A publication year is required");
        }
	} else {
		# if they chose ref_has_opinion, then we also need to make sure that there
		# are no other opinions about the current taxon (child_no) which use 
		# this as the reference.  
        if ($isNewEntry) {
		    my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion='YES'".
                      " AND child_no=".$dbh->quote($fields{'child_no'}).
                      " AND reference_no=".$dbh->quote($fields{'reference_no'});
            my $row = ${$dbt->getData($sql)}[0];
            if ($row->{'c'} > 0) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		# ref_has_opinion is YES
		# so make sure the other publication info is empty.
		
		if ($q->param('author1init') || $q->param('author1last') || $q->param('author2init') || $q->param('author2last') || $q->param('otherauthors') || $q->param('pubyr') || $q->param('2nd_pages') || $q->param('2nd_figures')) {
			$errors->add("Don't enter any other publication information if you chose the 'first named in primary reference' radio button");	
		}
	}
	
	{
		# also make sure that the pubyr of this opinion isn't older than
		# the pubyr of the authority record the opinion is about
		my $ref = Reference->new($dbt, $q->param('reference_no'));
		if ( $ref && $childTaxon->pubyr() > $ref->get('pubyr') ) {
			$errors->add("The publication year (".$ref->get('pubyr').") for this opinion can't be earlier than the year the taxon was named (".$childTaxon->pubyr().")");	
        }
        if ( $childTaxon->pubyr() > $q->param('pubyr') && $q->param('pubyr') > 1700 ) {
			$errors->add("The publication year (".$q->param('pubyr').") for the authority listed in this opinion can't be earlier than the year the taxon was named (".$childTaxon->pubyr().")");	
		}
	}

    # Get the parent name and rank, and parent spelling
    my $parentName = '';
    my $parentRank = '';
    if ($q->param('parent_spelling_no')) {
        # This is a second pass through, b/c there was a homonym issue, the user
        # was presented with a pulldown to distinguish between homonyms, and has submitted the form
        my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_no=".$dbh->quote($q->param('parent_spelling_no'));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            croak("Fatal error, parent_spelling_no ".$q->param('parent_spelling_no')." was set but not in the authorities table");
        }
        $parentName = $row->{'taxon_name'};
        $parentRank = $row->{'taxon_rank'};
        $fields{'parent_spelling_no'} = $q->param('parent_spelling_no');
        $fields{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$fields{'parent_spelling_no'});
    } else {
        # This block of code deals with a first pass through, when no homonym problems have yet popped up
        # We want to:
        #  * Hit the DB to make sure theres exactly 1 copy of the parent name (error if > 1 or 0)
        if ($q->param('taxon_status') eq 'belongs to') {
            $parentName = $q->param('belongs_to_parent');	
        } elsif ($q->param('taxon_status') eq 'invalid1') {
            $parentName = $q->param('synonym_parent');
        } else {
            $parentName = undef;
        }

        # Get a single parent no or we have an error
        if ($q->param('taxon_status') ne 'invalid2') {
            if (!$parentName) {
                if ($q->param('taxon_status') eq 'invalid1') {
                    $errors->add("You must enter the name of a taxon of equal rank to synonymize or replace this name with");
                } else {
                    $errors->add("You must enter the name of a higher taxon this taxon belongs to");
                }
            } else {    
                my @parents = TaxonInfo::getTaxon($dbt,'taxon_name'=>$parentName); 
                if (scalar(@parents) > 1) {
                    $errors->add("The taxon '$parentName' exists multiple times in the database. Please select the one you want");	
                } elsif (scalar(@parents) == 0) {
                    $errors->add("The taxon '$parentName' doesn't exist in our database.  Please <A HREF=\"bridge.pl?action=displayAuthorityForm&taxon_no=-1&taxon_name=$parentName\">create a new authority record for '$parentName'</a> <i>before</i> entering this opinion");	
                } elsif (scalar(@parents) == 1) {
                    $fields{'parent_spelling_no'} = $parents[0]->{'taxon_no'};
                    $fields{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$fields{'parent_spelling_no'});
                    $parentRank = $parents[0]->{'taxon_rank'};
                }
            }
        } else {
            $fields{'parent_spelling_no'} = 0;
            $fields{'parent_no'} = 0;
        }
    }

    # get the (child) spelling name and rank. 
    my $childSpellingName = '';
    my $childSpellingRank = '';
    my $createAuthority = 0;
    if ($q->param('child_spelling_no')) {
        # This is a second pass through, b/c there was a homonym issue, the user
        # was presented with a pulldown to distinguish between homonyms, and has submitted the form
        my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_no=".$dbh->quote($q->param('child_spelling_no'));
        my $row = ${$dbt->getData($sql)}[0];
        if (!$row) {
            croak("Fatal error, child_spelling_no ".$q->param('child_spelling_no')." was set but not in the authorities table");
        }
        $childSpellingName = $row->{'taxon_name'};
        $childSpellingRank = $row->{'taxon_rank'};
        $fields{'child_spelling_no'} = $q->param('child_spelling_no');
    } else {
        if ($childName eq $q->param('child_spelling_name')) {
            # This is the simplest case - if the childName and childSpellingName are the same, they get
            # the same taxon_no - don't present a pulldown in this case, even if the name is a homonym,
            # since we already resolved the homonym issue before we even got to this form
            $childSpellingName = $childName;
            $childSpellingRank = $childRank;
            $fields{'child_spelling_no'} = $fields{'child_no'};
        } else {
            # Otherwise, go through the whole big routine
            $childSpellingName = $q->param('child_spelling_name');
            my @spellings = TaxonInfo::getTaxon($dbt,'taxon_name'=>$childSpellingName); 
            if (scalar(@spellings) > 1) {
                $errors->add("The spelling '$childSpellingName' exists multiple times in the database. Please select the one you want");	
            } elsif (scalar(@spellings) == 0) {
                if ($q->param('spelling_status') =~ /rank changed as/) {
                    $errors->add("The taxon '$childSpellingName' doesn't exist in our database.  Please <a href=\"bridge.pl?action=submitTaxonSearch&goal=authority&taxon_no=-1&taxon_name=$childSpellingName\">add this authority</a> through the authorities form before proceeding");	
                } else {
                    if ($q->param('confirm_create_authority') eq $q->param('child_spelling_name')) {
                        $createAuthority = 1;
                        $childSpellingRank = $childRank;
                    } else {
                        $q->param('confirm_create_authority'=>$q->param('child_spelling_name'));
                        $errors->add("The taxon '$childSpellingName' doesn't exist in our database.  If this isn't a typo, hit submit again to automatically create an authority record for it");	
                    }
                }
            } elsif (scalar(@spellings) == 1) {
                $fields{'child_spelling_no'} = $spellings[0]->{'taxon_no'};
                $childSpellingRank = $spellings[0]->{'taxon_rank'};
            }
        }
    }

    main::dbg("child_no $fields{child_no} childRank $childRank childName $childName ");
    main::dbg("child_spelling_no $fields{child_spelling_no} childSpellingRank $childSpellingRank childSpellingName $childSpellingName");
    main::dbg("parent_no $fields{parent_no}  parent_spelling_no $fields{parent_spelling_no} parentSpellingRank $parentRank parentSpellingName $parentName");
    

    ############################
    # Validate the form, bottom section
    ############################
    # Flatten the status
    my $status;
    if ($q->param('taxon_status') =~ /belongs to/) {
        $status = $q->param('spelling_status');
    } elsif ($q->param('taxon_status') eq 'invalid1') {
        $status = $q->param('synonym');
    } elsif ($q->param('taxon_status') eq 'invalid2') {
        $status = $q->param('nomen');
    }
	$fields{'status'} = $status;

	if (!$q->param("taxon_status")) {
		$errors->add("You must choose one of radio buttons in the \"How was it classified?\" section");	
	} elsif (!$status) {
        $errors->add("You must choose one of the values in the status pulldown");
    }
    
    if ($q->param('taxon_status') ne 'belongs to' && $q->param('type_taxon')) {
        $errors->add("The valid taxon radio button in the \"How was it classified?\" section must be selected if the is the type taxon");
    } 

    if ($q->param('spelling_status') eq 'rank changed as') {
        if ($childRank =~ /species/ || $parentRank =~ /species/ || $childName =~ / / || $parentName =~ / /) {
            $errors->add("You may not change the rank of a taxon if it is a species");
        }
    }

    # Error checking related to ranks
    # Only bother if we're down to one parent
    if ($fields{'parent_spelling_no'}) {
	    if ($q->param('taxon_status') eq 'belongs to') {
		    # for belongs to, the parent rank should always be higher than the child rank.
		    # unless either taxon is an unranked clade (JA)
		    if ($rankToNum{$parentRank} <= $rankToNum{$childSpellingRank} && 
                $parentRank ne "unranked clade" && 
                $childSpellingRank ne "unranked clade") {
		    	$errors->add("The rank of the higher taxon '$parentName' ($parentRank) must be higher than the rank of '$childName' ($childRank)");	
		    }
        } elsif ($q->param('taxon_status') eq 'invalid1') {
    		# the parent rank should be the same as the child rank...
	    	if ( $parentRank ne $childSpellingRank) {
		    	$errors->add("The rank of a taxon and the rank of its synonym, homonym, or replacement name must be the same");
            }    
		} 
    }

    # Some more rank checks
    if ($fields{'child_spelling_no'}) {
        if ($q->param('spelling_status') eq 'rank changed as') {
    		# JA: ... except if the status is "rank changed as," which is actually the opposite case
		    if ( $childSpellingRank eq $childRank) {
			    $errors->add("If you change a taxon's rank, its old and new ranks must be different");
            }
	    } else {
		    if ($childSpellingRank ne $childRank) {
			    $errors->add("If a taxon's name is corrected or recombined the rank of its new spelling must be the same as the rank of its original spelling");
		    }
        }    
    }    

    # error checks related to naming
    # If something is marked as a corrected/recombination/rank change, its spellingName should be differenct from its childName
    # and the opposite is true its an original spelling (they're the same);
    if ($q->param('spelling_status') eq 'belongs to') {
        if ($childSpellingName ne $childName) {
            $errors->add("If \"This name is the original spelling of '$childName'\" is selected, you must enter '$childName' in the \"How was it spelled?\" section");
        }
    } else {
        if ($childSpellingName eq $childName) {
            $errors->add("If you use the original spelling in the \"How was it spelled?\" section, please select \"This name was the original spelling of '$childName'\" in the dropdown");
        }
    }
        
    # the genus name should differ for recombinations, but be the same for everything else
    if ($childRank =~ /species/) {
        my @childBits = split(/ /,$childName);
        pop @childBits;
        my $childParent = join(' ',@childBits);
        my @spellingBits = split(/ /,$childSpellingName);
        pop @spellingBits;
        my $spellingParent = join(' ',@spellingBits);
        if ($q->param('spelling_status') eq 'recombined as') {
            if ($spellingParent eq $childParent) {
			    $errors->add("The genus name in the new combination must be different from the genus name in the original combination");
		    }
        } else {
            if ($spellingParent ne $childParent) {
                $errors->add("The genus name of the spelling must be the same as the original genus name when choosing \"This name is a correction\" or \"This name is the original spelling\"");
            }
        }
        if ($q->param('taxon_status') eq 'belongs to' && $spellingParent ne $parentName) {
            $errors->add("The genus name in the \"How was it classified?\" and the \"How was it spelled?\" sections must be the same");
        }
    } else {
        if ($q->param('spelling_status') eq 'recombined as') {
            $errors->add("Don't mark this as a recombination if the taxon isn't a species or subspecies");
        }
    }

    # Misc error checking 
    if (($parentName eq $childName || $parentName eq $childSpellingName) && $childSpellingRank !~ /subgenus/) {
        $errors->add("The taxon you are entering and the one it belongs to can't have the same name");	
    } elsif ($fields{'child_no'} == $fields{'parent_no'}) {
        $errors->add("The taxon you are entering and the one it belongs to can't be the same");	
    }

	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( (! ($q->param('spelling_status') eq 'recombined as' || $q->param('spelling_status') eq 'belongs to')) && ($q->param('diagnosis'))) {
		$errors->add("Don't enter a diagnosis unless you choose the 'belongs to' or 'recombined as' radio button");
	}

	# this $editAny variable is true if they can edit any field,
	# false if they can't.
    my $editAny = ($isNewEntry || $s->isSuperUser() || $s->get('authorizer_no') == $o->get('authorizer_no')) ? 1 : 0;
    # Add editable fields to fields hash
	foreach my $formField ($q->param()) {
		my $okayToEdit = $editAny;
		if (! $okayToEdit) {
			if (! $o->get($formField)) {
				$okayToEdit = 1;
			}
		}
		if ($okayToEdit) {
			if (! $fields{$formField}) {
				$fields{$formField} = $q->param($formField);
			}
		}
	}

	
	# correct the ref_has_opinion field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($fields{'ref_has_opinion'} eq 'NO') {
		$fields{'ref_has_opinion'} = '';
	}
	
	# at this point, we should have a nice hash array (%fields) of
	# fields and values to enter into the authorities table.
	if ($errors->count() > 0) {
		# put a message in a hidden to let us know that we have already displayed
		# some errors to the user and this is at least the second time through (well,
		# next time will be the second time through - whatever).

		# stick the errors in the CGI object for display.
		my $message = $errors->errorMessage();

		Opinion::displayOpinionForm($dbt, $hbo, $s, $q, $message);
		return;
	}
	
	
	# now we'll actually insert or update into the database.

	# first step is to create the parent taxon if a species is being
	#  recombined and the new combination doesn't exist JA 14.4.04
	# WARNING: this is very dangerous; typos in parent names will
	# create bogus combinations, and likewise if the opinion create/update
	#  code below bombs
	if ($createAuthority) {
	    # next we need to steal data from the opinion
        my %record = (
            'reference_no' => $fields{'reference_no'},
            'taxon_rank'=> $childSpellingRank,
            'taxon_name'=> $childSpellingName
        );

        # author information comes from the original combination,
        # I'm doing this the "old" way instead of using some
        #  ridiculously complicated Poling-style objects
		my $sql = "SELECT * FROM authorities WHERE taxon_no=" . $fields{'child_no'};
		my $aref = ${$dbt->getData($sql)}[0];
		my @origAuthFields = ( "taxon_rank", "pages","figures", "comments" );
		for my $af ( @origAuthFields )	{
			if ( $aref->{$af} )	{
                $record{$af}=$aref->{$af};
			}
		}
		@origAuthFields = ( "author1init", "author1last","author2init", "author2last","otherauthors", "pubyr" );
		if ( $aref->{'author1last'} )	{
			for my $af ( @origAuthFields )	{
				if ( $aref->{$af} )	{
                    $record{$af}=$aref->{$af};
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
                    $record{$af}=$rref->{$af};
				}
			}
		}

        my ($return_code, $taxon_no) = $dbt->insertRecord($s,'authorities', \%record);
        TaxaCache::addName($dbt,$taxon_no);
        main::dbg("create new authority record, got return code $return_code");
        if (!$return_code) {
            die("Unable to create new authority record for $record{taxon_name}. Please contact support");
        }
        $fields{'child_spelling_no'} = $taxon_no;

        # if the taxon name is unique, find matches to it in the
        #  occurrences table and set the taxon numbers appropriately
        # start with a test for uniqueness
        $sql = "SELECT taxon_no FROM authorities WHERE taxon_name=".$dbh->quote($record{'taxon_name'});
        my @taxon_nos = @{$dbt->getData($sql)};
        @taxon_nos = map {$_->{'taxon_no'}} @taxon_nos;

        if (scalar(@taxon_nos) > 1) {
            # Deal with homonym issue
            my $sql1 = "UPDATE occurrences SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
            my $sql2 = "UPDATE reidentifications SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
            $dbt->getData($sql1);
            $dbt->getData($sql2);
            push @warnings, "Since $record{taxon_name} is a homonym, occurrences of it are no longer classified.  Please go to \"<a target=\"_BLANK\" href=\"bridge.pl?action=displayCollResults&type=reclassify_occurrence&taxon_name=$record{taxon_name}\">Reclassify occurrences</a>\" and manually classify <b>all</b> occurrences of this taxon.";
        } elsif (scalar(@taxon_nos) == 1) {
            # start composing update sql
            # NOTE: in theory, taxon no for matching records always
            #  should be zero, unless the new name is a species and
            #  some matching records were set on the basis of their
            #  genus, in which case we assume users will want the
            #  new number for the species instead; that's why there
            #  is no test to make sure the taxon no is empty
            my $sql1 = "UPDATE occurrences SET modified=modified,taxon_no=".$taxon_no." WHERE ";
            my $sql2 = "UPDATE reidentifications SET modified=modified,taxon_no=".$taxon_no. " WHERE ";

            my ($genus,$species) = split / /,$record{'taxon_name'};
            $sql1 .= " genus_name=".$dbh->quote($genus);
            $sql2 .= " genus_name=".$dbh->quote($genus);
            if ($species) {
                $sql1 .= " AND species_name=".$dbh->quote($species);
                $sql2 .= " AND species_name=".$dbh->quote($species);
            }
            # update the occurrences and reidentifications tables
            $dbt->getData($sql1);
            $dbt->getData($sql2);
            main::dbg("sql to update occs: $sql1");
            main::dbg("sql to update reids: $sql2");
        }
	}

	my $resultOpinionNumber;
    my $resultReferenceNumber = $fields{'reference_no'};

    main::dbg("submitOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");
	if ($isNewEntry) {
		my $code;	# result code from dbh->do.
	
		# make sure we have a taxon_no for this entry...
		if (!$fields{'child_no'} ) {
			croak("Opinion::submitOpinionForm, tried to insert a record without knowing its child_no (original taxon)");
			return;	
		}
		
		($code, $resultOpinionNumber) = $dbt->insertRecord($s,'opinions', \%fields);

		if ($code && ($fields{'spelling_status'} =~ /recombined as|corrected as|rank changed as/)) { 
		    # At this point, *if* the status of the new opinion was 'recombined as',
		    # migrated opinions to the original combination.
            if ($fields{'child_no'} && $fields{'child_spelling_no'}) {
                my $rsql = "UPDATE opinions SET child_no=$fields{child_no} WHERE child_no=$fields{child_spelling_no}";
                main::dbg("Move opinions to point FROM: $rsql");;
                my $return = $dbt->getData($rsql);
                if (!$return) {
                    carp "Failed to move opinions off of recombined|corrected as|rank changed as taxon ($fields{child_spelling_no} to original combination ($fields{child_no}) for opinion no $code";
                }
            }
            # Secondly, opinions must point to the original combination as well
            if ($fields{'child_no'} && $fields{'child_spelling_no'}) {
                my $rsql = "UPDATE opinions SET parent_no=$fields{child_no} WHERE parent_no=$fields{child_spelling_no} AND child_no != $fields{child_no}";
                main::dbg("Move opinions to point TO: $rsql");;
                my $return = $dbt->getData($rsql);
                if (!$return) {
                    carp "Failed to move opinions to point to original comb ($fields{child_no} that pointed to recomb name ($fields{parent_no}) for opinion no $code";
                }
            }
            # Lastly, reset the status of opinions to be 'corrected as','recombined as', or 'rank changed as' as necessary
            # of other migrated opinions
            my $sql = "SELECT * FROM opinions WHERE status LIKE 'belongs to' AND child_no != child_spelling_no AND child_no=$fields{child_no}";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                my $taxon = {};
                $taxon->{'child_no'} = $row->{'child_no'};
                $taxon->{'child_spelling_no'} = $row->{'child_spelling_no'};
                my $spelling_status = getSpellingStatus($dbt,$taxon);
                if ($spelling_status =~ /rank|recombined|corrected/) {
                    $sql = "UPDATE opinions SET modified=modified,status='$spelling_status' WHERE opinion_no=$row->{opinion_no} LIMIT 1";
                    main::dbg("Also changing opinion # $row->{opinion_no} from 'belongs to' to $spelling_status with sql $sql");
                    $dbh->do($sql);
                }
            }
		}	
        #TaxaCache::updateCache($dbt,$fields{'child_no'});
        #TaxaCache::markForUpdate($dbt,$fields{'child_no'});
	} else {
		# if it's an old entry, then we'll update.
		# Delete some fields that should never be updated...
		delete $fields{reference_no};
		
		$resultOpinionNumber = $o->get('opinion_no');
		$dbt->updateRecord($s,'opinions', 'opinion_no',$resultOpinionNumber, \%fields);
        #TaxaCache::updateCache($dbt,$fields{'child_no'});
        #TaxaCache::markForUpdate($dbt,$fields{'child_no'});

	}
    my $pid = fork();
    if (!defined($pid)) {
        carp "ERROR, could not fork";
    }

    if ($pid) {
        # Child fork
        # Don't exit here, have child go on to print message
    } else {
        #my $session_id = POSIX::setsid();

        # Make new dbh and dbt objects - for some reason one connection
        # gets closed whent the other fork exits, so split them here
        my $dbh2 = DBConnection::connect();
        my $dbt2 = DBTransactionManager->new($dbh2); 

        # This is the parent fork.  Have the parent fork
        # Do the useful work, the child fork will be terminated
        # when the parent is so don't have it do anything long running
        # (just terminate). The defined thing is in case the work didn't work

        # Close references to stdin and stdout so Apache
        # can close the HTTP socket conneciton
        if (defined $pid) {
            open STDIN, "</dev/null";
            open STDOUT, ">/dev/null";
            #open STDOUT, ">>SOMEFILE";
        }
        TaxaCache::updateCache($dbt2,$fields{'child_no'});
        exit;
    }         


	
    $o = Opinion->new($dbt,$resultOpinionNumber); 
    my $opinionHTML = $o->formatAsHTML();
    $opinionHTML =~ s/according to/of/i;

	my $enterupdate = ($isNewEntry) ? 'entered into' : 'updated in';

    my $end_message = "";
    if (@warnings) {
        $end_message .= "<DIV class=\"warning\">";
        $end_message .= "Warnings inserting/updating opinion:<BR>";
        $end_message .= "<LI>$_</LI>" for (@warnings);
        $end_message .= "</DIV>";
    }  

    $end_message .= qq|
<div align="center">
<h3> The opinion $opinionHTML has been $enterupdate the database</h3>
<p>
<table cellpadding=10><tr><td valign=top>
  <br><li><b><a href="bridge.pl?action=displayAuthorityForm&taxon_no=$fields{child_spelling_no}">Edit $childSpellingName</a></b></li>
  <br><li><b><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$fields{child_no}">Get general information about $childName</a></b></li>   
  <br><li><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit names from the same reference</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm&use_reference=current">Add/edit another taxon</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm">Add/edit another taxon from another reference</a></b></li>
</td>
<td valign=top>
  <br><li><b><a href="bridge.pl?action=displayOpinionForm&opinion_no=$resultOpinionNumber">Edit this opinion</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionForm&opinion_no=-1&skip_ref_check=1&child_spelling_no=$fields{child_spelling_no}&child_no=$fields{child_no}">Add another opinion about $childSpellingName</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}">Edit other opinions about $childSpellingName</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit opinions from the same reference</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionTaxonSearchForm&use_reference=current">Add/edit opinion about another taxon</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionTaxonSearchForm">Add/edit opinion about another taxon from another reference</a></b></li>
</td></tr></table>
</p>
</div>|;

    # See Taxon::displayTypeTaxonSelectForm for details
    Taxon::displayTypeTaxonSelectForm($dbt,$s,$fields{'type_taxon'},$fields{'child_no'},$childName,$childRank,$resultReferenceNumber,$end_message);
    
}

# Displays a form which lists all opinions that currently exist
# for a reference no/taxon
# Moved/Adapted from Taxon::displayOpinionChoiceForm PS 01/24/2004
sub displayOpinionChoiceForm{
    my $dbt = shift;
    my $s = shift;
    my $q = shift;
    my $dbh = $dbt->dbh;

    print "<div align=\"center\">";
    print "<table><tr><td>";
    if ($q->param('taxon_no')) {
        my $child_no = $q->param('taxon_no');
        my $orig_no = TaxonInfo::getOriginalCombination($dbt,$child_no);
        my $sql = "SELECT o.opinion_no FROM opinions o ".
                  " LEFT JOIN refs r ON r.reference_no=o.reference_no".
                  " WHERE o.child_no=$orig_no".
                  " ORDER BY IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) ASC";
        my @results = @{$dbt->getData($sql)};
        
        my $t = Taxon->new($dbt,$child_no);
        print "<div align=\"center\">";
        print "<h3>Which opinion about ".$t->taxonNameHTML()." do you want to edit?</h3>\n";
        
        print qq|<form method="POST" action="bridge.pl">
                 <input type="hidden" name="action" value="displayOpinionForm">\n
                 <input type="hidden" name="child_no" value="$orig_no">\n
                 <input type="hidden" name="child_spelling_no" value="$child_no">\n|;
        if ($q->param('use_reference') eq 'current' && $s->get('reference_no')) {
            print qq|<input type="hidden" name="skip_ref_check" value="1">|;
        }
        print "<table border=0>";
        foreach my $row (@results) {
            my $o = Opinion->new($dbt,$row->{'opinion_no'});
            print "<tr>".
                  qq|<td><input type="radio" name="opinion_no" value="$row->{opinion_no}"></td>|.
                  "<td>".$o->formatAsHTML()."</td>".
                  "</tr>\n";
        }
        print qq|<tr><td><input type="radio" name="opinion_no" value="-1" checked></td><td>Create a <b>new</b> opinion record</td></tr>\n|;
        print qq|</table>|;
#        print qq|<tr><td align="center" colspan=2><p><input type=submit value="Submit"></p><br></td></tr>|;
        print qq|<p><input type="submit" value="Submit"></p><br>|;
        print "</div>";
    } elsif ($q->param("reference_no")) {
        my $sql = "SELECT o.opinion_no FROM opinions o ".
                  " LEFT JOIN authorities a ON a.taxon_no=o.child_no".
                  " WHERE o.reference_no=".int($q->param("reference_no")).
                  " ORDER BY a.taxon_name ASC";
        my @results = @{$dbt->getData($sql)};
        if (scalar(@results) == 0) {
            print "<div align=\"center\"<h3>No opinions found for this reference</h3></div><br><br>";
            return;
        }
        print "<div align=\"center\">";
        print "<h3>Select an opinion to edit:</h3>";

        print qq|<form method="POST" action="bridge.pl">
                 <input type="hidden" name="action" value="displayOpinionForm">\n|;
        print "<table border=0>";
        foreach my $row (@results) {
            my $o = Opinion->new($dbt, $row->{'opinion_no'});
            print "<tr>".
                  qq|<td><input type="radio" name="opinion_no" value="$row->{opinion_no}"></td>|.
                  "<td>".$o->formatAsHTML()."</td>".
                  "</tr>\n";
        }
        print "</table>";
#        print qq|<tr><td align="center" colspan=2><p><input type=submit value="Edit"></p><br></td></tr>|;
        print qq|<p><input type=submit value="Edit"></p><br>|;
        print "</div>";
    } else {
        print "<div align=\"center\">No terms were entered.</div>";
    }
    
    if ($q->param("taxon_no")) {
        print qq|<tr><td align="left" colspan=2><p><span class="tiny">An "opinion" is when an author classifies or synonymizes a taxon.<br>\nSelect an old opinion if it was entered incorrectly or incompletely.<br>\nCreate a new one if the author whose opinion you are looking at right now is not in the above list.</p>\n|;
    } elsif ($q->param('reference_no')) {
        print qq|<tr><td align="left" colspan=2><p><span class="tiny">An "opinion" is when an author classifies or synonymizes a taxon.<br>|;
        print qq|You may want to read the <a href="javascript:tipsPopup('/public/tips/taxonomy_tips.html')">tip sheet</a>.</span></p>\n|;
       # print "</span></p></td></tr>\n";
       # print "</table>\n";
        print "</td></tr></table>";
        print "</form>\n";
        print "</div>\n";
    }
}

1;


