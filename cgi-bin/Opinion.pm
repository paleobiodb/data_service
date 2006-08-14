# created by rjp, 3/2004.
# Represents information about a particular opinion

# Reworked PS 04/30/2005 - reworked accessor methods to make sense.  Also, return undef
# if the opinion isn't in the opinions table, since a Opinion object with a opinion_no is pointless 

package Opinion;
use strict;

use DBConnection;
use DBTransactionManager;
use CGI::Carp;
use Data::Dumper;
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
# This can't determine misspellings, which must be determined externally
sub guessSpellingReason {
    my $child = shift;
    my $spelling = shift;
    
    my $spelling_reason = "";
    
    if ($child->{'taxon_no'} == $spelling->{'taxon_no'}) {
        $spelling_reason ='original spelling';
    } else {
       
        # For a recombination, the upper names will always differ. If they're the same, its a correction
        if ($child->{'taxon_rank'} =~ /species|subgenus/) {
            my @childBits = split(/ /,$child->{'taxon_name'});
            my @spellingBits= split(/ /,$spelling->{'taxon_name'});
            pop @childBits;
            pop @spellingBits;
            my $childParent = join(' ',@childBits);
            my $spellingParent = join(' ',@spellingBits);
            if ($childParent eq $spellingParent) {
                # If the genus/subgenus/species names are the same, its a correction
                $spelling_reason = 'correction';
            } else {
                # If they differ, its a bad record or its a recombination
                if ($child->{'taxon_rank'} =~ /subgenus/) {
                    if ($child->{'taxon_rank'} ne $spelling->{'taxon_rank'}) {
                        $spelling_reason = 'rank change';
                    } else {
                        $spelling_reason = 'reassignment';
                    } 
                } else {
                    $spelling_reason = 'recombination';
                } 
            }
        } elsif ($child->{'taxon_rank'} ne $spelling->{'taxon_rank'}) {
            $spelling_reason = 'rank change';
        } else {
            $spelling_reason = 'correction';
        }
    }
    main::dbg("Get spelling status called, return $spelling_reason");
    return $spelling_reason;
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
    my $dbt = $self->{'dbt'};
	
    my $output = "";

    if ($row->{'status'} =~ /homonym/) {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}},['taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
        my $child_html  = ($child->{'taxon_rank'} =~ /species|genus/) 
                        ? "<i>$child->{'taxon_name'}</i>" 
                        : $child->{'taxon_name'};
        my $child_pub_info = Reference::formatShortRef($child);
        $output .= "'$child_html, <small>$child_pub_info</small> is a $row->{status}";
        my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}},['taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
        if ($parent) {
            my $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                            ? "<i>$parent->{'taxon_name'}</i>" 
                            : $parent->{'taxon_name'};
            my $parent_pub_info = Reference::formatShortRef($parent);
            $output .= " $parent_html, <small>$parent_pub_info</small>";
        }
        $output .= "'";
    } elsif ($row->{'status'} =~ /synonym|homonym|replace|nomen|revalidated|misspell|subgroup/) {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
        my $child_html  = ($child->{'taxon_rank'} =~ /species|genus/) 
                        ? "<i>$child->{'taxon_name'}</i>" 
                        : $child->{'taxon_name'};
        if ($row->{'status'} =~ /^[aeiou]/) {
            $output .= "'$child_html is an $row->{status}";
        } else {
            $output .= "'$child_html is a $row->{status}";
        }
        if ($row->{'status'} =~ /synonym|homonym|replace|misspell|subgroup/) {
            my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            if ($parent) {
                my $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                                ? "<i>$parent->{'taxon_name'}</i>" 
                                : $parent->{'taxon_name'};
                $output .= " $parent_html";
            }
        }
        $output .= "'";
    } else {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_no'}});
        my $child_html  = ($child->{'taxon_rank'} =~ /species|genus/) ? "<i>$child->{'taxon_name'}</i>" : $child->{'taxon_name'};

        if ($row->{'spelling_reason'} =~ /correction|misspelling/) {
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
            my $spelling_html = ($spelling->{'taxon_rank'} =~ /species|genus/) 
                              ? "<i>$spelling->{'taxon_name'}</i>" 
                              : $spelling->{'taxon_name'};
            my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            my $parent_html = "";
            if ($parent) {
                $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                             ? "<i>$parent->{'taxon_name'}</i>" 
                             : $parent->{'taxon_name'};
            }
            if ($row->{'spelling_reason'} eq 'misspelling') {
		        $output .= "'$child_html [misspelled as $spelling_html] belongs to $parent_html'";
            } else {
		        $output .= "'$child_html is corrected as $spelling_html and belongs to $parent_html'";
            }
        } elsif ($row->{'spelling_reason'} =~ /rank change/) {
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
            my $spelling_html = ($spelling->{'taxon_rank'} =~ /species|genus/) 
                              ? "<i>$spelling->{'taxon_name'}</i>" 
                              : $spelling->{'taxon_name'};
            my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            my $parent_html = "";
            if ($parent) {
                $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                             ? "<i>$parent->{'taxon_name'}</i>" 
                             : $parent->{'taxon_name'};
            }
            if ($spelling_html ne $child_html) {
                if ($child->{'taxon_rank'} =~ /genus/) {
		            $output .= "'$child_html was reranked as $spelling_html ";
                } else {
		            $output .= "'$child_html was reranked as the $spelling->{taxon_rank} $spelling_html ";
                }
            } else {
		        $output .= "'$child_html was reranked as a $spelling->{taxon_rank} ";
            }
            $output .= " and belongs to $parent_html'";
        } elsif ($row->{'spelling_reason'} =~ /reassignment/) {
            my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            my $parent_html = "";
            if ($parent) {
                $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                             ? "<i>$parent->{'taxon_name'}</i>" 
                             : $parent->{'taxon_name'};
            }
		    $output .= "'$child_html is reassigned into $parent_html'";
        } elsif ($row->{'spelling_reason'} =~ /recombination/) {
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
            my $spelling_html = ($spelling->{'taxon_rank'} =~ /species|genus/) 
                              ? "<i>$spelling->{'taxon_name'}</i>" 
                              : $spelling->{'taxon_name'};
		    $output .= "'$child_html is recombined as $spelling_html'";
        } else {
            my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            my $parent_html = "";
            if ($parent) {
                $parent_html = ($parent->{'taxon_rank'} =~ /species|genus/) 
                             ? "<i>$parent->{'taxon_name'}</i>"
                             : $parent->{'taxon_name'};
            }
		    $output .= "'$child_html belongs to $parent_html'";
        } 
    }
    $output .= " according to ".$self->authors();
    return $output;
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
    # Don't do anymore, anyone can edit
	#my @nonEditables; 	
	
	if ((!$dbt) || (!$hbo) || (! $s) || (! $q)) {
		croak("Opinion::displayOpinionForm had invalid arguments passed to it.");
		return;
	}

    # Simple variable assignments
    my $isNewEntry = ($q->param('opinion_no') > 0) ? 0 : 1;
    my $reSubmission = ($error_message) ? 1 : 0;
	my @belongsArray = ('belongs to', 'recombined as', 'revalidated', 'rank changed as','corrected as');
	my @synArray = ('','subjective synonym of', 'objective synonym of','homonym of','replaced by','misspelling of','invalid subgroup of');
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
            my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_no'}});
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_spelling_no'}});
            my $reason = guessSpellingReason($child,$spelling);
            $fields{'spelling_reason'} = $reason;
        } else {
            %fields = %{$o->getRow()};

            if ($fields{'ref_has_opinion'} =~ /YES/i) {
                $fields{'ref_has_opinion'} = 'PRIMARY';
            } else {
                $fields{'ref_has_opinion'} = 'NO';
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
            if ($fields{'status'} eq 'misspelling of') {
                $fields{'taxon_status'} = 'misspelling of';
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
    my $childSpellingRank = "";
    if ($fields{'child_spelling_no'} > 0) {
        # This will happen on an edit (first submission) OR resubmission w/homonyms
        # SQL trick: get not only the authoritiy data for child_spelling_no, but all its homonyms as well
        my $sql = "SELECT a2.* FROM authorities a1, authorities a2 WHERE a1.taxon_name=a2.taxon_name AND a1.taxon_no=".$dbh->quote($fields{'child_spelling_no'});
        my @results= @{$dbt->getData($sql)}; 
        foreach my $row (@results) {
            push @child_spelling_nos, $row->{'taxon_no'};
            $childSpellingName = $row->{'taxon_name'};
            $childSpellingRank = $row->{'taxon_rank'};
        }
    } else {
        $childSpellingName = $q->param('child_spelling_name') || $childName;
        $childSpellingRank = $q->param('child_spelling_rank') || $childRank;
        @child_spelling_nos = TaxonInfo::getTaxonNos($dbt,$childSpellingName,$childSpellingRank);
    }
    # If the childSpellingName and childName are the same (and possibly amiguous)
    # Use the child_no as the spelling_no so we unneccesarily don't get a radio select to select
    # among the different homonyms
    if ($childSpellingName eq $childName && $childSpellingRank eq $childRank) {
        @child_spelling_nos = ($fields{'child_no'});
        $fields{'child_spelling_no'} = $fields{'child_no'};
    } elsif (@child_spelling_nos ==  1) {
        $fields{'child_spelling_no'} = $child_spelling_nos[0];
    }   
    
    $fields{'child_name'} = $childName;
    $fields{'child_rank'} = $childRank;
    $fields{'child_spelling_name'} = $childSpellingName;
    $fields{'child_spelling_rank'} = $childSpellingRank;

    $fields{'taxon_display_name'} = $childSpellingName;
    if ($fields{'taxon_status'} eq 'invalid1' && $fields{'synonym'} eq 'homonym of') {
        my $authority = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_no'}},['author1last','author2last','otherauthors','pubyr']);
        my $pub_info = Reference::formatShortRef($authority);
        if ($pub_info !~ /^\s*$/) {
            $fields{'taxon_display_name'} .= ", $pub_info";
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

    if (@parent_nos == 1) {
        $fields{'parent_spelling_no'} = $parent_nos[0];
    }


    my @opinions_to_migrate2;
    if ($fields{'taxon_status'} eq 'invalid1' && $fields{'synonym'} eq 'misspelling of') {
        if (scalar(@parent_nos) == 1) {
            @opinions_to_migrate2 = getOpinionsToMigrate($dbt,$fields{'child_no'},$parent_nos[0],$fields{'opinion_no'});
        }
    } 

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $parent_pulldown;
	if ( scalar(@parent_nos) > 1 || (scalar(@parent_nos) == 1 && @opinions_to_migrate2 && $fields{'taxon_status'} eq 'invalid1' && $fields{'synonym'} eq 'misspelling of')) {
        foreach my $parent_no (@parent_nos) {
	        my $parent = TaxaCache::getParent($dbt,$parent_no);
            my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$parent_no},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
            my $pub_info = Reference::formatShortRef($taxon);

            my $selected = ($fields{'parent_spelling_no'} == $parent_no) ? "CHECKED" : "";
            $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
            my $higher_class;
            if ($parent) {
                $higher_class = $parent->{'taxon_name'}
            } else {
                my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$parent_no});
                $higher_class = "unclassified $taxon->{taxon_rank}";
            }
			$parent_pulldown .= qq|<input type="radio" name="parent_spelling_no" $selected value='$parent_no'> $parentName, $taxon->{taxon_rank}$pub_info [$higher_class]<br>\n|;
        }
        $parent_pulldown .= qq|<input type="radio" name="parent_spelling_no" value=""> \nOther taxon: <input type="text" name="belongs_to_parent" value=""><br>\n|;
	}

    main::dbg("parentName $parentName parents: ".scalar(@parent_nos));
    main::dbg("childSpellingName $childSpellingName spellings: ".scalar(@child_spelling_nos));

    if (!$isNewEntry) {
        if ($o->get('authorizer_no')) {
            $fields{'authorizer_name'} = "<B>Authorizer:</B> " . Person::getPersonName($dbt,$o->get('authorizer_no')); 
        }   
        if ($o->get('enterer_no')) { 
            $fields{'enterer_name'} = " <B>Enterer:</B> " . Person::getPersonName($dbt,$o->get('enterer_no')); 
        }
        if ($o->get('modifier_no')) { 
            $fields{'modifier_name'} = " <B>Modifier:</B> ".Person::getPersonName($dbt,$o->get('modifier_no'));
        }
        $fields{'modified'} = "<B>Modified: </B>".$fields{'modified'};
        $fields{'created'} = "<B>Created: </B>".$fields{'created'}; 
    }

    if ($fields{'reference_no'}) {
        my $ref = Reference->new($dbt,$fields{'reference_no'}); 
        $fields{formatted_primary_reference} = $ref->formatAsHTML() if ($ref);
    }

    if ($s->get('reference_no') && $s->get('reference_no') != $fields{'reference_no'}) {
        if ($s->get('reference_no')) {
            my $ref = Reference->new($dbt,$s->get('reference_no'));
            $fields{formatted_current_reference} = $ref->formatAsHTML() if ($ref);
        }
        $fields{'current_reference'} = 'yes';
    } 
	
	# Anyone can now edit anything. Restore from CVS if we have to backpedal PS 5/17/2006

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
        $belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid</b>, classified as belonging to |;
    } else {
        $belongs_to_row .= qq|<td $colspan valign="top" nowrap><b>Valid or reranked $childRank</b>, classified as belonging to $higher_rank |;
    }

    if ($parent_pulldown && $selected) {
        $belongs_to_row .= "<td width='100%'>$parent_pulldown</td>";
    } else	{
        my $parentTaxon = ($selected || ($isNewEntry && $childRank =~ /species/)) ? $parentName : "";
        $belongs_to_row .= qq|<input name="belongs_to_parent" size="24" value="$parentTaxon">|;
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
	$synonym_row .= $hbo->htmlSelect('synonym',\@synArray, \@synArray, $fields{'synonym'});
	if ($parent_pulldown && $selected) {
        $parent_pulldown =~ s/belongs_to_parent/synonym_parent/;
		$synonym_row .= "<td width='100%'>$parent_pulldown</td>"; 
	} else	{
        my $parentTaxon = ($selected) ? $parentName : "";
		$synonym_row .= qq|<input name="synonym_parent" size="30" value="$parentTaxon">|;
	}
    $synonym_row .= "</td></tr>";

	# actually build the nomen popup menu.
    $selected = ($fields{'taxon_status'} eq 'invalid2') ? "CHECKED" : "";
    $nomen_row = qq|<tr><td valign="top"><input type="radio" name="taxon_status" value="invalid2" $selected></td>|;
	$nomen_row .= "<td colspan=2><b>Invalid</b>, and no other name can be used.</td></tr>";
    $nomen_row .= "<tr><td></td><td colspan=2>Status: "; 
    $nomen_row .= $hbo->htmlSelect('nomen',\@nomArray, \@nomArray, $fields{'nomen'});
    $nomen_row .= "</td></tr>";

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
    # build the spelling pulldown, if necessary, else the spelling box

    my $all_ranks = "";
    if ($childSpellingRank eq 'subspecies') {
        $all_ranks = ': e.g. "Genus (Subgenus) species subspecies"';
    } elsif ($childSpellingRank eq 'species') {
        $all_ranks = ': e.g. "Genus (Subgenus) species"';
    }  elsif ($childSpellingRank eq 'subgenus') {
        $all_ranks = ': e.g. "Genus (Subgenus)"';
    }

    my @ranks = ();
    if ($childRank =~ /subspecies|species/) {
        @ranks = ('subspecies','species');
    } elsif ($childRank =~ /subgenus|genus/) {
        @ranks = ('subgenus','genus');
    } else {
        @ranks = grep {!/subspecies|species|subgenus|genus/} $hbo->getList('taxon_rank');
    } 

    my @opinions_to_migrate1;
    if (scalar(@child_spelling_nos) == 1) {
        @opinions_to_migrate1 = getOpinionsToMigrate($dbt,$fields{'child_no'},$child_spelling_nos[0],$fields{'opinion_no'});
    }

    my $spelling_note = "";
#    my $spelling_note = "<small>Note that the name may be different than listed above due to a correction, recombination, or rank change.</small>";
    $spelling_row .= "<tr><td colspan=2>Please enter the full name and rank of the taxon as used in the reference${all_ranks}:</td></tr>";

    my $spelling_rank_pulldown = $hbo->htmlSelect('child_spelling_rank',\@ranks, \@ranks, $fields{'child_spelling_rank'});
	if (scalar(@child_spelling_nos) > 1 || (scalar(@child_spelling_nos) == 1 && @opinions_to_migrate1)) {
        $spelling_row .= "<tr><td nowrap width=\"100%\">";
        foreach my $child_spelling_no (@child_spelling_nos) {
	        my $parent = TaxaCache::getParent($dbt,$child_spelling_no);
            my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$child_spelling_no},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
            my $pub_info = Reference::formatShortRef($taxon);
            my $selected = ($fields{'child_spelling_no'} == $child_spelling_no) ? "CHECKED" : "";
            $pub_info = ", ".$pub_info if ($pub_info !~ /^\s*$/);
            my $orig_no = TaxonInfo::getOriginalCombination($dbt,$child_spelling_no);
            my $orig_info = "";
            if ($orig_no != $child_spelling_no) {
                my $orig = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$orig_no});
                $orig_info = ", originally $orig->{taxon_name}";
            }
			$spelling_row .= qq|<input type="radio" name="child_spelling_no" $selected value='$child_spelling_no'> ${childSpellingName}, $taxon->{taxon_rank}${pub_info}${orig_info} <br>\n|;
        }
        $spelling_row .= qq|<input type="radio" name="child_spelling_no" value=""> \nOther taxon: <input type="text" name="child_spelling_name" value="">$spelling_rank_pulldown<br>\n|;
	    $spelling_row .= qq|<input type="hidden" name="new_child_spelling_name" value="$childSpellingName">|;
        my $new_child_spelling_rank_pulldown = $hbo->htmlSelect('new_child_spelling_rank',\@ranks, \@ranks, $fields{'child_spelling_rank'});
	    $spelling_row .= qq|<input type="radio" name="child_spelling_no" value="-1"> Create a new '$childSpellingName' based off '$childName' with rank $new_child_spelling_rank_pulldown<br>|;
        $spelling_row .= "$spelling_note</td></tr>";
	} else {
        $spelling_row .= qq|<tr><td nowrap width="100%"><input id="child_spelling_name" name="child_spelling_name" size=30 value="$childSpellingName">$spelling_rank_pulldown<br>$spelling_note</td></tr>|;
    }


    my @select_values = ();
    my @select_keys = ();
    if ($childRank =~ /subgenus/) {
        my ($genusName,$subGenusName) = Taxon::splitTaxon($childName);
        @select_values = ('original spelling','correction','misspelling','rank change','reassignment');
        @select_keys = ("is the original spelling and rank of this taxon", "is a correction of '$childName'","is a misspelling of this taxon","has had its rank changed from its original rank of $childRank","has been reassigned from its original genus '$genusName'");
    } elsif ($childRank =~ /species/) {
        @select_values = ('original spelling','recombination','correction','misspelling');
        @select_keys = ("is the original spelling and rank of this taxon","is a recombination or rank change of '$childName'","is a correction of '$childName'","is a misspelling of this taxon");
    } else {
        @select_values = ('original spelling','correction','misspelling','rank change');
        @select_keys = ("is the original spelling and rank of this taxon","is a correction of '$childName'","is a misspelling of this taxon","has had its rank changed from its original rank of $childRank");
    }
    $spelling_row .= "<tr><td>&nbsp;</td></tr>";
    $spelling_row .= "<tr><td>Enter the reason why this spelling and rank was used:<br>This ". $hbo->htmlSelect('spelling_reason',\@select_keys,\@select_values,$fields{'spelling_reason'})."</td></tr>";

    main::dbg("showOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");

	$fields{belongs_to_row} = $belongs_to_row;
	$fields{nomen_row} = $nomen_row;
	$fields{synonym_row} = $synonym_row;
	$fields{spelling_row} = $spelling_row;

	# print the form	
    $fields{'error_message'} = $error_message;

	my $html = $hbo->populateHTML("add_enter_opinion", \%fields);

	print $html;
}


# Call this when you want to submit an opinion form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
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
	if (($q->param('ref_has_opinion') ne 'PRIMARY') && 
	    ($q->param('ref_has_opinion') ne 'CURRENT') && 
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
        if ($isNewEntry && $q->param('taxon_status') ne 'misspelling of') {
		    my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion !='YES' ".
                      " AND child_no=".$dbh->quote($fields{'child_no'}).
                      " AND author1last=".$dbh->quote($q->param('author1last')).
                      " AND author2last=".$dbh->quote($q->param('author2last')).
                      " AND pubyr=".$dbh->quote($q->param('pubyr')).
                      " AND status NOT IN ('misspelling of','homonym of')";
            my $row = ${$dbt->getData($sql)}[0];
            if ( $row->{'c'} > 0 ) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		# merge the pages and primary_pages, figures and current_figures fields
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

        my $lookup_reference = "";
        if ($q->param('ref_has_opinion') eq 'CURRENT') {
            $lookup_reference = $s->get('reference_no');
        } else {
            $lookup_reference = $fields{'reference_no'};
        }  

		if ($q->param('pubyr')) {
            my $pubyr = $q->param('pubyr');
			
			if (! Validation::properYear($pubyr)) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new($dbt,$lookup_reference);
			if ($ref && $pubyr > $ref->get('pubyr')) {
				$errors->add("The publication year ($pubyr) can't be more recent than that of the primary reference (" . $ref->get('pubyr') . ")");
			}
		} else {
            $errors->add("A publication year is required");
        }
	} else {
        my $lookup_reference = "";
        if ($q->param('ref_has_opinion') eq 'CURRENT') {
            $lookup_reference = $s->get('reference_no');
        } else {
            $lookup_reference = $fields{'reference_no'};
        }
		# if they chose ref_has_opinion, then we also need to make sure that there
		# are no other opinions about the current taxon (child_no) which use 
		# this as the reference.  
        my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion='YES'".
                  " AND child_no=".$dbh->quote($fields{'child_no'}).
                  " AND reference_no=".$dbh->quote($lookup_reference).
                  " AND status NOT IN ('misspelling of','homonym of')";
        if (! $isNewEntry) {
            $sql .= " AND opinion_no != ".$o->{'opinion_no'};
        }
        my $row = ${$dbt->getData($sql)}[0];
        if ($row->{'c'} > 0) {
            unless ($q->param('taxon_status') eq 'invalid1' && $q->param("synonym") eq 'misspelling of') {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }
		# ref_has_opinion is PRIMARY or CURRENT
		# so make sure the other publication info is empty.
		
		if ($q->param('author1init') || $q->param('author1last') || $q->param('author2init') || $q->param('author2last') || $q->param('otherauthors') || $q->param('pubyr') || $q->param('2nd_pages') || $q->param('2nd_figures')) {
			$errors->add("Don't enter any other publication information if you chose the 'primary reference argues ...' or 'current reference argues ...' radio button");	
		}
        
		# also make sure that the pubyr of this opinion isn't older than
		# the pubyr of the authority record the opinion is about
		my $ref = Reference->new($dbt, $lookup_reference);
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
                my @parents = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$parentName}); 
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
    my $createSpelling = 0;
    if ($q->param('child_spelling_no')) {
        if ($q->param('child_spelling_no') == -1) {
            $createSpelling = 1;
            $childSpellingName = $q->param('new_child_spelling_name');
            $childSpellingRank = $q->param('new_child_spelling_rank');
        } else {
            # This is a second pass through, b/c there was a homonym issue, the user
            # was presented with a pulldown to distinguish between homonyms, and has submitted the form
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$q->param('child_spelling_no')});
            $childSpellingName = $spelling->{'taxon_name'};
            $childSpellingRank = $spelling->{'taxon_rank'};
            $fields{'child_spelling_no'} = $q->param('child_spelling_no');
        }
    } else {
        if ($childName eq $q->param('child_spelling_name') && $childRank eq $q->param('child_spelling_rank')) {
            # This is the simplest case - if the childName and childSpellingName are the same, they get
            # the same taxon_no - don't present a pulldown in this case, even if the name is a homonym,
            # since we already resolved the homonym issue before we even got to this form
            $childSpellingName = $childName;
            $childSpellingRank = $childRank;
            $fields{'child_spelling_no'} = $fields{'child_no'};
        } else {
            # Otherwise, go through the whole big routine
            $childSpellingName = $q->param('child_spelling_name');
            $childSpellingRank = $q->param('child_spelling_rank');
            my @spellings = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$childSpellingName,'taxon_rank'=>$childSpellingRank}); 
            if (scalar(@spellings) > 1) {
                $errors->add("The spelling '$childSpellingName' exists multiple times in the database. Please select the one you want");	
            } elsif (scalar(@spellings) == 0) {
                if ($q->param('confirm_create_spelling') eq $q->param('child_spelling_name') && 
                    $q->param('confirm_create_rank') eq $q->param('child_spelling_rank')) {
                    $createSpelling = 1;
                } else {
                    $q->param('confirm_create_spelling'=>$q->param('child_spelling_name'));
                    $q->param('confirm_create_rank'=>$q->param('child_spelling_rank'));
                    $errors->add("The ".$q->param('child_spelling_rank')." '$childSpellingName' doesn't exist in our database.  If this isn't a typo, hit submit again to automatically create a new database record for it");	
                }
            } elsif (scalar(@spellings) == 1) {
                $fields{'child_spelling_no'} = $spellings[0]->{'taxon_no'};
            }
        }
    }

    # This is a bit tricky, we change the childName and childNo midstream for lapsus calami type records
    # Get the child name and rank
    if ($q->param("taxon_status") eq 'invalid1' && $q->param('synonym') eq 'misspelling of' && $fields{'parent_spelling_no'}) {
        my $new_orig = TaxonInfo::getOriginalCombination($dbt,$fields{'parent_spelling_no'});
        $childTaxon = Taxon->new($dbt,$new_orig);
        $childName = $childTaxon->get('taxon_name');
        $childRank = $childTaxon->get('taxon_rank');
        $fields{'child_no'} = $new_orig;
    } 

    main::dbg("child_no $fields{child_no} childRank $childRank childName $childName ");
    main::dbg("child_spelling_no $fields{child_spelling_no} childSpellingRank $childSpellingRank childSpellingName $childSpellingName");
    main::dbg("parent_no $fields{parent_no}  parent_spelling_no $fields{parent_spelling_no} parentSpellingRank $parentRank parentSpellingName $parentName");
    

    ############################
    # Validate the form, bottom section
    ############################
    # Flatten the status
    if ($q->param('taxon_status') eq 'belongs to') {
	    $fields{'status'} = 'belongs to';
    } elsif ($q->param('taxon_status') eq 'invalid1') {
        $fields{'status'} = $q->param('synonym');
    } elsif ($q->param('taxon_status') eq 'invalid2') {
        $fields{'status'} = $q->param('nomen');
    }
	$fields{'spelling_reason'} = $q->param('spelling_reason');

	if (!$q->param("spelling_reason")) {
		$errors->add("No option was selected in the \"Enter the reason why this spelling was used\" pulldown");
    } 
    
	if (!$q->param("taxon_status")) {
		$errors->add("You must choose one of radio buttons in the \"How was it classified?\" section");	
	} 
    
    if ($q->param('taxon_status') ne 'belongs to' && $q->param('type_taxon')) {
        $errors->add("The valid taxon radio button in the \"How was it classified?\" section must be selected if the is the type taxon");
    } 

    if ($q->param('spelling_reason') =~ /rank change/) {
        if ($childRank =~ /species/) {
            $errors->add("Don't select rank change for species, mark it as a recombination.");
        }
    }

    my @opinions_to_migrate1;
    my @opinions_to_migrate2;
    if ($fields{'status'} eq 'misspelling of') {
        if ($fields{'parent_spelling_no'}) {
            @opinions_to_migrate2 = getOpinionsToMigrate($dbt,$fields{'parent_no'},$fields{'child_no'},$fields{'opinion_no'});
        }
    }
    if ($fields{'child_spelling_no'}) {
        @opinions_to_migrate1 = getOpinionsToMigrate($dbt,$fields{'child_no'},$fields{'child_spelling_no'},$fields{'opinion_no'});
    }
    if ((@opinions_to_migrate1 || @opinions_to_migrate2) && $q->param("confirm_migrate_opinions") !~ /YES/i) {
        main::dbg("MIGRATING:<PRE>".Dumper(\@opinions_to_migrate1)."</PRE><PRE>".Dumper(\@opinions_to_migrate2)."</PRE>"); 
        my $msg = "";
        if (@opinions_to_migrate1) {
            $msg .= "The taxon <b>$childSpellingName</b> already exists with <a href=\"bridge.pl?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}\" target=\"_BLANK\"> opinions classifying it</a>."; 
        }
        if (@opinions_to_migrate2) {
            $msg .= "The taxon <b>$childSpellingName</b> already exists with <a href=\"bridge.pl?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}\" target=\"_BLANK\"> opinions classifying it</a>."; 
        }
        $msg .= " If you hit submit again, this taxonomic name will be permanently combined with the existing taxon.  This means: <ul>";
        $msg .= " <li> '$childName' will be considered the 'original' name.  If another spelling is actually the original usage and '$childName' is actually a subsequent usage, please enter opinions based on that other taxon.</li>";
        $msg .= " <li> Authority information will be made identical and linked.  Changes to reference or authority for this name will automatically be copied over to the names above and alternate spellings/combinations.</li>";
        $msg .= " <li> These names will be considered the same taxon and thus be inseparably linked when editing/adding opinions, for classification during downloads or collections searchs, etc.</li>";
        $msg .= "</ul>";
        if ($fields{'status'} ne 'misspelling of') {
            $msg .= " If '$childName' is actually a misspelling of '$childSpellingName', please enter 'Invalid, this taxon is a misspelling of $childSpellingName' in the \"How was it classified\" section, and enter '$childName' in the \"How was it spelled section\"<br>";
        }
        if (@opinions_to_migrate1) {
            $msg .= " If '$childSpellingName' is actually a homonym (separate taxon), please select 'Create a new '$childSpellingName' in the 'How was it spelled?' section below";
        }
        $errors->add($msg);
        $q->param('confirm_migrate_opinions'=>'YES');
    }

    # Error checking related to ranks
    # Only bother if we're down to one parent
    if ($fields{'parent_spelling_no'}) {
	    if ($q->param('taxon_status') eq 'belongs to' || ($q->param('taxon_status') eq 'invalid1' && $q->param('synonym') eq 'invalid subgroup of')) {
		    # for belongs to, the parent rank should always be higher than the child rank.
		    # unless either taxon is an unranked clade (JA)
		    if ($rankToNum{$parentRank} <= $rankToNum{$childSpellingRank} && 
                $parentRank ne "unranked clade" && 
                $childSpellingRank ne "unranked clade") {
		    	$errors->add("The rank of the higher taxon '$parentName' ($parentRank) must be higher than the rank of '$childSpellingName' ($childSpellingRank)");	
		    }
        } elsif ($q->param('taxon_status') eq 'invalid1') {
    		# the parent rank should be the same as the child rank...
            if ( $parentRank ne $childSpellingRank) {
                $errors->add("The rank of a taxon and the rank of its synonym, homonym, or replacement name must be the same");
            }    
		} 
    }

    # Some more rank checks
    if ($q->param('spelling_reason') =~ 'rank change') {
        # JA: ... except if the status is "rank changed as," which is actually the opposite case
        if ( $childSpellingRank eq $childRank) {
            $errors->add("If you change a taxon's rank, its old and new ranks must be different");
        }
    } else {
        if ($childSpellingRank ne $childRank && $q->param('spelling_reason') !~ /recombination|misspelling/) {
            $errors->add("Unless a taxon has its rank changed or is recombined, the rank entered in he \"How was it spelled?\" section must match the original taxon's rank. If the rank has changed, select \"rank change\" even if the spelling remains the same");
        }
    }    

    # error checks related to naming
    # If something is marked as a corrected/recombination/rank change, its spellingName should be differenct from its childName
    # and the opposite is true its an original spelling (they're the same);
    # If we're marking a misspelling
    if ($fields{'status'} eq 'misspelling of') {
        if ($q->param('spelling_reason') ne 'misspelling') {
            $errors->add("Select \"This is a misspelling\" in the \"How was it spelled section\" when entering a misspelling");
        }
    } else {
        if ($q->param('spelling_reason') =~ /original spelling/) {
            if ($childSpellingName ne $childName || $childSpellingRank ne $childRank) {
                $errors->add("If \"This is the original spelling and rank of this taxon\" is selected, you must enter '$childName', '$childRank' in the \"How was it spelled?\" section");
            }
        } else {
            if ($childSpellingName eq $childName && $childSpellingRank eq $childRank) {
                $errors->add('If you leave the name and rank unchanged, please select "This is the original spelling and rank" in the "How was it spelled" section');
            }
        }
    }
        
    # the genus name should differ for recombinations, but be the same for everything else
    if ($childRank =~ /species|subgenus/ && $q->param('spelling_reason') ne 'misspelling') {
        my @childBits = split(/ /,$childName);
        pop @childBits;
        my $childParent = join(' ',@childBits);
        my @spellingBits = split(/ /,$childSpellingName);
        pop @spellingBits;
        my $spellingParent = join(' ',@spellingBits);
        if ($childRank =~ /species/) {
            if ($q->param('spelling_reason') eq 'recombination') {
                if ($spellingParent eq $childParent) {
                    $errors->add("The genus or subgenus in the new combination must be different from the genus or subgenus in the original combination");
                }
            } else {
                if ($spellingParent ne $childParent) {
                    $errors->add("The genus and subgenus of the spelling must be the same as the original genus and subgenus when choosing \"This name is a correction\" or \"This is the original spelling and rank\"");
                }
            }
        } else { # Subgenus
            if ($q->param('spelling_reason') eq 'reassignment') {
                if ($spellingParent eq $childParent) {
                    $errors->add("The genus must be changed if \"This subgenus has been reassigned\" is selected in the \"How was it spelled\" section");
                }
            } else {
                if ($spellingParent ne $childParent && $q->param('spelling_reason') ne 'rank change') {
                    $errors->add("If the genus is changed, selected \"This subgenus has been reassigned\" in the \"How was it spelled\" section");
                }
            }
        }
        if ($q->param('spelling_reason') eq 'original spelling' && $spellingParent ne $childParent) {
            $errors->add("The genus and subgenus in the \"How was it classified?\" and the \"How was it spelled?\" sections must be the same is this is marked as the original spelling");
        }
    } else {
        if ($q->param('spelling_reason') eq 'reassignment') {
            $errors->add("Don't mark this as a reassignment if the taxon isn't a subgenus");
        }
        if ($q->param('spelling_reason') eq 'recombination') {
            $errors->add("Don't mark this as a recombination if the taxon isn't a species or subspecies");
        }
    }


    # Misc error checking 
    if ($fields{'status'} eq 'misspelling of') {
        if ($parentName eq $childSpellingName) {
            $errors->add("The names entered in the \"How was it spelled\" and \"How was it classified\" sections must be different when noting a misspelling");
        }
    } elsif ($fields{'status'} eq 'homonym of') {
        if ($parentName ne $childSpellingName) {
            $errors->add("If you select 'homonym of', the taxon and its homonym must be spelled the same");
        }
        if ($fields{'child_spelling_no'} == $fields{'parent_spelling_no'} ||
            $fields{'child_no'} == $fields{'parent_no'} ||
            $fields{'child_spelling_no'} == $fields{'parent_no'} ||
            $fields{'child_no'} == $fields{'parent_spelling_no'}) {
            $errors->add("The taxon you are entering and the one it belongs to can't be the same");	
        }
    } else {
        if ($parentName eq $childName || $parentName eq $childSpellingName) {
            $errors->add("The taxon you are entering and the one it belongs to can't have the same name");	
        } 
        if ($fields{'child_no'} == $fields{'parent_no'}) {
            $errors->add("The taxon you are entering and the one it belongs to can't be the same");	
        }
    }

    my $rankFromSpaces = Taxon::guessTaxonRank($childSpellingName);
    if (($rankFromSpaces eq 'subspecies' && $childSpellingRank ne 'subspecies') ||
        ($rankFromSpaces eq 'species' && $childSpellingRank ne 'species') ||
        ($rankFromSpaces eq 'subgenus' && $childSpellingRank ne 'subgenus') ||
        ($rankFromSpaces eq ''     && $childSpellingRank =~ /subspecies|species|subgenus/)) {
        $errors->add("The selected rank '$childSpellingRank' doesn't match the spacing of the taxon name '$childSpellingName'");
    }
	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( $q->param('taxon_status') !~ /belongs to/ && $q->param('spelling_reason') !~ /original spelling|recombination/ && $q->param('diagnosis')) {
		$errors->add("Don't enter a diagnosis unless this taxon is valid or a recombination");
	}

    # Get the fields from the form and get them ready for insertion
    # All other fields should have been set or thrown an error message at some previous time
    foreach my $f ('author1init','author1last','author2init','author2last','otherauthors','pubyr','pages','figures','comments','diagnosis','phylogenetic_status','type_taxon') {
        if (!$fields{$f}) {
            $fields{$f} = $q->param($f);
        }
    }
	
	# correct the ref_has_opinion field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($q->param('ref_has_opinion') =~ /PRIMARY|CURRENT/) {
		$fields{'ref_has_opinion'} = 'YES';
	} elsif ($q->param('ref_has_opinion') eq 'NO') {
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

	
    # Replace the reference with the current reference if need be
    if ($q->param('ref_has_opinion') =~ /CURRENT/ && $s->get('reference_no')) {
        $fields{'reference_no'} = $s->get('reference_no');
    }
	
	# now we'll actually insert or update into the database.

	# first step is to create the parent taxon if a species is being
	#  recombined and the new combination doesn't exist JA 14.4.04
	# WARNING: this is very dangerous; typos in parent names will
	# create bogus combinations, and likewise if the opinion create/update
	#  code below bombs
	if ($createSpelling) {
	    # next we need to steal data from the opinion
        my %record = (
            'reference_no' => $fields{'reference_no'},
            'taxon_rank' => $childSpellingRank,
            'taxon_name' => $childSpellingName
        );

        # author information comes from the original combination,
        my $orig = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_no'}},['*']);

		my @dataFields = ("pages", "figures", "extant", "preservation");
		my @origAuthFields = ("author1init", "author1last","author2init", "author2last","otherauthors", "pubyr" );
        
        if ($orig->{'ref_is_authority'} =~ /yes/i) {
            $record{'reference_no'}=$orig->{'reference_no'};
            foreach my $f (@dataFields) {
                $record{$f} = $orig->{$f};
            }
            foreach my $f (@origAuthFields) {
                $record{$f} = "";
            }
            $record{'ref_is_authority'}='YES';
        } else {
            foreach my $f (@dataFields,@origAuthFields) {
                $record{$f} = $orig->{$f};
            }
        }

        my ($return_code, $taxon_no) = $dbt->insertRecord($s,'authorities', \%record);
        TaxaCache::addName($dbt,$taxon_no);
        main::dbg("create new authority record, got return code $return_code");
        if (!$return_code) {
            die("Unable to create new authority record for $record{taxon_name}. Please contact support");
        }
        $fields{'child_spelling_no'} = $taxon_no;
        my @set_warnings = Taxon::setOccurrencesTaxonNoByTaxon($dbt,$s,$taxon_no);
        push @warnings, @set_warnings;
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

	} else {
		# if it's an old entry, then we'll update.
        unless ($q->param('ref_has_opinion') =~ /CURRENT/) {
            # Delete this field so its never updated unless we're swithing to current ref
            delete $fields{'reference_no'};
        }

		$resultOpinionNumber = $o->get('opinion_no');
		$dbt->updateRecord($s,'opinions', 'opinion_no',$resultOpinionNumber, \%fields);

	}
    
    if (@opinions_to_migrate1 || @opinions_to_migrate2) {
        main::dbg("Migrating ".(scalar(@opinions_to_migrate1)+scalar(@opinions_to_migrate2))." opinions");
        my @parents = ();
        foreach my $row  (@opinions_to_migrate1,@opinions_to_migrate2) {
            my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_no'}});
            my $spelling;
            if ($row->{'status'} eq 'misspelling of') {
                $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'parent_spelling_no'}});
            } else {
                $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
            }
            my $is_misspelling = TaxonInfo::isMisspelling($dbt,$row->{'child_spelling_no'});
            $is_misspelling = 1 if ($row->{'spelling_reason'} eq 'misspelling');
            my $newSpellingReason;
            if ($is_misspelling) {
                $newSpellingReason = 'misspelling';
            } else {
                $newSpellingReason = guessSpellingReason($child,$spelling);
            }
            my $sql = "UPDATE opinions SET modified=modified,spelling_reason='$newSpellingReason',child_no=$fields{child_no} WHERE opinion_no=$row->{opinion_no}";
            main::dbg("Migrating child: $sql");
            $dbh->do($sql);
            if ($row->{'status'} eq 'misspelling of') {
                push @parents,$row->{'parent_spelling_no'};
            }
            if ($row->{'child_spelling_no'} =~ /^\d+$/) {
                push @parents,$row->{'child_spelling_no'};
            }
            if ($row->{'child_no'} =~ /^\d+$/) {
                push @parents,$row->{'child_no'};
            }
        }
        
        # We also have to modify the parent_no so it points to the original combination of an taxa classified into
        # any migrated opinion
        if (@parents) {
            my $sql = "UPDATE opinions SET modified=modified, parent_no=$fields{'child_no'} WHERE parent_no IN (".join(",",@parents).")";
            main::dbg("Migrating parents: $sql");
            $dbh->do($sql);
        }
        
        # Make sure opinions authority information is synchronized with the original combination
        Taxon::propagateAuthorityInfo($dbt,$fields{'child_no'});

        # Remove any duplicates that may have been added as a result of the migration
        removeDuplicateOpinions($dbt,$s,$fields{'child_no'});
    }
    my $pid = fork();
    if (!defined($pid)) {
        carp "ERROR, could not fork";
    }

    if ($pid) {
        # Child fork
        # Don't exit here, have child go on to print message

        # Make new dbh and dbt objects - for some reason one connection
        # gets closed whent the other fork exits, so split them here
        $dbh = DBConnection::connect();
        $dbt = DBTransactionManager->new($dbh);  
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
  <br><li><b><a href="bridge.pl?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}">Add/edit opinion about $childSpellingName from another reference</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit opinions from the same reference</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionSearchForm&use_reference=current">Add/edit opinion about another taxon</a></b></li>
  <br><li><b><a href="bridge.pl?action=displayOpinionSearchForm">Add/edit opinion about another taxon from another reference</a></b></li>
</td></tr></table>
</p>
</div>|;

    # See Taxon::displayTypeTaxonSelectForm for details
    Taxon::displayTypeTaxonSelectForm($dbt,$s,$fields{'type_taxon'},$fields{'child_no'},$childName,$childRank,$resultReferenceNumber,$end_message);
}

# Gets a list of opinions that will be moved from a spelling to an original name.  Made into
# its own function so we can prompt the user before the move actually happens to make
# sure they're not making a mistake. The exclude_opinion_no is passed so we exclude the
# current opinion in the migration, which will only happen on an edit
sub getOpinionsToMigrate {
    my ($dbt,$child_no,$child_spelling_no,$exclude_opinion_no) = @_;
 
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$child_spelling_no);
    my $sql = "SELECT * FROM opinions WHERE child_no=$orig_no";
    if ($exclude_opinion_no =~ /^\d+$/) {
        $sql .= " AND opinion_no != $exclude_opinion_no";
    }
    my @results = @{$dbt->getData($sql)};
  
    my @opinions = ();
    foreach my $row (@results) {
        if ($row->{'child_no'} != $child_no) {
            push @opinions, $row;
        }
    }
    return @opinions;
}

# Displays a form which lists all opinions that currently exist
# for a reference no/taxon
# Moved/Adapted from Taxon::displayOpinionChoiceForm PS 01/24/2004
sub displayOpinionChoiceForm {
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
    } else {
        my @where = ();
        my @errors = ();
        my $join_refs = "";
        if ($q->param("reference_no")) {
            push @where, "o.reference_no=".int($q->param("reference_no"));
        }
        if ($q->param("authorizer_reversed")) {
            my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('authorizer_reversed')));
            my $authorizer_no = ${$dbt->getData($sql)}[0]->{'person_no'};
            if (!$authorizer_no) {
                push @errors, $q->param('authorizer_reversed')." is not a valid authorizer. Format like 'Sepkoski, J.'" if (!$authorizer_no); 
            } else {
                push @where, "o.authorizer_no=".$authorizer_no; 
            }
        }
        if ($q->param('created_year')) {
            my ($yyyy,$mm,$dd) = ($q->param('created_year'),$q->param('created_month'),$q->param('created_day'));
            my $date = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
            my $sign = ($q->param('created_before_after') eq 'before') ? '<=' : '>=';
            push @where,"o.created $sign $date";
        }
        if ($q->param('pubyr')) {
            my $pubyr = $dbh->quote($q->param('pubyr'));
            push @where,"((o.ref_has_opinion NOT LIKE 'YES' AND o.pubyr LIKE $pubyr) OR (o.ref_has_opinion LIKE 'YES' AND r.pubyr LIKE $pubyr))";
            $join_refs = " LEFT JOIN refs r ON r.reference_no=o.reference_no";
        }
        if ($q->param('author')) {
            my $author = $dbh->quote($q->param('author'));
            my $authorWild = $dbh->quote('%'.$q->param('author').'%');
            push @where,"((o.ref_has_opinion NOT LIKE 'YES' AND (o.author1last LIKE $author OR o.author2last LIKE $author OR o.otherauthors LIKE $authorWild)) OR".
                        "(o.ref_has_opinion LIKE 'YES' AND (r.author1last LIKE $author OR r.author2last LIKE $author OR r.otherauthors LIKE $authorWild)))";
            $join_refs = " LEFT JOIN refs r ON r.reference_no=o.reference_no";
        }

        if (@where && !@errors) {
            my $sql = "SELECT o.opinion_no FROM opinions o ".
                      " LEFT JOIN authorities a ON a.taxon_no=o.child_no".
                      $join_refs.
                      " WHERE ".join(" AND ",@where).
                      " ORDER BY a.taxon_name ASC";
            my @results = @{$dbt->getData($sql)};
            if (scalar(@results) == 0) {
                print "<div align=\"center\"<h3>No opinions found</h3></div><br><br>";
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
            print qq|<p><input type=submit value="Edit"></p><br>|;
            print "</div>";
        } else {
            if (@errors) {
                my $plural = (scalar(@errors) > 1) ? "s" : "";
                my $message = "<br><div align=center><table width=600 border=0>" .
                      "<tr><td class=darkList><font size='+1'><b> Error$plural</b></font></td></tr>" .
                      "<tr><td>";
                $message .= "<li class='medium'>$_</li>" for (@errors);
                $message .= "</td></tr></table>";
                $message .= "</div><br>"; 
                print $message;
            } else {
                print "<div align=\"center\">No terms were entered.</div>";
            }
        }
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

# Occasionally duplicate opinions will be created sort of due to user err.  User will enter
# an opinions 'A b belongs to A' when 'A b' isn't the original combination.  They they
# enter 'C b recombined as A b' from the same source, and the original 'A b' original gets
# migrated when its actually the same opinion.  Find these opinions.  Don't delete them,
# but just set all their key fields to zero and mark changes into the comments field
sub removeDuplicateOpinions {
    my ($dbt,$s,$child_no,$debug_only) = @_;
    my $dbh = $dbt->dbh;
    return if !($child_no);
    my $sql = "SELECT * FROM opinions WHERE child_no=$child_no AND child_no != parent_no AND status !='misspelling of'";
    my @results = @{$dbt->getData($sql)};
    my %dupe_hash = ();
    foreach my $row (@results) {
        if ($row->{'ref_has_opinion'} =~ /yes/i) {
            my $dupe_key = $row->{'reference_no'}.' '.$row->{'child_no'};
            push @{$dupe_hash{$dupe_key}},$row;
        } else {
            my $dupe_key = $row->{'child_no'}.' '.$row->{'author1last'}.' '.$row->{'author2last'}.' '.$row->{'otherauthors'}.' '.$row->{'pubyr'};
            if ($row->{'author1last'}) { #Deal with some older screwy data records just missing authority info
                push @{$dupe_hash{$dupe_key}},$row;
            }
        }
    }
    while (my ($key,$array_ref) = each %dupe_hash) {
        my @opinions = @$array_ref;
        if (scalar(@opinions) > 1) {
            my $orig_row = shift @opinions;
            foreach my $row (@opinions) {
                main::dbg("Found duplicate row for $orig_row->{opinion_no} in $row->{opinion_no}");
                $dbt->deleteRecord($s,'opinions','opinion_no',$row->{'opinion_no'},"Deleted by Opinion::removeDuplicateOpinion, duplicates $orig_row->{opinion_no}");
            }
        }
    }
}



1;


