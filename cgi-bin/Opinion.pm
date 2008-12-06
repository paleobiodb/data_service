package Opinion;
use strict;

use TypoChecker;
use CGI::Carp;
use Data::Dumper;
use TaxaCache;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD);

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
    dbg("Get spelling status called, return $spelling_reason");
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
    my %options = @_;
	my $row = $self->{'DBrow'};
    my $dbt = $self->{'dbt'};
	
    my $output = "";

    if ($row->{'status'} =~ /synonym|replace|nomen|revalidated|misspell|subgroup/) {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'child_spelling_no'}});
        my $child_html  = ($child->{'taxon_rank'} =~ /species|genus/) 
                        ? "<i>$child->{'taxon_name'}</i>" 
                        : $child->{'taxon_name'};
        if ($row->{'status'} =~ /^[aeiou]/) {
            $output .= "'$child_html is an $row->{status}";
        } elsif ($row->{'status'} =~ /^replaced/ ) {
            $output .= "'$child_html is $row->{status}";
        } else {
            $output .= "'$child_html is a $row->{status}";
        }
        if ($row->{'status'} =~ /synonym|replace|misspell|subgroup/) {
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

    my $short_ref = "";
    if ($row->{'ref_has_opinion'}) {
        $short_ref = Reference::formatShortRef($dbt,$row->{reference_no});
    } else {
        $short_ref = Reference::formatShortRef($row);
    }
    if ($options{'return_array'}) {
        return ($output," according to ".$short_ref);
    } else {
        $output .= " according to $short_ref";
        return $output;
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
    my ($dbt,$hbo,$s,$q,$error_message) = @_;
	
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
	my @synArray = ('','subjective synonym of', 'objective synonym of','replaced by','misspelling of','invalid subgroup of');
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

    # Grab the appropriate data to auto-fill the form
        # always give this a try because field values may have been passed
        #  in even if this is a new opinion
	%fields = %{$q->Vars};
	if (! $reSubmission) {
        if ($isNewEntry) {
            $fields{'child_no'} = $q->param('child_no');
            $fields{'child_spelling_no'} = $q->param('child_spelling_no') || $q->param('child_no');
    	    $fields{'reference_no'} = $s->get('reference_no');
            # to speed up entry, assume that the primary (current) ref has
            #  the opinion JA 29.8.06
            $fields{'ref_has_opinion'} = 'PRIMARY';
            # and even assume the taxon is valid 18.12.06
            $fields{'status_category'} = 'belongs to'; 
            # also hit the database to get the last values used for some of the
            #  fields, but only if the same reference was used JA 18.12.06
            # user are annoyed when the data are prefilled from autogenerated
            #  species-belongs-to-genus opinions, so don't do this if the
            #  opinion looks like such a thing because it is species level,
            #  "belongs to," original spelling, stated with evidence, and with a new
            #  diagnosis JA 6.6.07
            my $sql = "SELECT diagnosis_given,ref_has_opinion,o.pages,basis FROM opinions o,authorities a WHERE child_no=taxon_no AND ((diagnosis_given!='' AND diagnosis_given IS NOT NULL) OR (o.pages!='' AND o.pages IS NOT NULL) OR (basis!='' AND basis IS NOT NULL)) AND o.reference_no=" . $s->get('reference_no') . " AND o.enterer_no=" . $s->get('enterer_no') . " AND child_no!=" . $q->param('child_no') . " AND (taxon_rank NOT LIKE '%species%' OR status!='belongs to' OR spelling_reason!='original spelling' OR basis!='stated with evidence' OR diagnosis_given!='new') ORDER BY opinion_no DESC LIMIT 1";
            my $lastopinion = @{$dbt->getData($sql)}[0];
            if ( $lastopinion->{ref_has_opinion} eq "YES" )	{
                $fields{'diagnosis_given'} = $lastopinion->{diagnosis_given};
                $fields{'pages'} = $lastopinion->{pages};
                $fields{'basis'} = $lastopinion->{basis};
            }
            my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_no'}});
            my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{'child_spelling_no'}});
            my $reason = guessSpellingReason($child,$spelling);
            $fields{'spelling_reason'} = $reason;
        } else {
            %fields = %{$o->getRow()};

            if ($fields{'max_interval_no'}) {
                my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals WHERE interval_no=$fields{'max_interval_no'}";
                my $row = ${$dbt->getData($sql)}[0];
                $fields{'max_interval_name'} = $row->{'eml_interval'}." ".$row->{'interval_name'};
                $fields{'max_interval_name'} =~ s/^\s*//;
                $fields{'max_interval_name'} =~ s/Late\/Upper/Late/;
                $fields{'max_interval_name'} =~ s/Early\/Lower/Early/;
            }
            if ($fields{'min_interval_no'}) {
                my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals WHERE interval_no=$fields{'min_interval_no'}";
                my $row = ${$dbt->getData($sql)}[0];
                $fields{'min_interval_name'} = $row->{'eml_interval'}." ".$row->{'interval_name'};
                $fields{'min_interval_name'} =~ s/^\s*//;
                $fields{'min_interval_name'} =~ s/Late\/Upper/Late/;
                $fields{'min_interval_name'} =~ s/Early\/Lower/Early/;
            }

            if ($fields{'ref_has_opinion'} =~ /YES/i) {
                $fields{'ref_has_opinion'} = 'PRIMARY';
            } else {
                $fields{'ref_has_opinion'} = 'NO';
            }

            # if its from the DB, populate appropriate form vars
            for (@belongsArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'status_category'} = 'belongs to';
                }
            }    
            if ($fields{'status'} eq 'misspelling of') {
                $fields{'status_category'} = 'misspelling of';
            }
            for (@synArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'status_category'} = 'invalid1';
                    $fields{'synonym'} = $_;
                    last;
                }
            } 
            for (@nomArray) { 
                if ($_ eq $fields{'status'}) {
                    $fields{'status_category'} = 'invalid2';
                    last;
                }
            }    
        }
    }

        
    # Get the child name and rank
    my $childTaxon = Taxon->new($dbt,$fields{'child_no'});
    my $childName = $childTaxon->get('taxon_name');
    my $childRank = $childTaxon->get('taxon_rank');

    # This block gets a list of potential homonyms, either from the database for an edit || resubmission
    # or from the values passed in for a new && resubmission
    my @child_spelling_nos = ();
    my $childSpellingName = "";
    my $childSpellingRank = "";
    if ($fields{'child_spelling_no'} > 0) {
        # This will happen on an edit (first submission) OR resubmission w/homonyms
        # SQL trick: get not only the authority data for child_spelling_no, but all its homonyms as well
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
    # If the childSpellingName and childName are the same (and possibly ambiguous)
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
        if ($q->param('belongs_to_parent')) { 
            $parentName = $q->param('belongs_to_parent');
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
    my @parents_to_migrate2;
    if ($fields{'status_category'} eq 'invalid1' && $fields{'synonym'} eq 'misspelling of') {
        if (scalar(@parent_nos) == 1) {
            # errors are ignored because the form is only being displayed
            my ($ref1,$ref2,$error) = getOpinionsToMigrate($dbt,$fields{'child_no'},$parent_nos[0],$fields{'opinion_no'});
            @opinions_to_migrate2 = @{$ref1};
            @parents_to_migrate2 = @{$ref2};
        }
    } 

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
	my $parent_pulldown;
	if ( scalar(@parent_nos) > 1 || (scalar(@parent_nos) == 1 && @opinions_to_migrate2 && $fields{'status_category'} eq 'invalid1' && $fields{'synonym'} eq 'misspelling of')) {
        $parent_pulldown .= qq|\n<span class="small">\n|;
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
            $parent_pulldown .= qq|<br><nobr><input type="radio" name="parent_spelling_no" $selected value='$parent_no'> $parentName, $taxon->{taxon_rank}$pub_info [$higher_class]</nobr>\n|;
        }
        $parent_pulldown .= qq|<br><input type="radio" name="parent_spelling_no" value=""> \n<span class=\"prompt\">Other taxon:</span> <input type="text" name="belongs_to_parent" value=""><br>\n|;
        $parent_pulldown .= "\n</span>\n";
	}

    dbg("parentName $parentName parents: ".scalar(@parent_nos));
    dbg("childSpellingName $childSpellingName spellings: ".scalar(@child_spelling_nos));

    if (!$isNewEntry) {
        if ($o->get('authorizer_no')) {
            $fields{'authorizer_name'} = "<span class=\"fieldName\">Authorizer:</span> " . Person::getPersonName($dbt,$o->get('authorizer_no')); 
        }   
        if ($o->get('enterer_no')) { 
            $fields{'enterer_name'} = " <span class=\"fieldName\">Enterer:</span> " . Person::getPersonName($dbt,$o->get('enterer_no')); 
        }
        if ($o->get('modifier_no')) { 
            $fields{'modifier_name'} = " <span class=\"fieldName\">Modifier:</span> ".Person::getPersonName($dbt,$o->get('modifier_no'));
        }
        $fields{'modified'} = "<span class=\"fieldName\">Modified:</span> ".$fields{'modified'};
        $fields{'created'} = "<span class=\"fieldName\">Created:</span> ".$fields{'created'}; 
    }

    my $fossil_record_ref = 0;
    if ($fields{'reference_no'}) {
        my $ref = Reference->new($dbt,$fields{'reference_no'}); 
        if ($ref->{'project_name'} =~ /fossil record/) {
            $fossil_record_ref = 1;
        }
        $fields{formatted_primary_reference} = $ref->formatAsHTML() if ($ref);
    }

    if ($s->get('reference_no') && $s->get('reference_no') != $fields{'reference_no'}) {
        my $ref = Reference->new($dbt,$s->get('reference_no'));
        $fields{formatted_current_reference} = $ref->formatAsHTML() if ($ref);
        $fields{'current_reference'} = 'yes';
    } 
	
	# Anyone can now edit anything. Restore from CVS if we have to backpedal PS 5/17/2006

    # Each of the 'status' row options
	my $belongs_to_row;
    my $spelling_row;

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
    my $selected= "CHECKED";
    $belongs_to_row .= "<tr>\n<td colspan=\"2\">\n<span class=\"small\"><span class=\"prompt\">Status and parent:</span>&nbsp;</span> ";
    my @statusArray;
    if ( $childRank =~ /species|genus/ )	{
	@statusArray = ( 'belongs to', @synArray );
        push @statusArray , @nomArray;
    } else	{
	@statusArray = ( 'belongs to', @synArray );
    }
    $belongs_to_row .= $hbo->htmlSelect('status',\@statusArray, \@statusArray, $fields{'status'});

    if ($parent_pulldown && $selected) {
        $belongs_to_row .= "$parent_pulldown";
    } else	{
        my $parentTaxon = ($selected || ($isNewEntry && $childRank =~ /species/)) ? $parentName : "";
        $belongs_to_row .= qq|<input name="belongs_to_parent" size="24" value="$parentTaxon">|;
    }
    $belongs_to_row .= qq|</td></tr>|;


    if (!$reSubmission && !$isNewEntry) {

        my @taxa = Taxon::getTypeTaxonList($dbt,$fields{'child_no'},$fields{'reference_no'});
        $fields{'type_taxon'} = 0;
        foreach my $row (@taxa) {
            if ($row->{'type_taxon_no'} == $fields{'child_no'}) {
                $fields{'type_taxon'} = 1;
            }
        }
    }
    my $type_select = "";
    if ($childRank =~ /species|genus|tribe|family/) {
        my $checked = ($fields{'type_taxon'}) ? "CHECKED" : "";
        $type_select = "&nbsp;&nbsp;<input name=\"type_taxon\" type=\"checkbox\" $checked  value=\"1\">".
                       " <span class=\"prompt\">This is the type $childRank</span>";
    }
    my ($phyl_keys,$phyl_values) = $hbo->getKeysValues('phylogenetic_status');
    my $phyl_select = $hbo->htmlSelect('phylogenetic_status',$phyl_keys,$phyl_values,$fields{'phylogenetic_status'});
    
    $belongs_to_row .= qq|<tr><td><div class="small">|;
    if ( $childRank !~ /species/ )	{
        $belongs_to_row .= "<span class=\"prompt\">Phylogenetic status:</span> $phyl_select";
    } 
    if ( $childRank =~ /species|genus|tribe|family/ )	{
        $belongs_to_row .= $type_select;
    }
    $belongs_to_row .= "</div></td></tr>";

	# if this is a second pass and we have a list of alternative taxon
	#  numbers, make a pulldown menu listing the taxa JA 25.4.04
    # build the spelling pulldown, if necessary, else the spelling box

    my @ranks = ();
    if ($childRank =~ /subspecies|species/) {
        @ranks = ('subspecies','species');
    } elsif ($childRank =~ /subgenus|genus/) {
        @ranks = ('subgenus','genus');
    } else {
        @ranks = grep {!/subspecies|species|subgenus|genus/} $hbo->getList('taxon_rank');
    } 

    my @opinions_to_migrate1;
    my @parents_to_migrate1;
    if (scalar(@child_spelling_nos) == 1) {
        # errors are ignored because the form is only being displayed
        my ($ref1,$ref2,$error) = getOpinionsToMigrate($dbt,$fields{'child_no'},$child_spelling_nos[0],$fields{'opinion_no'});
        @opinions_to_migrate1 = @{$ref1};
        @parents_to_migrate1 = @{$ref2};
    }

    my $spelling_note = "<small>If the name is invalid, enter the invalid name and not its senior synonym, replacement, etc.</small>";
    $spelling_row .= "<tr><td colspan=\"2\" class=\"small\"><span class=\"prompt\">Full name and rank of the taxon used in the reference:</span></td></tr>";

	my $spelling_rank_pulldown = $hbo->htmlSelect('child_spelling_rank',\@ranks, \@ranks, $fields{'child_spelling_rank'});
	if (scalar(@child_spelling_nos) > 1 || (scalar(@child_spelling_nos) == 1 && @opinions_to_migrate1)) {
		$spelling_row .= "<tr><td nowrap width=\"100%\" class=\"small\">";
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
		$spelling_row .= qq|<tr><td nowrap width="100%" class="small"><input id="child_spelling_name" name="child_spelling_name" size=30 value="$childSpellingName">$spelling_rank_pulldown<br>$spelling_note</td></tr>|;
	}


    my @select_values = ();
    my @select_keys = ();
    if ($childRank =~ /subgenus/) {
        my ($genusName,$subGenusName) = Taxon::splitTaxon($childName);
        @select_values = ('original spelling','correction','misspelling','rank change','reassignment');
        @select_keys = ("is the original spelling and rank", "is a correction of '$childName'","is a misspelling","has had its rank changed from $childRank","has been reassigned from its original genus '$genusName'");
    } elsif ($childRank =~ /species/) {
        @select_values = ('original spelling','recombination','correction','misspelling');
        @select_keys = ("is the original spelling and rank","is a recombination or rank change of '$childName'","is a correction of '$childName'","is a misspelling");
    } else {
        @select_values = ('original spelling','correction','misspelling','rank change');
        @select_keys = ("is the original spelling and rank","is a correction of '$childName'","is a misspelling","has had its rank changed from its original rank of $childRank");
    }
    $spelling_row .= "<tr><td>&nbsp;</td></tr>";
    $spelling_row .= "<tr><td class=\"small\"><span class=\"prompt\">Reason why this spelling and rank was used:</span><br>This ". $hbo->htmlSelect('spelling_reason',\@select_keys,\@select_values,$fields{'spelling_reason'})."</td></tr>";

    dbg("showOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");

	$fields{belongs_to_row} = $belongs_to_row;
	$fields{spelling_row} = $spelling_row;

	# print the form	
    $fields{'error_message'} = $error_message;

    if ($fossil_record_ref) {
        print $hbo->populateHTML("fossil_record_opinion", \%fields);
    } else {
        print $hbo->populateHTML("add_enter_opinion", \%fields);
    }
}


# Call this when you want to submit an opinion form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
sub submitOpinionForm {
    my ($dbt,$hbo,$s,$q) = @_;

	my %rankToNum = %Taxon::rankToNum;
  
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
        $fields{'opinion_no'} = $o->get('opinion_no');
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
	my $lookup_reference = "";
	if ( $q->param('ref_has_opinion') eq 'CURRENT' )	{
		$lookup_reference = $s->get('reference_no');
	} else	{
		$lookup_reference = $fields{'reference_no'};
	}
	my $ref = Reference->new($dbt,$lookup_reference);


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
        if ($isNewEntry && $q->param('status') ne 'misspelling of') {
		    my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion !='YES' ".
                      " AND child_no=".$dbh->quote($fields{'child_no'}).
                      " AND author1last=".$dbh->quote($q->param('author1last')).
                      " AND author2last=".$dbh->quote($q->param('author2last')).
                      " AND pubyr=".$dbh->quote($q->param('pubyr')).
                      " AND status NOT IN ('misspelling of')";
            my $row = ${$dbt->getData($sql)}[0];
            # also make sure there isn't a primary report of this opinion
            #  JA 9.1.07
		    my $sql = "SELECT count(*) c FROM opinions o,refs r WHERE ref_has_opinion ='YES' ".
                      " AND child_no=".$dbh->quote($fields{'child_no'}).
                      " AND o.reference_no=r.reference_no".
                      " AND r.author1last=".$dbh->quote($q->param('author1last'));
                      if ( $q->param('author2last') )	{
                          $sql .= " AND r.author2last=".$dbh->quote($q->param('author2last'));
                      }
                      $sql .= " AND r.pubyr=".$dbh->quote($q->param('pubyr')).
                      " AND status NOT IN ('misspelling of','homonym of')";
            my $row2 = ${$dbt->getData($sql)}[0];

            if ( $row->{'c'} > 0 || $row2->{'c'} > 0 ) {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }

		if (! $q->param('author1last')) {
			$errors->add('You must enter at least the last name of the first author');	
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
        my $sql = "SELECT count(*) c FROM opinions WHERE ref_has_opinion='YES'".
                  " AND child_no=".$dbh->quote($fields{'child_no'}).
                  " AND reference_no=".$dbh->quote($lookup_reference).
                  " AND status NOT IN ('misspelling of')";
        if (! $isNewEntry) {
            $sql .= " AND opinion_no != ".$o->{'opinion_no'};
        }
        my $row = ${$dbt->getData($sql)}[0];
        # also make sure there isn't a secondary report of this opinion
        #  JA 9.1.07
        my $sql = "SELECT author1last,author2last,pubyr FROM refs WHERE reference_no=".$dbh->quote($lookup_reference);
        my $row2 = ${$dbt->getData($sql)}[0];
        my $row3;
        if ( $row2->{author1last} )	{
            my $sql = "SELECT count(*) c FROM opinions WHERE author1last=".$dbh->quote($row2->{author1last})." AND pubyr=".$dbh->quote($row2->{pubyr});
            if ( $row2->{author2last} )	{
                $sql .= " AND author2last=".$dbh->quote($row2->{author2last});
            }
            $sql .= " AND child_no=".$dbh->quote($fields{'child_no'}).
                    " AND status NOT IN ('misspelling of','homonym of')";
            if (! $isNewEntry) {
                $sql .= " AND opinion_no != ".$o->{'opinion_no'};
            }
            $row3 = ${$dbt->getData($sql)}[0];
        }

        if ($row->{'c'} > 0 || $row3->{'c'} > 0) {
            unless ($q->param('status') eq 'misspelling of') {
                $errors->add("The author's opinion on ".$childName." already has been entered - an author can only have one opinion on a name");
            }
        }
		# ref_has_opinion is PRIMARY or CURRENT
		# so make sure the other publication info is empty.
		
		if ($q->param('author1init') || $q->param('author1last') || $q->param('author2init') || $q->param('author2last') || $q->param('otherauthors') || $q->param('pubyr')) {
			$errors->add("Don't enter any other publication information if you chose the 'primary reference argues ...' or 'current reference argues ...' radio button");	
		}
        
		# also make sure that the pubyr of this opinion isn't older than
		# the pubyr of the authority record the opinion is about
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
        #  * Hit the DB to make sure there's exactly 1 copy of the parent name (error if > 1 or 0)

	$parentName = $q->param('belongs_to_parent');

        # Get a single parent no or we have an error
        if (!$parentName) {
            if ($q->param('status') =~ /nomen/) {
                $errors->add("Even if a name is invalid, you must enter a different name that should be used instead of it");
            } elsif ($q->param('status') !~ /belongs to/) {
                $errors->add("You must enter a taxonomic name that should be used instead of this one");
            } else {
                $errors->add("You must enter the name of a higher taxon this one belongs to");
            }
        } else {    
            my @parents = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$parentName}); 
            if (scalar(@parents) > 1) {
                $errors->add("The taxon '$parentName' exists multiple times in the database. Please select the one you want");	
            } elsif (scalar(@parents) == 0) {
                $errors->add("The taxon '$parentName' doesn't exist in our database.  Please <A HREF=\"$WRITE_URL?action=displayAuthorityForm&taxon_no=-1&taxon_name=$parentName\">create a new authority record for '$parentName'</a> <i>before</i> entering this opinion");	
            } elsif (scalar(@parents) == 1) {
                $fields{'parent_spelling_no'} = $parents[0]->{'taxon_no'};
                $fields{'parent_no'} = TaxonInfo::getOriginalCombination($dbt,$fields{'parent_spelling_no'});
                $parentRank = $parents[0]->{'taxon_rank'};
            }
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
    if ($q->param('status') eq 'misspelling of' && $fields{'parent_spelling_no'}) {
        my $new_orig = TaxonInfo::getOriginalCombination($dbt,$fields{'parent_spelling_no'});
        $childTaxon = Taxon->new($dbt,$new_orig);
        $childName = $childTaxon->get('taxon_name');
        $childRank = $childTaxon->get('taxon_rank');
        $fields{'child_no'} = $new_orig;
    } 

    dbg("child_no $fields{child_no} childRank $childRank childName $childName ");
    dbg("child_spelling_no $fields{child_spelling_no} childSpellingRank $childSpellingRank childSpellingName $childSpellingName");
    dbg("parent_no $fields{parent_no}  parent_spelling_no $fields{parent_spelling_no} parentSpellingRank $parentRank parentSpellingName $parentName");
    

    ############################
    # Validate the form, bottom section
    ############################
    # Flatten the status
	$fields{'status'} = $q->param('status');
	$fields{'spelling_reason'} = $q->param('spelling_reason');

	if (!$fields{'status'}) {
		$errors->add("No option was selected in the \"Status\" pulldown");
	} 
	if (!$fields{'spelling_reason'}) {
		$errors->add("No option was selected in the \"Enter the reason why this spelling was used\" pulldown");
	} 


    my @opinions_to_migrate1;
    my @opinions_to_migrate2;
    my @parents_to_migrate1;
    my @parents_to_migrate2;
    if ($fields{'status'} eq 'misspelling of') {
        if ($fields{'parent_spelling_no'}) {
            my ($ref1,$ref2,$error) = getOpinionsToMigrate($dbt,$fields{'parent_no'},$fields{'child_no'},$fields{'opinion_no'});
            if ( $error )	{
                $errors->add("$childSpellingName can't be a misspelling of $parentName because there is already a '$error' opinion linking them, so they must be biologically distinct");
            } else	{
                @opinions_to_migrate2 = @{$ref1};
                @parents_to_migrate2 = @{$ref2};
            }
        }
    }
    if ($fields{'child_spelling_no'}) {
        my ($ref1,$ref2,$error) = getOpinionsToMigrate($dbt,$fields{'child_no'},$fields{'child_spelling_no'},$fields{'opinion_no'});
        if ( $error )	{
            $errors->add("$childSpellingName can't be an alternate spelling of $childName because there is already a '$error' opinion linking them, so they must be biologically distinct");
        } else	{
            @opinions_to_migrate1 = @{$ref1};
            @parents_to_migrate1 = @{$ref2};
        }
    }
    if ((@opinions_to_migrate1 || @opinions_to_migrate2 || @parents_to_migrate1 || @parents_to_migrate2) && $q->param("confirm_migrate_opinions") !~ /YES/i && $errors->count() == 0) {
        dbg("MIGRATING:<PRE>".Dumper(\@opinions_to_migrate1)."</PRE><PRE>".Dumper(\@opinions_to_migrate2)."</PRE>"); 
        my $msg = "";
        if (@opinions_to_migrate1) {
            $msg .= "<b>$childSpellingName</b> already exists with <a href=\"$WRITE_URL?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}\" target=\"_BLANK\"> opinions classifying it</a>."; 
        }
        if (@opinions_to_migrate2) {
            $msg .= "<b>$childSpellingName</b> already exists with <a href=\"$WRITE_URL?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}\" target=\"_BLANK\"> opinions classifying it</a>."; 
        }
        if ( ! @opinions_to_migrate1 && ! @opinions_to_migrate2 && ( @parents_to_migrate1 || @parents_to_migrate2 ) ) {
            $msg .= "<b>$childSpellingName</b> already exists</a>."; 
        }
        $msg .= " If you hit submit again, this name will be combined permanently with the existing one. This means: <ul>";
        $msg .= " <li> '$childName' will be considered the 'original' name.  If another spelling is actually the original one, please enter opinions based on that other name.</li>";
        $msg .= " <li> Authority information will be made identical and linked.  Changes to one name's authority record will be copied over automatically to the other's.</li>";
        $msg .= " <li> These names will be considered the same when editing/adding opinions, downloading, searching, etc.</li>";
        $msg .= "</ul>";
        if ($fields{'status'} ne 'misspelling of') {
            $msg .= " If '$childName' is actually a misspelling of '$childSpellingName', please enter 'Invalid, this taxon is a misspelling of $childSpellingName' in the 'How was it classified' section, and enter '$childName' in the 'How was it spelled' section.<br>";
        }
        if (@opinions_to_migrate1) {
            $msg .= " If '$childSpellingName' is actually a homonym (same spelling, totally different taxon), please select 'Create a new '$childSpellingName' in the 'How was it spelled?' section below.";
        }
        $errors->add($msg);
        $q->param('confirm_migrate_opinions'=>'YES');
    }

    # Error checking related to ranks
    # Only bother if we're down to one parent
    if ($fields{'parent_spelling_no'}) {
        if ($q->param('status') eq 'belongs to')	{ 
            # for belongs to, the parent rank should always be higher than the child rank.
            # unless either taxon is an unranked clade (JA)
            if ($rankToNum{$parentRank} <= $rankToNum{$childSpellingRank}
                && $parentRank ne "unranked clade" 
                && $childSpellingRank ne "unranked clade")	{
                $errors->add("The rank of the higher taxon '$parentName' ($parentRank) must be higher than the rank of '$childSpellingName' ($childSpellingRank)");	
            }
        } elsif ($q->param('status') !~ /invalid subgroup of|nomen/ && $parentRank ne $childSpellingRank && $parentRank !~ /unranked/ && $childSpellingRank !~ /unranked/)	{
            # synonyms should be of the same rank, but invalid subgroups can be
            #  of any rank because (for example) sometimes a taxon is considered
            #  simultaneously to be of too high a rank, and an invalid subgroup
            #  of a lower-ranked taxon JA 29.1.07
            # ranks are irrelevant if unranked clades are involved JA 14.6.07
            $errors->add("The rank of a taxon and the rank of its synonym, homonym, or replacement name must be the same");
        # nomina dubia can belong to anything as long as they are not species
        #  JA 11.11.07
        } elsif ($q->param('status') =~ /nomen/ && $parentRank eq $childSpellingRank && $parentRank =~ /species/ && $childSpellingRank =~ /species/)	{
            $errors->add("A ".$q->param('status')." cannot be identifiable as a species");
        } 
        if ($q->param('status') eq 'belongs to') {
            if ($childSpellingRank eq 'species' && $parentRank !~ /genus/) {
                $errors->add("A species must be assigned to a genus or subgenus and not a higher order name");
            }
        }
    }

    # Some more rank checks
    if ($q->param('spelling_reason') =~ 'rank change') {
        # JA: ... except if the status is "rank changed as," which is actually the opposite case
        if ( $childSpellingRank eq $childRank) {
            $errors->add("If you change a taxon's rank, its old and new ranks must be different");
        } elsif ($childRank eq "subgenus" && $childSpellingRank eq "genus") {
            my ($childStart,$g) = split / /,$childName;
            $childStart =~ s/[\(\)]//g;
            if ($childSpellingName eq $childStart)	{
                $errors->add("If the two parts of a subgenus name are identical, the genus and subgenus must be biologically different, so you can't make $childSpellingName a new spelling of $childName");
            }
        } elsif ($childRank eq "genus" && $childSpellingRank eq "subgenus") {
            my ($childStart,$g) = split / /,$childSpellingName;
            $childStart =~ s/[\(\)]//g;
            if ($childName eq $childStart)	{
                $errors->add("If the two parts of a subgenus name are identical, the genus and subgenus must be biologically different, so you can't make $childSpellingName a new spelling of $childName");
            }
        }
    } else {
        if ($childSpellingRank ne $childRank && $q->param('spelling_reason') !~ /recombination|misspelling/) {
            $errors->add("Unless a taxon has its rank changed or is recombined, the rank entered in the \"How was it spelled?\" section must match the taxon's original rank (if the rank has changed, select \"rank change\" even if the spelling remains the same)");
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
                $errors->add("If \"This is the original spelling and rank\" is selected, you must enter '$childName', '$childRank' in the \"How was it spelled?\" section");
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
        if ($fields{'status'} =~ /belongs to|correction/) {
            if ($spellingParent ne $parentName && 
                 ($childRank =~ /species/ || 
                   ($childRank =~ /subgenus/ && $q->param('spelling_reason') ne 'rank change'))){
		        $errors->add("The $childSpellingRank entered in the \"How was it spelled?\" should match with the higher order name entered in \"How was it classified?\" section");
            }
        }
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
            $errors->add("The genus and subgenus in the \"How was it classified?\" and the \"How was it spelled?\" sections must be the same if the latter is marked as the original spelling");
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
            $errors->add("The name entered in the \"How was it spelled\" section must be different from the name of the parent");
        }
    } else {
        if ($parentName eq $childName || $parentName eq $childSpellingName) {
            $errors->add("The taxon you enter and the one it belongs to can't have the same name");	
        } elsif ($fields{'child_no'} == $fields{'parent_no'}) {
            $errors->add("A taxon can't belong to itself");	
        }
    }

    my $rankFromSpaces = Taxon::guessTaxonRank($childSpellingName);
    if (($rankFromSpaces eq 'subspecies' && $childSpellingRank ne 'subspecies') ||
        ($rankFromSpaces eq 'species' && $childSpellingRank ne 'species') ||
        ($rankFromSpaces eq 'subgenus' && $childSpellingRank ne 'subgenus') ||
        ($rankFromSpaces !~ /species|genus/ && $childSpellingRank =~ /subspecies|species|subgenus/)) {
        $errors->add("The selected rank '$childSpellingRank' doesn't match the spacing of the taxon name '$childSpellingName'");
    }
	
	# The diagnosis field only applies to the case where the status
	# is belongs to or recombined as.
	if ( $q->param('status') !~ /belongs to/ && $q->param('diagnosis')) {
		$errors->add("Don't enter a diagnosis if the taxon is invalid");
	}
	if ( $q->param('status') !~ /belongs to/ && $q->param('diagnosis_given')) {
		$errors->add("Don't select a diagnosis category if the taxon is invalid");
	}

	if ($q->param('diagnosis') && $q->param("diagnosis_given") =~ /^$|none/) {
		$errors->add("If you enter a diagnosis, please also select a category for it in the \"Diagnosis\" pulldown");
	}

    if ($IS_FOSSIL_RECORD) {
        if ($q->param('max_interval_name')) {
            my ($max_no,$err1) = FossilRecord::parseIntervalName($dbt,$q->param('max_interval_name'));
            $fields{'max_interval_no'} = $max_no;
            foreach (@$err1) {
                $errors->add($_->{'message'});
            }
        } elsif ($childRank =~ /genus/) {
            $errors->add('First interval is required');
        }

        if ($q->param('min_interval_name')) {
            my ($min_no,$err2) = FossilRecord::parseIntervalName($dbt,$q->param('min_interval_name'));
            $fields{'min_interval_no'} = $min_no;
            foreach (@$err2) {
                $errors->add($_->{'message'});
            }
        } elsif ($childRank =~ /genus/) {
            $errors->add('Last interval is required');
        }
    }

    # Get the fields from the form and get them ready for insertion
    # All other fields should have been set or thrown an error message at some previous time
    foreach my $f ('author1init','author1last','author2init','author2last','otherauthors','pubyr','pages','figures','comments','diagnosis','phylogenetic_status','basis','type_taxon','diagnosis_given') {
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
        my ($new_taxon_no,$set_warnings) = Taxon::addSpellingAuthority($dbt,$s,$fields{'child_no'},$childSpellingName,$childSpellingRank,$fields{'reference_no'});
            
        
        $fields{'child_spelling_no'} = $new_taxon_no;
        if (ref($set_warnings) eq 'ARRAY') {
            push @warnings, @{$set_warnings};
        }
	}

	my $resultOpinionNumber;
    my $resultReferenceNumber = $fields{'reference_no'};

    dbg("submitOpinionForm, fields are: <pre>".Dumper(\%fields)."</pre>");
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
            # Delete this field so its never updated unless we're switching to current ref
            delete $fields{'reference_no'};
        }

		$resultOpinionNumber = $o->get('opinion_no');
		$dbt->updateRecord($s,'opinions', 'opinion_no',$resultOpinionNumber, \%fields);

	}
    
    if ( @opinions_to_migrate1 || @opinions_to_migrate2 || @parents_to_migrate1 || @parents_to_migrate2 )	{
        dbg("Migrating ".(scalar(@opinions_to_migrate1)+scalar(@opinions_to_migrate2))." opinions");
        foreach my $row  (@opinions_to_migrate1,@opinions_to_migrate2) {
            resetOriginalNo($dbt,$fields{'child_no'},$row);
        }

        # We also have to modify the parent_no so it points to the original
        #  combination of any taxa classified into any migrated opinion
        if ( @parents_to_migrate1 || @parents_to_migrate2 ) {
            push @parents_to_migrate1, @parents_to_migrate2;
            my $sql = "UPDATE opinions SET modified=modified, parent_no=$fields{'child_no'} WHERE parent_no IN (".join(",",@parents_to_migrate1).")";
            dbg("Migrating parents: $sql");
            $dbh->do($sql);
        }
        
        # Make sure opinions authority information is synchronized with the original combination
        Taxon::propagateAuthorityInfo($dbt,$fields{'child_no'});

        # Remove any duplicates that may have been added as a result of the migration
        $resultOpinionNumber = removeDuplicateOpinions($dbt,$s,$fields{'child_no'},$resultOpinionNumber);
    }

    $o = Opinion->new($dbt,$resultOpinionNumber); 
    my $opinionHTML = $o->formatAsHTML();
    $opinionHTML =~ s/according to/of/i;

	my $enterupdate = ($isNewEntry) ? 'entered' : 'updated';

    # we need to warn about the nasty case in which the author has synonymized
    #  genera X and Y, but we do not know the author's opinion on one or more
    #  species placed at some point in X
    if ( $childRank =~ /genus/ && $q->param('status') !~ /belongs to/ )	{
        # get every opinion on every child ever assigned to this genus
        # we join on o2 to make sure that they have been
        my $sql = "SELECT taxon_name,o.child_no,o.ref_has_opinion,o.reference_no reference_no,IF (o.ref_has_opinion='YES',r.author1last,o.author1last) author1last,IF (o.ref_has_opinion='YES',r.author2last,o.author2last) author2last,IF (o.ref_has_opinion='YES',r.pubyr,o.pubyr) pubyr FROM refs r,opinions o,opinions o2,authorities WHERE r.reference_no=o.reference_no AND taxon_no=o.child_no AND taxon_no=o2.child_no AND o2.parent_spelling_no =" . $fields{child_spelling_no} . " ORDER BY pubyr";
        my @childrefs = @{$dbt->getData($sql)};
        my %authorHasOpinion;
        my %speciesName;
        for my $cr ( @childrefs )	{
            if ( ( $fields{'ref_has_opinion'} ne "YES" && $cr->{'pubyr'} <= $fields{'pubyr'} ) || ( $fields{'ref_has_opinion'} eq "YES" && $cr->{'pubyr'} <= $ref->get('pubyr') ) )	{
                $speciesName{$cr->{child_no}} = $cr->{taxon_name};
                if ( ! $authorHasOpinion{$cr->{child_no}} )	{
                    $authorHasOpinion{$cr->{child_no}} = "NO";
                }
        # we test only on author1last, author2last, and pubyr to avoid
        #  false mismatches due to typos
                if ( $cr->{reference_no} == $resultReferenceNumber && $cr->{ref_has_opinion} eq "YES" && $fields{'ref_has_opinion'} eq "YES" )	{
                    $authorHasOpinion{$cr->{child_no}} = "YES";
                } elsif ( $cr->{author1last} eq $fields{author1last} && $cr->{author2last} eq $fields{author2last} && $cr->{pubyr} eq $fields{pubyr} && $cr->{ref_has_opinion} ne "YES" && $fields{'ref_has_opinion'} ne "YES" )	{
                    $authorHasOpinion{$cr->{child_no}} = "YES";
                }
            }
        }
        my @children = sort { $speciesName{$a} cmp $speciesName{$b} } keys %authorHasOpinion;
        my $needOpinion;
        for my $ch ( @children )	{
            if ( $authorHasOpinion{$ch} eq "NO" )	{
                if ( ! $needOpinion )	{
                    $needOpinion = $speciesName{$ch};
                } else	{
                    if ( $needOpinion !~ / and / )	{
                        $needOpinion .= " and " . $speciesName{$ch};
                    } else	{
                        $needOpinion =~ s/ and /, /;
                        $needOpinion .= " and " . $speciesName{$ch};
                    }
                }
            }
        }
        $needOpinion =~ s/^, //;
        my $authors;
        if ( $opinionHTML =~ / and | et al/ )	{
            $authors = "These authors'";
        } else	{
            $authors = "This author's";
        }
        if ( $needOpinion =~ / and / )	{
            push @warnings , $authors . " opinions on " . $needOpinion . " still may need to be entered";
        } elsif ( $needOpinion )	{
            push @warnings , $authors . " opinion on " . $needOpinion . " still may need to be entered";
        }
    }

    my $end_message .= qq|
<div align="center">
<p class="medium">The opinion $opinionHTML has been $enterupdate</p>
|;

    if (@warnings) {
        $end_message .= "<DIV class=\"warning\">";
        if ( $#warnings > 0 )	{
            $end_message .= "Warnings:<BR>";
            $end_message .= "<LI>$_</LI>" for (@warnings);
        } else	{
            $end_message .= "Warning: " . $warnings[0];
        }
        $end_message .= "</DIV>";
    }

    # the authority data are very useful for deciding whether to also edit them
    #  JA 15.7.07
    my $auth = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$fields{child_spelling_no}},['author1last','author2last','otherauthors','pubyr']);
    my $authors = $auth->{'author1last'};
    if ( $auth->{'otherauthors'} )	{
        $authors .= " et al.";
    } elsif ( $auth->{'author2last'} )	{
        $authors .= " and " . $auth->{'author2last'};
    }
    $authors .= " " . $auth->{'pubyr'};

    my $style = qq| style="padding-top: 0.75em;"|;
    $end_message .= qq|
<div class="displayPanel">
<p>
<table cellpadding="10"><tr><td valign="top" class="small">
  <p class="large" style="margin-left: 2em;">Name functions</p>
  <ul>
  <li><a href="$WRITE_URL?action=displayAuthorityTaxonSearchForm">Add/edit another taxon</a></li>
  <br><li><a href="$WRITE_URL?action=displayAuthorityForm&taxon_no=$fields{child_spelling_no}">Edit $childSpellingName $authors</a></li>
  <br><li><a href="$WRITE_URL?action=displayTaxonomicNamesAndOpinions&display=authorities&reference_no=$resultReferenceNumber">Edit a name from the same reference</a></li>
  <br><li><a href="$WRITE_URL?action=displayAuthorityTaxonSearchForm&use_reference=new">Add/edit another taxon from another reference</a></li>
  <br><li><a href="$READ_URL?action=checkTaxonInfo&taxon_no=$fields{child_no}">Get general information about $childName</a></li>   
  </ul>
</td>
<td valign="top" class="small">
  <p class="large" style="margin-left: 2em;">Opinion functions</p>
  <ul>
  <li><a href="$WRITE_URL?action=displayOpinionSearchForm">Add/edit opinion about another taxon</a></li>
  <li$style><a href="$WRITE_URL?action=displayOpinionForm&opinion_no=$resultOpinionNumber">Edit this opinion</a></li>
  <li$style><a href="$WRITE_URL?action=displayOpinionChoiceForm&taxon_no=$fields{child_spelling_no}">Edit another opinion about $childSpellingName</a></li>
  <li$style><a href="$WRITE_URL?action=displayOpinionForm&opinion_no=-1&child_spelling_no=$fields{child_spelling_no}&child_no=$fields{child_no}">Add another opinion about $childSpellingName</a></li>
  <li$style><a href="$WRITE_URL?action=displayTaxonomicNamesAndOpinions&display=opinions&reference_no=$resultReferenceNumber">Edit an opinion from the same reference</a></li>
  <li$style><a href="$WRITE_URL?action=displayOpinionSearchForm&use_reference=new">Add/edit opinion about another taxon from another reference</a></li>
  <li$style><a href="$WRITE_URL?action=startProcessPrintHierarchy&reference_no=$resultReferenceNumber&maximum_levels=100">Print this reference's classification</a></li>
  </ul>
</td></tr></table>
</p>
</div>
</div>|;

    # See Taxon::displayTypeTaxonSelectForm for details
    Taxon::displayTypeTaxonSelectForm($dbt,$s,$fields{'type_taxon'},$fields{'child_no'},$childName,$childRank,$resultReferenceNumber,$end_message);
}

# row is an opinion database row and must contain the following fields:
#   child_no,status,child_spelling_no,parent_spelling_no,opinion_no
sub resetOriginalNo{
    my ($dbt,$new_orig_no,$row) = @_;
    my $dbh = $dbt->dbh;
    return unless $new_orig_no;
    
    my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$new_orig_no});
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
    my $sql = "UPDATE opinions SET modified=modified,spelling_reason='$newSpellingReason',child_no=$new_orig_no  WHERE opinion_no=$row->{opinion_no}";
    dbg("Migrating child: $sql");
    $dbh->do($sql);
}

# Gets a list of opinions that will be moved from a spelling to an original name.  Made into
# its own function so we can prompt the user before the move actually happens to make
# sure they're not making a mistake. The exclude_opinion_no is passed so we exclude the
# current opinion in the migration, which will only happen on an edit
sub getOpinionsToMigrate {
    my ($dbt,$child_no,$child_spelling_no,$exclude_opinion_no) = @_;


    my $sql = "SELECT * FROM opinions WHERE ((child_no=".$child_no." AND (parent_no=".$child_spelling_no." OR parent_spelling_no=".$child_spelling_no.")) OR (child_no=".$child_no." AND (parent_no=".$child_spelling_no." OR parent_spelling_no=".$child_spelling_no."))) AND status!='misspelling of'";
    if ($exclude_opinion_no =~ /^\d+$/) {
        $sql .= " AND opinion_no != $exclude_opinion_no";
    }
    my @results = @{$dbt->getData($sql)};
    if ( @results )	{
        return ([],[],$results[0]->{'status'});
    }
 
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$child_spelling_no);
    $sql = "SELECT * FROM opinions WHERE child_no=$orig_no";
    if ($exclude_opinion_no =~ /^\d+$/) {
        $sql .= " AND opinion_no != $exclude_opinion_no";
    }
    @results = @{$dbt->getData($sql)};
  
    my @parents = ();

    # there is a potential bizarre case where child_spelling_no has been
    #  used as a parent_no, but it completely unclassified itself, so we
    #  need to add it to the list of parents to be moved JA 12.6.07
    if ( ! @results && $child_no != $orig_no )	{
        $sql = "SELECT count(*) c FROM opinions WHERE parent_no=$orig_no";
        my $count = ${$dbt->getData($sql)}[0]->{c};
        if ( $count > 0 )	{
            push @parents , $orig_no;
        }
    }

    my @opinions = ();
    foreach my $row (@results) {
        if ($row->{'child_no'} != $child_no) {
            push @opinions, $row;
            if ($row->{'status'} eq 'misspelling of') {
                if ($row->{'parent_spelling_no'} =~ /^\d+$/) {
                    push @parents,$row->{'parent_spelling_no'};
                }
            }
            if ($row->{'child_spelling_no'} =~ /^\d+$/) {
                push @parents,$row->{'child_spelling_no'};
            }
            if ($row->{'child_no'} =~ /^\d+$/) {
                push @parents,$row->{'child_no'};
            }
        }
    }

    return (\@opinions,\@parents);
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
    my $sepkoski;
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
        print "<p class=\"pageTitle\">Which opinion about ".$t->taxonNameHTML()." do you want to edit?</p>\n";
        
	    print qq|<div class="displayPanel" style="padding: 1em;">\n|;
        print qq|<div align="left"><ul>|;
        foreach my $row (@results) {
            my $o = Opinion->new($dbt,$row->{'opinion_no'});
            my ($opinion,$authority) = $o->formatAsHTML('return_array'=>1);
            if ( $o->{'reference_no'} != 6930 )	{
                print qq|<li><a href="$WRITE_URL?action=displayOpinionForm&amp;child_no=$orig_no&amp;child_spelling_no=$child_no&amp;opinion_no=$row->{opinion_no}">$opinion</a>$authority</li>|;
            } else	{
                print qq|<li>$opinion $authority*</li>|;
                $sepkoski = qq|<br>\n*Opinions from Sepkoski's Compendium cannot be edited.|;
            }
        }
        print qq|<li><a href="$WRITE_URL?action=displayOpinionForm&amp;child_no=$orig_no&amp;child_spelling_no=$child_no&amp;opinion_no=-1">Create a <b>new</b> opinion record</a></li>|;
        print qq|</ul></div>\n|;
#        print qq|<tr><td align="center" colspan=2><p><input type=submit value="Submit"></p><br></td></tr>|;
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
        if ($q->param("enterer_reversed")) {
            my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('enterer_reversed')));
            my $enterer_no = ${$dbt->getData($sql)}[0]->{'person_no'};
            if (!$enterer_no) {
                push @errors, $q->param('enterer_reversed')." is not a valid enterer. Format like 'Sepkoski, J.'" if (!$enterer_no); 
            } else {
                push @where, "o.enterer_no=".$enterer_no; 
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
            my $sql = "SELECT o.opinion_no,o.reference_no,o.ref_has_opinion FROM opinions o ".
                      " LEFT JOIN authorities a ON a.taxon_no=o.child_no".
                      $join_refs.
                      " WHERE ".join(" AND ",@where).
                      " ORDER BY a.taxon_name ASC";
            my @results = @{$dbt->getData($sql)};
            if (scalar(@results) == 0) {
                print "<div align=\"center\"<p class=\"pageTitle\">No opinions found</p></div><br><br>";
                return;
            }
            print "<div align=\"center\">";
            if ($s->isDBMember())	{
                print "<p class=\"pageTitle\">Select an opinion to edit</p>\n";
            } else	{
                print "<p class=\"pageTitle\">Opinions from ".Reference::formatShortRef($dbt,$q->param("reference_no"))."</p>\n";
            }

            print qq|<div class="displayPanel" style="padding: 1em; margin-left: 2em; margin-right: 2em;">\n|;
            print qq|<div class="small" align="left">|;
            print qq|<ul>|;
            foreach my $row (@results) {
                my $o = Opinion->new($dbt,$row->{'opinion_no'});
                my ($opinion,$authority) = $o->formatAsHTML('return_array'=>1);
                if ( $q->param('reference_no') == $row->{'reference_no'} && $row->{'ref_has_opinion'} eq "YES" )	{
                    $authority = "";
                }
                if ($s->isDBMember())	{
                    print "<li><a href=\"$WRITE_URL?action=displayOpinionForm&amp;opinion_no=$row->{opinion_no}\">$opinion</a>$authority</li>\n";
                } else	{
                    print "<li>$opinion $authority</li>\n";
                }
            }
            print "</ul>";
            print "</div>";
            print "</div>";
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
        print qq|<div class="verysmall" style="margin-left: 4em; text-align: left;"><p>An "opinion" is when an author classifies or synonymizes a taxon.<br>\nCreate a new opinion if your author's name is not in the above list.<br>\nDo not select an old opinion unless it was entered incorrectly or incompletely.$sepkoski</p></div>\n|;
    } elsif ($q->param('reference_no') && $s->isDBMember())	{
        print qq|<tr><td align="left" colspan=2><div class="tiny" style="padding-left: 8em;"><p>An "opinion" is when an author classifies or synonymizes a taxon.<br>|;
        print qq|You may want to read the <a href="javascript:tipsPopup('/public/tips/taxonomy_FAQ.html')">FAQ</a>.</p></div>\n|;
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
    my ($dbt,$s,$child_no,$resultOpinionNumber,$debug_only) = @_;
    my $dbh = $dbt->dbh;
    return if !($child_no);
    my $sql = "SELECT * FROM opinions WHERE child_no=$child_no AND child_no != parent_no AND status !='misspelling of'";
    my @results = @{$dbt->getData($sql)};
    my %dupe_hash = ();
    # "Reverse" prevents a bug where we delete teh last entered opinion (if its a dupe)
    # which causes the scripts to crash later. So delete the earlier entered dupe opinion
    foreach my $row (reverse @results) {
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
    my $newNo = $resultOpinionNumber;
    while (my ($key,$array_ref) = each %dupe_hash) {
        my @opinions = @$array_ref;
        if (scalar(@opinions) > 1) {
            my $orig_row = shift @opinions;
            foreach my $row (@opinions) {
                dbg("Found duplicate row for $orig_row->{opinion_no} in $row->{opinion_no}");
                $dbt->deleteRecord($s,'opinions','opinion_no',$row->{'opinion_no'},"Deleted by Opinion::removeDuplicateOpinion, duplicates $orig_row->{opinion_no}");
                if ( $orig_row->{'opinion_no'} != $resultOpinionNumber )	{
                    $newNo = $orig_row->{'opinion_no'};
                }
            }
        }
    }
    return($newNo);
}


1;


