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
use DBConnection;
use DBTransactionManager;
use Errors;
use Data::Dumper;
use CGI::Carp;
use URI::Escape;
use Mail::Mailer;
use TaxaCache;

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
    my $dbh = $dbt->dbh;
	
	my %fields;  # a hash of fields and values that
				 # we'll pass to HTMLBuilder to pop. the form.
				 
	
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

        if ($fields{'ref_is_authority'} =~ /YES/i) {
            $fields{'ref_is_authority'} = 'PRIMARY';
        } else {
            $fields{'ref_is_authority'} = 'NO';
            $fields{'2nd_pages'} = $fields{'pages'};
            $fields{'2nd_figures'} = $fields{'figures'};
            $fields{'pages'} = '';
            $fields{'figures'} = '';
        }  

    } else { # brand new, first submission
        $fields{'taxon_name'} = $q->param('taxon_name');
        $fields{'reference_no'} = $s->get('reference_no');
        # to speed things up, assume that the primary (current) ref is the
        #  authority when the taxon is new JA 29.8.06
        $fields{'ref_is_authority'} = 'PRIMARY';
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
        $fields{'current_reference'} = 1;
    } 
	

	# If this taxon is a type taxon for something higher, mark the check box as checked
    if (!$isNewEntry && !$reSubmission && $fields{'taxon_rank'} =~ /species/) {
        my $lookup_reference = "";
        if ($q->param('ref_is_authority') eq 'CURRENT') {
            $lookup_reference = $s->get('reference_no');
        } else {
            $lookup_reference = $fields{'reference_no'};
        } 
        my @taxa = getTypeTaxonList($dbt,$fields{'taxon_no'},$lookup_reference);
        $fields{'type_taxon'} = 0;
        foreach my $row (@taxa) {
            if ($row->{'type_taxon_no'} == $fields{'taxon_no'}) {
                $fields{'type_taxon'} = 1;
            }
        }  
    }
    $fields{'type_taxon_checked'} = ($fields{'type_taxon'}) ? 'CHECKED' : '';
	
	# Now we need to deal with the taxon rank select menu.
	# If we've already displayed the form and the user is now making changes
	# from an error message, then we should use the rank they chose on the last form.
	# Else, if it's the first display of the form, then we use the rank from the database
	# if it's an edit of an old record, or we use the rank from the spacing of the name
	# they typed in if it's a new record.
	
	if ($reSubmission) {
		$fields{'taxon_rank'} = $q->param('taxon_rank'); 
	} else { 
		# first time
		if ($isNewEntry) {
	        # Figure out the rank based on spacing of the name.
			$fields{'taxon_rank'} = guessTaxonRank($q->param('taxon_name'));
		} else {
			# not a new entry
			$fields{'taxon_rank'} = $t->get('taxon_rank');
		}
	}
	
	# remove the type taxon stuff, it'll be assigned in opinions
	if ($fields{'taxon_rank'} =~ /species/) {
		$fields{'show_type_taxon'} = 1;
		$fields{'show_type_specimen'} = 1;
    }
	
	## If this is a new species or subspecies, then we will automatically
	# create an opinion record with a state of 'belongs to'.  However, we 
	# have to make sure that we use the correct parent taxon if we have multiple
	# ones in the database.  For example, if they enter a  new taxon named
	# 'Equus newtaxon' and we have three entries in authorities for 'Equus'
	# then we should present a menu and ask them which one to use.

    my $parent_no; my @parents;
	if ($fields{'taxon_rank'} =~ /subspecies|species|subgenus/) {
		my @bits = split(/ /,$fields{'taxon_name'});
        pop @bits;
	    my $parentName = join(" ",@bits);	
		
        my $parentRank = guessTaxonRank($parentName);
        if (!$parentRank) { 
            $parentRank = 'genus';
        }
        @parents = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$parentName,'taxon_rank'=>$parentRank},['*']);
		
		if (@parents) {
			my $select;
            # if only one record, then we don't have to ask the user anything.
            # otherwise, we should ask them to pick which one.
            my @parent_nos = ();
            my @parent_descs = ();
			foreach my $row (@parents) {
                push @parent_nos, $row->{'taxon_no'};	
                push @parent_descs, formatTaxon($dbt,$row);
			}
            if (@parents == 1) {
                $parent_no = $parents[0]->{'taxon_no'};
            } else {
                if ($fields{'taxon_no'}) {
                    my $parent_nos = join ",",map{$_->{'taxon_no'}} @parents;
                    my $sql = "SELECT DISTINCT parent_spelling_no FROM opinions WHERE child_spelling_no=$fields{taxon_no} AND parent_spelling_no IN ($parent_nos)";
                    my @selected = @{$dbt->getData($sql)};
                    if (@selected == 1) {
                        $parent_no = $selected[0]->{'parent_spelling_no'};
                    }
                }
            }
			
			$fields{'parent_taxon_select'} = "<b>Belongs to:</b> ".
                $hbo->htmlSelect('parent_taxon_no',\@parent_descs,\@parent_nos,$parent_no);
		} else {
			# count = 0, so we need to warn them to enter the parent taxon first.
#	        my $errors = Errors->new();
#			$errors->add("The $parentRank '$parentName' for this $fields{'taxon_rank'} doesn't exist in our database.  Please <A HREF=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_name=$parentName\">create a new authority record for '$parentName'</A> before trying to add this $fields{'taxon_rank'}.");
#            print $errors->errorMessage();
#            return;
		}
	}


    my @taxon_ranks;
    if ($fields{'taxon_rank'} =~ /genus/) {
        @taxon_ranks = ('subgenus','genus');
    } elsif ($fields{'taxon_rank'} =~ /species/) {
        @taxon_ranks = ('subspecies','species');
    } else {
        @taxon_ranks = grep {!/^\s*$|species|subgenus/} $hbo->getList('taxon_rank');
    }
    $fields{'taxon_rank_select'} = $hbo->htmlSelect('taxon_rank',\@taxon_ranks,\@taxon_ranks,$fields{'taxon_rank'}); 

    

    # Build extant select
    my @extant_values = ('','YES','NO');
    $fields{'extant_select'} = $hbo->htmlSelect('extant',\@extant_values,\@extant_values,$fields{'extant'});
    
    # add in the error message
    if ($error_message) {
        $fields{'error_message'}=$error_message;
    }
	
	# print the form	
    my $html = $hbo->populateHTML("add_enter_authority", \%fields);
    
	## Make the taxon_name non editable if this is a new entry to simplify things
	if ($isNewEntry) {
		$html =~ s/<input type="input" name="taxon_name" value="(.*?)">/$1<input type="hidden" name="taxon_name" value="$1">/;
	}
    print $html;
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
    my ($dbt,$hbo,$s,$q) = @_;
    my $dbh = $dbt->dbh;

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
	my %fields;
	
	if ($isNewEntry) {
		$fields{'reference_no'} = $s->get('reference_no');
		if (! $fields{'reference_no'} ) {
			$errors->add("You must set your current reference before submitting a new authority");	
		}
        $fields{'type_taxon'} = ($q->param('type_taxon')) ? 1 : 0;
	} 
	
	if (($q->param('ref_is_authority') ne 'PRIMARY') && 
	    ($q->param('ref_is_authority') ne 'CURRENT') && 
		($q->param('ref_is_authority') ne 'NO')) {
		$errors->add("You must choose one of the reference radio buttons");
	} elsif ($q->param('ref_is_authority') eq 'NO') {
        # merge the pages and 2nd_pages, figures and 2nd_figures fields together
        # since they are one field in the database.
		$fields{'pages'} = $q->param('2nd_pages');
		$fields{'figures'} = $q->param('2nd_figures');

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
       
        my $lookup_reference = "";
        if ($q->param('ref_is_authority') eq 'CURRENT') {
            $lookup_reference = $s->get('reference_no');
        } else {
            $lookup_reference = $fields{'reference_no'};
        }  
			
		if ($q->param('pubyr')) {
            my $pubyr = $q->param('pubyr');

			if (! Validation::properYear( $pubyr ) ) {
				$errors->add("The year is improperly formatted");
			}
			
			# make sure that the pubyr they entered (if they entered one)
			# isn't more recent than the pubyr of the reference.  
			my $ref = Reference->new($dbt,$lookup_reference);
			if ($ref && $pubyr > $ref->get('pubyr')) {
				$errors->add("The publication year ($pubyr) can't be more 
				recent than that of the primary reference (" . $ref->get('pubyr') . ")");
			}
		}
        if ($q->param('taxon_rank') =~ /species|subgenus/) {
            if (!$q->param('author1last')) {
                $errors->add("If entering a subgenus, species, or subspecies, enter at least the last name of the first author");
            }
            if (!$q->param('pubyr')) {
                $errors->add("If entering a subgenus, species, or subspecies, the publication year is required");
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


    if (! validTaxonName($q->param('taxon_name'))) {
        $errors->add("The taxon's name is invalid; please check spacing and capitalization");	
    }

    if (!$isNewEntry) {
        my $old_name = $t->get('taxon_name');
        my $new_name = $q->param('taxon_name');
        if ($old_name ne $new_name) {
            my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$new_name});
            if ($taxon) {
                $errors->add("Can't change the taxon's name from '$old_name' to '$new_name' because '$new_name' already exists in the database");
            }
        }
    }
    
    my $rankFromSpaces = guessTaxonRank($q->param('taxon_name'));
    if (($rankFromSpaces eq 'subspecies' && $q->param('taxon_rank') ne 'subspecies') ||
        ($rankFromSpaces eq 'species' && $q->param('taxon_rank') ne 'species') ||
        ($rankFromSpaces eq 'subgenus' && $q->param('taxon_rank') ne 'subgenus') ||
        ($rankFromSpaces eq ''     && $q->param('taxon_rank') =~ /subspecies|species|subgenus/)) {
        $errors->add("The selected rank '".$q->param('taxon_rank')."' doesn't match the spacing of the taxon name '".$q->param('taxon_name')."'");
    }
	
	foreach my $formField ($q->param()) {
        # if the value isn't already in our fields to enter
        if (! $fields{$formField}) {
            $fields{$formField} = $q->param($formField);
        }
	}


	$fields{'taxon_name'} = $q->param('taxon_name');
	
	# correct the ref_is_authority field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
    if ($q->param('ref_is_authority') =~ /PRIMARY|CURRENT/) {
        $fields{'ref_is_authority'} = 'YES';
    } elsif ($q->param('ref_is_authority') eq 'NO') {
        $fields{'ref_is_authority'} = '';
    }
       
	# If the rank was species or subspecies, then we also need to insert
	# an opinion record automatically which has the state of "belongs to"
	# For example, if the child taxon is "Equus blah" then we need to 
	# make sure we have an opinion that it belongs to "Equus".
	#
	my $parent_no;
	if ($q->param('taxon_rank') =~ /^species|^subspecies|^subgenus/) {
        my @bits = split(/ /,$fields{'taxon_name'});
        pop @bits;
        my $parent_name = join(" ",@bits);
        if ($q->param('parent_taxon_no')) {
		    my $parent = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$q->param('parent_taxon_no')});
            if ($parent->{'taxon_name'} eq $parent_name) {
                $parent_no=$q->param('parent_taxon_no');
            } 
        }
        if (!$parent_no) {
            my @parents = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$parent_name});
            if (@parents > 1) {
                $errors->add("The taxon '$parent_name' exists multiple times in our database.  Please select the version you mean.");
            } elsif (@parents == 1) {
                $parent_no = $parents[0]->{'taxon_no'};
            } else {
                $errors->add("The parent taxon '$parent_name' that this taxon belongs to doesn't exist in our database.  Please add an authority record for this '$parent_name' before continuing.");
            }
        } 
	}
	## end of hack
	####
	
	# at this point, we should have a nice hash array (%fields) of
	# fields and values to enter into the authorities table.
	
	
	# *** NOTE, if they try to enter a record which has the same name and
	# taxon_rank as an existing record, we should display a warning page stating
	# this fact..  However, if they *really* want to submit a duplicate, we should 
	# let them.  
	#
	# This only applies to new entries, and to edits where they changed the taxon_name
	# field to be the name of a different taxon which already exists.
	if ($q->param('confirmed_taxon_name') ne $q->param('taxon_name')) {
        my @taxon = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$fields{'taxon_name'}},['*']);
        my $taxonExists = scalar(@taxon);
        
		if (($isNewEntry && $taxonExists) ||
		    (!$isNewEntry && $taxonExists && $q->param('taxon_name') ne $t->get('taxon_name'))) {
            my @pub_info = ();
            my %ranks = ();
            foreach my $row (@taxon) {
                $ranks{$row->{'taxon_rank'}} = 1;
            }
            my $different_ranks = scalar(keys(%ranks));
            foreach my $row (@taxon) {
                my $pub_info = Reference::formatShortRef($row);
                if ($different_ranks > 1) {
                    $pub_info .=" ($row->{taxon_rank})";
                }
                push @pub_info, $pub_info;
            }
            my $plural = ($taxonExists == 1) ? "" : "s";
            $q->param('confirmed_taxon_name'=>$q->param('taxon_name'));
			$errors->add("This taxonomic name already appears $taxonExists time$plural in the database: ".join(", ",@pub_info).". If this record is a homonym and you want to create a new record, hit submit again. If its a rank change, just enter an opinion based on the existing taxon that uses the new rank and it'll be automatically created.");
		}
	}
	
	if ($errors->count() > 0) {
        # If theres an error message, then we know its the second time through
		my $message = $errors->errorMessage();
		displayAuthorityForm($dbt,$hbo, $s, $q, $message);
		return;
	}

    # Replace the reference with the current reference if need be
    if ($q->param('ref_is_authority') =~ /CURRENT/ && $s->get('reference_no')) {
        $fields{'reference_no'} = $s->get('reference_no');
    }  
	
	# now we'll actually insert or update into the database.
	my $resultTaxonNumber;
    my $resultReferenceNumber = $fields{'reference_no'};
    my $status;
	
	if ($isNewEntry) {
		($status, $resultTaxonNumber) = $dbt->insertRecord($s,'authorities', \%fields);
        TaxaCache::addName($dbt,$resultTaxonNumber);
		
		if ($parent_no) {
            addImplicitChildOpinion($dbt,$s,$resultTaxonNumber,$parent_no,\%fields);
            #TaxaCache::updateCache($dbt,$resultTaxonNumber);
            #TaxaCache::markForUpdate($dbt,$resultTaxonNumber);
		}
	} else {
		# if it's an old entry, then we'll update.
		$resultTaxonNumber = $t->get('taxon_no');
		$status = $dbt->updateRecord($s,'authorities','taxon_no',$resultTaxonNumber, \%fields);
        propagateAuthorityInfo($dbt,$resultTaxonNumber,1);
        # Changing a genus|subgenus|species|subspecies is tricky since we have to change
        # other related opinions and authorities
        if ($t->get('taxon_name') ne $fields{'taxon_name'} &&
            $t->get('taxon_rank') =~ /^genus|^subgenus|^species/){
            updateChildNames($dbt,$s,$t->get('taxon_no'),$t->get('taxon_name'),$fields{'taxon_name'});
        }
        updateImplicitBelongsTo($dbt,$s,$t->get('taxon_no'),$parent_no,$t->get('taxon_name'),$fields{'taxon_name'},\%fields);
	}

    my $pid = fork();
    if (!defined($pid)) {
        carp "ERROR, could not fork";
    }

    if ($pid) {
        $dbh = DBConnection::connect();
        $dbt = DBTransactionManager->new($dbh);
        # Child fork
    } else {
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
        TaxaCache::updateCache($dbt2,$resultTaxonNumber);
        # The main connection gets closes when this fork exits (either fork exiting will kill it)
        # so sleep for a while to give enough time for the other fork to finish up with the DB
        sleep(4);
        exit;
    }

    # JA 2.4.04
    # if the taxon name is unique, find matches to it in the
    #  occurrences table and set the taxon numbers appropriately
    if ($status && ($isNewEntry || ($t->get('taxon_name') ne $fields{'taxon_name'}))) {
        my @set_warnings = setOccurrencesTaxonNoByTaxon($dbt,$s->get('authorizer_no'),$resultTaxonNumber);
        push @warnings, @set_warnings;
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
	
        my $end_message;
        if (@warnings) {
            $end_message .= Debug::printWarnings(\@warnings);
        }
        $end_message .= "<div align=\"center\"><p><b>" . $fields{'taxon_name'} . " " .Reference::formatShortRef(\%fields). " has been $enterupdate the database</b></p></div>";

        my $origResultTaxonNumber = TaxonInfo::getOriginalCombination($dbt,$resultTaxonNumber);
        
        $end_message .= qq|
    <div align="center" class="displayPanel">
    <table cellpadding="10" class="small"><tr><td>
      <p><b>Name functions</b></p>
      <li><b><a href="bridge.pl?action=displayAuthorityForm&taxon_no=$resultTaxonNumber">Edit $fields{taxon_name}</a></b></li>
      <br><li><b><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$resultTaxonNumber">Get general information about $fields{taxon_name}</a></b></li>   
      <br><li><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit a name from the same reference</a></b></li>
      <br><li><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm">Add/edit another taxon</a></b></li>
      <br><li><b><a href="bridge.pl?action=displayAuthorityTaxonSearchForm&use_reference=new">Add/edit another taxon from another reference</a></b></li>
    </td>
    <td valign=top>
      <p><b>Opinion functions</b></p>|;
        if ($fields{'taxon_rank'} =~ /species/) {
            $end_message .= qq|<li><b><a href="bridge.pl?action=displayOpinionForm&opinion_no=-1&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber&use_reference=new">Add an opinion about $fields{taxon_name} from another reference</a></b></li>|;
        } else {
            $end_message .= qq|<li><b><a href="bridge.pl?action=displayOpinionForm&opinion_no=-1&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber">Add an opinion about $fields{taxon_name}</a></b></li>|;
        }
        $end_message .= qq|<br><li><b><a href="bridge.pl?action=displayOpinionChoiceForm&taxon_no=$resultTaxonNumber">Edit an opinion about $fields{taxon_name}</a></b></li>
      <br><li><b><a href="bridge.pl?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit an opinion from the same reference</a></b></li>
      <br><li><b><a href="bridge.pl?action=displayOpinionSearchForm">Add/edit opinion about another taxon</a></b></li>
      <br><li><b><a href="bridge.pl?action=displayOpinionSearchForm&use_reference=new">Add/edit opinion about another taxon from another reference</a></b></li>
    </td></tr></table>
    </div>|;

        displayTypeTaxonSelectForm($dbt,$s,$fields{'type_taxon'},$resultTaxonNumber,$fields{'taxon_name'},$fields{'taxon_rank'},$resultReferenceNumber,$end_message);
	}
	
	print "<BR>";
	print "</CENTER>";
}

sub updateChildNames {
    my ($dbt,$s,$old_taxon_no,$old_name,$new_name) = @_;
    return if ($old_name eq $new_name || !$old_name);
    main::dbg("UPDATE CHILD NAMES CALLED WITH: $old_name --> $new_name");

    # Get only the common denominator.  I.E. is a subgenus
    # in one but not the other, just change the genus part if aplicable
    my $old_rank = guessTaxonRank($old_name) || 'genus';
    my $new_rank = guessTaxonRank($new_name) || 'genus';
    # Sort of punk on this for now
    return unless $new_rank eq $old_rank;

    my @q = ($old_taxon_no);
    my %to_change = ();
    while (my $taxon_no = pop @q) {
        my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE parent_spelling_no=$taxon_no";
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            push @q, $row->{'child_spelling_no'};
            $to_change{$row->{'child_spelling_no'}} = 1;
        }
    }
    my $quoted_old_name = quotemeta $old_name;
    foreach my $t (keys %to_change) {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$t});
        my $taxon_name = $child->{'taxon_name'};
        $taxon_name =~ s/^$quoted_old_name/$new_name/; 
        main::dbg("Changing parent from $old_name to $new_name.  child taxon from $child->{taxon_name} to $taxon_name");
        $dbt->updateRecord($s,'authorities','taxon_no',$child->{'taxon_no'},{'taxon_name'=>$taxon_name});
    }
}
sub updateImplicitBelongsTo {
    my ($dbt,$s,$taxon_no,$parent_no,$old_name,$new_name,$fields) = @_;
    return if ($old_name eq $new_name);

    my @old_name = split(/ /,$old_name);
    my @new_name = split(/ /,$new_name);
    my $old_last = pop @old_name;
    my $new_last = pop @new_name;
    my $old_higher = join(" ",@old_name);
    my $new_higher = join(" ",@new_name);

    my %old_parents;
    if ($old_higher) {
        main::dbg("Looking for opinions to migrate for $old_higher");
        foreach my $p (TaxonInfo::getTaxa($dbt,{'taxon_name'=>$old_higher})) {
            $old_parents{$p->{'taxon_no'}} = 1;
        }
    }
    my $sql = "SELECT * FROM opinions WHERE child_spelling_no=$taxon_no";
    my @old_opinions = @{$dbt->getData($sql)};
    #    main::dbg("Found ".scalar(@old_opinions)." existing opinions to migrate for $old_higher");

    if ($new_higher && !$old_higher) {
        # Insert a new opinion, switch from genus --> subgenus
        main::dbg("Inserting belongs to since taxa changed from genus $old_name to subgenus $new_name");
        addImplicitChildOpinion($dbt,$s,$taxon_no,$parent_no,$fields);
        if (@old_opinions) {
            my $subgenus = $new_last;
            $subgenus =~ s/\(|\)//g;
            my ($new_taxon_no) = addSpellingAuthority($dbt,$s,$taxon_no,$subgenus,'genus');
            foreach my $row (@old_opinions) {
                my $changes = {'child_spelling_no'=>$new_taxon_no,'spelling_reason'=>'rank change'};
                $dbt->updateRecord($s,'opinions','opinion_no',$row->{'opinion_no'},$changes);
                my $sql = "SELECT * FROM opinions WHERE parent_spelling_no=$taxon_no";
                foreach my $c (@{$dbt->getData($sql)}) {
                    $dbt->updateRecord($s,'opinions','opinion_no',$c->{'opinion_no'},{'parent_spelling_no'=>$new_taxon_no});
                }
            }
        }
    } 
    if ($old_higher && !$new_higher) {
        # Delete old opinion, switch from subgenus --> genus
        foreach my $row (@old_opinions) {
            if ($old_parents{$row->{'parent_spelling_no'}}) { 
                main::dbg("Deleting belongs to record since taxa changed from $old_name to $new_name");
                $dbt->deleteRecord($s,'opinions','opinion_no',$row->{'opinion_no'},"taxon name changed from $old_name to $new_name");
            }
        }
    }
    if ($old_higher && $new_higher) {
        my $orig_parent_no = TaxonInfo::getOriginalCombination($dbt,$parent_no);
        my $found_old_parent = 0;
        if (@old_opinions) {
            foreach my $row (@old_opinions) {
                # Switch opinion
                if ($old_parents{$row->{'parent_spelling_no'}}) { 
                    $found_old_parent = 1;
                    main::dbg("Updating belongs to since taxa changed from $old_name to $new_name");
                    $dbt->updateRecord($s,'opinions','opinion_no',$row->{opinion_no},{'parent_spelling_no'=>$parent_no,'parent_no'=>$orig_parent_no});
                }
            }
        } 
        if (!$found_old_parent) {
            # Insert new opinion
            main::dbg("Inserting belongs to since taxa changed from $old_name to $new_name");
            addImplicitChildOpinion($dbt,$s,$taxon_no,$parent_no,$fields);
        }
    } 
}

sub addImplicitChildOpinion {
    my ($dbt,$s,$child_no,$parent_no,$fields) = @_;
    return unless ($child_no && $parent_no);
    # Get original combination for parent no PS 04/22/2005
    my $orig_parent_no = TaxonInfo::getOriginalCombination($dbt,$parent_no);
    
    my %opinionHash = (
        'status'=>'belongs to',
        'spelling_reason'=>'original spelling',
        'child_no'=>$child_no,
        'child_spelling_no'=>$child_no,
        'parent_no'=>$orig_parent_no,
        'parent_spelling_no'=>$parent_no,
        'ref_has_opinion'=>$fields->{'ref_is_authority'}
    );
    my @fields = ('reference_no','author1init','author1last','author2init','author2last','otherauthors','pubyr','pages','figures');
    $opinionHash{$_} = $fields->{$_} for @fields;

    $dbt->insertRecord($s,'opinions',\%opinionHash);
}

sub addSpellingAuthority {
    my ($dbt,$s,$taxon_no,$new_name,$new_rank,$reference_no) = @_;

    my $orig = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no},['*']);

    # next we need to steal data from the opinion
    my %record = ();
    $record{taxon_name} = $new_name;
    if (!$reference_no) {
        $record{reference_no} = $orig->{reference_no};
    } else {
        $record{reference_no} = $reference_no;
    }
    if (!$new_rank) {
        $record{taxon_rank} = $orig->{taxon_rank};
    } else {
        $record{taxon_rank} = $new_rank;
    }

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

    my ($return_code, $new_taxon_no) = $dbt->insertRecord($s,'authorities', \%record);
    TaxaCache::addName($dbt,$new_taxon_no);
    main::dbg("create new authority record, got return code $return_code");
    if (!$return_code) {
        die("Unable to create new authority record for $record{taxon_name}. Please contact support");
    }
    my @set_warnings = Taxon::setOccurrencesTaxonNoByTaxon($dbt,$s->get('authorizer_no'),$new_taxon_no);
    return ($new_taxon_no,\@set_warnings);
}


sub setOccurrencesTaxonNoByTaxon {
    my $dbt = shift;
    my $authorizer_no = shift;
    my $dbh = $dbt->dbh;
    my $taxon_no = shift;
    my @warnings = ();

    my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no});
    return if (!$t);

    my $taxon_name = $t->{'taxon_name'};
    my ($genus,$subgenus,$species,$subspecies) = splitTaxon($taxon_name);
    $genus = "" if (!$genus);
    $subgenus = "" if (!$subgenus);
    $species = "" if (!$species);
    $subspecies = "" if (!$subspecies);

    # Don't support resolutioin at the subspecies level, so don't set it for subspecies.
    # If they set a species the taxon_no will equal the species taxon_no already since
    # they have to enter the species first, so this should be ok
    if ($subspecies) {
        return ();
    }

    # start with a test for uniqueness
    my @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$taxon_name},['taxon_no','taxon_rank','taxon_name','author1last','author2last','pubyr']);
    my @taxon_nos= ();
    for (my $i=0;$i<@taxa;$i++) {
        my $orig_no_i = TaxonInfo::getOriginalCombination($dbt,$taxa[$i]->{'taxon_no'});
        my $is_same_taxon = 0;
        for (my $j=$i+1;$j<@taxa;$j++) {
            my $orig_no_j = TaxonInfo::getOriginalCombination($dbt,$taxa[$j]->{'taxon_no'});
            if ($orig_no_j == $orig_no_i) {
                $is_same_taxon = 1;
            }
            if ($taxa[$i]->{'author1last'} && 
                $taxa[$i]->{'author1last'} eq $taxa[$j]->{'author1last'} &&
                $taxa[$i]->{'author2last'} eq $taxa[$j]->{'author2last'} &&
                $taxa[$i]->{'pubyr'} eq $taxa[$j]->{'pubyr'}) {
                $is_same_taxon = 1;
            }
        }
        if (!$is_same_taxon) {
            push @taxon_nos, $taxa[$i]->{'taxon_no'};
        } else {
            main::dbg("Not counting taxa as a homonym, it seems to match a another taxa exactly:".Dumper($taxa[$i]));
        }
    }
    
    if (scalar(@taxon_nos) > 1) {
        my $sql1 = "SELECT p.person_no, p.name, p.email, count(*) cnt FROM occurrences o,person p WHERE o.authorizer_no=p.person_no AND o.taxon_no IN (".join(",",@taxon_nos).") group by p.person_no";
        my $sql2 = "SELECT p.person_no, p.name, p.email, count(*) cnt FROM reidentifications re,person p WHERE re.authorizer_no=p.person_no AND re.taxon_no IN (".join(",",@taxon_nos).") group by p.person_no";
        my @results = @{$dbt->getData($sql1)};
        push @results,@{$dbt->getData($sql2)};
        my %emails = ();
        my %counts = ();
        my %names = ();
        foreach my $row (@results) {
            $names{$row->{'person_no'}} = $row->{'name'};
            $emails{$row->{'person_no'}} = $row->{'email'};
            $counts{$row->{'person_no'}} += $row->{'cnt'};
        }

        while (my ($person_no,$email) = each %emails) {
            my $name = $names{$person_no};
            my $link = "bridge.pl?action=displayCollResults&type=reclassify_occurrence&taxon_name=$taxon_name&occurrences_authorizer_no=$person_no";
            my %headers = ('Subject'=> 'Please reclassify your occurrences','From'=>'alroy');
            if ($ENV{'BRIDGE_HOST_URL'} =~ /paleodb\.org/) {
                if ($email) {
                    $headers{'To'} = $email; 
                } else {
                    # This will happen if email is blank, such as with Sepkoski
                    $headers{'To'} = 'alroy@nceas.ucsb.edu';
                }
            } else {
                # DEBUGGING EMAIL ADDRESS
                $headers{'To'} = 'schroeter@nceas.ucsb.edu';
            }
            my $taxon_count = scalar(@taxon_nos);
            my $occ_count = $counts{$person_no};
            my $body = <<END_OF_MESSAGE;
Dear $name:

This is an automated message from the Paleobiology Database. Please don't reply to this message directly, but rather send replies to John Alroy (alroy\@nceas.ucsb.edu).

This message has been sent to you because the taxonomic name $taxon_name has just been entered into the database, and other taxa with the same name already have been entered. So, we have more than one version. This taxonomic name is tied to $occ_count occurrences and reidentifications you own. We can't be sure which version of the name these records should be tied to, so the records must be manually reclassified to choose between them. 

To fix your records, Please click this link while logged in:
http://paleodb.org/cgi-bin/$link

Or log in, go to the main menu, click "Reclassify occurrences" and enter $taxon_name into the taxon name field.
END_OF_MESSAGE
            my $mailer = new Mail::Mailer;
            $mailer->open(\%headers);
            print $mailer $body; 
            $mailer->close;
        }
        
        # Deal with homonym issue
        # Default behavior changed: leave occurrences classified by default, since whoever entered them in thet first
        # place probably wants them to be classifeid to the exiting taxa in the datbaase.
#        $sql1 = "UPDATE occurrences SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
#        $sql2 = "UPDATE reidentifications SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
#        $dbt->getData($sql1);
#        $dbt->getData($sql2);
        push @warnings, "Since $taxon_name is a homonym, occurrences of it may be incorrectly classified using the wrong homonym.  Please go to \"<a target=\"_BLANK\" href=\"bridge.pl?action=displayCollResults&type=reclassify_occurrence&taxon_name=$taxon_name&occurrences_authorizer_no=".$authorizer_no."\">Reclassify occurrences</a>\" and manually classify <b>all</b> your  occurrences of this taxon.";
    } elsif (scalar(@taxon_nos) == 1) {
        my @matchedOccs = ();
        my @matchedReids = ();
        # Name is unique, so set taxon_nos in the occurrences table
        my @higher_names = ($dbh->quote($genus));
        if ($subgenus) {
            push @higher_names, $dbh->quote($subgenus);
        }
        # Algorithm is as follows:
        # First get all potential matches.  Potential matches means where the species matches, if there is a species
        # and the genus or subgenus of the occurrence/reid matches the genus or subgenus of the authorities table
        # record.  Note a genus can match a subgenus and vice versa as well, so this is pretty fuzzy.  If the new
        # authorities table match is BETTER than the old authorities table match, then replace the taxon_no.  
        # See computeMatchLevel to see how matches are ranked. PS 4/21/2006
        my $sql1 = "SELECT occurrence_no,o.taxon_no,genus_name,subgenus_name,species_name,taxon_name,taxon_rank FROM occurrences o "
                . " LEFT JOIN authorities a ON o.taxon_no=a.taxon_no"
                . " WHERE genus_name IN (".join(", ",@higher_names).")";
        my $sql2 = "SELECT reid_no,re.taxon_no,genus_name,subgenus_name,species_name,taxon_name,taxon_rank FROM reidentifications re "
                . " LEFT JOIN authorities a ON re.taxon_no=a.taxon_no"
                . " WHERE genus_name IN (".join(", ",@higher_names).")";
        my $sql3 = "SELECT occurrence_no,o.taxon_no,genus_name,subgenus_name,species_name,taxon_name,taxon_rank FROM occurrences o "
                . " LEFT JOIN authorities a ON o.taxon_no=a.taxon_no"
                . " WHERE subgenus_name IN (".join(", ",@higher_names).")";
        my $sql4 = "SELECT reid_no,re.taxon_no,genus_name,subgenus_name,species_name,taxon_name,taxon_rank FROM reidentifications re "
                . " LEFT JOIN authorities a ON re.taxon_no=a.taxon_no"
                . " WHERE subgenus_name IN (".join(", ",@higher_names).")";
        if ($species) {
            $sql1 .= " AND species_name LIKE ".$dbh->quote($species);
            $sql2 .= " AND species_name LIKE ".$dbh->quote($species);
            $sql3 .= " AND species_name LIKE ".$dbh->quote($species);
            $sql4 .= " AND species_name LIKE ".$dbh->quote($species);
        }
        my @results1 = @{$dbt->getData($sql1)};
        my @results2 = @{$dbt->getData($sql2)};
        my @results3 = @{$dbt->getData($sql3)};
        my @results4 = @{$dbt->getData($sql4)};
        foreach my $row (@results1,@results2,@results3,@results4) {
#            print "MATCHING: $row->{genus_name} ($row->{subgenus_name}) $row->{species_name} TIED TO  $row->{taxon_name}\n";
            my $old_match_level = 0;
            my $new_match_level = 0;

            # Maybe not necessary to cast these again as variables, but do just
            # to be safe.  PERL subs screw up if you try to pass in an undef var.
            my $occ_genus = $row->{'genus_name'};
            my $occ_subgenus = $row->{'subgenus_name'};
            my $occ_species = $row->{'species_name'};
            $occ_genus = "" if (!$occ_genus);
            $occ_subgenus = "" if (!$occ_subgenus);
            $occ_species = "" if (!$occ_species);
            if ($row->{'taxon_no'}) {
                # The "tied" variables refer to the taxonomic name to which the the occurrence is currently
                # set.  I.E. the taxon_name associated with the taxon_no.
                my ($tied_genus,$tied_subgenus,$tied_species) = splitTaxon($row->{'taxon_name'});
                $tied_genus = "" if (!$tied_genus);
                $tied_subgenus = "" if (!$tied_subgenus);
                $tied_species = "" if (!$tied_species);

                $old_match_level = computeMatchLevel($occ_genus,$occ_subgenus,$occ_species,$tied_genus,$tied_subgenus,$tied_species);
            }
            $new_match_level = computeMatchLevel($occ_genus,$occ_subgenus,$occ_species,$genus,$subgenus,$species);
            if ($new_match_level > $old_match_level) {
                if ($row->{'reid_no'}) { 
                    push @matchedReids, $row->{'reid_no'};
                } else {
                    push @matchedOccs, $row->{'occurrence_no'};
                }
            }
        }

        # Compose final SQL
        if (@matchedOccs) {
            my $sql = "UPDATE occurrences SET modified=modified,taxon_no=$taxon_no WHERE occurrence_no IN (".join(",",@matchedOccs).")";
            main::dbg("Updating matched occs:".$sql);
            $dbh->do($sql);
        }
        if (@matchedReids) {
            my $sql = "UPDATE reidentifications SET modified=modified,taxon_no=$taxon_no WHERE reid_no IN (".join(",",@matchedReids).")";
            main::dbg("Updating matched reids:".$sql);
            $dbh->do($sql);
        }
    }
    return @warnings;
}

# This section handles updating of the type_taxon_no field in the authorities table and is used both
# when entering subspecies/species in the authorities form, and entering opinions in the opinions form
# Behavior is:
#  Find out how many possible higher taxa this taxon can be a type for:
#    if its 0: this is bad, it should always be 1 unless the entering of the opinion was botched
#    if its 1: do the insertion or deletion on the spot
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
                print "<input type=\"checkbox\" name=\"taxon_no\" value=\"$row->{taxon_no}\" $checked> ";
                print "$row->{taxon_name} ($row->{taxon_rank})";
                if ($row->{'type_taxon_no'} && $row->{'type_taxon_no'} != $type_taxon_no) {
                    print " - <small>type taxon currently $row->{type_taxon_name} ($row->{type_taxon_rank})</small>";
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
                push @warnings,"Can't set this as the type taxon for authority $parents[0]->{taxon_name}";
            }
        } else {
            my $sqlr = "SELECT author1init,author1last,author2init,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$reference_no";
            my $formatted_ref = Reference::formatShortRef(${$dbt->getData($sqlr)}[0]);
            push @warnings, "Can't set this as the type taxon because no valid higher taxa were found.  There must be opinions linking this taxon to its higher taxa from the same reference ($formatted_ref). If this is a problem, email the admin (pbdbadmin\@nceas.ucsb.edu).";
            carp "Maybe something is wrong in the opinions script, got no parents for current taxon after adding an opinion.  (in section dealing with type taxon). Vars: tt_no $type_taxon_no ref $reference_no tt_name $type_taxon_name tt_rank $type_taxon_rank"; 
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
        print Debug::printWarnings(\@warnings);
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
        print Debug::printWarnings(\@warnings);
    }

    print $end_message;
}
    
# This function returns an array of potential higher taxa for which the focal taxon can be a type.
# The array is an array of hash refs with the following keys: taxon_no, taxon_name, taxon_rank, type_taxon_no, type_taxon_name, type_taxon_rank
sub getTypeTaxonList {
    my $dbt = shift;
    my $type_taxon_no = shift;   
    my $reference_no = shift;
            
    my $focal_taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$type_taxon_no});
            
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
        my $parent_taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$parent->{'taxon_no'}},['taxon_no','type_taxon_no','authorizer_no']);
        $parent->{'authorizer_no'} = $parent_taxon->{'authorizer_no'};
        $parent->{'type_taxon_no'} = $parent_taxon->{'type_taxon_no'};
        if ($parent->{'type_taxon_no'}) {
            my $type_taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$parent->{'type_taxon_no'}});
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
#   I.E. data from getTaxa($dbt,{'taxon_name'=>$taxon_name},['*']) -- see function for details
# 
# it returns some HTML to display the authority information.
sub formatTaxon{
    my $dbt = shift;
    my $taxon = shift;
    my %options = shift;
	my $authLine;

	# Print the name
	# italicize if genus or species.
	if ( $taxon->{'taxon_rank'} =~ /subspecies|species|genus/) {
        if ($options{'no_html'}) {
            $authLine .= ", $taxon->{taxon_rank}";
        } else {
		    $authLine .= "<i>" . $taxon->{'taxon_name'} . "</i>";
        }
	} else {
		$authLine .= $taxon->{'taxon_name'};
        if ($taxon->{'taxon_rank'} && $taxon->{'taxon_rank'} !~ /unranked clade/) {
            $authLine .= ", $taxon->{taxon_rank}";
        }
	}

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
    my $is_recomb = ($orig_no == $taxon->{'taxon_no'}) ? 0 : 1;
	# If the authority is a PBDB ref, retrieve and print it
    my $pub_info = Reference::formatShortRef($taxon,'is_recombination'=>$is_recomb);
    if ($pub_info !~ /^\s*$/) {
        $authLine .= ', '.$pub_info;
    }

	# Print name of higher taxon JA 10.4.03
	# Get the status and parent of the most recent opinion
    my %master_class=%{TaxaCache::getParents($dbt, [$taxon->{'taxon_no'}],'array_full')};

    my @parents = @{$master_class{$taxon->{'taxon_no'}}};
    if (@parents) {
        $authLine .= " [";
        my $foundParent = 0;
        foreach (@parents) {
            if ($_->{'taxon_rank'} =~ /^(?:family|order|class)$/) {
                $foundParent = 1;
                $authLine .= $_->{'taxon_name'}.", ";
                last;
            }
        }
        $authLine =~ s/, $//;
        if (!$foundParent) {
            $authLine .= $parents[0]->{'taxon_name'};
        }
        $authLine .= "]";
    } else {
        $authLine .= " [unclassified";
        if ($taxon->{taxon_rank} && $taxon->{taxon_rank} !~ /unranked/) {
            $authLine .= " $taxon->{taxon_rank}";
        }
        $authLine .= "]";
    }

	return $authLine;
}


sub splitTaxon {
    my $name = shift;
    my ($genus,$subgenus,$species,$subspecies) = ("","","","");
  
    if ($name =~ /^([A-Z][a-z]+)(?:\s\(([A-Z][a-z]+)\))?(?:\s([a-z.]+))?(?:\s([a-z.]+))?/) {
        $genus = $1 if ($1);
        $subgenus = $2 if ($2);
        $species = $3 if ($3);
        $subspecies = $4 if ($4);
    }

    if (!$genus && $name) {
        # Loose match, capitalization doesn't matter. The % is a wildcard symbol
        if ($name =~ /^([a-z%]+)(?:\s\(([a-z%]+)\))?(?:\s([a-z.]+))?(?:\s([a-z.]+))?/) {
            $genus = $1 if ($1);
            $subgenus = $2 if ($2);
            $species = $3 if ($3);
            $subspecies = $4 if ($4);
        }
    }
    
    return ($genus,$subgenus,$species,$subspecies);
}

sub guessTaxonRank {
    my $taxon = shift;
    
    if ($taxon =~ /^[A-Z][a-z]+ (\([A-Z][a-z]+\) )?[a-z\.]+ [a-z\.]+$/) {
        return "subspecies";
    } elsif ($taxon =~ /^[A-Z][a-z]+ (\([A-Z][a-z]+\) )?[a-z.]+$/) {
        return "species";
    } elsif ($taxon =~ /^[A-Z][a-z]+ \([A-Z][a-z]+\)$/) {
        return "subgenus";
    } 

    return "";
}  

sub validTaxonName {
    my $taxon = shift;
    
    if ($taxon =~ /[()]/) {
        if ($taxon =~ /^[A-Z][a-z]+ \([A-Z][a-z]+\)( [a-z]+){0,2}$/) {
            return 1;
        }
    } else {
        if ($taxon =~ /^[A-Z][a-z]+( [a-z]+){0,2}$/) {
            return 1;
        }
    }

    return 0;
}  

# This function takes two taxonomic names -- one from the occurrences/reids
# table and one from the authorities table (broken down in genus (g), 
# subgenus (sg) and species (s) components -- use splitTaxonName to
# do this for entries from the authorities table) and compares
# How closely they match up.  The higher the number, the better the
# match.
# 
# < 30 but > 20 = species level match
# < 20 but > 10 = genus/subgenus level match
# 0 = no match
sub computeMatchLevel {
    my ($occ_g,$occ_sg,$occ_sp,$taxon_g,$taxon_sg,$taxon_sp) = @_;

    my $match_level = 0;
    return 0 if ($occ_g eq '' || $taxon_g eq '');

    if ($taxon_sp) {
        if ($occ_g eq $taxon_g && 
            $occ_sg eq $taxon_sg && 
            $occ_sp eq $taxon_sp) {
            $match_level = 30; # Exact match
        } elsif ($occ_g eq $taxon_g && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 28; # Genus and species match, next best thing
        } elsif ($occ_g eq $taxon_sg && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 27; # The authorities subgenus being used a genus
        } elsif ($occ_sg eq $taxon_g && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 26; # The authorities genus being used as a subgenus
        } elsif ($occ_sg && $taxon_sg && $occ_sg eq $taxon_sg && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 25; # Genus don't match, but subgenus/species does, pretty weak
        } 
    } elsif ($taxon_sg) {
        if ($occ_g eq $taxon_g  &&
            $occ_sg eq $taxon_sg) {
            $match_level = 19; # Genus and subgenus match
        } elsif ($occ_g eq $taxon_sg) {
            $match_level = 17; # The authorities subgenus being used a genus
        } elsif ($occ_sg eq $taxon_g) {
            $match_level = 16; # The authorities genus being used as a subgenus
        } elsif ($occ_sg eq $taxon_sg) {
            $match_level = 14; # Subgenera match up but genera don't, very junky
        }
    } else {
        if ($occ_g eq $taxon_g) {
            $match_level = 18; # Genus matches at least
        } elsif ($occ_sg eq $taxon_g) {
            $match_level = 15; # The authorities genus being used as a subgenus
        }
    }
    return $match_level;
}

# This function will determine get the best taxon_no for a taxon.  Can pass in either 
# 6 arguments, or 1 argument thats a hashref to an occurrence or reid database row 

sub getBestClassification{
    my $dbt = shift;
    my ($genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name);
    if (scalar(@_) == 1) {
        $genus_reso    = $_[0]->{'genus_reso'} || "";
        $genus_name    = $_[0]->{'genus_name'} || "";
        $subgenus_reso = $_[0]->{'subgenus_reso'} || "";
        $subgenus_name = $_[0]->{'subgenus_name'} || "";
        $species_reso  = $_[0]->{'species_reso'} || "";
        $species_name  = $_[0]->{'species_name'} || "";
    } else {
        ($genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name) = @_;
    }
    my $dbh = $dbt->dbh;
    my @matches = ();

    if ( $genus_reso !~ /informal/ && $genus_name) {
        my $species_sql = "";
        if ($species_reso  !~ /informal/ && $species_name =~ /^[a-z]+$/ && $species_name !~ /^sp(\.)?$|^indet(\.)?$/) {
            $species_sql = "AND ((taxon_rank='species' and taxon_name like '% $species_name') or taxon_rank != 'species')";
        }
        my $sql = "(SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name LIKE '$genus_name%' $species_sql)";
        $sql .= " UNION ";
        $sql .= "(SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_rank='subgenus' AND taxon_name LIKE '% ($genus_name)')";
        if ($subgenus_reso !~ /informal/ && $subgenus_name) {
            $sql .= " UNION ";
            $sql .= "(SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name LIKE '$subgenus_name%' $species_sql)";
            $sql .= " UNION ";
            $sql .= "(SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_rank='subgenus' AND taxon_name LIKE '% ($subgenus_name)')";
        }

        #print "Trying to match $genus_name ($subgenus_name) $species_name\n";
#        print $sql,"\n";
        my @results = @{$dbt->getData($sql)};

        my @more_results = ();
        # Do this query separetly cause it needs to do a full table scan and is SLOW
        foreach my $row (@results) {
            my ($taxon_genus,$taxon_subgenus,$taxon_species) = splitTaxon($row->{'taxon_name'});
            if ($taxon_subgenus && $genus_name eq $taxon_subgenus && $genus_name ne $taxon_genus) {
                my $last_sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name LIKE '% ($taxon_subgenus) %' AND taxon_rank='species'";
#                print "Querying for more results because only genus didn't match but subgenus (w/g) did matched up with $row->{taxon_name}\n";
#                print $last_sql,"\n";
                @more_results = @{$dbt->getData($last_sql)};
                last;
            }
            if ($taxon_subgenus && $subgenus_name eq $taxon_subgenus && $genus_name ne $taxon_subgenus) {
                my $last_sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name LIKE '% ($taxon_subgenus) %' and taxon_rank='species'";
#                print "Querying for more results because only genus didn't match but subgenus (w/subg) did matched up with $row->{taxon_name}\n";
#                print $last_sql,"\n";
                @more_results = @{$dbt->getData($last_sql)};
                last;
            }
        }                     

        foreach my $row (@results,@more_results) {
            my ($taxon_genus,$taxon_subgenus,$taxon_species,$taxon_subspecies) = splitTaxon($row->{'taxon_name'});
            if (!$taxon_subspecies) {
                my $match_level = Taxon::computeMatchLevel($genus_name,$subgenus_name,$species_name,$taxon_genus,$taxon_subgenus,$taxon_species);
                if ($match_level > 0) {
                    $row->{'match_level'} = $match_level;
                    push @matches, $row;
#                    print "MATCH found at $match_level for matching occ $genus_name $subgenus_name $species_name to taxon $row->{taxon_name}\n";
                }
            }
        }
    }

    @matches = sort {$b->{'match_level'} <=> $a->{'match_level'}} @matches;

    if (wantarray) {
        # If the user requests a array, then return all matches that are in the same class.  The classes are
        #  30: exact match, no need to return any others
        #  20-29: species level match
        #  10-19: genus level match
        if (@matches) {
            my $best_match_class = int($matches[0]->{'match_level'}/10);
            my @matches_in_class;
            foreach my $row (@matches) {
                my $match_class = int($row->{'match_level'}/10);
                if ($match_class >= $best_match_class) {
                    push @matches_in_class, $row;
                }
            }
            return @matches_in_class;
        } else {
            return ();
        }
    } else {
        # If the user requests a scalar, only return the best match, if it is not a homonym
        if (scalar(@matches) > 1) {
            if ($matches[0]->{'taxon_name'} eq $matches[1]->{'taxon_name'}) {
                # matches are homonyms - if they're the same taxon thats been reranked, return
                # the original.
                my $orig0 = TaxonInfo::getOriginalCombination($dbt,$matches[0]->{'taxon_no'});
                my $orig1 = TaxonInfo::getOriginalCombination($dbt,$matches[1]->{'taxon_no'});
                if ($orig0 == $orig1) {
                    if ($matches[0]->{taxon_no} == $orig0) {
                        return $orig0;
                    } elsif ($matches[1]->{taxon_no} == $orig1) {
                        return $orig1;
                    } else {
                        return $matches[0]->{taxon_no};
                    }
                } else {
                    # homonym and not a reranking - return a 0
                    return 0;
                }
            } else {
                # Not a homonym, just some stray subgenus match or something still return the best
                return $matches[0]->{'taxon_no'};
            }
            return $matches[0]->{'taxon_no'}; # Dead code
        } elsif (scalar(@matches) == 1) {
            return $matches[0]->{'taxon_no'};
        } else {
            return 0;
        }
    }
}

sub propagateAuthorityInfo {
    my $dbt = shift;
    my $taxon_no = shift;
    my $this_is_best = shift;
    
    my $dbh = $dbt->dbh;
    return if (!$taxon_no);

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    return if (!$orig_no);

    main::dbg("propagateAuthorityInfo called with taxon_no $taxon_no and orig $orig_no");

    my @spelling_nos = TaxonInfo::getAllSpellings($dbt,$orig_no);
    # Note that this is the taxon_no passed in, not the original combination -- an update to
    # a spelling should proprate around as well
    my $me = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no},['*']);

    my @authority_fields = ('author1init','author1last','author2init','author2last','otherauthors','pubyr');
    my @more_fields = ('pages','figures','extant','preservation');

    # Two steps: find best authority info, then propagate to all spelling variants
    my @spellings;
    foreach my $spelling_no (@spelling_nos) {
        my $spelling = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$spelling_no},['*']);
        push @spellings, $spelling;
    }

    my $getDataQuality = sub {
        my $taxa = shift;
        my $quality = 0;
        # Taxa where the ref is authority are preferred - in the cases where there
        # are multiple refs that fit this criteria, go with the original combination
        # Else if there is anything, go with that, otherwise we're stuck with nothing
        if ($taxa->{'ref_is_authority'} =~ /yes/i) {
            if ($taxa->{'taxon_no'} == $orig_no) {
                $quality = 5;
            } else {
                $quality = 4;
            }
        } elsif ($taxa->{'author1last'}) {
            if ($taxa->{'taxon_no'} == $orig_no) {
                $quality = 3;
            } else {
                $quality = 2;
            }
        } else {
            $quality = 1;
        }
        return $quality;
    };
   
    # Sort by quality in descending rder
    @spellings = 
        map  {$_->[1]}
        sort {$b->[0] <=> $a->[0]}
        map  {[$getDataQuality->($_),$_]}
        @spellings;

    my @toUpdate;
    # Get this additional metadata from wherever we can find it, giving preference
    # to the taxa with better authority data
    my %seenMore = ();
    if ($this_is_best) {
        foreach my $f (@more_fields) {
            $seenMore{$f} = $me->{$f};
        }
    } else {
        foreach my $spelling (@spellings) {
            foreach my $f (@more_fields) {
                if ($spelling->{$f} ne '' && !exists $seenMore{$f}) {
                    $seenMore{$f} = $spelling->{$f};
                }
            }
        }
    }
    if (%seenMore) {
        foreach my $f (@more_fields) {
            push @toUpdate, "$f=".$dbh->quote($seenMore{$f});
        }
    }

    # Set all taxa to be equal to the reference form the best authority data we have
    my $best;
    if ($this_is_best) {
        $best = $me;
    } else {
        $best = $spellings[0];
    }
    if ($best->{'ref_is_authority'} =~ /yes/i) {
        foreach my $f (@authority_fields) {
            push @toUpdate, "$f=''";
        }
        push @toUpdate, "reference_no=$best->{reference_no}";
        push @toUpdate, "ref_is_authority='YES'";
    } else {
        foreach my $f (@authority_fields) {
            push @toUpdate, "$f=".$dbh->quote($best->{$f});
        }
        push @toUpdate, "reference_no=$best->{reference_no}";
        push @toUpdate, "ref_is_authority=''";
    }

    if (@toUpdate) {
        foreach my $spelling_no (@spelling_nos) {
            my $u_sql =  "UPDATE authorities SET modified=modified, ".join(",",@toUpdate)." WHERE taxon_no=$spelling_no";
            main::dbg("propagateAuthorityInfo updating authority: $u_sql");
            $dbh->do($u_sql);
        }
    }
}                                                                                                                                                                   

# end of Taxon.pm

1;
