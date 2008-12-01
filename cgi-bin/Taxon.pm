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

use Errors;
use Data::Dumper;
use CGI::Carp;
use URI::Escape;
use Mail::Mailer;
use TaxaCache;
use Classification;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HOST_URL);

use Reference;

use fields qw(dbt DBrow);


%Taxon::rankToNum = (  'subspecies' => 1, 'species' => 2, 'subgenus' => 3,
		'genus' => 4, 'subtribe' => 5, 'tribe' => 6,
		'subfamily' => 7, 'family' => 8, 'superfamily' => 9,
		'infraorder' => 10, 'suborder' => 11,
		'order' => 12, 'superorder' => 13, 'infraclass' => 14,
		'subclass' => 15, 'class' => 16, 'superclass' => 17,
		'subphylum' => 18, 'phylum' => 19, 'superphylum' => 20,
		'subkingdom' => 21, 'kingdom' => 22, 'superkingdom' => 23,
		'unranked clade' => 24, 'informal' => 25 );

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
        }  

    } else { # brand new, first submission
        $fields{'taxon_name'} = $q->param('taxon_name');
        $fields{'reference_no'} = $s->get('reference_no');
        # to speed things up, assume that the primary (current) ref is the
        #  authority when the taxon is new JA 29.8.06
        $fields{'ref_is_authority'} = 'PRIMARY';
        # prefill some fields based on the last entry from the same ref
        #  JA 6.5.07
        my $sql = "SELECT ref_is_authority,pages,preservation,form_taxon,extant FROM authorities WHERE ((pages!='' AND pages IS NOT NULL) OR (preservation!='' AND preservation IS NOT NULL) OR (form_taxon!='' AND form_taxon IS NOT NULL) OR (extant!='' AND extant IS NOT NULL)) AND reference_no=" . $s->get('reference_no') . " AND enterer_no=" . $s->get('enterer_no') . " ORDER BY taxon_no DESC LIMIT 1";
        my $lastauthority = @{$dbt->getData($sql)}[0];
        if ( $lastauthority )	{
            if ( $lastauthority ->{ref_is_authority} eq "YES" )	{
                $fields{'pages'} = $lastauthority->{pages};
            }
            $fields{'preservation'} = $lastauthority->{preservation};
            $fields{'form_taxon'} = $lastauthority->{form_taxon};
            $fields{'extant'} = $lastauthority->{extant};
        }
    }

    # hack needed because form can display preservation in either of two places
    if ( ! $fields{'preservation2'} )	{
        $fields{'preservation2'} = $fields{'preservation'};
    }

    # Grab the measurement data if they exist
    # added some sanity checks here: "holotype" record in specimens table has
    #  to have measurements of exactly one specimen, and the part has to match
    #  the type body part or part details fields JA 10.8.07
    if (!$isNewEntry) {
        my $taxon_no = $t->get('taxon_no');
        my $sql = "(SELECT m.measurement_type,s.magnification,s.specimen_part,s.specimen_no,m.average,m.real_average FROM specimens s LEFT JOIN measurements m ON s.specimen_no=m.specimen_no WHERE s.is_type='holotype' AND s.specimens_measured=1 AND (specimen_part='".$fields{'type_body_part'}."' OR specimen_part='".$fields{'part_details'}."') AND s.taxon_no=$taxon_no)"
            . " UNION "
	    . "(SELECT m.measurement_type,s.magnification,s.specimen_part,s.specimen_no,m.average,m.real_average FROM specimens s, occurrences o LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no LEFT JOIN measurements m ON s.specimen_no=m.specimen_no WHERE o.occurrence_no=s.occurrence_no AND s.is_type='holotype' AND s.specimens_measured=1 AND (specimen_part='".$fields{'type_body_part'}."' OR specimen_part='".$fields{'part_details'}."') AND o.taxon_no=$taxon_no AND re.reid_no IS NULL)"
	    . " UNION "
            . "(SELECT m.measurement_type,s.magnification,s.specimen_part,s.specimen_no,m.average,m.real_average FROM (specimens s, occurrences o, reidentifications re) LEFT JOIN measurements m ON s.specimen_no=m.specimen_no WHERE o.occurrence_no=re.occurrence_no AND o.occurrence_no=s.occurrence_no AND s.is_type='holotype' AND s.specimens_measured=1 AND (specimen_part='".$fields{'type_body_part'}."' OR specimen_part='".$fields{'part_details'}."') AND re.taxon_no=$taxon_no AND re.most_recent='YES')";
        my @rows = @{$dbt->getData($sql)};

        if (@rows) {
            my %specimen_count = ();
            foreach my $row (@rows) {
                if ($row->{'magnification'} > 1) {
                    $fields{'hide_measurements'} = 1;
                    last;
                }
                $specimen_count{$row->{'specimen_no'}}++;
                if ($row->{'measurement_type'} eq 'width') {
                    $fields{'width'} = $row->{'average'};
                } elsif ($row->{'measurement_type'} eq 'length') {
                    $fields{'length'} = $row->{'average'};
                }
            }
            if (scalar keys %specimen_count > 1) {
                $fields{'hide_measurements'} = 1;
            } else {
                my %generic_parts = ();
                foreach my $p ($hbo->getList('type_body_part')) {
                    $generic_parts{$p} = 1;
                }
                $fields{'specimen_no'} = $rows[0]->{'specimen_no'};
                if ($rows[0]->{'specimen_part'}) {
                    if ($generic_parts{$rows[0]->{'specimen_part'}}) {
                        $fields{'type_body_part'} = $rows[0]->{'specimen_part'};
                    } else {
                        $fields{'part_details'} = $rows[0]->{'specimen_part'};
                    }
                }
            }
        }
    }

	# fill out the authorizer/enterer/modifier info at the bottom of the page
	if (!$isNewEntry) {
		if ($fields{'authorizer_no'}) { 
            $fields{'authorizer_name'} = " <span class=\"fieldName\">Authorizer:</span> " . Person::getPersonName($dbt,$fields{'authorizer_no'}); 
        }
		if ($fields{'enterer_no'}) { 
            $fields{'enterer_name'} = " <span class=\"fieldName\">Enterer:</span> " . Person::getPersonName($dbt,$fields{'enterer_no'}); 
        }
		if ($fields{'modifier_no'}) { 
            $fields{'modifier_name'} = " <span class=\"fieldName\">Modifier:</span> ".Person::getPersonName($dbt,$fields{'modifier_no'}); 
        }
        $fields{'modified'} = "<span class=\"fieldName\">Modified: </span>".$fields{'modified'};
        $fields{'created'} = "<span class=\"fieldName\">Created: </span>".$fields{'created'};
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
			
			$fields{'parent_taxon_select'} = "<span class=\"prompt\">Belongs to:</span>&nbsp;".
                $hbo->htmlSelect('parent_taxon_no',\@parent_descs,\@parent_nos,$parent_no);
		} else {
			# count = 0, so we need to warn them to enter the parent taxon first.
#	        my $errors = Errors->new();
#			$errors->add("The $parentRank '$parentName' for this $fields{'taxon_rank'} hasn't been entered yet.  Please <A HREF=\"/cgi-bin/$WRITE_URL?action=displayAuthorityForm&taxon_name=$parentName\">create a new authority record for '$parentName'</A> before trying to add this $fields{'taxon_rank'}.");
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

    # Build original name select
    if ($fields{taxon_no}) {
        my $orig_no = TaxonInfo::getOriginalCombination($dbt,$fields{taxon_no});
        my @spellings = TaxonInfo::getAllSpellings($dbt,$orig_no);
        my @taxa = ();
        my %seen_name = ();
        my $duplicate_names = 0;
        foreach my $spelling_no (@spellings) {
            my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$spelling_no});
            push @taxa, [$spelling_no,$taxon->{taxon_name},$taxon->{taxon_rank}];
            if ($seen_name{$taxon->{taxon_name}}) {
                $duplicate_names++;
            }
            $seen_name{$taxon->{taxon_name}} = 1;
        }
        if ($duplicate_names) {
            foreach my $t (@taxa) {
                $t->[1] .= ", $t->[2]";
            }
        }
        @taxa = sort {$a->[1] cmp $b->[1]} @taxa;
        my @names = map {$_->[1]} @taxa;
        my @nos =   map {$_->[0]} @taxa;
        if (scalar(@names) > 1) {
            my $original_no_select = HTMLBuilder::htmlSelect('original_no',\@names,\@nos,$orig_no);
            $fields{'original_no_select'} = $original_no_select;
        }
    }

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
	
	if ($q->param('ref_is_authority') =~ /PRIMARY|CURRENT/ && ( $q->param('author1init') || $q->param('author1last') || $q->param('author2init') || $q->param('author2last') || $q->param('pubyr') || $q->param('otherauthors') ) ) {
		$errors->add("You entered author and year data but did not check 'named in an earlier publication,' so it has now been checked for you");
		$q->param('ref_is_authority' => 'NO');
	}

	if (($q->param('ref_is_authority') ne 'PRIMARY') && 
	    ($q->param('ref_is_authority') ne 'CURRENT') && 
		($q->param('ref_is_authority') ne 'NO')) {
		$errors->add("You must choose one of the reference radio buttons");
	} elsif ($q->param('ref_is_authority') eq 'NO') {

	# commented out 10.5.04 by JA because we often need to add (say) genera
	#  without any data when we create and classify species for which we
	#  do have data
#		if (! $q->param('author1last')) {
#			$errors->add('You must enter at least the last name of a first author');	
#		}
		
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
       

        if ($q->param('taxon_rank') =~ /species|subgenus/) {
            if (!$q->param('author1last')) {
                $errors->add("If you enter a subgenus, species, or subspecies, enter at least the last name of the first author");
            }
            if (!$q->param('pubyr')) {
                $errors->add("If you enter a subgenus, species, or subspecies, the publication year is required");
            }
        }
	}

	foreach my $formField ($q->param()) {
	# if the value isn't already in our fields to enter
		if (! $fields{$formField}) {
			$fields{$formField} = $q->param($formField);
		}
	}
	# hack needed because form can display preservation in either of
	#  two places
	if ( $fields{'preservation2'} =~ /[a-z]/ )	{
		$fields{'preservation'} = $fields{'preservation2'};
	}
	


	$fields{'taxon_name'} = $q->param('taxon_name');

	# correct the ref_is_authority field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($q->param('ref_is_authority') =~ /PRIMARY|CURRENT/) {
		$fields{'ref_is_authority'} = 'YES';
	} elsif ($q->param('ref_is_authority') eq 'NO') {
		$fields{'ref_is_authority'} = '';
	}

        my $lookup_reference;
        if ($q->param('ref_is_authority') eq 'CURRENT') {
            $lookup_reference = $s->get('reference_no');
        } else {
            $lookup_reference = $fields{'reference_no'};
        }
        my $pubyr;
        my $ref = Reference->new($dbt,$lookup_reference);
        if ($q->param('pubyr')) {
            $pubyr = $q->param('pubyr');
            if ($lookup_reference) {
                if ($ref && $pubyr > $ref->get('pubyr')) {
                    $errors->add("The publication year ($pubyr) can't be more recent than that of the primary reference (" . $ref->get('pubyr') . ")");
                }
            }
        } elsif ($lookup_reference) {
            $pubyr = $ref->get('pubyr');
        } else	{
        # paranoia check, should never happen
            $errors->add("The publication year can't be determined");
        }

        if (! Validation::properYear( $pubyr ) ) {
            $errors->add("The year is improperly formatted");
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
        ($rankFromSpaces !~ /species|genus/ && $q->param('taxon_rank') =~ /subspecies|species|subgenus/)) {
        $errors->add("The selected rank '".$q->param('taxon_rank')."' doesn't match the spacing of the taxon name '".$q->param('taxon_name')."'");
    }

    if ($q->param('length') && $q->param('length') !~ /^[0-9]*\.?[0-9]+$/) {
        $errors->add("Length must be a decimal number");
    }
    if ($q->param('width') && $q->param('width') !~ /^[0-9]*\.?[0-9]+$/) {
        $errors->add("Width must be a decimal number");
    }

	# type locality handling JA 8.9.08
	if ( $q->param('type_locality') && $q->param('type_locality') !~ /^[0-9]*$/ )	{
		$errors->add("You must enter a number in the type locality field");
	# lazy check, the widget should really be excluded from the page if the
	#  taxon is a subspecies
	} elsif ( $q->param('type_locality') && $rankFromSpaces eq "subspecies" )	{
		$errors->add("We currently do not store type locality data for subspecies");
	# paranoia check, this widget should only appear on species forms
	} elsif ( $q->param('type_locality') && $rankFromSpaces ne "species" )	{
		$errors->add("You can't enter type locality data for a $rankFromSpaces");
	} elsif ( $q->param('type_locality') )	{
	# check very narrowly to see if the species has an apparent type
	#  locality in the system: there must be an exact match between the
	#  spellings on this form and in the occurrences/reIDs table
		my $coll;
		if ( ! $isNewEntry )	{
			$coll = $t->get('type_locality');
		}
		if ( $coll != $q->param('type_locality') )	{
			my $sql;
			if ( $q->param('taxon_no') > 0 )	{
				$sql = "(SELECT collection_no FROM occurrences WHERE taxon_no=".$q->param('taxon_no')." AND species_reso='n. sp.') UNION (SELECT collection_no FROM reidentifications WHERE taxon_no=".$q->param('taxon_no')." AND species_reso='n. sp.')";
			} else	{
				my($g,$sg,$s,$ss) = splitTaxon($q->param('taxon_name'));
				$sql = "(SELECT collection_no FROM occurrences WHERE genus_name='$g' AND (subgenus_name='$sg' OR subgenus_name IS NULL OR subgenus_name='') AND species_name='$s' AND species_reso='n. sp.') UNION (SELECT collection_no FROM reidentifications WHERE genus_name='$g' AND (subgenus_name='$sg' OR subgenus_name IS NULL OR subgenus_name='') AND species_name='$s' AND species_reso='n. sp.')";
			}
			my @locs = @{$dbt->getData($sql)};
			my $nlocs = $#locs + 1;
			if ( $nlocs > 2 )	{
				my $collnos = join ', ',map {$_->{collection_no}} @locs;
				$collnos =~ s/(, )([0-9]*$)/, and $2/;
				$errors->add("Collections $collnos are all marked as the type locality of ".$q->param('taxon_name').", so it's not clear what should go in the authorities table");
			} elsif ( $nlocs == 2 )	{
				my $collnos = join ' and ',map {$_->{collection_no}} @locs;
				$errors->add("Collections $collnos are both marked as the type locality of ".$q->param('taxon_name').", so it's not clear what should go in the authorities table");
			} elsif ( $nlocs == 0 )	{
				$errors->add("No occurrences of ".$q->param('taxon_name')." that are spelt this way have been marked 'n. sp.'");
			} else	{
				$coll = $locs[0]->{'collection_no'};
				if ( $coll != $q->param('type_locality') )	{
					$errors->add("You entered ".$q->param('type_locality')." as the type locality, but the only occurrence of this species marked with an 'n. sp.' is in collection $coll");
				} else	{
					$fields{'type_locality'} = $coll;
				}
			}
		}
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
                $errors->add("There are multiple versions of the name '$parent_name' in the database.  Please select the right one.");
            } elsif (@parents == 1) {
                $parent_no = $parents[0]->{'taxon_no'};
            } else {
                $errors->add("The name '$parent_name' isn't in the database yet.  Please add an authority record for it before continuing.");
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
			addImplicitChildOpinion($dbt,$s,$resultTaxonNumber,$parent_no,\%fields,$pubyr);
		}
	} else {
		# if it's an old entry, then we'll update.
		$resultTaxonNumber = $t->get('taxon_no');
		$status = $dbt->updateRecord($s,'authorities','taxon_no',$resultTaxonNumber, \%fields);
        propagateAuthorityInfo($dbt,$resultTaxonNumber,1);

        my $db_orig_no = TaxonInfo::getOriginalCombination($dbt,$resultTaxonNumber);

        if ($fields{'original_no'} =~ /^\d+$/ && $fields{'original_no'} != $db_orig_no) {
            my $sql = "SELECT * FROM opinions WHERE child_no=$db_orig_no"; 
            my @results = @{$dbt->getData($sql)};
            my @parents = ();
            foreach my $row (@results) {
                Opinion::resetOriginalNo($dbt,$fields{'original_no'},$row);
#                if ($row->{'child_no'} != $fields{original_no}) {
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
#                }
            }
            # We also have to modify the parent_no so it points to the original
            #  combination of any taxa classified into any migrated opinion
            if ( @parents ) {
                my %unique_parents = ();
                foreach my $p (@parents) {
                    $unique_parents{$p} = 1;
                }
                my @unique_parents = keys %unique_parents;
                my $sql = "UPDATE opinions SET modified=modified, parent_no=$fields{'original_no'} WHERE parent_no IN (".join(",",@unique_parents).")";
                dbg("Migrating parents: $sql");
                $dbh->do($sql);
            }

        }
        # Changing a genus|subgenus|species|subspecies is tricky since we have to change
        # other related opinions and authorities
        if ($t->get('taxon_name') ne $fields{'taxon_name'} &&
            $t->get('taxon_rank') =~ /^genus|^subgenus|^species/){
            updateChildNames($dbt,$s,$t->get('taxon_no'),$t->get('taxon_name'),$fields{'taxon_name'});
        }

        
        updateImplicitBelongsTo($dbt,$s,$t->get('taxon_no'),$parent_no,$t->get('taxon_name'),$fields{'taxon_name'},\%fields);
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
        $end_message .= "<div align=\"center\"><p class=\"large\">" . $fields{'taxon_name'} . " " .Reference::formatShortRef(\%fields). " has been $enterupdate the database</p></div>";

        my $origResultTaxonNumber = TaxonInfo::getOriginalCombination($dbt,$resultTaxonNumber);
        
        $end_message .= qq|
    <div align="center" class="displayPanel">
    <table cellpadding="10" class="small"><tr><td valign="top">
      <p class="large" style="margin-left: 2em;">Name functions</p>
      <ul>
      <li><a href="$WRITE_URL?action=displayAuthorityTaxonSearchForm">Add/edit another taxon</a></li>
      <br><li><a href="$WRITE_URL?action=displayAuthorityForm&taxon_no=$resultTaxonNumber">Edit $fields{taxon_name}</a></li>
      <br><li><a href="$WRITE_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit a name from the same reference</a></li>
      <br><li><a href="$WRITE_URL?action=displayAuthorityTaxonSearchForm&use_reference=new">Add/edit another taxon from another reference</a></li>
      <br><li><a href="$READ_URL?action=checkTaxonInfo&taxon_no=$resultTaxonNumber">Get general information about $fields{taxon_name}</a></li>   
      </ul>
    </td>
    <td valign=top>
      <p class="large" style="margin-left: 2em;">Opinion functions</p>
        <ul>
          <li><a href="$WRITE_URL?action=displayOpinionSearchForm">Add/edit opinion about another taxon</a></li>
|;
        # user may want to immediately enter or edit either:
        # (1) the opinion of the taxon's author, if not ref is authority
        #  and assuming that the actual reference is nowhere in the
        #  database, or (2) regardless, the current reference's opinion
        # check everywhere for the author's opinion, because it could
        #  come from any reference

        my $style = qq| style="padding-top: 0.65em;"|;
        if ( $q->param('ref_is_authority') !~ /PRIMARY/ )	{
            my $cleanauth1 = $q->param('author1last');
            $cleanauth1 =~ s/'/\\'/;
            my $cleanauth2 = $q->param('author2last');
            $cleanauth2 =~ s/'/\\'/;
            my $sql = "SELECT opinion_no FROM opinions WHERE author1last='$cleanauth1' AND author2last='$cleanauth2' AND pubyr='" . $q->param('pubyr') . "' AND child_spelling_no=$resultTaxonNumber AND child_no=$origResultTaxonNumber ORDER BY opinion_no DESC";
            my $opinion_no = ${$dbt->getData($sql)}[0]->{opinion_no};
            if ( $opinion_no > 0 )	{
                $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionForm&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber&opinion_no=$opinion_no">Edit this author's opinion about $fields{taxon_name}</a></li>
|;
            } elsif ( $q->param('author1last') )	{
            # if that didn't work, either this is not a species, or
            #   something is wrong because an implicit opinion of the
            #   author should have been created; regardless, create a link
                  $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionForm&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber&author1init=|.$q->param('author1init').qq|&author1last=|.$q->param('author1last').qq|&author2init=|.$q->param('author2init').qq|&author2last=|.$q->param('author2last').qq|&otherauthors=|.$q->param('otherauthors').qq|&pubyr=|.$q->param('pubyr').qq|&reference_no=$resultReferenceNumber&opinion_no=-1">Add this author's opinion about $fields{taxon_name}</a></li>
|;
            }
        }
        # one way or another, the current reference may have an opinion,
        #  so try to retrieve it
        my $sql = "SELECT opinion_no FROM opinions WHERE ref_has_opinion='YES' AND reference_no=$resultReferenceNumber AND child_spelling_no=$resultTaxonNumber AND child_no=$origResultTaxonNumber ORDER BY opinion_no DESC";
        my $opinion_no = ${$dbt->getData($sql)}[0]->{opinion_no};
        if ( $opinion_no > 0 )	{
            $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionForm&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber&opinion_no=$opinion_no">Edit this reference's opinion about $fields{taxon_name}</a></li>
|;
        } else	{
            $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionForm&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber&opinion_no=-1">Add this reference's opinion about $fields{taxon_name}</a></li>
|;
        }
        $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionForm&opinion_no=-1&child_spelling_no=$resultTaxonNumber&child_no=$origResultTaxonNumber">Add an opinion about $fields{taxon_name}</a></li>
|;
        $end_message .= qq|<li$style><a href="$WRITE_URL?action=displayOpinionChoiceForm&taxon_no=$resultTaxonNumber">Edit an opinion about $fields{taxon_name}</a></li>
|;
        $end_message .= qq|
          <li$style><a href="$WRITE_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$resultReferenceNumber">Edit an opinion from the same reference</a></li>
          <li$style><a href="$WRITE_URL?action=displayOpinionSearchForm&use_reference=new">Add/edit opinion about another taxon from another reference</a></li>
          <li$style><a href="$WRITE_URL?action=startProcessPrintHierarchy&reference_no=$resultReferenceNumber&maximum_levels=100">Print this reference's classification</a></li>
          </ul>
        </td></tr></table>
        </div>|;

        processSpecimenMeasurement($dbt,$s,$resultTaxonNumber,$resultReferenceNumber,\%fields);

        displayTypeTaxonSelectForm($dbt,$s,$fields{'type_taxon'},$resultTaxonNumber,$fields{'taxon_name'},$fields{'taxon_rank'},$resultReferenceNumber,$end_message);
	}
	
	print "<BR>";
	print "</CENTER>";
}

sub processSpecimenMeasurement {
    my ($dbt,$s,$taxon_no,$reference_no,$fields) = @_;
    
    my $dbh = $dbt->dbh;
    my $specimen_no = int($fields->{'specimen_no'});

    my $sql = "(SELECT specimen_no,specimens_measured,specimen_id,specimen_part FROM specimens s WHERE s.is_type='holotype' AND s.specimens_measured=1 AND s.taxon_no=$taxon_no) UNION (SELECT specimen_no,specimens_measured,specimen_id,specimen_part FROM specimens s, occurrences o left join reidentifications re on o.occurrence_no=re.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.is_type='holotype' AND s.specimens_measured=1 AND o.taxon_no=$taxon_no AND re.reid_no IS NULL) UNION (SELECT specimen_no,specimens_measured,specimen_id,specimen_part FROM specimens s, occurrences o, reidentifications re WHERE o.occurrence_no=re.occurrence_no AND s.occurrence_no=o.occurrence_no AND s.is_type='holotype' AND s.specimens_measured=1 AND re.taxon_no=$taxon_no AND re.most_recent='YES')";
    my @rows = @{$dbt->getData($sql)};

    if ( scalar @rows <= 1 ) {

    # bomb out if there is a direct conflict, i.e., the parts are different or
    #  the specimen record involves more than one specimen
        if ( @rows && $rows[0]->{'specimens_measured'} > 1 || ( $rows[0]->{'specimen_part'} ne $fields->{'part_details'} && $rows[0]->{'specimen_part'} ne $fields->{'type_body_part'} ) )	{
            return;
        }

        if ($fields->{'length'} || $fields->{'width'}) {
            my $part = $fields->{'part_details'} || $fields->{'type_body_part'};
            my $sfields = {
                taxon_no=>$taxon_no,
                reference_no=>$reference_no,
                specimens_measured=>1,
                specimen_id=>$fields->{'type_specimen'},
                specimen_part=>$part,
                magnification=>1,
                is_type=>'holotype'
            };
            if (!$specimen_no) {
                my $result;
                ($result,$specimen_no) = $dbt->insertRecord($s,'specimens',$sfields);
            } else {
                $dbt->updateRecord($s,'specimens','specimen_no',$specimen_no,$sfields);
            }
        }
        
        if ($specimen_no) {
            foreach my $type ('length','width') {
                my $value = $fields->{$type};
                my $sql = "SELECT measurement_no FROM measurements WHERE specimen_no=$specimen_no AND measurement_type='$type'";
                my $db_row = ${$dbt->getData($sql)}[0];

                my $quoted_value = $dbh->quote($value);
                if ($value && $db_row) {
                    my $sql = "UPDATE measurements SET average=$quoted_value, real_average=$quoted_value WHERE measurement_no=$db_row->{measurement_no}";
                    dbg($sql);
                    $dbh->do($sql);
                } elsif ($value && !$db_row) {
                    my $sql = "INSERT measurements (specimen_no,average,real_average,measurement_type) VALUES ($specimen_no,$quoted_value,$quoted_value,'$type')";
                    dbg($sql);
                    $dbh->do($sql);
                } elsif (!$value && $db_row) {
                    my $sql = "DELETE FROM measurements WHERE measurement_no=$db_row->{measurement_no}";
                    dbg($sql);
                    $dbh->do($sql);
                }
            }
        } 
    }
}

# When a genus is changed becaues of a typo (i.e. Equuus -> Equus) and theres
# children assigned to that genus, the children will be found and changed to match
# the genus.  Doesn't come up too often
sub updateChildNames {
    my ($dbt,$s,$old_taxon_no,$old_name,$new_name) = @_;
    
    return if ($old_name eq $new_name || !$old_name);
    dbg("UPDATE CHILD NAMES CALLED WITH: $old_name --> $new_name");

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
    # this quotes parentheses (in subgenera) and any other weirdness
    my $quoted_old_name = quotemeta $old_name;
    foreach my $t (keys %to_change) {
        my $child = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$t});
        my $taxon_name = $child->{'taxon_name'};
        $taxon_name =~ s/^$quoted_old_name/$new_name/; 
        dbg("Changing parent from $old_name to $new_name.  child taxon from $child->{taxon_name} to $taxon_name");
        $dbt->updateRecord($s,'authorities','taxon_no',$child->{'taxon_no'},{'taxon_name'=>$taxon_name});
    }
}


# This happens when we change the name of a species, but change the genus part so
# the species gets assigned to a different genus with some very similar spelling.  We 
# have to find the name, find the opinion from the same reference, and then change
# the parent_no of that opinion.  This can also happen when change from a Genus A species B
# combo to a Subgenus C (Subgenus A) species B type scenario
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
        dbg("Looking for opinions to migrate for $old_higher");
        foreach my $p (TaxonInfo::getTaxa($dbt,{'taxon_name'=>$old_higher})) {
            $old_parents{$p->{'taxon_no'}} = 1;
        }
    }
    my $sql = "SELECT * FROM opinions WHERE child_spelling_no=$taxon_no";
    my @old_opinions = @{$dbt->getData($sql)};
    #    dbg("Found ".scalar(@old_opinions)." existing opinions to migrate for $old_higher");

    if ($new_higher && !$old_higher) {
        # Insert a new opinion, switch from genus --> subgenus
        dbg("Inserting belongs to since taxa changed from genus $old_name to subgenus $new_name");
        addImplicitChildOpinion($dbt,$s,$taxon_no,$parent_no,$fields,0);
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
                dbg("Deleting belongs to record since taxa changed from $old_name to $new_name");
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
                    dbg("Updating belongs to since taxa changed from $old_name to $new_name");
                    $dbt->updateRecord($s,'opinions','opinion_no',$row->{opinion_no},{'parent_spelling_no'=>$parent_no,'parent_no'=>$orig_parent_no});
                }
            }
        } 
        if (!$found_old_parent) {
            # Insert new opinion
            dbg("Inserting belongs to since taxa changed from $old_name to $new_name");
            addImplicitChildOpinion($dbt,$s,$taxon_no,$parent_no,$fields,0);
        }
    } 
}

sub addImplicitChildOpinion {
    my ($dbt,$s,$child_no,$parent_no,$fields,$pubyr) = @_;
    
    return unless ($child_no && $parent_no);
    # Get original combination for parent no PS 04/22/2005
    my $orig_parent_no = TaxonInfo::getOriginalCombination($dbt,$parent_no);
   
    # several things are always true by definition of the original author's
    #  opinion on a name: it provides evidence, gives a new diagnosis, and
    #  uses the original spelling
    my %opinionHash = (
        'status'=>'belongs to',
        'spelling_reason'=>'original spelling',
        'diagnosis_given'=>'new',
        'child_no'=>$child_no,
        'child_spelling_no'=>$child_no,
        'parent_no'=>$orig_parent_no,
        'parent_spelling_no'=>$parent_no,
        'ref_has_opinion'=>$fields->{'ref_is_authority'}
    );
    # evidence can be assumed only for opinions postdating the
    #  Règles internationales de la Nomenclature zoologique of 1905 JA 13.8.8
    # only do this for opinions that represent original assignments of species,
    #  which we also check with pubyr because other calls of this function
    #  do not pass one in
    # note that we use pubyr and not fields->{pubyr} because the latter only
    #  exists if ref_is_authority is false
    if ( $pubyr >= 1905 )	{
        $opinionHash{'diagnosis_given'} = 'new';
        $opinionHash{'basis'} = 'stated with evidence';
    }
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

    my @dataFields = ("pages", "figures", "common_name", "extant", "form_taxon", "preservation");
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
    dbg("create new authority record, got return code $return_code");
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
    my $no_email = shift;
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
            dbg("Not counting taxa as a homonym, it seems to match a another taxa exactly:".Dumper($taxa[$i]));
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
            my $link = "$WRITE_URL?action=displayCollResults&type=reclassify_occurrence&taxon_name=$taxon_name&occurrences_authorizer_no=$person_no";
            my %headers = ('Subject'=> 'Please reclassify your occurrences','From'=>'alroy');
            if ($HOST_URL =~ /paleodb\.org/) {
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
            unless ($no_email) {
                my $mailer = new Mail::Mailer;
                $mailer->open(\%headers);
                print $mailer $body; 
                $mailer->close;
            }
        }
        
        # Deal with homonym issue
        # Default behavior changed: leave occurrences classified by default, since whoever entered them in the first
        # place probably wants them to be classified in the existing taxa in the database.
#        $sql1 = "UPDATE occurrences SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
#        $sql2 = "UPDATE reidentifications SET modified=modified,taxon_no=0 WHERE taxon_no IN (".join(",",@taxon_nos).")";
#        $dbt->getData($sql1);
#        $dbt->getData($sql2);
        push @warnings, "Since $taxon_name is a homonym, occurrences of it may be incorrectly classified.  Please \"<a target=\"_BLANK\" href=\"$WRITE_URL?action=displayCollResults&type=reclassify_occurrence&taxon_name=$taxon_name&occurrences_authorizer_no=".$authorizer_no."\">reclassify your occurrences</a>\" of this taxon.";
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
            dbg("Updating matched occs:".$sql);
            $dbh->do($sql);
        }
        if (@matchedReids) {
            my $sql = "UPDATE reidentifications SET modified=modified,taxon_no=$taxon_no WHERE reid_no IN (".join(",",@matchedReids).")";
            dbg("Updating matched reids:".$sql);
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

    dbg("displayTypeTaxonSelectForm called with is_tt_form_value $is_tt_form_value tt_no $type_taxon_no tt_name $type_taxon_name tt_rank $type_taxon_rank ref_no $reference_no");

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
    dbg("TYPE TAXON PARENTS:\n<PRE>".Dumper(\@parents)."</PRE>");
    if ($is_tt_form_value) {
        if (scalar(@parents) > 1) {
            print "<div align=\"center\">";
            print "<form method=\"POST\" action=\"$WRITE_URL\">\n";
            print "<input type=\"hidden\" name=\"action\" value=\"submitTypeTaxonSelect\">\n";
            print "<input type=\"hidden\" name=\"reference_no\" value=\"$reference_no\">\n";
            print "<input type=\"hidden\" name=\"type_taxon_no\" value=\"$type_taxon_no\">\n";
            print "<input type=\"hidden\" name=\"end_message\" value=\"".uri_escape($end_message)."\">\n";
            print "<table><tr><td style=\"border: 1px solid lightgray; padding: 1em;\">\n";
            print "<p class=\"large\">For which taxa is $type_taxon_name a type $type_taxon_rank?</p>";
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
            push @warnings, "Can't set this taxon as the type because no valid higher taxa were found.  There must be opinions linking this taxon to its higher taxa from the same reference ($formatted_ref).";
            carp "Maybe something is wrong in the opinions script, got no parents for current taxon after adding an opinion.  (in section dealing with type taxon). Vars: tt_no $type_taxon_no ref $reference_no tt_name $type_taxon_name tt_rank $type_taxon_rank"; 
        }
    } else {
        # This is not a type taxon.  Find all parents from the same reference, and set the
        # type_taxon_no to 0 if its set to this taxon, otherwise leave it alone
        dbg("Handling deletion of type taxon no $type_taxon_no");
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
    my $dbh = $dbt->dbh;
            
    my $focal_taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$type_taxon_no});
            
    my $parents = Classification::get_classification_hash($dbt,'all',[$type_taxon_no],'array',$reference_no);
    # This array holds possible higher taxa this taxon can be a type taxon for
    # Note the reference_no passed to get_classification_hash - parents must be linked by opinions from
    # the same reference as the reference_no of the opinion which is currently being inserted/edited
    my @parents = @{$parents->{$type_taxon_no}}; # is an array ref

# JA: we need not just potential parents, but all immediate parents that ever
#  have been proposed, so also hit the opinion table directly 17.6.07
    my @parent_nos;
    for my $p ( @parents )	{
        push @parent_nos , $p->{'taxon_no'};
    }
    my $sql = "SELECT taxon_no,taxon_rank,taxon_name FROM authorities a,opinions o WHERE child_no=". $focal_taxon->{taxon_no} ." AND taxon_rank!='". $focal_taxon->{'taxon_rank'} ."' AND parent_no=taxon_no";
    if ( $#parents > -1 )	{
        $sql .= " AND parent_no NOT IN (". join(',',@parent_nos) .")";
    }
    $sql .= " GROUP BY parent_no";
    push @parents , @{$dbt->getData($sql)};

    if ($focal_taxon->{'taxon_rank'} =~ /species/) {
        # A species may be a type for genus/subgenus only
        my $i = 0;
        for($i=0;$i<scalar(@parents);$i++) {
            last if ($parents[$i]->{'taxon_rank'} !~ /species|genus|subgenus/);
        }
        splice(@parents,$i);
    } else {
        # A higher order taxon may be a type for subtribe/tribe/family/subfamily/superfamily only
        # Don't know about unranked clade, leave it for now
        my $i = 0;
        for($i=0;$i<scalar(@parents);$i++) {
            last if ($parents[$i]->{'taxon_rank'} !~ /tribe|family|unranked clade/);
        }
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
    my %options = @_;
	my $nameLine;
    my $authLine;

	# Print the name
	# italicize if genus or species.
	if ( $taxon->{'taxon_rank'} =~ /subspecies|species|genus/) {
        if ($options{'no_html'}) {
            $nameLine .= "$taxon->{taxon_name}, $taxon->{taxon_rank}";
        } else {
		    $nameLine .= "<i>" . $taxon->{'taxon_name'} . "</i>";
        }
	} else {
		$nameLine .= $taxon->{'taxon_name'};
        if ($taxon->{'taxon_rank'} && $taxon->{'taxon_rank'} !~ /unranked clade/) {
            $authLine .= ", $taxon->{taxon_rank}";
        }
	}

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
    my $is_recomb = ($orig_no == $taxon->{'taxon_no'}) ? 0 : 1;
	# If the authority is a PBDB ref, retrieve and print it
    my $pub_info = Reference::formatShortRef($taxon,'is_recombination'=>$is_recomb);
    if ($pub_info !~ /^\s*$/) {
        $authLine .= ',' unless ($is_recomb);
        $authLine .= " ".$pub_info;
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

    if ($options{'return_array'}) {
        return ($nameLine,$authLine);
    } else {
	    return $nameLine.$authLine;
    }
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
    } elsif ($taxon =~ /ini$/)	{
        return "tribe";
    } elsif ($taxon =~ /inae$/)	{
        return "subfamily";
    } elsif ($taxon =~ /idae$/)	{
        return "family";
    } elsif ($taxon =~ /eae$/)	{
        return "family";
    } elsif ($taxon =~ /oidea$/)	{
        return "superfamily";
    } elsif ($taxon =~ /ida$/)	{
        return "order";
    } elsif ($taxon =~ /formes$/)	{
        return "order";
    } elsif ($taxon =~ /ales$/)	{
        return "order";
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

    #dbg("propagateAuthorityInfo called with taxon_no $taxon_no and orig $orig_no");

    my @spelling_nos = TaxonInfo::getAllSpellings($dbt,$orig_no);
    # Note that this is the taxon_no passed in, not the original combination -- an update to
    # a spelling should proprate around as well
    my $me = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$taxon_no},['*']);

    my @authority_fields = ('author1init','author1last','author2init','author2last','otherauthors','pubyr');
    my @more_fields = ('pages','figures','common_name','type_specimen','type_body_part','part_details','type_locality','extant','form_taxon','preservation');

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
   
    # Sort by quality in descending order
    @spellings = 
        map  {$_->[1]}
        sort {$b->[0] <=> $a->[0]}
        map  {[$getDataQuality->($_),$_]}
        @spellings;

    my @toUpdate;
    # Get this additional metadata from wherever we can find it, giving preference
    # to the taxa with better authority data
    my %seenMore = ();
    foreach my $spelling (@spellings) {
        foreach my $f (@more_fields) {
            if ($spelling->{$f} ne '' && !exists $seenMore{$f}) {
                $seenMore{$f} = $spelling->{$f};
            }
        }
    }
    # the user just entered these data, so if they exist, they should be used
    # slightly dangerous because you cannot erase data completely if they're
    #  wrong; you have to replace them with something
    # this won't mess with authority data
    foreach my $f (@more_fields) {
        if ( $me->{$f} )	{
            $seenMore{$f} = $me->{$f};
        }
    }
    if (%seenMore) {
        foreach my $f (@more_fields) {
            push @toUpdate, "$f=".$dbh->quote($seenMore{$f});
        }
    }

    # Set all taxa to be equal to the reference from the best authority data we have
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
            #dbg("propagateAuthorityInfo updating authority: $u_sql");
            $dbh->do($u_sql);
        }
    }
}                                                                                                                                                                   

# end of Taxon.pm

1;
