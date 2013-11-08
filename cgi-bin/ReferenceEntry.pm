# includes entry functions extracted from Reference.pm JA 4.6.13

package ReferenceEntry;
use strict;
use AuthorNames;
use Class::Date qw(now date);
use Debug qw(dbg);
use Constants qw($WRITE_URL $IS_FOSSIL_RECORD $HTML_DIR $TAXA_TREE_CACHE $DB $COLLECTIONS $COLLECTION_NO $PAGE_TOP $PAGE_BOTTOM);
use Nexusfile;
# three calls to Reference functions will eventually need to be replaced
use Reference;

# Paths from the Apache environment variables (in the httpd.conf file).

use fields qw(reference_no
				reftitle
				pubtitle
				editors
				pubyr
				pubvol
				pubno
				firstpage
				lastpage
				project_name
				author1init
				author1last
				author2init
				author2last
				otherauthors
			
                dbt);  # list of allowable data fields.

						

sub new {
	my $class = shift;
    my $dbt = shift;
    my $reference_no = shift;
	my ReferenceEntry $self = fields::new($class);

    my $error_msg = "";

    if (!$reference_no) { 
        $error_msg = "Could not create Reference object with reference_no=undef."
    } else {
        my @fields = qw(reference_no reftitle pubtitle editors pubyr pubvol pubno firstpage lastpage author1init author1last author2init author2last otherauthors project_name);
        my $sql = "SELECT ".join(",",@fields)." FROM refs WHERE reference_no=".$dbt->dbh->quote($reference_no);
        my @results = @{$dbt->getData($sql)};
        if (@results) {
            foreach $_ (@fields) {
                $self->{$_}=$results[0]->{$_};
            }
        } else {
            $error_msg = "Could not create Reference object with reference_no=$reference_no."
        }
    }

    if ($error_msg) {
        my $cs = "";
        for(my $i=0;$i<10;$i++) {
            my ($package, $filename, $line, $subroutine) = caller($i);
            last if (!$package);
            $cs .= "$package:$line:$subroutine ";
        }
        $cs =~ s/\s*$//;
        $error_msg .= " Call stack is $cs.";
        carp $error_msg;
        return undef;
    } else {
        return $self;
    }
}

# return the referenceNumber
sub get {
	my ReferenceEntry $self = shift;
	my $field = shift;

	return ($self->{$field});	
}

sub pages {
	my ReferenceEntry $self = shift;
	
	my $p = $self->{'firstpage'};
	if ($self->{'lastpage'}) {
		$p .= "-" . $self->{'lastpage'};	
	}
	
	return $p;	
}

# get all authors and year for reference
sub authors {
	my ReferenceEntry $self = shift;
    return Reference::formatShortRef($self);
}


# Given an object representing a PaleoDB reference, return a representation of
# that reference in RIS format (text).

sub formatRISRef {
    
    my ($dbt, $ref) = @_;
    
    return '' unless ref $ref;
    
    my $output = '';
    my $refno = $ref->{reference_no};
    my $pubtype = $ref->{publication_type};
    my $reftitle = $ref->{reftitle} || '';
    my $pubtitle = $ref->{pubtitle} || '';
    my $pubyr = $ref->{pubyr} || '';
    my $misc = '';
    
    # First, figure out what type of publication the reference refers to.
    # Depending upon publication type, generate the proper RIS data record.
    
    if ( $pubtype eq 'journal article' or $pubtype eq 'serial monograph' )
    {
	$output .= risLine('TY', 'JOUR');
    }
    
    elsif ( $pubtype eq 'book chapter' or ($pubtype eq 'book/book chapter' and 
					   defined $ref->{editors} and $ref->{editors} ne '' ) )
    {
	$output .= risLine('TY', 'CHAP');
    }
    
    elsif ( $pubtype eq 'book' or $pubtype eq 'book/book chapter' or $pubtype eq 'compendium' 
	    or $pubtype eq 'guidebook' )
    {
	$output .= risLine('TY', 'BOOK');
    }
    
    elsif ( $pubtype eq 'serial monograph' )
    {
	$output .= risLine('TY', 'SER');
    }
    
    elsif ( $pubtype eq 'Ph.D. thesis' or $pubtype eq 'M.S. thesis' )
    {
	$output .= risLine('TY', 'THES');
	$misc = $pubtype;
    }
    
    elsif ( $pubtype eq 'abstract' )
    {
	$output .= risLine('TY', 'ABST');
    }
    
    elsif ( $pubtype eq 'news article' )
    {
	$output .= risLine('TY', 'NEWS');
    }
    
    elsif ( $pubtype eq 'unpublished' )
    {
	$output .= risLine('TY', 'UNPD');
    }
    
    else
    {
	$output .= risLine('TY', 'GEN');
    }
    
    # The following fields are common to all types:
    
    $output .= risLine('ID', "paleodb:ref:$refno");
    $output .= risLine('DB', "Paleobiology Database");
    
    $output .= risAuthor('AU', $ref->{author1last}, $ref->{author1init})  if $ref->{author1last};
    $output .= risAuthor('AU', $ref->{author2last}, $ref->{author2init}) if $ref->{author2last};
    $output .= risOtherAuthors('AU', $ref->{otherauthors}) if $ref->{otherauthors};
    $output .= risOtherAuthors('A2', $ref->{editors}) if $ref->{editors};
    
    $output .= risYear('PY', $pubyr) if $pubyr > 0;
    $output .= risLine('TI', $reftitle);
    $output .= risLine('T2', $pubtitle);
    $output .= risLine('M1', $misc) if $misc;
    $output .= risLine('VL', $ref->{pubvol}) if $ref->{pubvol};
    $output .= risLine('IS', $ref->{pubno}) if $ref->{pubno};
    $output .= risLine('PB', $ref->{publisher}) if $ref->{publisher};
    $output .= risLine('CY', $ref->{pubcity}) if $ref->{pubcity};
    
    if ( defined $ref->{refpages} and $ref->{refpages} ne '' )
    {
	if ( $ref->{refpages} =~ /^(\d+)-(\d+)$/ )
	{
	    $output .= risLine('SP', $1);
	    $output .= risLine('EP', $2);
	}
	else
	{
	    $output .= risLine('SP', $ref->{refpages});
	}
    }
    
    else
    {
	$output .= risLine('SP', $ref->{firstpage}) if $ref->{firstpage};
	$output .= risLine('EP', $ref->{lastpage}) if $ref->{lastpage};
    }
    
    $output .= risLine('N1', $ref->{comments}) if defined $ref->{comments} and $ref->{comments} ne '';
    $output .= risLine('LA', $ref->{language}) if defined $ref->{language} and $ref->{language} ne '';
    $output .= risLine('DO', $ref->{doi}) if defined $ref->{doi} and $ref->{doi} ne '';
    
    $output .= risLine('ER');
    
    return $output;
}


# Generate an arbitrary line in RIS format, given a tag and a value.  The value
# may be empty.

sub risLine {
    
    my ($tag, $value) = @_;
    
    $value ||= '';
    $tag = "\nTY" if $tag eq 'TY';
    
    return "$tag  - $value\n";
}


# Generate an "author" line in RIS format, given a tag (which may be 'AU' for
# author, 'A2' for editor, etc.), and the three components of a name: last,
# first, and suffix.  The first and suffix may be null.

sub risAuthor {
    
    my ($tag, $last, $init, $suffix) = @_;
    
    $init ||= '';
    $init =~ s/ //g;
    $suffix ||= '';
    
    # If the last name includes a suffix, split it out
    
    if ( $last =~ /^(.*),\s*(.*)/ or $last =~ /^(.*)\s+(jr.?|iii|iv)$/i or $last =~ /^(.*)\s+\((jr.?|iii|iv)\)$/ )
    {
	$last = $1;
	$suffix = $2;
	if ( $suffix =~ /^([sSjJ])/ ) { $suffix = $1 . "r."; }
    }
    
    # Generate the appropriate line, depending upon which of the three components
    # are non-empty.
    
    if ( $suffix ne '' )
    {
	return "$tag  - $last,$init,$suffix\n";
    }
    
    elsif ( $init ne '' )
    {
	return "$tag  - $last,$init\n";
    }
    
    else
    {
	return "$tag  - $last\n";
    }
}


# Generate a "date" line in RIS format, given a tag and year, month and day
# values.  The month and day values may be null.  An optional "other" value
# may also be included, which can be arbitrary text.

sub risYear {

    my ($tag, $year, $month, $day, $other) = @_;
    
    my $date = sprintf("%04d", $year + 0) . "/";
    
    $date .= sprintf("%02d", $month + 0) if defined $month and $month > 0;
    $date .= "/";
    
    $date .= sprintf("%02d", $day + 0) if defined $day and $day > 0;
    $date .= "/";
    
    $date .= $other if defined $other;
    
    return "$tag  - $date\n";
}


# Generate one or more "author" lines in RIS format, given a tag and a value
# which represents one or more names separated by commas.  This is a bit
# tricky, because we need to split out name suffixes such as 'jr' and 'iii'.
# If we come upon something we can't handle, we generate a line whose value is
# 'PARSE ERROR'.

sub risOtherAuthors {

    my ($tag, $otherauthors) = @_;
    
    $otherauthors =~ s/^\s+//;
    
    my $init = '';
    my $last = '';
    my $suffix = '';
    my $output = '';
    
    while ( $otherauthors =~ /[^,\s]/ )
    {
	if ( $otherauthors =~ /^(\w\.)\s*(.*)/ )
	{
	    $init .= $1;
	    $otherauthors = $2;
	}
	
	elsif ( $otherauthors =~ /^([^,]+)(?:,\s+(.*))?/ )
	{
	    $last = $1;
	    $otherauthors = $2;
	    
	    if ( $otherauthors =~ /^(\w\w+\.?)(?:,\s+(.*))$/ )
	    {
		$suffix = $1;
		$otherauthors = $2;
	    }
	    
	    $output .= risAuthor($tag, $last, $init, $suffix);
	    $init = $last = $suffix = '';
	}
	
	else
	{
	    $output .= risLine($tag, "PARSE ERROR");
	    last;
	}
    }
    
    return $output;
}


sub getSecondaryRefs {
    my $dbt = shift;
    my $collection_no = int(shift);
    
    my @refs = ();
    if ($collection_no) {
        my $sql = "SELECT sr.reference_no FROM secondary_refs sr, refs r WHERE sr.reference_no=r.reference_no AND sr.collection_no=$collection_no ORDER BY r.author1last, r.author1init, r.author2last, r.pubyr";
        foreach my $row (@{$dbt->getData($sql)}) {
            push @refs, $row->{'reference_no'};
        }
    }
    return @refs;
}

sub displayReferenceForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $reference_no = $q->param('reference_no');

    my $isNewEntry = ($reference_no > 0) ? 0 : 1;
    
    my %defaults = (
        'basis'=>'stated without evidence',
        'language'=>'English'
    );

    if ($IS_FOSSIL_RECORD) {
        $defaults{'pubtitle'} = "Fossil Record 3";
    }

    my %db_row = ();
    if (!$isNewEntry) {
	    my $sql = "SELECT * FROM refs WHERE reference_no=$reference_no";
        my $row = ${$dbt->getData($sql)}[0];
        %db_row= %{$row};
    }

    my %form_vars;

	# Pre-populate the form with the search terms:
	if ( $isNewEntry )	{
		%form_vars = $q->Vars();
		delete $form_vars{'reftitle'};
		delete $form_vars{'pubtitle'};
		my %query_hash = ("name" => "author1last",
					  "year" => "pubyr",
					  "project_name" => "project_name");

		foreach my $s_param (keys %query_hash){
			if($form_vars{$s_param}) {
				$form_vars{$query_hash{$s_param}} = $form_vars{$s_param};
			}
		}
	}

	# Defaults, then database, then a resubmission/form data
	my %vars = (%defaults,%db_row,%form_vars);

	if ($isNewEntry)	{
		$vars{"page_title"} = "New reference form";
	} else	{
		$vars{"page_title"} = "Reference number $reference_no";
	}
	print $hbo->populateHTML('js_reference_checkform');
	print $hbo->populateHTML("enter_ref_form", \%vars);
}

#  * Will either add or edit a reference in the database
sub processReferenceForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;
    my $reference_no = int($q->param('reference_no'));
    
    my $isNewEntry = ($reference_no > 0) ? 0 : 1;
    
    my @child_nos = ();
    # If taxonomy basis is changed on an edit, the classifications that refer to that ref
    # may also change, so we may have to update the taxa cache in that case
    if ($reference_no) {
        my $sql = "SELECT basis FROM refs WHERE reference_no=$reference_no";
        my $row = ${$dbt->getData($sql)}[0];
        if ($row) {
            if ($row->{'basis'} ne $q->param('basis')) {
                my $sql = "SELECT DISTINCT child_no FROM opinions WHERE reference_no=$reference_no";
                my @results = @{$dbt->getData($sql)};
                foreach my $row (@results) {
                    push @child_nos, $row->{'child_no'};
                }
            }
        }
    }


    # Set the pubtitle to the pull-down pubtitle unless it's set in the form
    $q->param(pubtitle => $q->param('pubtitle_pulldown')) unless $q->param("pubtitle");
    
    my %vars = $q->Vars();


    if ($IS_FOSSIL_RECORD && $isNewEntry) {
        $vars{'publication_type'} = 'book/book chapter';              
        $vars{'language'} = 'English';
        $vars{'basis'} = 'second hand';
        $vars{'project_name'} = 'fossil record';
    } elsif ($IS_FOSSIL_RECORD) {
        # do not edit this value
        delete $vars{'project_name'};
    }
    
    my $fraud = checkFraud($q);
    if ($fraud) {
        if ($fraud eq 'Gupta') {
            print qq|<center><p class="medium"><font color='red'>WARNING: Data published by V. J. Gupta have been called into question by Talent et al. 1990, Webster et al. 1991, Webster et al. 1993, and Talent 1995. Please hit the back button, copy the comment below to the reference title, and resubmit.  Do NOT enter
any further data from the reference.<br><br> "DATA NOT ENTERED: SEE |.$s->get('authorizer').qq| FOR DETAILS"|;
            print "</font></p></center>\n";
        } else {
            print qq|<center><p class="medium"><font color='red'>WARNING: Data published by M. M. Imam have been called into question by <a href='http://www.up.ac.za/organizations/societies/psana/Plagiarism_in_Palaeontology-A_New_Threat_Within_The_Scientific_Community.pdf'>J. Aguirre 2004</a>. Please hit the back button, copy the comment below to the reference title, and resubmit.  Do NOT enter any further data from the reference.<br><br> "DATA NOT ENTERED: SEE |.$s->get('authorizer').qq| FOR DETAILS"|;
        }
        return;
    }
    
    # Suppress fields that do not match publication type
    
    if ( $vars{publication_type} eq 'unpublished' )
    {
	delete $vars{publisher};
	delete $vars{pubcity};
    }
    
    my ($dupe,$matches);
    
    if ($isNewEntry) {
        $dupe = $dbt->checkDuplicates('refs',\%vars);
#    my $matches = $dbt->checkNearMatch('refs','reference_no',$q,5,"something=something?");
        $matches = 0;
    }

    if ($dupe) {
        $reference_no = $dupe;
        print "<div align=\"center\">".Debug::printWarnings("This reference was not entered since it is a duplicate of reference $reference_no")."</div>";
    } elsif ($matches) {
        # Nothing to do, page generation and form processing handled
        # in the checkNearMatch function
	} else {
        if ($isNewEntry) {
            my ($status,$ref_id) = $dbt->insertRecord($s,'refs', \%vars);
            $reference_no = $ref_id;
        } else {
            my $status = $dbt->updateRecord($s,'refs','reference_no',$reference_no,\%vars);
        }
	}

    my $verb = ($isNewEntry) ? "added" : "updated";
    if ($dupe) {
        $verb = "";
    }
    print "<center><p class=\"pageTitle\">Reference number $reference_no $verb</p></center>";

    # Set the reference_no
    if ($reference_no) {
        $s->setReferenceNo($reference_no);
        my $ref = Reference::getReference($dbt,$reference_no);
        my $formatted_ref = Reference::formatLongRef($ref);

        # print a list of all the things the user should now do, with links to
        #  popup windows JA 28.7.06
        my $box_header = ($dupe || !$isNewEntry) ? "Full reference" : "New reference";
        print qq|
        <div class="displayPanel" align="left" style="margin: 1em;">
        <span class="displayPanelHeader">$box_header</span>
        <table><tr><td valign=top>$formatted_ref <small><a href="$WRITE_URL?a=displayRefResults&type=edit&reference_no=$reference_no">edit</a></small></td></tr></table>
        </span>
        </div>|;
        

       
       
	print qq|</center>
        <div class="displayPanel" align="left" style="margin: 1em;">
        <span class="displayPanelHeader">Please enter all the data</span>
        <div class="displayPanelContent large">
|;
	print qq|
        <ul class="small" style="text-align: left;">
            <li>Add or edit all the <a href="#" onClick="popup = window.open('$WRITE_URL?a=displayAuthorityTaxonSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">taxonomic names</a>, especially if they are new or newly combined
            <li>Add or edit all the new or second-hand <a href="#" onClick="popup = window.open('$WRITE_URL?a=displayOpinionSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">taxonomic opinions</a> about classification or synonymy
	    <li>Add <a href="#" onClick="popup = window.open('$WRITE_URL?a=uploadNexusFile', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">phylogenetic character matrices</a> describing these taxa
            <li>Edit <a href="#" onClick="popup = window.open('$WRITE_URL?a=displaySearchColls&type=edit', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">existing collections</a> if new details are given
            <li>Add all the <a href="#" onClick="popup = window.open('$WRITE_URL?a=displaySearchCollsForAdd', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">new collections</a>
            <li>Add all new <a href="#" onClick="popup = window.open('$WRITE_URL?a=displayOccurrenceAddEdit', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">occurrences</a> in existing or new collections
            <li>Add all new <a href="#" onClick="popup = window.open('$WRITE_URL?a=displayReIDCollsAndOccsSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">reidentifications</a> of existing occurrences
            <li>Add <a href="#" onClick="popup = window.open('$WRITE_URL?a=startStartEcologyTaphonomySearch', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">ecological/taphonomic data</a>, <a href="#" onClick="popup = window.open('$WRITE_URL?a=displaySpecimenSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">specimen measurements</a>, and <a href="#" onClick="popup = window.open('$WRITE_URL?a=startImage', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">images</a>
        <ul>
|;
	print "</div>\n";
	
	print qq|
<form method="POST" action="$WRITE_URL">
<input type="hidden" name="action" value="displayRefResults">
<input type="hidden" name="reference_no" value="$reference_no">
<input type="submit" value="Use this reference">
</form>
|;	
	print "</div>\n</center>\n";
    }
}

sub checkFraud {
    my $q = shift;
    dbg("checkFraud called". $q->param('author1last'));

    if ($q->param('reftitle') =~ /DATA NOT ENTERED: SEE (.*) FOR DETAILS/) {
        dbg("found gupta/imam bypassed by finding data not entered");
        return 0;
    }
    
    if ($q->param('author1init') =~ /V/i &&
        $q->param('author1last') =~ /^Gupta/i)  {
        dbg("found gupta in author1");    
        return 'Gupta';
    }
    if ($q->param('author1init') =~ /M/i &&
        $q->param('author1last') =~ /^Imam/i)  {
        dbg("found imam in author1");    
        return 'Imam';
    }
    if ($q->param('otherauthors') =~ /V[J. ]+Gupta/i) {
        dbg("found gupta in other authors");    
        return 'Gupta';
    }
    if ($q->param('otherauthors') =~ /M[M. ]+Imam/i) {
        dbg("found imam in other authors");    
        return 'Imam';
    }
    if ($q->param('author2init') =~ /V/i &&
        $q->param('author2last') =~ /^Gupta/i)  {
        dbg("found gupta in author2");    
        return 'Gupta';
    }
    if ($q->param('author2init') =~ /M/i &&
        $q->param('author2last') =~ /^Imam/i)  {
        dbg("found imam in author2");    
        return 'Imam';
    }
    return 0;
}

1;
