# created by rjp, 1/2004.

package Reference;
use strict;
use AuthorNames;
use CGI::Carp;
use Data::Dumper;
use Class::Date qw(now date);
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD $HTML_DIR);

# Paths from the Apache environment variables (in the httpd.conf file).

use fields qw(reference_no
				reftitle
				pubtitle
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
	my Reference $self = fields::new($class);

    my $error_msg = "";

    if (!$reference_no) { 
        $error_msg = "Could not create Reference object with reference_no=undef."
    } else {
        my @fields = qw(reference_no reftitle pubtitle pubyr pubvol pubno firstpage lastpage author1init author1last author2init author2last otherauthors project_name);
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
	my Reference $self = shift;
    my $field = shift;

	return ($self->{$field});	
}

sub pages {
	my Reference $self = shift;
	
	my $p = $self->{'firstpage'};
	if ($self->{'lastpage'}) {
		$p .= "-" . $self->{'lastpage'};	
	}
	
	return $p;	
}

# get all authors and year for reference
sub authors {
	my Reference $self = shift;
    return formatShortRef($self);
}

# returns a nicely formatted HTML reference line.
sub formatAsHTML {
	my Reference $self = shift;
	
	if ($self->{reference_no} == 0) {
		# this is an error, we should never have a zero reference.
		return "no reference";	
	}
	
	my $html = $self->authors() . ". ";
	if ($self->{reftitle})	{ $html .= $self->{reftitle}; }
	if ($self->{pubtitle})	{ $html .= " <i>" . $self->{pubtitle} . "</i>"; }
	if ($self->{pubvol}) 	{ $html .= " <b>" . $self->{pubvol} . "</b>"; }
	if ($self->{pubno})		{ $html .= "<b>(" . $self->{pubno} . ")</b>"; }

	if ($self->pages())		{ $html .= ":" . $self->pages(); }
	
	return $html;
}

sub getReference {
    my $dbt = shift;
    my $reference_no = int(shift);

    if ($reference_no) {
        my $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.classification_quality,r.language,r.doi,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=$reference_no";
        my $ref = ${$dbt->getData($sql)}[0];
        return $ref;
    } else {
        return undef;
    }
    
}
# JA 16-17.8.02
# Moved and extended by PS 05/2005 to accept a number (reference_no) or hashref (if all the pertinent data has been grabbed already);
sub formatShortRef  {
    my $refData;
    my %options;
    if (UNIVERSAL::isa($_[0],'DBTransactionManager')) {
        my $dbt = shift;
        my $reference_no = int(shift);
        if ($reference_no) {
            my $sql = "SELECT reference_no,author1init,author1last,author2init,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$reference_no";
            $refData = ${$dbt->getData($sql)}[0];
        }
        %options = @_;
    } else {
        $refData = shift;
        %options = @_;
    }
    return if (!$refData);

    # stuff like Jr. or III often is in the last name fields, and for a short
    #  ref we don't care about it JA 18.4.07
    $refData->{'author1last'} =~ s/( Jr)|( III)|( II)//;
    $refData->{'author1last'} =~ s/\.$//;
    $refData->{'author1last'} =~ s/,$//;
    $refData->{'author2last'} =~ s/( Jr)|( III)|( II)//;
    $refData->{'author2last'} =~ s/\.$//;
    $refData->{'author2last'} =~ s/,$//;


    my $shortRef = "";
    $shortRef .= $refData->{'author1init'}." " if $refData->{'author1init'} && ! $options{'no_inits'};
    $shortRef .= $refData->{'author1last'};
    if ( $refData->{'otherauthors'} ) {
        $shortRef .= " et al.";
    } elsif ( $refData->{'author2last'} ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if($refData->{'author2last'} ne "et al."){
            $shortRef .= " and ";
        } else {
            $shortRef .= " ";
        }
        $shortRef .= $refData->{'author2init'}." " if $refData->{'author2init'} && ! $options{'no_inits'};
        $shortRef .= $refData->{'author2last'};
    }
    if ($refData->{'pubyr'}) {
        if ($options{'alt_pubyr'}) {
            $shortRef .= " (" . $refData->{'pubyr'} . ")"; 
        } else {
            $shortRef .= " " . $refData->{'pubyr'};
        }
    }

    if ($options{'show_comments'}) {
        if ($refData->{'comments'}) {
            $shortRef .= " [" . $refData->{'comments'}."]";
        }
    }
    if ($options{'is_recombination'}) {
        $shortRef = "(".$shortRef.")";
    }
    if ($options{'link_id'}) {
        if ($refData->{'reference_no'}) {
            $shortRef = qq|<a href="$READ_URL?action=displayReference&reference_no=$refData->{reference_no}">$shortRef</a>|;
        }
    }

    return $shortRef;
}

sub formatLongRef {
    my $ref;
    if (UNIVERSAL::isa($_[0],'DBTransactionManager')) {
        $ref = getReference(@_);
    } else {
        $ref = shift;
    }
    return if (!$ref);

    return "" if (!$ref);

    my $longRef = "";
    my $an = AuthorNames->new($ref);
	$longRef .= $an->toString();

	$longRef .= "." if $longRef && $longRef !~ /\.\Z/;
	$longRef .= " ";

	$longRef .= $ref->{'pubyr'}.". " if $ref->{'pubyr'};

	$longRef .= $ref->{'reftitle'} if $ref->{'reftitle'};
	$longRef .= "." if $ref->{'reftitle'} && $ref->{'reftitle'} !~ /\.\Z/;
	$longRef .= " " if $ref->{'reftitle'};

	$longRef .= "<i>" . $ref->{'pubtitle'} . "</i>" if $ref->{'pubtitle'};
	$longRef .= " " if $ref->{'pubtitle'};

	$longRef .= "<b>" . $ref->{'pubvol'} . "</b>" if $ref->{'pubvol'};

	$longRef .= "<b>(" . $ref->{'pubno'} . ")</b>" if $ref->{'pubno'};

	$longRef .= ":" if $ref->{'pubvol'} && ( $ref->{'firstpage'} || $ref->{'lastpage'} );

	$longRef .= $ref->{'firstpage'} if $ref->{'firstpage'};
	$longRef .= "-" if $ref->{'firstpage'} && $ref->{'lastpage'};
	$longRef .= $ref->{'lastpage'};
	# also displays authorizer and enterer JA 23.2.02
	$longRef .= "<span class=\"small\"> [".$ref->{'authorizer'}."/".
			   $ref->{'enterer'};
	if($ref->{'modifier'}){
		$longRef .= "/".$ref->{'modifier'};
	}
	$longRef .= "]</span>";
    return $longRef;
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

# This shows the actual references.
sub displayRefResults {
    my ($dbt,$q,$s,$hbo) = @_;

    my $type = $q->param('type');

	# use_primary is true if the user has clicked on the "Current reference" link at
	# the top or bottom of the page.  Basically, don't bother doing a complicated 
	# query if we don't have to.
    my ($data,$query_description) = ([],'');
	unless($q->param('use_primary')) {
		($data,$query_description) = getReferences($dbt,$q,$s,$hbo);
	} 
	my @data = @$data;

	if ((scalar(@data) == 1 && $type ne 'add') || $q->param('use_primary')) {
		# Do the action, don't show results...

		# Set the reference_no
		unless($q->param('use_primary') || $q->param('type') eq 'view') {
			$s->setReferenceNo( $data[0]->{'reference_no'});
		}

		# QUEUE
		my %queue = $s->unqueue();
		my $action = $queue{'action'};

		# Get all query params that may have been stuck on the queue
		# back into the query object:
		foreach my $key (keys %queue) {
			$q->param($key => $queue{$key});
		}

		# if there's an action, go straight back to it without showing the ref
		if ($action)	{
            main::execAction($action);
		} elsif ($q->param('type') eq 'edit') {  
            $q->param("reference_no"=>$data[0]->{'reference_no'});
            displayReferenceForm($dbt,$q,$s,$hbo);
		} elsif ($q->param('type') eq 'select') {  
            main::displayMenuPage();
        } else {
			# otherwise, display a page showing the ref JA 10.6.02
            displayReference($dbt,$q,$s,$hbo,$data[0]);
		}
		return;		# Out of here!
	} elsif ( scalar(@data) > 0 ) {
        # Needs to be > 0 for add -- case where its 1 is handled above explicitly
	    print $hbo->stdIncludes( "std_page_top" );
        # Print the sub header
        my $offset = (int($q->param('refsSeen')) || 0);
        my $limit = 30;
        print "<div align=\"center\"><p class=\"pageTitle\" style=\"margin-bottom: 1em;\">$query_description matched ";
        if (scalar(@data) > 1 && scalar(@data) > $limit) {
            print scalar(@data)." references</p><p class=\"medium\">Here are ";
            if ($offset == 0)	{
                print "the first $limit";
            } elsif ($offset + $limit > scalar(@data)) {
                print "the remaining ".(scalar(@data)-$offset)." references";
            } else	{
                print "references ",($offset + 1), " through ".($offset + $limit);
            }
            print "</p>";
        } elsif ( scalar(@data) == 1) {
            print "exactly one reference</p>";
        } else	{
            print scalar(@data)." references</p>";
        }
        print "</div>\n";
#        if ($type eq 'add') {
#            print "If the reference is not already in the system press \"Add reference.\"<br><br>";
#        } elsif ($type eq 'edit') {
#            print "Click the reference number to edit the reference<br><br>";
#        } elsif ($type eq 'select') {
#            print "Click the reference number to select the reference<br><br>";
#        } else {
#        }

		# Print the references found
        print "<div style=\"margin: 0.5em; border: 1px solid #E0E0E0;\">\n";
		print "<table border=0 cellpadding=5 cellspacing=0>\n";

        my $exec_url = ($type =~ /view/) ? $READ_URL : $WRITE_URL;

		# Only print the last 30 rows that were found JA 26.7.02
        for(my $i=$offset;$i < $offset + 30 && $i < scalar(@data); $i++) {
            my $row = $data[$i];
            if ( ($offset - $i) % 2 == 0 ) {
                print "<tr class=\"darkList\">";
            } else {
                print "<tr>";
            }
            print "<td valign=\"top\">";
            if ($s->isDBMember()) {
                if ($type eq 'add') {
                    print "<a href=\"$exec_url?action=displayReference&reference_no=$row->{reference_no}\">$row->{reference_no}</a>";
                } elsif ($type eq 'edit') {
                    print "<a href=\"$exec_url?action=displayRefResults&reference_no=$row->{reference_no}&type=edit\">$row->{reference_no}</a>";
                } elsif ($type eq 'view') {
                    print "<a href=\"$exec_url?action=displayReference&reference_no=$row->{reference_no}\">$row->{reference_no}</a><br>";
                } else {
                    print "<a href=\"$exec_url?action=displayRefResults&reference_no=$row->{reference_no}&type=select\">$row->{reference_no}</a><br>";
                }
            } else {
                print "<a href=\"$READ_URL?action=displayReference&reference_no=$row->{reference_no}\">$row->{reference_no}</a>";
            }
            print "</td>";
            my $formatted_reference = formatLongRef($row);
            print "<td>".$formatted_reference;
            if ( $type eq 'view' && $s->isDBMember() ) {
                print qq| <small><b><a href="$WRITE_URL?action=displayRefResults&type=edit&reference_no=$row->{reference_no}">edit</a></b></small>|;
            }
            my $reference_summary = getReferenceLinkSummary($dbt,$s,$row->{'reference_no'});
            print "<br><small>$reference_summary</small></td>";
            print "</tr>";
		}
		print "</table>\n";
        print "</div>";

        # Now print links at bottom
        print  "<center><p>";
        if ($offset + 30 < scalar(@data)) {
            my %vars = $q->Vars();
            $vars{'refsSeen'} += 30;
            my $old_query = "";
            foreach my $k (sort keys %vars) {
                $old_query .= "&$k=$vars{$k}" if $vars{$k};
            }
            $old_query =~ s/^&//;
            print qq|<a href="$exec_url?$old_query"><b>Get the next 30 references</b></a> - |;
        } 

        my $authname = $s->get('authorizer');
        $authname =~ s/\. //;
        printRefsCSV(\@data,$authname);
        print qq|<a href="/public/references/${authname}_refs.csv"><b>Download all the references</b></a> -\n|;
	    print qq|<a href="$exec_url?action=displaySearchRefs&type=$type"><b>Do another search</b></a>\n|;
	    print "</p></center><br>\n";
        
        if ($type eq 'add') {
            print "<div align=\"center\">";
            print "<form method=\"POST\" action=\"$WRITE_URL\">";
            print "<input type=\"hidden\" name=\"action\" value=\"displayReferenceForm\">";
            foreach my $f ("name","year","reftitle","project_name") {
                print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">";
            }
            print "<input type=submit value=\"Add reference\"></center>";
            print "</form>";
            print "</div>";
        }
	} else	{ # 0 Refs found
		if ($q->param('type') eq 'add')	{
            $q->param('reference_no'=>'');
			displayReferenceForm($dbt,$q,$s,$hbo);
			return;
		} else	{
			my $error = "<center><p class=\"medium\">Nothing matches $query_description</p>\n";
			displaySearchRefs($dbt,$q,$s,$hbo,$error);
			exit(0);
		}
	}



	print $hbo->stdIncludes("std_page_bottom");
}

sub displayReference {
    my ($dbt,$q,$s,$hbo,$ref) = @_;
    my $dbh = $dbt->dbh;

    if (!$ref) {
        $ref = getReference($dbt,$q->param('reference_no'));
    } 

    if (!$ref) {
        $hbo->htmlErr("Valid reference not supplied"); 
        return;
    }
    my $reference_no = $ref->{'reference_no'};

    
    # Create the thin line boxes
    my $box = sub { 
        my $html = '<div class="displayPanel" align="left" style="margin: 1em;">'
                 . qq'<span class="displayPanelHeader">$_[0]</span>'
                 . qq'<div class="displayPanelContent">'
                 . qq'<div class="displayPanelText">$_[1]'
                 . '</div></div></div>';
        return $html;
    };

	print $hbo->stdIncludes("std_page_top");
    print "<div align=\"center\"><p class=\"pageTitle\">" . formatShortRef($ref) . "</p></div>";

    my $citation = formatLongRef($ref);
    if ($s->isDBMember())	{
        $citation .= qq| <small><b><a href="$WRITE_URL?action=displayRefResults&type=edit&reference_no=$ref->{reference_no}">edit</a></b></small>|;
    }
    $citation = "<div style=\"text-indent: -0.75em; margin-left: 1em;\">" . $citation . "</div>";
    print $box->("Full reference",$citation);
   
    # Start Metadata box
    my $html = "<table border=0 cellspacing=0 cellpadding=0\">";
    $html .= "<tr><td class=\"fieldName\">ID number: </td><td>$reference_no</td></tr>";
    if ($ref->{'created'}) {
        $html .= "<tr><td class=\"fieldName\">Created: </td><td>$ref->{'created'}</td></tr>";
    }
    if ($ref->{'modified'}) {
        my $modified = date($ref->{'modified'});
        $html .= "<tr><td class=\"fieldName\">Modified: </td><td> $modified</td></tr>" unless ($modified eq $ref->{'created'});
    }
    if($ref->{'project_name'}) {
        $html .= "<tr><td class=\"fieldName\">Project name: </td><td>$ref->{'project_name'}";
        if ($ref->{'project_ref_no'}) {
            $html .= " $ref->{'project_ref_no'}";
        }
        $html .= "</td></tr>";
    }
    if($ref->{'publication_type'}) {
        $html .= "<tr><td class=\"fieldName\">Publication type: </td><td>$ref->{'publication_type'}</td></tr>";
    }
    if($ref->{'classification_quality'}) {
        $html .= "<tr><td class=\"fieldName\">Taxonomic classification quality: </td><td>$ref->{'classification_quality'}</td></tr>";
    }
    if($ref->{'language'}) {
        $html .= "<tr><td class=\"fieldName\">Language: </td><td>$ref->{'language'} </td></tr>";
    }
    if($ref->{'doi'}) {
        $html .= "<tr><td class=\"fieldName\">DOI: </td><td>$ref->{'doi'}</td></tr>";
    }
    if($ref->{'comments'}) {
        $html .= "<tr><td colspan=2><span class=\"fieldName\">Comments: </span> $ref->{'comments'}</td></tr>";
    }
    $html .= "</table>";
    if ($html) {
        print $box->("Metadata",$html);
    }

  
    my @uploads = @{$dbt->getData("SELECT upload_id,file_name,comments FROM uploads WHERE reference_no=$reference_no AND finished=1")};
    if (@uploads) {
        my $html = "";
        foreach my $row (@uploads) {
            $html .= "<a href=\"download.pl?what=upload&download_id=$row->{upload_id}\">$row->{file_name}</a> <br>$row->{comments}<br>";
            $html .= "<br>" if ($row->{comments}); 
        }
        $html =~ s/(<br>)*$//;

        print $box->("Downloads",$html);
    }


    # Get counts
    my $sql = "SELECT count(*) c FROM authorities WHERE reference_no=$reference_no";
    my $authority_count = ${$dbt->getData($sql)}[0]->{'c'};
    
    # TBD: scales, ecotaph, images, specimens/measurements, occs+reids

    # Handle taxon names box
    if ($authority_count) {
        my $html = "";
        if ($authority_count < 100) {
            my $sql = "SELECT taxon_no,taxon_name FROM authorities WHERE reference_no=$reference_no ORDER BY taxon_name";
            my @results = 
                map { qq'<a href="$READ_URL?action=checkTaxonInfo&taxon_no=$_->{taxon_no}">$_->{taxon_name}</a>' }
                @{$dbt->getData($sql)};
            $html = join(", ",@results);
        } else {
            $html .= qq|<b><a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no&display=authorities">|;
            my $plural = ($authority_count == 1) ? "" : "s";
            $html .= "view taxonomic name$plural";
            $html .= qq|</a></b> |;
        }
        print $box->(qq'Taxonomic names (<a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no">$authority_count</a>)',$html);
    }
    
    # Handle opinions box
    $sql = "SELECT count(*) c FROM opinions WHERE reference_no=$reference_no";
    my $opinion_count = ${$dbt->getData($sql)}[0]->{'c'};

    if ($opinion_count) {
        my $html = "";
        if ($opinion_count < 30) {
            my $sql = "SELECT opinion_no FROM opinions WHERE reference_no=$reference_no";
            my @results = 
                map {$_->[1] }
                sort { $a->[0] cmp $b->[0] }
                map { 
                    my $o = Opinion->new($dbt,$_->{'opinion_no'}); 
                    my $html = $o->formatAsHTML; 
                    my $name = $html;
                    $name =~ s/^'(<i>)?//; 
                    $name =~ s/(belongs |replaced |invalid subgroup |recombined |synonym | homonym | misspelled).*?$//; 
                    [$name,$html] }
                @{$dbt->getData($sql)};
            $html = join("<br>",@results);
        } else {
            $html .= qq|<b><a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no&display=opinions">|;
            if ($opinion_count) {
                my $plural = ($opinion_count == 1) ? "" : "s";
                $html .= "view taxonomic opinion$plural";
            }
            $html .= qq|</a></b> |;
        }
    
        my $class_link = qq| - <small><a href="$READ_URL?action=startProcessPrintHierarchy&amp;reference_no=$reference_no&amp;maximum_levels=100">view classification</a></small>|;
        print $box->(qq'Taxonomic opinions (<a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no">$opinion_count</a>) $class_link',$html);
    }      

    # Handle collections box
    $sql = "SELECT count(*) c FROM collections WHERE reference_no=$reference_no";
    my $collection_count = ${$dbt->getData($sql)}[0]->{'c'};
    $sql = "SELECT count(*) c FROM secondary_refs WHERE reference_no=$reference_no";
    $collection_count += ${$dbt->getData($sql)}[0]->{'c'}; 
    if ($collection_count) {
        my $html = "";
        if ($collection_count < 100) {
            # primary ref in first SELECT, secondary refs in second SELECT
            # the '1 is primary' and '0 is_primary' is a cool trick - alias the value 1 or 0 to column is_primary
            # any primary referneces will have a  virtual column called "is_primary" set to 1, and secondaries will not have it.  PS 04/29/2005
            my $sql = "(SELECT collection_no,authorizer_no,collection_name,access_level,research_group,release_date,DATE_FORMAT(release_date, '%Y%m%d') rd_short, 1 is_primary FROM collections where reference_no=$reference_no)";
            $sql .= " UNION ";
            $sql .= "(SELECT c.collection_no, c.authorizer_no, c.collection_name, c.access_level, c.research_group, release_date, DATE_FORMAT(c.release_date,'%Y%m%d') rd_short, 0 is_primary FROM collections c, secondary_refs s WHERE c.collection_no = s.collection_no AND s.reference_no=$reference_no) ORDER BY collection_no";

            my $sth = $dbh->prepare($sql);
            $sth->execute();

            my $p = Permissions->new($s,$dbt);
            my $results = [];
            if($sth->rows) {
                my $limit = 100;
                my $ofRows = 0;
                $p->getReadRows($sth,$results,$limit,\$ofRows);
            }

            foreach my $row (@$results) {
                my $style;
                if (! $row->{'is_primary'}) {
                    $style = " class=\"boring\"";
                }
                my $coll_link = qq|<a href="$READ_URL?action=displayCollectionDetails&collection_no=$row->{collection_no}" $style>$row->{collection_no}</a>|;
                $html .= $coll_link . ", ";
            }
            $html =~ s/, $//;
        } else {
            my $plural = ($collection_count == 1) ? "" : "s";
            $html .= qq|<b><a href="$READ_URL?action=displayCollResults&type=view&wild=N&reference_no=$reference_no">view collection$plural</a></b>|;
        }
        if ($html) {
            print $box->(qq'Collections (<a href="$READ_URL?action=displayCollResults&type=view&wild=N&reference_no=$reference_no">$collection_count</a>)',$html);
        }
    }
	print $hbo->stdIncludes("std_page_bottom");
}
# Shows the search form
# modified by rjp, 3/2004
# JA: Poling completely fucked this up and I restored it from backup 13.4.04
# $Message tells them why they are here
sub displaySearchRefs {
    my ($dbt,$q,$s,$hbo,$message) = @_;

	my $type = $q->param("type");

	my $html = "";

	# Prepend the message and the type

    my $vars = {'message'=>$message,'type'=>$type};
	# If we have a default reference_no set, show another button.
	# Don't bother to show if we are in select mode.
	my $reference_no = $s->get("reference_no");
	if ( $reference_no && $type ne "add" ) {
		$vars->{'use_current'} = "<input type='submit' name='use_current' value='Use current reference ($reference_no)'>\n";
	} 

	print $hbo->populateHTML("search_refs_form", $vars);
}

sub displayReferenceForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $reference_no = $q->param('reference_no');

    my $isNewEntry = ($reference_no > 0) ? 0 : 1;
    
    my %defaults = (
        'classification_quality'=>'standard',
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
    
    if ($isNewEntry) {
        $vars{"page_title"} = "New reference form";
    } else {
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
    # If classification quality is changed on an edit, the classifications that refer to that ref
    # may also change, so we may have to update the taxa cache in that case
    if ($reference_no) {
        my $sql = "SELECT classification_quality FROM refs WHERE reference_no=$reference_no";
        my $row = ${$dbt->getData($sql)}[0];
        if ($row) {
            if ($row->{'classification_quality'} ne $q->param('classification_quality')) {
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
        $vars{'classification_quality'} = 'compendium';
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
        my $ref = getReference($dbt,$reference_no);
        my $formatted_ref = formatLongRef($ref);

        # print a list of all the things the user should now do, with links to
        #  popup windows JA 28.7.06
        my $box_header = ($dupe || !$isNewEntry) ? "Full reference" : "New reference";
        print qq|
        <div class="displayPanel" align="left" style="margin: 1em;">
        <span class="displayPanelHeader"><b>$box_header</b></span>
        <table><tr><td valign=top>$formatted_ref <small><b><a href="$WRITE_URL?action=displayRefResults&type=edit&reference_no=$reference_no">edit</a></b></small></td></tr></table>
        </span>
        </div>|;
        

       
       
        if ($IS_FOSSIL_RECORD) {
        } else {
        print qq|</center>|;
        print qq|
        <div class="displayPanel" align="left" style="margin: 1em;">
        <span class="displayPanelHeader"><b>Please enter all the data</b></span>
        <div class="displayPanelContent">
        <ul class="small" style="text-align: left;">
            <li>Add or edit all the <a href="#" onClick="popup = window.open('$WRITE_URL?action=displayAuthorityTaxonSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">taxonomic names</a>, especially if they are new or newly combined
            <li>Add or edit all the new or second-hand <a href="#" onClick="popup = window.open('$WRITE_URL?action=displayOpinionSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">taxonomic opinions</a> about classification or synonymy
            <li>Edit <a href="#" onClick="popup = window.open('$WRITE_URL?action=displaySearchColls&type=edit', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">existing collections</a> if new details are given
            <li>Add all the <a href="#" onClick="popup = window.open('$WRITE_URL?action=displaySearchCollsForAdd', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">new collections</a>
            <li>Add all new <a href="#" onClick="popup = window.open('$WRITE_URL?action=displayOccurrenceAddEdit', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">occurrences</a> in existing or new collections
            <li>Add all new <a href="#" onClick="popup = window.open('$WRITE_URL?action=displayReIDCollsAndOccsSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">reidentifications</a> of existing occurrences
            <li>Add <a href="#" onClick="popup = window.open('$WRITE_URL?action=startStartEcologyTaphonomySearch', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">ecological/taphonomic data</a>, <a href="#" onClick="popup = window.open('$WRITE_URL?action=displaySpecimenSearchForm', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">specimen measurements</a>, and <a href="#" onClick="popup = window.open('$WRITE_URL?action=startImage', 'blah', 'left=100,top=100,height=700,width=700,toolbar=yes,scrollbars=yes,resizable=yes');">images</a>
        <ul>
        </div>
        </div>
        </center>|;
        }
    }
}

# JA 23.2.02
sub getReferenceLinkSummary {
    my ($dbt,$s,$reference_no) = @_;
    my $dbh = $dbt->dbh;
	my $retString = "";

    # Handle Authorities
    my $sql = "SELECT count(*) c FROM authorities WHERE reference_no=$reference_no";
    my $authority_count = ${$dbt->getData($sql)}[0]->{'c'};

    if ($authority_count) {
        $retString .= qq|<a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no">|;
        my $plural = ($authority_count == 1) ? "" : "s";
        $retString .= "$authority_count taxonomic name$plural";
        $retString .= qq|</a>, |;
    }
    
    # Handle Opinions
    $sql = "SELECT count(*) c FROM opinions WHERE reference_no=$reference_no";
    my $opinion_count = ${$dbt->getData($sql)}[0]->{'c'};

    if ($opinion_count) {
        $retString .= qq|<a href="$READ_URL?action=displayTaxonomicNamesAndOpinions&reference_no=$reference_no">|;
        if ($opinion_count) {
            my $plural = ($opinion_count == 1) ? "" : "s";
            $retString .= "$opinion_count taxonomic opinion$plural";
        }
        $retString .= qq|</a> (<a href="$READ_URL?action=startProcessPrintHierarchy&amp;reference_no=$reference_no&amp;maximum_levels=100">show classification</a>), |;
    }      

    # Handle Collections
	# make sure displayed collections are readable by this person JA 24.6.02


	# primary ref in first SELECT, secondary refs in second SELECT
    # the '1 is primary' and '0 is_primary' is a cool trick - alias the value 1 or 0 to column is_primary
    # any primary referneces will have a  virtual column called "is_primary" set to 1, and secondaries will not have it.  PS 04/29/2005
    $sql = "(SELECT collection_no,authorizer_no,collection_name,access_level,research_group,release_date,DATE_FORMAT(release_date, '%Y%m%d') rd_short, 1 is_primary FROM collections where reference_no=$reference_no)";
    $sql .= " UNION ";
    $sql .= "(SELECT c.collection_no, c.authorizer_no, c.collection_name, c.access_level, c.research_group, release_date, DATE_FORMAT(c.release_date,'%Y%m%d') rd_short, 0 is_primary FROM collections c, secondary_refs s WHERE c.collection_no = s.collection_no AND s.reference_no=$reference_no) ORDER BY collection_no";

    my $sth = $dbh->prepare($sql);
    $sth->execute();

	my $p = Permissions->new($s,$dbt);
    my $results = [];
	if($sth->rows) {
	    my $limit = 999;
	    my $ofRows = 0;
        $p->getReadRows($sth,$results,$limit,\$ofRows);
    }

    my $collection_count = scalar(@$results);
    if ($collection_count == 0) {
        $retString .= "no collections";
    } else {
        my $plural = ($collection_count == 1) ? "" : "s";
        $retString .= qq|<a href="$READ_URL?action=displayCollResults&type=view&wild=N&reference_no=$reference_no">$collection_count collection$plural</a>  (|;
        foreach my $row (@$results) {
            my $style;
            if (! $row->{'is_primary'}) {
                $style = " class=\"boring\"";
            }
            my $coll_link = qq|<a href="$READ_URL?action=displayCollectionDetails&collection_no=$row->{collection_no}" $style>$row->{collection_no}</a>|;
            $retString .= $coll_link . " ";
        }
        $retString .= ")";
    } 
    
	return $retString;
}



# Greg Ederer function that is our standard method for querying the refs table
# completely messed up by Poling 3.04 and restored by JA 10.4.04
sub getReferences {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;
    my %options = $q->Vars();

    if ($options{'use_current'}) {
        $options{'reference_no'} = $s->get('reference_no');
    }

	# build a string that will tell the user what they asked for
	my $query_description = '';

    my @where = ();
    if ($options{'reference_no'}) {
        push @where, "r.reference_no=".int($options{'reference_no'}) if ($options{'reference_no'});
        $query_description .= " reference ".$options{'reference_no'} 
    } else {
        if ($options{'name'}) {
            $query_description .= " ".$options{'name'};
            push @where,"(r.author1last LIKE ".$dbh->quote('%'.$options{'name'}.'%').
                        " OR r.author2last LIKE ".$dbh->quote('%'.$options{'name'}.'%').
                        " OR r.otherauthors LIKE ".$dbh->quote('%'.$options{'name'}.'%').')';
        }
        if ($options{'year'}) {
            push @where, "r.pubyr LIKE ".$dbh->quote($options{'year'});
            $query_description .= " ".$options{'year'};
        }
        if ($options{'reftitle'}) {
            push @where, "r.reftitle LIKE ".$dbh->quote('%'.$options{'reftitle'}.'%');
            $query_description .= " ".$options{'reftitle'};
        }
        if ($options{'pubtitle'}) {
            push @where, "r.pubtitle LIKE ".$dbh->quote('%'.$options{'pubtitle'}.'%');
            $query_description .= " ".$options{'pubtitle'};
        }
        if ($options{'project_name'}) {
            push @where, "FIND_IN_SET(".$dbh->quote($options{'project_name'}).",r.project_name)";
            $query_description .= " ".$options{'project_name'};
        }
        if ( $options{'authorizer_reversed'}) {
            push @where, "p1.name LIKE ".$dbh->quote(Person::reverseName($options{'authorizer_reversed'}));
            $query_description .= " authorizer ".$options{'authorizer_reversed'};
        }
        if ( $options{'enterer_reversed'}) {
            push @where, "p2.name LIKE ".$dbh->quote(Person::reverseName($options{'enterer_reversed'}));
            $query_description .= " enterer ".$options{'enterer_reversed'};
        }
    }

    if (@where) {
        my $tables = "(refs r, person p1, person p2)".
                     " LEFT JOIN person p3 ON p3.person_no=r.modifier_no";
        # This exact order is very important due to work around with inflexible earlier code
        my $from = "p1.name authorizer, p2.name enterer, p3.name modifier, r.reference_no, r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.publication_type,r.classification_quality,r.doi,r.comments,r.language,r.created,r.modified";
        my @join_conditions = ("r.authorizer_no=p1.person_no","r.enterer_no=p2.person_no");
        my $sql = "SELECT $from FROM $tables WHERE ".join(" AND ",@join_conditions,@where);
        my $orderBy = " ORDER BY ";
        my $refsortby = $options{'refsortby'};
        my $refsortorder = ($options{'refsortorder'} =~ /desc/i) ? "DESC" : "ASC"; 

        # order by clause is mandatory
        if ($refsortby eq 'year') {
            $orderBy .= "r.pubyr $refsortorder, ";
        } elsif ($refsortby eq 'publication') {
            $orderBy .= "r.pubtitle $refsortorder, ";
        } elsif ($refsortby eq 'authorizer') {
            $orderBy .= "p1.last_name $refsortorder, p1.first_name $refsortorder, ";
        } elsif ($refsortby eq 'enterer') {
            $orderBy .= "p2.last_name $refsortorder, p2.first_name $refsortorder, ";
        } elsif ($refsortby eq 'entry date') {
            $orderBy .= "r.reference_no $refsortorder, ";
        }
        
        if ($refsortby)	{
            $orderBy .= "r.author1last $refsortorder, r.author1init $refsortorder, r.pubyr $refsortorder";
        }

        # only append the ORDER clause if something is in it,
        #  which we know because it doesn't end with "BY "
        if ( $orderBy !~ /BY $/ )	{
            $orderBy =~ s/, $//;
            $sql .= $orderBy;
        }

        dbg("RefQuery SQL".$sql);
        
	    if ( $query_description ) { 
            $query_description =~ s/^\s*//;
            $query_description = "'$query_description' "; 
        }
        my @data = @{$dbt->getData($sql)};
	    return (\@data,$query_description);
	} else {
        my $type = $q->param('type');
        my $exec_url = ($type =~ /view/) ? $READ_URL : $WRITE_URL;
		my $error = "<center><p class=\"medium\">Please fill out at least one field</p>\n";
		displaySearchRefs($dbt,$q,$s,$hbo,$error);
		exit(0);
	}
}

sub getReferencesXML {
    my ($dbt,$q,$s,$hbo) = @_;
    require XML::Generator;

    my ($data,$query_description) = getReferences($dbt,$q,$s,$hbo);
    my @data = @$data;
    my $dataRowsSize = scalar(@data);

    my $g = XML::Generator->new(escape=>'always',conformance=>'strict',empty=>'args',pretty=>2);

    print "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" standalone=\"yes\"?>\n";
    print "<references total=\"$dataRowsSize\">\n";
    foreach my $row (@data) {
        my $an = AuthorNames->new($row);
        my $authors = $an->toString();

        my $pages = $row->{'firstpage'};
        if ($row->{'lastpage'} ne "") {
            $pages .= " - $row->{lastpage}";
        }

        # left out: authorizer/enterer, class. quality, language, doi, comments, project_name
        print $g->reference(
            $g->reference_no($row->{reference_no}),
            $g->authors($authors),
            $g->year($row->{pubyr}),
            $g->title($row->{reftitle}),
            $g->publication($row->{pubtitle}),
            $g->publication_volume($row->{pubvol}),
            $g->publication_no($row->{pubno}),
            $g->pages($pages),
            $g->publication_type($row->{publication_type})
        );
        print "\n";
    }
    print "</references>";
}
   
sub printRefsCSV {
    my @data = @{$_[0]};
    my $authname = $_[1];
    $authname =~ s/\. //;
    # Dump the refs to a flat file JA 1.7.02
    my $csv = Text::CSV_XS->new({'binary'=>1});
    PBDBUtil::autoCreateDir("$HTML_DIR/public/references");
    open REFOUTPUT,">$HTML_DIR/public/references/${authname}_refs.csv";

    my @fields = qw(authorizer enterer modifier reference_no author1init author1last author2init author2last otherauthors pubyr reftitle pubtitle pubvol pubno firstpage lastpage publication_type classification_quality language doi comments created modified); 
    if ($csv->combine(@fields)) {
        print REFOUTPUT $csv->string(),"\n";
    }
    for my $row (@data)	{
        my @row;
        foreach (@fields) {
            push @row, $row->{$_};
        }
        if ($csv->combine(@row))	{
            print REFOUTPUT $csv->string(),"\n";
        } else {
            print "ERR";
        }
    }
    close REFOUTPUT;

} 
# check for the presence of the nefarious V.J. Gupta or M.M. Imam
sub checkFraud {
    my $q = shift;
    dbg("checkFraud called". $q->param('author1last'));

    if ($q->param('reftitle') =~ /DATA NOT ENTERED: SEE (.*) FOR DETAILS/) {
        dbg("found gupta/imam bypassed by finidng data not entered");
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
