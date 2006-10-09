# created by rjp, 1/2004.

package Reference;
use strict;
use AuthorNames;
use CGI::Carp;
use Data::Dumper;

use fields qw(
				reference_no
				reftitle
				pubtitle
				pubyr
				pubvol
				pubno
				firstpage
				lastpage
				
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

    if (!$reference_no) { 
        carp "Could not create Reference object with $reference_no";
        return undef; 
    }
    my @fields = qw(reference_no reftitle pubtitle pubyr pubvol pubno firstpage lastpage author1init author1last author2init author2last otherauthors);
	my $sql = "SELECT ".join(",",@fields)." FROM refs WHERE reference_no=".$dbt->dbh->quote($reference_no);
    my @results = @{$dbt->getData($sql)};
    if (@results) {
        foreach $_ (@fields) {
            $self->{$_}=$results[0]->{$_};
        }
        return $self;
    } else {
        carp "Could not create Reference object with $reference_no";
        return undef; 
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
        my $sql = "SELECT p1.name authorizer,p2.name enterer,p3.name modifier,r.reference_no,r.author1init,r.author1last,r.author2init,r.author2last,r.otherauthors,r.pubyr,r.reftitle,r.pubtitle,r.pubvol,r.pubno,r.firstpage,r.lastpage,r.created,r.modified,r.publication_type,r.classification_quality,r.language,r.comments,r.project_name,r.project_ref_no FROM refs r LEFT JOIN person p1 ON p1.person_no=r.authorizer_no LEFT JOIN person p2 ON p2.person_no=r.enterer_no LEFT JOIN person p3 ON p3.person_no=r.modifier_no WHERE r.reference_no=$reference_no";
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

    my $shortRef = "";
    $shortRef .= $refData->{'author1init'}." " if $refData->{'author1init'} && ! $options{'no_inits'};
    $shortRef .= $refData->{'author1last'};
    if ( $refData->{'otherauthors'} ) {
        $shortRef .= " et al.";
    } elsif ( $refData->{'author2last'} ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if($refData->{'author2last'} ne "et al."){
            $shortRef .= " and ";
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
            $shortRef = qq|<a href="bridge.pl?action=displayReference&reference_no=$refData->{reference_no}">$shortRef</a>|;
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

1;
