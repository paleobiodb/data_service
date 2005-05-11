#!/usr/bin/perl

# created by rjp, 1/2004.
# Represents information about a particular reference


package Reference;

use strict;
use DBI;
use DBConnection;
use DBTransactionManager;
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


# for internal use only
# gets author information for the reference
#
# pass it true to get initials, or 
# false to not get them..
sub internalGetAuthors {
	my Reference $self = shift;
	
	my $getInitials = shift;  # should we get initials or not?
	
	my $auth = Globals::formatAuthors($getInitials, $self->{author1init}, $self->{author1last}, $self->{author2init}, $self->{author2last}, $self->{otherauthors} );
	
	my $ref_no = $self->{reference_no};
	
	$auth .= " $self->{pubyr}";  # pubyr
		
	return $auth;
}


# get all authors and year for reference
sub authors {
	my Reference $self = shift;
	
	return $self->internalGetAuthors(0);  # no initials
}


# gets author names with initials for reference
sub authorsWithInitials {
	my Reference $self = shift;
	
	return $self->internalGetAuthors(1);  # initials
}


# returns a nicely formatted HTML reference line.
sub formatAsHTML {
	my Reference $self = shift;
	
	if ($self->{reference_no} == 0) {
		# this is an error, we should never have a zero reference.
		return "no reference";	
	}
	
	my $html = "<SPAN class=\"smallRef\"><b>" . $self->{'reference_no'} . "</b> ";
	$html .= $self->authorsWithInitials() . ". ";
	if ($self->{reftitle})	{ $html .= $self->{reftitle}; }
	if ($self->{pubtitle})	{ $html .= " <i>" . $self->{pubtitle} . "</i>"; }
	if ($self->{pubvol}) 	{ $html .= " <b>" . $self->{pubvol} . "</b>"; }
	if ($self->{pubno})		{ $html .= "<b>(" . $self->{pubno} . ")</b>"; }

	if ($self->pages())		{ $html .= ":" . $self->pages(); }
	$html .= "</SPAN>";
	
	return $html;
}


# JA 16-17.8.02
sub formatShortRef  {
    my $refDataRef = shift;
    my %refData = %{$refDataRef};

    my $shortRef = $refData{'author1init'} . " " . $refData{'author1last'};
    if ( $refData{'otherauthors'} ) {
        $shortRef .= " et al.";
    } elsif ( $refData{'author2last'} ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if($refData{'author2last'} ne "et al."){
            $shortRef .= " and ";
        } 
        $shortRef .= $refData{'author2init'} . " ". $refData{'author2last'};
    }
    $shortRef .= " " . $refData{'pubyr'} if ($refData{'pubyr'});

    return $shortRef;
}
# JA 16-17.8.02
sub formatRef  {
    my $refDataRef = shift;
    my %refData = %{$refDataRef};

    my $shortRef = $refData{'author1init'} . " " . $refData{'author1last'};
    if ( $refData{'otherauthors'} ) {
        $shortRef .= " et al.";
    } elsif ( $refData{'author2last'} ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if($refData{'author2last'} ne "et al."){
            $shortRef .= " and ";
        } 
        $shortRef .= $refData{'author2init'} . " ". $refData{'author2last'};
    }
    $shortRef .= " (" . $refData{'pubyr'}.")" if ($refData{'pubyr'});
    $shortRef .= " [" . $refData{'comments'}."]" if ($refData{'comments'});

    return $shortRef;
}

# end of Reference.pm


1;
