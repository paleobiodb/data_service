#!/usr/bin/perl

# created by rjp, 1/2004.
# Represents information about a particular reference


package Reference;

use strict;
use DBI;
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
	
	my $html = "<SPAN class=\"smallRef\"><b>" . $self->{'reference_no'} . "</b> ";
	$html .= $self->authors() . ". ";
	if ($self->{reftitle})	{ $html .= $self->{reftitle}; }
	if ($self->{pubtitle})	{ $html .= " <i>" . $self->{pubtitle} . "</i>"; }
	if ($self->{pubvol}) 	{ $html .= " <b>" . $self->{pubvol} . "</b>"; }
	if ($self->{pubno})		{ $html .= "<b>(" . $self->{pubno} . ")</b>"; }

	if ($self->pages())		{ $html .= ":" . $self->pages(); }
	$html .= "</SPAN>";
	
	return $html;
}


# JA 16-17.8.02
# Moved and extended by PS 05/2005 to accept a number (reference_no) or hashref (if all the pertinent data has been grabbed already);
sub formatShortRef  {
    my $refData = shift;
    return if (!$refData);

    my %options = @_;
    my $shortRef = "";


    $shortRef .= $refData->{'author1init'} . " " . $refData->{'author1last'};
    if ( $refData->{'otherauthors'} ) {
        $shortRef .= " et al.";
    } elsif ( $refData->{'author2last'} ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if($refData->{'author2last'} ne "et al."){
            $shortRef .= " and ";
        } 
        $shortRef .= $refData->{'author2init'} . " ". $refData->{'author2last'};
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
    if ($options{'link_id'}) {
        if ($refData->{'reference_no'}) {
            $shortRef = qq|<a href="bridge.pl?action=displayRefResults&no_set=1&reference_no=$refData->{reference_no}">$shortRef</a>|;
        }
    }

    return $shortRef;
}

1;
