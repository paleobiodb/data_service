#!/usr/bin/perl

# created by rjp, 1/2004.
# Represents information about a particular reference


package Reference;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use URLMaker;
use CGI::Carp qw(fatalsToBrowser);


use fields qw(	reference_no
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
				
				SQLBuilder
							);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Reference $self = fields::new($class);
	
	# set up some default values
	#$self->clear();	

	return $self;
}


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
sub getSQLBuilder {
	my Reference $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
	}
	
	return $SQLBuilder;
}


# sets it with the reference number
sub setWithReferenceNumber {
	my Reference $self = shift;
	
	if (my $input = shift) {
		$self->{reference_no} = $input;
		
		# get the pubyr and save it
		my $sql = $self->getSQLBuilder();
		$sql->setSQLExpr("SELECT reftitle, pubtitle, pubyr, pubvol, pubno, firstpage, lastpage, author1init, author1last, author2init, author2last, otherauthors FROM refs WHERE reference_no = $input");
		$sql->executeSQL();
		
		my $results = $sql->nextResultArrayRef();
		
		$self->{reftitle} = $results->[0];
		$self->{pubtitle} = $results->[1];
		$self->{pubyr} = $results->[2];
		$self->{pubvol} = $results->[3];
		$self->{pubno} = $results->[4];
		$self->{firstpage} = $results->[5];
		$self->{lastpage} = $results->[6];
		
		$self->{author1init} = $results->[7];
		$self->{author1last} = $results->[8];
		$self->{author2init} = $results->[9];
		$self->{author2last} = $results->[10];
		$self->{otherauthors} = $results->[11];
	}
}


# return the referenceNumber
sub referenceNumber {
	my Reference $self = shift;

	return ($self->{reference_no});	
}

# return the publication year for this reference
sub pubyr {
	my Reference $self = shift;
	return ($self->{pubyr});
}

# return the reference title for this reference
sub reftitle {
	my Reference $self = shift;
	return ($self->{reftitle});
}


# return the publication title for this reference
sub pubtitle {
	my Reference $self = shift;
	return ($self->{pubtitle});
}

# return the publication volume for this reference
sub pubvol {
	my Reference $self = shift;
	return ($self->{pubvol});
}
# return the publication number for this reference
sub pubno {
	my Reference $self = shift;
	return ($self->{pubno});
}
# return the publication first page for this reference
sub firstpage {
	my Reference $self = shift;
	return ($self->{firstpage});
}
# return the publication last page for this reference
sub lastpage {
	my Reference $self = shift;
	return ($self->{lastpage});
}

sub pages {
	my Reference $self = shift;
	
	my $p = $self->{firstpage};
	if ($self->{lastpage}) {
		$p .= "-" . $self->{lastpage};	
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
	
	#my $auth = $self->{author1last};	# first author

	#if ($getInitials) {
	#	$auth = $self->{author1init} . " " . $auth;	# first author
	#}
	
	#if ($self->{otherauthors}) {	# we have other authors (implying more than two)
	#	$auth .= " et al."; 
	#} elsif ($self->{author2last}) {	# exactly two authors
	#	$auth .= " and ";
		
	#	if ($getInitials) {
	#		$auth .= $self->{author2init} . " ";
	#	}
			
	#	$auth .= $self->{author2last};
	#}
	
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


# returns a reference URL
sub referenceURL {	
	my Reference $self = shift;

	my $url = URLMaker::URLForReferenceNumber($self->{reference_no});
	my $authors = $self->authors();
	
	return ("<A HREF=\"$url\">$authors</A>");
	
}

# returns a nicely formatted HTML reference line.
sub formatAsHTML() {
	my Reference $self = shift;
	
	if ($self->{reference_no} == 0) {
		# this is an error, we should never have a zero reference.
		return "no reference";	
	}
	
	my $html = "<SPAN class=\"smallRef\"><b>" . $self->referenceNumber() . "</b> ";
	$html .= $self->authorsWithInitials() . ". ";
	if ($self->{reftitle})	{ $html .= $self->{reftitle}; }
	if ($self->{pubtitle})	{ $html .= " <i>" . $self->{pubtitle} . "</i>"; }
	if ($self->{pubvol}) 	{ $html .= " <b>" . $self->{pubvol} . "</b>"; }
	if ($self->{pubno})		{ $html .= "<b>(" . $self->{pubno} . ")</b>"; }

	if ($self->pages())		{ $html .= ":" . $self->pages(); }
	$html .= "</SPAN>";
	
	return $html;
}


# end of Reference.pm


1;