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


# sets the occurrence
sub setWithReferenceNumber {
	my Reference $self = shift;
	
	if (my $input = shift) {
		$self->{reference_no} = $input;
		
		# get the pubyr and save it
		my $sql = $self->getSQLBuilder();
		$sql->setSQLExpr("SELECT reftitle, pubtitle, pubyr, pubvol, pubno, firstpage, lastpage FROM refs WHERE reference_no = $input");
		$sql->executeSQL();
		
		my $results = $sql->nextResultArrayRef();
		
		$self->{reftitle} = $results->[0];
		$self->{pubtitle} = $results->[1];
		$self->{pubyr} = $results->[2];
		$self->{pubvol} = $results->[3];
		$self->{pubno} = $results->[4];
		$self->{firstpage} = $results->[5];
		$self->{lastpage} = $results->[6];
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


# get all authors and year for reference
sub authors {
	my Reference $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $ref_no = $self->{reference_no};
	$sql->setSQLExpr("SELECT author1last, author2last, otherauthors FROM refs WHERE reference_no = $ref_no");
	$sql->executeSQL();
	
	my @result = $sql->nextResultArray();
	$sql->finishSQL();
	
	my $auth = $result[0];	# first author
	if ($result[2]) {	# we have other authors (implying more than two)
		$auth .= " et al."; 
	} elsif ($result[1]) {	# exactly two authors
		$auth .= " and $result[1]";
	}
	
	$auth .= " $self->{pubyr}";  # pubyr
		
	return $auth;
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
	
	my $html = "<SPAN class=\"smallRef\"><b>" . $self->referenceNumber() . "</b> ";
	$html .= $self->authors() . ". ";
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