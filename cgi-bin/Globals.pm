# some global functions which can be used by any other module.
# mostly written by rjp.

package Globals;

use strict;
use Debug;





# who is the "god" user (ie, full access to everything?)
# This is an inline function, so it should be fast.
sub god () {
	return 'J. Alroy';	
}


# pass this a warning message and it will print it directly
# to the web page.  Note, you still need to add the standard page 
# header and footer before and after this.
sub printWarning {
	my $warning = shift;

	print "<CENTER><H3><FONT COLOR='red'>Warning:</FONT> $warning</H3></CENTER>\n";
}


# Pass this a CGI object.
# Returns a hashref of all parameters in the CGI object.
#
sub copyCGIToHash {
	my $q = shift;
	
	my %hash;
	
	if (!$q) {
		return \%hash;
	}
	
	foreach my $p ($q->param()) {
		$hash{$p} = $q->param($p);	
	}
	
	return \%hash;
}

# pass this a number like "5" and it will return the name ("five").
# only works for numbers up through 19.  Above that and it will just return
# the original number.
#
sub numberToName {
	my $num = shift;
	
	my %numtoname = (  "0" => "zero", "1" => "one", "2" => "two",
                         "3" => "three", "4" => "four", "5" => "five",
                         "6" => "six", "7" => "seven", "8" => "eight",
                         "9" => "nine", "10" => "ten",
                         "11" => "eleven", "12" => "twelve", "13" => "thirteen",
						 "14" => "fourteen", "15" => "fifteen", "16" => "sixteen",
						 "17" => "seventeen", "18" => "eighteen", "19" => "nineteen");
	
	my $name;
	
	if ($num < 20) {
		$name = $numtoname{$num};
	} else {
		$name = $num;	
	}
	
	return $name;
}


# pass it an array ref and a scalar
# loops through the array to see if the scalar is a member of it.
# returns true or false value.
sub isIn {
	my $arrayRef = shift;
	my $val = shift;
	
	# if they don't exist
	if ((!$arrayRef) || (!$val)) {
		return 0;
	}
	
	foreach my $k (@$arrayRef) {
		if ($val eq $k) {
			return 1;	
		}
	}
	
	
	return 0;
}


# pass it an arrayRef of values
# it returns true if each value is an empty string (or zero),
# and false if any of them are not empty (or zero).
sub isEmpty {
	my $ref = shift;
	
	if (!$ref) {
		return 1;  # it's empty if they don't pass anything!	
	}
	my @ary = @$ref;
	foreach my $v (@ary) {
		if (($v ne '') || ($v != 0)) {
			return 0;
		}
	}
	
	return 1;
}


# Pass this the following arguments:
# printinitials 	: 0 = don't print, 1 = print
# author1init
# author1last
# author2init
# author2last
# otherauthors
#
# It will return a properly formatted author name
# list, ie, putting the "and", "et al.", in the right place.
# 
# The first three arguments are required - the rest are optional.
# 
# rjp, 3/2004.
sub formatAuthors {
	my $printInitials = shift;
	my $author1init = shift;
	my $author1last = shift;
	my $author2init = shift;
	my $author2last = shift;
	my $otherauthors = shift;
	
	my $auth = $author1last;	# first author

	if ($printInitials) {
		$auth = $author1init . " " . $auth;	# first author
	}
	
	if ($otherauthors) {	# we have other authors (implying more than two)
		$auth .= " et al."; 
	} elsif ($author2last) {	# exactly two authors
		$auth .= " and ";
		
		if ($printInitials) {
			$auth .= $author2init . " ";
		}
			
		$auth .= $author2last;
	}
		
	return $auth;
}


# pass this a hashref, and it will return a new hashref
# which contains a copy of the hash refererred to by the first hashref.
#
# Note, I think there's a better way to do this..
#
# rjp, 3/2004
sub copyHash {
    my $ref = shift;
    my %copy;
    
    if (!$ref) { return \%copy; }
    
    foreach my $key (keys(%$ref)) {
        $copy{$key} = $ref->{$key};  
    }
  
    return \%copy;  
}


# Pass this an array ref and 
# an element to delete.
#
# It will search through the array and
# delete *any* and all occurrences of the passed
# element if it finds them. 
#
# It returns a reference to a new array but *doesn't* modify the original.
#
# Does a string compare (eq)
sub deleteElementFromArray {
	my $ref = shift;
	my $toDelete = shift;
	
	my @newArray;
	
	foreach my $element (@$ref) {
		if ($element ne $toDelete) {
			push(@newArray, $element);
		}
	}
	
	return \@newArray;
}


1;


