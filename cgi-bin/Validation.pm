#!/usr/bin/perl

# Written by rjp, 1/2004.
#
# Used to validate data entered by a user, for example, in HTML forms.
# Can also be used to clean up data from a user (remove weird characters, etc.)

#
# Each validate routine expects a to receive two parameters:
# 1.  a string to verify
# 2.  a maximum allowable length for that string
# 
# If no maximum length is passed, then it defaults to ANY length (ie, no max)
#

#
# The clean routine cleans the passed string by removing/escaping all
# questionable characters such as <,>*/|\~`", etc.

package Validation;
use strict;


# pass this a string and it returns a "cleaned" copy of it.
# basically removes any funny characters which shouldn't be in the database,
# escapes single quote marks (JA didn't want double quotes escaped),
# and also removes HTML code such as italics <i>
sub clean {
	my $in = shift;
	
	# remove html code
	# looks for "<" followed by optional "/" followed by one or more
	# characters which *aren't* "<" or ">" followed by ">".  This pretty
	# much defines an HTML tag, so this should remove all of them.
	
	$in =~ s/<[\/]?[^<>]+>//g;

	# comment out the escape quote marks for now..
	# because it would be an issue if some other code tried to escape them again... 
	
	# escape quote marks.
	# don't escape quotes which are already escaped...
	#$in =~ s/([^\]['])|(^['])/\\'/g;
	
	return $in;
}


# pass this a CGI object, normally called $q, and it will
# go through and run the clean() function on every parameter in it.
#
# doesn't return anything.
sub cleanCGIParams {
	my $q = shift;
	
	if (! $q) { return; }
	
	my @params = $q->param();
	foreach my $p (@params) {
		$q->param($p => clean($q->param($p)));		
		#dbPrint("$p = " . $q->param($p));	
	}
}


# this routine is meant for internal use only
# pass it a string and a length, it returns true if the string is <= that length.
sub lenCheck {
	my $string = shift;
	my $maxLen = shift;
	if (($maxLen) && (length($string) > $maxLen)) {
		return 0;
	}
	
	return 1;
}


sub isNumeric {
	my $string = shift;
	my $maxLen = shift;
	my $result;
	
	# starts with optional + or - followed by one or more digits
	# followed by optional decimal point and more digits (also optional)
	# OR
	# starts with decimal point followed by one or more digits
	$result = ($string =~ m/(^[-+]?\d+([.](\d+)?)?$)|(^[.]\d+$)/);
	$result &= (lenCheck($string, $maxLen));
	
	return $result;
}


sub isInteger {
	my $string = shift;
	my $maxLen = shift;
	my $result;
	
	$result = ($string =~ m/^\d+$/ );
	$result &= (lenCheck($string, $maxLen));
	
	return $result;
}



1;