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
	
		#my $validChars = '(?:[-A-Za-z"=0-9%._]|\s)*';

	#Debug::dbPrint("in = $in");
	$in =~ s/ < [\/]? (?:[-A-Za-z"=0-9%._]|\s)+ >//gx;

	#Debug::dbPrint("new in = $in");

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
	
	# note, this is a little complicated because each parameter may
	# be a scalar or a list.. If it's a list, evaluating it in a scalar 
	# context only returns the first element, so we need to evaluate it in a list context.
	
	my @params = $q->param();
	my @plist;
	my $i;
	foreach my $p (@params) {
		@plist = $q->param($p);
		
		$i = 0;
		foreach my $pItem (@plist) {
			$plist[$i] = clean($pItem);
			$i++;
		}
		
		$q->param($p => @plist);
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


# returns true if the input is in a proper format for a last name
# JA: added \W meaning any letter including accented ones 17.4.04
sub properLastName {
	my $input = shift;
	
	if ((!$input) || $input eq "") { return 0; }
	
	#Debug::dbPrint("properLastName($input) returns " . 
	
	if ($input !~ m/^[A-Za-z,-.\'][A-Za-z ,-.\'\W]*$/) {
		return 0;	
	}
	
	return 1;
}

# returns true if the input is in a proper format for an initial 
sub properInitial {
	my $input = shift;
	
	if ((!$input) || $input eq "") { return 0; }


	if ($input !~ m/\./) {
		return 0;
	}
	if ($input !~ m/^[A-Z][A-Za-z .-]*$/) {
		return 0;
	}
	
	return 1;
}

# returns true if it's a proper year format which begins with a 1 or 2,
# ie, 1900 and something or 2000 something.
#
# Also prevents the year from being older than 1700.
sub properYear {
	my $input = shift;
	
	if ((!$input) || $input eq "") { return 0; }
	
	if ($input !~ m/^[12]\d{3}$/) {
		return 0;
	}
	
	# prevent dates before 1700.
	if ($input < 1700) {
		return 0;
	}
	
	return 1;
}

# To be done: have better matches that take whether or not its informal into account
sub validOccurrenceGenus {
    my ($reso,$name) = @_;
    if ($reso =~ /informal/i) {
        return 1;
    }
    if ($name =~ /^[A-Za-z0-9 \-]+$/) {
        return 1;
    } else {
        return 0;
    }
}

sub validOccurrenceSpecies {
    my ($reso,$name) = @_;
    if ($reso =~ /informal/i) {
        return 1;
    }
    if ($name =~ /^[a-z0-9 \-\.'"]+$/) {
        return 1;
    } else {
        return 0;
    }
}

# Trivial function to check if an interval name is valid. Used in form checking
# two params = eml, interval name
sub checkInterval {
    my $dbt = shift || return;
    my $eml_interval = shift || "";
    my $interval_name = shift || "";
    my $result = 0;
    if ($interval_name ne "") {
        my $sql = "SELECT count(*) AS cnt FROM intervals WHERE interval_name=".$dbt->dbh->quote($interval_name);
        if ($eml_interval ne "") {
            $sql .= " AND eml_interval=".$dbt->dbh->quote($eml_interval);
        }
        my @results = @{$dbt->getData($sql)};
        if ($results[0]->{'cnt'} > 0) { $result = 1; }
    }
    if (!$result) {
        my @binnames = TimeLookup::getTenMYBins();
        for (@binnames) {
            if ($interval_name eq $_) {
                $result = 1; 
                last;
            }
        }
    }
    return $result;
}


1;
