#!/usr/bin/perl

# created by rjp, 3/2004.
# Used to keep track of error messages to display to the user.


package Errors;

use strict;
use CGI::Carp qw(fatalsToBrowser);
use Globals;


use fields qw(	
				count
				errorString
				displayEndingMessage
			
				);  # list of allowable data fields.

						

sub new {
	my $class = shift;
	my Errors $self = fields::new($class);
	
	$self->{count} = 0;
	$self->{displayEndingMessage} = 1;

	return $self;
}


# adds an error with a bullet point to the list of error messages.
sub add {
	my Errors $self = shift;
	my $newError = shift;

	if ($newError) {
		$self->{errorString} .= "<LI>$newError</LI>\n";
		$self->{count} += 1;
	}
}

# returns a count of how many errors the user has added.
sub count {
	my Errors $self = shift;
	
	return $self->{count};
}

# pass this a boolean,
# should we display the ending message ("make corrections as necessary, etc.") or not?
sub setDisplayEndingMessage {
	my Errors $self = shift;	
	$self->{displayEndingMessage} = shift;
}

# returns the error message.
sub errorMessage {
	my Errors $self = shift;
	
	my $count = Globals::numberToName($self->{count});
	
	if ($self->{count} == 1) {
		$count = "error";
	} else {
		$count .= " errors";	
	}
	
	Debug::dbPrint("in errorMessage");
	
	my $errString = "<DIV class=\"errorMessage\">
				<UL STYLE=\"text-align:left;\"><DIV class=\"errorTitle\">Please fix the following $count</DIV>" . 
				$self->{errorString} . "</UL>
				<SPAN class=\"tiny\">(hint: if you get a formatting error, check for extra spaces in the fields)</SPAN><BR><BR>";

	if ($self->{displayEndingMessage}) { 
		$errString .= "Make corrections as necessary and resubmit the form.<BR>To cancel, use the back button on your browser.";
	}
	
	$errString .= "</DIV>";
		
	return $errString;
}

# pass this method another Error object, and it will append the
# new object onto the end of itself.
sub appendErrors {
	my Errors $self = shift;
	
	my $errorsToAppend = shift;
	
	if ($errorsToAppend) {
		$self->{errorString} .= $errorsToAppend->{errorString};
		$self->{count} += $errorsToAppend->{count};
	}
}

# end of Errors.pm


1;