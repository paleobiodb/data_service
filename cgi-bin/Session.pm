package Session;


require Exporter;
@ISA = qw(Exporter);

use DBI;
use CookieFactory;
use Class::Date qw(date localdate gmdate now);
use CGI::Carp qw(fatalsToBrowser);

use Debug;
use Globals;
use Constants;
use DBTransactionManager;


# I THINK THESE ARE BOGUS. THERE ARE NO SUCH METHODS/VARIABLES IN THIS MODULE
@EXPORT = qw(setName setHref toHTML);
@EXPORT_OK = qw($test_val);

my $sql;

sub new {
  my $class = shift;
  my $self = {};
  bless $self, $class;

  return $self;
}



# Processes the login from the submitted authorizer/enterer names.
# Creates a session_data table row if the login is valid.
#
# modified by rjp, 3/2004.
sub processLogin {

	my $self = shift;
	my $dbh = shift;
	my $q = shift;
	my $valid = 0;

	# First do some housekeeping
	# This cleans out ALL records in session_data older than 48 hours.
	$self->houseCleaning( $dbh );

	my $enterer = $q->param('enterer');
	my $authorizer = $q->param('authorizer');

	# We want them to specify both an authorizer and an enterer
	# otherwise kick them out to the public site.
	if (!(authorizer && enterer)) {
		return "";
	}
	
	# also check that both names exist in the database.
	my $person = Person->new();
	if (! ($person->isValidName($enterer) &&
		$person->isValidName($authorizer)) ) {
		return '';
	}
	
	# need to reverse the login name.
	# added by rjp, 2/17/2004 to work correctly with the new login page.
	my $auth = $q->param('authorizer');
	my $ent = $q->param('enterer');
	
	# basically takes the part after the comma and puts it in the front..
	# so "Sepkoski, J." becomes "J. Sepkoski"
	if ($auth =~ /,/) {
		$auth =~ s/^\s*(.*)\s*,\s*(.*)\s*$/\2 \1/;
		$q->param(authorizer => $auth);
	}
	
	if ($ent =~ /,/) {
		$ent =~ s/^\s*(.*)\s*,\s*(.*)\s*$/\2 \1/;
		$q->param(enterer => $ent);
	}
	

	# Get info from database on this authorizer.
	$sql =	"SELECT * FROM person " .
			 "WHERE name = '".$q->param("authorizer")."' ";
	
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	
	$sth->execute();
	
	if ( $sth->rows ) {

		my $rs = $sth->fetchrow_hashref ( );

		# Check the password
		my $password = $rs->{password};
		my $plaintext = $rs->{plaintext};

		# First try the plain text version
		if ( $plaintext eq $q->param("password") && $plaintext ne "") { 
			$valid = 1; # VALID LOGIN!
			# If that worked but there is still an old encrypted password,
			#   zorch that version to make sure it is never used again
			#   JA 12.6.02
			if ($password ne "")	{
				$sql =	"UPDATE person SET password = '' ".
						"WHERE person_no = ".$rs->{person_no};
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
			}
		# If that didn't work and there is no plain text password,
		#   try the old encrypted password
		} elsif ($plaintext eq "") {
			# Legacy: Test the encrypted password
			# For encrypted passwords
			my $salt = substr ( $password, 0, 2);
			my $encryptedPassword = crypt ( $q->param("password"), $salt );

			if ( $password eq $encryptedPassword ) {
				$valid = 1; # VALID LOGIN!
				# Mysteriously collect their plaintext password
				$sql =	"UPDATE person SET plaintext = '".$q->param("password")."' ".
						" WHERE person_no = ".$rs->{person_no};
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
			}
		}

		# If valid, do some stuff
		if ( $valid ) {

			my $cf = CookieFactory->new();

			# Create a unique ID (22 chars)
			$session_id = $cf->getUniqueID();

			# Make it into a formatted cookie string
			my $cookie = $cf->buildSessionId ( $session_id );

			# Store the session id (for later)
			$self->{session_id} = $session_id;

			# Are they superuser?
			my $superuser = 0;
			if ( $rs->{superuser} && $rs->{is_authorizer} && $q->param("enterer") eq $q->param("authorizer") ) { $superuser = 1; }

			# Store the session data into the db
			# The research groups are stored so as not to do many db lookups

			# first clean up the names to avoid the O'Regan bug
			#   JA 24.8.03
			my $authorizer = $q->param("authorizer");
			my $enterer = $q->param("enterer");
			$authorizer =~ s/'/\\'/g;
			$enterer =~ s/'/\\'/g;

			
			# record the authorizer_no and enterer_no
			$sth = $dbh->prepare("SELECT person_no FROM person WHERE name = ?");

			$sth->execute($authorizer);
			my @results = $sth->fetchrow_array();
			$self->{authorizer_no} = $results[0];
			
			$sth->execute($enterer);
			my @results = $sth->fetchrow_array();
			$self->{enterer_no} = $results[0];
			
			$sth->finish();
			
			
			# record the authorizer and enterer names
			$self->{authorizer} = $authorizer;
			$self->{enterer} = $enterer;

			
			# Insert all of the session data into a row in the session_data table
			# so we will still have access to it the next time the user tries to do something.
			#
			$sql =	"INSERT INTO session_data ( 
						session_id, authorizer, authorizer_no, 
						enterer, enterer_no, 
						superuser, marine_invertebrate, 
						paleobotany, taphonomy, vertebrate )
						
					VALUES (   
					'$session_id', '$authorizer', '" . $self->{authorizer_no} . "',
					'$enterer', '" . $self->{enterer_no} . "' , 
					'$superuser', " .	
					$rs->{marine_invertebrate} . ", " . $rs->{paleobotany} . ", " .
					$rs->{taphonomy} . ", " . $rs->{vertebrate} . " ) ";
					
			$dbh->do( $sql ) || die ( "$sql<HR>$!" );

			$sth->finish ( );
			
	
			return $cookie;
		}
	}
	return "";
}


# Handles the Guest login.  No password required.
# Anyone who passes through this routine becomes guest.
sub processGuestLogin {

	my $self = shift;
	my $dbh = shift;
	my $q = shift;

	my $session_id = $q->cookie("session_id");
	if ( $session_id ) {
		# Who are they?
		$sql = "SELECT * FROM session_data WHERE session_id='$session_id'";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		$sth->execute();
		if ( $sth->rows ) {

			my $rs = $sth->fetchrow_hashref ( );
			if ( $rs->{authorizer} ne "Guest" ) {
				# Log out this non-guest enterer
				$sql = "DELETE FROM session_data WHERE session_id='$session_id'";
				$dbh->do( $sql );
			} else {

				# Validated GUEST

				# Store some values (for later)
				$self->{session_id} = $session_id;
			# ERROR (EXTRA UNUSED KEYS): this stores keys and values
			# SHOULD BE:  foreach my $field ( keys(%{$rs}) ) {
				foreach my $field ( %{$rs} ) {
					$self->{$field} = $rs->{$field};
				}
				return $session_id;
			}
		}
	}

	my $cf = CookieFactory->new();

	# Create a unique ID (22 chars)
	$session_id = $cf->getUniqueID();

	# Make it into a formatted cookie string
	my $cookie = $cf->buildSessionId ( $session_id );

	# Store the session id (for later)
	$self->{session_id} = $session_id;

	# Store the session data into the db
	$sql =	"INSERT INTO session_data ( ".
			"	session_id, ".
			"	authorizer, ".
			"	enterer ".
			"	) VALUES ( ".
			"'$session_id', ".
			"'Guest', ".	
			"'Guest' ".	
			" ) ";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );

	# A few other goodies
	$self->{authorizer} = "Guest";
	$self->{enterer} = "Guest";

	return $cookie;
}

# Ensures the user is logged in.
sub validateUser {

	my $self = shift;
	my $dbh = shift;
	my $session_id = shift;

	if ( $session_id ) {

		# Ensure their session_id corresponds to a valid database entry
		$sql = "SELECT * FROM session_data WHERE session_id='$session_id'";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		# execute returns number of rows affected for NON-select statements
		# and true/false (success) for select statements.
		if($sth->execute()){
			my $rs = $sth->fetchrow_hashref();
			
			if($rs->{session_id} eq $session_id){

				# Store some values (for later)
				$self->{session_id} = $session_id;
				foreach my $field ( keys %{$rs} ) {
					$self->{$field} = $rs->{$field};
				}
				# now update the session_data record to the current time
				$sql = "UPDATE session_data set record_date=NULL ".
					   "WHERE session_id='$session_id'";
				$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
				if($sth->execute() == 1){
					return $session_id;
				}
				else{
					die "$sth->errstr";
				}
			}
		}
	}
	return "";
}

# Cleans stale entries from the session_data table.
# 48 hours is the current time considered
sub houseCleaning {
	my $self = shift;
	my $dbh = shift;

	# COULD ALSO USE 'DATE_SUB'
	$sql = 	"DELETE FROM session_data ".
			" WHERE record_date < DATE_ADD( now(), INTERVAL -2 DAY)";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );

	# COULD ALSO USE 'DATE_SUB'
	# Nix the Guest users @ 1 day
	$sql = 	"DELETE FROM session_data ".
			" WHERE record_date < DATE_ADD( now(), INTERVAL -1 DAY) ".
			"	AND authorizer = 'Guest' ";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	1;
}

# Sets the reference_no
sub setReferenceNo {
	my $self = shift;
	my $dbh = shift;
	my $reference_no = shift;

	if ( $reference_no ) {
		$sql =	"UPDATE session_data ".
				"	SET reference_no = $reference_no ".
				" WHERE session_id = '".$self->get("session_id")."'";
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );

		# Update our reference_no
		$self->{reference_no} = $reference_no;
	}
}

# A destination is used for procedural requests.  For 
# example, when requesting a ReID and you need to select
# a reference first.
sub enqueue {
	my $self = shift;
	my $dbh = shift;
	my $queue = shift;
	my $current_contents = "";

	# Get the current contents
	$sql =	"SELECT queue ".
			"  FROM session_data ".
			" WHERE session_id = '".$self->get("session_id")."'";
    my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();

	if ( $sth->rows ) {
		my $rs = $sth->fetchrow_hashref ( );
		$current_contents = $rs->{queue};
	} 
	$sth->finish();

	# If there was something, tack it on the front of the queue
	if ( $current_contents ) { $queue = $queue."|".$current_contents; }

	$sql =	"UPDATE session_data ".
			"	SET queue = '$queue' ".
			" WHERE session_id = '".$self->get("session_id")."'";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );
}

# Pulls an action off the queue
sub unqueue {
	my $self = shift;
	my $dbh = shift;
	my $queue = "";
	my %hash = {};

	$sql =	"SELECT queue ".
			"  FROM session_data ".
			" WHERE session_id = '".$self->get("session_id")."'";
    my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();

	my $rs = $sth->fetchrow_hashref ( );
	$queue = $rs->{queue};

	if ( $queue ) {

		# Parse the contents on the | delimiter
		my @entries = split ( /\|/, $queue );

		# Take off the first one
		$entry = shift ( @entries );

		# Write the rest out
		$queue = join ( "|", @entries );
		$sql =	"UPDATE session_data ".
				"	SET queue = '$queue' ".
				" WHERE session_id = '".$self->get("session_id")."'";
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );

		# Parse the entry.  Since it is any valid URL, use the CGI routine.
		my $cgi = CGI->new ( $entry );

		# Return it as a hash
		my @names = $cgi->param();
		foreach $field ( @names ) {
			$hash{$field} = $cgi->param($field);
		}
		# Save entire line in case we want it
		$hash{queue} = $queue;
	}

	$sth->finish();

	return %hash;
}




sub clearQueue {
	my $self = shift;
	my $dbh = shift;

	$sql =	"UPDATE session_data ".
			"	SET queue = NULL ".
			" WHERE session_id = '".$self->get("session_id")."'";
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );
}

# Puts a variable into memory
sub put {
	my $self = shift;
	my $key = shift;
	my $value = shift;

	$self->{$key} = $value;	
	$stateChanged = 1;	# Unsure what this is --tone
}

# Gets a variable from memory
sub get {
	my $self = shift;
	my $key = shift;

	return $self->{$key};
}

# pass this a person number and it 
# will return the person's name
sub personNameForNumber {
	my $self = shift;
	
	my $num = shift;
	
	if (! $num) {
		return '';
	}
	
	my $sql = DBTransactionManager->new();
	my @result = $sql->getSingleSQLResult("SELECT name FROM person WHERE person_no = '$num'");
	
	if (@result) {
		return $result[0];	
	}
	
	return '';
}

sub authorizerNumber {
	my $self = shift;
	
	return $self->{'authorizer_no'};
}

sub entererNumber {
	my $self = shift;
	
	return $self->{'enterer_no'};
}


# returns the current reference number
sub currentReference {
	my $self = shift;
	return $self->{reference_no};
}

# Is the current user superuser?  This is true
# if the authorizer is alroy and the enterer is alroy.  
sub isSuperUser {
	my $self = shift;

	if ( ($self->{authorizer} eq Globals::god()) && 
	($self->{enterer} eq Globals::god())) {
		return 1;	
	}
	
	return 0;
}


# Tells if we are guest or not
sub guest {
	my $self = shift;

	return ( $self->{authorizer} eq "Guest" );
}


# rjp, 3/2004
# This was made a more generic function since it may be used my multiple
# forms - currently this is just the authority and opinion forms (as of 3/24/2004)
#
# Pass this two aguments:
# First is a boolean which is true if they're editing a *NEW* record.  This is
# usually called $isNewEntry or something like that.
#
# Second argument is the authorizer_no of the record, if it already exists.
#
# Returns a boolean - true if they can edit any field in this form, and false
# otherwise.
sub editAnyFormField {
	my $self = shift;
	
	my $isNewEntry = shift;
	my $authorizer_no = shift;
	
	my $editAny = FALSE;
	
	if ($isNewEntry) {
		$editAny = TRUE;	# new entries can edit any field.
	} else {
		# edits of pre-existing records have more restrictions. 
	
		# if the authorizer of the record doesn't match the current
		# authorizer, then *only* let them edit empty fields.
		$editAny = FALSE;
	
		# if the authorizer of the record matches the authorizer
		# who is currently trying to edit this data, then allow them to change
		# any field.
		if ($self->authorizerNumber() == $authorizer_no) {
			$editAny = TRUE;
		}
		
		if ($self->isSuperUser()) {
			# super user can edit any field no matter what.
			$editAny = TRUE;	
		}
	}
	
	return $editAny;
}



# returns a string of all keys and values set in this
# session.  intended for debugging purposes
#
# rjp, 2/2004
sub allKeysAndValues() {
	my $self = shift;
	
	my %hash = %$self;
	
	my @keys = keys(%hash);
		
	my $result;
	foreach my $k (@keys) {
		@list = $hash{$k}; 
		$result .= "$k = " . "'" . join(", ", @list) . "'\n";	
	}
	
	return $result;
}


1;
