package Session;

use Digest::MD5;
use CGI::Cookie;
use strict;

# Handles validation of the user
sub new {
    my ($class,$dbt,$session_id) = @_;
    my $dbh = $dbt->dbh;
    my $self;

	if ($session_id) {
		# Ensure their session_id corresponds to a valid database entry
		my $sql = "SELECT * FROM session_data WHERE session_id=".$dbh->quote($session_id)." LIMIT 1";
		my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
		# execute returns number of rows affected for NON-select statements
		# and true/false (success) for select statements.
		$sth->execute();
        my $rs = $sth->fetchrow_hashref();
        if($rs) {
            # Store some values (for later)
            foreach my $field ( keys %{$rs} ) {
                $self->{$field} = $rs->{$field};
            }
            # These are used in lots of places (anywhere with a 'Me' button), convenient to create here
            my $authorizer_reversed = $rs->{'authorizer'};
            $authorizer_reversed =~ s/^\s*([^\s]+)\s+([^\s]+)\s*$/$2, $1/;
            my $enterer_reversed = $rs->{'enterer'};
            $enterer_reversed =~ s/^\s*([^\s]+)\s+([^\s]+)\s*$/$2, $1/;
            $self->{'authorizer_reversed'} = $authorizer_reversed;
            $self->{'enterer_reversed'} = $enterer_reversed;    

            # Update the person data
            my $sql = "UPDATE person SET last_action=NOW() WHERE person_no=$self->{enterer_no}";
            $dbh->do( $sql ) || die ( "$sql<HR>$!" );

            # now update the session_data record to the current time
            $sql = "UPDATE session_data SET record_date=NULL WHERE session_id=".$dbh->quote($session_id);
            $dbh->do($sql);
            $self->{'logged_in'} = 1;
        } else {
            $self->{'logged_in'} = 0;
        }
	} else {
        $self->{'logged_in'} = 0;
    }
    $self->{'dbt'} = $dbt;
    bless $self, $class;
    return $self;
}


# Processes the login from the submitted authorizer/enterer names.
# Creates a session_data table row if the login is valid.
#
# modified by rjp, 3/2004.
sub processLogin {
	my $self = shift;
	my $authorizer  = shift;
    my $enterer = shift;
    my $password = shift;

    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;
    
	my $valid = 0;


	# First do some housekeeping
	# This cleans out ALL records in session_data older than 48 hours.
	$self->houseCleaning( $dbh );


	# We want them to specify both an authorizer and an enterer
	# otherwise kick them out to the public site.
	if (!$authorizer || !$enterer || !$password) {
		return '';
	}

	# also check that both names exist in the database.
	if (! Person::checkName($dbt,$enterer) || ! Person::checkName($dbt,$authorizer)) {
		return '';
	}

    my ($sql,@results,$authorizer_row,$enterer_row);
	# Get info from database on this authorizer.
	$sql =	"SELECT * FROM person WHERE name=".$dbh->quote($authorizer);
    @results =@{$dbt->getData($sql)};
    $authorizer_row  = $results[0];

	# Get info from database on this enterer.
	$sql =	"SELECT * FROM person WHERE name=".$dbh->quote($enterer);
    @results =@{$dbt->getData($sql)};
    $enterer_row  = $results[0];

    if ($authorizer_row) {
		# Check the password
		my $db_password = $authorizer_row->{'password'};
		my $plaintext = $authorizer_row->{'plaintext'};

		# First try the plain text version
		if ( $plaintext && $plaintext eq $password) {
			$valid = 1; 
			# If that worked but there is still an old encrypted password,
			#   zorch that version to make sure it is never used again
			#   JA 12.6.02
			if ($db_password ne "")	{
				$sql =	"UPDATE person SET password='' WHERE person_no = ".$authorizer_row->{'person_no'};
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
			}
		# If that didn't work and there is no plain text password,
		#   try the old encrypted password
		} elsif ($plaintext eq "") {
			# Legacy: Test the encrypted password
			# For encrypted passwords
			my $salt = substr ( $db_password, 0, 2);
			my $encryptedPassword = crypt ( $password, $salt );

			if ( $db_password eq $encryptedPassword ) {
				$valid = 1; 
				# Mysteriously collect their plaintext password
				$sql =	"UPDATE person SET password='',plaintext=".$dbh->quote($password).
						" WHERE person_no = ".$authorizer_row->{person_no};
				$dbh->do( $sql ) || die ( "$sql<HR>$!" );
			}
		}

		# If valid, do some stuff
		if ( $valid ) {
			my $session_id = $self->buildSessionID();

            my $cookie = new CGI::Cookie(
                -name    => 'session_id',
                -value   => $session_id, 
                -expires => '+1y',
                -path    => "/",
                -secure  => 0);

			# Store the session id (for later)
			$self->{session_id} = $session_id;

			# Are they superuser?
			my $superuser = 0;
			if ( $authorizer_row->{'superuser'} && 
                 $authorizer_row->{'is_authorizer'} && 
                 $authorizer eq $enterer) {
                 $superuser = 1; 
            }

			# Insert all of the session data into a row in the session_data table
			# so we will still have access to it the next time the user tries to do something.
            my %row = ('session_id'=>$session_id,
                       'authorizer'=>$authorizer_row->{'name'},
                       'authorizer_no'=>$authorizer_row->{'person_no'},
                       'enterer'=>$enterer_row->{'name'},
                       'enterer_no'=>$enterer_row->{'person_no'},
                       'superuser'=>$superuser,
                       'marine_invertebrate'=>$authorizer_row->{'marine_invertebrate'}, 
                       'micropaleontology'=>$authorizer_row->{'micropaleontology'},
                       'paleobotany'=>$authorizer_row->{'paleobotany'},
                       'taphonomy'=>$authorizer_row->{'taphonomy'},
                       'vertebrate'=>$authorizer_row->{'vertebrate'});

            # Copy to the session objet
            while (my ($k,$v) = each %row) {
                $self->{$k} = $v;
            }
           
            my $keys = join(",",keys(%row));
            my $values = join(",",map { $dbh->quote($_) } values(%row));
            
			$sql =	"INSERT INTO session_data ($keys) VALUES ($values)";
			$dbh->do( $sql ) || die ( "$sql<HR>$!" );
	
			return $cookie;
		}
	}
	return "";
}


# Handles the Guest login.  No password required.
# Anyone who passes through this routine becomes guest.
sub processGuestLogin {
	my $self = shift;
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;
    my $name = shift;

    my $session_id = $self->buildSessionID();

    my $cookie = new CGI::Cookie(
        -name    => 'session_id',
        -value   => $session_id
        -expires => '+1y',
        -domain  => '',
        -path    => "/",
        -secure  => 0);

    # Store the session id (for later)
    $self->{session_id} = $session_id;

    # The research groups are stored so as not to do many db lookups
    $self->{enterer_no} = 0;
    $self->{enterer} = $name;
    $self->{authorizer_no} = 0;
    $self->{authorizer} = $name;
    
    # Insert all of the session data into a row in the session_data table
    # so we will still have access to it the next time the user tries to do something.
    #
    my %row = ('session_id'=>$session_id,
               'authorizer'=>$self->{'authorizer'},
               'authorizer_no'=>$self->{'authorizer_no'},
               'enterer'=>$self->{'enterer'},
               'enterer_no'=>$self->{'enterer_no'});
   
    my $keys = join(",",keys(%row));
    my $values = join(",",map { $dbh->quote($_) } values(%row));
    
    my $sql = "INSERT INTO session_data ($keys) VALUES ($values)";
    $dbh->do( $sql ) || die ( "$sql<HR>$!" );

    return $cookie;
}

sub buildSessionID {
  my $self = shift;
  my $md5 = Digest::MD5->new();
  my $remote = $ENV{REMOTE_ADDR} . $ENV{REMOTE_PORT};
  # Concatenates args: epoch, this interpreter PID, $remote (above)
  # returned as base 64 encoded string
  my $id = $md5->md5_base64(time, $$, $remote);
  # replace + with -, / with _, and = with .
  $id =~ tr|+/=|-_.|;
  return $id;
}

# Cleans stale entries from the session_data table.
# 48 hours is the current time considered
sub houseCleaning {
	my $self = shift;
    my $dbh = $self->{'dbt'}->dbh;

	# COULD ALSO USE 'DATE_SUB'
	my $sql = 	"DELETE FROM session_data ".
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

	if ($reference_no =~ /^\d+$/) {
		my $sql =	"UPDATE session_data ".
				"	SET reference_no = $reference_no ".
				" WHERE session_id = ".$dbh->quote($self->get("session_id"));
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
	my $sql =	"SELECT queue ".
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
	my %hash = ();

	my $sql =	"SELECT queue ".
			"  FROM session_data ".
			" WHERE session_id = '".$self->get("session_id")."'";
    my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
	my $rs = $sth->fetchrow_hashref();
	$sth->finish();
	$queue = $rs->{queue};

	if ( $queue ) {

		# Parse the contents on the | delimiter
		my @entries = split ( /\|/, $queue );

		# Take off the first one
		my $entry = shift ( @entries );

		# Write the rest out
		$queue = join ( "|", @entries );
		$sql =	"UPDATE session_data ".
				"	SET queue=".$dbh->quote($queue).
				" WHERE session_id=".$dbh->quote($self->{'session_id'});
		$dbh->do( $sql ) || die ( "$sql<HR>$!" );

		# Parse the entry.  Since it is any valid URL, use the CGI routine.
		my $cgi = CGI->new ( $entry );

		# Return it as a hash
		my @names = $cgi->param();
		foreach my $field ( @names ) {
			$hash{$field} = $cgi->param($field);
		}
		# Save entire line in case we want it
		$hash{'queue'} = $queue;
	} 

	return %hash;
}

sub clearQueue {
	my $self = shift;
	my $dbh = shift;

	my $sql =	"UPDATE session_data ".
			"	SET queue = NULL ".
			" WHERE session_id = ".$dbh->quote($self->{'session_id'});
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );
}

# Gets a variable from memory
sub get {
	my $self = shift;
	my $key = shift;

	return $self->{$key};
}

# Is the current user superuser?  This is true
# if the authorizer is alroy and the enterer is alroy.  
sub isSuperUser {
	my $self = shift;
    return $self->{'superuser'};
}


# Tells if we are are logged in and a valid database member
sub isDBMember {
	my $self = shift;

	my $isDBMember = ($self->{'authorizer'} !~ /^guest$/i &&
            $self->{'enterer'} !~ /^guest$/i &&
            $self->{'authorizer_no'} =~ /^\d+$/ && 
            $self->{'enterer_no'} =~ /^\d+$/) ? 1 : 0;

    return $isDBMember;
}

sub isGuest {
    my $self = shift;
    return (!$self->isDBMember());
}

1;
