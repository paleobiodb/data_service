# Used for permissions checking to make sure the current user has permission to access each row.
# applies to collections, occurrences, and reidentifications for read permissions, and to several
# other tables for read/write and write permissions.
#
# Relies on the access_level and release_date fields of the collection table.
#
# The useHasReadPermissionsForCollectionNumber() method is now cached for performance.
# There are three caches, one for read, write, and read/write permissions.  They just use the
# collection_no as a key and store a boolean integer value denoting if the user has permission or not.
# This cache is only peristent as long as the object is in existance, so it should be perfectly safe.
#
# updated by rjp, 1/2004.

package Permissions;

use strict;

use SQLBuilder;
use Debug;

use constant CACHESIZE => 1000;		# number of entries in the cache.

use fields qw(	
				readCache
				writeCache
				readWriteCache
				session
				SQLBuilder
				
				date
			);  # list of data members

# cache			:	reference to a cache hash of collection numbers and permissions.
# date			:	stored for speed.. This object will never be alive more than a few seconds, so it's okay.
# session		:	the session object passed in with new()
# SQLBuilder	:	SQLBuilder object so we don't have to keep recreating it.
			

# Flags and constants
# These are class variables.
my $DEBUG = 0;		# DEBUG flag


# **Note: Must pass the session variable when creating this object.
sub new {
	my $class = shift;
	my Permissions $self = fields::new($class);
	
	my $session = shift;		# session object
	$self->{session} = $session;
	
	if (! $session) {
		Debug::logError("Permissions must be created with valid Session object!");
		return undef;
	}
	
	# create SQLBuilder object
	my $sql = SQLBuilder->new();
	$self->{SQLBuilder} = $sql;  # store in data member
	
	# create empty caches
	my %emptyR = ();
	my %emptyW = ();
	my %emptyRW = ();
	
	$self->{readCache} = \%emptyR;
	$self->{writeCache} = \%emptyW;
	$self->{readWriteCache} = \%emptyRW;
	
	# store the date for speed.
	# this is okay to do since the object won't be alive for more than a few seconds at a time.
	$self->{date} = $self->getDate();
	
	return $self;
}





# pass it a collection_no
# returns a boolean value 
# false (0) if the user can't read this collection
# true (1) if they can.
#
# Note, this may be slower than the older method, but it is cleaner
# since it only requires the collection_no to determine access.
#
# Tested on XServe, and it took 6.2 seconds to check 10,000 rows = 0.00063 s. per row
# which isn't too bad.
#
# by rjp, 1/2004.
sub userHasReadPermissionForCollectionNumber {
	my $self = shift;
	
	my $cnum = shift; 	# the collection_no to check
	
	if (! $cnum) {
		Debug::logError("Permissions, no collection_no passed.");
		return 0;  # false if no collection_no
	}
	
	if (! $self->{session}) {
		return 0;
	}
	
	# grab a reference to the readCache.		
	my $cacheRef = $self->{readCache};
	
	#my %cache = %$cacheRef;
	#my @keys = keys(%cache);
	#Debug::dbPrint("keys = @keys\n\n");
	
	
	# first, check the cache to see if we already have a value for this collection no
	if (my $cacheVal = $cacheRef->{$cnum}) {
		return $cacheVal;	# if this entry in the cache hash exists,
		# then we already know the permissions, and can just return it (0 or 1).
	}
	
	
	# if we make it to here, then the value was not cached,
	# so we actually have to query the database.
	
	my $perm = $self->queryDatabaseForReadPerm($cnum);
	
	$cacheRef->{$cnum} = $perm;	# store the new value in the cache
	$self->{readCache} = $cacheRef;
	
	return $perm;	# return it.	
}



# For INTERNAL use only.
# actually queries the database to get read permissions for the passed
# collection_no.  This is only used when the value is not *already* in the readCache. 
sub queryDatabaseForReadPerm {
	my $self = shift;
	my $cnum = shift;

	my $ses = $self->{session};
	

	my $sql = $self->{SQLBuilder};
	$sql->setSQLExpr("SELECT access_level, 
						DATE_FORMAT(release_date,'%Y%m%d') rd_short,
						research_group, authorizer 
						FROM collections WHERE collection_no = $cnum");						
	$sql->executeSQL();
	my $resultRef = $sql->nextResultArrayRef();
	
	if (! $resultRef) {
		return 0;
	}
	
	my $access_level = $resultRef->[0];
	my $rd_short = $resultRef->[1];
	
	
	if ($rd_short < $self->{date}) {
		# the release date has already passed, so it reverts to public access
		return 1;		# okay to access.
	}
	
	# if we make it to here, then the release date has not yet passed
	my $session_auth = $ses->get("authorizer");
	
	my $authorizer = $resultRef->[3];	
	
	if ($access_level eq "the public") {
		return 1;	# public access level is always visible, even if release date hasn't passed..	
	}
	
	if (($ses->get("superuser") == 1) || ($session_auth eq $authorizer)) {
		return 1;	# superuser can read anything
					# and if the current authorizer authorized this record, then they can read it too.
	}
	
	if (($access_level eq "database members") && ($session_auth ne "guest")){
		return 1;	# okay as long as user isn't a guest
	}
	
	if (($access_level eq "authorizer only") && ($session_auth eq $authorizer)) {
		return 1;
	}
	
	my $research_group = $resultRef->[2];
	$research_group =~ tr/ /_/;		# replace spaces with underscores for comparison
	
	if (($access_level eq "group members") && ($ses->get($research_group))){
		# note, spaces have already been replaced with underscores in the $research_group variable
		return 1;
	}
	
	return 0;		# if we make it to here, then there must be a problem, so disallow access.	
}



# rjp note: pass it the $sth, a reference to an array of data rows, 
# a limit number, and a reference to a scalar for the number of rows.
# 
# Produces an array of rows that this person has permissions to READ
sub getReadRows {
	my $self = shift;
	
	my $sth = shift;
	my $dataRows = shift;
	my $limit = shift;
	my $ofRows = shift;
	
	my $s = $self->{session};

	# Get today's date in the lexical comparison format
	my $now = $self->getDate ( );

	# Ensure they had rd_date in the result set
	my %requiredResults = ( );
	my @requiredFields = ("access_level", "rd_short", "research_group");
	# NAME returns a reference to an array of field (column) names.
	my @fields = @{$sth->{NAME}};
	# Compare the database column names to the required fields
	foreach my $field ( @fields ) {
		foreach my $required ( @requiredFields ) {
			if ( $field eq $required ) { $requiredResults{$field} = 1; }
		}
	}
	
	my $required;
	
	foreach $required ( @requiredFields ) {
		if ( ! $requiredResults{$required} ) { 
			$self->htmlError ( "Improperly formed SQL.  Must have field [$required]" );
		}
	}

	# Check each row returned by the database for permission.
	while ( my $row = $sth->fetchrow_hashref ( ) ) {

		my $okToRead = "";			# Clear
		my $failedReason = "";		# Clear

		if ( $s->get("superuser") == 1 ) {
			# Superuser is omniscient
			$okToRead = "superuser";
		} elsif ( $s->get("authorizer") eq $row->{authorizer} ) { 
			# If it is your row, you can see it regardless of access_level
			$okToRead = "authorizer";
		} elsif ( $row->{rd_short} > $now ) {
			# Future... must do checks
			# Access level overrides the release date

			# Determine the access level
			ACCESS: {
				# Public?
				if ( $row->{access_level} eq "the public" ) { $okToRead = "public access"; last; }
	
				# DB member?
				if ( $row->{access_level} eq "database members" ) {
					if ( $s->get("authorizer") ne "guest" ) { 
						$okToRead = "db member"; 
					} else {
						$failedReason = "not db member";
					}
					last; 
				}
	
				# Group member?
				if ( $row->{access_level} eq "group members" ) {
					my $researchGroup = $row->{"research_group"};
					$researchGroup =~ tr/ /_/;
					if ( $s->get($researchGroup) ) { 
						$okToRead = "group member[$researchGroup]"; 
					} else {
						$failedReason = "not group member";
					}
					last; 
				}

				# Authorizer?
				if ( $row->{access_level} eq "authorizer only" ) {
					if ( $s->get("authorizer") eq $row->{authorizer} ) { 
						$okToRead = "authorizer"; 
					} else {
						$failedReason = "not authorizer";
					}
					last;
				}
			} # :SSECCA
		} else {
			# Past... everything public
			$okToRead = "past record";
		}

		if ( $okToRead ) {
			# May see row
			&dbg ( "okToRead [".$row->{collection_no}."]: ".$row->{rd_short}." > ".$now." $okToRead" );

			# Stow away the limit of rows (for later...)
			if ( $$ofRows < $limit ) { push ( @{$dataRows}, $row ); }
			$$ofRows++;		# This is the number of rows they could see, not the limit
		} else {
			# May not see row
	 			&dbg (	"<font color='red'>".
	 					"Not ok[".$row->{collection_no}."]: ".$row->{rd_short}." > ".$now.
	 					"</font>".
	 					" al: ".$row->{access_level}.
	 					" rg: ".$row->{research_group}.
	 					" you: ".$s->get("enterer").
	 					" aut: ".$s->get("authorizer").  
	 					" pb: ".$s->get("paleobotany").
	 					$failedReason );
		}
	}
}

# Produces an array of rows that this person has permissions to WRITE
sub getWriteRows {
	my $self = shift;
	my $sth = shift;
	my $dataRows = shift;
	my $limit = shift;
	my $ofRows = shift;
	
	my $s = $self->{session};

	while ( my $row = $sth->fetchrow_hashref ( ) ) {

		my $okToWrite = "";				# Clear
		my $failedReason = "";			# Clear

		if ( $s->get("superuser") ) {
			# Superuser is omnicient
			$okToWrite = "superuser";
		} elsif ( $s->get("authorizer") eq $row->{authorizer} ) { 
			# If it is your row, you can see it regardless of access_level
			$okToWrite = "you own it"; 
		} else {
			$failedReason = "not your row";
		}

		if ( $okToWrite ) {
			# May see row
			&dbg ( "okToWrite [".$row->{collection_no}."]: $okToWrite" );

			# Stow away the limit of rows (for later...)
			if ( $$ofRows < $limit ) { push ( @{$dataRows}, $row ); }
			$$ofRows++;		# This is the number of rows they could see, not the limit
		} else {
			# May not see row
	 			&dbg ("<font color='red'>".
	 					"Not ok[".$row->{collection_no}."]: ".
	 					"</font>".
	 					" al: ".$row->{access_level}.
	 					" rg: ".$row->{research_group}.
	 					" you: ".$s->get("enterer").
	 					" aut: ".$s->get("authorizer").  
	 					" pb: ".$s->get("paleobotany").
	 					$failedReason );
		}
	}
}

####
## getReadWriteRows($self, $sth)
#
#	description:	Returns ALL rows of data for the given query, and
#			includes a hash key with each row that tells whether
#			the row is readable or writeable.
#
#	arguments:	$sth - statement handle for executed query
#
#	returns:	array of hash refs of data; one hash reference
#			per row of data, including a key 'writeable'
#			that is boolean for read/write permissions on the row. 
####
sub getReadWriteRowsForEdit{
	my $self = shift;
	my $sth = shift;

	my $s = $self->{session};
	
	# for returning data
	my @results = ();

	while ( my $row = $sth->fetchrow_hashref() ) {

		my $okToWrite = "";	# Clear
		my $failedReason = "";	# Clear

		if ( $s->get("superuser") ) {
			# Superuser is omnicient
			$okToWrite = "superuser";
		} elsif ( $s->get("authorizer") eq $row->{authorizer} ) { 
			# Your row: you can see it regardless of access_level
			$okToWrite = "you own it"; 
		} else {
			$failedReason = "not your row";
		}

		if($okToWrite eq ""){ $row->{'writeable'} = 0; }
		else{ $row->{'writeable'} = 1; }

		# return all data
	        push (@results, $row);

		if ( $okToWrite ) {
		    # May see row
		    dbg( "okToWrite [".$row->{collection_no}."]: $okToWrite" );

		} else {
			# May not see row
	 		dbg("<font color='red'>".
	 			"Not ok[".$row->{collection_no}."]: ".
	 			"</font>".
	 			" ac_lev: ".$row->{access_level}.
	 			" res_grp: ".$row->{research_group}.
	 			" entr: ".$s->get("enterer").
	 			" aut: ".$s->get("authorizer").  
	 			" paleobot: ".$s->get("paleobotany").
	 			$failedReason );
		}
	}
	return @results;
}

# Returns the day, month, and year
sub getDate {
	my $self = shift;

	(	my $sec,
		my $min,
		my $hour,
		my $mday,
		my $mon,
		my $year,
		my $wday,
		my $yday,
		my $isdst) = localtime(time);

	return sprintf ( "%4d%02d%02d",  $year+1900, $mon+1, $mday );
}

sub dbg {
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

# This only shown for internal errors
sub htmlError {
	my $self = shift;
    my $message = shift;

    print $message;
    exit 1;
}

1;
