#!/usr/bin/perl
#
# Script for administrative functions

use CGI;
use DBI;
use HTMLBuilder;
use Class::Date qw(date localdate gmdate now);
use Session;
use DBConnection;
use DBTransactionManager;

# Make standard objects
my $q = CGI->new();
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);
my $s = Session->new($dbt,$q->cookie('session_id'));

my $hb = HTMLBuilder->new($dbt,$s,0,1);


                                                       
# Flags and constants
my $DEBUG = 0;			# Shows debug information regarding the page

# A few declarations
my $sql="";				# Any SQL string
my $rs;					# Generic recordset

# Get the action
my $action = $q->param("action");
if ( ! $action ) { $action = "displayHomePage"; }

# Print a header
print $q->header(	-type => "text/html", 
					-expires =>"now" );

unless ($s->get('authorizer_no') eq '4' || $s->get('authorizer_no') eq '48') {
    print "Not logged in";
    die;
}
&$action;

exit;

# --------------------------------------- subroutines --------------------------------

sub displayPerson {
	my $type = $q->param("type");
	my @values;
	my @fieldNames;
	my $person_no = $q->param("person_no");

	# Minimally we need the metadata
	if ( $type eq "add" ) { $person_no = 0; }

	# Read values to populate page
	$sql =	"SELECT * ".
			"  FROM person ".
			" WHERE person_no = $person_no";
	dbg ( "$sql<hr>\n" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	@fieldNames = @{$sth->{NAME}};
	@values = $sth->fetchrow_array();
	$sth->finish();

	# Upper case
	$type =~ s/^./\U$&/;
	unshift ( @values, $type );
	unshift ( @fieldNames, "%%type%%" );

	print $hb->populateHTML( "std_page_top" );
	print $hb->populateHTML ( "person", \@values, \@fieldNames );
}

sub processPersonAdd {

	my $person_no=0;

	# Do the reversal
	my $name = $q->param("name");
	$name =~ s/^(.\.) (.*)$/\2, \1/;
	$q->param ( "reversed_name" => $name );
	# Created data, unless specified somehow.
	my $nowString = now();
	$q->param( created => "$nowString" ) unless $q->param('created');

    my $return = insertRecord ( "person", "person_no", \$person_no);
	if ( ! $return ) { &htmlError ( "$0: Unable to insert record" ); }

	&displayHomePage();
}

sub processPersonEdit {
	my $person_no=$q->param("person_no");
	# Modified data, unless specified somehow.
	my $nowString = now();
	$q->param( modified => "$nowString" ) unless $q->param('modified');

    my $return = updateRecord ( "person", "person_no", $person_no);
	if ( ! $return ) { &htmlError ( "$0: Unable to update record" ); }

	&displayHomePage();

}

# Lists all persons for Edit
sub displayPersonList {

	print $hb->populateHTML( "std_page_top" );
	print createList( "person", 1, "last_name,first_name" );
	
}

# Creates a list of all entries in a table for edit
sub createList {
	my $table = shift;
	my $edit = shift;
	my $sort = shift;
	my $return = "";

	$sql = "SELECT * FROM $table ";
	if ( $sort ) { $sql .= "ORDER BY $sort"; }
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	@fieldNames = @{$sth->{NAME}};

	my $columns = @fieldNames;


	# column names
	$return .= "<table border=0 cellpadding=4 cellspacing=1 bgcolor='Silver'>\n";
	$return .= "<tr>\n";
	if ( $edit ) { $return .= "<td class='darkList'>Action</a>"; }
	for ( my $i=0; $i<$columns; $i++ ) {
		if ( $fieldNames[$i] !~ /preferences/ )	{
			$return .= "<td class='darkList'>".$fieldNames[$i]."</td>";
		} else	{
			$prefCol = $i;
		}
	}		
	$return .= "</tr>\n";

	# Print values
	while ( my @values = $sth->fetchrow_array() ) {
		$return .= "<tr>\n";
		$return .= &buildControls ( $edit, $values[0] );
		for ( my $i=0; $i<$columns; $i++ ) {
			if ( $i != $prefCol )	{
				$return .= "<td bgcolor='White'>".$values[$i]."</td>";
			}
		}		
		$return .= "</tr>\n";
	}

	$return .= "</table>\n";

	$sth->finish();

	return $return;
}

sub buildControls {
	my $edit = shift;
	my $keyColumn = shift;

	if ( ! $edit ) { return ""; }

	my $return = "<td bgcolor='White' nowrap><font size='1'>";

	if ( $edit ) {
		$return .= "<a href='".$q->url."?action=displayPerson&type=edit&person_no=$keyColumn'>Edit</a> ";
	}

	$return .= "</font></td>";

	return $return;
}

# This only shown for internal errors
sub htmlError {
	my $message = shift;

	print $message;
	exit 1;
}



sub displayHomePage {
	print $hb->populateHTML( "std_page_top" );
	print $hb->populateHTML( "index" );

}

# JA 27.6.02
sub displayActions	{

	print $hb->populateHTML( "std_page_top" );
	print $hb->populateHTML( "index" );

	my $sql = "SELECT first_name, last_name, last_action FROM person";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	
	my $lastlogin;
	while ( my $row = $sth->fetchrow_hashref() ) {
		$lastlogin{$row->{'first_name'}.' '.$row->{'last_name'}} = $row->{'last_action'};
	}
	$sth->finish();
	
	@names = sort({ $lastlogin{$b} <=> $lastlogin{$a} } keys %lastlogin);
	
	print "<p><center><table cellpadding=6>\n";
	
	for $i (0..29)	{
		if ($lastlogin{$names[$i]} > 20020630150000)	{
			$d = date($lastlogin{$names[$i]});
			printf "<tr><td>%d",$i + 1;
			print "<td>$names[$i]</td><td>$d</td></tr>\n";
		}
	}
	
	print "</table></center>\n";

}

sub insertRecord {
	my $table = shift;
	my $primaryKeyName = shift;
	my $primaryKey = shift;

	# Get the database metadata
	$sql = "SELECT * FROM $table WHERE $primaryKeyName=0";
	dbg ( "$sql<HR>" );
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	# Get a list of database field names
	my @fieldNames = @{$sth->{NAME}};

	# Get a list of database field types for discerning which fields need quotes
	my @fieldTypes = @{$sth->{mysql_is_num}};

	my @fieldTypeCodes = @{$sth->{mysql_type}};

	$sth->finish();

	# Generate the INSERT string
	my @fields;
	my @values;
	my $fieldCount = 0;
	foreach my $fieldName (@fieldNames) {
		my $value = "";
		# Is the database field on the form?
		if ( defined $q->param($fieldName) ) {
			push(@fields, $fieldName);			# Add it to the array
			$value = $q->param($fieldName);		# Get the value
			
			# Set: separate with commas
			my @formVals = $q->param($fieldName);
			if ( $fieldTypeCodes[$fieldCount] == 254 ) {
				my $numSetValues = @formVals;
				if ( $numSetValues ) {
					$value = join(',', @formVals);
				}
			}

			# Add quotes if this is character data
			$value = $dbh->quote($value) unless $fieldTypes[$fieldCount];
			# dbg ( "fn:[$fieldName] val:[$value] type:[".$fieldTypes[$fieldCount]."] tc:[".$fieldTypeCodes[$fieldCount]."]" );
			$value = "NULL" unless $value =~ /\w/;

			push(@values, $value);
		}
		$fieldCount++;
	}

	# Insert the record
	$sql =	"INSERT INTO $table (" . join(',', @fields) . ") ".
			" VALUES (" . join(',', @values) . ")";
	$sql =~ s/\s+/ /gms;					# We like our SQL clean
	dbg ( "$sql<HR>" );
	$dbh->do( $sql ) || die ( "$sql<HR>$!" );

	# Retrieve and display the record
	$$primaryKey = $dbh->{'mysql_insertid'};

	return 1;
}

sub dbg
{
  return unless $DEBUG;
  
  print $_[0]."<BR>\n";
}

sub updateRecord
{
	my $table = shift;
	my $primaryKeyName = shift;
	my $primaryKey = shift;

	# Get the database metadata
	$sql = "SELECT * FROM $table WHERE $primaryKeyName = 0";
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();

	my @fieldNames = @{$sth->{NAME}};			# Field names
	my @fieldTypes = @{$sth->{mysql_is_num}};	# Field types
	my @fieldTypeCodes = @{$sth->{mysql_type}}; # Type codes
	$sth->finish();

	# Generate the UPDATE string
	my @updates;
	my @fields;
	my @vals;
	my $fieldCount = -1;
	my $updateString = "UPDATE $table SET ";
	foreach my $fieldName (@fieldNames) {
		$fieldCount++;

		# Skip the primary key
		next if $fieldName eq $primaryKeyName;
		
		# Skip fields that aren't defined in the form
		next unless defined $q->param($fieldName);
		
		# Get the value from the form
		my $value = $q->param($fieldName);
		
		# If this is an enum, separate with commas
		my @formVals = $q->param($fieldName);
		if($fieldTypeCodes[$fieldCount] == 254) {
			#print "<b>Here: $fieldName: " . join(',', @formVals) . "</b><br><br>";
			$value = join(',', @formVals) if @formVals > 1;
		}
		
		# Add quotes if this is character data
		$value = $dbh->quote($value) unless $fieldTypes[$fieldCount];
		$value = 'NULL' unless $value =~ /\w/;
		
		push(@updates, "$fieldName=$value");
	}
	
	# Update the record
	$updateString .= join(',', @updates) . " WHERE $primaryKeyName = $primaryKey";
	$updateString =~ s/\s+/ /gms;
	dbg ( "$updateString<hr>\n" );
	$dbh->do( $updateString ) || die ( "$updateString<HR>$!" );

	return $primaryKey;
}
