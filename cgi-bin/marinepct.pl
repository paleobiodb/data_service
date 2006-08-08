#!/usr/local/bin/perl
use CGI;
use DBI;
use Permissions;
use Session;
use DBConnection;
use DBTransactionManager;

# made from a copy of fivepct.pl 19.8.03


# Create the CGI, Session, and some other objects.
my $q = CGI->new();
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);
my $s = Session->new($dbt,$q->cookie('session_id'));

print $q->header( -type => "text/html" );
&PrintHeader();

unless ($s->isDBMember()) {
    print qq|<p><div align=center>Please <a href="bridge.pl?action=displayLoginPage">log in</a> first.</div></p>|;
    exit;
}      


# Search the refs and print matches
if ( $q->param("action") eq "search" )	{
	print "<center><h3>Marine invertebrate 1% project reference query results</h3></center>\n\n";
	print "<form method=post action=marinepct.pl>\n\n";
	print "<input type=hidden name=action value=update>\n\n";

	@statusvals = ("unknown","junk","desirable","help","claimed","copied","discarded","entered");

	$sql = "SELECT ref_no,title,author,pub,subjects,language,status,modifier FROM marinepct WHERE ";
	if ( $q->param("status") ne "all" )	{
		$sql .= "status='" . $q->param("status") . "' AND ";
	}
	$field = $q->param("field");
	if ( $field eq "publication" )	{
		$field = "pub";
	}
	if ( $field eq "ID number" )	{
		$sql .= "ref_no=";
		$sql .= $q->param("searchstring") / 20;
	} else	{
		my $searchstring = $q->param("searchstring");
		# escape single quotes
		$searchstring =~ s/'/\\'/g;
		if ( $q->param("andor") eq "all of the words" )	{
			$searchstring =~ s/ /%' AND $field LIKE '%/g;
		} elsif ( $q->param("andor") eq "any of the words" )	{
			$searchstring =~ s/ /%' OR $field LIKE '%/g;
		}
		$sql .= "($field LIKE '%" . $searchstring . "%')";
	}
	if ( $q->param("language") ne "any language" and $q->param("language") ne "" )	{
		$sql .= " AND language='" . $q->param("language") . "'";
	}
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	print "<table>\n";
	while ( my %refrow = %{$sth->fetchrow_hashref()} )	{
		$found++;
		push @refnos,$refrow{'ref_no'};
		if ( $found == 1 )	{
			print "<tr><td><b>status</b></td><td><b>ID#</b></td><td><b>reference</b></td>\n";
		}
		$idstring = "status_" . $refrow{'ref_no'};
		print "<tr><td valign='top' align='center'><select name=$idstring>";
		for $s (@statusvals)	{
			if ($refrow{'status'} eq $s)	{
				print "<option selected>$s";
			} else	{
				print "<option>$s";
			}
		}
		print "</select><br><i>$refrow{'modifier'}</i> </td>\n";
		print "<td valign=top><b>";
		printf "%d",20*$refrow{'ref_no'};
		print "</b></td>\n";
		print "<td valign=top>$refrow{'author'} \n";
		print "\"$refrow{'title'}\" \n";
		print "<i>$refrow{'pub'}</i><br>\n";
		print "<font size=1px>[$refrow{'subjects'}]";
		print " <b>$refrow{'language'}</b></font></td>\n";
		print "</tr>\n";
	}
	print "</table>\n\n";
	print "<input type=hidden name=refnos value=\"" , join ',',@refnos , "\">\n";
	$sth->finish();

	if ( $found > 0 )	{
		print "<center><p><b>$found</b> references were found</p></center>\n\n";
		print "<center><p><input type=submit value=\"Update\"></p></center>\n\n";
	} else	{
		print "<center><p>No references were found! Please <a href=\"marinepct.pl?action=display\">try again</a>.</p></center>\n";
	}
	print "</form>\n\n";
}
# Or update the ref table and print the search form
elsif ( $q->param("action") eq "update" )	{

	@refnos = split /,/,$q->param("refnos");
	for $r (@refnos)	{
		$sql = "SELECT status FROM marinepct WHERE ref_no=" . $r;
		my $sth = $dbh->prepare($sql);
		$sth->execute();
		my %refrow = %{$sth->fetchrow_hashref()};
		$idstring = "status_" . $r;
		if ( $refrow{'status'} ne $q->param($idstring) )	{
			$sql = "UPDATE marinepct SET status='";
			$sql .= $q->param($idstring);
			$sql .= "', modifier='" . $s->get("enterer");
			$sql .= "' WHERE ref_no=" . $r;
			my $sth2 = $dbh->prepare($sql);
			$sth2->execute();
			$sth2->finish();
			$updated++;
			$updates{$r} = $q->param($idstring);
		}
		$sth->finish();
	}
	if ( $updated == 1 )	{
		print "<center><h3>One reference was updated</h3></center>\n";
		$outstring = "ID number and new status: \n";
	} elsif ( $updated > 1 )	{
		print "<center><h3>$updated references were updated</h3></center>\n";
		$outstring = "ID numbers and new status values: \n";
	}
	for $r ( keys %updates )	{
		my $n = $r * 20;
		$outstring .= "<b>" . $n . "</b> (<i>$updates{$r}</i>), ";
	}
	$outstring =~ s/, $//;
	if ( $outstring )	{
		print "<center><p>$outstring</p></center>\n<hr>\n";
	}
	&DisplayQueryPage;
}
else	{
	&DisplayQueryPage;
}

# print a link to the main menu at the bottom 6.2.04
print "<center><p><b><a href=\"/cgi-bin/bridge.pl?action=displayMenuPage&clear=clearQueue\">Back to the main menu</a></b></p></center>";


sub PrintHeader	{

	print "<html>\n<head>\n<title>Marine Invertebrate 1% Project</title>\n\n";
	print "<link REL=\"stylesheet\" TYPE=\"text/css\" HREF=\"/StyleSheets/common.css\">"; 
	print "</head>\n\n";
	print "<body>\n";
}

sub DisplayQueryPage	{

	open IN,"<./templates/marinepctquery.html";
	while (<IN>)	{
		print $_;
	}
	close IN;

}

