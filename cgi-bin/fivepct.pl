#!/usr/bin/perl
use CGI;
use CGI::Carp qw(fatalsToBrowser);
use DBI;
use Permissions;

# written 20.10.02
# restricts searches to refs with a particular status; uses GeoRef numbers
#   instead of table key numbers 21.10.02

require "connection.pl";

my $q = CGI->new();
my $dbh = DBI->connect("DBI:mysql:database=$db;host=$host", $user, $password, {RaiseError => 1});

print $q->header( -type => "text/html" );
&PrintHeader();

# Search the refs and print matches
if ( $q->param("action") eq "search" )	{
	print "<center><h3>5% project reference query results</h3></center>\n\n";
	print "<form method=post action=fivepct.pl>\n\n";
	print "<input type=hidden name=action value=update>\n\n";

	@statusvals = ("unknown","junk","desirable","copied","entered");

	$sql = "SELECT ref_no,title,author,pub,subjects,status FROM fivepct WHERE ";
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
		$sql .= "$field LIKE '%";
		my $searchstring = $q->param("searchstring");
		# escape single quotes
		$searchstring =~ s/'/\\'/g;
		if ( $q->param("andor") eq "all of the words" )	{
			$searchstring =~ s/ /%' AND $field LIKE '%/g;
		} elsif ( $q->param("andor") eq "any of the words" )	{
			$searchstring =~ s/ /%' OR $field LIKE '%/g;
		}
		$sql .= $searchstring . "%'";
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
		print "<tr><td valign=top><select name=$idstring>";
		for $s (@statusvals)	{
			if ($refrow{'status'} eq $s)	{
				print "<option selected>$s";
			} else	{
				print "<option>$s";
			}
		}
		print "</select> </td>\n";
		print "<td valign=top><b>";
		printf "%d",20*$refrow{'ref_no'};
		print "</b></td>\n";
		print "<td valign=top>$refrow{'author'} \n";
		print "\"$refrow{'title'}\" \n";
		print "<i>$refrow{'pub'}</i> </td>\n";
		print "</tr>\n";
	}
	print "</table>\n\n";
	print "<input type=hidden name=refnos value=\"" , join ',',@refnos , "\">\n";
	$sth->finish();

	if ( $found > 0 )	{
		print "<center><p><b>$found</b> references were found</p></center>\n\n";
		print "<center><p><input type=submit value=\"Update\"></p></center>\n\n";
	} else	{
		print "<center><p>No references were found! Please <a href=\"fivepct.pl?action=display\">try again</a>.</p></center>\n";
	}
	print "</form>\n\n";
}
# Or update the ref table and print the search form
elsif ( $q->param("action") eq "update" )	{

	@refnos = split /,/,$q->param("refnos");
	for $r (@refnos)	{
		$sql = "SELECT status FROM fivepct WHERE ref_no=" . $r;
		my $sth = $dbh->prepare($sql);
		$sth->execute();
		my %refrow = %{$sth->fetchrow_hashref()};
		$idstring = "status_" . $r;
		if ( $refrow{'status'} ne $q->param($idstring) )	{
			$sql = "UPDATE fivepct SET status='";
			$sql .= $q->param($idstring);
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


sub PrintHeader	{

	print "<html>\n<head>\n<title>5% Project</title>\n\n";
	print "<STYLE type=\"text/css\"> <!--\n";
	print "H3	{ font-family : Arial, Verdana, Helvetica; }\n";
	print "TD	{ font-family : Arial, Verdana, Helvetica; font-size : 12px }\n";
	print "P	{ font-family : Arial, Verdana, Helvetica; font-size : 14px; }\n";
	print "	-->\n</STYLE>\n\n</head>\n\n";
	print "<body bgcolor=\"white\" background=\"/public/PDbg.gif\">\n";
}

sub DisplayQueryPage	{

	open IN,"<./templates/fivepctquery.html";
	while (<IN>)	{
		print $_;
	}
	close IN;

}

