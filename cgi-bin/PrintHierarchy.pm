package PrintHierarchy;

use PBDBUtil;
use Classification;

$DEBUG = 0;

# 21.9.03 JA

sub startPrintHierarchy	{

	my $dbh = shift;
	my $hbo = shift;

	$|=1;

	print main::stdIncludes( "std_page_top" );

	print "<center><h3>Taxon classification search form</h3></center>\n";

	print "<form method=post action=\"/cgi-bin/bridge.pl\"> " .
		"<input id=\"action\" type=hidden name=\"action\"" .
		"value=\"startProcessPrintHierarchy\">" .
		"<center><table>\n" .
		"<tr><td valign=middle align=right>Taxon name:" .
		"<input name=\"taxon_name\" type=\"text\" size=25>" .
		"</td>\n" .
		"<td valign=middle>Hierarchical levels:" .
		"<select name=\"maximum_levels\"><option>1<option>2<option selected>3<option>4<option>5</select>" .
		"<td align=left>" .
		"<input id=\"submit\" name=\"submit\" value=\"Show hierarchy\"" .
		" type=\"submit\">" .
		"</td></tr></table></center></form>\n" ;

	print main::stdIncludes("std_page_bottom");

	return;
}

# WARNING: this routine assumes that the FIRST MATCHED name is the correct
#   parent name in the authorities table, which WILL NOT work for homonyms
sub processPrintHierarchy	{

	my $dbh = shift;
	my $q = shift;
	my $dbt = shift;
	my $exec_url = shift;

	my $OUT_HTTP_DIR = "/paleodb/data";
	my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};

	$MAX = $q->param('maximum_levels');
	%shortrank = ("species" => "", "subgenus" => "Subg.", "genus" => "G.",
			"tribe" => "Tr.", "subfamily" => "Subfm", "family" => "Fm.",
			"superfamily" => "Superfm." ,
			"infraorder" => "Infraor.", "suborder" => "Subor.",
			"order" => "Or.", "superorder" => "Superor.",
			"infraclass" => "Infracl.",  "subclass" => "Subcl.", "class" => "Cl.",
			"subphylum" => "Subph.", "phylum" => "Ph.");

	print main::stdIncludes( "std_page_top" );

# get focal taxon name from query parameters, then figure out taxon number
	$sql = "SELECT taxon_no,taxon_rank FROM authorities WHERE taxon_name='" . $q->param('taxon_name') . "'";
	$ref = @{$dbt->getData($sql)}[0];

	if ( ! $ref )	{
		print "<center><h3>Taxon not found</h3>\n";
		print "<p>You may want to <a href=\"$exec_url?action=startStartPrintHierarchy\">try again</a></p></center>\n";
		print main::stdIncludes( "std_page_bottom" );
		exit;
	}

	push @parents , $ref->{taxon_no};

	print "<center><h3>Classification of ";
	if ( $ref->{taxon_rank} ne "genus" )	{
		print "the ";
	}
	print $q->param('taxon_name') . "</h3></center>";
	$id{$ref->{taxon_no}} = 10**(3*($MAX-1));
	my $rank = $shortrank{$ref->{taxon_rank}};
	$name{$ref->{taxon_no}} = "<b>" . $rank . "</b> " . $q->param('taxon_name');
	$outrank{$ref->{taxon_no}} = $ref->{taxon_rank};
	$outname{$ref->{taxon_no}} = $q->param('taxon_name');

# work through three hierarchical levels
	for $level ( 1..$MAX )	{

# go through the parent list
		my @children = ();
		$childcount = 0;
		for $p ( @parents )	{

	# find all children of this parent
			my $sql = "SELECT child_no FROM opinions WHERE status='belongs to' AND parent_no=" . $p;
			@refs = @{$dbt->getData($sql)};

			# now the hard part: make sure the most recent opinion
			#  on this name is a "belongs to"
			@goodrefs = ();
			for my $ref ( @refs )	{
			# first check if the child is a recombination
				my $lastopinion = "";
				$sql = "SELECT child_no FROM opinions WHERE status='recombined as' AND parent_no=" . $ref->{child_no};
				$orgcombref = @{$dbt->getData($sql)}[0];
				# OH NO! this is recombination
				# find the most recent opinion, and if it is
				#   NOT "recombined as" the taxon then continue
				if ( $orgcombref )	{
					$sql = "SELECT parent_no,status,reference_no,ref_has_opinion,pubyr FROM opinions WHERE child_no=" . $orgcombref->{child_no};
					@crefs = @{$dbt->getData($sql)};
					my $maxyr = 0;
					for $cref ( @crefs )	{
						if ( $cref->{pubyr} > $maxyr )	{
							$maxyr = $cref->{pubyr};
							$lastopinion = $cref->{status};
							$lastparent = $cref->{parent_no};
						} elsif ( $cref->{ref_has_opinion} eq "YES" )	{
							$sql = "SELECT pubyr FROM refs WHERE reference_no=" . $cref->{reference_no};
							$rref = @{$dbt->getData($sql)}[0];
							if ( $rref->{pubyr} > $maxyr )	{
								$maxyr = $rref->{pubyr};
								$lastopinion = $cref->{status};
								$lastparent = $cref->{parent_no};
							}
						}
					}
					if ( $lastopinion ne "recombined as" || $lastparent != $ref->{child_no} )	{
						next;
					}
				}

				$sql = "SELECT status,reference_no,ref_has_opinion,pubyr FROM opinions WHERE child_no=" . $ref->{child_no};
				@crefs = @{$dbt->getData($sql)};
				my $maxyr = 0;
				for $cref ( @crefs )	{
					if ( $cref->{pubyr} > $maxyr )	{
						$maxyr = $cref->{pubyr};
						$lastopinion = $cref->{status};
					} elsif ( $cref->{ref_has_opinion} eq "YES" )	{
						$sql = "SELECT pubyr FROM refs WHERE reference_no=" . $cref->{reference_no};
						$rref = @{$dbt->getData($sql)}[0];
						if ( $rref->{pubyr} > $maxyr )	{
							$maxyr = $rref->{pubyr};
							$lastopinion = $cref->{status};
						}
					}
				}
				if ( $lastopinion =~ /belongs to/ )	{
					push @goodrefs , $ref;
				}
			}

			for my $ref ( @goodrefs )	{
				$childcount++;
				push @children,$ref->{child_no};
				$sql = "SELECT taxon_name,taxon_rank FROM authorities WHERE taxon_no=" . $ref->{child_no};
				$cref = @{$dbt->getData($sql)}[0];
				my $rank = $shortrank{$cref->{taxon_rank}};
			# rock 'n' roll: save the child name
				if ( ! $seen{$cref->{taxon_name}} )	{
					$name{$ref->{child_no}} = "<b>" . $rank . "</b> ";
					if ( $cref->{taxon_rank} =~ /(species)|(genus)/ )	{
						$name{$ref->{child_no}} .= "<i>";
					}
					$name{$ref->{child_no}} .= $cref->{taxon_name};
					if ( $cref->{taxon_rank} =~ /(species)|(genus)/ )	{
						$name{$ref->{child_no}} .= "</i>";
					}
					$outrank{$ref->{child_no}} = $cref->{taxon_rank};
					$outname{$ref->{child_no}} = $cref->{taxon_name};
					my ($genus,$species) = split / /,$cref->{taxon_name};
					my @gletts = split //,$genus;
					my $code = "";
					for my $l ( 0..9 )	{
						if ( $gletts[$l] )	{
							$code .= $gletts[$l];
						} else	{
							$code .= "0";
						}
					}
					if ( $species )	{
						my @sletts = split //,$species;
						for my $l ( 0..9 )	{
							if ( $sletts[$l] )	{
								$code .= $sletts[$l];
							} else	{
								$code .= "0";
							}
						}
					} else	{
						$code .= "0000000000";
					}
					$id{$ref->{child_no}} = $id{$p} . $code;
					$mylevel{$ref->{child_no}} = $level;
					$seen{$cref->{taxon_name}}++;
				}
			}
		}

# replace the parent list with the child list
		@parents = @children;

# END of main routine

	}

# now print out the data
	open OUT, ">$OUT_FILE_DIR/hierarchy.csv";
	print "<center><table>\n";
	@sorted = keys %id;
	@sorted = sort { $id{$a} cmp $id{$b} } @sorted;
	for $sk ( @sorted )	{
		print "<tr>";
		for $i ( 1..$mylevel{$sk} )	{
			print "<td></td>";
		}
		print "<td>";
		print $name{$sk};
		print OUT "$outrank{$sk},$outname{$sk}\n";
		print "</td>";
		print "</tr>\n";
		$nrecords++;
	}
	print "</table></center><p>\n";
	close OUT;

	chmod 0664, "$OUT_FILE_DIR/hierarchy.csv";

	print "<hr><center><p>$nrecords records were printed to the file <b><a href='$OUT_HTTP_DIR/hierarchy.csv'>hierarchy.csv</a></b></p></center>";

	print "<center><p>You may <b><a href=\"$exec_url?action=startStartPrintHierarchy\">classify another taxon</a></b></p></center>\n";
	print main::stdIncludes( "std_page_bottom" );

	return;
}

1;
