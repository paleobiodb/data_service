package Scales;

use Data::Dumper;
use TimeLookup;
use Constants qw($READ_URL $WRITE_URL);
use strict;

# written by JA 7-20.7.03
# caught a bug JA 23.7.03
# fixes of EML logic/use of EML
# +/+ Javascript
# +/+ displayEnterColl
# +/+ processEnterColl - as with processEditColl
# +/+ displayEditColl
# +/+ processEditColl - add subroutine call to get back name from number at end
# +/- displayCollectionDetails
# +/+ processShowEditForm
# +/+ processViewTimeScale
# +/+ processEditScaleForm

sub startSearchScale {
	my $dbt = shift;
	my $s = shift;

	my $dbh = $dbt->dbh;

	# Print the form
	print "<div align=\"center\"><h2>Select a time scale to view</h2>\n";

	# Retrieve each scale's name from the database
	my $sql = "SELECT authorizer_no,scale_no,scale_name,reference_no,continent,scale_rank FROM scales";
	my @results = @{$dbt->getData($sql)};
	my %scale_strings;
	foreach my $scaleref (@results)	{
		my $option = "<option value=\"" . $scaleref->{scale_no} . "\">";
		my $name = $scaleref->{scale_name};
		my $sql2 = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $scaleref->{reference_no};
		my @results2 = @{$dbt->getData($sql2)};
		my $auth = $results2[0]->{author1last};
		if ( $results2[0]->{otherauthors} || $results2[0]->{author2last} eq "et al." )	{
			$auth .= " et al.";
		} elsif ( $results2[0]->{author2last} )	{
			$auth .= " and " . $results2[0]->{author2last};
		}
		$auth .= " " . $results2[0]->{pubyr};
		$auth = " [" . $auth . "]\n";
		$scale_strings{$scaleref->{continent}.$scaleref->{scale_rank}}{$name} = $option . $name . $auth;

		# Get the authorizer's name
#		$sql2 = "SELECT name FROM person WHERE person_no=" . $scaleref->{authorizer_no};
#		@results2 = @{$dbt->getData($sql2)};
#		$scale_authorizer{$name} = $results2[0]->{name};
	}

	print "<table cellpadding=5>\n";
	print "<tr><td align=\"left\"> ";
	print "<form name=\"scale_view_form\" id=\"scale_view_form\" action=\"$WRITE_URL\" method=\"POST\">\n";
	print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"processViewScale\">\n\n";

	# WARNING: if African or Antarctic scales are ever entered, they need
	#  to be added to this list
	for my $c ( 'global','Asia','Australia','Europe','New Zealand','North America','South America' )	{
		my $continent = $c;
		$continent =~ s/global/Global/;
		print qq|<div class="displayPanel" align="left" style="padding-left: 2em;">
  <span class="displayPanelHeader"><b>$continent</b></span>
  <div class="displayPanelContent">\n|;
		for my $r ( 'eon/eonothem','era/erathem','period/system','subperiod/system','epoch/series','subepoch/series','age/stage','subage/stage','chron/zone' )	{
			my @sorted = sort keys %{$scale_strings{$c.$r}};
			if ( $#sorted > -1 )	{
				my $crclean = $c . $r;
				$crclean =~ s/[^A-Za-z]//g;
				print "<div class=\"tiny\" style=\"padding-bottom: 2px;\">$r</div>\n";
				print "<div class=\"verysmall\" style=\"padding-bottom: 4px;\">&nbsp;&nbsp;<select class=\"tiny\" name=\"scale$crclean\" onChange=\"document.getElementById('scale_view_form').submit()\">\n";
				print "<option>\n";
				for my $string ( @sorted )	{
					print $scale_strings{$c.$r}{$string};
				}
				print "</select></div>\n\n";
			}
		}
		print "</div>\n</div>\n\n";
	}

	print "</td></tr>";
	print "<noscript>\n";
	print "<tr><td align=\"center\"> ";
	print "<input type=\"submit\" value=\"View scale\">";
	print "</td></tr> ";
	print "</noscript>\n";
	print "</table>\n\n";
	print "</form>\n\n";

	if ( $s->get('enterer') eq "J. Alroy" || $s->get('enterer') eq "S. Holland" )	{

		print qq|
		<form name="scale_add_form" action="$WRITE_URL" method="POST">
		<input id="action" type="hidden" name="action" value="processShowForm">
		<input type="hidden" name="scale" value="add">
		<input type="submit" value="Add scale">
		</form>|;
	}

	print "<p align=\"left\" class=\"tiny\" style=\"margin-left: 2em; margin-right: 2em;\">All data on the web site are prepared using an automatically generated composite time scale. The composite scale is based on the latest published correlations and boundary estimates for each time interval that are given above.</p>\n";

	print qq|
<script language="JavaScript" type="text/javascript">
<!-- Begin

document.scale_view_form.reset();

window.onunload = unloadPage;

function unloadPage()	{
	document.scale_view_form.reset();
}

//  End -->
</script>

|;


	return;

}

# WARNING: facelift of display page means that editing is no longer possible,
#  because pulldown with scales has been dropped
sub processShowEditForm	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;
	my $dbh = $dbt->dbh;

	# Have to have a reference #
	my $reference_no = $s->get("reference_no");
	my $scale = int($q->param('scale'));
	if ( ! $reference_no )	{
		if ( $q->param('scale') )	{
			$s->enqueue( "action=processShowForm&scale=$scale");
		} else	{
			$s->enqueue( "action=processShowForm" );
		}
		main::displaySearchRefs ( "Please choose a reference first" );
		exit;
	}


	# Get data on the scale if it's an old one
	my @results;
	my @olddata;
	my @times;

	if ( $q->param('scale') =~ /[0-9]/ )	{
		my $sql = "SELECT * FROM scales WHERE scale_no=" . $q->param('scale');
		@results = @{$dbt->getData($sql)};

		push @olddata, $results[0]->{scale_no};
		push @olddata, $results[0]->{scale_name};
		push @olddata, $results[0]->{continent};
		push @olddata, $results[0]->{basis};
		push @olddata, $results[0]->{scale_rank};
		push @olddata, $results[0]->{scale_comments};

		# Get the scale's intervals
		$sql = "SELECT * FROM correlations WHERE scale_no=" . $results[0]->{scale_no};
		@times = @{$dbt->getData($sql)};
	} else	{
		push @olddata, '';
		push @olddata, '';
		push @olddata, 'global';
		push @olddata, 'paleontological';
		push @olddata, 'age/stage';
		push @olddata, '';
	}

	print $hbo->populateHTML('js_enter_scale');

	print "<form name=\"edit_scale_form\" action=\"$WRITE_URL\" method=\"POST\" onSubmit=\"return checkFields();\">\n";
	print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"processEditScale\">\n\n";

	# print out the time scale entry fields
	print $hbo->populateHTML('enter_scale_top', \@olddata, ['scale_no', 'scale_name', 'continent', 'basis', 'scale_rank', 'scale_comments']);

	# print out the time interval rows

	print "<div>\n<table>\n";

	print "<tr><td align=\"center\" colspan=2><b><font color=\"red\">Interval</a></b> </td><td align=\"center\" colspan=2>Maximum correlate </td><td align=\"center\" colspan=2>Minimum correlate </td><td align=\"center\" valign=\"bottom\">Lower<br>boundary </td></tr>\n";

	my $maxi = $#times;
	if ( $maxi < 1 )	{
		$maxi = 24;
	} else	{
		$maxi = $maxi + 10;
	}
	foreach my $i (0..$maxi)	{
		my $eml_interval;
		my $interval;
		my $eml_max_interval;
		my $max_interval;
		my $eml_min_interval;
		my $min_interval;
		my $lower_boundary;
		my $corr_comments;
		# translate numbers into names for interval variables
		if ( $times[$i]->{interval_no} )	{

			my $sqlstem = "SELECT eml_interval FROM intervals WHERE interval_no=";
			my $sql = $sqlstem . $times[$i]->{interval_no};
			my @emls = @{$dbt->getData($sql)};
			$eml_interval = $emls[0]->{eml_interval};

			$sql = $sqlstem . $times[$i]->{max_interval_no};
			@emls = @{$dbt->getData($sql)};
			$eml_max_interval = $emls[0]->{eml_interval};

			$sql = $sqlstem . $times[$i]->{min_interval_no};
			@emls = @{$dbt->getData($sql)};
			$eml_min_interval = $emls[0]->{eml_interval};

			$sqlstem = "SELECT interval_name FROM intervals WHERE interval_no=";

			$sql = $sqlstem . $times[$i]->{interval_no};
			my @names = @{$dbt->getData($sql)};
			$interval = $names[0]->{interval_name};

			$sql = $sqlstem . $times[$i]->{max_interval_no};
			@names = @{$dbt->getData($sql)};
			$max_interval = $names[0]->{interval_name};

			$sql = $sqlstem . $times[$i]->{min_interval_no};
			@names = @{$dbt->getData($sql)};
			$min_interval = $names[0]->{interval_name};

			$lower_boundary = $times[$i]->{lower_boundary};
			$corr_comments = $times[$i]->{corr_comments};
		}
		print "<input type=\"hidden\" name=\"correlation_no\" value=\"" , $times[$i]->{correlation_no} , "\">\n";
		print "<input type=\"hidden\" name=\"interval_no\" value=\"" , $times[$i]->{interval_no} , "\">\n";
		if ( $times[$i]->{max_interval_no} )	{
			print "<input type=\"hidden\" name=\"max_interval_no\" value=\"" , $times[$i]->{max_interval_no} , "\">";
		}
		if ( $times[$i]->{min_interval_no} )	{
			print "<input type=\"hidden\" name=\"min_interval_no\" value=\"" , $times[$i]->{min_interval_no} , "\">";
		}

		print $hbo->populateHTML('enter_scale_row', [$eml_interval, $interval, $eml_max_interval, $max_interval, $eml_min_interval, $min_interval, $lower_boundary, $corr_comments], ['eml_interval', 'interval', 'eml_max_interval', 'max_interval', 'eml_min_interval', 'min_interval', 'lower_boundary', 'corr_comments']);
	}

	print "<tr><td colspan=6 align=\"right\">";
	print "<input type=\"submit\" value=\"Submit\"></form>";
	print "</td></tr>\n";

	print "</table>\n</div>\n<p>\n";

	print "</form>\n";


	return;
}

sub processViewTimeScale	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;
	my $stage = shift;
	my $bad = shift;

    my @badintervals;
    @badintervals = @$bad if $bad;

	# new submit form has a ton of scale parameters; strip out the first
	# one that isn't blank
	if ( ! $q->param('scale') || $q->param('scale') =~ /[^0-9] / )	{
		my @params = $q->param();
		for my $p ( @params )	{
			if ( $p =~ /^scale/ && $p !~ /_name/ && $q->param($p) )	{
				$q->param('scale' => $q->param($p) );
				last;
			}
		}
	}
    my $scale_no = int($q->param('scale'));
    return unless $scale_no;

	# Get basic data on the time scale (WARNING: assumes one was selected properly)
	my $sql = "SELECT s.*,p1.name authorizer,p2.name enterer FROM scales s LEFT JOIN person p1 ON s.authorizer_no=p1.person_no LEFT JOIN person p2 ON s.enterer_no=p2.person_no WHERE s.scale_no=$scale_no";
	my @results = @{$dbt->getData($sql)};
    my $row = $results[0];


	print "<h3 align=\"center\">",$row->{scale_name},"</h3>\n\n";

	if ( @badintervals )	{
		print "<div align=\"center\"><p><b><font color='red'>WARNING!</font></b> ";
		if ( $#badintervals == 0 )	{
			print "The following correlative interval was not recognized: ";
		} else	{
			print "The following correlative intervals were not recognized: ";
		}
		for $b ( @badintervals )	{
			print "<i>$b";
			if ( $b ne $badintervals[$#badintervals] )	{
				print ",</i> ";
			} else	{
				print "</i>";
			}
		}
		print "</p></div>";
	}

	print "<p>\n\n<div align=\"center\"><div style=\"width: 30em\">";
	print "<table class=\"verysmall\"><tr><td bgcolor=\"black\"><table bgcolor=\"white\">\n\n";

	# Get the scale's intervals
	$sql = "SELECT * FROM correlations WHERE scale_no=$scale_no";
	my @times = @{$dbt->getData($sql)};

	# figure out what headers are needed

	my $nmax;
	my $nmin;
	my $nlower;
	for my $time (@times)	{
		if ( $time->{max_interval_no} > 0 )	{
			$nmax++;
		}
		if ( $time->{min_interval_no} > 0 )	{
			$nmin++;
		}
		if ( $time->{lower_boundary} > 0 )	{
			$nlower++;
		}
	}

	print "<tr>";
	print "<td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">&nbsp;<b>Interval</b></td>\n";
	print "<td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nmax > 0 )	{
		print "<b>Maximum&nbsp;correlate</b> ";
	}
	print "</td><td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nmin > 0 )	{
		print "&nbsp;&nbsp;<b>Minimum&nbsp;correlate</b> ";
	}
	print "</td><td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nlower > 0 )	{
		print "<b>Ma</b> ";
	}
	print "</td>\n";
	print "</tr>\n";


	# Print the rows
	for my $time (@times)	{

		my $sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{interval_no};
		my @names = @{$dbt->getData($sql2)};
        my $interval = $names[0]->{interval_name};
		$interval = $names[0]->{eml_interval}." ".$interval if ($names[0]->{eml_interval});
		$interval = "<a href=\"$READ_URL?action=displayInterval&interval_no=$time->{interval_no}\">$interval</a>";

		$sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{max_interval_no};
		@names = @{$dbt->getData($sql2)};
        my $max_interval = $names[0]->{interval_name};
		$max_interval = $names[0]->{eml_interval}." ".$max_interval if ($names[0]->{eml_interval});
		$max_interval = "<a href=\"$READ_URL?action=displayInterval&interval_no=$time->{max_interval_no}\">$max_interval</a>";

		$sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{min_interval_no};
		@names = @{$dbt->getData($sql2)};
        my $min_interval = $names[0]->{interval_name};
		$min_interval = $names[0]->{eml_interval}." ".$min_interval if ($names[0]->{eml_interval});
		$min_interval = "<a href=\"$READ_URL?action=displayInterval&interval_no=$time->{min_interval_no}\">$min_interval</a>";

		my $lower_boundary = $time->{lower_boundary};
		$lower_boundary =~ s/000$//;
		$lower_boundary =~ s/00$//;
		$lower_boundary =~ s/0$//;
		$lower_boundary =~ s/\.$//;
		if ( $lower_boundary == 0 )	{
			$lower_boundary = "";
		}

		print $hbo->populateHTML('view_scale_row', [ $interval, $max_interval, $min_interval, $lower_boundary, $time->{corr_comments}], ['interval', 'max_interval', 'min_interval', 'lower_boundary', 'corr_comments']);
	}

	if ( $nlower > 0 )	{
		print "<tr><td colspan=\"3\" style=\"height: 0.5em; border-top: 1px gray solid;\"></td></tr>\n";
	}

	print "</table>\n</table>\n</div></div>\n<p>\n";

    $row->{'reference'} = Reference::formatShortRef($dbt,$row->{'reference_no'},'link_id'=>1);
	print $hbo->populateHTML('view_scale_top', $row);

	if ( $stage eq "summary" )	{
		print "<div align=\"center\"><p><b><a href=\"$WRITE_URL?action=processShowForm&scale=" , $q->param('scale') , "\">Edit this time scale</a></b> - ";
		print "<b><a href=\"$WRITE_URL?action=processShowForm\">Create a new time scale</a></b> - ";
		print "<b><a href=\"$WRITE_URL?action=startScale\">Edit another time scale</a></b></p></div>\n\n";
	} else	{
		print "<div align=\"center\"><p><b><a href=\"$WRITE_URL?action=startScale\">View another time scale</a></b></p></div>\n\n";
	}

	return;
}

sub processEditScaleForm	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;

	my $authorizer_no = $s->get('authorizer_no');
	my $enterer_no = $s->get('enterer_no');
	return unless $authorizer_no;

    my $dbh_r = $dbt->dbh;

	my $scale_no = int($q->param('scale_no'));

	my $scale_name = $q->param('scale_name');

	my @correlation_nos = $q->param('correlation_no');
	my @interval_nos = $q->param('interval_no');

	my @eml_intervals = $q->param('eml_interval');
	my @eml_max_intervals = $q->param('eml_max_interval');
	my @eml_min_intervals = $q->param('eml_min_interval');

	my @interval_names = $q->param('interval');
	my @max_interval_names = $q->param('max_interval');
	my @min_interval_names = $q->param('min_interval');

	my @lower_boundaries = $q->param('lower_boundary');
	my @comments_fields = $q->param('corr_comments');

	my %vars = (
		reference_no=>$s->get('reference_no'),
		scale_name  =>$scale_name,
		continent   =>$q->param('continent'),
		basis       =>$q->param('basis'),
		scale_rank  =>$q->param('scale_rank'),
		scale_comments=>$q->param('scale_comments')
	);
	if ($scale_no > 0 )	{
	    # update the scales table
		$dbt->updateRecord($s,'scales','scale_no',$scale_no,\%vars);
	} else	{
    	# add to the scales table
		my ($result,$id) = $dbt->insertRecord($s,'scales',\%vars);
		$scale_no = $id;
	}

	my %fieldused;
    my %badseen;
    my (@badintervals);
    my $next_interval_no;
	foreach my $i ( 0..$#interval_names )	{

		# not sure if this syntax is correct, but nothing more
		#  complicated seems to work

		# try to figure out the interval no of the max and min
		#  interval names that were entered

		my $max_interval_no = "";
		my $min_interval_no = "";
		if ( $max_interval_names[$i] =~ /[A-Za-z]/ )	{
			my $sql = "SELECT interval_no FROM intervals WHERE ";
			$sql .= "eml_interval='" . $eml_max_intervals[$i] . "'";
			$sql .= " AND interval_name='" . $max_interval_names[$i] . "'";
			my @nos = @{$dbt->getData($sql)};
			$max_interval_no = $nos[0]->{interval_no};
			if ( $max_interval_no < 1 && ! $badseen{$max_interval_names[$i]} )	{
				push @badintervals, $max_interval_names[$i];
				$badseen{$max_interval_names[$i]} = "Y";
			}
		}

		if ( $min_interval_names[$i] =~ /[A-Za-z]/ )	{
			my $sql = "SELECT interval_no FROM intervals WHERE ";
			$sql .= "eml_interval='" . $eml_min_intervals[$i] . "'";
			$sql .= " AND interval_name='" . $min_interval_names[$i] . "'";
			my @nos = @{$dbt->getData($sql)};
			$min_interval_no = $nos[0]->{interval_no};
			if ( $min_interval_no < 1 && ! $badseen{$min_interval_names[$i]} )	{
				push @badintervals, $min_interval_names[$i];
				$badseen{$min_interval_names[$i]} = "Y";
			}
		}

		# if the interval name and no don't match, erase the no
		if ( $correlation_nos[$i] > 0 && $interval_nos[$i] > 0 )	{
			my $sql = "SELECT eml_interval,interval_name FROM intervals WHERE ";
			$sql .= " interval_no=" . $interval_nos[$i];
			my @names = @{$dbt->getData($sql)};
			if ( $names[0]->{eml_interval} ne $eml_intervals[$i] || $names[0]->{interval_name} ne $interval_names[$i] )	{
				$interval_nos[$i] = "";
			}
		}

		if ( $correlation_nos[$i] > 0 )	{
			# update the correlations able
			my $sql = "UPDATE correlations SET ";
			$sql .= "modifier_no='" . $enterer_no;
			$sql .= "',next_interval_no='" . $next_interval_no;
			$sql .= "',max_interval_no='" . $max_interval_no;
			$sql .= "',min_interval_no='" . $min_interval_no;
			$sql .= "',lower_boundary='" . $lower_boundaries[$i];
			$sql .= "',corr_comments='" . $comments_fields[$i] . "'";
			$sql .= " WHERE correlation_no=" . $correlation_nos[$i];
			$dbh_r->do($sql);
		}

		if ( $correlation_nos[$i] > 0 && $interval_nos[$i] > 0 )	{
			# update the intervals table
			my $sql = "UPDATE intervals SET ";
			$sql .= "modifier_no='" . $enterer_no . "'";
			$sql .= " WHERE interval_no=" . $interval_nos[$i];
			$dbh_r->do($sql);

			$next_interval_no = $interval_nos[$i];

		} elsif ( $interval_names[$i] =~ /[A-Za-z]/)	{
			# look for the interval
			my $sql = "SELECT interval_no FROM intervals WHERE ";
			$sql .= "eml_interval='" . $eml_intervals[$i] . "'";
			$sql .= " AND interval_name='" . $interval_names[$i] . "'";
			my @nos = @{$dbt->getData($sql)};
			my $interval_no = $nos[0]->{interval_no};

			# add to the intervals table if the interval wasn't found
			if ( $interval_no eq "" )	{
				$sql = "INSERT INTO intervals (authorizer_no, enterer_no, eml_interval, reference_no, interval_name) VALUES (";
				$sql .= $authorizer_no . ", ";
				$sql .= $enterer_no . ", '";
				$sql .= $eml_intervals[$i] . "', ";
				$sql .= $s->get('reference_no') . ", '";
				$sql .= $interval_names[$i] . "')";
				$dbh_r->do($sql);

				# set the interval no
				$sql = "SELECT interval_no FROM intervals WHERE ";
				$sql .= "eml_interval='" . $eml_intervals[$i] . "'";
				$sql .= " AND interval_name='" . $interval_names[$i] . "'";
				my @nos = @{$dbt->getData($sql)};
				$interval_no = $nos[0]->{interval_no};

				# set the created date
				$sql = "SELECT modified FROM intervals WHERE interval_no='";
				$sql .= $interval_no . "'";
				my @modifieds = @{$dbt->getData($sql)};
				$sql = "UPDATE intervals SET modified=modified,created=";
				$sql .= $modifieds[0]->{modified} . " WHERE interval_no=" . $interval_no;
				$dbh_r->do($sql);

			# now that the interval exists, put its number wherever
			#  it belongs in the collections table
			# this is pretty complicated and the code here is
			#  basically new JA 11-12,15-16.12.05
				my @fields = ('period','period eml','epoch','epoch eml','intage','intage eml','locage','locage eml');
				for my $minmax ( 'max','min' )	{
			# figure out which field was used to populate max or
			#  min interval_no for all the collections
			# only do this the first time a new time interval name
			#  is encountered
					if ( ! %{$fieldused{$minmax}} )	{
						my $sql;
						if ( $minmax eq "max" )	{
							$sql = "SELECT collection_no,eml_interval,interval_name,emlperiod_max,period_max,emlepoch_max,epoch_max,emlintage_max,intage_max,emllocage_max,locage_max FROM collections,intervals WHERE max_interval_no=interval_no";
						} else	{
							$sql = "SELECT collection_no,eml_interval,interval_name,emlperiod_min,period_min,emlepoch_min,epoch_min,emlintage_min,intage_min,emllocage_min,locage_min FROM collections,intervals WHERE min_interval_no=interval_no";
						}
						my @collnos = @{$dbt->getData($sql)};
						foreach my $fieldno ( 0..$#fields )	{
							my ($f,$e) = split / /, $fields[$fieldno];
							my $emlfield_minmax = "eml" . $f . "_" . $minmax;
							my $field_minmax = $f . "_" . $minmax;
							foreach my $coll ( @collnos )	{
								if ( $e eq "eml" && $coll->{eml_interval} =~ /[A-Za-z]/ && $coll->{eml_interval} eq $coll->{$emlfield_minmax} && lc($coll->{interval_name}) eq lc($coll->{$field_minmax}) )	{
									$fieldused{$minmax}{$coll->{collection_no}} = $fieldno + 1;
								} elsif ( $e ne "eml" && $coll->{eml_interval} !~ /[A-Za-z]/ && lc($coll->{interval_name}) eq lc($coll->{$field_minmax}) )	{
									$fieldused{$minmax}{$coll->{collection_no}} = $fieldno + 1;
								}
							}
						}
					}
					my @matches = ();
					for my $fieldno ( 0..$#fields )	{
						my ($f,$e) = split / /, $fields[$fieldno];
						if ( ( $eml_intervals[$i] eq "" && $e eq "" ) || ( $eml_intervals[$i] ne "" && $e ne "" ) )	{
			# figure out which collections might use the new
			#  interval name because it appears in one of the
			#  legacy time term fields
							my $sql;
			# if the interval has no EML, the collection may or
			#  may not
							if ( $eml_intervals[$i] eq "" )	{
								$sql = "SELECT collection_no FROM collections WHERE " . $f . "_" . $minmax . "='" . $interval_names[$i] . "'";
							}
			# if the interval has an EML, the collection also has to
							else	{
								$sql = "SELECT collection_no FROM collections WHERE eml" . $f . "_" . $minmax . "='" . $eml_intervals[$i] . "' AND " . $f . "_" . $minmax . "='" . $interval_names[$i] . "'";
							}
							my @collnos = @{$dbt->getData($sql)};
			# find the collections that currently have interval
			#  numbers tied to more general time terms than the
			#  one that has just been entered
							foreach my $coll ( @collnos )	{
								if ( $fieldused{$minmax}{$coll->{collection_no}} < $fieldno + 1 && $fieldused{$minmax}{$coll->{collection_no}} > 0 )	{
									push @matches,$coll->{collection_no};
								}
							}
						}
					}

			# fix the interval numbers of the matching collections
					if ( $#matches > -1 )	{
						if ( $minmax eq "max" )	{
							my $sql = "UPDATE collections SET modified=modified,max_interval_no='" . $interval_no . "' WHERE collection_no IN ( " . join(",",@matches) . " )";
							$dbh_r->do($sql);
						} elsif ( $minmax eq "min" )	{
							my $sql = "UPDATE collections SET modified=modified,min_interval_no='" . $interval_no . "' WHERE collection_no IN ( " . join(",",@matches) . " )";
							$dbh_r->do($sql);
						}
					}
				}
			}

			# get the ID nos of the max and min intervals
			if ( $max_interval_names[$i] =~ /[A-Za-z]/ )	{
				$sql = "SELECT interval_no FROM intervals WHERE ";
				$sql .= "eml_interval='" . $eml_max_intervals[$i] . "'";
				$sql .= " AND interval_name='" . $max_interval_names[$i] . "'";
				my @nos = @{$dbt->getData($sql)};
				$max_interval_no = $nos[0]->{interval_no};
			} else	{
				$max_interval_no = 0;
			}

			if ( $min_interval_names[$i] =~ /[A-Za-z]/ )	{
				$sql = "SELECT interval_no FROM intervals WHERE ";
				$sql .= "eml_interval='" . $eml_min_intervals[$i] . "'";
				$sql .= " AND interval_name='" . $min_interval_names[$i] . "'";
				my @nos = @{$dbt->getData($sql)};
				$min_interval_no = $nos[0]->{interval_no};
			} else	{
				$min_interval_no = 0;
			}

			# if the correlation already exists, it was mostly
			#  updated above, but the interval number needs to be
			#  handled
			if ( $correlation_nos[$i] > 0 )	{
				$sql = "UPDATE correlations SET ";
				$sql .= "interval_no=" . $interval_no;
				$sql .= " WHERE correlation_no=" . $correlation_nos[$i];
				$dbh_r->do($sql);
			# otherwise, add to the correlations table
			} else	{
				$sql = "INSERT INTO correlations (authorizer_no, enterer_no, reference_no, scale_no, interval_no, next_interval_no, max_interval_no, min_interval_no, lower_boundary, corr_comments) VALUES (";
				$sql .= $authorizer_no . ", ";
				$sql .= $enterer_no . ", ";
				$sql .= $s->get('reference_no') . ", '";
				$sql .= $scale_no . "', '";
				$sql .= $interval_no . "', '";
				$sql .= $next_interval_no . "', '";
				$sql .= $max_interval_no . "', '";
				$sql .= $min_interval_no . "', '";
				$sql .= $lower_boundaries[$i] . "', '";
				$sql .= $comments_fields[$i] . "')";

				$dbh_r->do($sql);

				# set the created date
				$sql = "SELECT modified FROM correlations WHERE scale_no=" . $scale_no . " AND interval_no=" . $interval_no;
				my @modifieds = @{$dbt->getData($sql)};
				$sql = "UPDATE correlations SET modified=modified,created=";
				$sql .= $modifieds[0]->{modified} . " WHERE scale_no=" . $scale_no . " AND interval_no=" . $interval_no;
				$dbh_r->do($sql);
			}

			if ( $interval_no > 0 )	{
				$next_interval_no = $interval_no;
			}
		}

	}
    my $t = new TimeLookup($dbt);
    $t->generateLookupTable;

	$q->param('scale' => $scale_no);
	processViewTimeScale($dbt, $hbo, $q, $s, 'summary', \@badintervals);

}


# JA 9.8.04
sub displayTenMyBins	{
    my ($dbt,$q,$s,$hbo) = @_;
    my $t = new TimeLookup($dbt);


	print "<center><h2>10 m.y.-Long Sampling Bins</h2></center>\n\n";

	print "These bin definitions are used by the <a href=\"$READ_URL?action=displayCurveForm\">diversity curve generator</a>.\n\n";

	my @binnames = $t->getBins;
    my ($upperbin,$lowerbin) = $t->getBoundariesReal('bins');


	my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @results = @{$dbt->getData($sql)};

	my (%intervalname,%intervalalias);
    foreach my $row (@results) {
        my $name = "";
        $name = $row->{'eml_interval'}.' ' if ($row->{'eml_interval'});
        $name .= $row->{'interval_name'};

        my $alias = $row->{'interval_name'};
        if ( $row->{eml_interval} =~ /middle/i )	{
            $alias .= "F";
        } else	{
            $alias .= $row->{'eml_interval'};
		}
        $intervalname{$row->{'interval_no'}} = $name;
        $intervalalias{$row->{'interval_no'}} = $alias;
	}

	print "<hr>\n\n";

	print "<table><tr>\n";
	print "<td valign=top><b>Bin name</b></td>  <td valign=top><b>Age&nbsp;at&nbsp;base&nbsp;(Ma)</b></td>  <td valign=top><b>Included intervals</b></td></tr>\n";

    foreach my $bin (@binnames) {
        my @intervals = $t->mapIntervals($bin);  

		print "<tr><td valign=top nowrap>$bin</td>\n";
		printf "<td align=center valign=top>%.1f</td>\n",$lowerbin->{$bin};
		#printf "<td align=center valign=top>%.1f - %.1f</td>\n",$lowerbin->{$bin},$upperbin->{$bin};
		print "<td class=tiny>";
		@intervals = sort { $intervalalias{$a} cmp $intervalalias{$b} } @intervals;
		my $printed = 0;
		foreach my $int ( @intervals )	{
			if ( $printed > 0 )	{
				print ", ";
			}
			print "<a href=\"$READ_URL?action=displayInterval&interval_no=$int\">$intervalname{$int}</a>";
			$printed++;
		}
		print "</td></tr>\n\n";
	}
	print "</table>\n<p>\n\n";

}

sub displayTenMyBinsDebug {
    my ($dbt,$q,$s,$hbo) = @_;
    my $t = new TimeLookup($dbt);


	print "<center><h2>10 m.y.-Long Sampling Bins</h2></center>\n\n";

	print "These bin definitions are used by the <a href=\"$READ_URL?action=displayCurveForm\">diversity curve generator</a>.\n\n";

	my @binnames = $t->getBins;
    my $binning = $t->getBinning;
    my ($upperbin,$lowerbin) = $t->getBoundariesReal('bins');
    my $ig = $t->getIntervalGraph;

	my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @results = @{$dbt->getData($sql)};

	my (%intervalname,%intervalalias);
    foreach my $row (@results) {
        my $name = "";
        $name = $row->{'eml_interval'}.' ' if ($row->{'eml_interval'});
        $name .= $row->{'interval_name'};

        my $alias = $row->{'interval_name'};
        if ( $row->{eml_interval} =~ /middle/i )	{
            $alias .= "F";
        } else	{
            $alias .= $row->{'eml_interval'};
		}
        $intervalname{$row->{'interval_no'}} = $name;
        $intervalalias{$row->{'interval_no'}} = $alias;
	}

	print "<hr>\n\n";

	print "<table><tr>\n";
	print "<td valign=top><b>Bin name</b></td>  <td valign=top><b>Age&nbsp;at&nbsp;base&nbsp;(Ma)</b></td>  <td valign=top><b>Included intervals</b></td></tr>\n";


    foreach my $bin (@binnames) {
        my @intervals = map{$ig->{$_}} $t->mapIntervals($bin);  
        my %ok_ints = ();
        $ok_ints{$_->{'interval_no'}} = 1 foreach (@intervals);
        my $filter = sub {
            return $ok_ints{$_[0]->{'interval_no'}}
        };
        my @base = ();
        foreach (@intervals) {
            if ($binning->{$_->{'interval_no'}}) {
                push @base, $_;
            }
        }

		print "<tr><td valign=top>$bin</td>\n";
		printf "<td align=center valign=top>%.1f - %.1f</td>\n",$lowerbin->{$bin},$upperbin->{$bin};
		print "<td class=tiny>";
        printTree($ig,\@base,$filter);
		print "</td></tr>\n\n";
	}
	print "</table>\n<p>\n\n";

}

sub itvsort {
    $a->{'all_scales'}->[0]->{'scale_no'} <=> $b->{'all_scales'}->[0]->{'scale_no'}
    ||
    $b->{'lower_boundary'} <=> $a->{'lower_boundary'} 
    ||
    $b->{'interval_no'} <=> $a->{'interval_no'}
}

sub displayFullScale {
    my ($dbt,$hbo) = @_;
    my $t = new TimeLookup($dbt);
    $t->getBoundaries();
    my $ig = $t->getIntervalGraph();

    my @base = ();
    foreach my $itv (values %$ig) {
        if (!$itv->{'max'}) {
            push @base, $itv;
        }
    }
    printTree($ig,\@base);
}

sub printInterval {
    my $itv = shift;
    my $overline = shift;
    print "&nbsp;" x ($itv->{'depth'}*4);
    if ($overline) {
        print "<span style='border-top: 1px black solid'>";
    }
    my $scale = "scale $itv->{best_scale}->{scale_no}:$itv->{best_scale}->{continent}:$itv->{best_scale}->{pubyr}";
    my $bounds = "lower [$itv->{lower_max}/$itv->{lower_boundary}/$itv->{lower_min}] to upper [$itv->{upper_max}/$itv->{upper_boundary}/$itv->{upper_min}]";
    print "$itv->{interval_no}: $itv->{name} - $bounds - $scale <br>";
    if ($overline) {
        print "</span>";
    }
}

sub printTree {
    my ($ig,$base,$filter) = @_;
    my @base = @$base;
   
    $_->{'depth'} = 0 foreach (@base);
    @base = sort itvsort @base; 

    my $last_depth = 0;
    my $last_scale = 0;
    print "<small>";
    while (my $itv = pop @base) {
        my $split = 0;
        if ($last_depth == $itv->{'depth'} && $last_scale != $itv->{'next_scale'}->{'scale_no'}) {
            $split = 1;
        }
        printInterval($itv,$split);
        $last_depth = $itv->{'depth'};
        $last_scale = $itv->{'next_scale'}->{'scale_no'};

        my @children = ();
        foreach (@{$itv->{'children'}}) {
            if ($_ == $itv->{'max'} || $_ == $itv->{'min'}) {
                print "SKIPPING $_->{interval_no}:$_->{name}, Loop";
            } else {
                if (!$filter || $filter->($_)) {
                    push @children, $_;
                } 
            }
        }

        $_->{'depth'} = $itv->{'depth'}+1 foreach (@children);
        @children = sort itvsort @children; 

        push @base, @children;
    }
    print "</small>";
}

sub displayInterval {
    my ($dbt,$hbo,$q) = @_;
    my $i = int($q->param('interval_no'));
    return unless $i;

    my $t = new TimeLookup($dbt);
    my $lookup_hash =  $t->lookupIntervals([$i],['interval_name','period_no','period_name','epoch_no','epoch_name','subepoch_no','subepoch_name','stage_no','stage_name','ten_my_bin','lower_boundary','upper_boundary','interval_hash']);
    my $itv_hash = $lookup_hash->{$i};
    return unless $itv_hash;

    my $itv = $t->deserializeItv($itv_hash->{'interval_hash'});
    my $best_scale = $itv->{best_scale};

    my $type = "";
    if ($best_scale) {
        my $rank = $best_scale->{scale_rank};
        $type = ($rank eq 'eon/eonothem') ? "eon"
              : ($rank eq 'era/erathem') ? "era"
              : ($rank eq 'subperiod/system') ? "subperiod"
              : ($rank eq 'period/system') ? "period"
              : ($rank eq 'subepoch/series') ? "subepoch"
              : ($rank eq 'epoch/series') ? "epoch"
              : ($rank eq 'age/stage') ? $rank
              : ($rank eq 'subage/stage') ? $rank
              : ($rank eq 'chron/zone') ? 'zone'
              : "";
    }
    my %shared_lower = ();
    my %shared_upper= ();
    foreach (@{$itv->{'shared_lower'}}) {
        $shared_lower{$_->{'interval_no'}} = 1;
    }
    foreach (@{$itv->{'shared_upper'}}) {
        $shared_upper{$_->{'interval_no'}} = 1;
    }

    print "<div align=\"center\"><h3>$itv->{interval_name} $type</h3></div>";

    sub describeEstimate {
        my $itv = shift;
        my $type = shift;
        my $src = $itv->{$type.'_boundarysrc'};
        my $src_link = "<a href=\"$READ_URL?action=displayInterval&interval_no=$src->{interval_no}\">$src->{interval_name}</a>";
        my $i = $src->{interval_no};
        if ($itv->{$type."_boundary"} eq "0" && $itv->{$type.'_estimate_type'} =~ /direct/) {
            return "direct";
        } if ($itv->{$type.'_estimate_type'} =~ /direct/ && $itv->{$type.'_boundarysrc_no'} == $i) {
            return "direct";
        } elsif ($itv->{$type.'_estimate_type'} =~ /direct/ && $itv->{$type.'_boundarysrc_no'} != $i) {
            my $msg;
            unless ($itv->{$type.'_boundarysrc_no'} == $itv->{'next_no'}) { 
                $msg = "base of $src_link";
            } else {
                $msg = "direct";
            }
            return $msg;
        } elsif ($itv->{$type.'_estimate_type'} =~ /correlated/) {
#            return "correlated with base of $src_link";
            return "base of $src_link (approximate)";
        } elsif ($itv->{$type.'_estimate_type'} =~ /children/) {
            return "estimated from subintervals"; 
        } elsif ($itv->{$type.'_estimate_type'} =~ /next/) {
            return "from next interval $src_link";
        } elsif ($itv->{$type.'_estimate_type'} =~ /previous/) {
            return "from previous interval $src_link";
        }
    }
   
    print "<table width=\"100%\" cellspacing=\"5\" cellpadding=\"0\" border=\"0\"><tr><td valign=top width=\"50%\">";
    my $general_html = "";
    if ($itv->{'lower_boundary'} =~ /\d/) {
        my $lower = TimeLookup::printBoundary($itv->{'lower_boundary'}); 
        $general_html .= "<b>Lower boundary</b>: $lower Ma<br>";
        my $estimate = describeEstimate($itv,'lower');
        if ($estimate) {
            $general_html .= "<b>Lower boundary source</b>: ".$estimate."<br>";
        }
        
        my $upper = TimeLookup::printBoundary($itv->{'upper_boundary'}); 
        $general_html .= "<b>Upper boundary</b>: $upper Ma<br>";
        $estimate = describeEstimate($itv,'upper');
        if ($estimate) {
            $general_html .= "<b>Upper boundary source</b>: ".$estimate."<br>";
        }

        if ($itv->{best_scale}) {
            $general_html .= "<b>Continent</b>: $itv->{best_scale}->{continent}<br>";
        }
    }
    $general_html .= "<a href=# onClick=\"document.doColls.submit();\">See collections within this interval</a><br>";
    $general_html .= "<a href=# onClick=\"document.doMap.submit();\">See map of collections within this interval</a><br>";
    $general_html .= qq|<form method="POST" action="$READ_URL" name="doColls"><input type="hidden" name="action" value="displayCollResults"><input type="hidden" name="max_interval_no" value="$i"></form>|;
    $general_html .= qq|<form method="POST" action="$READ_URL" name="doMap"><input type="hidden" name="action" value="displaySimpleMap"><input type="hidden" name="max_interval_no" value="$i"></form>|;

    print $hbo->htmlBox("General",$general_html);
    
    my $corr_html; 
    if ($itv_hash->{'period_no'} && $itv_hash->{'period_no'} != $i) {
        $corr_html .= "<b>Period</b>: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv_hash->{period_no}\">$itv_hash->{period_name}</a><br>";
    }
    if ($itv_hash->{'epoch_no'} && $itv_hash->{'epoch_no'} != $i) {
        $corr_html .= "<b>Epoch</b>: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv_hash->{epoch_no}\">$itv_hash->{'epoch_name'}</a><br>";
    }
    if ($itv_hash->{'subepoch_no'} && $itv_hash->{'subepoch_no'} != $i) {
        $corr_html .= "<b>Subepoch</b>: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv_hash->{subepoch_no}\">$itv_hash->{'subepoch_name'}</a><br>";
    }
    if ($itv_hash->{'ten_my_bin'}) {
        $corr_html .= "<b>10 million year bin</b>: $itv_hash->{ten_my_bin}<br>";
    }
    if ($itv->{'max_no'} == $itv->{'min_no'} && 
        $itv->{'max_no'} != $itv_hash->{'epoch_no'} &&
        $itv->{'max_no'} != $itv_hash->{'period_no'} ) {
        my $shares = ($shared_upper{$itv->{'max_no'}}) ? " (shares upper boundary)"
                   : ($shared_lower{$itv->{'max_no'}}) ? " (shares lower boundary)" : "";
        $corr_html .= "<b>Contained within</b>: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{max_no}\">$itv->{max}->{interval_name}</a> $shares<br>";
    }
    if ($corr_html) {
        print $hbo->htmlBox("Correlations",$corr_html);
    }

    if ($itv->{'all_prev'}) {
        my $html = "";
        for(my $i=0;$i<@{$itv->{'all_prev'}};$i++) {
            my $prev = $itv->{'all_prev'}->[$i];     
            $html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$prev->{interval_no}\">$prev->{interval_name}</a></li>";
        }
        my $s = (@{$itv->{'all_prev'}} > 1) ? "s" : "";
        print $hbo->htmlBox("Previous interval$s",$html);
    }
    if ($itv->{'all_next'}) {
        my $html = "";
        for(my $i=0;$i<@{$itv->{'all_next'}};$i++) {
            my $next = $itv->{'all_next'}->[$i];     
            $html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$next->{interval_no}\">$next->{interval_name}</a></li>";
        }
        my $s = (@{$itv->{'all_next'}} > 1) ? "s" : "";
        print $hbo->htmlBox("Next interval$s",$html);
    }
    
    my @direct_equiv;
    foreach (@{$itv->{'shared_upper'}}) {
        if ($shared_lower{$_->{'interval_no'}}) {
            push @direct_equiv, $_ unless $_->{'interval_no'} == $i;
        }
    }
    my @equiv = @{$itv->{'equiv'}} if ($itv->{'equiv'});
    if (@direct_equiv || @equiv) {
        my $html = "";
        my $range = "";
        foreach my $e (@direct_equiv) {
            $html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$e->{interval_no}\">$e->{interval_name}</a> $range</li>";
        }
        if (@equiv) {
            my @intervals = ();
            foreach my $e (@equiv) {
                push @intervals, "<a href=\"$READ_URL?action=displayInterval&interval_no=$e->{interval_no}\">$e->{interval_name}</a>";
            }
            $html .= "<li>".join(" + ",@intervals)."</li>";
        }
        print $hbo->htmlBox("Equivalent to",$html);
    }
    print "</td><td valign=top width=\"50%\">";
    
    my @overlaps;
    my @contains;
    if ($itv->{'children'}) {
        foreach (@{$itv->{'children'}}) {
            if ($_->{'max_no'} == $i && $_->{'min_no'} != $i) {
                push @overlaps, $_;
            } elsif ($_->{'max_no'} != $i && $_->{'min_no'} == $i) {
                push @overlaps, $_;
            } elsif ($_->{'max_no'} == $i && $_->{'min_no'} == $i) {
                push @contains, $_;
            }
        }
    }
    if ($itv->{'max_no'} != $itv->{'min_no'}) {
        push @overlaps, $itv->{'max'}, $itv->{'min'};
    }
    if (@overlaps) {
        my $html = "";
        my $range = "";
        foreach my $o (@overlaps) {
#            if ($o->{'lower_boundary'}) {
#                $range = "(".$o->{'lower_boundary'}." - ".$o->{'upper_boundary'}.")";
#            }
            $html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$o->{interval_no}\">$o->{interval_name}</a> $range</li>";
        }
        print $hbo->htmlBox("Overlaps with",$html);
    }
    if (@contains) {
        my $html = "";
        my $range = "";
        foreach my $c (@contains) {
#            if ($c->{'lower_boundary'}) {
#                $range = "(".$c->{'lower_boundary'}." - ".$c->{'upper_boundary'}.")";
#            }
            my $shares = ($shared_upper{$c->{'interval_no'}}) ? " (shares upper boundary)"
                       : ($shared_lower{$c->{'interval_no'}}) ? " (shares lower boundary)" : "";
            $html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$c->{interval_no}\">$c->{interval_name}</a> $range $shares</li>";
        }
        print $hbo->htmlBox("Contains",$html);
    }


    my %all_scales;
    foreach ('all_next_scales','all_max_scales') {
        if ($itv->{$_}) {
            foreach my $scale (@{$itv->{$_}}) {
                $all_scales{$scale->{'scale_no'}} = $scale;
            }
        }
    }
    if (%all_scales) {
        my $html = "";
        my @scales = values %all_scales;
        @scales = sort {$b->{'pubyr'} <=> $a->{'pubyr'} ||
                        $b->{'scale_no'} <=> $a->{'scale_no'}} @scales;

        foreach my $scale (@scales) {
            $html .= "<li><a href=\"$READ_URL?action=processViewScale&scale_no=$scale->{scale_no}\">$scale->{scale_name}</a> ($scale->{pubyr})<br>";
        }
        my $s = (@scales  > 1) ? "s" : "";
        print $hbo->htmlBox("Appears in scale$s",$html);
    }
    print "</td></tr></table>";
    print PBDBUtil::printIntervalsJava($dbt,1);
    print $hbo->populateHTML("search_intervals_form");
}

sub submitSearchInterval {
    my ($dbt,$hbo,$q) = @_;
    my $dbh = $dbt->dbh;
    my $eml  = $q->param('eml_interval');
    my $name = $q->param('interval_name');
    my $sql = "SELECT interval_no FROM intervals WHERE eml_interval LIKE ".$dbh->quote($eml)." AND interval_name LIKE ".$dbh->quote($name);
    my $row = ${$dbt->getData($sql)}[0];
    if (!$row && $eml) {
        $sql = "SELECT interval_no FROM intervals WHERE eml_interval LIKE '' AND interval_name LIKE ".$dbh->quote($name);
        $row = ${$dbt->getData($sql)}[0];
    }
    if (!$row) {
        print "<div align=\"center\">";
        print "<h3>Could not find ".$q->param('eml')." ".$q->param('interval_name')."</h3>";
        print "<h4>(Please try again)</h4>";
        print PBDBUtil::printIntervalsJava($dbt,1);
        print $hbo->populateHTML("search_intervals_form");
        print "</div>";
    } else {
        $q->param('interval_no'=>$row->{'interval_no'});
        displayInterval($dbt,$hbo,$q);
    }
}

sub displayIntervalDebug {
    my ($dbt,$hbo,$q) = @_;
    my $i = int($q->param('interval_no'));
    return unless $i;

    print "<div class=small>";
   
    my $t = new TimeLookup($dbt);
    my @bounds = $t->getBoundaries;
    my $ig = $t->getIntervalGraph();
    my $itv = $ig->{$i};
    
    my $printer = sub {
        my $itv = shift;
        my $from_scale = shift;
        my $txt = "";
        if ($itv) {
            #$txt = "<a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{interval_no}\">$itv->{name}</a>: $itv->{lower_boundary} - $itv->{upper_boundary}";
            $txt = "<a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{interval_no}\">$itv->{name}</a>";
            #<a href=\"$READ_URL?action=processViewScale&scale=$bestscale{$i}\">scale:$bestscale{$i}</a><br>";
        }
        return $txt;
    };

    print "<div align=\"center\"><h3>".$printer->($itv)."</h3></div>";

    if ($itv->{'max'}) {
        print "Maximum correlate: ".$printer->($itv->{'max'})."<br>";
        if ($itv->{'min'} != $itv->{'max'}) {
            print "Minimum correlate: ".$printer->($itv->{'max'})."<br>";
        }
    }
    if ($itv->{'all_prev'}) {
        my @prev = ();
        foreach my $p (@{$itv->{'all_prev'}}) {
            if ($p->{'next'} == $itv) {
                push @prev, $p;
            }
        } 

        if (@prev > 1) {
            print "Previous intervals: ".join(", ",map {$printer->($_)} @prev)."<br>";
        } elsif (@prev == 1) {
            print "Previous interval: ".$printer->($prev[0])."<br>";
        }
    }

    if ($itv->{'next'}) {
        print "Next interval: ".$printer->($itv->{'next'})."<br>";
    }

    foreach my $c (sort itvsort @{$itv->{'children'}}) {
        print "$c->{interval_no}:$c->{name} ";
    }
    print "</td></tr></table>";

    foreach my $abbrev ('gl','As','Au','Eu','NZ','NA','SA') {
        my $lower_max = 'lower_max'.$abbrev;
        my $lower_boundary = 'lower_boundary'.$abbrev;
        my $lower_min = 'lower_min'.$abbrev;
        my $upper_max = 'upper_max'.$abbrev;
        my $upper_boundary = 'upper_boundary'.$abbrev;
        my $upper_min = 'upper_min'.$abbrev;
        if ($itv->{$lower_max} || $itv->{$lower_boundary} || $itv->{$lower_min} ||
            $itv->{$upper_max} || $itv->{$upper_boundary} || $itv->{$upper_min}) {
            print "  $abbrev:lower:[$itv->{$lower_max}/$itv->{$lower_boundary}/$itv->{$lower_min}] - $abbrev:upper:[$itv->{$upper_max}/$itv->{$upper_boundary}/$itv->{$upper_min}]<br>";
        } 
    }

    print "Scales : <br>";
    foreach my $s (@{$itv->{'all_scales'}}) {
        print " $s->{scale_name} $s->{continent} $s->{pubyr}<br>";
    }
    print "<br>";
    foreach my $c (@{$itv->{'constraints'}}) {
        print TimeLookup::_printConstraint($c)."<br>";
    }
    foreach my $c (@{$itv->{'conflicts'}}) {
        print TimeLookup::_printConstraint($c)."<br>";
    }

    print "</td></tr>";
    print "</table>";


}

1;
