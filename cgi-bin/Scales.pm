package Scales;

$DEBUG = 0;

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

sub startSearchScale	{
	my $dbh = shift;
	my $dbt = shift;
	my $session = shift;
	my $exec_url = shift;

	# Print the form
	print main::stdIncludes("std_page_top");
	print "<DIV class=\"title\">Select a time scale to view</DIV>\n<CENTER>";

	# Retrieve each scale's name from the database
	my $sql = "SELECT authorizer_no,scale_no,scale_name,reference_no FROM scales";
	my @results = @{$dbt->getData($sql)};
	my @scale_strings;
	for my $scaleref (@results)	{
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
		$scale_strings{$name} = $option . $name . $auth;

		# Get the authorizer's name
		$sql2 = "SELECT name FROM person WHERE person_no=" . $scaleref->{authorizer_no};
		@results2 = @{$dbt->getData($sql2)};
		$scale_authorizer{$name} = $results2[0]->{name};
	}

	print "<table cellpadding=5>\n";
	print "<tr><td align=\"right\"> ";
	print "<form name=\"scale_view_form\" action=\"$exec_url\" method=\"POST\">\n";
	print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"processViewScale\">\n\n";
	print "<select name=\"scale\">\n";

	my @sorted = sort keys %scale_strings;
	for my $string (@sorted)	{
		print $scale_strings{$string};
	}

	print "</select>\n\n";
	print "</td><td align=\"left\"> ";
	print "<input type=\"submit\" value=\"View scale\"></form>";
	print "</td></tr> ";

	print "<tr><td><p></p></td></tr>\n";

#	if ( $session->get('enterer') ne "Guest" && $session->get('enterer') ne "" )	{
	if ( $session->get('enterer') eq "J. Alroy" || $session->get('enterer') eq "S. Holland" )	{
		print "<tr><td colspan=2 align=\"center\" valign=\"bottom\"><h3>... or select a time scale to add or edit</h3></td></tr>\n";

		print "<tr><td align=\"right\"> ";
		print "<form name=\"scale_add_form\" action=\"$exec_url\" method=\"POST\">\n";
		print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"processShowForm\">\n\n";
		print "<select name=\"scale\">\n";
		print "<option selected value=\"\">------------ select to create a new time scale ------------\n";
		for my $string (@sorted)	{
			if ( $session->get('authorizer') eq $scale_authorizer{$string} )	{
				print $scale_strings{$string};
			}
		}
		print "</select>\n\n";
		print "</td><td align=\"left\"> ";
		print "<input type=\"submit\" value=\"Add/edit scale\"></form>";
		print "</td></tr> ";
	}
	print "</table>\n<p>\n";

	print "<p align=\"left\" class=\"small\">All data on the web site are prepared using an automatically generated composite time scale. The composite scale is based on the latest published correlations and boundary estimates for each time interval. Individual scales may be viewed by selecting from the pulldown menu above. Note, however, that the correlations in an individual scale might not be used in computations because they might not be the most recently published.</p>\n";

	print main::stdIncludes("std_page_bottom");

	return;

}

sub processShowEditForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $session = shift;
	my $exec_url = shift;

	# Have to have a reference #
	my $reference_no = $session->get("reference_no");
	if ( ! $reference_no )	{
		if ( $q->param('scale') )	{
			$session->enqueue( $dbh, "action=processShowForm&scale=" . $q->param('scale') );
		} else	{
			$session->enqueue( $dbh, "action=processShowForm" );
		}
		main::displaySearchRefs ( "Please choose a reference first" );
		exit;
	}

	print main::stdIncludes("std_page_top");

	# Get data on the scale if it's an old one
	my @results;
	my @olddata;
	my @times;
	if ( $q->param('scale') )	{
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

	print "<form name=\"edit_scale_form\" action=\"$exec_url\" method=\"POST\" onSubmit=\"return checkFields();\">\n";
	print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"processEditScale\">\n\n";

	# print out the time scale entry fields
	print $hbo->populateHTML('enter_scale_top', \@olddata, ['scale_no', 'scale_name', 'continent', 'basis', 'scale_rank', 'scale_comments']);

	# print out the time interval rows

	print "<center>\n<table>\n";

	print "<tr><td align=\"center\" colspan=2><b><font color=\"red\">Interval</a></b> </td><td align=\"center\" colspan=2>Maximum correlate </td><td align=\"center\" colspan=2>Minimum correlate </td><td align=\"center\" valign=\"bottom\">Lower<br>boundary </td></tr>\n";

	$maxi = $#times;
	if ( $maxi < 1 )	{
		$maxi = 24;
	} else	{
		$maxi = $maxi + 10;
	}
	for my $i (0..$maxi)	{
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
			$sql = $sqlstem . $times[$i]->{interval_no};
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

	print "</table>\n</center>\n<p>\n";

	print "</form>\n";

	print main::stdIncludes("std_page_bottom");

	return;
}

sub processViewTimeScale	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $session = shift;
	my $exec_url = shift;
	my $stage = shift;
	my $bad = shift;

	my @badintervals = @$bad;

	if ( $stage ne "summary" )	{
		print main::stdIncludes("std_page_top");
	}

	# Get basic data on the time scale (WARNING: assumes one was selected properly)
	my $sql = "SELECT * FROM scales WHERE scale_no=" . $q->param('scale');
	my @results = @{$dbt->getData($sql)};

	# Get the authorizer and enterer names
	$sql = "SELECT name FROM person WHERE person_no=" . $results[0]{authorizer_no};
	my @names = @{$dbt->getData($sql)};
	$auth_name = $names[0]->{name};

	$sql = "SELECT name FROM person WHERE person_no=" . $results[0]{enterer_no};
	@names = @{$dbt->getData($sql)};
	$enterer_name = $names[0]->{name};

	print $hbo->populateHTML('view_scale_top', [ $auth_name, $enterer_name, $results[0]->{scale_name}, $results[0]->{continent}, $results[0]->{basis}, $results[0]->{scale_rank}, $results[0]->{scale_comments} ], [ 'authorizer', 'enterer', 'scale_name', 'continent', 'basis', 'scale_rank', 'scale_comments' ]);

	if ( @badintervals )	{
		print "<center><p><b><font color='red'>WARNING!</font></b> ";
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
		print "</p></center>";
	}

	print "<p>\n\n<center><table cellspacing=2><tr><td bgcolor=\"black\"><table bgcolor=\"white\">\n\n";

	# Get the scale's intervals
	$sql = "SELECT * FROM correlations WHERE scale_no=" . $q->param('scale');
	@times = @{$dbt->getData($sql)};

	# figure out what headers are needed

	my $nmax;
	my $nmin;
	my $nlower;
	my $ncomments;
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
		if ( $time->{corr_comments} ne "" )	{
			$ncomments++;
		}
	}

	print "<tr>";
	print "<td align=\"center\" valign=\"bottom\">&nbsp;<b>Interval</b></td>\n";
	print "<td valign=\"top\"><font size=+2>&nbsp;</font></td>\n";
	print "<td align=\"center\" valign=\"bottom\">";
	if ( $nmax > 0 )	{
		print "<b>Maximum&nbsp;correlate</b> ";
	}
	print "</td><td align=\"center\" valign=\"bottom\">";
	if ( $nmin > 0 )	{
		print "<b>Minimum&nbsp;correlate</b> ";
	}
	print "</td><td align=\"center\" valign=\"bottom\">";
	if ( $nlower > 0 )	{
		print "<b>Ma</b> ";
	}
	print "</td><td align=\"center\" valign=\"bottom\">";
	if ( $ncomments > 0 )	{
		print "<b>&nbsp;&nbsp;Comments&nbsp;&nbsp;</b> ";
	}
	print "</td>\n";
	print "</tr>\n";

	print "<tr><td colspan=6 valign=\"middle\">\n";
	print "<hr>\n";
	print "</td></tr>\n";


	# Print the rows
	for my $time (@times)	{

		$sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{interval_no};
		my @names = @{$dbt->getData($sql2)};
		my $eml_interval = $names[0]->{eml_interval};
		my $interval = $names[0]->{interval_name};

		$sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{max_interval_no};
		@names = @{$dbt->getData($sql2)};
		my $eml_max_interval = $names[0]->{eml_interval};
		my $max_interval = $names[0]->{interval_name};

		$sql2 = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $time->{min_interval_no};
		@names = @{$dbt->getData($sql2)};
		my $eml_min_interval = $names[0]->{eml_interval};
		my $min_interval = $names[0]->{interval_name};

		my $lower_boundary = $time->{lower_boundary};
		$lower_boundary =~ s/000$//;
		$lower_boundary =~ s/00$//;
		$lower_boundary =~ s/0$//;
		$lower_boundary =~ s/\.$//;
		if ( $lower_boundary == 0 )	{
			$lower_boundary = "";
		}

		print $hbo->populateHTML('view_scale_row', [ $eml_interval, $interval, $eml_max_interval, $max_interval, $eml_min_interval, $min_interval, $lower_boundary, $time->{corr_comments}], ['eml_interval', 'interval', 'eml_max_interval', 'max_interval', 'eml_min_interval', 'min_interval', 'lower_boundary', 'corr_comments']);
	}


	print "</table>\n</table>\n</center>\n<p>\n";

	if ( $stage eq "summary" )	{
		print "<center><p><b><a href=\"$exec_url?action=processShowForm&scale=" , $q->param('scale') , "\">Edit this time scale</a></b> - ";
		print "<b><a href=\"$exec_url?action=processShowForm\">Create a new time scale</a></b> - ";
		print "<b><a href=\"$exec_url?action=startScale\">Edit another time scale</a></b></p></center>\n\n";
	} else	{
		print "<center><p><b><a href=\"$exec_url?action=startScale\">View another time scale</a></b></p></center>\n\n";
	}

	if ( $stage ne "summary" )	{
		print main::stdIncludes("std_page_bottom");
	}

	return;

}

sub processEditScaleForm	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $session = shift;
	my $exec_url = shift;

	print main::stdIncludes("std_page_top");

	$scale_name = $q->param('scale_name');
	$scale_comments = $q->param('scale_comments');

	# escape single quotes
	$scale_name =~ s/'/\\'/g;
	$scale_comments =~ s/'/\\'/g;

	@correlation_nos = $q->param('correlation_no');
	@interval_nos = $q->param('interval_no');

	@eml_intervals = $q->param('eml_interval');
	@eml_max_intervals = $q->param('eml_max_interval');
	@eml_min_intervals = $q->param('eml_min_interval');

	@interval_names = $q->param('interval');
	@max_interval_names = $q->param('max_interval');
	@min_interval_names = $q->param('min_interval');

	@lower_boundaries = $q->param('lower_boundary');
	@comments_fields = $q->param('corr_comments');

	my $sql;

	$sql = "SELECT person_no FROM person WHERE name='";
	$sql .= $session->get('authorizer') . "'";
	my @nos = @{$dbt->getData($sql)};
	my $authorizer_no = $nos[0]->{person_no};

	$sql = "SELECT person_no FROM person WHERE name='";
	$sql .= $session->get('enterer') . "'";
	@nos = @{$dbt->getData($sql)};
	my $enterer_no = $nos[0]->{person_no};

	my $scale_no;
	if ( $q->param('scale_no') )	{
	# update the scales table
		$scale_no = $q->param('scale_no');
		$sql = "UPDATE scales SET modifier_no=";
		$sql .= $enterer_no . ", ";
		$sql .= "scale_name='". $scale_name . "', ";
		$sql .= "continent='" . $q->param('continent') . "', ";
		$sql .= "basis='" . $q->param('basis') . "', ";
		$sql .= "scale_rank='" . $q->param('scale_rank') . "', ";
		$sql .= "scale_comments='" . $scale_comments . "'";
		$sql .= " WHERE scale_no=" . $q->param('scale_no');
		$dbt->getData($sql);
	} else	{
	# add to the scales table

		$sql = "INSERT INTO scales (authorizer_no,enterer_no,reference_no,scale_name,continent,basis,scale_rank,scale_comments) VALUES ('";
		$sql .= $authorizer_no . "', '";
		$sql .= $enterer_no . "', '";
		$sql .= $session->get('reference_no') . "', '";
		$sql .= $scale_name . "', '";
		$sql .= $q->param('continent') . "', '";
		$sql .= $q->param('basis') . "', '";
		$sql .= $q->param('scale_rank') . "', '";
		$sql .= $scale_comments . "')";
		$dbt->getData($sql);

		$sql = "SELECT scale_no FROM scales WHERE scale_name='";
		$sql .= $scale_name . "'";
		my @nos = @{$dbt->getData($sql)};
		$scale_no = $nos[0]->{scale_no};

		# set the created date
		$sql = "SELECT modified FROM scales WHERE scale_name='";
		$sql .= $scale_name . "'";
		my @modifieds = @{$dbt->getData($sql)};
		$sql = "UPDATE scales SET modified=modified,created=";
		$sql .= $modifieds[0]->{modified} . " WHERE scale_no=" . $scale_no;
		$dbt->getData($sql);

	}

	my %fieldused;
	for $i ( 0..$#interval_names )	{

		# not sure if this syntax is correct, but nothing more
		#  complicated seems to work

		# try to figure out the interval no of the max and min
		#  interval names that were entered

		my $max_interval_no = "";
		my $min_interval_no = "";
		if ( $max_interval_names[$i] =~ /[A-Za-z]/ )	{
			$sql = "SELECT interval_no FROM intervals WHERE ";
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
			$sql = "SELECT interval_no FROM intervals WHERE ";
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
			$sql = "SELECT eml_interval,interval_name FROM intervals WHERE ";
			$sql .= " interval_no=" . $interval_nos[$i];
			my @names = @{$dbt->getData($sql)};
			if ( $names[0]->{eml_interval} ne $eml_intervals[$i] || $names[0]->{interval_name} ne $interval_names[$i] )	{
				$interval_nos[$i] = "";
			}
		}

		if ( $correlation_nos[$i] > 0 )	{
			# update the correlations able
			$sql = "UPDATE correlations SET ";
			$sql .= "modifier_no='" . $enterer_no;
			$sql .= "',next_interval_no='" . $next_interval_no;
			$sql .= "',max_interval_no='" . $max_interval_no;
			$sql .= "',min_interval_no='" . $min_interval_no;
			$sql .= "',lower_boundary='" . $lower_boundaries[$i];
			$sql .= "',corr_comments='" . $comments_fields[$i] . "'";
			$sql .= " WHERE correlation_no=" . $correlation_nos[$i];
			$dbt->getData($sql);
		}

		if ( $correlation_nos[$i] > 0 && $interval_nos[$i] > 0 )	{
			# update the intervals table
			$sql = "UPDATE intervals SET ";
			$sql .= "modifier_no='" . $enterer_no . "'";
			$sql .= " WHERE interval_no=" . $interval_nos[$i];
			$dbt->getData($sql);

			$next_interval_no = $interval_nos[$i];

		} elsif ( $interval_names[$i] =~ /[A-Za-z]/)	{


			# look for the interval
			$sql = "SELECT interval_no FROM intervals WHERE ";
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
				$sql .= $session->get('reference_no') . ", '";
				$sql .= $interval_names[$i] . "')";
				$dbt->getData($sql);

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
				$dbt->getData($sql);

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
						for my $fieldno ( 0..$#fields )	{
							my ($f,$e) = split / /, $fields[$fieldno];
							$emlfield_minmax = "eml" . $f . "_" . $minmax;
							$field_minmax = $f . "_" . $minmax;
							for $coll ( @collnos )	{
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
							for $coll ( @collnos )	{
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
							$dbt->getData($sql);
						} elsif ( $minmax eq "min" )	{
							my $sql = "UPDATE collections SET modified=modified,min_interval_no='" . $interval_no . "' WHERE collection_no IN ( " . join(",",@matches) . " )";
							$dbt->getData($sql);
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
				$dbt->getData($sql);
			# otherwise, add to the correlations table
			} else	{
				$sql = "INSERT INTO correlations (authorizer_no, enterer_no, reference_no, scale_no, interval_no, next_interval_no, max_interval_no, min_interval_no, lower_boundary, corr_comments) VALUES (";
				$sql .= $authorizer_no . ", ";
				$sql .= $enterer_no . ", ";
				$sql .= $session->get('reference_no') . ", '";
				$sql .= $scale_no . "', '";
				$sql .= $interval_no . "', '";
				$sql .= $next_interval_no . "', '";
				$sql .= $max_interval_no . "', '";
				$sql .= $min_interval_no . "', '";
				$sql .= $lower_boundaries[$i] . "', '";
				$sql .= $comments_fields[$i] . "')";

				$dbt->getData($sql);

				# set the created date
				$sql = "SELECT modified FROM correlations WHERE scale_no=" . $scale_no . " AND interval_no=" . $interval_no;
				my @modifieds = @{$dbt->getData($sql)};
				$sql = "UPDATE correlations SET modified=modified,created=";
				$sql .= $modifieds[0]->{modified} . " WHERE scale_no=" . $scale_no . " AND interval_no=" . $interval_no;
				$dbt->getData($sql);
			}

			if ( $interval_no > 0 )	{
				$next_interval_no = $interval_no;
			}
		}

	}

	$q->param('scale' => $scale_no);
	processViewTimeScale($dbt, $hbo, $q, $s, $exec_url, 'summary', \@badintervals);

	print main::stdIncludes("std_page_bottom");

	return;
}

1;
