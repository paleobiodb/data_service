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

sub searchScale {
	my ($dbt,$hbo,$s,$q) = @_;
	my $dbh = $dbt->dbh;
	my %vars = $q->Vars();

	my @where = ("s.scale_no=c.scale_no AND c.interval_no=il.interval_no");
	if ( $q->param('period_max') )	{
		my $sql = "SELECT base_age,top_age FROM intervals i,interval_lookup il where i.interval_no=il.interval_no AND (eml_interval IS NULL OR eml_interval='') AND interval_name='".$q->param('period_max')."'";
		my $i = ${$dbt->getData($sql)}[0];
		push @where , "il.base_age>=".$i->{'top_age'}." AND il.top_age<=".$i->{'base_age'};
	}
	if ( ! $q->param('continent') )	{
		$q->param('continent' => 'global');
		$vars{'continent'} = "global";
	}
	if ( $q->param('continent') =~ /Aust/i )	{
		push @where , "(continent='Australia' OR continent='New Zealand')";
	} elsif ( $q->param('continent') && $q->param('continent') !~ /all/i )	{
		push @where , "continent='".$q->param('continent')."'";
	}
	if ( $q->param('scale_rank') && $q->param('scale_rank') !~ /all/i )	{
		push @where , "scale_rank='".$q->param('scale_rank')."'";
	}

	# Retrieve each scale's name from the database
	my $sql = "SELECT s.authorizer_no,s.scale_no,scale_name,s.reference_no,continent,scale_rank FROM scales s,correlations c,interval_lookup il WHERE ".join(' AND ',@where);
	my @results = @{$dbt->getData($sql)};
	my (%scale_strings,%hasScales);

	foreach my $scaleref (@results)	{
		$hasScales{$scaleref->{'continent'}}++;
		my $option = "<div class=\"tiny\"><a href=\"$READ_URL?a=processViewScale&amp;scale_no=" . $scaleref->{scale_no} . "\" style=\"margin-left: 2em;\">";
		my $name = $scaleref->{scale_name};
		my $sql2 = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $scaleref->{reference_no};
		my @results2 = @{$dbt->getData($sql2)};
		my $auth = $results2[0]->{author1last};
		$auth =~ s/, (2nd\.|Jr\.)//;
		if ( $results2[0]->{otherauthors} || $results2[0]->{author2last} eq "et al." )	{
			$auth .= " et al.";
		} elsif ( $results2[0]->{author2last} )	{
			$auth .= " and " . $results2[0]->{author2last};
		}
		my $authyr = $auth . " " . $results2[0]->{pubyr};
		$name =~ s/$authyr//;
		$name =~ s/$auth//;
		$name =~ s/$results2[0]->{author1last}//;
		$authyr = " [" . $authyr . "]\n";
		$scale_strings{$scaleref->{continent}.$scaleref->{scale_rank}}{$authyr} = $option . $name. "</a>" . $authyr . "</div>\n";
	}
	if ( ! @results )	{
		$vars{'panels'} = "<div style=\"margin-bottom: 2em;\"><i>No time scales met your search criteria.<br>Please try again.</i></div>\n\n";
	}

	# WARNING: if African or Antarctic scales are ever entered, they need
	#  to be added to this list
	for my $c ( 'global','Asia','Australia','Europe','New Zealand','North America','South America' )	{
		if ( $q->param('continent') !~ /all/i )	{
			if ( $q->param('continent') && $q->param('continent') !~ /$c/i )	{
				next;
			} elsif ( ! $hasScales{$c} )	{
				next;
			}
		}
		my $continent = $c;
		$continent =~ s/global/Global/;
		$vars{'panels'} .= qq|<div class="displayPanel" align="left" style="padding-left: 2em;">\n|;
		$vars{'panels'} .= qq|<span class="displayPanelHeader">$continent</span>\n|;
		$vars{'panels'} .= qq|<div class="displayPanelContent">\n|;
		for my $r ( 'eon/eonothem','era/erathem','period/system','subperiod/system','epoch/series','subepoch/series','age/stage','subage/stage','chron/zone' )	{
			my @sorted = sort keys %{$scale_strings{$c.$r}};
			if ( $#sorted > -1 )	{
				$vars{'panels'} .= "<div style=\"margin-bottom: 0.5em;\">\n";
				$vars{'panels'} .= "<div class=\"tiny\" style=\"padding-bottom: 2px;\">$r</div>\n";
				for my $string ( @sorted )	{
					$vars{'panels'} .= $scale_strings{$c.$r}{$string};
				}
				$vars{'panels'} .= "</div>\n\n";
			}
		}
		$vars{'panels'} .= "</div>\n</div>\n\n";
	}

	if ( $s->get('enterer') eq "J. Alroy" || $s->get('enterer') eq "S. Holland" )	{

		$vars{'add_form'} = qq|
		<form name="scale_add_form" action="$WRITE_URL" method="POST">
		<input id="action" type="hidden" name="action" value="processShowForm">
		<input type="hidden" name="scale" value="add">
		<input type="submit" value="Add scale">
		</form>
|;
	}
	print $hbo->populateHTML('scale_search',\%vars);

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

	print "<tr><td align=\"center\" colspan=2><font color=\"red\">Interval</a> </td><td align=\"center\" colspan=2>Maximum correlate </td><td align=\"center\" colspan=2>Minimum correlate </td><td align=\"center\" valign=\"bottom\">Lower<br>boundary </td></tr>\n";

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
		my $base_age;
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

			$base_age = $times[$i]->{base_age};
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

		print $hbo->populateHTML('enter_scale_row', [$eml_interval, $interval, $eml_max_interval, $max_interval, $eml_min_interval, $min_interval, $base_age, $corr_comments], ['eml_interval', 'interval', 'eml_max_interval', 'max_interval', 'eml_min_interval', 'min_interval', 'base_age', 'corr_comments']);
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


	print "<p align=\"center\" class=\"pageTitle\">",$row->{scale_name},"</p>\n\n";

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
		if ( $time->{base_age} > 0 )	{
			$nlower++;
		}
	}

	print "<tr>";
	print "<td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">&nbsp;Interval</td>\n";
	print "<td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nmax > 0 )	{
		print "Maximum&nbsp;correlate ";
	}
	print "</td><td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nmin > 0 )	{
		print "&nbsp;&nbsp;Minimum&nbsp;correlate ";
	}
	print "</td><td align=\"center\" valign=\"bottom\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">";
	if ( $nlower > 0 )	{
		print "Ma ";
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

		my $base_age = $time->{base_age};
		$base_age =~ s/000$//;
		$base_age =~ s/00$//;
		$base_age =~ s/0$//;
		$base_age =~ s/\.$//;
		if ( $base_age == 0 )	{
			$base_age = "";
		}

		print $hbo->populateHTML('view_scale_row', [ $interval, $max_interval, $min_interval, $base_age, $time->{corr_comments}], ['interval', 'max_interval', 'min_interval', 'base_age', 'corr_comments']);
	}

	if ( $nlower > 0 )	{
		print "<tr><td colspan=\"3\" style=\"height: 0.5em; border-top: 1px gray solid;\"></td></tr>\n";
	}

	print "</table>\n</table>\n</div></div>\n<p>\n";

    $row->{'reference'} = Reference::formatShortRef($dbt,$row->{'reference_no'},'link_id'=>1);
	print $hbo->populateHTML('view_scale_top', $row);

	if ( $stage eq "summary" )	{
		print "<div align=\"center\"><p><a href=\"$WRITE_URL?action=processShowForm&scale=" , $q->param('scale') , "\">Edit this time scale</a> - ";
		print "<a href=\"$WRITE_URL?action=processShowForm\">Create a new time scale</a> - ";
		print "<a href=\"$WRITE_URL?action=searchScale\">Edit another time scale</a></p></div>\n\n";
	} else	{
		print "<div align=\"center\"><p><a href=\"$WRITE_URL?action=searchScale\">View another time scale</a></p></div>\n\n";
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

	my @lower_boundaries = $q->param('base_age');
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
			$sql .= "',base_age='" . $lower_boundaries[$i];
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
				$sql = "UPDATE intervals SET modified=modified,created='";
				$sql .= $modifieds[0]->{modified} . "' WHERE interval_no=" . $interval_no;
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
					if ( ! $fieldused{$minmax} )	{
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
				$sql = "INSERT INTO correlations (authorizer_no, enterer_no, reference_no, scale_no, interval_no, next_interval_no, max_interval_no, min_interval_no, base_age, corr_comments) VALUES (";
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
				$sql = "UPDATE correlations SET modified=modified,created='";
				$sql .= $modifieds[0]->{modified} . "' WHERE scale_no=" . $scale_no . " AND interval_no=" . $interval_no;
				$dbh_r->do($sql);
			}

			if ( $interval_no > 0 )	{
				$next_interval_no = $interval_no;
			}
		}

	}
	TimeLookup::buildLookupTable($dbt);

	$q->param('scale' => $scale_no);
	processViewTimeScale($dbt, $hbo, $q, $s, 'summary', \@badintervals);

}


# JA 9.8.04
sub displayTenMyBins	{
	my ($dbt,$q,$s,$hbo) = @_;
	my $t = new TimeLookup($dbt);

	print "<center><p class=\"pageTitle\">10 m.y.-long sampling bins</p></center>\n\n";

	print "These bin definitions are used by the <a href=\"$READ_URL?action=displayCurveForm\">diversity curve generator</a>.\n\n";

	my @binnames = $t->getBins;
	my ($upperbin,$lowerbin) = $t->computeBinBounds('bins');


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
	print "<td valign=top>Bin name</td>  <td valign=top>Age&nbsp;at&nbsp;base&nbsp;(Ma)</td>  <td valign=top>Included intervals</td></tr>\n";

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

# old Schroeter function almost completely rewritten by JA 23-24.10.11
sub displayInterval	{
	my ($dbt,$hbo,$q) = @_;
	my $i = int($q->param('interval_no'));
	return unless $i;

	my $sql = "SELECT i.*,il.* FROM intervals i,interval_lookup il WHERE i.interval_no=il.interval_no AND i.interval_no=".$i;
	my $itv = ${$dbt->getData($sql)}[0];

	my ($base_source,$top_source);
	if ( $itv->{base_age_source} )	{
		$sql = "SELECT s.scale_no,TRIM(CONCAT(eml_interval,' ',interval_name)) AS name FROM scales s,intervals i,correlations c WHERE s.scale_no=c.scale_no AND i.interval_no=c.interval_no AND c.correlation_no=".$itv->{base_age_source};
		$base_source = ${$dbt->getData($sql)}[0];
	}
	if ( $itv->{top_age_source} )	{
		# by default, say that the top age comes from some base age
		$sql = "SELECT s.scale_no,TRIM(CONCAT(eml_interval,' ',interval_name)) AS name,c.interval_no AS correlated_no FROM scales s,intervals i,correlations c WHERE s.scale_no=c.scale_no AND i.interval_no=c.interval_no AND c.correlation_no=".$itv->{top_age_source};
		$top_source = ${$dbt->getData($sql)}[0];
		# because this is confusing, try to get the name of the
		#  interval preceding the source in the same scale (there
		#  won't be one if the scale starts with the source)
		$sql = "SELECT TRIM(CONCAT(eml_interval,' ',interval_name)) AS name FROM intervals i,correlations c WHERE i.interval_no=c.interval_no AND c.scale_no=".$top_source->{scale_no}." AND c.next_interval_no=".$top_source->{correlated_no};
		my $better_source = ${$dbt->getData($sql)}[0];
		if ( $better_source )	{
			$top_source->{name} = $better_source->{name};
		}
	}

	# just get it over with, the table is tiny
	$sql = "SELECT interval_no,TRIM(CONCAT(eml_interval,' ',interval_name)) AS name FROM intervals";
	my %name;
	$name{$_->{interval_no}} = $_->{name} foreach @{$dbt->getData($sql)};

	# find all overlapping intervals
	$sql = "SELECT i.interval_no,base_age,base_age_relation,top_age,top_age_relation FROM intervals i,interval_lookup il WHERE i.interval_no=il.interval_no AND base_age>=".$itv->{top_age}." AND top_age<=".$itv->{base_age}." AND i.interval_no!=$i";
	my @relations = @{$dbt->getData($sql)};
	my (@equals,@within,@contains,@shares_base,@shares_top,@overlaps,@next,@previous);
	for my $r ( @relations )	{
		if ( $r->{base_age} == $itv->{base_age} && $r->{top_age} == $itv->{top_age} )	{
			if ( $r->{base_age_relation} eq "equal to" && $r->{top_age_relation} eq "equal to" && $itv->{base_age_relation} eq "equal to" && $itv->{top_age_relation} eq "equal to" )	{
				push @equals , $r;
			} elsif ( $r->{base_age_relation} eq "equal to" && $r->{top_age_relation} eq "equal to" && ( $itv->{base_age_relation} eq "after" || $itv->{top_age_relation} eq "before" ) )	{
				push @within , $r;
			} elsif ( ( $r->{base_age_relation} eq "after" || $r->{top_age_relation} eq "before" ) && $itv->{base_age_relation} eq "equal to" && $itv->{top_age_relation} eq "equal to" )	{
				push @contains , $r;
			} else	{
				push @overlaps , $r;
			}
		} elsif ( $r->{base_age} > $itv->{base_age} && $r->{top_age} < $itv->{top_age} )	{
			push @within , $r;
		} elsif ( $r->{base_age} < $itv->{base_age} && $r->{top_age} > $itv->{top_age} )	{
			push @contains , $r;
		} elsif ( $r->{base_age} == $itv->{base_age} && $r->{top_age} != $itv->{top_age} )	{
			push @shares_base , $r;
		} elsif ( $r->{base_age} != $itv->{base_age} && $r->{top_age} == $itv->{top_age} )	{
			push @shares_top , $r;
		} elsif ( $r->{base_age} == $itv->{top_age} )	{
			push @next ,  $r;
		} elsif ( $r->{top_age} == $itv->{base_age} )	{
			push @previous ,  $r;
		} elsif ( $r->{base_age} != $itv->{top_age} && $r->{top_age} != $itv->{base_age} )	{
			push @overlaps , $r;
		}
	}

	$sql = "SELECT scale_name,s.scale_no,scale_rank,continent,pubyr,next_interval_no FROM scales s,correlations c,refs r WHERE s.scale_no=c.scale_no AND interval_no=$i AND c.reference_no=r.reference_no ORDER BY pubyr DESC";
	my @scales = @{$dbt->getData($sql)};
	my $best_scale = $scales[0];

	my $type = "";
	if ( $best_scale )	{
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

	@within = sort { $a->{base_age} <=> $b->{base_age} || $b->{top_age} <=> $a->{top_age} } @within;
	@contains = sort { $name{$a->{interval_no}} cmp $name{$b->{interval_no}} || $a->{interval_no} <=> $b->{interval_no} } @contains;
	@shares_base = sort { $name{$a->{interval_no}} cmp $name{$b->{interval_no}} || $a->{interval_no} <=> $b->{interval_no} } @shares_base;
	@shares_top = sort { $name{$a->{interval_no}} cmp $name{$b->{interval_no}} || $a->{interval_no} <=> $b->{interval_no} } @shares_top;
	@overlaps = sort { $a->{base_age} <=> $b->{base_age} || $a->{top_age} <=> $b->{top_age} } @overlaps;

	print "<div align=\"center\"><p class=\"pageTitle\">$name{$itv->{interval_no}} $type</p></div>";

	print "<table width=\"100%\" cellspacing=\"5\" cellpadding=\"0\" border=\"0\"><tr><td valign=top width=\"50%\">";
	my $general_html = "";
	if ($itv->{'base_age'} =~ /\d/)	{
		my $lower = TimeLookup::printBoundary($itv->{'base_age'});
		my $see = ( $name{$itv->{interval_no}} ne $base_source->{name} ) ? "based on ".$base_source->{name} : "see scale";
		$general_html .= "Lower boundary: $itv->{base_age_relation} $lower Ma (<a href=\"$READ_URL?a=processViewScale&scale_no=$base_source->{scale_no}\">$see</a>)<br>";
		my $upper = TimeLookup::printBoundary($itv->{'top_age'}); 
		$see = ( $name{$itv->{interval_no}} ne $top_source->{name} ) ? "based on ".$top_source->{name} : "see scale";
		$general_html .= "Upper boundary: $itv->{top_age_relation} $upper Ma (<a href=\"$READ_URL?a=processViewScale&scale_no=$top_source->{scale_no}\">$see</a>)<br>";

		if ($best_scale)	{
			$general_html .= "Continent: $best_scale->{continent}<br>";
		}
	}
	$general_html .= "<a href=# onClick=\"document.doColls.submit();\">See collections within this interval</a><br>";
	$general_html .= "<a href=# onClick=\"document.doMap.submit();\">See map of collections within this interval</a><br>";
	$general_html .= qq|<form method="POST" action="$READ_URL" name="doColls"><input type="hidden" name="action" value="displayCollResults"><input type="hidden" name="max_interval_no" value="$i"></form>|;
	$general_html .= qq|<form method="POST" action="$READ_URL" name="doMap"><input type="hidden" name="action" value="displaySimpleMap"><input type="hidden" name="max_interval_no" value="$i"></form>|;

	print $hbo->htmlBox("General information",$general_html);
 
	my $corr_html; 
	if ($itv->{'period_no'} > 0 && $itv->{'period_no'} != $i)	{
		$corr_html .= "Period: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{period_no}\">$name{$itv->{period_no}}</a><br>";
	}
	if ($itv->{'epoch_no'} > 0 && $itv->{'epoch_no'} != $i)	{
		$corr_html .= "Epoch: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{epoch_no}\">$name{$itv->{epoch_no}}</a><br>";
	}
	if ($itv->{'subepoch_no'} > 0 && $itv->{'subepoch_no'} != $i)	{
		$corr_html .= "Subepoch: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{subepoch_no}\">$name{$itv->{subepoch_no}}</a><br>";
	}
	if ($itv->{'stage_no'} > 0 && $itv->{'stage_no'} != $i)	{
		$corr_html .= "Stage: <a href=\"$READ_URL?action=displayInterval&interval_no=$itv->{stage_no}\">$name{$itv->{stage_no}}</a><br>";
	}

	if ($itv->{'ten_my_bin'})	{
		$corr_html .= "10 million year bin: $itv->{ten_my_bin}<br>";
	}
	if ($corr_html)	{
		print $hbo->htmlBox("Key correlations",$corr_html);
	}

	if (@within)	{
		my $html = "";
		foreach my $w (@within)	{
			$html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$w->{interval_no}\">$name{$w->{interval_no}}</a></li>";
		}
		print $hbo->htmlBox("Contained within",$html);
	}


 	if ( @previous )	{
		my $html = "";
		$html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$_->{interval_no}\">$name{$_->{interval_no}}</a></li>" foreach @previous;
		my $s = ($#previous > 0) ? "s" : "";
		print $hbo->htmlBox("Previous interval$s",$html);
	}
 	if ( @next )	{
		my $html = "";
		$html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$_->{interval_no}\">$name{$_->{interval_no}}</a></li>" foreach @next;
		my $s = ($#next > 0) ? "s" : "";
		print $hbo->htmlBox("Next interval$s",$html);
	}

	if (@equals)	{
		my $html = "";
		my @intervals = ();
		for my $e (@equals)	{
			push @intervals, "<a href=\"$READ_URL?action=displayInterval&interval_no=$e->{interval_no}\">$name{$e->{interval_no}}</a>";
		}
		$html .= "<li>".join(', ',@intervals)."</li>";
		print $hbo->htmlBox("Equivalent to",$html);
	}

	print "</td><td valign=top width=\"50%\">";

	if (@contains)	{
		my @list;
		for my $c (@contains)	{
			push @list , "<a href=\"$READ_URL?action=displayInterval&interval_no=$c->{interval_no}\">$name{$c->{interval_no}}</a>";
		}
		print $hbo->htmlBox("Contains",("<li>".join(', ',@list)."</li>"));
	}
	if (@shares_base)	{
		my @list;
		for my $c (@shares_base)	{
			push @list , "<a href=\"$READ_URL?action=displayInterval&interval_no=$c->{interval_no}\">$name{$c->{interval_no}}</a>";
		}
		print $hbo->htmlBox("Shares base with",("<li>".join(', ',@list)."</li>"));
	}

	if (@shares_top)	{
		my @list;
		for my $c (@shares_top)	{
			push @list , "<a href=\"$READ_URL?action=displayInterval&interval_no=$c->{interval_no}\">$name{$c->{interval_no}}</a>";
		}
		print $hbo->htmlBox("Shares top with",("<li>".join(', ',@list)."</li>"));
	}

	if (@overlaps)	{
		my $html = "";
		my $range = "";
		for my $o (@overlaps)	{
			my ($base,$top) = ($o->{base_age},$o->{top_age});
			$base =~ s/0+$//;
			$top =~ s/0+$//;
			$base =~ s/\.$/.0/;
			$top =~ s/\.$/.0/;
			$html .= "<li><a href=\"$READ_URL?action=displayInterval&interval_no=$o->{interval_no}\">$name{$o->{interval_no}}</a> ($base to $top Ma)</li>";
		}
		print $hbo->htmlBox("Overlaps with",$html);
	}

	my $html = "";
	for my $scale (@scales)	{
		$html .= "<li><a href=\"$READ_URL?action=processViewScale&scale_no=$scale->{scale_no}\">$scale->{scale_name}</a> ($scale->{pubyr})<br>";
	}
	my $s = (@scales  > 1) ? "s" : "";
	print $hbo->htmlBox("Appears in scale$s",$html);
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
        $eml = PBDBUtil::stripTags($eml);
        $name = PBDBUtil::stripTags($name);
        print "<div align=\"center\">";
        print "<p>Could not find $eml $name</p>";
        print "<p>(Please try again)</p>";
        print PBDBUtil::printIntervalsJava($dbt,1);
        print $hbo->populateHTML("search_intervals_form");
        print "</div>";
    } else {
        $q->param('interval_no'=>$row->{'interval_no'});
        displayInterval($dbt,$hbo,$q);
    }
}

1;
