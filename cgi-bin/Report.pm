# Report.pm
# written 10-12.3.99 by JA
# chronological data type report 24.3.99
# revised to March 1999 field numbering 29.3.99
# enterer field may be searched 18.6.99
# minor bug fix 6.7.99
# new genus field cleanup routine 7.8.99
# more efficient class searches 13.8.99
# multiple genera can be searched at once; bug fixes 15.8.99
# periods, epochs and stages printed in temporal order; data output
#   to report.csv 16.8.99
# better field cleaning 20.8.99
# structure update 21-23.8.99 MAK/JA
# weighted lithology search 22.11.99 JA
# U.S.A. bug fix 15.1.00 JA
# discards records created before a date based on DOC of record,
#   not collection 17.3.00 JA
# bug fix in date routine 5.4.00 JA
# uses UNIX dates 7.12.00 JA
# bug fix in monthid values 29.1.00 JA
# bug fix in path name for class files 10.3.01 JA
# output cleanup 11.3.01 JA
# checks lithification field 22.3.01 JA
# bug fix in stripping of quotes from second lithology field 26.3.01 JA
# maximum number of columns set by pull-down menu 11.7.01 JA
# counts data for individual research groups 13.3.02 JA
# counts data by continent 15-16.3.02 JA
package Report;

use Text::CSV_XS;
use PBDBUtil;

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;
my $dbt;
my $q;					# Reference to the parameters
my $s;

my $HOST_URL = $ENV{BRIDGE_HOST_URL};
$DDIR=$ENV{REPORT_DDIR};
$DDIR2=$ENV{REPORT_DDIR2};
                                        # the default working directory
$BACKGROUND="/public/PDbg.gif";

# I think these are for the collections table.
$LOCIDCOL=3;
$LITHIFCOL=56;
$LITH1COL=57;
$LITH2COL=59;
$RESGRPCOL=42;
# And these are the occurrences table.
$GENCOL=6;
$SPCOL=8;
$ABCOL=9;

my $authorizer;

sub new {
	my $class = shift;
	$dbh = shift;
	$q = shift;
	$s = shift;
	$dbt = shift;
	my $self = {};

	bless $self, $class;
	return $self;
}

sub buildReport {
	my $self = shift;

	$OUTPUT_FILE = $DDIR2;
	# customize the subdirectory holding the output files
	if ( $s->get('enterer') ne "Guest")     {
		$authorizer = $s->get("authorizer");
		$authorizer =~ s/ //g;
		$authorizer =~ s/\.//g;
		$authorizer =~ tr/[A-Z]/[a-z]/;
		$OUTPUT_FILE .= "/" . $authorizer;
		mkdir $OUTPUT_FILE;
		chmod 0777, $OUTPUT_FILE;
	}
	$OUTPUT_FILE .= "/report.csv";

	# compute the numeric equivalent of the date limit
	if ( $q->param('day') < 10)	{ $q->param('day' => "0".$q->param('day')); }

	%monthid = ( "January" => "01", "February" => "02", "March" => "03",
             	"April" => "04", "May" => "05", "June" => "06", "July" => "07",
             	"August" => "08", "September" => "09", "October" => 10,
             	"November" => 11, "December" => 12 );

	# old method, assuming non-UNIX date format
	$datelimit = $q->param('year').$monthid{$q->param('mon')}.$q->param('day');
	$self->dbg("datelimit: $datelimit<br>");

	$self->readRegions();
	$self->tallyFieldTerms();
}

sub readRegions	{
	my $self = shift;

	if ( ! open REGIONS,"<$DDIR/PBDB.regions" ) {
		$self->htmlError ( "$0:Couldn't open $DDIR/PBDB.regions<BR>$!" );
	}
	while (<REGIONS>)	{
		s/\n//;
		my ($temp,$temp2) = split /:/, $_, 2;
		@countries = split /\t/,$temp2;
		for $country (@countries)	{
			$region{$country} = $temp;
		}
	}
	close REGIONS;
}

sub tallyFieldTerms	{
	my $self = shift;

	# 'searchfield1' and 'searchfield2'
	for $suffix (1..2)	{
		if ($q->param('searchfield'.$suffix) eq "period")	{
			push @{ $fieldterms[$suffix] }, "", "Modern", "Quaternary",
					                      "Tertiary", "Cretaceous", "Jurassic",
					                      "Triassic", "Permian", "Carboniferous",
					                      "Devonian", "Silurian", "Ordovician",
					                      "Cambrian", "Neoproterozoic";
			$nterms[$suffix] = $#{$fieldterms[$suffix]};
		}
		elsif (($q->param('searchfield'.$suffix) eq "epoch") ||
					 ($q->param('searchfield'.$suffix) eq "international age/stage"))	{
			if ($q->param('searchfield'.$suffix) eq "epoch")	{
				$EorS = "E";
			}
			else	{
				$EorS = "S";
			}
			push @{ $fieldterms[$suffix] }, "";
			if ( ! open TIMESCALE,"<$DDIR2/harland.epochs" ) {
				$self->htmlError ( "$0:Couldn't open $DDIR2/harland.epochs<BR>$!" );
			}
			while (<TIMESCALE>)	{
				s/\n//;
				$_ = "STARTLINE".$_;
				s/STARTLINE //;
				s/STARTLINE//;
				($temp,$temp2) = split(/ /,$_,2);
				($temp2,$temp3) = split(/ = /,$temp2,2);
				if ($temp eq $EorS)	{
					push @{ $fieldterms[$suffix] }, $temp2;
					$nterms[$suffix]++;
				}
			}
			close TIMESCALE;
		}
		$self->dbg("fieldterms[$suffix]: @{$fieldterms[$suffix]}<br>");
		$self->dbg("nterms[$suffix]: $nterms[$suffix]<br>");
	}
	
	print "<html>\n";
	print "<head><title>Paleobiology Database tabular report";
	if ($q->param('taxon_name') )	{
		print ": ".$q->param('taxon_name');
	}
	print "</title></head>\n";
	print "<body bgcolor=\"white\" background=\"";
	print $BACKGROUND;
	print "\" text=black link=\"#0055FF\" vlink=\"#990099\">\n\n";
	
	print "<center>\n";
	print "<h2>Paleobiology Database tabular report";
	if ($q->param('taxon_name') )	{
		print ": ".$q->param('taxon_name');
	}
	print "</h2>\n\n";
	
	$csv = Text::CSV_XS->new();
	
	# flush all output immediately to stdout
	$| =1;

	# get a list of Sepkoski's genera, if needed JA 28.2.03
	if ( $q->param('Sepkoski') eq "Y" )	{
		$sql = "SELECT taxon_name FROM authorities WHERE authorizer_no=48 AND taxon_rank='genus'";
		my $sth = $dbh->prepare($sql) || die "Prepare query failed\n";
		$sth->execute() || die "Execute query failed\n";
		my @jackrefs = @{$sth->fetchall_arrayref()};
		$sth->finish();
		for $jackref (@jackrefs)  {
			$jacklist .= "','" . ${$jackref}[0];
		}
		$jacklist =~ s/^,'//;
	}
	
	# if output is means or records, count records at each collection
	# this section rewritten to do a bona fide database query by JA 2.8.02
	if ($q->param('output') ne "collections")	{
	 # discard records created before a given date if records are
	 #   being counted
		# query the database
		$sql = "SELECT occurrences.collection_no, occurrences.genus_name, ".
			   "occurrences.species_name, occurrences.created ".
			   "FROM occurrences ";
		if($q->param('taxon_name')){
		  # Get occs that haven't been reID'ed
		  $sql .= "LEFT JOIN reidentifications ON occurrences.occurrence_no = ".
				  "reidentifications.occurrence_no WHERE ".
				  "reidentifications.occurrence_no IS NULL AND ";
		}
		# restrict query to Compendium genera
		elsif ( $jacklist )	{
				$sql .= " WHERE occurrences.genus_name IN ('".$jacklist."')";
		}
		# neither taxon name nor Jack's list.
		else{
			$sql .= " WHERE ";
		}
		if($datelimit){
			$sql .= "occurrences.created >= $datelimit AND ";
		}
		elsif($q->param('taxon_name')){
			my $name = $q->param('taxon_name');
			$genus_names_string = `./recurse $name`;
			if($genus_names_string){
					$sql .= " occurrences.genus_name IN ($genus_names_string)";
			}
		}
		# Might have to filter out the 'else' append, above, if no datelimit.
		$sql =~ s/WHERE\s+$//;

		$self->dbg("non-collection sql: $sql<br>");
		my $sth = $dbh->prepare($sql) || die "Prepare query failed\n";
		$sth->execute() || die "Execute query failed\n";
		my @rowrefs = @{$sth->fetchall_arrayref()};
		$sth->finish();
		# get the reid'ed data:
		if($q->param('taxon_name')){
			$sql = "SELECT count(occurrence_no), occurrence_no, collection_no,".
				   " genus_name, species_name, created ".
				   "FROM reidentifications WHERE genus_name IN (".
					$genus_names_string.") GROUP BY occurrence_no";
			$self->dbg("reid sql: $sql<br>");
			my $sth = $dbh->prepare($sql) || die "Prepare query failed\n";
			$sth->execute() || die "Execute query failed\n";
			my @temp =  @{$sth->fetchall_arrayref()};
			foreach my $ref (@temp){
				if($ref->[0] > 1){
					# Put the most recent reID in this slot in the array
					my $hr = PBDBUtil::getMostRecentReIDforOcc($dbt,$ref->[1],1);
					my @record = ($hr->{collection_no}, $hr->{genus_name}, $hr->{species_name}, $hr->{created});
					$ref = \@record;
				}
			}
			push(@rowrefs, @temp);
			$sth->finish();
		}

		foreach my $rowref ( @rowrefs ){
			my ($collno,$reid_collno,$genus,$reid_genus,$species,$reid_species) = @{$rowref};
			$recs[$collno]++;
			$include[$collno]++;
			push @{$taxlist[$collno]} , $genus;
			if($genus && $reid_genus && ($reid_genus ne $genus)){
				push @{$taxlist[$collno]} , $reid_genus;
			}
		}
		$doesappear = scalar(@rowrefs);
	
		if ($doesappear == 0)	{
			print "The taxon \"<i>".$q->param('taxon_name')."</i>\"";
			print " does not appear anywhere in the database.<p>\n";
			exit(0);
		}
	}
	$self->dbg("numrecs: ".@recs."<br>");
	$totaltotal = 0;
	for(my $index = 0; $index < scalar @recs; $index++){
		$totaltotal += $recs[$index] if(defined $recs[$index] && $recs[$index] > 0);
	}
	$self->dbg("total count: $totaltotal<br>");

	# restrict counts to a particular genus or taxonomic class
#	if ( $q->param('taxon_name') )	{
		#$genus_names_string = PBDBUtil::taxonomic_search($q->param('taxon_name'),$dbt);
#		my $name = $q->param('taxon_name');
#		$genus_names_string = `./recurse $name`;
	# find the genus or class in the occurrence table
	# this section rewritten to do a bona fide database query by JA 2.8.02
	# query the database for the necessary fields
	# WARNING: list of Compendium genera isn't referenced because
	#  class list is supposed to come from the Compendium; needs to
	#  be rewritten once /classdata reads are replaced with a proper
	#  authorities table query JA 28.2.03
#		$gsql = "SELECT collection_no,occurrence_no,genus_name ".
#			    "FROM occurrences WHERE genus_name IN ($genus_names_string)";
#		if($datelimit){
#			$gsql .= " AND created >= $datelimit";
#		}
#		$self->dbg("genus_name sql: $gsql<br>");
#		my $sth = $dbh->prepare($gsql) || die "Prepare query failed\n";
#		$sth->execute() || die "Execute query failed\n";
#		my @rowrefs = @{$sth->fetchall_arrayref()};

		# nuke the lists of genera because they'll need to be
		#   recomputed
#		@taxlist = ();

#		foreach my $rowref ( @rowrefs )	{
#			my ($collno,$occno,$genus) = @{$rowref};
#			$include[$collno]++;
#			push @{$taxlist[$collno]} , $genus;
#		}
#		$sth->finish();
#		$doesappear = scalar(@rowrefs);
	
#		if ($doesappear == 0)	{
#			print "The taxon \"<i>".$q->param('taxon_name')."</i>\"";
#			print " does not appear anywhere in the database.<p>\n";
#			exit(0);
#		}
#	}
	$self->dbg("include: @include<br>doesappear: $doesappear<br>");
	
	if ($q->param('searchfield2') )	{
		$nsearchfields = 2;
	} else	{
		$nsearchfields = 1;
	}

	## NEW (Database)
	my %fieldnames = ("authorizer" => "authorizer", "enterer" => "enterer",
				"research group" => "research_group",
				"country" => "country", "continent" => "country", "state" => "state",
				"period" => "period_max", "epoch" => "epoch_max",
				"international age/stage" => "intage_max", "local age/stage" => "locage_max",
				"chronological data type" => "stratcomments", "formation" => "formation",
				"paleoenvironment" => "environment",
				"scale of geographic resolution" => "geogscale",
				"scale of stratigraphic resolution" => "stratscale",
				"tectonic setting" => "tectonic_setting",
				"preservation mode" => "pres_mode",
				"assemblage components" => "assembl_comps",
				"publication type" => "collection_type",
				"list coverage" => "collection_coverage");

	# section condensed by JA 3.8.02	
	# WARNING: if "chronological data type " is selected, script
	#  wipes out the stratigraphic comments field on the assumption it
	#   won't be needed
	for ($i = 1; $i <= $nsearchfields; $i++)	{
		if ( $fieldnames{$q->param('searchfield'.$i)} ne "")	{
			# Global array @fieldno created here...
			$field[$i] = $fieldnames{$q->param('searchfield'.$i)};
		}
		if ($q->param('searchfield'.$i) eq "international age/stage")	{
			$field2[$i] = 36;
		}
		elsif ($q->param('searchfield'.$i) eq "local age/stage")	{
			$field2[$i] = 40;
		}
		elsif ($q->param('searchfield'.$i) eq "lithification")	{
			$field[$i] = $LITHIFCOL;
		}
		elsif ($q->param('searchfield'.$i) eq "lithology - all combinations")	{
			$field[$i] = $LITH1COL;
		}
		elsif ($q->param('searchfield'.$i) eq "lithology - weighted")	{
			$field[$i] = $LITH1COL;
			$field2[$i] = $LITH2COL;
		}
	}
	
	if ($q->param('maxrows') =~ /the .{2} most frequent/)	{
		($foo,$maxterms,$foo2) = split / /,$q->param('maxrows');
	}
	else	{
		$maxterms = 200;
	}
	
	# GO get data from collections table in mysql
	$csql = "SELECT * FROM collections";

    my $resgrp = $q->param('research_group');
	my $coll_where = 0;

	## Date conditional
	if($datelimit){
		$csql .= " WHERE created >= $datelimit";
		$coll_where = 1;
	}

	## Research group conditional
    if($resgrp && $resgrp =~ /(^ETE$)|(^5%$)|(^PACED$)|(^PGAP$)/){
        require PBDBUtil;
        my $resprojstr = PBDBUtil::getResearchProjectRefsStr($dbh,$q);
        if($resprojstr ne ""){
			if($coll_where){
				$csql .= " AND collections.reference_no IN (" . $resprojstr . ")";
			}
			else{
				$csql .= " WHERE collections.reference_no IN (" . $resprojstr . ")";
			}
        }
    }
    elsif($resgrp){
		if($coll_where){
			$csql .= " AND FIND_IN_SET( '$resgrp', collections.research_group )";
		}
		else{
			$csql .= " WHERE FIND_IN_SET( '$resgrp', collections.research_group )";
		}
    }
	my @nums = ();
	for(my $index=0; $index<@recs; $index++){
		if($recs[$index]){
			push @nums, "'$index'";
		}
	}
	if(@nums){
		if($coll_where){
			$csql .= " AND collection_no IN (".join(",",@nums).")";
		}
		else{
			$csql .= " WHERE collection_no IN (".join(",",@nums).")";
		}
	}

	$self->dbg("coll_sql: $csql<br>");

	@all_coll_rows = @{$dbt->getData($csql)};

	$self->dbg("number of coll results: ".@all_coll_rows."<br>");

	foreach my $rowref ( @all_coll_rows ) {
		%collrow = %{$rowref};

			if ( $collrow{'collection_no'} > 0 &&
				( $include[$collrow{'collection_no'}] > 0 ||
				  $q->param('taxon_name') eq "" ) )	{
				$lines++;
				
				if ($include[$collrow{'collection_no'}] > 0)	{
					$recs[$collrow{'collection_no'}] = $include[$collrow{'collection_no'}];
				}
				$rectotal = $rectotal + $recs[$collrow{'collection_no'}];
				# record that the genera in this collection
				#  have been included in the tallies JA 28.2.03
				my @temp = @{$taxlist[$collrow{'collection_no'}]};
				for my $t ( @temp )	{
					$genuscounted{$t}++;
				}
				if (($collrow{'country'} eq "") || ($collrow{'country'} eq "U.S.A."))	{
					$collrow{'country'} = "USA";
				}
	 # concatenate two lithology fields if interbedded/mixed with is non-null
				if (($collrow{'lithology2'} ) && ($field2[1] == 0) &&
					  ($field2[2] == 0))	{
					$collrow{'lithology1'} = $collrow{'lithology1'}." + ".$collrow{'lithology2'};
				}
				
	 # determine the identity of the BEST type of chronological data
	 # available for this collection: ranking is international age/stage,
	 # local age/stage, epoch, and period (latter assumed to be known)
			 if (($q->param('searchfield1') eq "chronological data type") || ($q->param('searchfield2') eq "chronological data type"))	{

				if ( $collrow{'intage_max'} || $collrow{'intage_min'} )	{
					$collrow{'stratcomments'} = "International age/stage";
				}
				elsif ( $collrow{'locage_max'}  || $collrow{'locage_min'} )	{
					$collrow{'stratcomments'} = "Local age/stage";
				}
				elsif ( $collrow{'epoch_max'} )	{
					$collrow{'stratcomments'} = "Epoch";
				}
				else	{
					$collrow{'stratcomments'} = "Period";
				}
			}
			for ($r = 1; $r <= $nsearchfields; $r++)	{
	 # remove quotes, question marks, and extra spaces from field
			 $collrow{$field[$r]} =~ s/"//g;
			 $collrow{$field[$r]} =~ s/ \?//g;
			 $collrow{$field[$r]} =~ s/\? //g;
			 $collrow{$field[$r]} =~ s/\?//g;
			 $collrow{$field[$r]} =~ s/  / /g;
			 $collrow{$field[$r]} =~ s/^ //g;
			 $collrow{$field[$r]} =~ s/ $//g;
	
			 if ($field2[$r] > 0)	{
				 $collrow{$field2[$r]} =~ s/"//g;
				 $collrow{$field2[$r]} =~ s/ \?//g;
				 $collrow{$field2[$r]} =~ s/\? //g;
				 $collrow{$field2[$r]} =~ s/\?//g;
				 $collrow{$field2[$r]} =~ s/  / /g;
				 $collrow{$field2[$r]} =~ s/^ //g;
				 $collrow{$field2[$r]} =~ s/ $//g;
			 }
	
			 if ($q->param('searchfield'.$r) eq "continent")	{
if ($region{$collrow{$field[$r]}} eq "") { print "$collrow{$field[$r]}<br>\n"; }
					 $collrow{$field[$r]} = $region{$collrow{$field[$r]}};
			 }
	
			 $termid[$r] = 0;
			 $term2id[$r] = 0;
			 for ($i=1;$i<=$nterms[$r];$i++)	{
				if ($collrow{$field[$r]} eq $fieldterms[$r][$i])	{
					$termid[$r] = $i;
					if (($field2[$r] > 0) && ($collrow{$field[$r]} ) &&
					    ($collrow{$field2[$r]} ))	{
					  $timesused[$r][$i] = $timesused[$r][$i] + 0.5;
					  $recsused[$r][$i] = $recsused[$r][$i] + ($recs[$collrow{'collection_no'}]/2);
					}
					elsif (($collrow{$field[$r]} ) ||
					       ($collrow{$field2[$r]} eq "") ||
					       ($field2[$r] eq ""))	{
					  $timesused[$r][$i]++;
					  $recsused[$r][$i] = $recsused[$r][$i] + $recs[$collrow{'collection_no'}];
					}
					$i = $nterms[$r] + 2;
				}
			 }
			 if (($i == $nterms[$r] + 1) || ($i == 1))	{
				$nterms[$r]++;
				$termid[$r] = $nterms[$r];
				$fieldterms[$r][$nterms[$r]] = $collrow{$field[$r]};
				if (($field2[$r] > 0) && ($collrow{$field[$r]} ) &&
					 ($collrow{$field2[$r]} ))	{
					$timesused[$r][$nterms[$r]] = 0.5;
					$recsused[$r][$nterms[$r]] = $recs[$collrow{'collection_no'}]/2;
				}
				elsif (($collrow{$field[$r]} ) ||
					     ($collrow{$field2[$r]} eq "") ||
					     ($field2[$r] eq ""))	{
					$timesused[$r][$nterms[$r]] = 1;
					$recsused[$r][$nterms[$r]] = $recs[$collrow{'collection_no'}];
				}
			 }
			 if (($field2[$r] > 0) && ($collrow{$field2[$r]} ))	{
				for ($i=1;$i<=$nterms[$r];$i++) {
					if ($collrow{$field2[$r]} eq $fieldterms[$r][$i])	{
					  $term2id[$r] = $i;
					  if ($collrow{$field[$r]} )	{
					    $timesused[$r][$i] = $timesused[$r][$i] + 0.5;
					    $recsused[$r][$i] = $recsused[$r][$i] + ($recs[$collrow{'collection_no'}]/2);
					  }
					  else	{
					    $timesused[$r][$i]++;
					    $recsused[$r][$i] = $recsused[$r][$i] + $recs[$collrow{'collection_no'}];
					  }
					  $i = $nterms[$r] + 2;
					}
				}
				if (($i == $nterms[$r] + 1) || ($i == 1))	{
					$nterms[$r]++;
					$term2id[$r] = $nterms[$r];
					$fieldterms[$r][$nterms[$r]] = $collrow{$field2[$r]};
					if ($collrow{$field[$r]} )	{
					  $timesused[$r][$nterms[$r]] = 0.5;
					  $recsused[$r][$nterms[$r]] = $recs[$collrow{'collection_no'}]/2;
					}
					else	{
					  $timesused[$r][$nterms[$r]] = 1;
					  $recsused[$r][$nterms[$r]] = $recs[$collrow{'collection_no'}];
					}
				}
			 }
			}
			if ($nsearchfields > 1)	{
				if (($term2id[1] == 0) && ($term2id[2] == 0))	{
					$co[$termid[1]][$termid[2]]++;
					$corec[$termid[1]][$termid[2]] = $corec[$termid[1]][$termid[2]] + $recs[$collrow{'collection_no'}];
				}
				elsif (($term2id[1] == 0) && ($term2id[2] > 0))	{
					$co[$termid[1]][$termid[2]] = $co[$termid[1]][$termid[2]] + 0.5;
					$co[$termid[1]][$term2id[2]] = $co[$termid[1]][$term2id[2]] + 0.5;
					$corec[$termid[1]][$termid[2]] = $corec[$termid[1]][$termid[2]] + ($recs[$collrow{'collection_no'}])/2;
					$corec[$termid[1]][$term2id[2]] = $corec[$termid[1]][$term2id[2]] + ($recs[$collrow{'collection_no'}])/2;
				}
				elsif (($term2id[1] > 0) && ($term2id[2] == 0))	{
					$co[$termid[1]][$termid[2]] = $co[$termid[1]][$termid[2]] + 0.5;
					$co[$term2id[1]][$termid[2]] = $co[$term2id[1]][$termid[2]] + 0.5;
					$corec[$termid[1]][$termid[2]] = $corec[$termid[1]][$termid[2]] + ($recs[$collrow{'collection_no'}])/2;
					$corec[$term2id[1]][$termid[2]] = $corec[$term2id[1]][$termid[2]] + ($recs[$collrow{'collection_no'}])/2;
				}
				else	{
					$co[$termid[1]][$termid[2]] = $co[$termid[1]][$termid[2]] + 0.25;
					$co[$term2id[1]][$termid[2]] = $co[$term2id[1]][$termid[2]] + 0.25;
					$co[$termid[1]][$term2id[2]] = $co[$termid[1]][$term2id[2]] + 0.25;
					$co[$term2id[1]][$term2id[2]] = $co[$term2id[1]][$term2id[2]] + 0.25;
	
					$corec[$termid[1]][$termid[2]] = $corec[$termid[1]][$termid[2]] + ($recs[$collrow{'collection_no'}])/4;
					$corec[$term2id[1]][$termid[2]] = $corec[$term2id[1]][$termid[2]] + ($recs[$collrow{'collection_no'}])/4;
					$corec[$termid[1]][$term2id[2]] = $corec[$termid[1]][$term2id[2]] + ($recs[$collrow{'collection_no'}])/4;
					$corec[$term2id[1]][$term2id[2]] = $corec[$term2id[1]][$term2id[2]] + ($recs[$collrow{'collection_no'}])/4;
				}
			}
			}
	}
	
	# bubble float the terms
	# section condensed and rewritten to use standard Perl sort function
	#  by JA 3.8.02
	for $r (1..$nsearchfields)	{
		for $i (0..$nterms[$r])	{
			$id[$r][$i]=$i;
		}
		if (($q->param('searchfield'.$r) ne "period") &&
				($q->param('searchfield'.$r) ne "epoch") &&
				($q->param('searchfield'.$r) ne "international age/stage"))	{
			my @rectemp;
			if ($q->param('output') eq "collections")	{
				@rectemp = @{$timesused[$r]};
			} elsif ($q->param('output') eq "occurrences")	{
				@rectemp = @{$recsused[$r]};
			} elsif ($q->param('output') eq "mean occurrences")	{
				@rectemp = @{$recsused[$r]};
				my @rectemp2 = @{$timesused[$r]};
				for $i (0..$nterms[$r])	{
					if ( $rectemp2[$i] > 0 )	{
						$rectemp[$i] = $rectemp[$i] / $rectemp2[$i];
					} else	{
						$rectemp[$i] = 0;
					}
				}
			}

			@{$id[$r]} = sort { $rectemp[$b] <=> $rectemp[$a] } @{$id[$r]};

			my @temp = @{$timesused[$r]};
			for my $i (0..$nterms[$r])	{
				$timesused[$r][$i] = $temp[$id[$r][$i]];
			}
			my @temp = @{$recsused[$r]};
			for my $i (0..$nterms[$r])	{
				$recsused[$r][$i] = $temp[$id[$r][$i]];
			}
			my @temp = @{$fieldterms[$r]};
			for my $i (0..$nterms[$r])	{
				$fieldterms[$r][$i] = $temp[$id[$r][$i]];
			}
		}
	}
	
	if ( ! open OUTFILE,">$OUTPUT_FILE" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_FILE<BR>$!" );
	}
	
	if ($q->param('searchfield1') =~ /lithology/)	{
		$q->param('searchfield1' => "lithology");
	}
	if ($q->param('searchfield2') =~ /lithology/)	{
		$q->param('searchfield2' => "lithology");
	}
	
	if ($nsearchfields == 1)	{
        print "<table><tr><td>\n";
		if ($maxterms == 9999)	{
			$maxterms = $nterms[1];
			print "Here are all of the terms found in the ".$q->param('searchfield1')." field<p>\n\n"
		}
		else	{
			print "Here are the most frequent terms found in the <b>".$q->param('searchfield1')."</b> field<p>\n\n";
			if ($maxterms > $nterms[1])	{
				$maxterms = $nterms[1];
			}
		}
        print "</td></tr></table>\n";
	
		print "<table><p>\n";
		if ($q->param('output') eq "collections") {
			print "<tr><td valign=bottom><u>Collections</u><td valign=bottom><u>Percent</u><td valign=bottom><u>Term</u>\n";
			print OUTFILE "collections,percent,term\n";
		} elsif ( $q->param('output') eq "occurrences" ) {
			print "<tr><td valign=bottom><u>Occurrences</u><td valign=bottom><u>Percent</u><td valign=bottom><u>Term</u>\n";
			print OUTFILE "occurrences,percent,term\n";
		} elsif ( $q->param('output') eq "mean occurrences" ) {
			print "<tr><td valign=bottom><u>Mean occurrences</u><td valign=bottom><u>Percent</u><td valign=bottom><u>Term</u>\n";
			print OUTFILE "mean occurrences,percent,term\n";
		}
		for $i (0..$maxterms)	{
			if (($recsused[1][$i] > 0) || ($timesused[1][$i] > 0))	{
				if ($q->param('output') eq "collections")	{
					$xx = $timesused[1][$i];
					$yy = int($timesused[1][$i]/$lines*1000);
				}
				elsif ($q->param('output') eq "occurrences")	{
					$xx = $recsused[1][$i];
					$yy = int($recsused[1][$i]/$rectotal*1000);
				}
				elsif ($q->param('output') eq "mean occurrences")	{
					$xx = int($recsused[1][$i]/$timesused[1][$i]*10)/10;
					$yy = int($recsused[1][$i]/$rectotal*1000);
				}
				$yy = $yy / 10;
				if ($fieldterms[1][$i] eq "")	{
					$fieldterms[1][$i] = "<i>(no term entered)</i>";
				}
				printf "<tr><td align=center>%d<td align=center>%.1f<td>%s\n",$xx,$yy,$fieldterms[1][$i];
				print OUTFILE "$xx,$yy,\"$fieldterms[1][$i]\"\n";
			}
		}
		print "</table><p>\n";
	}
	else	{
		$maxterms1 = $nterms[1];
		($foo,$maxterms2,$foo2) = split / /,$q->param('maxcols');
		if ($nterms[2] < $maxterms2 || $maxterms2 == 0)	{
			$maxterms2 = $nterms[2];
		}
		print "<table border=2><p>\n";
		print "<tr><td>";
		print OUTFILE $q->param('searchfield1').",";
		for ($i = 0; $i <= $maxterms2; $i++)	{
			if (($timesused[2][$i] > 0) || ($recsused[2][$i]))	{
				if ($fieldterms[2][$i] )	{
					print "<td>$fieldterms[2][$i] ";
					print OUTFILE "\"$fieldterms[2][$i]\"";
				}
				else	{
					print "<td><i>(no term entered)</i> ";
					print OUTFILE "(no term entered)";
				}
				print OUTFILE ",";
			}
		}
		print "<td>TOTALS\n";
		print OUTFILE "TOTALS\n";
		for ($i = 0; $i <= $maxterms1; $i++)	{
			if (($timesused[1][$i] > 0) || ($recsused[1][$i] > 0))	{
				if ($fieldterms[1][$i] )	{
					print "<tr><td>$fieldterms[1][$i] ";
					print OUTFILE "\"$fieldterms[1][$i]\",";
				}
				else	{
					print "<tr><td><i>(no term entered)</i> ";
					print OUTFILE "(no term entered),";
				}
				for ($j = 0; $j <= $maxterms2; $j++)	{
					if (($timesused[2][$j] > 0) || ($recsused[2][$j]))	{
					  if ($co[$id[1][$i]][$id[2][$j]] > 0)	{
					    if ($q->param('output') eq "collections")	{
					      print "<td align=center>$co[$id[1][$i]][$id[2][$j]] ";
					      print OUTFILE "$co[$id[1][$i]][$id[2][$j]],";
					    }
					    elsif ($q->param('output') eq "occurrences")	{
					      print "<td align=center>$corec[$id[1][$i]][$id[2][$j]] ";
					      print OUTFILE "$corec[$id[1][$i]][$id[2][$j]],";
					    }
					    elsif ($q->param('output') eq "mean occurrences")	{
					      print "<td align=center>",int($corec[$id[1][$i]][$id[2][$j]]/$co[$id[1][$i]][$id[2][$j]]*10)/10," ";
					      print OUTFILE int($corec[$id[1][$i]][$id[2][$j]]/$co[$id[1][$i]][$id[2][$j]]*10)/10,",";
					    }
					  }
					  else	{
					    print "<td align=center>- ";
					    print OUTFILE "-,";
					  }
					}
				}
		# print row totals
				if ($q->param('output') eq "collections")	{
					print "<td align=center>$timesused[1][$i]";
					print OUTFILE $timesused[1][$i];
				}
				elsif ($q->param('output') eq "occurrences")	{
					print "<td align=center>$recsused[1][$i]";
					print OUTFILE $recsused[1][$i];
				}
				elsif ($q->param('output') eq "mean occurrences")	{
					print "<td align=center>",int($recsused[1][$i]/$timesused[1][$i]*10)/10;
					print OUTFILE int($recsused[1][$i]/$timesused[1][$i]*10)/10;
				}
				print "\n";
				print OUTFILE "\n";
			}
		}
	
	 # print column totals
		print "<tr><td>TOTALS ";
		print OUTFILE "TOTALS,";
		for ($i = 0; $i <= $maxterms2; $i++)	{
			if (($timesused[2][$i] > 0) || ($recsused[2][$i]))	{
				if ($q->param('output') eq "collections")	{
					print "<td align=center>$timesused[2][$i]";
					print OUTFILE "$timesused[2][$i],";
				}
				elsif ($q->param('output') eq "occurrences")	{
					print "<td align=center>$recsused[2][$i]";
					print OUTFILE "$recsused[2][$i],";
				}
				elsif ($q->param('output') eq "mean occurrences")	{
					print "<td align=center>",int($recsused[2][$i]/$timesused[2][$i]*10)/10;
					print OUTFILE int($recsused[2][$i]/$timesused[2][$i]*10)/10,",";
				}
			}
		}
	 # print grand total
		if ($q->param('output') eq "collections")	{
			print "<td align=center>$lines";
			print OUTFILE "$lines\n";
		}
		elsif ($q->param('output') eq "occurrences")	{
			print "<td align=center>$rectotal";
			print OUTFILE "$rectotal\n";
		}
		elsif ($q->param('output') eq "mean occurrences")	{
			print "<td align=center>",int($rectotal/$lines*10)/10;
			print OUTFILE int($rectotal/$lines*10)/10,"\n";
		}
		print "</table><p>\n";
	}
	
	close OUTFILE;
	
	if (	$q->param('searchfield1') eq "epoch" ||
			$q->param('searchfield2') eq "epoch" ||
			$q->param('searchfield1') eq "international age/stage" ||
			$q->param('searchfield2') eq "international age/stage" )	{
		print "<i>WARNING: local stages were not translated into international stages";
	
		if (($q->param('searchfield1') eq "epoch") ||
				($q->param('searchfield2') eq "epoch"))	{
			print ", and stages were not translated into epochs";
		}
		print "</i><p>\n";
	}
	
	for ( $r=1; $r<=$nsearchfields; $r++) {
		print "<p>\n";
		if($nterms[$r]){
			print "<b>$nterms[$r]</b> different terms were found";
		}
		else{
			print "<b>0</b> different terms were found";
		}
		if ( $nsearchfields > 1)       {
			print " in the ".$q->param('searchfield'.$r)." field<p>\n";
		}
		else  {
			print "<p>\n<b>$lines</b> collections";
			if ($q->param('output') ne "collections")	{
				print " and <b>$rectotal</b> occurrences of <b>";
				my @temp = keys(%genuscounted);
				printf "%d</b> taxa",$#temp + 1;
			}
			print " were checked<p>\n";
		}
	}
	if ($q->param('taxon_name') )	{
		print "The search was restricted to collections including <i>".$q->param('taxon_name')."</i><p>\n";
	}

	print "\nThe report data have been saved to \"<a href=\"$HOST_URL/public/data/$authorizer/report.csv\">report.csv</a>\"<p>\n";
    print "</center>\n";

}

# This only shown for internal errors
sub htmlError {
	my $self = shift;
    my $message = shift;

    print $message;
    exit 1;
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

1;
