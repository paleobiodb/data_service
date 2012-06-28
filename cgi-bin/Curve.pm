# see bottom of file for historical notes

package Curve;

use TimeLookup;
use Text::CSV_XS;
use PBDBUtil;
use TaxaCache;
use GD;
use Constants qw($READ_URL $DATA_DIR $HTML_DIR);

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;
my $q;					# Reference to the parameters
my $s;
my $dbt;
my $hbo;
my ($method,@genus,@freq,@u);
my $downloadForm = "displayDownloadForm";

sub new {
	my $class = shift;
	$q = shift;
	$s = shift;
	$dbt = shift;
	$dbh = $dbt->dbh;
	my $self = {};

	bless $self, $class;
	return $self;
}

sub buildCurve {
	my $self = shift;
	$hbo = shift;

	$self->setArrays;
	PBDBUtil::autoCreateDir($OUTPUT_DIR);
	# compute the sizes of intermediate steps to be reported in subsampling curves
	if ($q->param('stepsize') ne "")	{
	  $self->setSteps;
	}
	$self->assignGenera;
	$self->subsample;
	$self->printResults;
}

sub setArrays	{
	my $self = shift;

	# Setup the variables
	# this is the output directory used by Download.pm, not this program
	$DOWNLOAD_FILE_DIR = $HTML_DIR."/public/downloads";# the default working directory

	# customize the subdirectory holding the output files
	# modified to retrieve authorizer automatically JA 4.10.02
	# modified to use yourname JA 17.7.05
	$PRINTED_DIR = "/public/curve";
	if ( $s->get('authorizer') ne "" || $q->param('yourname') ne "" )	{
		my $temp;
		if ( $q->param('yourname') ne "" )	{
			$temp = $q->param('yourname');
		} else	{
			$temp = $s->get("authorizer");
		}
		$temp =~ s/ //g;
		$temp =~ s/\.//g;
		$temp =~ tr/[A-Z]/[a-z]/;
		$PRINTED_DIR .= "/" . $temp;
	}
	$OUTPUT_DIR = $HTML_DIR.$PRINTED_DIR;
	PBDBUtil::autoCreateDir($OUTPUT_DIR);

	$exponent = $q->param('exponent');
	if ($q->param('samplingmethod') eq "shareholder quorum subsampling")	{
		$method = "SQS";
		$q->param('stepsize' => '');
	}
	elsif ($q->param('samplingmethod') eq "classical rarefaction")	{
		$method = "CR";
	}
	elsif ($q->param('samplingmethod') eq "by collection (unweighted)")	{
		$method = "UW";
	}
# replaced occurrences-squared with occurrences-exponentiated 27.10.05
	elsif ($q->param('samplingmethod') eq "by collection (occurrences-weighted)")	{
		if ( $q->param('exponent') == 1 )	{
			$method = "OW";
		} else	{
			$method = "OXW";
		}
	}
	elsif ($q->param('samplingmethod') eq "by specimen")	{
		$method = "specimens";
	} else	{
		$q->param('samplesize' => '');
		$q->param('samplingtrials' => '');
		$q->param('exponent' => '');
	}
}

sub printError	{
	my $error = shift;
	my $download = shift;
	my $openFailed = shift;

	print "<center><div class=\"warning\" style=\"margin-top: 2em; padding-left: 1em; text-align: left;\">\n<p style=\"margin-left: 1em; text-indent: -1em;\">$error\n";
	my $link = "<a href=\"$READ_URL?action=$downloadForm\">download another file</a> that includes it";
	if ( $openFailed )	{
		$link = "<a href=\"$READ_URL?action=$downloadForm\">download a data file</a>";
	}
	if ( $download )	{
		print "</p>\n<p>\n";
		if ( $q->param('yourname') )	{
			print "If you didn't simply enter the wrong name, you should $link.";
		} else	{
			print "If you didn't simply forget to enter your name, you should $link.";
		}
	}
	print "</p></div></center>\n";
	print $hbo->stdIncludes("std_page_bottom");
	exit;
}


# compute the sizes of intermediate steps to be reported in subsampling curves
sub setSteps	{
	my $self = shift;

	my $i = 1;
	$atstep[0] = 0;
	$atstep[1] = 1;
	$wink = 0;
	while ($i <= $q->param('samplesize') * 11)	{
		push @samplesteps,$i;
		$x = $i;
		if ($q->param('stepsize') eq "1, 10, 100...")	{
			$i = $i * 10;
		}
		elsif ($q->param('stepsize') eq "1, 3, 10, 30...")	{
			if ($i =~ /1/)	{
				$i = $i * 3;
			}
			elsif ($i =~ /3/)	{
				$i = $i * 10 / 3;
			}
		}
		elsif ($q->param('stepsize') eq "1, 2, 5, 10...")	{
			if ($i =~ /1/ || $i =~ /5/)	{
				$i = $i * 2;
			}
			elsif ($i =~ /2/)	{
				$i = $i * 5 / 2;
			}
		}
		elsif ($q->param('stepsize') eq "1, 2, 4, 8...")	{
			$i = $i * 2;
		}
		elsif ($q->param('stepsize') eq "1, 1.4, 2, 2.8...")	{
			if ($wink == 0)	{
				$i = $i * 1.4;
				$wink = 1;
			}
			else	{
				$i = $i / 1.4 * 2;
				$wink = 0;
			}
		}
		elsif ($q->param('stepsize') eq "1000, 2000, 3000...")	{
			$i = $i + 1000;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "100, 200, 300...")	{
			$i = $i + 100;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "50, 100, 150...")	{
			$i = $i + 50;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "20, 40, 60...")	{
			$i = $i + 20;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "10, 20, 30...")	{
			$i = $i + 10;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "5, 10, 15...")	{
			$i = $i + 5;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "2, 4, 6...")	{
			$i = $i + 2;
			if ( $x == 1 ) { $i--; }
		}
		elsif ($q->param('stepsize') eq "1, 2, 3...")	{
			$i++;
		}
		for $j ($x..$i-1)	{
			$atstep[$j] = $#samplesteps + 1;
		}
		$stepback[$#samplesteps+1] = $i;
	}
}


sub assignGenera	{
	my $self = shift;

	# BEGINNING of input file parsing routine
    my $name = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
    my $filename = PBDBUtil::getFilename($name); 

    my ($occsfilecsv,$occsfiletab);
    if ($q->param("time_scale") =~ /neptune-pbdb pacman/i) {
        $occsfilecsv = $DOWNLOAD_FILE_DIR."/$filename-neptune_pbdb_pacman.csv";
        $occsfiletab = $DOWNLOAD_FILE_DIR."/$filename-neptune_pbdb_pacman.tab";
    } elsif ($q->param("time_scale") =~ /neptune-pbdb/i) {
        $occsfilecsv = $DOWNLOAD_FILE_DIR."/$filename-neptune_pbdb.csv";
        $occsfiletab = $DOWNLOAD_FILE_DIR."/$filename-neptune_pbdb.tab";
    } elsif ($q->param("time_scale") =~ /neptune pacman/i) {
        $occsfilecsv = $DOWNLOAD_FILE_DIR."/$filename-neptune_pacman.csv";
        $occsfiletab = $DOWNLOAD_FILE_DIR."/$filename-neptune_pacman.tab";
    } elsif ($q->param("time_scale") =~ /neptune/i) {
        $occsfilecsv = $DOWNLOAD_FILE_DIR."/$filename-neptune.csv";
        $occsfiletab = $DOWNLOAD_FILE_DIR."/$filename-neptune.tab";
    } else {
        $occsfilecsv = $DOWNLOAD_FILE_DIR."/$filename-occs.csv";
        $occsfiletab = $DOWNLOAD_FILE_DIR."/$filename-occs.tab";
    }
    if ((-e $occsfiletab && -e $occsfilecsv && ((-M $occsfiletab) < (-M $occsfilecsv))) ||
        (-e $occsfiletab && !-e $occsfilecsv)){
        $self->dbg("using tab $occsfiletab");
        $occsfile = $occsfiletab;
        $sepChar = "\t";
    } else {
        $self->dbg("using csv $occsfilecsv");
        $occsfile = $occsfilecsv;
        $sepChar = ",";
    }

	if ( $q->param("time_scale") =~ /neptune/i )	{
		$q->param('count_refs' => 'no');
		$q->param('one_occs_or_refs' => 'one-occurrence');
	}

    $csv = Text::CSV_XS->new({
        'quote_char'  => '"',
        'escape_char' => '"',
        'sep_char'    => $sepChar,
        'binary'      => 1
    });

	if ( ! open OCCS,"<$occsfile" )	{
		printError("The data can't be analyzed because you haven't yet downloaded a file of occurrences.",1,1);
	}

	if ( $method )	{
		if ( $method ne "SQS" && $q->param('samplesize') == 0 )	{
			printError("The sampling quota must be a positive integer. Please enter a number and resubmit the form.");
		} elsif ( $method ne "SQS" && $q->param('samplesize') < 1 )	{
			printError("The sampling quota must be greater than one. Please enter another number and resubmit the form.");
		} elsif ( $method ne "SQS" && $q->param('samplesize') =~ /[^0-9]/ )	{
			printError("The sampling quota must be a positive integer. Please enter another number and resubmit the form.");
		} elsif ( $method eq "SQS" && $q->param('samplesize') <= 0 )	{
			printError("The sampling quorum must be a positive fraction. Please enter a number and resubmit the form.");
		} elsif ( $method eq "SQS" && $q->param('samplesize') >= 1 )	{
			printError("The sampling quorum must be less than one. Please enter another number and resubmit the form.");
		}
		if ( $q->param('samplingtrials') == 0 )	{
			printError("You forgot to select the number of sampling trials. Please choose a value and resubmit the form.");
		}
	}

	# following fields need to be pulled from the input file:
	#  collection_no (should be mandatory)
	#  reference_no (optional, if refs are categories instead of genera)
	#  genus_name (mandatory)
	#  species_name (optional)
	#  abund_value and abund_unit (iff method 5 is selected)
	#  epoch or "locage_max" (can be 10 m.y. bin)
	$_ = <OCCS>;
	$status = $csv->parse($_);
	if (!$status) { print "Warning, error parsing CSV line $count"; }
	my @fieldnames = $csv->fields();
	@OCCDATA = <OCCS>;
	close OCCS;

	my $fieldcount = 0;
	my $field_collection_no = -1;
	for my $fn (@fieldnames)	{
		if ( $fn eq "collection_no" )	{
			$field_collection_no = $fieldcount;
		} elsif ( $fn eq "sample_id" ) {
			$field_collection_no = $fieldcount;
		} elsif ( $fn eq "occurrence.reference_no" && $q->param('count_refs') eq "yes" )	{
			$field_genus_name = $fieldcount;
		} elsif ( $fn eq "occurrence.genus_name" && $q->param('count_refs') ne "yes" )	{
			$field_genus_name = $fieldcount;
		} elsif ( $fn eq "occurrence.species_name" )	{
			$field_species_name = $fieldcount;
		} elsif ( $fn eq "parent_name" )	{
			$field_parent_name = $fieldcount;
		} elsif ( $fn eq "family_name" )	{
			$field_family_name = $fieldcount;
		} elsif ( $fn eq "order_name" )	{
			$field_order_name = $fieldcount;
		} elsif ( $fn eq "resolved_fossil_name" )   {
			$field_genus_name = $fieldcount;
			$field_species_name = $fieldcount;
		} elsif ( $fn eq "abund_unit" )	{
			$field_abund_unit = $fieldcount;
		} elsif ( $fn eq "abund_value" )	{
			$field_abund_value = $fieldcount;
		} elsif ( $fn eq "collections.reference_no" )	{
			$field_refno = $fieldcount;
		# field_midpt isn't used now, but could be used to force collections
		#  with broad age estimates into a bin (a rainy day project)
		# WARNING: uses the last seen of the two fields
		} elsif ( $fn eq "collections.ma_mid" || $fn eq "collections.interpolated_mid" )	{
			$field_midpt = $fieldcount;
		} elsif ( $fn eq "period" && $q->param('time_scale') eq "periods" )	{
			$field_bin = $fieldcount;
			$bin_type = "period";
		} elsif ( $fn eq "epoch" && $q->param('time_scale') eq "epochs" )	{
			$field_bin = $fieldcount;
			$bin_type = "epoch";
		} elsif ( $fn eq "subepoch" && $q->param('time_scale') eq "Cenozoic subepochs" )	{
			$field_bin = $fieldcount;
			$bin_type = "subepoch";
		} elsif ( $fn eq "stage" && $q->param('time_scale') eq "stages" )	{
			$field_bin = $fieldcount;
			$bin_type = "stage";
		} elsif ( $fn eq "10_my_bin" && $q->param('time_scale') eq "10 m.y. bins" )	{
			$field_bin = $fieldcount;
			$bin_type = "10my";
		} elsif ( $fn eq "FR2_bin" && $q->param('time_scale') =~ /fossil record/i )	{
			$field_bin = $fieldcount;
			$bin_type = "FossilRecord2";
		} elsif ( $fn eq "sample_age_ma" && $q->param('time_scale') =~ /neptune pacman/i) {
			$field_bin = $fieldcount;
			$bin_type = "neptune_pacman";
		} elsif ( $fn eq "sample_age_ma" && $q->param('time_scale') =~ /neptune/i) {
			$field_bin = $fieldcount;
			$bin_type = "neptune";
		}
		$fieldcount++;
	}


	# this first condition never should be met, just being careful here
	if ($q->param('time_scale') =~ /neptune/i) {
		$downloadForm = "displayDownloadNeptuneForm";
	}
	if ( $field_collection_no < 0)	{
		my $collection_field = "collection number";
		if ($q->param('time_scale') =~ /neptune/i)	{
			$collection_field = "sample id";
		} 
		printError("The data can't be analyzed because the $collection_field field hasn't been downloaded.",1);
	# this one is crucial and might be missing
	} elsif ( ! $field_bin )	{
		my $time_scale_field = $q->param('time_scale');
		$time_scale_field =~ s/s$//;
		if ($time_scale_field =~ /neptune/i)	{
			$time_scale_field = "sample_age_ma";
		}
		printError("The data can't be analyzed because the $time_scale_field field hasn't been downloaded.",1);
	# this one also always should be present anyway, unless the user
	#  screwed up and didn't download the ref numbers despite wanting
	#  refs counted instead of genera
	} elsif ( ! $field_genus_name && $q->param("taxonomic_level") ne "family" and $q->param("taxonomic_level") ne "order" )	{
		if ( $q->param('count_refs') ne "yes" )	{
			printError("The data can't be analyzed because the genus name field hasn't been downloaded.",1);
		} else	{
			printError("The data can't be analyzed because the reference number field hasn't been downloaded.",1);
		}
	} elsif ( $q->param("taxonomic_level") eq "species" && ! $field_species_name && $q->param('count_refs') ne "yes") {
		printError("The data can't be analyzed because the species name field hasn't been downloaded.",1);
	} elsif ( $q->param("taxonomic_level") eq "family" && ! $field_family_name && $q->param('count_refs') ne "yes") {
		printError("The data can't be analyzed because the family name field hasn't been downloaded.",1);
	} elsif ( $q->param("taxonomic_level") eq "order" && ! $field_order_name && $q->param('count_refs') ne "yes") {
		printError("The data can't be analyzed because the order name field hasn't been downloaded.",1);
	} elsif ( $q->param("taxonomic_level") =~ /family|order/ && ! $field_parent_name && $q->param('replace_with_parent') =~ /y/i) {
		printError("The data can't be analyzed because the parent name field hasn't been downloaded.",1);
	# these two might be missing
	} elsif ( ! $field_abund_value && ( $method eq "specimens" || $q->param("print_specimens") eq "YES" ) )	{
		printError("The data can't be analyzed because the abundance value field hasn't been downloaded.",1);
	} elsif ( ! $field_abund_unit && ( $method eq "specimens" || $q->param("print_specimens") eq "YES" ) )	{
		printError("The data can't be analyzed because the abundance unit field hasn't been downloaded.",1);
	} elsif ( ! $field_refno && ( $q->param('print_refs_raw') eq "yes" || $q->param('print_refs_ss') eq "yes" || $q->param('print_one_refs') =~ /yes/i || $q->param('one_occs_or_refs') =~ /ref/i ) )	{
		printError("The data can't be analyzed because the reference number field <i>from the collections table</i> hasn't been downloaded.",1);
	} elsif ( $q->param("time_scale") =~ /neptune/i ) {
		if ($q->param("neptune_bin_size") !~ /^(\d+(\.\d+)?|\.\d+)$/) {
			printError("Please enter a positive decimal number for the bin size to use if you are analyzing data from the Neptune database.");
		} elsif ($q->param("neptune_bin_size") < .1 || $q->param("neptune_bin_size") > 100) {
			printError("The bin size must be between 0.1 and 100.");
		}
	}

	# figure out the ID numbers of the bins from youngest to oldest
	# we do this in a messy way, i.e., using preset arrays; if the
	#  10 m.y. bin scheme ever changes, this will need to be updated
	# also get the bin boundaries in Ma
	# switched from Harland to Gradstein scales JA 5.12.05
	# Schroeter switched this to a TimeLookup call at some point
	my @binnames;
	my $t = new TimeLookup($dbt);
	my %name;
	my %intervals = TimeLookup::allIntervals($dbt);
	$name{$_} = $intervals{$_}->{'name'} foreach keys %intervals;

	if ( $bin_type eq "period" )	{
		my @intervals = $t->getScaleOrder(69,'number');
		@binnames = map {$name{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
		$basema{$name{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "epoch" )	{
		my @intervals = $t->getScaleOrder(71,'number');
		@binnames = map {$name{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
		$basema{$name{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "subepoch" )	{
		my @intervals = $t->getScaleOrder(72,'number');
		@binnames = map {$name{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
		$basema{$name{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "stage" )	{
		my @intervals = $t->getScaleOrder(73,'number');
		@binnames = map {$name{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
		$basema{$name{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "10my" ) {
		@binnames = $t->getBins();
		my ($top,$base) = $t->computeBinBounds('bins');
		%basema = %$base;
	} elsif ( $bin_type eq "FossilRecord2" ) {
		@binnames = $t->getFR2Bins();
		my ($top,$base) = $t->computeBinBounds('FR2');
		%basema = %$base;
	} elsif ( $bin_type =~ /neptune/i ) {
	# Neptune data ranges from -3 to 150 mA right now, use those at defaults
		$neptune_range_min = 0;
		$neptune_bin_count = int(180/$q->param('neptune_bin_size'));
		$neptune_range_max = $q->param('neptune_bin_size')*$neptune_bin_count;
		@binnames = ();
		for(my $i=$neptune_range_min;$i<$neptune_range_max;$i+=$q->param('neptune_bin_size')) {
			my $name = "$i - ".($i + $q->param('neptune_bin_size'));
			$basema{$name} = $i + $q->param('neptune_bin_size');
			push @binnames , $name;
		}
	}
	# assign chron numbers to named bins
	# note: $chrons and $chname are key variables used later
	for my $bn (@binnames)	{
		$chrons++;
		$binnumber{$bn} = $chrons;
		$chname[$chrons] = $bn;
	}

	# PS had a hard coded level value of genus for standard PBDB data
	#  here before, but it's not needed now that this is a proper query
	#  parameter JA 18.2.06

	my $count=0;
    # The sample id variables maps neptune sample ids (text strings) to integers so that the curve script may
    # process them correctly
    #my %sampleid = ();
    #my $sampleid_count = 0;
        foreach (@OCCDATA) {
		s/\n//;
		$status = $csv->parse($_);
		my @occrow = $csv->fields();
		$count++;

    		# set the bin ID number (chid) for the collection
	    	# time interval name has some annoying quotes
            if ($bin_type =~ /neptune/i) {
                # The curve script assumes that collection_no will be an integer while
                # the Neptune database uses non integer collection numbers.  So we map
                # the non-integer collection_nos to integers and keep track of the mapping
                # with a hash array called %sampleid PS 1/10/2005
                #my $sampleid = $occrow[$field_collection_no];
                #if ($sampleids{$sampleid}) {
                #    my $collection_no = $sampleids{$sampleid};
                #    $occrow[$field_collection_no] = $collection_no;
                #} else {
                #    $sampleid_count++;
                #    $sampleids{$sampleid} = $sampleid_count;
                #    $occrow[$field_collection_no] = $sampleid_count;
                #}
                my $bottom_binname = int($occrow[$field_bin]/$q->param('neptune_bin_size'))*$q->param('neptune_bin_size');
                my $binname = $bottom_binname." - ".($bottom_binname+$q->param('neptune_bin_size'));

                $chid[$occrow[$field_collection_no]] = $binnumber{$binname};
            } else {
                $occrow[$field_bin] =~ s/"//g;
                $chid[$occrow[$field_collection_no]] = $binnumber{$occrow[$field_bin]};
            }

	# Handle analysis by genus/species.  For simplicity sake we just
	# subsitute the full species name into the genus name field so the rest
	# of the script doesn't need to be changed below. Bit of a hack, PS 1/10/2005
	# adapted this section to handle family and order level data
	#  JA 18.2.06 
		if ($q->param("taxonomic_level") eq "genus") {
			if ($bin_type =~ /neptune/i && $field_genus_name == $field_species_name) {
				my $taxon_name = $occrow[$field_genus_name];
				my ($genus_name) = split(/ /,$taxon_name);
				$occrow[$field_genus_name] = $genus_name;
               		} 
                # else, nothing needs to be done for PBDB data
		# family and order level only apply to PBDB data
		} elsif ( $q->param("taxonomic_level") eq "family" && $occrow[$field_family_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_family_name]; 
		} elsif ( $q->param("taxonomic_level") eq "family" && $q->param('replace_with_parent') =~ /y/i && $occrow[$field_parent_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_parent_name]; 
		} elsif ( $q->param("taxonomic_level") eq "family" && $q->param('replace_with_parent') =~ /y/i && $occrow[$field_parent_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_parent_name]; 
		} elsif ( $q->param("taxonomic_level") eq "order" && $occrow[$field_order_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_order_name]; 
		} elsif ( $q->param("taxonomic_level") eq "order" && $q->param('replace_with_parent') =~ /y/i && $occrow[$field_parent_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_parent_name]; 
		} elsif ( $q->param("taxonomic_level") =~ /family|order/ ) {
			$occrow[$field_genus_name] = ""; 
		} elsif ( $q->param("taxonomic_level") !~ /genus|family|order/ )	{ #species
			if ($bin_type =~ /neptune/i && $field_genus_name == $field_species_name) {
                    # do this since we may have subspecies
				my $taxon_name = $occrow[$field_genus_name];
				my ($genus_name,$species_name) = split(/ /,$taxon_name);
				$occrow[$field_genus_name] = $genus_name." ".$species_name;
			} else {
				$occrow[$field_genus_name] .= " ".$occrow[$field_species_name]; 
		}
	}
	
		# get rid of records with no specimen/individual counts for specimen-
		#  based rarefaction
			if ($method eq "specimens")   {
		} elsif ( $q->param("taxonomic_level") eq "order" && $occrow[$field_order_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_order_name]; 
		} elsif ( $q->param("taxonomic_level") eq "order" && $q->param('replace_with_parent') =~ /y/i && $occrow[$field_parent_name] ) {
			$occrow[$field_genus_name] = $occrow[$field_parent_name]; 
		} elsif ( $q->param("taxonomic_level") !~ /genus|family|order/ )	{ #species
			if ($bin_type =~ /neptune/i) {
                    # do this since we may have subspecies
				my $taxon_name = $occrow[$field_genus_name];
				my ($genus_name,$species_name) = split(/ /,$taxon_name);
				$occrow[$field_genus_name] = $genus_name." ".$species_name;
			} else {
				$occrow[$field_genus_name] .= " ".$occrow[$field_species_name]; 
		}
	}
	
		# get rid of records with no specimen/individual counts for specimen-
		#  based rarefaction
			if ($method eq "specimens")   {
				if ($occrow[$field_abund_value] eq "" || $occrow[$field_abund_value] == 0 ||
				    ($occrow[$field_abund_unit] ne "specimens" && $occrow[$field_abund_unit] ne "individuals"))	{
				  $occrow[$field_genus_name] = "";
				}
			}
	
			$collno = $occrow[$field_collection_no];

		# through 9.4.05, a conditional here excluded occurrences where
		#  the species name was indet., to avoid counting higher taxa;
		#  whether to do this really is the business of the user,
		#  however
			if ( $occrow[$field_genus_name] ne "" && $chid[$collno] > 0 )      {
				$g = $occrow[$field_genus_name];
		# we used to do some cleanups here that assumed error checking
		#  on data entry didn't always work
		# disabled 30.6.04 because on that date almost all genus-level
		#  records lacked quotation marks, question marks, double
		#  spaces, trailing spaces, and subgenus names
	
		 # create a list of all distinct genus names
				if ( ! $genid{$g} )	{
					$ngen++;
					$genus[$ngen] = $g;
					$genid{$g} = $ngen;
				}

				$nsp = 1;
				if ($method eq "specimens")       {
					$nsp = $occrow[$field_abund_value];
				}

				# we used to knock out repeated occurrences of
				#  the same genus here, but that's now handled
				#  by the download script

				# add genus to master occurrence list and
				#  store the abundances
				push @occs,$genid{$g};
				if ( $lastocc[$collno] !~ /[0-9]/ )	{
					push @stone , -1;
				} else	{
					push @stone,$lastocc[$collno];
				}
				push @abund,$occrow[$field_abund_value];
				$lastocc[$collno] = $#occs;

				# count the collections belonging to
				#  each ref and record which ref this
				#  collection belongs to 9.4.05
				if ( $collrefno[$collno] < 1 && $occrow[$field_refno] > 0 && $field_refno > 0 )	{
					$collsfromref[$occrow[$field_refno]][$chid[$collno]]++;
					$collrefno[$collno] = $occrow[$field_refno];
				}

				$toccsinlist[$collno] = $toccsinlist[$collno] + $nsp;
				if ( $occrow[$field_refno] > 0 )	{
					$refisinchron[$chid[$collno]]{$occrow[$field_refno]}++;
				}
			}
		}
	# END of input file parsing routine

		if ( $q->param('recent_genera') =~ /yes/i )	{
			$self->findRecentTaxa;
	# declare Recent genera present in bin 1
			for my $taxon ( keys %extantbyname )	{
				if ( $genid{$taxon} =~ /[0-9]/ )	{
					$extantbyid[$genid{$taxon}][1] = 1;
				}
			}
		}

	 # get basic counts describing lists and their contents
		for $collno (1..$#lastocc+1)	{
			my $xx = $lastocc[$collno];
			while ( $xx >= 0 && $xx =~ /[0-9]/ )	{
				$nsp = 1;
				if ($method eq "specimens")	{
					$nsp = $abund[$xx];
				}
				if ( $q->param('print_specimens') eq "YES" && $abund[$xx] > 0 )	{
					$specimensinchron[$chid[$collno]] += $abund[$xx];
				}
				$occsread = $occsread + $nsp;
				push @{$list[$collno]} , $occs[$xx];
				$occsoftax[$occs[$xx]] = $occsoftax[$occs[$xx]] + $nsp;
				for $qq ($occsinchron[$chid[$collno]]+1..$occsinchron[$chid[$collno]]+$nsp)	{
					$occsbychron[$chid[$collno]][$qq] = $occs[$xx];
				}
				$occsinchron[$chid[$collno]] = $occsinchron[$chid[$collno]] + $nsp;
				$present[$occs[$xx]][$chid[$collno]]++;
				$inref[$chid[$collno]]{$occs[$xx]}{$collrefno[$collno]}++;
				$occsinlist[$collno] = $occsinlist[$collno] + $nsp;
				if ($occsinlist[$collno] == $nsp)	{
					$listsread++;
					$listsinchron[$chid[$collno]]++;
					$listsbychron[$chid[$collno]][$listsinchron[$chid[$collno]]] = $collno;
					$listsfromref[$collrefno[$collno]][$chid[$collno]]++;
				}
				$xx = $stone[$xx];
			}
		}

		if ( $listsread == 0 )	{
			printError("The occurrence file was found, but data can't be analyzed because the data records were all skipped.",1,1);
		}

		for my $c ( 0..$#collrefno )	{
			if ( $collrefno[$c] > 0 )	{
				$occsfromref[$collrefno[$c]][$chid[$c]] = $occsfromref[$collrefno[$c]][$chid[$c]] + $occsinlist[$c];
			}
		}

	# find the number of references in each chron
	for $i (1..$chrons)	{
		my @temp = keys %{$refisinchron[$i]};
		$refsinchron[$i] = $#temp + 1;
		$refsread = $refsread + $refsinchron[$i];
	}

	# compute median richness of lists in each chron
	for $i (1..$chrons)	{
		@temp = ();
		for $j (1..$listsinchron[$i])	{
			push @temp , $listsbychron[$i][$j];
		}
		@temp = sort { $occsinlist[$b] <=> $occsinlist[$a] } @temp;
		$j = int(($#temp)/2);
		$k = int(($#temp + 1)/2);
		$median[$i] = ($occsinlist[$temp[$j]] + $occsinlist[$temp[$k]])/2;
	# flag taxa in the largest collection
	# these will not be counted in the one-occurrence or reference count
		$inlargest[$i]{$_}++ foreach @{$list[$temp[0]]};
	}

	# compute sum of (squared) richnesses across lists
	if ( $method =~ /CR|specimens/ )	{
		for $i (1..$#occsinlist+1)	{
			$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
		}
	}
	elsif ($method eq "OW")	{
		for $i (1..$#occsinlist+1)	{
			if ($occsinlist[$i] <= $q->param('samplesize'))	{
				$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
			}
		}
	}
	for $i (1..$#occsinlist+1)	{
	# don't blow out the quota if you have a long list and the method is 4
		if ($method ne "OXW" ||
				$occsinlist[$i]**$exponent <= $q->param('samplesize') || $q->param('samplesize') == 0)	{
			$occsinchron2[$chid[$i]] = $occsinchron2[$chid[$i]] + $occsinlist[$i]**$exponent;
		}
	}

	# tabulate genus ranges and turnover data
	if ( ! open PADATA,">$OUTPUT_DIR/presences.txt" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/presences.txt<BR>$!" );
	}

	# For neptune data, we make bin names for all bins up to 200 ma year ago, now we
	# trim those bins down after the fact by setting chrons to be the last bin with
	# occurrences in it
	if ($bin_type =~ /neptune/i)	{
		my $i = 0;
		foreach($i=$chrons;$i > 0;$i--)	{
			if ($listsinchron[$i])	{
				last;
			}
		}
		$chrons = $i;
	}

	my $firstdata;
	for $j (1..$chrons)	{
		for $i (1..$ngen)	{
			if ($present[$i][$j] > 0)	{
				$firstdata = $j;
				last;
			}
		}
	}
	my $lastdata;
	for $j (reverse 1..$chrons)	{
		for $i (1..$ngen)	{
			if ($present[$i][$j] > 0)	{
				$lastdata = $j;
				last;
			}
		}
	}
	# compute sampled diversity, range through diversity, originations,
	#  extinctions, singletons and doubletons (Chao l and m)
	print PADATA $q->param("taxonomic_level")."\t"."total occs";
	for $i (reverse $lastdata..$firstdata)	{
		print PADATA "\t$chname[$i]";
	}
	if ( $q->param('recent_genera') )	{
		print PADATA "\tRecent";
	}
	print PADATA "\n";
	for $i (1..$ngen)	{
		$first = 0;
		$last = 0;
		for $j (1..$chrons)	{
			if ($present[$i][$j] > 0)	{
				$richness[$j]++;
				if ($last == 0)	{
					$last = $j;
				}
				$first = $j;
			}
			# find one-reference taxa
			my @refs = keys %{$inref[$j]{$i}};
			if ($#refs == 0 && ($q->param('exclude_largest_coll') !~ /yes/i || ! $inlargest[$j]{$i}))	{
				$onerefs[$j]++;
			# find dominant taxon (must be in multiple refs)
			} elsif ($present[$i][$j] > $maxoccs[$j])	{
				$maxoccs[$j] = $present[$i][$j];
				$dominant[$j] = $i;
			}
			if ( $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
				$twotimers[$j]++;
			}
			if ( $present[$i][$j-1] > 0 && $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
				$threetimers[$j]++;
				$rawsum3timers++;
			}
			if ( $present[$i][$j-1] > 0 && $present[$i][$j] == 0 && $present[$i][$j+1] > 0 )	{
				$parttimers[$j]++;
				$rawsumPtimers++;
			}
			if ( $j > 1 && $j < $chrons - 1 && ( $present[$i][$j-1] > 0 || $present[$i][$j] > 0 ) && ( $present[$i][$j+1] > 0 || $present[$i][$j+2] > 0 ) )	{
				$localbc[$j]++;
			}
			if ( ( $present[$i][$j-1] > 0 && $present[$i][$j+1] > 0 ) || $present[$i][$j] > 0 )	{
				$localrt[$j]++;
			}
			# find single-occurrence taxa
			# chaom is the notation for Chao-2, q1 for ICE
			if ($present[$i][$j] == 1)	{
				$chaol[$j]++;
				$q1[$j]++;
				if ($q->param('exclude_largest_coll') !~ /yes/i || ! $inlargest[$j]{$i})	{
					$oneoccs[$j]++;
				}
			}
			elsif ($present[$i][$j] == 2)	{
				$chaom[$j]++;
			}
			# stats needed for ICE
			if ( $present[$i][$j] > 10 )	{
				$sfreq[$j]++;
			} elsif ( $present[$i][$j] > 0 )	{
				$q[abs($present[$i][$j])][$j]++;
				$ninf[$j] += abs($present[$i][$j]);
				$sinf[$j]++;
			}
		}
		if ($first > 0)	{
			print PADATA "$genus[$i]\t$occsoftax[$i]";
			for $j (reverse $lastdata..$firstdata)	{
				print PADATA "\t$present[$i][$j]";
			}
			if ( $q->param('recent_genera') )	{
				printf PADATA "\t%d",$extantbyid[$i][1];
			}
			print PADATA "\n";
		}
		if (($first > 0) && ($last > 0))	{
			if ( $extantbyid[$i][1] == 1 )	{
				$last = 0;
			}
			for $j ($last..$first)	{
				$rangethrough[$j]++;
			}
			if ($first == $last)	{
				$singletons[$first]++;
			}
			$originate[$first]++;
			$extinct[$last]++;
			$foote[$first][$last]++;
		# stats needed for Jolly-Seber estimator
		# note that "first" is bigger than "last" because time bins
		#  are numbered from youngest to oldest
			for $j ($last..$first)	{
				if ( $present[$i][$j] + $extantbyid[$i][1] > 0 )	{
					if ( $first > $j )	{
						$earlier[$j]++;
					}
					if ( $last < $j )	{
						$later[$j]++;
					}
				}
			}
		# Hurlbert's PIE
			for $j ($last..$first)	{
				if ( $present[$i][$j] > 0 && $occsinchron[$j] > 1 )	{
					$pie[$j] = $pie[$j] + ( $present[$i][$j] / $occsinchron[$j] )**2;
				}
			}
		}
	}
	close PADATA;

	# compute Good's u
	for $i (1..$chrons)	{
		if ( $occsinchron[$i] > 0 )	{
			my $d = 0;
			if ( $q->param('exclude_dominant') =~ /y/i )	{
				$d = $maxoccs[$i];
			}
			if ( $q->param('one_occs_or_refs') =~ /ref/i && $occsinchron[$i] - $d > 0 )	{
				$u[$i] = 1 - ( $onerefs[$i] / ( $occsinchron[$i] - $d ) );
			} elsif ( $occsinchron[$i] - $d > 0 )	{
				$u[$i] = 1 - ( $oneoccs[$i] / ( $occsinchron[$i] - $d ) );
			}
		# frequencies are only needed with SQS, and if the method is
		#  by-specimen occsinchron will be wrong anyway
			if ( $method eq "SQS" )	{
				for $j (1..$ngen)	{
					if ( $q->param('exclude_dominant') =~ /y/i && $j == $dominant[$i] )	{
						next;
					}
					if ( $occsinchron[$i] - $d == 0 )	{
						next;
					}
					$freq[$j][$i] = $present[$j][$i] / ( $occsinchron[$i] - $d ) * $u[$i];
				}
			}
		}
	}

	# compute ICE
$| = 1;
	for $i (1..$chrons)	{
		if ( $ninf[$i] > 0 )	{
			my $minf = 0;
			for my $j ( 1..$listsinchron[$i] )	{
				my $xx = $lastocc[$listsbychron[$i][$j]];
				while ( $xx >= 0 && $xx =~ /[0-9]/ )	{
					if ( $present[$occs[$xx]][$i] <= 10 )	{
						$minf++;
						$xx = -1;
					} else	{
						$xx = $stone[$xx];
					}
				}
			}
			my $cice = 1 - ( $q1[$i] / $ninf[$i] );
			if ( $cice > 0 && $minf > 0 )	{
				my $sum = 0;
				for $j (1..10)	{
					$sum += $j * ( $j - 1 ) * $q[$j][$i];
				}
				my $g2 = ( ( $sinf[$i] / $cice ) * ( $minf / ( $mimf - 1 ) ) * ( sum / $ninf[$i]**2 ) ) - 1;
				if ( $g2 < 0 )	{
					$g2 = 0;
				}
				$ice[$i] = $sfreq[$i] + ( $sinf[$i] / $cice ) + ( $q1[$i] / $cice * $g2 );
			}
		}
	}
	for $i (1..$chrons)	{
		if ( $occsinchron[$i] > 1 )	{
			$pie[$i] = ( 1 - $pie[$i] ) / ( $occsinchron[$i] / ( $occsinchron[$i] - 1 ) );
		}
	}
	
	if ( ! open FOOTE,">$OUTPUT_DIR/firstlast.txt" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/firstlast.txt<BR>$!" );
	}
	for $i (reverse 1..$chrons)	{
		print FOOTE "$chname[$i]";
		if ($i > 1)	{
			print FOOTE "\t";
		}
	}
	print FOOTE "\n";
	for $i (reverse 1..$chrons)	{
		print FOOTE "$chname[$i]\t";
		for $j (reverse 1..$chrons)	{
			if ($foote[$i][$j] eq "")	{
				$foote[$i][$j] = 0;
			}
			print FOOTE "$foote[$i][$j]";
			if ($j > 1)	{
				print FOOTE "\t";
			}
		}
		print FOOTE "\n";
	}
	close FOOTE;

	# compute Jolly-Seber estimator
	# based on Nichols and Pollock 1983, eqns. 2 and 3, which reduce to
	#  N = n^2z/rm + n where N = estimate, n = sampled, z = range-through
	#  minus sampled, r = sampled and present earlier, and m = sampled
	#  and present later
	for $i (reverse 1..$chrons)	{
		if ( $earlier[$i] > 0 && $later[$i] > 0 )	{
			$jolly[$i] = ($richness[$i]**2 * ($rangethrough[$i] - $richness[$i]) / ($earlier[$i] * $later[$i])) + $richness[$i];
		}
	}

	# free some variables
	@lastocc = ();
	@occs = ();
	@stone = ();
	@abund = ();

}

# JA 16.8.04
sub findRecentTaxa	{

	my $self = shift;

	my $asql = "SELECT taxon_no,taxon_name FROM authorities WHERE extant='YES' AND taxon_name IN ('" . join("','",@genus) . "')";
	my @arefs = @{$dbt->getData($asql)};
	my @taxon_nos= map {$_->{taxon_no}} @arefs;

	my $parents = TaxaCache::getParents($dbt,\@taxon_nos,'array_full');
	# extantbyname must be global!
	for my $aref ( @arefs )	{
		$extantbyname{$aref->{taxon_name}}++;
		# genera are extant if their species are, families if their
		#  genera are, etc. (important for family or order level
		#  analyses) JA 28.9.06
		if ($parents->{$aref->{taxon_no}}) {
			my @parent_list = @{$parents->{$aref->{taxon_no}}};
			for my $p (@parent_list) {
				$extantbyname{$p->{taxon_name}}++;
			}
		}
	}
	return;

}

sub subsample	{
	my $self = shift;

	my $trials = $q->param('samplingtrials');
	if ($q->param('samplingtrials') > 0)	{
		for my $trial (1..$q->param('samplingtrials'))	{
			my @sampled = ();
			my @lastsampled = ();
			my @subsrichness = ();
			my @lastsubsrichness = ();
			my @present = ();
			my @refsampled = ();

			for $i ( 1..$chrons )	{
				if ( ($q->param('printall') eq "yes" && $listsinchron[$i] > 0) || (
	($u[$i] >= $q->param('samplesize') && $method eq "SQS") ||
	($usedoccsinchron[$i] >= $q->param('samplesize') && $method !~ /SQS|UW|OXW/) ||
	($listsinchron[$i] >= $q->param('samplesize') && $method eq "UW") ||
	($occsinchron2[$i] >= $q->param('samplesize') && $method eq "OXW") ) )	{

		# record number of items available to be drawn
					if ( $method =~ /SQS|UW/ )	{
						$inbin = $listsinchron[$i];
					} elsif ( $method eq "OXW" )	{
						$inbin = $occsinchron2[$i];
					} else	{
						$inbin = $usedoccsinchron[$i];
					}

		# set the number of items that must be drawn
		# WARNING: an "item" here means an occurrence or a collection;
		#  in the output an "item" may be a record-squared
					$ndrawn = 0;
					$tosub = $q->param('samplesize');
	# special hacks for OW and OXW to handle overshooting of quotas
	# old method was to track the number of items sampled and set the quota
	#  based on this number; new method is to track the number of lists
	#  regardless of the units of interest, and set quota based on the ratio
	#  of the items quota to the average number of items per list 19.8.00
					if ($method eq "OW")	{
						$tosub = ($tosub/($usedoccsinchron[$i]/$listsinchron[$i])) - 0.5;
					}
					elsif ($method eq "OXW")	{
						$tosub = ($tosub/($occsinchron2[$i]/$listsinchron[$i])) - 0.5;
					}
		 # make a list of items that may be drawn
					if ( $method !~ /CR|specimens/ )	{
					  for $j (1..$listsinchron[$i])	{
					    $listid[$j] = $listsbychron[$i][$j];
					  }
					  $nitems = $listsinchron[$i];
		 # delete lists that would single-handedly blow out the record
		 #   quota for this interval (OW or OXW only)
					  if ( $method =~ /OW|OXW/ )	{
					    for $j (1..$listsinchron[$i])	{
					      my $xx = $occsinlist[$listid[$j]];
					      if ( $method eq "OXW" )	{
					        $xx = $xx**$exponent;
					      }
					      if ($xx > $q->param('samplesize'))	{
					        $listid[$j] = $listid[$nitems];
					        $nitems--;
					      }
					    }
					  }
					}
					else	{
					  for $j ( 1..$occsinchron[$i] )	{
					    $occid[$j] = $occsbychron[$i][$j];
					  }
					  $nitems = $occsinchron[$i];
					}
		# main sampling loop
					while ($tosub > 0 && $sampled[$i] < $inbin)	{
						$lastsampled[$i] = $sampled[$i];
		# draw an item
						$j = int(rand $nitems) + 1;
						if ( $field_refno > 0 && $collrefno[$listid[$j]] > 0 )	{
							if ( $q->param('disperse') =~ /y/i )	{
								$j = -1;
								while ( $j == -1 )	{
									$j = int(rand $nitems) + 1;
									if ( rand() > 1 / $listsfromref[$collrefno[$listid[$j]]][$i] )	{
										$j = -1;
									}
								}
							}
							$refsampled[$collrefno[$listid[$j]]][$i]++;
							if ( $refsampled[$collrefno[$listid[$j]]][$i] == 1 )	{
								$msubsrefrichness[$i]++;
							}
						}
	
		# modify the counter
					  if ( $method =~ /CR|UW|specimens/ )	{
					      $tosub--;
					      $sampled[$i]++;
					  } elsif ( $method eq "OW" )	{
					      my $xx = $#{$list[$listid[$j]]} + 1;
					      $tosub--;
					      $sampled[$i] = $sampled[$i] + $xx;
					  } elsif ( $method eq "SQS" )	{
					# needed because a list may include the same
					#  taxon more than once
					      my @timesinlist = ();
					      for my $k ( @{$list[$listid[$j]]} )	{
					          $timesinlist[$k]++;
					          if ( $timesinlist[$k] == 1 && $present[$k][$i] == 0 )	{
					              $tosub -= $freq[$k][$i];
					          }
					      }
					      $sampled[$i]++;
					  }
					# OXW
					  else	{
					    my $xx = $#{$list[$listid[$j]]} + 1;
					    $tosub--;
					    $sampled[$i] = $sampled[$i] + $xx**$exponent;
					  }

		# for SQS, stop with a probability inversely proportional to
		#  the overshoot if the quorum has been exceed (i.e.,
		#  counterintuitively, if the overshoot is large keep the draw,
		#  and if it is small effectively throw it back by bombing out)
					  if ( $method eq "SQS" && $tosub < 0 )	{
					      my $plus = 1;
					      my $over = $q->param('samplesize') - $tosub;
					      $plus = 1 - $q->param('samplesize') / $over;
					      if ( rand() > $plus )	{
					          $sampled[$i]--;
					          last;
                                              }
					  }


		 # declare the genus (or all genera in a list) present in this chron
					  $lastsubsrichness[$i] = $subsrichness[$i];
					  if ( ! $lastsubsrichness[$i] )	{
						$lastsubsrichness[$i] = 1;
					  }
					  if ( $method =~ /CR|specimens/ )	{
					    if ($present[$occid[$j]][$i] == 0)	{
					      $subsrichness[$i]++;
					    }
					    $present[$occid[$j]][$i]++;
					  } else	{
					    for my $k ( @{$list[$listid[$j]]} )	{
					      if ($present[$k][$i] == 0)	{
					        $subsrichness[$i]++;
					      }
					      if ( $sampled[$i] == 1 && $method =~ /SQS|UW/ )	{
					        $lastsubsrichness[$i] = $subsrichness[$i];
					      }
					      $present[$k][$i]++;
					    }
					  }
	
		 # record data in the complete subsampling curve
		 # (but only at intermediate step sizes requested by the user)
					  if ($atstep[int($sampled[$i])] > $atstep[int($lastsampled[$i])])	{
					    $z = $sampled[$i] - $lastsampled[$i];
					    $y = $subsrichness[$i] - $lastsubsrichness[$i];
					    for $k ($atstep[int($lastsampled[$i])]..$atstep[int($sampled[$i])]-1)	{
					      $x = $stepback[$k] - $lastsampled[$i];
					      $sampcurve[$i][$k] = $sampcurve[$i][$k] + ($x * $y / $z) + $lastsubsrichness[$i];
					    }
					  }
		# for UW, compute the honest to goodness complete subsampling curve
					  if ( $method eq "UW" )	{
					    $fullsampcurve[$i][$sampled[$i]] = $fullsampcurve[$i][$sampled[$i]] + $subsrichness[$i];
					  }
	
		 # erase the list or occurrence that has been drawn
					  if ( $method !~ /CR|specimens/  )	{
						my $xx = $lastocc[$listid[$j]];
						while ( $xx >= 0 && $xx =~ /[0-9]/ )	{
							if ( $present[$occs[$xx]][$i] <= 10 )	{
								$msubsminf[$i]++;
								$xx = -1;
							} else	{
								$xx = $stone[$xx];
							}
						}
						$listid[$j] = $listid[$nitems];
					  }
					  else	{
						$occid[$j] = $occid[$nitems];
					  }
					  $nitems--;
					}
		 # end of main sampling loop
				}
			# finish off recording data in complete subsampling curve
				if ($atstep[$q->param('samplesize')]+1 > $atstep[int($sampled[$i])] && $inbin > $sampled[$i])	{
					$w = $inbin;
					if ($inbin > $q->param('samplesize'))	{
					  $w = $q->param('samplesize');
					}
					$z = $w - $sampled[$i];
					$y = $subsrichness[$i] - $lastsubsrichness[$i];
					if ($z > 0)	{
					  for $k ($atstep[int($sampled[$i])]..$atstep[int($w)])	{
					    $x = $stepback[$k] - $sampled[$i];
					    $sampcurve[$i][$k] = $sampcurve[$i][$k] + ($x * $y / $z) + $lastsubsrichness[$i];
					  }
					}
				}
		 # end of chrons (= i) loop
			}
			for $i (1..$chrons)	{
				$tsampled[$i] = $tsampled[$i] + $sampled[$i];
			}
			@tlocalbc = ();
			@tlocalrt = ();
			@ttwotimers = ();
			@trangethrough = ();
			@tsingletons = ();
			@toriginate = ();
			@textinct = ();
			for $i (1..$ngen)	{
				$first = 0;
				$last = 0;
				for $j (1..$chrons)	{
					if ($present[$i][$j] > 0)	{
						if ($last == 0)	{
							$last = $j;
						}
						$first = $j;
					}
					if ( $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
						$ttwotimers[$j]++;
						$mtwotimers[$j]++;
					}
					if ( $present[$i][$j-1] > 0 && $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
						$mthreetimers[$j]++;
						$sumthreetimers++;
					}
					if ( $present[$i][$j-1] > 0 && $present[$i][$j] == 0 && $present[$i][$j+1] > 0 && $meanrichness{'SIB'}[$j] > 0 )	{
						$mparttimers[$j]++;
						$sumparttimers++;
					}
					if ( $j > 1 && $j < $chrons - 1 && ( $present[$i][$j-1] > 0 || $present[$i][$j] > 0 ) && ( $present[$i][$j+1] > 0 || $present[$i][$j+2] > 0 ) )	{
						$tlocalbc[$j]++;
					}
					if ( ( $present[$i][$j-1] > 0 && $present[$i][$j+1] > 0 ) || $present[$i][$j] > 0 )	{
						$tlocalrt[$j]++;
					}
				}
				if ( $extantbyid[$i][1] == 1 )	{
					$last = 0;
				}
				for $j ($last..$first)	{
					$msubsrangethrough[$j]++;
					$trangethrough[$j]++;
				}
				if ($first == $last)	{
					$msubssingletons[$first]++;
					$tsingletons[$first]++;
				}
				$msubsoriginate[$first]++;
				$msubsextinct[$last]++;
				$toriginate[$first]++;
				$textinct[$last]++;
				for $j ($last..$first)	{
					if ( $present[$i][$j] > 0 )	{
						if ($present[$i][$j] == 1)	{
							$msubschaol[$j]++;
							$msubsq1[$j]++;
						}
						elsif ($present[$i][$j] == 2)	{
							$msubschaom[$j]++;
						}
					# stats needed for ICE
						if ( $present[$i][$j] > 10 )	{
							$msubssfreq[$j]++;
						} elsif ( $present[$i][$j] < 0 )	{
							$msubsq[$present[$i][$j]][$j]++;
							$msubsninf[$j] += $present[$i][$j];
							$msubssinf[$j]++;
						}
						if ( $first > $j )	{
							$msubsearlier[$j]++;
						}
						if ( $last < $j )	{
							$msubslater[$j]++;
						}
					}
				}
			}

			for $i (1..$chrons)	{
				$meanrichness{'BC'}[$i] = $meanrichness{'BC'}[$i] + $trangethrough[$i] - $toriginate[$i];
				$meanrichness{'RT'}[$i] = $meanrichness{'RT'}[$i] + $trangethrough[$i];
				push @{$outrichness[$i]} , $subsrichness[$i];
				$meanrichness{'SIB'}[$i] = $meanrichness{'SIB'}[$i] + $subsrichness[$i];
			}
	 # end of trials loop
		}

		for $i (1..$chrons)	{
			if ($msubsrangethrough[$i] > 0)	{
				$tsampled[$i] = $tsampled[$i]/$trials;
				$msubsrefrichness[$i] = $msubsrefrichness[$i] / $trials;
				$mtwotimers[$i] = $mtwotimers[$i]/$trials;
				$mthreetimers[$i] = $mthreetimers[$i]/$trials;
				$mparttimers[$i] = $mparttimers[$i]/$trials;
				$msubsnewbc[$i] = $msubsnewbc[$i]/$trials;
				$msubsrangethrough[$i] = $msubsrangethrough[$i]/$trials;
				$meanrichness{'BC'}[$i] = $meanrichness{'BC'}[$i]/$trials;
				$meanrichness{'RT'}[$i] = $meanrichness{'RT'}[$i]/$trials;
				$meanrichness{'SIB'}[$i] = $meanrichness{'SIB'}[$i]/$trials;
				$msubsoriginate[$i] = $msubsoriginate[$i]/$trials;
				$msubsextinct[$i] = $msubsextinct[$i]/$trials;
				$msubssingletons[$i] = $msubssingletons[$i]/$trials;
				$msubschaol[$i] = $msubschaol[$i]/$trials;
				$msubschaom[$i] = $msubschaom[$i]/$trials;
				$msubsearlier[$i] = $msubsearlier[$i]/$trials;
				$msubslater[$i] = $msubslater[$i]/$trials;
				for $j (0..$atstep[$q->param('samplesize')])	{
					$sampcurve[$i][$j] = $sampcurve[$i][$j]/$trials;
				}
			}
		}
	}
	
	for $i (1..$chrons)	{
		@{$outrichness[$i]} = sort { $a <=> $b } @{$outrichness[$i]};
	}

	# three-timer gap proportion JA 23.8.04
	if ( $sumthreetimers + $sumparttimers > 0 )	{
		$threetimerp = $sumthreetimers / ( $sumthreetimers + $sumparttimers);
	}

	# compute extinction rate, origination rate, corrected BC, and 
	#  corrected SIB using three timer equations (Meaning of Life
	#  Equations or Fundamental Equations of Paleobiology) plus three-timer
	#  gap analysis sampling probability plus eqn. A29 of Raup 1985
	#  JA 22-23.8.04
	# as usual, the notation here is confusing because the next youngest
	#  interval is i - 1, not i + 1; also, mnewsib i is the estimate for
	#  the bin between boundaries i and i - 1

	if ( ( $q->param('print_origination_rate_ss') =~ /yes/i || $q->param('print_extinction_rate_ss') =~ /yes/i ) && $threetimerp > 0)	{

		# get the turnover rates
		# note that the rates are offset by the sampling probability,
		#  so we need to add it
		for $i (1..$chrons)	{
			if ( $mtwotimers[$i] > 0 && $mtwotimers[$i-1] > 0 && $mthreetimers[$i] > 0 )	{
				$mu[$i] = ( log ( $mthreetimers[$i] / $mtwotimers[$i] ) * -1 ) + log ( $threetimerp );
				$lam[$i] = ( log ( $mthreetimers[$i] / $mtwotimers[$i-1] ) * -1 ) + log ( $threetimerp );
			}
		}

		# get the corrected boundary crosser estimates
		for $i (1..$chrons)	{
			if ( $mtwotimers[$i] > 0 )	{
				$mnewbc[$i] = $mtwotimers[$i] / ( $threetimerp**2 );
			}
		}

		# get the SIB counts using the BC counts and the Raup equation
		for $i (1..$chrons)	{
			if ( $mu[$i] && $lam[$i] )	{
				if ( $mu[$i] != $lam[$i] )	{
					$mnewsib[$i] = $mnewbc[$i] * ( ( $mu[$i] - ( $lam[$i] * exp ( ( $lam[$i] - $mu[$i] ) ) ) ) / ( $mu[$i] - $lam[$i] ) );
				} else	{
					$mnewsib[$i] = $mnewbc[$i] * ( 1 + $lam[$i] );
				}
			}
		}

		# get the midpoint diversity estimates
		for $i (1..$chrons)	{
			if ( $mnewbc[$i] > 0 && $mnewbc[$i-1] > 0 )	{
				$mmidptdiv[$i] = $mnewbc[$i] * exp ( 0.5 * ( $lam[$i] - $mu[$i] ) );
			}
		}
		# get mesa diversity estimates 25.9.04
		# note: the idea is that mesa diversity is either starting
		#  diversity plus originating taxa, or ending diversity plus
		#  ending taxa, but in either case this reduces to t1 t2 / t12
		# algebraically, if t1 and t2 are biased by p^2 and t12 is
		#  biased by p^2, then mesa diversity is biased by p, so divide
		#  it by p
		# again, chrons are in reverse order, so we are looking at
		#  i - 1 instead of i + 1
		for $i (1..$chrons)	{
			if ( $mthreetimers[$i] > 0 && $threetimerp > 0 )	{
				$mmesa[$i] = $mtwotimers[$i] * $mtwotimers[$i-1] / ( $mthreetimers[$i] * $threetimerp );
			}
		}
	}
	# end of Meaning of Life Equations

	# compute incidence-based coverage estimator
	for $i (1..$chrons)	{
		if ( $msubsninf[$i] > 0 )	{
			my $cice = 1 - ( $msubsq1[$i] / $msubsninf[$i] );
			my $sum = 0;
			for $j (1..10)	{
				$sum += $j * ( $j - 1 ) * $msubsq[$j][$i];
			}
			my $g2 = ( ( $msubssinf[$i] / $cice ) * ( $msubsminf[$i] / ( $mimf - 1 ) ) * ( sum / $msubsninf[$i]**2 ) ) - 1;
			if ( $g2 < 0 )	{
				$g2 = 0;
			}
			$msubsice[$i] = $msubssfreq[$i] + ( $msubssinf[$i] / $cice ) + ( $msubsq1[$i] / $cice * $g2 );
			$msubsice[$i] /= $trials;
		}
	}

	# compute Jolly-Seber estimator
	for $i (reverse 1..$chrons)	{
		if ( $msubsearlier[$i] > 0 && $msubslater[$i] > 0 )	{
			$msubsjolly[$i] = ($meanrichness{'SIB'}[$i]**2 * ($msubsrangethrough[$i] - $meanrichness{'SIB'}[$i]) / ($msubsearlier[$i] * $msubslater[$i])) + $meanrichness{'SIB'}[$i];
		}
	}

	# fit Michaelis-Menten equation using Raaijmakers maximum likelihood
	#  equation (Colwell and Coddington 1994, p. 106) 13.7.04
	# do this only for UW because the method assumes you are making a UW curve
	if ( $method eq "UW" && $q->param('printall') eq "no")	{
		for $i (1..$chrons)	{
			if ($meanrichness{'SIB'}[$i] > 0)	{
				# get means
				my $sumx = 0;
				my $sumy = 0;
				my $curvelength = 0;
				for $j (1..$q->param('samplesize'))	{
					if ( $fullsampcurve[$i][$j] > 0 )	{
						$fullsampcurve[$i][$j] = $fullsampcurve[$i][$j] / $trials;
						$curvelength++;
						$xj[$j] = $fullsampcurve[$i][$j] / $j;
						$yj[$j] = $fullsampcurve[$i][$j];
						$sumx = $sumx + $xj[$j];
						$sumy = $sumy + $yj[$j];
					}
				}
				my $meanx = $sumx / $curvelength;
				my $meany = $sumy / $curvelength;
				# get sums of squares and cross-products
				my $cov = 0;
				my $ssqx = 0;
				my $ssqy = 0;
				for $j (1..$q->param('samplesize'))	{
					if ( $fullsampcurve[$i][$j] > 0 )	{
						$cov = $cov + ( ($xj[$j] - $meanx) * ($yj[$j] - $meany) );
						$ssqx = $ssqx + ($xj[$j] - $meanx)**2;
						$ssqy = $ssqy + ($yj[$j] - $meany)**2;
					}
				}
				my $B = (($meanx * $ssqy) - ($meany * $cov)) / (($meany * $ssqx) - ($meanx * $cov));
				$michaelis[$i] = $meany + ($B * $meanx);
			}
		}
	}

}

sub printResults	{
	my $self = shift;

	if ( ! open TABLE,">$OUTPUT_DIR/raw_curve_data.csv" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/raw_curve_data.csv<BR>$!" );
	}
	
	if ( $method && $q->param('stepsize') ne "" )	{
		if ( ! open CURVES, ">$OUTPUT_DIR/subcurve.tab" ) {
			$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/subcurve.tab<BR>$!" );
		}
		print CURVES "items";
		for $i (reverse 1..$chrons)	{
			if ($sampcurve[$i][1] > 0)	{
				print CURVES "\t$chname[$i]";
			}
		}
		print CURVES "\n";
		for $i (0..$atstep[$q->param('samplesize')])	{
			print CURVES "$samplesteps[$i]\t";
			$temp = 0;
			for $j (reverse 1..$chrons)	{
				if ($sampcurve[$j][1] > 0)	{
					if ($temp > 0)	{
						print CURVES "\t";
					}
					$temp++;
					if ( $sampcurve[$j][$i] > 0 )	{
						printf CURVES "%.1f",$sampcurve[$j][$i];
					} else	{
						print CURVES "NA";
					}
				}
			}
			print CURVES "\n";
			if ($samplesteps[$i+1] > $q->param('samplesize'))	{
				last;
			}
		}
		close CURVES;
	}
	if ( ! open SUB_TABLE,">$OUTPUT_DIR/subsampled_curve_data.csv" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/subsampled_curve_data.csv<BR>$!" );
	}
	
	if ($listsread > 0)	{
		$listorfm = "Collections"; # I'm not sure why this is a variable
					# but what the heck
		$generaorrefs = "genera";
		if ( $q->param('taxonomic_level') eq 'species') {
			$generaorrefs = "species";
		} elsif ( $q->param('taxonomic_level') eq 'family') {
			$generaorrefs = "families";
		} elsif ( $q->param('taxonomic_level') eq 'order') {
			$generaorrefs = "orders";
		}
	my $panels = ( $q->param('samplesize') > 0 ) ? 3 : 2;

        print qq|<script src="/JavaScripts/tabs.js" language="JavaScript" type="text/javascript"></script>
    <div align=center>
         <table cellpadding=0 cellspacing=0 border=0 style="width: 54em;"><tr>
         <td id="tab1" class="tabOff" onClick="switchToPanel(1,$panels);">
             Raw data</td>
         <td id="tab2" class="tabOff" onClick="switchToPanel(2,$panels);">
|;
    if ($q->param('samplesize'))	{
        print qq|
                     Subsampled data </td>
                   <td id="tab3" class="tabOff" onClick="switchToPanel(3,$panels);">
                     Diversity curve </td>
|;
    } else	{
        print qq|
             Diversity curve </td>
|;
    }
        print '         </tr></table>
       </div>';

	print "\n\n<div align=\"center\"><p class=\"pageTitle\" style=\"margin-bottom: 0.5em;\">Diversity curve report</p></div>\n\n";

	print "<div id=\"panel1\" class=\"panel\">\n\n";

		if ( $q->param('count_refs') eq "yes" )	{
			$generaorrefs = "references";
		}
		# need this diversity value for origination rates to make sense
		#  when ranging through taxa to the Recent in Pull of the
		#  Recent analyses
		$bcrich[0] = $rangethrough[0];
		print qq|<div class="displayPanel" style="padding-top: 1em;">
<table cellpadding="4">
|;
		print "<tr><td class=tiny valign=top><b>Interval</b>\n";
		my @header;
		push @header , 'Bin,"Bin name"';
		if ( $q->param('print_base_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Base (Ma)</b>";
			push @header , '"Base (Ma)"';
		}
		if ( $q->param('print_midpoint_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Midpoint (Ma)</b>";
			push @header , '"Midpoint (Ma)"';
		}
		if ( $q->param('print_sampled') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Sampled<br>$generaorrefs</b>";
			push @header , '"Sampled '.$generaorrefs.'"';
		}
		if ( $q->param('print_range-through') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Range-through<br>$generaorrefs</b> ";
			push @header , '"Range-through '.$generaorrefs.'"';
		}
		if ( $q->param('print_boundary-crosser') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Boundary-crosser<br>$generaorrefs</b> ";
			push @header , '"Boundary-crosser '.$generaorrefs.'"';
		}
		if ( $q->param('print_first_appearances_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>First<br>appearances</b>";
			push @header , '"First appearances"';
		}
		if ( $q->param('print_last_appearances_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Last<br>appearances</b>";
			push @header , '"Last appearances"';
		}
		if ( $q->param('print_singletons_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Singletons</b> ";
			push @header , 'Singletons';
		}
		if ( $q->param('print_two_timers') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Two&nbsp;timer<br>$generaorrefs</b> ";
			push @header , '"Two timer '.$generaorrefs.'"';
		}
		if ( $q->param('print_three_timers') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three&nbsp;timer<br>$generaorrefs</b> ";
			push @header , '"Three timer '.$generaorrefs.'"';
		}
		if ( $q->param('print_part_timers') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Part&nbsp;timer<br>$generaorrefs</b> ";
			push @header , '"Part timer '.$generaorrefs.'"';
		}
		if ( $q->param('print_origination_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>3T origination<br>rate</b> ";
			push @header , '"3T origination rate"';
		}
		if ( $q->param('print_Foote_origination_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>BC origination<br>rate</b> ";
			push @header , '"BC origination rate"';
		}
		if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>3T extinction<br>rate</b> ";
			push @header , '"3T extinction rate"';
		}
		if ( $q->param('print_Foote_extinction_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>BC extinction<br>rate</b> ";
			push @header , '"BC extinction rate"';
		}
		if ( $q->param('print_one_occs') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>One-occurrence<br>$generaorrefs</b> ";
			push @header , '"One-occurrence '.$generaorrefs.'"';
		}
		if ( $q->param('print_one_refs') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>One-reference<br>$generaorrefs</b> ";
			push @header , '"One-reference '.$generaorrefs.'"';
		}
		if ( $q->param('print_u') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b><nobr>Good's <i>u</i></nobr></b> ";
			push @header , qq|"Good's u"|;
		}
		if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Gap analysis<br>sampling stat</b> ";
			push @header , '"Gap analysis sampling stat"';
		}
		if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Gap analysis<br>diversity estimate</b> ";
			push @header , '"Gap analysis diversity estimate"';
		}
		if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three timer<br>sampling stat</b> ";
			push @header , '"Three timer sampling stat"';
		}
		if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three timer<br>diversity estimate</b> ";
			push @header , '"Three timer diversity estimate"';
		}
		if ( $q->param('print_dominance') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Dominance</b> ";
			push @header , 'Dominance';
		}
		if ( $q->param('print_dominant') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Dominant<br>taxon</b> ";
			push @header , '"Dominant taxon"';
		}
		if ( $q->param('print_pie') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Evenness of<br>occurrences (PIE)</b> ";
			push @header , '"Evenness of occurrences (PIE)"';
		}
		if ( $q->param('print_chao-2_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Chao-2<br>estimate</b> ";
			push @header , '"Chao-2 estimate"';
		}
		if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Jolly-Seber<br>estimate</b> ";
			push @header , '"Jolly-Seber estimate"';
		}
		if ( $q->param('print_ice_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Incidence-based<br>coverage estimate</b> ";
			push @header , '"Incidence-based coverage estimate"';
		}
		if ( $q->param('print_refs_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>References</b> ";
			push @header , 'References';
		}
		if ( ( $q->param('samplesize') > 0 && $method ne "specimens" ) || $q->param('print_lists') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>$listorfm</b> ";
			push @header , $listorfm;
		}
		if ( $q->param('print_occurrences') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Occurrences</b> ";
			push @header , 'Occurrences';
		}
		if ( $q->param('print_occurrences-exponentiated') eq "YES" && $method ne "specimens" )	{
			print "<td class=tiny align=center valign=top><b>Occurrences<br><nobr>-exponentiated</nobr></b> ";
			push @header , '"Occurrences-exponentiated"';
		}
		if ( $method eq "specimens" || $q->param('print_specimens') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Specimens/<br>individuals</b> ";
			push @header , 'Specimens/individuals';
		}
		if ( $q->param('print_mean_richness') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Mean<br>richness</b>";
			push @header , '"Mean richness"';
		}
		if ( $q->param('print_median_richness') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Median<br>richness</b> ";
			push @header , '"Median richness"';
		}

		print TABLE join(',',@header),"\n";

	 # make sure all the files are read/writeable
#		chmod 0664, "$OUTPUT_DIR/*";
	
		for $i (1..$chrons)	{
			if ( $rangethrough[$i] > 0 || ( $listsinchron[$i] > 0 || ( $q->param('recent_genera') && $i == 1 ) ) )	{
				$gapstat = $richness[$i] - $originate[$i] - $extinct[$i] + $singletons[$i];
				if ($gapstat > 0)	{
					$gapstat = $gapstat / ( $rangethrough[$i] - $originate[$i] - $extinct[$i] + $singletons[$i] );
				}
				else	{
					$gapstat = "NA";
				}
				if ($threetimers[$i] + $parttimers[$i] > 0)	{
					$ttstat = $threetimers[$i] / ( $threetimers[$i] + $parttimers[$i] );
				}
				else	{
					$ttstat = "NA";
				}
				if ($chaom[$i] > 0)	{
					$chaostat = $richness[$i] + ($chaol[$i] * $chaol[$i] / (2 * $chaom[$i]));
				}
				else	{
					$chaostat = "NA";
				}

				$temp = $chname[$i];
				$temp =~ s/ /&nbsp;/;
				print "<tr><td class=tiny valign=top>$temp";
				my @data = ();
				push @data , sprintf "%d",$chrons - $i + 1;
				push @data , '"'.$chname[$i].'"';

				if ( $q->param('print_base_raw') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f",$basema{$chname[$i]};
					push @data , sprintf "%.1f",$basema{$chname[$i]};
				}
				if ( $q->param('print_midpoint_raw') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
					push @data , sprintf "%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
				}
				if ( $q->param('print_sampled') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						print "<td class=tiny align=center valign=top>$richness[$i] ";
						push @data , sprintf "%d",$richness[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_range-through') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$rangethrough[$i] ";
					push @data , sprintf "%d",$rangethrough[$i];
				}
		# compute boundary crossers
		# this is total range through diversity minus singletons minus
		#  first-appearing crossers into the next bin; the latter is
		#  originations - singletons, so the singletons cancel out and
		#  you get total diversity minus originations
				if ( $q->param('print_boundary-crosser') eq "YES" )	{
					$bcrich[$i] = $rangethrough[$i] - $originate[$i];
					printf "<td class=tiny align=center valign=top>%d ",$bcrich[$i];
					push @data , $bcrich[$i];
				}
				if ( $q->param('print_first_appearances_raw') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$originate[$i];
						push @data , sprintf "%d",$originate[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_last_appearances_raw') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$extinct[$i];
						push @data , sprintf "%d",$extinct[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_singletons_raw') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$singletons[$i];
						push @data , sprintf "%d",$singletons[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_two_timers') eq "YES" )	{
					if ( $richness[$i] > 0 && $richness[$i+1] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$twotimers[$i];
						push @data , $twotimers[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_three_timers') eq "YES" )	{
					if ( $richness[$i-1] > 0 && $richness[$i] > 0 && $richness[$i+1] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$threetimers[$i];
						push @data , $threetimers[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_part_timers') eq "YES" )	{
					if ( $richness[$i-1] > 0 && $richness[$i] > 0 && $richness[$i+1] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$parttimers[$i];
						push @data , $parttimers[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
			# 3T origination rate
				if ( $q->param('print_origination_rate_raw') eq "YES" )	{
					if ( $threetimers[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.3f ",log( $twotimers[$i-1] / $threetimers[$i] ) - log( $rawsum3timers / ( $rawsum3timers + $rawsumPtimers ) );
						push @data , sprintf "%.3f",log( $twotimers[$i-1] / $threetimers[$i] ) - log( $rawsum3timers / ( $rawsum3timers + $rawsumPtimers ) );
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
			# Foote origination rate - note: extinction counts must
			#  exclude singletons
				if ( $q->param('print_Foote_origination_rate_raw') eq "YES" )	{
					if ( $bcrich[$i-1] > 0 && $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.3f",log( $bcrich[$i-1] / ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) );
						push @data , sprintf "%.3f",log( $bcrich[$i-1] / ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) );
					} else	{
						print "<td class=tiny align=center valign=top>NA";
						push @data , 'NA';
					}
				}
			# 3T extinction rate
				if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
					if ( $threetimers[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.3f ",log( $twotimers[$i] / $threetimers[$i] ) - log( $rawsum3timers / ( $rawsum3timers + $rawsumPtimers ) );
						push @data , sprintf "%.3f",log( $twotimers[$i] / $threetimers[$i] ) - log( $rawsum3timers / ( $rawsum3timers + $rawsumPtimers ) );
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
			# Foote extinction rate
				if ( $q->param('print_Foote_extinction_rate_raw') eq "YES" )	{
					if ( $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 && $bcrich[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.3f",log( ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) / $bcrich[$i] ) * -1;
						push @data , sprintf "%.3f",log( ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) / $bcrich[$i] ) * -1;
					} else	{
						print "<td class=tiny align=center valign=top>NA";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_one_occs') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$q1[$i];
						push @data , $q1[$i];
					} else    {
				  		print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_one_refs') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$onerefs[$i];
						push @data , $onerefs[$i];
					} else    {
				  		print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_u') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.3f ",$u[$i];
						push @data , sprintf "%.3f",$u[$i];
					} else    {
				  		print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $gapstat > 0 )	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.3f ",$gapstat;
						push @data , sprintf "%.3f",$gapstat;
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$richness[$i] / $gapstat;
						push @data , sprintf "%.1f",$richness[$i] / $gapstat;
					}
				} else	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $ttstat > 0 )	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.3f ",$ttstat;
						push @data , sprintf "%.3f",$ttstat;
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$richness[$i] / $ttstat;
						push @data , sprintf "%.1f",$richness[$i] / $ttstat;
					}
				} else	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_dominance') eq "YES" )	{
					if ($maxoccs[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.4f ",$maxoccs[$i] / $occsinchron[$i];
						push @data , sprintf "%.4f",$maxoccs[$i] / $occsinchron[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_dominant') eq "YES" )	{
					if ($maxoccs[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%s ",$genus[$dominant[$i]];
						push @data , $genus[$dominant[$i]];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_pie') eq "YES" )	{
					if ($pie[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.5f ",$pie[$i];
						push @data , sprintf "%.5f",$pie[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_chao-2_raw') eq "YES" )	{
					if ($chaostat > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$chaostat;
						push @data , sprintf "%.1f",$chaostat;
					} else    {
				  		print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
					if ($jolly[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$jolly[$i];
						push @data , sprintf "%.1f",$jolly[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_ice_raw') eq "YES" )	{
					if ($ice[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$ice[$i];
						push @data , sprintf "%.1f",$ice[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_refs_raw') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%d ",$refsinchron[$i];
						push @data , $refsinchron[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( ( $q->param('samplesize') > 0 && $method ne "specimens" ) || $q->param('print_lists') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%d ",$listsinchron[$i];
					push @data , $listsinchron[$i];
				}
				if ( $q->param('print_occurrences') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						print "<td class=tiny align=center valign=top>$occsinchron[$i] ";
						push @data , $occsinchron[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_occurrences-exponentiated') eq "YES" && $method ne "specimens" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$occsinchron2[$i];
						push @data , sprintf "%.1f",$occsinchron2[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $method eq "specimens" || $q->param('print_specimens') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						print "<td class=tiny align=center valign=top>$specimensinchron[$i] ";
						push @data , $specimensinchron[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NA ";
						push @data , 'NA';
					}
				}
				if ( $q->param('print_mean_richness') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$occsinchron[$i]/$listsinchron[$i];
						push @data , sprintf "%.1f",$occsinchron[$i]/$listsinchron[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NA ";
						 push @data , 'NA';
					}
				}
				if ( $q->param('print_median_richness') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f ",$median[$i];
					push @data , sprintf "%.1f",$median[$i];
				}

				print "<p>\n";
				print TABLE join(',',@data),"\n";
			}
		}
		close TABLE;
		print qq|</table>
</div>

<div class="small" style="padding-left: 2em; padding-right: 2em;">
|;

		if ( $refsread == 0 )	{
			print "\n<b>$listsread</b> collections and <b>$occsread</b> occurrences met the search criteria.<p>\n";
		} else	{
			print "\n<b>$refsread</b> reference and interval combinations, <b>$listsread</b> collections, and <b>$occsread</b> occurrences met the search criteria.<p>\n";
		}

		if ( $rawsum3timers + $rawsumPtimers > 0 )	{
			printf "The average per-interval sampling probability based on three timer analysis of the raw data is <b>%.3f</b>.<p>\n",$rawsum3timers / ( $rawsum3timers + $rawsumPtimers );
		}

		print "\nThe following data files have been created:<p>\n";
		print "<ul>\n";
		print "<li>The above diversity curve data (<a href=\"$HOST_URL$PRINTED_DIR/raw_curve_data.csv\">raw_curve_data.csv</a>)<p>\n";
	
	
		print "<li>A first-by-last occurrence count matrix (<a href=\"$HOST_URL$PRINTED_DIR/firstlast.txt\">firstlast.txt</a>)<p>\n";
	
		print "<li>A list of each ".$q->param('taxonomic_level').", the number of collections including it,  and the ID number of the intervals in which it was found (<a href=\"$HOST_URL$PRINTED_DIR/presences.txt\">presences.txt</a>)<p>\n";

		print "</ul><p>\n";

		if ($q->param('time_scale') =~ /neptune/i) {
			$downloadForm = "displayDownloadNeptuneForm";
		}  
		print "\nYou may wish to <a href=\"$READ_URL?action=$downloadForm\">download another data set</a></b> before you run another analysis.<p>\n";
		print "</div>\n";

		print "</div>\n\n"; # END PANEL1 DIV

		if ($q->param('samplesize') ne '') {

			print '<div id="panel2" class="panel">';
			print qq|<div class="displayPanel" style="padding-top: 1em;">
<table cellpadding="4">
|;
			print SUB_TABLE "Bin,Bin name";

			print "<tr><td class=tiny valign=top><b>Interval</b>\n";
			if ( $q->param('print_base_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Base (Ma)</b> ";
				print SUB_TABLE ",Base (Ma)";
			}
			if ( $q->param('print_midpoint_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Midpoint (Ma)</b> ";
				print SUB_TABLE ",Midpoint (Ma)";
			}
			if ( $q->param('print_items') eq "YES" )	{
				my $items = "Collections";
				if ( $method =~ /OW|CR/ )	{
					$items = "Occurrences";
				} elsif ( $method eq "OXW" )	{
					$items = "Occurrences^".$q->param('exponent');
				} elsif ( $method eq "specimens" )	{
					$items = "Specimens";
				}
				print "<td class=tiny align=center valign=top><b>$items<br>sampled</b> ";
				print SUB_TABLE ",$items sampled";
			}
			if ( $q->param('print_refs_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>References<br>sampled</b> ";
				print SUB_TABLE ",References sampled";
			}
			if ( $q->param('print_median') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Median sampled diversity</b>";
				print SUB_TABLE ",Median sampled diversity";
			}
			if ( $q->param('print_ci') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>1-sigma CI</b> ";
				print SUB_TABLE ",1-sigma lower CI,1-sigma upper CI";
			}
			if ( $q->param('print_mean') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Mean sampled diversity</b>";
				print SUB_TABLE ",Mean sampled diversity";
			}
			if ( $q->param('print_range-through_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Range-through diversity</b>";
				print SUB_TABLE ",Range-through diversity";
			}
			if ( $q->param('print_boundary-crosser_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Boundary-crosser diversity</b>";
				print SUB_TABLE ",Boundary-crosser diversity";
			}
			if ( $q->param('print_corrected_bc') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Corrected BC<br>diversity</b> ";
				print SUB_TABLE ",Corrected BC diversity";
			}
			if ( $q->param('print_estimated_midpoint') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Estimated midpoint<br>diversity</b> ";
				print SUB_TABLE ",Estimated midpoint diversity";
			}
			if ( $q->param('print_estimated_mesa') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Estimated mesa<br>diversity</b> ";
				print SUB_TABLE ",Estimated mesa diversity";
			}
			if ( $q->param('print_corrected_sib') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Corrected SIB<br>diversity</b> ";
				print SUB_TABLE ",Corrected SIB diversity";
			}

			if ( $q->param('print_first_appearances_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>First<br>appearances</b> ";
				print SUB_TABLE ",First appearances";
			}
			if ( $q->param('print_last_appearances_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Last<br>appearances</b> ";
				print SUB_TABLE ",Last appearances";
			}
			if ( $q->param('print_singletons_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Singletons</b> ";
				print SUB_TABLE ",Singletons";
			}
			if ( $q->param('print_two_timers_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Two timers</b> ";
				print SUB_TABLE ",Two timers";
			}
			if ( $q->param('print_three_timers_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Three timers</b> ";
				print SUB_TABLE ",Three timers";
			}
			if ( $q->param('print_part_timers_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Part timers</b> ";
				print SUB_TABLE ",Part timers";
			}
			if ( $q->param('print_origination_rate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>3T origination<br>rate</b> ";
				print SUB_TABLE ",3T origination rate";
			}

			if ( $q->param('print_Foote_origination_rate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>BC origination<br>rate</b> ";
				print SUB_TABLE ",BC origination rate";
			}
			if ( $q->param('print_origination_percentage') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>RT origination<br>percentage</b> ";
				print SUB_TABLE ",RT origination percentage";
			}
			if ( $q->param('print_Foote_origination_percentage') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>BC origination<br>percentage</b> ";
				print SUB_TABLE ",BC origination percentage";
			}

			if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>3T extinction<br>rate</b> ";
				print SUB_TABLE ",3T extinction rate";
			}
			if ( $q->param('print_Foote_extinction_rate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>BC extinction<br>rate</b> ";
				print SUB_TABLE ",BC extinction rate";
			}
			if ( $q->param('print_extinction_percentage') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>RT extinction<br>percentage</b> ";
				print SUB_TABLE ",RT extinction percentage";
			}
			if ( $q->param('print_Foote_extinction_percentage') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>BC extinction<br>percentage</b> ";
				print SUB_TABLE ",BC extinction percentage";
			}

			if ( $q->param('print_one_occs_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>One occurrence $generaorrefs</b> ";
				print SUB_TABLE '"One occurrence '.$generaorrefs.'"';
			}
			if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Gap analysis<br>sampling stat</b> ";
				print SUB_TABLE ",Gap analysis sampling stat";
			}
			if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Gap analysis<br>diversity estimate</b> ";
				print SUB_TABLE ",Gap analysis diversity estimate";
			}
			if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Three timer<br>sampling stat</b> ";
				print SUB_TABLE ",Three timer sampling stat";
			}
			if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Three timer<br>diversity estimate</b> ";
				print SUB_TABLE ",Three timer diversity estimate";
			}
			if ( $q->param('print_chao-2_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Chao-2<br>estimate</b> ";
				print SUB_TABLE ",Chao-2 estimate";
			}
			if ( $q->param('print_jolly-seber_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Jolly-Seber<br>estimate</b> ";
				print SUB_TABLE ",Jolly-Seber estimate";
			}
			if ( $q->param('print_ice_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Incidence-based<br>coverage estimate</b> ";
				print SUB_TABLE ",Incidence-based coverage estimate";
			}
			if ( $method eq "UW" && $q->param('print_michaelis-menten') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Michaelis-Menten<br>estimate</b> ";
				print SUB_TABLE ",Michaelis-Menten estimate";
			}

			print SUB_TABLE "\n";

			for ($i = 1; $i <= $chrons; $i++)     {
				if ($rangethrough[$i] > 0)  {
					$gapstat = $meanrichness{'SIB'}[$i] - $msubsoriginate[$i] - $msubsextinct[$i] + $msubssingletons[$i];
					if ($gapstat > 0)	{
						$gapstat = $gapstat / ( $msubsrangethrough[$i] - $msubsoriginate[$i] - $msubsextinct[$i] + $msubssingletons[$i] );
					}
					else	{
						$gapstat = "NA";
					}
					if ( $mthreetimers[$i] + $mparttimers[$i] > 0 )	{
						$ttstat = $mthreetimers[$i] / ( $mthreetimers[$i] + $mparttimers[$i] );
					} else	{
				  		$ttstat = "NA";
					}
					if ($msubschaom[$i] > 0)	{
						$msubschaostat = $meanrichness{'SIB'}[$i] + ($msubschaol[$i] * $msubschaol[$i] / (2 * $msubschaom[$i]));
					}
					else	{
						$msubschaostat = "NA";
					}

					$temp = $chname[$i];
					$temp =~ s/ /&nbsp;/;
					print "<tr><td class=tiny valign=top>$temp";
					print SUB_TABLE $chrons - $i + 1;
					print SUB_TABLE ",$chname[$i]";

					if ( $q->param('print_base_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$basema{$chname[$i]};
						printf SUB_TABLE ",%.1f",$basema{$chname[$i]};
					}
					if ( $q->param('print_midpoint_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
						printf SUB_TABLE ",%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
					}
					if ( $q->param('print_items') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$tsampled[$i];
						printf SUB_TABLE ",%.1f",$tsampled[$i];
					}
					if ( $q->param('print_refs_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubsrefrichness[$i];
						printf SUB_TABLE ",%.1f",$msubsrefrichness[$i];
					}
					my $trials = $q->param('samplingtrials');
					if ( $q->param('print_median') eq "YES" )	{
						if ( $tsampled[$i] > 0 )	{
							my $s = int(0.5*$trials)+1;
							print "<td class=tiny align=center valign=top>$outrichness[$i][$s] ";
							print SUB_TABLE ",$outrichness[$i][$s]";
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_ci') eq "YES" )	{
						if ( $tsampled[$i] > 0 )	{
							my $qq = int(0.1587*$trials)+1;
							my $r = int(0.8413*$trials)+1;
							printf "<td class=tiny align=center valign=top><nobr>$outrichness[$i][$qq] - $outrichness[$i][$r]</nobr> ";
							print SUB_TABLE ",$outrichness[$i][$qq]";
							print SUB_TABLE ",$outrichness[$i][$r]";
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_mean') eq "YES" )	{
						if ( $tsampled[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$meanrichness{'SIB'}[$i];
							printf SUB_TABLE ",%.1f",$meanrichness{'SIB'}[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}

					if ( $q->param('print_range-through_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$meanrichness{'RT'}[$i];
						printf SUB_TABLE ",%.1f",$meanrichness{'RT'}[$i];
					}
					if ( $q->param('print_boundary-crosser_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$meanrichness{'BC'}[$i];
						printf SUB_TABLE ",%.1f",$meanrichness{'BC'}[$i];
					}

		# print assorted stats yielded by two timer analysis JA 23.8.04
					if ( $q->param('print_corrected_bc') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$mnewbc[$i];
						printf SUB_TABLE ",%.1f",$mnewbc[$i];
					}
					if ( $q->param('print_estimated_midpoint') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$mmidptdiv[$i];
						printf SUB_TABLE ",%.1f",$mmidptdiv[$i];
					}
					if ( $q->param('print_estimated_mesa') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$mmesa[$i];
						printf SUB_TABLE ",%.1f",$mmesa[$i];
					}
					if ( $q->param('print_corrected_sib') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$mnewsib[$i];
						printf SUB_TABLE ",%.1f",$mnewsib[$i];
					}

					if ( $q->param('print_first_appearances_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubsoriginate[$i];
						printf SUB_TABLE ",%.1f",$msubsoriginate[$i];
					}
					if ( $q->param('print_last_appearances_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubsextinct[$i];
						printf SUB_TABLE ",%.1f",$msubsextinct[$i];
					}
					if ( $q->param('print_singletons_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubssingletons[$i];
						printf SUB_TABLE ",%.1f",$msubssingletons[$i];
					}
					if ( $q->param('print_two_timers_ss') eq "YES" )	{
						if ( $meanrichness{'SIB'}[$i] > 0 && $meanrichness{'SIB'}[$i+1] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mtwotimers[$i];
							printf SUB_TABLE ",%.1f",$mtwotimers[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_three_timers_ss') eq "YES" )	{
						if ( $meanrichness{'SIB'}[$i-1] > 0 && $meanrichness{'SIB'}[$i] > 0 && $meanrichness{'SIB'}[$i+1] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mthreetimers[$i];
							printf SUB_TABLE ",%.1f",$mthreetimers[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_part_timers_ss') eq "YES" )	{
						if ( $meanrichness{'SIB'}[$i-1] > 0 && $meanrichness{'SIB'}[$i] > 0 && $meanrichness{'SIB'}[$i+1] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mparttimers[$i];
							printf SUB_TABLE ",%.1f",$mparttimers[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_one_occs_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubsq1[$i];
						printf SUB_TABLE ",%.1f",$msubsq1[$i];
					}

					if ( $q->param('print_origination_rate_ss') eq "YES" )	{
						if ( $mtwotimers[$i-1] > 0 && $mthreetimers[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$lam[$i];
							printf SUB_TABLE ",%.3f",$lam[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_Foote_origination_rate_ss') eq "YES" )	{
						if ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] > 0 && $meanrichness{'BC'}[$i-1] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.3f ",log( $meanrichness{'BC'}[$i-1] / ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] ) );
							printf SUB_TABLE ",%.3f",log( $meanrichness{'BC'}[$i-1] / ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] ) );
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_origination_percentage') eq "YES" )	{
						if ( $meanrichness{'SIB'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsoriginate[$i] / $meanrichness{'SIB'}[$i] * 100;
							printf SUB_TABLE ",%.1f",$msubsoriginate[$i] / $meanrichness{'SIB'}[$i] * 100;
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_Foote_origination_percentage') eq "YES" )	{
						if ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",( 1 - ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i-1] ) * 100;
							printf SUB_TABLE ",%.1f",( 1 - ( $meanrichness{'BC'}[$i-1] - $msubsoriginate[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i-1] ) * 100;
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}

					if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
						if ( $mthreetimers[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$mu[$i];
							printf SUB_TABLE ",%.3f",$mu[$i];
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_Foote_extinction_rate_ss') eq "YES" )	{
						if ( $meanrichness{'BC'}[$i] - $msubsextinct[$i] + $msubssingletons[$i] > 0 && $meanrichness{'BC'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.3f ",log( ( $meanrichness{'BC'}[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i] ) * -1;
							printf SUB_TABLE ",%.3f",log( ( $meanrichness{'BC'}[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i] ) * -1;
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_extinction_percentage') eq "YES" )	{
						if ( $meanrichness{'SIB'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsextinct[$i] / $meanrichness{'SIB'}[$i] * 100;
							printf SUB_TABLE ",%.1f",$msubsextinct[$i] / $meanrichness{'SIB'}[$i] * 100;
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_Foote_extinction_percentage') eq "YES" )	{
						if ( $meanrichness{'BC'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",( 1 - ( $meanrichness{'BC'}[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i] ) * 100;
							printf SUB_TABLE ",%.1f",( 1 - ( $meanrichness{'BC'}[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanrichness{'BC'}[$i] ) * 100;
						} else	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}

					if ($gapstat > 0)	{
						if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$gapstat;
							printf SUB_TABLE ",%.3f",$gapstat;
						}
						if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$meanrichness{'SIB'}[$i] / $gapstat;
							printf SUB_TABLE ",%.3f",$meanrichness{'SIB'}[$i] / $gapstat;
						}
					} else	{
						if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
						if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ($ttstat > 0)	{
						if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$ttstat;
							printf SUB_TABLE ",%.3f",$ttstat;
						}
						if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mnewsib[$i] / $ttstat;
							printf SUB_TABLE ",%.3f",$mnewsib[$i] / $ttstat;
						}
					} else	{
						if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
						if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_chao-2_ss') eq "YES" )	{
						if ($msubschaostat > 0 && $meanrichness{'SIB'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubschaostat;
							printf SUB_TABLE ",%.1f",$msubschaostat;
						} else    {
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_jolly-seber_ss') eq "YES" )	{
						if ($msubsjolly[$i] > 0 && $meanrichness{'SIB'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsjolly[$i];
							printf SUB_TABLE ",%.1f",$msubsjolly[$i];
						} else    {
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_ice_ss') eq "YES" )	{
						if ($msubsice[$i] > 0 && $meanrichness{'SIB'}[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsice[$i];
							printf SUB_TABLE ",%.1f",$msubsice[$i];
						} else    {
							print "<td class=tiny align=center valign=top>NA ";
							print SUB_TABLE ",NA";
						}
					}
					if ( $q->param('print_michaelis-menten') eq "YES" )	{
						if ( $method eq "UW" )	{
							if ($michaelis[$i] > 0 && $meanrichness{'SIB'}[$i] > 0 )	{
					  			printf "<td class=tiny align=center valign=top>%.1f ",$michaelis[$i];
					  			printf SUB_TABLE ",%.1d",$michaelis[$i];
							}
							else    {
								print "<td class=tiny align=center valign=top>NA ";
								print SUB_TABLE ",NA";
							}
						}
					}
					print "<p>\n";
					print SUB_TABLE "\n";
				}
			}
			close SUB_TABLE;
			print qq|</table>
</div>

<div class="small" style="padding-left: 2em; padding-right: 2em;">
|;
			print "The subsampling method was <b>".$q->param('samplingmethod')."</b>.<p>\n";
			if ( $method ne "SQS" )	{
				print "<p>The sampling quota per time interval was <b>".$q->param('samplesize')."</b>.</p>\n";
			} else	{
				print "<p>The sampling quorum per time interval was <b>".$q->param('samplesize')."</b>.</p>\n";
				if ( $q->param('time_scale') =~ /neptune/i )	{
					print "<p>Good's <i>u</i> was based on counts of one-occurrence taxa because Neptune data files do not include a reference field.</p>\n";
				}
			}
			print "The total number of trials was <b>".$q->param('samplingtrials')."</b>.<p>\n";
			if ( $threetimerp )	{
				printf "The average per-interval sampling probability based on three timer analysis of the subsampled data is <b>%.3f</b>.<p>\n",$threetimerp;
			}
			if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
				print "Corrected SIB, not raw SIB, was used for the three timer diversity estimate.<p>\n";
			}
			if ($q->param('stepsize') ne "" && $method ne "SQS")	{
				print "\n<p>The following data files have been created:</p>\n";
				print "<ul>\n";
				print "<li>The subsampled diversity curve data (<a href=\"$HOST_URL$PRINTED_DIR/subsampled_curve_data.csv\">subsampled_curve_data.csv</a>)<p>\n";
				print "<li>The subsampling curves (<a href=\"$HOST_URL$PRINTED_DIR/subcurve.tab\">subcurve.tab</a>)<p>\n";
				print "</ul>\n";
			} else	{
				print "<p>You may download the <a href=\"$HOST_URL$PRINTED_DIR/subsampled_curve_data.csv\">subsampled diversity curve data.</a></p>\n";
			}
			print "</div>\n\n";
			print "</div>\n\n"; # End PANEL2 div
		}

	# print a very simple graph JA 8.6.09
		if ($q->param('samplesize') ne '')	{
			print "<div id=\"panel3\" class=\"panel\">\n\n";
		} else	{
			print "<div id=\"panel2\" class=\"panel\">\n\n";
		}
		print "<div class=\"displayPanel\" style=\"padding-top: 1em;\">\n\n";
		my ($width,$height) = (680,520);
		$im = new GD::Image($width,$height,1);
		my %rgbs = ("white", "255,255,255",
			"black", "0,0,0",
			"gray", "128,128,128",
			"lightgray", "192,192,192",
			"red", "255,0,0",
			"pink", "223,0,223",
			"purple", "128,0,223",
			"blue", "0,0,255",
			"green", "0,128,0",
			"orange", "255,165,0",
			"yellow", "255,255,0");
		my %col;
		for my $color ( keys %rgbs )        {
			my ($r,$g,$b) = split /,/,$rgbs{$color};
			$col{$color} = $im->colorClosest($r,$g,$b);
		}
		$col{'unantialiased'} = $im->colorAllocate(-1,-1,-1);
		$im->interlaced('true');
		my $transparent = $im->colorAllocate(11, 22, 33);
		$im->filledRectangle(0,0,$width,$height,$transparent);
		$im->transparent($transparent);
		my ($pwidth,$pheight,$pleft,$ptop,$maxdiv,$maxma) = (600,420,60,70,0,0);
		$im->rectangle( $pleft, $height - $ptop, $pwidth + $pleft, $height - $ptop - $pheight, $col{'black'} );
		my $FONT = "$DATA_DIR/fonts/Vera.ttf";

		my @vars = $q->param('variable');
		my @colors = $q->param('linecolor');
		my @scalings = $q->param('linescale');
		my $withcolors = 0;
		my @count;
		my @linekey;
		for my $v ( 0..$#vars )	{
			if ( $colors[$v] )	{
				$withcolors++;
				my $var = $vars[$v];
				my $scale = $scalings[$v];
				$scale =~ s/[A-Za-z ]//g;
				if ( $scale > 1 )	{
					$var .= " ".$scalings[$v];
				}
				push @linekey , "$var: $colors[$v]";
				for my $i ( 1..$chrons )     {
					if ( $vars[$v] =~ /sampled diversity/i && $q->param('samplesize') eq "" )	{
						$count[$v][$i] = $richness[$i];
					} elsif ( $vars[$v] =~ /sampled diversity/i )	{
						$count[$v][$i] = $meanrichness{'SIB'}[$i];
					} elsif ( $vars[$v] =~ /range.through/i && $q->param('samplesize') eq "" )	{
						$count[$v][$i] = $rangethrough[$i];
					} elsif ( $vars[$v] =~ /range.through/i )	{
						$count[$v][$i] = $meanrichness{'RT'}[$i];
					} elsif ( $vars[$v] =~ /boundary.crosser/i && $q->param('samplesize') eq "" )	{
						$count[$v][$i] = $bcrich[$i];
					} elsif ( $vars[$v] =~ /boundary.crosser/i )	{
						$count[$v][$i] = $meanrichness{'BC'}[$i];
					} elsif ( $vars[$v] =~ /first.appear/i && $q->param('samplesize') eq "" )	{
						$count[$v][$i] = $originate[$i];
					} elsif ( $vars[$v] =~ /first.appear/i )	{
						$count[$v][$i] = $msubsoriginate[$i];
					} elsif ( $vars[$v] =~ /last.appear/i && $q->param('samplesize') eq "" )	{
						$count[$v][$i] = $extinct[$i];
					} elsif ( $vars[$v] =~ /last.appear/i )	{
						$count[$v][$i] = $msubsextinct[$i];
					} elsif ( $vars[$v] =~ /singletons/i )	{
						$count[$v][$i] = $singletons[$i];
					} elsif ( $vars[$v] =~ /dominance/i )	{
						if ( $occsinchron[$i] > 0 )	{
							$count[$v][$i] = $maxoccs[$i] / $occsinchron[$i];
						} else	{
							$count[$v][$i] = 0;
						}
					} elsif ( $vars[$v] =~ /collections/i )	{
						$count[$v][$i] = $listsinchron[$i];
					} elsif ( $vars[$v] =~ /occurrences/i )	{
						$count[$v][$i] = $occsinchron[$i];
					}
					if ( $scale > 1 )	{
						$count[$v][$i] *= $scale;
					}
					if ( ! $count[$v][$i] )	{
						$count[$v][$i] = 0;
					}
					if ( $count[$v][$i] > 0 && $basema{$chname[$i+1]} > 0 && $basema{$chname[$i+1]} > $maxma )	{
						$maxma = $basema{$chname[$i+1]};
					}
					if ( $count[$v][$i] > $maxdiv )	{
						$maxdiv = $count[$v][$i];
					}
				}
			}
		}

		if ( $maxdiv <= 100 )	{
			$maxdiv = int( $maxdiv / 10 ) * 10 + 10;
		} elsif ( $maxdiv <= 200 )	{
			$maxdiv = int( $maxdiv / 20 ) * 20 + 20;
		} elsif ( $maxdiv <= 500 )	{
			$maxdiv = int( $maxdiv / 50 ) * 50 + 50;
		} else	{
			$maxdiv = int( $maxdiv / 100 ) * 100 + 100;
		}

		$maxma = int( $maxma / 10 ) * 10 + 10;
		my %bases = ( "Ng" => 23.03, "Pg" => 65.5, "K" => 145.5, "J" => 199.6, "Tr" => 251, "P" => 299, "C" => 360.7, "D" => 418.1, "S" => 443.7, "O" => 490, "Cm" => 542);
		my $suppress;
		for my $ma ( values %bases )	{
			if ( $ma < $maxma && $ma + 30 > $maxma )	{
				$suppress++;
			}
		}

		$im->stringFT($col{'unantialiased'},$FONT,10,0,$pwidth + $pleft - 4,$height - $ptop + 18,'0');
		if ( ! $suppress )	{
			$im->stringFT($col{'unantialiased'},$FONT,10,0,$pleft - 12,$height - $ptop + 18,sprintf("%d",$maxma));
		}
		$im->stringFT($col{'unantialiased'},$FONT,14,3.142857/2,$pleft - 30,$pheight / 2 + $ptop,'Count');
		$im->stringFT($col{'unantialiased'},$FONT,14,0,$pwidth / 2 + $pleft - 50,$height - $ptop + 50,'Years ago (Ma)');
		if ( $maxdiv < 100 )	{
			$im->stringFT($col{'unantialiased'},$FONT,10,0,$pleft - 26,$height - $pheight - $ptop + 6,sprintf("%d",$maxdiv));
		} else	{
			$im->stringFT($col{'unantialiased'},$FONT,10,0,$pleft - 32,$height - $pheight - $ptop + 6,sprintf("%d",$maxdiv));
		}
		my $xscale = -1 * $pwidth / $maxma;
		my $yscale = $pheight / $maxdiv;
		for my $period ( keys %bases )	{
			my $ma = $bases{$period};
			if ( $maxma > $ma )	{
				$im->line( $ma * $xscale + $pleft + $pwidth, $height - ( $maxdiv * $yscale + $ptop ), $ma * $xscale + $pleft + $pwidth, $height - $ptop, $col{'lightgray'} );
				$im->stringFT($col{'unantialiased'},$FONT,10,0,$ma * $xscale + $pleft + $pwidth + 12,50,$period);
				$im->stringFT($col{'unantialiased'},$FONT,10,0,$ma * $xscale + $pleft + $pwidth - 16,$height - $ptop + 18,$ma);
			}
		}
		for my $v ( 0..$#vars )	{
			if ( $colors[$v] )	{
				for my $i ( 1..$chrons )     {
					my ($div1,$div2) = ($count[$v][$i],$count[$v][$i+1]);
					if ( $div1 > 0 )	{
						my $x1 = $xscale * ( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2 + $pleft + $pwidth;
						my $x2 = $xscale * ( $basema{$chname[$i+1]} + $basema{$chname[$i]} ) / 2 + $pleft + $pwidth;
						my $y1 = $div1 * $yscale + $ptop;
						my $y2 = $div2 * $yscale + $ptop;
						if ( $div1 > 0 && $div2 > 0 )	{
$im->setThickness(2);
							$im->line( $x1, $height - $y1, $x2, $height - $y2, $col{$colors[$v]} );
$im->setThickness(1);
						}
						if ( $q->param('points_and_lines') =~ /points/i )	{
							$im->filledEllipse( $x1, $height - $y1, 8, 8, $col{$colors[$v]} );
						}
					}
				}
			}
		}

		my $pngfile = "curve".int(rand(1000)).".png";
		open PNG_OUT,">$HTML_DIR/public/curve/$pngfile";
		print PNG_OUT $im->png;
		close PNG_OUT;
		print "<center><img alt=\"diversity curve\" src=\"/public/curve/$pngfile\"></center>\n";
		print "<center><p>".join('; ',@linekey)."</p>";
	# correlations between illustrated counts with bootstrapped p-values
		$| = 1;
		if ( $withcolors > 1 && $q->param('correlate') =~ /y/i )	{
		print "<table cellpadding=4>\n";
		print "<tr><td colspan=6>Correlations between logged counts:</td></tr>\n";
		print "<tr><td style=\"border-bottom: 1px solid black;\">count 1</td>\n<td style=\"border-bottom: 1px solid black;\">count 2</td>\n<td style=\"border-bottom: 1px solid black;\">N</td>\n<td style=\"border-bottom: 1px solid black;\">&nbsp;<i>r</i></td>\n<td style=\"border-bottom: 1px solid black;\"><i>p</i>-value</td><td style=\"border-bottom: 1px solid black;\">N<sup>*</sup></td>\n<td style=\"border-bottom: 1px solid black;\">&nbsp;<i>r<sup>*</sup></i></td>\n<td style=\"border-bottom: 1px solid black;\"><i>p</i>-value<sup>*</sup></td></tr>\n";
		for my $v ( 1..$#vars )	{
			for my $w ( 0..$v-1 )	{
				if ( $colors[$v] && $colors[$w] )	{
					print "<tr>\n<td>$vars[$w]</td>\n<td>$vars[$v]</td>\n";
					my $real;
					my @v1 = ();
					my @v2 = ();
					for my $i ( 1..$chrons )     {
						if ( $count[$v][$i] > 0 && $count[$w][$i] > 0 )	{
							push @v1 , log( $count[$v][$i] );
							push @v2 , log( $count[$w][$i] );
						}
					}
					# three data points required
					if ( $#v1 < 2 )	{
						next;
					}
					$real = correlate(\@v1,\@v2);
					printf "<td>%d</td>\n",$#v1+1;
					if ( $real eq "NA" )	{
						print "<td>NA</td><td>NA</td><td>NA</td><td>NA</td><td>NA</td>\n";
						next;
					}
					my $up = 0;
					my $trials = 100000;
					for my $t ( 1..$trials )	{
						my @boot1 = ();
						my @boot2 = ();
						for my $i ( 0..$#v1 )	{
							push @boot1 , $v1[int(rand($#v1+1))];
							push @boot2 , $v2[int(rand($#v2+1))];
						}
						if ( correlate(\@boot1,\@boot2 ) >= $real )	{
							$up++;
						}
					}
					if ( $up > $trials / 2 )	{
						$up = $trials - $up;
					}
					printf "<td>%.3f</td>\n",$real;
					my $pvalue = sprintf "%.3f",$up / $trials;
					if ( $pvalue == 0 )	{
						$pvalue = "< 0.001";
					}
					print "<td>$pvalue</td>\n";
					my @diff1 = ();
					my @diff2 = ();
				# differences only can be computed when neighboring
				#  intervals have non-zero counts
					for my $i ( 1..$chrons )     {
						if ( $count[$v][$i] > 0 && $count[$w][$i] > 0 && $count[$v][$i+1] > 0 && $count[$w][$i+1] > 0 )	{
							push @diff1 , $count[$v][$i] - $count[$v][$i+1];
							push @diff2 , $count[$w][$i] - $count[$w][$i+1];
						}
					}
					$real2 = correlate(\@diff1,\@diff2);
					$up = 0;
					for my $t ( 1..$trials )	{
						my @boot1 = ();
						my @boot2 = ();
						for my $i ( 0..$#diff1 )	{
							push @boot1 , $diff1[int(rand($#diff1+1))];
							push @boot2 , $diff2[int(rand($#diff2+1))];
						}
						if ( correlate(\@boot1,\@boot2 ) >= $real2 )	{
							$up++;
						}
					}
					if ( $up > $trials / 2 )	{
						$up = $trials - $up;
					}
					printf "<td>%d</td>\n",$#diff1+1;
					printf "<td>%.3f</td>\n",$real2;
					$pvalue = sprintf "%.3f",$up / $trials;
					if ( $pvalue == 0 )	{
						$pvalue = "< 0.001";
					}
					print "<td>$pvalue</td>\n";
					print "</tr>\n";
				}
			}
		}
		print "<tr><td colspan=6><span class=\"verysmall\">&nbsp;&nbsp;<sup>*</sup> after taking first differences of the counts</span></td></tr>\n";
		print "</table>\n</center>\n";
		}

		sub correlate	{
			my ($v1ref,$v2ref) = @_;
			my @v1 = @$v1ref;
			my @v2 = @$v2ref;
			my ($matches,$mean1,$mean2,$ssq1,$ssq2,$cov) = (0,0,0,0,0,0);
			for my $i ( 0..$#v1 )	{
				$mean1 += $v1[$i];
				$mean2 += $v2[$i];
				$matches++;
			}
			$mean1 /= $matches;
			$mean2 /= $matches;
			for my $i ( 0..$#v1 )	{
				$ssq1 += ( $v1[$i] - $mean1 )**2;
				$ssq2 += ( $v2[$i] - $mean2 )**2;
				$cov += ( $v1[$i] - $mean1 ) * ( $v2[$i] - $mean2 );
			}
			my $cor = 0;
			if ( $ssq1 > 0 && $ssq2 > 0 )	{
				$cor = $cov / sqrt( $ssq1 * $ssq2 );
				return( $cor );
			} else	{
				return( "NA" );
			}
		}
		print "</div>\n";
		print "</div>\n\n"; # End PANEL3 div
		print qq|<script language="JavaScript" type="text/javascript">
switchToPanel(1,$panels);
</script>
|;

	}

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


# historical notes

# pmpd-curve.cgi
# Written by John Alroy 25.3.99
# updated to handle new database structure 31.3.99 JA
# minor bug fixes 7.4.99, 30.6.99, 5.7.99 JA
# can use arbitrary interval definitions given in timescale.10my file;
#   searches may be restricted to a single class 28.6.99 JA
# three major subsampling algorithms 7.7.99 JA
# gap analysis and Chao-2 diversity estimators 8.7.99 JA
# analyses restricted by lithologies; regions set by checkboxes
#   instead of pulldown; Early/Middle/Late fields interpreted 27.7.99 JA
# more genus field cleanups 29-30.7.99; 7.8.99
# occurrences-squared algorithm; quota blowout prevention rule 9.8.99
# first-by-last occurrence count matrix output 14.8.99
# faster genus cleanups 15.8.99
# prints presence-absence data to presences.txt; prints
#   occurences-squared in basic table 16.8.99
# prints "orphan" collections 18.8.99
# prints number of records per genus to presences.txt; country field
#   clean up 20.8.99
# prints gap analysis completeness stat; geographic dispersal algorithm
#   31.8.99
# prints mean and median richness 2.9.99
# genus occurrence upper or lower cutoff 19.9.99
# prints confidence intervals around subsampled richness 16.12.99
# prints median richness instead of mean 18.12.99
# U.S.A. bug fix 15.1.00
# allows local stages to span multiple international stages (see instances
#   of "maxbelongsto" variable) 17.3.00
# restricts data by paleoenvironmental zone (5-way scheme of Holland);
#   prints full subsampling curves to CURVES 20-23.3.00
# linear interpolation of number of taxa added by a subsampling draw;
#   may print stats for intervals with less items than the quota 24.3.00 JA
# bug fix in linear interpolation 26.3.00 JA
# prints counts of occurrences per bin for each genus to presences.txt
#   10.4.00 JA
# more efficient stage/epoch binning 13-14.8.00 JA
# reads occs file only once; lumps by formation; lumps multiple occurrences
#   of the same genus in a list 14.8.00 JA
# "combine" lithology field searches 17-18.8.00 JA
# quotas always based on lists (instead of other units) 19.8.00 JA
# reads Sepkoski time scale; rewritten Harland epoch input routine 26.8.00 JA
# may report CIs for any of five diversity counts 17.9.00 JA
# prints country of each list to binning.csv 19.9.00 JA
# reads stages time scale; may require formation and/or member data 11.3.01 JA
# orders may be required or allowed 22.3.01
# prints data to presences.txt in matrix form 25.9.01 JA
# may use data from only one research group 24.2.02 JA
# prints names of enterers responsible for bad stage names 14.3.02 JA
# restricts by narrowest allowable strat scale instead of just broadest
#   15.3.02 JA
# integrated into bridge.pl 7.6.02 Garot 
# debugging related to above 21.6.02 JA
# column headers printed to presences.txt 24.6.02 JA
# major rewrite to use data output by Download.pm instead of vetting all
#  the data in this program 30.6.04 JA
# to do this, the following subroutines were eliminated:
#  readRegions
#  readOneClassFile
#  readClassFiles
#  readScale
#  assignLocs

1;
