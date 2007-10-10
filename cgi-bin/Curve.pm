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

package Curve;

use TimeLookup;
use Text::CSV_XS;
use PBDBUtil;
use TaxaCache;
use Constants qw($READ_URL $DATA_DIR $HTML_DIR);

# FOO ##  # CGI::Carp qw(fatalsToBrowser);

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;
my $q;					# Reference to the parameters
my $s;
my $dbt;

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

	$self->setArrays;
    PBDBUtil::autoCreateDir($OUTPUT_DIR);
	# compute the sizes of intermediate steps to be reported in subsampling curves
	if ($q->param('stepsize') ne "")	{
	  $self->setSteps;
	}
	if ( $q->param('recent_genera') =~ /yes/i )	{
		$self->findRecentGenera;
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
	if ( $s->get('enterer') ne "Guest" || $q->param('yourname') ne "" )	{
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

	if ($q->param('samplingmethod') eq "classical rarefaction")	{
		$samplingmethod = 1;
	}
	elsif ($q->param('samplingmethod') eq "by list (unweighted)")	{
		$samplingmethod = 2;
	}
	elsif ($q->param('samplingmethod') eq "by list (occurrences weighted)")	{
		$samplingmethod = 3;
	}
# replaced occurrences-squared with occurrences-exponentiated 27.10.05
	elsif ($q->param('samplingmethod') eq "by list (occurrences-exponentiated weighted)")	{
		$samplingmethod = 4;
	}
	elsif ($q->param('samplingmethod') eq "by specimen")	{
		$samplingmethod = 5;
	}
	$exponent = $q->param('exponent');

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

    $csv = Text::CSV_XS->new({
        'quote_char'  => '"',
        'escape_char' => '"',
        'sep_char'    => $sepChar,
        'binary'      => 1
    });


	if ( ! open OCCS,"<$occsfile" )	{
        if ($q->param("time_scale") =~ /neptune/i) {
		    print "<p class=\"warning\">The data can't be analyzed because you haven't yet downloaded a data file of occurrences with sample age data. <a href=\"$READ_URL?action=displayDownloadNeptuneForm\">Download the data again</a> and make sure to check off this field in the form.</p>\n";
        } else {
		    print "<p class=\"warning\">The data can't be analyzed because you haven't yet downloaded a data file of occurrences with period, epoch, stage, or 10 m.y. bin data. <a href=\"$READ_URL?action=displayDownloadForm\">Download the data again</a> and make sure to check off the field you want in the \"Collection fields\" part of the form.</p>\n";
        }
        return;
	}

	if ( $q->param('weight_by_ref') eq "yes" && $q->param('ref_quota') > 0 )	{
		print "<p class=\"warning\">The data can't be analyzed because you can't set a reference quota and avoid collections from references with many collections at the same time.</p>\n";
		exit;
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
	#s/\n//;
	#my @fieldnames;
	## tab-delimited file
	#if ( $_ =~ /\t/ )	{
	#	@fieldnames = split /\t/,$_;
	#}
	# comma-delimited file
	#else	{
	#	@fieldnames = split /,/,$_;
	#}

	my $fieldcount = 0;
	my $field_collection_no = -1;
	for my $fn (@fieldnames)	{
		if ( $fn eq "collection_no" )	{
			$field_collection_no = $fieldcount;
        } elsif ( $fn eq "sample_id" ) {
            $field_collection_no = $fieldcount;
		} elsif ( $fn eq "occurrences.reference_no" && $q->param('count_refs') eq "yes" )	{
			$field_genus_name = $fieldcount;
		} elsif ( $fn eq "occurrences.genus_name" && $q->param('count_refs') ne "yes" )	{
			$field_genus_name = $fieldcount;
		} elsif ( $fn eq "occurrences.species_name" )	{
			$field_species_name = $fieldcount;
		} elsif ( $fn eq "occurrences.family_name" )	{
			$field_family_name = $fieldcount;
		} elsif ( $fn eq "occurrences.order_name" )	{
			$field_order_name = $fieldcount;
		} elsif ( $fn eq "resolved_fossil_name" )   {
			$field_genus_name = $fieldcount;
			$field_species_name = $fieldcount;
		} elsif ( $fn eq "occurrences.abund_unit" )	{
			$field_abund_unit = $fieldcount;
		} elsif ( $fn eq "occurrences.abund_value" )	{
			$field_abund_value = $fieldcount;
		# only get the collection's ref no if we're weighting by
		#  collections per ref and the sampling method is by-list
		#  9.4.05
		} elsif ( $fn eq "collections.reference_no" )	{
# FOO NOT SURE WHY I NEEDED THIS && ( $q->param('weight_by_ref') eq "yes" || $q->param('ref_quota') > 0 ) && $samplingmethod > 1 && $samplingmethod < 5 )	{
			$field_refno = $fieldcount;
		} elsif ( $fn eq "collections.period" && $q->param('time_scale') eq "periods" )	{
			$field_bin = $fieldcount;
			$bin_type = "period";
		} elsif ( $fn eq "collections.epoch" && $q->param('time_scale') eq "epochs" )	{
			$field_bin = $fieldcount;
			$bin_type = "epoch";
		} elsif ( $fn eq "collections.subepoch" && $q->param('time_scale') eq "Cenozoic subepochs" )	{
			$field_bin = $fieldcount;
			$bin_type = "subepoch";
		} elsif ( $fn eq "collections.stage" && $q->param('time_scale') eq "stages" )	{
			$field_bin = $fieldcount;
			$bin_type = "stage";
		} elsif ( $fn eq "collections.10mybin" && $q->param('time_scale') eq "10 m.y. bins" )	{
			$field_bin = $fieldcount;
			$bin_type = "10my";
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
    my $downloadForm = "displayDownloadForm";
    if ($q->param('time_scale') =~ /neptune/i) {
        $downloadForm = "displayDownloadNeptuneForm";
    }
	if ( $field_collection_no < 0)	{
		my $collection_field = "collection number";
		if ($q->param('time_scale') =~ /neptune/i)	{
			$collection_field = "sample id";
		} 
		print "<p class=\"warning\">The data can't be analyzed because the $collection_field field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to check off this field in the form.</p>\n";
		exit;
	# this one is crucial and might be missing
	} elsif ( ! $field_bin )	{
		my $time_scale_field = $q->param('time_scale');
		$time_scale_field =~ s/s$//;
		if ($time_scale_field =~ /neptune/i)	{
			$time_scale_field = "sample_age_ma";
		}
		print "<p class=\"warning\">The data can't be analyzed because the $time_scale_field field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to check off this field in the \"Collection fields\" part of the form.</p>\n";
		exit;
	# this one also always should be present anyway, unless the user
	#  screwed up and didn't download the ref numbers despite wanting
	#  refs counted instead of genera
	} elsif ( ! $field_genus_name && $q->param("taxonomic_level") ne "family" and $q->param("taxonomic_level") ne "order" )	{
		if ( $q->param('count_refs') ne "yes" )	{
			print "<p class=\"warning\">The data can't be analyzed because the genus name field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		} else	{
			print "<p class=\"warning\">The data can't be analyzed because the reference number field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		}
		exit;
	} elsif ( $q->param("taxonomic_level") eq "species" && ! $field_species_name && $q->param('count_refs') ne "yes") {
		print "<p class=\"warning\">The data can't be analyzed because the species name field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	} elsif ( $q->param("taxonomic_level") eq "family" && ! $field_family_name && $q->param('count_refs') ne "yes") {
		print "<p class=\"warning\">The data can't be analyzed because the family name field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	} elsif ( $q->param("taxonomic_level") eq "order" && ! $field_order_name && $q->param('count_refs') ne "yes") {
		print "<p class=\"warning\">The data can't be analyzed because the order name field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	# these two might be missing
	} elsif ( ! $field_abund_value && ( $samplingmethod == 5 || $q->param("print_specimens") eq "YES" ) )	{
		print "<p class=\"warning\">The data can't be analyzed because the abundance value field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	} elsif ( ! $field_abund_unit && ( $samplingmethod == 5 || $q->param("print_specimens") eq "YES" ) )	{
		print "<p class=\"warning\">The data can't be analyzed because the abundance unit field hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	} elsif ( ! $field_refno && ( $q->param('weight_by_ref') eq "yes" || $q->param('ref_quota') > 0 || $q->param('print_refs_raw') eq "yes" || $q->param('print_refs_ss') eq "yes" ) )	{
		print "<p class=\"warning\">The data can't be analyzed because the reference number field <i>from the collections table</i> hasn't been downloaded. <a href=\"$READ_URL?action=$downloadForm\">Download the data again</a> and make sure to include this field.</p>\n";
		exit;
	} elsif ( $q->param("time_scale") =~ /neptune/i ) {
        if ($q->param("neptune_bin_size") !~ /^(\d+(\.\d+)?|\.\d+)$/) {
            print "<p class=\"warning\">Please enter a positive decimal number for the bin size to use if you are analyzing data from the Neptune database</p>";
            exit;
        } elsif ($q->param("neptune_bin_size") < .1 || $q->param("neptune_bin_size") > 100) {
            print "<p class=\"warning\">Bin size must be between .1 and 100</p>";
            exit;
        }
    }

	# figure out the ID numbers of the bins from youngest to oldest
	# we do this in a messy way, i.e., using preset arrays; if the
	#  10 m.y. bin scheme ever changes, this will need to be updated
	# also get the bin boundaries in Ma
	# switched from Harland to Gradstein scales JA 5.12.05
	my @binnames;
    my $t = new TimeLookup($dbt);
    my $ig = $t->getIntervalGraph;
    my $interval_name = {};
    $interval_name->{$_->{'interval_no'}} = $_->{'name'} foreach values %$ig;
	if ( $bin_type eq "period" )	{
        my @intervals = $t->getScaleOrder(69,'number');
        @binnames = map {$interval_name->{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
        $topma{$interval_name->{$_}} = $top->{$_} for @intervals;
        $basema{$interval_name->{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "epoch" )	{
        my @intervals = $t->getScaleOrder(71,'number');
        @binnames = map {$interval_name->{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
        $topma{$interval_name->{$_}} = $top->{$_} for @intervals;
        $basema{$interval_name->{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "subepoch" )	{
        my @intervals = $t->getScaleOrder(72,'number');
        @binnames = map {$interval_name->{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
        $topma{$interval_name->{$_}} = $top->{$_} for @intervals;
        $basema{$interval_name->{$_}} = $base->{$_} for @intervals;
	} elsif ( $bin_type eq "stage" )	{
        my @intervals = $t->getScaleOrder(73,'number');
        @binnames = map {$interval_name->{$_}} @intervals;
		my ($top,$base) = $t->getBoundaries();
        $topma{$interval_name->{$_}} = $top->{$_} for @intervals;
        $basema{$interval_name->{$_}} = $base->{$_} for @intervals;
    } elsif ( $bin_type eq "10my" ) {
        @binnames = $t->getBins();
        my ($top,$base) = $t->getBoundariesReal('bins');
        %topma = %$top;
        %basema = %$base;
	} elsif ( $bin_type =~ /neptune/ ) {
        # Neptune data ranges from -3 to 150 mA right now, use those at defaults
        $neptune_range_min = 0;
        $neptune_bin_count = int(180/$q->param('neptune_bin_size'));
        $neptune_range_max = $q->param('neptune_bin_size')*$neptune_bin_count;
        #foreach (@OCCDATA) {
        #    
        #}
        @binnames = ();
        for(my $i=$neptune_range_min;$i<$neptune_range_max;$i+=$q->param('neptune_bin_size')) {
            push @binnames,"$i - ".($i+$q->param('neptune_bin_size'));
        }
    }
	# assign chron numbers to named bins
	# note: $chrons and $chname are key variables used later
	for my $bn (@binnames)	{
		$chrons++;
		$binnumber{$bn} = $chrons;
		$chname[$chrons] = $bn;
	}

    # HARDCODED for NOW, change this later PS 1/10/2005
    if ( $bin_type =~ /neptune/i) {
        $q->param("taxonomic_level"=>"species");
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
			#s/\n//;
            $status = $csv->parse($_);
            my @occrow = $csv->fields();
            if (!$status) { print "Warning, error parsing CSV line $count"; }
            $count++;
			# tab-delimited file
			#if ( $_ =~ /\t/ )	{
			#	@occrow = split /\t/,$_;
			#}
			# comma-delimited file
			#else	{
		    #		@occrow = split /,/,$_;
			#}

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
			if ($bin_type =~ /neptune/i) {
				my $taxon_name = $occrow[$field_genus_name];
				my ($genus_name) = split(/ /,$taxon_name);
				$occrow[$field_genus_name] = $genus_name;
               		} 
                # else, nothing needs to be done for PBDB data
		# family and order level only apply to PBDB data
		} elsif ($q->param("taxonomic_level") eq "family") {
			$occrow[$field_genus_name] = $occrow[$field_family_name]; 
		} elsif ($q->param("taxonomic_level") eq "order") {
			$occrow[$field_genus_name] = $occrow[$field_order_name]; 
		} else { #species
			if ($bin_type =~ /neptune/i) {
                    # do this since we may have subspecies
				my $taxon_name = $occrow[$field_genus_name];
				my ($genus_name,$species_name) = split(/ /,$taxon_name);
				$occrow[$field_genus_name] = $genus_name." ".$species_name;
			} else {
				$occrow[$field_genus_name] .= " ".$occrow[$field_species_name]; 
		}
	}
	
		# get rid of records with no specimen/individual counts for method 5
			if ($samplingmethod == 5)   {
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
				$temp = $occrow[$field_genus_name];
		# we used to do some cleanups here that assumed error checking
		#  on data entry didn't always work
		# disabled 30.6.04 because on that date almost all genus-level
		#  records lacked quotation marks, question marks, double
		#  spaces, trailing spaces, and subgenus names
	
		 # create a list of all distinct genus names
				$ao = 0;
				$ao = $genid{$temp};
				if ($ao == 0)	{
					$ngen++;
					$genus[$ngen] = $temp;
					$genid{$temp} = $ngen;
				}

				$nsp = 1;
				if ($samplingmethod == 5)       {
					$nsp = $occrow[$field_abund_value];
				}

				# we used to knock out repeated occurrences of
				#  the same genus here, but that's now handled
				#  by the download script

				# add genus to master occurrence list and
				#  store the abundances
				if ($xx != -9)	{
					push @occs,$genid{$temp};
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
					if ( $collrefno[$collno] < 1 && $occrow[$field_refno] > 0 )	{
						$collsfromref[$occrow[$field_refno]][$chid[$collno]]++;
						$collrefno[$collno] = $occrow[$field_refno];
					}

					$toccsinlist[$collno] = $toccsinlist[$collno] + $nsp;
				}
				if ( $occrow[$field_refno] > 0 )	{
					$refisinchron[$chid[$collno]]{$occrow[$field_refno]}++;
				}
			}
		}
	# END of input file parsing routine

	# declare Recent genera present in bin 1
		if ( $q->param('recent_genera') )	{
			for my $taxon ( keys %extantbyname )	{
				if ( $genid{$taxon} =~ /[0-9]/ )	{
					$extantbyid[$genid{$taxon}][1] = 1;
				}
			}
		}

	 # get basic counts describing lists and their contents
		for $collno (1..$#lastocc+1)	{
			$xx = $lastocc[$collno];
			while ( $xx >= 0 && $xx =~ /[0-9]/ )	{
				$nsp = 1;
				if ($samplingmethod == 5)	{
					$nsp = $abund[$xx];
				}
				if ( $q->param('print_specimens') eq "YES" && $abund[$xx] > 0 )	{
					$specimensinchron[$chid[$collno]] += $abund[$xx];
				}
				$occsread = $occsread + $nsp;
				$occsoftax[$occs[$xx]] = $occsoftax[$occs[$xx]] + $nsp;
				for $qq ($occsinchron[$chid[$collno]]+1..$occsinchron[$chid[$collno]]+$nsp)	{
					$occsbychron[$chid[$collno]][$qq] = $occs[$xx];
				}
				$occsinchron[$chid[$collno]] = $occsinchron[$chid[$collno]] + $nsp;
				$present[$occs[$xx]][$chid[$collno]]++;
				$occsinlist[$collno] = $occsinlist[$collno] + $nsp;
				if ($occsinlist[$collno] == $nsp)	{
					$listsread++;
					$listsinchron[$chid[$collno]]++;
					$listsbychron[$chid[$collno]][$listsinchron[$chid[$collno]]] = $collno;
					$baseocc[$collno] = $occsinchron[$chid[$collno]] - $nsp + 1;
					$listsfromref[$collrefno[$collno]][$chid[$collno]]++;
				}
				$topocc[$collno] = $occsinchron[$chid[$collno]];
				$xx = $stone[$xx];
			}
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
			push @temp,$occsinlist[$listsbychron[$i][$j]];
		}
		@temp = sort { $a <=> $b } @temp;
		$j = int(($#temp)/2);
		$k = int(($#temp + 1)/2);
		$median[$i] = ($temp[$j] + $temp[$k])/2;
	}

	# compute sum of (squared) richnesses across lists
	if (($samplingmethod == 1) || ($samplingmethod == 5))	{
		for $i (1..$#occsinlist+1)	{
			$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
		}
	}
	elsif ($samplingmethod == 3)	{
		for $i (1..$#occsinlist+1)	{
			if ($occsinlist[$i] <= $q->param('samplesize'))	{
				$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
			}
		}
	}
	for $i (1..$#occsinlist+1)	{
	# don't blow out the quota if you have a long list and the method is 4
		if (($samplingmethod != 4) ||
				($occsinlist[$i]**$exponent <= $q->param('samplesize')))	{
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
    if ($bin_type =~ /neptune/i) {
        #foreach($i = 0; $i < $chrons;$i++) {
        #    print "$i : listsinchron $listsinchron[$i]<BR>";
        #}
        my $i = 0;
        foreach($i=$chrons;$i > 0;$i--) {
            if ($listsinchron[$i]) {
                last;
            }
        }
        $chrons = $i;
        #print "last $i";
    }

	# compute sampled diversity, range through diversity, originations,
	#  extinctions, singletons and doubletons (Chao l and m)
	print PADATA $q->param("taxonomic_level")."\t"."total occs";
	for $i (reverse 1..$chrons)	{
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
			if ( $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
				$twotimers[$j]++;
			}
		if ( $present[$i][$j-1] > 0 && $present[$i][$j] > 0 && $present[$i][$j+1] > 0 )	{
				$threetimers[$j]++;
			}
			if ( $present[$i][$j-1] > 0 && $present[$i][$j] == 0 && $present[$i][$j+1] > 0 )	{
				$parttimers[$j]++;
			}
			if ( $j > 1 && $j < $chrons - 1 && ( $present[$i][$j-1] > 0 || $present[$i][$j] > 0 ) && ( $present[$i][$j+1] > 0 || $present[$i][$j+2] > 0 ) )	{
				$localbc[$j]++;
			}
			if ( ( $present[$i][$j-1] > 0 && $present[$i][$j+1] > 0 ) || $present[$i][$j] > 0 )	{
				$localrt[$j]++;
			}
			# chaom is the notation for Chao-2, q1 for ICE
			if ($present[$i][$j] == 1)	{
				$chaol[$j]++;
				$q1[$j]++;
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
			for $j (reverse 1..$chrons)	{
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
sub findRecentGenera	{

	my $self = shift;

	# draw all comments pertaining to Jack's genera
	my $asql = "SELECT taxon_no,taxon_name FROM authorities WHERE extant='YES'";
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

	if ($q->param('samplingtrials') > 0)	{
		for ($trials = 1; $trials <= $q->param('samplingtrials'); $trials++)	{
			my @sampled = ();
			my @lastsampled = ();
			my @subsrichness = ();
			my @lastsubsrichness = ();
			my @present = ();
			my @refsampled = ();

			for $i (1..$chrons)	{
				if (($q->param('printall') eq "yes" && $listsinchron[$i] > 0)||
					  (($usedoccsinchron[$i] >= $q->param('samplesize') &&
					    $samplingmethod != 2 && $samplingmethod != 4) ||
					   ($listsinchron[$i] >= $q->param('samplesize') &&
					    $samplingmethod == 2) ||
					   ($occsinchron2[$i] >= $q->param('samplesize') &&
					    $samplingmethod == 4)))	{
		# figure out how many items must be drawn
		# WARNING: an "item" in this section of code means a record or a list;
		#   in the output an "item" may be a record-squared
					if ($samplingmethod != 2 && $samplingmethod != 4)	{
					  $inbin = $usedoccsinchron[$i];
					}
					elsif ($samplingmethod == 2)	{
					  $inbin = $listsinchron[$i];
					}
					elsif ($samplingmethod == 4)	{
					  $inbin = $occsinchron2[$i];
					}
					$ndrawn = 0;
		# old method was to track the number of items sampled and set the quota
		#  based on this number; new method is to track the number of lists
		#  regardless of the units of interest, and set quota based on the ratio
		#  of the items quota to the average number of items per list 19.8.00
					$tosub = $q->param('samplesize');
					if ($samplingmethod == 3)	{
			#     $tosub = $tosub - ($usedoccsinchron[$i]/$listsinchron[$i])/2;
				# old fashioned method needed if using this
				#  option 10.4.05
				# oddly, the denominator of 2 in the old
				#  equation appears to have been in error
					  if ( $q->param('weight_by_ref') eq "yes" )	{
					    $tosub = $tosub - ( $usedoccsinchron[$i] / $listsinchron[$i] ); 
					  }
				# modern method
					  else	{
					    $tosub = ($tosub/($usedoccsinchron[$i]/$listsinchron[$i])) - 0.5;
					  }
					}
					elsif ($samplingmethod == 4)	{
			#     $tosub = $tosub - ($occsinchron2[$i]/$listsinchron[$i])/2;
					  $tosub = ($tosub/($occsinchron2[$i]/$listsinchron[$i])) - 0.5;
					}
					my @refincluded = ();
					if ( $q->param('ref_quota') > 0 )	{
						my $refsallowed = 0;
						my $itemsinrefs = 0;
					# need a temporary list of refs
						my @temprefs = keys %{$refisinchron[$i]};
						while ( ( $refsallowed < $q->param('ref_quota') || $itemsinrefs < $tosub ) && $#temprefs > - 1 )	{
$| = 1;
							my $x = int( rand() * ( $#temprefs + 1 ) );
							my $tempref = $temprefs[$x];
							$refsallowed++;
							$refincluded[$tempref] = "YES";
							$itemsinrefs = $itemsinrefs + $listsfromref[$tempref][$i];
							splice @temprefs , $x , 1;
						}
$| = 1;
					}
		 # make a list of items that may be drawn
					if (($samplingmethod != 1) && ($samplingmethod != 5))	{
					  for $j (1..$listsinchron[$i])	{
					    $listid[$j] = $listsbychron[$i][$j];
					  }
					  $nitems = $listsinchron[$i];
			 # delete lists that would single-handedly blow out the record
			 #   quota for this interval
					  if (($samplingmethod > 2) && ($samplingmethod < 5))	{
					    for $j (1..$listsinchron[$i])	{
					      $xx = $occsinlist[$listid[$j]];
					      if ($samplingmethod == 4)	{
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
					  for ($j = 1; $j <= $occsinchron[$i]; $j++)	{
					    $occid[$j] = $occsbychron[$i][$j];
					  }
					  $nitems = $occsinchron[$i];
					}
		 # draw an item
					while ($tosub > 0 && $sampled[$i] < $inbin)	{
					  $lastsampled[$i] = $sampled[$i];
					  $j = int(rand $nitems) + 1;
		# throw back collections with a probability proportional to
		#  the number of collections belonging to the reference that
		#  yielded the chosen collection 9.4.05
		# WARNING: this only works for UW or OW (methods 2 and 3)
					  if ( $collsfromref[$collrefno[$listid[$j]]][$i] > 0 )	{
					    if ( rand > 1 / $collsfromref[$collrefno[$listid[$j]]][$i] && $q->param('weight_by_ref') eq "yes" && $samplingmethod > 1 && $samplingmethod < 4 )	{
					      $j = int(rand() * $nitems) + 1;
					      while ( rand > 1 / $collsfromref[$collrefno[$listid[$j]]][$i] )	{
					        $j = int(rand() * $nitems) + 1;
					      }
					    }
					  }
		# throw back collections that are not on the restricted list
		#  if using the reference quota algorithm
					  if ( $q->param('ref_quota') > 0 && $refincluded[$collrefno[$listid[$j]]] ne "YES" && $samplingmethod != 1 && $samplingmethod != 5 )	{
					    while ( $refincluded[$collrefno[$listid[$j]]] ne "YES" )	{
					      $j = int(rand $nitems) + 1;
					    }
					  }
					  if ( $field_refno > 0 && $collrefno[$listid[$j]] > 0 )	{
					    $refsampled[$collrefno[$listid[$j]]][$i]++;
					    if ( $refsampled[$collrefno[$listid[$j]]][$i] == 1 )	{
					      $msubsrefrichness[$i]++;
					    }
					  }
		# modify the counter
					  if (($samplingmethod < 3) || ($samplingmethod > 4))	{
					    $tosub--;
					    $sampled[$i]++;
					  }
					  elsif ($samplingmethod == 3)	{
					    $xx = $topocc[$listid[$j]] - $baseocc[$listid[$j]] + 1;
					 #  $tosub = $tosub - $xx;
					# old fashioned method needed if using
					#  this option 10.4.05
					    if ( $q->param('weight_by_ref') eq "yes" )	{
					      $tosub = $tosub - $xx;
					    }
					#  standard method
					    else	{
					      $tosub--;
					    }
					    $sampled[$i] = $sampled[$i] + $xx;
					  }
					# method 4
					  else	{
					    $xx = $topocc[$listid[$j]] - $baseocc[$listid[$j]] + 1;
					 #  $tosub = $tosub - ($xx**2);
					    $tosub--;
					    $sampled[$i] = $sampled[$i] + $xx**$exponent;
					  }
	
		 # declare the genus (or all genera in a list) present in this chron
					  $lastsubsrichness[$i] = $subsrichness[$i];
					  if (($samplingmethod == 1) || ($samplingmethod == 5))	{
					    if ($present[$occid[$j]][$i] == 0)	{
					      $subsrichness[$i]++;
					    }
					    $present[$occid[$j]][$i]++;
					  }
					  else	{
					    for $k ($baseocc[$listid[$j]]..$topocc[$listid[$j]])	{
					      if ($present[$occsbychron[$i][$k]][$i] == 0)	{
					        $subsrichness[$i]++;
					      }
					      $present[$occsbychron[$i][$k]][$i]++;
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
		# for method 2 = UW, compute the honest to goodness
		#  complete subsampling curve
					  if ( $samplingmethod == 2 )	{
					    $fullsampcurve[$i][$sampled[$i]] = $fullsampcurve[$i][$sampled[$i]] + $subsrichness[$i];
					  }
	
		 # erase the list or occurrence that has been drawn
					  if (($samplingmethod != 1) && ($samplingmethod != 5))	{
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
		 # end of while loop
				}
			# finish off recording data in complete subsampling curve
				if ($atstep[$q->param('samplesize')]+1 > $atstep[int($sampled[$i])] &&
					  $inbin > $sampled[$i])	{
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
					if ( $present[$i][$j-1] > 0 && $present[$i][$j] == 0 && $present[$i][$j+1] > 0 )	{
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
				if ($msubsrangethrough[$i] > 0)	{
					$msubsrichness[$i] = $msubsrichness[$i] + $subsrichness[$i];
				}
				if ($q->param('diversity') =~ /^boundary-crossers/)	{
					$outrichness[$i][$trials] = $trangethrough[$i] - $toriginate[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $trangethrough[$i] - $toriginate[$i];
				}
				elsif ($q->param('diversity') =~ /range-through\b/)	{
					$outrichness[$i][$trials] = $trangethrough[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $trangethrough[$i];
				}
				elsif ($q->param('diversity') =~ /range-through minus/)	{
					$outrichness[$i][$trials] = $trangethrough[$i] - $tsingletons[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $trangethrough[$i] - $tsingletons[$i];
				}
				elsif ($q->param('diversity') =~ /sampled-in-bin/)	{
					$outrichness[$i][$trials] = $subsrichness[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $subsrichness[$i];
				}
				elsif ($q->param('diversity') =~ /sampled minus/)	{
					$outrichness[$i][$trials] = $subsrichness[$i] - $tsingletons[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $subsrichness[$i] - $tsingletons[$i];
				}
				elsif ($q->param('diversity') =~ /two timers/)	{
					$outrichness[$i][$trials] = $ttwotimers[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $ttwotimers[$i];
				}
				elsif ($q->param('diversity') =~ /local boundary-crossers/)	{
					$outrichness[$i][$trials] = $tlocalbc[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $tlocalbc[$i];
				}
				elsif ($q->param('diversity') =~ /local range-through/)	{
					$outrichness[$i][$trials] = $tlocalrt[$i];
					$meanoutrichness[$i] = $meanoutrichness[$i] + $tlocalrt[$i];
				}
			}
	 # end of trials loop
		}
		$trials = $q->param('samplingtrials');

		for $i (1..$chrons)	{
			if ($msubsrangethrough[$i] > 0)	{
				$tsampled[$i] = $tsampled[$i]/$trials;
				$msubsrefrichness[$i] = $msubsrefrichness[$i] / $trials;
				$msubsrichness[$i] = $msubsrichness[$i]/$trials;
				$mtwotimers[$i] = $mtwotimers[$i]/$trials;
				$mthreetimers[$i] = $mthreetimers[$i]/$trials;
				$mparttimers[$i] = $mparttimers[$i]/$trials;
				$msubsnewbc[$i] = $msubsnewbc[$i]/$trials;
				$msubsrangethrough[$i] = $msubsrangethrough[$i]/$trials;
				$meanoutrichness[$i] = $meanoutrichness[$i]/$trials;
				$msubsoriginate[$i] = $msubsoriginate[$i]/$trials;
				$msubsextinct[$i] = $msubsextinct[$i]/$trials;
				$msubssingletons[$i] = $msubssingletons[$i]/$trials;
				$msubschaol[$i] = $msubschaol[$i]/$trials;
				$msubschaom[$i] = $msubschaom[$i]/$trials;
				$msubsearlier[$i] = $msubsearlier[$i]/$trials;
				$msubslater[$i] = $msubslater[$i]/$trials;
				for $j (1..$atstep[$q->param('samplesize')])	{
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

	if ($q->param('diversity') =~ /two timers/ && $threetimerp > 0)	{

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
			$msubsjolly[$i] = ($msubsrichness[$i]**2 * ($msubsrangethrough[$i] - $msubsrichness[$i]) / ($msubsearlier[$i] * $msubslater[$i])) + $msubsrichness[$i];
		}
	}

	# fit Michaelis-Menten equation using Raaijmakers maximum likelihood
	#  equation (Colwell and Coddington 1994, p. 106) 13.7.04
	# do this only for method 2 (UW) because the method assumes you are
	#  making a UW curve
	if ( $samplingmethod == 2)	{
		for $i (1..$chrons)	{
			if ($msubsrichness[$i] > 0)	{
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

	if ($q->param('stepsize') ne "")	{
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
		for $i (1..$atstep[$q->param('samplesize')])	{
			print CURVES "$samplesteps[$i]\t";
			$foo = 0;
			for $j (reverse 1..$chrons)	{
				if ($sampcurve[$j][1] > 0)	{
					if ($foo > 0)	{
					  print CURVES "\t";
					}
					$foo++;
					printf CURVES "%.1f",$sampcurve[$j][$i];
				}
			}
			print CURVES "\n";
			if ($samplesteps[$i+1] > $q->param('samplesize'))	{
				last;
			}
		}
		close CURVES;
	}
	
	if ( ! open TABLE,">$OUTPUT_DIR/raw_curve_data.csv" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/raw_curve_data.csv<BR>$!" );
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


    if ($q->param('samplesize')) {
        print '<script src="/JavaScripts/tabs.js" language="JavaScript" type="text/javascript"></script>';
        print '<div align=center>
                 <table cellpadding=0 cellspacing=0 border=0 width=600><tr>
                   <td id="tab1" class="tabOff" style="white-space: nowrap;"
                     onClick="showPanel(1);" 
                     onMouseOver="hover(this);" 
                     onMouseOut="setState(1)">Raw data</td>
                   <td id="tab2" class="tabOff" style="white-space: nowrap;"
                     onClick="showPanel(2);" 
                     onMouseOver="hover(this);" 
                     onMouseOut="setState(2)">Subsampled data </td>
                 </tr></table>
               </div>';
    }

	print '<div align="center"><p class="pageTitle" style="margin-bottom: 0.5em;">Diversity curve report</p></div>';

	if ($q->param('samplesize')) {
		print '<div id="panel1" class="panel">';
	}

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
		if ( $q->param('print_base_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Base (Ma)</b>";
		}
		if ( $q->param('print_midpoint_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Midpoint (Ma)</b>";
		}
		if ( $q->param('print_sampled') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Sampled<br>$generaorrefs</b>";
		}
		if ( $q->param('print_range-through') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Range-through<br>$generaorrefs</b> ";
		}
		if ( $q->param('print_boundary-crosser') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Boundary-crosser<br>$generaorrefs</b> ";
		}
		if ( $q->param('print_two_timer') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Two&nbsp;timer<br>$generaorrefs</b> ";
		}
		if ( $q->param('print_three_timer') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three&nbsp;timer<br>$generaorrefs</b> ";
		}
		if ( $q->param('print_first_appearances_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>First<br>appearances</b>";
		}
		if ( $q->param('print_origination_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Origination<br>rate</b> ";
		}
		if ( $q->param('print_last_appearances_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Last<br>appearances</b>";
		}
		if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Extinction<br>rate</b> ";
		}
		if ( $q->param('print_singletons_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Singletons</b> ";
		}
		if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Gap analysis<br>sampling stat</b> ";
		}
		if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Gap analysis<br>diversity estimate</b> ";
		}
		if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three timer<br>sampling stat</b> ";
		}
		if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Three timer<br>diversity estimate</b> ";
		}
		if ( $q->param('print_chao-2_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Chao-2<br>estimate</b> ";
		}
		if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Jolly-Seber<br>estimate</b> ";
		}
		if ( $q->param('print_ice_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Incidence-based<br>coverage estimate</b> ";
		}
		if ( $q->param('print_pie') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Evenness of<br>occurrences (PIE)</b> ";
		}
		if ( $q->param('print_refs_raw') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>References</b> ";
		}
		if ( $q->param('print_lists') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>$listorfm</b> ";
		}
		if ($samplingmethod != 5)	{
			if ( $q->param('print_occurrences') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Occurrences</b> ";
			}
			if ( $q->param('print_occurrences-exponentiated') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Occurrences<br><nobr>-exponentiated</nobr></b> ";
			}
		}
		if ( $samplingmethod == 5 || $q->param('print_specimens') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Specimens/<br>individuals</b> ";
		}
		if ( $q->param('print_mean_richness') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Mean<br>richness</b>";
		}
		if ( $q->param('print_median_richness') eq "YES" )	{
			print "<td class=tiny align=center valign=top><b>Median<br>richness</b> ";
		}

		print TABLE "Bin";
		print TABLE ",Bin name";
		if ( $q->param('print_base_raw') eq "YES" )	{
			print TABLE ",Base (Ma)";
		}
		if ( $q->param('print_midpoint_raw') eq "YES" )	{
			print TABLE ",Midpoint (Ma)";
		}
		if ( $q->param('print_sampled') eq "YES" )	{
			print TABLE ",Sampled $generaorrefs";
		}
		if ( $q->param('print_range-through') eq "YES" )	{
			print TABLE ",Range-through $generaorrefs";
		}
		if ( $q->param('print_boundary-crosser') eq "YES" )	{
			print TABLE ",Boundary-crosser $generaorrefs";
		}
		if ( $q->param('print_two_timer') eq "YES" )	{
			print TABLE ",Two timer $generaorrefs";
		}
		if ( $q->param('print_three_timer') eq "YES" )	{
			print TABLE ",Three timer $generaorrefs";
		}
		if ( $q->param('print_first_appearances_raw') eq "YES" )	{
			print TABLE ",First appearances";
		}
		if ( $q->param('print_origination_rate_raw') eq "YES" )	{
			print TABLE ",Origination rate";
		}
		if ( $q->param('print_last_appearances_raw') eq "YES" )	{
			print TABLE ",Last appearances";
		}
		if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
			print TABLE ",Extinction rate";
		}
		if ( $q->param('print_singletons_raw') eq "YES" )	{
			print TABLE ",Singletons";
		}
		if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
			print TABLE ",Gap analysis sampling stat";
		}
		if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
			print TABLE ",Gap analysis diversity estimate";
		}
		if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
			print TABLE ",Three timer sampling stat";
		}
		if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
			print TABLE ",Three timer diversity estimate";
		}
		if ( $q->param('print_chao-2_raw') eq "YES" )	{
			print TABLE ",Chao-2 estimate";
		}
		if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
			print TABLE ",Jolly-Seber estimate";
		}
		if ( $q->param('print_ice_raw') eq "YES" )	{
			print TABLE ",Incidence-based coverage estimate";
		}
		if ( $q->param('print_pie') eq "YES" )	{
			print TABLE ",Evenness of occurrences (PIE)";
		}
		if ( $q->param('print_refs_raw') eq "YES" )	{
			print TABLE ",References";
		}
		if ($samplingmethod != 5)	{
			if ( $q->param('print_lists') eq "YES" )	{
				print TABLE ",$listorfm";
			}
			if ( $q->param('print_occurrences') eq "YES" )	{
				print TABLE ",Occurrences";
			}
			if ( $q->param('print_occurrences-exponentiated') eq "YES" )	{
				print TABLE ",Occurrences-exponentiated";
			}
		}
		else	{
			print TABLE ",$listorfm";
		}
		if ( $samplingmethod == 5 || $q->param('print_specimens') eq "YES" )	{
			print TABLE ",Specimens/individuals";
		}
		if ( $q->param('print_mean_richness') eq "YES" )	{
			print TABLE ",Mean richness";
		}
		if ( $q->param('print_median_richness') eq "YES" )	{
			print TABLE ",Median richness";
		}
		print TABLE "\n";

	 # make sure all the files are read/writeable
#		chmod 0664, "$OUTPUT_DIR/*";
	
		for $i (1..$chrons)	{
			if ( $rangethrough[$i] > 0 || ( $listsinchron[$i] > 0 || ( $q->param('recent_genera') && $i == 1 ) ) )	{
				$gapstat = $richness[$i] - $originate[$i] - $extinct[$i] + $singletons[$i];
				if ($gapstat > 0)	{
					$gapstat = $gapstat / ( $rangethrough[$i] - $originate[$i] - $extinct[$i] + $singletons[$i] );
				}
				else	{
					$gapstat = "NaN";
				}
				if ($threetimers[$i] + $parttimers[$i] > 0)	{
					$ttstat = $threetimers[$i] / ( $threetimers[$i] + $parttimers[$i] );
				}
				else	{
					$ttstat = "NaN";
				}
				if ($chaom[$i] > 0)	{
					$chaostat = $richness[$i] + ($chaol[$i] * $chaol[$i] / (2 * $chaom[$i]));
				}
				else	{
					$chaostat = "NaN";
				}
				$temp = $chname[$i];
				$temp =~ s/ /&nbsp;/;
				print "<tr><td class=tiny valign=top>$temp";
				if ( $q->param('print_base_raw') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f",$basema{$chname[$i]};
				}
				if ( $q->param('print_midpoint_raw') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
				}
				if ( $q->param('print_sampled') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$richness[$i] ";
				}
				if ( $q->param('print_range-through') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$rangethrough[$i] ";
				}
		# compute boundary crossers
		# this is total range through diversity minus singletons minus
		#  first-appearing crossers into the next bin; the latter is
		#  originations - singletons, so the singletons cancel out and
		#  you get total diversity minus originations
				if ( $q->param('print_boundary-crosser') eq "YES" )	{
					$bcrich[$i] = $rangethrough[$i] - $originate[$i];
					printf "<td class=tiny align=center valign=top>%d ",$bcrich[$i];
				}
				if ( $q->param('print_two_timer') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%d ",$twotimers[$i];
				}
				if ( $q->param('print_three_timer') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%d ",$threetimers[$i];
				}
				if ( $q->param('print_first_appearances_raw') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$originate[$i] ";
				}
			# Foote origination rate - note: extinction counts must
			#  exclude singletons
				if ( $q->param('print_origination_rate_raw') eq "YES" )	{
					if ( $bcrich[$i-1] > 0 && $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.4f",log( $bcrich[$i-1] / ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) );
					} else	{
						print "<td class=tiny align=center valign=top>NaN";
					}
				}
				if ( $q->param('print_last_appearances_raw') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$extinct[$i] ";
				}
			# Foote extinction rate
				if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
					if ( $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 && $bcrich[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.4f",log( ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) / $bcrich[$i] ) * -1;
					} else	{
						print "<td class=tiny align=center valign=top>NaN";
					}
				}
				if ( $q->param('print_singletons_raw') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$singletons[$i] ";
				}
				if ( $gapstat > 0 )	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.3f ",$gapstat;
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$richness[$i] / $gapstat;
					}
				} else	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NaN ";
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $ttstat > 0 )	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.3f ",$ttstat;
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$richness[$i] / $ttstat;
					}
				} else	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NaN ";
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $q->param('print_chao-2_raw') eq "YES" )	{
					if ($chaostat > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$chaostat;
					} else    {
				  		print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
					if ($jolly[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$jolly[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $q->param('print_ice_raw') eq "YES" )	{
					if ($ice[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$ice[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $q->param('print_pie') eq "YES" )	{
					if ($pie[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.5f ",$pie[$i];
					} else    {
						print "<td class=tiny align=center valign=top>NaN ";
					}
				}
				if ( $q->param('print_refs_raw') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%d ",$refsinchron[$i];
				}
				if ( $q->param('print_lists') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$listsinchron[$i] ";
				}
				if ( $q->param('print_occurrences') eq "YES" )	{
					print "<td class=tiny align=center valign=top>$occsinchron[$i] ";
				}
				if ( $q->param('print_occurrences-exponentiated') eq "YES" )	{
					if ($samplingmethod != 5)	{
						printf "<td class=tiny align=center valign=top>%.1f ",$occsinchron2[$i];
					}
				}
				if ( $q->param('print_specimens') eq "YES" && $samplingmethod != 5 )	{
					print "<td class=tiny align=center valign=top>$specimensinchron[$i] ";
				}
				if ( $q->param('print_mean_richness') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$occsinchron[$i]/$listsinchron[$i];
					} else	{
						print "<td class=tiny align=center valign=top>NaN "
					}
				}
				if ( $q->param('print_median_richness') eq "YES" )	{
					printf "<td class=tiny align=center valign=top>%.1f ",$median[$i];
				}

				print TABLE $chrons - $i + 1;
				print TABLE ",$chname[$i]";
				if ( $q->param('print_base_raw') eq "YES" )	{
					printf TABLE ",%.1f",$basema{$chname[$i]};
				}
				if ( $q->param('print_midpoint_raw') eq "YES" )	{
					printf TABLE ",%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
				}
				if ( $q->param('print_sampled') eq "YES" )	{
					print TABLE ",$richness[$i]";
				}
				if ( $q->param('print_range-through') eq "YES" )	{
					print TABLE ",$rangethrough[$i]";
				}
			# boundary crossers
				if ( $q->param('print_boundary-crosser') eq "YES" )	{
					printf TABLE ",%d",$bcrich[$i];
				}
				if ( $q->param('print_two_timer') eq "YES" )	{
					printf TABLE ",%d",$twotimers[$i];
				}
				if ( $q->param('print_three_timer') eq "YES" )	{
					printf TABLE ",%d",$threetimers[$i];
				}
				if ( $q->param('print_first_appearances_raw') eq "YES" )	{
					print TABLE ",$originate[$i]";
				}
			# Foote origination rate
				if ( $q->param('print_origination_rate_raw') eq "YES" )	{
					if ( $bcrich[$i-1] > 0 && $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 )	{
						printf TABLE ",%.4f",log( $bcrich[$i-1] / ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) );
					} else	{
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_last_appearances_raw') eq "YES" )	{
					print TABLE ",$extinct[$i]";
				}
			# Foote extinction rate
				if ( $q->param('print_extinction_rate_raw') eq "YES" )	{
					if ( $bcrich[$i] - $extinct[$i] + $singletons[$i] > 0 && $bcrich[$i] > 0 )	{
						printf TABLE ",%.4f",log( ( $bcrich[$i] - $extinct[$i] + $singletons[$i] ) / $bcrich[$i] ) * -1;
					} else	{
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_singletons_raw') eq "YES" )	{
					print TABLE ",$singletons[$i]";
				}
				if ( $gapstat > 0 )	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						printf TABLE ",%.3f",$gapstat;
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						printf TABLE ",%.1f",$richness[$i] / $gapstat;
					}
				} else	{
					if ( $q->param('print_gap_analysis_stat_raw') eq "YES" )	{
						print TABLE ",NaN";
					}
					if ( $q->param('print_gap_analysis_estimate_raw') eq "YES" )	{
						print TABLE ",NaN";
					}
				}
				if ( $ttstat > 0 )	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						printf TABLE ",%.3f",$ttstat;
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						printf TABLE ",%.1f",$richness[$i] / $ttstat;
					}
				} else	{
					if ( $q->param('print_three_timer_stat_raw') eq "YES" )	{
						print TABLE ",NaN";
					}
					if ( $q->param('print_three_timer_estimate_raw') eq "YES" )	{
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_chao-2_raw') eq "YES" )	{
					if ($chaostat > 0 )	{
						printf TABLE ",%.1f",$chaostat;
					} else    {
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_jolly-seber_raw') eq "YES" )	{
					if ($jolly[$i] > 0 )	{
						printf TABLE ",%.1f",$jolly[$i];
					} else    {
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_ice_raw') eq "YES" )	{
					if ($ice[$i] > 0 )	{
						printf TABLE ",%.1f",$ice[$i];
					} else    {
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_pie') eq "YES" )	{
					if ($pie[$i] > 0 )	{
						printf TABLE ",%.5f",$pie[$i];
					} else    {
						print TABLE ",NaN";
					}
				}
				if ( $q->param('print_refs_raw') eq "YES" )	{
					print TABLE ",$refsinchron[$i]";
				}
				if ( $q->param('print_lists') eq "YES" )	{
					print TABLE ",$listsinchron[$i]";
				}
				if ( $q->param('print_occurrences') eq "YES" )	{
					print TABLE ",$occsinchron[$i]";
				}
				if ( $q->param('print_occurrences-exponentiated') eq "YES" )	{
					if ($samplingmethod != 5)	{
						printf TABLE ",%.1f",$occsinchron2[$i];
					}
				}
				if ( $q->param('print_specimens') eq "YES" && $samplingmethod != 5 )	{
					print TABLE ",$specimensinchron[$i]";
				}
				if ( $q->param('print_mean_richness') eq "YES" )	{
					if ( $listsinchron[$i] > 0 )	{
						printf TABLE ",%.1f",$occsinchron[$i]/$listsinchron[$i];
					} else	{
						 print TABLE ",NaN";
					}
				}
				if ( $q->param('print_median_richness') eq "YES" )	{
					printf TABLE ",%.1f",$median[$i];
				}
				print "<p>\n";
				print TABLE "\n";
			}
		}
		print qq|</table>
</div>

<div class="small" style="padding-left: 2em; padding-right: 2em;">
|;

		if ( $refsread == 0 )	{
			print "\n<b>$listsread</b> collections and <b>$occsread</b> occurrences met the search criteria.<p>\n";
		} else	{
			print "\n<b>$refsread</b> reference and interval combinations, <b>$listsread</b> collections, and <b>$occsread</b> occurrences met the search criteria.<p>\n";
		}
	
		print "\nThe following data files have been created:<p>\n";
		print "<ul>\n";
		print "<li>The above diversity curve data (<a href=\"$HOST_URL$PRINTED_DIR/raw_curve_data.csv\">raw_curve_data.csv</a>)<p>\n";
	
	
		print "<li>A first-by-last occurrence count matrix (<a href=\"$HOST_URL$PRINTED_DIR/firstlast.txt\">firstlast.txt</a>)<p>\n";
	
		print "<li>A list of each ".$q->param('taxonomic_level').", the number of collections including it,  and the ID number of the intervals in which it was found (<a href=\"$HOST_URL$PRINTED_DIR/presences.txt\">presences.txt</a>)<p>\n";

		print "</ul><p>\n";

        my $downloadForm = "displayDownloadForm";
        if ($q->param('time_scale') =~ /neptune/i) {
            $downloadForm = "displayDownloadNeptuneForm";
        }  
		print "\nYou may wish to <a href=\"$READ_URL?action=$downloadForm\">download another data set</a></b> before you run another analysis.<p>\n";
		print "</div>\n";


        if ($q->param('samplesize') ne '') {
            print "</div>"; # END PANEL1 DIV
            print '<div id="panel2" class="panel">';


			print qq|<div class="displayPanel" style="padding-top: 1em;">
<table cellpadding="4">
|;
			print "<tr><td class=tiny valign=top><b>Interval</b>\n";
			if ( $q->param('print_base_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Base (Ma)</b> ";
			}
			if ( $q->param('print_midpoint_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Midpoint (Ma)</b> ";
			}
#			if ( $q->param('print_') eq "YES" )	{
#				print "<td class=tiny align=center valign=top><b>Sampled<br>$generaorrefs</b> ";
#			}
#			if ( $q->param('print_') eq "YES" )	{
#				print "<td class=tiny align=center valign=top><b>Range-through<br>$generaorrefs</b> ";
#			}
			if ( $q->param('print_items') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Items<br>sampled</b> ";
			}
			if ( $q->param('print_refs_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>References<br>sampled</b> ";
			}
			if ( $q->param('print_median') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Median ";
				printf "%s diversity</b>",$q->param('diversity');
			}
			if ( $q->param('print_ci') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>1-sigma CI</b> ";
			}
			if ( $q->param('print_mean') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Mean ";
				printf "%s diversity</b>",$q->param('diversity');
			}
			if ($q->param('diversity') =~ /two timers/)	{
				if ( $q->param('print_three_timers_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Mean three timers</b> ";
				}
				if ( $q->param('print_corrected_bc') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Corrected BC<br>diversity</b> ";
				}
				if ( $q->param('print_estimated_midpoint') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Estimated midpoint<br>diversity</b> ";
				}
				if ( $q->param('print_estimated_mesa') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Estimated mesa<br>diversity</b> ";
				}
				if ( $q->param('print_raw_sib') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Raw SIB<br>diversity</b> ";
				}
				if ( $q->param('print_corrected_sib') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Corrected SIB<br>diversity</b> ";
				}
				if ( $q->param('print_origination_rate_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Origination<br>rate</b> ";
				}
				if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Extinction<br>rate</b> ";
				}
			}
			elsif ($q->param('diversity') =~ /boundary-crossers/)	{
				if ( $q->param('print_first_appearances_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>First<br>appearances</b> ";
				}
				if ( $q->param('print_origination_rate_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Origination<br>rate</b> ";
				}
				if ( $q->param('print_origination_percentage') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Origination<br>percentage</b> ";
				}
				if ( $q->param('print_last_appearances_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Last<br>appearances</b> ";
				}
				if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Extinction<br>rate</b> ";
				}
				if ( $q->param('print_extinction_percentage') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Extinction<br>percentage</b> ";
				}
			} else	{
				if ( $q->param('print_origination_percentage') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Origination<br>percentage</b> ";
				}
				if ( $q->param('print_extinction_percentage') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Extinction<br>percentage</b> ";
				}
			}
			if ( $q->param('print_singletons_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Singletons</b> ";
			}
			if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Gap analysis<br>sampling stat</b> ";
			}
			if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Gap analysis<br>diversity estimate</b> ";
			}
			if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Three timer<br>sampling stat</b> ";
			}
			if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Three timer<br>diversity estimate</b> ";
			}
			if ( $q->param('print_chao-2_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Chao-2<br>estimate</b> ";
			}
			if ( $q->param('print_jolly-seber_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Jolly-Seber<br>estimate</b> ";
			}
			if ( $q->param('print_ice_ss') eq "YES" )	{
				print "<td class=tiny align=center valign=top><b>Incidence-based<br>coverage estimate</b> ";
			}
			if ( $samplingmethod == 2)	{
				if ( $q->param('print_michaelis-menten') eq "YES" )	{
					print "<td class=tiny align=center valign=top><b>Michaelis-Menten<br>estimate</b> ";
				}
			}
			print SUB_TABLE "Bin,Bin name";
			if ( $q->param('print_base_ss') eq "YES" )	{
				print SUB_TABLE ",Base (Ma)";
			}
			if ( $q->param('print_midpoint_ss') eq "YES" )	{
				print SUB_TABLE ",Midpoint (Ma)";
			}
	#		if ( $q->param('print_') eq "YES" )	{
	#			print SUB_TABLE ",Sampled $generaorrefs";
	#		}
	#		if ( $q->param('print_') eq "YES" )	{
	#			print SUB_TABLE ",Range-through $generaorrefs";
	#		}
			if ( $q->param('print_items') eq "YES" )	{
				print SUB_TABLE ",Items sampled";
			}
			if ( $q->param('print_refs_ss') eq "YES" )	{
				print SUB_TABLE ",References sampled";
			}
			if ( $q->param('print_median') eq "YES" )	{
				print SUB_TABLE ",Median ";
				printf SUB_TABLE "%s diversity",$q->param('diversity');
			}
			if ( $q->param('print_ci') eq "YES" )	{
				print SUB_TABLE ",1-sigma lower CI,1-sigma upper CI";
			}
			if ( $q->param('print_mean') eq "YES" )	{
				print SUB_TABLE ",Mean ";
				printf SUB_TABLE "%s diversity",$q->param('diversity');
			}
			if ($q->param('diversity') =~ /two timers/)	{
				if ( $q->param('print_three_timers_ss') eq "YES" )	{
					print SUB_TABLE ",Mean three timers";
				}
				if ( $q->param('print_corrected_bc') eq "YES" )	{
					print SUB_TABLE ",Corrected BC diversity";
				}
				if ( $q->param('print_estimated_midpoint') eq "YES" )	{
					print SUB_TABLE ",Estimated midpoint diversity";
				}
				if ( $q->param('print_estimated_mesa') eq "YES" )	{
					print SUB_TABLE ",Estimated mesa diversity";
				}
				if ( $q->param('print_raw_sib') eq "YES" )	{
					print SUB_TABLE ",Raw SIB diversity";
				}
				if ( $q->param('print_corrected_sib') eq "YES" )	{
					print SUB_TABLE ",Corrected SIB diversity";
				}
				if ( $q->param('print_origination_rate_ss') eq "YES" )	{
					print SUB_TABLE ",Origination rate";
				}
				if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
					print SUB_TABLE ",Extinction rate";
				}
			}
			elsif ($q->param('diversity') =~ /boundary-crossers/)	{
				if ( $q->param('print_first_appearances_ss') eq "YES" )	{
					print SUB_TABLE ",First appearances";
				}
				if ( $q->param('print_origination_rate_ss') eq "YES" )	{
					print SUB_TABLE ",Origination rate";
				}
				if ( $q->param('print_origination_percentage') eq "YES" )	{
					print SUB_TABLE ",Origination percentage";
				}
				if ( $q->param('print_last_appearances_ss') eq "YES" )	{
					print SUB_TABLE ",Last appearances";
				}
				if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
					print SUB_TABLE ",Extinction rate";
				}
				if ( $q->param('print_extinction_percentage') eq "YES" )	{
					print SUB_TABLE ",Extinction percentage";
				}
			} else	{
				if ( $q->param('print_origination_percentage') eq "YES" )	{
					print SUB_TABLE ",Origination percentage";
				}
				if ( $q->param('print_extinction_percentage') eq "YES" )	{
					print SUB_TABLE ",Extinction percentage";
				}
			}
			if ( $q->param('print_singletons_ss') eq "YES" )	{
				print SUB_TABLE ",Singletons";
			}
			if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
				print SUB_TABLE ",Gap analysis sampling stat";
			}
			if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
				print SUB_TABLE ",Gap analysis diversity estimate";
			}
			if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
				print SUB_TABLE ",Three timer sampling stat";
			}
			if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
				print SUB_TABLE ",Three timer diversity estimate";
			}
			if ( $q->param('print_chao-2_ss') eq "YES" )	{
				print SUB_TABLE ",Chao-2 estimate";
			}
			if ( $q->param('print_jolly-seber_ss') eq "YES" )	{
				print SUB_TABLE ",Jolly-Seber estimate";
			}
			if ( $q->param('print_ice_ss') eq "YES" )	{
				print SUB_TABLE ",Incidence-based coverage estimate";
			}
			if ( $samplingmethod == 2)	{
				if ( $q->param('print_michaelis-menten') eq "YES" )	{
					print SUB_TABLE ",Michaelis-Menten estimate";
				}
			}
			print SUB_TABLE "\n";

			for ($i = 1; $i <= $chrons; $i++)     {
				if ($rangethrough[$i] > 0)  {
					$gapstat = $msubsrichness[$i] - $msubsoriginate[$i] - $msubsextinct[$i] + $msubssingletons[$i];
					if ($gapstat > 0)	{
						$gapstat = $gapstat / ( $msubsrangethrough[$i] - $msubsoriginate[$i] - $msubsextinct[$i] + $msubssingletons[$i] );
	#         $gapstat = $richness[$i] / $gapstat;
					}
					else	{
						$gapstat = "NaN";
					}
					if ( $mthreetimers[$i] + $mparttimers[$i] > 0 )	{
						$ttstat = $mthreetimers[$i] / ( $mthreetimers[$i] + $mparttimers[$i] );
					} else	{
				  		$ttstat = "NaN";
					}
					if ($msubschaom[$i] > 0)	{
						$msubschaostat = $msubsrichness[$i] + ($msubschaol[$i] * $msubschaol[$i] / (2 * $msubschaom[$i]));
					}
					else	{
						$msubschaostat = "NaN";
					}
					$temp = $chname[$i];
					$temp =~ s/ /&nbsp;/;
					print "<tr><td class=tiny valign=top>$temp";
					if ( $q->param('print_base_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$basema{$chname[$i]};
					}
					if ( $q->param('print_midpoint_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
					}
#					printf "<td class=tiny align=center valign=top>%.1f ",$msubsrichness[$i];
#					printf "<td class=tiny align=center valign=top>%.1f ",$msubsrangethrough[$i];
					if ( $q->param('print_items') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$tsampled[$i];
					}
					if ( $q->param('print_refs_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubsrefrichness[$i];
					}
					if ( $q->param('print_median') eq "YES" )	{
						$s = int(0.5*$trials)+1;
						print "<td class=tiny align=center valign=top>$outrichness[$i][$s] ";
					}
					if ( $q->param('print_ci') eq "YES" )	{
						$qq = int(0.1587*$trials)+1;
						$r = int(0.8413*$trials)+1;
						print "<td class=tiny align=center valign=top>$outrichness[$i][$qq]-$outrichness[$i][$r] ";
					}
					if ( $q->param('print_mean') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$meanoutrichness[$i];
					}

		# print assorted stats yielded by two timer analysis JA 23.8.04
					if ($q->param('diversity') =~ /two timers/)	{
						if ( $q->param('print_three_timers_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mthreetimers[$i];
						}
						if ( $q->param('print_corrected_bc') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mnewbc[$i];
						}
						if ( $q->param('print_estimated_midpoint') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mmidptdiv[$i];
						}
						if ( $q->param('print_estimated_mesa') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mmesa[$i];
						}
			# we want the raw standardized SIB data for comparison
			#  with the correction
						if ( $q->param('print_raw_sib') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsrichness[$i];
						}
						if ( $q->param('print_corrected_sib') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$mnewsib[$i];
						}
						if ( $q->param('print_origination_rate_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$lam[$i];
						}
						if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$mu[$i];
						}
					}
					elsif ($q->param('diversity') =~ /boundary-crossers/)	{
				# Foote origination rate
						if ( $q->param('print_first_appearances_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsoriginate[$i];
						}
						if ( $q->param('print_origination_rate_ss') eq "YES" )	{
							if ( $meanoutrichness[$i-1] > 0 && $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.4f ",log( $meanoutrichness[$i-1] / ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) );
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
						if ( $q->param('print_origination_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.1f ",$msubsoriginate[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
				# Foote extinction rate
						if ( $q->param('print_last_appearances_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsextinct[$i];
						}
						if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
							if ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] > 0 && $meanoutrichness[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.4f ",log( ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanoutrichness[$i] ) * -1;
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
						if ( $q->param('print_extinction_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.1f ",$msubsextinct[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
					} else	{
						if ( $q->param('print_origination_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.1f ",$msubsoriginate[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
						if ( $q->param('print_extinction_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf "<td class=tiny align=center valign=top>%.1f ",$msubsextinct[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print "<td class=tiny align=center valign=top>NaN ";
							}
						}
					}
					if ( $q->param('print_singletons_ss') eq "YES" )	{
						printf "<td class=tiny align=center valign=top>%.1f ",$msubssingletons[$i];
					}
					print SUB_TABLE $chrons - $i + 1;
					print SUB_TABLE ",$chname[$i]";
					if ( $q->param('print_base_ss') eq "YES" )	{
						printf SUB_TABLE ",%.1f",$basema{$chname[$i]};
					}
					if ( $q->param('print_midpoint_ss') eq "YES" )	{
						printf SUB_TABLE ",%.1f",( $basema{$chname[$i]} + $basema{$chname[$i-1]} ) / 2;
					}
#					printf SUB_TABLE ",%.1f",$msubsrichness[$i];
#					printf SUB_TABLE ",%.1f",$msubsrangethrough[$i];
					if ( $q->param('print_items') eq "YES" )	{
						printf SUB_TABLE ",%.1f",$tsampled[$i];
					}
					if ( $q->param('print_refs_ss') eq "YES" )	{
						printf SUB_TABLE ",%.1f",$msubsrefrichness[$i];
					}
					if ( $q->param('print_median') eq "YES" )	{
						print SUB_TABLE ",$outrichness[$i][$s]";
					}
					if ( $q->param('print_ci') eq "YES" )	{
						print SUB_TABLE ",$outrichness[$i][$qq]";
					}
					if ( $q->param('print_ci') eq "YES" )	{
						print SUB_TABLE ",$outrichness[$i][$r]";
					}
					if ( $q->param('print_mean') eq "YES" )	{
						printf SUB_TABLE ",%.1f",$meanoutrichness[$i];
					}
					if ($q->param('diversity') =~ /two timers/)	{
						if ( $q->param('print_three_timers_ss') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$mthreetimers[$i];
						}
						if ( $q->param('print_corrected_bc') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$mnewbc[$i];
						}
						if ( $q->param('print_estimated_midpoint') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$mmidptdiv[$i];
						}
						if ( $q->param('print_estimated_mesa') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$mmesa[$i];
						}
						if ( $q->param('print_raw_sib') eq "YES" )	{
							printf SUB_TABLE ",%.3f",$msubsrichness[$i];
						}
						if ( $q->param('print_corrected_sib') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$mnewsib[$i];
						}
						if ( $q->param('print_origination_rate_ss') eq "YES" )	{
							printf SUB_TABLE ",%.3f",$lam[$i];
						}
						if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
							printf SUB_TABLE ",%.3f",$mu[$i];
						}
					}
					elsif ($q->param('diversity') =~ /boundary-crossers/)	{
					# Foote origination rate
						if ( $q->param('print_first_appearances_ss') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$msubsoriginate[$i];
						}
						if ( $q->param('print_origination_rate_ss') eq "YES" )	{
							if ( $meanoutrichness[$i-1] > 0 && $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] > 0 )	{
								printf SUB_TABLE ",%.4f",log( $meanoutrichness[$i-1] / ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) );
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
						if ( $q->param('print_origination_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf SUB_TABLE ",%.1f",$msubsoriginate[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
					# Foote extinction rate
						if ( $q->param('print_last_appearances_ss') eq "YES" )	{
							printf SUB_TABLE ",%.1f",$msubsextinct[$i];
						}
						if ( $q->param('print_extinction_rate_ss') eq "YES" )	{
							if ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] > 0 && $meanoutrichness[$i] > 0 )	{
								printf SUB_TABLE ",%.4f",log( ( $meanoutrichness[$i] - $msubsextinct[$i] + $msubssingletons[$i] ) / $meanoutrichness[$i] ) * -1;
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
						if ( $q->param('print_extinction_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf SUB_TABLE ",%.1f",$msubsextinct[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
					} else	{
						if ( $q->param('print_origination_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf SUB_TABLE ",%.1f",$msubsoriginate[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
						if ( $q->param('print_extinction_percentage') eq "YES" )	{
							if ( $meanoutrichness[$i] > 0 )	{
								printf SUB_TABLE ",%.1f",$msubsextinct[$i] / $meanoutrichness[$i] * 100;
							} else	{
								print SUB_TABLE ",NaN";
							}
						}
					}
					if ( $q->param('print_singletons_ss') eq "YES" )	{
						printf SUB_TABLE ",%.1f",$msubssingletons[$i];
					}
					if ($gapstat > 0)	{
						if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.3f ",$gapstat;
							printf SUB_TABLE ",%.3f",$gapstat;
						}
						if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsrichness[$i] / $gapstat;
							printf SUB_TABLE ",%.3f",$msubsrichness[$i] / $gapstat;
						}
					}
					else	{
						if ( $q->param('print_gap_analysis_stat_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
						if ( $q->param('print_gap_analysis_estimate_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
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
					}
					else	{
						if ( $q->param('print_three_timer_stat_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
						if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
					}
					if ( $q->param('print_chao-2_ss') eq "YES" )	{
						if ($msubschaostat > 0 && $msubsrichness[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubschaostat;
							printf SUB_TABLE ",%.1f",$msubschaostat;
						} else    {
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
					}
					if ( $q->param('print_jolly-seber_ss') eq "YES" )	{
						if ($msubsjolly[$i] > 0 && $msubsrichness[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsjolly[$i];
							printf SUB_TABLE ",%.1f",$msubsjolly[$i];
						} else    {
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
					}
					if ( $q->param('print_ice_ss') eq "YES" )	{
						if ($msubsice[$i] > 0 && $msubsrichness[$i] > 0 )	{
							printf "<td class=tiny align=center valign=top>%.1f ",$msubsice[$i];
							printf SUB_TABLE ",%.1f",$msubsice[$i];
						} else    {
							print "<td class=tiny align=center valign=top>NaN ";
							print SUB_TABLE ",NaN";
						}
					}
					if ( $q->param('print_michaelis-menten') eq "YES" )	{
						if ( $samplingmethod == 2)	{
							if ($michaelis[$i] > 0 && $msubsrichness[$i] > 0 )	{
					  			printf "<td class=tiny align=center valign=top>%.1f ",$michaelis[$i];
					  			printf SUB_TABLE ",%.1d",$michaelis[$i];
							}
							else    {
								print "<td class=tiny align=center valign=top>NaN ";
								print SUB_TABLE ",NaN";
							}
						}
					}
					print "<p>\n";
					print SUB_TABLE "\n";
				}
			}
			print qq|</table>
</div>

<div class="small" style="padding-left: 2em; padding-right: 2em;">
|;
			print "The selected method was <b>".$q->param('samplingmethod')."</b>.<p>\n";
			print "The number of items selected per temporal bin was <b>".$q->param('samplesize')."</b>.<p>\n";
			if ( $q->param('ref_quota') > 0 )	{
				print "The maximum number of references selected per temporal bin was <b>".$q->param('ref_quota')."</b>.<p>\n";
			}
			print "The total number of trials was <b>".$q->param('samplingtrials')."</b>.<p>\n";
			if ( $threetimerp )	{
				printf "The gap proportion based on three timer analysis of the subsampled data is <b>%.3f</b>.<p>\n",$threetimerp;
			}
			if ( $q->param('print_three_timer_estimate_ss') eq "YES" )	{
				print "Corrected SIB, not raw SIB, was used for the three timer diversity estimate.<p>\n";
			}
            if ($q->param('stepsize') ne "")	{
		        print "\nThe following data files have been created:<p>\n";
            } else {
		        print "\nThe following data file has been created:<p>\n";
            }
		    print "<ul>\n";
		    print "<li>The subsampled diversity curve data (<a href=\"$HOST_URL$PRINTED_DIR/subsampled_curve_data.csv\">subsampled_curve_data.csv</a>)<p>\n";
            if ($q->param('stepsize') ne "")	{
                print "<li>The subsampling curves (<a href=\"$HOST_URL$PRINTED_DIR/subcurve.tab\">subcurve.tab</a>)<p>\n";
            }
    		print "</ul><p>\n";
		print "</div>\n";

            print '</div>'; # End PANEL2 div
            print '<script language="JavaScript" type="text/javascript">
                    showPanel(1);
                  </script> ';
		}
	
	
	}
	else	{
		print "\n<b>Sorry, the search failed.</b> No collections met the search criteria.<p>\n";
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

1;
