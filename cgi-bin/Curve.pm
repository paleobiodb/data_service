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

package Curve;

#require Text::CSV_XS;
# FOO ##  # CGI::Carp qw(fatalsToBrowser);

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $q;					# Reference to the parameters
my $s;

sub new {
	my $class = shift;
	$q = shift;
	$s = shift;
	my $self = {};

	bless $self, $class;
	return $self;
}

sub buildCurve {
	my $self = shift;

	$self->setArrays;
	$self->printHeader;
	$self->readRegions;
	if ($q->param('class') ne "")	{
	  $self->readOneClassFile;
	}
	elsif ($q->param('brachiopods') ne "" || $q->param('bryozoans') ne "" ||
	       $q->param('cephalopods') ne "" || $q->param('conodonts') ne "" ||
	       $q->param('corals') ne "" || $q->param('echinoderms') ne "" ||
	       $q->param('graptolites') ne "" || $q->param('molluscs') ne "" ||
	       $q->param('trilobites') ne "")	{
	  $self->readClassFiles;
	}
	# read the working time scale
	$self->readScale;
	# compute the sizes of intermediate steps to be reported in subsampling curves
	if ($q->param('stepsize') ne "")	{
	  $self->setSteps;
	}
	# match collections to sampling intervals
	$self->assignLocs;
	$self->assignGenera;
	$self->subsample;
	$self->printResults;
}

sub setArrays	{
	my $self = shift;

	# Setup the variables
	$DDIR="./data";
	$PUBLIC_DIR = $ENV{CURVE_PUBLIC_DIR};
	$CURVE_HOST = $ENV{CURVE_HOST};
	$CLASS_DATA_DIR = "$DDIR/classdata";
					                                # the default working directory
	$PRINTED_DIR = "/public/data";
	$BACKGROUND="/public/PDbg.gif";
	$LOCS_FILE="$DDIR/pmpd-locs";           # location of the primary collection data
	$OCCS_FILE="$DDIR/pmpd-occs";           # location of the occurences data
	$GEN_FILE="$DDIR/pmpd-genera";
	$ENTERERCOL=1;
	$LOCIDCOL=3;
	$PERIODCOL=24;
	$EPOCCOL=26;
	$ISTAGCOL1=28;
	$ISTAGCOL2=30;
	$LSTAGCOL1=32;
	$LSTAGCOL2=34;
	$FORMATIONCOL=36;
	$MEMBERCOL=37;
	$STRATSCALECOL=44;
	$LITH1COL=49;
	$LITH2COL=51;
    $CREATEDCOL=63;
	$RESGRPCOL=65;
	$OCCIDCOL=3;
	$GENCOL=6;
	$SPCOL=8;
	$ABCOL=9;
	
	$OUTPUT_DIR = $PUBLIC_DIR;
	# customize the subdirectory holding the output files
	# modified to retrieve authorizer automatically JA 4.10.02
	if ( $s->get('enterer') ne "Guest")	{
		my $temp = $s->get("authorizer");
		$temp =~ s/ //g;
		$temp =~ s/\.//g;
		$temp =~ tr/[A-Z]/[a-z]/;
		$OUTPUT_DIR .= "/" . $temp;
		$PRINTED_DIR .= "/" . $temp;
		mkdir $OUTPUT_DIR;
		chmod 0777, $OUTPUT_DIR;
	}
	
	$LTYPE{'siliciclastic'} = "siliciclastic";
	$LTYPE{'claystone'} = "siliciclastic";
	$LTYPE{'mudstone'} = "siliciclastic";
	$LTYPE{'shale'} = "siliciclastic";
	$LTYPE{'siltstone'} = "siliciclastic";
	$LTYPE{'sandstone'} = "siliciclastic";
	$LTYPE{'conglomerate'} = "siliciclastic";
	$LTYPE{'mixed carbonate-siliciclastic'} = "other";
	$LTYPE{'marl'} = "other";
	$LTYPE{'lime mudstone'} = "carbonate";
	$LTYPE{'wackestone'} = "carbonate";
	$LTYPE{'packstone'} = "carbonate";
	$LTYPE{'grainstone'} = "carbonate";
	$LTYPE{'reef rocks'} = "carbonate";
	$LTYPE{'floatstone'} = "carbonate";
	$LTYPE{'rudstone'} = "carbonate";
	$LTYPE{'bafflestone'} = "carbonate";
	$LTYPE{'bindstone'} = "carbonate";
	$LTYPE{'framestone'} = "carbonate";
	$LTYPE{'limestone'} = "carbonate";
	$LTYPE{'dolomite'} = "carbonate";
	$LTYPE{'carbonate'} = "carbonate";
	$LTYPE{'coal'} = "other";
	$LTYPE{'peat'} = "other";
	$LTYPE{'lignite'} = "other";
	$LTYPE{'subbituminous coal'} = "other";
	$LTYPE{'bituminous coal'} = "other";
	$LTYPE{'anthracite'} = "other";
	$LTYPE{'coal ball'} = "other";
	$LTYPE{'tar'} = "other";
	$LTYPE{'amber'} = "other";
	$LTYPE{'chert'} = "other";
	$LTYPE{'evaporite'} = "other";
	$LTYPE{'phosphorite'} = "other";
	$LTYPE{'ironstone'} = "other";
	$LTYPE{'siderite'} = "other";
	$LTYPE{'phyllite'} = "other";
	$LTYPE{'slate'} = "other";
	$LTYPE{'schist'} = "other";
	$LTYPE{'quartzite'} = "other";
	
	%ENVTYPE = ("(paralic indet.)" => "zone 1", "estuarine/bay" => "zone 1",
					    "lagoonal" => "zone 1",
					    "foreshore" => "zone 2", "shoreface" => "zone 2",
					    "transition zone/lower shoreface" => "zone 3",
					    "offshore" => "zone 4",
					    "delta plain" => "zone 1",
					    "interdistributary bay" => "zone 1", "delta front" => "zone 2",
					    "prodelta" => "zone 3", "(deep-water indet.)" => "zone 5",
					    "submarine fan" => "zone 5", "basinal (siliciclastic)" => "zone 5",
					    "peritidal" => "zone 1",
					    "shallow subtidal" => "zone 2", "sand shoal" => "zone 2",
					    "reef, buildup or bioherm" => "zone 2",
					    "deep subtidal ramp" => "zone 3",
					    "deep subtidal shelf" => "zone 3",
					    "(deep subtidal indet.)" => "zone 3",
					    "offshore ramp" => "zone 4", "offshore shelf" => "zone 4",
					    "(offshore indet.)" => "zone 4", "slope" => "zone 5",
					    "basinal (carbonate)" => "zone 5");
	if ($q->param('paleoenvironment') eq "zone 4")	{
		$ENVTYPE{'prodelta'} = "zone 4";
	}
	if ($q->param('samplingmethod') eq "classical rarefaction")	{
		$samplingmethod = 1;
	}
	elsif ($q->param('samplingmethod') eq "by-list subsampling (lists tallied)")	{
		$samplingmethod = 2;
	}
	elsif ($q->param('samplingmethod') eq "by-list subsampling (occurrences tallied)")	{
		$samplingmethod = 3;
	}
	elsif ($q->param('samplingmethod') eq "by-list subsampling (occurrences-squared tallied)")	{
		$samplingmethod = 4;
	}
	elsif ($q->param('samplingmethod') eq "by-specimen subsampling")	{
		$samplingmethod = 5;
	}

	if ( $q->param('year') )	{
		%month2num = (  "January" => "01", "February" => "02", "March" => "03",
                                "April" => "04", "May" => "05", "June" => "06",
                                "July" => "07", "August" => "08", "September" => "09",
                                "October" => "10", "November" => "11",
                                "December" => "12");
		if ( length $q->param('date') == 1 )	{
			$q->param(date => "0".$q->param('date') );
		}
		$created_date = $q->param('year').$month2num{$q->param('month')}.$q->param('date')."000000";
	}
}

sub printHeader	{
	my $self = shift;

	print "<html>\n<head><title>Paleobiology Database diversity curve report";
	if ($q->param('class') ne "")	{
		print ": ".$q->param('class');
	}
	print "</title></head>\n";
	print "<body bgcolor=\"white\" background=\"";
	print $BACKGROUND;
	print "\" text=black link=\"#0055FF\" vlink=\"#990099\">\n\n";

	print "<center>\n<h1>Paleobiology Database diversity curve report";
	if ($q->param('class') ne "")	{
		print ": ".$q->param('class');
	}
	print "</h1></center>\n";

}

sub readRegions {
	my $self = shift;

	if ( ! open REGIONS,"<$DDIR/PBDB.regions" ) {
		$self->htmlError ( "$0:Couldn't open $DDIR/PBDB.regions<BR>$!" );
	}
	while (<REGIONS>)	{
		s/\n//;
		($temp,$temp2) = split /:/, $_, 2;
		@countries = split /\t/,$temp2;
		for $country (@countries)	{
			$region{$country} = $temp;
		}
	}
	close REGIONS;

}

sub readOneClassFile	{
	my $self = shift;

	my $cleanclass = $q->param('class');
	$cleanclass =~ tr/[a-zA-Z0-9]/_/c;

	if ( ! open CLASSFILE,"<$CLASS_DATA_DIR/class.$cleanclass" ) {
		$self->htmlError ( "$0:Couldn't open $CLASS_DATA_DIR/class.$cleanclass<BR>$!" );
	}
	while (<CLASSFILE>)	{
		s/\n//;
		($genus,$_) = split(/,/,$_,2);
		$required{$genus} = "required";
	}
	close CLASSFILE;

}

sub readClassFiles	{
	my $self = shift;

	if ($q->param('brachiopods') ne "")	{
		@temp = ("Articulata", "Inarticulata", "Lingulata");
		for $t (@temp)	{
			$orderstatus{$t} = $q->param('brachiopods');
		}
		push @classes,@temp;
		$q->param('class' => "Brachiopoda");
	}
	if ($q->param('bryozoans') ne "")	{
		@temp = ("Gymnolaemata", "Stenolaemata");
		for $t (@temp)	{
			$orderstatus{$t} = $q->param('bryozoans');
		}
		push @classes,@temp;
		$q->param('class' => "Bryozoa");
	}
	if ($q->param('cephalopods') ne "")	{
		push @classes,"Cephalopoda";
		$orderstatus{"Cephalopoda"} = $q->param('cephalopods');
		$q->param('class' => "Cephalopoda");
	}
	if ($q->param('conodonts') ne "")	{
		push @classes,"Conodonta";
		$orderstatus{"Conodonta"} = $q->param('conodonts');
		$q->param('class' => "Conodonta");
	}
	if ($q->param('corals') ne "")	{
		push @classes,"Anthozoa";
		$orderstatus{"Anthozoa"} = $q->param('corals');
		$q->param('class' => "Anthozoa");
	}
	if ($q->param('echinoderms') ne "")	{
		@temp =  ("Asteroidea", "Blastoidea", "Camptostromoidea",
		 "Coronata", "Crinoidea", "Ctenocystoidea", "Diploporita",
		 "Echinoidea", "Edrioasteroidea", "Eocrinoidea", "Helicoplacoidea",
		 "Holothuroidea", "Homoiostelea", "Homostelea", "Ophiocistioidea",
		 "Ophiuroidea", "Parablastoidea", "Paracrinoidea", "Rhombifera",
		 "Somasteroidea", "Stylophora");
		for $t (@temp)	{
			$orderstatus{$t} = $q->param('echinoderms');
		}
		push @classes,@temp;
		$q->param('class' => "Echinodermata");
	}
	if ($q->param('graptolites') ne "")	{
		push @classes,"Graptolithina";
		$orderstatus{"Graptolithina"} = $q->param('graptolites');
		$q->param('class' => "Graptolithina");
	}
	if ($q->param('molluscs') ne "")	{
		@temp = ("Bivalvia", "Gastropoda", "Polyplacophora", "Scaphopoda",
					         "Tergomya", "Helcionelloida", "Paragastropoda",
					         "Rostroconchia", "Scaphopoda", "Tentaculitoidea");
		for $t (@temp)	{
			$orderstatus{$t} = $q->param('molluscs');
		}
		push @classes,@temp;
		$q->param('class' => "Mollusca (less Cephalopoda)");
	}
	if ($q->param('trilobites') ne "")	{
		push @classes,"Trilobita";
		$orderstatus{"Trilobita"} = $q->param('trilobites');
		$q->param('class' => "Trilobita");
	}
	
	# if ALL the classes are optional, then make all of them together
	#   one big "required" group (i.e., any of them is sufficient)
	$xx = 0;
	for $class (@classes)	{
		if ($orderstatus{$class} eq "required")	{
			$xx++;
		}
	}
	if ($xx == 0)	{
		for $class (@classes)	{
			$orderstatus{$class} = "required";
		}
	}
	$xx = 0;
	
	for $class (@classes)	{
		if ( ! open CLASSIF, "<$CLASS_DATA_DIR/class.$class" ) {
			$self->htmlError ( "$0:Couldn't open $CLASS_DATA_DIR/class.$class<BR>$!" );
		}
		while (<CLASSIF>)	{
			s/\n//;
			@temp = split(/,/,$_,2);
		# distinguish genera belonging to required and allowed orders
			$required{$temp[0]} = $orderstatus{$class};
		}
		close CLASSIF;
	}
}

# read whatever time scale will be used
sub readScale	{
	my $self = shift;

	if ($q->param('scale') eq "Harland epochs")	{
		if ( ! open SCALE,"<$PUBLIC_DIR/harland.epochs" ) {
			$self->htmlError ( "$0:Couldn't open $PUBLIC_DIR/harland.epochs<BR>$!" );
		}
		while (<SCALE>)	{
			s/\n//;
			s/^ //;
			@words = split / /,$_;
			if ($words[0] eq "E")	{
		 # use last stage name encountered as first stage of preceding epoch
				$longstartstage[$chrons] = $laststage;
				$startstage[$chrons] = $laststage;
				$startstage[$chrons] =~ s/ian$//;
				$startstage[$chrons] =~ s/Early /Early\/Lower /;
				$startstage[$chrons] =~ s/Late /Late\/Upper /;
				$startstage[$chrons] =~ s/^Lower /Early\/Lower /;
				$startstage[$chrons] =~ s/^Upper /Late\/Upper /;
		 # increment epoch counter
				$chrons++;
				$chname[$chrons] = $words[1];
				if ($words[1] eq "Early" || $words[1] eq "Middle" ||
					  $words[1] eq "Late" || $words[1] eq "Lower" ||
					  $words[1] eq "Upper")	{
					$chname[$chrons] = $chname[$chrons]." ".$words[2];
				}
		 # store epoch name so it can be used as last stage name
				$laststage = $chname[$chrons];
			}
			elsif ($words[0] eq "S")	{
				if ($words[1] ne "-")	{
					$laststage = $words[1];
					if ($words[1] eq "Early" || $words[1] eq "Middle" ||
					    $words[1] eq "Late" || $words[1] eq "Lower" ||
					    $words[1] eq "Upper")	{
					  $laststage = $laststage." ".$words[2];
					}
				}
			}
		}
		close SCALE;
		$longstartstage[$chrons] = $laststage;
		$startstage[$chrons] = $laststage;
		$startstage[$chrons] =~ s/ian$//;
		$startstage[$chrons] =~ s/Early /Early\/Lower /;
		$startstage[$chrons] =~ s/Late /Late\/Upper /;
		$startstage[$chrons] =~ s/^Lower /Early\/Lower /;
		$startstage[$chrons] =~ s/^Upper /Late\/Upper /;
	}
	elsif ($q->param('scale') eq "stages" ||
				 $q->param('scale') eq "10 m.y. intervals" ||
				 $q->param('scale') eq "Sepkoski intervals")	{
		if ($q->param('scale') eq "Sepkoski intervals")	{
			if ( ! open SCALE,"<$PUBLIC_DIR/timescale.Sepkoski" ) {
				$self->htmlError ( "$0:Couldn't open $PUBLIC_DIR/timescale.Sepkoski<BR>$!" );
			}
		}
		elsif ($q->param('scale') eq "10 m.y. intervals")	{
			if ( ! open SCALE,"<$PUBLIC_DIR/timescale.10my" ) {
				$self->htmlError ( "$0:Couldn't open $PUBLIC_DIR/timescale.10my<BR>$!" );
			}
		}
		else	{
			if ( ! open SCALE,"<$PUBLIC_DIR/timescale.stages" ) {
				$self->htmlError ( "$0:Couldn't open $PUBLIC_DIR/timescale.stages<BR>$!" );
			}
		}
		while (<SCALE>)	{
			s/\n//;
			$chrons++;
			if ($q->param('scale') ne "stages")	{
				($chname[$chrons],$longstartstage[$chrons],$basema[$chrons]) = split(/\t/,$_,3);
				$midptma[$chrons] = ($basema[$chrons] + $lastma) / 2;
				$lastma = $basema[$chrons];
				$startstage[$chrons] = $longstartstage[$chrons];
			}
			else	{
				$chname[$chrons] = $_;
				$startstage[$chrons] = $_;
			}
			$startstage[$chrons] =~ s/ian$//;
			$startstage[$chrons] =~ s/Early /Early\/Lower /;
			$startstage[$chrons] =~ s/Late /Late\/Upper /;
			$startstage[$chrons] =~ s/^Lower /Early\/Lower /;
			$startstage[$chrons] =~ s/^Upper /Late\/Upper /;
		}
		close SCALE;
		if ($chname[$chrons] eq "")	{
			$chrons--;
		}
		$upto = 1;
		$lastchron = $chname[1];
	}
	else	{
		print "Fatal error: scale not specified correctly.<p>\n";
		exit(0);
	}
	
	# make a lookup table of epochs, international stages, and local stages
	#   referring to a numbered list of international stages
	
	if ( ! open SCALE,"<$PUBLIC_DIR/harland.epochs" ) {
		$self->htmlError ( "$0:Couldn't open $PUBLIC_DIR/harland.epochs<BR>$!" );
	}
	# WARNING!!! when updating field numbers, make sure to change
	#   the following (i.e., ID number of epoch field)
	$upto = 1;
	while (<SCALE>)	{
		s/\n//;
		($rank,$timeterms) = split(' ',$_,2);
	
	# if line describes an international stage...
		if ($rank eq "S")	{
			$nstage++;
			$belongsto[$nstage] = $upto;
			@synonym = ();
		# modify Early or Late terms
			$timeterms =~ s/Early /Early\/Lower /g;
			$timeterms =~ s/Late /Late\/Upper /g;
			($atstage,@synonym) = split(/ = /,$timeterms);
			if ($atstage eq "-")	{
				$atstage = $lastepoch;
			}
		# WARNING: strips "ian" so later parsing assumes this is absent!
			$longatstage = $atstage;
			$atstage =~ s/ian$//;
			$minstage{$atstage} = $nstage;
			$maxstage{$atstage} = $nstage;
		# if youngest stage of bin has not yet been defined, it is this stage
			if ($endstage[$upto] eq "")	{
				$endstage[$upto] = $longatstage;
			}
		# if stage defines the beginning of a bin, increment bin count
			if ($atstage eq $startstage[$upto])	{
				$upto++;
			}
		# define stage ID number of local stages
			for $syn (@synonym)	{
				$syn =~ s/ian$//;
				if ($syn ne "")	{
					if ($minstage{$syn} eq "")	{
					  $minstage{$syn} = $nstage;
					  $maxstage{$syn} = $nstage;
					}
					else	{
					  $maxstage{$syn} = $nstage;
			 # if local stage spans multiple international stages, define mapping
			 #    of its E/M/L substages
					  $x = $maxstage{$syn} - $minstage{$syn} + 1;
					  $subst = "Late/Upper ".$syn;
					  $y = int(($x-1)/3);
					  $minstage{$subst} = $minstage{$syn};
					  $maxstage{$subst} = $minstage{$syn} + $y;
					  $subst = "Middle ".$syn;
					  $y = int($x/3);
					  $minstage{$subst} = $minstage{$syn} + $y;
					  $maxstage{$subst} = $maxstage{$syn} - $y;
					  $subst = "Early/Lower ".$syn;
					  $y = int(($x-1)/3);
					  $minstage{$subst} = $maxstage{$syn} - $y;
					  $maxstage{$subst} = $maxstage{$syn};
					}
				}
			}
		}
	 # if line describes an epoch...
		elsif ($rank eq "E")	{
			@synonym = ();
			@synonym = split(/ = /,$timeterms);
			$minstage{$synonym[0]} = $nstage+1;
			for $syn (@synonym)	{
				$minstage{$syn} = $nstage+1;
			}
			for $syn (@lastsyn)	{
				$maxstage{$syn} = $nstage;
		# define stage mappings of E/M/L subepochs
				$x = $maxstage{$lastepoch} - $minstage{$lastepoch} + 1;
				$y = int(($x-1)/3);
				$subep = "Late/Upper ".$syn;
				$minstage{$subep} = $minstage{$syn};
				$maxstage{$subep} = $minstage{$syn} + $y;
				$subep = "Middle ".$syn;
				$y = int($x/3);
				$minstage{$subep} = $minstage{$syn} + $y;
				$maxstage{$subep} = $maxstage{$syn} - $y;
				$subep = "Early/Lower ".$syn;
				$y = int(($x-1)/3);
				$minstage{$subep} = $maxstage{$syn} - $y;
				$maxstage{$subep} = $maxstage{$syn};
			}
			$lastepoch = $synonym[0];
			@lastsyn = @synonym;
		}
	}
	$maxstage{$epoch} = $nstage;
	close SCALE;

}

# compute the sizes of intermediate steps to be reported in subsampling curves
sub setSteps	{
	my $self = shift;

	my $i = 1;
	if ($q->param('stepsize') eq "100, 200, 300..." ||
			$q->param('stepsize') eq "50, 100, 150..." ||
			$q->param('stepsize') eq "20, 40, 60...")	{
		$i = 0;
	}
	$atstep[0] = 0;
	$atstep[1] = 1;
	$wink = 0;
	while ($i <= $q->param('samplesize') * 5)	{
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
		}
		elsif ($q->param('stepsize') eq "100, 200, 300...")	{
			$i = $i + 100;
		}
		elsif ($q->param('stepsize') eq "50, 100, 150...")	{
			$i = $i + 50;
		}
		elsif ($q->param('stepsize') eq "20, 40, 60...")	{
			$i = $i + 20;
		}
		for $j ($x..$i-1)	{
			$atstep[$j] = $#samplesteps + 1;
		}
		$stepback[$#samplesteps+1] = $i;
	}
}

# determine the chron that includes each collection
sub assignLocs	{
	my $self = shift;

	if ( ! open LOCS,"<$LOCS_FILE" ) {
		$self->htmlError ( "$0:Couldn't open $LOCS_FILE<BR>$!" );
	}
	if ( ! open ORPHANS,">$OUTPUT_DIR/orphans.txt" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/orphans.txt<BR>$!" );
	}
	
	$lithquery1 = $q->param('lithology1');
	$lithquery1 =~ s/all //g;
	$lithquery1 =~ s/ lithologies//g;
	$lithquery2 = $q->param('lithology2');
	$lithquery2 =~ s/all //g;
	$lithquery2 =~ s/ lithologies//g;
	
	$csv = Text::CSV_XS->new();
	while (<LOCS>)	{
	 # clean up funny accented characters JA 30.10.01
		s/à/a/g; # a grave
		s/â/a/g; # a funny hat
		s/é/e/g; # e acute
		s/è/e/g; # e grave
		s/ê/e/g; # e funny hat
		s/í/i/g; # i acute
        s/ï/i/g; # i umlaut
		s/î/i/g; # i funny hat
		s/ó/o/g; # o acute
		s/ô/o/g; # o funny hat
		s/ú/u/g; # u acute
		s/ö/o/g; # u umlaut
		s/ü/u/g; # u umlaut
		s/ñ/n/g; # n tilde
		s/°/deg/g; # degree symbol
        s/ //g; # dagger
        s/…//g; # some damn thang
        s/[^" \+\(\)\-=A-Za-z0-9,\.\/]//g;
		if ( $csv->parse($_) )	{
			@columns = $csv->fields();
	
			$rawagedata = $columns[$PERIODCOL]." ".$columns[$EPOCCOL]." ".$columns[$ISTAGCOL1]." ".$columns[$ISTAGCOL2]." ".$columns[$LSTAGCOL1]." ".$columns[$LSTAGCOL2];
	
			if ($q->param('North America') eq "Y" || $q->param('Europe') eq "Y" ||
					$q->param('South America') eq "Y" || $q->param('Africa') eq "Y" ||
					$q->param('Antarctica') eq "Y" ||
					$q->param('Asia') eq "Y" || $q->param('Australia') eq "Y")	{
	
		# clean country field
				$columns[8] =~ s/  / /g;
				$columns[8] = "STARTFIELD".$columns[8]."ENDFIELD";
				$columns[8] =~ s/STARTFIELD //g;
				$columns[8] =~ s/STARTFIELD//g;
				$columns[8] =~ s// ENDFIELD/g;
				$columns[8] =~ s/ENDFIELD//g;
				
				if ($columns[8] eq "" || $columns[8] eq "U.S.A." ||
					  $columns[8] eq "USA") {
					$columns[8]="United States";
				}
	
	
		# toss lists from regions that are not included
			 if (($q->param('North America') ne "Y") &&
					 ($region{$columns[8]} eq "North America"))	{
				 $columns[$LOCIDCOL] = 0;
			 }
			 elsif ($q->param('South America') ne "Y" &&
					    $region{$columns[8]} eq "South America")	{
				 $columns[$LOCIDCOL] = 0;
			 }
			 elsif ($q->param('Europe') ne "Y" &&
					    $region{$columns[8]} eq "Europe")	{
				 $columns[$LOCIDCOL] = 0;
			 }
			 elsif ($q->param('Africa') ne "Y" &&
					    $region{$columns[8]} eq "Africa")	{
				 $columns[$LOCIDCOL] = 0;
			 }
			 elsif ($q->param('Asia') ne "Y" &&
					    $region{$columns[8]} eq "Asia")	{
				 $columns[$LOCIDCOL] = 0;
			 }
			 elsif ($q->param('Australia') ne "Y" &&
					    $region{$columns[8]} eq "Australia")	{
				 $columns[$LOCIDCOL] = 0;
			 }
				elsif ($region{$columns[8]} eq "")	{
					$columns[$LOCIDCOL] = 0;
			 }
		 }
	
		# save country name to be printed in binning.csv file
		 if ($columns[$LOCIDCOL] > 0)	{
			 $country[$columns[$LOCIDCOL]] = $columns[8];
		 }
	
	 # toss lists with no county or lat/long
			if (($q->param('strictgeography') eq "no, exclude them") && ($columns[10] eq "") &&
					(($columns[11] eq "") || ($columns[16] eq "")))	{
				$columns[$LOCIDCOL] = 0;
		 }
	
			if ($q->param('strictchronology') eq "no, exclude them")	{
	 # toss lists with no age/stage data at all
				if (($columns[$ISTAGCOL1] eq "") && ($columns[$ISTAGCOL2] eq "") &&
					($columns[$LSTAGCOL1] eq "") && ($columns[$LSTAGCOL2] eq ""))	{
					$columns[$LOCIDCOL] = 0;
			 }
	 # toss lists with a range of possible international age/stage values
				elsif (($columns[$ISTAGCOL1] ne "") && ($columns[$ISTAGCOL2] ne "") &&
					     ($columns[$ISTAGCOL1] ne $columns[$ISTAGCOL2]))	{
					$columns[$LOCIDCOL] = 0;
				}
	 # toss lists with a range of possible local age/stage values
				elsif (($columns[33] ne "") && ($columns[$LSTAGCOL2] ne "") &&
					     ($columns[$LSTAGCOL1] ne $columns[$LSTAGCOL2]))	{
					$columns[$LOCIDCOL] = 0;
				}
			}

	 # toss lists with excluded creation dates
			if ( $created_date > 0 )	{
			# clean up the creation date field
				my $date = $columns[$CREATEDCOL];
				$date =~ s/[ \-:]//g;
				if ( $q->param('created_before_after') eq "before" &&
					 $date > $created_date )	{
					$columns[$LOCIDCOL] = 0;
				} elsif ( $q->param('created_before_after') eq "after" &&
					 $date < $created_date )	{
					$columns[$LOCIDCOL] = 0;
				}
			}
	
	 # toss lists with broad scale of resolution
			if ($q->param('stratscale') ne "")	{
				$ao = 0;
				if ($columns[$STRATSCALECOL] =~ /group of bed/)	{
					$ao = 3;
				}
				elsif ($columns[$STRATSCALECOL] =~ /^bed$/)	{
					$ao = 4;
				}
				elsif ($columns[$STRATSCALECOL] =~ /member/)	{
					$ao = 2;
				}
				elsif ($columns[$STRATSCALECOL] =~ /formation/)	{
					$ao = 1;
				}
				if ($q->param('stratscale') =~ /group of bed/ &&
					  ($q->param('stratscale_minmax') eq "broadest" && $ao < 3 ||
					   $q->param('stratscale_minmax') eq "narrowest" && $ao > 3))	{
					$columns[$LOCIDCOL] = 0;
				}
				elsif ($q->param('stratscale') =~ /^bed$/ &&
					  ($q->param('stratscale_minmax') eq "broadest" && $ao < 4 ||
					   $q->param('stratscale_minmax') eq "narrowest" && $ao > 4))	{
					$columns[$LOCIDCOL] = 0;
				}  
				elsif ($q->param('stratscale') =~ /member/ &&
					  ($q->param('stratscale_minmax') eq "broadest" && $ao < 2 ||
					   $q->param('stratscale_minmax') eq "narrowest" && $ao > 2))	{
					$columns[$LOCIDCOL] = 0;
				}  
				elsif ($q->param('stratscale') =~ /formation/ &&
					  ($q->param('stratscale_minmax') eq "broadest" && $ao == 0 ||
					   $q->param('stratscale_minmax') eq "narrowest" && $ao > 1))	{
					$columns[$LOCIDCOL] = 0;
				}
			}
	 # toss lists without formation data if this is required
			if ($q->param('requiredfm') ne "" && $columns[$FORMATIONCOL] eq "")	{
				$columns[$LOCIDCOL] = 0;
			}
	 # toss lists without member data if this is required
			if ($q->param('requiredmbr') ne "" && $columns[$MEMBERCOL] eq "")	{
				$columns[$LOCIDCOL] = 0;
			}
	
			if ($q->param('lithology1') ne "")	{
				if ($columns[$LITH1COL] eq "" && $columns[$LITH2COL] eq "")	{
					$columns[$LOCIDCOL] = 0;
				}
			# evaluate all... lithologies
				elsif ($q->param('lithology1') =~ /^all /)	{
				 $lithdata = $LTYPE{$columns[$LITH1COL]}.$LTYPE{$columns[$LITH2COL]};
			 # if two non-null lithologies are present and query is exactly one
			 #   lithology, toss the list
					if (($q->param('lithonlyor') eq "equal") &&
					    ($LTYPE{$columns[$LITH1COL]} ne $LTYPE{$columns[$LITH2COL]}) &&
					    ($LTYPE{$columns[$LITH1COL]} ne "") &&
					    ($LTYPE{$columns[$LITH2COL]} ne ""))	{
					  $columns[$LOCIDCOL] = 0;
					}
			# toss list if query is exact and category doesn't match
			# WARNING: assumes that if only one lithology is present, the submit
			#  script has correctly placed it in the primary lithology field
					elsif (($q->param('lithonlyor') eq "equal") &&
					       ($LTYPE{$columns[$LITH1COL]} eq "carbonate" || $LTYPE{$columns[$LITH1COL]} eq "other") &&
					       ($q->param('lithology1') eq "all siliciclastic lithologies"))	{
					  $columns[$LOCIDCOL] = 0;
					}
					elsif (($q->param('lithonlyor') eq "equal") &&
					       ($LTYPE{$columns[$LITH1COL]} eq "siliciclastic" || $LTYPE{$columns[$LITH1COL]} eq "other") &&
					       ($q->param('lithology1') eq "all carbonate lithologies"))	{
					  $columns[$LOCIDCOL] = 0;
					}
					elsif (($q->param('lithonlyor') eq "include") &&
					       ($LTYPE{$columns[$LITH1COL]} ne "siliciclastic") &&
					       ($LTYPE{$columns[$LITH2COL]} ne "siliciclastic") &&
					       ($q->param('lithology1') eq "all siliciclastic lithologies"))	{
					  $columns[$LOCIDCOL] = 0;
					}
					elsif (($q->param('lithonlyor') eq "include") &&
					       ($LTYPE{$columns[$LITH1COL]} ne "carbonate") &&
					       ($LTYPE{$columns[$LITH2COL]} ne "carbonate") &&
					       ($q->param('lithology1') eq "all carbonate lithologies"))	{
					  $columns[$LOCIDCOL] = 0;
					}
					elsif ($q->param('lithonlyor') eq "combine" &&
					       ($lithdata !~ $lithquery1 ||
					        $lithdata !~ $lithquery2))	{
					  $columns[$LOCIDCOL] = 0;
					}
				}
	 # evaluate uncombined (i.e., not all...) lithologies
				else	{
					$lithdata = $columns[$LITH1COL].$columns[$LITH2COL];
	 # for "equal" lith searches, toss lists with any other lithology
	 # WARNING: assumes first pull-down is used and second isn't
					if (($q->param('lithonlyor') eq "equal") &&
					    ($lithdata ne $q->param('lithology1')))	{
					  $columns[$LOCIDCOL] = 0;
					}
	 # for "include" lith searches, toss lists that entirely lack the lithology
	 # WARNING: ditto
					elsif ($q->param('lithonlyor') eq "include" &&
					       $lithdata !~ $q->param('lithology1'))	{
					  $columns[$LOCIDCOL] = 0;
					}
	 # for "combine" lith searches, toss lists that lack either search lithology
					elsif ($q->param('lithonlyor') eq "combine" &&
					       ($lithdata !~ $q->param('lithology1') ||
					        $lithdata !~ $q->param('lithology2') ||
					        length $lithdata != length($q->param('lithology1')) +
					        length($q->param('lithology2'))))	{
					  $columns[$LOCIDCOL] = 0;
					}
				}
			}
	 # if a category of paleoenvironments is specified and this collection does
	 #   not fall in that category, exclude it
			if ($q->param('paleoenvironment') ne "")	{
				if ($q->param('paleoenvironment') ne $ENVTYPE{$columns[53]})	{
					$columns[$LOCIDCOL] = 0;
				}
			}
	 # toss lists from other research groups if only one is used
			if ($q->param('research_group') ne $columns[$RESGRPCOL] &&
				$q->param('research_group') ne "")	{
				$columns[$LOCIDCOL] = 0;
			}
	
	 # ASSIGN COLLECTION TO TEMPORAL BIN
			if ($columns[$LOCIDCOL] > 0)	{
				$stagemin = "";
				$stagemax = "";
	 # start by trying to use stage data
	 # first move local age/stage data into international age/stage fields
				if (($columns[$ISTAGCOL1] eq "") && ($columns[$LSTAGCOL1] ne ""))	{
					$columns[$ISTAGCOL1] = $columns[$LSTAGCOL1];
				}
				if (($columns[$ISTAGCOL2] eq "") && ($columns[$LSTAGCOL2] ne ""))	{
					$columns[$ISTAGCOL2] = $columns[$LSTAGCOL2];
				}
		 # duplicate the min/max stage name if it is empty but max/min is known
				if (($columns[$ISTAGCOL2] eq "") && ($columns[$ISTAGCOL1] ne ""))	{
					$columns[$ISTAGCOL2] = $columns[$ISTAGCOL1];
					$columns[$ISTAGCOL2-1] = $columns[$ISTAGCOL1-1];
				}
				elsif (($columns[$ISTAGCOL1] eq "") && ($columns[$ISTAGCOL2] ne ""))	{
					$columns[$ISTAGCOL1] = $columns[$ISTAGCOL2];
					$columns[$ISTAGCOL1-1] = $columns[$ISTAGCOL2-1];
				}
				if ($columns[$ISTAGCOL1] ne "")	{
					$c1temp = "";
					$c1temp = $columns[$ISTAGCOL1];
					$c1temp =~ s/\? //;
					$c1temp =~ s/ \?//;
					$c1temp =~ s/\?//;
					$c1temp =~ s/ian$//;
		 # use max value if it exists because this is the maximum field
		 # first try prepending the E/M/L data 
					if ($columns[$ISTAGCOL1-1] ne "")	{
					  $prec1temp = $columns[$ISTAGCOL1-1]." ".$c1temp;
					  if ($maxstage{$prec1temp} ne "")	{
					    $stagemax = $maxstage{$prec1temp};
					    $stagemin = $minstage{$prec1temp};
					  }
					}
					if ($maxstage{$c1temp} ne "")	{
					  if ($stagemax eq "" || $stagemax < $maxstage{$c1temp})	{
					    $stagemax = $maxstage{$c1temp};
					  }
					  if ($stagemin eq "" || $stagemin > $minstage{$c1temp})	{
					    $stagemin = $minstage{$c1temp};
					  }
					}
				}
				if ($columns[$ISTAGCOL2] ne "")	{
					$c2temp = "";
					$c2temp = $columns[$ISTAGCOL2];
					$c2temp =~ s/\? //;
					$c2temp =~ s/ \?//;
					$c2temp =~ s/\?//;
					$c2temp =~ s/ian$//;
					if ($columns[$ISTAGCOL2-1] ne "")	{
					  $prec2temp = $columns[$ISTAGCOL2-1]." ".$c2temp;
					  if ($stagemax eq "" || $stagemax < $maxstage{$prec2temp})	{
					    $stagemax = $maxstage{$prec2temp};
					  }
					  if ($stagemin eq "" || $stagemin > $minstage{$prec2temp})	{
					    $stagemin = $minstage{$prec2temp};
					  }
					}
					if ($minstage{$c2temp} ne "")	{
					  if ($stagemax eq "" || $stagemax < $maxstage{$c2temp}) 	{
					    $stagemax = $maxstage{$c2temp};
					  }
					  if ($stagemin eq "" || $stagemin > $minstage{$c2temp})	{
					    $stagemin = $minstage{$c2temp};
					  }
					}
				}
		 # use max data for min and vice versa
				if ($stagemax ne "" && $stagemin eq "")	{
					$stagemin = $stagemax;
				}
				elsif ($stagemin ne "" && $stagemax eq "")	{
					$stagemax = $stagemin;
				}
			# if none of that works but there is stage data, the stage is bogus
				if ($stagemax eq "" && $columns[$ISTAGCOL1] ne "")	{
					if (!$badnames{$columns[$ISTAGCOL1]})	{
					  $badnames{$columns[$ISTAGCOL1]} = "<i>$columns[$ENTERERCOL]" . ":</i>";
					}
					$badnames{$columns[$ISTAGCOL1]} .= " $columns[$LOCIDCOL]";
				}
				if ($stagemin eq "" && $columns[$ISTAGCOL2] ne "")	{
					if (!$badnames{$columns[$ISTAGCOL2]})	{
					  $badnames{$columns[$ISTAGCOL2]} = "<i>$columns[$ENTERERCOL]" . ":</i>" ;
					}
					if ($badnames{$columns[$ISTAGCOL2]} !~ / $columns[$LOCIDCOL]$/)	{
					  $badnames{$columns[$ISTAGCOL2]} .= " $columns[$LOCIDCOL]";
					}
				}
	 # IF STAGE DATA FAIL, TRY EPOCH DATA
				if ($columns[$EPOCCOL] ne "" && $stagemin eq "")	{
		 # find the stage min/max for this epoch
					if ($columns[$EPOCCOL-1] ne "")	{
					  @parts = ();
				# if E/M/L has more than one value...
					  @parts = split(/ - /,$columns[$EPOCCOL-1]);
					  if ($#parts > 0)	{
					    $minterm = $parts[1]." ".$columns[$EPOCCOL];
					    $maxterm = $parts[0]." ".$columns[$EPOCCOL];
					  }
				# but if E/M/L is a single word...
					  else	{
					    $minterm = $parts[0]." ".$columns[$EPOCCOL];
					    $maxterm = $parts[0]." ".$columns[$EPOCCOL];
					  }
					}
					else	{
					  $minterm = $columns[$EPOCCOL];
					  $maxterm = $columns[$EPOCCOL];
					}
					$stagemin = $minstage{$minterm};
					$stagemax = $maxstage{$maxterm};
			 # if that didn't work, try variations on the name
					if ($stagemin eq "")	{
					  $temp = $minterm;
					  $temp2 = $minterm;
					  $temp =~ s/ian$//;
					  $temp2 =~ s/ian$//;
					  $stagemin = $minstage{$temp};
					  $stagemax = $maxstage{$temp2};
					}
					if ($stagemin eq "")	{
					  $temp = $minterm;
					  $temp2 = $minterm;
					  $temp = $temp."ian";
					  $temp2 = $temp2."ian";
					  $stagemin = $minstage{$temp};
					  $stagemax = $maxstage{$temp2};
					}
				}
		 # if the "epoch" is just Lower/Middle/Upper then add the period name to it
		 # WARNING: this may or may not be useful if the scale = epochs option is
		 #   ever debugged, but right now it does nothing useful
			 #  if ((index($columns[$EPOCCOL],"Lower") > -1) ||
			 #      (index($columns[$EPOCCOL],"Early") > -1))	{
			 #    $columns[$EPOCCOL] = "Lower"." ".$columns[$PERIODCOL];
			 #  }
			 #  elsif (index($columns[$EPOCCOL],"Middle") > -1)	{
			 #    $columns[$EPOCCOL] = "Middle"." ".$columns[$PERIODCOL];
			 #  }
			 #  elsif ((index($columns[$EPOCCOL],"Upper") > -1) ||
			 #         (index($columns[$EPOCCOL],"Late") > -1))	{
			 #    $columns[$EPOCCOL] = "Upper"." ".$columns[$PERIODCOL];
			 #  }
			 #  if ($sensyn{$columns[$EPOCCOL]} ne "")	{
			 #    $columns[$EPOCCOL] = $sensyn{$columns[$EPOCCOL]};
			 #  }
		# END BINNING 
		# if list is assignable, do so
				if ($belongsto[$stagemax] == $belongsto[$stagemin] && $stagemin ne "" &&
					  ($columns[$FORMATIONCOL] ne "" || $q->param('lumpbyfm') ne "Y"))	{
		 # if using the geographic dispersion algorithm, increment the number of
		 #   lists in this country, state, and county or lat/long combination
					$i = $belongsto[$stagemin];
			 # assign formation ID number
					if ($q->param('lumpbyfm') eq "Y")	{
					  $columns[$FORMATIONCOL] =~ s/ fm$//i;
					  $columns[$FORMATIONCOL] =~ s/ fm\.$//i;
					  $columns[$FORMATIONCOL] =~ s/ formation$//i;
					  $columns[$FORMATIONCOL] =~ tr/A-Z/a-z/;
				# append the chron ID number so each formation-chron combo is unique
					  $columns[$FORMATIONCOL] = $columns[$FORMATIONCOL].$i;
					  if ($formationID{$columns[$FORMATIONCOL]} eq "")	{
					    $nformations++;
					    $formationID{$columns[$FORMATIONCOL]} = $nformations;
					  }
					  $formation[$columns[$LOCIDCOL]] = $formationID{$columns[$FORMATIONCOL]};
				 # WARNING! this irrevocably modifies the collection ID number
					  $columns[$LOCIDCOL] = $formationID{$columns[$FORMATIONCOL]};
					}
	
					$chid[$columns[$LOCIDCOL]] = $i;
	
	if ($columns[$STRATSCALECOL] =~ /group of/)	{
	$sscale[$columns[$LOCIDCOL]] = 3;
	}
	elsif ($columns[$STRATSCALECOL] =~ /bed/)	{
	$sscale[$columns[$LOCIDCOL]] = 4;
	}
	elsif ($columns[$STRATSCALECOL] =~ /member/)	{
	$sscale[$columns[$LOCIDCOL]] = 2;
	}
	elsif ($columns[$STRATSCALECOL] =~ /form/)	{
	$sscale[$columns[$LOCIDCOL]] = 1;
	}
	else	{
	$sscale[$columns[$LOCIDCOL]] = 0;
	}
	
					if ($q->param('disperse') eq "yes")	{
					  $temp = $i.$columns[8].$columns[9];
					  if ($columns[10] ne "")	{
					    $temp = $temp.$columns[10];
					  }
					  else	{
					    $temp = $temp.$columns[11].$columns[15].$columns[16].$columns[20];
					  }
					  $locsatpoint{$temp}++;
					  $locpoint[$columns[$LOCIDCOL]] = $temp;
					}
				}
	 # if collection cannot be placed in an interval, print its temporal
	 #   data to a complaint file
				else	{
					$rawagedata =~ s/  / /g;
					print ORPHANS "$columns[$LOCIDCOL]\t$stagemin - $stagemax\t$columns[6]\t$columns[8] $columns[9]\t$rawagedata\n";
				}
			}
		}
		else	{
			$err = $csv->error_input;
			print "Can't read collection record as follows: ", $err, "\n";
		}
	}
	close LOCS;
	close ORPHANS;

}


sub assignGenera	{
	my $self = shift;

	if ( ! open GEN,"<$GEN_FILE" ) {
		$self->htmlError ( "$0:Couldn't open $GEN_FILE for read<BR>$!" );
	}
	while (<GEN>)	{
		s/\n//;
		$ngen++;
		$genid{$_} = $ngen;
		$genus[$ngen] = $_;
	}
	close GEN;
	
	# replacements suggested by Ederer 12.7.01
	$csv = Text::CSV_XS->new({
			'quote_char'  => '"',
			'escape_char' => '"',
			'sep_char'    => ',',
			'binary'      => 1
	});
	
	if ( ! open GEN,">>$GEN_FILE" ) {
		#$self->htmlError ( "$0:Couldn't open $GEN_FILE for append.<BR>$!" );
		print "Couldn't open $GEN_FILE for append.<BR>\n";
	}
	
	# when using a genus count cutoff for lists, the number of
	#   genera in each list must be counted ahead of time
		if ( ! open OCCS,"<$OCCS_FILE" ) {
			$self->htmlError ( "$0:Couldn't open $OCCS_FILE<BR>$!" );
		}
		while (<OCCS>)  {
			if ( $csv->parse($_) )        {
				@columns = $csv->fields();
	
		# get rid of records with no specimen/individual counts for method 5
				if ($samplingmethod == 5)   {
					if ($columns[$ABCOL] eq "" || $columns[$ABCOL] == 0 ||
					    ($columns[10] ne "specimens" && $columns[10] ne "individuals"))
	{
					  $columns[$OCCIDCOL] = 0;
					}
				}
	
		# give collection identity of its formation if lumping
				$collno = $columns[$OCCIDCOL+1];
				if ($q->param('lumpbyfm') eq "Y")	{
					$collno = $formation[$collno];
				}
	
				if (($columns[$OCCIDCOL] > 0) && ($columns[$SPCOL] ne "indet.") &&
					  ($columns[$SPCOL] ne "indet") && ($chid[$collno] > 0))      {
					$temp = $columns[$GENCOL];
					($temp,$temp2) = split(/ \(/,$temp,2);
					$temp =~ s/"//g;
					$temp =~ s/\?//g;
	
					$temp =~ s/  / /g;
					$temp =~ s/ $//;
	
		 # update file keeping list of all distinct genus names
					$ao = 0;
					$ao = $genid{$temp};
					if ($ao == 0)	{
					  $ngen++;
					  $genus[$ngen] = $temp;
					  $genid{$temp} = $ngen;
					   print GEN "$temp\n";
					}
	
					if ($required{$temp} ne "" || $q->param('class') eq "")  {
					  $nsp = 1;
					  if ($samplingmethod == 5)       {
					    $nsp = $columns[$ABCOL];
					  }
	
				# check to see if genus already is listed in collection
					  $xx = $lastocc[$collno];
					  while ($xx > 0)	{
					    if ($occs[$xx] ne $genid{$temp})	{
					      $xx = $stone[$xx];
					    }
					    else	{
					      $abund[$xx] = $abund[$xx] + $columns[$ABCOL];
					      $xx = -9;
					    }
					  }
				 # if not, add genus to master occurrence list
					  if ($xx != -9)	{
					    push @occs,$genid{$temp};
					    push @stone,$lastocc[$collno];
					    push @abund,$columns[$ABCOL];
					    $lastocc[$collno] = $#occs;
					    $toccsinlist[$collno] = $toccsinlist[$collno] + $nsp;
					    if ($required{$temp} eq "required")	{
					      $hasrequired[$collno]++;
					    }
					  }
					}
				}
			}
			else  {
				$err = $csv->error_input;
				print "Can't read occurrence record as follows: ", $err, "\n";
			}
		}
		close OCCS;
	
	 # get rid of too-small or too-large lists if a cutoff is being used
	 # because all the crucial stats computed later on depend on starting
	 #   searches from lastocc, setting its value to zero deletes the lists
		if ($q->param('cutoff') ne "" || $q->param('class') ne "")  {
			for $qq (1..$#lastocc)	{
		# get rid of null lists or lists without any required higher taxa
				if ($toccsinlist[$qq] == -1 ||
					  ($hasrequired[$qq] < 1 && $q->param('class') ne ""))    {
					$lastocc[$qq] = 0;
					$toccsinlist[$qq] = -1;
				}
				elsif ($q->param('lowerupper') eq "at least") {
					if ($toccsinlist[$qq] < $q->param('cutoff'))      {
					  $lastocc[$qq] = 0;
					  $toccsinlist[$qq] = -1;
					}
				}
				elsif ($q->param('lowerupper') eq "no more than")     {
					if ($toccsinlist[$qq] > $q->param('cutoff'))      {
					  $lastocc[$qq] = 0;
					  $toccsinlist[$qq] = -1;
					}
				}
			}
		}
	
	 # get basic counts describing lists and their contents
		for $collno (1..$#lastocc)	{
			$xx = $lastocc[$collno];
			while ($xx > 0)	{
				$nsp = 1;
				if ($samplingmethod == 5)	{
					$nsp = $abund[$xx];
				}
				$occsread = $occsread + $nsp;
				$occsoftax[$occs[$xx]] = $occsoftax[$occs[$xx]] + $nsp;
				for $qq ($occsinchron[$chid[$collno]]+1..$occsinchron[$chid[$collno]]+$nsp)	{
					$occsbychron[$chid[$collno]][$qq] = $occs[$xx];
				}
				$occsinchron[$chid[$collno]] = $occsinchron[$chid[$collno]] + $nsp;
				$present[$occs[$xx]][$chid[$collno]]--;
				$occsinlist[$collno] = $occsinlist[$collno] + $nsp;
				if ($occsinlist[$collno] == $nsp)	{
					$listsread++;
					$listsinchron[$chid[$collno]]++;
					$listsbychron[$chid[$collno]][$listsinchron[$chid[$collno]]] = $collno;
					$baseocc[$collno] = $occsinchron[$chid[$collno]] - $nsp + 1;
				}
				$topocc[$collno] = $occsinchron[$chid[$collno]];
				$xx = $stone[$xx];
			}
		}
	
	# free some variables
		@lastocc = ();
		@occs = ();
		@stone = ();
		@abund = ();
	
	# compute median richness in each chron
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
	
	if ( ! open BINNING,">$OUTPUT_DIR/binning.csv" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/binning.csv<BR>$!" );
	}
	print BINNING "Collection,Bin,Country,Occurrences,Strat scale\n";
	for $i (1..$#occsinlist)	{
		if ($occsinlist[$i] > 0)	{
			printf BINNING "%d,%d,",$i,$chrons-$chid[$i]+1;
			print BINNING "$country[$i],";
			print BINNING "$occsinlist[$i],$sscale[$i]\n";
		}
	}
	close BINNING;
	
	# compute sum of (squared) richnesses across lists
	if (($samplingmethod == 1) || ($samplingmethod == 5))	{
		for $i (1..$#occsinlist)	{
			$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
		}
	}
	elsif ($samplingmethod == 3)	{
		for $i (1..$#occsinlist)	{
			if ($occsinlist[$i] <= $q->param('samplesize'))	{
				$usedoccsinchron[$chid[$i]] = $usedoccsinchron[$chid[$i]] + $occsinlist[$i];
			}
		}
	}
	for $i (1..$#occsinlist)	{
		if (($samplingmethod != 4) ||
				($occsinlist[$i]**2 <= $q->param('samplesize')))	{
			$occsinchron2[$chid[$i]] = $occsinchron2[$chid[$i]] + $occsinlist[$i]**2;
		}
	}
	
	# tabulate genus ranges and turnover data
	if ( ! open PADATA,">$OUTPUT_DIR/presences.txt" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/presences.txt<BR>$!" );
	}
	print PADATA "genus\ttotal occs";
	for $i (reverse 1..$chrons)	{
		print PADATA "\t$chname[$i]";
	}
	print PADATA "\n";
	for $i (1..$ngen)	{
		$first = 0;
		$last = 0;
		for $j (1..$chrons)	{
			if ($present[$i][$j] < 0)	{
				$richness[$j]++;
				if ($last == 0)	{
					$last = $j;
				}
				$first = $j;
			}
			if ($present[$i][$j] == -1)	{
				$chaol[$j]++;
			}
			elsif ($present[$i][$j] == -2)	{
				$chaom[$j]++;
			}
		}
		if ($first > 0)	{
			print PADATA "$genus[$i]\t$occsoftax[$i]";
			for $j (reverse 1..$chrons)	{
			# if ($present[$i][$j] < 0)	{
					$fx = abs($present[$i][$j]);
			#   print PADATA " ",$chrons-$j+1," ($fx)";
					print PADATA "\t$fx";
			# }
			}
			print PADATA "\n";
		}
		if (($first > 0) && ($last > 0))	{
			for $j ($last..$first)	{
				$rangethrough[$j]++;
			}
			if ($first == $last)	{
				$singletons[$first]++;
			}
			$originate[$first]++;
			$extinct[$last]++;
			$foote[$first][$last]++;
		}
	}
	close PADATA;
	
	if ( ! open FOOTE,">$OUTPUT_DIR/firstlast.txt" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/firstlast.txt<BR>$!" );
	}
	for $i (reverse 1..$chrons)	{
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

}

sub subsample	{
	my $self = shift;

	if ($q->param('samplingtrials') > 0)	{
		for ($trials = 1; $trials <= $q->param('samplingtrials'); $trials++)	{
			@sampled = ();
			@lastsampled = ();
			@subsrichness = ();
			@lastsubsrichness = ();
			for $i (1..$chrons)	{
				if (($q->param('printall') eq "yes" && $listsinchron[$i] > 0)||
					  (($usedoccsinchron[$i] > $q->param('samplesize') &&
					    $samplingmethod != 2 && $samplingmethod != 4) ||
					   ($listsinchron[$i] > $q->param('samplesize') &&
					    $samplingmethod == 2) ||
					   ($occsinchron2[$i] > $q->param('samplesize') &&
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
					  $tosub = ($tosub/($usedoccsinchron[$i]/$listsinchron[$i])) - 0.5;
					}
					elsif ($samplingmethod == 4)	{
			#     $tosub = $tosub - ($occsinchron2[$i]/$listsinchron[$i])/2;
					  $tosub = ($tosub/($occsinchron2[$i]/$listsinchron[$i])) - 0.5;
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
					        $xx = $xx**2;
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
			 # if using the geographic dispersion algorithm, throw back the list
			 #   (x-1)/x of the time where x is the number of lists at the same
			 #   geographic coordinate in this temporal bin
					  if ($samplingmethod != 1 && $samplingmethod != 5 &&
					      $q->param('disperse') eq "yes")	{
					    $xx = int(rand $locsatpoint{$locpoint[$listid[$j]]}) + 1;
					    while ($xx > 1)	{
					      $j = int(rand $nitems) + 1;
					      $xx = int(rand $locsatpoint{$locpoint[$listid[$j]]}) + 1;
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
					    $tosub--;
					    $sampled[$i] = $sampled[$i] + $xx;
					  }
					  else	{
					    $xx = $topocc[$listid[$j]] - $baseocc[$listid[$j]] + 1;
					 #  $tosub = $tosub - ($xx**2);
					    $tosub--;
					    $sampled[$i] = $sampled[$i] + $xx**2;
					  }
	
		 # declare the genus (or all genera in a list) present in this chron
					  $lastsubsrichness[$i] = $subsrichness[$i];
					  if (($samplingmethod == 1) || ($samplingmethod == 5))	{
					    if ($present[$occid[$j]][$i] != $trials)	{
					      $present[$occid[$j]][$i] = $trials;
					      $subsrichness[$i]++;
					    }
					  }
					  else	{
					    for $k ($baseocc[$listid[$j]]..$topocc[$listid[$j]])	{
					      if ($present[$occsbychron[$i][$k]][$i] != $trials)	{
					        $present[$occsbychron[$i][$k]][$i] = $trials;
					        $subsrichness[$i]++;
					      }
					    }
					  }
	
		 # record data in the complete subsampling curve
					  if ($atstep[$sampled[$i]] > $atstep[$lastsampled[$i]])	{
					    $z = $sampled[$i] - $lastsampled[$i];
					    $y = $subsrichness[$i] - $lastsubsrichness[$i];
					    for $k ($atstep[$lastsampled[$i]]..$atstep[$sampled[$i]]-1)	{
					      $x = $stepback[$k] - $lastsampled[$i];
					      $sampcurve[$i][$k] = $sampcurve[$i][$k] + ($x * $y / $z) + $lastsubsrichness[$i];
					    }
					  }
	
		 # erase the list or occurrence that has been drawn
					  if (($samplingmethod != 1) && ($samplingmethod != 5))	{
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
				if ($atstep[$q->param('samplesize')]+1 > $atstep[$sampled[$i]] &&
					  $inbin > $sampled[$i])	{
					$w = $inbin;
					if ($inbin > $q->param('samplesize'))	{
					  $w = $q->param('samplesize');
					}
					$z = $w - $sampled[$i];
					$y = $subsrichness[$i] - $lastsubsrichness[$i];
					if ($z > 0)	{
					  for $k ($atstep[$sampled[$i]]..$atstep[$w])	{
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
			@tsubsrangethrough = ();
			@tsubssingletons = ();
			@tsubsoriginate = ();
			@tsubsextinct = ();
			for $i (1..$ngen)	{
				$first = 0;
				$last = 0;
				for $j (1..$chrons)	{
					if ($present[$i][$j] == $trials)	{
					  if ($last == 0)	{
					    $last = $j;
					  }
					  $first = $j;
					}
				}
				for $j ($last..$first)	{
					$msubsrangethrough[$j]++;
					$tsubsrangethrough[$j]++;
				}
				if ($first == $last)	{
					$msubssingletons[$first]++;
					$tsubssingletons[$first]++;
				}
				$msubsoriginate[$first]++;
				$msubsextinct[$last]++;
				$tsubsoriginate[$first]++;
				$tsubsextinct[$last]++;
			}
			for $i (1..$chrons)	{
				if ($msubsrangethrough[$i] > 0)	{
					$msubsrichness[$i] = $msubsrichness[$i] + $subsrichness[$i];
				}
				if ($q->param('diversity') =~ /boundary-crossers/)	{
					$outrichness[$i][$trials] = $tsubsrangethrough[$i] - $tsubsoriginate[$i];
				}
				elsif ($q->param('diversity') =~ /range-through\b/)	{
					$outrichness[$i][$trials] = $tsubsrangethrough[$i];
				}
				elsif ($q->param('diversity') =~ /range-through minus/)	{
					$outrichness[$i][$trials] = $tsubsrangethrough[$i] - $tsubssingletons[$i];
				}
				elsif ($q->param('diversity') =~ /sampled-in-bin/)	{
					$outrichness[$i][$trials] = $subsrichness[$i];
				}
				elsif ($q->param('diversity') =~ /sampled minus/)	{
					$outrichness[$i][$trials] = $subsrichness[$i] - $tsubssingletons[$i];
				}
			}
	 # end of trials loop
		}
		$trials = $q->param('samplingtrials');
		for $i (1..$chrons)	{
			if ($msubsrangethrough[$i] > 0)	{
				$tsampled[$i] = $tsampled[$i]/$trials;
				$msubsrichness[$i] = $msubsrichness[$i]/$trials;
				$msubsrangethrough[$i] = $msubsrangethrough[$i]/$trials;
				$msubssingletons[$i] = $msubssingletons[$i]/$trials;
				$msubsoriginate[$i] = $msubsoriginate[$i]/$trials;
				$msubsextinct[$i] = $msubsextinct[$i]/$trials;
				for $j (1..$atstep[$q->param('samplesize')])	{
					$sampcurve[$i][$j] = $sampcurve[$i][$j]/$trials;
				}
			}
		}
	}
	
	for $i (1..$chrons)	{
		@{$outrichness[$i]} = sort { $a <=> $b } @{$outrichness[$i]};
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
	
	if ( ! open TABLE,">$OUTPUT_DIR/curvedata.csv" ) {
		$self->htmlError ( "$0:Couldn't open $OUTPUT_DIR/curvedata.csv<BR>$!" );
	}
	
	if ($listsread > 0)	{
		$listorfm = "Lists";
		if ($q->param('lumpbyfm') eq "Y")	{
			$listorfm = "Formations";
		}
		if ($q->param('samplesize') ne "")	{
			print "<hr>\n<h3>Raw data</h3>\n\n";
		}
		print "<table cellpadding=4>\n";
		print "<tr><td><u>Sampled<br>genera</u> <td><u>Range-through<br>genera</u> ";
		print "<td><u>First<br>appearances</u> <td><u>Last<br>appearances</u> <td><u>Singleton<br>genera</u> ";
		print "<td><u>$listorfm</u> ";
		if ($samplingmethod != 5)	{
			print "<td><u>Occurrences</u> ";
			print "<td><u>Occurrences<br>-squared</u> ";
		}
		else	{
			print "<td><u>Specimens</u> ";
		}
		print "<td><u>Mean<br>richness</u> <td><u>Median<br>richness</u> ";
		print "<td><u>Interval</u>\n";
		print TABLE "Bin,Sampled genera,Range-through genera,";
		print TABLE "First appearances,Last appearances,Singleton genera,";
		if ($samplingmethod != 5)	{
			print TABLE "$listorfm,Occurrences,Occurrences-squared,";
		}
		else	{
			print TABLE "$listorfm,Specimens,";
		}
		print TABLE "Mean richness,Median richness,Base (Ma),Midpoint (Ma),Bin name,Stages\n";
	
		for $i (1..$chrons)	{
			if ($rangethrough[$i] > 0 && $listsinchron[$i] > 0)	{
				print "<tr><td align=center>$richness[$i] ";
				print "<td align=center>$rangethrough[$i] ";
				print "<td align=center>$originate[$i] ";
				print "<td align=center>$extinct[$i] ";
				print "<td align=center>$singletons[$i] ";
				print "<td align=center>$listsinchron[$i] ";
				print "<td align=center>$occsinchron[$i] ";
				if ($samplingmethod != 5)	{
					print "<td align=center>$occsinchron2[$i] ";
				}
				printf "<td>%.1f ",$occsinchron[$i]/$listsinchron[$i];
				printf "<td>%.1f ",$median[$i];
				print "<td>$chname[$i]";
	
				print TABLE $chrons - $i + 1;
				print TABLE ",$richness[$i]";
				print TABLE ",$rangethrough[$i]";
				print TABLE ",$originate[$i]";
				print TABLE ",$extinct[$i]";
				print TABLE ",$singletons[$i]";
				print TABLE ",$listsinchron[$i]";
				print TABLE ",$occsinchron[$i]";
				if ($samplingmethod != 5)	{
					print TABLE ",$occsinchron2[$i]";
				}
				printf TABLE ",%.1f",$occsinchron[$i]/$listsinchron[$i];
				printf TABLE ",%.1f",$median[$i];
				printf TABLE ",%.1f",$basema[$i];
				printf TABLE ",%.1f",$midptma[$i];
				print TABLE ",$chname[$i]";
				if ($longstartstage[$i] ne "")	{
					print " ($longstartstage[$i]";
					print TABLE ",$longstartstage[$i]";
					if (($endstage[$i] ne "") && ($endstage[$i] ne $longstartstage[$i]))	{
					  print " - $endstage[$i]";
					  print TABLE " - $endstage[$i]";
					}
					print ")";
				}
				print "<p>\n";
				print TABLE "\n";
			}
		}
		print "</table><p>\n";
	
		print "\n<b>$listsread</b> lists and <b>$occsread</b> occurrences met the search criteria.<p>\n";
	
		if ($q->param('samplesize') ne "")	{
			print "\n<hr>\n<h3>Results of subsampling analysis</h3>\n\n";
			print "<table cellpadding=4>\n";
			print "<tr><td><u>Items<br>sampled</u> ";
			print "<td><u>Median<br>richness</u> ";
			print "<td><u>1-sigma CI</u> ";
			print "<td><u>Sampled<br>genera</u> ";
			print "<td><u>Range-through<br>genera</u> ";
			print "<td><u>First<br>appearances</u> <td><u>Last<br>appearances</u> <td><u>Singleton<br>genera</u> ";
			print "<td align=center><u>Gap analysis<br>sampling stat</u> ";
			print "<td align=center><u>Gap analysis<br>diversity estimate</u> ";
			print "<td align=center><u>Chao-2<br>estimate</u> ";
			print "<td><u>Interval</u>\n";
			print TABLE "Bin,Items sampled,Median richness,";
			print TABLE "1-sigma lower CI,1-sigma upper CI,";
			print TABLE "Sampled genera,Range-through genera,";
			print TABLE "First appearances,Last appearances,Singleton genera,";
			print TABLE "Gap analysis completeness,";
			print TABLE "Gap analysis diversity,";
			print TABLE "Chao-2 estimate,Base (Ma),Midpoint (Ma),Bin name,Stages\n";
			for ($i = 1; $i <= $chrons; $i++)     {
				if ($rangethrough[$i] > 0)  {
					$gapstat = $richness[$i] - $originate[$i] - $extinct[$i] + $singletons[$i];
					if ($gapstat > 0)	{
					  $gapstat = $gapstat/($rangethrough[$i] - $originate[$i] - $extinct[$i] + $singletons[$i]);
	#         $gapstat = $richness[$i] / $gapstat;
					}
					else	{
					  $gapstat = "";
					}
					if ($chaom[$i] > 0)	{
					  $chaostat = $richness[$i] + ($chaol[$i] * $chaol[$i] / (2 * $chaom[$i]));
					}
					else	{
					  $chaostat = "";
					}
					printf "<tr><td align=center>%.1f ",$tsampled[$i];
					$s = int(0.5*$trials)+1;
					print "<td align=center>$outrichness[$i][$s] ";
					$qq = int(0.1587*$trials)+1;
					$r = int(0.8413*$trials)+1;
					print "<td align=center>$outrichness[$i][$qq]-$outrichness[$i][$r]";
					printf "<td align=center>%.1f ",$msubsrichness[$i];
					printf "<td align=center>%.1f ",$msubsrangethrough[$i];
					printf "<td align=center>%.1f ",$msubsoriginate[$i];
					printf "<td align=center>%.1f ",$msubsextinct[$i];
					printf "<td align=center>%.1f ",$msubssingletons[$i];
					print TABLE $chrons - $i + 1;
					printf TABLE ",%.1f",$tsampled[$i];
					print TABLE ",$outrichness[$i][$s]";
					print TABLE ",$outrichness[$i][$qq]";
					print TABLE ",$outrichness[$i][$r]";
					printf TABLE ",%.1f",$msubsrichness[$i];
					printf TABLE ",%.1f",$msubsrangethrough[$i];
					printf TABLE ",%.1f",$msubsoriginate[$i];
					printf TABLE ",%.1f",$msubsextinct[$i];
					printf TABLE ",%.1f",$msubssingletons[$i];
					if ($gapstat > 0)	{
					  printf "<td align=center>%.3f ",$gapstat;
					  printf "<td align=center>%.1f ",$richness[$i] / $gapstat;
					  printf TABLE ",%.3f",$gapstat;
					  printf TABLE ",%.3f",$richness[$i] / $gapstat;
					}
					else	{
					  print "<td align=center> <td align=center> ";
					  print TABLE ",,";
					}
					if ($chaostat > 0)	{
					  printf "<td align=center>%.1f ",$chaostat;
					  print TABLE ",$chaostat";
					}
					else    {
					  print "<td align=center> ";
					  print TABLE ",";
					}
					print "<td>$chname[$i]";
					printf TABLE ",%.1f",$basema[$i];
					printf TABLE ",%.1f",$midptma[$i];
					print TABLE ",$chname[$i]";
					if ($longstartstage[$i] ne "")        {
					  print " ($longstartstage[$i]";
					  print TABLE ",$longstartstage[$i]";
					  if (($endstage[$i] ne "") && ($endstage[$i] ne $longstartstage[$i]))        {
					    print " - $endstage[$i]";
					    print TABLE " - $endstage[$i]";
					  }
					  print ")";
					}
					print "<p>\n";
					print TABLE "\n";
				}
			}
			print "</table><p>\n\n";
			print "The selected method was <b>".$q->param('samplingmethod')."</b>.<p>\n";
			print "The number of items selected per temporal bin was <b>".$q->param('samplesize')."</b>.<p>\n";
			print "The total number of trials was <b>".$q->param('samplingtrials')."</b>.<p>\n";
			print "<hr>\n";
		}
	
	 # make sure all the files are read/writeable
		chmod 0664, "$PRINTED_DIR/*";
	
		print "\nThe following data files have been created and can be downloaded by clicking on their names:<p>\n";
		print "<ul>\n";
		print "<li>The above diversity curve data (<a href=\"http://$CURVE_HOST$PRINTED_DIR/curvedata.csv\">curvedata.csv</a>)<p>\n";
	
		if ($q->param('stepsize') ne "")	{
			print "<li>The subsampling curves (<a href=\"http://$CURVE_HOST$PRINTED_DIR/subcurve.tab\">subcurve.tab</a>)<p>\n";
		}
	
		print "<li>An abstract of the collections assigned to each bin (<a href=\"http://$CURVE_HOST$PRINTED_DIR/binning.csv\">binning.csv</a>)<p>\n";
	
		print "<li>A first-by-last occurrence count matrix (<a href=\"http://$CURVE_HOST$PRINTED_DIR/firstlast.txt\">firstlast.txt</a>)<p>\n";
	
		print "<li>A list of each genus, the number of collections including it,  and the ID number of the intervals in which it was found (<a href=\"http://$CURVE_HOST$PRINTED_DIR/presences.txt\">presences.txt</a>)<p>\n";
	
		if ( $s->get('enterer') ne "Guest" )	{
			print "<li>A list of collections that could not be placed in temporal bins (<a href=\"http://$CURVE_HOST$PRINTED_DIR/orphans.txt\">orphans.txt</a>)<p>\n";
			print "</ul>\n";
		}
	
	}
	else	{
		print "\n<b>Sorry, the search failed.</b> No lists met the search criteria.<p>\n";
	}
	
	if (%badnames ne () && $s->get('enterer') ne "Guest")	{
		print "\nThe following age/stage names were not recognized:<p>\n<ul>\n";
		my @temp = keys %badnames;
		@temp = sort { $a cmp $b } @temp;
		for $badstage (@temp)	{
			print "<li>$badstage ($badnames{$badstage})\n";
		}
		print "</ul><p>\n";
	}
	
	print "\n</body>\n";

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
