#!/usr/bin/perl -w

# by rjp, 3/22/2004
# 
# Some common constants which can be used throughout the modules.
# To include them, simply say "use Constants;" at the top of the file.

package Constants;

use strict;

# exporter allows us to export the symbols so we don't have to qualify
# their names in the other packages which use Constants.

require Exporter;
our @ISA = qw(Exporter);

# every constant you want to export must be listed in this list.
our @EXPORT = qw(
					TRUE
					FALSE


					SUBSPECIES
					SPECIES
					SUBGENUS
					GENUS
					SUBTRIBE
					TRIBE
					SUBFAMILY
					FAMILY
					SUPERFAMILY
					INFRAORDER
					SUBORDER
					ORDER
					SUPERORDER
					INFRACLASS
					SUBCLASS
					CLASS
					SUPERCLASS
					SUBPHYLUM
					PHYLUM
					SUPERPHYLUM
					SUBKINGDOM
					KINGDOM
					SUPERKINGDOM
					UNRANKEDCLADE
					INFORMAL
				);



use constant TRUE => 1;
use constant FALSE => 0;


use constant SUBSPECIES => 'subspecies';
use constant SPECIES 	=> 'species';
use constant SUBGENUS => 'subgenus';
use constant GENUS => 'genus';
use constant SUBTRIBE => 'subtribe';
use constant TRIBE => 'tribe';
use constant SUBFAMILY => 'subfamily';
use constant FAMILY => 'family';
use constant SUPERFAMILY => 'superfamily';
use constant INFRAORDER => 'infraorder';
use constant SUBORDER => 'suborder';
use constant ORDER => 'order';
use constant SUPERORDER => 'superorder';
use constant INFRACLASS => 'infraclass';
use constant SUBCLASS => 'subclass';
use constant CLASS => 'class';
use constant SUPERCLASS => 'superclass';
use constant SUBPHYLUM => 'subphylum';
use constant PHYLUM => 'phylum';
use constant SUPERPHYLUM => 'superphylum';
use constant SUBKINGDOM => 'subkingdom';
use constant KINGDOM => 'kingdom';
use constant SUPERKINGDOM => 'superkingdom';
use constant UNRANKEDCLADE => 'unranked clade';
use constant INFORMAL => 'informal';



1;