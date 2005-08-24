#!/usr/bin/perl -w

#
# This module is deprecated PS 08/10/2005
# Useless module, poling crap
#

# Constants.pm
# created by rjp, 3/22/2004
# 
# Some common constants which can be used throughout the modules.
# To include them, simply say "use Constants;" at the top of the file.
#
# Constants in here *should* be fairly generic to the entire program - ie -
# values we might want to use anywhere.  It wouldn't make sense to put specialized
# constants for a single module in here - put those in the module instead.

package Constants;

use strict;

# exporter allows us to export the symbols so we don't have to qualify
# their names in the other packages which use Constants.

require Exporter;
our @ISA = qw(Exporter);

# Every constant you want to export must be listed in this list.
# This means that they will be available in modules which say "use Constants" without
# having to qualify their names with the Constants:: package name.
our @EXPORT = qw(
					TRUE
					FALSE
					
					YES

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


# for booleans
use constant TRUE => 1;
use constant FALSE => 0;

# in the database, some boolean fields are stored with a 'YES' value for true
# and a '' value for false.  
use constant YES => 'YES';

# standard names for ranks which we use in the database taxon_rank fields.
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
