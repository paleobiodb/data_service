# JA 26-31.7, 6.8.07

use GD;
use Reference;

my $FONT = "./data/fonts/Vera.ttf";
my $FONT2 = "./data/fonts/VeraBd.ttf";

# debugging line
drawCladogram();

# user has to search for the higher taxon including the entire cladogram
sub displayCladeSearchForm	{

}

sub processCladeSearch	{

	# if there are no matches, call displayCladeSearchForm and insert
	#  a message saying "No matches were found, please search again"

	# if there are multiple matches, present the usual list of choices

	# if there is one match, call displayCladogramEntryForm

	# first, though, hit the cladograms table to confirm that no existing
	#  cladogram matches both the session ref and the clade's taxon_no,
	#  because that would be a duplicate

	# call displayCladeSearchForm with an error if there is such a match

}

sub displayCladogramEntryForm	{

	# the name of the overall taxon needs to be inserted at the top

	# the matrix of divs that will be used to draw the cladogram needs
	#   to be inserted into the form

}

sub processCladogramEntry	{

	# first do some minimal sanity checking in case the JavaScript
	#  didn't catch something

	# all errors will result later in calling displayCladogramEntryForm

	# source must be entered

	# bootstrap values should all be integers between 1 and 100

	# at least one contents line should have been filled out

	# then parse out the lines

	# replace all apparent separators with commas; these include
	#  . : ; - + &

	# also collapse down all spaces and replace them with commas, but
	#  do not do this in the [A-Z][a-z]+ [a-z] case, because this should
	#  indicate a genus-species combination

	# extract apparent taxon names and check their formatting

	# all such names should either be integers not starting with zero,
	#  or taxon names in the form "Equidae" or "Equus caballus"
	# a trailing * after a proper taxon name is fine, but if this is seen,
	#  strip it and set plesiomorphic = YES for this taxon
	# add errors saying "'Equidae' is misformatted" if needed

	# check for duplicate clade names

	# check for taxa of any kind assigned to the same clade twice

	# check for taxa of any kind assigned to more than one clade

	# check for clades including just one thing

	# check to see that exactly one clade is assigned to nothing

	# if more than one clade is assigned to nothing, complain that
	#  there is more than one root node

	# if all clades are assigned to other clades, complain that there
	#  is no root node because each clade is assigned to another one

	# next run through all apparent taxonomic names and match them
	#  against the authorities table

	# the error in case of not finding matches is "Please add authority
	#  data for the following taxa: Equidae, Felidae, and Canide" or for
	#  a single problem taxon "Please add authority data for Equidae"

	# if all names were found but some were homonyms or exist at multiple
	#  ranks, list them with the usual homonym choice pulldowns also
	#  used by Reclassify.pm

	# if there are any errors, call displayCladogramEntryForm, populate
	#  it with submitted values, and add a bulleted list of errors
	#  under the header

	# if there are no errors, insert the data

	# authorizer_no, enterer_no, reference_no, taxon_no, pages, figures,
	#  source, comments, created, and modified (= created) are all
	#  easy to determine, so the cladograms table as a whole is easy

	# nodes table is mostly easy

	# parent_no and taxon_no (for valid Linnean names) follow from the
	#  above parsed contents fields, the hit on authorities, and any
	#  pulldown choices made by the user

	# outgroup and bootstrap are standard form parameters

	# plesiomorphic has been set during parsing to YES for all taxa
	#  whose names are followed with asterisks

	# call displayCladeSearchForm and populate it with a congratulations
	#  message ("The cladogram for ... was successfully entered"), plus
	# "tree: ..." immediately following, with an image generated
	#  by formatTreeData

}

# hits the cladograms and nodes table and returns a string in NHX (similar
#  to NEXUS) format that describes the topology of the cladogram
sub formatTreeData	{

	# hit cladograms and nodes for cladograms.taxon_no, node_no, taxon_no,
	#  parent_no, and bootstrap

	# hit authorities for the names where taxon_no > 0

	# spaces in species names need to be replaced with _

	# format the data like this:
 	# taxon_name = ((A:1,B:0)C:1,D:1));
	# where A = terminal taxon name, D = parent node name, and the
	#  numbers indicate branch lengths: 1 = default, 0 = value if
	#  plesiomorphic = YES
	# if bootstrap values like 98 exist, add them like this:
	#  ((A:1,B:0)D:1[&&NSX:B=98],C:1));

	# return the formatted string

}

sub drawCladogram	{

# shift standard objects like dbt, hbo, s, q, plus
#  $cladogram_no and $focal_taxon_no
my $cladogram_no;
my $focal_taxon_no;

# cladograms table should include following fields:

# authorizer_no
# enterer_no
# modifier_no
# cladogram_no
# reference_no
# taxon_no
# pages text
# figures text
# source enum('','text','illustration','supertree','most parsimonious tree','consensus tree','likelihood tree')
# comments (user entered)
# created
# modified
# upload enum('','YES');

# nodes table fields:

# node_no
# taxon_no
# parent_no
# outgroup enum('','YES');
# plesiomorphic enum('','YES');
# bootstrap int default null

# parent_no points to another node_no in the same cladogram, and is zero
#  for the root
# bootstrap is a percentage between 0 and 100 with no fractional values
# created/modified/upload and authorizer_no etc. excluded because these are
#  all (more or less) properties of entire cladograms

	# pull data from cladograms table based on cladogram_no

	# test data
	# the subscripts are primary key numbers starting at 0
	# so, to get the numbers right, subtract the node_no of the first
	#  taxon in the list from each node_no
	$reference_no = 3850;
	my @node;
	# these would be gotten by a join on authorities using taxon_no
	my @taxon = ("N. eurystyle","N. gidleyi","","Neohipparion leptode","","N. trampasense","","N. affine","Neohipparion","M. republicanus","Pseudhipparion","","","M. coloradense","","Pseudoparablastomeryx olcotti","Hipparionini","M. insignis","Equinae");
	my @parent = (2,2,4,4,6,6,8,8,12,11,11,12,14,14,16,16,18,18,0);
	my @plesiomorphic = ('','YES','','','','YES','','','','','','','','','','','','','');
	my @bootstrap = ('','','99','','87','','100','','','','','35','100','','','','68','','');

	# more dummy values for testing
	$focal_taxon_no = 5;
	# taxon_no array here is unrelated to the taxon_no field in nodes
	my @taxon_no;
	for my $t ( 0..$#taxon )	{
		$node[$t] = $t;
		$taxon_no[$t] = $t;
	}

	my @depth;
	for my $t ( 0..$#taxon )	{
		$depth[$t] = 0;
	}
	# the depth of each node is the maximum number of nodes traversed by
	#  each subnode
	for my $n ( @node )	{
		my $z = $n;
		my $d = 1;
		while ( $parent[$z] > 0 )	{
			if ( $d > $depth[$parent[$z]] )	{
				$depth[$parent[$z]] = $d;
			}
			$d++;
			$z = $parent[$z];
		}
	}

	# nodes with depth = 0 are terminals
	my $terminals = 0;
	my @terminal_no;
	my $maxdepth = 0;
	for my $n ( @node )	{
		if ( $depth[$n] == 0 )	{
			$terminals++;
			$terminal_no[$n] = $terminals;
		} else	{
			$clades++;
			$clade_no[$n] = $clades;
			if ( $depth[$n] > $maxdepth )	{
				$maxdepth = $depth[$n];
			}
		}
	}
	for my $n ( @node )	{
		if ( $clade_no[$n] )	{
			$clade_no[$n] = $clades - $clade_no[$n] + 1;
		}
	}

	# the vertical position of each internal node is the arithmetic
	#  mean of all its terminals' terminal numbers
	my @subterminals;
	my @sumterminalnos;
	for my $t ( 0..$#taxon )	{
		if ( $terminal_no[$node[$t]] > 0 )	{
			my $z = $node[$t];
			while ( $parent[$z] > 0 )	{
				$subterminals[$parent[$z]]++;
				$sumterminalnos[$parent[$z]] += $terminal_no[$node[$t]];
				$z = $parent[$z];
			}
		}
	}
	for my $t ( 0..$#taxon )	{
		if ( $subterminals[$node[$t]] > 0 )	{
			$height[$node[$t]] = $sumterminalnos[$node[$t]] / $subterminals[$node[$t]];
		} else	{
			$height[$node[$t]] = $terminal_no[$node[$t]];
		}
	}

	# the cladograms look "right" when the scaling numbers are equal
	#  because the lines branch from each other at 90 degrees
	my $height_scale = 23;
	my $width_scale = 23;
	my $maxletts = 1;
	for my $t ( @taxon )	{
		my @letts = split //,$t;
		if ( $#letts + 1 > $maxletts )	{
			$maxletts = $#letts + 1;
		}
	}
	# the multiplier constant is specific to the font
	my $border = int( $maxletts * 8.25 );
	my $imgheight = $height_scale * ( $terminals + 1 );
	my $imgwidth = ( $width_scale * ( $maxdepth + 1 ) ) + $border;
	my $im = GD::Image->new($imgwidth,$imgheight,1);
	
	$unantialiased = $im->colorAllocate(-1,-1,-1);
	$orangeunantialiased = $im->colorAllocate(-255,-127,-63);
	$white = $im->colorAllocate(255,255,255);
	$black = $im->colorAllocate(0,0,0);
	$lightgray = $im->colorAllocate(193,193,193);

	# this line is said to work in the standard GD.pm manual, but does not
	#$im->transparent($white);
	$im->interlaced('true');
	# so we clear the image the hard way
	$im->filledRectangle(0,0,$imgwidth,$imgheight,$white);
	# also add a frame
	$im->rectangle(0,0,$imgwidth - 1,$imgheight - 1,$lightgray);

	# might want to mess with this sometime
	#$im->setThickness(1);
	$im->setAntiAliased($black);
	for my $n ( @node )	{
		if ( $terminal_no[$n] > 0 )	{
			# focal taxon's name is bold orange
			if ( $taxon_no[$n] == $focal_taxon_no )	{
				$im->stringFT($orangeunantialiased,$FONT2,10,0,$imgwidth - $border + 8,( $terminal_no[$n] * $height_scale ) + 5,$taxon[$n]);
			} else	{
				$im->stringFT($unantialiased,$FONT,10,0,$imgwidth - $border + 8,( $terminal_no[$n] * $height_scale ) + 5,$taxon[$n]);
			}
			$im->line($imgwidth - $border,$terminal_no[$n] * $height_scale,$imgwidth - $border - ( $depth[$parent[$n]] * $width_scale ),$height[$parent[$n]] * $height_scale,$gdAntiAliased);
			# small circle indicates an automorphic
			#  (=  non-plesiomorphic) taxon
			if ( ! $plesiomorphic[$taxon_no[$n]] )	{
				$im->filledArc($imgwidth - $border,( $terminal_no[$n] * $height_scale ),6,6,0,360,"$black");
			}

		}
		# connect internal nodes
		elsif ( $parent[$n] )	{
			$nodex = $imgwidth - $border - ( $depth[$n] * $width_scale );
			$nodey = $height[$n] * $height_scale;
			$im->line($nodex,$nodey,$imgwidth - $border - ( $depth[$parent[$n]] * $width_scale ),$height[$parent[$n]] * $height_scale,$gdAntiAliased);
		}
	}

	# draw node numbers and write caption
	my $printednodes = 0;
	#my $caption = Reference::formatShortRef($dbt,$reference_no,'no_inits'=>1) . "<br>";
	# debugging line
	my $caption = qq|<a href="">Hulbert 1986</a><br>|;
	for my $n ( reverse @node )	{
		$nodex = $imgwidth - $border - ( $depth[$n] * $width_scale );
		$nodey = $height[$n] * $height_scale;
		if ( $terminal_no[$n] == 0 && $taxon[$taxon_no[$n]] ne "" )	{
			$printednodes++;
			$caption .= "$printednodes = $taxon[$node[$n]], ";
			# tweaks specific to this font
			my $xoffset = 3;
			if ( $clade_no[$n] =~ /1$/ )	{
				$xoffset = 2;
			} elsif ( $clade_no[$n] =~ /4$/ )	{
				$xoffset = 4;
			}
			if ( $clade_no[$n] < 10 )	{
				$im->filledArc($nodex,$nodey,15,15,0,360,$white);
				# debugging line
				#$im->arc($nodex,$nodey,15,15,0,360,$black);
				$im->stringFT($unantialiased,$FONT,10,0,$nodex - $xoffset,$nodey + 5,$printednodes);
			} else	{
				$im->filledArc($nodex,$nodey,19,15,0,360,$white);
				# debugging line
				#$im->arc($nodex,$nodey,19,15,0,360,$black);
				$im->stringFT($unantialiased,$FONT,10,0,$nodex - $xoffset - 2,$nodey + 5,$printednodes);
			}
		}
		# print bootstrap proportions
		if ( $terminal_no[$n] == 0 && $bootstrap[$taxon_no[$n]] > 0 )	{
			# there needs to be a white box in the background in
			#  case bending of lines would cause an overlap
			# has to be positioned very exactly
			if ( $bootstrap[$taxon_no[$n]] < 100 )	{
				$im->filledRectangle($nodex - 9,$nodey - 16,$nodex + 11,$nodey - 7, $white);
				$im->stringFT($unantialiased,$FONT,6,0,$nodex - 7,$nodey - 7,$bootstrap[$taxon_no[$n]] . "%");
			} else	{
				$im->filledRectangle($nodex - 10,$nodey - 16,$nodex + 12,$nodey - 7, $white);
				$im->stringFT($unantialiased,$FONT,6,0,$nodex - 9,$nodey - 7,$bootstrap[$taxon_no[$n]] . "%");
			}
		}
	}

	$caption =~ s/, $//;
	# save caption for printing by TaxonInfo
	#my $sql = "UPDATE cladograms SET modified=modified,caption=$caption WHERE cladogram_no=$cladogram_no";

	# test directory and file name
	my $PNG_DIR = "/Users/alroy/html/public";
	my $pngname = "blah.png";
	open PNG,">$PNG_DIR/$pngname";
	binmode(PNG);
	print PNG $im->png;
	close PNG;
	chmod 0664, "$PNG_DIR/$pngname";

}

# outline only
# pass in a "backbone" (more reliable) cladogram and a "secondary" cladogram
#  taken directly from the database, then merge them and pass back the
#  merged cladogram
# the backbone is gotten either from the database or from previous calls of
#   this function
sub mergeCladograms	{

	# shift data corresponding to fields such as parent_no from nodes table
	# these could be packaged as a hash of arrays such as:
	#  $backbone{$parent_no[$i]}

	# find set of terminal taxa found in both cladograms
	# all taxon_nos have to be converted into synonym_nos taken from
	#  taxa_tree_cache
	# record the node_nos of the terminals in a hash where the secondary
	#  node_no is the key and the backbone node_no is the value (i.e.,
	#  a mapping hash)

	# return if there are no overlapping taxa at all

	# compute two trimmed cladograms each only including the terminals
	#  whose taxon_nos are in both the backbone and secondary cladogram
	# there could be trouble if some taxa are terminals in one cladogram
	#  but parent nodes in another, not sure what to do about this

	# compare each parent in the secondary cladogram to those in the
	#  backbone and record whether it conflicts (is not matched) because
	#  it includes a different set of terminals
	# if there is no conflict, record the matching parent_nos in the
	#  mapping hash

	# having figured that out, for each conflicting node find the next
	#  highest parent node that does not conflict, and record that
	#  relationship in the mapping hash
	# now every node in the secondary cladogram has a mapping value

	# having dealt with the overlapping nodes, handle the ones only found
	#  in the secondary cladogram
	# for each one:
	#  if the parent_no does not map to one in the backbone, leave it alone
	#  if it does, translate the parent_no into the node_no in the backbone
	#   using the mapping hash 
	#  after deciding, add the taxon's node_no and its parent_no to
	#   the backbone; the addition order shouldn't matter

	# there should be no conflicts at this point, because each taxon found
	#  in both cladograms maps to a node_no originally found in the
	#  backbone, whereas the others each map to a node_no that was unique
	#  to its own cladogram, so there are no duplicate node_nos

	# return the merged cladogram

}



