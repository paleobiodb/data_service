# 17.12.06
# generate a page with dinosaur factoids

use DBI;
use DBConnection;
use DBTransactionManager;
use Session;

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

open OUT,">./guest_templates/dinosaurs.html";
$| = 1;

print OUT qq|
<div style="margin-left: 1em; margin-right: 1em;">
<h3 style="text-align: center;">Dinosaur facts and figures</h3>

<p class="medium">Here are some of the coolest dinosaur factoids you can get out of the Paleobiology Database, useful or not. Most of the data are based on Matt Carrano's <a href="/cgi-bin/bridge.pl?user=Guest&action=displayPage&page=OSA_Dinosauria">non-avian dinosaur systematics archive</a>.</p>
<p class="small">Technical note: birds are dinosaurs. However, birds are not included in any of the following because, well, they're birds.</p>
|;

my $sql;

$sql = "select lft,rgt from taxa_tree_cache t,authorities a where t.taxon_no=a.taxon_no and taxon_name='dinosauria' and taxon_rank='order'";
my $dlft = @{$dbt->getData($sql)}[0]->{lft};
my $drgt = @{$dbt->getData($sql)}[0]->{rgt};

$sql = "select lft,rgt from taxa_tree_cache t,authorities a where t.taxon_no=a.taxon_no and taxon_name='avialae'";
my $alft = @{$dbt->getData($sql)}[0]->{lft};
my $argt = @{$dbt->getData($sql)}[0]->{rgt};


print OUT "\n<h4>Collections with the most dinosaurs</h4>\n";

$sql = "select count(*) c,c.collection_no,collection_name from occurrences o,collections c,authorities a,taxa_tree_cache t where o.taxon_no=t.taxon_no and c.collection_no=o.collection_no and a.taxon_no=t.taxon_no and preservation not in ('form taxon','ichnofossil') and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) group by c.collection_no having c>9 order by c desc limit 10";

my @collrefs = @{$dbt->getData($sql)};
print OUT "\n<ul>\n";
for my $cr ( @collrefs )	{
	print OUT qq|<li><a href="bridge.pl?action=displayCollectionDetails&collection_no=$cr->{collection_no}">$cr->{collection_name}</a>\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Collections from the farthest ends of the Earth</h4>\n";
print OUT qq|<p class="small">Based on paleocoordinates.</p>|;

$sql = "select max(paleolat) latmax,min(paleolat) latmin,max(paleolng) lngmax,min(paleolng) lngmin from collections c,occurrences o,taxa_tree_cache t where c.collection_no=o.collection_no and o.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt)";

my $maxminref = @{$dbt->getData($sql)}[0];

$latmax = $maxminref->{latmax};
$latmin = $maxminref->{latmin};
$lngmax = $maxminref->{lngmax};
$lngmin = $maxminref->{lngmin};

$sql = "select c.collection_no,collection_name from collections c,occurrences o,taxa_tree_cache t where c.collection_no=o.collection_no and o.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and abs(paleolat-$latmax)<0.01";
my $cr = @{$dbt->getData($sql)}[0];
print OUT qq|<p class="medium">Farthest north: <a href="bridge.pl?action=displayCollectionDetails&collection_no=$cr->{collection_no}">$cr->{collection_name}</a></p>\n|;

$sql = "select c.collection_no,collection_name from collections c,occurrences o,taxa_tree_cache t where c.collection_no=o.collection_no and o.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and abs(paleolat-$latmin)<0.01";
$cr = @{$dbt->getData($sql)}[0];
print OUT qq|<p class="medium">Farthest south: <a href="bridge.pl?action=displayCollectionDetails&collection_no=$cr->{collection_no}">$cr->{collection_name}</a></p>\n|;

$sql = "select c.collection_no,collection_name from collections c,occurrences o,taxa_tree_cache t where c.collection_no=o.collection_no and o.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and abs(paleolng-$lngmax)<0.01";
$cr = @{$dbt->getData($sql)}[0];
print OUT qq|<p class="medium">Farthest east: <a href="bridge.pl?action=displayCollectionDetails&collection_no=$cr->{collection_no}">$cr->{collection_name}</a></p>\n|;

$sql = "select c.collection_no,collection_name from collections c,occurrences o,taxa_tree_cache t where c.collection_no=o.collection_no and o.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and abs(paleolng-$lngmin)<0.01";
$cr = @{$dbt->getData($sql)}[0];
print OUT qq|<p class="medium">Farthest west: <a href="bridge.pl?action=displayCollectionDetails&collection_no=$cr->{collection_no}">$cr->{collection_name}</a></p>\n|;


print OUT "\n<h4>Formations with the most dinosaur occurrences</h4>\n";

$sql = "select count(*) c,formation from occurrences o,collections c,taxa_tree_cache t where t.taxon_no=o.taxon_no and c.collection_no=o.collection_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and formation is not null and formation!='' group by formation having c>9 order by c desc limit 10";

my @collrefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for my $cr ( @collrefs )	{
	print OUT qq|<li><a href="bridge.pl?action=displayStrata&group_hint=&group_formation_member=$cr->{formation}">$cr->{formation}</a> ($cr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Countries with the most dinosaur occurrences</h4>\n";

$sql = "select count(*) c,country from occurrences o,collections c,taxa_tree_cache t where t.taxon_no=o.taxon_no and c.collection_no=o.collection_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and country is not null and country!='' group by country having c>9 order by c desc limit 10";

my @collrefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for my $cr ( @collrefs )	{
	print OUT qq|<li><a href="bridge.pl?action=displayCollResults&country=$cr->{country}">$cr->{country}</a> ($cr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Genera with the most occurrences</h4>\n";

$sql = "select taxon_name,o.taxon_no tn,count(*) c from occurrences o,authorities a,taxa_tree_cache t where t.taxon_no=a.taxon_no and a.taxon_no=o.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='genus' and preservation not in ('form taxon','ichnofossil') group by o.taxon_no order by c desc limit 10";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}">$sr->{taxon_name}</a> ($sr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Papers that named the most species</h4>\n";

$sql ="select count(*) as c,r.reference_no rn,r.author1last a1,r.author2last a2,r.otherauthors oa,r.pubyr py from refs r,authorities a,taxa_tree_cache t where r.reference_no=a.reference_no and a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') and ref_is_authority!='YES' group by a.reference_no order by c desc limit 30";
@rrefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
my $refs = 0;
my $minnames = 0;
for $rr ( @rrefs )	{
	$refs++;
	if ( $refs == 10 )	{
		$minnames = $rr->{c};
	}
	if ( $rr->{c} < $minnames )	{
		last;
	}
	my $ref;
	if ( $rr->{oa} =~ /[A-Za-z]/ )	{
		$ref = $rr->{a1} . " et al. " . $rr->{py};
	} elsif ( $rr->{a2} =~ /[A-Za-z]/ )	{
		$ref = $rr->{a1} . " and " . $rr->{a2} . " " . $rr->{py};
	} else	{
		$ref = $rr->{a1} . " " . $rr->{py};
	}
	printf OUT qq|<li><a href="bridge.pl?action=displayReference&reference_no=$rr->{rn}">$ref</a> ($rr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Most discussed species</h4>\n";
print OUT qq|<p class="small">Based on the number of published taxonomic opinions.</p>\n|;

$sql ="select count(*) c,taxon_name,a.taxon_no tn from authorities a,opinions o,taxa_tree_cache t where a.taxon_no=t.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') group by o.child_no order by c desc limit 20";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
my $spp = 0;
my $minops = 0;
for $sr ( @sprefs )	{
	$spp++;
	if ( $spp == 10 )	{
		$minops = $sr->{c};
	}
	if ( $sr->{c} < $minops )	{
		last;
	}
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}">$sr->{taxon_name}</a> ($sr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Species with the most synonymous names</h4>\n";

$sql ="select taxon_name,a.taxon_no tn,lft,rgt from authorities a,taxa_tree_cache t where a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') order by rgt-lft desc limit 10";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}">$sr->{taxon_name}</a> (%d)\n|,($sr->{rgt} - $sr->{lft} + 1) / 2;
}
print OUT "</ul>\n";
@sprefs = ();


print OUT "\n<h4>Species with the longest names</h4>\n";

$sql = "select taxon_name,a.taxon_no tn,length(taxon_name) l from authorities a,taxa_tree_cache t where a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') and taxon_name not like '%(%' order by l desc";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
my $chars = " characters";
my $minchar = 0;
for $i ( 0..29 )	{
	if ( ! @sprefs )	{
		last;
	}
	if ( $i == 9 )	{
		$minchar = $sprefs[$i]->{l};
	}
	if ( $sprefs[$i]->{l} < $minchar )	{
		last;
	}
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sprefs[$i]->{tn}">$sprefs[$i]->{taxon_name}</a> ($sprefs[$i]->{l}$chars)\n|;
	$chars = "";
}
print OUT "</ul>\n";


# reuse the data to find weird names
# "weird" means using letters that on average have the lowest frequency
#  for their positions
# weird properties:
#  (1) counts are of pairs of letters, not individual letters
#  (2) species names are ordered backwards because endings are more
#       standardized

my $tlets = 0;
for $sr ( @sprefs )	{
	my ($gen,$sp) = split / /,$sr->{taxon_name};
	my @glets = split //,$gen;
	my @slets = split //,$sp;
	for my $i ( 1..$#glets )	{
		$letfreq{$glets[$i]}++;
		$tlets++;
	}
	for my $i ( 0..$#slets )	{
		$letfreq{$slets[$i]}++;
		$tlets++;
	}
	# special treatment for first two letters
	$ginittotal++;
	$gfreq{$glets[0].$glets[1]}++;
	for my $i ( 1..$#glets-1 )	{
		$gtotal++;
		#$gtotal[$i]++;
		$gfreq{$glets[$i].$glets[$i+1]}++;
		#$gfreq[$i]{$glets[$i].$glets[$i+1]}++;
	}
	for my $i ( reverse 1..$#slets )	{
		my $j = $#slets - $i;
		$stotal++;
		#$stotal[$j]++;
		$sfreq{$slets[$j].$slets[$j-1]}++;
		#$sfreq[$j]{$slets[$j].$slets[$j-1]}++;
	}
}

for $sr ( @sprefs )	{
	my ($gen,$sp) = split / /,$sr->{taxon_name};
	my @glets = split //,$gen;
	my @slets = split //,$sp;
	my $lets = 0;
	$weird{$sr->{taxon_name}} += log($gfreq{$glets[0].$glets[1]} / $ginittotal);
	for my $i ( 1..$#glets-1 )	{
		$weird{$sr->{taxon_name}} += log($gfreq{$glets[$i].$glets[$i+1]} / $gtotal);
		#$weird{$sr->{taxon_name}} += $gfreq{$glets[$i].$glets[$i+1]} / $gtotal;
		#$weird{$sr->{taxon_name}} += $gfreq[$i]{$glets[$i].$glets[$i+1]} / $gtotal[$i];
		$lets++;
	}
	for my $i ( reverse 1..$#slets )	{
		my $j = $#slets - $i;
		$weird{$sr->{taxon_name}} += log($sfreq{$slets[$j].$slets[$j-1]} / $stotal);
		#$weird{$sr->{taxon_name}} += $sfreq{$slets[$j].$slets[$j-1]} / $stotal;
		#$weird{$sr->{taxon_name}} += $sfreq[$j]{$slets[$j].$slets[$j-1]} / $stotal[$j];
		$lets++;
	}
	$weird{$sr->{taxon_name}} /= $lets;
	$weird2{$sr->{taxon_name}} = $weird{$sr->{taxon_name}} *$lets / ( $lets - 1 );
	# weirdness should go on for more than a few letters
	$weird{$sr->{taxon_name}} /= $lets / ( $lets - 1 );
}

my @spp = keys %weird;
@spp2 = @spp;
@spp = sort { $weird{$a} <=> $weird{$b} } @spp;
@spp2 = sort { $weird2{$b} <=> $weird2{$a} } @spp2;

print OUT "\n<h4>Species with the weirdest names</h4>\n";
print OUT qq|<p class="small">Based on a very tasty secret formula.</p>\n|;

print OUT "\n<ul>\n";
my $points = " points";
for $i (0..9)	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_name=$spp[$i]">$spp[$i]</a> (%.2f$points)\n|,-1 * $weird{$spp[$i]};
	#printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_name=$spp[$i]">$spp[$i]</a> (%.1f$points)\n|,1000 * (0.1 - $weird{$spp[$i]});
	$points = "";
}
print OUT "\n</ul>\n";

print OUT "\n<h4>Species with the dullest names</h4>\n";
print OUT qq|<p class="small">Based on almost the same very tasty secret formula.</p>\n|;

print OUT "\n<ul>\n";
my $points = " points";
for $i (0..9)	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_name=$spp2[$i]">$spp2[$i]</a> (%.2f$points)\n|,-1 * $weird2{$spp2[$i]};
	$points = "";
}
print OUT "\n</ul>\n";


print OUT "\n<h4>Species named per decade</h4>\n";

$sql = qq|select floor(r.pubyr/10) decade,count(distinct(a.taxon_no)) c from refs r,authorities a,opinions o,taxa_tree_cache t where r.reference_no=a.reference_no and r.pubyr>1700 and ref_is_authority='YES' and t.taxon_no=a.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') group by decade order by decade|;
@sprefs = @{$dbt->getData($sql)};

$sql = qq|select floor(a.pubyr/10) decade,count(distinct(a.taxon_no)) c from authorities a,opinions o,taxa_tree_cache t where a.pubyr>1700 and ref_is_authority!='YES' and t.taxon_no=a.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') group by decade order by decade|;
@sprefs2 = @{$dbt->getData($sql)};

for $sr ( @sprefs )	{
	$countbydecade{$sr->{decade}} = $sr->{c}; 
}
for $sr ( @sprefs2 )	{
	$countbydecade{$sr->{decade}} += $sr->{c}; 
}

print OUT "\n<ul>\n";
for $decade ( 180..201 )	{
	if ( $countbydecade{$decade} )	{
		printf OUT qq|<li>%ds: $countbydecade{$decade}\n|,$decade * 10;
	}
}
print OUT "\n</ul>\n";


print OUT "\n<h4>The first species ever named</h4>\n";

# join on child_no in opinions to make sure the species is an original
#  combination (admittedly a nasty trick)
$sql = qq|select taxon_name,a.taxon_no tn,r.author1last a1,r.author2last a2,r.otherauthors oa,r.pubyr year,concat(r.pubyr," ",r.author1last) yrauth from refs r,authorities a,opinions o,taxa_tree_cache t where r.reference_no=a.reference_no and r.pubyr>1700 and r.pubyr<1845 and ref_is_authority='YES' and t.taxon_no=a.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') group by a.taxon_no order by year|;
@sprefs = @{$dbt->getData($sql)};

$sql = qq|select taxon_name,a.taxon_no tn,a.author1last a1,a.author2last a2,a.otherauthors oa,a.pubyr year,concat(a.pubyr," ",a.author1last) yrauth from authorities a,opinions o,taxa_tree_cache t where a.pubyr>1700 and a.pubyr<1845 and ref_is_authority!='YES' and t.taxon_no=a.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') group by a.taxon_no order by year|;
push @sprefs , @{$dbt->getData($sql)};

@sprefs = sort { $a->{yrauth} <=> $b->{yrauth} } @sprefs;

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}">$sr->{taxon_name}</a> ($sr->{year})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Species named in 2006</h4>\n";
print OUT qq|\n<p class="small">Silly as they may seem.</p>\n|;

$sql = "select taxon_name,a.taxon_no tn from refs r,authorities a,taxa_tree_cache t where r.reference_no=a.reference_no and r.pubyr=2006 and ref_is_authority='YES' and t.taxon_no=a.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' and preservation not in ('form taxon','ichnofossil') order by taxon_name";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}">$sr->{taxon_name}</a>\n|;
}
print OUT "</ul>\n";

print OUT "\n</div>\n";

