# 17.12.06
# generate a page with dinosaur factoids

use DBI;
use DBConnection;
use DBTransactionManager;
use Session;

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

open OUT,">./guest_templates/dinosaurs.html";

print OUT qq|
<div style="margin-left: 1em; margin-right: 1em;">
<h3 style="text-align: center;">Dinosaur facts and figures</h3>

<p class="medium">Here are some of the coolest dinosaur factoids you can get out of the Paleobiology Database, useful or not.</p>
<p class="small">Technical note: birds are dinosaurs. However, birds are not included in any of the following because, well, they're birds.</p>
|;

my $sql;

$sql = "select lft,rgt from taxa_tree_cache t,authorities a where t.taxon_no=a.taxon_no and taxon_name='dinosauria' and taxon_rank='order'";
my $dlft = @{$dbt->getData($sql)}[0]->{lft};
my $drgt = @{$dbt->getData($sql)}[0]->{rgt};

$sql = "select lft,rgt from taxa_tree_cache t,authorities a where t.taxon_no=a.taxon_no and taxon_name='aves' and taxon_rank='class'";
my $alft = @{$dbt->getData($sql)}[0]->{lft};
my $argt = @{$dbt->getData($sql)}[0]->{rgt};


print OUT "\n<h4>Collections with the most dinosaurs</h4>\n";

$sql = "select count(*) c,c.collection_no,collection_name from occurrences o,collections c,taxa_tree_cache t where t.taxon_no=o.taxon_no and c.collection_no=o.collection_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) group by c.collection_no having c>9 order by c desc limit 10";

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


print OUT "\n<h4>Species with the most occurrences</h4>\n";

$sql = "select taxon_name,o.taxon_no tn,count(*) c from occurrences o,authorities a,taxa_tree_cache t where t.taxon_no=a.taxon_no and a.taxon_no=o.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' group by o.taxon_no order by c desc limit 10";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}&is_real_user=1">$sr->{taxon_name}</a> ($sr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Papers that named the most species</h4>\n";

$sql ="select count(*) as c,r.reference_no rn,r.author1last a1,r.author2last a2,r.otherauthors oa,r.pubyr py from refs r,authorities a,taxa_tree_cache t where r.reference_no=a.reference_no and a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' group by a.reference_no order by c desc limit 30";
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


print OUT "\n<h4>Most debated species</h4>\n";
print OUT qq|<p class="small">Based on the number of published taxonomic opinions.</p>\n|;

$sql ="select count(*) c,taxon_name,a.taxon_no tn from authorities a,opinions o,taxa_tree_cache t where a.taxon_no=t.taxon_no and a.taxon_no=o.child_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' group by o.child_no order by c desc limit 20";
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
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}&is_real_user=1">$sr->{taxon_name}</a> ($sr->{c})\n|;
}
print OUT "</ul>\n";


print OUT "\n<h4>Species with the most synonymous names</h4>\n";

$sql ="select taxon_name,a.taxon_no tn,lft,rgt from authorities a,taxa_tree_cache t where a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' order by rgt-lft desc limit 10";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}&is_real_user=1">$sr->{taxon_name}</a> (%d)\n|,($sr->{rgt} - $sr->{lft} + 1) / 2;
}
print OUT "</ul>\n";


print OUT "\n<h4>Species with the longest names</h4>\n";

$sql = "select taxon_name,a.taxon_no tn,length(taxon_name) l from authorities a,taxa_tree_cache t where a.taxon_no=t.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and t.taxon_no=spelling_no and taxon_rank='species' and taxon_name not like '%(%' order by l desc";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
my $chars = " characters";
my $minchar = 0;
for $i ( 0..29 )	{
	if ( $i == 9 )	{
		$minchar = $sprefs[$i]->{l};
	}
	if ( $sprefs[$i]->{l} < $minchar )	{
		last;
	}
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sprefs[$i]->{tn}&is_real_user=1">$sprefs[$i]->{taxon_name}</a> ($sprefs[$i]->{l}$chars)\n|;
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

for $sr ( @sprefs )	{
	my ($gen,$sp) = split / /,$sr->{taxon_name};
	my @glets = split //,$gen;
	my @slets = split //,$sp;
	for my $i ( 0..$#glets-1 )	{
		$gtotal[$i]++;
		$gfreq[$i]{$glets[$i].$glets[$i+1]}++;
	}
	for my $i ( reverse 1..$#slets )	{
		my $j = $#slets - $i;
		$stotal[$j]++;
		$sfreq[$j]{$slets[$i].$slets[$i-1]}++;
	}
}

for $sr ( @sprefs )	{
	my ($gen,$sp) = split / /,$sr->{taxon_name};
	my @glets = split //,$gen;
	my @slets = split //,$sp;
	my $tlets = 0;
	for my $i ( 0..$#glets-1 )	{
		$weird{$sr->{taxon_name}} += $gfreq[$i]{$glets[$i].$glets[$i+1]} / $gtotal[$i];
		$tlets++;
	}
	for my $i ( reverse 1..$#slets )	{
		my $j = $#slets - $i;
		$weird{$sr->{taxon_name}} += $sfreq[$j]{$slets[$i].$slets[$i-1]} / $stotal[$j];
		$tlets++;
	}
	$weird{$sr->{taxon_name}} /= $tlets;
}

my @spp = keys %weird;
@spp = sort { $weird{$a} <=> $weird{$b} } @spp;

print OUT "\n<h4>Species with the weirdest names</h4>\n";
print OUT qq|<p class="small">Based on a very tasty secret formula.</p>\n|;

print OUT "\n<ul>\n";
my $points = " points";
for $i (0..9)	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_name=$spp[$i]&is_real_user=1">$spp[$i]</a> (%.1f$points)\n|,1000 * (0.1 - $weird{$spp[$i]});
	$points = "";
}
print OUT "\n</ul>\n";


print OUT "\n<h4>Species named in 2006</h4>\n";
print OUT qq|\n<p class="small">Silly as they may seem.</p>\n|;

$sql = "select taxon_name,a.taxon_no tn from refs r,authorities a,taxa_tree_cache t where r.reference_no=a.reference_no and r.pubyr=2006 and ref_is_authority='YES' and t.taxon_no=a.taxon_no and lft>=$dlft and rgt<=$drgt and (lft<$alft or rgt>$argt) and taxon_rank='species' order by taxon_name";
@sprefs = @{$dbt->getData($sql)};

print OUT "\n<ul>\n";
for $sr ( @sprefs )	{
	printf OUT qq|<li><a href="bridge.pl?action=checkTaxonInfo&taxon_no=$sr->{tn}&is_real_user=1">$sr->{taxon_name}</a>\n|;
}
print OUT "</ul>\n";

print OUT "\n</div>\n";

