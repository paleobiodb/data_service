package TaxonInfo;

use PBDBUtil;
use Classification;
use Globals;
use Debug;

use URLMaker;
use DBTransactionManager;
use Taxon;

use POSIX qw(ceil floor);


$DEBUG = 0;

my %GLOBALVARS;


sub startTaxonInfo {
	my $q = shift;

	print main::stdIncludes( "std_page_top" );
	print searchForm($q);
	print main::stdIncludes("std_page_bottom");
}



# JA: rjp did a big complicated reformatting of the following and I can't
#  confirm that no damage was done in the course of it
sub searchForm{
	my $q = shift;
	my $search_again = (shift or 0);

	my $html = "";

	unless($search_again){
		$html .= "<div class=\"title\">Taxon search form</div>";
	}

	$html .= "\n<script language=\"Javascript\">\n".
			 "function checkInput(){\n".
			 "	var rank = document.forms[0].taxon_rank.options[document.forms[0].taxon_rank.selectedIndex].text;\n".
			 "	var value = document.forms[0].taxon_name.value;\n".
			 "	var author = document.forms[0].author.value;\n".
			 "	var pubyr = document.forms[0].pubyr.value;\n".
			 "	if(value == \"\" && author == \"\" && pubyr == \"\"){\n".
			 "		alert('Please enter a search term.');\n".
			 "		return false;\n".
			 "	}\n".
			 "	if(value.match(/[A-Za-z]+\\s+[a-z]+/) && rank != 'Genus and species'){\n".
			 "		alert('Please choose rank \"Genus and species\" to search for a species');\n".
			 "		return false;\n".
			 "	}\n".
			 "	if(rank == 'Genus and species' && value.match(/^[A-Z]{1}[a-z]+\\s+[a-z]+\$/) == null){\n".
			 "		alert('Please enter a name in the form \"Genus species\"');\n".
			 "		return false;\n".
			 "	}\n".
			 "	else{\n".
			 "		return true;\n".
			 "	}\n".
			 "}\n".
			 "</script>\n";
			 
	$html .= "<FORM method=post action=\"/cgi-bin/bridge.pl\" onSubmit=\"return checkInput();\">
	
		   <input id=\"action\" type=hidden name=\"action\" value=\"checkTaxonInfo\">
		   <input id=\"user\" type=hidden name=\"user\" value=\"".$q->param("user")."\">
		   

		   <TABLE align=center border=0>
		   <TR>
		   <TD align=right>Rank:</TD>
		   <TD>
		   <SELECT name=\"taxon_rank\">
		   
		  <!-- <option selected></option> -->
		   
		   <option>Higher taxon</option>
		   <option selected>Genus<option>Genus and species</option>
		   <option>species</option>
		   </SELECT>
		   </TD>
		   <TD align=right>Name:</TD>
		   <TD>
		   <INPUT name=\"taxon_name\" type=\"text\" size=25>
		   </TD>
		   </TR>
		   
		   <TR>
		   <TD align=right>Author:</TD>
		   <TD>
		   <INPUT id=\"author\" name=\"author\" size=12>
		   </TD>
		   <TD align=right>Year:</TD>
		   <TD>
		   <INPUT id=\"pubyr\" name=\"pubyr\" size=5>
		   </TD>
		   </TR>

		<!--
		   <TR>
		   <TD align=right>
		   <INPUT id=\"authorities_only\" type=checkbox name=\"authorities_only\" value=\"YES\" checked>
		   </TD>
		   <TD align=left>
		   Search in authorities only.
		   </TD>
		   </TR>
		 -->
		  	
		  	</TABLE>
		  	<BR> 
		  
		   <CENTER>
		   <input id=\"submit\" name=\"submit\" value=\"Get info\"
		    type=\"submit\"></td></tr></table>
		   <!--<p><i>The taxon name field is required.</i></p>-->
		   </CENTER>
		   <BR>
		   </form>";

	return $html;
}


# pass this a taxon name string with spaces between each part,
# ie, Homo sapiens, or Homo, or Bogus bogus bogus, and it will
# return a hash three strings - the Genus/Higher taxon (higher), 
# species (species), and subspecies name (subspecies). 
# JA: this is rjp code only used in following, unused, unneeded, and 
#  undebugged code, so probably should be deleted
sub splitTaxonName {
	my $input = shift;
	
	$input =~ m/^([A-Z][a-z]+)([ ][a-z]+)?([ ][a-z]+)?/;
	my $genusOrHigher = $1;
	my $species = $2;
	my $subspecies = $3;
	
	# if they only passed in a species name, then it won't be capitialized.
	if (! $genusOrHigher) {
		$input =~ m/(^[a-z]+).*$/;
		$species = $1;
	}
	
	my %toreturn;
	$toreturn{'higher'} = $genusOrHigher;
	$toreturn{'species'} = $species;
	$toreturn{'subspecies'} = $subspecies;

	return %toreturn;
}


# Note, this is not finished yet.. Don't use it for anything other than testing.
# Started writing this to deal with a bug report.. but have abandoned it for now.
# rjp 2/2004.
# JA: indeed, this isn't used anywhere, needlessly duplicates existing
#  functionality, and probably should be deleted - implementing it would
#  require needless and dangerous debugging
sub newCheckStartForm {
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
	
	my $LIMIT = 500;	# don't allow more than this many results...
	
	$GLOBALVARS{session} = $s;
	
	my $sql1 = DBTransactionManager->new(\%GLOBALVARS);
	my $sql2 = DBTransactionManager->new(\%GLOBALVARS);
	
	$sql1->setLimitExpr($LIMIT);
	$sql2->setLimitExpr($LIMIT);
	
	# grab the parameters off the form.
	my $taxon_rank = $q->param('taxon_rank');
	my $taxon_name = $q->param('taxon_name');
	my $author = $q->param('author');
	my $pubyr = $q->param('pubyr');

	# build up the SQL to check for ref_is_authority authors/pubyrs and 
	# authors/pubyrs stored in the actual authorities table simultaneously.
	$sql1->setWhereSeparator("AND");
	$sql2->setWhereSeparator("AND");
	
			
	$sql1->setSelectExpr("a.taxon_no");
	$sql2->setSelectExpr("taxon_no");
	
	$sql1->setFromExpr("authorities a, refs r");
	$sql2->setFromExpr("authorities");
	
	$sql1->addWhereItem("a.ref_is_authority = 'YES'");
	
	# add author to the query
	if ($author ne '') {
		$sql1->addWhereItem(" (r.author1last = '" . $author ."' OR 
			r.author2last = '" . $author . "' OR 
			r.otherauthors LIKE '\%" . $author ."\%')");
		$sql2->addWhereItem(" (author1last = '" . $author ."' OR 
			author2last = '" . $author . "' OR 
			otherauthors LIKE '\%" . $author ."\%')");
	}
	
	# add pubyr to the query
	if ($pubyr ne '') {
		$sql1->addWhereItem("r.pubyr = '" . $pubyr . "'");
		$sql2->addWhereItem("pubyr = '" . $pubyr . "'");
	}
	
	# add taxon_rank to the query
	if ($taxon_rank ne '') {
		
		# figure out the SQL for the rank.
		my $rankSQL;
		
		if ($taxon_rank eq 'Higher taxon') {
			$rankSQL = "taxon_rank NOT IN ('subspecies', 'species', 'subgenus', 'genus')";
		} elsif ($taxon_rank eq 'Genus') {
			$rankSQL = "taxon_rank = 'genus'";
		} elsif ($taxon_rank eq 'Genus and Species') {
			$rankSQL = "taxon_rank = 'species'";
		} elsif ($taxon_rank eq 'species') {
			# note, "species" is a special case for this form - we're supposed to literally
			# search for a species, not genus.. Ie, only the second part of the name.
			$rankSQL = "taxon_rank = 'species'";
		}
		
		$sql1->addWhereItem($rankSQL);
		$sql2->addWhereItem($rankSQL);
	}
	
	my %ranks;
	# add taxon_name to the query
	if ($taxon_name ne '') {
		my $nameSQL;
		
		# split the taxon name into its constituents.
		%ranks = splitTaxonName($taxon_name);

		if ($taxon_rank eq 'species') {
			# special case, for this form, we just want the second name, not the Genus and species.
			
			$nameSQL = "taxon_name LIKE '%". $ranks{'species'} ."%'"; 	
		} else {
			$nameSQL = "taxon_name = '" . $taxon_name . "'";
		}
		
		$sql1->addWhereItem($nameSQL);
		$sql2->addWhereItem($nameSQL);
	}
	

	$sql1->addWhereItem("a.reference_no = r.reference_no");
	

	my @taxa;  # array of all taxa to return (Taxon objects)
	my $taxon;
	
	# execute the SQL queries and add the results (taxa) to the @taxa array
	
	my $resultRef = $sql1->allResultsArrayRef();
	print "sql = " . $sql1->SQLExpr() . "<BR><BR>";
	
	foreach my $result (@$resultRef) {
		$taxon = Taxon->new();
		$taxon->setWithTaxonNumber($result->[0]);
		push (@taxa, $taxon);
	}
	
	$resultRef = $sql2->allResultsArrayRef();
	print "sql = " . $sql2->SQLExpr() . "<BR><BR>";
	
	foreach my $result (@$resultRef) {
		$taxon = Taxon->new();
		$taxon->setWithTaxonNumber($result->[0]);
		push (@taxa, $taxon);	
	}
	
	
	# at this point, we have found all of the taxa which are in the authorities
	# table.. This still leaves us with no listing for the taxa which only 
	# exist in the occurrences and reids table...
	
	# only look for these if the authorities_only checkbox is turned off.
	
	if ($q->param("authorities_only") ne "YES") {
		# Note, if a genus_name exists in several occurrence or reid records, then
		# this will find one for each distinct collection number.
		
		my $halfLIMIT = floor($LIMIT/2);
		
		# if they just search for a genus, then the $ranks variable
		# will list it as a higher taxon because it can't tell.. However,
		# if they search for genus and species, then it will be in the genus variable.
		my $genus = $ranks{'genus'} || $ranks{'higher'};
		my $species = $ranks{'species'};
		
		my $genusSQL = "";
		my $speciesSQL = "";
		
		if ($genus ne '') {
			$genusSQL = " genus_name LIKE '%$genus%' ";
		}
		if ($species ne '') {
			$speciesSQL = " species_name LIKE '%$species%' ";
		}
		
		print "genus = $genus, species = $species";
		
		if ($genus || $species) {
			my $tempstr = "SELECT DISTINCT collection_no, genus_name FROM reidentifications
			WHERE $genusSQL $speciesSQL LIMIT $halfLIMIT 
			UNION 
			SELECT DISTINCT collection_no, genus_name FROM occurrences
			WHERE $genusSQL $speciesSQL LIMIT $halfLIMIT";
	
			
			$sql1->clear();
			$sql1->setSQLExpr($tempstr);
			
			print "sql = " . $sql1->SQLExpr() . "<BR><BR>";
		
			# must use permissions for reidentifications and occurrences
			$resultRef = $sql1->allResultsArrayRefUsingPermissions();
		
			foreach my $result (@$resultRef) {
				$taxon = Taxon->new();
				
				my $name = $result->[1];
				if ($result->[2]) {
					$name .= " " . $result->[2];
				}
				
				$taxon->setWithTaxonName($name);
				push (@taxa, $taxon);
			}
		}
	}	
	
	
	# print out the results
	foreach my $taxon (@taxa) {
		print $taxon->taxonName() . " (" . $taxon->taxonNumber() . ") <BR>";	
	}
	
	return; 

}
	
	
	
sub checkStartForm {	
	
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
	
	$GLOBALVARS{session} = $s;
	
	# the "Original Rank" popup menu on the entry form
	my $taxon_type = $q->param("taxon_rank");
	
	$taxon_type =~ s/\+/ /g;  # remove plus signs??  why?
	
	if($taxon_type ne "Genus" && $taxon_type ne "Genus and species" && 
			$taxon_type ne "species") {
		$taxon_type = "Higher taxon";
		# note, does this really make sense?  is a "subgenus" a "higher taxon"?
	}
	
	# the "Name of type taxon" text entry field on the entry form
	my $taxon_name = $q->param("taxon_name");
	$taxon_name =~ s/\+/ /g;
	
	# Where do these two parameters come from?  The current reference?
	my $author = $q->param("author");
	my $pubyr = $q->param("pubyr");
	
	my $results = "";
	my $genus = "";
	my $species = "";
	my $sql =""; 
	my @results;
	my @refnos;

	# find possibly matching refs in the refs table, needed if ref_is_authority below
	if ( $author && $pubyr )	{
		$sql = "SELECT reference_no FROM refs WHERE author1last='" . $author . "' AND pubyr='" . $pubyr . "'";
		@refrefs = @{$dbt->getData($sql)};
	} elsif ( $author )	{
		$sql = "SELECT reference_no FROM refs WHERE author1last='" . $author . "'";
		@refrefs = @{$dbt->getData($sql)};
	} elsif ( $pubyr )	{
		$sql = "SELECT reference_no FROM refs WHERE pubyr='" . $pubyr . "'";
		@refrefs = @{$dbt->getData($sql)};
	}
	
	for $refref ( @refrefs )	{
		push @refnos, $refref->{reference_no};
	}
	
	if ( @refnos )	{
		$refinlist = "(" . join(',',@refnos) . ")";
	}
	
	# whoops, author and/or year search failed  - make a match impossible
	if ( ( $author || $pubyr ) && ! $refinlist )	{
		$refinlist = "( -1 )";
	}

	# If they gave us nothing, start again.
	if($taxon_type eq "" or ( $taxon_name eq "" && ! $author && ! $pubyr ) ){
	    print $q->redirect(-url=>$BRIDGE_HOME."?action=beginTaxonInfo");
	    exit;
	}

	if ( ! $taxon_name && ( $author || $pubyr ) )	{
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities WHERE";
		if ( $author )	{
			$sql .= " ( author1last='" . $author . "'";
		# be kind of slack here: if the record ref exists and matches
		#  the author and there is no author1last, just assume that
		#  the record ref is the authority
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		if ( $pubyr )	{
			if ( $author )	{
				$sql .= " AND";
			}
			$sql .= " ( pubyr='" . $pubyr. "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		@results = @{$dbt->getData($sql)};
		foreach my $ref (@results){
			$ref->{genus_name} = $ref->{taxon_name};
 			delete $ref->{taxon_name};
		}
		# DON'T GO to the occs and reID tables because they have
		#  no author/year data
	}
	# Higher taxon
	elsif($taxon_type eq "Higher taxon"){
		$q->param("genus_name" => $taxon_name);
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities ".
			   "WHERE taxon_name='$taxon_name'";
		if ( $author )	{
			$sql .= " AND ( author1last='" . $author . "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		if ( $pubyr )	{
			$sql .= " AND ( pubyr='" . $pubyr. "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity down the line
		foreach my $ref (@results){
			$ref->{genus_name} = $ref->{taxon_name};
 			delete $ref->{taxon_name};
		}
		# if nothing from authorities, go to occurrences and reidentifications
		if(scalar @results < 1 && ! $author && ! $pubyr ){
			$q->param("no_classification" => "true");
			$sql = "SELECT DISTINCT(genus_name) FROM occurrences ".
				   "WHERE genus_name like '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql = "SELECT DISTINCT(genus_name) FROM reidentifications ".
				   "WHERE genus_name like '$taxon_name'";
			# collapse duplicates
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name")};
		}
	}
	# Exact match search for 'Genus and species'
	elsif($taxon_type eq "Genus and species"){
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities ".
			   "WHERE taxon_name = '$taxon_name' AND taxon_rank='species'";
		if ( $author )	{
			$sql .= " AND ( author1last='" . $author . "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		if ( $pubyr )	{
			$sql .= " AND ( pubyr='" . $pubyr. "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		@results = @{$dbt->getData($sql)};
		# NOTE: this may not be necessary, but we'll leave it in for now.
		# Check for homonyms
		if(scalar @results > 1){
			@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1 && ! $author && ! $pubyr ){
			($genus,$species) = split(/\s+/,$taxon_name);
			$sql ="select distinct(species_name),genus_name ".
				  "from occurrences ".
				  "where genus_name ='$genus' ".
				  "and species_name = '$species'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(species_name),genus_name ".
				  "from reidentifications ".
				  "where genus_name ='$genus' ".
				  "and species_name = '$species'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name", "species_name")};
		}
	}
	# 'Genus'
	elsif($taxon_type eq "Genus"){
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities WHERE ".
			   "taxon_name='$taxon_name' AND taxon_rank='Genus'";
		if ( $author )	{
			$sql .= " AND ( author1last='" . $author . "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		if ( $pubyr )	{
			$sql .= " AND ( pubyr='" . $pubyr. "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1 && ! $author && ! $pubyr ){
			$sql ="select distinct(genus_name),species_name ".
				  "from occurrences ".
				  "where genus_name = '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(genus_name),species_name ".
				  "from reidentifications ".
				  "where genus_name = '$taxon_name'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name","species_name")};
		}
	}
	# or a species
	else{
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities WHERE ".
			   "taxon_name like '% $taxon_name' AND taxon_rank='species'";
		if ( $author )	{
			$sql .= " AND ( author1last='" . $author . "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		if ( $pubyr )	{
			$sql .= " AND ( pubyr='" . $pubyr. "'";
			$sql .= " OR ( ref_is_authority='YES' AND reference_no IN " . $refinlist . " ) )";
		}
		@results = @{$dbt->getData($sql)};
		# DON'T NEED TO DO THIS WITH SPECIES, METHINKS...
		# Check for homonyms
		#if(scalar @results > 1){
		#	@results = @{deal_with_homonyms($dbt, \@results)};
		#}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1 && ! $author && ! $pubyr ){
			$sql ="select distinct(species_name),genus_name ".
				  "from occurrences ".
				  "where species_name = '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(species_name),genus_name ".
				  "from reidentifications ".
				  "where species_name = '$taxon_name'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name","species_name")};
		}
	}
	# now deal with results:
	if(scalar @results < 1 ){
		print main::stdIncludes("std_page_top");
		print "<center><h3>No results found</h3>";
		print "<p><b>Please search again</b></center>";
		print searchForm($q, 1); # param for not printing header with form
		if($s->get("enterer") ne "Guest" && $s->get("enterer") ne ""){
			print "<center><p><a href=\"/cgi-bin/bridge.pl?action=startTaxonomy\"><b>Add taxonomic information</b></a></center>";
		}
		print main::stdIncludes("std_page_bottom");
	}
	# if we got just one result, it could be higher taxon, or an exact
	# 'Genus and species' match.
	elsif(scalar @results == 1){
		$q->param("genus_name" => $results[0]->{genus_name}." ".$results[0]->{species_name});
		displayTaxonInfoResults($q, $dbh, $s, $dbt);
	}
	# Show 'em their choices (radio buttons for genus-species)
	else{
		# REWRITE NO_MAP OR NO_CLASSIFICATION AS HIDDENS
		print main::stdIncludes( "std_page_top" );
		print "<center><h3>Please select a taxon</h3>";
		print "<form method=post action=\"/cgi-bin/bridge.pl\">".
			  "<input id=\"action\" type=\"hidden\"".
			  " name=\"action\"".
			  " value=\"displayTaxonInfoResults\">".
			  "<input name=\"taxon_rank\" type=\"hidden\" value=\"$taxon_type\">".
			  "<table width=\"100%\">";
		my $newrow = 0;
		my $choices = @results;
		my $NUMCOLS = 3;
		my $numrows = int($choices / $NUMCOLS);
		if($numrows == 0){
			$numrows = 1;
		}
		elsif($choices % $NUMCOLS > 0){
			$numrows++;
		}
		for(my $index=0; $index<$numrows; $index++){
			print "<tr>";
			for(my $counter=0; $counter<$NUMCOLS; $counter++){
				last if($index+($counter*$numrows) >= @results);
				print "<td><input type=\"radio\" name=\"genus_name\" value=\"";
				print $results[$index+($counter*$numrows)]->{genus_name}.
					  " ". $results[$index+($counter*$numrows)]->{species_name};
				if($results[$index+($counter*$numrows)]->{clarification_info}){
					print " (".$results[$index+($counter*$numrows)]->{taxon_no}.")";
				}
				print "\"><i>&nbsp;".$results[$index+($counter*$numrows)]->{genus_name}."&nbsp;".
					  $results[$index+($counter*$numrows)]->{species_name};
				if($results[$index+($counter*$numrows)]->{clarification_info}){
					print " </i><small>".$results[$index+($counter*$numrows)]->{clarification_info}."</small></td>";
				}
				else{
					print "</i></td>";
				}
			}
			print "</tr>";
		}
		print "<tr><td align=\"middle\" colspan=3>";
		print "<input type=\"submit\" value=\"Get taxon info\">";
		print "</td></tr></table></form></center>";

		print main::stdIncludes("std_page_bottom");
	}

}




# by the time we get here, the occurrences table has been queried.
# ALSO by the time we're here, we have either a single name
# higher taxon, or a "Genus species" combination.
#
# Called from bridge::processModuleNavigation() as well as other places.
sub displayTaxonInfoResults {
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;

	$GLOBALVARS{session} = $s;
	
	my $genus_name = $q->param("genus_name");
	my $taxon_type = $q->param("taxon_rank");
	my $taxon_no = 0;


	# Looking for "Genus (23456)" or something like that.
	
	# rjp, note, 2/20/2004 - this won't work if the taxon isn't 
	# in the authorities table...	
	# JA: of course it will, (23456) wouldn't be in the form in the first
	#  place unless it did exist in the table; if it does then $taxon_no
	#  will be set correctly, otherwise it's blank as it should be
	#
	# Apparently the taxon_no is sometimes passed in parenthesis??!?!
	# JA: duh, it's always passed in parentheses if present
	# ie, Homo (1231234)  ??
	$genus_name =~ /(.*?)\((\d+)\)/;
	$taxon_no = $2;

	# following section restored by JA from PM code 27.3.04
	# cut off the other stuff if it exists (JA: i.e., the number and
	#  NOT the species name
	if ($taxon_no) {
		$genus_name = $1;
		$genus_name =~ s/\s+$//; # remove trailing spaces.
	} else {
		# just do this
		# JA: at this point genus_name may actually be a genus
		#  plus species combination
		$genus_name =~ s/\s+$//; # remove trailing spaces.
	}

	# JA: point here is to reset genus_name param to be a string only
	#  with no numbers, not to actually wipe out a species name with
	#  a genus name
	if ($taxon_no) {  # if it's in the authorities table
		$q->param("genus_name" => $genus_name);
	} 

	
	# Keep track of entered name for link at bottom of page
	# JA: this is crucial, at this point genus_name would definitely
	#  still include the species name if any had been entered by the
	#  user, and so must be saved
	my $entered_name = $genus_name;
	if (!$taxon_no) {
		my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='".
				  $entered_name."'";
		$taxon_no = ${$dbt->getData($sql)}[0]->{taxon_no};
		# Strip extraneous spaces in name in case we got a 'Genus species' combo
		$entered_name =~ s/\s+/ /g;
	}
	my $entered_no = $taxon_no;

	# Verify taxon:  If what was entered's most recent parent is a "belongs to"
	# or a "nomen *" relationship, then do the display page on what was entered.
	# If any other relationship exists for the most recent parent, display info 
	# on that parent.  
	my @verified = verify_chosen_taxon($genus_name, $taxon_no, $dbt);
	$genus_name = $verified[0];
	$q->param("genus_name" => $genus_name);
	$taxon_no = $verified[1];
	
	# Get the sql IN list for a Higher taxon:
	my $in_list = "";

	if($taxon_type eq "Higher taxon"){
		my $name = $q->param('genus_name');
		$in_list = `./recurse $name`;
	} elsif ( ! $taxon_no )	{
	# Don't go looking for junior synonyms if this taxon isn't even
	#  in the authorities table (because it has no taxon_no) JA 8.7.03
		$in_list = $q->param('genus_name');
	} else	{

	# Find all the junior synonyms of this genus or species JA 4.7.03
	# First find all taxa that ever were children of this taxon no
	my $sql = "SELECT child_no, count(*) FROM opinions WHERE parent_no=";
	$sql .= $taxon_no . " AND status != 'belongs to' GROUP BY child_no";
	my @results = @{$dbt->getData($sql)};
	for my $ref (@results)	{
		push @childlist,$ref->{child_no};
	}
	# For each child, confirm that this is the most recent opinion
	for my $child (@childlist)	{
		my $sql = "SELECT parent_no,pubyr,reference_no,status FROM opinions WHERE child_no=";
		$sql .= $child . " AND status!='belongs to'";
		my @results = @{$dbt->getData($sql)};
		my $currentParent = "";
		my %recombined = ();

	# rewrote this section to employ selectMostRecentParentOpinion
	# JA 5.4.04
		$currentParent = selectMostRecentParentOpinion($dbt, \@results);

		my $maxyr = 0;
		for my $ref (@results)	{
			if ( $ref->{status} eq "recombined as" )	{
				$recombined{$ref->{parent_no}}++;
			}
		}
		# If the most recent opinion makes this a synonym, record its
		#  name AND those of recombinations
		if ( $currentParent == $taxon_no )	{
			my @recombs = keys %recombined;
			push @recombs,$child;
			for my $comb_no (@recombs)	{
				Debug::dbPrint("test1 = $comb_no");
				my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=";
				$sql .= $comb_no;
				my @results = @{$dbt->getData($sql)};
				for my $ref (@results)	{
					push @synonyms, $ref->{taxon_name};
				}
			}
		}
	}
	$in_list =  join ',',@synonyms;
	}


	print main::stdIncludes("std_page_top");

	# Write out a hidden with the 'genus_name' and 'taxon_rank' for subsequent
	# hits to this page
	my $exec_url = $q->url();
	print "<form name=module_nav_form method=POST action=\"$exec_url\">";
    print "<input type=hidden name=action value=processModuleNavigation>";
    print "<input type=hidden name=genus_name value=\"".
		  $q->param('genus_name');
    if ( $taxon_no )	{
      print " ($taxon_no)";
    }
    print "\">";
    print "<input type=hidden name=taxon_rank value=\"$taxon_type\">";

	# Now, the checkboxes and submit button, 

	my @modules_to_display = $q->param('modules');
	my %module_num_to_name = (1 => "classification",
							  2 => "taxonomic history",
							  3 => "synonymy",
							  4 => "ecology/taphonomy",
							  5 => "map",
							  6 => "collections");

	# Set the default:
	if(!@modules_to_display){
		$modules_to_display[0] = 1;
	}
	
	# Put in order
	@modules_to_display = sort {$a <=> $b} @modules_to_display;

	# First module has the checkboxes on the side.
	print "<table width=\"80%\"><tr><td></td>";
	print "<td align=center><h2>$genus_name</h2></td></tr>";	
	print "<tr><td valign=top><table cellspacing=2><tr><td bgcolor=black>";
	print "<table class='darkList'><tr><td valign=\"top\">".
		  "<center><b><div class=\"large\">Display</div></b></center></td></tr>";
	print "<tr><td align=left valign=top>";
	
	foreach my $key (sort keys %module_num_to_name){
		print "<nobr><input type=checkbox name=modules value=$key";
		foreach my $checked (@modules_to_display){
			if($key == $checked){
				print " checked";
				last;
			}
		}
		print ">$module_num_to_name{$key}</nobr><br>";
	}

	# image thumbs:
	my @selected_images = $q->param('image_thumbs');
	require Images;
	# not sure why, but somehow $in_list gets fatally cluttered with
	#  single quotes before this point
	$in_list =~ s/\'//g;
	my @thumbs = Images::processViewImages($dbt, $q, $s, $in_list);
	foreach my $thumb (@thumbs){
		print "<input type=checkbox name=image_thumbs value=";
		print $thumb->{image_no};
		foreach my $image_num (@selected_images){
			if($image_num == $thumb->{image_no}){
				print " checked";
				last;
			}
		}
		print ">";
		my $thumb_path = $thumb->{path_to_image};
		$thumb_path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
		print "<img align=middle src=\"$thumb_path\" border=1 vspace=3><br>";
	}

	print "<p><center><input type=submit value=\"update\"></center>";
	print "</td></tr></table></td></tr></table></td>";
	print "<td>";
	# First module here
	my $first_module = shift @modules_to_display;
	doModules($dbt,$dbh,$q,$s,$exec_url,$first_module,$genus,$species,$in_list,$taxon_no);
	print "</td></tr></table>";
	print "<hr width=\"100%\">";

	# Go through the list
	foreach my $module (@modules_to_display){
		print "<center><table>";
		print "<tr><td>";
		doModules($dbt,$dbh,$q,$s,$exec_url,$module,$genus,$species,$in_list,$taxon_no);
		print "</td></tr>";
		print "</table></center>";
		print "<hr width=\"100%\">";
	}
	# images are last
	if(@selected_images){
		print "<center><h3>Images</h3></center>";
		foreach my $image (@selected_images){
			foreach my $res (@thumbs){
				if($image == $res->{image_no}){
					print "<center><table><tr><td>";
					print "<center><img src=\"".$res->{path_to_image}.
						  "\" border=1></center><br>\n";
					print "<i>".$res->{caption}."</i><p>\n";
					if ( $res->{taxon_no} != $taxon_no )	{
						print "<div class=\"small\"><b>Original identification:</b> ".$res->{taxon_name}."</div>\n";
					}
					print "<div class=\"small\"><b>Original name of image:</b> ".$res->{original_filename}."</div>\n";
					if ( $res->{reference_no} > 0 )	{
						$sql = "SELECT author1last, author2last, otherauthors, pubyr FROM refs WHERE reference_no=" . $res->{reference_no};
						my @refresults = @{$dbt->getData($sql)};
						my $refstring = $refresults[0]->{author1last};
						if ( $refresults[0]->{otherauthors} )	{
							$refstring .= " et al.";
						} elsif ( $refresults[0]->{author2last} )	{
							$refstring .= " and " . $refresults[0]->{author2last};
						}
						$refstring =~ s/and et al\./et al./;
						print "<div class=\"small\"><b>Reference:</b> 
<a href=$exec_url?action=displayRefResults&reference_no=".$res->{reference_no}.">".$refstring." ".$refresults[0]->{pubyr}."</a></div>\n";
					}
					print "</td></tr></table></center>";
					print "<hr width=\"100%\">";
					last;
				}
			}
		}
	}

	my $clean_entered_name = $entered_name;
	$clean_entered_name =~ s/ /\+/g;

	print "<div style=\"font-family : Arial, Verdana, Helvetica; font-size : 14px;\">";
	if($s->get("enterer") ne "Guest" && $s->get("enterer") ne ""){
		# Entered Taxon
		print "<center><a href=\"/cgi-bin/bridge.pl?action=".
			  "startTaxonomy&taxon_name=$clean_entered_name";
		if($entered_no){
			  print "&taxon_no=$entered_no";
		}
		print "\"><b>Edit taxonomic data for $entered_name</b></a> - ";
		
		unless($entered_name eq $genus_name){

			my $clean_genus_name = $genus_name;
			$clean_genus_name =~ s/ /\+/g;

			# Verified Taxon
			print "<a href=\"/cgi-bin/bridge.pl?action=".
				  "startTaxonomy&taxon_name=$clean_genus_name";
			if($taxon_no){
				  print "&taxon_no=$taxon_no";
			}
			print "\"><b>Edit taxonomic data for $genus_name</b></a> - \n";
		}
		print "<a href=\"/cgi-bin/bridge.pl?action=startImage\">".
			  "<b>Enter an image</b></a> - \n";
	}
	else{
		print "<center>";
	}

	print "<a href=\"/cgi-bin/bridge.pl?action=beginTaxonInfo\">".
		  "<b>Get info on another taxon</b></a></center></div>\n";

	print "</form><p>";
	print main::stdIncludes("std_page_bottom");
}

sub doModules{
	my $dbt = shift;
	my $dbh = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;
	my $module = shift;
	my $genus = shift;
	my $species = shift;
	my $in_list = shift;
	my $taxon_no = shift;
	
	$GLOBALVARS{session} = $s;
	
	# If $q->param("genus_name") has a space, it's a "Genus species" combo,
	# otherwise it's a "Higher taxon."
	($genus, $species) = split /\s+/, $q->param("genus_name");

	Debug::dbPrint("module = $module");

	# classification
	if($module == 1){
		print "<table width=\"100%\">".
			  "<tr><td align=\"middle\"><h3>Classification</h3></td></tr>".
			  "<tr><td valign=\"top\" align=\"middle\">";

		print displayTaxonClassification($dbt, $genus, $species, $taxon_no);
		print "</td></tr></table>";

	}
	# synonymy
	elsif($module == 2){
		Debug::dbPrint("synhere8");
		print displayTaxonSynonymy($dbt, $genus, $species, $taxon_no);
		Debug::dbPrint("synhere9");	
	}
	elsif ( $module == 3 )	{
		print displaySynonymyList($dbt, $q, $genus, $species, $taxon_no);
	}
	# ecology
	elsif ( $module == 4 )	{
		print displayEcology($dbt,$taxon_no,$genus,$species);
	}
	# map
	elsif($module == 5){
		print "<center><table><tr><td align=\"middle\"><h3>Distribution</h3></td></tr>".
			  "<tr><td align=\"middle\" valign=\"top\">";
		# MAP USES $q->param("genus_name") to determine what it's doing.
		my $map_html_path = doMap($dbh, $dbt, $q, $s, $in_list);
		if($map_html_path =~ /^\/public/){
			# reconstruct the full path the image.
			$map_html_path = $ENV{DOCUMENT_ROOT}.$map_html_path;
		}
		open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
		while(<MAP>){
			print;
		}
		close MAP;
		print "</td></tr></table></center>";
		# trim the path down beyond apache's root so we don't have a full
		# server path in our html.
		$map_html_path =~ s/.*?(\/public.*)/$1/;
		print "<input type=hidden name=\"map_num\" value=\"$map_html_path\">";
	}
	# collections
	elsif($module == 6){
		print doCollections($exec_url, $q, $dbt, $in_list);
	}
}


# PASS this a reference to the collection list array and it
# should figure out the min/max/center lat/lon 
# RETURNS an array of parameters (see end of routine for order)
# written 12/11/2003 by rjp.
sub calculateCollectionBounds {
	my $collections = shift;  #collections to plot

	# calculate the min and max latitude and 
	# longitude with 1 degree resolution
	my $latMin = 360;
	my $latMax = -360;
	my $lonMin = 360;
	my $lonMax = -360;

	foreach (@$collections) {
		%coll = %$_;

		# note, this is *assuming* that latdeg and lngdeg are 
		# always populated, even if the user set the lat/lon with 
		# decimal degrees instead.  So if this isn't the case, then
		# we need to check both of them.  
		my $latDeg = $coll{'latdeg'};
		if ($coll{'latdir'} eq "South") {
			$latDeg = -1*$latDeg;
		}

		my $lonDeg = $coll{'lngdeg'};
		if ($coll{'lngdir'} eq "West") {
			$lonDeg = -1* $lonDeg;
		}

		#print "lat = $latDeg<BR>";
		#print "lon = $lonDeg<BR>";

		if ($latDeg > $latMax) { $latMax = $latDeg; }
		if ($latDeg < $latMin) { $latMin = $latDeg; }
		if ($lonDeg > $lonMax) { $lonMax = $lonDeg; }
		if ($lonDeg < $lonMin) { $lonMin = $lonDeg; }
	}

	$latCenter = (($latMax - $latMin)/2) + $latMin;
	$lonCenter = (($lonMax - $lonMin)/2) + $lonMin;

	#print "latCenter = $latCenter<BR>";
	#print "lonCenter = $lonCenter<BR>";
	#print "latMin = $latMin<BR>";
	#print "latMax = $latMax<BR>";
	#print "lonMin = $lonMin<BR>";
	#print "lonMax = $lonMax<BR>";

	return ($latCenter, $lonCenter, $latMin, $latMax, $lonMin, $lonMax);
}





sub doMap{
	my $dbh = shift;
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $in_list = shift;
	my $map_num = $q->param('map_num');

	$GLOBALVARS{session} = $s;

	if($q->param('map_num')){
		return $q->param('map_num');
	}

	$q->param(-name=>"taxon_info_script",-value=>"yes");
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape', 'dotcolor', 'dotborder');
	my %user_prefs = main::getPreferences($s->get('enterer'));
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}
	# Not covered by prefs:
	if(!$q->param('pointshape')){
		$q->param('pointshape' => 'circles');
	}
	if(!$q->param('dotcolor')){
		$q->param('dotcolor' => 'red');
	}
	if(!$q->param('coastlinecolor')){
		$q->param('coastlinecolor' => 'black');
	}
	$q->param('mapresolution'=>'medium');

	# note, we need to leave this in here even though it's 
	# redunant (since we scale below).. taking it out will
	# cause a division by zero error in Map.pm.
	$q->param('mapscale'=>'X 1');


	$q->param('pointsize'=>'tiny');

	if(!$q->param('projection') or $q->param('projection') eq ""){
		$q->param('projection'=>'rectilinear');
	}

	require Map;
	my $m = Map->new( $dbh, $q, $s, $dbt );
	my $perm_rows = $m->buildMapOnly($in_list);
	my @perm_rows = @{$perm_rows};

	if(@perm_rows > 0) {

		# this section added by rjp on 12/11/2003
		# at this point, we need to figure out the bounds 
		# of the collections and the center point.  
		my @bounds = calculateCollectionBounds(\@perm_rows);

		$q->param('maplat' => shift(@bounds));
		$q->param('maplng' => shift(@bounds));

		# note, we must constrain the map size to be in a ratio
		# of 360 wide by 180 high, so figure out what ratio to use
		my $latMin = shift(@bounds);	my $latMax = shift(@bounds);
		my $lonMin = shift(@bounds);	my $lonMax = shift(@bounds);

		my $latWidth = abs($latMax - $latMin);
		my $lonWidth = abs($lonMax - $lonMin);

		my $scale = 8;  # default scale value
		if (not (($latWidth == 0) and ($lonWidth == 0))) {
			# only do this if they're not both zero...
		
			if ($latWidth == 0) { $latWidth = 1; } #to prevent divide by zero
			if ($lonWidth == 0) { $lonWidth = 1; }
		
			# multiply by 0.9 to give a slight boundary around the zoom.
			my $latRatio = (0.9 * 180) / $latWidth;
			my $lonRatio = (0.9 * 360) / $lonWidth;

			#print "latRatio = $latRatio\n";
			#print "lonRatio = $lonRatio\n";

			if ($latRatio < $lonRatio) {
				$scale = $latRatio;
			} else { 
				$scale = $lonRatio;
			}
		}

		if ($scale > 8) { $scale = 8; } # don't let it zoom too far in!
		$q->param('mapscale' => "X $scale");
		

		# note, we have already set $q in the map object,
		# so we have to set it again with the new values.
		# this is not the ideal way to do it, so perhaps change
		# this at a future date.
		$m->setQAndUpdateScale($q);
		
	
		# now actually draw the map
		return $m->mapDrawMap($perm_rows);
	} else {
		return "<i>No distribution data are available</i>";
	}
}



sub doCollections{
	my $exec_url = shift;
	my $q = shift;
	my $dbt = shift;
	my $in_list = shift;
	my $output = "";

	$q->param(-name=>"limit",-value=>1000000);
	$q->param(-name=>"taxon_info_script",-value=>"yes");
	
	# Get all the data from the database, bypassing most of the normal behavior
	# of displayCollResults
	@data = @{main::displayCollResults($in_list)};	

	require Collections;

	# Process the data:  group all the collection numbers with the same
	# time-place string together as a hash.
	%time_place_coll = ();
	foreach my $row (@data){
	    $res = Collections::createTimePlaceString($row,$dbt);
	    if(exists $time_place_coll{$res}){
			push(@{$time_place_coll{$res}}, $row->{"collection_no"});
	    }
	    else{
			$time_place_coll{$res} = [$row->{"collection_no"}];
			push(@order,$res);
	    }
	}

	my @sorted = sort (keys %time_place_coll);

	if(scalar @sorted > 0){
		# Do this locally because the module never gets exec_url
		#   from bridge.pl
		my $exec_url = $q->url();
		$output .= "<center><h3>Collections</h3></center>";
		$output .= "<table width=\"100%\"><tr>";
		$output .= "<th align=\"middle\">Country or state</th>";
		$output .= "<th align=\"middle\">Time interval</th>";
		$output .= "<th align=\"left\">PBDB collection number</th></tr>";
		my $row_color = 0;
		foreach my $key (@sorted){
			if($row_color % 2 == 0){
				$output .= "<tr class='darkList'>";
			} 
			else{
				$output .= "<tr>";
			}
			$output .= "<td align=\"middle\" valign=\"top\">".
				  "<span class=tiny>$key</span></td><td align=\"left\">";
			foreach  my $val (@{$time_place_coll{$key}}){
				my $link=Collections::createCollectionDetailLink($exec_url,$val,$val);
				$output .= "$link ";
			}
			$output .= "</td></tr>\n";
			$row_color++;
		}
		$output .= "</table>";
	}
	return $output;
}



## displayTaxonClassification
#
# SEND IN GENUS OR HIGHER TO GENUS_NAME, ONLY SET SPECIES IF THERE'S A SPECIES.
##
sub displayTaxonClassification{
	my $dbt = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $easy_number = (shift or "");
	
	my $taxon_rank;
	my $taxon_name;
	my %classification = ();

	# if we didn't get a species, figure out what rank we got
	if($genus && ($species eq "")){
		$taxon_name = $genus;
		# Initialize the classification hash:
		my $sql="SELECT taxon_rank FROM authorities WHERE taxon_name='$genus'";
		my @results = @{$dbt->getData($sql)};
		$taxon_rank = $results[0]->{taxon_rank};
		${$classification{$taxon_rank}}[0] = $genus;
	}
	else{
		$taxon_name = $genus;
		$taxon_name .= " $species";
		$taxon_rank = "species";
		# Initialize our classification hash with the info
		# that came in as an argument to this method.
		${$classification{"species"}}[0] = $species;
		${$classification{"genus"}}[0] = $genus;
	}

	# default to a number that doesn't exist in the database.
	my $child_no = -1;
	my $parent_no = -1;
	my %parent_no_visits = ();
	my %child_no_visits = ();

	my $status = "";
	my $first_time = 1;
	# Loop at least once, but as long as it takes to get full classification
	while($parent_no){
		my @results = ();
		# We know the taxon_rank and taxon_name, so get its number
		unless($easy_number){
			my $sql_auth_inv = "SELECT taxon_no ".
					   "FROM authorities ".
					   "WHERE taxon_name = '$taxon_name' ".
					   "AND taxon_rank = '$taxon_rank'";
			PBDBUtil::debug(1,"authorities inv: $sql_auth_inv<br>");
			@results = @{$dbt->getData($sql_auth_inv)};
		}
		else{
			$results[0] = {"taxon_no" => $easy_number};
			${$classification{$taxon_rank}}[1] = $easy_number;
			# reset to zero so we don't try to use this on successive loops.
			$easy_number = 0;
		}
		# Keep $child_no at -1 if no results are returned.
		if(defined $results[0]){
			# Save the taxon_no for keying into the opinions table.
			$child_no = $results[0]->{taxon_no};
			${$classification{$taxon_rank}}[1] = $results[0]->{taxon_no};

			# Insurance for self referential / bad data in database.
			# NOTE: can't use the tertiary operator with hashes...
			# How strange...
			if($child_no_visits{$child_no}){
				$child_no_visits{$child_no} += 1; 
			}
			else{
				$child_no_visits{$child_no} = 1;
			}
			PBDBUtil::debug(1,"child_no_visits{$child_no}:$child_no_visits{$child_no}.<br>");
			last if($child_no_visits{$child_no}>1); 

		}
		# no taxon number: if we're doing "Genus species", try to find a parent
		# for just the Genus, otherwise give up.
		else{ 
			if($genus && $species){
				$sql_auth_inv = "SELECT taxon_no ".
                   "FROM authorities ".
                   "WHERE taxon_name = '$genus' ".
                   "AND taxon_rank = 'Genus'";
				@results = @{$dbt->getData($sql_auth_inv)};
				# THIS IS LOOKING IDENTICAL TO ABOVE...
				# COULD CALL SELF WITH EMPTY SPECIES NAME AND AN EXIT...
				if(defined $results[0]){
					$child_no = $results[0]->{taxon_no};
					${$classification{genus}}[0] = $genus;
					${$classification{genus}}[1] = $results[0]->{taxon_no};

					if($child_no_visits{$child_no}){
						$child_no_visits{$child_no} += 1; 
					}
					else{
						$child_no_visits{$child_no} = 1;
					}
					PBDBUtil::debug(1,"child_no_visits{$child_no}:$child_no_visits{$child_no}.<br>");
					last if($child_no_visits{$child_no}>1); 
				}
			}
			else{
				last;
			}
		}
		
		# Now see if the opinions table has a parent for this child
		my $sql_opin =  "SELECT status, parent_no, pubyr, reference_no ".
						"FROM opinions ".
						"WHERE child_no=$child_no AND status='belongs to'";
		PBDBUtil::debug(1,"opinions: $sql_opin<br>");
		@results = @{$dbt->getData($sql_opin)};

		if($first_time && $taxon_rank eq "species" && scalar @results < 1){
			my ($genus, $species) = split(/\s+/,$taxon_name);
            my $last_ditch_sql = "SELECT taxon_no ".
							     "FROM authorities ".
							     "WHERE taxon_name = '$genus' ".
							     "AND taxon_rank = 'Genus'";
            @results = @{$dbt->getData($last_ditch_sql)};
			my $child_no = $results[0]->{taxon_no};
			if($child_no > 0){
				$last_ditch_sql = "SELECT status, parent_no, pubyr, ".
								  "reference_no FROM opinions ".
								  "WHERE child_no=$child_no AND ".
								  "status='belongs to'";
				@results = @{$dbt->getData($last_ditch_sql)};
			}
		}
		$first_time = 0;

		if(scalar @results){
			$parent_no=selectMostRecentParentOpinion($dbt,\@results);

			# Insurance for self referential or otherwise bad data in database.
			if($parent_no_visits{$parent_no}){
				$parent_no_visits{$parent_no} += 1; 
			}
			else{
				$parent_no_visits{$parent_no}=1;
			}
			PBDBUtil::debug(1,"parent_no_visits{$parent_no}:$parent_no_visits{$parent_no}.<br>");
			last if($parent_no_visits{$parent_no}>1); 

			if($parent_no){
				# Get the name and rank for the parent
				my $sql_auth = "SELECT taxon_name, taxon_rank ".
					       "FROM authorities ".
					       "WHERE taxon_no=$parent_no";
				PBDBUtil::debug(1,"authorities: $sql_auth<br>");
				@results = @{$dbt->getData($sql_auth)};
				if(scalar @results){
					$auth_hash_ref = $results[0];
					# reset name and rank for next loop pass
					$taxon_rank = $auth_hash_ref->{"taxon_rank"};
					$taxon_name = $auth_hash_ref->{"taxon_name"};
					#print "ADDING $taxon_rank of $taxon_name<br>";
					#$classification{$taxon_rank} = [];
					${$classification{$taxon_rank}}[0] = $taxon_name;
					${$classification{$taxon_rank}}[1] = $parent_no;
				}
				else{
					# No results might not be an error: 
					# it might just be lack of data
				    # print "ERROR in sql: $sql_auth<br>";
				    last;
				}
				$easy_number = $parent_no;
			}
			# If we didn't get a parent or status ne 'belongs to'
			else{
				#$parent_no = 0;
				$parent_no = undef;
			}
		}
		else{
			# No results might not be an error: it might just be lack of data
			# print "ERROR in sql: $sql_opin<br>";
			last;
		}
	}

	my $output = "";
	$output .= "<table width=\"50%\"><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
	my $counter = 0;
	# Print these out in correct order
	my @taxon_rank_order = ('superkingdom','kingdom','subkingdom','superphylum','phylum','subphylum','superclass','class','subclass','infraclass','superorder','order','suborder','infraorder','superfamily','family','subfamily','tribe','subtribe','genus','subgenus','species','subspecies');
	my %taxon_rank_order = ('superkingdom'=>0,'kingdom'=>1,'subkingdom'=>2,'superphylum'=>3,'phylum'=>4,'subphylum'=>5,'superclass'=>6,'class'=>7,'subclass'=>8,'infraclass'=>9,'superorder'=>10,'order'=>11,'suborder'=>12,'infraorder'=>13,'superfamily'=>14,'family'=>15,'subfamily'=>16,'tribe'=>17,'subtribe'=>18,'genus'=>19,'subgenus'=>20,'species'=>21,'subspecies'=>22);
	my $lastgood;
	my $lastrank;
	foreach my $rank (@taxon_rank_order){
		# Don't provide links for any rank higher than 'order'
		my %auth_yr;
		# get the authority data
		if(exists $classification{$rank}){
		  if(exists $classification{$rank}[1]){
		# for a species, first find out the original combination
			my $orig_taxon_no = $classification{$rank}[1];
			if ( $rank eq "species" )	{
				$orig_taxon_no = getOriginalCombination($dbt, $orig_taxon_no);
			}
			%auth_yr = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$orig_taxon_no)};
			if ( $classification{$rank}[1] != $orig_taxon_no )	{
				$auth_yr{author1last} = "(" . $auth_yr{author1last};
				$auth_yr{pubyr} .= ")";
			}
		  }
		  $auth_yr{author1last} =~ s/\s+/&nbsp;/g;
		  # Don't link 'sp, 'sp.', 'indet' or 'indet.' either.
		  if($taxon_rank_order{$rank} < 11 || $classification{$rank}[0] =~ /(sp\.{0,1}|indet\.{0,1})$/){
			if($counter % 2 == 0){
				$output .="<tr class='darkList'><td align=\"middle\">$rank</td>".
						  "<td align=\"middle\">$classification{$rank}[0]</td>".
						  "<td>$auth_yr{author1last}&nbsp;".
						  "$auth_yr{pubyr}</td></tr>\n";
			}
			else{
				$output .="<tr><td align=\"middle\">$rank</td>".
					      "<td align=\"middle\">$classification{$rank}[0]</td>".
						  "<td>$auth_yr{author1last}&nbsp;".
						  "$auth_yr{pubyr}</td></tr>\n";
			}
		  }
		  else{
			# URL encoding
			my $temp_rank = $rank;
			if($temp_rank eq "genus"){
				$temp_rank = "Genus";
			}
			if($temp_rank ne "Genus" && $temp_rank ne "Genus and species" &&
					$temp_rank ne "species"){
				$temp_rank = "Higher taxon";
			}
			$temp_rank =~ s/\s/+/g;
			$classification{$rank}[0] =~ s/\s/+/g;
			if($counter % 2 == 0){
				$output .="<tr class='darkList'><td align=\"middle\">$rank".
						  "</td><td align=\"middle\">".
						  "<a href=\"/cgi-bin/bridge.pl?action=checkTaxonInfo".
						  "&taxon_rank=$temp_rank&taxon_name=".
					      "$classification{$rank}[0]\">".
					      "$classification{$rank}[0]</a></td>".
						  "<td>$auth_yr{author1last}&nbsp;".
						  "$auth_yr{pubyr}</td></tr>\n";
			}
			else{
				$output .="<tr><td align=\"middle\">$rank</td>".
					      "<td align=\"middle\"><a href=\"/cgi-bin/bridge.pl?".
					      "action=checkTaxonInfo&taxon_rank=$temp_rank&".
						  "taxon_name=$classification{$rank}[0]\">".
					      "$classification{$rank}[0]</a></td>".
						  "<td>$auth_yr{author1last}&nbsp;".
						  "$auth_yr{pubyr}</td></tr>\n";
			}
		  }
			$counter++;
			# Keep track of the last successful item  and rank so we have the 
			# lowest on the chain when we're all done.
			$lastgood = $classification{$rank}[0];
			$lastrank = $rank;
		}
	}
	$output .= "</table>";
	# Now, print out a hyperlinked list of all taxa below the one at the
	# bottom of the Classification section.
	if($counter <1){
		$output .= "<i>No classification data are available</i><br>";
	}
	my $index;
	for($index=0; $index<@taxon_rank_order; $index++){
		last if($lastrank eq $taxon_rank_order[$index]);
	}
	# NOTE: Don't do this if the last rank was 'species.'
	CHILDREN:{
	if($index < 21){ # species is position 21
		#$lastrank = $taxon_rank_order[$index+1];
		#if($lastrank eq "genus"){
		#	$lastrank = "Genus";
		#}
		# genus is position 19.
		#elsif($index < 19){
		#	$lastrank = "Higher taxon";
		#}
		my $sql = "SELECT taxon_no FROM authorities ".
				  "WHERE taxon_name='$lastgood'";
		PBDBUtil::debug(1,"lastgood sql: $sql");
		my @quickie = @{$dbt->getData($sql)};
		if(scalar @quickie < 1){
			# BAIL
			last CHILDREN;
		}
		
		
		# rjp, 2/2004 - this is the new way to get a taxonomic list.
		# This new version will list species which have no opinion records
		# (only authority records).
		{ 
			my $taxHigh = Taxon->new();
			$taxHigh->setWithTaxonNumber($quickie[0]->{taxon_no});
			my @taxa = @{$taxHigh->listOfChildren()};
		
			my $r;
			foreach my $t (@taxa) {
				$r .= "<A HREF=\"" . $t->URLForTaxonName() . "\">" .
					 $t->taxonName() . "</A>, ";
			}
			
			$r =~ s/, $//;
			
			if (@taxa > 0) { # if we found 1 or more...
				$output .= "<p><i>This taxon includes:</i><BR>";
				$output .= $r;
			}
		}
		
		# end of new section, 
				
	}
	} # CHILDREN block
	return $output;
}



sub displayTaxonSynonymy{
	my $dbt = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $taxon_no = (shift or "");
	
	my $taxon_rank;
	my $taxon_name;
	my $output = "";  # html output...
	
	Debug::dbPrint("in displayTaxonSynonymy, taxon_no = $taxon_no");
	
	# figure out the taxon rank (class, genus, species, etc.)
	$taxon_name = $genus;
	if ($genus && ($species eq "")) {
		my $sql="SELECT taxon_rank FROM authorities WHERE taxon_no=$taxon_no";
#		my $sql="SELECT taxon_rank FROM authorities WHERE taxon_name='$genus'";
		my @results = @{$dbt->getData($sql)};
		Debug::dbPrint("synhere1");
		
		$taxon_rank = $results[0]->{taxon_rank};
	} else {
		if ($genus eq "") {
			$taxon_name = $species;
		} else {
			$taxon_name .= " $species";
		}
		$taxon_rank = "species";
	}

	$output .= "<center><h3>Taxonomic history</h3></center>";

	my $sql = "SELECT taxon_no, reference_no, author1last, pubyr, ".
			  "ref_is_authority FROM authorities ".
			  "WHERE taxon_no=$taxon_no";
#			  "WHERE taxon_name='$taxon_name' AND taxon_rank='$taxon_rank'";
	my @results = @{$dbt->getData($sql)};
	Debug::dbPrint("synhere2");
	
#	my $taxon_no = $results[0]->{taxon_no};
	PBDBUtil::debug(1,"taxon rank: $taxon_rank");
	PBDBUtil::debug(1,"taxon name: $taxon_name");
	PBDBUtil::debug(1,"taxon number from authorities: $taxon_no");
	unless($taxon_no) {
		return ($output .= "<i>No taxonomic history is available for $taxon_rank $taxon_name.</i><br>");
	}

	$output .= "<ul>";

	# Get the original combination (of the verified name, not the focal name)
	my $original_combination_no = getOriginalCombination($dbt, $taxon_no);
	Debug::dbPrint("synhere3");
	PBDBUtil::debug(1,"original combination_no: $original_combination_no");
	
	# Select all parents of the original combination whose status' are
	# either 'recombined as,' 'corrected as,' or 'rank changed as'
	$sql = "SELECT DISTINCT(parent_no), status FROM opinions ".
		   "WHERE child_no=$original_combination_no ".	
		   "AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
	@results = @{$dbt->getData($sql)};
	Debug::dbPrint("synhere4");

	# Combine parent numbers from above for the next select below. If nothing
	# was returned from above, use the original combination number.
	my @parent_list = ();
	if (scalar @results <1) {
		push(@parent_list, $original_combination_no);
	} else {
		foreach my $rec (@results) {
			push(@parent_list,$rec->{parent_no});
		}
		# don't forget the original (verified) here, either: the focal taxon	
		# should be one of its children so it will be included below.
		push(@parent_list, $original_combination_no);
	}

	# Select all synonymies for the above list of taxa.
	$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
		   "WHERE parent_no IN (".
			join(',',@parent_list).") ".
		   "AND (status like '%synonym%' OR status='homonym of' OR status='replaced by')";
	@results = @{$dbt->getData($sql)};
	Debug::dbPrint("synhere5");

	# Reduce these results to original combinations:
	foreach my $rec (@results) {
		$rec = getOriginalCombination($dbt, $rec->{child_no});	
	}

	Debug::dbPrint("synhere5a");

	# NOTE: "corrected as" could also occur at higher taxonomic levels.

	# Get synonymies for all of these original combinations
	foreach my $child (@results) {
		my $list_item = getSynonymyParagraph($dbt, $child);
		push(@paragraphs, "<br><br>$list_item\n") if($list_item ne "");
	}
	
	Debug::dbPrint("synhere6");
	
	# Print the info for the original combination of the passed in taxon first.
	$output .= getSynonymyParagraph($dbt, $original_combination_no);

	# Now alphabetize the rest:
	@paragraphs = sort {lc($a) cmp lc($b)} @paragraphs;
	foreach my $rec (@paragraphs) {
		$output .= $rec;
	}

	Debug::dbPrint("synhere7");
	
	$output .= "</ul>";
	return $output;
}


# updated by rjp, 1/22/2004
# gets paragraph displayed in places like the
# taxonomic history, for example, if you search for a particular taxon
# and then check the taxonomic history box at the left.
#
sub getSynonymyParagraph{
	my $dbt = shift;
	my $taxon_no = shift;
	
	my %synmap = ( 'recombined as' => 'recombined as ',
				   'replaced by' => 'replaced with ',
				   'corrected as' => 'corrected as ',
				   'rank changed as' => 'changed to another rank and altered to ',
				   'belongs to' => 'revalidated by ',
				   'nomen dubium' => 'considered a nomen dubium ',
				   'nomen nudum' => 'considered a nomen nudum ',
				   'nomen vanum' => 'considered a nomen vanum ',
				   'nomen oblitum' => 'considered a nomen oblitum ',
				   'subjective synonym of' => 'synonymized subjectively with ',
				   'objective synonym of' => 'synonymized objectively with ');
	my $text = "";

	# "Named by" part first:
	# Need to print out "[taxon_name] was named by [author] ([pubyr])".
	# - select taxon_name, author1last, pubyr, reference_no, comments from authorities
	
	Debug::dbPrint("synpara1, taxon_no = $taxon_no");
	
	my $sql = "SELECT taxon_name, author1last, pubyr, reference_no, comments, ".
			  "ref_is_authority FROM authorities WHERE taxon_no=$taxon_no";
	my @auth_rec = @{$dbt->getData($sql)};
	Debug::dbPrint("synpara2");
	
	# Get ref info from refs if 'ref_is_authority' is set
	if($auth_rec[0]->{ref_is_authority} =~ /YES/i){
		PBDBUtil::debug(1,"author and year from refs<br>");
		# If we didn't get an author and pubyr and also didn't get a 
		# reference_no, we're at a wall and can go no further.
		if(!$auth_rec[0]->{reference_no}){
			$text .= "Cannot determine taxonomic history for ".
					 $auth_rec[0]->{taxon_name}."<br>.";
			return $text;	
		}
		$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs ".
			   "WHERE reference_no=".$auth_rec[0]->{reference_no};
		@results = @{$dbt->getData($sql)};
		Debug::dbPrint("synpara3");
		
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
			  	 $results[0]->{author1last};
		if($results[0]->{otherauthors} ne ""){
			$text .= " et al. ";
		}
		elsif($results[0]->{author2last} ne ""){
			# We have at least 120 refs where the author2last is 'et al.'
			if($key_list[$j]->{author2last} eq "et al."){
				$text .= " et al. ";
			}
			else{
				$text .= " and ".$results[0]->{author2last}." ";
			}
		}
		$text .= " (".$results[0]->{pubyr}.")";
		if ( $auth_rec[0]->{comments} )	{
			$text .= " <span class=small>[" . $auth_rec[0]->{comments} . "]</span>";
		}
	}
	# If ref_is_authority is not set, use the authorname and pubyr in this
	# record.
	elsif($auth_rec[0]->{author1last}){
#	elsif($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
		PBDBUtil::debug(1,"author and year from authorities<br>");
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
			  	 $auth_rec[0]->{author1last};
		if ( $auth_rec[0]->{pubyr} )	{
			$text .= " (".$auth_rec[0]->{pubyr}.")";
		}
		if ( $auth_rec[0]->{comments} )	{
			$text .= " <span class=small>[" . $auth_rec[0]->{comments} . "]</span>";
		}
	}
	# if there's nothing from above, give up.
	else{
		$text .= "</ul>";
		$text .= "<i>The author of ".
				 $auth_rec[0]->{taxon_rank}." ".
				 $auth_rec[0]->{taxon_name}." is not known.</i><br>";
		return $text;
	}

	# Now, synonymies:
	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last, ".
		   "author2last, otherauthors ".
		   "FROM opinions WHERE child_no=$taxon_no AND status != 'belongs to'".
		   " AND status NOT LIKE 'nomen%'";
	@results = @{$dbt->getData($sql)};
	Debug::dbPrint("synpara4");

	my %synonymies = ();
	my @syn_years = ();
	# check for synonymies - status' of anything other than "belongs to"
	foreach my $row (@results){
		# get the proper reference (record first, refs table second)
		if(!$row->{author1last} || !$row->{pubyr}){
			# select into the refs table.
			$sql = "SELECT author1last,author2last,otherauthors,pubyr ".
				   "FROM refs ".
				   "WHERE reference_no=".$row->{reference_no};
			my @real_ref = @{$dbt->getData($sql)};
			Debug::dbPrint("synpara5");
			
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{author2last} = $real_ref[0]->{author2last};
			$row->{otherauthors} = $real_ref[0]->{otherauthors};
			$row->{pubyr} = $real_ref[0]->{pubyr};
		}
		# put all syn's referring to the same taxon_name together
		if(exists $synonymies{$row->{parent_no}}){
			push(@{$synonymies{$row->{parent_no}}}, $row);
		}	
		else{
			$synonymies{$row->{parent_no}} = [$row];
		}
	}
	
	# Sort the items in each synonymy value by pubyr
	foreach my $key (keys %synonymies){
		my @years = @{$synonymies{$key}};
		@years = sort{$a->{pubyr} cmp $b->{pubyr}} @years;
		$synonymies{$key} = \@years;
		push(@syn_years, $years[0]->{pubyr});
	}

	# sort the list of beginning syn_years
	@syn_years = sort{$a cmp $b} @syn_years;

	# Revalidations and nomen*'s
	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last, ".
		   "author2last, otherauthors, comments, opinion_no ".
		   "FROM opinions WHERE child_no=$taxon_no AND (status='belongs to' ".
		   "OR status like 'nomen%')";
	@results = @{$dbt->getData($sql)};
	Debug::dbPrint("synpara6");
	
	my %nomen_or_reval = ();
	my @nomen_or_reval_numbers = ();
	my $has_nomen = 0;
	foreach my $row (@results){
		# get the proper reference (record first, refs table second)
		if(!($row->{author1last} && $row->{pubyr})){
			# select into the refs table.
			$sql = "SELECT author1last,author2last,otherauthors,pubyr ".
				   "FROM refs ".
				   "WHERE reference_no=".$row->{reference_no};
			my @real_ref = @{$dbt->getData($sql)};
			Debug::dbPrint("synpara7");
			
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{author2last} = $real_ref[0]->{author2last};
			$row->{otherauthors} = $real_ref[0]->{otherauthors};
			$row->{pubyr} = $real_ref[0]->{pubyr};
		}
		# use opinion numbers to keep recs separate for now
		$nomen_or_reval{$row->{opinion_no}} = $row;
		push(@nomen_or_reval_numbers, $row->{opinion_no});
		if($row->{status} =~ /nomen/){
			$has_nomen = 1 
		}
	}
	
	Debug::dbPrint("synpara8");
	
	@nomen_or_reval_numbers = sort{$nomen_or_reval{$a}->{pubyr} <=> $nomen_or_reval{$b}->{pubyr}} @nomen_or_reval_numbers;	
	# Since these are arranged numerically now chop of any leading "belongs to"
	# recs whose pubyr is not newer than the oldest synonymy. Keep the whole 
	# list if the oldest thing going is a nomen* record.
	for(my $index = 0; $index < @nomen_or_reval_numbers; $index++){
		# Because of the redo, below:
		last if(scalar @nomen_or_reval_numbers < 1);
		last if($nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status} =~ /nomen/);
		# otherwise, it's a 'belongs to' status
		if(scalar @syn_years < 1 || $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{pubyr} < $syn_years[0]){
			delete $nomen_or_reval{$nomen_or_reval_numbers[$index]};
			shift @nomen_or_reval_numbers;
			redo;
		}
		else{
			last;
		}
	}
	
	Debug::dbPrint("synpara9");
	
	# Combine all adjacent like status types from %nomen_or_reval.
	# (They're chronological: nomen, reval, reval, nomen, nomen, reval, etc.)
	my %additional = ();
	my $last_status;
	my $last_key;
	for(my $index = 0; $index < @nomen_or_reval_numbers; $index++){
		if($last_status && $last_status eq $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status}){
			push(@{$additional{$last_key}}, $nomen_or_reval{$nomen_or_reval_numbers[$index]});
		}
		else{
			$last_key = "nomen_reval$index";
			$additional{$last_key} = [$nomen_or_reval{$nomen_or_reval_numbers[$index]}];
		}
		$last_status = $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status};
	}

	Debug::dbPrint("synpara10");

	# Put revalidations and nomen*'s in synonymies hash.
	if(scalar(keys %synonymies) or $has_nomen){
		foreach my $key (keys %additional){
			$synonymies{$key} = $additional{$key};
		}
	}

	# Now print it all out
	my @syn_keys = keys %synonymies;

	# Order by ascending pubyr of first record in each synonymy list.
	@syn_keys = sort{$synonymies{$a}[0]->{pubyr} cmp $synonymies{$b}[0]->{pubyr}} @syn_keys;
	
	# Exception to above:  the most recent opinion should appear last.
	# Splice it
	if(scalar @syn_keys > 1){
		my $oldest = 0;
		my $oldest_index = 0;
		# The rows are already ordered by pubyr, so find the most recent last
		# element in any row
		for(my $index = 0; $index < @syn_keys; $index++){
			my $date = ${$synonymies{$syn_keys[$index]}}[-1]->{pubyr};	
			if($date > $oldest){
				$oldest = $date;
				$oldest_index = $index;
			}
		}	
		my $new_oldest_key = splice(@syn_keys, $oldest_index, 1);
		# And put it at the end.
		push(@syn_keys, $new_oldest_key);
	}
	
	Debug::dbPrint("synpara11");

	# Loop through unique parent number from the opinions table.
	# Each parent number is a hash key whose value is an array ref of records.
	for(my $index = 0; $index < @syn_keys; $index++){
		# $syn_keys[$index] is a parent_no, so $synonymies{$syn_keys[$index]}
		# is a record from the immediately preceeding 'opinions' select.

		
		$text .= "; it was ".$synmap{$synonymies{$syn_keys[$index]}[0]->{status}};
		$sql = "SELECT taxon_name FROM authorities ".
			   "WHERE taxon_no=";
		
		my $tn = 0;
		if ($synonymies{$syn_keys[$index]}[0]->{status} =~ /nomen/ or
		   $synonymies{$syn_keys[$index]}[0]->{status} =~ /belongs/) {
			
			Debug::dbPrint("err1");
		   
			$tn = $synonymies{$syn_keys[$index]}[0]->{parent_no};
		} else {	
		
			Debug::dbPrint("err2");
			$tn = $syn_keys[$index];
		}
		
		# Added by rjp, 3/29/2004 to fix an error caused by a missing parent number.
		# Still not sure why the number was empty to begin with, but this seems to 
		# fix it for now.
		if (!$tn) {
			next;
		}
		$sql .= $tn;
		
		Debug::dbPrint("errparent_no = " . $tn);
		
		@results = @{$dbt->getData($sql)};
		unless($synmap{$synonymies{$syn_keys[$index]}[0]->{status}} eq "revalidated by "){
			$text .= "<i>".$results[0]->{taxon_name}."</i> by ";
		}
		# Dereference the hash value (array ref of opinions recs), so we can
		# write out all of the authors/years for this synonymy.
		my @key_list = @{$synonymies{$syn_keys[$index]}};
		# sort the list by date (ascending)
		@key_list = sort {$a->{pubyr} <=> $b->{pubyr}} @key_list;
		for(my $j = 0; $j < @key_list; $j++){
			$text .= $key_list[$j]->{author1last};
			if($key_list[$j]->{otherauthors} ne ""){
				$text .= " et al. ";
			}
			elsif($key_list[$j]->{author2last} ne ""){
				# We have at least 120 refs where the author2last is 'et al.'
				if($key_list[$j]->{author2last} eq "et al."){
					$text .= " et al. ";
				}
				else{
					$text .= " and ".$key_list[$j]->{author2last}." ";
				}
			}
			$text .= " (".$key_list[$j]->{pubyr}.")";
			if ( $key_list[$j]->{comments} )	{
				$text .= "<span class=small> [" . $key_list[$j]->{comments} . "]</span>";
			}
			$text .= ", ";
		}
		if($text =~ /,\s+$/){
			# remove the last comma
			$text =~ s/,\s+$//;
			# replace the last comma-space sequence with ' and '
			$text =~ s/(,\s+([a-zA-Z\-']+\s+(and\s+[a-zA-Z\-']+\s+|et al.\s+){0,1}\(\d{4}\)))$/ and $2/;
			# put a semi-colon on the end to separate from any following syns.
		}
	}
	if($text ne ""){
		# Add a period at the end.
		$text .= '.';
		# remove a leading semi-colon and any space.
		$text =~ s/^;\s+//;
		# capitalize the first 'I' in the first word (It).
		$text =~ s/^i/I/;
	}


	Debug::dbPrint("synparalast");
	
	return $text;
}


sub getOriginalCombination{
	my $dbt = shift;
	my $taxon_no = shift;
	# You know you're an original combination when you have no children
	# that have recombined or corrected as relations to you.
	my $sql = "SELECT DISTINCT(child_no), status FROM opinions ".
			  "WHERE parent_no=$taxon_no AND (status='recombined as' OR ".
			  "status='corrected as' OR status='rank changed as')";
	my @results = @{$dbt->getData($sql)};

	my $has_status = 0;
	foreach my $rec (@results){
		$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
			   "WHERE parent_no=".$rec->{child_no}.
			   " AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
		my @second_level = @{$dbt->getData($sql)};
		if(scalar @second_level < 1){
			return $rec->{child_no};
		}
		# else
		$has_status = 1;
	}
	# If all results in the loop above gave results, follow the first one
	# down (assuming they all point to the same place eventually) recursively.
	if($has_status){
		$taxon_no = getOriginialCombination($dbt,$results[0]->{child_no});
	}

	# If we fall through above but $has_status was not set, we just
	# return the original $taxon_no passed in.
	return $taxon_no;
}


sub selectMostRecentParentOpinion{
	my $dbt = shift;
	my $array_ref = shift;
	my $return_index = (shift or 0); # return index in array (or parent_no)
	my @array_of_hash_refs = @{$array_ref};
	
	if(scalar @array_of_hash_refs == 1){
		if($return_index == 1){
			return 0;
		}
		else{
			return $array_of_hash_refs[0]->{parent_no};
		}
	}
	elsif(scalar @array_of_hash_refs > 1){
		PBDBUtil::debug(1,"FOUND MORE THAN ONE PARENT<br>");
		# find the most recent opinion
		# Algorithm: For each opinion (parent), if opinions.pubyr exists, 
		# use it.  If not, get the pubyr from the reference and use it.
		# Finally, compare across parents to find the most recent year.

		# Store only the greatest value in $years
		my $years = 0;
		my $index_winner = -1;
		for(my $index = 0; $index < @array_of_hash_refs; $index++){
			# pubyr is recorded directly in the opinion record,
			#  so use it
			if($array_of_hash_refs[$index]->{pubyr} &&
					$array_of_hash_refs[$index]->{pubyr} > $years){
				$years = $array_of_hash_refs[$index]->{pubyr};
				$index_winner = $index;
			}
			else{
				# get the year from the refs table
				# JA: also get the publication type because
				#  everything else is preferred to compendia
				my $sql = "SELECT pubyr,publication_type AS pt FROM refs WHERE reference_no=".
						  $array_of_hash_refs[$index]->{reference_no};
				my @ref_ref = @{$dbt->getData($sql)};
		# this is kind of ugly: use pubyr if it's the first one
		#  encountered, or if the winner's pub type is compendium and
		#  the current ref's is not, or if both are type compendium and
		#  the current ref is more recent, or if neither are type
		#  compendium and the current ref is more recent
				if($ref_ref[0]->{pubyr} &&
					( ! $years ||
					( $ref_ref[0]->{pt} ne "compendium" &&
					  $winner_pt eq "compendium" ) ||
					( $ref_ref[0]->{pubyr} > $years &&
					  $ref_ref[0]->{pt} eq "compendium" &&
					  $winner_pt eq "compendium" ) ||
					( $ref_ref[0]->{pubyr} > $years &&
					  $ref_ref[0]->{pt} ne "compendium" &&
					  $winner_pt ne "compendium" ) ) )	{
					$years = $ref_ref[0]->{pubyr};
					$index_winner = $index;
					$winner_pt = $ref_ref[0]->{pt};
				}
			}	
		}
		if($return_index == 1){
			return $index_winner;
		}
		else{
			return $array_of_hash_refs[$index_winner]->{parent_no};
		}
	}
	# nothing was passed to us in the first place, return the favor.
	else{
		return undef;
	}
}

## Deal with homonyms
sub deal_with_homonyms{
	my $dbt = shift;
	my $array_ref = shift;
	my @array_of_hash_refs = @{$array_ref};

	if(scalar @array_of_hash_refs == 1){
		return $array_ref;
	}
	else{
		# for each child, find its parent
		foreach my $ref (@array_of_hash_refs){
			# first, use the pubyr/author for clarification:
			if($ref->{pubyr} && $ref->{author1last}){
				$ref->{clarification_info} = $ref->{author1last}." ".$ref->{pubyr}." ";
			}
			elsif($ref->{ref_is_authority} && $ref->{reference_no}){
				my $sql = "SELECT pubyr, author1last FROM refs ".
						  "WHERE reference_no=".$ref->{reference_no};
				my @auth_ref = @{$dbt->getData($sql)};
				$ref->{clarification_info} = $auth_ref[0]->{author1last}." ".$auth_ref[0]->{pubyr}." ";
			}
			my $sql = "SELECT parent_no, status, pubyr, reference_no ".
					  "FROM opinions WHERE child_no=".$ref->{taxon_no}.
					  " AND status='belongs to'";	
			my @ref_ref = @{$dbt->getData($sql)};
			# if it has no parent, find the child that's either 
			# "corrected as", "recombined as", "rank changed as", or "replaced by" and use 
			# that as the focal taxon.
			if(scalar @ref_ref < 1){
				$sql = "SELECT child_no FROM opinions WHERE parent_no=".
					   $ref->{taxon_no}." AND status IN ('recombined as',".
					   "'replaced by','corrected as','rank changed as')";
				@ref_ref = @{$dbt->getData($sql)};
				if(scalar @ref_ref < 1){
					# Dead end: clarification_info?
					return $array_ref;	
				}
				# try above again:
				$sql = "SELECT parent_no, status, pubyr, reference_no ".
					   "FROM opinions WHERE child_no=".$ref_ref[0]->{child_no};	
				@ref_ref = @{$dbt->getData($sql)};
			}
			# get the most recent parent
			#	if it's not "belongs to", get most recent grandparent, etc.
			#	until we get a "belongs to."
			my $index = selectMostRecentParentOpinion($dbt, \@ref_ref, 1);
			while($ref_ref[$index]->{status} ne "belongs to"){
				my $child_no = $ref_ref[$index]->{parent_no};
				# HIT UP THE NEXT GENERATION OF PARENTS:
				#	-start over with the first select in this method.
				$sql = "SELECT parent_no, status, pubyr, reference_no ".
					   "FROM opinions WHERE child_no=$child_no";	
				@ref_ref = @{$dbt->getData($sql)};
				$index = selectMostRecentParentOpinion($dbt, \@ref_ref, 1);
			}
			# Then, add another key to the hash for "clarification_info"
			Debug::dbPrint("test2 = " .  $ref_ref[$index]->{parent_no} );
			$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".
				   $ref_ref[$index]->{parent_no};
			@ref_ref = @{$dbt->getData($sql)};
			$ref->{clarification_info} .= "(".$ref_ref[0]->{taxon_name}.")";
		}
	}
	return \@array_of_hash_refs;
}


## Add items from new array to old array iff they are not duplicates
## Where arrays are db record sets (arrays of hashes)
sub array_push_unique{
	my $orig_ref = shift;
	my $new_ref = shift;
	my $field = shift;
	my $field2 = shift;
	my @orig = @{$orig_ref};
	my @new = @{$new_ref};
	my $duplicate = 0;

	foreach my $item (@new){
		my $duplicate = 0;
		foreach my $old (@orig){
			if($field2){
				if($item->{$field} eq $old->{$field} && 
						$item->{$field2} eq $old->{$field2}){
					$duplicate = 1;
					last;
				}
			}
			elsif($item->{$field} eq $old->{$field}){
				$duplicate = 1;
				last;
			}
		}
		unless($duplicate){
			push(@orig, $item);
		}
	}
	return \@orig;
}

# Verify taxon:  If what was entered's most recent parent is a "belongs to"
# or a "nomen *" relationship, then do the display page on what was entered.
# If any other relationship exists for the most recent parent, display info 
# on that parent.
sub verify_chosen_taxon{
	my $taxon = shift;
	my $num = shift;
	my $dbt = shift;
	my $sql = "";
 	my @results = ();

	# First, see if this name has any recombinations:
	if(!$num){
		$sql = "SELECT taxon_no FROM authorities WHERE taxon_name='$taxon'";
		@results = @{$dbt->getData($sql)};
		# Elephas maximus won't return anything, but the script will still
		# work for it because we have one occurrence in the db. (so don't
		# let it choke out here)
		if(@results > 0){
			$num = $results[0]->{taxon_no};
			#$temp_num = $results[0]->{taxon_no};
		}
	}
	if($num){
		$sql = "SELECT child_no,taxon_name FROM opinions,authorities ".
			   "WHERE parent_no=$num AND taxon_no=child_no ".
			   #"WHERE parent_no=$temp_num AND taxon_no=child_no ".
			   "AND status='recombined as'";
		@results = @{$dbt->getData($sql)};
		# might not get any recombinations; that's fine - keep going either way.
		if(scalar @results > 0){
			$num = $results[0]->{child_no};
			$taxon = $results[0]->{taxon_name};
		}
	}

	# Now, get the senior synonym or most current combination:
	$sql = "SELECT authorities.taxon_no,opinions.parent_no,opinions.pubyr, ".
			  "opinions.status, opinions.reference_no ".
			  "FROM authorities, opinions ".
			  "WHERE authorities.taxon_name='$taxon' ".
			  "AND authorities.taxon_no = opinions.child_no";

	if($num){
		$sql = "SELECT parent_no, pubyr, reference_no, status ".
			   "FROM opinions WHERE child_no=$num";
	}
	@results = @{$dbt->getData($sql)};
	
	# Elephas maximus won't return anything for the same reason mentioned above.
	if(@results <1){
		return ($taxon, $num);
	}
	
	my $index = selectMostRecentParentOpinion($dbt, \@results, 1);

	# if the most recent parent is a 'belongs to' relationship, just return
	# what we were given (or found in recombinations).
	if($results[$index]->{status} eq "belongs to" ||
	  							$results[$index]->{status} =~ /nomen/i){
		return ($taxon, $num);
	}
	# Otherwise, return the name and number of the parent.
	else{
		my $parent_no = $results[$index]->{parent_no};
		Debug::dbPrint("test3 = " . $results[$index]->{parent_no});
		
		$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".
			   $results[$index]->{parent_no};
		@results = @{$dbt->getData($sql)};
		return ($results[0]->{taxon_name},$parent_no);
	}
}

# JA 1.8.03
sub displayEcology	{
	my $dbt = shift;
	my $taxon_no = shift;
	my $genus = shift;
	my $species = shift;

	print "<center><h3>Ecology</h3></center>\n";

	if ( ! $taxon_no )	{
		print "<i>No ecological data are available</i>";
		return;
	}

	my $taxon_name;
	if ( $species )	{
		$taxon_name = $genus . " " . $species;
	} else	{
		$taxon_name = $genus;
	}

	# get the field names from the ecotaph table
	my %attrs = ("NAME"=>'');
	my $sql = "SELECT * FROM ecotaph WHERE taxon_no=0";
	%ecotaphRow = %{@{$dbt->getData($sql,\%attrs)}[0]};
	for my $name (@{$attrs{"NAME"}})	{
		$ecotaphRow{$name} = "";
		push @ecotaphFields,$name;
	}

	# grab all the data for this taxon from the ecotaph table
	$sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $taxon_no;
	$ecotaphVals = @{$dbt->getData($sql)}[0];

	# also get values for ancestors (always do this because data for the
	#   taxon could be partial)
	# WARNING: this will completely screw up if the name has homonyms
	# JA: changed this on 4.4.04 to use taxon_no instead of taxon_name,
	#  avoiding homonym problem
	push my @tempnames, $taxon_no;
	my @ancestors = Classification::get_classification_hash($dbt,'class',\@tempnames,'yes');

	Debug::dbPrint("ancestors = @ancestors");
	
	my $tempVals;
	if ( @ancestors )	{
		for my $a ( @ancestors )	{
			$sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $a;
			
			Debug::dbPrint("sql = $sql");
			
			$tempVals = @{$dbt->getData($sql)}[0];
			if ( $tempVals )	{
				for my $field ( @ecotaphFields )	{
					if ( $tempVals->{$field} && ! $ecotaphVals->{$field} && $field ne "created" && $field ne "modified" )	{
						$ecotaphVals->{$field} = $tempVals->{$field};
					}
				}
			}
		}
	}

	if ( ! $ecotaphVals )	{
		print "<i>No ecological data are available</i>";
		return;
	} else	{
		print "<table cellspacing=5><tr>";
		my $cols = 0;
		for my $i (0..$#ecotaphFields)	{
			my $name = $ecotaphFields[$i];
			my $nextname = $ecotaphFields[$i+1];
			my $n = $name;
			my @letts = split //,$n;
			$letts[0] =~ tr/[a-z]/[A-Z]/;
			$n = join '',@letts;
			$n =~ s/_/ /g;
			$n =~ s/1$/&nbsp;1/g;
			$n =~ s/2$/&nbsp;2/g;
			if ( $ecotaphVals->{$name} && $name !~ /_no$/ )	{
				$cols++;
				my $v = $ecotaphVals->{$name};
				$v =~ s/,/, /g;
				print "<td valign=\"top\"><table><tr><td align=\"left\" valign=\"top\"><b>$n:</b></td><td align=\"right\" valign=\"top\">$v</td></tr></table></td> \n";
			}
			if ( $cols == 2 || $nextname =~ /^created$/ || $nextname =~ /^size_value$/ || $nextname =~ /1$/ )	{
				print "</tr>\n<tr>\n";
				$cols = 0;
			}
		}
		if ( $cols > 0 )	{
			print "</tr></table>\n";
		} else	{
			print "</table>\n";
		}
	}


	return $text;

}

# JA 11-12,14.9.03
sub displaySynonymyList	{
	my $dbt = shift;
	my $q = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $taxon_no = (shift or "");
	my $taxon_name;
	my $output = "";

	if ( $genus && $species ne "" )	{
		$taxon_name = $genus . " " . $species;
	}
	else{
		$taxon_name = $genus;
	}

	print "<center><h3>Synonymy</h3></center>";

# find all distinct children where relation is NOT "belongs to"
#  (mostly synonyms and earlier combinations)
	$sql = "SELECT DISTINCT child_no FROM opinions WHERE status!='belongs to' AND parent_no=" . $taxon_no;
	my @childrefs = @{$dbt->getData($sql)};

# for each one, find the most recent parent
	for $childref ( @childrefs )	{
		$sql = "SELECT parent_no, status, pubyr, reference_no ".
			   "FROM opinions WHERE child_no=" . $childref->{child_no};
		@ref_ref = @{$dbt->getData($sql)};
		my $itsparent = selectMostRecentParentOpinion($dbt, \@ref_ref);
	# if current parent is the focal taxon, save to a list
		if ( $itsparent == $taxon_no )	{
			push @syns, $childref->{child_no};
		}
	}

# go through list finding all "recombined as" something else cases for each
# need to do this because synonyms might have their own recombinations, and
#  the original combination might have alternative combinations
# don't do this if the parent is actually the focal taxon
	my @synparents;
	for my $syn (@syns)	{
		$sql = "SELECT parent_no FROM opinions WHERE status='recombined as' AND child_no=" . $syn . " AND parent_no!=" . $taxon_no;
		@synparentrefs = @{$dbt->getData($sql)};
		for $synparentref ( @synparentrefs )	{
			push @synparents, $synparentref->{parent_no};
		}
	}

# save each "recombined as" taxon to the list
	push @syns, @synparents;

# add the focal taxon itself to the synonymy list
	push @syns,$taxon_no;

# go through all the alternative names and mark the original combinations
#  (or really "never recombinations")
	for $syn (@syns)	{
		$sql = "SELECT count(*) AS c FROM opinions WHERE parent_no=$syn AND status='recombined as'";
		 $timesrecombined = ${$dbt->getData($sql)}[0]->{c};
		if ( ! $timesrecombined )	{
			$isoriginal{$syn} = "YES";
		}
	}

# now we have a list of the focal taxon, its alternative combinations, its
#  synonyms, and their alternative combinations
# go through the list finding all instances of each name's use as a parent
#  in a recombination or synonymy
# so, we are getting the INSTANCES and not just the alternative names,
#  which we already know
	for my $syn (@syns)	{
		$sql = "SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE status!='belongs to' AND parent_no=" . $syn;
		my @userefs =  @{$dbt->getData($sql)};

		$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $syn;

		my $parent_name = @{$dbt->getData($sql)}[0]->{taxon_name};
		if ( $q->param("taxon_rank") =~ /(genus)|(species)/i )	{
			$parent_name = "<i>" . $parent_name . "</i>";
		}
		for $useref ( @userefs )	{
			if ( $useref->{pubyr} )	{
				$synkey = "<td>" . $useref->{pubyr} . "</td><td>" . $parent_name . " " . $useref->{author1last};
				if ( $useref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $useref->{author2last} )	{
					$synkey .= " and " . $useref->{author2last};
				}
				$mypubyr = $useref->{pubyr};
		# no pub data, get it from the refs table
			} else	{
				$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
				$refref = @{$dbt->getData($sql)}[0];
				$synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $parent_name . " " . $refref->{author1last};
				if ( $refref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $refref->{author2last} )	{
					$synkey .= " and " . $refref->{author2last};
				}
				$mypubyr = $refref->{pubyr};
			}
			if ( $useref->{pages} )	{
				if ( $useref->{pages} =~ /[ -]/ )	{
					$synkey .= " pp. " . $useref->{pages};
				} else	{
					$synkey .= " p. " . $useref->{pages};
				}
			}
			if ( $useref->{figures} )	{
				if ( $useref->{figures} =~ /[ -]/ )	{
					$synkey .= " figs. " . $useref->{figures};
				} else	{
					$synkey .= " fig. " . $useref->{figures};
				}
			}
			$synline{$synkey} = $mypubyr;
		}
	}

# likewise appearances in the authority table
	for my $syn (@syns)	{
	if ( $isoriginal{$syn} eq "YES" )	{
		$sql = "SELECT taxon_name,author1last,author2last,otherauthors,pubyr,pages,figures,ref_is_authority,reference_no FROM authorities WHERE taxon_no=" . $syn;
		@userefs = @{$dbt->getData($sql)};
	# save the instance as a key with pubyr as a value
	# note that @userefs only should have one value because taxon_no
	#  is the primary key
		for $useref ( @userefs )	{
			my $auth_taxon_name = $useref->{taxon_name};
			if ( $q->param("taxon_rank") =~ /(genus)|(species)/i )	{
				$auth_taxon_name = "<i>" . $auth_taxon_name . "</i>";
			}
			if ( $useref->{pubyr} )	{
				$synkey = "<td>" . $useref->{pubyr} . "</td><td>" . $auth_taxon_name . " " . $useref->{author1last};
				if ( $useref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $useref->{author2last} )	{
					$synkey .= " and " . $useref->{author2last};
				}
				$mypubyr = $useref->{pubyr};
		# no pub data, get it from the refs table
			} else	{
				$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
				$refref = @{$dbt->getData($sql)}[0];
				$synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $auth_taxon_name . " " . $refref->{author1last};
				if ( $refref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $refref->{author2last} )	{
					$synkey .= " and " . $refref->{author2last};
				}
				$mypubyr = $refref->{pubyr};
			}
			if ( $useref->{pages} )	{
				if ( $useref->{pages} =~ /[ -]/ )	{
					$synkey .= " pp. " . $useref->{pages};
				} else	{
					$synkey .= " p. " . $useref->{pages};
				}
			}
			if ( $useref->{figures} )	{
				if ( $useref->{figures} =~ /[ -]/ )	{
					$synkey .= " figs. " . $useref->{figures};
				} else	{
					$synkey .= " fig. " . $useref->{figures};
				}
			}
			$synline{$synkey} = $mypubyr;
		}
	}
	}

# sort the synonymy list by pubyr
	@synlinekeys = keys %synline;
	@synlinekeys = sort { $synline{$a} <=> $synline{$b} } @synlinekeys;

# print each line of the synonymy list
	print "<table cellspacing=5>\n";
	print "<tr><td><b>Year</b></td><td><b>Name and author</b></td></tr>\n";
	for $synline ( @synlinekeys )	{
		print "<tr>$synline</td></tr>\n";
	}
	print "</table>\n";

	return "";

}

1;
