package TaxonInfo;

use PBDBUtil;
use Classification;
use Globals;
use Debug;

# URLMaker is a lousy Poling module and this one shouldn't use it
use URLMaker;
use HTMLBuilder;
use DBTransactionManager;
use Taxon;
use TimeLookup;

use POSIX qw(ceil floor);


$DEBUG = 0;

my %GLOBALVARS;


sub startTaxonInfo {
	my $hbo = shift;
	my $q = shift;

	print main::stdIncludes( "std_page_top" );
	searchForm($hbo, $q);
	print main::stdIncludes("std_page_bottom");
}



# JA: rjp did a big complicated reformatting of the following and I can't
#  confirm that no damage was done in the course of it
# him being an idiot, I think it's his fault that all the HTML originally was
#  embedded instead of being in a template HTML file that's passed to
#  populateHTML
# possibly Muhl's fault because he uses populateHTML nowhere in this module
# I fixed this 21.10.04
sub searchForm{
	my $hbo = shift;
	my $q = shift;
	my $search_again = (shift or 0);

	print $hbo->populateHTML('js_search_taxon_form' , [ ], [ ]);

	unless($search_again){
		print "<div class=\"title\">Taxon search form</div>";
	}

	print $hbo->populateHTML('search_taxon_form' , [ ], [ ]);

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
    my $hbo = shift;
	
	$GLOBALVARS{session} = $s;

    # If we have is a taxon_no, use that:
    my ($auth,$pubyr,$taxon_name,$taxon_rank);
    if ($q->param('taxon_no')) {
        my $sql = "SELECT taxon_rank,taxon_name FROM authorities WHERE taxon_no=".$dbh->quote($q->param('taxon_no'));
        my @results = @{$dbt->getData($sql)};
        if (@results) {
            $taxon_name = $results[0]->{'taxon_name'};
            if ($results[0]->{'taxon_rank'} eq 'species' && $taxon_name =~ / /) {
                $taxon_rank = "Genus and species";
            } elsif ($results[0]->{'taxon_rank'} eq 'species') {
                $taxon_rank = "Species";
            } elsif ($results[0]->{'taxon_rank'} eq 'genus') {
                $taxon_rank = "Genus";
            } else {
                $taxon_rank = "Higher taxon";
            }
        }
    } else {
        # the "Original Rank" popup menu on the entry form
        $taxon_rank = $q->param("taxon_rank");
        $taxon_rank =~ s/\+/ /g;  # remove plus signs??  why?
        
        if($taxon_rank ne "Genus" && $taxon_rank ne "Genus and species" && 
           $taxon_rank ne "species") {
            $taxon_rank = "Higher taxon";
            # note, does this really make sense?  is a "subgenus" a "higher taxon"?
        }
    
        # the "Name of type taxon" text entry field on the entry form
        $taxon_name = $q->param("taxon_name");
        $taxon_name =~ s/\+/ /g;
    }
	# Where do these two parameters come from?  The current reference?
	$author = $q->param("author");
	$pubyr = $q->param("pubyr");
	
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
	if($taxon_rank eq "" or ( $taxon_name eq "" && ! $author && ! $pubyr ) ){
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
	elsif($taxon_rank eq "Higher taxon"){
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
	elsif($taxon_rank eq "Genus and species"){
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
	elsif($taxon_rank eq "Genus"){
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
		searchForm($hbo, $q, 1); # param for not printing header with form
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
			  "<input name=\"taxon_rank\" type=\"hidden\" value=\"$taxon_rank\">".
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
	#$genus_name = $verified[0];
	#$taxon_no = $verified[1];
    #print "verified $genus_name, $taxon_no orig $entered_name, $entered_no<br>";

    if ($taxon_no) {
        $orig_taxon_no = getOriginalCombination($dbt,$taxon_no);
        my $c_row = PBDBUtil::getCorrectedName($dbt,$orig_taxon_no,0,1);
        $taxon_no=$c_row->{'taxon_no'};
        $genus_name =$c_row->{'taxon_name'};
    }

	$q->param("genus_name" => $genus_name);

	# Get the sql IN list for a Higher taxon:
	my $in_list = "";

	if($orig_taxon_no) {
	# JA: replaced recurse call with taxonomic_search call 7.5.04 because
	#  I am not maintaining recurse
		#my $name = $q->param('genus_name');
		#$in_list = `./recurse $name`;
        @in_list=PBDBUtil::taxonomic_search($q->param('genus_name'),$dbt,'','return_taxon_nos');
        $in_list=\@in_list;
	} else {
	    # Don't go looking for junior synonyms if this taxon isn't even
	    #  in the authorities table (because it has no taxon_no) JA 8.7.03
		$in_list = [$q->param('genus_name')];
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
							  6 => "age range/collections");

	# if the modules are known and the user is not a guest,
	#  set the module preferences in the person table
	# if the modules are not known try to pull them from the person table
	# of course, we don't have to reset the preferences in that case
	# JA 21.10.04
	if ( $s->get("enterer") ne "Guest" && $s->get("enterer") ne "" )	{
		if ( @modules_to_display )	{
			my $pref = join ' ', @modules_to_display;
			my $prefsql = "UPDATE person SET taxon_info_modules='$pref',last_action=last_action WHERE name='" . $s->get("enterer") . "'";
			$dbt->getData($prefsql);
		}
		elsif ( ! @modules_to_display )	{
			my $prefsql = "SELECT taxon_info_modules FROM person WHERE name='" . $s->get("enterer") . "'";
			$pref = @{$dbt->getData($prefsql)}[0]->{taxon_info_modules};
			@modules_to_display = split / /,$pref;
		}
	}

	# if that didn't work, set the default
	if ( ! @modules_to_display )	{
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

	my $first_module = shift @modules_to_display;
	print "<p><center><input type=submit value=\"update\"></center>";
	print "</td></tr></table></td></tr></table></td>";
	print "<td>";
	# First module here
	doModules($dbt,$dbh,$q,$s,$exec_url,$first_module,$genus,$species,$in_list,$orig_taxon_no);
	print "</td></tr></table>";
	print "<hr width=\"100%\">";

	# Go through the list
	foreach my $module (@modules_to_display){
		print "<center><table>";
		print "<tr><td>";
		doModules($dbt,$dbh,$q,$s,$exec_url,$module,$genus,$species,$in_list,$orig_taxon_no);
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
        if ($taxon_no) {
    		print displayTaxonSynonymy($dbt, $genus, $species, $taxon_no);
        } else {
            print "<table width=\"100%\">".
                  "<tr><td align=\"middle\"><h3>Taxonomic history</h3></td></tr>".
                  "<tr><td valign=\"top\" align=\"middle\">".
                  "<i>No taxonomic history data are available</i>".
                  "</td></tr></table>\n";
        }
	}
	elsif ( $module == 3 )	{
        if ($taxon_no) {
    		print displaySynonymyList($dbt, $q, $genus, $species, $taxon_no);
        } else {
            print "<table width=\"100%\">".
                  "<tr><td align=\"middle\"><h3>Synonymy</h3></td></tr>".
                  "<tr><td valign=\"top\" align=\"middle\">".
                  "<i>No synonymy data are available</i>".
                  "</td></tr></table>\n";
        }
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
		if ( $map_html_path )	{
			if($map_html_path =~ /^\/public/){
				# reconstruct the full path the image.
				$map_html_path = $ENV{DOCUMENT_ROOT}.$map_html_path;
			}
			open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
			while(<MAP>){
				print;
			}
			close MAP;
		} else {
		    print "<i>No distribution data are available</i>";
        }
		print "</td></tr></table></center>";
		# trim the path down beyond apache's root so we don't have a full
		# server path in our html.
		if ( $map_html_path )	{
			$map_html_path =~ s/.*?(\/public.*)/$1/;
			print "<input type=hidden name=\"map_num\" value=\"$map_html_path\">";
		}
	}
	# collections
	elsif($module == 6){
		print doCollections($exec_url, $q, $dbt, $dbh, $in_list);
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
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape1', 'dotcolor1', 'dotborder1');
	my %user_prefs = main::getPreferences($s->get('enterer'));
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}
	# Not covered by prefs:
	if(!$q->param('pointshape1')){
		$q->param('pointshape1' => 'circles');
	}
	if(!$q->param('dotcolor1')){
		$q->param('dotcolor1' => 'red');
	}
	if(!$q->param('coastlinecolor')){
		$q->param('coastlinecolor' => 'black');
	}
	$q->param('mapresolution'=>'medium');

	# note, we need to leave this in here even though it's 
	# redunant (since we scale below).. taking it out will
	# cause a division by zero error in Map.pm.
	$q->param('mapscale'=>'X 1');


	$q->param('pointsize1'=>'tiny');

	if(!$q->param('projection') or $q->param('projection') eq ""){
		$q->param('projection'=>'rectilinear');
	}

	require Map;
	my $m = Map->new( $dbh, $q, $s, $dbt );
	my $dataRowsRef = $m->buildMapOnly($in_list);

	if(scalar(@{$dataRowsRef}) > 0) {
		# this section added by rjp on 12/11/2003
		# at this point, we need to figure out the bounds 
		# of the collections and the center point.  
		my @bounds = calculateCollectionBounds($dataRowsRef);

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
		return $m->drawMapOnly($dataRowsRef);
	}  else {
        return;
	}
}



sub doCollections{
	my $exec_url = shift;
	my $q = shift;
	my $dbt = shift;
	my $dbh = shift;
	my $in_list = shift;
    # age_range_format changes appearance html formatting of age/range information
    # used by the strata module
    my $age_range_format = shift;  
	my $output = "";

	$q->param(-name=>"limit",-value=>1000000);
	$q->param(-name=>"taxon_info_script",-value=>"yes");

	# get a lookup of the boundary ages for all intervals JA 25.6.04
	# the boundary age hashes are keyed by interval nos
        @_ = TimeLookup::findBoundaries($dbh,$dbt);
        my %upperbound = %{$_[0]};
        my %lowerbound = %{$_[1]};

	# get all the interval names because we need them to print the
	#  total age range below
	my $isql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intrefs =  @{$dbt->getData($isql)};
	my %interval_name;
	for my $ir ( @intrefs )	{
		$interval_name{$ir->{'interval_no'}} = $ir->{'interval_name'};
		if ( $ir->{'eml_interval'} )	{
			$interval_name{$ir->{'interval_no'}} = $ir->{'eml_interval'} . " " . $ir->{'interval_name'};
		}
	}

	# Get all the data from the database, bypassing most of the normal behavior
	# of displayCollResults
	@data = @{main::displayCollResults($in_list)};	
	require Collections;

	# figure out which intervals are too vague to use to set limits on
	#  the joint upper and lower boundaries
	# "vague" means there's some other interval falling entirely within
	#  this one JA 26.1.05
	my %seeninterval;
	my %toovague;
	for my $row ( @data )	{
		if ( ! $seeninterval{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}} )	{
			$max1 = $lowerbound{$row->{'max_interval_no'}};
			$min1 = $upperbound{$row->{'min_interval_no'}};
			if ( $min1 == 0 )	{
				$min1 = $upperbound{$row->{'max_interval_no'}};
			}
			for $intervalkey ( keys %seeninterval )	{
				my ($maxno,$minno) = split / /,$intervalkey;
				$max2 = $lowerbound{$maxno};
				$min2 = $upperbound{$minno};
				if ( $min2 == 0 )	{
					$min2 = $upperbound{$maxno};
				}
				if ( $max1 < $max2 && $max1 > 0 && $min1 > $min2 && $min2 > 0 )	{
					$toovague{$intervalkey}++;
				}
				elsif ( $max1 > $max2 && $max2 > 0 && $min1 < $min2 && $min1 > 0 )	{
					$toovague{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}}++;
				}
			}
			$seeninterval{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}}++;
		}
	}

	# Process the data:  group all the collection numbers with the same
	# time-place string together as a hash.
	%time_place_coll = ();
	my $oldestlowerbound;
	my $oldestlowername;
	my $youngestupperbound = 9999;
	my $youngestuppername;
	foreach my $row (@data){
	    $res = Collections::createTimePlaceString($row,$dbt);
	    if(exists $time_place_coll{$res}){
			push(@{$time_place_coll{$res}}, $row->{"collection_no"});
	    }
	    else{
			$time_place_coll{$res} = [$row->{"collection_no"}];
			push(@order,$res);
		# create a hash array where the keys are the time-place strings
		#  and each value is a number recording the min and max
		#  boundary estimates for the temporal bins JA 25.6.04
		# this is kind of tricky because we want bigger bins to come
		#  before the bins they include, so the second part of the
		#  number recording the upper boundary has to be reversed
		my $upper = $upperbound{$row->{'max_interval_no'}};
		$max_interval_name{$res} = $interval_name{$row->{'max_interval_no'}};
		$min_interval_name{$res} = $max_interval_name{$res};
		if ( $row->{'max_interval_no'} != $row->{'min_interval_no'} &&
			$row->{'min_interval_no'} > 0 )	{
			$upper = $upperbound{$row->{'min_interval_no'}};
			$min_interval_name{$res} = $interval_name{$row->{'min_interval_no'}};
		}
		# also store the overall lower and upper bounds for
		#  printing below JA 26.1.05
		# don't do this if the interval is too vague (see above)
		if ( ! $toovague{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}} )	{
			if ( $lowerbound{$row->{'max_interval_no'}} > $oldestlowerbound )	{
				$oldestlowerbound = $lowerbound{$row->{'max_interval_no'}};
				$oldestlowername = $max_interval_name{$res};
			}
			if ( $upper < $youngestupperbound )	{
				$youngestupperbound = $upper;
				$youngestuppername = $min_interval_name{$res};
			}
		}
		# WARNING: we're assuming upper boundary ages will never be
		#  greater than 999 million years
		# also, we're just going to ignore fractions of m.y. estimates
		#  because those would screw up the sort below
		$upper = int($upper);
		$upper = 999 - $upper;
		if ( $upper < 10 )	{
			$upper = "00" . $upper;
		} elsif ( $upper < 100 )	{
			$upper = "0" . $upper;
		}
		$bounds_coll{$res} = int($lowerbound{$row->{'max_interval_no'}}) . $upper;
	    }
	}

	# a little cleanup
	$oldestlowerbound =~ s/00$//;
	$youngestupperbound =~ s/00$//;


	# sort the time-place strings temporally or by geographic location
	my @sorted = sort { $bounds_coll{$b} <=> $bounds_coll{$a} || $a cmp $b } keys %bounds_coll;

	# legacy: originally the sorting was just on the key
#	my @sorted = sort (keys %time_place_coll);

	if(scalar @sorted > 0){
		# Do this locally because the module never gets exec_url
		#   from bridge.pl
		my $exec_url = $q->url();

		# print the first and last appearance (i.e., the age range)
		#  JA 25.6.04
		if ($age_range_format eq "for_strata_module") {
			$output .= "<p><b>Age range:</b> ";
			$output .= $oldestlowername;
			if ( $oldestlowername ne $youngestuppername )	{
				$output .= " to " . $youngestuppername;
			}
			$output .= " <i>or</i> " . $oldestlowerbound . " to " . $youngestupperbound . " Ma";
			$output .= "</p><br>\n<hr>\n";
		} else {
			$output .= "<center><h3>Age range</h3>\n";
			$output .= $oldestlowername;
			if ( $oldestlowername ne $youngestuppername )	{
	 			$output .= " to " . $youngestuppername;
			}
			$output .= " <i>or</i> " . $oldestlowerbound . " to " . $youngestupperbound . " Ma";
			$output .= "<center><p>\n<hr>\n";
		}

		$output .= "<center><h3>Collections</h3></center>\n";

		$output .= "<table width=\"100%\"><tr>";
		$output .= "<th align=\"middle\">Time interval</th>";
		$output .= "<th align=\"middle\">Country or state</th>";
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
    my $dbh = $dbt->dbh;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $orig_no = (shift or ""); #Pass in original combination no

    # Theres one case where we might want to do upward classification when theres no taxon_no:
    #  The Genus+species isn't in authorities, but the genus is
    my $genus_no = 0;
    if ($genus && $species && !$orig_no) {
        my @results = getTaxonNos($dbt,$genus);
        if (@results == 1) {
            $orig_no=$results[0];
            $genus_no=$results[0]; 
            $append_species=1;
        }
    }

    #
    # Do the classification
    #
    my ($taxon_no,$taxon_rank,$taxon_name,$recomb_no);
    my @table_rows = ();
    if ($orig_no) {
        # format of table_rows: taxon_rank,taxon_name,taxon_no(original combination),taxon_no(recombination, if recombined)
       
        # This will happen if the genus has a taxon_no but not the species
        if ($append_species) {
            push @table_rows, ['species',$species,0,0];
        }

        # First, find the rank, name, publication info of the focal taxon
        my $correct_row = PBDBUtil::getCorrectedName($dbt,$orig_no);
        $recomb_no = $correct_row->{'taxon_no'};    
        $taxon_name = $correct_row->{'taxon_name'};    
        $taxon_rank = $correct_row->{'taxon_rank'};    
        $recomb_no = ($orig_no != $recomb_no) ? $recomb_no: 0;

        push @table_rows, [$taxon_rank,$taxon_name,$orig_no,$recomb_no];

        # Now find the rank,name, and publication of all its parents
        $first_link = Classification::get_classification_hash($dbt,'all',[$orig_no],'linked_list');
        $next_link = $first_link->{$orig_no};
        for(my $counter = 0;%$next_link && $counter < 30;$counter++) {
            push @table_rows, [$next_link->{'rank'},$next_link->{'name'},$next_link->{'number'},$next_link->{'recomb_no'}];
            last if ($next_link->{'rank'} eq 'kingdom');
            $next_link = $next_link->{'next_link'};
		}

    } else {
        # This block is if no taxon no is found - go off the occurrences table
        push @table_rows,['species',"$genus $species",0,0] if $species;
        push @table_rows,['genus',$genus,$genus_no,0];
    }

    #
    # Print out the table in the reverse order that we initially made it
    #
    my $output = "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
    my $class = '';
    for($i = scalar(@table_rows)-1;$i>=0;$i--) {
        $class = $class eq '' ? 'class="darkList"' : '';
        $output .= "<tr $class>";
        my($taxon_rank,$taxon_name,$taxon_no,$recomb_no) = @{$table_rows[$i]};
        if ($taxon_rank =~ /species/) {
            @taxon_name = split(/\s+/,$taxon_name);
            $taxon_name = $taxon_name[-1];
        }
        my %auth_yr = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$taxon_no)} if $taxon_no;
        my $pub_info = $auth_yr{author1last}.' '.$auth_yr{pubyr};
        if ($recomb_no) {
            $pub_info = "(".$pub_info.")" if $pub_info ne ' ';
        } 
        if ($taxon_no) {
            #if ($species !~ /^sp(\.)*$|^indet(\.)*$/) {
            $link = qq|<a href="/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_no=$taxon_no">$taxon_name</a>|;
        } else {
            my $show_rank = ($taxon_rank eq 'species') ? 'Genus and species' : 
                            ($taxon_rank eq 'genus')   ? 'Genus' : 
                                                         'Higher taxon'; 
            $link = qq|<a href="/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_name=$taxon_rank&taxon_rank=$show_rank">$taxon_name</a>|;
        }
        $output .= qq|<td align="middle">$taxon_rank</td>|.
                   qq|<td align="middle">$link</td>|.
                   qq|<td align="middle" style="white-space: nowrap">$pub_info</td>|; 
        $output .= '</tr>';
    }
    $output .= "</table>";

    #
    # Begin getting sister/child taxa
    # PS 01/20/2004 - rewrite: Use getChildren function
    #
    $focal_taxon_no = $table_rows[0][2];
    $focal_taxon_rank = $table_rows[0][0];
    $parent_taxon_no = $table_rows[1][2];

    my $taxon_records = [];
    $taxon_records = PBDBUtil::getChildren($dbt,$focal_taxon_no,1,1) if ($focal_taxon_no);
    if (@{$taxon_records}) {
        my @child_taxa_links;
        foreach $record (@{$taxon_records}) {
            my @syn_links;                                                         
            my @synonyms = @{$record->{'synonyms'}};
            push @syn_links, $_->{'taxon_name'} for @synonyms;
            my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}">$record->{taxon_name}|;
            $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
            $link .= "</a>";
            push @child_taxa_links, $link;
        }
        if (@child_taxa_links) {
            $output .= "<p><i>This taxon includes:</i><br>"; 
            $output .= join(", ",@child_taxa_links);
            $output .= "</p>";
        }
    }

    # Get sister taxa as well
    # PS 01/20/2004
    $taxon_records = [];
    $taxon_records = PBDBUtil::getChildren($dbt,$parent_taxon_no,1,1) if ($parent_taxon_no);
    if (@{$taxon_records}) {
        my @child_taxa_links;
        foreach $record (@{$taxon_records}) {
            next if ($record->{'taxon_no'} == $orig_no);
            if ($focal_taxon_rank ne $record->{'taxon_rank'}) {
                PBDBUtil::debug(1,"rank mismatch $focal_taxon_rank -- $record->{taxon_rank} for sister $record->{taxon_name}");
            } else {
                my @syn_links;
                my @synonyms = @{$record->{'synonyms'}};
                push @syn_links, $_->{'taxon_name'} for @synonyms;
                my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}">$record->{taxon_name}|;
                $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
                $link .= "</a>";
                push @child_taxa_links, $link;
            }
        }
        if (@child_taxa_links) {
            $output .= "<p><i>Sister taxa include:</i><br>"; 
            $output .= join(", ",@child_taxa_links);
            $output .= "</p>";
        }
    }
####
# Not imp'd - potentially additional sister taxa can be gotten by 
# searching occurrences for 'parent_taxon_name' and splicing results in
# If a taxon no exists, have to make sure its unique before we do this, to avoid homonyms
# there still might be homonym problems we just don't knwo about though, screwing things up
    if (0) {
        $genus =  $parent_taxon_name;
            $sql = "(SELECT taxon_no,genus_name,species_name FROM occurrences WHERE genus_name=".$dbh->quote($genus)." GROUP BY species_name)";
            $sql .= " UNION ";
            $sql .= "(SELECT taxon_no,genus_name,species_name FROM reidentifications WHERE genus_name=".$dbh->quote($genus)." GROUP BY species_name)";
            $sql .= " ORDER BY species_name ASC";
            @results = @{$dbt->getData($sql)};
   
        my @taxa_links;
        foreach $row (@results) {
            next if ($species && $row->{'species_name'} eq $species);
            if ($row->{'taxon_no'}) {
                push @taxa_links, qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$row->{taxon_no}">$row->{genus_name} $row->{species_name}</a>|;
            } else {
                push @taxa_links, qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_name=$row->{genus_name} $row->{species_name}&taxon_rank=Genus and species">$row->{genus_name} $row->{species_name}</a>|;
            }
        }
        if (@taxa_links) {
            if ($rank eq 'Genus') {
                $output .= "<p><i>This taxon includes:</i><br>"; 
            } else {
                $output .= "<p><i>Sister taxa include:</i><br>"; 
            }
            $output .= join(", ",@taxa_links);
            $output .= "</p>";
        }
    }
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
	PBDBUtil::debug(1,"original combination_no: $original_combination_no");
	
	# Select all parents of the original combination whose status' are
	# either 'recombined as,' 'corrected as,' or 'rank changed as'
	$sql = "SELECT DISTINCT(parent_no), status FROM opinions ".
		   "WHERE child_no=$original_combination_no ".	
		   "AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
	@results = @{$dbt->getData($sql)};

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

	# Reduce these results to original combinations:
	foreach my $rec (@results) {
		$rec = getOriginalCombination($dbt, $rec->{child_no});	
	}


	# NOTE: "corrected as" could also occur at higher taxonomic levels.

	# Get synonymies for all of these original combinations
	foreach my $child (@results) {
		my $list_item = getSynonymyParagraph($dbt, $child);
		push(@paragraphs, "<br><br>$list_item\n") if($list_item ne "");
	}
	
	
	# Print the info for the original combination of the passed in taxon first.
	$output .= getSynonymyParagraph($dbt, $original_combination_no);

	# Now alphabetize the rest:
	@paragraphs = sort {lc($a) cmp lc($b)} @paragraphs;
	foreach my $rec (@paragraphs) {
		$output .= $rec;
	}

	
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
	
	my $sql = "SELECT taxon_name, author1last, pubyr, reference_no, comments, ".
			  "ref_is_authority FROM authorities WHERE taxon_no=$taxon_no";
	my @auth_rec = @{$dbt->getData($sql)};
	
	# Get ref info from refs if 'ref_is_authority' is set
	if($auth_rec[0]->{ref_is_authority} =~ /YES/i){
		PBDBUtil::debug(2,"author and year from refs");
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
		PBDBUtil::debug(2,"author and year from authorities");
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
			
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{author2last} = $real_ref[0]->{author2last};
			$row->{otherauthors} = $real_ref[0]->{otherauthors};
			$row->{pubyr} = $real_ref[0]->{pubyr};
		}
        # if this belongs to is paired with recombination, don't count it again, so skip it
        $found = 0; 
        while (($parent_no,$syn_array)=each %synonymies) {
            foreach $record (@$syn_array) {
                if ($row->{author1last} eq $record->{author1last} &&
                    $row->{author2last} eq $record->{author2last} &&
                    $row->{otherauthors} eq $record->{otherauthors} &&
                    $row->{pubyr} eq $record->{pubyr} && 
                    $record->{'status'} =~ /^(corrected|recombined|rank changed)/) {
                    $found = 1;
                 }
            }
        }
        next if $found;
       
		# use opinion numbers to keep recs separate for now
		$nomen_or_reval{$row->{opinion_no}} = $row;
		push(@nomen_or_reval_numbers, $row->{opinion_no});
		if($row->{status} =~ /nomen/){
			$has_nomen = 1;
		}
	}
	
	
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
			$tn = $synonymies{$syn_keys[$index]}[0]->{parent_no};
		} else {	
			$tn = $syn_keys[$index];
		}
		
		# Added by rjp, 3/29/2004 to fix an error caused by a missing parent number.
		if (!$tn) {
			next;
		}
		$sql .= $tn;
		
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

	return $text;
}


sub getOriginalCombination{
	my $dbt = shift;
	my $taxon_no = shift;
	my $loop_no = (shift || 0);
	# You know you're an original combination when you have no children
	# that have recombined or corrected as relations to you.
	my $sql = "SELECT DISTINCT(child_no), status FROM opinions".
			  " WHERE opinions.parent_no=$taxon_no".
              " AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
	my @results = @{$dbt->getData($sql)};

	my $has_status = 0;
	foreach my $rec (@results){
		$sql = "SELECT DISTINCT(child_no), status FROM opinions".
			   " WHERE parent_no=".$rec->{child_no}.
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
		$loop_no++;
		if ($loop_no > 10) { die ("loop in getOrigComb $results[0]->{child_no} $sql"); }
		$taxon_no = getOriginalCombination($dbt,$results[0]->{child_no},$loop_no);
	}

	# If we fall through above but $has_status was not set, we just
	# return the original $taxon_no passed in.
	return $taxon_no;
}


# JA 18.11.04
# wrapper for selectMostRecentParentOpinion (a much older function) that keeps
#  climbing the classification tree until a "belongs to" relation is found,
#  skipping over non-belongs to, non-nomen opinions
# WARNING: except in unusual circumstances (e.g., you want to know if the child
#  taxon is or isn't currently a synonym), this function really should be used
#  in preference to selectMostRecentParentOpinion
# also, unlike selectMostRecentParentOpinion this function returns a single
#  reference (as opposed to an index to an array of references or a parent no)
# I'm not 100% this routine works because the debugging case I was using
#  turns out to have involved corrupt data
sub  selectMostRecentBelongsToOpinion	{
	my $dbt = shift;
	my $array_ref = shift;
	my @parent_refs = @{$array_ref};

	# this is kind of tricky: we're going to get an index for
	#  a hash array even if a parent no is wanted, because
	#  otherwise we can't test for a "belongs to" status
	my $index = selectMostRecentParentOpinion($dbt, $array_ref, 1);

	while ( $parent_refs[$index]->{status} ne "belongs to" && $parent_refs[$index]->{status} !~ /^nomen/ )	{
		my $sql = "SELECT * FROM opinions WHERE child_no=" . $parent_refs[$index]->{parent_no};
		my @new_parent_refs = @{$dbt->getData($sql)};
		my $new_index = selectMostRecentParentOpinion($dbt, \@new_parent_refs, 1);
		if ( $new_index )	{
			@parent_refs = @new_parent_refs;
			$index = $new_index;
		} else	{
			last;
		}
	}

	return $parent_refs[$index];
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
		PBDBUtil::debug(2,"FOUND MORE THAN ONE PARENT");
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
			if ( $array_of_hash_refs[$index]->{pubyr} )	{
				if ( $array_of_hash_refs[$index]->{pubyr} > $years)	{
					$years = $array_of_hash_refs[$index]->{pubyr};
					$index_winner = $index;
				}
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
	my $ancestor_ref = Classification::get_classification_hash($dbt,'class,order,family',[$taxon_no],'numbers');
    my @ancestors = split(/,/,$ancestor_ref->{$taxon_no},-1);

	my $tempVals;
	if ( @ancestors)	{
		for my $a ( @ancestors )	{
            if ($a) {
                $sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $a;
                main::dbg($sql);
                
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
		$sql = "SELECT parent_no FROM opinions WHERE status='recombined as' AND child_no=" . $syn . " AND parent_no != " . $taxon_no;
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

# Small utility function, added 01/06/2004
# Used in Report, Map, Download, but haven't bothered to change TaxonInfo to use it
sub getTaxonNos {
    my $dbt = shift;
    my $name = shift;
    my $rank = shift;
    my @taxon_nos = ();
    if ($dbt && $name)  {
        my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name=".$dbt->dbh->quote($name);
        if ($rank) {
            $sql .= " AND taxon_rank=".$dbt->dbh->quote($name);
        }
        @results = @{$dbt->getData($sql)};
        push @taxon_nos, $_->{'taxon_no'} for @results;
    }
                                                                                                                                                         
    return @taxon_nos;
}


1;
