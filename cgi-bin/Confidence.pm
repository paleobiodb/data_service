#!/usr/bin/perl

package Confidence;

use DBTransactionManager;
use Debug;
use Data::Dumper; 
use GD;

# written 03.31.04 by Josh Madin as final product
# Still doesn't allow choice of time-scale when examining the conf ints of taxa

#----------------------FIRST-PAGE-----------------------------------------------

sub displayQueryPage    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    print "<DIV CLASS=\"title\">Confidence interval form</DIV><BR>";
    print "<BODY><CENTER><TABLE CELLPADDING=5 BORDER=0>";
    print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"databaseCheckForm\">";
    print "<TR><TD ALIGN=\"right\">Enter a <SELECT NAME=\"input_type\"><OPTION VALUE=\"strat\">stratigraphic section</OPTION><OPTION VALUE=\"taxon\">taxonomic name</OPTION></SELECT>:</TD>";
    print "<TD ALIGN=\"left\"><INPUT TYPE=\"text\" SIZE=\"20\" NAME=\"input\"></TD></TR>"; 
    print "<TR><TD ALIGN=\"right\">Taxonomic resolution:</TD>";
    print "<TD ALIGN=\"left\"><SELECT NAME=\"taxon_resolution\"><OPTION>species</OPTION><OPTION>genus</OPTION></SELECT></TD></TR>"; 
    print "</TABLE><BR>";
#	print "<TEXTAREA ROWS=\"5\" COLUMNS=\"20\" NAME=\"input\"></TEXTAREA></TD></TR></TABLE>";

#    print "<SPAN CLASS=\"tiny\">(This form will accept a taxa list copied from a text file)</SPAN><BR><BR><BR>";
    print "<INPUT NAME=\"full\" TYPE=\"submit\" VALUE=\"Submit\"></FORM></CENTER><BR><BR>";
    
    return;
}

sub checkData    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
#-------------------CHECK-IF-TAXON-------------------------------------------------
    if ($q->param('input_type') eq 'taxon') {
        my @taxon = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('input'));
# ----------------------REMAKE SPECIES LIST-----ALSO REMOVE UNCHECKED--------------
        my $i=0;
        my @splist;
        my $testspe =0;
        my $testyes =0;
        while ($q->param("speciesname$i"))  {
            if ($q->param("keepspecies$i") eq "yes")   {
                push @splist, $q->param("speciesname$i");
                $testyes++;
            }
            $testspe++;
            $i++;
        }    

        if (scalar(@taxon) == 2) { #genus+species
            $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
            $sql .= " AND species_name=" . $dbh->quote($taxon[1]);
        } elsif (scalar(@taxon) ==1) { #genus
            $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
        } else    {
            if (scalar(@splist) == 0)   {
                displayQueryPage($q, $s, $dbt);
                print "<center><table><tr><th><font color=\"red\">Sorry, couldn't understand your entry</font></th></tr></table></CENTER><br>";
                return;
            } else {
                if ($testspe == $testyes)   {
                    optionsForm($q, $s, $dbt, \@splist);
                    return;
                }
            }
        }
        main::dbg("Species list: ".Dumper(\@splist));
        main::dbg("Species sql: $sql");
        my @tempory = @{$dbt->getData($sql)};
        my @checktaxon;
        for my $counter (@tempory)        {
            push @checktaxon, $counter->{collection_no};
        }
        if (scalar(@checktaxon) > 0)    {
            push @splist, $q->param('input');
            buildList($q, $s, $dbt, \@splist);
        } else {
            buildList($q, $s, $dbt, \@splist);
            print "<center><table><tr><th><font color=\"red\">Sorry, </font><font color=\"blue\"><i>".$q->param('input')."</i></font><font color=\"red\"> is not in the database</font></th></tr></table></CENTER><BR><BR>";
        }
#-------------------CHECK-IF-STRAT-------------------------------------------------
    } else {
        my $strat = $q->param("input");
        my $sql = "SELECT count(*) as cnt FROM collections WHERE localsection = " . $dbh->quote($strat) . " AND localbed !=''";
        my $row = @{$dbt->getData($sql)}[0];
    
        if ($row->{'cnt'} > 0)    {
            showStrat($q, $s, $dbt);
            return;
        } else  {
            displayQueryPage($q, $s, $dbt);
            print "<center><table><tr><th><font color=\"red\">Sorry, </font><font color=\"blue\"><i>$strat</i></font><font color=\"red\"> is not in the database</font></th></tr></table></CENTER><BR><BR>";
            return;
        }
    }
}
#--------------------------TAXON LIST BUILDER------------------------------------

sub buildList    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $splist=shift;
    my @splist = @$splist;
    print "<DIV CLASS=\"title\">Confidence interval taxon list</DIV><BR>";
    print "<BODY><CENTER><TABLE CELLPADDING=5 BORDER=0>";
    print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"databaseCheckForm\">";
    print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"".$q->param('input_type')."\">";
    my $i = 0;
    foreach my $speciesname (@splist)   {
        print "<TR><TH ALIGN=\"right\"><INPUT TYPE=checkbox NAME=keepspecies$i VALUE=yes CHECKED=checked></TH><TD ALIGN=\"left\"><i>$speciesname</i><INPUT TYPE=hidden NAME=\"speciesname$i\" VALUE=\"$speciesname\"></TD>";
        $i++;
    }     
    print "<TR><TH></TH><TD ALIGN=\"left\"><SPAN CLASS=\"tiny\">(To remove taxon from list, uncheck and press 'Submit')</SPAN></TD></TR>";
    print "<TR><TH ALIGN=\"right\">Add another genus or species to list: </TH><TD ALIGN=\"left\">";
    print "<INPUT TYPE=\"text\" SIZE=\"30\" NAME=\"input\"></TD></TR>";
    print "<TR><TH></TH><TD ALIGN=\"left\"><SPAN CLASS=\"tiny\">(To calculate confidence intervals, leave text box empty and press 'Submit')</SPAN></TD></TR></TABLE><BR>"; 
    print "<INPUT TYPE=\"submit\" VALUE=\"Submit\"><A HREF=\"/cgi-bin/bridge.pl?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\"></A></FORM></CENTER><BR><BR>";

    return;
}
#--------------DISPLAYS TAXA IN STRATIGRAPHIC SECTION FOR EDITING BY USER-------

sub showStrat    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my @splist;
    my $local_sect = $q->param("input");
    my $sql = "SELECT occurrence_no, occurrences.taxon_no, genus_name, species_name, taxon_name, taxon_rank".
              " FROM collections, occurrences ".
              " LEFT JOIN authorities ON occurrences.taxon_no=authorities.taxon_no".
              " WHERE occurrences.collection_no=collections.collection_no ".
              " AND localsection = " . $dbh->quote($local_sect) . " AND localbed !=''".
              " GROUP BY taxon_no,genus_name,species_name ";
    my @strat_taxa_list= @{$dbt->getData($sql)};
    my %taxonList;
    # We build a comma separated list of taxon_nos to pass in. If the taxon_resolution is species,
    # the list will always have one taxon_no in it, if its genus, it may have more. If theres no
    # taxon_no, use the genus+species name
    foreach my $row (@strat_taxa_list) {
        if ($row->{'taxon_no'}) {
            if ($q->param('taxon_resolution') eq 'genus') {
                $taxonList{$row->{'genus_name'}} .= $row->{'taxon_no'}.",";
            } else { #species resolution
                $taxonList{$row->{'genus_name'}.' '.$row->{'species_name'}} .= $row->{'taxon_no'}.",";
            }
        } else {
            if ($q->param('taxon_resolution') eq 'genus') {
                $taxonList{$row->{'genus_name'}} .= $row->{'genus_name'}.",";
            } else { #species resolution
                $taxonList{$row->{'genus_name'}.' '.$row->{'species_name'}} .= $row->{'genus_name'}.' '.$row->{'species_name'}.",";
            }
        }
    }

    print "<DIV CLASS=\"title\">Stratigraphic section taxon list</DIV><BR>";
    print "<BODY><CENTER><TABLE CELLPADDING=5 BORDER=0>";
    print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"showOptionsForm\"><INPUT TYPE=\"hidden\" NAME=\"input\" VALUE=\"$local_sect\"><INPUT TYPE=\"hidden\" NAME=\"taxon_resolution\" VALUE=\"".$q->param("taxon_resolution")."\">";
    my $type = 1;
    my @sortList = sort alphabetically keys(%taxonList);
    my $columns = int(scalar(@sortList)/3)+1;
    for($i=0;$i<$columns;$i++) {
        print "<TR>";
        for($j=$i;$j<scalar(@sortList);$j=$j+$columns) {
            $taxonList{$sortList[$j]} =~ s/,$//; 
            print "<TD><INPUT TYPE=checkbox NAME=keepstrat$j VALUE=yes CHECKED=checked>" . 
                  "<i>".$sortList[$j] . "</i><INPUT TYPE=hidden NAME=\"speciesname$j\" VALUE=\"$taxonList{$sortList[$j]}\"></TD>\n";
        }
        print "</TR>";
    }
    print "</CENTER></TABLE><BR>";
    print "<CENTER><SPAN CLASS=\"tiny\">(To remove taxon from list for analysis, uncheck before pressing 'Submit')</SPAN><BR><BR>";
    print "<INPUT TYPE=\"submit\" VALUE=\"Submit\"><A HREF=\"/cgi-bin/bridge.pl?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\"></A></CENTER><BR><BR></FORM>";
    
    return;
}
#---------------OPTIONS FORM FOR CALCULATING CONFIDENCE INTERVALS---------------

sub optionsForm    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $splist=shift;
    my @splist = @$splist;
    my $local_sect = $q->param("input");
    my @list;
    my $type;
    if (scalar(@splist) > 0)   {
        $type = 0;
        @list = @splist;
    } else  {
        $type = 1;
# -----------------REMAKE STRAT LIST-----------(REMOVES UNCHECKED)-----------
        my $i=0;
        my @testspe =0;
        my $testyes =0;
    
        while ($q->param("speciesname$i"))  {
            if ($q->param("keepstrat$i") eq "yes")   {
                push @list, $q->param("speciesname$i");
                $testyes++;
            }
            $testspe++;
            $i++;
        }    
    }
#----------------------BUILD LIST OF SCALES TO CHOOSE FROM--------------------
    my $sql = "SELECT authorizer_no,scale_no,scale_name,reference_no FROM scales";
    my @results = @{$dbt->getData($sql)};
    my %scale_strings;
    for my $scaleref (@results)     {
        my $option;
        if ($scaleref->{scale_no} == 6)	{
            $option = "<option VALUE=\"" . $scaleref->{scale_no} . "\" selected>";
        } else {
            $option = "<option VALUE=\"" . $scaleref->{scale_no} . "\">";
        }
        my $name = $scaleref->{scale_name};
        my $sql2 = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $scaleref->{reference_no};
                my @results2 = @{$dbt->getData($sql2)};
                my $auth = $results2[0]->{author1last};
                if ( $results2[0]->{otherauthors} || $results2[0]->{author2last} eq "et al." )  {
                        $auth .= " et al.";
                } elsif ( $results2[0]->{author2last} ) {
                        $auth .= " and " . $results2[0]->{author2last};
                }
                $auth .= " " . $results2[0]->{pubyr};
                $auth = " [" . $auth . "]\n";
                $scale_strings{$name} = $option . $name . $auth;
        }
        
#------------------------------OPTIONS FORM----------------------------------
    
    print "<DIV CLASS=\"title\">Confidence interval options form</DIV>";
    print "<BODY><CENTER><TABLE CELLPADDING=5 BORDER=0>";
    if ($type == 0)   {
        print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateTaxonomicInterval\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"type\" VALUE=0>";
    } else  {
        print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateStratigraphicInterval\"><INPUT TYPE=\"hidden\" NAME=\"input\" VALUE=\"$local_sect\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"type\" VALUE=1>";
        print "<INPUT TYPE=\"hidden\" NAME=\"taxon_resolution\" VALUE=\"".$q->param("taxon_resolution")."\">";
    }    
    my $i = 0;
    foreach my $speciesname (@list)   {
        print "<INPUT TYPE=hidden NAME=\"speciesname$i\" VALUE=\"$speciesname\"></TD>";
        $i++;
    }     
    if ($type == 0)   {    
        print "<TR><TH ALIGN=\"right\">Time-scale: </TH><TD ALIGN=\"left\"><SELECT NAME=\"scale\">";
        my @sorted = sort keys %scale_strings;
        for my $string (@sorted)        {
            print $scale_strings{$string};
        }       
        print "</SELECT></TD></TR>";
        print "<TR><TH></TH><TD><SPAN CLASS=\"tiny\">(Please select a time-scale that is appropriate for the taxa you have chosen)</SPAN></TD></TR>";
    }
    print "<TR><TH ALIGN=\"right\">Confidence interval method: </TH><TD ALIGN=\"left\"><SELECT NAME=\"conftype\"><OPTION>Strauss and Sadler (1989)<OPTION>Marshall (1994)<OPTION>Solow (1996)</SELECT><A HREF=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">   Help</A></TD></TR>";
    #<OPTION>Marshall (1990)<OPTION>Marshall (1997)<OPTION>Solow (2003)<OPTION>Holland (2003)
#    print "<TR><TH></TH><TD><SPAN CLASS=\"tiny\">(Warning: Know thine assumptions)</SPAN></TD></TR>";
    print "<TR><TH ALIGN=\"right\">Estimate: </TH><TD ALIGN=\"left\"><SELECT NAME=\"conffor\"><OPTION>total duration<OPTION>first appearance<OPTION>last appearance<OPTION>no confidence intervals</SELECT></TD><TR>";
    print "<TR><TH ALIGN=\"right\">Confidence level: </TH><TD ALIGN=\"left\"><SELECT NAME=\"alpha\"><OPTION>0.99<OPTION SELECTED>0.95<OPTION>0.8<OPTION>0.5<OPTION>0.25</SELECT></TD></TR>";
    print "<TR><TH ALIGN=\"right\">Order taxa by: </TH><TD ALIGN=\"left\"><SELECT NAME=\"order\"><OPTION>name<OPTION SELECTED>first appearance<OPTION>last appearance<OPTION>stratigraphic range</SELECT></TD><TR></TABLE><BR>";
    print "<INPUT NAME=\"full\" TYPE=\"submit\" VALUE=\"Submit\"><A HREF=\"/cgi-bin/bridge.pl?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\" STYLE=\"color:red\"></A></FORM></CENTER><BR><BR>";

    return;
}

#----------------------------------CALCULATE-INTERVALS-----------------------------------------

sub calculateTaxaInterval {

    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my $i=0;
    my @list;
    while ($q->param("speciesname$i")) {
        push @list, $q->param("speciesname$i");
        $i++;
    }
    @splist = @list;
    
    
    my $fig_wide = scalar(@list);
    my $scale = $q->param("scale");
    my $C = $q->param("alpha");
#    my $C = 1 - $CC;
    my $conffor = $q->param("conffor");
    my $conftype = $q->param("conftype");

    if (scalar(@list) == 1 && $conftype eq "Solow (1996)")   {

#    my $i = 0;


#    foreach my $speciesname (@list)   {
#        print "<INPUT TYPE=hidden NAME=\"speciesname$i\" VALUE=\"$speciesname\"></TD>";
#        $i++;
#    }     

        optionsForm($q, $s, $dbt, \@splist);
        print "<center><table><tr><th><font color=\"red\">The Solow (1996) method requires more than one taxon</font></th></tr></table></CENTER><br>";
        return;
    }

    my $order = $q->param("order");
    my $local_sect = $q->param("input");
    my %theHash;
    my %allscale;
    my %namescale;
    my @intervalnumber;
    $AILEFT;
    $AITOP;
    $fig_width;
    $fig_lenth;
    $aifig_size = 500;

    $_ = TimeLookup::processScaleLookup($dbh,$dbt,$scale);
    my %timeHash = %{$_};

#        foreach my $keycounter (keys(%timeHash)) {
#            print "$keycounter: $timeHash{$keycounter}";
#            print "<BR>";
#        }
    
    @_ = TimeLookup::findBoundaries($dbh,$dbt);
    my %upperbound = %{$_[0]};
    my %lowerbound = %{$_[1]};
    my %upperboundname = %{$_[2]};
    my %lowerboundname = %{$_[3]};
    
#        foreach my $keycounter (sort numerically keys(%upperbound)) {
#            print "$keycounter: ";
#                print "$upperbound{$keycounter} -- $lowerbound{$keycounter}";
#            print "<BR>";
#        }

    my $sql = "SELECT interval_no, next_interval_no, lower_boundary FROM correlations WHERE scale_no = " . $scale . " "; # Get all the necessary stuff to create a scale
    my @results = @{$dbt->getData($sql)};
    foreach my $counter (@results) {
        $allscale{$counter->{interval_no}} = [$counter->{next_interval_no}, $counter->{lower_boundary}];
        push @intervalnumber, $counter->{interval_no};
    }
    my $sql = "SELECT eml_interval, interval_no, interval_name FROM intervals WHERE interval_no IN (" . join(',',keys(%allscale)) . ")";
    my @results = @{$dbt->getData($sql)};
    
    foreach my $counter (@results)	{
        my $temp = $counter->{interval_name};
        if ($eml_interval ne "")	{
            $temp = $counter->{eml_interval} . " " . $counter->{interval_name};
        }
        $namescale{$counter->{interval_no}} = $temp;
    }

    my %solowHash;
    my %masterHash;
    foreach my $keycounter (keys(%namescale)) {
        push @{$masterHash{$keycounter}}, $namescale{$keycounter};
        push @{$masterHash{$keycounter}}, $upperbound{$keycounter};
        push @{$masterHash{$keycounter}}, $lowerbound{$keycounter};
    }
#        foreach my $keycounter (sort numerically keys(%masterHash)) {
#            print "$keycounter: ";
#            foreach my $arrcounter (@{$masterHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }

# ----------------------------------------------------------------------------
    my $rusty = 1;
    foreach my $tryout (@list) {
        my @maxintervals;
        my @subsetinterval;
        my @nextinterval;
        my @lowerbound;
        my %taxaHash;
        my $sql;
# ---------------------START TAXON CHECKER----------------------------------
        my @taxon = split(" ",$tryout);

        if (scalar(@taxon) == 2)        {
            $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
            $sql .= " AND species_name=" . $dbh->quote($taxon[1]);
        } elsif (scalar(@taxon) ==1)      {
            $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
        } 
        my @col_nos = @{$dbt->getData($sql)};
        my @collnums;
        for my $col_n (@col_nos)        {
            push @collnums, $col_n->{collection_no};
        }    
        #All the collection numbers containing a certain taxon
        #need to find max_inteval for each collection in chosen scale.
        my %anotherHash;
        my $count;
        foreach my $counter (keys(%timeHash)) {
            foreach my $arraycounter (@collnums) {
                if ($arraycounter == $counter) {
                    push @{$anotherHash{$timeHash{$counter}}}, $arraycounter;
                    $count++;
                } 
            }
        }
        
#        foreach my $keycounter (sort numerically keys(%anotherHash)) {
#            print "$keycounter: ";
##            foreach my $arrcounter (@{$anotherHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }

        foreach my $keycounter (keys(%namescale)) {
            push @{$masterHash{$keycounter}}, scalar(@{$anotherHash{$namescale{$keycounter}}});
        }
        
        
#        print "$count<BR>";
        
        #------------FIND FIRST AND LAST OCCURRENCES---------------
        my $firstint;
        my $lastint;
        my @gaplist;
        my $temp = 0;
        my $temptime;
        my $m = 0;
        
        foreach my $keycounter (sort numerically keys(%masterHash)) {
            if (@{$masterHash{$keycounter}}[$rusty + 2] ne "" && $temp == 0) {
                $lastint = $keycounter;
                $temptime = @{$masterHash{$keycounter}}[2];
                $temp = 1;
            }
            if (@{$masterHash{$keycounter}}[$rusty + 2] ne "" && $temp == 1) {
                $firstint = $keycounter;
                push @gaplist, sprintf("%.1f", @{$masterHash{$keycounter}}[2] - $temptime);  #round to 1 decimal precision
                $temptime = @{$masterHash{$keycounter}}[2];
                $m++;
            }
        }
        
        my $first = @{$masterHash{$firstint}}[2];
        my $last = @{$masterHash{$lastint}}[1];
        my $lastbottom = @{$masterHash{$lastint}}[2];
        my $intervallength = $first - $last;
        my $N = scalar(@gaplist);

        @gaplist = @gaplist[1 .. (scalar(@gaplist) - 1)];
        transpositionTest(\@gaplist, $N);   # TRANSPOSITION TEST
        @gaplist = sort numerically @gaplist;
        
#        foreach my $gapcounter (@gaplist) {
#            print "gaplist: $gapcounter<BR>";
#        }
 
            if ($conftype eq "Strauss and Sadler (1989)") {
                straussSadler($count,$C,$conffor,$intervallength);
            }
            if ($conftype eq "Marshall (1994)") {
                distributionFree(\@gaplist,$N,$C);
            }
            
            if ($conftype eq "Solow (1996)") {  
                $solowHash{$tryout} = [$m,$last,$first];
            }

            my $upper = $last;
            my $lower = $first;
        
            if ($conffor eq "last appearance" || $conffor eq "total duration")	{
                $upper = $last - $lastconfidencelengthlong;
                $uppershort = $last - $lastconfidencelengthshort;
                if ($upper > $uppershort) {$upper = $uppershort}
                if ($upper < 0) {$upper = 0};
                if ($uppershort < 0) {$uppershort = 0};
            }
        
            if ($conffor eq "first appearance" || $conffor eq "total duration")   {
                $lower = $first + $firstconfidencelengthlong;
                $lowershort = $first + $firstconfidencelengthshort;
                if ($lower < $lowershort) {$lower = $lowershort}
            }
        
            $theHash{$tryout} = [$upper, $lower, $first, $last, $firstconfidencelengthlong, $intervallength, $uppershort, $lowershort, $correlation,$N,$firstconfidencelengthshort,$lastconfidencelengthlong,0];
            
#IMPORTANT: UNLIKE OTHER METHODS, CAN"T CALCULATE SOLOW UNTIL FULL LIST IS BUILT

        $rusty++;
    }
    
#    @solowHash{"crania"} = [11,65,390.4];
#    @solowHash{"porites"} = [11,3.55,42];
#    @solowHash{"acropora"} = [6,3.55,57.9];

#        foreach my $keycounter (keys(%solowHash)) {
#            print "$keycounter: ";
#            foreach my $arrcounter (@{$solowHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }

    my @mx;
    foreach my $keycounter (keys(%solowHash)) {
        push @mx, $solowHash{$keycounter}[2];
        push @mx, $solowHash{$keycounter}[1];
    }  #there must be an easier may to find the maximum horizon!!
    @mx = sort numerically @mx;
    my $Smax = $mx[scalar(@mx) - 1];
    my $Smin = $mx[0];

    if ($conftype eq "Solow (1996)") {  
#        print "HERE 1<BR>";
        commonEndPoint(\%solowHash,$C,$conffor);
#        print "HERE 2<BR>";
#        foreach my $keycounter (keys(%solowHash)) {
#            print "$keycounter: ";
#            foreach my $arrcounter (@{$solowHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }
        
#        print "return: FS: $firstsig, FC: $firstconfidencelengthlong, LS: $lastsig, LC: $lastconfidencelengthlong<BR>";
    

#        print "MAX: $Smax, MIN: $Smin<BR>";
    
        foreach my $keycounter (keys(%theHash)) {
            if ($firstconfidencelengthlong != -999) {
                $theHash{$keycounter}[1] = $Smax + $firstconfidencelengthlong;
                $theHash{$keycounter}[4] = $firstconfidencelengthlong;
#                    print "HERE 3<BR>";
            }
            if ($lastconfidencelengthlong != -999) {
                $theHash{$keycounter}[0] = $Smin - $lastconfidencelengthlong;
                $theHash{$keycounter}[11] = $lastconfidencelengthlong;
#                    print "HERE 4<BR>";
            }        
        }
#        print "HERE 5<BR>";
    }
#            $theHash{$tryout} = [$upper, $lower, $first, $last, $firstconfidencelengthlong, $intervallength, $uppershort, $lowershort, $correlation,$N,$firstconfidencelengthshort,$lastconfidencelengthlong,0];


#        foreach my $keycounter (keys(%theHash)) {
#            print "$keycounter: ";
#            foreach my $arrcounter (@{$theHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }
            
                

#        foreach my $keycounter (sort numerically keys(%masterHash)) {
#            print "$keycounter: ";
#            foreach my $arrcounter (@{$masterHash{$keycounter}}) {
#                print "$arrcounter, ";
#            }
#            print "<BR>";
#        }

#print "Chi: " . chiSquaredDensity(4,0.05,0);    
       
#--------------------------------GD-------------------------------------------------------------
    $fig_width = 130 + (16 * $fig_wide);
    $fig_lenth = 150 + 400;
    $AILEFT = 0;
    $AITOP = 580;   
    my $imagenamejpg = "confimagejpg.jpg";
    my $imagenamepng = "confimagepng.png";
    my $imagenameai = "confimageai.ai";
    my $imagedir = $ENV{BRIDGE_HTML_DIR} . "/public/confidence";
    open(IMAGEJ, ">$imagedir/$imagenamejpg");
    open(IMAGEP, ">$imagedir/$imagenamepng");
    open(AI,">$imagedir/$imagenameai");
    open(AIHEAD,"<./data/AI.header");
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;
    my $gd = GD::Image->new($fig_width,$fig_lenth);       
    my $poly = GD::Polygon->new();    
    my $white  = $gd->colorAllocate(255, 255, 255);
    my $aiwhite = "0.00 0.00 0.00 0.00 K"; 
    my $grey= $gd->colorAllocate(  192,   192,   192);
    my $aigrey = "0.23 0.16 0.13 0.02 K";
    my $black  = $gd->colorAllocate(  0,   0,   0);
    my $aiblack = "0.00 0.00 0.00 1.00 K";
#    $gd->rectangle(0,0,$fig_width - 1,$fig_lenth - 1,$black);
#-------------------------MAKING SCALE----------------
    my $upperlim = 1000;
    my $lowerlim = 0;
    foreach my $count (keys(%theHash)) {
        my $tempupp = 0;
        my $templow = 0;
        foreach my $counter (sort numerically keys(%masterHash))	{ 
            if ($masterHash{$counter}[2] > @{$theHash{$count}}[0] && $tempupp == 0)	{
                $tempupp = $counter - 2;
            }
            if ($masterHash{$counter}[1] > @{$theHash{$count}}[1] && $templow == 0)	{
                $templow = $counter - 1;
            } 
        }
        if ($tempupp < $upperlim) {$upperlim = $tempupp};
        if ($templow > $lowerlim) {$lowerlim = $templow};
    }
    my %periodinclude;
    foreach my $counter ($upperlim..$lowerlim)	{
        $periodinclude{$counter} = $masterHash{$counter}[2];
    }
    
#        foreach my $keycounter (sort numerically keys(%periodinclude)) {
#            print "$keycounter: $periodinclude{$keycounter}";
#            print "<BR>";
#        }

    
    
    my $upperval = $periodinclude{$upperlim};
    my $lowerval = $periodinclude{$lowerlim};    
    my $totalval = $lowerval - $upperval;
    my $millionyr = 400 / $totalval;
    my $marker = 110;
    my $Smarker = 110;
    my $aimarker = 150;
    my $tempn = 0;
    foreach my $key (sort numerically keys(%periodinclude))	{
        my $temp = 130 + (($periodinclude{$key} - $upperval) * $millionyr);
        if (($temp - $tempn) > 25 && $tempn > 0)	{
            $gd->string(gdTinyFont, 10, ((($temp - $tempn)/2) + $tempn - 3) , @{$masterHash{$key}}[0], $black);
            aiText("null",10, ((($temp - $tempn)/2) + $tempn + 3) , @{$masterHash{$key}}[0],$aiblack);       #AI
#            print "distance: " . ($temp - $tempn) . ", name: " . @{$masterHash{$key}}[0] . ", key: " . $key . "<BR>";
        }
        if (($temp - $tempn) > 10)	{
            $gd->string(gdTinyFont, 45, $temp - 3, sprintf("%.1f", $masterHash{$key}[2]), $black);
            aiText("null", $aimarker - 75, $temp + 3, sprintf("%.1f", $masterHash{$key}[2]),$aiblack);       #AI
        }
        $gd->line($marker - 35, $temp, $marker - 30, $temp, $black);
        aiLine($aimarker - 35, $temp, $aimarker - 30, $temp,$aiblack);    #AI
        $tempn =+ $temp;
    }
    $gd->line($marker - 30, 130, $marker - 30, $fig_lenth - 20, $black);
    aiLine($aimarker - 30, 130,$aimarker - 30, 530, $aiblack);    #AI
# -------------------------------SORT OUTPUT----------------------------
    sub sortHashLasti {$theHash{$b}[2] <=> $theHash{$a}[2]};
    sub sortHashFirsti {$theHash{$b}[3] <=> $theHash{$a}[3]};
    sub sortHashLenthi {$theHash{$a}[5] <=> $theHash{$b}[5]};
    my @sortedKeys = keys(%theHash);
    if ($order eq "first appearance")   {
        @sortedKeys = sort sortHashLasti sort sortHashFirsti sort alphabetically (@sortedKeys);
    } elsif ($order eq "last appearance") {
        @sortedKeys = sort sortHashFirsti sort sortHashLasti sort alphabetically (@sortedKeys);
    } elsif ($order eq "name")   {
        @sortedKeys = sort alphabetically (@sortedKeys);
    } else  {
        @sortedKeys = sort sortHashLenthi sort sortHashFirsti sort alphabetically (@sortedKeys);    
    }
#---------------------------BARS---------------------------
    my $dotmarkerfirst;
    my $dotmarkerlast;
    my $dottemp = 0;
    my $barup;
    my $bardn;
    foreach my $something (@sortedKeys) {
        $barup = 130 + (@{$theHash{$something}}[3] - $upperval) * $millionyr;
        $bardn = 130 + (@{$theHash{$something}}[2] - $upperval) * $millionyr;
        
        if ($dottemp == 0) { 
            $dotmarkerfirst = $bardn;
            $dotmarkerlast = $barup;
            $dottemp = 1;
        }
        
#        print "upper:" . @{$theHash{$something}}[0] . "<BR>";
#        print "lower:" . @{$theHash{$something}}[1] . "<BR>";
#        print "first:" . @{$theHash{$something}}[2] . "<BR>";
#        print "last:" . @{$theHash{$something}}[3] . "<BR>";
#        print "uppershort:" . @{$theHash{$something}}[6] . "<BR>";
#        print "lowershort:" . @{$theHash{$something}}[7] . "<BR>";

        my $limup;
        my $limdn;
        my $triangle = 0;
        if (@{$theHash{$something}}[7] == @{$theHash{$something}}[1]) {
            $limup = 130 + (@{$theHash{$something}}[6] - $upperval) * $millionyr;
            $limdn = 130 + (@{$theHash{$something}}[7] - $upperval) * $millionyr;
            $triangle = 1;
        } else {
            $limup = 130 + (@{$theHash{$something}}[0] - $upperval) * $millionyr;
            $limdn = 130 + (@{$theHash{$something}}[1] - $upperval) * $millionyr;
        }
        
        my $limupshort = 130 + (@{$theHash{$something}}[6] - $upperval) * $millionyr;
        my $limdnshort = 130 + (@{$theHash{$something}}[7] - $upperval) * $millionyr;
        
        # Draw confidence bar
        $gd->line($marker,$barup,$marker,$bardn, $black);
        $gd->line($marker + 5,$barup,$marker + 5,$bardn, $black);
        $gd->line($marker,$barup,$marker + 5,$barup, $black);
        $gd->line($marker,$bardn,$marker + 5,$bardn, $black);
        
        aiLine($aimarker,$barup,$aimarker,$bardn, $aiblack);    #AI
        aiLine($aimarker + 5,$barup,$aimarker + 5,$bardn, $aiblack);    #AI
        aiLine($aimarker,$barup,$aimarker + 5,$barup, $aiblack);    #AI
        aiLine($aimarker,$bardn,$aimarker + 5,$bardn, $aiblack);    #AI
        my $recent = 0;
        if ($barup == 50)   {
            $recent = 1;  
        } elsif ($limup <= 50) {
            $recent = 2;
        } else  {
            if (($conffor eq "last appearance" || $conffor eq "total duration") && $conftype ne "Solow (1996)")	{
                $gd->rectangle($marker + 2,$barup,$marker + 3,$limup, $black);
                $gd->line($marker + 0,$limup,$marker + 5,$limup, $black); 
                $gd->line($marker + 0,$limupshort,$marker + 5,$limupshort, $black);
                aiRectangle($aimarker + 2,$barup,$aimarker + 3,$limup, $aiblack);
                aiLine($aimarker + 0,$limup,$aimarker + 5, $limup, $aiblack);
                aiLine($aimarker + 0,$limupshort,$aimarker + 5, $limupshort, $aiblack);
                if ($triangle == 1 && @{$theHash{$something}}[6] > 0) {
                    $gd->line($marker + 1,$limupshort - 1,$marker + 4,$limupshort - 1, $black);
                    $gd->line($marker + 1,$limupshort - 2,$marker + 4,$limupshort - 2, $black);
                    $gd->line($marker + 2,$limupshort - 3,$marker + 3,$limupshort - 3, $black);
                    $gd->line($marker + 2,$limupshort - 4,$marker + 3,$limupshort - 4, $black);
                    aiLine($aimarker + 1,$limupshort - 1,$aimarker + 4,$limupshort - 1, $aiblack);
                    aiLine($aimarker + 1,$limupshort - 2,$aimarker + 4,$limupshort - 2, $aiblack);
                    aiLine($aimarker + 2,$limupshort - 3,$aimarker + 3,$limupshort - 3, $aiblack);
                    aiLine($aimarker + 2,$limupshort - 4,$aimarker + 3,$limupshort - 4, $aiblack);
                }
            }
        }
        if (($conffor eq "first appearance" || $conffor eq "total duration") && $conftype ne "Solow (1996)")   {
            $gd->rectangle($marker + 2,$bardn,$marker + 3,$limdn, $black);
            $gd->line($marker + 0,$limdn,$marker + 5,$limdn, $black); 
            $gd->line($marker + 0,$limdnshort,$marker + 5,$limdnshort, $black); 
            aiRectangle($aimarker + 2,$bardn,$aimarker + 3,$limdn, $aiblack);
            aiLine($aimarker + 0,$limdn,$aimarker + 5, $limdn, $aiblack);
            aiLine($aimarker + 0,$limdnshort,$aimarker + 5, $limdnshort, $aiblack);
                if ($triangle == 1) {
                    $gd->line($marker + 1,$limdnshort + 1,$marker + 4,$limdnshort + 1, $black);
                    $gd->line($marker + 1,$limdnshort + 2,$marker + 4,$limdnshort + 2, $black);
                    $gd->line($marker + 2,$limdnshort + 3,$marker + 3,$limdnshort + 3, $black);
                    $gd->line($marker + 2,$limdnshort + 4,$marker + 3,$limdnshort + 4, $black);
                    aiLine($aimarker + 1,$limdnshort + 1,$aimarker + 4,$limdnshort + 1, $aiblack);
                    aiLine($aimarker + 1,$limdnshort + 2,$aimarker + 4,$limdnshort + 2, $aiblack);
                    aiLine($aimarker + 2,$limdnshort + 3,$aimarker + 3,$limdnshort + 3, $aiblack);
                    aiLine($aimarker + 2,$limdnshort + 4,$aimarker + 3,$limdnshort + 4, $aiblack);
                }
        }
        $gd->stringUp(gdSmallFont, $marker-5, 100, "$something", $black);
        if (@{$theHash{$something}}[8] == 1) {
            $gd->stringUp(gdTinyFont, $marker-1, 106, "*", $black);
            aiTextVert(       "null", $aimarker-1,106, "*", $aiblack);
        }
        $gd->string(gdSmallFont, 50, 100, 'Ma', $black);
        $gd->string(gdTinyFont, $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $black);
        aiTextVert(        "null", $aimarker+7, 100, "$something", $aiblack);      #AI
        $marker = $marker + 16;
        $aimarker = $aimarker + 16;
    }
    
    my $center = ((($marker - 11) - $Smarker) / 2) + $Smarker;
    
    if ($conftype eq "Solow (1996)" && $lastconfidencelengthlong != -999) {
        $color = $black;
        for (my $counter = $Smarker; $counter <= ($marker - 11); $counter=$counter + 2) {
            $gd->line($counter,130 + ($Smin - $upperval) * $millionyr,$counter,130 + ($Smin - $upperval) * $millionyr, $black);
        }
        my $temptemp = (130 + ($Smax - $upperval) * $millionyr);
#        print "barup: $temptemp <BR>";
        my $temptemp = (130 + ($Smin - $upperval) * $millionyr);
#        print "barup: $temptemp <BR>";       

#        print "dotmarkerlast: $dotmarkerlast <BR>";              
        for (my $counter = (130 + ($Smin - $upperval) * $millionyr); $counter <= $dotmarkerlast; $counter=$counter + 2) {
            $gd->line($Smarker, $counter,$Smarker,$counter, $black);
#        print "counter: $counter <BR>";

        }
        
        for (my $counter = (130 + ($Smin - $upperval) * $millionyr); $counter <= $barup; $counter=$counter + 2) {
            $gd->line($marker - 11, $counter,$marker - 11,$counter, $black);
#        print "counter: $counter <BR>";

        }
        my $conftemp = $lastconfidencelengthlong;
#        print "lastconfidencelengthlong: $lastconfidencelengthlong <BR>";
        
        if (($Smin - $lastconfidencelengthlong) < 0) {
            $conftemp = $Smin;
            $gd->line($center -2,130 + 0,$center + 3,130 +0, $black);
            $gd->line($center -1,130 -1,$center + 2,130 -1, $black);
            $gd->line($center -1,130 -2,$center + 2,130 -2, $black);
            $gd->line($center ,130 -3,$center + 1,130 -3, $black);
            $gd->line($center ,130 -4,$center + 1,130 -4, $black);  
        } else {
#        print "conftemp: $conftemp <BR>";
            $gd->line($Smarker,(130 + ($Smin - $conftemp - $upperval) * $millionyr),$marker - 11,(130 + ($Smin - $conftemp - $upperval) * $millionyr), $black); 
        }
        $gd->rectangle($center , (130 + ($Smin - $conftemp - $upperval) * $millionyr), $center + 1,(130 + ($Smin - $upperval) * $millionyr), $black);
        

        
    }
    

    
    if ($conftype eq "Solow (1996)" && $firstconfidencelengthlong != -999) {
        $color = $black;
        for (my $counter = $Smarker; $counter <= ($marker - 11); $counter=$counter + 2) {
            $gd->line($counter,130 + ($Smax - $upperval) * $millionyr,$counter,130 + ($Smax - $upperval) * $millionyr, $black);
        }
        for (my $counter = $dotmarkerfirst; $counter <= (130 + ($Smax - $upperval) * $millionyr); $counter=$counter + 2) {
            $gd->line($Smarker, $counter,$Smarker,$counter, $black);
#        print "counter: $counter <BR>";

        }
        for (my $counter = $bardn; $counter <= (130 + ($Smax - $upperval) * $millionyr); $counter=$counter + 2) {
            $gd->line($marker - 11, $counter,$marker - 11,$counter, $black);
#        print "counter: $counter <BR>";

        }
        $gd->rectangle($center , (130 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr), $center + 1,(130 + ($Smax - $upperval) * $millionyr), $black);
        $gd->line($Smarker,(130 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr),$marker - 11,(130 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr), $black); 

    }

    aiText("null", 90, 100, 'Ma', $aiblack);                                     #AI
    aiText("null", $fig_width - 50,$fig_lenth - 0, "J. Madin 2004", $aiblack);      #AI
    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>) {print AI $_};
    close AIFOOT;
    print IMAGEJ $gd->jpeg;
    print IMAGEP $gd->png;
    close IMAGEJ;
    close IMAGEP;
# ---------------------------------RESULTS-PAGE----------------------------------------
    print "<DIV CLASS=\"title\">Confidence interval results</DIV>";
    print "<FORM METHOD=\"post\" ACTION=\"bridge.pl\">";
    print "<INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"displayFirstForm\">";

    print "<CENTER><A HREF=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">Help</A></CENTER>";
    
    if ($recent == 1)    {
        print "<center><table><tr><th><font color=\"red\">
            This taxon may not be extinct</font></th></tr></table></CENTER><BR><BR>";
    }
    if ($recent == 2)    {
        print "<center><table><tr><th><font color=\"red\">
            The upper confidence interval extends into the future and is therefore unreliable
            </font></th></tr></table></CENTER><BR><BR>";
    }
    
    print "<CENTER><TABLE><TD><IMG SRC=\"/public/confidence/$imagenamepng\"></TD><TD WIDTH=40></TD>";
  
    print "<TD>";
    if($conftype eq "Strauss and Sadler (1989)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('first occurrence','last occurrence (Ma)','confidence interval (Ma)', 'number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[2],$confVals[3],$confVals[4],$confVals[9],$confVals[8]];
        }
        printResultTable('',\@tableRowHeader,\@tableColHeader,\@table);
    } elsif($conftype eq "Marshall (1994)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('first occurrence','last occurrence (Ma)','lower confidence interval (Ma)', 'upper confidence interval (Ma)','number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[2],$confVals[3],$confVals[10],$confVals[4],$confVals[9],$confVals[8]];
        }
        printResultTable('',\@tableRowHeader,\@tableColHeader,\@table);
    } elsif($conftype eq "Solow (1996)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('first occurrence','last occurrence (Ma)','number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[2],$confVals[3],$confVals[9],$confVals[8]];
        }
        printResultTable('table 1',\@tableRowHeader,\@tableColHeader,\@table);
        print "<BR><BR>";
        
        my $temp1;
        my $temp2;
        if ($firstconfidencelengthlong == -999) {
            $temp1 = "NA";
        } else {$temp1 = sprintf("%.3f", $firstconfidencelengthlong)}
        
        if ($lastconfidencelengthlong == -999) {
            $temp2 = "NA";
        } else {$temp2 = sprintf("%.3f", $lastconfidencelengthlong)}
      
        # print table 2 
        print "<TABLE CELLSPACING=1 BGCOLOR=\"black\" CELLPADDING=5><TR BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD>table 2</TD><TD WIH=70><B>significance level</B></TD><TD WIH=70><B>confidence limit (Ma)</B></TD></TR>";
        if ($firstsig != -999) {
        print "<TR  BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD WIH=70><B>common first occurrence</B></TD><TD>$firstsig</TD><TD>$temp1</TD></TR>";
        }
        if ($lastsig != -999) {
        print "<TR  BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD WIH=70><B>common last occurrence</B></TD><TD>$lastsig</TD><TD>$temp2</TD></TR>";
        }
        print "</TABLE>";
    }
    print "</TD></TR>";
        
    print "</TABLE></TD>";

    print "</TABLE></CENTER><BR><BR>";
    
    print "<CENTER><b>Download figure as: <a href=\"/public/confidence/$imagenamepng\" TARGET=\"xy\">PNG</a>";
    print ", <a href=\"/public/confidence/$imagenamejpg\" TARGET=\"xy\">JPEG</a>";
    print ", <a href=\"/public/confidence/$imagenameai\">AI</a><BR><BR></b>";
    print "<INPUT TYPE=submit VALUE=\"Start again\"><BR><BR><BR>";

    return;
} #End Subroutine CalculateIntervals

# Used in CalculateTaxaInterval, print HTML table
sub printResultTable { 
    $tableName = $_[0];
    @tableRowHeader = @{$_[1]};
    @tableColHeader = @{$_[2]};
    @table = @{$_[3]};

    # RESULTS TABLE HEADER
    print "<TABLE CELLSPACING=1 BGCOLOR=\"black\" CELLPADDING=5><TR BGCOLOR=\"white\" ALIGN=\"CENTER\">";
    print "<TD BGCOLOR=\"white\" ALIGN=\"CENTER\">$tableName</TD>";
    foreach $col (@tableColHeader) { 
        print "<TD BGCOLOR=\"white\" ALIGN=\"center\"><I><B>$col</B></I></TD>";
    }    
    print "</TR>";
    # RESULTS TABLE BODY
    for(my $rowNum=0;$rowNum<scalar(@tableRowHeader);$rowNum++){
        print "<TR><TD BGCOLOR=\"white\" ALIGN=\"center\"><B>$tableRowHeader[$rowNum]</B></TD>";
        for(my $colNum=0;$colNum<scalar(@tableColHeader);$colNum++){
            print "<TD BGCOLOR=\"white\" ALING=\"center\">".$table[$colNum][$rowNum]."</TD>";
        }
        print "</TR>";
    }
    print "</TABLE>";
}

#--------------CALCULATE STRATIGRAPHIC RELATIVE CONFIDENCE INTERVALS----------------
sub calculateStratInterval	{
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my $marker = 100;
    $AILEFT;
    $AITOP;
    $fig_width;
    $fig_lenth;
    $aifig_size = 500;
    my $local_sect = $q->param("input");
    my $alpha = $q->param("alpha");
#   $alpha = 1 - $alpha;
    my $conffor = $q->param("conffor");
    my $conftype = $q->param("conftype");
    my $stratres = $q->param("stratres");
    my $order = $q->param("order");

    my ($taxon_nos_string,$genus_species_sql);
    for(my $i=0;$q->param("speciesname$i");$i++) {
        my @taxon_nos = split(",",$q->param("speciesname$i"));
        foreach $taxon_no_or_name (@taxon_nos) {
            if ($taxon_no_or_name =~ /^\d+$/) {
                $taxon_nos_string .= $taxon_no_or_name . ",";
            } else {
                my ($genus,$species) = split(/ /,$taxon_no_or_name);
                if ($genus) {
                    $genus_species_sql .= " OR (occurrences.genus_name=".$dbh->quote($genus);
                    if ($species) {
                        $genus_species_sql .= "AND occurrences.species_name=".$dbh->quote($species);
                    }
                    $genus_species_sql .= ")";
                }
            }
        }
    }
    if ($taxon_nos_string) { $taxon_nos_string =~ s/,$//;}
    else {$taxon_nos_string='-1'};

    my $sql = "SELECT localbed, localorder, occurrences.collection_no, genus_name, species_name FROM collections, occurrences". 
              " WHERE collections.collection_no=occurrences.collection_no".
              " AND (taxon_no IN ($taxon_nos_string) $genus_species_sql)".
              " AND localsection=".$dbh->quote($local_sect);
              " AND localbed != ''";
    main::dbg("sql to get beds from species list: $sql");
    my @beds_and_colls = @{$dbt->getData($sql)};
    foreach my $row (@beds_and_colls) {
        my $genus_species;
        if ($q->param('taxon_resolution') eq 'genus') {
            $genus_species = $row->{'genus_name'};
        } else {
            $genus_species = join ' ',$row->{'genus_name'},$row->{'species_name'};
        }
        $localbed{$row->{'collection_no'}}=$row->{'localbed'};
        push @{$mainHash{$genus_species}}, $row->{'localbed'};
    }


# ----------------------------GENERAL FIGURE DIMENSIONS---------------------------
    my @tempp = sort numerically values %localbed;                        
    main::dbg("sorted localbed values:".Dumper(\@tempp));
    main::dbg("mainHash (genus->beds array):".Dumper(\%mainHash));
    my $number_horizons = scalar(@tempp);            # how many horizons for whole section
    my $maxhorizon = $tempp[$number_horizons-1];     # the maximum horizon number, e.g., 17 (+1, for upper bound)
    my $minhorizon = $tempp[0] - 1;                      # thw minimum horizon number, e.g., 3
# -------------------CALCULATE CONFIEDENCE INTS------------------------------
    my %stratHash;
    my $upper_lim = $maxhorizon;
    my $lower_lim = $minhorizon;
    my $upper_horizon_lim = $upper_lim;
    my $lower_horizon_lim = $lower_lim;
# ---------------------------STRAUSS AND SADLER----------------------------------
    foreach my $counter (sort alphabetically keys(%mainHash))  {
        my $conf = 0;
        my @array = sort numerically @{$mainHash{$counter}};
        my $count = scalar(@array);         # number of horizons
    #        print "$counter: $count<BR>";
        my $lower = $array[0] - 1;              # lower species horizon, say 6
        my $upper = $array[$count - 1]; # upper species horizon, say 10 (+ 1, for upper bound of interval)
        my $lenth = $upper - $lower;        # total number of horizons for species, therefore 5;
        my $limit = 0;
        # -----------------------------------------
        if ($conffor eq 'last appearance' || $conffor eq 'first appearance' || $conffor eq 'total duration')  {

            if ($count > 2) {                   # make sure that enough horizons for analysis
                my $iterate;
                if ($conffor eq 'last appearance' || $conffor eq 'first appearance')	{    # one tailed
                    $conf = ((1 - $alpha)**(-1/($count - 1))) - 1;
                } else	{                                                        # two tailed
                    foreach my $scounter (1..30000)	{
                        $conf = $scounter/1000;
                        $iterate = (1 - (2 * ((1 + $conf)**( - ($count - 1)) ) ) + ((1 + 2 * $conf)**( - ($count - 1))));
                        if ($iterate > $alpha)	{
                            last;
                        }
                    }
                }
            }
        }
        # -----------------------------------------
        $limit = ($lenth * $conf)/$lenth;   # length of conf interval as number of horizons
        $upper_horizon_lim = $upper + $limit;
        $lower_horizon_lim = $lower - $limit;
        if ($upper_horizon_lim > $upper_lim)   {$upper_lim = $upper_horizon_lim;}
        if ($lower_horizon_lim < $lower_lim)   {$lower_lim = $lower_horizon_lim;}
        $stratHash{$counter} = [$lower, $upper, $limit, $lenth];  # fill in Hash array with necessary info
    }
# ----------------------------THE MAX AND MIN CONFIDENCE RANGES-------------------

    if ($conffor eq "last appearance" || $conffor eq "total duration")	{
        $upper_lim = (int $upper_lim) + 1;
    } else {
        $upper_lim = $maxhorizon;
    }
    if ($conffor eq "first appearance" || $conffor eq "total duration")	{    
        if ($lower_lim < 0) {
            $lower_lim = (int $lower_lim) - 3;
        } else  {
            $lower_lim = (int $lower_lim) - 2;
        }
    } else  {
        $lower_lim = $minhorizon - 3;
    }    
    my $fig_wide = scalar(keys(%mainHash));
    my $fig_long = $upper_lim - $lower_lim;
    my $horizon_unit = 400/$fig_long;
    my $lateral_unit = 16;
    $fig_width = 120 + ($lateral_unit * $fig_wide);
    $fig_lenth = 250 + 400;#($horizon_unit * $fig_long);
    $AILEFT = 0;
    $AITOP = 580;    
    my $image_map = "<map name='ConfidenceMap'>";
# ------------------------------------GD------------------------
    my $imagenamejpg = "confimagejpg.jpg";
    my $imagenamepng = "confimagepng.png";
    my $imagenameai = "confimageai.ai";
    my $imagedir = $ENV{BRIDGE_HTML_DIR} . "/public/confidence";
    open(IMAGEJ, ">$imagedir/$imagenamejpg");
    open(IMAGEP, ">$imagedir/$imagenamepng");
    open(AI,">$imagedir/$imagenameai");
    open(AIHEAD,"<./data/AI.header");
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;
    my $gd = GD::Image->new($fig_width,$fig_lenth);   
    my $poly = GD::Polygon->new();    
    my $white  = $gd->colorAllocate(255, 255, 255);
    my $aiwhite = "0.00 0.00 0.00 0.00 K"; 
    my $grey= $gd->colorAllocate(  192,   192,   192);
    my $aigrey = "0.23 0.16 0.13 0.02 K";
    my $black  = $gd->colorAllocate(  0,   0,   0);
    my $aiblack = "0.00 0.00 0.00 1.00 K";
    $gd->rectangle(0,0,$fig_width-1,$fig_lenth - 1,$black);
# ---------------------------------SCALE BAR---------------------
    my $i = 0;
    my $j = 0;
    print AI "u\n";                                                     # AI start the group 
    foreach my $counter (($lower_lim)..($upper_lim+1))  {
        if ($i > $j)    {
            $gd->line(65,($fig_lenth - 20) - $i,70,($fig_lenth - 20) - $i,$black);   #GD
            $gd->string(gdTinyFont,55-length($counter)*5,($fig_lenth - 20) - ($i) - 4,$counter,$black);      #GD
            aiLine(65,($fig_lenth - 20) - $i,70,($fig_lenth - 20) - $i,$aiblack);    #AI
            aiText("null",55-length($counter)*6,(($fig_lenth - 20) - $i) + 2,$counter,$aiblack);       #AI
            if ($counter > $minhorizon && $counter <= $maxhorizon) {
                $image_map .= "<area shape=rect coords=\"".(55-length($counter)*6).",".int($fig_lenth - $i - 15).",55,".int($fig_lenth - $i - 30)."\" HREF=\"bridge.pl?action=displayStrataSearch&localsection=$local_sect&localbed=$counter\" ALT=\"local bed $counter of $local_sect\">";
            }
            $j = $j + 8;
        }
        if ($counter == $maxhorizon || $counter == $minhorizon) {
            $gd->dashedLine(70,($fig_lenth - 20) - $i,$fig_width - 20,($fig_lenth - 20) - $i,$black);   #GD
            aiLineDash(70,($fig_lenth - 20) - $i,$fig_width - 20,($fig_lenth - 20) - $i,$aiblack);      #AI
        }
        $i = $i + $horizon_unit;
    }
    print AI "U\n";                                                     # AI terminate the group 
    $gd->line(70,$fig_lenth - 20 - $horizon_unit,70,$fig_lenth - (($fig_long + 1)*$horizon_unit) - 20,$black);   #GD    
    $gd->stringUp(gdMediumBoldFont, 13,(250 + (($fig_lenth - 220)/2)), "Section: $local_sect", $black);
    #$image_map .= "<area shape=rect coords=\"12,".int(260 + ($fig_lenth - 220)/2).",28,".int(260 + ($fig_lenth-380)/2-length($local_sect)*7)."\" HREF=\"bridge.pl?action=displayStrataSearch&localsection=$local_sect\" ALT=\"section $local_sect\">";
    $gd->string(gdTinyFont, $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $black);
    aiLine(70,$fig_lenth - 20 - $horizon_unit,70,$fig_lenth - (($fig_long + 1)*$horizon_unit) - 20,$aiblack);   #AI    
    aiTextVert("null", 13,(250 + (($fig_lenth - 220)/2)), "Section: $local_sect", $aiblack);
    aiText("null", $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $aiblack);    
# -------------------------------SORT OUTPUT----------------------------
    sub sortHashLast {$stratHash{$a}[0] <=> $stratHash{$b}[0]};
    sub sortHashFirst {$stratHash{$a}[1] <=> $stratHash{$b}[1]};
    sub sortHashLenth {$stratHash{$a}[3] <=> $stratHash{$b}[3]};
            
    my @sortedKeys = keys(%stratHash);
    if ($order eq "first appearance")   {
        @sortedKeys = sort sortHashLast sort sortHashFirst sort alphabetically (@sortedKeys);
    } elsif ($order eq "last appearance") {
        @sortedKeys = sort sortHashFirst sort sortHashLast sort alphabetically (@sortedKeys);
    } elsif ($order eq "name")   {
        @sortedKeys = sort alphabetically (@sortedKeys);
    } else  {
        @sortedKeys = sort sortHashLenth sort sortHashFirst sort alphabetically (@sortedKeys);    
    }
# -------------------------------SPECIES BARS----------------------------
    foreach my $counter (@sortedKeys) {
        # -----------------GREY BOXES IN BAR (PS)--------------------------
        my @localbeds = @{$mainHash{$counter}};
        my %seenBeds = ();
        foreach $bed (@localbeds) {
            if (!$seenBeds{$bed}) {
                $gd->filledRectangle($marker+1,$fig_lenth-20-(($bed-$lower_lim)*$horizon_unit),$marker+4,$fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit),$grey);
                aiFilledRectangle($marker+1,$fig_lenth-20-(($bed-$lower_lim)*$horizon_unit),$marker+4,$fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit),$aigrey);
                $image_map .= "<area shape=rect coords=\"".$marker.",".int($fig_lenth-20-(($bed-$lower_lim)*$horizon_unit)).",".($marker+5).",".int($fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit))."\" HREF=\"bridge.pl?action=displayCollResults&localsection=$local_sect&localbed=$bed&genus_name=$counter\" ALT=\"$counter in local bed $bed\">";
            }
            $seenBeds{$bed} = 1;
        }
        $gd->rectangle($marker, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), $marker + 5, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit) , $black);
        aiRectangle(   $marker, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), $marker + 5, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit) , $aiblack);
# -----------------CONFIDENCE BARS--------------------------
        if ($conffor eq "first appearance" || $conffor eq "total duration")	{
            $color = $black;
            $gd->rectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[0] - $stratHash{$counter}[2])) * $horizon_unit), $color);
            aiRectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[0] - $stratHash{$counter}[2])) * $horizon_unit), $aicolor);            
        }
        if ($conffor eq "last appearance" || $conffor eq "total duration")   {
            $color = $black;
            $gd->rectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[1] + $stratHash{$counter}[2])) * $horizon_unit), $color);
            aiRectangle((   $marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[1] + $stratHash{$counter}[2])) * $horizon_unit), $aicolor);
        }    
        $gd->stringUp(gdSmallFont, $marker-5, 200, "$counter", $black);
        aiTextVert(        "null", $marker+7, 200, "$counter", $aiblack);
        $image_map .= "<area shape=rect coords=\"".($marker-5).",205,".($marker+7).",".(200-length($counter)*6)."\" HREF=\"bridge.pl?action=checkTaxonInfo&taxon_name=$counter\" ALT=\"$counter\">";

        
        $marker = $marker + $lateral_unit;
    }

# ------------------------------------MAKE FIGURE------------------------------
    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>) {print AI $_};
    close AIFOOT;
    print IMAGEJ $gd->jpeg;
    print IMAGEP $gd->png;
    close IMAGEJ;
    close IMAGEP;
    $image_map .= "</map>";
# ---------------------------------RESULTS-PAGE----------------------------------------
    print "<DIV CLASS=\"title\">Confidence interval results for the <i>$local_sect</i> stratigraphic section</DIV><BR>";
    print "<FORM METHOD=\"post\" ACTION=\"bridge.pl\">";
    print "<INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"displayFirstForm\">";
    #if ($fig_width > 750)  {
    #   print "<CENTER><IMG WIDTH=750 SRC=\"/public/confidence/$imagenamepng\"></CENTER><BR><BR>";
    #} else {
    print $image_map;
    print "<CENTER><IMG SRC=\"/public/confidence/$imagenamepng\" USEMAP=\"#ConfidenceMap\" ISMAP BORDER=0></CENTER><BR><BR>";
    #}
    print "<CENTER><b>Download figure as: <a href=\"/public/confidence/$imagenamepng\" TARGET=\"xy\">PNG</a>";
    print ", <a href=\"/public/confidence/$imagenamejpg\" TARGET=\"xy\">JPEG</a>";
    print ", <a href=\"/public/confidence/$imagenameai\">AI</a><BR><BR></b>";
    print "<INPUT TYPE=submit VALUE=\"Start again\"><BR><BR><BR>";

    return;
} 

sub alphabetically {lc($a) cmp lc($b)};
sub numerically {$a <=> $b};

sub aiLine {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    print AI "S\n";
}

sub aiLineDash {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[6 ]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    print AI "S\n";
}

sub aiFilledRectangle {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "0 O\n";
    print AI "$color\n";
    print AI "0 G\n";
    print AI "4 M\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    print AI "f\n";
}

sub aiRectangle {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    print AI "S\n";
}

sub aiTextVert {
    my $font=shift;
    my $x=shift;
    my $y=shift;
    my $text=shift;
    my $color=shift;
    print AI "0 To\n";
    printf AI "0 1 -1 0 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x,$AITOP-$y;
    printf AI "0 Tr\n0 O\n%s\n",$color;
    printf AI "/_Courier %.1f Tf\n",12; 
    printf AI "0 Tw\n";
    print AI "($text) Tx 1 0 Tk\n";
    print AI "(\r) Tx 1 0 Tk\nTO\n";
}

sub aiText {
    my $font=shift;
    my $x=shift;
    my $y=shift;
    my $text=shift;
    my $color=shift;
    print AI "0 To\n";
    printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x,$AITOP-$y;
    printf AI "0 Tr\n0 O\n%s\n",$color;
    printf AI "/_Courier %.1f Tf\n",12; 
    printf AI "0 Tw\n";
    print AI "($text) Tx 1 0 Tk\n";
    print AI "(\r) Tx 1 0 Tk\nTO\n";
}

sub transpositionTest {
    my $gaplist = shift;
    my @gaplist = @$gaplist;
    my $N = shift;
    my $Tmax = ($N * ($N - 1))/2;
    foreach my $counter (1 .. 2) {    
        my $T = 0;
        my $Tequal = 0;
        my @done;
        foreach my $i (0 .. ($N - 2)) {
            my $temp = 1;
            foreach my $j (($i + 1) .. ($N - 1)) {
                if (@gaplist[$j] <  @gaplist[$i]) {
                    $T++;
                }
                my $pmet = 0;
                foreach my $counter (@done) {
                    if ($i == $counter) {
                        $pmet = 1;
                    }
                }
                if ((@gaplist[$i] == @gaplist[$j]) && ($pmet == 0)) {
                    $temp++;
                }
            }    
            push @done, @gaplist[$i];
            $Tequal = $Tequal + ($temp*($temp - 1))/4;
        }
        $Total = $T + $Tequal;
        if($Total > ($Tmax/2)) {
            @gaplist = reverse(@gaplist);
        } else {
            break;
        }
    }
    $correlation = 0;
    #-----------------p(0.95) ONLY------------------
    if ($Total <= ((0.215*($N**2))-(1.15*$N)+0.375)) {
        print "*Significant correlation at alpha = 0.95  <BR>";
        $correlation = 1;
    } else {
#        print "No correlataion  <BR>";
    }
    return $correlation;
}

sub factorial {
    if ($_[0] == 0) {
        1;
    }
    else {
        $_[0] * factorial($_[0] - 1);
    }
}

sub straussSadler {
    my $count = shift;
    my $C = shift;
    my $conffor = shift;
    my $intervallength = shift;
    
    my $alpha = 0;      # calculate intervals Strauss and Saddler (1989)
    my $iterate;
    if ($count > 2) {
        if ($conffor eq 'last appearance' || $conffor eq 'first appearance')	{
            $alpha = ((1 - $C)**(-1/($count - 1))) - 1;
        } else	{
            foreach my $counter (1..3000)	{
                $alpha = $counter/100;
                $iterate = (1 - (2 * ((1 + $alpha)**( - ($count - 1)) ) ) + ((1 + 2 * $alpha)**( - ($count - 1))));
                if ($iterate > $C)	{
                    last;
                }
            }			
        }
    }
    $firstconfidencelengthlong = $intervallength * $alpha;
    $lastconfidencelengthlong = $firstconfidencelengthlong;
    $confidencelengthshort = 0;
    $lastconfidencelengthshort = $firstconfidencelengthshort;
    
    return $firstconfidencelengthlong, $lastconfidencelengthlong, $firstconfidencelengthshort, $lastconfidencelengthshort;          
}

sub distributionFree {          #FOR MARSHALL 1994
    my $gaplist = shift;
    my @gaplist = @$gaplist;
    my $N = shift;
    my $C = shift;
    @gaplist = sort numerically @gaplist;
            
    my $alpha = 0.95;       #STANDARD FOR CONFIDENCE PROBABILITIES OF CONFIDENCE INTERVALS ($C)
    my $gamma = (1 - $alpha)/2;
    my $ll = 0;
    my $uu = 0;
#        print "N: $N<BR>";
    if ($N > 5) {
        foreach my $x (1 .. $N) {
            my $sum = 0;
            foreach my $i (1 .. $x) {
                $Nx = factorial($N) / (factorial($i) * factorial($N - ($i)));
                $sum = $sum + ($Nx * ($C**$i)) * ((1 - $C)**($N - $i));
            }
            if (($sum > $gamma) && ($ll == 0)) {
                $ll = 1;
                $low = $x;
            }
            if (($sum > (1 - $gamma)) && ($uu == 0)) {
                $uu = 1;
                $upp = $x + 1;
            }
        }
#        print "upp: $upp<BR>";
#        print "low: $low<BR>";
        
        if ($upp > $N) {
            $firstconfidencelengthlong = 0;
            $firstconfidencelengthshort = @gaplist[$low - 1];        
        } else {
            $firstconfidencelengthlong = @gaplist[$upp - 1];
            $firstconfidencelengthshort = @gaplist[$low - 1];        
        }
        
    } else {
        $firstconfidencelengthlong = 0;
        $firstconfidencelengthshort = 0;            
    }
    $lastconfidencelengthlong = $firstconfidencelengthlong;
    $lastconfidencelengthshort = $firstconfidencelengthshort;

    return $firstconfidencelengthlong, $lastconfidencelengthlong, $firstconfidencelengthshort, $lastconfidencelengthshort;           
}

sub commonEndPoint {        #FOR SOLOW (1996)
    my $solowHash = shift;
    my %solowHash = %$solowHash;
    my $alpha = shift;
    my $conffor = shift;
    $alpha = 1 - $alpha;
#    my $lastsig;
#    my $firstsig;
#    my $lastconfidencelengthlong;
#    my $firstconfidencelengthlong;
    
    if ($conffor eq 'total duration' || $conffor eq 'first appearance')	{
        my @mx;
        foreach my $keycounter (keys(%solowHash)) {
            push @mx, $solowHash{$keycounter}[2];
        }  #there must be an easier may to find the maximum horizon!!
        @mx = sort numerically @mx;
        my $max = $mx[scalar(@mx) - 1];

        #-------------------------SIGNIFICANCE FINDER-----------------------------

        my $df = 2 * (length(%solowHash) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        foreach my $i (keys(%solowHash)) {
            $lambda = $lambda * ((($solowHash{$i}[2] - $solowHash{$i}[1]) / ($max - $solowHash{$i}[1])) ** ($solowHash{$i}[0] - 1));
        }
#        print "lambda: $lambda<BR>";

        $firstsig = chiSquaredDensity($df,0,-2*log($lambda));

#        print "Significance: $lastsig<BR>";
        #----------------------UPPER CONFIDENCE FINDER-----------------------------
        
        if ($firstsig > 0.05) {
            my $df = 2 * scalar(%solowHash);
            my $tester = chiSquaredDensity($df,$alpha,0);
#            print "tester: $tester<BR>";
            my $j;
            for ($j = $max; $j <= $max + 1000; $j = $j + 0.1) {
                my $Rstore = 1;
                foreach my $i (keys(%solowHash)) {
                    $Rstore = $Rstore * ((($solowHash{$i}[2] - $solowHash{$i}[1])/($j - $solowHash{$i}[1]))**($solowHash{$i}[0] - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {last}
            }
            $firstconfidencelengthlong = sprintf("%.3f", $j - $max);
        } else {$firstconfidencelengthlong = -999}
         
    } else {
        $firstsig = -999;
        $firstconfidencelengthlong = -999;
    }
#    print "first SIG: $firstsig, first CONF: $firstconfidencelengthlong<BR>";

    my %tempsolowHash = %solowHash;
    
    if ($conffor eq 'total duration' || $conffor eq 'last appearance')	{
        foreach my $keycounter (keys(%tempsolowHash)) {
            my $temp = $tempsolowHash{$keycounter}[1];
            $tempsolowHash{$keycounter}[1] = -$tempsolowHash{$keycounter}[2];
            $tempsolowHash{$keycounter}[2] = -$temp;
        }

        my @mx;
        foreach my $keycounter (keys(%tempsolowHash)) {
            push @mx, $tempsolowHash{$keycounter}[2];
        }  #there must be an easier may to find the maximum horizon!!
        @mx = sort numerically @mx;
        my $max = $mx[scalar(@mx) - 1];

        #-------------------------SIGNIFICANCE FINDER-----------------------------

        my $df = 2 * (length(%tempsolowHash) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        foreach my $i (keys(%tempsolowHash)) {
            $lambda = $lambda * ((($tempsolowHash{$i}[2] - $tempsolowHash{$i}[1]) / ($max - $tempsolowHash{$i}[1])) ** ($tempsolowHash{$i}[0] - 1));
        }
#        print "lambda: $lambda<BR>";

        $lastsig = chiSquaredDensity($df,0,-2*log($lambda));

#        print "Significance: $firstsig<BR>";
        #----------------------LOWER CONFIDENCE FINDER-----------------------------
        
        if ($lastsig > 0.05) {
            my $df = 2 * scalar(%tempsolowHash);
            my $tester = chiSquaredDensity($df,$alpha,0);
            my $j;
            for ($j = $max; $j <= $max + 1000; $j = $j + 0.1) {
                my $Rstore = 1;
                foreach my $i (keys(%tempsolowHash)) {
                    $Rstore = $Rstore * ((($tempsolowHash{$i}[2] - $tempsolowHash{$i}[1])/($j - $tempsolowHash{$i}[1]))**($tempsolowHash{$i}[0] - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {last}
            }
            $lastconfidencelengthlong = sprintf("%.3f", $j - $max);
        } else {$lastconfidencelengthlong = -999}
         
    } else {
        $lastsig = -999;
        $lastconfidencelengthlong = -999;
    }
#    print "last SIG: $lastsig, last CONF: $lastconfidencelengthlong<BR>";
    return $firstsig, $firstconfidencelengthlong, $lastsig, $lastconfidencelengthlong;
}

sub chiSquaredDensity { #for calculating both types, either alpha or lower must be zero to work
    my $df = shift;
    my $alpha = shift;      #ONE TAILED
    my $lower = shift;
    
    my $resolution = 0.01;      #WILL GET ACCURACY TO APRROX 3 DECIMAL PLACES
    my $upper = 1000;           #MY SURROGATE FOR INFINITE!!
    my $chi = 0;
    my $x;
    
    if ($lower == 0) {
        for ($x = $upper; $x >= 0; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
            if($chi >= $alpha) {last}
        }
        return sprintf("%.3f", $x);
    } elsif ($alpha == 0 ) {
        for ($x = $upper; $x >= $lower; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
        }
        return sprintf("%.3f", $chi);    
    }
}

sub gamma {             #THE GAMMA FUNCTION
    my $x = shift;
    my ($y1, $res, $z, $i);
    my $sqrtpi = 0.9189385332046727417803297e0;     # log(sqrt(2*pi))
    my $pi     = 3.1415926535897932384626434e0;
    my $xbig   = 171.624e0;
    my $xminin = 2.23e-308;
    my $eps    = 2.22e-16;
    my $xinf   = 1.79e308;
    
#---Numerator and denominator coefficients for rational minimax approximation over (1,2).
    my @P = (-1.71618513886549492533811e+0, 2.47656508055759199108314e+1, -3.79804256470945635097577e+2, 6.29331155312818442661052e+2, 8.66966202790413211295064e+2, -3.14512729688483675254357e+4, -3.61444134186911729807069e+4, 6.64561438202405440627855e+4);
    my @Q = (-3.08402300119738975254353e+1, 3.15350626979604161529144e+2, -1.01515636749021914166146e+3, -3.10777167157231109440444e+3, 2.25381184209801510330112e+4, 4.75584627752788110767815e+3, -1.34659959864969306392456e+5, -1.15132259675553483497211e+5);
#------------Coefficients for minimax approximation over (12, INF).
    my @C = (-1.910444077728e-03, 8.4171387781295e-04, -5.952379913043012e-04, 7.93650793500350248e-04, -2.777777777777681622553e-03, 8.333333333333333331554247e-02, 5.7083835261e-03);

    my $parity = 0;  
    my $fact = 1;
    my $n = 0;
    my $y = $x;
    if ($y <= 0) {
        $y = -$x;
        $y1 = int($y);
        $res = $y - $y1;
        if ($res != 0) {
            if ($y1 != int($y1*0.5)*2) { $parity = 1 }
            $fact = -$pi / sin($pi*$res);
            $y = $y + 1;
        } else {
            $res = $xinf;
            return $res;
        }
    }
    if ($y < $eps) {
        if ($y >= $xminin) {
            $res = 1 / $y;
        } else {
            $res = $xinf;
            return $res;
        }
    } elsif ($y < 12) {
        $y1 = $y;
        if ($y < 1) {
            $z = $y;
            $y = $y + 1;
        } else {
            $n = int($y) - 1;
            $y = $y - $n;
            $z = $y - 1;
        }
        my $xnum = 0;
        my $xden = 1;
        foreach my $i (0 .. 7) {
            $xnum = ($xnum + $P[$i]) * $z;
            $xden = $xden * $z + $Q[$i];
        }
        $res = $xnum / $xden + 1;
        if ($y1 < $y) {
            $res = $res / $y1;
        } elsif ($y1 > $y) {
            foreach $i ( 1 .. $n ) {
                $res = $res * $y;
                $y = $y + 1;
            }
        }
    } else {
        if ($y <= $xbig) {
            my $ysq = $y * $y;
            my $sum = $C[6];
            foreach my $i (0 .. 5) {
                $sum = $sum / $ysq + $C[$i];
            }
            $sum = $sum/$y - $y + $sqrtpi;
            $sum = $sum + ($y-0.5)*log($y);
            $res = exp($sum);
        } else {
            $res = $xinf;
            return $res;
        }
    }
    if ($parity) { $res = -$res }
    if ($fact != 1) { $res = $fact / $res }
    return $res;
}

1;
