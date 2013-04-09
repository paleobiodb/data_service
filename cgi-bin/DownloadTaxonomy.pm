ackage DownloadTaxonomy;

#use strict;
use PBDBUtil;
use DBTransactionManager;
use Taxonomy;
use Person;
use Debug qw(dbg);
use Constants qw($READ_URL $DATA_DIR $HTML_DIR $TAXA_TREE_CACHE $TAXA_LIST_CACHE);

use strict;

use Data::Dumper;
use CGI::Carp;


# JA 19-21,23.9.08
# provides XML formatted taxonomic data for taxa matching a string search
# uses the same format as the Catalogue of Life Annual Checklist Web Service:
#  http://webservice.catalogueoflife.org/annual-checklist/
# doesn't relate to other subroutines here, but has to go somewhere...

sub getTaxonomyXML {
    
	my ($dbt,$taxonomy,$q,$s,$hbo) = @_;

	print "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n";

	# we'll do this with simple table hits instead of recycling
	#  getTaxonomicNames or getTaxonomicOpinions because the allowed query
	#  parameters and output are both really simple

	# request is assumed to go through an HTTP GET

	my $searchString;
	if ( $q->param('name') )	{
		$searchString = $q->param('name');
	} elsif ( $q->param('id') )	{
		$searchString = $q->param('id');
	}
	$searchString =~ s/\+/ /g;
	$searchString =~ s/\*/%/g;
	if ( $searchString !~ /^(|% )[0-9A-Za-z]/ || $searchString =~ /[^0-9A-Za-z %]/ )	{
		print '<results id="' . $q->param('id') . '" name="' . $q->param('name') . '" total_number_of_results="0" start="0" number_of_results_returned="0" error_message="The search string "'.$searchString.'" is formatted incorrectly" version="1.0">'."\n</results>\n";
		return;
	}
	if ( $searchString !~ /[A-Za-z]{3,}/ && $searchString =~ /[^0-9]/ )	{
		print '<results id="' . $q->param('id') . '" name="' . $q->param('name') . '" total_number_of_results="0" start="0" number_of_results_returned="0" error_message="The taxonomic name is too short" version="1.0">'."\n</results>\n";
		return;
	}
	if ( $searchString =~ /[0-9]/ && $searchString =~ /[^0-9]/ )	{
		print '<results id="' . $q->param('id') . '" name="' . $q->param('name') . '" total_number_of_results="0" start="0" number_of_results_returned="0" error_message="The record ID number includes some non-numerical characters" version="1.0">'."\n</results>\n";
		return;
	}

	# find senior synonyms of names matching query
	my $sql;
	if ( $searchString =~ /\%/ )	{
		$sql = "SELECT t.taxon_no,spelling_no,synonym_no,status FROM authorities a,$TAXA_TREE_CACHE t, opinions o WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND taxon_name LIKE '" . $searchString . "'";
	} elsif ( $searchString > 0 )	{
		$sql = "SELECT t.taxon_no,spelling_no,synonym_no,status FROM authorities a,$TAXA_TREE_CACHE t,opinions o WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND t.taxon_no=$searchString";
	} else	{
		$sql = "SELECT t.taxon_no,spelling_no,synonym_no,status FROM authorities a,$TAXA_TREE_CACHE t,opinions o WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND taxon_name='$searchString'";
	}

	# for each senior name, find all junior names
	my @matches = @{$dbt->getData($sql)};
	my $results = $#matches + 1;
	if ( $results == 0 )	{
		print '<results id="' . $q->param('id') . '" name="' . $q->param('name') . '" total_number_of_results="0" start="0" number_of_results_returned="0" error_message="No names found" version="1.0">'."\n</results>\n";
		return;
	}
	print '<results id="' . $q->param('id') . '" name="' . $q->param('name') . '" total_number_of_results="' . $results . '" start="0" number_of_results_returned="' . $results . '" error_message="" version="1.0">' . "\n";
	for my $m ( @matches )	{
		# if a bad name's parent is not itself bad, replace it with
		#  the grandparent
		# good luck if the grandparent is bad...
		if ( $m->{status} ne "belongs to" )	{
			$sql = "SELECT parent_no FROM $TAXA_TREE_CACHE t,opinions o WHERE " . $m->{synonym_no} . "=child_no AND t.opinion_no=o.opinion_no AND status!='belongs to'";
			my $bad = ${$dbt->getData($sql)}[0];
			if ( $bad && $bad->{parent_no} > 0 )	{
				$m->{synonym_no} = $bad->{parent_no};
			}
		}
		print "<result>\n";
		$sql = "(SELECT status,a.taxon_no,spelling_no,synonym_no,taxon_name,taxon_rank,common_name,DATE_FORMAT(a.modified,'%Y-%m-%d %T') modified,IF(a.ref_is_authority='YES',r.author1last,a.author1last) author1last,IF(a.ref_is_authority='YES',r.author2last,a.author2last) author2last,IF(a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,IF(a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,a.comments FROM authorities a,$TAXA_TREE_CACHE t,refs r,opinions o WHERE a.taxon_no=t.taxon_no AND a.reference_no=r.reference_no AND synonym_no=" . $m->{synonym_no} . " AND t.taxon_no!=" . $m->{taxon_no} . " AND t.opinion_no=o.opinion_no ORDER BY spelling_no,taxon_name) UNION (SELECT status,a.taxon_no,spelling_no,synonym_no,taxon_name,taxon_rank,common_name,DATE_FORMAT(a.modified,'%Y-%m-%d %T') modified,IF(a.ref_is_authority='YES',r.author1last,a.author1last) author1last,IF(a.ref_is_authority='YES',r.author2last,a.author2last) author2last,IF(a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,IF(a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,a.comments FROM authorities a,$TAXA_TREE_CACHE t,refs r,opinions o WHERE a.taxon_no=t.taxon_no AND a.reference_no=r.reference_no AND t.taxon_no=" . $m->{taxon_no} . " AND t.opinion_no=o.opinion_no)";
		my @variants = @{$dbt->getData($sql)};
		# needed to recover references
		my @nos = ($m->{taxon_no});
		push @nos , $_->{taxon_no} foreach @variants;
		$sql = "SELECT child_spelling_no AS taxon_no,r.* FROM refs r,opinions o WHERE r.reference_no=o.reference_no AND child_spelling_no IN (" . join(',',@nos) . ") GROUP BY child_spelling_no,r.reference_no ORDER BY author1last,author2last,pubyr";
		my @refs = @{$dbt->getData($sql)};

		my $acno = $m->{synonym_no};
		my $matched;
		my $accepted;

		for my $v ( @variants )	{
			if ( $v->{status} ne "belongs to" )	{
				if ( $v->{comments} )	{
					$v->{comments} .= " [exact status: " . $v->{status} . "]";
				} else	{
					$v->{comments} = "exact status: " . $v->{status};
				}
			}
		}
		for my $v ( @variants )	{
			if ( $v->{taxon_no} == $m->{taxon_no} )	{
				$matched = $v;
				last;
			}
		}

		# MATCHED BUT INVALID NAME BLOCK
		if ( $m->{taxon_no} == $acno )	{
			$accepted = $matched;
		} else	{
			for my $v ( @variants )	{
				if ( $v->{taxon_no} == $acno )	{
					$accepted = $v;
					last;
				}
			}
			formatNameXML($q,$matched,$acno,@refs);
			print "<accepted_name>\n";
		}

		# ACCEPTED NAME BLOCK
		formatNameXML($q,$accepted,$acno,@refs);

		if ( $q->param('response') eq "full" )  	{
			# CLASSIFICATION BLOCK
			$sql = "SELECT a.taxon_no,taxon_rank,taxon_name FROM authorities a,$TAXA_TREE_CACHE t,$TAXA_LIST_CACHE l WHERE a.taxon_no=t.spelling_no and t.taxon_no=spelling_no AND a.taxon_no=parent_no AND taxon_rank IN ('kingdom','phylum','class','order','family') AND taxon_name NOT IN ('Therapsida','Cetacea','Avetheropoda') AND child_no=" . $accepted->{taxon_no} . " ORDER BY lft";
			my @parents = @{$dbt->getData($sql)};
			print "<classification>\n";
			for my $p ( @parents )	{
				print "<taxon>\n<id>$p->{taxon_no}</id>\n";
				print "<name>$p->{taxon_name}</name>\n";
				my @letts = split(//,$p->{taxon_rank});
				$letts[0] =~ tr/[a-z]/[A-Z]/;
				print "<rank>".join('',@letts)."</rank>\n";
				print "<name_html>$p->{taxon_name}</name_html>\n";
				print "<url>http://paleodb.org/cgi-bin/bridge.pl?action=basicTaxonInfo&amp;taxon_no=$p->{taxon_no}&amp;is_real_user=0</url>\n";
				print "</taxon>\n";
			}
			print "</classification>\n";

			# INFRASPECIES BLOCK
			# only list accepted names of current subspecies
			if ( $accepted->{taxon_rank} eq "species" )	{
				$sql = "SELECT a.taxon_no,spelling_no,taxon_name,taxon_rank,IF(a.ref_is_authority='YES',r.author1last,a.author1last) author1last,IF(a.ref_is_authority='YES',r.author2last,a.author2last) author2last,IF(a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,IF(a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr FROM authorities a,$TAXA_TREE_CACHE t,refs r,opinions o WHERE a.taxon_no=t.taxon_no AND a.reference_no=r.reference_no AND t.taxon_no=synonym_no AND t.taxon_no=child_no AND t.opinion_no=o.opinion_no AND parent_no=" . $accepted->{taxon_no} . " AND taxon_rank='subspecies' ORDER BY spelling_no,taxon_name";
				my @infras = @{$dbt->getData($sql)};
				if ( @infras )	{
					print "<infraspecies_for_this_species>\n";
					for my $i ( @infras )	{
						print "<infraspecies>\n";
						print "<id>$i->{taxon_no}</id>\n";
						print "<name>$i->{taxon_name}</name>\n";
						my ($auth,$name_html) = formatAuthXML($i);
						print "<name_html>$name_html</name_html>\n";
						my ($genus,$species,$infraspecies);
						($genus,$species,$infraspecies) = split / /,$i->{taxon_name};
						print "<genus>$genus</genus>\n";
						print "<species>$species</species>\n";
						print "<infraspecies_marker></infraspecies_marker>\n";
						print "<infraspecies>$infraspecies</infraspecies>\n";
						print "<author>$auth</author>\n";
						print "<url>http://paleodb.org/cgi-bin/bridge.pl?action=basicTaxonInfo&amp;taxon_no=$i->{taxon_no}&amp;is_real_user=0</url>\n";
						print "</infraspecies>\n";
					}
					print "</infraspecies_for_this_species>\n";
				}
			}

			# SYNONYMS BLOCK
			if ( $#variants > 0 )	{
				print "<synonyms>\n";
				for my $v ( @variants )	{
					if ( $v->{taxon_no} != $m->{synonym_no} )	{
						print "<synonym>\n";
						formatNameXML($q,$v,$acno,@refs);
						print "</synonym>\n";
					}
				}
				print "</synonyms>\n";
			}

			# COMMON NAMES BLOCK
			if ( $accepted->{common_name} )	{
				print "<common_names>\n<common_name>\n";
				print "<name>".$accepted->{common_name}."</name>\n";
				print "<language>English</language>\n";
				print "<country>United States</country>\n";
				print "</common_name>\n</common_names>\n";
				# could include a real reference here, but it's
				#  optional and overkill
				print "<references>\n</references>\n";
			}
		}

		if ( $m->{taxon_no} != $m->{synonym_no} )	{
			print "</accepted_name>\n";
		}
		print "</result>\n";
	}
	print "</results>";
	return;
}

sub formatNameXML	{

	my ($q,$n,$acno,@refs) = @_;

	# id = our primary key number
	print "<id>".$n->{taxon_no}."</id>\n";
	print "<name>".$n->{taxon_name}."</name>\n";
	my @letts = split(//,$n->{taxon_rank});
	$letts[0] =~ tr/[a-z]/[A-Z]/;
	print "<rank>".join('',@letts)."</rank>\n";
	if ( $n->{taxon_no} == $acno )	{
		print "<name_status>accepted name</name_status>\n";
	} else	{
		print "<name_status>synonym</name_status>\n";
	}
	my ($auth,$name_html) = formatAuthXML($n);
	print "<name_html>$name_html</name_html>\n";
	if ( $q->param('response') eq "full" )  	{
		if ( $n->{taxon_rank} =~ /genus|species/ )	{
			my ($genus,$species,$infraspecies);
			if ( $n->{taxon_name} =~ / / )	{
				($genus,$species,$infraspecies) = split / /,$n->{taxon_name};
			} else	{
				$genus = $n->{taxon_name};
				$species = "";
			}
			print "<genus>".$genus."</genus>\n";
			print "<species>".$species."</species>\n";
			if ( $infraspecies )	{
				print "<infraspecies_marker>\n<infraspecies>".$species."</infraspecies>\n</infraspecies_marker>\n";
			}
		}
		print "<author>$auth</author>\n";
		$n->{comments} =~ s/\& /&amp; /g;
		print "<additional_comments>".$n->{comments}."</additional_comments>\n";
	}
	# call to taxon info using taxon_no
	print "<url>http://paleodb.org/cgi-bin/bridge.pl?action=basicTaxonInfo&amp;taxon_no=$n->{taxon_no}&amp;is_real_user=0</url>\n";
	if ( $q->param('response') ne "full" )  	{
		print "<online_resource></online_resource>\n";
	}
	print "<source_database>The Paleobiology Database</source_database>\n";
	print "<source_database_url>http://paleodb.org</source_database_url>\n";
	# another URL if URL went to a portal, not source_database
	# REFERENCES BLOCK
	# format is not constrained
	if ( $q->param('response') eq "full" )  	{
		print "<record_scrutiny_date>".$n->{modified}."</record_scrutiny_date>\n";
		print "<online_resource></online_resource>\n";
		print "<references>\n";
		for my $r ( @refs )	{
			if ( $r->{taxon_no} == $n->{taxon_no} )	{
				$r->{reftitle} =~ s/\&/&amp;/g;
				$r->{pubtitle} =~ s/\&/&amp;/g;
				$r->{pubvol} =~ s/\&/&amp;/g;
				print "<reference>\n";
				my $auth = $r->{author1init} . " " . $r->{author1last};
				if ( $r->{otherauthors} ) { $auth .= " <i>et al.</i>"; }
				elsif ( $r->{author2last} ) { $auth .= " and " . $r->{author2init} . " " . $r->{author2last} }
				my $source = $r->{pubtitle};
				if ( $r->{pubvol} ) { $source .= " " . $r->{pubvol}; }
				if ( $r->{pubvol} && $r->{firstpage} ) { $source .= ":" . $r->{firstpage}; }
				elsif ( $r->{firstpage} ) { $source .= ", pp. " . $r->{firstpage}; }
				if ( $r->{lastpage} ) { $source .= "-" . $r->{lastpage}; }
				print "<author>$auth</author>\n";
				print "<year>$r->{pubyr}</year>\n";
				print "<title>$r->{reftitle}</title>\n";
				print "<source>$source</source>\n";
				print "</reference>\n";
			}
		}
		print "</references>\n";
	}

	return;
}

sub formatAuthXML	{

	my $n = shift;
	my $auth = $n->{author1last};
	if ( $n->{otherauthors} )	{
		$auth .= " et al.";
	} elsif ( $n->{author2last} )	{
		$auth .= " and ".$n->{author2last};
	}
	if ( $n->{pubyr} > 1500 ) { $auth .= " " . $n->{pubyr}; }
	my $name_html;
	if ( $n->{taxon_rank} =~ /genus|species/ )	{
		$name_html = "<i>".$n->{taxon_name}."</i>";
	}
	if ( $n->{taxon_no} == $n->{spelling_no} )	{
		$name_html = $n->{taxon_name}." $auth";
	} else	{
		$name_html = $n->{taxon_name}." ($auth)";
	}
	if ( $n->{taxon_rank} =~ /genus|species/ )	{
		$name_html = "<i>".$n->{taxon_name}."</i>";
	}
	return($auth,$name_html);

}

# Builds the itis format. files output are:
#   taxonomic_units.dat - authorities table
#   synonym_links.dat - synonyms
#   taxon_authors_lookup.dat - author data for taxonomic_units, either author1last from refs or authorities table
#   publications.dat - references
#   reference_links.dat - joins publications and taxonomic units I think
#   comments.dat - comments fields (from authorities and opinions)
#   tu_comments_links.dat - joins comments with taxonomic_units, 
#    we have to make this up since our tables aren't denormalized.
# These ITIS files are not output:
#   vernaculars.dat, vern_ref_links.dat, experts.dat, geographic_division.dat, jurisdiction.dat, other_sources.dat

sub displayITISDownload {

    my ($dbt,$taxonomy,$q,$s) = @_;
    
    my $dbh = $dbt->dbh;
    my @errors = ();

    # First do some processing on the $q (CGI) object and after getting out
    # the parameters.  Store the parameters in the %options hash and pass that in
    my %options = $q->Vars();
    if ($options{'taxon_name'}) {
        my @taxon = $taxonomy->getTaxaByName($options{'taxon_name'});
        if (scalar(@taxon) > 1) {
            push @errors, "Taxon name is a homonym";
        } elsif (scalar(@taxon) < 1) {
            push @errors, "Taxon name not found";
        } else {
            $options{taxon_no} = $taxon[0]->{taxon_no};
        }
    }
    
    if ($q->param('person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('person_reversed')));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};  
        if ($person_no) {
            $options{'person_no'} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("person_reversed")." in the database";
        }
    }
    
    if (@errors) {
        displayErrors(@errors);
        print "<div align=\"center\"><h5><a href=\"$READ_URL?action=displayDownloadTaxonomyForm\">Please try again</a></h5></div><br>";
        return;
    }

    print "<div align=\"center\"><p class=\"pageTitle\">Taxonomy download results</p></div>";

    print '<div align="center">
        <table border=0 width=600><tr><td>
        <p class="darkList" class="verylarge" style="padding-left: 0.5em; padding-top: 0.3em; padding-bottom: 0.3em; margin-bottom: 0em;">Output data</p>';

    my ($filesystem_dir,$http_dir) = makeDataFileDir($s);
    
    my ($names) = getTaxonomicNames($dbt,$taxonomy,\%options);
    my %references;
    
    my $sepChar = ($q->param('output_type') eq 'pipe') ? '|'
                                                       : ",";
    my $csv = Text::CSV_XS->new({
            'quote_char'  => '"',
            'escape_char' => '"',
            'sep_char'    => $sepChar,
            'binary'      => 1
    }); 
    
    my $taxon_author_count = 0;
    my $taxon_count = 0;
    my $synonym_count = 0;
    my $comment_count = 0;
    
    open FH_AL, ">$filesystem_dir/taxon_authors_lookup.dat"
        or die "Could not create $filesystem_dir/taxon_authors_lookup.dat: $!";
    
    open FH_TU, ">$filesystem_dir/taxonomic_units.dat"
        or die "Could not create $filesystem_dir/taxonomic_units.dat: $!";
    
    open FH_SL, ">$filesystem_dir/synonym_links.dat"
        or die "Could not create $filesystem_dir/synonym_links.dat: $!";
    
    open FH_C, ">$filesystem_dir/comments.dat"
	or die "Could not create $filesystem_dir/comments.dat: $!";
    
    open FH_CL, ">$filesystem_dir/tu_comments_links.dat"
	or die "Could not create $filesystem_dir/tu_comments_links.dat: $!";
    
    if ( ref $names eq 'ARRAY' and @$names )
    {
	# First, print out all of the different authors and/or author combinations.
	
	# The author1init,author1last,etc fields have been denormalized out into this table, which
	# is effectively 1 field (the authors, all globbed in one field). Since PBDB isn't denormalized
	# in this fashion, we use a semi-arbitrary number. this number is equal to the first taxon_no
	# which uses this author/pubyr combination, so should be semi-stable
	# This section needs to come benfore the taxonomic_units section so we can the use the numbers
	# we pick here as a key in that file
	
	my @sorted_names = sort {$a->{taxon_no} <=> $b->{taxon_no}} @$names;
	my %seen_ref = ();
	my %taxon_author_id_map = ();
	
	foreach my $t (@sorted_names) {
	    if ($t->{'author1last'}) {
		my $refline = formatAuthors($t);
		
		if ($t->{'spelling_no'} != $t->{'taxon_no'}) {
		    $refline = "(".$refline.")";
		}
		my $taxon_author_id = '';
		if (!$seen_ref{$refline}) {
		    $seen_ref{$refline} = $t->{'taxon_no'};
		    $taxon_author_id = $t->{'taxon_no'};
		    my $modified_short = "";
		    my @line = ($taxon_author_id,$refline,$modified_short,$t->{kingdom});
		    $csv->combine(@line);
		    my $csv_string = $csv->string();
		    $csv_string =~ s/\r|\n//g;
		    print FH_AL $csv_string."\n";
		    $taxon_author_count++;
		} else {
		    $taxon_author_id = $seen_ref{$refline};
		}
		$taxon_author_id_map{$t->{'taxon_no'}} = $taxon_author_id;
	    }
	}
	
	$taxon_count = scalar(@$names);
	
	# Then, print out all of the taxonomic units.
	
	# taxon_no, taxon_name, 'unnamed taxon ind?', 'valid/invalid','invalid reason', 'TWG standards met?','complete/partial','related to previous?','','modified','parent name','parent_no','kingdom','taxon_rank',?,?
	
	my @columns= ('taxon_no','','taxon_name','is_valid','invalid_reason','','','','','created','parent_name','taxon_author_id','hybrid_author_id','kingdom','taxon_rank','modified_short','');
    
	foreach my $t (@$names) {
	    my @line = ();
	    foreach my $val (@columns) {
		my $csv_val;
		if ($val eq 'is_valid') {
		    $csv_val = ($t->{'is_valid'}) ? 'valid' : 'invalid';
		    if (! $t->{'is_valid'}) {
			if ($t->{'invalid_reason'} =~ /synonym/) {
			    $t->{'invalid_reason'} = 'junior synonym';
			} elsif ($t->{'invalid_reason'} =~ /nomen vanum/) {
			    $t->{'invalid_reason'} = 'unavailable, nomen vanum';
			} elsif ($t->{'invalid_reason'} =~ /homonym/) {
			    $t->{'invalid_reason'} = 'junior homonym';
			} elsif ($t->{'invalid_reason'} =~ /invalid/) {
			    $t->{'invalid_reason'} = 'invalid subgroup';
			} elsif ($t->{'invalid_reason'} =~ /replaced/) {
			    $t->{'invalid_reason'} = 'unavailable, incorrect original spelling';
			} elsif ($t->{'invalid_reason'} =~ /recombined|corrected/) {
			    $t->{'invalid_reason'} = 'original name/combination';
			}
		    }
		} elsif ($val eq 'taxon_author_id') {
		    $csv_val = $taxon_author_id_map{$t->{'taxon_no'}};
		} else {
		    $csv_val = $t->{$val} || '';
		}
		$csv_val =~ s/\r|\n//g;
		push @line, $csv_val;
	    }
	    $csv->combine(@line);
	    my $csv_string = $csv->string();
	    $csv_string =~ s/\r|\n//g;
	    print FH_TU $csv_string."\n";
	    $references{$t->{'reference_no'}} = 1;
	}
	
	# Then, all synonyms.
	
	foreach my $t (@$names) {
	    my @line = ();
	    # Does this apply to recombinations or just junior synonyms?
	    # Right now only doing junior synonyms
	    if ($t->{'spelling_no'} != $t->{'senior_synonym_no'}) {
		#print "$t->{taxon_no} ss $t->{senior_synonym_no} s $t->{synonym_no}<BR>";
		@line = ($t->{'taxon_no'},$t->{'synonym_no'},$t->{'modified_short'});
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_SL $csv_string."\n";  
		$synonym_count++;
	    }
	}
	
	# Then, all comments.
	
	# Note that our comments aren't denormalized so the comment_id key
	# (primary key for comments table for ITIS is just the primary key taxon_no for us
	# header:     #comment_id,author,   comment,  created,  modified
	
	@columns = ("taxon_no","enterer","comments","created","modified_short");
	
	foreach my $taxon (@$names) {
	    if ($taxon->{'comments'}) {
		my @line = ();
		foreach my $col (@columns) {
		    push @line, $taxon->{$col}; 
		}
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_C $csv_string."\n";  
		$comment_count++;
	    }
	}

	# Note that our comments aren't denormalized so the comment_id key
	# (primary key for comments table for ITIS is just the primary key taxon_no for us
	# Why a modified value exists for a many-to-one type join table is beyond me
	# header:     #taxon_no,comment_id,   modified
	
	@columns = ("taxon_no","taxon_no","modified_short");
	
	foreach my $taxon (@$names) {
	    if ($taxon->{'comments'}) {
		my @line = ();
		foreach my $col (@columns) {
		    push @line, $taxon->{$col}; 
		}
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_CL $csv_string."\n";  
	    }
	}
    }
    
    close FH_AL or carp "Problem closing $filesystem_dir/taxon_authors_lookup.dat: $!";
    close FH_TU or carp "Problem closing $filesystem_dir/taxonomic_units.dat: $!";
    close FH_SL or carp "Problem closing $filesystem_dir/synonym_links.dat: $!";    
    close FH_C or carp "Problem closing $filesystem_dir/comments.dat: $!";
    close FH_CL or carp "Problem closing $filesystem_dir/tu_comments_links.dat: $!";
    
    $taxon_author_count = "No" if ($taxon_author_count == 0);
    print "<p>$taxon_author_count taxon authors names were printed</p>";
    
    $taxon_count = "No" if ($taxon_count == 0);
    print "<p>$taxon_count taxononomic units were printed</p>";
    
    $synonym_count = "No" if ($synonym_count == 0);
    print "<p>$synonym_count synonym links were printed</p>";
    
    $comment_count = "No" if ($comment_count == 0);
    print "<p>$comment_count comments and comment links were printed</p>";
    
    # Now, output all references.
    
    my @references = keys %references; 
    
    open FH_P, ">$filesystem_dir/publications.dat"
	or die "Could not open $filesystem_dir/publications.dat: $!";
    
    my $ref_count = 0;
    
    if (@references)
    {
        # originally written by PS with an extremely tedious triple join on
        #  person meant to avoid using the authorizer/enterer/modifier fields,
        #  which might be space-intensive but do speed up this kind of thing
        #  JA 5.8.08
        my $sql = 'SELECT authorizer, enterer, modifier, DATE_FORMAT(r.modified,\'%m/%e/%Y\') modified_short, r.* FROM refs r  WHERE r.reference_no IN ('.join(',',@references).')';
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        
        while (my $row = $sth->fetchrow_hashref()) {
            my @line = ();
            $ref_count++;
            push @line, 'PUB';
            push @line, $row->{'reference_no'};
            my $refline = formatAuthors($row);
            push @line, $refline;
            push @line, ($row->{'reftitle'} || "");
            my $pubtitle = $row->{'pubtitle'};
            $pubtitle .= " vol. ".$row->{'pubvol'} if ($row->{'pubvol'});
            $pubtitle .= " no. ".$row->{'pubno'} if ($row->{'pubno'});
            push @line, ($pubtitle || "");
            push @line, ($row->{'pubyr'} || ""); # Listed pub date
            push @line, '','','','',''; #Actual pub date, publisher, pub place, isbn, issn
	    #my $pages = Reference::coalescePages($pages{$row->{reference_no}});
            push @line, ($row->{'pages'} || ""); #pages
            push @line, ($row->{'comments'} || "");
            push @line, $row->{'modified_short'};
            
            $csv->combine(@line);
            my $csv_string = $csv->string();
            $csv_string =~ s/\r|\n//g;
            print FH_P $csv_string."\n";  
        }
    }
    
    close FH_P or carp "Problem closing $filesystem_dir/publications.dat: $!";
    
    $ref_count = "No" if ($ref_count == 0);
    print "</p>$ref_count publications were printed</p>";
    
    my ($opinions) = getTaxonomicOpinions($dbt, $taxonomy, \%options); 
    
    open FH_RL, ">$filesystem_dir/reference_links.dat"
	or die "Could not create $filesystem_dir/reference_links.dat: $!";
    
    if ( ref $opinions eq 'ARRAY' and @$opinions )
    {
	my $ref_link_count = 0;
	foreach my $o (@$opinions) {
	    my %seen_ref = ();
	    if (!$seen_ref{$o->{'reference_no'}}) {
		# taxon_no, PUB, reference_no, origianl_desc_ind?, initial_itis_desc_ind?,  change_track_id?, obsolete, update
		my @line  = ($o->{'child_no'}, "PUB", $o->{'reference_no'}, "","","","",$o->{'modified_short'});
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_RL $csv_string."\n";  
		$seen_ref{$o->{'reference_no'}} = 1;
		$ref_link_count++;
	    }
	}
	$ref_link_count = "No" if ($ref_link_count == 0);
        print "<p>$ref_link_count reference links were printed</p>";
    }
    else
    {
        print "<p>No reference links could be downloaded because no search criteria related to \"Taxonomic opinions\" were entered</p>";
    }
    
    close FH_RL or carp "Problem closing $filesystem_dir/reference_links.dat: $!";
    
    # Now copy the documentation (.doc) and zip it up and link to the zipped file
    
    #  0    1    2     3     4    5     6     7     8
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
    my $date = sprintf("%d%02d%02d",($year+1900),$mon,$mday);   
    
    my $dirname = ($s->isDBMember()) ? $s->{'enterer'} : "guest_".$date."_".$$;
    $dirname =~ s/[^a-zA-Z0-9_\/]//g;
    umask '022';

    my $datafile = $DATA_DIR."/ITISCustomizedDwnld.doc";
    my $cmd = "cp $datafile $filesystem_dir";
    my $ot = `$cmd`; 
    #print "$cmd -- $ot -- <BR>";
    $cmd = "cd $filesystem_dir/.. && tar -zcvf $dirname.tar.gz $dirname/*.dat $dirname/*.doc";
    $ot = `$cmd`; 
    #print "$cmd -- $ot -- <BR>";


    print "<div align=\"center\"><h5><a href='/public/taxa_downloads/$dirname.tar.gz'>Download file</a> - <a href=\"$READ_URL?action=displayDownloadTaxonomyForm\">Do another download</a></h5></div><br>";
    print "</td></tr></table></div>";
    #print "<a href='/paleodb/data/JSepkoski/taxonomic_units.dat'>taxonomic units</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/publications.dat'>publications</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/reference_links.dat'>reference links</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/comments.dat'>comments</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/tu_comments_links.dat'>tu comments links</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/taxon_authors_lookup.dat'>taxon authors</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/synonym_links.dat'>synonym_links</a><BR>";

    cleanOldGuestFiles();
}


our (%FIELD_MAP) = ( 'type_taxon' => 'type_taxon_name',
		     'original_taxon_no' => 'orig_no',
		     'original_taxon_name' => 'orig_name',
		     'original_taxon_rank' => 'orig_rank' );

# Builds the pbdb type output files
#  There are 4 files output:
#   taxonomic_names, current
#     only the valid, most recently used names, partially denomalized with opinions (has parent)
#     author fields denormalized
#   taxonomic_names, not current
#     same as above but has valid name and reason for this name being invalid
#   opinions
#     raw dump of opinions with author fields and taxon fields denormalized, also basis
#   references
#     raw dump of references used

sub displayPBDBDownload {

    my ($dbt,$taxonomy,$q,$s) = @_;
    
    my $dbh = $dbt->dbh;
    my @errors = ();
    
    my %options = $q->Vars();
    if ($options{taxon_name}) {
        my @taxon = $taxonomy->getTaxaByName($options{taxon_name});
        if (scalar(@taxon) > 1) {
            push @errors, "Taxon name is a homonym";
        } elsif (scalar(@taxon) < 1) {
            push @errors, "Taxon name not found";
        } else {
            $options{taxon_no} = $taxon[0]->{taxon_no};
        }
    }
    
    if ($q->param('person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($q->param('person_reversed')));
        my $person_no = ${$dbt->getData($sql)}[0]->{person_no};  
        if ($person_no) {
            $options{person_no} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("person_reversed")." in the database";
        }
    }
    
    my $sepChar = ($q->param('output_type') eq 'pipe') ? '|'
                                                       : ",";
    my $csv = Text::CSV_XS->new({
            'quote_char'  => '"',
            'escape_char' => '"',
            'sep_char'    => $sepChar,
            'binary'      => 1
    }); 

    if (@errors) {
        displayErrors(@errors);
        print "<div align=\"center\"><h5><a href=\"$READ_URL?action=displayDownloadTaxonomyForm\">Please try again</a></h5></div><br>";
        return;
    }

    print "<div align=\"center\"><p class=\"pageTitle\">Taxonomy download results</p></div>";
    
    print '<div align="center">
        <table border="0" width="600"><tr><td>
        <p class="darkList" class="verylarge" style="padding-left: 0.5em; padding-top: 0.3em; padding-bottom: 0.3em; margin-bottom: 0em;">Output files</p>';

    my ($filesystem_dir,$http_dir) = makeDataFileDir($s);

    my (%references, %pages);
    
    # Create the opinions file 
    # Note that the opinions file MUST be created first -- this is because there is an option to get taxa
    # that have an opinion that fit the criteria attached to them, so we need to get a additional list of taxa
    # from the opinions function to download
    
    my ($opinions) = getTaxonomicOpinions($dbt, $taxonomy, \%options);
    
    open FH_OP, ">$filesystem_dir/opinions.csv"
        or die "Could not open $filesystem_dir/opinions.csv: $!";
    
    my @header;
    
    if ( $q->param('output_data') =~ /basic/ )	{
        @header = ("child_name","status","parent_name","author1init","author1last","author2init","author2last","otherauthors","pubyr");
    } else	{
        @header = ("authorizer","enterer","modifier","reference_no","opinion_no","child_no","child_name","child_spelling_no","child_spelling_name","status","phylogenetic_status","spelling_reason","parent_no","parent_name","parent_spelling_no","parent_spelling_name","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","basis","comments","created","modified");
    }
    
    $csv->combine(@header);
    
    print FH_OP $csv->string()."\n";
    
    if ( ref $opinions eq 'ARRAY' and @$opinions )
    {
	foreach my $o (@$opinions) {
	    my @line = ();
	    foreach my $val (@header) {
		my $field = $FIELD_MAP{$val} || $val;
		my $csv_val = $o->{$field} || '';
		push @line, $csv_val;
	    }
	    $csv->combine(@line);
	    my $csv_string = $csv->string();
	    $csv_string =~ s/\r|\n//g;
	    print FH_OP $csv_string."\n";  
	    $references{$o->{'reference_no'}} = 1;
	    $pages{$o->{reference_no}}{$o->{pages}} = 1;
	}
    }
    
    close FH_OP or carp "Problem closing $filesystem_dir/opinions.csv: $!";

    if ( ref $opinions eq 'ARRAY' and @$opinions )
    {
        print "<p>" . scalar(@$opinions) . " taxonomic opinions were printed to <a href=\"$http_dir/opinions.csv\">opinions.csv</a></p>";
    }
    else
    {
        print "<p>No taxonomic opinions were downloaded because no search criteria were entered</p>";
    }
    
    # If the user selects an option to get taxonomic names used by the downloaded opinions
    # then make a list of additional taxa to downlod
    
    if ( $options{get_referenced_taxa} and ref $opinions eq 'ARRAY' and @$opinions )
    {
        my %referenced_taxa = (-1=>1);
        foreach my $o (@$opinions) {
            $referenced_taxa{$o->{'child_no'}} = 1; 
            $referenced_taxa{$o->{'child_spelling_no'}} = 1; 
            if ($o->{'status'} eq 'misspelling of') {
                $referenced_taxa{$o->{'parent_spelling_no'}} = 1;
            }
        }
        $options{referenced_taxa} = join(',',keys %referenced_taxa);
    }
    
    # Now get all of the requested taxonomic names (including the additional
    # ones specified in opinions).
    
    my ($names) = getTaxonomicNames($dbt,$taxonomy,\%options);
    
    my $valid_count = 0;
    my $invalid_count = 0;
    
    # First, output the valid ones.
    
    open FH_VT, ">$filesystem_dir/valid_taxa.csv"
        or die "Could not open $filesystem_dir/valid_taxa.csv: $!";
    
    if ( $q->param('output_data') =~ /basic/ )
    {
        @header = ("taxon_rank","taxon_name","author1init","author1last","author2init","author2last","otherauthors","pubyr");
    }
    else
    {
        @header = ("authorizer","enterer","modifier","reference_no","taxon_no","taxon_name","spelling_reason","common_name","taxon_rank","original_taxon_no","original_taxon_name","original_taxon_rank","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","parent_name","extant","preservation","type_taxon","type_specimen","type_body_part","part_details","comments","created","modified");
    }
    
    $csv->combine(@header);
    print FH_VT $csv->string()."\n";
    
    if ( ref $names eq 'ARRAY' and @$names )
    {
	foreach my $t (@$names) {
	    if ($t->{'is_valid'}) {
		my @line = ();
		foreach my $val (@header) {
		    my $field = $FIELD_MAP{$val} || $val;
		    my $csv_val = $t->{$field} || '';
		    push @line, $csv_val;
		}
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_VT $csv_string."\n";
		$valid_count++;
		$references{$t->{'reference_no'}} = 1;
		$pages{$t->{reference_no}}{$t->{pages}} = 1;
	    }
	}
    }
    
    close FH_VT or carp "Problem closing $filesystem_dir/valid_taxa.csv: $!";
    
    # Then output the invalid taxa.
    
    open FH_IT, ">$filesystem_dir/invalid_taxa.csv"
        or die "Could not open $filesystem_dir/invalid_taxa.csv: $!";
    
    if ( $q->param('output_data') =~ /basic/ )
    {
        @header = ("taxon_rank","taxon_name","author1init","author1last","author2init","author2last","otherauthors","pubyr","invalid_reason");
    }
    else
    {
        @header = ("authorizer","enterer","modifier","reference_no","taxon_no","taxon_name","common_name","taxon_rank","invalid_reason","original_taxon_no","original_taxon_name","original_taxon_rank","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","parent_name","extant","preservation","type_taxon","type_specimen","type_body_part","part_details","comments","created","modified");
    }
    
    $csv->combine(@header);
    print FH_IT $csv->string()."\n";
	
    if ( ref $names eq 'ARRAY' and @$names )
    {
	foreach my $t (@$names) {
	    if (!$t->{'is_valid'}) {
		my @line = ();
		foreach my $val (@header) {
		    my $field = $FIELD_MAP{$val} || $val;
		    my $csv_val = $t->{$field} || '';
		    push @line, $csv_val;
		}
		$csv->combine(@line);
		my $csv_string = $csv->string();
		$csv_string =~ s/\r|\n//g;
		print FH_IT $csv_string."\n";
		$invalid_count++;
		$references{$t->{'reference_no'}} = 1;
	    $pages{$t->{reference_no}}{$t->{pages}} = 1;
	    }
	}
    }
    
    close FH_IT or carp "Problem closing $filesystem_dir/invalid_taxa.csv: $!";
    
    unless ( ref $names eq 'ARRAY' and @$names )
    {
	print "<p>No taxonomic names were downloaded because no search criteria were entered</p>\n";
    }
    
    else
    {
        print "<p>$valid_count valid taxa were printed to <a href=\"$http_dir/valid_taxa.csv\">valid_taxa.csv</a></p>\n";
        print "<p>$invalid_count invalid taxa were printed to <a href=\"$http_dir/invalid_taxa.csv\">invalid_taxa.csv</a></p>\n";
    }
    
    my @references = keys %references;
    
    open FH_REF, ">$filesystem_dir/references.csv";
    open FH_RIS, ">$filesystem_dir/references_ris.txt";
    
    if ( $q->param('output_data') =~ /basic/ )
    {
        @header = ('author1init','author1last','author2init','author2last','otherauthors','pubyr','reftitle','pubtitle','pubvol','pubno','firstpage','lastpage');
    }
    else
    {
        @header = ('authorizer','enterer','modifier','reference_no','author1init','author1last','author2init','author2last','otherauthors','pubyr','reftitle','pubtitle','pubvol','pubno','firstpage','lastpage','publication_type','basis','comments','created','modified');
    }
    
    $csv->combine(@header);
    print FH_REF $csv->string()."\n";
    
    my @ris_bad_list;
    if (@references)
    {
	my $ref_list = join(',', @references);
	
        my $sql = "
		SELECT r.*, pp1.name as authorizer, pp2.name as enterer, pp3.name as modifier
		FROM refs as r
			LEFT JOIN person as pp1 on pp1.person_no = r.authorizer_no
			LEFT JOIN person as pp2 on pp2.person_no = r.enterer_no
			LEFT JOIN person as pp3 on pp3.person_no = r.modifier_no
		WHERE r.reference_no IN ($ref_list)";
        
	my $sth = $dbh->prepare($sql);
        $sth->execute();
        
        my $ref_count = 0;
	my $ris_ref_count = 0;
        while (my $row = $sth->fetchrow_hashref()) {
            $ref_count++;
            my @line = ();
            foreach my $val (@header) {
                my $csv_val = $row->{$val} || '';
                push @line, $csv_val;
            }
            $csv->combine(@line);
            my $csv_string = $csv->string();
            $csv_string =~ s/\r|\n//g;
            print FH_REF $csv_string."\n";
	    $row->{refpages} = Reference::coalescePages($pages{$row->{reference_no}});
	    my $outref = Reference::formatRISRef($dbt, $row);
	    if ( $outref =~ /PARSE ERROR/ )
	    {
		push @ris_bad_list, $row->{reference_no};
	    }
	    else
	    {
		$ris_ref_count++;
		print FH_RIS $outref;
	    }
        }
        my $ref_link = $http_dir."/references.csv";
	my $ris_link = $http_dir."/references_ris.txt";
        print "<p>$ref_count references were printed to <a href=\"$ref_link\">references.csv</a></p>\n";
	print "<p>$ris_ref_count references were printed to <a href=\"$ris_link\">references_ris.txt</a></p>\n";
	
	if ( @ris_bad_list )
	{
	    print "<p>The following references could not be printed due to erroneous data:<ul>\n";
	    print "<li>$_</li>\n" foreach @ris_bad_list;
	    print "</ul></p>\n";
	}
    } else {
        print "<p>No references were printed</p>";
    }

    print '</td></tr></table></div>';
    print "<div align=\"center\"><h5><a href=\"$READ_URL?action=displayDownloadTaxonomyForm\">Do another download</a></h5></div><br>";
    
    close FH_REF;
    close FH_RIS;
    
    cleanOldGuestFiles();
}


# Gets invalid and valid names
#   distinguish by field "is_valid" (boolean)
#   if is_valid == false, then invalid_reason will be populated with the reason why:
#       can be "synonym of, recombined as, corrected as, replaced by" etc
# Gets immediate parent of taxa (at end, in separate query)
# heavily written by JA 29.9.11
sub getTaxonomicNames {
    
    my ($dbt, $taxonomy, $options) = @_;
    
    my $dbh = $dbt->dbh;
    my $query_options = {};
    
    # First process the filtering options.
    
    if ( $options->{reference_no} > 0 )
    {
        $query_options->{reference_no} = $options->{reference_no} + 0;
    }
    
    if ( $options->{pubyr} > 0 )
    {
	$query_options->{pubyr} = $options->{pubyr} + 0;
	
	if ( $options->{pubyr_before_after} eq 'before' )
	{
	    $query_options->{pubyr_rel} = 'before';
	}
	else
	{
	    $query_options->{pubyr_rel} = 'after';
	}
    }
    
    if ( $options->{author} =~ /\w/ )
    {
	$query_options->{author} = $options->{author};
    }
    
    if ( $options->{person_no} > 0 )
    {
	$query_options->{person_no} = $options->{person_no} + 0;
	
	if ( $options->{person_type} != '' )
	{
	    $query_options->{person_rel} = $options->{person_type};
	}
	else
	{
	    $query_options->{person_rel} = 'authorizer';
	}
    }
    
    if ( $options->{created_year} > 0 )
    {
        my ($yyyy,$mm,$dd) = ($options->{created_year},$options->{created_month},$options->{created_day});
        $query_options->{created} = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
	$query_options->{created_rel} = $options->{created_before_after} eq 'before' ? 'before' : 'after';
    }
    
    if ( $options->{taxon_rank} && $options->{taxon_rank} ne "all ranks")
    {
	$query_options->{rank} = $options->{taxon_rank};
    }
    
    # Specify the fields that need to be returned.
    
    my $query_fields = ['oldattr', 'link', 'parent', 'orig', 'tt', 'person',
			'specimen', 'pages', 'created', 'modshort', 
			'comments'];
    
    $query_options->{fields} = $query_fields;
    $query_options->{status} = 'all';
    
    # Then, apply these to the specified taxa.
    
    my @taxa_list;
    
    if ( $options->{taxon_no} > 0 )
    {
	push @taxa_list, $taxonomy->getTaxa('all_children', $options->{taxon_no},
						    $query_options);
    }
    
    else
    {
	push @taxa_list, $taxonomy->getTaxa('all_taxa', undef, $query_options);
    }
    
    # Next, add in all taxa specified by the 'referenced_taxa' option (those
    # which have not already been included).
    
    if ( $options->{referenced_taxa} )
    {
	my (%found_taxon, @additions);
	
	foreach my $taxon (@taxa_list)
	{
	    $found_taxon{$taxon->{taxon_no}} = 1;
	}
	
	push @additions, $taxonomy->getTaxa('self', $options->{referenced_taxa},
					    { include => $query_fields });
	
	foreach my $taxon (@additions)
	{
	    push @taxa_list, $taxon unless $found_taxon{$taxon->{taxon_no}};
	}
    }
    
    # Now process each row
    
    unless ( @taxa_list )
    {
	return [];
    }
    
    my ($valid_count,$invalid_count) = (0,0);
    
    foreach my $row (@taxa_list)
    {
	$row->{is_valid} = "";
	
	# some legacy nomen opinions lack parents (argh)
	if ( $row->{status} =~ /nomen/ && ! $row->{parent_name} )
	{
	    $row->{is_valid} = "";
	    $row->{invalid_reason} = $row->{status};
	    $invalid_count++;
	}
	
	# invalid names with known parents
	elsif ( $row->{taxon_no} != $row->{synonym_no} )
	{
	    $row->{invalid_reason} .= ( $row->{invalid_reason} =~ /^nomen/ ) ? " belonging to" : "";
	    $row->{invalid_reason} = "$row->{invalid_reason} $row->{parent_name}";
	    $row->{synonym_no} = $row->{parent_no}; 
	    $invalid_count++;
	}
	
	# valid names with changed spellings
	elsif ( $row->{taxon_no} != $row->{orig_no} )
	{
	    if ($row->{spelling_reason} =~ /^corr/) {
		$row->{spelling_reason} = "corrected as $row->{parent_name}";
	    } elsif ($row->{spelling_reason} =~ /^missp/) {
		$row->{'spelling_reason'} = "misspelling of $row->{original_taxon_name}";
	    } elsif ($row->{spelling_reason} =~ /^recomb/) {
		$row->{spelling_reason} = "recombined into $row->{parent_name}";
	    } elsif ($row->{spelling_reason} =~ /^reass/) {
		$row->{spelling_reason} = "reassigned into $row->{parent_name}";
	    } elsif ($row->{spelling_reason} =~ /^rank/) {
		$row->{spelling_reason} = "$row->{orig_no} rank changed from $row->{orig_rank} to $row->{taxon_rank}";
		if ($row->{taxon_name} ne $row->{parent_name}) {
                        $row->{spelling_reason} .= ", parent to $row->{parent_name}";
                    }
                } else	{
                    $row->{spelling_reason} = "belongs to $row->{parent_name}";
                }
                $row->{is_valid} = 1;
                $valid_count++;
	}
	
	# valid names with original spellings
	else {
	    # taxa_tree_cache does track the correct spelling even if
	    #  the current opinion is mispelled, so the name will be
	    #  printed with the right spelling one way or another
	    if ($row->{spelling_reason} =~ /^missp/) {
		$row->{spelling_reason} = "original spelling";
	    }
	    $row->{is_valid} = 1;
	    $valid_count++;
	}
    }
    
    return \@taxa_list;
}


# Get taxonomic opinions.

sub getTaxonomicOpinions {
    
    my ($dbt, $taxonomy, $options) = @_;
    
    my $dbh = $dbt->dbh;
    my $query_options = {};
    
    # First process the filtering options
    
    if ( $options->{reference_no} > 0 )
    {
        $query_options->{reference_no} = $options->{reference_no} + 0;
    }
    
    if ( $options->{pubyr} > 0 )
    {
	$query_options->{pubyr} = $options->{pubyr} + 0;
	
	if ( $options->{pubyr_before_after} eq 'before' )
	{
	    $query_options->{pubyr_rel} = 'before';
	}
	else
	{
	    $query_options->{pubyr_rel} = 'after';
	}
    }
    
    if ( $options->{author} =~ /\w/ )
    {
	$query_options->{author} = $options->{author};
    }
    
    if ( $options->{person_no} > 0 )
    {
	$query_options->{person_no} = $options->{person_no} + 0;
	
	if ( $options->{person_type} != '' )
	{
	    $query_options->{person_rel} = $options->{person_type};
	}
	else
	{
	    $query_options->{person_rel} = 'authorizer';
	}
    }
    
    if ( $options->{created_year} > 0 )
    {
        my ($yyyy,$mm,$dd) = ($options->{created_year},$options->{created_month},$options->{created_day});
        $query_options->{created} = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
	$query_options->{created_rel} = $options->{created_before_after} eq 'before' ? 'before' : 'after';
    }
    
    # Specify the fields that need to be returned.
    
    my $query_fields = ['oldattr', 'person', 'pages', 'created', 'modshort',
			'comments', 'names'];
    
    $query_options->{fields} = $query_fields;
    $query_options->{status} = 'all';
    
    # Then apply these to the specified opinions.
    
    my @opinion_list;
    
    if ( $options->{taxon_no} > 0 )
    {
	push @opinion_list, $taxonomy->getOpinions('child_desc', $options->{taxon_no},
						   $query_options);
    }
    
    else
    {
	push @opinion_list, $taxonomy->getOpinions('all_opinions', undef, 
						   $query_options);
    }
    
    # Next, add in all taxa specified by the 'referenced_taxa' option (those
    # which have not already been included).
    
    if ( $options->{referenced_taxa} )
    {
	my (%found_opinion, @additions);
	
	foreach my $opinion (@opinion_list)
	{
	    $found_opinion{$opinion->{opinion_no}} = 1;
	}
	
	push @additions, $taxonomy->getOpinions('child', $options->{reference_taxa},
						{ include => $query_fields });
	
	foreach my $opinion (@additions)
	{
	    push @opinion_list, $opinion unless $found_opinion{$opinion->{opinion_no}};
	}
    }
    
    return \@opinion_list;
    
}


sub makeDataFileDir {
    my $s = shift;

    my $name = ($s->get("enterer")) ? $s->get("enterer") : "";
    my $filename = PBDBUtil::getFilename($name,1); 
    my $filesystem_dir = $HTML_DIR."/public/taxa_downloads/".$filename;
    my $http_dir = "/public/taxa_downloads/".$filename;

    PBDBUtil::autoCreateDir($filesystem_dir);

    umask '022';
    dbg("File dir is $filesystem_dir");
    if (! -e $filesystem_dir) {
        mkdir($filesystem_dir)
            or die "Could not create directory $filesystem_dir ($!)";
    }

    return ($filesystem_dir,$http_dir);
}

sub formatAuthors {
    my $row = shift;
    my @authors = ();
    my $author1 = $row->{'author1last'};
    $author1 = $row->{'author1init'}." ".$author1 if ($row->{'author1init'});
    push @authors, $author1;

    my $author2 = $row->{'author2last'};
    $author2 = $row->{'author2init'}." ".$author2 if ($row->{'author2init'});
    push @authors, $author2 if ($author2);
    my @otherauthors = split /\s*,\s*/,$row->{'otherauthors'};
    push @authors, @otherauthors;
    my $refline = "";
    if (scalar(@authors) > 1) {
        my $last_author = pop @authors;
        $refline = join(", ",@authors);
        $refline .= " and $last_author";
    } else {
        $refline = $authors[0];
    }
    $refline .= ", ".$row->{'pubyr'} if ($row->{'pubyr'});
    return $refline;
}

sub cleanOldGuestFiles {
    # erase all files that haven't been accessed in more than a day

    my $filedir = $HTML_DIR."/public/taxa_downloads/";
    opendir(DIR,$filedir) or die "couldn't open $filedir ($!)";
    # grab only guest files
    my @filenames = grep { /^guest/ } readdir(DIR);
    closedir(DIR);

    foreach my $f (@filenames){
        my $file = "$filedir/$f";
        if((-M "$file") > 1){ # > than 1 day old
            if (-d "$file") {
                opendir(DIR,$file);
                my @subfiles = grep {/csv$|dat$|doc$/} readdir(DIR);
                closedir(DIR);
                foreach my $subf (@subfiles) {
                    my $subfile = "$file/$subf";
                    unlink $subfile;
                }
                rmdir($file);
            } else {
                unlink $file;
            }
        }
    }
}

sub displayErrors {
    if (scalar(@_)) { 
        my $plural = (scalar(@_) > 1) ? "s" : "";
        print "<br><div align=center><table width=600 border=0>" .
              "<tr><td class=darkList><font size='+1'><b> Error$plural</b></font></td></tr>" .
              "<tr><td>";
        print "<li class='medium'>$_</li>" for (@_);
        print "</td></tr></table></div><br>";
    } 
}

1;
