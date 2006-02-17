package DownloadTaxonomy;

#use strict;
use PBDBUtil;
use Classification;
use TimeLookup;
use Data::Dumper;
use DBTransactionManager;
use TaxaCache;
use CGI::Carp;
use Class::Date qw(date localdate gmdate now);

# Flags and constants
my $DEBUG=0; # The debug level of the calling program
$|=1; #free flowing data

use strict;

# Builds the itis format. files output are:
#   taxonomic_units.dat - authorities table
#   synonym_links.dat - synonyms
#   taxon_authors_lookup.dat - author data for taxonomic_units, either author1last from refs or authorities table
#   publications.dat - references
#   reference_links.dat - joins publications and taxonomic units I think
#   comments.dat - comments fields (from authorities and opinions)
#   tu_comments_links.dat - joins comments with taxonomic_units, 
#    we have to make this up since our tables aren't denormalized.
# These itis files are not output:
#   vernaculars.dat, vern_ref_links.dat, experts.dat, geographic_division.dat, jurisdiction.dat, other_sources.dat
sub displayITISDownload {
    my ($dbt,$q,$s) = @_;
    my $dbh = $dbt->dbh;
    my @errors = ();

    # First do some processing on the $q (CGI) object and after getting out
    # the parameters.  Store the parameters in the %options hash and pass that in
    my %options = $q->Vars();
    if ($options{'taxon_name'}) {
        my @taxon = TaxonInfo::getTaxon($dbt,'taxon_name'=>$options{'taxon_name'});
        if (scalar(@taxon) > 1) {
            push @errors, "Taxon name is homonym";
        } elsif (scalar(@taxon) < 1) {
            push @errors, "Taxon name not found";
        } else {
            $options{'taxon_no'} = $taxon[0]->{'taxon_no'};
        }
    }

    if ($q->param('opinion_person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('opinion_person_reversed'));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};  
        if ($person_no) {
            $options{'opinion_person_no'} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("opinion_person_reversed")." in the database";
        }
    }
    if ($q->param('taxon_person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('taxon_person_reversed'));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};  
        if ($person_no) {
            $options{'taxon_person_no'} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("taxon_person_reversed")." in the database";
        }
    }


    if (@errors) {
        displayErrors(@errors);
        return;
    }

    print "<div align=\"center\"><h2>Taxonomy download results</h2></div>";

    my ($filesystem_dir,$http_dir) = makeDataFileDir($s);

    my ($names,$taxon_file_message) = getTaxonomicNames($dbt,$http_dir,%options);
    my @names = @$names;
    my %references;

    my $sepChar = ($q->param('output_type') eq 'pipe') ? '|'
                                                       : ",";
    my $csv = Text::CSV_XS->new({
            'quote_char'  => '"',
            'escape_char' => '"',
            'sep_char'    => $sepChar,
            'binary'      => 1
    }); 

    # A Map of taxon_no --> kingdom_name. Needed for itis, since the kingdom name
    # is used as a foreign key in multiple places for some reason
    my %kingdom = getKingdomMap($dbt);
    
    # The author1init,author1last,etc fields have been denormalized out into this table, which
    # is effectively 1 field (the authors, all globbed in one field). Since PBDB isn't denormalized
    # in this fashion, we use a semi-arbitrary number. this number is equal to the first taxon_no
    # which uses this author/pubyr combination, so should be semi-stable
    # This section needs to come benfore the taxonomic_units section so we can the use the numbers
    # we pick here as a key in that file
    open FH_AL, ">$filesystem_dir/taxon_authors_lookup.dat";
    my @sorted_names = sort {$a->{taxon_no} <=> $b->{taxon_no}} @names;
    my %seen_ref = ();
    my %taxon_author_id_map = ();
    my $taxon_author_count = 0;
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
                my @line = ($taxon_author_id,$refline,$modified_short,$kingdom{$taxon_author_id});
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
    close FH_AL;
    $taxon_author_count = "No" if ($taxon_author_count == 0);
    print "<p>$taxon_author_count taxon authors names were printed to file</p>";

    
    open FH_TU, ">$filesystem_dir/taxonomic_units.dat"
        or die "Could not create taxonomic_units.dat";
    my @columns= ('taxon_no','','taxon_name','is_valid','invalid_reason','','','','','created','parent_name','taxon_author_id','hybrid_author_id','kingdom','taxon_rank','modified_short','');

    # taxon_no, taxon_name, 'unnamed taxon ind?', 'valid/invalid','invalid reason', 'TWG standards met?','complete/partial','related to previous?','','modified','parent name','parent_no','kingdom','taxon_rank',?,?
    foreach my $t (@names) {
        my @line = ();
        foreach my $val (@columns) {
            my $csv_val;
            if ($val eq 'kingdom') {
                $csv_val = $kingdom{$t->{'taxon_no'}} || '';
            } elsif ($val eq 'is_valid') {
                $csv_val = ($t->{'is_valid'}) ? 'valid' : 'invalid';
                if (! $t->{'is_valid'}) {
                    if ($t->{'invalid_reason'} =~ /synonym/) {
                        $t->{'invalid_reason'} = 'junior synonym';
                    } elsif ($t->{'invalid_reason'} =~ /nomen vanum/) {
                        $t->{'invalid_reason'} = 'unavailable, nomen vanum';
                    } elsif ($t->{'invalid_reason'} =~ /homonym/) {
                        $t->{'invalid_reason'} = 'junior homonym';
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
    close FH_TU;
    my $taxon_count = scalar(@names); 
    $taxon_count = "No" if ($taxon_count == 0);
    print "<p>$taxon_count taxononomic units were printed to file</p>";

    open FH_SL, ">$filesystem_dir/synonym_links.dat";
    my $synonym_count = 0;
    foreach my $t (@names) {
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
    close FH_SL;
    $synonym_count = "No" if ($synonym_count == 0);
    print "<p>$synonym_count synonym links were printed to file</p>";
    
    my @references = keys %references; 
    open FH_P, ">$filesystem_dir/publications.dat";
    my $ref_count = 0;
    if (@references) {
        my $sql = 'SELECT p1.name authorizer, p2.name enterer, p3.name modifier, DATE_FORMAT(r.modified,\'%m/%e/%Y\') modified_short, r.*'.
                  ' FROM refs r '.
                  ' LEFT JOIN person p1 ON p1.person_no=r.authorizer_no'.
                  ' LEFT JOIN person p2 ON p2.person_no=r.enterer_no'.
                  ' LEFT JOIN person p3 ON p3.person_no=r.modifier_no'.
                  ' WHERE r.reference_no IN ('.join(',',@references).')';
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
            push @line, ($row->{'pages'} || ""); #pages
            push @line, ($row->{'comments'} || "");
            push @line, $row->{'modified_short'};
            
            $csv->combine(@line);
            my $csv_string = $csv->string();
            $csv_string =~ s/\r|\n//g;
            print FH_P $csv_string."\n";  
        }
    }     
    close FH_P;
    $ref_count = "No" if ($ref_count == 0);
    print "<p>$ref_count publications were printed to file</p>";
    
    my ($opinions,$opinion_file_message) = getTaxonomicOpinions($dbt,$http_dir,%options); 
    my @opinions = @$opinions;
    open FH_RL, ">$filesystem_dir/reference_links.dat";
    my $ref_link_count = 0;
    foreach my $o (@opinions) {
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
    close FH_RL;
    $ref_link_count = "No" if ($ref_link_count == 0);
    if ($opinion_file_message =~ /no search criteria/) {
        print "<p>No reference links could be downloaded because no search criteria related to \"Taxonomic opinions\" were entered</p>";
    } else {
        print "<p>$ref_link_count reference links were printed to file</p>";
    }
   
   
    my @comments = ();
    open FH_C, ">$filesystem_dir/comments.dat";
    # Note that our comments aren't denormalized so the comment_id key
    # (primary key for comments table for ITIS is just the primary key taxon_no for us
    # header:     #comment_id,author,   comment,  created,  modified
    my @columns = ("taxon_no","enterer","comments","created","modified_short");
    my $comment_count = 0;
    foreach my $taxon (@names) {
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
    close FH_C;
    $comment_count = "No" if ($comment_count == 0);
    print "<p>$comment_count comments and comment links were printed to file</p>";

    open FH_CL, ">$filesystem_dir/tu_comments_links.dat";
    # Note that our comments aren't denormalized so the comment_id key
    # (primary key for comments table for ITIS is just the primary key taxon_no for us
    # Why a modified value exists for a many-to-one type join table is beyond me
    # header:     #taxon_no,comment_id,   modified
    my @columns = ("taxon_no","taxon_no","modified_short");
    foreach my $taxon (@names) {
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
    close FH_CL;

    # Now copy the documentation (.doc) and zip it up and link to the zipped file

   #  0    1    2     3     4    5     6     7     8
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
   my $date = sprintf("%d%02d%02d",($year+1900),$mon,$mday);   

    my $dirname = ($s->isDBMember()) ? $s->{'enterer'} : "guest".$date."_".$$;
    $dirname =~ s/[^a-zA-Z0-9_\/]//g;
    umask '022';

    my $datafile = $ENV{'DOWNLOAD_DATAFILE_DIR'}."/ITISCustomizedDwnld.doc";
    my $cmd = "cp $datafile $filesystem_dir";
    my $ot = `$cmd`; 
    #print "$cmd -- $ot -- <BR>";
    $cmd = "cd $filesystem_dir/.. && tar zcvf $dirname.tar.gz $dirname/*.dat $dirname/*.doc";
    $ot = `$cmd`; 
    #print "$cmd -- $ot -- <BR>";


    print "<div align=\"center\"><a href='/paleodb/data/$dirname.tar.gz'>Download file</a></div>";
    #print "<a href='/paleodb/data/JSepkoski/taxonomic_units.dat'>taxonomic units</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/publications.dat'>publications</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/reference_links.dat'>reference links</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/comments.dat'>comments</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/tu_comments_links.dat'>tu comments links</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/taxon_authors_lookup.dat'>taxon authors</a><BR>";
    #print "<a href='/paleodb/data/JSepkoski/synonym_links.dat'>synonym_links</a><BR>";

    cleanOldGuestFiles();
}


# Builds the pbdb type output files
#  There are 4 files output:
#   taxonomic_names, current
#     only the valid, most recently used names, partially denomalized with opinions (has parent)
#     author fields denormalized
#   taxonomic_names, not current
#     same as above but has valid name and reason for this name being invalid
#   opinions
#     raw dump of opinions with author fields and taxon fields denormalized, also classification quality
#   references
#     raw dump of references used
sub displayPBDBDownload {
    my ($dbt,$q,$s) = @_;
    my $dbh = $dbt->dbh;
    my @errors = ();

    my %options = $q->Vars();
    if ($options{'taxon_name'}) {
        my @taxon = TaxonInfo::getTaxon($dbt,'taxon_name'=>$options{'taxon_name'});
        if (scalar(@taxon) > 1) {
            push @errors, "Taxon name is homonym";
        } elsif (scalar(@taxon) < 1) {
            push @errors, "Taxon name not found";
        } else {
            $options{'taxon_no'} = $taxon[0]->{'taxon_no'};
        }
    }

    if ($q->param('opinion_person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('opinion_person_reversed'));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};  
        if ($person_no) {
            $options{'opinion_person_no'} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("opinion_person_reversed")." in the database";
        }
    }
    if ($q->param('taxon_person_reversed')) {
        my $sql = "SELECT person_no FROM person WHERE reversed_name like ".$dbh->quote($q->param('taxon_person_reversed'));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};  
        if ($person_no) {
            $options{'taxon_person_no'} = $person_no;
        } else {
            push @errors, "Could not find person ".$q->param("taxon_person_reversed")." in the database";
        }
    }


    if (@errors) {
        displayErrors(@errors);
        return;
    }

    print "<div align=\"center\"><h2>Taxonomy download results</h2></div>";
    

    my ($filesystem_dir,$http_dir) = makeDataFileDir($s);

    my ($names,$taxon_file_message) = getTaxonomicNames($dbt,$http_dir,%options);
    my @names = @$names;
    my %references;

    my $sepChar = ($q->param('output_type') eq 'pipe') ? '|'
                                                       : ",";
    my $csv = Text::CSV_XS->new({
            'quote_char'  => '"',
            'escape_char' => '"',
            'sep_char'    => $sepChar,
            'binary'      => 1
    }); 

    open FH_VT, ">$filesystem_dir/valid_taxa.csv"
        or die "Could not open valid_taxa.csv ($!)";
    my @header = ("authorizer","enterer","modifier","reference_no","taxon_no","taxon_name","taxon_rank","original_taxon_no","original_taxon_name","original_taxon_rank","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","parent_name","extant","type_taxon","type_specimen","comments","created","modified");
    $csv->combine(@header);
    print FH_VT $csv->string()."\n";
    foreach my $t (@names) {
        if ($t->{'is_valid'}) {
            my @line = ();
            foreach my $val (@header) {
                my $csv_val = $t->{$val} || '';
                push @line, $csv_val;
            }
            $csv->combine(@line);
            my $csv_string = $csv->string();
            $csv_string =~ s/\r|\n//g;
            print FH_VT $csv_string."\n";  
            $references{$t->{'reference_no'}} = 1;
        }
    }
    close FH_VT;


    open FH_IT, ">$filesystem_dir/invalid_taxa.csv"
        or die "Could not open invalid_taxa.csv ($!)";
    @header = ("authorizer","enterer","modifier","reference_no","taxon_no","taxon_name","taxon_rank","invalid_reason","original_taxon_no","original_taxon_name","original_taxon_rank","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","parent_name","extant","type_taxon","comments","created","modified");
    $csv->combine(@header);
    print FH_IT $csv->string()."\n";
    foreach my $t (@names) {
        if (!$t->{'is_valid'}) {
            my @line = ();
            foreach my $val (@header) {
                my $csv_val = $t->{$val} || '';
                push @line, $csv_val;
            }
            $csv->combine(@line);
            my $csv_string = $csv->string();
            $csv_string =~ s/\r|\n//g;
            print FH_IT $csv_string."\n";  
            $references{$t->{'reference_no'}} = 1;
        }
    }
    close FH_IT;
    print $taxon_file_message;


    # Create the opinions file 
    my ($opinions,$opinion_file_message) = getTaxonomicOpinions($dbt,$http_dir,%options); 
    my @opinions = @$opinions;
    open FH_OP, ">$filesystem_dir/opinions.csv"
        or die "Could not open opinions.csv ($!)";
    @header = ("authorizer","enterer","modifier","reference_no","opinion_no","child_no","child_name","child_spelling_no","child_spelling_name","status","parent_no","parent_name","parent_spelling_no","parent_spelling_name","author1init","author1last","author2init","author2last","otherauthors","pubyr","pages","figures","classification_quality","created","modified");
    $csv->combine(@header);
    print FH_OP $csv->string()."\n";
    foreach my $o (@opinions) {
        my @line = ();
        foreach my $val (@header) {
            my $csv_val = $o->{$val} || '';
            push @line, $csv_val;
        }
        $csv->combine(@line);
        my $csv_string = $csv->string();
        $csv_string =~ s/\r|\n//g;
        print FH_OP $csv_string."\n";  
        $references{$o->{'reference_no'}} = 1;
    }
    close FH_OP;
    print $opinion_file_message;

    my @references = keys %references; 
    open FH_REF, ">$filesystem_dir/references.csv";
    my @header = ('authorizer','enterer','modifier','reference_no','author1init','author1last','author2init','author2last','otherauthors','pubyr','reftitle','pubtitle','pubvol','pubno','firstpage','lastpage','publication_type','classification_quality','comments','created','modified');
    $csv->combine(@header);
    print FH_REF $csv->string()."\n";
    if (@references) {
        my $sql = 'SELECT p1.name authorizer, p2.name enterer, p3.name modifier, r.* '.
                  ' FROM refs r '.
                  ' LEFT JOIN person p1 ON p1.person_no=r.authorizer_no'.
                  ' LEFT JOIN person p2 ON p2.person_no=r.enterer_no'.
                  ' LEFT JOIN person p3 ON p3.person_no=r.modifier_no'.
                  ' WHERE r.reference_no IN ('.join(',',@references).')';
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        
        my $ref_count = 0;
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
        }
        my $ref_link = $http_dir."/references.csv";
        print "<p>$ref_count references were printed to <a href=\"$ref_link\">references.csv</a></p>";
    } else {
        print "<p>No references were printed to file</p>";
    }

    cleanOldGuestFiles();
}


# Gets invalid and valid names
#   distinguish by field "is_valid" (boolean)
#   if is_valid == false, then invalid_reason will be populated with the reason why:
#       can be "synonym of, recombined as, corrected as, replaced by" etc
# Gets immediate parent of taxa (at end, in separate query)
sub getTaxonomicNames {
    my $dbt = shift;
    my $http_dir = shift;
    my $dbh = $dbt->dbh;
    my %options = @_;
    
    my @where = ();
    
    if ($options{'taxon_no'}) {
        my $sql = "SELECT lft,rgt FROM taxa_tree_cache WHERE taxon_no=$options{taxon_no}";
        my @results = @{$dbt->getData($sql)};
        my $lft = $results[0]->{'lft'};
        my $rgt = $results[0]->{'rgt'};
        if (!$lft || !$rgt) {
            die "Error in DownloadTaxonomy::getTaxonomicNames, could not find $options{taxon_no} in taxa_tree_cache";
        }
        push @where, "(t.lft BETWEEN $lft AND $rgt)";
        push @where, "(t.rgt BETWEEN $lft AND $rgt)";
    }

    if ($options{'taxon_reference_no'}) {
        push @where, "a.reference_no=".int($options{'taxon_reference_no'});
    }

    if ($options{'taxon_pubyr'}) {
        my $sign = ($options{'taxon_pubyr_before_after'} eq 'before') ? '<=' 
                 : ($options{'taxon_pubyr_before_after'} eq 'exactly') ? '=' 
                                                                       : '>=';
        my $pubyr = int($options{'taxon_pubyr'});
        push @where, "IF(a.ref_is_authority='YES',r.pubyr $sign $pubyr AND r.pubyr REGEXP '[0-9]+',a.pubyr $sign $pubyr AND a.pubyr REGEXP '[0-9]+')";
    }

    if ($options{'taxon_author'}) {
        my $author = $dbh->quote($options{'taxon_author'});
        my $authorWild = $dbh->quote('%'.$options{'taxon_author'}.'%');
        push @where, "IF(a.ref_is_authority='YES',".
            "r.author1last LIKE $author OR r.author2last LIKE $author OR r.otherauthors LIKE $authorWild,". # If ref_is_authority, use ref
            "a.author1last LIKE $author OR a.author2last LIKE $author OR a.otherauthors LIKE $authorWild)"; # Else, use record itself
    }

    if ($options{'taxon_person_no'}) {
        if ($options{'taxon_person_type'} eq 'all') {
            my $p = $options{'taxon_person_no'};
            push @where, "(a.authorizer_no=$p OR a.enterer_no=$p OR a.modifier_no=$p)";
        } elsif ($options{'taxon_person_type'} eq 'enterer') {
            push @where, 'a.enterer_no='.int($options{'taxon_person_no'});
        } elsif ($options{'taxon_person_type'} eq 'modifier') {
            push @where, 'a.modifier_no='.int($options{'taxon_person_no'});
        } else { # defaults to authorizer
            push @where, 'a.authorizer_no='.int($options{'taxon_person_no'});
        }
    }

    if ($options{'taxon_created_year'}) {
        my ($yyyy,$mm,$dd) = ($options{'taxon_created_year'},$options{'taxon_created_month'},$options{'taxon_created_day'});
        my $date = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
        my $sign = ($options{'created_before_after'} eq 'before') ? '<=' : '>=';
        push @where,"a.created $sign $date";
    }

    # use between and both values so we'll use a key for a smaller tree;
    my @results;
    my $message;
    if (@where) {
        my $sql = "SELECT p1.name authorizer, p2.name enterer, p3.name modifier, tt.taxon_name type_taxon,"
                . "a.taxon_no,a.reference_no,a.taxon_rank,a.taxon_name,a.type_specimen,a.extant,"
                . "a.pages,a.figures,a.created,a.modified,a.comments,t.spelling_no,t.synonym_no senior_synonym_no,"
                . " IF (a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,"
                . " IF (a.ref_is_authority='YES',r.author1init,a.author1init) author1init,"
                . " IF (a.ref_is_authority='YES',r.author1last,a.author1last) author1last,"
                . " IF (a.ref_is_authority='YES',r.author2init,a.author2init) author2init,"
                . " IF (a.ref_is_authority='YES',r.author2last,a.author2last) author2last,"
                . " IF (a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,"
                . " DATE_FORMAT(a.modified,'%m/%e/%Y') modified_short "
                . " FROM taxa_tree_cache t, authorities a"
                . " LEFT JOIN person p1 ON p1.person_no=a.authorizer_no"
                . " LEFT JOIN person p2 ON p2.person_no=a.enterer_no"
                . " LEFT JOIN person p3 ON p3.person_no=a.modifier_no"
                . " LEFT JOIN authorities tt ON tt.taxon_no=a.type_taxon_no"
                . " LEFT JOIN refs r ON r.reference_no=a.reference_no"
                . " WHERE t.taxon_no=a.taxon_no"
                . " AND ".join(" AND ",@where)
                . " ORDER BY a.taxon_name";
        main::dbg("getTaxonomicNames called: ($sql)");
        @results = @{$dbt->getData($sql)};

        my ($valid_count,$invalid_count) = (0,0);
        my %parent_name_cache = ();
        my %taxa_cache = ();
        foreach my $row (@results) {
            $taxa_cache{$row->{'taxon_no'}} = $row;
            if ($parent_name_cache{$row->{'senior_synonym_no'}}) {
                $row->{'parent_name'} = $parent_name_cache{$row->{'senior_synonym_no'}}{'taxon_name'};
            }
            my $orig_no = TaxonInfo::getOriginalCombination($dbt,$row->{'senior_synonym_no'});
            my $parent = TaxonInfo::getMostRecentParentOpinion($dbt,$orig_no);
            if ($parent && $parent->{'parent_no'}) {
                my $sql = "SELECT a.taxon_no, a.taxon_name FROM taxa_tree_cache t, authorities a WHERE a.taxon_no=t.synonym_no AND t.taxon_no=$parent->{parent_no}";
                my @r = @{$dbt->getData($sql)};
                $row->{'parent_name'} = $r[0]->{'taxon_name'};
                $row->{'parent_no'} = $r[0]->{'taxon_no'};
                $parent_name_cache{$row->{'senior_synonym_no'}} = $r[0];
            }

            # If this is a recombination, then use the old combinations reference information
            my $orig_row = {};
            if ($taxa_cache{$orig_no}) {
                $orig_row = $taxa_cache{$orig_no};
            } elsif ($orig_no) {
                my $sql = "SELECT a.taxon_no,a.reference_no,a.taxon_rank,a.taxon_name,"
                        . " IF (a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,"
                        . " IF (a.ref_is_authority='YES',r.author1init,a.author1init) author1init,"
                        . " IF (a.ref_is_authority='YES',r.author1last,a.author1last) author1last,"
                        . " IF (a.ref_is_authority='YES',r.author2init,a.author2init) author2init,"
                        . " IF (a.ref_is_authority='YES',r.author2last,a.author2last) author2last,"
                        . " IF (a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors"
                        . " FROM authorities a LEFT JOIN refs r ON a.reference_no=r.reference_no"
                        . " WHERE a.taxon_no=$orig_no";
                my @r = @{$dbt->getData($sql)};
                $orig_row = $r[0];
                $taxa_cache{$orig_row->{'taxon_no'}} = $orig_row;
            }
            $row->{'original_taxon_name'} = $orig_row->{'taxon_name'};
            $row->{'original_taxon_no'} = $orig_row->{'taxon_no'};
            $row->{'original_taxon_rank'} = $orig_row->{'taxon_rank'};
            if ($orig_no != $row->{'taxon_no'}) {
                $row->{'author1init'} = $orig_row->{'author1init'};
                $row->{'author1last'} = $orig_row->{'author1last'};
                $row->{'author2init'} = $orig_row->{'author2init'};
                $row->{'author2last'} = $orig_row->{'author2last'};
                $row->{'otherauthors'} = $orig_row->{'otherauthors'};
                $row->{'pubyr'} = $orig_row->{'pubyr'};
                $row->{'reference_no'} = $orig_row->{'reference_no'};
            }
           
            if ($parent && $parent->{'status'} =~ /nomen/) {
                $row->{'is_valid'} = 0;
                $row->{'invalid_reason'} = $parent->{'status'};
            } elsif ($row->{'taxon_no'} != $row->{'senior_synonym_no'}) {
                $row->{'is_valid'} = 0;
                if ($row->{'spelling_no'} != $row->{'senior_synonym_no'}) {
                    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$row->{'taxon_no'});
                    my $parent = TaxonInfo::getMostRecentParentOpinion($dbt,$orig_no);
                    if ($parent && $parent->{'parent_no'}) {
                        my $sql = "SELECT taxon_name FROM authorities where taxon_no=$parent->{parent_no}";
                        my @s = @{$dbt->getData($sql)};
                        $row->{'invalid_reason'} = "$parent->{status} $s[0]->{taxon_name}";
                        $row->{'synonym_no'} = $parent->{'parent_no'}; 
                    } else {
                        my $sql = "SELECT taxon_name FROM authorities where taxon_no=$row->{senior_synonym_no}";
                        my @s = @{$dbt->getData($sql)};
                        $row->{'invalid_reason'} = "synonym of $s[0]->{taxon_name}";
                        $row->{'synonym_no'} = $row->{'senior_synonym_no'};
                    }
                } else {
                    my $sql = "SELECT taxon_name,taxon_rank FROM authorities where taxon_no=$row->{spelling_no}";
                    my @s = @{$dbt->getData($sql)};
                    my $spelling_reason = guessSpellingReason($row->{'taxon_name'},$row->{'taxon_rank'},$s[0]->{'taxon_name'},$s[0]->{'taxon_rank'});
                    $row->{'invalid_reason'} = "$spelling_reason $s[0]->{taxon_name}";
                }
                $invalid_count++;
            } else {
                $row->{'is_valid'} = 1;
                $valid_count++;
            }
        }
        my $it_link = $http_dir."/invalid_taxa.csv";
        my $vt_link = $http_dir."/valid_taxa.csv";
        $message .= "<p>$valid_count valid taxa were printed to <a href=\"$vt_link\">valid_taxa.csv</a></p>";
        $message .= "<p>$invalid_count invalid taxa were printed to <a href=\"$it_link\">invalid_taxa.csv</a></p>";
    } else {
        $message = "<p>No taxonomic names were downloaded because no search criteria were entered</p>";
    }
    
    return (\@results, $message);
}

sub guessSpellingReason {
    my ($taxon_name1,$taxon_rank1,$taxon_name2,$taxon_rank2) = @_;
    my $spelling_status;
    # For a recombination, the upper names will always differ. If they're the same, its a correction
    if ($taxon_rank1 =~ /species/) {
        my @childBits = split(/ /,$taxon_name1);
        my @spellingBits= split(/ /,$taxon_name2);
        pop @childBits;
        pop @spellingBits;
        my $childName = join(' ',@childBits);
        my $spellingName = join(' ',@spellingBits);
        if ($childName eq $spellingName) {
            # If the genus/subgenus/species names are the same, its a correction
            $spelling_status ='corrected as';
        } else {
            # If they differ, its a bad record or its a recombination
            $spelling_status ='recombined as';
        }
    } elsif ($taxon_rank1 ne $taxon_rank2) {
        $spelling_status = 'rank changed as';
    } else {
        $spelling_status = 'corrected as';
    } 
    return $spelling_status;
}

sub getTaxonomicOpinions {
    my $dbt = shift;
    my $http_dir = shift;
    my $dbh = $dbt->dbh;
    my %options = @_;
    
    my @where = ();
    
    if ($options{'taxon_no'}) {
        my $sql = "SELECT lft,rgt FROM taxa_tree_cache WHERE taxon_no=$options{taxon_no}";
        my @results = @{$dbt->getData($sql)};
        my $lft = $results[0]->{'lft'};
        my $rgt = $results[0]->{'rgt'};
        if (!$lft || !$rgt) {
            die "Error in DownloadTaxonomy::getTaxonomicOpinions, could not find $options{taxon_no} in taxa_tree_cache";
        }
        push @where, "(t.lft BETWEEN $lft AND $rgt)";
        push @where, "(t.rgt BETWEEN $lft AND $rgt)";
    }

    if ($options{'opinion_reference_no'}) {
        push @where, "o.reference_no=".int($options{'opinion_reference_no'});
    }

    if ($options{'opinion_pubyr'}) {
        my $sign = ($options{'opinion_pubyr_before_after'} eq 'before') ? '<=' 
                 : ($options{'opinion_pubyr_before_after'} eq 'exactly') ? '=' 
                                                                       : '>=';
        my $pubyr = int($options{'opinion_pubyr'});
        push @where, "IF(o.ref_has_opinion='YES',r.pubyr $sign $pubyr AND r.pubyr REGEXP '[0-9]+',o.pubyr $sign $pubyr AND o.pubyr REGEXP '[0-9]+')";
    }

    if ($options{'opinion_author'}) {
        my $author = $dbh->quote($options{'opinion_author'});
        my $authorWild = $dbh->quote('%'.$options{'opinion_author'}.'%');
        push @where, "IF(o.ref_has_opinion='YES',".
            "r.author1last LIKE $author OR r.author2last LIKE $author OR r.otherauthors LIKE $authorWild,". # If ref_is_authority, use ref
            "o.author1last LIKE $author OR o.author2last LIKE $author OR o.otherauthors LIKE $authorWild)"; # Else, use record itself
    }

    if ($options{'opinion_person_no'}) {
        if ($options{'opinion_person_type'} eq 'all') {
            my $p = $options{'opinion_person_no'};
            push @where, "(o.authorizer_no=$p OR o.enterer_no=$p OR o.modifier_no=$p)";
        } elsif ($options{'opinion_person_type'} eq 'enterer') {
            push @where, 'o.enterer_no='.int($options{'opinion_person_no'});
        } elsif ($options{'opinion_person_type'} eq 'modifier') {
            push @where, 'o.modifier_no='.int($options{'opinion_person_no'});
        } else { # defaults to authorizer
            push @where, 'o.authorizer_no='.int($options{'opinion_person_no'});
        }  
    }

    # use between and both values so we'll use a key for a smaller tree;
    my @results = ();
    my $message = "";
    if (@where) {
        my $sql = "(SELECT p1.name authorizer, p2.name enterer, p3.name modifier, "
                . "a1.taxon_name child_name, a2.taxon_name child_spelling_name, "
                . "a3.taxon_name parent_name, a4.taxon_name parent_spelling_name,"
                . "o.opinion_no,o.reference_no,o.status,o.child_no,o.child_spelling_no,o.parent_no,o.parent_spelling_no, "
                . "o.pages,o.figures,o.created,o.modified,o.comments,r.classification_quality,"
                . " IF (o.ref_has_opinion='YES',r.pubyr,o.pubyr) pubyr,"
                . " IF (o.ref_has_opinion='YES',r.author1init,o.author1init) author1init,"
                . " IF (o.ref_has_opinion='YES',r.author1last,o.author1last) author1last,"
                . " IF (o.ref_has_opinion='YES',r.author2init,o.author2init) author2init,"
                . " IF (o.ref_has_opinion='YES',r.author2last,o.author2last) author2last,"
                . " IF (o.ref_has_opinion='YES',r.otherauthors,o.otherauthors) otherauthors, "
                . " DATE_FORMAT(o.modified,'%m/%e/%Y') modified_short "
                . " FROM taxa_tree_cache t, opinions o"
                . " LEFT JOIN authorities a1 ON a1.taxon_no=o.child_no"
                . " LEFT JOIN authorities a2 ON a2.taxon_no=o.child_spelling_no"
                . " LEFT JOIN authorities a3 ON a3.taxon_no=o.parent_no"
                . " LEFT JOIN authorities a4 ON a4.taxon_no=o.parent_spelling_no"
                . " LEFT JOIN person p1 ON p1.person_no=o.authorizer_no"
                . " LEFT JOIN person p2 ON p2.person_no=o.enterer_no"
                . " LEFT JOIN person p3 ON p3.person_no=o.modifier_no"
                . " LEFT JOIN refs r ON r.reference_no=o.reference_no"
                . " WHERE t.taxon_no = o.child_no"
                . " AND ".join(" AND ",@where)
                . ") ORDER BY child_name,pubyr";
        main::dbg("getTaxonomicOpinions called: ($sql)");
        @results = @{$dbt->getData($sql)};
        my $op_link = $http_dir."/opinions.csv";
        
        $message .= "<p>".scalar(@results)." taxonomic opinions were printed to <a href=\"$op_link\">opinions.csv</a></p>";
    } else {
        $message .= "<p>No taxonomic opinions were downloaded because no search criteria were entered</p>";
    }
    return (\@results,$message);
}

sub getKingdomMap {
    my $dbt = shift;
    my %kingdom = ();

    my $sql = "SELECT taxon_no,taxon_name FROM authorities WHERE taxon_rank LIKE 'kingdom'";

    my @results = @{$dbt->getData($sql)};

    foreach my $row (@results) {
        my @children = TaxaCache::getChildren($dbt,$row->{'taxon_no'});
        foreach my $child (@children) {
            $kingdom{$child} = $row->{'taxon_name'};
        }
    }

    return %kingdom;
}


sub makeDataFileDir {
    my $s = shift;

    #  0    1    2     3     4    5     6     7     8
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time); 
    my $date = sprintf("%d%02d%02d",($year+1900),$mon,$mday);

    my $filesystem_dir;
    my $http_dir;
    if ($s->isDBMember()) {
        $filesystem_dir = $ENV{'DOWNLOAD_OUTFILE_DIR'}."/".$s->{'enterer'};
        $http_dir = "/paleodb/data/".$s->{'enterer'};
    } else {
        $filesystem_dir = $ENV{'DOWNLOAD_OUTFILE_DIR'}."/guest".$date."_".$$;
        $http_dir = "/paleodb/data/guest".$date."_".$$;
    }
    $filesystem_dir =~ s/[^a-zA-Z0-9_\/]//g;
    $http_dir =~ s/[^a-zA-Z0-9_\/]//g;
    umask '022';
    main::dbg("File dir is $filesystem_dir");
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

    my $filedir = $ENV{'DOWNLOAD_OUTFILE_DIR'};
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
