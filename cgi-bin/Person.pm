package Person;
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD $PAGE_TOP $PAGE_BOTTOM);
use strict;
use Reference;

# Poling code calved off from displayLoginPage by JA 13.4.04
sub makeAuthEntJavascript {
    my $dbt = shift;

	####
	## We need to build a list of the enterers and authorizers for 
	## the java script to use for autocompletion.
	####
	my $authListRef = listOfAuthorizers($dbt);
	my $entListRef = listOfEnterers($dbt);
	
	my $authList;
	my $entList;
	foreach my $p (@$authListRef) {
		$authList .= "\"" . reverseName($p->{'name'}) . "\", ";  # reversed name
	}
	$authList =~ s/,\s*$//; # remove last comma and space if present.


	foreach my $p (@$entListRef) {
		$entList .= "\"" . reverseName($p->{'name'}) . "\", ";  # reversed name
	}
	$entList =~ s/,\s*$//; # remove last comma and space if present.

	my $javaScript = '<SCRIPT language="JavaScript" type="text/javascript">
	// returns an array of enterer names
	function entererNames() {
		var names = new Array(' . $entList . ');
		return names;
	}
		
	// returns an array of enterer names
	function authorizerNames() {
		var names = new Array(' . $authList . ');
		return names;
	} 
	</SCRIPT>
	';

	return $javaScript;
}

# can pass it an optional argument, activeOnly
# if true, then only return active authorizers.
sub listOfAuthorizers {
	my ($dbt,$active_only,$fossil_record_only) = @_;
	
	my $sql = "SELECT name, person_no FROM person WHERE FIND_IN_SET('authorizer',role)";
	if ($active_only) { 
		$sql .= " AND active=1 "; 
	} 
	if ($fossil_record_only) {
		$sql .= " AND fossil_record=1";
	} 
	$sql .= " ORDER BY last_name,first_name";

	return $dbt->getData($sql);
}


# can pass it an optional argument, activeOnly
# if true, then only return active enterers. all authorizers
# are counted as enterers as well
sub listOfEnterers {
	my ($dbt,$active_only,$fossil_record_only) = @_;
	
	my $sql = "SELECT name, person_no FROM person";
	if ($active_only) { 
		$sql .= " WHERE active=1 "; 
	}
	if ($active_only && $fossil_record_only) {
		$sql .= " AND fossil_record=1";
	} elsif ($fossil_record_only) {
		$sql .= " WHERE fossil_record=1";
	}
	$sql .= " ORDER BY last_name,first_name";

	return $dbt->getData($sql);
	
}

# Pass this an enterer or authorizer name (reversed or normal - doesn't matter)
# Returns a true value if the name exists in our database of people.
sub checkName {
    my $dbt = shift;
	my $name = shift;

    my $sql = "SELECT COUNT(*) as c FROM person WHERE name=".$dbt->dbh->quote($name);
	my $count = ${$dbt->getData($sql)}[0]->{'c'};	

	if ($count) { return 1; }
	else { return 0; }
}

# pass this a person number and it 
# will return the person's name
sub getPersonName {
    my $dbt = shift;
    my $num = shift;

    if (! $num) {
        return '';
    }
    my $result = ${$dbt->getData("SELECT name FROM person WHERE person_no=$num")}[0]->{'name'};

    return $result;
}

# a trivial function - reverse the order of the last name and initial
# If it was Sepkoski, J. before, now its J. Sepkoski
# Likewise J. Sepkoski will be reversed into Sepkoski, J.
sub reverseName {
    my $name = shift;
    if ($name =~ /,/) {
        $name =~ s/^\s*(.*)\s*,\s*(.*)\s*$/$2 $1/;
    } else {
        $name =~ s/^\s*(\w\.)\s*(.*)\s*$/$2, $1/;
    }
    return $name;
}

sub showEnterers {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "<div align=\"center\"><div class=\"pageTitle\">Data enterers</div></div>";
    $html .= "<p \"align=left\">The following students and research assistants have entered data into the Database.
    Institution names are placed in parentheses to indicate students who have since moved on.
    <i>IMPORTANT: if your e-mail address is not on this list and should be, please notify the database administrator.</a></i></p><br>";

    my $sql = "SELECT first_name,last_name,institution,email FROM person WHERE role IN ('student','technician') OR role IS NULL";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    $html .= '<table cellpadding="3" cellspacing="0" border="0" width="100%">';
    $html .= "<tr><th align=\"left\">Name</th><th align=\"left\">Institution</th><th align=\"left\">Email</th></tr>";
    for(my $i=0;$i<@results;$i++) {
        my $row = $results[$i];
        my $name = "$row->{first_name} $row->{last_name}";
        if ($i % 2 == 0) {
            $html .= "<tr class=\"darkList\">";
        } else {
            $html .= "<tr>";
        }
        my $email;
        if ($row->{'email'}) {
            $email = scramble("$row->{'email'}");
        }
        $html .= "<td>$name</td><td>$row->{institution}</td><td>$email</td></tr>";
    }
    $html .= "</table>";
    return $html;
}

sub showAuthorizers {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT first_name,last_name,institution,email FROM person,refs WHERE person_no=authorizer_no";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " GROUP BY authorizer_no ORDER BY last_name,first_name";
    my @results = @{$dbt->getData($sql)};

    my @firsthalf;
    my @secondhalf;
    for my $r ( 0..int($#results/2) )	{
        push @firsthalf , $results[$r];
    }
    for my $r ( int($#results/2)+1..$#results )	{
        push @secondhalf , $results[$r];
    }

    $html .= '<div align="center"><div class="pageTitle">Contributing researchers</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 1em; padding-right: 1em;"><p class="small">The following Database members have entered data and/or supervised data entry by their students. See also our list of <a href="$READ_URL?action=showInstitutions">contributing institutions</a>.</p></div>|;
    $html .= "\n<table><tr><td valign=\"top\" width=\"50%\">\n\n";
    $html .= formatAuthorizerTable(\@firsthalf);
    $html .= "\n</td><td valign=\"top\" width=\"50%\">\n";
    $html .= formatAuthorizerTable(\@secondhalf);
    $html .= "\n</td></tr></table>\n\n";

}

sub formatAuthorizerTable	{
    my $peopleref = shift;
    my @people = @{$peopleref};

    my $html .= '<table cellpadding="3" cellspacing="0" border="0">';
    for(my $i=0;$i<@people;$i++) {
        my $row = $people[$i];
        my $name = "$row->{first_name} $row->{last_name}";
        $html .= "<tr>";
        my $email;
        if ($row->{'email'}) {
            $email = scramble("$row->{'email'}");
            $email =  ", ".$email;
        }
        $html .= "<td class=\"tiny\" style=\"text-indent: -1em; padding-left: 1.5em; padding-right: 0.5em;\">$name, $row->{institution}$email</td></tr>\n";
    }
    $html .= "</table>";
    return $html;

}

sub scramble {
    my $email = shift;
    if ($email =~ /@/) {
        my ($part1,$part2) = split(/@/,$email);
        $part2 = reverse($part2);
        $part1 = reverse($part1);
        $part1 =~ s/'/\\'/g;
        $part2 =~ s/'/\\'/g;
        $email = "<script type=\"text/javascript\">descram('$part2','$part1')</script>";
    }  
    return $email;
}

# JA 22.12.07
sub showFeaturedAuthorizers	{
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT p.first_name,p.last_name,p.institution,p.country,p.homepage,p.photo,max(reference_no) AS max_ref FROM person p,person p2,refs r WHERE FIND_IN_SET('authorizer',p.role) AND p2.last_entry IS NOT NULL AND r.created>DATE_SUB( now(), INTERVAL 1 YEAR) AND p.person_no=authorizer_no AND p2.person_no=enterer_no GROUP BY enterer_no";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY p2.last_entry DESC LIMIT 12";
    my @results = @{$dbt->getData($sql)};
    @results = sort { $a->{'last_name'} cmp $b->{'last_name'} || $a->{'first_name'} cmp $b->{'first_name'} } @results;
    my @refnos;
    my %seen = ();
    my @newresults;
    for my $r ( @results )	{
        if ( ! $seen{$r->{'first_name'}." ".$r->{'last_name'}} && $#newresults < 11 )	{
            push @refnos , $r->{'max_ref'};
            $seen{$r->{'first_name'}." ".$r->{'last_name'}}++;
            push @newresults , $r;
        }
    }
    @results = @newresults;

    $sql = "SELECT * FROM refs WHERE reference_no IN (" . join(',',@refnos) . ")";
    my @refrefs = @{$dbt->getData($sql)};
    my %refdata;
    for my $r ( @refrefs )	{
        $refdata{$r->{'reference_no'}} = $r;
    }

    $html .= '<div align="center"><div class="pageTitle">Featured contributors</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 2.1em; padding-right: 2em; padding-bottom: 0em;"><p class="small">Here are some Database members who have entered data and/or supervised data entry recently. See also our full list of <a href="$READ_URL?action=showAuthorizers">contributors</a> and our list of <a href="$READ_URL?action=showInstitutions">contributing institutions</a>.</p></div>|;
    $html .= "<div class=\"small\" style=\"padding-left: 1em;\">\n";
    $html .= "<table><tr><td width=\"50%\" valign=\"top\">\n";
    for(my $i=0;$i<@results;$i++) {
        my $row = $results[$i];
        $html .= "<div class=\"displayPanel\" style=\"padding: 0.75em; border-right: 1px solid #909090; border-bottom: 1px solid #808080;\">";
        if ( $row->{'photo'} )	{
            $html .= '<div style="float: right; position: relative; top: 0px; right: 0px; padding-left: 1em; padding-bottom: 0.5em;"><img src="/public/mugshots/' . $row->{'photo'} . '"></div>';
        }
        my $name = "$row->{first_name} $row->{last_name}";
        if ( $row->{'homepage'} )	{
            $name = '<a href="http://' . $row->{'homepage'} . '">' . $name . '</a>';
        }
        $html .= "$name<br>\n";
        $html .= "$row->{'institution'}, $row->{'country'}<br>\n";
        my $longref = Reference::formatLongRef($refdata{$row->{'max_ref'}});
        # authorizer/enterer not needed
        $longref =~ s/ \[.*//;
        $html .= "<div class=\"verysmall\" style=\"padding-top: 0.3em;\"><i><a href=\"$READ_URL?action=displayReference&reference_no=$row->{'max_ref'}\">Latest reference:</a></i> " . $longref. "</div>";
        $html .= "<div style=\"clear: both;\"></div></div>\n";
        if ( $i == int( $#results / 2 ) )	{
            $html .= "</td><td width=\"50%\" valign=\"top\">\n";
        }
    }
    $html .= "</td></tr></table></div><br>\n";
    return $html;
}

# heavy rewrite by JA 9.9.11
sub showInstitutions {
    my ($dbt,$fossil_record_only) = @_;
    my $html = "";

    my $sql = "SELECT first_name,last_name,country,institution,email FROM person WHERE FIND_IN_SET('authorizer',role) AND last_entry IS NOT NULL AND institution IS NOT NULL AND institution!=''";
    if ($fossil_record_only) {
        $sql .= " AND fossil_record=1";
    }
    $sql .= " ORDER BY country,institution,last_name,first_name";
    my @rows = @{$dbt->getData($sql)};

    $html .= '<div align="center"><div class="pageTitle">Contributing institutions</div></div>';
    $html .= qq|<div align="center" style="text-align: left; padding-left: 2em; padding-right: 1em;"><p class="small"><a href="$READ_URL?action=showAuthorizers">Database members</a> who have contributed data or supervised data entry by students are affiliated with the following institutions.</p></div>|;

    $html .= "\n<table style=\"margin-left: 0em;\"><tr><td valign=\"top\" width=\"50%\">\n\n";
    my ($lastCountry,$lastInstitution,@names);
    for my $r ( @rows )	{
        if ( $r->{'institution'} ne $lastInstitution )	{
            if ( $lastInstitution )	{
                $html .= join(', ',@names)."</div>\n";
                @names = ();
            }
            if ( $r->{'country'} ne $lastCountry )	{
                if ( $lastCountry )	{
                    $html .= "</div>\n\n";
                }
                if ( $r->{'country'} eq "United States" )	{
                    $html .= "\n</td><td valign=\"top\" width=\"50%\">\n\n";
                    $html .= "<div>\n";
                }
                $html .= "<div class=\"displayPanel\" style=\"padding-top: 0.5em; padding-bottom: 0.5em;\">\n$r->{'country'}\n";
            }
            if ( $r->{'country'} ne $lastCountry )	{
                $lastCountry = $r->{'country'};
            }
            $html .= "<div class=\"tiny\" style=\"margin-top: 0.5em; text-indent: -1em; padding-left: 1.5em;\">\n";
            $html .= "$r->{'institution'}<br>\n";
            $lastInstitution = $r->{'institution'};
        }
        push @names , $r->{'first_name'}." ".$r->{'last_name'};
    }
    $html .= join(', ',@names)."</div>\n";
    $html .= "</div>\n</td></tr></table>\n\n";

    return $html;

}

# JA 1.1.09
sub homePageEntererList	{
	my $dbt = shift;
	my $html;
	my $sql = "SELECT first_name,last_name,institution FROM person WHERE hours IS NOT NULL ORDER BY hours DESC LIMIT 13";
	my @rows = @{$dbt->getData($sql)};
	for my $i ( 0..$#rows )	{
		my $r = $rows[$i];
		$r->{'institution'} =~ s/(University of )(.)(.*)(, |-| - )/U$2 /;
		$r->{'institution'} =~ s/Natural History Museum/NHM/;
                $r->{'institution'} =~ s/(Mus.*)(, .*)/$1/;
		$html .= "<div class=\"verysmall enteringNow\">$r->{'first_name'} $r->{'last_name'}<br>$r->{'institution'}</div>\n";
	}
	return $html;
}

# JA 22.9.11
sub publications	{
	my ($dbt,$q,$s,$hbo) = @_;
	my @pubs = @{$dbt->getData("SELECT * FROM pubs")};
	my @other_pubs = @{$dbt->getData("SELECT * FROM other_pubs ORDER BY last_names,initials,year")};
	my %vars;
	if ( $s->get('enterer') )	{
		$vars{'add_link'} = qq|<div class="verysmall" style="float: right; clear: both; margin-right: 3em;"><a href="?a=publicationForm&amp;new_entry=Y">add an entry</a></div>\n|;
	}
	$vars{'publications'} = formatPublications($s,\@pubs);
	$vars{'other_publications'} = formatPublications($s,\@other_pubs);
	$vars{'panel'} = ( $q->param('other_pub_no') > 0 ) ? "2" : "1";
	print $hbo->populateHTML('publications', \%vars);
}

sub formatPublications	{
	my ($s,$pubs) = @_;
	my @lines;
	for my $p ( @$pubs )	{
		my @authors;
		if ( $p->{'authors'} )	{
			@authors = split /, /,$p->{'authors'};
		} else	{
			my @inits = split /,/,$p->{'initials'};
			s/([A-Z])(\.)([A-Z])/$1. $3/g foreach @inits;
			my @lasts = split /,/,$p->{'last_names'};
			push @authors , $inits[$_]." ".$lasts[$_] foreach 0..$#inits;
		}
		my $authorlist = $authors[0];
		if ( $#authors > 1 )	{
			$authors[$#authors] = " and ".$authors[$#authors];
			$authorlist = join(', ',@authors);
		}
		elsif ( $#authors == 1 )	{
			$authorlist = $authors[0]." and ".$authors[1];
		}
		if ( $p->{'pub_no'} )	{
			$p->{'pub_no'} .= ".";
		}
		if ( ! $p->{'year'} )	{
			$p->{'year'} = "In press";
		}
		if ( $p->{'title'} !~ /\?$/ )	{
			$p->{'title'} .= ".";
		}
		my $editors;
		if ( $p->{'editors'} =~ /, | and / )	{
			$editors = "<i>In</i> ".$p->{'editors'}." (eds.), ";
		} elsif ( $p->{'editors'} )	{
			$editors = "<i>In</i> ".$p->{'editors'}." (ed.), ";
		}
		my $pages = ( $p->{'volume'} ) ? " ".$p->{'volume'} : "";
		my $first = $p->{'first_page'};
		my $last = $p->{'last_page'};
		if ( $p->{'volume'} && $first )	{
			$pages .= ":".$first;
		} elsif ( $first )	{
			$pages = ", pp. ".$first;
		}
		if ( $last )	{
			$pages .= "-".$last;
		}
		$pages .= ".";
		if ( $s->get('enterer') && $p->{'pub_no'} )	{
			$pages .= qq| <a href="?a=publicationForm&amp;pub_no=$p->{'pub_no'}">(edit)</a>|;
		} elsif ( $s->get('enterer') && $p->{'other_pub_no'} )	{
			$pages .= qq| <a href="?a=publicationForm&amp;other_pub_no=$p->{'other_pub_no'}">(edit)</a>|;
		}
		$p->{'doi'} = ( $p->{'doi'} ) ? " DOI: http://dx.doi.org/".$p->{'doi'} : "";
		my $extras = ( $p->{'extras'} ) ? " ".$p->{'extras'} : "";
		$extras .= ( $p->{'doi'} && $p->{'extras'} ) ? " &mdash; " : "";
		$extras .= $p->{'doi'};
		push @lines , '<p class="verysmall" style="margin-left: 1em; text-indent: -1em; margin-bottom: -0.8em;"/>'.$p->{'pub_no'}." $authorlist. ".$p->{'year'}.". ".$p->{'title'}." $editors <i>".$p->{'journal'}."</i>$pages$extras</p>\n";
	}
	$lines[$#lines] =~ s/margin-bottom: .*"/"/;
	return qq|<div style="float: left; clear: both;">\n|.join("\n",@lines)."</div>\n";
}

sub publicationForm	{
    my ($q,$dbt,$hbo) = @_;
    print $hbo->stdIncludes($PAGE_TOP);
    my $pub;
    if ( $q->param('pub_no') )	{
        $pub = ${$dbt->getData("SELECT * FROM pubs WHERE pub_no=".$q->param('pub_no'))}[0];
    } elsif ( $q->param('other_pub_no') )	{
        $pub = ${$dbt->getData("SELECT * FROM other_pubs WHERE other_pub_no=".$q->param('other_pub_no'))}[0];
    }
    my %vars;
    $vars{$_} = $pub->{$_} foreach ( 'pub_no','other_pub_no','authors','year','title','journal','editors','publisher','volume','no','first_page','last_page','first_page','last_page','doi','extras' );
    my @authors;
    if ( ! $vars{'authors'} && $pub->{'last_names'} )	{
        my @inits = split /,/,$pub->{'initials'};
        s/(\.)([A-Z])/. $2/g foreach @inits;
        s/^([A-Z])([A-Z])$/$1. $2./g foreach @inits;
        my @lasts = split /,/,$pub->{'last_names'};
        push @authors , $inits[$_]." ".$lasts[$_] foreach 0..$#lasts;
        $vars{'authors'} = join(', ',@authors);
    }
    $vars{'new_entry'} = $q->param('new_entry');
    print $hbo->populateHTML('publication_form', \%vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}

sub editPublication	{
    my ($q,$dbt) = @_;
    # author fields in official publication table pubs aren't editable because
    #  they should never change and users might completely screw up the entries
    #  if they were
    my @authors = split /, /,$q->param('authors');
    s/(,)( jr.)/$2/gi foreach @authors;
    my @inits = @authors;
    my @lasts = @authors;
    s/ [A-Z]['\-\p{Latin}]+//g foreach @inits;
    s/[A-Z](|\.\-[A-Z])\. //g foreach @lasts;
    s/'/\\'/g foreach @lasts;
    my (@updates,@values);
    my @fields = ( 'year','title','journal','editors','publisher','volume','no','first_page','last_page','doi','extras' );
    for my $f ( @fields )	{
        my $p = $q->param($f);
        $p =~ s/^( )$//;
        $p =~ s/'/\\'/g;
        $p = ( $p ) ? "'".$p."'" : "NULL";
        push @updates , "$f=$p";
        push @values , $p;
    }
    # note that the new_entry param is a pure sanity check
    if ( $q->param('pub_no') )	{
        $dbt->dbh->do("UPDATE pubs SET ".join(",",@updates)." WHERE pub_no=".$q->param('pub_no'));
    } elsif ( $q->param('other_pub_no') )	{
        push @updates , "initials='".join(',',@inits)."'";
        push @updates , "last_names='".join(',',@lasts)."'";
        $dbt->dbh->do("UPDATE other_pubs SET ".join(",",@updates)." WHERE other_pub_no=".$q->param('other_pub_no'));
    } elsif ( $q->param('new_entry') eq "Y" )	{
        push @fields , "initials,last_names";
        push @values, "'".join(',',@inits)."'";
        push @values , "'".join(',',@lasts)."'";
	 $dbt->dbh->do("INSERT INTO other_pubs(".join(',',@fields).") VALUES (".join(',',@values).")");
    }
    main::publications();
}



# end of Person.pm

1;
