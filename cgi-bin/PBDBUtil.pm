package PBDBUtil;
use File::Path;
use strict;

use Debug qw(dbg);

# This contains various miscellaneous functions that don't belong anywhere
# else or haven't been moved out yet

## getResearchGroupSQL($dbt, $research_group)
# 	Description:    Returns an SQL snippet to filter all collections corresponding to a project or research group
#                   Assumes that the secondary_refs table has been left joined against the collections table
#	Parameters:	$dbt - DBTransactionManager object
#				$research_group - can be a research_group, project_name, or both
#               $restricted_to - boolean (default 0) - if st to 1 and input is a research group,
#                   restrict so it includes collections that belong to that research group and it alone
#                   not collections that might belong to it and others
#	Returns:	SQL snippet, to be appended with AND
##
sub getResearchGroupSQL {
	my $dbt = shift;
	my $research_group = shift;
    my $restricted_to = shift;

    my @terms = ();
    if($research_group =~ /^(?:decapod|divergence|ETE|5%|1%|PACED|PGAP)$/){
        my $sql = "SELECT reference_no FROM refs WHERE ";
        if ($restricted_to) {
            $sql .= " FIND_IN_SET(".$dbt->dbh->quote($research_group).",project_name)";
        } else {
            $sql .= " project_name=".$dbt->dbh->quote($research_group);
        }
        my @results = @{$dbt->getData($sql)};
        my $refs = join(", ",map {$_->{'reference_no'}} @results);
        $refs = '-1' if (!$refs );
        if ($restricted_to) {
            # In the restricted to case the collections research group is only looked
            # at for these overlapping cases
            if ($research_group !~ /^(?:decapod|divergence|ETE|PACED)$/) {
                push @terms, "c.reference_no IN ($refs)";
            }
        } else {
            push @terms, "c.reference_no IN ($refs)";
            push @terms, "sr.reference_no IN ($refs)";
        }
    } 
    if($research_group =~ /^(?:decapod|divergence|ETE|marine invertebrate|micropaleontology|PACED|paleobotany|paleoentomology|taphonomy|vertebrate)$/) {
        if ($restricted_to) {
            push @terms, "c.research_group=".$dbt->dbh->quote($research_group);
        } else {
            push @terms, "FIND_IN_SET( ".$dbt->dbh->quote($research_group).", c.research_group ) ";
        }
    } 

    my $sql_terms;
    if (@terms) {
        $sql_terms = "(".join(" OR ",@terms).")";  
    }
    return $sql_terms;
}

sub getMostRecentReIDforOcc {
	my $dbt = shift;
	my $occ = shift;
	my $returnTheRef = shift;

    my $sql = "SELECT re.*, r.pubyr FROM reidentifications re, refs r WHERE r.reference_no=re.reference_no AND re.occurrence_no=".int($occ)." ORDER BY r.pubyr DESC, re.reid_no DESC LIMIT 1";  

	my @results = @{$dbt->getData($sql)};

	if(scalar @results < 1){
		return "";
	} else {
		if($returnTheRef) {
			return $results[0];
		} else {
			return $results[0]->{'reid_no'};
		}
	}
}


# Generation of filenames standardized here to avoid security issues or
# potential weirdness. PS 3/6/2006
# If filetype == 1, use date/pid in randomizing filename.  Else use the ip
# Generally filetype == 1 is good, unless the files need to stick around and
# be reused for some reason (like in the download script)
sub getFilename {
    my $enterer = shift;
    my $filetype = shift;

    my $filename = "";
    if ($enterer eq '' || !$enterer) {
        if ($filetype == 1) {
            #  0    1    2     3     4    5     6     7     8
            my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
            my $date = sprintf("%d%02d%02d",($year+1900),$mon,$mday);
            $filename = "guest_".$date."_".$$;
        } else {
            my $ip = $ENV{'REMOTE_ADDR'}; 
            $ip =~ s/\.//g;
            #my @bits = split(/\./,$ip);
            #my $longip = ($bits[0] << 24) | ($bits[1] << 16) | ($bits[2] << 8) | ($bits[3]);
            $filename = $ip;
        }
    } else {
        #$enterer =~ s/['-]+/_/g;
        $enterer =~ s/[^a-zA-Z0-9_]//g;
        if (length($enterer) > 30) {
            $enterer = substr($enterer,0,30);
        }
        $filename = $enterer;
    }
    return $filename;
}


# pass this a number like "5" and it will return the name ("five").
# only works for numbers up through 19.  Above that and it will just return
# the original number.
#
sub numberToName {
    my $num = shift;

    my %numtoname = (  "0" => "zero", "1" => "one", "2" => "two",
                         "3" => "three", "4" => "four", "5" => "five",
                         "6" => "six", "7" => "seven", "8" => "eight",
                         "9" => "nine", "10" => "ten",
                         "11" => "eleven", "12" => "twelve", "13" => "thirteen",
                         "14" => "fourteen", "15" => "fifteen", "16" => "sixteen",
                         "17" => "seventeen", "18" => "eighteen", "19" => "nineteen");

    my $name;

    if ($num < 20) {
        $name = $numtoname{$num};
    } else {
        $name = $num;
    }

    return $name;
}   



# pass it an array ref and a scalar
# loops through the array to see if the scalar is a member of it.
# returns true or false value.
sub isIn {
    my $arrayRef = shift;
    my $val = shift;

    # if they don't exist
    if ((!$arrayRef) || (!$val)) {
        return 0;
    }

    foreach my $k (@$arrayRef) {
        if ($val eq $k) {
            return 1;
        }
    }

    return 0;
}
    

# Pass this an array ref and an element to delete.
# It returns a reference to a new array but *doesn't* modify the original.
# Does a string compare (eq)
sub deleteElementFromArray {
    my $ref = shift; 
    my $toDelete = shift;
    
    my @newArray;
    
    foreach my $element (@$ref) {
        if ($element ne $toDelete) {
            push(@newArray, $element);
        }
    }
    
    return \@newArray;
}   

# print Javascript to limit entry of time interval names
# WARNING: if "Early/Late Interval" is submitted but only "Interval"
#  is present in the intervals table, the submission will be rejected
# the CheckIntervalNames is used for form validation, the intervalNames is used
# for autocompletion.  They're slightly different in that checkIntervalNames is interested in
# fully qualified names (i.e. early X) while we don't care about the early/middle/late for the intervalNames
sub printIntervalsJava  {
    my $dbt = shift;
    my $include_ten_my_bins = shift;
    my $sql = "SELECT eml_interval,interval_name FROM intervals";
    my @results = @{$dbt->getData($sql)};
    
    my %intervals_seen;
    my $intervals = "";
    foreach my $row (@results)  {
        if (!$intervals_seen{$row->{'interval_name'}}) {
            $intervals .= "'$row->{interval_name}', ";
            $intervals_seen{$row->{'interval_name'}} = 1;
        }
    }
    $intervals =~ s/, $//;
                                                                                                                                                             
print <<EOF;
<script language="JavaScript" type="text/javascript">
<!-- Begin
function intervalNames() {
    var intervals = new Array($intervals);
    return intervals;
}

function checkIntervalNames(require_field) {
    var frm = document.forms[0];
    var badname1 = "";
    var badname2 = "";
    var alertmessage = "";
    var eml1 = frm.eml_max_interval.options[frm.eml_max_interval.selectedIndex].value;
    var time1 = frm.max_interval.value;
    var eml2 = frm.eml_min_interval.options[frm.eml_min_interval.selectedIndex].value;
    var time2 = frm.min_interval.value;
    var emltime1 = eml1 + time1;
    var emltime2 = eml2 + time2;
    
    var isInt = /^[0-9.]+\$/;
    if ( time1 == "" || isInt.test(time1))   {
        if (require_field) {
            var noname ="WARNING!\\n" +
                    "The maximum interval field is required.\\n" +
                    "Please fill it in and submit the form again.\\n" +
                    "Hint: epoch names are better than nothing.\\n";
            alert(noname);
            return false;
        } else {
            return true;
        }
    } 
EOF
    for my $i (1..2) {
        my $check = "    if(";
        for my $row ( @results) {
            # this is kind of ugly: we're just not going to let users
            #  enter a time term that has double quotes because that
            #  would break the JavaScript
            if ( $row->{'interval_name'} !~ /"/ )   {
                $check .= qq| emltime$i != "| . $row->{'eml_interval'} . $row->{'interval_name'} . qq|" &&\n|;
            }
        }
        if ($include_ten_my_bins) {
            my @binnames = TimeLookup::getBins();
            foreach my $binname (@binnames) {
                $check .= qq| emltime$i != "|.$binname. qq|" &&\n|;
            }
        }
        if ($i == 1) {
            chop($check); chop($check); chop($check);#remove trailing &&\n
        } else {
            $check .= qq|time$i != ""|;
        }
        $check .= ") {\n";
        $check .= "        badname$i += \"YES\";\n";
        $check .= "    }\n";
        print $check;
    }
print <<EOF;
                                                                                                                                                             
    if ( badname1 != "" || badname2 != "" ) {
        alertmessage = "WARNING!\\n";
    }
                                                                                                                                                             
    if ( badname1 != "" && badname2 != "" ) {
        alertmessage += eml1 + " " + time1 +
                        " and " + eml2 + " " + time2 +
                        " aren't official time terms.\\n";
        alertmessage += "Please correct them and submit the form again.\\n";
    } else if ( badname1 != "" ) {
        alertmessage += eml1 + " " + time1;
        alertmessage += " isn't an official time term.\\n" +
                        "Please correct it and submit the form again.\\n";
    } else if ( badname2 != "" ) {
        alertmessage += eml2 + " " + time2;
        alertmessage += " isn't an official time term.\\n" +
                        "Please correct it and submit the form again.\\n";
    }
    if ( alertmessage != "" ) {
        alertmessage += "Hint: try epoch names instead.";
        alert(alertmessage);
        return false;
    }
    return true;
}
// END -->
</script>
EOF
    return;
}

sub stripTags {
    my $s = shift;
    $s =~ s/<(?:[^>'"]*|(['"]).*?\1)*>//gs;
    $s =~ s/\[.*?\]//gs;
    $s =~ s/http:\/\/.*?(\s|$)//gs;
    return $s;
}

sub checkForBot {
    if ($ENV{'HTTP_USER_AGENT'} =~ /slurp|bot|spider|ask jeeves|crawl|archive|holmes|findlinks|webcopier|cfetch|stackrambler/i || $ENV{'REMOTE_ADDR'} =~ /^194.85./) {
        return 1;
    }
    return 0;
}

sub autoCreateDir {
    my $dir = shift;
    if (! -e $dir) {
        mkpath($dir);
    }
}

1;
