package Collections;

use strict;

## sub createTimePlaceString($hash_ref)
#
#    description:  Creates a string representing the time and place for
#		   the collection
#
#    parameters:   $hash_ref is a reference to a hash which is a row of
#				   			 collection data.
#		   		   $class is the CSS class attribute for the size of the text
#
#    returns:	   the 'time - place' string.
##
sub createTimePlaceString{
    my $data_hash = shift;
    my $dbt = shift;
    my $class = shift or "tiny";

    my $timeplace = "";
#   my @timeterms = ( "locage", "intage", "epoch", "period" );

#   for my $tt (@timeterms){
#       if (!$timeplace){
#	    $timeplace = $data_hash->{$tt."_max"};
#	    if ( $data_hash->{$tt."_min"} && $data_hash->{$tt."_min"} ne $data_hash->{$tt."_max"} ){
#		# string could still be empty from missing the previous tt.
#		if($timeplace ne ""){
#		    $timeplace .= "/" . $data_hash->{$tt."_min"};
#		}
#		else{
#		    $timeplace = $data_hash->{$tt."_min"};
#		}
#	    }
#	}
#	if($timeplace){
#	    last;
#	}
#   }

    my $isql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $data_hash->{"max_interval_no"};
    my $intref = @{$dbt->getData($isql)}[0];
    my $interval_name = $intref->{'interval_name'};
    if ( $intref->{'eml_interval'} )	{
      $interval_name = $intref->{'eml_interval'} . " " . $interval_name;
    }
	
	my $interval_name2;
	
    my $isql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $data_hash->{"min_interval_no"};
    $intref =  @{$dbt->getData($isql)}[0];
    if ( $intref )	{
		$interval_name2 = $intref->{interval_name};
		if ( $intref->{'eml_interval'} )	{
			$interval_name2 = $intref->{'eml_interval'} . " " . $interval_name2;
		}
    }

    $timeplace .= $interval_name;
    if ( $interval_name2 )	{
    	$timeplace .= " - " . $interval_name2;
    }

    $timeplace .= "</span></td><td align=\"middle\" valign=\"top\"><span class=tiny>";

    $data_hash->{"country"} =~ s/ /&nbsp;/;
    $timeplace .= $data_hash->{"country"};
    if($data_hash->{"state"}){
	$data_hash->{"state"} =~ s/ /&nbsp;/;
	$timeplace = $timeplace . " (" . $data_hash->{"state"} . ")";
    }

    $timeplace = "</b> <span class=$class>" . $timeplace . "</span>\n";

    return $timeplace;
}

sub createCollectionDetailLink{
    my $exec_url = shift;
    my $coll_no = shift;
    my $link_text = shift;

    return "<a href=\"$exec_url?action=displayCollectionDetails&".
	   "collection_no=$coll_no\">$link_text</a>";
}

1;
