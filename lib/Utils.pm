

package ReferenceSources;

sub strcontent {

    my ($string, $start, $end) = @_;
    
    if ( $start =~ /^(\d+)..(\d+)$/ )
    {
	$start = $1;
	$end = $2;
    }

    $start ||= 0;
    $end ||= length($string) - 1;

    my $output = '';

    foreach my $i ( $start .. $end )
    {
	my $c = substr($string, $i, 1);
	my $l = $i < 10 ? " $i" : $i;
	my $o = ord($c);

	$output .= "$l '$c' $o\n";
    }

    return $output;
}


1;
