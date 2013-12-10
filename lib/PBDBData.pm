# CollectionData
# 
# A class that contains common routines for formatting and processing PBDB data.
# 
# Author: Michael McClennen



package PBDBData;

use strict;
use base 'Exporter';

our (@EXPORT_OK) = qw(generateAttribution generateReference);



# generateAttribution ( )
# 
# Generate an attribution string for the given record.  This relies on the
# fields "a_al1", "a_al2", "a_ao", and "a_pubyr".

sub generateAttribution {

    my ($self, $row) = @_;
    
    my $auth1 = $row->{a_al1} || '';
    my $auth2 = $row->{a_al2} || '';
    my $auth3 = $row->{a_ao} || '';
    my $pubyr = $row->{a_pubyr} || '';
    
    $auth1 =~ s/( Jr)|( III)|( II)//;
    $auth1 =~ s/\.$//;
    $auth1 =~ s/,$//;
    $auth2 =~ s/( Jr)|( III)|( II)//;
    $auth2 =~ s/\.$//;
    $auth2 =~ s/,$//;
    
    my $attr_string = $auth1;
    
    if ( $auth3 ne '' or $auth2 =~ /et al/ )
    {
	$attr_string .= " et al.";
    }
    elsif ( $auth2 ne '' )
    {
	$attr_string .= " and $auth2";
    }
    
    $attr_string .= " $pubyr" if $pubyr ne '';
    
    if ( $attr_string ne '' )
    {
	$attr_string = "($attr_string)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && defined $row->{taxon_no} && 
		$row->{orig_no} != $row->{taxon_no};
	
	return $attr_string;
    }
    
    return;
}


# generateReference ( )
# 
# Generate a reference string for the given record.  This relies on the
# fields "r_al1", "r_ai1", "r_al2", "r_ai2", "r_ao", "r_pubyr", "r_reftitle",
# "r_pubtitle", "r_pubvol", "r_pubno".
# 

sub generateReference {

    my ($self, $row) = @_;
    
    # First format the author string.  This includes stripping extra periods
    # from initials and dealing with "et al" where it occurs.
    
    my $ai1 = $row->{r_ai1} || '';
    my $al1 = $row->{r_al1} || '';
    
    $ai1 =~ s/\.//g;
    $ai1 =~ s/([A-Za-z])/$1./g;
    
    my $auth1 = $ai1;
    $auth1 .= ' ' if $ai1 ne '' && $al1 ne '';
    $auth1 .= $al1;
    
    my $ai2 = $row->{r_ai2} || '';
    my $al2 = $row->{r_al2} || '';
    
    $ai2 =~ s/\.//g;
    $ai2 =~ s/([A-Za-z])/$1./g;
    
    my $auth2 = $ai2;
    $auth2 .= ' ' if $ai2 ne '' && $al2 ne '';
    $auth2 .= $al2;
    
    my $auth3 = $row->{r_ao} || '';
    
    $auth3 =~ s/\.//g;
    $auth3 =~ s/\b(\w)\b/$1./g;
    
    # Then construct the author string
    
    my $authorstring = $auth1;
    
    if ( $auth2 =~ /et al/ )
    {
	$authorstring .= " $auth2";
    }
    elsif ( $auth2 ne '' && $auth3 ne '' )
    {
	$authorstring .= ", $auth2";
	if ( $auth3 =~ /et al/ )
	{
	    $authorstring .= " $auth3";
	}
	else
	{
	    $authorstring .= ", and $auth3";
	}
    }
    elsif ( $auth2 )
    {
	$authorstring .= " and $auth2";
    }
    
    # Now start building the reference with authorstring, publication year,
    # reference title and publication title
    
    my $longref = $authorstring;
    
    if ( $authorstring ne '' )
    {
	$longref .= '.' unless $authorstring =~ /\.$/;
	$longref .= ' ';
    }
    
    my $pubyr = $row->{r_pubyr} || '';
    
    if ( $pubyr ne '' )
    {
	$longref .= "$pubyr. ";
    }
    
    my $reftitle = $row->{r_reftitle} || '';
    
    if ( $reftitle ne '' )
    {
	$longref .= $reftitle;
	$longref .= '.' unless $reftitle =~ /\.$/;
	$longref .= ' ';
    }
    
    my $pubtitle = $row->{r_pubtitle} || '';
    my $editors = $row->{r_editors} || '';
    
    if ( $pubtitle ne '' )
    {
	my $pubstring = "<i>$pubtitle</i>";
	
	if ( $editors =~ /,| and / )
	{
	    $pubstring = " In $editors (eds.), $pubstring";
	}
	elsif ( $editors )
	{
	    $pubstring = " In $editors (ed.), $pubstring";
	}
	
	$longref .= $pubstring . " ";
    }
    
    # Now add volume and page number information if available
    
    my $pubvol = $row->{r_pubvol} || '';
    my $pubno = $row->{r_pubno} || '';
    
    if ( $pubvol ne '' || $pubno ne '' )
    {
	$longref .= '<b>';
	$longref .= $pubvol if $pubvol ne '';
	$longref .= "($pubno)" if $pubno ne '';
	$longref .= '</b>';
    }
    
    my $fp = $row->{r_fp} || '';
    my $lp = $row->{r_lp} || '';
    
    if ( ($pubvol ne '' || $pubno ne '') && ($fp ne '' || $lp ne '') )
    {
	$longref .= ':';
	$longref .= $fp if $fp ne '';
	$longref .= '-' if $fp ne '' && $lp ne '';
	$longref .= $lp if $lp ne '';
    }
    
    return $longref if $longref ne '';
    return;
}


1;
