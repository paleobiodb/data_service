# CollectionData
# 
# A class that contains common routines for formatting and processing PBDB data.
# 
# Author: Michael McClennen



package CommonData;

use Web::DataService qw(:validators);

use strict;
use parent 'Exporter';

our (@EXPORT_OK) = qw(generateAttribution generateReference generateRISReference);



sub initialize {
    
    my ($class, $ds, $config, $dbh) = @_;
    
    $ds->define_ruleset('1.1:common_params' => 
	"The following parameters can be used with most requests:",
	{ optional => 'limit', valid => [POS_ZERO_VALUE, ENUM_VALUE('all')], 
	  error => "acceptable values for 'limit' are a positive integer, 0, or 'all'",
	  default => 500 },
	    "Limits the number of records returned.  The value may be a positive integer, zero, or C<all>.",
	    "It defaults to 500, in order to prevent people from accidentally sending requests that",
	    "might generate megabytes of data in response.  If you really want the entire result set,",
		"specify <limit=all>.",
	{ optional => 'offset', valid => POS_ZERO_VALUE },
	    "Returned records start at this offset in the result set.  The value may be a positive integer or zero.",
	{ optional => 'count', valid => FLAG_VALUE },
	    "If specified, then the response includes the number of records found and the number returned.",
	    "For more information about how this information is encoded, see the documentation pages",
	    "for the various response formats.",
	{ optional => 'vocab', valid => $ds->valid_vocab },
	    "Selects the vocabulary used to name the fields in the response.  You only need to use this if",
	    "you want to override the default vocabulary for your selected format.",
	    "Possible values depend upon the particular URL path, and include:", $ds->document_vocab,
	">The following parameters are only relevant to the text formats (.csv, .tsv, .txt):",
	{ optional => 'no_header', valid => FLAG_VALUE },
	    "If specified, then the header line which gives the field names is omitted.",
	{ optional => 'linebreak', valid => ENUM_VALUE('cr','crlf'), default => 'crlf' },
	    "Specifies the linebreak character sequence.",
	    "The value may be either 'cr' or 'crlf', and defaults to the latter.",
	{ ignore => 'splat' });

    $ds->define_block( '1.1:common:crmod' =>
      { select => ['$mt.created', '$mt.modified'] },
      { output => 'created', com_name => 'dcr' },
	  "The date and time at which this record was created.",
      { output => 'modified', com_name => 'dmd' },
	  "The date and time at which this record was last modified.");
    
}

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


# Given an object representing a PaleoDB reference, return a representation of
# that reference in RIS format (text).

sub formatRISRef {
    
    my ($dbt, $ref) = @_;
    
    return '' unless ref $ref;
    
    my $output = '';
    my $refno = $ref->{reference_no};
    my $pubtype = $ref->{publication_type};
    my $reftitle = $ref->{reftitle} || '';
    my $pubtitle = $ref->{pubtitle} || '';
    my $pubyr = $ref->{pubyr} || '';
    my $misc = '';
    
    # First, figure out what type of publication the reference refers to.
    # Depending upon publication type, generate the proper RIS data record.
    
    if ( $pubtype eq 'journal article' or $pubtype eq 'serial monograph' )
    {
	$output .= risLine('TY', 'JOUR');
    }
    
    elsif ( $pubtype eq 'book chapter' or ($pubtype eq 'book/book chapter' and 
					   defined $ref->{editors} and $ref->{editors} ne '' ) )
    {
	$output .= risLine('TY', 'CHAP');
    }
    
    elsif ( $pubtype eq 'book' or $pubtype eq 'book/book chapter' or $pubtype eq 'compendium' 
	    or $pubtype eq 'guidebook' )
    {
	$output .= risLine('TY', 'BOOK');
    }
    
    elsif ( $pubtype eq 'serial monograph' )
    {
	$output .= risLine('TY', 'SER');
    }
    
    elsif ( $pubtype eq 'Ph.D. thesis' or $pubtype eq 'M.S. thesis' )
    {
	$output .= risLine('TY', 'THES');
	$misc = $pubtype;
    }
    
    elsif ( $pubtype eq 'abstract' )
    {
	$output .= risLine('TY', 'ABST');
    }
    
    elsif ( $pubtype eq 'news article' )
    {
	$output .= risLine('TY', 'NEWS');
    }
    
    elsif ( $pubtype eq 'unpublished' )
    {
	$output .= risLine('TY', 'UNPD');
    }
    
    else
    {
	$output .= risLine('TY', 'GEN');
    }
    
    # The following fields are common to all types:
    
    $output .= risLine('ID', "paleodb:ref:$refno");
    $output .= risLine('DB', "Paleobiology Database");
    
    $output .= risAuthor('AU', $ref->{author1last}, $ref->{author1init})  if $ref->{author1last};
    $output .= risAuthor('AU', $ref->{author2last}, $ref->{author2init}) if $ref->{author2last};
    $output .= risOtherAuthors('AU', $ref->{otherauthors}) if $ref->{otherauthors};
    $output .= risOtherAuthors('A2', $ref->{editors}) if $ref->{editors};
    
    $output .= risYear('PY', $pubyr) if $pubyr > 0;
    $output .= risLine('TI', $reftitle);
    $output .= risLine('T2', $pubtitle);
    $output .= risLine('M1', $misc) if $misc;
    $output .= risLine('VL', $ref->{pubvol}) if $ref->{pubvol};
    $output .= risLine('IS', $ref->{pubno}) if $ref->{pubno};
    $output .= risLine('PB', $ref->{publisher}) if $ref->{publisher};
    $output .= risLine('CY', $ref->{pubcity}) if $ref->{pubcity};
    
    if ( defined $ref->{refpages} and $ref->{refpages} ne '' )
    {
	if ( $ref->{refpages} =~ /^(\d+)-(\d+)$/ )
	{
	    $output .= risLine('SP', $1);
	    $output .= risLine('EP', $2);
	}
	else
	{
	    $output .= risLine('SP', $ref->{refpages});
	}
    }
    
    else
    {
	$output .= risLine('SP', $ref->{firstpage}) if $ref->{firstpage};
	$output .= risLine('EP', $ref->{lastpage}) if $ref->{lastpage};
    }
    
    $output .= risLine('N1', $ref->{comments}) if defined $ref->{comments} and $ref->{comments} ne '';
    $output .= risLine('LA', $ref->{language}) if defined $ref->{language} and $ref->{language} ne '';
    $output .= risLine('DO', $ref->{doi}) if defined $ref->{doi} and $ref->{doi} ne '';
    
    $output .= risLine('ER');
    
    return $output;
}


# Generate an arbitrary line in RIS format, given a tag and a value.  The value
# may be empty.

sub risLine {
    
    my ($tag, $value) = @_;
    
    $value ||= '';
    $tag = "\nTY" if $tag eq 'TY';
    
    return "$tag  - $value\n";
}


# Generate an "author" line in RIS format, given a tag (which may be 'AU' for
# author, 'A2' for editor, etc.), and the three components of a name: last,
# first, and suffix.  The first and suffix may be null.

sub risAuthor {
    
    my ($tag, $last, $init, $suffix) = @_;
    
    $init ||= '';
    $init =~ s/ //g;
    $suffix ||= '';
    
    # If the last name includes a suffix, split it out
    
    if ( $last =~ /^(.*),\s*(.*)/ or $last =~ /^(.*)\s+(jr.?|iii|iv)$/i or $last =~ /^(.*)\s+\((jr.?|iii|iv)\)$/ )
    {
	$last = $1;
	$suffix = $2;
	if ( $suffix =~ /^([sSjJ])/ ) { $suffix = $1 . "r."; }
    }
    
    # Generate the appropriate line, depending upon which of the three components
    # are non-empty.
    
    if ( $suffix ne '' )
    {
	return "$tag  - $last,$init,$suffix\n";
    }
    
    elsif ( $init ne '' )
    {
	return "$tag  - $last,$init\n";
    }
    
    else
    {
	return "$tag  - $last\n";
    }
}


# Generate a "date" line in RIS format, given a tag and year, month and day
# values.  The month and day values may be null.  An optional "other" value
# may also be included, which can be arbitrary text.

sub risYear {

    my ($tag, $year, $month, $day, $other) = @_;
    
    my $date = sprintf("%04d", $year + 0) . "/";
    
    $date .= sprintf("%02d", $month + 0) if defined $month and $month > 0;
    $date .= "/";
    
    $date .= sprintf("%02d", $day + 0) if defined $day and $day > 0;
    $date .= "/";
    
    $date .= $other if defined $other;
    
    return "$tag  - $date\n";
}


# Generate one or more "author" lines in RIS format, given a tag and a value
# which represents one or more names separated by commas.  This is a bit
# tricky, because we need to split out name suffixes such as 'jr' and 'iii'.
# If we come upon something we can't handle, we generate a line whose value is
# 'PARSE ERROR'.

sub risOtherAuthors {

    my ($tag, $otherauthors) = @_;
    
    $otherauthors =~ s/^\s+//;
    
    my $init = '';
    my $last = '';
    my $suffix = '';
    my $output = '';
    
    while ( $otherauthors =~ /[^,\s]/ )
    {
	if ( $otherauthors =~ /^(\w\.)\s*(.*)/ )
	{
	    $init .= $1;
	    $otherauthors = $2;
	}
	
	elsif ( $otherauthors =~ /^([^,]+)(?:,\s+(.*))?/ )
	{
	    $last = $1;
	    $otherauthors = $2;
	    
	    if ( $otherauthors =~ /^(\w\w+\.?)(?:,\s+(.*))$/ )
	    {
		$suffix = $1;
		$otherauthors = $2;
	    }
	    
	    $output .= risAuthor($tag, $last, $init, $suffix);
	    $init = $last = $suffix = '';
	}
	
	else
	{
	    $output .= risLine($tag, "PARSE ERROR");
	    last;
	}
    }
    
    return $output;
}


1;
