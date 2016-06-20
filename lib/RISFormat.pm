#
# Web::DataService::Text
# 
# This module is responsible for putting data responses into RIS format.
# Unlike most of the other format modules, it is useful only for one type of
# data: bibliographic references.
# 
# For more information, see: https://en.wikipedia.org/wiki/RIS_(file_format)
# 
# Author: Michael McClennen

use strict;

package RISFormat;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);



# emit_header ( request, field_list )
# 
# Generate the initial text that is necessary for an RIS format response.
# Because of the inflexibility of the RIS format, any extra header material
# must be included in the form of dummy reference entries.

sub emit_header {

    my ($class, $request, $field_list) = @_;
    
    my $output = '';
    my $linebreak = "\r\n";
    my $info = $request->data_info;
    my $base = $request->base_url;
    my $url_rest = $request->request_url;
    
    # Start by generating the initial lines that start any RIS document.
    
    $output .= "Provider: $info->{data_provider}$linebreak" if $info->{data_provider};
    $output .= "Database: $info->{data_source}$linebreak" if $info->{data_source};
    $output .= "Content: text/plain";
    
    my $ds = $request->ds;
    my $charset = $ds->{_config}{charset};
    
    $output .= "; charset=\"$charset\"" if $charset;
    
    $output .= $linebreak;
    $output .= $linebreak;
    
    # If we were directed to include the data source and/or record counts,
    # then we need to add some dummy reference entries of type 'GEN' (Generic) to
    # represent that information.
    
    if ( $request->display_datainfo )
    {
	$output .= $class->emit_line('TY', 'GEN');
	$output .= $class->emit_line('TI', 'Data Source');
	$output .= $class->emit_line('DP', $info->{data_provider});
	$output .= $class->emit_line('DB', $info->{data_source})
	    unless $info->{data_source} eq $info->{data_provider};
	$output .= $class->emit_line('AB', 'This entry provides the exact URL that was used to generate the contents',
				     'of this file, along with the access time and the parameter values.',
				     'The latter are expressed as keywords.');
	$output .= $class->emit_line('UR', $info->{data_url});
	$output .= $class->emit_line('Y2', $info->{access_time});
	
	my @params = $request->params_for_display;
	
	while ( @params )
	{
	    my $param = shift @params;
	    my $value = shift @params;
	    
	    next unless defined $param && $param ne '';
	    $value //= '';
	    
	    if ( ref $value eq 'ARRAY' )
	    {
		$value = join(', ', @$value);
	    }
	    
	    $output .= $class->emit_line('KW', "$param = $value");
	}
	
	$output .= $class->emit_line('ER');
	
	# We need a separate entry for the "Documentation", because each
	# entry is allowed only one URL field according to the sketchy
	# documentation of the RIS file format.
	
	$output .= $class->emit_line('TY', 'GEN');
	$output .= $class->emit_line('TI', 'Documentation');
	$output .= $class->emit_line('AB', 'This entry provides a URL describing the parameters accepted',
				     'and the output fields produced by the URL that',
				     'generated the contents of this file.');
	$output .= $class->emit_line('UR', $info->{documentation_url});
	$output .= $class->emit_line('ER');
    }
    
    # If we were directed to include the record counts, we need yet another
    # dummy entry to convey these values.
    
    if ( $request->display_counts )
    {
	my $counts = $request->result_counts;
	
	$output .= $class->emit_line('TY', 'GEN');
	$output .= $class->emit_line('TI', 'Record Counts');
	$output .= $class->emit_line('AB', 'This entry gives the number of records found.');
	$output .= $class->emit_line('KW', "Records Found = ", $counts->{found});
	$output .= $class->emit_line('KW', "Records Returned = ", $counts->{returned});
	$output .= $class->generate-line('KW', "Starting Index = ", $counts->{offset})
	    if defined $counts->{offset} && $counts->{offset} > 0;
    }
    
    return $output;
}


# emit_footer ( request )
# 
# The RIS format does not involve any text after the last record,
# so we just return the empty string.

sub emit_footer {

    return '';
}


# emit_empty ( request )
# 
# Just return the empty string.

sub emit_empty {

    return '';
}


# emit_record (request, record, field_list)
# 
# Return a series of text lines expressing the given record as a reference
# entry in RIS format.

sub emit_record {

    my ($class, $request, $record, $field_list) = @_;
    
    my $output = '';
    
    # Return empty unless we this record has some chance of representing a
    # reference.
    
    return $output unless ref $record && ($record->{r_al1} || $record->{r_pubtitle});
    
    # Grab some of the main fields.
    
    my $refno = $record->{reference_no};
    my $pubtype = $record->{r_pubtype} || '';
    my $reftitle = $record->{r_reftitle} || '';
    my $pubtitle = $record->{r_pubtitle} || '';
    my $pubyr = $record->{r_pubyr} || '';
    my $misc = '';
    
    # First, figure out what type of publication the reference refers to.
    # Depending upon publication type, generate the proper RIS data record.
    
    if ( $pubtype eq 'journal article' or $pubtype eq 'serial monograph' )
    {
	$output .= $class->emit_line('TY', 'JOUR');
    }
    
    elsif ( $pubtype eq 'book chapter' or ($pubtype eq 'book/book chapter' and 
					   defined $record->{editors} and $record->{editors} ne '' ) )
    {
	$output .= $class->emit_line('TY', 'CHAP');
    }
    
    elsif ( $pubtype eq 'book' or $pubtype eq 'book/book chapter' or $pubtype eq 'compendium' 
	    or $pubtype eq 'guidebook' )
    {
	$output .= $class->emit_line('TY', 'BOOK');
    }
    
    elsif ( $pubtype eq 'serial monograph' )
    {
	$output .= $class->emit_line('TY', 'SER');
    }
    
    elsif ( $pubtype eq 'Ph.D. thesis' or $pubtype eq 'M.S. thesis' )
    {
	$output .= $class->emit_line('TY', 'THES');
	$misc = $pubtype;
    }
    
    elsif ( $pubtype eq 'abstract' )
    {
	$output .= $class->emit_line('TY', 'ABST');
    }
    
    elsif ( $pubtype eq 'news article' )
    {
	$output .= $class->emit_line('TY', 'NEWS');
    }
    
    elsif ( $pubtype eq 'unpublished' )
    {
	$output .= $class->emit_line('TY', 'UNPD');
    }
    
    else
    {
	$output .= $class->emit_line('TY', 'GEN');
    }
    
    # The following fields are common to all types:
    
    $output .= $class->emit_line('ID', "ref:$refno");
    
    $output .= $class->ris_author_line('AU', $record->{r_al1}, $record->{r_ai1})
	if $record->{r_al1};
    $output .= $class->ris_author_line('AU', $record->{r_al2}, $record->{r_ai2})
	if $record->{r_al2};
    $output .= $class->ris_other_author_lines('AU', $record->{r_oa})
	if $record->{r_oa};
    $output .= $class->ris_other_author_lines('A2', $record->{r_editors})
	if $record->{r_editors};
    
    $output .= $class->ris_year_line('PY', $pubyr) if $pubyr > 0;
    $output .= $class->emit_line('TI', $reftitle);
    $output .= $class->emit_line('T2', $pubtitle);
    $output .= $class->emit_line('M3', $misc) if $misc;
    $output .= $class->emit_line('VL', $record->{r_pubvol}) if $record->{r_pubvol};
    $output .= $class->emit_line('IS', $record->{r_pubno}) if $record->{r_pubno};
    $output .= $class->emit_line('PB', $record->{r_publisher}) if $record->{r_publisher};
    $output .= $class->emit_line('CY', $record->{r_pubcity}) if $record->{r_pubcity};
    
    if ( defined $record->{r_refpages} and $record->{r_refpages} ne '' )
    {
	$output .= $class->emit_line('SP', $record->{r_refpages});
    }
    
    else
    {
	my $pages = '';
	$pages = $record->{r_fp} if defined $record->{r_fp} && $record->{r_fp} ne '';
	$pages .= '-' . $record->{r_lp} if defined $record->{r_lp} && $record->{r_lp} ne '';
	
	$output .= $class->emit_line('SP', $pages) if $pages ne '';
    }

    if ( defined $record->{r_comments} && $record->{r_comments} ne '' )
    {
	my @lines = split(/\n|\r|\r\n/, $record->{r_comments});

	foreach my $l ( @lines )
	{
	    $output .= $class->emit_line('N1', $l) if $l ne '';
	}
    }
    
    $output .= $class->emit_line('LA', $record->{r_language})
	if defined $record->{r_language} and $record->{r_language} ne '';
    
    $output .= $class->emit_line('DO', $record->{r_doi})
	if defined $record->{r_doi} and $record->{r_doi} ne '';

    if ( $request->{my_reftype} )
    {
	my @rt; my $type = $request->{my_reftype};
	
	push @rt, "taxa = $record->{n_reftaxa}" if $record->{n_reftaxa};
	
	push @rt, "auth = $record->{n_refauth}" if $record->{n_refauth} &&
	    ($type->{auth} || $type->{var} || $type->{taxonomy} || $type->{all});
	
	push @rt, "var = $record->{n_refvar}" if $record->{n_refvar} &&
	    ($type->{var} || $type->{all});
	
	push @rt, "class = $record->{n_refclass}" if $record->{n_refclass} &&
	    ($type->{ops} || $type->{class} || $type->{taxonomy} || $type->{all});
	
	push @rt, "unclass = $record->{n_refunclass}" if $record->{n_refunclass} &&
	    ($type->{ops} || $type->{all});
	
	push @rt, "occ = $record->{n_refoccs}" if $record->{n_refoccs} &&
	    ($type->{occs} || $type->{all});
	
	push @rt, "spec = $record->{n_refspecs}" if $record->{n_refspecs} &&
	    ($type->{specs} || $type->{all});

	push @rt, "coll = $record->{n_refcolls}" if $record->{n_refcolls} &&
	    ($type->{colls} || $type->{all});

	if ( @rt )
	{
	    my $rt = join(', ', @rt);
	    $output .= $class->emit_line('KW', $rt);
	}
    }
    
    elsif ( defined $record->{ref_type} && $record->{ref_type} ne '' )
    {
	my $rt = $record->{ref_type};
	$rt =~ s/,/, /g;
	
	$output .= $class->emit_line('KW', $rt);
    }
    
    $output .= $class->emit_line('ER');
    
    return $output;

}


# Generate an "author" line in RIS format, given a tag (which may be 'AU' for
# author, 'A2' for editor, etc.), and the three components of a name: last,
# first, and suffix.  The first and suffix may be null.

sub ris_author_line {
    
    my ($class, $tag, $last, $init, $suffix) = @_;
    
    $init ||= '';
    $init =~ s/ //g;
    $suffix ||= '';
    
    # If the last name includes a suffix, split it out
    
    if ( $last =~ /^(.*),\s*(.*)/ or $last =~ /^(.*)\s+(jr.?|sr.?|iii|iv)$/i or $last =~ /^(.*)\s+\((jr.?|sr.?|iii|iv)\)$/ )
    {
	$last = $1;
	$suffix = $2;
	if ( $suffix =~ /^([sSjJ])/ ) { $suffix = $1 . "r."; }
    }
    
    # Generate the appropriate line, depending upon which of the three components
    # are non-empty.
    
    if ( $suffix ne '' )
    {
	return "$tag  - $last,$init,$suffix\r\n";
    }
    
    elsif ( $init ne '' )
    {
	return "$tag  - $last,$init\r\n";
    }
    
    else
    {
	return "$tag  - $last\r\n";
    }
}


# Generate a "date" line in RIS format, given a tag and year, month and day
# values.  The month and day values may be null.  An optional "other" value
# may also be included, which can be arbitrary text.

sub ris_year_line {

    my ($class, $tag, $year, $month, $day, $other) = @_;
    
    my $date = sprintf("%04d", $year + 0) . "/";
    
    $date .= sprintf("%02d", $month + 0) if defined $month and $month > 0;
    $date .= "/";
    
    $date .= sprintf("%02d", $day + 0) if defined $day and $day > 0;
    $date .= "/";
    
    $date .= $other if defined $other;
    
    return "$tag  - $date\r\n";
}


# Generate one or more "author" lines in RIS format, given a tag and a value
# which represents one or more names separated by commas.  This is a bit
# tricky, because we need to split out name suffixes such as 'jr' and 'iii'.
# If we come upon something we can't handle, we generate a line whose value is
# 'PARSE ERROR'.

sub ris_other_author_lines {

    my ($class, $tag, $otherauthors) = @_;
    
    return unless $otherauthors;
    
    $otherauthors =~ s/^\s+//;
    
    my $init = '';
    my $last = '';
    my $suffix = '';
    my $output = '';
    
    while ( defined $otherauthors && $otherauthors =~ /[^,\s]/ )
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
	    
	    if ( defined $otherauthors && $otherauthors =~ /^(\w\w+\.?)(?:,\s+(.*))$/ )
	    {
		$suffix = $1;
		$otherauthors = $2;
	    }
	    
	    $output .= $class->ris_author_line($tag, $last, $init, $suffix);
	    $init = $last = $suffix = '';
	}
	
	else
	{
	    $output .= $class->emit_line($tag, "PARSE ERROR");
	    last;
	}
    }
    
    return $output;
}


# emit_line ( tag, value ... )
# 
# Generate an arbitrary line in RIS format, given a tag and a value.  The
# value may be empty.  If more than one value is given, they are joined by
# spaces.  If the tag is 'TY', which starts a new entry, then make sure it is
# preceded by a blank line.

sub emit_line {
    
    my ($class, $tag, @values) = @_;
    
    my $value = join ' ', @values;
    
    $value ||= '';
    $value =~ s/[\n\r].*//s;
    
    $tag = "\r\nTY" if $tag eq 'TY';
    
    return "$tag  - $value\r\n";
}


1;
