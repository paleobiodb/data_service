#
# DataQuery
# 
# A base class that implements a data service for the PaleoDB.  This can be
# subclassed to produce any necessary data service.  For examples, see
# TaxonQuery.pm and CollectionQuery.pm. 
# 
# Author: Michael McClennen

package DataQuery;

use strict;

use JSON;
use Encode;


our ($DEFAULT_RESULT_LIMIT) = 500;	# Default limit on the number of
                                        # results, unless overridden by the
                                        # query parameter "limit=all".

our ($STREAM_THRESHOLD) = 102400;	# If the result is above this size,
                                        # and the server is capable of
                                        # streaming, then it will be streamed.


# new ( dbh, version )
# 
# Generate a new query object, using the given database handle and version
# label.  The usual version labels are 'single' and 'multiple'.  These
# indicate whether the query should return a single record or multiple
# records.

sub new {
    
    my ($class, $dbh, $version) = @_;
    my $self = { dbh => $dbh, version => $version };
    
    return bless $self, $class;
}


# describeParameters ( )
# 
# Returns a message describing the available parameters for this query.
# Subclasses should not override this method; rather, they should define the
# package variable $PARAM_DESC (or $PARAM_DESC_<VERSION>) which this routine
# will make use of.

sub describeParameters {
    
    my ($self) = @_;
    
    no strict 'refs';
    
    my $v1 = ref($self) . '::PARAM_DESC_' . uc($self->{version});
    my $v2 = ref($self) . '::PARAM_DESC';
    
    my $desc = $$v1 || $$v2;
    
    $desc .= "\n  limit - return at most the specified number of records (positive integer or 'all') - defaults to $DataQuery::DEFAULT_RESULT_LIMIT if not specified";
    
    $desc .= "\n  count - (boolean) return two additional fields along with the record set: 'records_found' = total number of records found, 'records_returned' = number actually returned" unless $self->{version} eq 'single';
    
    return $desc;
}


# describeRequirements ( )
# 
# Returns a message describing the parameter requirements for this query.
# Subclasses should not override this method; rather, they should define the
# package variable $PARAM_REQS (or $PARAM_REQS_<VERSION>) which this routine
# will make use of.

sub describeRequirements {
    
    my ($self) = @_;
    
    no strict 'refs';
    
    my $v1 = ref($self) . '::PARAM_REQS_' . uc($self->{version});
    my $v2 = ref($self) . '::PARAM_REQS';
    
    return $$v1 || $$v2;
}


# setParameters ( params )
# 
# This method is designed to be overridden by subclasses and then called via
# SUPER.  It accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.
# 
# This method handles only parameters that are common to this class.

sub setParameters {

    my ($self, $params) = @_;
    
    # The 'limit' parameter, if given, limits the size of the result set.  If
    # not specified, a default limit of $DEFAULT_RESULT_LIMIT will be used.
    # If 'all' is specified as the value, then the full result set will be
    # returned no matter how large it is.
    
    if ( defined $params->{limit} )
    {
	if ( $params->{limit} eq 'all' )
	{
	    delete $self->{limit_results};	# remove any limit that might
                                                # have been in place
	}
	
	elsif ( $params->{limit} >= 0 )
	{
	    $self->{limit_results} = $params->{limit} + 0;
	}
	
	else
	{
	    die "400 The parameter 'limit' takes a nonnegative integer or the string 'all'\n";
	}
    }
    
    else
    {
	$self->{limit_results} = $DEFAULT_RESULT_LIMIT;
    }
    
    # The 'count' parameter only makes sense with a JSON-format result.  If
    # true, it causes an extra piece of information to be returned:
    # 'records_found', which gives the total number of records found
    # regardless of any limit on the number returned.  Because of the 'limit'
    # parameter, the number of records actually returned may be less than this
    # number.
    
    if ( exists $params->{count} )
    {
	$self->{report_count} = 1;
    }
    
    # The undocumented parameter 'stream' allows for debugging streamed data
    # responses.  Value should be 'yes' or 'no'.
    
    $self->{should_stream} = '';
    
    if ( $params->{stream} )
    {
	$self->{should_stream} = $params->{stream};
    }
    
    # The 'ct' parameter sets the output format.  This must be specified in
    # all cases.  Depending on the content type, we select the appropriate set
    # of routines for generating the response body from the query result.
    
    if ( defined $params->{ct} )
    {
	my $ct = lc $params->{ct};
	
	if ( $ct eq 'xml' ) {
	    $self->{output_format} = 'xml';
	}
	
	elsif ( $ct eq 'json' ) {
	    $self->{output_format} = 'json';
	}
	
	else
	{
	    die "415 The output format must be one of 'xml' or 'json'\n";
	}
    }
}


# checkParameters ( params, good_params )
# 
# Make sure that all of the keys in %$params are valid for this query.
# $good_params should be a hash whose keys are parameters understood by the
# calling script or application.

sub checkParameters {
    
    my ($self, $params, $good_params) = @_;
    
    return unless ref $params eq 'HASH';
    
    # Construct our initial check list.
    
    my (@good_list) = {limit => 1, count => 1};
    unshift @good_list, $good_params if ref $good_params eq 'HASH';
    
    # If this object is actually in a subclass, look for a variable in that
    # package called $PARAM_CHECK.  If there is one, and it is a hashref, add
    # it to @good_list.
    
    {
	no strict 'refs';
	my $v1 = ref($self) . '::PARAM_CHECK';
	push @good_list, $$v1 if ref $$v1 eq 'HASH';
    }
        
    # Now check each parameter to make sure it falls into at least one of the
    # good-key hashes.
    
 key:
    foreach my $key (keys %$params)
    {
    hash:
	foreach my $hash (@good_list)
	{
	    next hash unless ref $hash eq 'HASH';
	    next key if exists $hash->{$key};
	}
	
	$self->{warnings} = [] unless defined $self->{warnings};
	push @{$self->{warnings}}, "ignored unknown parameter '$key'";
    }
}


# countRecords ( )
# 
# Return the result count of the main query, regardless of any limit on the
# number of records returned.  This routine should only be called after all of
# the results have been fetched, so that we have an accurate count.

sub countRecords {

    my ($self) = @_;
    
    # If there is no limit, or the number of records fetched is less than the
    # limit, then we just report the number of records actually fetched.
    
    unless ( defined $self->{limit_results} and
	     $self->{row_count} == $self->{limit_results} )
    {
	return $self->{row_count};
    }
    
    # If there is a limit, and the number of rows fetched equals that limit,
    # then there might be more rows that matched the query.  So in this case
    # we need to execute the count statement previously stashed by
    # fetchMultiple().
    
    my ($dbh) = $self->{dbh};
    
    my ($result_count) = $dbh->selectrow_array($self->{count_sql});
    return $result_count;
}


# generateSingleResult ( )
# 
# Return the query result formatted as a single string according to the output
# format previously set by setOutputFormat().

sub generateSingleResult {

    my ($self) = @_;
    
    $self->{row_count} = 1;
    
    return $self->generateHeader() . $self->generateRecord($self->{main_row}, 1) .
	    $self->generateFooter();
}


# generateCompoundResult ( options )
# 
# Return the query result formatted as a single string according to the output
# format previously set by setOutputFormat().  The only option available at
# this time is:
# 
#   can_stream => 1
# 
# This option can be used to inform this function that it should stream the
# result if the result is large.  This option should only be used if the
# server does in fact support streaming.  If the size of the result will
# exceed $STREAM_THRESHOLD characters, the result computed so far is stashed
# in the query object and the function returns false.  The calling application
# can then setup a call to streamResult to send the stashed data followed by
# the rest of the data as it is retrieved from the database.

sub generateCompoundResult {

    my ($self, %options) = @_;
    
    my $sth = $self->{main_sth};
    my $output = '';
    my $is_first_row = 1;
    my $row;
    
    $self->{row_count} = 0;
    
    # We call the initOutput method first, in case the query class has
    # overridden it.  Anything it returns will get prepended to the output.
    
    my $initial = $self->initOutput();
    $output .= $initial if defined $initial;
    
    # Now fetch the data rows one at a time.
    
    while ( $row = $sth->fetchrow_hashref )
    {
	# For each row, we start by calling the processRecord method (in case
	# the query class has overridden it) and then call generateRecord to
	# generate the actual output.
	
	$self->processRecord($row);
	my $row_output = $self->generateRecord($row, is_first => $is_first_row);
	$output .= $row_output;
	
	$is_first_row = 0;
	$self->{row_count}++;
	
	# If streaming is a possibility, check whether we have passed the
	# threshold for result size.  If so, then we need to immediately
	# generate the header and stash it along with the output so far.  We
	# then return false, which should lead to a subsequent call to
	# streamResult().
	
	if ( defined $options{can_stream} and $self->{should_stream} ne 'no' and
	     (length($output) > $STREAM_THRESHOLD or $self->{should_stream} eq 'yes') )
	{
	    #PBDB_Data::debug_msg('STREAMING');
	    #PBDB_Data::debug_msg($output);
	    $self->{stashed_output} = $self->generateHeader(streamed => 1) . $output;
	    return;
	}
    }
    
    my $final = $self->finishOutput();
    $output .= $final if defined $final;
    
    return $self->generateHeader() . $output . $self->generateFooter();
}


# streamResult ( )
# 
# Continue to generate a compound query result from where
# generateCompoundResult() left off, and stream it to the client record by
# record.  This routine should only be called if generateCompoundResult()
# returned false.
# 
# This routine must be passed a Plack 'writer' object, to which will be
# written in turn the stashed output from generateCompoundResult(), each
# subsequent record, and then the footer.  Each of these chunks of data will
# be immediately sent off to the client, instead of being marshalled together
# in memory.  This allows the server to send results up to hundreds of
# megabytes in length without bogging down.

sub streamResult {
    
    my ($self, $writer) = @_;
    
    my $sth = $self->{main_sth};
    my $row;
    
    PBDB_Data::debug_msg("STREAMING");
    
    # First send out the partial output previously stashed by
    # generateCompoundResult().
    
    $writer->write( encode_utf8($self->{stashed_output}) );
    
    # Then generate the remaining output.  We don't have to worry about
    # 'is_first', because we know that we're past the first row already.
    
    while ( $row = $sth->fetchrow_hashref )
    {
	$self->processRecord($row);
	$self->{row_count}++;
	my $output = $self->generateRecord($row);
	$writer->write( encode_utf8($output) ) if defined $output and $output ne '';
    }
    
    # Call the finishOutput() method, and send whatever if returns (if
    # anything).
    
    my $final = $self->finishOutput();
    $writer->write( encode_utf8($final) ) if defined $final and $final ne '';
    
    # Finally, send out the footer and then close the writer object.
    
    my $footer = $self->generateFooter(streamed => 1);
    $writer->write( encode_utf8($footer) ) if defined $footer and $footer ne '';
    $writer->close();
}


# generateHeader ( )
# 
# Generate the proper header for the requested output format.

sub generateHeader {
    
    my ($self, @options) = @_;
    
    if ( $self->{output_format} eq 'json' )
    {
	return $self->generateHeaderJSON(@options);
    }
    
    else
    {
	return $self->generateHeaderXML(@options);
    }
}


# generateHeaderXML ( )
# 
# Return the proper header for an XML document in Darwin Core format.  We
# ignore any options, because XML/Darwin Core is such a rigid format that we don't
# have much ability to customize it.

sub generateHeaderXML {

    my ($self) = @_;
    
    return <<END_XML;
<?xml version="1.0" standalone="yes"?>
<dwr:DarwinRecordSet xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dwr="http://rs.tdwg.org/dwc/dwcrecord/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://rs.tdwg.org/dwc/dwcrecord/ http://rs.tdwg.org/dwc/xsd/tdwg_dwc_classes.xsd">
END_XML

}


# generateHeaderJSON ( options )
# 
# Return the proper header for a JSON result.  The accepted options are:
# 
#   streamed - if true, ignore report_count.  We don't yet know how many
#		records there will be, so we have to wait and let
#		generateFooterJSON() report this information.

sub generateHeaderJSON {

    my ($self, %options) = @_;
    
    my $json = JSON->new->allow_nonref;
    
    my $output = '{' . "\n";
    
    if ( defined $self->{warnings} and $self->{warnings} > 0 )
    {
	$output .= '"warnings":' . $json->encode($self->{warnings}) . ",\n";
    }
    
    if ( defined $self->{object_class} )
    {
	$output .= '"class":' . $json->encode($self->{object_class}) . ",\n";
    }
    
    if ( defined $self->{report_count} and not $options{streamed} )
    {
	$output .= '"records_found":' . $json->encode($self->countRecords + 0) . ",\n";
	$output .= '"records_returned":' . $json->encode($self->{row_count} + 0) . ",\n";
    }
    
    $output .= '"records": [';
    return $output;
}


# generateFooter ( )
# 
# Generate the proper footer for the requested output format.

sub generateFooter {
    
    my ($self, @options) = @_;
    
    if ( $self->{output_format} eq 'json' )
    {
	return $self->generateFooterJSON(@options);
    }
    
    else
    {
	return $self->generateFooterXML(@options);
    }
}


# generateFooterXML ( )
# 
# Return the proper footer for an XML document in Darwin Core format.

sub generateFooterXML {
    
    my ($self) = @_;
    
    my $output = <<END_XML;
</dwr:DarwinRecordSet>
END_XML

    if ( defined $self->{report_count} )
    {
	$output .= "<!-- records_found: " . $self->countRecords . " -->\n";
	$output .= "<!-- records_returned: " . $self->{row_count} . " -->\n";
    }
    
    return $output;
}


# generateFooterJSON ( )
# 
# Return a string that will be valid as the end of a JSON result, after the
# header and zero or more records.  If the option "streamed" is given, then
# add in the record count.

sub generateFooterJSON {
    
    my ($self, %options) = @_;
    
    my $json = JSON->new->allow_nonref;
    my $output = "\n]";
    
    if ( defined $self->{report_count} and $options{streamed} )
    {
	$output .= ",\n" . '"records_found":' . $json->encode($self->countRecords + 0);
	$output .= ",\n" . '"records_returned":' . $json->encode($self->{row_count} + 0);
    }
    
    $output .= "\n}\n";
    
    return $output;
}


# initOutput ( )
# 
# This method is intended to be overridden.  It is called before the first
# record is generated, and if it returns a string that string is appended to
# the output just after the header and before the first record.

sub initOutput {

}


# finishOutput ( )
# 
# This method is intended to be overridden.  It is called after the last
# record is generated, and if it returns a string that string is appended to
# the output just after the last record and before the header.

sub finishOutput {

}


# processRecord ( )
# 
# This method is intended to be overridden.  It is called once for each row,
# after the row is fetched but before the row's output is generated.  The row
# hash is given as the first argument.

sub processRecord {

}


# Following are some utility routines.

# xml_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of an XML document.

my (%ENTITY) = ( '&' => '&amp;', '<' => '&lt;', '>', '&gt;' );

sub xml_clean {
    
    my ($string, $preserve_tags) = @_;
    
    # Do nothing unless our first argument is defined
    
    return unless defined $string;
    
    # First, turn any numeric character references into actual Unicode
    # characters.  The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Next, rename any instance of & that doesn't start a valid character
    # entity reference to &amp; and rename all < and > to &lt; and &gt;
    
    $string =~ s/(&(?!\w+;)|<|>)/$ENTITY{$1}/ge;
    
    # Finally, delete all control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them) as well as invalid utf-8 characters.
    
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
}


# json_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of a JSON result.

my (%ESCAPE) = ( '\\' => '\\\\', '"' => '\\"', "\t" => '\\t', "\n" => '\\n',
		 "\r" => '\\r' );	#'

sub json_clean {
    
    my ($string) = @_;
    
    # Do nothing unless our first argument is defined
    
    return unless defined $string;
    
    # First, turn any numeric character references into actual Unicode
    # characters.  The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Next, escape all backslashes, double-quotes and whitespace control characters
    
    $string =~ s/(\\|\"|\n|\t|\r)/$ESCAPE{$1}/ge;
    
    # Finally, delete all other control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them).
    
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
}


# generateAttribution ( )
# 
# Generate an attribution string for the given record.  This relies on the
# fields "a_al1", "a_al2", "a_ao", and "a_pubyr".

sub generateAttribution {

    my ($self, $row) = @_;
    
    my $auth1 = $row->{a_al1} || '';
    my $auth2 = $row->{a_a2l} || '';
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
    
    $row->{attribution} = $attr_string if $attr_string ne '';
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
    
    $row->{pubref} = $longref if $longref ne '';
    
    if ( $longref ne '' )
    {
	$row->{pubref} = $longref;
    }
}


# reportError ( )
# 
# Return an error message, in the proper format.

sub reportError {

    my ($self) = @_;
    
    my $message = $self->{error} || "An error occurred.";
    
    if ( $self->{output_format} eq 'json' )
    {
	return '{"error":"' . $message . '"}';
    }
    
    else
    {
	return $message;
    }   
}

1;
