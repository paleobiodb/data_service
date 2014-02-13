#
# Web::DataService::XML
# 
# This module is responsible for generating data responses in XML format.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::XML;

use Encode;
use Scalar::Util qw(reftype);
use Carp qw(croak);

use parent 'Exporter';


# emit_header ( request, field_list )
# 
# Return the initial text of an XML result.

sub emit_header {

    my ($class, $request, $field_list) = @_;
    
    # First check to see if we have any warning messages to convey
    
    my $header = '';
    
    if ( ref $request->{warnings} eq 'ARRAY' and @{$request->{warnings}} > 0 )
    {
	foreach my $w ( @{$request->{warnings}} )
	{
	    $header .= $class->xml_comment_value("Warning: $w");
	}
    }
    
    # Add the result counts if we were directed to do so.

    if ( $request->display_counts )
    {
	my $counts = $request->result_counts;
	
	$header .= $class->xml_comment_value("Records found: $counts->{found}");
	$header .= $class->xml_comment_value("Records returned: $counts->{returned}");
	$header .= $class->xml_comment_value("Record offset: $counts->{offset}")
	    if defined $counts->{offset} && $counts->{offset} > 0;
    }
    
    # Then generate the header
    
    return <<END_XML;
<?xml version="1.0" standalone="yes"?>
$warnings
<dwr:DarwinRecordSet xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dwr="http://rs.tdwg.org/dwc/dwcrecord/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://rs.tdwg.org/dwc/dwcrecord/ http://rs.tdwg.org/dwc/xsd/tdwg_dwc_classes.xsd">
END_XML

}


# emit_footer ( )
# 
# Return the final text for an XML result.

sub emit_footer {
    
    my ($class, $request, $field_list) = @_;
    
    my $output = <<END_XML;
</dwr:DarwinRecordSet>
END_XML
    
    return $output;
}


sub xml_comment_value {
    
    my ($self, $value) = @_;
    
    return unless defined $value && $value ne '';
    
    return "\n<!-- " . xml_clean($value) . "-->\n";
}
    

# xml_clean ( string )
# 
# Given a string value, return an equivalent string value that will be valid
# as part of an XML document.

my (%ENTITY) = ( '&' => '&amp;', '<' => '&lt;', '>', '&gt;' );

sub xml_clean {
    
    my ($string) = @_;
    
    # Return an empty string unless the value is defined.
    
    return "" unless defined $string;
    
    # Do a quick check for okay characters.  If there's nothing exotic, just
    # return the value as-is.
    
    return $string unless $string =~ /[^a-zA-Z0-9 _.,;:-]/;
    
    # Otherwise, we have more work to do.
    
    # First, turn any numeric character references into actual Unicode
    # characters.  The database does contain some of these.
    
    $string =~ s/&\#(\d)+;/pack("U", $1)/eg;
    
    # Then, take out all <b> and <i>.  We can't leave these in, because they
    # would look like XML tags instead of the HTML markup that they are.
    
    $string =~ s/<\/?[bi]>//g;
    
    # Next, rename any instance of & that doesn't start a valid character
    # entity reference to &amp; and rename all < and > to &lt; and &gt;
    
    $string =~ s/(&(?!\w+;)|<|>)/$ENTITY{$1}/ge;
    
    # Finally, delete all control characters (they shouldn't be in the
    # database in the first place, but unfortunately some rows do contain
    # them) as well as invalid utf-8 characters.
    
    $string =~ s/[\0-\037\177]//g;
    
    return $string;
}
