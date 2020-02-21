# 
# Web::DataService::PodParser
# 
# This module implements a Pod-to-HTML formatter, subclassed from Pod::Simple.
# 

use strict;

package Web::DataService::PodParser;
use Pod::Simple;
use Carp ();

our(@ISA) = qw(Pod::Simple);


# new ( options )
# 
# Create a new Pod-to-HTML translator. $options must be a hashref, and may contain any of the
# following keys:
# 
# target	Currently, must be 'html'; other target formats may be added later.
# html_header	If specified, this string will be used as the beginning of the HTML output. It
#		  should start with <html> and end with a <body> tag. If not specified, this
#		  module will generate the header.
# html_footer   If specified, this string will be used as the end of the HTML output.  It should
#		  include </body> and </html>.  If not specified, those two closing tags will
#		  be appended to the end of the output.
# css		URL of the stylesheet for the generated documentation.  This may be site-relative,
#	          need not be absolute.  If not specified, no stylesheet link will be generated.
# page_title	Title for the generated HTML page. If the page contains a '=for wds_title'
# 	          directive, its value will override this option value.
# url_formatter A code ref to be called for translating URL specifications for output.
# debug		If true, then debugging output will be printed to STDERR
# no_tables	If true, then item lists will be translated to HTML <dl> lists
#		  instead of HTML tables.
# 

our (%FORMATTER_OPTION) = ( target => 1, html_header => 1, html_footer => 1, css =>1,
			    page_title => 1, url_formatter => 1, debug => 1, no_tables => 1 );

sub new {

    my ($class, $options) = @_;
    
    # Create a new Pod::Simple formatter.  We tell it to accept all targets
    # because Pod::Simple behaves strangely when it encounters targets it
    # doesn't know what to do with.  We turn off the automatically generated
    # errata section, since we will be generating this ourselves.  Finally, we
    # provide it a subroutine that will strip indentation from verbatim blocks
    # according to the indentation on the first line.
    
    my $new = $class->SUPER::new;
    
    $new->accept_target_as_text('wds_nav');
    $new->accept_targets('*');
    $new->no_errata_section(1);
    $new->strip_verbatim_indent(sub {
	my $lines = shift;
	(my $indent = $lines->[0]) =~ s/\S.*//;
	return $indent;
    });
    
    # Decorate the formatter with some fields relevant to this subclass.
    
    $new->{wds_fields} = { body => [ '' ], target => [ 'body' ],
			   listlevel => 0, listcol => 0 };
    
    # Add any options that were specified.
    
    Carp::croak "you must specify an options hash, with at least the option 'target'"
	unless ref $options eq 'HASH' && defined $options->{target};
    
    foreach my $k ( keys %$options )
    {
	Carp::croak "invalid option '$k'" unless $FORMATTER_OPTION{$k};
	
	$new->{wds_fields}{options}{$k} = $options->{$k};
    }
    
    Carp::croak "the only allowed target is 'html'" unless lc $options->{target} eq 'html';
    
    # Bless the new instance into the current package and return it.
    
    return bless $new;
}


# _handle_element_start ( parser, element_name, attr_hash )
# 
# This method will be called automatically by the Pod::Simple parsing code at the beginning of each
# Pod element that it recognizes.

sub _handle_element_start {
    
    my ($parser, $element_name, $attr_hash) = @_;
    
    # Shortcut access the object fields for this subclass.
    
    my $wds = $parser->{wds_fields};
    
    # If debugging mode is turned on, emit debugging output.
    
    if ( $wds->{options}{debug} )
    {
    	print STDERR "START $element_name";
	
    	foreach my $k (keys %$attr_hash)
    	{
    	    print STDERR " $k=" . $attr_hash->{$k};
    	}
	
    	print STDERR "\n";
    }
    
    # If the last element found was '=for wds_table_header', the current element must be '=over'
    # or else an error will be reported.
    
    if ( $wds->{pending_columns} )
    {
	unless ( $element_name eq 'over-text' )
	{
	    push @{$wds->{errors}}, [ $wds->{header_source_line}, 
		   "improperly placed '=for wds_table_header': must immediately precede '=over'" ];
	    $wds->{header_source_line} = undef;
	    $wds->{table_no_header} = undef;
	    $wds->{pending_columns} = undef;
	}
    }
    
    # If we have found an ordinary paragraph and are not inside a list, generate a <p> tag.
    
    if ( $element_name eq 'Para' && ! $wds->{listlevel} )
    {
	my $attrs = qq{ class="pod_para"};
	
	# If we have a pending anchor, use it as the identifier for this paragraph.
	
	if ( defined $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	$parser->add_output_text( qq{\n\n<p$attrs>} );
    }
    
    # If we have found a data paragraph, the 'Data' element start/end will be surrounded by a
    # 'for' element start/end. We handle any necessary processing on the latter.
    
    elsif ( $element_name eq 'Data' )
    {
	# nothing to do here
    }
    
    # If we have found a Verbatim paragraph, generate a <pre> tag.
    
    elsif ( $element_name eq 'Verbatim' )
    {
	my $attrs = qq{ class="pod_verbatim"};
	
	# If we have a pending anchor, use it as the identifier for this paragraph.
	
	if ( defined $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	$parser->add_output_text( qq{\n\n<pre$attrs>} );
    }
    
    # If we have found =head1, =head2, etc., then start capturing heading text. We
    # will generate the appropriate HTML tag when we finish. This is necessary because the default
    # identifier for the heading tag will be the heading text.
    
    elsif ( $element_name =~ qr{ ^ head ( \d ) }xs )
    {
	$parser->capture_output_text('head');
    }
    
    # If we have found =over where the first item indicates a bullet or a number, then we are starting a
    # list. We will generate <ul> or <ol> as appropriate.
    
    elsif ( $element_name =~ qr{ ^ over-(bullet|number) $ }xs )
    {
	my $tag = $1 eq 'bullet' ? 'ul' : 'ol';
	my $class = $wds->{listlevel} > 1 ? 'pod_list2' : 'pod_list';
	my $attrs = qq{ class="$class"};
	
	# If we have a pending anchor, use it as the identifier for this list..
	
	if ( defined $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	$parser->add_output_text( qq{\n\n<$tag$attrs>} );
	$wds->{listlevel}++;
    }
    
    # If we have found =item inside a bulleted or numbered list, then generate an <li> tag.
    
    elsif ( $element_name =~ qr{ ^ item-(bullet|number) $ }xs )
    {
	# We use a different CSS class for top-level lists than for sublists, but not a separate
	# one for sub-sublists.
	
	my $class = $wds->{listlevel} > 1 ? 'pod_def2' : 'pod_def';
	my $attrs = qq{ class="$class"};
	
	# If an explicit number was specified, use that as the item value. This allows the author
	# to explicitly number list items if desired, with automatic numbering as a fallback.
	
	if ( $1 =~ qr{^n}i && defined $attr_hash->{'~orig_content'} && defined $attr_hash->{number} )
	{
	    $attr_hash->{'~orig_content'} =~ qr{ (\d+) }xs;
	    if ( $1 ne $attr_hash->{number} )
	    {
		$attrs .= qq{ value="$1"};
	    }
	}
	
	# If we have a pending anchor, use it as the identifier for this list item.
	
	if ( defined $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	$parser->add_output_text( qq{\n\n<li$attrs>} );
    }
    
    # If we have found =over where the first item is NOT a bullet or a number, then we are
    # generating a table. Unless, that is, this formatter was instantiated with the no_tables option
    # in which case we generate a definition-list using <dl>, <dt>, and <dd>.
    
    elsif ( $element_name =~ qr{ ^ over-text $ }xs )
    {
	my $tag = $wds->{options}{no_tables} ? 'dl' : 'table';
	my $class = $wds->{listlevel} > 0 ? 'pod_list2' : 'pod_list';
	my $attrs = qq{ class="$class"};
	
	# If we have a pending anchor, use it as the identifier for this table or list.
	
	if ( defined $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	# Emit the tag that opens the list or table.
	
	$parser->add_output_text( qq{\n\n<$tag$attrs>} );
	
	# If have a pending list of table column definitions, and this formatter was not instantiated
	# with the 'no_tables' option, then process that list.  Unless the 'no_header' flag is
	# set, generate a header row now.  We must process the column definitions regardless,
	# since they may affect the style of the ordinary table cells.
	
	my $table_def = { n_cols => 0, n_subs => 0 };
	
	$table_def->{no_header} = 1 if $wds->{table_no_header};
	
	if ( $wds->{pending_columns} && ! $wds->{options}{no_tables} )
	{
	    my @columns;
	    
	    my $class = $wds->{listlevel} > 0 ? 'pod_th2' : 'pod_th';
	    
	    # Start the header row, unless 'no_header' is in effect for this table.
	    
	    $parser->add_output_text( qq{\n\n<tr class="$class">} ) unless $table_def->{no_header};
	    
	    # Process each column definition in turn.
	    
	    foreach my $col ( @{$wds->{pending_columns}} )
	    {
		my $col_def;
		my $attrs = '';
		my $multiplicity = 1;
		
		$table_def->{n_cols}++;
		
		# If the column definition ends in /n where n is an integer, then it represents n
		# separate columns. We must prepare to generate a subheader row with that many
		# cells under this one.  In this case, the first =item subsequently encountered
		# will provide the labels for these cells. This feature is used to generate
		# response field name columns for each vocabulary when there are multiple
		# vocabularies.
		# 
		# Note that /n only works on the FIRST COLUMN.
		
		if ( $col =~ qr{ ^ (.+) / ( \d+ ) $ }xs )
		{
		    # Strip off the suffix we just recognized.
		    
		    $col = $1;
		    
		    # If this is the first column, then give this cell a 'colspan' attribute equal
		    # to the column multiplicity. Also note the number of subheader cells we are
		    # expecting and set the 'expect_subheader' flag. This flag will cause the
		    # first =item paragraph we encounter to be treated specially as a list of
		    # subheader labels.
		    
		    unless ( @columns )
		    {
			$multiplicity = $2;
			$attrs = qq{ colspan="$multiplicity"};
			$table_def->{n_subs} += $multiplicity;
			$table_def->{expect_subheader} = 1;
		    }
		    
		    # If this is not the first column, then the suffix is ignored.
		}
		
		# If this is not the first column and the first column has subheaders, then
		# set the "rowspan" attribute for this header cell.
		
		if ( @columns && $table_def->{n_subs} )
		{
		    $attrs = qq{ rowspan="2"};
		}
		
		# If the column definition ends in *, then we set the 'term' flag to indicate that
		# the cells in this column should have a different style class.  Strip the * suffix
		# off the definition, and use the rest as the column name.
		
		if ( $col =~ qr{ ^ (.*) [*] $ }xs )
		{
		    $col = $1;
		    $col_def = { name => $col, term => 1 };
		}
		
		# Otherwise, just use the column definition as the column name.
		
		else
		{
		    $col_def = { name => $col };
		}
		
		# Add this column definition record to the column list for this table. If the
		# definition had an /n suffix, then add it that many times.
		
		push @columns, $col_def foreach 1..$multiplicity;
		
		# Use the remaining column definition after any suffixes have been stripped as the
		# label in the header cell.
		
		$parser->add_output_text( qq{<td$attrs>$col</td>} ) unless $table_def->{no_header};
	    }
	    
	    # Save the generated list of column definition records, for use in generating the
	    # body rows.
	    
	    $table_def->{columns} = \@columns;
	    
	    # Close the header row, if we are generating one.
	    
	    $parser->add_output_text( qq{</tr>\n\n} ) unless $table_def->{no_header};
	}
	
	# We keep a stack of table definitions, because we may be generating a table inside
	# another table cell.  In particular, this can happen when a table of acceptable values is
	# given inside the definition of a parameter or response field.
	
	unshift @{$wds->{table_def}}, $table_def;
	
	# Clear any table flags that had been set, since they are only valid for a table that
	# immediately follows them.
	
	$wds->{pending_columns} = undef;
	$wds->{header_source_line} = undef;
	$wds->{table_no_header} = undef;
	
	# Indicate that we are now inside a list/table or sublist/subtable, and note that we are
	# about to start a new row. The value of 'listcol' can be either 0 or 2. The value
	# 0 means that we are at the start of a row, and 2 means that we are in the middle of a
	# row and must close it before we start another one.
	
	$wds->{listlevel}++;
	$wds->{listcol} = 0;
    }
    
    # If we have found =item inside a list that is not bulleted or numbered, then we must start
    # capturing the item text to be processed and output when the item is done.
    
    elsif ( $element_name =~ qr{ ^ item-text $ }xsi )
    {
	# If the value of listcol is not 0, then we have an unclosed table row or <dd> section and
	# must close it before we start the new item.
	
	if ( $wds->{listcol} > 0 )
	{
	    if ( $wds->{options}{no_tables} )
	    {
		$parser->add_output_text( qq{\n</dd>} );
	    }
	    
	    elsif ( $wds->{listcol} == 2 )
	    {
		$parser->add_output_text( qq{\n</td></tr>} );
	    }
	}
	
	# Start capturing the item text. We will process it when we have it all.
	
	$parser->capture_output_text('item-text');
    }
    
    # If we have found a paragraph inside of a list, then append a <p> tag to the list item we are
    # currently processing. The style of a paragraph inside a list is different from an ordinary
    # paragraph not in a list. We have defined a separate style for paragraphs inside sublists.
    
    elsif ( $element_name eq 'Para' && $wds->{listlevel} )
    {
	my $class = $wds->{listlevel} > 1 ? 'pod_def2' : 'pod_def';
	my $attrs = qq{ class="$class"};
	
	# If we have a pending anchor, use it as the identifier for this paragraph.
	
	if ( $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"}
		if $wds->{pending_anchor} ne '' && $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	$parser->add_output_text( qq{\n<p$attrs>} );
	
	# Note that we are in the middle of a list item, in case this has not already been set.
	
	$wds->{listcol} = 2;
    }
    
    # If we have found an L<...> section, then we get all of its attributes with the start of the
    # element. So we can immediately generate an <a> tag.
    
    elsif ( $element_name eq 'L' )
    {
	my $href;	# this will hold the target of the link
	
	# The Pod::Simple code improperly handles certain kinds of links where there is also link
	# content.  I am writing this comment a few months after I wrote the code, and can no longer
	# remember what the exact problem was. At any rate, we ignore Pod::Simple's attempt to
	# parse the link content out from the link target and do it ourselves from the raw contents
	# which Pod::Simple fortunately provides us.
	
	if ( $attr_hash->{raw} =~ qr{ ^ (?: [^|]* [|] )? (.*) }xs )
	{
	    $href = $1;
	}
	
	# If there is no link content, then the link target will be given by the "to" attribute
	# passed to us by Pod::Simple. Unless the link started with "/", in which case it will be
	# in the "section" attribute instead.
	
	else
	{
	    $href = $attr_hash->{to} || "/$attr_hash->{section}";
	}
	
	# If a url_formatter attribute was provided to this formatter, then call it on the link
	# target value. This will translate something that looks like "node:a/b" into a
	# site-relative URL that links to the documentation page for node a/b; it will translate
	# something that looks like "op:a/b?foo" into a site-relative URL that will call the
	# operation a/b with argument foo. The exact form of the URL will depend on which features
	# are set for this data service.
	
	my $url_gen = $wds->{options}{url_formatter};
	$href = $url_gen->($href) if $href && ref $url_gen eq 'CODE';
	
	# If the "content-implicit" flag was set by Pod::Simple, it means that there is no link
	# content and that we should use the link target as the content.
	
	$wds->{override_text} = $href if $attr_hash->{'content-implicit'};
	
	# If the link target looks like an external URL (in other words, if it is not
	# site-relative) then add the attribute 'target="_blank"' so that it will open in a new
	# window or tab when activated.
	
	my $attrs = '';
	$attrs = qq{ target="_blank"} if $href =~ qr{ ^ \w+:// }xsi;
	
	# Output the <a> tag.
	
	$parser->add_output_text( qq{<a class="pod_link" href="$href"$attrs>} );
    }
    
    # If we have found one of the other formatting sections, then we generate a <span> tag with
    # the appropriate class. The tricky part is that we are re-purposing part of the Pod spec by
    # defining C<B<...>> to indicate the pod_term style class and B<C<...>> to indicate the
    # pod_term2 style class. Otherwise, the style class will be pod_B, pod_I, etc.
    
    elsif ( $element_name =~ qr{ ^ ( B | I | F | C | S ) $ }xs )
    {
	my $code = $1;
	
	# I tried using <em>, <code>, etc. but decided to give it up and just us <span> with the
	# appropriate style classes.
	
	my $tag = 'span';
	
	# If the output generated so far ends in <span class="pod_C"> or <span class="pod_B"> and
	# this section has the opposite formatting code, then rewrite that tag to have the appropriate
	# style class. Then set the 'no_span' flag to indicate that we should not generate </span>
	# at the end of this section because the enclosing element will already be generating it.
	
	if ( $wds->{body}[0] =~ qr{<(?:span|strong|em|code) class="pod_(.)">$}s )
	{
	    my $enclosing = $1;
	    
	    if ( $enclosing eq 'B' && $code eq 'C' )
	    {
		$wds->{body}[0] =~ s{<[^>]+>$}{<span class="pod_term">}s;
		$wds->{no_span} = 1;
	    }
	    
	    elsif ( $enclosing eq 'C' && $code eq 'B' )
	    {
		$wds->{body}[0] =~ s{<[^>]+>$}{<span class="pod_term2">}s;
		$wds->{no_span} = 1;
	    }
	}
	
	# Otherwise, just add a new <span> tag.
	
	else
	{
	    $parser->add_output_text( qq{<$tag class="pod_$code">} );
	}
    }
    
    # If we have found an X<...> or Z<...> section, then we capture the text inside and throw it
    # away.
    
    elsif ( $element_name =~ qr{ ^ ( X | Z ) $ }xs )
    {
	$parser->capture_output_text('xz');
    }
    
    # If we have found a data paragraph, we will need to process its contents specially.  Note
    # that Pod::Simple uses the element name 'for' to indicate data paragraphs even if they are
    # actually bounded by =begin and =end.
    
    elsif ( $element_name eq 'for' )
    {
	# Ignore any colon at the beginning of the data section identifier.  Pod::Simple will have
	# already figured out whether or not it is supposed to be parsing the contents for Pod
	# elements, based on the presence or absence of a colon. There is nothing we need to do
	# differently.
	
	my $identifier = $attr_hash->{target};
	$identifier =~ s{^:}{};
	
	# Start capturing the data paragraph text.
	
	$parser->capture_output_text($identifier);
	
	# If the identifier is 'wds_nav', set a flag to indicate that the contents should be
	# processed specially. This data section is processed as Pod text even if it is not
	# preceded by a colon (see the call to accept_target_as_text in &new).
	
	if ( $identifier eq 'wds_nav' )
	{
	    $wds->{in_wds_nav} = 1;
	}
    }
    
    # Any other elements passed to us by Pod::Simple are ignored. This might not be the best
    # approach, but it is probably better than displaying error messages. If Pod::Simple ever
    # changes the set of elements that it sends to this subroutine, then (a) this module will have
    # to be updated, and (b) any users who update Pod::Simple but not Web::DataService will be
    # screwed.
    
    my $a = 1;	# we can stop here when debugging
}


# _handle_element_end ( parser, element_name, attr_hash )
# 
# This method will be called automatically by the Pod::Simple parsing code at the end of each Pod
# element that it recognizes.

sub _handle_element_end {
    
    my ($parser, $element_name, $attr_hash) = @_;
    
    # Shortcut access the object fields for this subclass.
    
    my $wds = $parser->{wds_fields};
    
    # If debugging mode is turned on, emit debugging output.
    
    if ( $wds->{options}{debug} )
    {
    	print STDERR "END $element_name";
	
    	foreach my $k (keys %$attr_hash)
    	{
    	    print STDERR " $k=" . $attr_hash->{$k};
    	}
	
    	print STDERR "\n";
    }
    
    # If we are done processing an ordinary paragraph, generate the </p> tag. This is the same whether or
    # not the paragraph is in a list/table.
    
    if ( $element_name eq 'Para' )
    {
	$parser->add_output_text( qq{</p>} );
    }
    
    # If we are done processing a verbatim paragraph, generate the </pre> tag.
    
    elsif ( $element_name eq 'Verbatim' )
    {
	$parser->add_output_text( qq{</pre>} );
    }
    
    # If we are done processing a data paragraph, the 'Data' element start/end will be surrounded by
    # a 'for' element start/end. We handle any necessary processing on the latter.
    
    elsif ( $element_name eq 'Data' )
    {
	# nothing to do here
    }
    
    # If we are done processing a =head paragraph, then we can retrieve the captured paragraph
    # text and can generate <h1>, <h2>, etc. as appropriate.  The default identifier for a
    # heading is the heading text, which allows for URL fragments targeting a documentation
    # section, i.e. #PARAMETERS.
    
    elsif ( $element_name =~ qr{ ^ head ( \d ) $ }xsi )
    {
	my $level = $1;
	my $attrs = qq{ class="pod_heading"};
	
	# Finish capturing the paragraph text.
	
	my ($heading_text) = $parser->end_capture_text;
	
	# If we have a pending anchor, use it as the identifier for this heading unless the value
	# is '!'. Note that a value of '!' will result in no identifier at all for this heading.
	
	if ( $wds->{pending_anchor} )
	{
	    $attrs .= qq{ id="$wds->{pending_anchor}"} if $wds->{pending_anchor} ne '!';
	    $wds->{pending_anchor} = undef;
	}
	
	# Otherwise, use the heading text as the identifier.
	
	elsif ( $heading_text ne '' && ! $wds->{in_wds_nav} )
	{
	    $attrs .= qq{ id="$heading_text"}
	}
	
	# Generate the heading tag, text, and closing tag.
	
	$parser->add_output_text( qq{\n\n<h$level$attrs>} );
	$parser->add_output_text( $heading_text );
	$parser->add_output_text( qq{</h$1>} );
    }
    
    # If we are done processing a bulleted or numbered list, we simply need generate a closing tag
    # and decrement the list level.
    
    elsif ( $element_name =~ qr{ ^ over-(bullet|number) $ }xs )
    {
	my $tag = $1 eq 'bullet' ? 'ul' : 'ol';
	$parser->add_output_text( qq{\n\n</$tag>} );
	$wds->{listlevel}--;
    }
    
    # If we are done processing a bulleted or numbered list item, we simply need to generate a
    # </li> tag.
    
    elsif ( $element_name =~ qr{ ^ item-(bullet|number) $ }xs )
    {
	$parser->add_output_text( qq{</li>} );
    }
    
    # If we are done processing a list that not bulleted or numbered, then we need to generate the
    # appropriate closing tag.  But if 'listcol' is greater than zero then we are in the middle of
    # a list item and need to close it first.  We then decrement the list level.
    
    elsif ( $element_name eq 'over-text' )
    {
	# Generate the appropriate closing tag for the list, and for the last item if it is still
	# unclosed.
	
	if ( $wds->{options}{no_tables} )
	{
	    $parser->add_output_text( qq{</dd>} ) if $wds->{listcol} > 0;
	    $parser->add_output_text( qq{\n\n</dl>} );
	}
	
	else
	{
	    $parser->add_output_text( qq{\n</td></tr>} ) if $wds->{listcol} > 0;
	    $parser->add_output_text( qq{\n\n</table>} );
	    
	    # Remove the top element from the table definition stack. If we just ended a sub-table,
	    # then this will return us to the definition of the enclosing table.
	    
	    shift @{$wds->{table_def}};
 	}
	
	# Decrement the list level. If we are still inside a list, then set listcol to 2 because
	# we must still be inside a list item.
	
	$wds->{listlevel}--;
	$wds->{listcol} = $wds->{listlevel} > 0 ? 2 : 0;
    }
    
    # If we are done processing a list item that is not bulleted or numbered, then retrieve the
    # captured item text. Use this to generate either a <dt>...</dt> or to fill in all but the
    # last table column. Any ordinary or verbatim paragraphs following this =item will go into
    # either a <dd>...</dd> or into the last table column.
    
    elsif ( $element_name eq 'item-text' )
    {
	my ($item_text) = $parser->end_capture_text;
	
	# See if we have a table definition available.
	
	my $table_def = $wds->{table_def}[0];
	
	# If we do, then handle the item text according to the table definition.  Items will
	# generally be of the form "=item a / b / c" or "=item a / b / c ( d )". The values a, b,
	# c, d will be used to fill in all but the last column of the table. Any paragraphs
	# following the =item will be placed into the last table column. This allows us to
	# generate multi-column tables defining parameter values and output field values, while
	# still following the basic Pod specification. The Pod text could be turned into a manual
	# page instead, or into some other format, in which case the value lists in the item text will
	# still be intelligible.
	
	if ( ref $table_def->{columns} eq 'ARRAY' )
	{
	    my $last;
	    
	    # If the item text looks like ... ( d ) then split off d.
	
	    if ( $item_text =~ qr{ (.*) \s+ [(] \s+ ( [^)]+ ) \s+ [)] }xs )
	    {
		$item_text = $1;
		$last = $2;
	    }
	    
	    # If the rest of the item text looks like a / b / c, then split out this list of
	    # components. Add the item split off above, if there is one.
	    
	    my @values = split qr{ \s+ [|/] \s+ }xs, $item_text;
	    push @values, $last if defined $last && $last ne '';
	    
	    # If we are expecting a subheader (because this is the first =item and the definition
	    # of the first column ended with the suffix /n where n is an integer) then the list of
	    # values we just computed will be the labels for the subheader cells.
	    
	    if ( $table_def->{expect_subheader} )
	    {
		# Clear the expect_subheader flag. The first =item encountered in the list should
		# give the subheaders, and the rest are processed normally.
		
		$table_def->{expect_subheader} = undef;
		
		# Set the style class differently for a subtable than for a top-level table.
		
		my $class = $wds->{listlevel} > 1 ? 'pod_th2' : 'pod_th';
		
		# Add the subheader row with one cell for each of the values that we split out
		# above. If there are not enough values, the remaining cells will be empty. If
		# there are too many values, the extras will be ignored.
		
		$parser->add_output_text( qq{\n\n<tr class="$class">} );
		
		foreach my $i ( 0 .. $table_def->{n_subs} - 1 )
		{
		    my $v = @values ? shift(@values) : '';
		    $parser->add_output_text( qq{<td>$v</td>} );
		}
		
		# Close the subheader row. We set listcol to 0 to indicate that we are ready to
		# start a new row.
		
		$parser->add_output_text( qq{</tr>\n\n</td></tr>\n} );
		$wds->{listcol} = 0;
	    }
	    
	    # Otherwise, we process the item text as an ordinary table row.
	    
	    else
	    {
		# Generate a <tr> tag to open the row.
		
		$parser->add_output_text( qq{\n\n<tr>} );
		
		# Get a list of the column definitions, and discard the last one. The last column
		# will be filled with whatever paragraphs follow this =item.
		
		my @cols = @{$table_def->{columns}};
		pop @cols;
		
		foreach my $col ( @cols )
		{
		    my $v = @values ? shift(@values) : '';
		    my $attrs = '';
		    
		    # If there is a pending anchor, use its value as the identifier of the first
		    # table cell.  The anchor is then cleared, to make sure that it appears only
		    # on the first cell.
		    
		    if ( $wds->{pending_anchor} )
		    {
			$attrs .= qq{ id="$wds->{pending_anchor}"} if $wds->{pending_anchor} ne '!';
			$wds->{pending_anchor} = undef;
		    }
		    
		    # If this column has the 'term' flag set, then give it the "pod_term" or
		    # "pod_term2" style depending upon whether this is a top-level list or a
		    # sublist.
		    
		    if ( $col->{term} )
		    {
			my $class = $wds->{listlevel} > 1 ? 'pod_term2' : 'pod_term';
			$attrs .= qq{ class="$class"};
		    }
		    
		    # Otherwise, give it the "pod_def" or "pod_def2" style.
		    
		    else
		    {
			my $class = $wds->{listlevel} > 1 ? 'pod_def2' : 'pod_def';
			$attrs .= qq{ class="$class"};
		    }
		    
		    # Generate the table cell for this column.
		    
		    $parser->add_output_text( qq{<td$attrs>$v</td>\n} );
		}
		
		# Now generate the opening tag for the final cell in the row. Any subsequent
		# paragraphs until the next =item or =back will go into this cell. We set
		# 'listcol' to 2 indicating that we are in the middle of a table row that will
		# need to be closed.
		
		my $class = $wds->{listlevel} > 1 ? 'pod_def2' : 'pod_def';
		$parser->add_output_text( qq{<td class="$class">} );
		$wds->{listcol} = 2;
	    }
	}
	
	# If we do not have a table definition, then we either generate a <dt>, <dd> pair or a
	# table row with two clumns. This latter is the default for text lists when tables are
	# being generated. The first column gets the item text, and has the "pod_term" style. The
	# second gets any subsequent paragraphs, and has the "pod_def" style.
	
	else
	{
	    my $termclass = $wds->{listlevel} > 1 ? 'pod_term2' : 'pod_term';
	    my $defclass = $wds->{listlevel} > 1 ? 'pod_def2' : 'pod_def';
	    my $attrs = ''; $attrs .= qq{ class="$termclass"};
	    
	    # If we have a pending anchor, use its value as the identifier of this list item.
	    
	    if ( $wds->{pending_anchor} )
	    {
		$attrs .= qq{ id="$wds->{pending_anchor}"} if $wds->{pending_anchor} ne '!';
		$wds->{pending_anchor} = undef;
	    }
	    
	    # If we are not generating tables, then output a <dt>...</dt><dd>.
	    
	    if ( $wds->{options}{no_tables} )
	    {
		$parser->add_output_text( qq{\n\n<dt$attrs>$item_text</dt>\n<dd class="$defclass">} );
	    }
	    
	    # If we are generating tables, then output <tr><td>...</td><td>.
	    
	    else
	    {
		$parser->add_output_text( qq{\n\n<tr><td$attrs>$item_text</td>\n<td class="$defclass">} );
	    }
	    
	    # In either case, set 'listcol' to 2 indicating that we are in the middle of a list
	    # item that will need to be closed.
	    
	    $wds->{listcol} = 2;
	}
    }
    
    # If we are done processing an L<...> section, we just need to add the </a> tag. If we had any
    # override text for this section, clear it now just in case it was not cleared already.
    
    elsif ( $element_name eq 'L' )
    {
	$parser->add_output_text( qq{</a>} );
	$wds->{override_text} = undef;
    }
    
    # If we are done processing a formatting section, add the </span> tag unless the no_span flag
    # was set.
    
    elsif ( $element_name =~ qr{ ^ ( B | I | F | C | S ) $ }xs )
    {
	if ( $wds->{no_span} )
	{
	    $wds->{no_span} = undef;
	}
	
	else
	{
	    $parser->add_output_text( qq{</span>} );
	}
    }
    
    # If we are dong processing an X<...> or Z<...> section, then discard the captured text.
    
    elsif ( $element_name =~ qr{ ^ ( X | Z ) $ }xs )
    {
	$parser->end_capture_text;
    }
    
    # If we are done processing a data paragraph, then we must handle the captured text specially
    # depending on the data section identifier. This is used to implement special directives for
    # Web::DataService documentation pages, and to include html text and markup. Note that Pod::Simple
    # uses the 'for' element to indicate data paragraphs even if they were actually delimited by
    # =begin and =end.
    
    elsif ( $element_name eq 'for' )
    {
	# Retrieve the text and identifier of this data section.
	
	my ($body, $identifier) = $parser->end_capture_text;
	
	# If the identifier is 'wds_title', then remember this value and use it for the HTML page
	# title. For example:
	# 
	# =for wds_title The Title For This Page
	
	if ( $identifier eq 'wds_title' )
	{
	    $wds->{title} = $body;
	}
	
	# If the identifier is 'wds_anchor', then remember the value and use it as the "id"
	# attribute for the next major document element (heading, table cell, list item, or
	# paragraph) that is generated. If the value is '!', then generate no identifier at
	# all. This last can be used to remove the automatically generated identifier for a
	# heading. For example, the following will generate <p id="example">An example of...
	# 
	# =for wds_anchor example
	# 
	# An example of...
	
	elsif ( $identifier eq 'wds_anchor' )
	{
	    $wds->{pending_anchor} = $body;
	}
	
	# If the identifier is 'wds_table_header' or 'wds_table_no_header', then the value should
	# be a list of table column definitions separated by ' | '.  This directive is only valid
	# immediately preceding an =over paragraph that starts a text list, and specifies how the
	# contents of the list should be mapped into table columns.  In general, the text of each
	# =item will be used to fill all but the last column of one table row, and any subsequent
	# paragraphs will fill the last column. If the identifier is 'wds_table_no_header' then no
	# header row will be generated but the column definitions are still applied to the table
	# body. For example:
	# 
	# =for wds_table_header Value* | Definition
	# 
	# =over
	# 
	# =item foo
	# 
	# Foo speciifes bar.
	
	elsif ( $identifier =~ qr{ ^ wds_table_ (no_)? header $ }xs )
	{
	    $wds->{table_no_header} = 1 if $1;
	    my @columns = split qr{ \s+ [|] \s+ }xs, $body;
	    $wds->{pending_columns} = \@columns;
	    $wds->{header_source_line} = $attr_hash->{start_line};
	}
	
	# If the identifier is 'wds_nav' then we just output the captured text without further
	# processing. The contents of this section are processed as Pod (see the call to
	# accept_target_as_text in &new), so the major effect of this section is that the content
	# will be ignored by all other Pod formatters. This allows for content that will be be
	# translated to HTML by this formatter and will be ignored when the Pod is used for any other
	# purpose. This allows for navigational links that are only relevant in the context of web
	# pages. For example:
	# 
	# =begin wds_nav
	# 
	# =head3 L<Main documentation page|/data/>
	# 
	# =end wds_nav
	
	elsif ( $identifier eq 'wds_nav' )
	{
	    $parser->add_output_text( $body );
	    
	    # Clear the 'in_wds_nav' flag, which was set at the start of this element. This flag
	    # is currently only used to suppress the automatic generation of "id" attributes for
	    # headings.
	    
	    $wds->{in_wds_nav} = undef;
	}
	
	# If the identifier is 'wds_pod', then the value should be either 'on' or 'off'. If it is
	# 'on', then the 'suppress_output' flag is set. If 'off', the flag is cleared. All output
	# is suppressed while this flag is true. The purpose of this directive is to indicate
	# content that should be ignored by this formatter, but will be processed normally by
	# other formatters. In this sense, it is the inverse of wds_nav. This can, for example, be
	# used to indicate Pod content to be substituted for an 'html' section.  For example:
	# 
	# =begin html
	# 
	# <img src="header_banner.jpg">
	# 
	# =end html
	# 
	# =for wds_pod on
	# 
	# =head1 Data Service Documentation
	# 
	# =for wds_pod off
	
	elsif ( $identifier eq 'wds_pod' )
	{
	    # If the value is 'on', then set 'suppress_output'. If wds_pod is already "on"
	    # then display an error message. This directive is not meant to be nested.
	    
	    if ( lc $body eq 'on' )
	    {
		if ( $wds->{wds_pod_start_line} )
		{
		    my $line = $wds->{wds_pod_start_line};
		    push @{$wds->{errors}}, [ $attr_hash->{start_line}, "you already turned 'wds_pod' on at line '$line'" ];
		}
		
		$wds->{wds_pod_start_line} = $attr_hash->{start_line};
		$wds->{suppress_output} = 1;
	    }
	    
	    elsif ( lc $body eq 'off' )
	    {
		$wds->{wds_pod_start_line} = undef;
		$wds->{suppress_output} = undef;
	    }
	    
	    else
	    {
		push @{$wds->{errors}}, [ $attr_hash->{start_line}, "unrecognized value '$body' for data section 'wds_pod'" ];
	    }
	}
	
	# If the identifier is 'html', then output the captured text unchanged.  If the HTML
	# contains links to site-local resources, then the template from which the documentation page
	# is generated should use the URL() function to generate proper site-relative URLs from a
	# base specification. For example:
	# 
	# =begin html
	# 
	# <img src="<% URL('path:images/documentation_banner.jpg') %>">
	# 
	# =end html
	
	elsif ( $identifier eq 'html' )
	{
	    # my $url_gen = $wds->{options}{url_formatter};
	    
	    # if ( ref $url_gen eq 'CODE' )
	    # {
	    # 	$body =~ s{ (href|src)=" ([^"]+) " }{ $1 . '="' . $url_gen->($2) . '"' }xsie;
	    # }
	    
	    $parser->add_output_text( $body );
	}
	
	# If the identfier is 'comment' or 'wds_comment', discard the captured text. Similarly if
	# the identifier is 'wds_node'. This last is used to specify node attributes to go along
	# with the documentation page being formatted. It is read by the code in
	# Web::DataService::Document.pm, and can be ignored here.
	
	elsif ( $identifier eq 'comment' || $identifier eq 'wds_comment' || $identifier eq 'wds_node' )
	{
	    # ignore content
	}
	
	# If the identifier is anything else, display an error message.
	
	else
	{
	    push @{$wds->{errors}}, [ $attr_hash->{start_line}, "unrecognized data section identifier '$identifier'" ];
	}
    }
    
    my $a = 1;	# we can stop here when debugging
}


# _handle_text ( parser, text )
# 
# This method will be called automatically by the Pod::Simple parsing code for each run of text
# found in the document (other than commands and formatting codes).

sub _handle_text {
    
    my ($parser, $text) = @_;
    
    # Shortcut access the object fields for this subclass.
    
    my $wds = $parser->{wds_fields};
    
    # If debugging mode is turned on, emit debugging output.
    
    if ( $wds->{options}{debug} )
    {
    	print STDERR "TEXT $text\n";
    }
    
    # If the 'override_text' field is set, discard the text that was recognized by Pod::Simple and
    # substitute that value. Then clear 'override_text'.
    
    if ( defined $wds->{override_text} )
    {
	$text = $wds->{override_text};
	$wds->{override_text} = undef;
    }
    
    # All text that is not part of an 'html' data section should be HTML-escaped before being
    # output. The latter will be passed through unprocessed.
    
    unless ( $wds->{target}[0] eq 'html' )
    {    
	$parser->html_escape(\$text);
    }
    
    # Add this text to the output stream.
    
    $parser->add_output_text( $text );
}


# add_output_text ( parser, text )
# 
# This method adds the specified text to the current output stream. The output stream starts out
# as 'body', but can be diverted using the capture_output_text method. All text added to 'body'
# will be part of the eventual output. Text diverted to other targets may be captured for further
# processing before eventually being added to 'body', or in some cases just discarded.
# 
# Note: any text added to 'body' that hasn't already passed through _handle_text must be
# HTML-escaped first unless you are SURE that the contents are already proper HTML.

sub add_output_text {
    
    my $wds = $_[0]{wds_fields};
    
    # Ignore this text if the 'suppress_output' flag is true and output has not been redirected
    # away from the default 'body' output stream.
    
    return if $wds->{suppress_output} and @{$wds->{body}} == 1;
    
    # Otherwise, add this output to the current output stream.
    
    $wds->{body}[0] .= $_[1];
}


# capture_output_text ( target )
# 
# Redirect output to a different output stream. This is accomplished by pushing an empty string
# onto the 'body' stack and adding the specified target name to the 'target' stack. Subsequent
# output will be added to this empty string until 'end_capture_text' is called, which will pop and
# return the collected text.

sub capture_output_text {
    
    my ($parser, $target) = @_;
    
    unshift @{$parser->{wds_fields}{body}}, '';
    unshift @{$parser->{wds_fields}{target}}, $target;
}


# end_capture_text ( )
# 
# Pop the top output stream off the 'body' stack and return its collected text. Also pop and
# return the top value from the 'target' stack. Whatever output stream is next in the stack will
# then become the current one.

sub end_capture_text {
    
    my ($parser) = @_;
    
    my $text = shift @{$parser->{wds_fields}{body}};
    my $target = shift@{$parser->{wds_fields}{target}};
    
    return ($text, $target);
}


# current_target ( )
# 
# Report the top value on the 'target' stack, indicating which output stream any output is
# currently going to.

sub current_target {
    
    my ($parser) = @_;
    
    return $parser->{wds_fields}{target}[0];
}


# html_escape ( text_ref )
# 
# HTML-escape the contents of the specified scalar ref.

our (%HTML_ENTITY) = ( '&' => '&amp;', '>' => '&gt;', '<' => '&lt;', q{"} => '&quot;', 
		       q{'} => '&#39;', q{`} => '&#96;', '{' => '&#123;', '}' => '&#125;' );

sub html_escape {
    
    my ($parser, $text_ref) = @_;
    
    $$text_ref =~ s/([&><"'`{}])/$HTML_ENTITY{$1}/ge		#' for poor editors
	if defined $$text_ref;
}


# error_output ( )
# 
# Collect up all of the error messages (if any) that have been generated during formatting of this
# content, and return it as an HTML formatted list.

sub error_output {
    
    my ($parser) = @_;
    
    my $wds = $parser->{wds_fields};
    
    my $error_output = '';
    my @error_lines;
    
    foreach my $error ( @{$wds->{errors}} )
    {
	push @error_lines, qq{<li>Line $error->[0]: $error->[1]</li>\n};
    }
    
    my $errata = $parser->errata_seen;
    
    if ( ref $errata eq 'HASH' && %$errata )
    {
	my @lines = sort { $a <=> $b } keys %$errata;
	
	foreach my $line ( @lines )
	{
	    foreach my $message ( @{$errata->{$line}} )
	    {
		next if $message =~ qr{ alternative \s text .* non-escaped \s [|] }xs;
		
		push @error_lines, qq{<li> line $line: $message</li>\n};
	    }
	}
    }
    
    if ( @error_lines )
    {
	$error_output .= "<h2 class=\"pod_errors\">Errors were found in the source for this page:</h2>\n\n<ul>\n";
	$error_output .= $_ foreach @error_lines;
	$error_output .= "</ul>\n\n";
    }
    
    return $error_output;
}


# output ( )
# 
# Generate a complete HTML page from the Pod that has been processed so far.  This will include a
# header, followed by the generated body text, followed by a list of errors (if any occurred) and
# a foter. Return this text.

sub output {
    
    my ($parser) = @_;
    
    my $wds = $parser->{wds_fields};
    
    # If a header and/or footer were specified when this formatter was instantiated, use them.
    
    my $header = $wds->{options}{html_header};
    my $footer = $wds->{options}{html_footer};
    
    # If Pod::Simple was able to determine the encoding of this data, use that value. Otherwise,
    # default to ISO-8859-1.
    
    my $encoding = $parser->detected_encoding() || 'ISO-8859-1';
    
    # If a stylesheet link was specified when this formatter was instantiated, use it.
    
    my $css = $wds->{options}{css};
    
    # If no html header was provided, generate a default one.
    
    unless ( $header )
    {
	my $title = $wds->{title} || $wds->{options}{page_title};
	
	$header  = "<html><head>";
	$header .= "<title>$title</title>" if defined $title && $title ne '';
	$header .= "\n";
	$header .= "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=$encoding\" >\n";
	$header .= "<link rel=\"stylesheet\" type=\"text/css\" title=\"pod_stylesheet\" href=\"$css\">\n" if $css;
	$header .= "</head>\n\n";
	
	$header .= "<body class=\"pod\">\n\n";
	$header .= "<!-- generated by Web::DataService::PodParser.pm - do not change this file, instead alter the code that produced it -->\n";
    }
    
    # If errors occurred, list them now.
    
    my $error_output = $parser->error_output;
    
    # If no html footer was provided, generate a default one.
    
    unless ( $footer )
    {
	$footer  = "\n</body>\n";
	$footer .= "</html>\n";
    }
    
    return $header . $parser->{wds_fields}{body}[0] . $error_output . $footer;
}


1;


=head1 NAME

Web::DataService::PodParser - Pod-to-HTML formatter for Web::DataService

=head1 SYNOPSIS

This module is a subclass of Pod::Simple, providing an engine that can parse Pod and generate
HTML for use in generating data service documentation pages.  It is used as follows:

    my $parser = Web::DataService::PodParser->new({ target => 'html', ... });
    
    $parser->parse_string_document($doc_string);
    
    my $doc_html = $parser->output;

Several custom data sections are recognized, allowing for directives specific to the
Web::DataService system. In addition, formatting codes and L<...> sections are treated specially.

=head1 METHODS

This module provides the following methods:

=head2 new ( options )

This class method creates a new instance of the parser. The argument must be an options hash,
including any of the following keys:

=head3 target

Specifies the format into which the Pod should be translated. Currently, the only value accepted
is 'html'.

=head3 url_formatter

The value of this attribute should be a code ref. This subroutine will be called once for each URL
in the formatted document, and the return value will be substituted for that URL.

=head3 css

The value of this attribute should be the URL of a stylesheet, which will be included via an HTML
<link> tag. This URL will be passed through the url_formatter if one is specified. 

=head3 html_header

If specified, this string will be included at the beginning of the HTML output. It should start
with <html> and end with a <body> tag.  If not specified, this module will generate a header
automatically. 

=head3 html_footer

If specified, this string will be included at the end of the HTML output. It should include
</body> and </html>.  If not specified, these two closing tags will be appended to the end of the
formatted output.

=head3 no_tables

If this option has a true value, then Pod lists that are neither numbered nor bulleted will be
rendered using the <dl>, <dt>, and <dd> tags. Otherwise, and by default, they will be rendered as
tables.

=head3 debug

If this option has a true value, then voluminous debugging output will be written to STDERR.

=head2 parse_string_document

This method takes a single argument, which must be a string containing Pod text. This text is
parsed and formatted into HTML.

=head2 output

This method returns the formatted HTML content as a single string, which can then be sent as the
body of a response message.

=head1 SYNTAX

This module is a subclass of Pod::Simple, and as such can handle all valid Pod syntax. Certain
constructs are treated specially, as indicated here:

=head2 URLs

When this class is instantiated by Web::DataService::Documentation, it is passed a reference to a
URL formatter. This is used to process all C<< L<...> >> sections according to the L<Web::DataService
URL specification|Web::DataService::Documentation.pod#Embedded-links>.

=head2 Text formatting

The formatting codes C<< B<...> >> and C<< C<...> >> can be mixed in order to format text
according to the CSS styles "pod_term" and "pod_term2". This allows you to style parameter or
field names in description text to match the occurrences of these terms in the first column of the
parameter and field name tables. The sequence C<<< B<C<...>> >>> will generate a text span with
the style class "pod_term", while C<<< C<B<...>> >>> will generate a span with hte style class
"pod_term2". 

=head2 Special directives

Several directives can be included in a Web::DataService documentation page through the use of
particular data section identifiers.  These either be delimited with C<=begin> and C<=end>, or
specified using C<=for>.

=head3 wds_node

This directive specifies attributes for the Web::DataService node corresponding to the
documentation page on which it appears.  This means that you can create documentation pages in the
appropriate directory and give them the necessary attributes without having to add C<define_node>
calls to your data service application code.  For example:

    =for wds_node title=General notes about this data service

=head3 wds_title

This directive specifies the title for the page.  It overrides any value set using 
C<=for wds_node> or C<define_node>.

=head3 $$$

=head1 AUTHOR

mmcclenn "at" cpan.org

=head1 BUGS

Please report any bugs or feature requests to C<bug-web-dataservice at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Web-DataService>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 COPYRIGHT & LICENSE

Copyright 2014 Michael McClennen, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

