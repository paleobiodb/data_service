#
# Web::DataService::ParseXSD
# 
# This module implements a class that knows how to process XML Schema Documents.  It expects a
# reference to a tree data structure returned by XML::Parse with the 'Tree' style.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::ParseXSD;

use XML::Parser;
use Try::Tiny;
use File::Path qw(make_path);
use File::Spec;
use LWP;
use Carp 'croak';

use Moo;
use namespace::clean;

no warnings 'uninitialized';


# Whenever a new instance of this class is created, the following attributes are required.

has source => ( is => 'ro', required => 1 );

has record_element => ( is => 'rwp', required => 1 );

has debug => ( is => 'ro' );

has base_url => ( is => 'ro' );

# Default cache directory name.

my $DEFAULT_DIR = '.wds_xml';

# BUILD ( )
# 
# This is called automatically at object creation time.

sub BUILD {
    
    my ($self) = @_;
    
    # Create a hash of record element names.
    
    my @names = split qr{\s*,\s*}, $self->record_element;
    
    foreach my $n (@names)
    {
	$self->{record_element_name}{$n} = 1;
    }
    
    # Generate an LWP user agent in case we need to fetch documents.  We may need to do this even
    # if the source document is local, because it may refer to other documents that we don't have
    # local copies for.
    
    $self->{user_agent} = LWP::UserAgent->new;
    $self->{user_agent}->agent("ParseXSD/0.1");
    
    # Create a directory in which to cache any documents we retrieve.  If the environment variable
    # WDS_XML_DIR is set, use that.  Otherwise, default to "~/.wds_xml/
    
    my $cachedir;
    
    if ( $ENV{WDS_XML_DIR} )
    {
	$cachedir = $ENV{WDS_XML_DIR};
    }
    
    elsif ( $ENV{HOME} )
    {
	$cachedir = File::Spec->catdir($ENV{HOME}, $DEFAULT_DIR);
    }
    
    elsif ( my $tmpdir = File::Spec->tmpdir )
    {
	$cachedir = File::Spec->catdir($tmpdir, $DEFAULT_DIR);
    }
    
    elsif ( my $curdir = File::Spec->curdir )
    {
	$cachedir = File::Spec->catdir($curdir, $DEFAULT_DIR);
    }
    
    else
    {
	die "ERROR: could not create cache directory\n";
    }
    
    if ( -e $cachedir )
    {
	die "ERROR: could not open '$cachedir': $!\n"
	    unless -x $cachedir && -w $cachedir;
    }
    
    else
    {
	my $errstring;
	make_path($cachedir, { error => \$errstring }) or
	    die "ERROR: could not create '$cachedir': $errstring\n";
	
	my $docdir = File::Spec->catdir($cachedir, 'documents');
	make_path($docdir, { error => \$errstring }) or
	    die "ERROR: could not create '$docdir': $errstring\n";
    }
    
    $self->{cache_dir} = $cachedir;
    
    # We are done, and can stop here when debugging.
    
    my $a = 1;
}


# process ( )
# 
# This method will attempt to fetch the XML Schema Document whose source was specified when the
# object was created, either from the local file system or over the WWW.  It will then call
# XML::Parse to parse the schema and extract the element definitions and relationships in a manner
# that will enable the module Web::DataService::Plugin::XML to generate a valid XML document
# according to this schema.
# 
# The source of the document must be specified at the time th

my ($INSTANCE, @STACK, @ELTSTACK);
our ($DOCUMENT);

sub process {

    my ($self) = @_;
    
    # Start by processing the main document, whose source was already given when this instance was
    # created.  If an exception occurs, we do not try to catch it since there is no sense in
    # proceeding if the main document cannot be read.
    
    my $source = $self->{source};
    
    $self->process_document($source);
    
    # Note that as part of the above call, other documents may be processed due to "xs:import"
    # directives, etc.
    
    my $a = 1;	# we can stop here when debugging
}


sub process_document {
    
    my ($self, $source, $options) = @_;
    
    # First check if we have already processed this document.  If so, we don't need to process it
    # again.
    
    # $source =~ qr{ ( [^\\/]+ ) $ }xs;
    # my $leaf = $1;
    
    # return if $self->{processed}{$leaf};
    
    # $self->{processed}{$leaf} = 1;
    
    # First fetch the document contents, if we haven't already fetched them.
    
    my $document = $self->fetch_document($source, $options);
    
    # If we have already processed this document, then nothing more needs to
    # be done.  Otherwise, we mark it as processed and proceed to process it.
    
    return if $document->{processed};
    
    $document->{processed} = 1;
    
    # If this is the first document to be processed, then mark it as the main one.
    
    unless ( $self->{main_doc} )
    {
	$self->{main_doc} = $document;
	$document->{main} = 1;
    }
    
    # Then create a new XML parser instance.  Each instance is single-use.
    
    my $parser = XML::Parser->new();
    
    $parser->setHandlers( Start => \&process_start_tag,
			  End => \&process_end_tag );
    
    # Attempt to parse the document.  Exit with an error message on failure.  Since we have no way
    # to pass our own data structure to the handler, we stick references to $self in $INSTANCE
    # (defined above) and $document in $DOCUMENT.
    
    $INSTANCE = $self;
    local($DOCUMENT) = $document;
    @STACK = ();
    @ELTSTACK = ();
    
    try {
	$parser->parse($document->{content});
    }
    
    catch {
	my $error = $_;
	$DB::single = 1;
	$error =~ s{^\s*}{}s;
	$error =~ s{at /.*}{}s;
	die "ERROR: $source is not a valid XSD: $error\n";
    };
    
    # If the document generated a valid parse, then we recursively process any documents that it
    # imported.
    
 IMPORT:
    foreach my $imp ( @{$DOCUMENT->{imports}} )
    {
	my $ns = $imp->{namespace};
	my $source = $imp->{source};
	my $basefile = $document->{basefile};
	my $baseurl = $document->{baseurl};
	
	next IMPORT if $self->{processed}{$source};
	
	print STDERR "IMPORT: $source ($basefile / $baseurl )\n";
	
	$self->process_document($source, { basefile => $basefile, baseurl => $baseurl });
    }
    
    my $a = 1;
}

my %SKIP = ( 'xs:annotation' => 1,
	     'xs:documentation' => 1,
	     'xs:complexType' => 1, 
	     'xs:sequence' => 1,
	     'xs:choice' => 1,
	     'xs:all' => 1 );


sub process_start_tag {

    my ($expat, $element, %attrs) = @_;
    
    push @STACK, $element;
    
    if ( $SKIP{$element} )
    {
	# do nothing
    }
    
    elsif ( $element eq 'xs:schema' )
    {
	$DOCUMENT->{found_schema} = 1;
	
      ATTR:
	foreach my $a ( %attrs )
	{
	    if ( $a eq 'targetNamespace' || $a eq 'elementFormDefault' || $a eq 'attributeFormDefault' )
	    {
		$DOCUMENT->{$a} = $attrs{$a}
	    }
	    
	    elsif ( $a =~ qr{ xmlns : (.*) }xs )
	    {
		$DOCUMENT->{xmlns}{$1} = $attrs{$a};
		$INSTANCE->{nsprefix}{$attrs{$a}} = $1 if $DOCUMENT->{main};
	    }
	}
    }
    
    elsif ( $element eq 'xs:element' )
    {
	my $eltname = $attrs{name} || $attrs{ref};
	my $parentname = $ELTSTACK[-1];
	
	die "ERROR: element without a name\n" unless $eltname;
	
	my ($ns, $prefix, $name) = $INSTANCE->qualify_name($eltname);
	
	my $qname = $prefix ? "$prefix:$name" : $name;
	
	my $elt = { name => $qname,
		    parent => $parentname,
		    min => 1,
		    max => 1,
		  };
	
	if ( defined $attrs{minOccurs} )
	{
	    $elt->{min} = $attrs{minOccurs};
	}
	
	if ( defined $attrs{maxOccurs} )
	{
	    $elt->{max} = $attrs{maxOccurs};
	}
	
	$elt->{is_record} = 1 if $INSTANCE->{record_element_name}{$eltname};
	
	$INSTANCE->{elements}{$qname} = $elt;
	$INSTANCE->{parent_of}{$qname} = $eltparent;
	
	push @ELTSTACK, $qname;
    }
    
    elsif ( $element eq 'xs:import' )
    {
	my $ns = $attrs{namespace};
	my $loc = $attrs{schemaLocation};
	
	if ( $ns && $loc )
	{
	    my $imp = { namespace => $ns, source => $loc };
	    push @{$DOCUMENT->{imports}}, $imp;
	}
    }
    
    else
    {
	my $docbase = ref $DOCUMENT eq 'HASH' ? ($DOCUMENT->{basefile} || $DOCUMENT->{baseurl} || 'UNKNOWN' )
	    : 'UNKNOWN';
	
	$INSTANCE->{unrecognized}{$docbase}{$element} = 1;
    }
}


sub process_end_tag {
    
    my ($expat, $element) = @_;
    
    my $match = pop @STACK;
    
    unless ( $match eq $element )
    {
	die "mismatched tags caught by &process_end_tag";
    }
    
    if ( $element eq 'xs:element' )
    {
	pop @ELTSTACK;
    }
}


sub qualify_name {
    
    my ($self, $basename) = @_;
    
    my ($namespace, $name);
    
    if ( $basename =~ qr { ^ ( [^:]+ ) : (.*) }xs )
    {
	$namespace = $DOCUMENT->{xmlns}{$1};
	$name = $2;
    }
    
    else
    {
	$namespace = $DOCUMENT->{targetNamespace} || '';
	$name = $basename;
    }
    
    # If we can determine the namespace for this name, and if we know the prefix that was
    # specified for it in the main document, then return the name with that prefix.
    
    my $prefix = $INSTANCE->{nsprefix}{$namespace} || '';
    
    return ($namespace, $prefix, $name);
}


sub json {




}


sub debug_output {
    
    my ($self) = @_;
    
    foreach my $elt (keys %{$self->{parent_of}})
    {
	my $eltparent = $self->{parent_of}{$elt} || "(NONE)";
	print "ELEMENT $elt -> $eltparent\n";
    }
}


# fetch_document ( source, basefile, baseurl )
# 
# Fetch the specified document.  If $source looks like an absolute URL, attempt to get it with LWP
# and cache it in our cache directory.  If it looks like an absolute path, attempt to get it from
# the file system.  If it looks like a relative path, attempt to fetch it from one of the
# following locations:
# 
# 1) If $basefile was specified, try using $source relative to that.
# 2) If $baseurl was specified, check the cache directory for $source relative to that.
# 3) If $baseurl was specified, try fetching the documeent with LWP $source relative to that.
# 
# When I say "relative to", I mean removing the last component of the base name (which represents
# some other filename) and then concatenating the source path.

sub fetch_document {
    
    my ($self, $source, $options) = @_;
    
    croak "fetch_document: second argument must be a hashref\n"
	if defined $options && ref $options ne 'HASH';
    
    $options ||= {};
    
    # First make sure the source name is not empty.
    
    unless ( defined $source && $source ne '' )
    {
	croak "ERROR: no source name given\n";
    }
    
    # If the source looks like an absolute URL, parse it.
    
    if ( $source =~ qr{ ^ https?:// (.*) / ( [^/]+ ) $ }xs )
    {
	my $docname = $2;
	my $pathname = $1 . '/' . $2;
	
	# First check if we have already fetched this document during this run.  If so, just
	# return the document record.
	
	if ( $self->{documents}{$docname} )
	{
	    return $self->{documents}{$docname};
	}
	
	# Then check to see if we have the document contents cached from a previous run.  If so, read
	# the contents from disk.
	
	if ( my $content = $self->fetch_cached($pathname) )
	{
	    return $self->new_document($docname, $content, { baseurl => $source, cachepath => $pathname });
	}
	
	# Then, if $basefile was given, see if there is a file whose name is the specified
	# document name (i.e. last path component) in the directory of $basefile.
	
	my $basefile = $options->{basefile};
	
	if ( $basefile && (my $content = $self->fetch_aside_file($docname, $basefile)) )
	{
	    return $self->new_document($docname, $content, { basefile => $self->aside_name($docname, $basefile) });
	}
	
	# Otherwise, fetch it over the Internet via LWP.
	
	if ( my $content = $self->fetch_lwp($source) )
	{
	    $self->cache_content($pathname, $content);
	    return $self->new_document($docname, $content, { baseurl => $source, cachepath => $pathname });
	}
	
	# If we can't do that, then punt.
	
	else
	{
	    die "ERROR: could not fetch $source\n";
	}
    }
    
    # The only transport protocols we support are http and https.
    
    elsif ( $source =~ qr{ ^ (\w+) :// }xs )
    {
	die "ERROR: protocol '$1' is not supported.\n";
    }
    
    # If the source looks like an absolute filename, look for it on disk.
    
    elsif ( File::Spec->file_name_is_absolute($source) )
    {
	if ( -e $source )
	{
	    my ($volume, $dirs, $docname) = File::Spec->splitpath($source);
	    
	    # First check if we have already fetched this document during this run.  If so, just
	    # return the document record.
	    
	    if ( $self->{documents}{$docname} )
	    {
		return $self->{documents}{$docname};
	    }
	    
	    # Otherwise, read in the contents.
	    
	    my $content = $self->fetch_disk($source);
	    return $self->new_document($docname, $content, { basefile => $source });
	}
	
	else
	{
	    die "ERROR: not found: $source\n";
	}
    }
    
    # Otherwise, treat the source as a relative name.
    
    else
    {
	my $basefile = $options->{basefile};
	my $baseurl = $options->{baseurl};
	
	# We need to figure out the last component of the name.  Assume that any instance of
	# either '/' or '\' is a path separator.  That will cover both relative URLs and file
	# paths on various systems.  If no separator is found, assume that entire source string is
	# the document name.
	
	my $docname = $source;
	
	if ( $source =~ qr{ [/\\] ( [^/\\]+ ) $ }xs )
	{
	    $docname = $1;
	}
	
	if ( $self->{documents}{$docname} )
	{
	    return $self->{documents}{$docname};
	}
	
	# If $basefile was given, see if the file exists relative to that name.
	
	if ( $basefile )
	{
	    my ($volume, $dirname) = File::Spec->splitpath($basefile);
	    my $newpath = File::Spec->catpath($volume, $dirname, $source);
	    
	    if ( -e $newpath )
	    {
		my $content = $self->fetch_disk($newpath);
		return $self->new_document($docname, $content, { basefile => $newpath });
	    }
	}
	
	# Otherwise, see if $baseurl was given.  If so, and if it is valid, then attempt to fetch
	# the document from a URL relative to $baseurl.
	
	if ( $baseurl && $baseurl =~ qr{ ^ ( https? :// ) ( .+ / ) [^/]+ $ }xs )
	{
	    my $urlbase = $1;
	    my $cachepath = $2 . $source;
	    my $newurl = $1 . $2 . $source;
	    
	    # Look in the cache first.
	    
	    if ( my $content = $self->fetch_cached($cachepath) )
	    {
		return $self->new_document($docname, $content, { baseurl => $newurl, cachepath => $cachepath });
	    }
	    
	    # If no cached content was found, try to fetch using LWP
	    
	    elsif ( $content = $self->fetch_lwp($newurl) )
	    {
		$self->cache_content($cachepath, $content);
		return $self->new_document($docname, $content, { baseurl => $newurl, cachepath => $cachepath });
	    }
	    
	    else
	    {
		die "ERROR: could not fetch $source relative to $baseurl\n";
	    }
	}
	
	# Otherwise, try looking for the document relative to the current directory.
	
	my $filepath = File::Spec->catfile(File::Spec->curdir, $source);
	
	if ( -e $filepath )
	{
	    my $content = $self->fetch_disk($filepath);
	    return $self->new_document($docname, $content, { basefile => $filepath });
	}
	
	else
	{
	    die "ERROR: not found: $source\n";
	}
    }

    die "ERROR: could not locate $source\n";
}


sub fetch_lwp {
    
    my ($self, $source, $cachepath) = @_;
    
    print STDERR "FETCHING $source...\n";
    
    my $ua = $self->{user_agent};
    my $req = HTTP::Request->new(GET => $source);
    my $res = $ua->request($req);
    
    unless ( $res->is_success )
    {
	my $status = $res->status_line;
	die "ERROR: could not fetch '$source': $status\n";
    }
    
    return $res->content;
}


sub fetch_disk {
    
    my ($self, $filename) = @_;
    
    return unless -e $filename;
    
    open(my $infile, "<", $filename) or die "ERROR: could not open $filename: $!\n";
    
    my $content = '';
    
    while ( <$infile> )
    {
	$content .= $_;
    }
    
    if ( $! )
    {
	die "ERROR: while reading from $filename: $!\n";
    }
    
    close $infile;
    
    return $content;
}


sub fetch_aside_file {
    
    my ($self, $docname, $basefile) = @_;
    
    my $newpath;
    
    # If we can construct an "aside name", then try to fetch its contents from disk.
    
    $newpath = $self->aside_name($docname, $basefile) if $basefile;
    return $self->fetch_disk($newpath) if $newpath;
    
    # Otherwise, return undefined.
    
    return;
}


sub aside_name {
    
    my ($self, $docname, $basefile) = @_;
    
    return unless $basefile;
    
    my ($volume, $dirname) = File::Spec->splitpath($basefile);
    return File::Spec->catpath($volume, $dirname, $docname);
}


sub fetch_cached {
    
    my ($self, $cachepath) = @_;
    
    # If a file exists in the cache_dir under $cachepath, return it.
    
    my $cachedir = $self->{cache_dir};
    my $filename = File::Spec->catfile($cachedir, "documents", $cachepath);
    
    if ( -r $filename )
    {
	return $self->fetch_disk($filename);
    }
    
    # Otherwise return the undefined value.
    
    return;
}


sub cache_content {
    
    my ($self, $cachepath, $content) = @_;
    
    # Figure out the name under which we should save this content.
    
    my $cachedir = File::Spec->catdir($self->{cache_dir}, "documents");
    my @path = split qr{/}, $cachepath;
    my $leaf = pop @path;
    
    foreach my $p (@path)
    {
	$cachedir = File::Spec->catdir($cachedir, $p);
	
	unless ( -e $cachedir )
	{
	    my $errstring;
	    
	    unless ( make_path($cachedir, { error => \$errstring }) )
	    {
		print STDERR "WARNING: could not create cache directory '$cachedir': $errstring\n";
	    }
	}
    }
    
    my $filename = File::Spec->catfile($cachedir, $leaf);
    
    # If we cannot write the file for some reason, treat it as a warning rather than a fatal
    # error.
    
    my $outfile;
    
    unless ( open ($outfile, ">", $filename) )
    {
	print STDERR "WARNING: could not create cache file '$filename': $!\n";
	return;
    }
    
    print $outfile $content;
    
    unless ( close $outfile )
    {
	print STDERR "WARNING: error writing cache file '$filename': $!\n";
    }
}


sub new_document {

    my ($self, $docname, $content, $attrs) = @_;
    
    croak "new_document: third argument must be a hashref"
	if defined $attrs && ref $attrs ne 'HASH';
    
    $attrs ||= {};
    
    my $doc = { docname => $docname, content => $content, %$attrs };
    
    $self->{documents}{$docname} = $doc;
    $self->{doc_count}++;
    
    bless $doc, 'Web::DataService::ParseXSD::Document';
    return $doc;
}


1;
