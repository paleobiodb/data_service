#
# Nexusfile.pm
# 
# Created by Michael McClennen, 2013-01-31
# 
# The purpose of this module is to provide an interface for getting
# information about nexus files stored in the database.  If you wish to modify
# this information, use NexusfileWrite.pm.


package Nexusfile;
use strict;

use Carp qw(carp croak);

use Constants qw($HTML_DIR $READ_URL);

our ($SQL_STRING, $ERROR_STRING);


=head1 NAME

Nexusfile

=head1 SYNOPSIS

Objects of this class represent nexus files stored in the database.

=head1 INTERFACE

=head3 getFileInfo ( dbt, nexusfile_no, options )

This function returns Return a list of one or more objects containing
information about the specified nexus file(s).  If no matching file exists,
returns undef.

The argument C<nexusfile_no> can be either an object of type Nexusfile, a
positive integer identifier, or a list of either.  If not specified
(undefined) then files are selected based on the options.

The argument C<options>, if specified, must be a hashref.  Available options
include:

=over4

=item all_files

If specified with a true value, return information about all files known to
the database.  This could be a very long list.

=item filename

Selects files which have the given filename

=item authorizer_no

Selects files which belong to the specified authorizer.

=item md5_digest

Selects files which have the specified md5 digest.

=item reference_no

Selects files which are associated with the specified reference.

=item taxon_no

Selects files which are associated with the specified taxon.

=item taxon_name

Selects files which are associated with the specified taxon.  This cannot be
used together with the taxon_no or base_no option.

=item base_no

Selects files which are associated with the specified taxon or any taxon
contained within it.

=item base_name

Selects files which are associated with the specified taxon or any taxon
contained withn it.

=item ignore_taxon

The value of this option may be either a single taxon_no value, a Taxon object, or a
list.  Ignores files which are associated with the specified taxon or taxa or
any of their descendants, unless they are also associated with a non-excluded
taxon. (not yet implemented)

=item exclude_taxon

The value of this option may be either a single taxon_no value, a Taxon
object, or a list.  Excludes files which are associated with the specified
taxon or taxa or any of their descendants. (not yet implemented)

=item fields

This option allows you to specify additional fields to be included in the
resulting Nexusfile objects.  Its value should be a list of one or more of the
following values (as a listref or comma-separated string):

=over 4

=item taxa

Include a list of records listing the associated taxa.

=item refs

Include a list of records listing the associated references.

=item notes

Include any notes that were saved along with this nexus file.

=item all

Include all of the above.

=back

=back

=cut

sub getFileInfo {

    my ($dbt, $nexusfile_no, $options) = @_;
    
    my $dbh = $dbt->{dbh};
    $options ||= {};
    
    # First interpret the argument $nexusfile_no.
    
    my $file_nos = extractIds($nexusfile_no);
    
    # Then interpret the options.
    
    my (@filter_list, %tables, $valid_criteria);
    
    # If we were given a filename, use that.
    
    if ( defined $options->{filename} )
    {
	my $quoted = $dbh->quote($options->{filename});
	push @filter_list, "f.filename like $quoted";
	$valid_criteria = 1;
    }
    
    # If we were given an authorizer_no value, use that.  Make sure it is
    # numeric, and provide a value of 0 (which will select no files) if it is
    # not valid.
    
    if ( defined $options->{authorizer_no} )
    {
	my $authorizer_no = $options->{authorizer_no};
	$authorizer_no =~ tr/0-9//dc;
	$authorizer_no ||= 0;
	push @filter_list, "f.authorizer_no = $authorizer_no";
	$valid_criteria = 1;
    }
    
    # If we were given an md5_digest value, use that.
    
    if ( defined $options->{md5_digest} )
    {
	my $quoted = $dbh->quote($options->{md5_digest});
	push @filter_list, "f.md5_digest = $quoted";
	$valid_criteria = 1;
    }
    
    # If we were given a reference_no value, use that.
    
    if ( defined $options->{reference_no} )
    {
	my $reference_no = $options->{reference_no};
	$reference_no =~ tr/0-9//dc;
	$reference_no ||= 0;
	push @filter_list, "r.reference_no = $reference_no";
	$tables{r} = 1;
	$valid_criteria = 1;
    }
    
    # If we were given a taxon_no or taxon_name value, use that.  Only one of
    # these two options can be used.
    
    if ( defined $options->{taxon_no} )
    {
	my $taxon_no = $options->{taxon_no};
	$taxon_no =~ tr/0-9//dc;
	$taxon_no ||= 0;
	push @filter_list, "t.orig_no = $taxon_no";
	$tables{t} = 1;
	$valid_criteria = 1;
    }
    
    elsif ( defined $options->{taxon_name} )
    {
	my $quoted = $dbh->quote($options->{taxon_name});
	push @filter_list, "t.taxon_name = $quoted";
	$tables{t} = 1;
	$valid_criteria = 1;
    }
    
    elsif ( defined $options->{base_no} )
    {
	my $done;
	my $base_no = $options->{base_no};
	$base_no =~ tr/0-9//dc;
	
	if ( $base_no > 0 )
	{
	    $SQL_STRING = "SELECT lft, rgt FROM taxa_tree_cache WHERE taxon_no=$base_no";
	    my ($lft, $rgt) = $dbh->selectrow_array($SQL_STRING);
	    
	    if ( $lft > 0 and $rgt > 0 )
	    {
		$tables{t} = 1;
		$tables{tc} = 1;
		push @filter_list, "tc.lft >= $lft and tc.lft <= $rgt";
		$done = 1;
	    }
	}
	
	push @filter_list, "tc.lft = 0" unless $done;
	$valid_criteria = 1;
    }
    
    elsif ( defined $options->{base_name} )
    {
	my $done;
	my $t = TaxonInfo::getTaxa($dbt, { taxon_name => $options->{base_name} });
	
	if ( defined $t and $t->{taxon_no} > 0 )
	{
	    $SQL_STRING = "SELECT lft, rgt FROM taxa_tree_cache WHERE taxon_no=$t->{taxon_no}";
	    my ($lft, $rgt) = $dbh->selectrow_array($SQL_STRING);
	    
	    if ( $lft > 0 and $rgt > 0 )
	    {
		$tables{t} = 1;
		$tables{tc} = 1;
		push @filter_list, "tc.lft >= $lft and tc.lft <= $rgt";
		$done = 1;
	    }
	}
	
	push @filter_list, "tc.lft = 0" unless $done;
	$valid_criteria = 1;
    }
    
    # If 'all_files' was specified, then select all files.
    
    if ( $options->{all_files} )
    {
	$valid_criteria = 1;
    }
    
    # We need either a valid option or at least one nexusfile_no value.
    
    unless ( keys %$file_nos or $valid_criteria )
    {
	croak "you must specify either a valid filtering option or one or more nexusfile_no values";
    }
    
    # Now figure out which extra fields we need (if any).
    
    my $fields = extractFields($options->{fields});
    
    # Set up the necessary query.
    
    my $extra_joins = '';
    $extra_joins .= "JOIN nexus_refs as r on f.nexusfile_no = r.nexusfile_no " if $tables{r};
    $extra_joins .= "JOIN nexus_taxa as t on f.nexusfile_no = t.nexusfile_no " if $tables{t};
    $extra_joins .= "JOIN taxa_tree_cache as tc on t.orig_no = tc.taxon_no " if $tables{tc};
    
    if ( keys %$file_nos )
    {
	my $expr = join(',', keys %$file_nos);
	push @filter_list, "f.nexusfile_no in ($expr)";
    }
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    
    my $extra_fields = '';
    $extra_fields .= ', f.notes' if $fields->{notes};
    
    $SQL_STRING = "
		SELECT f.nexusfile_no, f.filename, f.authorizer_no, f.enterer_no,
		       f.created, f.modified, p.reversed_name as authorizer $extra_fields
		FROM nexus_files as f JOIN person as p on p.person_no = f.authorizer_no
		$extra_joins
		$filter_expr
		GROUP BY f.nexusfile_no ORDER BY f.filename";
    
    my ($result_list) = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # Return immediately unless we got some results.
    
    return unless ref $result_list eq 'ARRAY';
    
    # Otherwise, go through the results and index them by nexusfile_no so that
    # we can add in the refs and taxa.
    
    my %nf;
    
    foreach my $f (@$result_list)
    {
	bless $f, 'Nexusfile';
	
	$nf{$f->{nexusfile_no}} = $f;
	$f->{taxa} = [] if $fields->{taxa};
	$f->{refs} = [] if $fields->{refs};
	$f->{dbt} = $dbt;
    }
    
    return unless keys %nf;
    
    # If we were asked for taxa, fetch that info now and add it to the
    # corresponding Nexusfile objects.  Note that for the moment, we are using the
    # 'orig_no' field to hold taxon numbers, not the corresponding original
    # combination numbers.  That will come later, when this code is integrated
    # with the taxon_trees rewrite.
    
    if ( $fields->{taxa} )
    {
	my $nf_list = join(',', keys %nf);
	
	$SQL_STRING = "
		SELECT t.nexusfile_no, t.orig_no as taxon_no, t.taxon_name, a.taxon_rank,
		       t.inexact
		FROM nexus_taxa as t LEFT JOIN authorities as a on a.taxon_no = t.orig_no
		WHERE t.nexusfile_no in ($nf_list) ORDER BY t.index_no";
	
        my ($taxon_list) = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	
	if ( ref $taxon_list eq 'ARRAY' )
	{
	    foreach my $t (@$taxon_list)
	    {
		my $f = $nf{$t->{nexusfile_no}};
		push @{$f->{taxa}}, $t if $f;
	    }
	}
    }
    
    # If we were asked for references, fetch that info now and add it to the
    # corresponding Nexusfile objects.
    
    if ( $fields->{refs} )
    {
	my $nf_list = join(',', keys %nf);
	
	$SQL_STRING = "
		SELECT n.nexusfile_no, n.reference_no,
		       r.author1init, r.author1last, r.author2init, r.author2last, 
		       r.otherauthors, r.pubyr, r.reftitle, r.pubtitle, r.editors,
		       r.pubvol, r.pubno, r.firstpage, r.lastpage
		FROM nexus_refs as n LEFT JOIN refs as r using (reference_no)
		WHERE n.nexusfile_no in ($nf_list) GROUP BY n.reference_no ORDER BY n.index_no";
	
        my ($ref_list) = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	
	if ( ref $ref_list eq 'ARRAY' )
	{
	    foreach my $r (@$ref_list)
	    {
		my $f = $nf{$r->{nexusfile_no}};
		push @{$f->{refs}}, $r if $f;
		bless $r, 'Reference';
	    }
	}
    }
    
    # Now return the indicated object(s).
    
    return @$result_list;
}


sub extractIds {
    
    my ($id_list) = @_;
    
    my (%nexusfile_nos);
    
    if ( ref $id_list eq 'ARRAY' )
    {
	foreach my $f (@$id_list)
	{
	    if ( ref $f )
	    {
		unless ( defined $f->{nexusfile_no} )
		{
		    carp "could not determine nexusfile_no";
		    next;
		}
		
		$nexusfile_nos{$f->{nexusfile_no}} = 1 if $f->{nexusfile_no} =~ /^[0-9]+$/;
	    }
	    
	    elsif ( defined $f and $f =~ /^[0-9]+$/ )
	    {
		$nexusfile_nos{$f} = 1;
	    }
	}
	
	unless ( keys %nexusfile_nos )
	{
	    carp "no valid nexusfile_no values were specified";
	    return {};
	}
    }
    
    elsif ( ref $id_list eq 'HASH' and not exists $id_list->{nexusfile_no} )
    {
	foreach my $t (keys %$id_list)
	{
	    if ( $t =~ /^[0-9]+$/ )
	    {
		$nexusfile_nos{$t} = 1;
	    }
	}
    }
    
    elsif ( ref $id_list )
    {
	croak "could not determine nexusfile_no" unless
	    defined $id_list->{nexusfile_no};
	
	$nexusfile_nos{$id_list->{nexusfile_no}} = 1 if $id_list->{nexusfile_no} =~ /^[0-9]+$/;
    }
    
    elsif ( defined $id_list and $id_list =~ /^[0-9]+$/ )
    {
	$nexusfile_nos{$id_list} = 1;
    }
    
    return \%nexusfile_nos;
}


sub extractFields {
    
    my ($field_list) = @_;
    
    # Return the default if the parameter is undefined.
    
    return {} unless $field_list;
    
    # Turn the list into an array unless it already is one.
    
    unless ( ref $field_list )
    {
	my @strings = split(/\s*,\s*/, $field_list);
	$field_list = \@strings;
    }
    
    elsif ( ref $field_list ne 'ARRAY' )
    {
	croak "option 'fields' must be either a string or an arrayref";
    }
    
    # Now go through the list.
    
    my %fields;
    
    foreach my $inc (@$field_list)
    {
	if ( $inc eq 'all' )
	{
	    %fields = ( taxa => 1, refs => 1, notes => 1 );
	    last;
	}
	
	elsif ( $inc eq 'taxa' or $inc eq 'refs' or $inc eq 'notes' )
	{
	    $fields{$inc} = 1;
	}
	
	else
	{
	    carp "unrecognized value '$inc' for option 'fields'";
	}
    }
    
    return \%fields;
}


=head3 getFileData ( dbt, nexusfile_no )

Return the contents of the specified nexus file.  If the nexusfile_no does not
correspond to a nexus file stored in the system, return undef.

=cut

sub getFileData {

    my ($dbt, $nexusfile_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $nexusfile_no =~ tr/0-9//dc;
    return unless $nexusfile_no > 0;
    
    my ($data) = $dbh->selectrow_array("SELECT data FROM nexus_data
					WHERE nexusfile_no=$nexusfile_no");
    
    return $data;
}


=head2 Object methods

=head3 getFileData ( )

Return this file's data, as a single string.

=cut

sub getData {
    
    my ($self) = @_;
    
    return getFileData($self->{dbt}, $self->{nexusfile_no});
}

1;
