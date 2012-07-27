# 
# The Paleobiology Database
# 
#   Taxonomy.pm
# 

package Taxonomy;

use TaxonTrees;

use Carp qw(carp croak);

use strict;


our (%TAXONOMIC_RANK) = ( 'max' => 26, 'informal' => 26, 'unranked_clade' => 25, 'unranked' => 25, 
			 'kingdom' => 23, 'subkingdom' => 22,
			 'superphylum' => 21, 'phylum' => 20, 'subphylum' => 19,
			 'superclass' => 18, 'class' => 17, 'subclass' => 16,
			 'infraclass' => 15, 'superorder' => 14, 'order' => 13, 
			 'suborder' => 12, 'infraorder' => 11, 'superfamily' => 10,
			 'family' => 9, 'subfamily' => 8, 'tribe' => 7, 'subtribe' => 6,
			 'genus' => 5, 'subgenus' => 4, 'species' => 3, 'subspecies' => 2, 'min' => 2 );

our (%NOM_CODE) = ( 'iczn' => 1, 'phylocode' => 2, 'icn' => 3, 'icnb' => 4 );

our (%TREE_TABLE_ID) = ( 'taxon_trees' => 1 );

our (%KINGDOM_ALIAS) = ( 'metazoa' => 'Metazoa', 'animalia' => 'Metazoa', 'metaphyta' => 'Plantae',
			 'plantae' => 'Plantae', 'fungi' => 'Fungi', 'bacteria' => 'Bacteria',
			 'eubacteria' => 'Bacteria', 'archaea' => 'Archaea', 'protista' => 'Other',
			 'chromista' => 'Other', 'unknown' => 'Other' );

our ($TEMP_TABLE_INDEX) = 1;

=head1 NAME

Taxonomy

=head1 SYNOPSIS

An object of this class represents a hierarchy of taxonomic names.  The set of
names known to the database is stored in the C<authorities> table, with
primary key C<taxon_no>.  The main hierarchy is computed from the data in the
C<authorities> and C<opinions> tables, and is stored in the table
C<taxon_trees>.  Other hierarchies may be defined as well, and will be stored
in other tables with the same structure as C<taxon_trees>.  The taxon numbers
from C<authorities> are used extensively as foreign keys throughout the rest
of the database, because the taxonomic hierarchy is central to the
organization of the data.  The hierarchy stored in C<taxon_trees> (and
possibly others as well) is also used extensively by the rest of the database,
for example to select sets of related taxa.

=head2 Definitions

Each distinct taxonomic name/rank combination represented in the database has
a unique entry in the C<authorities> table, and a primary key (taxon_no)
assigned to it in that table.  In the documentation for this database, we use
the term I<spelling> to represent the idea "distinct taxonomic name/rank
combination".  So, for example, "Rhizopodea" as a class and "Rhizopodea" as a
phylum are considered to be distinct I<spellings> of the same I<taxonomic
concept>.  In this case, the taxon's rank was changed at some point in the
past.  It is also the case that "Cyrnaonyx" and "Cyraonyx" are distinct
spellings of the same taxonomic concept, but in this case one was used at some
point as a misspelling of the other.  Each spelling is a member of exactly one
taxonomic concept.  I<Note, however, that the taxonomic namespaces for
plants/fungi (ICN) and animals (ICZN) are not distinct.  In order to specify a
taxon uniquely by name, you must also specify a namespace>.

A taxonomic hierarchy is built as follows.  For each taxonomic concept in the
database, we algorithmically select a "classification opinion" from among the
entries in the C<opinions> table, representing the most recent and reliable
taxonomic opinion that specifies a relationship between this taxon and the
rest of the taxonomic hierarchy.  These classification opinions are then used
to arrange the taxa into a collection of trees.  Note that the taxa do not
necessarily form a single tree, because there are a number of fossil taxa for
which classification opinions have not yet been entered into the database.
Different taxonomies may use different rules for selecting classification
opinions, or may consider different subsets of the C<opinions> table.

=head2 Organization of taxa

The C<authorities> table contains one row for each distinct spelling
(name/rank combination) with C<taxon_no> as primary key.  The C<orig_no> field
associates each row in C<authorities> with the row representing the original
spelling of its taxonomic concept.  Thus, the distinct values of C<orig_no>
represent the distinct taxonomic concepts known to the database.  The
C<taxon_trees> table contains one row for each taxonomic concept, with
C<orig_no> as primary key.

The taxonomic spellings and concepts are organized according to four separate
relations, based on the data in C<authorities> and C<opinions>.  These
relations are discussed below; the name listed in parentheses after each one
is the name of the field in which records the relation.

=over 4

=item Taxonomic concept (orig_no)

This relation groups together all of the spellings (name/rank combinations)
that represent the same taxonomic concept.  It is recorded in the
C<authorities> table.  Each row that represents an original spelling has
C<taxon_no = orig_no>.  When a new spelling for a taxon is encountered, or an
opinion is entered which changes its rank, a new row is created in
C<authorities> with the same C<orig_no> but different C<taxon_no>.

Note that this relation can be taken as an equivalence relation, whereas two
spellings have the same C<orig_no> if and only if they represent the same
taxonomic concept.

=item Accepted spelling (spelling_no)

This relation selects from each taxonomic concept the currently accepted
spelling (in other words, the currently accepted name/rank combination).  It
is stored in C<taxon_trees>.  The value of C<spelling_no> for any concept is
the C<taxon_no> corresponding to the accpeted spelling.  The auxiliary field
C<trad_no> records nearly the same information, but with traditional taxon
ranks preferred over unranked clades.

=item Synonymy (synonym_no)

This relation groups together all of the taxonomic concepts which are
considered to be synonyms of each other.  Two taxa are considered to be
synonymous if one is a subjective or objective synonym of the other, or was
replaced by the other, or if one is an invalid subgroup or nomen dubium, nomen
vanum or nomen nudum inside the other.

The value of C<synonym_no> is the C<orig_no> of the most senior synonym for
the given concept group.  This means that all concepts which are synonyms of
each other will have the same C<synonym_no>, but different C<orig_no>, and the
senior synonym will have C<synonym_no = orig_no>.  This relation can also be
taken as an equivalence relation, whereas two concepts have the same
C<synonym_no> if and only if they represent the same group of organisms.  The
set of taxonomic concepts that share a particular value of C<synonym_no> are
called a synonym group.

=item Hierarchy (parent_no)

This relation associates lower with higher taxa.  It forms a collection of
trees, because (as noted above) there are a number of fossil taxa for which no
classifying opinion has yet been entered.  Any taxonomic concept for which no
opinion has been entered will have C<parent_no = 0>.

All concepts which are synonyms of each other will have the same C<parent_no>
value, and the C<parent_no> (if not 0) will be the one associated with the
classification opinion on the most senior synonym.  Thus, it is really a
relation on synonym groups.  In computing the hierarchy, we consider all
opinions on a synonym group together.

This relation, like the previous ones, can be taken as
an equivalence relation, whereas two taxonomic concepts have the same
C<parent_no> if and only if they are siblings of each other.

=back

=head2 Opinions

In addition to the fields listed above, each entry in C<taxon_trees> (or in
any of the alternative hierarchy tables) also has an C<opinion_no> field.
This field points to the classification opinion that has been algorithmically
selected from the available opinions for that taxon.

For a junior synonym, the value of opinion_no will be the opinion which
specifies its immediately senior synonym.  There may exist synonym chains in
the database, where A is a junior synonym of B which is a junior synonym of C.
In any case, C<synonym_no> should always point to the most senior synonym.

For all taxonomic concepts which are not junior synonyms, the value of
C<opinion_no> will be the opinion which specifies its immediately higher
taxon.  Note that this opinion will also specify a particular spelling of the
higher taxon, which may not be the currently accepted one.  In any case,
C<parent_no> will always point to the original spelling of the parent taxon.

=head2 Tree structure

In order to facilitate tree printouts and logical operations on the taxa
hierarchy, the entries in C<taxon_trees> are sequenced via preorder tree
traversal.  This is recorded in the fields C<lft> and C<rgt>.  The C<lft>
field stores the traversal sequence, and the C<rgt> field of a given entry
stores the maximum sequence number of the entry and all of its descendants.
An entry which has no descendants has C<lft> = C<rgt>.  The C<depth> field
stores the distance of a given entry from the root of its taxon tree, with
top-level nodes having C<depth> = 1.  All entries which have no parents or
children will have null values in C<lft>, C<rgt> and C<depth>.

Using these fields, we can formulate simple and efficient SQL queries to fetch
all of the descendants of a given entry and other similar operations.  For
more information, see L<http://en.wikipedia.org/wiki/Nested_set_model>.

=head2 Additional Tables

One auxiliary table is needed in order to properly compute the relations
described above.  This table, called C<suppress_opinions>, is needed because
the synonymy and hierarchy relations must be structured as collections of
trees.  Unfortunately, the set of opinions stored in the database may generate
cycles in one or both of these relations.  For example, there will be cases in
which the best opinion on taxon A states that it is a subjective synonym of B,
while the best opinion on taxon B states that it is a subjective synonym of A.
In order to resolve this, the algorithm that computes the synonymy and
hierarchy relations breaks each cycle by choosing the best (most recent and
reliable) opinion from those that define the cycle and suppressing any opinion
that contradicts the chosen one.  The C<suppress_opinions> table records which
opinions are so suppressed.

=cut

=head1 INTERFACE

I<Note: this is a draft specification, and may change>.

In the following documentation, the parameter C<dbh> is always a database
handle.  Here are some examples:

    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my ($base_taxon) = $taxonomy->getTaxaByName('Conus');
    my $taxon_rank = $base_taxon->{taxon_rank};
    my $reference = Reference->new($dbt, $base_taxon->{reference_no});
    
    my @list = $taxonomy->getRelatedTaxa($base_taxon, 'children', { id_only => 1 });
    my $child_id_list = '(' . join(',', @list) . ')';
    
    my $sth = $dbh->prepare("SELECT some_fields FROM some_table WHERE taxon_no IN $child_id_list");
    
    my @lineage = $taxonomy->getRelatedTaxa($base_taxon, 'parents');
    
    my ($id_table) = $taxonomy->getTaxonIdTable($base_taxon, 'all_children');
    
    my $sth = $dbh->prepare("SELECT some_fields FROM $id_table JOIN some_other_table WHERE condition");
    
    $dbh->do("DROP TABLE $id_table");

=cut

=head2 Class Methods

=head3 new ( dbh, name )

    $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

Creates a new Taxonomy object, which will use the database connection given by
C<dbh> and the taxonomy table named by C<name>.  As noted above, the main
taxonomy table is called I<taxon_trees>.

=cut

sub new {

    my ($class, $dbh, $table_name) = @_;
    
    croak "unknown tree table '$table_name'" unless $TREE_TABLE_ID{$table_name};
    croak "bad database handle" unless ref $dbh;
    
    my $self = { dbh => $dbh, 
		 tree_table => $table_name,
		 auth_table => $TaxonTrees::AUTH_TABLE,
		 opinion_table => $TaxonTrees::OPINION_CACHE };
    
    bless $self, $class;
    
    return $self;
}


# This expression lists the fields that will be returned as part of a Taxon object.

our ($INFO_EXPR) = "a.taxon_name, a.taxon_no, a.taxon_rank, a.common_name, a.orig_no, o.status, 
a.reference_no, t.spelling_no, t.synonym_no, t.parent_no, t.kingdom";


=head2 Object Methods

=head3 getTaxaByName ( name, options )

If this taxonomy contains one or more spellings matching the given name,
returns a list of objects of class C<Taxon>.  If no matching taxa are found,
returns the empty list.  If called in scalar context, returns the first 
taxon found.  The name may include the SQL wildcards % and _.

    @taxa = $taxonomy->getTaxaByName('Ficus', { kingdom => 'metazoa' });

Options include:

=over 4

=item rank

Only return taxa of the specified rank.  Examples: family, genus.

=item kingdom

Only return taxa from the specified kingdom.  If this option is specified, and
the name does not contain wildcards, only one taxon should be returned.  Examples:
Plantae, Metazoa.

=item status

If 'valid', only valid taxa which are not junior synonyms are returned.  If
'invalid', only invalid taxa are returned.  If 'synonyms', all matching valid
taxa are returned.  If 'all', all matching taxa are returned.  Defaults to
'all'.

=back

=cut

sub getTaxaByName {

    my ($self, $name, $options) = @_;
    
    # Check the arguments.  Carp a warning and return undefined if name is not
    # defined or is empty.  Treat a wildcard '%' as empty; if you want to get
    # all taxa, you should use getTaxonIdTable() with the 'all_taxa' parameter.
    
    unless ( defined $name and $name ne '' and $name ne '%' )
    {
	carp "taxon name is undefined or empty";
	return;
    }
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = defined $options->{status} ? lc $options->{status} : 'all';
    
    # Construct the appropriate modifiers based on the options.
    
    my (@filter_list) = 'a.taxon_name like ?';
    
    if ( $status eq 'valid' )
    {
	push @filter_list, "o.status = 'belongs to'";
	push @filter_list, "t.orig_no = t.synonym_no";
    }
    elsif ( $status eq 'synonyms' ) {
	push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'invalid' ) {
	push @filter_list, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'all' ) {
	# no filter needed
    }
    else {
	croak "invalid value '$status' for option 'status'";
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter($options->{rank});
    }
    
    if ( defined $options->{kingdom} )
    {
	my $label = $KINGDOM_ALIAS{lc $options->{kingdom}};
	croak "invalid value '$options->{kingdom}' for option 'kingdom'" unless defined $label;
	
	push @filter_list, "t.kingdom = '$label'";
    }
    
    my $filter_expr = join(' and ', @filter_list);
    
    # Now prepare and execute the necessary SQL statement, and return the
    # resulting list.
    
    my $sql =  "SELECT $INFO_EXPR
		FROM $auth_table a JOIN $tree_table t using (orig_no)
			JOIN $opinion_table o using (opinion_no)
		WHERE $filter_expr";
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => {} }, 
					       $name);
    
    if ( ref $result_list eq 'ARRAY' )
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Taxon';
	}
	
	if ( wantarray )
	{
	    return @$result_list;
	}
	
	else
	{
	    return $result_list->[0];
	}
    }
    
    else
    {
	return;
    }
}


=head3 getRelatedTaxon ( base_taxon, relationship, options )

Returns a Taxon object related in the specified way to the specified base
taxon.  The base taxon can be specified either by a Taxon object or a taxon
number.  The returned object might be the same as the one passed in, for
example if the accepted spelling is requested and the object passed in is
already the accepted spelling for its taxonomic concept.  Possible
relationships are:

=over 4

=item orig

Returns an object representing the original spelling of the base taxon.

=item spelling

Returns an object representing the accepted spelling of the base taxon.  

=item synonym

Returns an object representing the accepted spelling of the senior synonym of
the base taxon.

=item parent

Returns an object representing the accepted spelling of the parent taxon of
the base taxon.

=item classification

Returns an object representing the taxon under which the base taxon is
classified.  This will be the accepted spelling of either the immediate parent
or immediate senior synonym of the base taxon, depending upon its
classification opinion.

=back

Available options include:

=over 4

=item id_only

If true, return the taxon_no (as a scalar value) rather than a Taxon object.

=back

=cut
 
sub getRelatedTaxon {
    
    my ($self, $base_taxon, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $base_no;
    
    if ( ref $base_taxon )
    {
	croak "could not determine taxon_no from base_taxon" unless
	    exists $base_taxon->{taxon_no} || exists $base_taxon->{orig_no};
	
	$base_no = $base_taxon->{taxon_no} if defined $base_taxon->{taxon_no};
	$base_no = $base_taxon->{orig_no} if defined $base_taxon->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxon && $base_taxon > 0 )
    {
	$base_no = $base_taxon;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    my $rel = lc $parameter;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($taxon, $select);
    
    $options ||= {};
    
    # If the option 'trad' was specified, use trad_no instead of spelling_no.
    
    if ( $options->{trad} )
    {
	$select = 'trad';
	$rel eq 'trad' if $rel eq 'spelling';
    }
    
    else
    {
	$select = 'spelling';
    }
    
    # If we were asked for just the taxon_no, do that.
    
    if ( $options->{id_only} )
    {
	return $self->getRelatedTaxonId($base_no, $rel, $select);
    }
    
    # Parameters orig_no and spelling_no require a simple look-up
    
    if ( $rel eq 'orig' or $rel eq 'spelling' or $rel eq 'trad' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${rel}_no
			LEFT JOIN $opinion_table as o using (opinion_no) 
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    # Parameters synonym_no and parent_no require an extra join on $tree_table
    # to look up the current spelling.
    
    elsif ( $rel eq 'synonym' or $rel eq 'parent' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.${rel}_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    # Parameter 'classification' requires an additional use of $opinion_table as well
    
    elsif ( $rel eq 'classification' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    if ( defined $taxon )
    {
	return bless $taxon, "Taxon";
    }
    
    else
    {
	return;
    }
}


sub getRelatedTaxonId {
    
    my ($self, $base_no, $rel, $select) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($taxon_no);
    
    # Parameters orig_no and spelling_no require a simple lookup.
    
    if ( $rel eq 'orig' or $rel eq 'spelling' or $rel eq 'trad' )
    {
	($taxon_no) = $dbh->selectrow_array("
		SELECT t.${rel}_no
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    # Parameters synonym_no and parent_no require an extra join on $tree_table
    # to look up the spelling_no.
    
    elsif ( $rel eq 'synonym' or $rel eq 'parent' )
    {
	($taxon_no) = $dbh->selectrow_array("
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.${rel}_no
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    # Parameter 'classification' requires an additional use of $opinion_table
    # as well
    
    elsif ( $rel eq 'classification' )
    {
	($taxon_no) = $dbh->selectrow_array("
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table o using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o.parent_no
		WHERE a2.taxon_no = ?", undef, $base_no + 0);
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    # Return the taxon number, or undefined if none was found.
    
    return $taxon_no;
}


=head3 getRelatedTaxa ( base_taxon, relationship, options )

Returns a list of Taxon objects having the specified relationship to the
specified base taxon.  If no matching taxa are found, returns an empty list.
If called from scalar context, returns the first matching taxon.  Possible
relationships are:

=over 4

=item spelling

Returns a list of objects representing the various spellings of the base
taxon.

=item synonym

Returns a list of objects representing the various synonyms of the base taxon.

=item parent

Returns a list of objects representing all higher taxa of the base taxon, from
the kingdom level on down.

=item child

Returns a list of objects representing the immediate children of the given taxon.

=back

Possible options are:

=over 4 

=item status

This option filters the list of resulting taxa according to taxonomic status.
If 'valid', only valid taxa are returned.  If 'invalid', only invalid taxa are
returned.  If 'synonyms', only valid taxa and junior synonyms.  If 'all', all
matching taxa are returned regardless of taxonomic or nomenclatural status.
The default is 'valid' for the relationship 'parent' and 'child', and 'all'
for 'synonym' and 'spelling'.

=item rank

This option filters the list of resulting taxa, returning only those that
match the given rank.  The value can be a single rank, or a list.  In the
latter case, each item can be either a single rank, or a list of [min, max].

=item id_only

If true, return a list of taxon_no values instead of Taxon objects.

=back

=cut
 
sub getRelatedTaxa {
    
    my ($self, $base_taxon, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $base_no;
    
    if ( ref $base_taxon )
    {
	croak "could not determine taxon_no from base_taxon" unless
	    exists $base_taxon->{taxon_no} || exists $base_taxon->{orig_no};
	
	$base_no = $base_taxon->{taxon_no} if defined $base_taxon->{taxon_no};
	$base_no = $base_taxon->{orig_no} if defined $base_taxon->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxon && $base_taxon > 0 )
    {
	$base_no = $base_taxon;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    my $rel = lc $parameter;
    
    # Prepare to fetch the requested information
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($result_list);
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status;
    
    if ( defined $options->{status} and $options->{status} ne '' )
    {
	$status = lc $options->{status};
    }
    
    elsif ( $rel eq 'spelling' or $rel eq 'synonym' )
    {
	$status = 'all';
    }
    
    else
    {
	$status = 'valid';
    }

    # Construct the appropriate modifiers based on the options.
    
    my (@filter_list) = 'a2.taxon_no = ?';
    
    if ( $status eq 'valid' )
    {
	push @filter_list, "o.status = 'belongs to'";
	push @filter_list, "t.orig_no = t.synonym_no";
    }
    elsif ( $status eq 'synonyms' ) {
	push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'invalid' ) {
	push @filter_list, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'all' ) {
	# no filter needed
    }
    else {
	croak "invalid value '$status' for option 'status'";
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter($options->{rank});
    }
    
    my $select = $options->{trad} ? 'trad' : 'spelling';
    
    my $filter_expr = join(' and ', @filter_list);
    
    # If we were asked for just the taxon_no, do that.
    
    if ( $options->{id_only} )
    {
	return $self->getRelatedTaxaIds($base_no, $rel, $select, $filter_expr);
    }
    
    # for parameter 'spelling', make sure to return the currently accepted
    # spelling first
    
    if ( $rel eq 'spelling' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
		WHERE $filter_expr
		ORDER BY if(a.taxon_no = t.${select}_no, 0, 1)", 
		{ Slice => {} }, $base_no + 0);
    }
    
    # for parameter 'synonym', make sure to return the most senior synonym first
    
    elsif ( $rel eq 'synonym' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY if(a.orig_no = t.synonym_no, 0, 1)", 
		{ Slice => {} }, $base_no + 0);
    }
    
    # for parameter 'parent', order the results by higher to lower taxa
    
    elsif ( $rel eq 'parent' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft <= t2.lft and t.rgt >= t2.lft
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY t.lft",
		{ Slice => {} }, $base_no + 0);
    }
    
    # for parameter 'child', order the results by tree sequence
    
    elsif ( $rel eq 'child' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft > t2.lft and t.lft <= t2.rgt
				and t.depth = t2.depth + 1
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY t.lft", 
		{ Slice => {} }, $base_no + 0);
    }
    
    else
    {
	croak "invalid relationship '$parameter'";
    }
    
    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return;
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
    }
    
    # If we were called from list context, return the whole list.  Otherwise,
    # return the first element only.
    
    if ( wantarray )
    {
	return @$result_list;
    }
    
    else
    {
	return $result_list->[0];
    }
}


sub getRelatedTaxaIds {
    
    my ($self, $base_no, $rel, $select, $filter_expr) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($taxon_nos);
    
    # for parameter 'spelling', make sure to return the currently accepted
    # spelling first
    
    if ( $rel eq 'spelling' )
    {
	$taxon_nos = $dbh->selectcol_arrayref("
		SELECT a.taxon_no
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY if(a.taxon_no = t.${select}_no, 0, 1)", undef, $base_no + 0);
    }
    
    # for parameter 'synonym', make sure to return the most senior synonym first
    
    elsif ( $rel eq 'synonym' )
    {
	$taxon_nos = $dbh->selectcol_arrayref("
		SELECT t.{select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY if(t.orig_no = t.synonym_no, 0, 1)", undef, $base_no + 0);
    }
    
    # for parameter 'parent', order the results by higher to lower taxa
    
    elsif ( $rel eq 'parent' )
    {
	$taxon_nos = $dbh->selectcol_arrayref("
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft <= t2.lft and t.rgt >= t2.lft
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY t.lft", undef, $base_no + 0);
    }
    
    # for parameter 'child', order the results by tree sequence
    
    elsif ( $rel eq 'child' )
    {
	$taxon_nos = $dbh->selectcol_arrayref("
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft > t2.lft and t.lft <= t2.rgt
				and t.depth = t2.depth + 1
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		WHERE $filter_expr
		ORDER BY t.lft", undef, $base_no + 0);
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    # If we got some results, return them.  Otherwise, return the empty list.
    
    if ( ref $taxon_nos eq 'ARRAY' )
    {
	if ( wantarray )
	{
	    return @$taxon_nos;
	}
	
	else
	{
	    return $taxon_nos->[0];
	}
    }
    
    else
    {
	return;
    }
}


=head3 getTaxonIdTable ( base_taxon, relationship, options )

Returns the name of a newly created temporary table containing a list of
taxon_no values identifying taxa that have the specified relationship to the
specified base taxon.  Also returns the number of identifiers in the table, as
a second value.

Once you get this table, you can join it with the other tables in the database
to carry out any desired query.

   # get a table containing the taxon_no of every taxon in class Mammalia,
   # including junior synonyms

   ($table_name, $count) = $taxonomy->getTaxonIdTable(36651, 'all_children', {status => 'synonyms'});
   
   # use this table to select all collections that contain Mammalian fossils
   
   my $sth = $dbh->prepare("SELECT distinct collection_no FROM occurrences JOIN $table_name using (taxon_no) WHERE ...");

Valid relationships are:

=over 4

=item all_parents

Return a table containing the taxon_no of every higher taxon that contains the
base taxon, in order from kingdom level on down.

=item all_children

Return a table containing the taxon_no of the base taxon and all of the taxa
that it contains, as a preorder tree traversal.

=item all_taxa

Return a table containing the taxon_no of every taxon in the database.  In
this case, the base taxon is ignored and can be undefined.

=back

Each identifier in the table represents the accepted spelling of the most
senior synonym of a matching taxon.  The relationship 'all_parents' matches
all of the ancestors of the base taxon, while 'all_children' matches all
descendants.  This is potentially a very large list, which is the reason for
this routine.  The returned table can then be joined with other tables in
order to fetch whatever information is desired about the resulting taxa.

Valid options include:

=over 4

=item status

This option filters the list of resulting taxa according to taxonomic status.
If 'valid', only valid taxa are returned.  If 'invalid', only invalid taxa are
returned.  If 'synonyms', only valid taxa and junior synonyms.  If 'all', all
matching taxa are returned regardless of taxonomic or nomenclatural status.
The default is 'valid'.

=item rank

This option filters the list of resulting taxa, returning only those that
match the given rank.  The value can be a single rank, or a list.  In the
latter case, each item can be either a single rank, or a list of [min, max].

=back

=cut

sub getTaxonIdTable {
    
    my ($self, $base_taxon, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $base_no;
    
    if ( ref $base_taxon )
    {
	croak "could not determine taxon_no from base_taxon" unless
	    exists $base_taxon->{taxon_no} || exists $base_taxon->{orig_no};
	
	$base_no = $base_taxon->{taxon_no} if defined $base_taxon->{taxon_no};
	$base_no = $base_taxon->{orig_no} if defined $base_taxon->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxon && $base_taxon > 0 )
    {
	$base_no = $base_taxon;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    my $rel = lc $parameter;
    
    # Prepare to fetch the requested information
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = lc($options->{status}) || 'valid';
    
    # Construct the appropriate modifiers based on the options.
    
    push my @filter_list, 'a2.taxon_no = ?' unless $rel eq 'all_taxa';
    
    if ( $status eq 'valid' )
    {
	push @filter_list, "o.status = 'belongs to'";
	push @filter_list, "t.orig_no = t.synonym_no";
    }
    elsif ( $status eq 'synonyms' ) {
	push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'invalid' ) {
	push @filter_list, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'all' ) {
	# no filter needed
    }
    else {
	croak "invalid value '$status' for option 'status'";
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter($options->{rank});
    }
    
    my $select = $options->{trad} ? 'trad' : 'spelling';
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    
    # Create a temporary table to hold the requested information.  If it fails
    # because there is already a table with the same name, increase the index
    # and try again.
    
    my $table_name = "TEMP_TT_$TEMP_TABLE_INDEX"; $TEMP_TABLE_INDEX++;
    my $success;
    
    while (!$success)
    {
	eval {
	    $success = $dbh->do("CREATE TEMPORARY TABLE $table_name 
		       (taxon_no int unsigned not null,
			UNIQUE KEY (taxon_no))");
	};
	
	if ( $@ =~ /'(\w+)' already exists/ and $1 eq $table_name )
	{
	    $table_name = "TEMP_TT_$TEMP_TABLE_INDEX"; $TEMP_TABLE_INDEX++;
	}
	
	elsif ( $@ )
	{
	    die $@;
	}
    }
    
    # select the appropriate list of taxon_no values into the table
    
    if ( $rel eq 'all_parents' )
    {
	$dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft <= t2.lft and t.rgt >= t2.lft
			JOIN $opinion_table as o on o.opinion_no = t.opinion_no
	        $filter_expr
		ORDER BY t.lft", undef, $base_no + 0);
    }
    
    elsif ( $rel eq 'all_children' )
    {
	$dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $opinion_table as o on o.opinion_no = t.opinion_no
	        $filter_expr
		ORDER BY t.lft", undef, $base_no + 0);
    }
    
    elsif ( $rel eq 'all_taxa' )
    {
	$dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no
		FROM $tree_table as t
			JOIN $opinion_table as o using (opinion_no)
		$filter_expr
		ORDER BY t.lft", undef, $base_no + 0);
    }
    
    else
    {
	$dbh->do("DROP TABLE IF EXISTS $table_name");
	croak "invalid relationship '$parameter'";
    }
    
    # Now count the result set.  If we've got anything, return the table name
    # and result count if we were called from list context, or the table name otherwise.
    
    my ($result_count) = $dbh->selectrow_array("SELECT count(*) FROM $table_name");
    
    if ( $result_count > 0 )
    {
	if ( wantarray )
	{
	    return ($table_name, $result_count);
	}
	
	else
	{
	    return $table_name;
	}
    }
    
    else
    {
	$dbh->do("DROP TABLE IF EXISTS $table_name");
	return;
    }
}


# #=head3 getHistory ( dbh, base_taxon, options )

# Returns a list of objects representing all taxa which have had the same
# conceptual meaning as the given taxon at some point in history.  These are
# sorted by publication date, and include some but not all synonyms (i.e. not
# invalid subgroups of the given taxon).

# =cut

# sub getHistory {
    
#     my ($self, $base_taxon, $options) = @_;
    
#     # Check arguments
    
#     unless ( defined $base_taxon && $base_taxon > 0 )
#     {
# 	return error("invalid base_taxon '$base_taxon'");
#     }
    
#     # Then get the orig_no corresponding to the taxon we were given
    
#     my ($orig_no) = getTaxonIdParametrized($self->{dbh}, $base_taxon, 'orig')
# 	or return error("taxon $base_taxon not found");
    
#     # Prepare to fetch the requested information
    
#     my $dbh = $self->{dbh};
#     my $tree_table = $self->{tree_table};
#     my $auth_table = $self->{auth_table};
#     my $opinion_table = $self->{opinion_table};
    
#     # Then chain backward to find all related taxa.  This may be a tree, not
#     # necessarily a linear list, so we do a breadth-first search with
#     # @related_id_list as the search queue.
    
#     my (@related_id_list) = $orig_no;
    
#     my ($backward_sth) = $dbh->prepare("
# 		SELECT t.orig_no
# 		FROM $tree_table as t JOIN $OPINION_CACHE o USING (opinion_no)
# 		WHERE o.status in ('subjective synonym of', 'objective synonym of',
# 				   'replaced by') and o.parent_no = ?")
	
# 	or return error("database error: " . $dbh->errstr);
    
#     foreach my $t (@related_id_list)
#     {
# 	push @related_id_list, $dbh->selectrow_array($backward_sth, undef, $t);
#     }
    
#     # Then chain forward to find all related taxa.
    
#     my ($forward_sth) = $dbh->prepare("
# 		SELECT o.parent_no
# 		FROM $tree_table as t JOIN $OPINION_CACHE o USING (opinion_no)
# 		WHERE o.status in ('subjective synonym of', 'objective synonym of',
# 				   'replaced by') and t.orig_no = ?")
    
# 	or return error("database error: " . $dbh->errstr);
    
#     my ($t) = $orig_no;
    
#     while ($t)
#     {
# 	($t) = $dbh->selectrow_array($forward_sth, undef, $t);
# 	push @related_id_list, $t if $t;
#     }
    
#     # Now, fetch all of the indicated taxa.
    
#     my ($taxon_filter) = '(' . join(', ', @related_id_list) . ')';
    
#     my (@taxa) = $dbh->selectall_arrayref("
# 		SELECT $INFO_EXPR,
# 			if(a.pubyr IS NOT NULL AND a.pubyr != '', a.pubyr, r.pubyr) as pubyr
# 		FROM $tree_table as t JOIN $AUTH_TABLE as a ON a.orig_no = t.orig_no
# 			LEFT JOIN $OPINION_CACHE as o USING (opinion_no)
# 			LEFT JOIN $REFS_TABLE as r USING (reference_no)
# 		WHERE t.orig_no in $taxon_filter
# 		ORDER BY pubyr ASC", { Slice => {} });
    
#     # If an error occurred, report it.
    
#     if ( $dbh->err )
#     {
# 	return error("database error: " . $dbh->errstr);
#     }
    
#     # Otherwise, we got results.  Bless all of the objects into the proper
#     # package and return the list.
    
#     else
#     {
# 	foreach my $t (@taxa)
# 	{
# 	    bless $t, "Taxon";
# 	}
	
# 	return @taxa;
#     } 
# }


1;

